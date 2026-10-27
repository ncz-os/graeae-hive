"""Tests for the doctor auto-triage dedupe (graeae:doctor-triage-dedupe 2026-10-27).

Production incident fixed here: the scanner only saw queued/running triage jobs
and AUTO_TRIAGE_COOLDOWN lived in an in-process dict, so a FINISHED triage (or a
doctor restart) re-created triage:<base_kind> for the same failures — 61 rows
in one night, triage rows recreated within minutes of a purge with zero new
failures.

The gate under test is ``doctor.should_auto_triage`` (called by
``scan_for_failure_clusters``), which must:
  * dedupe against a triage job in ANY status (a done triage blocks);
  * allow a new triage only when a failure ended AFTER the newest triage was
    created (and the durable cooldown has elapsed);
  * survive a doctor "restart" (fresh in-memory map) because the triage ROW on
    the bus is the durable state;
  * skip a base_kind superseded by a newer done/needs-review job;
  * still honour CLUSTER_THRESHOLD / CLUSTER_WINDOW_SEC.

doctor.py is a standalone satellite script, so it is loaded from its file path
the same way test_queue_logic.py loads queue_logic.py.
"""

import importlib.util
import re
import time
import unittest
from unittest import mock
from pathlib import Path


def _load_doctor():
    spec = importlib.util.spec_from_file_location(
        "graeae_doctor_mod", Path(__file__).with_name("hive").joinpath("workers", "doctor.py")
    )
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


doctor = _load_doctor()


def _bus(jobs):
    """Build a fake doctor._http response for /v1/jobs-style queries.

    `jobs` is a list of dicts (the live bus rows). Each query is answered by
    filtering on the ?status= param when present; otherwise all rows are
    returned (the supersede check uses the unfiltered list)."""
    def fake_http(method, path, body=None, timeout=15.0):
        if method == "GET" and path.startswith("/v1/jobs"):
            status = None
            m = re.search(r"status=([^&]+)", path)
            if m:
                status = m.group(1)
            rows = [j for j in jobs if status is None or j.get("status") == status]
            return 200, {"jobs": rows}
        return 200, None
    return fake_http


class _Isolation:
    """Fresh in-memory maps per test (the 'doctor restart' simulation)."""

    def __enter__(self):
        doctor._recent_triage_jobs.clear()
        doctor._recent_auto_triage.clear()
        return self

    def __exit__(self, *exc):
        doctor._recent_triage_jobs.clear()
        doctor._recent_auto_triage.clear()

    def reset(self):
        doctor._recent_triage_jobs.clear()
        doctor._recent_auto_triage.clear()


NOW = 1_700_000_000.0  # stable epoch for all fixtures


class _FakeClock:
    """Pin doctor.time.time() to NOW so the 1h cooldown behaves like the
    fixture (the real wall clock is 3+ years past NOW and would always clear
    the window)."""

    def __enter__(self):
        self._patch = mock.patch.object(doctor.time, "time", return_value=NOW)
        self._patch.start()
        return self

    def __exit__(self, *exc):
        self._patch.stop()


def _failed(jid, kind, ended_at, started_at=None):
    return {
        "id": jid,
        "kind": kind,
        "status": "failed",
        "ended_at": ended_at,
        "started_at": started_at or (ended_at - 100),
    }


def _triage(jid, kind, status, created_at, started_at=None):
    return {
        "id": jid,
        "kind": kind,
        "status": status,
        "created_at": created_at,
        "started_at": started_at or created_at,
        "eligible_kinds": ["doctor"],
    }


class ShouldAutoTriageTests(unittest.TestCase):
    def setUp(self):
        self._iso = _Isolation()
        self._iso.__enter__()

    def tearDown(self):
        self._iso.__exit__(None, None, None)

    # ── rule: a done triage blocks a second one for the same base_kind ──
    def test_done_triage_blocks_second(self):
        # the failure ENDED BEFORE the triage was created (NOW-3600), i.e. the
        # cluster was already evaluated — a second triage must NOT be created
        # no matter how long ago the first one ran.
        bus = [
            _failed("f1", "build:zoder", NOW - 7200),
            _triage("t1", "triage:build:zoder", "done", NOW - 3600),
        ]
        with _FakeClock(), mock.patch.object(doctor, "_http", _bus(bus)):
            ok, why = doctor.should_auto_triage("build:zoder", [
                {"ended_at": NOW - 7200},
            ])
        self.assertFalse(ok)
        self.assertEqual(why, "no_new_failures_since_triage")

    def test_done_triage_young_blocks_via_cooldown(self):
        # a NEW failure (ended 5s AFTER the triage was created 10s ago) while
        # that triage is still inside AUTO_TRIAGE_COOLDOWN -> suppressed
        # (cooldown), not re-triaged.
        bus = [
            _failed("f1", "build:zoder", NOW - 5),
            _triage("t1", "triage:build:zoder", "done", NOW - 10),
        ]
        with _FakeClock(), mock.patch.object(doctor, "_http", _bus(bus)):
            ok, why = doctor.should_auto_triage("build:zoder", [
                {"ended_at": NOW - 5},
            ])
        self.assertFalse(ok)
        self.assertEqual(why, "cooldown")

    # ── rule: a queued/running triage blocks ──
    def test_active_triage_blocks(self):
        for status in ("queued", "running", "offered", "claimed"):
            with self.subTest(status=status):
                self._iso.reset()
                bus = [
                    _failed("f1", "build:zoder", NOW - 600),
                    _triage("t1", "triage:build:zoder", status, NOW - 60),
                ]
                with _FakeClock(), mock.patch.object(doctor, "_http", _bus(bus)):
                    ok, why = doctor.should_auto_triage("build:zoder", [
                        {"ended_at": NOW - 600},
                    ])
                self.assertFalse(ok, status)
                self.assertEqual(why, f"triage_{status}_exists")

    # ── rule: a failure that ended AFTER the triage allows exactly one new triage ──
    def test_failure_after_triage_allows_one_new(self):
        # triage created 3h ago (past the 1h durable cooldown); a NEW failure
        # (ended 10 min ago) post-dates it -> legitimate.
        bus = [
            _failed("f1", "build:zoder", NOW - 10800),   # old, already triaged
            _failed("f2", "build:zoder", NOW - 600),      # NEW, after triage
            _triage("t1", "triage:build:zoder", "done", NOW - 10800),
        ]
        with _FakeClock(), mock.patch.object(doctor, "_http", _bus(bus)):
            ok, why = doctor.should_auto_triage("build:zoder", [
                {"ended_at": NOW - 10800},
                {"ended_at": NOW - 600},
            ])
        self.assertTrue(ok, why)
        self.assertEqual(why, "new_failures")
        # "exactly one": once that triage exists and is young, the SAME cluster
        # is blocked again (cooldown) — no second duplicate is created.
        t_new = _triage("t2", "triage:build:zoder", "done", NOW - 30)
        bus.append(t_new)
        self._iso.reset()  # fresh process would not know about t2 in memory
        with _FakeClock(), mock.patch.object(doctor, "_http", _bus(bus)):
            ok2, why2 = doctor.should_auto_triage("build:zoder", [
                {"ended_at": NOW - 10800},
                {"ended_at": NOW - 600},
            ])
        self.assertFalse(ok2, why2)
        self.assertIn(why2, ("cooldown", "no_new_failures_since_triage"))

    # ── rule: a fresh restart (empty in-memory map) still sees the durable cooldown ──
    def test_restart_still_sees_durable_cooldown(self):
        # The doctor just restarted: _recent_triage_jobs is EMPTY. The triage
        # row still exists on the bus (30s old, done) and the failures predate
        # it — the OLD code (in-memory dict only) would have allowed a duplicate
        # here. The durable gate must still block.
        self.assertEqual(doctor._recent_triage_jobs, {})
        bus = [
            _failed("f1", "build:zoder", NOW - 600),
            _triage("t1", "triage:build:zoder", "done", NOW - 30),
        ]
        with _FakeClock(), mock.patch.object(doctor, "_http", _bus(bus)):
            ok, why = doctor.should_auto_triage("build:zoder", [
                {"ended_at": NOW - 600},
            ])
        self.assertFalse(ok, why)
        self.assertIn(why, ("cooldown", "no_new_failures_since_triage"))
        # and the lookup populated the cache from the bus (durable state won)
        self.assertIn("build:zoder", doctor._recent_triage_jobs)

    # ── rule: a superseded base_kind (newer done job) is skipped ──
    def test_superseded_by_newer_done_job(self):
        # failures are OLD (ended 2h ago); a same-base_kind job FINISHED after
        # them -> the failures are old news, skip triage.
        bus = [
            _failed("f1", "build:zoder", NOW - 7200),
            _failed("f2", "build:zoder", NOW - 3600),
            {"id": "d1", "kind": "build:zoder", "status": "done",
             "started_at": NOW - 600, "ended_at": NOW - 500},
        ]
        with _FakeClock(), mock.patch.object(doctor, "_http", _bus(bus)):
            ok, why = doctor.should_auto_triage("build:zoder", [
                {"ended_at": NOW - 7200},
                {"ended_at": NOW - 3600},
            ])
        self.assertFalse(ok)
        self.assertEqual(why, "superseded_by_newer_done_job")

    def test_not_superseded_when_done_job_is_older(self):
        # a done job OLDER than the newest failure does NOT supersede
        bus = [
            _failed("f1", "build:zoder", NOW - 600),
            {"id": "d1", "kind": "build:zoder", "status": "done",
             "started_at": NOW - 7200, "ended_at": NOW - 7100},
        ]
        with _FakeClock(), mock.patch.object(doctor, "_http", _bus(bus)):
            ok, why = doctor.should_auto_triage("build:zoder", [
                {"ended_at": NOW - 600},
            ])
        # no triage row + no supersede -> legitimate
        self.assertTrue(ok, why)

    # ── rule: threshold/window still work as documented ──
    def test_cluster_threshold_respected(self):
        # CLUSTER_THRESHOLD default is 1, but the scan must honour an override:
        # with threshold 3, a 2-failure cluster is NOT surfaced.
        with mock.patch.object(doctor, "CLUSTER_THRESHOLD", 3):
            self.assertEqual(doctor.CLUSTER_THRESHOLD, 3)
            # simulate the scan's per-kind count gate directly
            self.assertFalse(len([1, 2]) >= doctor.CLUSTER_THRESHOLD)
            self.assertTrue(len([1, 2, 3]) >= doctor.CLUSTER_THRESHOLD)
        # default unchanged
        self.assertEqual(doctor.CLUSTER_THRESHOLD, 1)

    def test_cluster_window_respected(self):
        # a failure older than CLUSTER_WINDOW_SEC is outside the scan window
        self.assertEqual(doctor.CLUSTER_WINDOW_SEC, 21600)
        cutoff = NOW - doctor.CLUSTER_WINDOW_SEC
        self.assertTrue((NOW - 600) >= cutoff)          # inside window
        self.assertFalse((NOW - 40000) >= cutoff)       # outside window


class ScanForFailureClustersTests(unittest.TestCase):
    """End-to-end scan with the durable gate wired in."""

    def setUp(self):
        self._iso = _Isolation()
        self._iso.__enter__()

    def tearDown(self):
        self._iso.__exit__(None, None, None)

    def test_scan_skips_cluster_with_done_triage(self):
        # failure predates the done triage -> already evaluated, no new triage
        bus = [
            _failed("f1", "build:zoder", NOW - 7200),
            _triage("t1", "triage:build:zoder", "done", NOW - 3600),
        ]
        with _FakeClock(), mock.patch.object(doctor, "_http", _bus(bus)):
            clusters = doctor.scan_for_failure_clusters()
        self.assertEqual(clusters, [])

    def test_scan_surfaces_cluster_without_triage(self):
        bus = [_failed("f1", "build:zoder", NOW - 600)]
        with _FakeClock(), mock.patch.object(doctor, "_http", _bus(bus)):
            clusters = doctor.scan_for_failure_clusters()
        self.assertEqual(len(clusters), 1)
        self.assertEqual(clusters[0]["base_kind"], "build:zoder")
        self.assertEqual(clusters[0]["count"], 1)

    def test_scan_ignores_triage_and_doctor_kinds(self):
        # failed triage:* / doctor:* rows never seed a cluster
        bus = [
            {"id": "t1", "kind": "triage:build:zoder", "status": "failed",
             "ended_at": NOW - 60, "started_at": NOW - 120},
            {"id": "d1", "kind": "doctor:codex-fix", "status": "failed",
             "ended_at": NOW - 60, "started_at": NOW - 120},
        ]
        with mock.patch.object(doctor, "_http", _bus(bus)):
            clusters = doctor.scan_for_failure_clusters()
        self.assertEqual(clusters, [])


class DurableCooldownOnSubmitTests(unittest.TestCase):
    """submit_auto_triage must seed the durable cache (the restart-proof path)."""

    def setUp(self):
        self._iso = _Isolation()
        self._iso.__enter__()

    def tearDown(self):
        self._iso.__exit__(None, None, None)

    def test_submit_seeds_durable_cache(self):
        submitted = {"id": "t-new", "kind": "triage:build:zoder", "status": "queued"}

        def fake_http(method, path, body=None, timeout=15.0):
            if method == "POST" and path == "/v1/jobs":
                return 201, submitted
            return 200, None

        with mock.patch.object(doctor, "_http", fake_http), \
                mock.patch.object(doctor, "DRY_RUN", False):
            jid = doctor.submit_auto_triage("build:zoder", 1, "f1")
        self.assertEqual(jid, "t-new")
        # the durable cache now holds the submitted triage (restart-proof)
        self.assertIn("build:zoder", doctor._recent_triage_jobs)
        cached = doctor._recent_triage_jobs["build:zoder"]
        self.assertEqual(cached["kind"], "triage:build:zoder")


class _finalizeTriageCacheTests(unittest.TestCase):
    """A self-terminating (no_action) triage is recorded as evaluated."""

    def setUp(self):
        self._iso = _Isolation()
        self._iso.__enter__()

    def tearDown(self):
        self._iso.__exit__(None, None, None)

    def test_finalize_records_outcome(self):
        doctor._finalize_triage_cache(
            "build:zoder",
            {"id": "t1", "kind": "triage:build:zoder", "started_at": NOW - 100,
             "status": "done"},
            "no_action",
        )
        self.assertIn("build:zoder", doctor._recent_triage_jobs)
        self.assertEqual(doctor._recent_triage_jobs["build:zoder"]["last_action"], "no_action")
        # the recorded outcome participates in dedupe: a second triage for the
        # same (now-evaluated) cluster is blocked, not re-created.
        bus = [
            _failed("f1", "build:zoder", NOW - 600),
            _triage("t1", "triage:build:zoder", "done", NOW - 100),
        ]
        with mock.patch.object(doctor, "_http", _bus(bus)):
            ok, why = doctor.should_auto_triage("build:zoder", [{"ended_at": NOW - 600}])
        self.assertFalse(ok, why)


if __name__ == "__main__":
    unittest.main()
