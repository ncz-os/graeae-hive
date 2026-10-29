"""The doctor acts on a loop's failure outcome instead of requeueing blindly.

Operator 2026-10-29: "if it knows the failure outcome then the doctor should be
able to act on it better and we shouldn't be filling up the queue as much."
"""

from __future__ import annotations

import importlib.util
import json
import os
import pathlib
import unittest
from unittest import mock

os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")

from hive.workers import doctor, loop_outcome

NO_EVENTS = lambda _jid: None  # noqa: E731


def _row(jid="job-1", reason=None, tail="", kind="build:zeroclaw"):
    result = {"tail": tail}
    if reason:
        result["loop_result"] = {"teardown": {"reason": reason}}
    return {"id": jid, "kind": kind, "result": result, "parent_job_id": None, "eligible_kinds": ["zeroclaw"]}


class ClassifyTests(unittest.TestCase):
    def test_every_teardown_reason_maps_to_a_class(self):
        cases = {
            "approved": loop_outcome.APPROVED_UNMERGED,
            "reviewer_unavailable": loop_outcome.REVIEWER_UNAVAILABLE,
            "author_failed": loop_outcome.AUTHOR_FAILED,
            "timeout": loop_outcome.TIMEOUT,
            "crash": loop_outcome.CRASH,
            "exhausted": loop_outcome.EXHAUSTED,
            "nothing_to_commit": loop_outcome.NOTHING_TO_COMMIT,
        }
        for reason, want in cases.items():
            self.assertEqual(loop_outcome.classify(_row(reason=reason), NO_EVENTS), want, reason)

    def test_result_stored_as_json_string_is_read(self):
        row = _row()
        row["result"] = json.dumps({"loop_result": {"teardown": {"reason": "exhausted"}}})
        self.assertEqual(loop_outcome.classify(row, NO_EVENTS), loop_outcome.EXHAUSTED)

    def test_events_teardown_used_when_result_has_no_loop_result(self):
        events = lambda _j: [{"phase": "author_start"}, {"phase": "teardown", "reason": "reviewer_unavailable"}]  # noqa: E731
        self.assertEqual(loop_outcome.classify(_row(), events), loop_outcome.REVIEWER_UNAVAILABLE)

    def test_trail_without_teardown_is_a_crash(self):
        events = lambda _j: [{"phase": "loop_start"}, {"phase": "author_start"}]  # noqa: E731
        self.assertEqual(loop_outcome.classify(_row(), events), loop_outcome.CRASH)

    def test_text_fallback_and_unknown(self):
        self.assertEqual(
            loop_outcome.classify(_row(tail='"verdict": "reviewer_unavailable"'), NO_EVENTS),
            loop_outcome.REVIEWER_UNAVAILABLE,
        )
        self.assertEqual(loop_outcome.classify(_row(tail="boom"), NO_EVENTS), loop_outcome.UNKNOWN)

    def test_classify_never_raises(self):
        def boom(_jid):
            raise RuntimeError("mnemos down")

        self.assertEqual(loop_outcome.classify({"id": "x", "result": 5}, boom), loop_outcome.UNKNOWN)
        self.assertEqual(loop_outcome.classify({}, boom), loop_outcome.UNKNOWN)

    def test_mnemos_unreachable_is_none(self):
        with mock.patch.dict(os.environ, {"MNEMOS_URL": "http://127.0.0.1:9", "MNEMOS_TOKEN": "t"}):
            self.assertIsNone(loop_outcome.fetch_loop_events("job-1", timeout=0.5))


def _killed_row(jid="job-k", exit_code=143, pushed=False, status="committed_local_stopped", **extra):
    """A worker killed by a signal; the finalizer preserved commits in the workspace."""
    result = {
        "via": "hive_worker",
        "exit_code": exit_code,
        "error": "killed by signal SIGTERM (-15)",
        "finalizer": {"status": status, "committed": True, "pushed": pushed, "commits": ["abc123"]},
    }
    result.update(extra)
    return {
        "id": jid,
        "kind": "build:zeroclaw",
        "result": result,
        "parent_job_id": None,
        "eligible_kinds": ["zeroclaw"],
    }


def _clone_row(jid="job-c", where="result.error"):
    row = {
        "id": jid,
        "kind": "build:zeroclaw",
        "result": {"via": "hive_worker"},
        "parent_job_id": None,
        "eligible_kinds": ["zeroclaw"],
    }
    if where == "result.error":
        row["result"]["error"] = "clone_failed: fatal: unable to access remote: Connection timed out"
    elif where == "result.failure_reason":
        row["result"]["failure_reason"] = "clone_failed"
    else:
        row["error"] = "clone_failed: remote hung up"
    return row


class NewClassClassifyTests(unittest.TestCase):
    def test_exit_codes_143_and_137_are_killed_by_signal(self):
        for code in (143, 137):
            row = {"id": "k", "result": {"exit_code": code}}
            self.assertEqual(loop_outcome.classify(row, NO_EVENTS), loop_outcome.KILLED_BY_SIGNAL, code)

    def test_real_killed_shape_with_local_finalizer(self):
        self.assertEqual(loop_outcome.classify(_killed_row(), NO_EVENTS), loop_outcome.KILLED_BY_SIGNAL)

    def test_finalizer_local_stopped_alone_is_killed_by_signal(self):
        row = {"id": "k", "result": {"finalizer": {"status": "committed_local_stopped", "pushed": False}}}
        self.assertEqual(loop_outcome.classify(row, NO_EVENTS), loop_outcome.KILLED_BY_SIGNAL)

    def test_text_fallback_for_signals(self):
        for text in ("worker killed by signal 15", "got SIGTERM from systemd", "SIGKILL (oom)"):
            row = {"id": "k", "result": {"error": text}}
            self.assertEqual(loop_outcome.classify(row, NO_EVENTS), loop_outcome.KILLED_BY_SIGNAL, text)

    def test_signal_wins_over_a_trail_without_teardown(self):
        events = lambda _j: [{"phase": "loop_start"}, {"phase": "author_start"}]  # noqa: E731
        self.assertEqual(loop_outcome.classify(_killed_row(), events), loop_outcome.KILLED_BY_SIGNAL)

    def test_explicit_exit_code_other_than_signal_is_not_killed(self):
        row = {"id": "k", "result": {"exit_code": 1, "tail": "boom"}}
        self.assertEqual(loop_outcome.classify(row, NO_EVENTS), loop_outcome.UNKNOWN)
        pushed = _killed_row(exit_code=1, pushed=True, status="committed_pushed")
        pushed["result"]["error"] = "tests failed"
        self.assertEqual(loop_outcome.classify(pushed, NO_EVENTS), loop_outcome.UNKNOWN)

    def test_clone_failed_from_every_field(self):
        for where in ("result.error", "result.failure_reason", "row.error"):
            self.assertEqual(
                loop_outcome.classify(_clone_row(where=where), NO_EVENTS), loop_outcome.CLONE_FAILED, where
            )

    def test_clone_failed_must_start_the_text(self):
        row = {"id": "c", "result": {"error": "retry after clone_failed earlier"}}
        self.assertEqual(loop_outcome.classify(row, NO_EVENTS), loop_outcome.UNKNOWN)

    def test_malformed_new_fields_never_raise(self):
        for res in (
            {"exit_code": "143"},
            {"exit_code": None, "finalizer": "committed_local_stopped"},
            {"finalizer": ["x"], "failure_reason": 7, "error": {"a": 1}},
            {"exit_code": True},
        ):
            self.assertEqual(loop_outcome.classify({"id": "m", "result": res}, NO_EVENTS), loop_outcome.UNKNOWN, res)
        self.assertFalse(loop_outcome.has_local_only_commits({"result": 5}))
        self.assertFalse(loop_outcome.has_local_only_commits({}))

    def test_local_only_commits_detection(self):
        self.assertTrue(loop_outcome.has_local_only_commits(_killed_row()))
        self.assertTrue(loop_outcome.has_local_only_commits(_killed_row(pushed=None)))
        self.assertFalse(loop_outcome.has_local_only_commits(_killed_row(pushed=True)))
        self.assertFalse(loop_outcome.has_local_only_commits(_killed_row(status="committed_pushed")))


class HealthTests(unittest.TestCase):
    def test_no_configured_url_means_nothing_to_probe(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DOCTOR_REVIEWER_HEALTH_URL", None)
            self.assertTrue(loop_outcome.reviewer_healthy())

    def test_unreachable_configured_url_is_unhealthy(self):
        with mock.patch.dict(os.environ, {"DOCTOR_REVIEWER_HEALTH_URL": "http://127.0.0.1:9/health"}):
            self.assertFalse(loop_outcome.reviewer_healthy(timeout=0.5))


class DecideTests(unittest.TestCase):
    def test_reviewer_unavailable_waits_for_a_healthy_reviewer(self):
        self.assertEqual(loop_outcome.decide("reviewer_unavailable", 0, lambda: False), "hold")
        self.assertEqual(loop_outcome.decide("reviewer_unavailable", 0, lambda: True), "requeue")

    def test_requeue_cap_then_needs_review(self):
        cap = loop_outcome.MAX_AUTO_REQUEUES
        self.assertEqual(loop_outcome.decide("reviewer_unavailable", cap, lambda: True), "needs_review")
        self.assertEqual(loop_outcome.decide("author_failed", cap, lambda: True), "needs_review")
        self.assertEqual(loop_outcome.decide("author_failed", cap - 1, lambda: True), "requeue")

    def test_human_classes_and_unknown(self):
        self.assertEqual(loop_outcome.decide("exhausted_request_changes", 0), "needs_review")
        self.assertEqual(loop_outcome.decide("nothing_to_commit", 0), "needs_review")
        self.assertEqual(loop_outcome.decide("approved_unmerged", 0), "skip")
        self.assertEqual(loop_outcome.decide("unknown", 0), "default")


class NewClassDecideTests(unittest.TestCase):
    def test_killed_by_signal_requeues_up_to_the_cap(self):
        cap = loop_outcome.MAX_AUTO_REQUEUES
        self.assertEqual(loop_outcome.decide("killed_by_signal", 0), "requeue")
        self.assertEqual(loop_outcome.decide("killed_by_signal", cap - 1), "requeue")
        self.assertEqual(loop_outcome.decide("killed_by_signal", cap), "needs_review")

    def test_killed_with_local_only_commits_is_never_requeued(self):
        for n in range(0, loop_outcome.MAX_AUTO_REQUEUES + 2):
            self.assertEqual(loop_outcome.decide("killed_by_signal", n, local_commits_only=True), "needs_review", n)

    def test_clone_failed_requeues_up_to_the_cap(self):
        cap = loop_outcome.MAX_AUTO_REQUEUES
        self.assertEqual(loop_outcome.decide("clone_failed", 0), "requeue")
        self.assertEqual(loop_outcome.decide("clone_failed", cap - 1), "requeue")
        self.assertEqual(loop_outcome.decide("clone_failed", cap), "needs_review")


class _FakeState:
    def __init__(self):
        self.marks = {}

    def last_mark(self, key):
        return self.marks.get(key)

    def mark(self, key, ttl=None):
        self.marks[key] = 1.0

    def seen(self, key, ttl):
        return key in self.marks


def _sweep(failed, healthy=True, state=None):
    calls = {"resubmit": [], "exclude": [], "parked": []}
    state = state or _FakeState()

    def fake_http(method, path, payload=None, timeout=10.0):
        if path.startswith("/v1/jobs?status=failed"):
            return 200, {"jobs": failed}
        if path.startswith("/v1/jobs?status=queued"):
            return 200, {"jobs": []}
        if path.startswith("/v1/agents"):
            return 200, {"agents": [{"kind": "zeroclaw"}]}
        if path == "/v1/admin/jobs/reclassify":
            calls["parked"].append(payload["job_ids"][0])
        return 200, {}

    def fake_resubmit(base_kind, max_resubmits=None, exclude_ids=None):
        calls["resubmit"].append(base_kind)
        calls["exclude"].append(set(exclude_ids or ()))
        return "requeued 1 jobs of x (api=200)"

    with (
        mock.patch.object(doctor, "_http", fake_http),
        mock.patch.object(doctor, "dlq_protected", lambda j: False),
        mock.patch.object(doctor, "action_resubmit_jobs", fake_resubmit),
        mock.patch.object(doctor, "STATE", state),
        mock.patch.object(doctor, "broadcast_doctor_message", lambda *a, **k: None),
        mock.patch.object(loop_outcome, "reviewer_healthy", lambda *a, **k: healthy),
        mock.patch.object(loop_outcome, "fetch_loop_events", NO_EVENTS),
    ):
        out = doctor.sweep_orphan_failures()
    return out, calls, state


class SweepTests(unittest.TestCase):
    def test_reviewer_down_holds_the_row_and_does_not_requeue(self):
        out, calls, _ = _sweep([_row(reason="reviewer_unavailable")], healthy=False)
        self.assertEqual(calls["resubmit"], [])
        self.assertEqual(calls["parked"], [])
        self.assertIn("reviewer_unavailable->hold", out["outcomes"].values())

    def test_reviewer_healthy_requeues_and_counts_toward_the_cap(self):
        row = _row(reason="reviewer_unavailable")
        state = _FakeState()
        for expected in (1, 2):
            _, calls, state = _sweep([row], healthy=True, state=state)
            self.assertEqual(calls["resubmit"], ["build:zeroclaw"])
            with mock.patch.object(doctor, "STATE", state):
                self.assertEqual(doctor._loop_requeue_count("job-1"), expected)
            for key in [k for k in state.marks if k.startswith("orphan_resubmit:")]:
                state.marks.pop(key)  # cooldown elapsed
        # third pass: cap reached -> parked, not requeued
        with mock.patch.object(doctor, "STATE", state):
            self.assertEqual(doctor._loop_requeue_count("job-1"), loop_outcome.MAX_AUTO_REQUEUES)
        _, calls, state = _sweep([row], healthy=True, state=state)
        self.assertEqual(calls["resubmit"], [])
        self.assertEqual(calls["parked"], ["job-1"])

    def test_exhausted_is_parked_once_and_never_requeued(self):
        row = _row(reason="exhausted")
        _, calls, state = _sweep([row])
        self.assertEqual(calls["parked"], ["job-1"])
        self.assertEqual(calls["resubmit"], [])
        _, calls2, _ = _sweep([row], state=state)
        self.assertEqual(calls2["parked"], [])  # already parked: no repeat

    def test_other_rows_of_the_same_kind_are_still_requeued_and_held_ones_excluded(self):
        held = _row(jid="held", reason="exhausted")
        ok = _row(jid="ok", reason="author_failed")
        _, calls, _ = _sweep([held, ok])
        self.assertEqual(calls["resubmit"], ["build:zeroclaw"])
        self.assertIn("held", calls["exclude"][0])
        self.assertNotIn("ok", calls["exclude"][0])

    def test_unknown_outcome_keeps_todays_behaviour(self):
        _, calls, _ = _sweep([_row(tail="boom")])
        self.assertEqual(calls["resubmit"], ["build:zeroclaw"])


class NewClassSweepTests(unittest.TestCase):
    def test_killed_with_local_commits_is_parked_and_never_requeued(self):
        row = _killed_row()
        out, calls, state = _sweep([row])
        self.assertEqual(calls["parked"], ["job-k"])
        self.assertEqual(calls["resubmit"], [])
        self.assertIn("killed_by_signal->needs_review", out["outcomes"].values())
        # still parked, never requeued, on later passes
        _, calls2, _ = _sweep([row], state=state)
        self.assertEqual(calls2["resubmit"], [])

    def test_killed_with_pushed_work_is_requeued_then_capped(self):
        row = _killed_row(pushed=True, status="committed_pushed")
        state = _FakeState()
        for _ in (1, 2):
            _, calls, state = _sweep([row], state=state)
            self.assertEqual(calls["resubmit"], ["build:zeroclaw"])
            self.assertEqual(calls["parked"], [])
            for key in [k for k in state.marks if k.startswith("orphan_resubmit:")]:
                state.marks.pop(key)
        _, calls, state = _sweep([row], state=state)
        self.assertEqual(calls["resubmit"], [])
        self.assertEqual(calls["parked"], ["job-k"])

    def test_killed_without_a_finalizer_is_requeued(self):
        row = {"id": "job-k", "kind": "build:zeroclaw", "parent_job_id": None, "result": {"exit_code": 137}}
        _, calls, _ = _sweep([row])
        self.assertEqual(calls["resubmit"], ["build:zeroclaw"])
        self.assertEqual(calls["parked"], [])

    def test_clone_failed_requeues_then_parks_at_the_cap(self):
        row = _clone_row()
        state = _FakeState()
        for expected in (1, 2):
            out, calls, state = _sweep([row], state=state)
            self.assertEqual(calls["resubmit"], ["build:zeroclaw"])
            self.assertIn("clone_failed->requeue", out["outcomes"].values())
            with mock.patch.object(doctor, "STATE", state):
                self.assertEqual(doctor._loop_requeue_count("job-c"), expected)
            for key in [k for k in state.marks if k.startswith("orphan_resubmit:")]:
                state.marks.pop(key)
        _, calls, _ = _sweep([row], state=state)
        self.assertEqual(calls["resubmit"], [])
        self.assertEqual(calls["parked"], ["job-c"])

    def test_class_counts_cover_every_row_seen(self):
        rows = [
            _killed_row(jid="k1"),
            _killed_row(jid="k2", pushed=True, status="committed_pushed"),
            _clone_row(jid="c1"),
            _row(jid="e1", reason="exhausted"),
            _row(jid="u1", tail="boom"),
            _row(jid="u2", tail="boom"),
            dict(_row(jid="child", tail="boom"), parent_job_id="p"),  # skipped: not seen
        ]
        out, _, _ = _sweep(rows)
        self.assertEqual(
            out["class_counts"],
            {"killed_by_signal": 2, "clone_failed": 1, "exhausted_request_changes": 1, "unknown": 2},
        )
        self.assertEqual(sum(out["class_counts"].values()), 6)

    def test_class_counts_empty_when_nothing_failed(self):
        out, _, _ = _sweep([])
        self.assertEqual(out["class_counts"], {})

    def test_class_counts_are_logged(self):
        with self.assertLogs(doctor.log, level="INFO") as cap:
            _sweep([_clone_row()])
        self.assertTrue(any("class_counts" in line and "clone_failed" in line for line in cap.output))


class ClusterTests(unittest.TestCase):
    def test_cluster_of_known_outcomes_creates_no_triage(self):
        rows = [dict(_row(jid=f"j{i}", reason="reviewer_unavailable"), ended_at=9e12) for i in range(6)]
        with (
            mock.patch.object(doctor, "_http", lambda *a, **k: (200, {"jobs": rows})),
            mock.patch.object(doctor, "is_protection_violation", lambda j: False),
            mock.patch.object(doctor, "dlq_protected", lambda j: False),
            mock.patch.object(doctor, "CLUSTER_THRESHOLD", 3),
        ):
            self.assertEqual(doctor.scan_for_failure_clusters(), [])

    def test_cluster_with_an_unknown_failure_still_triages(self):
        rows = [dict(_row(jid=f"j{i}", tail="boom"), ended_at=9e12) for i in range(6)]
        with (
            mock.patch.object(doctor, "_http", lambda *a, **k: (200, {"jobs": rows})),
            mock.patch.object(doctor, "is_protection_violation", lambda j: False),
            mock.patch.object(doctor, "dlq_protected", lambda j: False),
            mock.patch.object(doctor, "cooldown_active", lambda *a, **k: False),
            mock.patch.object(doctor, "triaged_through", lambda *a, **k: False),
            mock.patch.object(doctor, "_active_triage_exists", lambda bk: False),
            mock.patch.object(doctor, "CLUSTER_THRESHOLD", 3),
        ):
            self.assertEqual(len(doctor.scan_for_failure_clusters()), 1)


class WorkerParseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = pathlib.Path(__file__).parent / "deploy" / "hive_worker.py"
        spec = importlib.util.spec_from_file_location("hive_worker_under_test", path)
        cls.mod = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(cls.mod)
        except BaseException as exc:  # noqa: BLE001 - worker import needs env on some hosts
            raise unittest.SkipTest(f"hive_worker not importable here: {exc}")

    def test_last_result_line_wins_and_garbage_is_ignored(self):
        out = "\n".join(
            [
                "noise",
                'ZODER_LOOP_RESULT {"teardown": {"reason": "exhausted"}}',
                "ZODER_LOOP_RESULT {not json",
                'ZODER_LOOP_RESULT {"teardown": {"reason": "approved"}, "verdicts": ["approve"]}',
            ]
        )
        got = self.mod.parse_zoder_loop_result(out)
        self.assertEqual(got["teardown"]["reason"], "approved")
        self.assertIsNone(self.mod.parse_zoder_loop_result("nothing here"))
        self.assertIsNone(self.mod.parse_zoder_loop_result(None))
        self.assertIsNone(self.mod.parse_zoder_loop_result('ZODER_LOOP_RESULT {"x": 1}'))


if __name__ == "__main__":
    unittest.main()
