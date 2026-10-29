"""The needs-review sweep reports evidence and a suggested action, and writes nothing.

docs/NEEDS_REVIEW_POLICY.md section 6, step 1: before any writer exists the
doctor must say, per needs-review row, whether the work is already merged,
whether a newer job on the same head branch superseded it, and whether the
doctor itself parked it. No network: every evidence source is injected.
"""

from __future__ import annotations

import os
import unittest
from unittest import mock

os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")

from hive.workers import doctor

NOW = 2_000_000_000.0
HOUR = 3600.0


class _FakeState:
    """Same surface the doctor uses; records every write so tests can see none."""

    def __init__(self, marks=None):
        self.marks = dict(marks or {})
        self.writes = []

    def last_mark(self, key):
        return self.marks.get(key)

    def mark(self, key, ttl=None):
        self.writes.append(("mark", key))
        self.marks[key] = NOW

    def touch(self, key, *a, **k):
        self.writes.append(("touch", key))

    def seen(self, key, ttl):
        return key in self.marks


def _row(jid="job-1", head="feat/x", created=NOW - 100 * HOUR, repo="graeae-hive", **extra):
    row = {
        "id": jid,
        "status": "needs-review",
        "kind": "build:zeroclaw",
        "repo": repo,
        "head_branch": head,
        "base_branch": "main",
        "created_at": created,
        "updated_at": NOW - 90 * HOUR,
    }
    row.update(extra)
    return row


def _other(jid, status, created, head="feat/x", repo="graeae-hive"):
    return {"id": jid, "status": status, "created_at": created, "head_branch": head, "repo": repo}


def _gather(job, merged=False, branch_jobs=(), state=None):
    merged_fn = merged if callable(merged) else (lambda _j: merged)
    jobs_fn = branch_jobs if callable(branch_jobs) else (lambda _j: list(branch_jobs))
    evidence = doctor.gather_needs_review_evidence(
        job, merged_fn=merged_fn, branch_jobs_fn=jobs_fn, state=state or _FakeState(), now=NOW
    )
    return evidence, doctor.needs_review_suggestion(evidence)


class SuggestionTests(unittest.TestCase):
    def test_merged_branch_is_safe_to_cancel(self):
        evidence, action = _gather(_row(), merged=True)
        self.assertIs(evidence["merged"], True)
        self.assertEqual(action, "merged: safe to cancel")

    def test_running_or_done_newer_job_supersedes(self):
        for status in ("running", "done"):
            newer = _other("job-new", status, NOW - 10 * HOUR)
            evidence, action = _gather(_row(), branch_jobs=[newer])
            self.assertEqual(evidence["superseded_by"], "job-new", status)
            self.assertEqual(action, "superseded by job-new: safe to cancel", status)

    def test_newest_superseder_is_named_and_ties_are_deterministic(self):
        jobs = [
            _other("a", "done", NOW - 30 * HOUR),
            _other("c", "running", NOW - 10 * HOUR),
            _other("b", "done", NOW - 10 * HOUR),
        ]
        for ordering in (jobs, list(reversed(jobs))):
            evidence, _ = _gather(_row(), branch_jobs=ordering)
            self.assertEqual(evidence["superseded_by"], "c")

    def test_jobs_that_do_not_supersede(self):
        row = _row()
        cases = {
            "older job": [_other("old", "done", NOW - 200 * HOUR)],
            "newer but failed": [_other("f", "failed", NOW - 10 * HOUR)],
            "newer but queued": [_other("q", "queued", NOW - 10 * HOUR)],
            "newer but needs-review": [_other("n", "needs-review", NOW - 10 * HOUR)],
            "other branch": [_other("o", "done", NOW - 10 * HOUR, head="feat/y")],
            "other repo": [_other("r", "done", NOW - 10 * HOUR, repo="other-repo")],
            "itself": [dict(row, status="done", created_at=NOW)],
        }
        for name, jobs in cases.items():
            evidence, action = _gather(row, branch_jobs=jobs)
            self.assertIsNone(evidence["superseded_by"], name)
            self.assertTrue(action.startswith("needs a human: "), name)

    def test_merged_wins_over_superseded(self):
        newer = _other("job-new", "done", NOW - 10 * HOUR)
        _, action = _gather(_row(), merged=True, branch_jobs=[newer])
        self.assertEqual(action, "merged: safe to cancel")

    def test_needs_a_human_when_nothing_proves_it(self):
        _, action = _gather(_row())
        self.assertEqual(action, "needs a human: not merged and not superseded")

    def test_doctor_parked_row_reports_its_age(self):
        state = _FakeState({"loop_park:job-1": NOW - 50 * HOUR - 5})
        evidence, action = _gather(_row(), state=state)
        self.assertIs(evidence["doctor_parked"], True)
        self.assertEqual(evidence["parked_age_sec"], 50 * HOUR + 5)
        self.assertEqual(action, "needs a human: not merged and not superseded; parked by the doctor 50h ago")

    def test_row_the_doctor_did_not_park(self):
        evidence, _ = _gather(_row())
        self.assertIs(evidence["doctor_parked"], False)
        self.assertIsNone(evidence["parked_age_sec"])

    def test_row_without_head_branch_needs_a_human(self):
        calls = []
        evidence, action = _gather(
            _row(head=None), merged=lambda j: calls.append(j) or True, branch_jobs=lambda j: calls.append(j) or []
        )
        self.assertEqual(calls, [])  # nothing to look up for a row with no branch
        self.assertEqual(action, "needs a human: row has no head branch to check")
        self.assertIn("head_branch", evidence)


class UnknownEvidenceTests(unittest.TestCase):
    def test_merged_check_that_raises_or_returns_none_is_unknown(self):
        def boom(_j):
            raise RuntimeError("argonas down")

        for fn in (boom, lambda _j: None, lambda _j: "yes"):
            evidence, action = _gather(_row(), merged=fn)
            self.assertEqual(evidence["merged"], "unknown")
            self.assertEqual(action, "needs a human: evidence unknown (merged)")

    def test_job_listing_that_raises_is_unknown(self):
        def boom(_j):
            raise RuntimeError("bus 503")

        evidence, action = _gather(_row(), branch_jobs=boom)
        self.assertEqual(evidence["superseded_by"], "unknown")
        self.assertEqual(action, "needs a human: evidence unknown (superseded_by)")

    def test_both_unknown_names_both_gaps(self):
        def boom(_j):
            raise RuntimeError("x")

        _, action = _gather(_row(), merged=boom, branch_jobs=boom)
        self.assertEqual(action, "needs a human: evidence unknown (merged, superseded_by)")

    def test_unknown_source_does_not_hide_a_known_positive(self):
        def boom(_j):
            raise RuntimeError("x")

        _, action = _gather(_row(), merged=True, branch_jobs=boom)
        self.assertEqual(action, "merged: safe to cancel")
        newer = _other("job-new", "done", NOW - 10 * HOUR)
        _, action = _gather(_row(), merged=boom, branch_jobs=[newer])
        self.assertEqual(action, "superseded by job-new: safe to cancel")

    def test_missing_created_at_cannot_prove_supersession(self):
        row = _row()
        row.pop("created_at")
        evidence, _ = _gather(row, branch_jobs=[_other("job-new", "done", NOW)])
        self.assertIsNone(evidence["superseded_by"])

    def test_state_store_failure_is_unknown_not_an_exception(self):
        class Broken:
            def last_mark(self, key):
                raise OSError("state file unreadable")

        evidence, _ = _gather(_row(), state=Broken())
        self.assertEqual(evidence["doctor_parked"], "unknown")
        self.assertEqual(evidence["parked_age_sec"], "unknown")

    def test_default_merged_check_never_raises(self):
        # No catalog entry / unusable inputs: unknown (None), never an exception.
        self.assertIsNone(doctor.branch_merged_into_base({"id": "x"}))
        self.assertIsNone(doctor.branch_merged_into_base({"head_branch": "-bad", "base_branch": "main"}))
        self.assertIsNone(
            doctor.branch_merged_into_base({"head_branch": "a", "base_branch": "main", "repo": "no-such"})
        )

    def test_default_job_listing_failure_raises_for_the_caller_to_map(self):
        with mock.patch.object(doctor, "_http", lambda *a, **k: (503, None)):
            fetch = doctor._make_branch_jobs_fetcher()
            with self.assertRaises(RuntimeError):
                fetch(_row())

    def test_default_job_listing_is_fetched_once_and_filtered_by_branch(self):
        calls = []

        def fake_http(method, path, body=None, timeout=15.0):
            calls.append((method, path))
            return 200, {"jobs": [_other("a", "done", 1.0), _other("b", "done", 2.0, head="feat/y")]}

        with mock.patch.object(doctor, "_http", fake_http):
            fetch = doctor._make_branch_jobs_fetcher()
            self.assertEqual([j["id"] for j in fetch(_row())], ["a"])
            self.assertEqual([j["id"] for j in fetch(_row(head="feat/y"))], ["b"])
        self.assertEqual([m for m, _ in calls], ["GET"])


def _report(rows, state=None, merged=None, branch_jobs=None, http_log=None):
    """Run report_needs_review_sweep with every evidence source injected."""
    state = state or _FakeState()
    http_log = http_log if http_log is not None else []
    broadcasts = []

    def fake_http(method, path, body=None, timeout=15.0):
        http_log.append((method, path))
        if path.startswith("/v1/jobs?status=needs-review"):
            return 200, {"jobs": rows, "total": len(rows)}
        return 200, {}

    with (
        mock.patch.object(doctor, "_http", fake_http),
        mock.patch.object(doctor, "STATE", state),
        mock.patch.object(doctor.time, "time", lambda: NOW),
        mock.patch.object(doctor, "branch_merged_into_base", merged or (lambda _j: False)),
        mock.patch.object(doctor, "_make_branch_jobs_fetcher", lambda: branch_jobs or (lambda _j: [])),
        mock.patch.object(doctor, "complete_stuck_verdict_jobs", lambda jobs: {}),
        mock.patch.object(doctor, "broadcast_doctor_message", lambda topic, body: broadcasts.append((topic, body))),
    ):
        report = doctor.report_needs_review_sweep()
    return report, broadcasts, state


class ReportTests(unittest.TestCase):
    def _rows(self):
        return [
            _row("merged-1", head="feat/m"),
            _row("super-1", head="feat/s"),
            _row("human-1", head="feat/h"),
            _row("unknown-1", head="feat/u"),
        ]

    def _sources(self):
        def merged(job):
            if job["id"] == "unknown-1":
                raise RuntimeError("unreachable")
            return job["id"] == "merged-1"

        listing = [_other("job-new", "done", NOW - HOUR, head="feat/s")]
        return merged, (lambda job: [j for j in listing if j["head_branch"] == job["head_branch"]])

    def test_every_row_carries_evidence_and_a_suggestion(self):
        merged, jobs = self._sources()
        state = _FakeState({"loop_park:human-1": NOW - 72 * HOUR})
        report, _, _ = _report(self._rows(), state=state, merged=merged, branch_jobs=jobs)
        got = {r["job_id"]: r["suggested_action"] for r in report["rows"]}
        self.assertEqual(
            got,
            {
                "merged-1": "merged: safe to cancel",
                "super-1": "superseded by job-new: safe to cancel",
                "human-1": "needs a human: not merged and not superseded; parked by the doctor 72h ago",
                "unknown-1": "needs a human: evidence unknown (merged)",
            },
        )
        for row in report["rows"]:
            self.assertEqual(row["evidence"]["job_id"], row["job_id"])
            # the existing classification output is untouched
            self.assertIn("report-only needs-review sweep", row["orchestrator_note"])

    def test_suggestions_are_broadcast_with_the_row(self):
        merged, jobs = self._sources()
        _, broadcasts, _ = _report(self._rows(), merged=merged, branch_jobs=jobs)
        sent = [body for topic, body in broadcasts if topic == "doctor.needs_review"]
        self.assertEqual(len(sent), 4)
        self.assertTrue(all("suggested_action" in b and "evidence" in b for b in sent))

    def test_no_write_is_performed(self):
        merged, jobs = self._sources()
        http_log = []
        state = _FakeState({"loop_park:human-1": NOW - 72 * HOUR})
        _report(self._rows(), state=state, merged=merged, branch_jobs=jobs, http_log=http_log)
        writes = [(m, p) for m, p in http_log if m in ("POST", "PATCH", "PUT", "DELETE")]
        self.assertEqual(writes, [])
        self.assertEqual([m for m, _ in http_log], ["GET"])
        self.assertEqual(state.writes, [])

    def test_repeat_run_gives_an_identical_report(self):
        merged, jobs = self._sources()
        state = _FakeState({"loop_park:human-1": NOW - 72 * HOUR})
        first, _, _ = _report(self._rows(), state=state, merged=merged, branch_jobs=jobs)
        second, _, _ = _report(self._rows(), state=state, merged=merged, branch_jobs=jobs)
        self.assertEqual(first, second)

    def test_default_sources_are_fail_soft_without_a_bus_or_catalog(self):
        # No injected sources: the real defaults run, the job listing fails
        # (503) and nothing resolves in the catalog; the row must still report.
        rows = [_row("lone-1", head="feat/z", repo="no-such-repo")]
        http_log = []

        def fake_http(method, path, body=None, timeout=15.0):
            http_log.append(method)
            if path.startswith("/v1/jobs?status=needs-review"):
                return 200, {"jobs": rows, "total": 1}
            return 503, None

        with (
            mock.patch.object(doctor, "_http", fake_http),
            mock.patch.object(doctor, "STATE", _FakeState()),
            mock.patch.object(doctor, "complete_stuck_verdict_jobs", lambda jobs: {}),
            mock.patch.object(doctor, "broadcast_doctor_message", lambda *a, **k: None),
            mock.patch.object(doctor.time, "time", lambda: NOW),
        ):
            report = doctor.report_needs_review_sweep()
        self.assertEqual(
            report["rows"][0]["suggested_action"], "needs a human: evidence unknown (merged, superseded_by)"
        )
        self.assertEqual(set(http_log), {"GET"})


if __name__ == "__main__":
    unittest.main()
