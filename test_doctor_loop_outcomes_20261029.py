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
            state.marks.pop("orphan_resubmit:build:zeroclaw", None)  # cooldown elapsed
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
