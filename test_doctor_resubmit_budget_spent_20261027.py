"""A failed job with no retry budget is resubmitted, not parked.

Operator, 2026-10-27: "why is the doctor not acting on these?" — the doctor's
resubmit action refused any job with max_retries 0 or an exhausted retry_count,
then broadcast a needs-review message, so a budget-spent failure could only ever
become a triage row. The work itself is still real: it is now resubmitted as a
NEW job with a real budget, parented to the original for lineage.
"""

from __future__ import annotations

import os
import unittest
from unittest import mock

os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")

from hive.workers import doctor


FAILED = {
    "jobs": [
        {
            "id": "01a17fbd-eb8f-7375-b276-ebd1fd4bfac1",
            "kind": "review:zoder-standalone-profile-r3f",
            "description": "review the branch",
            "priority": 70,
            "project": "zoder",
            "repo": "zoder",
            "head_branch": "fix/standalone-profile-r3f",
            "base_branch": "master",
            "max_retries": 0,
            "retry_count": 0,
            "status": "failed",
        }
    ]
}


class ResubmitBudgetSpentTests(unittest.TestCase):
    def test_budget_spent_failure_is_resubmitted_with_a_reason(self):
        calls: list[tuple[str, str, dict]] = []

        def fake_http(method, path, payload=None, timeout=10.0):
            calls.append((method, path, payload or {}))
            if path.startswith("/v1/jobs?status=failed"):
                return 200, FAILED
            if path == "/v1/jobs":
                return 200, {"id": "new-job-1"}
            return 200, {"ok": True}

        with (
            mock.patch.object(doctor, "_http", fake_http),
            mock.patch.object(doctor, "broadcast_doctor_message", lambda *a, **k: None),
            mock.patch.object(doctor, "DRY_RUN", False),
        ):
            out = doctor.action_resubmit_jobs("review:zoder-standalone-profile-r3f")

        submits = [c for c in calls if c[1] == "/v1/jobs"]
        self.assertEqual(len(submits), 1, calls)
        payload = submits[0][2]
        self.assertEqual(payload["max_retries"], 2, payload)
        self.assertEqual(payload["parent_job_id"], "01a17fbd-eb8f-7375-b276-ebd1fd4bfac1")
        self.assertEqual(payload["head_branch"], "fix/standalone-profile-r3f")
        self.assertIn("resubmitted", out.lower())

    def test_dry_run_submits_nothing(self):
        calls: list[str] = []

        def fake_http(method, path, payload=None, timeout=10.0):
            calls.append(path)
            return (200, FAILED) if path.startswith("/v1/jobs?status=failed") else (200, {})

        with (
            mock.patch.object(doctor, "_http", fake_http),
            mock.patch.object(doctor, "broadcast_doctor_message", lambda *a, **k: None),
            mock.patch.object(doctor, "DRY_RUN", True),
        ):
            out = doctor.action_resubmit_jobs("review:zoder-standalone-profile-r3f")
        self.assertNotIn("/v1/jobs", calls)
        self.assertIn("DRY_RUN", out)


class ReplacementChainGuardTests(unittest.TestCase):
    def test_a_doctor_replacement_is_never_resubmitted_again(self):
        failed = {"jobs": [
            {"id": "orig", "kind": "review:x", "max_retries": 0, "retry_count": 0, "status": "failed",
             "description": "d", "priority": 1, "project": "p", "repo": "p", "head_branch": "b"},
            {"id": "replacement", "kind": "review:x", "max_retries": 2, "retry_count": 2, "status": "failed",
             "result": {"resubmitted_from": "orig"}},
        ]}
        calls = []

        def fake_http(method, path, payload=None, timeout=10.0):
            calls.append((method, path, payload or {}))
            if path.startswith("/v1/jobs?status=failed"):
                return 200, failed
            if path == "/v1/jobs":
                return 200, {"id": "gen2"}
            return 200, {}

        with (
            mock.patch.object(doctor, "_http", fake_http),
            mock.patch.object(doctor, "broadcast_doctor_message", lambda *a, **k: None),
            mock.patch.object(doctor, "DRY_RUN", False),
        ):
            doctor.action_resubmit_jobs("review:x")
        submitted = [c[2] for c in calls if c[1] == "/v1/jobs"]
        self.assertEqual(len(submitted), 1, "only the original may be resubmitted once")
        self.assertEqual(submitted[0]["parent_job_id"], "orig")

    def test_only_replacements_left_means_no_further_generation(self):
        failed = {"jobs": [
            {"id": "replacement", "kind": "review:x", "max_retries": 2, "retry_count": 2,
             "status": "failed", "result": {"resubmitted_from": "orig"}},
        ]}
        calls = []

        def fake_http(method, path, payload=None, timeout=10.0):
            calls.append(path)
            return (200, failed) if path.startswith("/v1/jobs?status=failed") else (200, {})

        with (
            mock.patch.object(doctor, "_http", fake_http),
            mock.patch.object(doctor, "broadcast_doctor_message", lambda *a, **k: None),
            mock.patch.object(doctor, "DRY_RUN", False),
        ):
            out = doctor.action_resubmit_jobs("review:x")
        self.assertNotIn("/v1/jobs", calls)
        self.assertIn("already a replacement", out)

    def test_the_create_payload_carries_the_replacement_marker(self):
        failed = {"jobs": [{"id": "orig", "kind": "review:x", "max_retries": 0, "retry_count": 0,
                            "status": "failed", "claimed_by": None, "result": {"commits": []}}]}
        calls = []

        def fake_http(method, path, payload=None, timeout=10.0):
            calls.append((method, path, payload or {}))
            if path.startswith("/v1/jobs?status=failed"):
                return 200, failed
            if path == "/v1/jobs":
                return 200, {"id": "gen1"}
            return 200, {"ok": True}

        with (
            mock.patch.object(doctor, "_http", fake_http),
            mock.patch.object(doctor, "broadcast_doctor_message", lambda *a, **k: None),
            mock.patch.object(doctor, "DRY_RUN", False),
        ):
            doctor.action_resubmit_jobs("review:x")
        created = [c[2] for c in calls if c[1] == "/v1/jobs"]
        self.assertEqual(created[0]["result"]["resubmitted_from"], "orig", created[0]["result"])

    def test_a_budget_spent_row_with_a_parent_is_never_resubmitted(self):
        """The durable signal is parent_job_id, not a field the worker may overwrite."""
        failed = {"jobs": [{"id": "child", "kind": "review:x", "max_retries": 0, "retry_count": 0,
                            "status": "failed", "parent_job_id": "orig", "result": {}}]}
        calls = []

        def fake_http(method, path, payload=None, timeout=10.0):
            calls.append(path)
            return (200, failed) if path.startswith("/v1/jobs?status=failed") else (200, {})

        with (
            mock.patch.object(doctor, "_http", fake_http),
            mock.patch.object(doctor, "broadcast_doctor_message", lambda *a, **k: None),
            mock.patch.object(doctor, "DRY_RUN", False),
        ):
            out = doctor.action_resubmit_jobs("review:x")
        self.assertNotIn("/v1/jobs", calls)
        self.assertIn("already a replacement", out)

    def test_a_replacement_that_still_has_budget_is_requeued(self):
        failed = {"jobs": [
            {"id": "replacement", "kind": "review:x", "max_retries": 2, "retry_count": 1,
             "status": "failed", "result": {"resubmitted_from": "orig"}},
        ]}
        calls = []

        def fake_http(method, path, payload=None, timeout=10.0):
            calls.append((method, path))
            return (200, failed) if path.startswith("/v1/jobs?status=failed") else (200, {"ok": True})

        with (
            mock.patch.object(doctor, "_http", fake_http),
            mock.patch.object(doctor, "broadcast_doctor_message", lambda *a, **k: None),
            mock.patch.object(doctor, "DRY_RUN", False),
        ):
            doctor.action_resubmit_jobs("review:x")
        self.assertIn(("POST", "/v1/admin/jobs/requeue"), calls)

    def test_the_original_is_pointed_at_its_replacement(self):
        failed = {"jobs": [
            {"id": "orig", "kind": "review:x", "max_retries": 0, "retry_count": 0,
             "status": "failed", "claimed_by": None, "result": {"commits": []}},
        ]}
        calls = []

        def fake_http(method, path, payload=None, timeout=10.0):
            calls.append((method, path, payload or {}))
            if path.startswith("/v1/jobs?status=failed"):
                return 200, failed
            if path == "/v1/jobs":
                return 200, {"id": "gen1"}
            return 200, {"ok": True}

        with (
            mock.patch.object(doctor, "_http", fake_http),
            mock.patch.object(doctor, "broadcast_doctor_message", lambda *a, **k: None),
            mock.patch.object(doctor, "DRY_RUN", False),
        ):
            doctor.action_resubmit_jobs("review:x")
        patches = [c for c in calls if c[0] == "PATCH"]
        self.assertEqual(len(patches), 1, calls)
        self.assertEqual(patches[0][2]["result"]["resubmitted_as"], "gen1")


if __name__ == "__main__":
    unittest.main()
