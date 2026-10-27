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


if __name__ == "__main__":
    unittest.main()
