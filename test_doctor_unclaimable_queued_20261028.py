"""A queued row pinned to a host with no worker must be re-dispatched.

Orchestrator coordination on the bus, 2026-10-28: a build job with
eligible_hosts=["hydra"] sat queued for hours because HYDRA runs no zeroclaw
worker. Nothing could ever claim it and no sweep looked at queued rows, so it
read as "the claim path is stalled".
"""

from __future__ import annotations

import os
import time
import unittest
from unittest import mock

os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")

from hive.workers import doctor


def _job(jid="j1", hosts=("hydra",), age=3600):
    return {
        "id": jid,
        "kind": "build:zeroclaw",
        "description": "do the thing",
        "priority": 0,
        "project": "p",
        "repo": "p",
        "head_branch": "b",
        "eligible_kinds": ["zeroclaw"],
        "eligible_hosts": list(hosts),
        "claimed_by": None,
        "started_at": time.time() - age,
    }


class UnclaimableQueuedTests(unittest.TestCase):
    def test_row_pinned_to_a_workerless_host_is_resubmitted_without_hosts(self):
        calls = []

        def fake_http(method, path, payload=None, timeout=10.0):
            calls.append((method, path, payload or {}))
            if path.startswith("/v1/jobs?status=queued"):
                return 200, {"jobs": [_job()]}
            if path.startswith("/v1/agents"):
                return 200, {"agents": [{"host": "minos", "kind": "zeroclaw", "status": "online"}]}
            if path == "/v1/jobs":
                return 200, {"id": "new1"}
            return 200, {}

        with (
            mock.patch.object(doctor, "_http", fake_http),
            mock.patch.object(doctor, "dlq_protected", lambda j: False),
        ):
            out = doctor.sweep_unclaimable_queued()

        submits = [c for c in calls if c[1] == "/v1/jobs"]
        self.assertEqual(len(submits), 1, out)
        payload = submits[0][2]
        self.assertNotIn("eligible_hosts", payload, payload)
        self.assertEqual(payload["parent_job_id"], "j1")
        self.assertEqual(payload["max_retries"], 2)

    def test_row_pinned_to_a_live_worker_host_is_left_alone(self):
        calls = []

        def fake_http(method, path, payload=None, timeout=10.0):
            calls.append((method, path, payload or {}))
            if path.startswith("/v1/jobs?status=queued"):
                return 200, {"jobs": [_job(hosts=("minos",))]}
            if path.startswith("/v1/agents"):
                return 200, {"agents": [{"host": "minos", "kind": "zeroclaw", "status": "online"}]}
            return 200, {}

        with (
            mock.patch.object(doctor, "_http", fake_http),
            mock.patch.object(doctor, "dlq_protected", lambda j: False),
        ):
            doctor.sweep_unclaimable_queued()
        self.assertNotIn("/v1/jobs", [c[1] for c in calls])

    def test_parked_and_young_rows_are_ignored(self):
        calls = []
        parked = _job("j2")
        parked["result"] = {"infrastructure_parked": True}
        young = _job("j3", age=10)

        def fake_http(method, path, payload=None, timeout=10.0):
            calls.append(path)
            if path.startswith("/v1/jobs?status=queued"):
                return 200, {"jobs": [parked, young]}
            if path.startswith("/v1/agents"):
                return 200, {"agents": []}
            return 200, {}

        with (
            mock.patch.object(doctor, "_http", fake_http),
            mock.patch.object(doctor, "dlq_protected", lambda j: False),
        ):
            doctor.sweep_unclaimable_queued()
        self.assertNotIn("/v1/jobs", calls)


if __name__ == "__main__":
    unittest.main()
