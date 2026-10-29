"""End-to-end doctor sweeps against the REAL bus app (in-process ASGI, temp sqlite).

The doctor's sweep logic is covered elsewhere with fakes; this test catches
contract drift between the doctor and the bus before a deploy: jobs are
seeded through the real submit/claim/patch API into each failure shape, the
doctor's HTTP helper is routed at the ASGI app (no real network), and the
sweeps run for real. Assertions read back through the bus API.

Shapes (via ``result.loop_result.teardown`` / structured result fields):
  author_failed -> requeued once (eligible, budget left)
  reviewer_unavailable + reviewer down -> held (stays failed)
  empty-diff loop -> parked once (needs-review)
  approved (approved_unmerged) -> never requeued (skip)
  timeout with 2 prior auto-requeues -> cap respected (parked, not requeued)
  exit 143 + finalizer committed_local_stopped -> parked (local-only rescue)
  clone_failed -> requeued
  a needs-review row -> visible to the needs-review sweep, untouched by it
A second sweep pass must change nothing (idempotent).
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus.deps import hive_repo  # noqa: E402
from hive.workers import doctor, loop_outcome  # noqa: E402
from hive.workers.doctor_state import DurableStateStore, MemoryBackend  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


class DoctorE2EBusTests(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        env = mock.patch.dict(
            os.environ,
            {
                "AGENT_BUS_DB": os.path.join(self._td.name, "e2e.db"),
                "HIVE_BACKEND": "sqlite",
            },
        )
        env.start()
        self.addCleanup(env.stop)
        for var in ("MNEMOS_URL", "MNEMOS_TOKEN", "HIVE_SHARED_SECRET"):
            if var in os.environ:
                clearer = mock.patch.dict(os.environ, {var: ""})
                clearer.start()
                self.addCleanup(clearer.stop)
                del os.environ[var]
        # Patch the same object the lifecycle harness patches.
        from hive.bus import deps as _deps

        repo = mock.patch.object(_deps, "_HIVE_REPO", None)
        repo.start()
        self.addCleanup(repo.stop)
        _run(hive_repo().init())

        # Register an orchestrator submitter and one worker; the worker URN
        # doubles as the doctor's mutation actor (any registered ACTIVE
        # agent satisfies require_mutation_actor).
        self.submitter = self._register(
            {
                "kind": "opencode",
                "host": "e2e",
                "runtime": "opencode",
                "status": "online",
                "capabilities": ["code", "orchestrate"],
                "cost_tier": "C",
                "auth_method": "api",
            }
        )
        self.worker = self._register(
            {
                "kind": "zeroclaw",
                "host": "e2e",
                "runtime": "zeroclaw",
                "status": "online",
                "capabilities": ["code", "bash", "python"],
                "cost_tier": "B",
                "auth_method": "api",
            }
        )
        self._post("/v1/agents/heartbeat", {"urn": self.worker})

        # Isolate doctor globals for the test.
        self._orig_http = doctor._http
        self._orig_urn = doctor._urn
        self._orig_dry = doctor.DRY_RUN
        self._orig_state = doctor.STATE
        self._orig_reviewer = loop_outcome.reviewer_healthy
        self._orig_older = doctor.NEEDS_REVIEW_OLDER_HOURS
        doctor._urn = self.worker
        doctor.NEEDS_REVIEW_OLDER_HOURS = 0
        doctor.DRY_RUN = False
        doctor.STATE = DurableStateStore(MemoryBackend(), namespace="e2e-doctor-20261029")
        loop_outcome.reviewer_healthy = lambda *a, **k: False  # reviewer down: hold path
        doctor._http = self._doctor_http
        self.addCleanup(self._restore_doctor)

    def _restore_doctor(self):
        doctor._http = self._orig_http
        doctor._urn = self._orig_urn
        doctor.NEEDS_REVIEW_OLDER_HOURS = self._orig_older
        doctor.DRY_RUN = self._orig_dry
        doctor.STATE = self._orig_state
        loop_outcome.reviewer_healthy = self._orig_reviewer

    # ---- bus helpers (each call opens its own client; the DB file persists) ----
    def _request(self, method, path, json_body=None, actor=None):
        async def _do():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t") as c:
                headers = {}
                if actor and method.upper() in ("POST", "PATCH", "PUT", "DELETE"):
                    headers["X-Hive-Actor"] = actor
                resp = await c.request(method, path, json=json_body, headers=headers or None)
                try:
                    return resp.status_code, resp.json()
                except Exception:
                    return resp.status_code, resp.text

        return _run(_do())

    def _register(self, payload):
        code, data = self._request("POST", "/v1/agents/register", payload)
        self.assertEqual(code, 200, data)
        return data["urn"]

    def _post(self, path, payload, actor=None):
        return self._request("POST", path, payload, actor=actor)

    def _doctor_http(self, method, path, body=None, timeout=15.0):
        # Route the doctor's urllib helper at the in-process ASGI app.
        code, data = self._request(method, path, body, actor=self.worker)
        if isinstance(data, dict):
            return code, data
        return code, None

    def _submit_claim_fail(self, kind, result, max_retries=2, zero_reason=None):
        payload = {
            "kind": kind,
            "description": f"E2E 20261029 {kind}",
            "submitter_urn": self.submitter,
            "priority": 50,
            "max_retries": max_retries,
        }
        if zero_reason is not None:
            payload["zero_retry_reason"] = zero_reason
        code, data = self._post("/v1/jobs", payload, actor=self.worker)
        self.assertIn(code, (200, 201), data)
        jid = data["id"]
        code, data = self._post(f"/v1/jobs/next?agent_urn={self.worker}", None, actor=self.worker)
        self.assertEqual(code, 200, data)
        self.assertEqual(data["id"], jid, f"worker should claim {kind}")
        body = {"status": "failed", "claimed_by": self.worker, "result": result}
        code, data = self._request("PATCH", f"/v1/jobs/{jid}", body, actor=self.worker)
        self.assertEqual(code, 200, data)
        # Seeded with retryable=False so the bus auto-retry leaves it failed.
        code, data = self._request("GET", f"/v1/jobs/{jid}")
        self.assertEqual(data["status"], "failed", f"{kind} should stay failed until the sweep")
        return jid

    def _get(self, jid):
        code, data = self._request("GET", f"/v1/jobs/{jid}")
        self.assertEqual(code, 200, data)
        return data

    # ---- the test ----
    def test_sweeps_against_real_bus(self):
        r = {"retryable": False}
        j_author = self._submit_claim_fail(
            "e2e:author-failed", {**r, "loop_result": {"teardown": {"reason": "author_failed"}}}
        )
        j_hold = self._submit_claim_fail(
            "e2e:reviewer-down",
            {**r, "loop_result": {"teardown": {"reason": "reviewer_unavailable"}}},
        )
        j_empty = self._submit_claim_fail(
            "e2e:empty-diff",
            {
                **r,
                "loop_result": {
                    "teardown": {"reason": "reviewer_unavailable", "error": "no review chunks were generated"}
                },
            },
        )
        j_approved = self._submit_claim_fail(
            "e2e:approved-unmerged", {**r, "loop_result": {"teardown": {"reason": "approved"}}}
        )
        j_cap = self._submit_claim_fail("e2e:timeout-capped", {**r, "loop_result": {"teardown": {"reason": "timeout"}}})
        # Cap already spent: two prior auto-requeues recorded.
        doctor._loop_requeue_record(j_cap)
        doctor._loop_requeue_record(j_cap)
        j_signal = self._submit_claim_fail(
            "e2e:killed-signal",
            {
                **r,
                "exit_code": 143,
                "failure_reason": "killed by signal SIGTERM",
                "finalizer": {"status": "committed_local_stopped"},
            },
        )
        j_clone = self._submit_claim_fail(
            "e2e:clone-failed", {**r, "failure_reason": "clone_failed: could not reach remote"}
        )
        # A needs-review row for the second sweep.
        j_nr = self._submit_claim_fail(
            "e2e:needs-review-seed", {**r, "loop_result": {"teardown": {"reason": "author_failed"}}}
        )
        code, data = self._request(
            "POST",
            "/v1/admin/jobs/reclassify",
            {"job_ids": [j_nr], "status": "needs-review", "reason": "e2e seed"},
            actor=self.worker,
        )
        self.assertEqual(code, 200, data)

        first = doctor.sweep_orphan_failures(max_bases=20)

        self.assertEqual(self._get(j_author)["status"], "queued", "author_failed should requeue")
        self.assertEqual(self._get(j_hold)["status"], "failed", "reviewer-down should hold")
        self.assertEqual(self._get(j_empty)["status"], "needs-review", "empty diff parks once")
        self.assertEqual(self._get(j_approved)["status"], "failed", "approved work never requeues")
        self.assertEqual(self._get(j_cap)["status"], "needs-review", "cap spent parks, no requeue")
        self.assertEqual(self._get(j_signal)["status"], "needs-review", "local-only signal parks")
        self.assertEqual(self._get(j_clone)["status"], "queued", "clone_failed requeues")
        self.assertIn("e2e:author-failed", str(first.get("acted", {})))

        needs = doctor.fetch_needs_review_jobs()
        ids = {j.get("id") for j in needs}
        for expected in (j_empty, j_cap, j_signal, j_nr):
            self.assertIn(expected, ids, "parked + seeded rows visible to needs-review sweep")
        report = doctor.report_needs_review_sweep()
        self.assertGreaterEqual(report.get("scanned", 0), 4)

        before = {}
        for jid in (j_author, j_hold, j_empty, j_approved, j_cap, j_signal, j_clone, j_nr):
            before[jid] = self._get(jid)["status"]
        code, listed = self._request("GET", "/v1/jobs?status=failed&limit=200")
        n_failed_before = len((listed or {}).get("jobs", []))

        second = doctor.sweep_orphan_failures(max_bases=20)
        report2 = doctor.report_needs_review_sweep()

        for jid, status in before.items():
            self.assertEqual(self._get(jid)["status"], status, "second pass changes nothing")
        code, listed2 = self._request("GET", "/v1/jobs?status=failed&limit=200")
        self.assertEqual(len((listed2 or {}).get("jobs", [])), n_failed_before, "no new failed rows from a second pass")
        self.assertEqual(report2.get("scanned", 0), report.get("scanned", 0), "needs-review sweep is report-only")
        _ = second  # attached for debugging; the status asserts above are the contract
