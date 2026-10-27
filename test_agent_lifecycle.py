"""Full agent lifecycle against the REAL bus app (in-process ASGI, temp sqlite).

Exercises the refactored bus end-to-end the way a fleet agent does:

    register (orchestrator + worker) -> heartbeat -> submit -> claim ->
    lease/progress -> complete -> verify -> reviewer-preserve

This is the "agent testing" harness: it drives the actual routes (not fakes), so
a regression in the claim path, the PATCH state machine, the completion-proof
gate, or the review-preservation merge fails here.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus.deps import hive_repo  # noqa: E402


class AgentLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from hive.bus import deps
        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        env = mock.patch.dict(os.environ, {"AGENT_BUS_DB": os.path.join(self._td.name, "lifecycle.db")})
        env.start()
        self.addCleanup(env.stop)
        repository = mock.patch.object(deps, "_HIVE_REPO", None)
        repository.start()
        self.addCleanup(repository.stop)
        await hive_repo().init()
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t"
        )

    async def asyncTearDown(self):
        await self.client.aclose()

    async def _json(self, resp: httpx.Response):
        try:
            return resp.json()
        except Exception:
            return resp.text

    async def test_full_lifecycle_submit_claim_complete_preserve(self):
        c = self.client
        # 1. register an orchestrator (submitter) — opencode is a valid orchestrator
        r = await c.post(
            "/v1/agents/register",
            json={"kind": "opencode", "host": "studio", "runtime": "opencode", "status": "online",
                  "capabilities": ["code", "orchestrate"], "cost_tier": "C", "auth_method": "api"},
        )
        self.assertEqual(r.status_code, 200, r.text)
        submitter = r.json()["urn"]

        # 2. register a worker
        r = await c.post(
            "/v1/agents/register",
            json={"kind": "zeroclaw", "host": "studio", "runtime": "zeroclaw", "status": "online",
                  "capabilities": ["code", "bash", "python"], "cost_tier": "B", "auth_method": "api"},
        )
        self.assertEqual(r.status_code, 200, r.text)
        worker = r.json()["urn"]

        # 3. heartbeat (agent must stay active)
        r = await c.post("/v1/agents/heartbeat", json={"urn": worker})
        self.assertEqual(r.status_code, 200, r.text)

        # 4. submit — the orchestrator job must be accepted
        r = await c.post(
            "/v1/jobs",
            json={"kind": "test:lifecycle", "description": "LIFECYCLE TEST", "submitter_urn": submitter,
                  "priority": 99, "max_retries": 0, "zero_retry_reason": "test terminal disposition without automatic retry"},
        )
        self.assertEqual(r.status_code, 200, r.text)
        jid = r.json()["id"]

        # 5. claim — the worker polls and gets OUR job
        r = await c.post(f"/v1/jobs/next?agent_urn={worker}")
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["id"], jid, "worker should be offered its eligible job")
        self.assertEqual(r.json()["status"], "claimed")

        # 6. progress: claimed -> running (PATCH with the claimer)
        r = await c.patch(f"/v1/jobs/{jid}", json={"status": "running", "claimed_by": worker,
                                                   "result": {"progress": "50%"}})
        self.assertEqual(r.status_code, 200, r.text)

        # 7. complete with real evidence (passes the completion-proof gate).
        #    A worker PATCH must carry its claimed_by (the claimant guard).
        r = await c.patch(f"/v1/jobs/{jid}", json={
            "status": "done", "claimed_by": worker,
            "result": {"summary": "ok", "commits": ["abc123"], "files_changed": ["a.py"]},
            "tokens_in": 11, "tokens_out": 22,
        })
        self.assertEqual(r.status_code, 200, r.text)

        # 8. verify persisted state
        r = await c.get(f"/v1/jobs/{jid}")
        job = r.json()
        self.assertEqual(job["status"], "done")
        res = job["result"]
        if isinstance(res, str):
            res = json.loads(res)
        self.assertEqual(res["summary"], "ok")
        self.assertEqual(job["estimated_cost_usd"] is not None, True)

        # 9. reviewer-preserve: the auto-reviewer appends hive_reviews; a later
        #    worker result write must NOT drop it (the clobber fix).
        async with hive_repo().connection() as tx:
            await tx.execute(
                "UPDATE jobs SET status=?, result=? WHERE id=?",
                ("running", json.dumps({"hive_reviews": [{"verdict": "PASS"}]}), jid),
            )
            await tx.commit()
        r = await c.patch(f"/v1/jobs/{jid}", json={"status": "done", "claimed_by": worker,
                                                   "result": {"summary": "second"}})
        self.assertEqual(r.status_code, 200, r.text)
        r = await c.get(f"/v1/jobs/{jid}")
        res2 = r.json()["result"]
        if isinstance(res2, str):
            res2 = json.loads(res2)
        self.assertEqual(res2["summary"], "second")
        self.assertTrue(res2.get("hive_reviews"), "reviewer keys must survive a worker PATCH")

    async def test_worker_only_runtime_cannot_submit(self):
        # zeroclaw is worker-only: submit must be rejected
        c = self.client
        r = await c.post(
            "/v1/agents/register",
            json={"kind": "zeroclaw", "host": "studio", "runtime": "zeroclaw", "status": "online",
                  "capabilities": ["bash"], "auth_method": "api"},
        )
        urn = r.json()["urn"]
        r = await c.post(
            "/v1/jobs",
            json={"kind": "feat:x", "description": "nope", "submitter_urn": urn},
        )
        self.assertEqual(r.status_code, 403, r.text)

    async def _needs_review_job(self):
        """Drive one job through claim -> rejected completion -> needs-review."""
        c = self.client
        r = await c.post(
            "/v1/agents/register",
            json={"kind": "opencode", "host": "studio", "runtime": "opencode", "status": "online",
                  "capabilities": ["code"], "auth_method": "api"},
        )
        sub = r.json()["urn"]
        r = await c.post(
            "/v1/jobs",
            json={"kind": "fix:thing", "description": "needs proof", "submitter_urn": sub, "max_retries": 0, "zero_retry_reason": "test terminal disposition without automatic retry"},
        )
        jid = r.json()["id"]
        await c.post(f"/v1/jobs/next?agent_urn={sub}")
        await c.patch(f"/v1/jobs/{jid}", json={"status": "running", "claimed_by": sub})
        r = await c.patch(f"/v1/jobs/{jid}", json={"status": "done", "claimed_by": sub,
                                                   "result": {"summary": "no evidence"}})
        self.assertEqual(r.status_code, 409, r.text)
        self.assertEqual(r.json().get("status"), "needs-review")
        return sub, jid

    async def test_delete_needs_review_allowed_running_still_refused(self):
        # needs-review is a QUARANTINE state: its worker already stopped, so
        # DELETE must succeed without ?force=true. Genuinely in-flight work
        # (claimed/running) must still be refused.
        sub, jid = await self._needs_review_job()
        c = self.client
        r = await c.delete(f"/v1/jobs/{jid}", headers={"X-Hive-Actor": sub})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["deleted"], 1)
        self.assertEqual(r.json()["prior_status"], "needs-review")
        self.assertEqual((await c.get(f"/v1/jobs/{jid}")).status_code, 404)

        # a running job is still refused (no force)
        r = await c.post("/v1/jobs", json={"kind": "fix:other", "description": "live",
                                            "submitter_urn": sub, "max_retries": 0, "zero_retry_reason": "test terminal disposition without automatic retry"})
        live = r.json()["id"]
        await c.post(f"/v1/jobs/next?agent_urn={sub}")
        await c.patch(f"/v1/jobs/{live}", json={"status": "running", "claimed_by": sub})
        r = await c.delete(f"/v1/jobs/{live}", headers={"X-Hive-Actor": sub})
        self.assertEqual(r.status_code, 409, r.text)
        self.assertIn("force=true", r.text)
        # ...and force still overrides it
        r = await c.delete(f"/v1/jobs/{live}?force=true", headers={"X-Hive-Actor": sub})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual((await c.get(f"/v1/jobs/{live}")).status_code, 404)

    async def test_completion_without_evidence_goes_to_needs_review(self):
        # a 'done' on an evidence-required kind with no proof -> needs-review
        c = self.client
        r = await c.post(
            "/v1/agents/register",
            json={"kind": "opencode", "host": "studio", "runtime": "opencode", "status": "online",
                  "capabilities": ["code"], "auth_method": "api"},
        )
        sub = r.json()["urn"]
        r = await c.post(
            "/v1/jobs",
            json={"kind": "fix:thing", "description": "needs proof", "submitter_urn": sub, "max_retries": 0, "zero_retry_reason": "test terminal disposition without automatic retry"},
        )
        jid = r.json()["id"]
        await c.post(f"/v1/jobs/next?agent_urn={sub}")
        # opencode is an orchestrator; it can also claim its own submitted job
        r = await c.patch(f"/v1/jobs/{jid}", json={"status": "running", "claimed_by": sub})
        r = await c.patch(f"/v1/jobs/{jid}", json={"status": "done", "claimed_by": sub,
                                                   "result": {"summary": "no evidence"}})
        # a 'done' without evidence is rejected (409) and routed to needs-review
        self.assertEqual(r.status_code, 409, r.text)
        self.assertEqual(r.json().get("status"), "needs-review")
        r = await c.get(f"/v1/jobs/{jid}")
        self.assertEqual(r.json()["status"], "needs-review")

    async def test_unknown_agent_heartbeat_is_409(self):
        # 409 (not 404) so hive_worker.py's main loop re-registers and self-heals
        ghost = "urn:agent:zeroclaw:ghost:00000000-0000-0000-0000-000000000000"
        r = await self.client.post("/v1/agents/heartbeat", json={"urn": ghost})
        self.assertEqual(r.status_code, 409, r.text)

    async def test_unknown_agent_claim_is_409(self):
        ghost = "urn:agent:zeroclaw:ghost:00000000-0000-0000-0000-000000000000"
        r = await self.client.post(f"/v1/jobs/next?agent_urn={ghost}")
        self.assertEqual(r.status_code, 409, r.text)


if __name__ == "__main__":
    unittest.main()
