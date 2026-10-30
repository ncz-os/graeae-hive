"""FB21: lifecycle findings follow-up (FB18 report).

1. `released` is in VALID_JOB_STATUSES but no router ever stores it
   (the sqlite CHECK excludes it; the doctor uses it as an in-memory
   variable only): it must not be a valid API status.
2. `failed_completion` is stored (DB CHECK, DEP_DEAD, SWEEPABLE) but has
   no STATUS_TRANSITIONS entry, so even an idempotent same-status PATCH
   409s: it needs the dead-letter-style self-loop.
3. `queued->claimed` / `offered->claimed` are in the PATCH map but PATCH
   always refuses them (claim bypass): the map must not list cells the
   endpoint refuses; the claim endpoints own those transitions.
4. The completion gate answers 409 while parking the row as
   needs-review (state change on a 409): documented as xfail, no
   production change (the worker treats non-200 as "not done", which is
   the safe reading; changing the code risks a done-remembered
   needs-review row).
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pytest

os.environ.setdefault("HIVE_BACKEND", "sqlite")

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus import deps as _deps  # noqa: E402
from hive.bus.deps import VALID_JOB_STATUSES, hive_repo  # noqa: E402
from hive.bus.domain.jobs import STATUS_TRANSITIONS  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


def test_released_not_a_stored_api_status():
    assert "released" not in VALID_JOB_STATUSES, (
        "released is a doctor in-memory variable, never stored (sqlite CHECK "
        "excludes it): it must not validate as an API status"
    )


def test_released_patch_refused_as_unknown_status():
    # Endpoint-level companion to test_released_not_a_stored_api_status:
    # with the fix PATCH answers 422 (not a valid status); on the base
    # line released passed validation and died later with 409.
    from hive.bus.deps import VALID_JOB_STATUSES as _V

    assert "released" not in _V
    # (live-endpoint half lives in FindingsEndpointTests below)


def test_failed_completion_self_loop_in_map():
    assert "failed_completion" in STATUS_TRANSITIONS, "stored legacy status needs a map entry"
    assert STATUS_TRANSITIONS["failed_completion"] == {"failed_completion"}, (
        "legacy terminal: idempotent same-status PATCH only, like dead-letter"
    )


def test_claim_cells_not_in_patch_map():
    assert "claimed" not in STATUS_TRANSITIONS.get("queued", set()), (
        "queued->claimed is claim-endpoint-only; PATCH refuses it (claim bypass)"
    )
    assert "claimed" not in STATUS_TRANSITIONS.get("offered", set()), (
        "offered->claimed is claim-endpoint-only; PATCH refuses it (claim bypass)"
    )


@pytest.mark.xfail(reason="completion gate parks the row but answers 409; behavior kept, documented only")
def test_completion_gate_returns_2xx():
    assert False, "placeholder: the gate answers 409 with a needs-review write (kept intentionally)"


class FindingsEndpointTests(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        env = mock.patch.dict(
            os.environ, {"AGENT_BUS_DB": str(Path(self._td.name) / "f.db"), "HIVE_BACKEND": "sqlite"}
        )
        env.start()
        self.addCleanup(env.stop)
        repo = mock.patch.object(_deps, "_HIVE_REPO", None)
        repo.start()
        self.addCleanup(repo.stop)
        _run(hive_repo().init())
        self.submitter = self._register(
            {
                "kind": "opencode",
                "host": "e",
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
                "host": "e",
                "runtime": "zeroclaw",
                "status": "online",
                "capabilities": ["code"],
                "cost_tier": "B",
                "auth_method": "api",
            }
        )

    def _request(self, method, path, body=None, actor=None):
        async def _do():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t"
            ) as c:
                headers = {"X-Hive-Actor": actor} if actor and method in ("POST", "PATCH", "PUT", "DELETE") else None
                resp = await c.request(method, path, json=body, headers=headers)
                try:
                    return resp.status_code, resp.json()
                except Exception:
                    return resp.status_code, resp.text

        return _run(_do())

    def _register(self, payload):
        code, data = self._request("POST", "/v1/agents/register", payload)
        self.assertEqual(code, 200, data)
        return data["urn"]

    def _set_status_direct(self, jid, status):
        async def _do():
            async with hive_repo().connection() as tx:
                await tx.execute("UPDATE jobs SET status=? WHERE id=?", (status, jid))
                await tx.commit()

        _run(_do())

    def test_failed_completion_same_status_patch_ok(self):
        code, data = self._request(
            "POST",
            "/v1/jobs",
            {"kind": "e2e:x", "description": "FINDINGS SPOT CHECK", "submitter_urn": self.submitter, "priority": 1},
            actor=self.worker,
        )
        self.assertIn(code, (200, 201), data)
        jid = data["id"]
        self._set_status_direct(jid, "failed_completion")
        code, _ = self._request("PATCH", f"/v1/jobs/{jid}", {"status": "failed_completion"}, actor=self.worker)
        self.assertEqual(code, 200, "legacy terminal self-PATCH must be idempotent")

    def test_released_status_patch_refused_with_422(self):
        code, data = self._request(
            "POST",
            "/v1/jobs",
            {"kind": "e2e:r", "description": "RELEASED CHECK", "submitter_urn": self.submitter, "priority": 1},
            actor=self.worker,
        )
        self.assertIn(code, (200, 201), data)
        jid = data["id"]
        code, data = self._request(
            "PATCH", f"/v1/jobs/{jid}", {"status": "released"}, actor=self.worker
        )
        self.assertEqual(code, 422, f"released must be an unknown status, got {code}: {data}")

    def test_failed_completion_non_self_transition_refused(self):
        code, data = self._request(
            "POST",
            "/v1/jobs",
            {"kind": "e2e:fc", "description": "FC ONLY CHECK", "submitter_urn": self.submitter, "priority": 1},
            actor=self.worker,
        )
        self.assertIn(code, (200, 201), data)
        jid = data["id"]
        self._set_status_direct(jid, "failed_completion")
        code, data = self._request(
            "PATCH", f"/v1/jobs/{jid}", {"status": "cancelled", "claimed_by": self.worker}, actor=self.worker
        )
        self.assertEqual(code, 409, f"failed_completion allows idempotent self-PATCH only, got {code}: {data}")

    def test_queued_claim_via_patch_refused_claim_endpoint_owns_it(self):
        code, data = self._request(
            "POST",
            "/v1/jobs",
            {"kind": "e2e:y", "description": "FINDINGS CLAIM CHECK", "submitter_urn": self.submitter, "priority": 300},
            actor=self.worker,
        )
        self.assertIn(code, (200, 201), data)
        jid = data["id"]
        code, _ = self._request(
            "PATCH", f"/v1/jobs/{jid}", {"status": "claimed", "claimed_by": self.worker}, actor=self.worker
        )
        self.assertEqual(code, 409, "PATCH queued->claimed stays refused (claim bypass)")
        code, data = self._request("POST", f"/v1/jobs/next?agent_urn={self.worker}", None, actor=self.worker)
        self.assertEqual(code, 200, data)
        self.assertEqual(data["id"], jid, "the claim endpoint still performs queued->claimed")

    def test_offered_claim_via_patch_refused(self):
        code, data = self._request(
            "POST",
            "/v1/jobs",
            {"kind": "e2e:o", "description": "OFFERED CLAIM CHECK", "submitter_urn": self.submitter, "priority": 1},
            actor=self.worker,
        )
        self.assertIn(code, (200, 201), data)
        jid = data["id"]
        self._set_status_direct(jid, "offered")
        code, data = self._request(
            "PATCH", f"/v1/jobs/{jid}", {"status": "claimed", "claimed_by": self.worker}, actor=self.worker
        )
        self.assertEqual(code, 409, f"PATCH offered->claimed stays refused (claim bypass), got {code}: {data}")

    def test_direct_claim_endpoint_still_works(self):
        code, data = self._request(
            "POST",
            "/v1/jobs",
            {"kind": "e2e:dc", "description": "DIRECT CLAIM CHECK", "submitter_urn": self.submitter, "priority": 1},
            actor=self.worker,
        )
        self.assertIn(code, (200, 201), data)
        jid = data["id"]
        code, data = self._request("POST", f"/v1/jobs/{jid}/claim?by={self.worker}", None, actor=self.worker)
        self.assertEqual(code, 200, f"direct claim endpoint must keep working, got {code}: {data}")

    def test_claimed_self_patch_still_renews(self):
        code, data = self._request(
            "POST",
            "/v1/jobs",
            {"kind": "e2e:cr", "description": "RENEWAL CHECK", "submitter_urn": self.submitter, "priority": 1},
            actor=self.worker,
        )
        self.assertIn(code, (200, 201), data)
        jid = data["id"]
        code, data = self._request("POST", f"/v1/jobs/next?agent_urn={self.worker}", None, actor=self.worker)
        self.assertEqual(code, 200, data)
        self.assertEqual(data["id"], jid)
        code, data = self._request(
            "PATCH", f"/v1/jobs/{jid}", {"status": "claimed", "claimed_by": self.worker}, actor=self.worker
        )
        self.assertEqual(code, 200, f"claimed->claimed renewal must keep working, got {code}: {data}")
