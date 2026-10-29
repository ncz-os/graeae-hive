"""Freebuff agents can register with the Hive bus (operator 2026-10-29).

Before: ``runtime=freebuff`` was rejected (422 "cannot infer runtime"), so a
Freebuff session could not join the bus. It is its own runtime and kind: it must
register, heartbeat and be an orchestrator, and must not alias into (or out of)
another kind or the doctor.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus.deps import hive_repo  # noqa: E402
from hive.bus.domain import eligibility  # noqa: E402
from hive.core import roles  # noqa: E402


class FreebuffRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from hive.bus import deps

        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        env = mock.patch.dict(os.environ, {"AGENT_BUS_DB": os.path.join(self._td.name, "fb.db")})
        env.start()
        self.addCleanup(env.stop)
        repo = mock.patch.object(deps, "_HIVE_REPO", None)
        repo.start()
        self.addCleanup(repo.stop)
        await hive_repo().init()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t")

    async def asyncTearDown(self):
        await self.client.aclose()

    async def _register(self, **extra):
        body = {"host": "studio", "status": "online", "capabilities": ["code"], "auth_method": "api"}
        body.update(extra)
        return await self.client.post("/v1/agents/register", json=body)

    async def test_registers_with_runtime_and_kind(self):
        r = await self._register(runtime="freebuff", kind="freebuff")
        self.assertEqual(r.status_code, 200, r.text)
        self.assertIn(":freebuff:", r.json()["urn"])

    async def test_registers_with_runtime_only_and_kind_only(self):
        for extra in ({"runtime": "freebuff"}, {"kind": "freebuff"}):
            r = await self._register(**extra)
            self.assertEqual(r.status_code, 200, f"{extra}: {r.text}")
            self.assertIn(":freebuff:", r.json()["urn"])

    async def test_heartbeat_keeps_the_agent_online(self):
        urn = (await self._register(runtime="freebuff", kind="freebuff")).json()["urn"]
        r = await self.client.post("/v1/agents/heartbeat", json={"urn": urn, "status": "online"})
        self.assertEqual(r.status_code, 200, r.text)

    async def test_cannot_masquerade_as_another_kind(self):
        for kind in ("claude", "opencode", "zeroclaw", "doctor"):
            r = await self._register(runtime="freebuff", kind=kind)
            self.assertEqual(r.status_code, 422, f"{kind}: {r.text}")
            self.assertIn("identity-mismatch", r.text)

    async def test_another_runtime_cannot_register_as_freebuff(self):
        r = await self._register(runtime="opencode", kind="freebuff")
        self.assertEqual(r.status_code, 422, r.text)

    def test_role_and_alias_tables(self):
        self.assertEqual(eligibility.RUNTIME_KIND_MAP["freebuff"], {"freebuff"})
        self.assertIn("freebuff", roles.ORCHESTRATOR_RUNTIMES)
        self.assertNotIn("freebuff", roles.WORKER_ONLY_RUNTIMES)
        for table in ("_DOCTOR_EXECUTOR_RUNTIMES", "_DOCTOR_EXECUTOR_ALIASES", "DOCTOR_OPT_IN_EXCLUDE_ALIASES"):
            self.assertIn("freebuff", getattr(eligibility, table), table)
        for rt, kinds in eligibility.RUNTIME_KIND_MAP.items():
            if rt != "freebuff":
                self.assertNotIn("freebuff", kinds, f"{rt} must not alias freebuff")
        self.assertNotIn("doctor", eligibility.RUNTIME_KIND_MAP["freebuff"])


if __name__ == "__main__":
    unittest.main()
