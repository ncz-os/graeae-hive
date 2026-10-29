"""Agent deregistration + the optional HIVE_AGENT_STALE_SECONDS reaper rule.

POST /v1/agents/{urn}/deregister takes an agent offline with a recorded reason.
It is allowed for the admin tier (``Authorization: Bearer $HIVE_BUS_ADMIN_TOKEN``)
or for the agent itself (``X-Hive-Actor`` == urn), and it is idempotent.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus.deps import hive_repo  # noqa: E402

ADMIN = "unit-test-admin-token"  # test fixture value, not a credential
ADMIN_HDR = {"Authorization": f"Bearer {ADMIN}"}


class AgentDeregisterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from hive.bus import deps

        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        env = mock.patch.dict(
            os.environ,
            {"AGENT_BUS_DB": os.path.join(self._td.name, "dereg.db"), "HIVE_BUS_ADMIN_TOKEN": ADMIN},
        )
        env.start()
        self.addCleanup(env.stop)
        # the suite may run with these set; the tests below choose explicitly
        for name in ("HIVE_AGENT_STALE_SECONDS", "HIVE_SHARED_SECRET"):
            os.environ.pop(name, None)
        repository = mock.patch.object(deps, "_HIVE_REPO", None)
        repository.start()
        self.addCleanup(repository.stop)
        await hive_repo().init()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t")

    async def asyncTearDown(self):
        await self.client.aclose()

    async def _register(self, kind="zeroclaw") -> str:
        r = await self.client.post(
            "/v1/agents/register",
            json={"kind": kind, "runtime": kind, "host": "studio", "status": "online", "auth_method": "api"},
        )
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()["urn"]

    async def _agent_row(self, urn):
        async with (
            hive_repo().connection() as tx,
            tx.execute("SELECT status, metadata FROM agents WHERE urn=?", (urn,)) as cur,
        ):
            return await cur.fetchone()

    async def _events(self, kind, urn):
        async with (
            hive_repo().connection() as tx,
            tx.execute("SELECT payload FROM events WHERE kind=? AND agent_urn=?", (kind, urn)) as cur,
        ):
            return [json.loads(r[0]) async for r in cur]

    async def _insert_agent(self, urn, status, hb_age):
        now = time.time()
        async with hive_repo().connection() as tx:
            await tx.execute(
                "INSERT INTO agents (urn, kind, host, session_id, started_at, last_heartbeat, status, runtime) "
                "VALUES (?, 'zeroclaw', 'h', 's', ?, ?, ?, 'zeroclaw')",
                (urn, now - 10_000, now - hb_age, status),
            )
            await tx.commit()

    # ---- endpoint ----------------------------------------------------------

    async def test_admin_deregisters_with_reason(self):
        urn = await self._register()
        r = await self.client.post(f"/v1/agents/{urn}/deregister", json={"reason": "stale feeder"}, headers=ADMIN_HDR)
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertEqual((body["urn"], body["status"], body["reason"]), (urn, "offline", "stale feeder"))
        status, meta = await self._agent_row(urn)
        self.assertEqual(status, "offline")
        self.assertEqual(json.loads(meta)["deregistered"]["reason"], "stale feeder")
        listed = (await self.client.get("/v1/agents", params={"include_offline": "true"})).json()["agents"]
        self.assertEqual([a["status"] for a in listed if a["urn"] == urn], ["offline"])

    async def test_reason_is_optional(self):
        urn = await self._register()
        r = await self.client.post(f"/v1/agents/{urn}/deregister", headers=ADMIN_HDR)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["status"], "offline")
        self.assertTrue(r.json()["reason"])

    async def test_second_identical_call_is_idempotent(self):
        urn = await self._register()
        url = f"/v1/agents/{urn}/deregister"
        first = await self.client.post(url, json={"reason": "gone"}, headers=ADMIN_HDR)
        meta_after_first = (await self._agent_row(urn))[1]
        second = await self.client.post(url, json={"reason": "gone"}, headers=ADMIN_HDR)
        self.assertEqual((first.status_code, second.status_code), (200, 200))
        self.assertEqual(first.json(), second.json())
        # no duplicate side effects: one event, metadata untouched
        self.assertEqual(len(await self._events("agent.offline", urn)), 1)
        self.assertEqual((await self._agent_row(urn))[1], meta_after_first)
        # a different reason on a repeat does not rewrite the record either
        third = await self.client.post(url, json={"reason": "other"}, headers=ADMIN_HDR)
        self.assertEqual(third.json(), first.json())

    async def test_unknown_urn_is_404(self):
        urn = "urn:agent:zeroclaw:studio:does-not-exist"
        admin = await self.client.post(f"/v1/agents/{urn}/deregister", headers=ADMIN_HDR)
        self.assertEqual(admin.status_code, 404, admin.text)
        self.assertIn("agent not found", admin.json()["detail"])
        own = await self.client.post(f"/v1/agents/{urn}/deregister", headers={"X-Hive-Actor": urn})
        self.assertEqual(own.status_code, 404, own.text)
        self.assertIn("agent not found", own.json()["detail"])

    async def test_other_non_admin_actor_is_refused(self):
        victim = await self._register()
        other = await self._register(kind="opencode")
        r = await self.client.post(f"/v1/agents/{victim}/deregister", headers={"X-Hive-Actor": other})
        self.assertEqual(r.status_code, 403, r.text)
        self.assertEqual((await self._agent_row(victim))[0], "online")
        # no identity at all, and a wrong admin token, are refused too
        r = await self.client.post(f"/v1/agents/{victim}/deregister")
        self.assertEqual(r.status_code, 401, r.text)
        r = await self.client.post(
            f"/v1/agents/{victim}/deregister", headers={"Authorization": "Bearer wrong", "X-Hive-Actor": other}
        )
        self.assertEqual(r.status_code, 403, r.text)
        self.assertEqual((await self._agent_row(victim))[0], "online")

    async def test_admin_token_unset_means_nobody_is_admin(self):
        urn = await self._register()
        with mock.patch.dict(os.environ):
            os.environ.pop("HIVE_BUS_ADMIN_TOKEN")
            r = await self.client.post(f"/v1/agents/{urn}/deregister", headers={"Authorization": "Bearer "})
            self.assertEqual(r.status_code, 401, r.text)
            r = await self.client.post(f"/v1/agents/{urn}/deregister", headers={"Authorization": "Bearer x"})
            self.assertEqual(r.status_code, 401, r.text)
        self.assertEqual((await self._agent_row(urn))[0], "online")

    async def test_agent_can_deregister_itself_and_repeat(self):
        urn = await self._register()
        url = f"/v1/agents/{urn}/deregister"
        first = await self.client.post(url, json={"reason": "shutting down"}, headers={"X-Hive-Actor": urn})
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual((await self._agent_row(urn))[0], "offline")
        # the agent is now offline, yet its repeat call still answers 200 (not 403)
        second = await self.client.post(url, json={"reason": "shutting down"}, headers={"X-Hive-Actor": urn})
        self.assertEqual(second.status_code, 200, second.text)
        self.assertEqual(first.json(), second.json())

    async def test_self_deregister_honours_shared_secret(self):
        urn = await self._register()
        with mock.patch.dict(os.environ, {"HIVE_SHARED_SECRET": "unit-test-shared"}):
            r = await self.client.post(f"/v1/agents/{urn}/deregister", headers={"X-Hive-Actor": urn})
            self.assertEqual(r.status_code, 401, r.text)
            r = await self.client.post(
                f"/v1/agents/{urn}/deregister", headers={"X-Hive-Actor": urn, "X-Hive-Token": "unit-test-shared"}
            )
            self.assertEqual(r.status_code, 200, r.text)

    async def test_stats_count_reflects_deregistration(self):
        keep = await self._register()
        gone = await self._register(kind="opencode")
        before = (await self.client.get("/v1/agents/stats")).json()
        self.assertEqual((before["online"], before["offline"], before["total"]), (2, 0, 2))
        r = await self.client.post(f"/v1/agents/{gone}/deregister", headers=ADMIN_HDR)
        self.assertEqual(r.status_code, 200, r.text)
        after = (await self.client.get("/v1/agents/stats")).json()
        self.assertEqual((after["online"], after["offline"], after["total"]), (1, 1, 2))
        self.assertEqual((await self._agent_row(keep))[0], "online")

    async def test_deregister_releases_unstarted_work(self):
        urn = await self._register()
        async with hive_repo().connection() as tx:
            await tx.execute(
                "INSERT INTO jobs (id, submitter_urn, kind, description, priority, status, claimed_by, "
                "claimed_at, started_at, retry_count, max_retries) "
                "VALUES ('j1', 'urn:agent:opencode:h:1', 'feat:x', 'd', 0, 'claimed', ?, ?, ?, 0, 2)",
                (urn, time.time(), time.time()),
            )
            await tx.commit()
        r = await self.client.post(f"/v1/agents/{urn}/deregister", headers=ADMIN_HDR)
        self.assertEqual(r.status_code, 200, r.text)
        async with (
            hive_repo().connection() as tx,
            tx.execute("SELECT status, claimed_by FROM jobs WHERE id='j1'") as cur,
        ):
            self.assertEqual(tuple(await cur.fetchone()), ("queued", None))

    # ---- reaper rule -------------------------------------------------------

    async def _reap(self):
        async with hive_repo().connection() as tx:
            await agent_bus.reap_agent_heartbeats(tx, time.time())

    async def test_reaper_disabled_when_env_unset(self):
        self.assertEqual(agent_bus.agent_offline_after(), agent_bus.HEARTBEAT_OFFLINE_AFTER)
        await self._insert_agent("u-60s", "online", 60)
        await self._insert_agent("u-250s", "online", 250)
        await self._insert_agent("u-400s", "online", 400)
        await self._reap()
        # exactly today's behaviour: <90s untouched, 90..300s stale, >300s offline
        self.assertEqual((await self._agent_row("u-60s"))[0], "online")
        self.assertEqual((await self._agent_row("u-250s"))[0], "stale")
        self.assertEqual((await self._agent_row("u-400s"))[0], "offline")

    async def test_reaper_marks_only_agents_past_the_threshold(self):
        await self._insert_agent("u-fresh", "online", 10)
        await self._insert_agent("u-inside", "online", 40)
        await self._insert_agent("u-past", "online", 70)
        await self._insert_agent("u-idle-past", "idle", 200)
        with mock.patch.dict(os.environ, {"HIVE_AGENT_STALE_SECONDS": "60"}):
            self.assertEqual(agent_bus.agent_offline_after(), 60.0)
            await self._reap()
        self.assertEqual((await self._agent_row("u-fresh"))[0], "online")
        self.assertEqual((await self._agent_row("u-inside"))[0], "online")
        self.assertEqual((await self._agent_row("u-past"))[0], "offline")
        self.assertEqual((await self._agent_row("u-idle-past"))[0], "offline")
        self.assertEqual(len(await self._events("agent.offline", "u-past")), 1)

    async def test_reaper_env_garbage_or_larger_value_changes_nothing(self):
        for raw in ("", "abc", "0", "-5", "9999"):
            with mock.patch.dict(os.environ, {"HIVE_AGENT_STALE_SECONDS": raw}):
                self.assertEqual(agent_bus.agent_offline_after(), agent_bus.HEARTBEAT_OFFLINE_AFTER, raw)


if __name__ == "__main__":
    unittest.main()
