"""Reaper scheduler — due selection, firing advance, and firing -> job creation."""

from __future__ import annotations

import json
import os
import tempfile
import time
import unittest

os.environ.setdefault("HIVE_BACKEND", "sqlite")
os.environ["AGENT_BUS_DB"] = os.path.join(tempfile.mkdtemp(prefix="hive-sched-"), "t.db")

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus.deps import hive_repo  # noqa: E402

NOW = time.time()


class SchedulerReaperTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await hive_repo().init()
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t"
        )

    async def asyncTearDown(self):
        await self.client.aclose()

    async def _schedule(self, sid, *, enabled, next_fire, template=None):
        async with hive_repo().connection() as tx:
            await hive_repo().create_schedule(
                tx,
                id=sid,
                name=sid,
                created_by_urn="urn:agent:opencode:h:1",
                interval_seconds=3600,
                job_template_json=json.dumps(template or {"kind": "sched:x", "description": "s"}),
                enabled=1 if enabled else 0,
                next_fire_at=next_fire,
                created_at=NOW - 100,
            )
            await tx.commit()

    async def test_select_due_only_enabled_and_past(self):
        await self._schedule("due", enabled=True, next_fire=NOW - 5)
        await self._schedule("future", enabled=True, next_fire=NOW + 3600)
        await self._schedule("disabled", enabled=False, next_fire=NOW - 5)
        async with hive_repo().connection() as tx:
            due = await hive_repo().select_due_schedules(tx, now=NOW)
        self.assertEqual([d[0] for d in due], ["due"])

    async def test_mark_fired_advances_and_counts(self):
        await self._schedule("s1", enabled=True, next_fire=NOW - 5)
        async with hive_repo().connection() as tx:
            await hive_repo().mark_schedule_fired(tx, sched_id="s1", now=NOW, next_at=NOW + 3600)
            await tx.commit()
        async with hive_repo().connection() as tx:
            async with tx.execute(
                "SELECT last_fired_at, next_fire_at, fire_count FROM scheduled_jobs WHERE id=?", ("s1",)
            ) as cur:
                last, nxt, count = await cur.fetchone()
        self.assertEqual(last, NOW)
        self.assertEqual(nxt, NOW + 3600)
        self.assertEqual(count, 1)

    async def test_firing_the_template_creates_a_job(self):
        # register the orchestrator that owns the schedule
        r = await self.client.post(
            "/v1/agents/register",
            json={"kind": "opencode", "host": "h", "runtime": "opencode", "status": "online",
                  "capabilities": ["code"], "auth_method": "api"},
        )
        urn = r.json()["urn"]
        await self._schedule("s2", enabled=True, next_fire=NOW - 5,
                             template={"kind": "sched:daily", "description": "nightly"})
        async with hive_repo().connection() as tx:
            await tx.execute("UPDATE scheduled_jobs SET created_by_urn=? WHERE id=?", (urn, "s2"))
            await tx.commit()
            due = await hive_repo().select_due_schedules(tx, now=NOW)
        self.assertEqual(len(due), 1)
        sid, name, sub_urn, interval, tpl_json, fcount = due[0]
        self.assertEqual(sub_urn, urn)

        # the reaper's firing step: synth the JobCreate and create it
        from hive.bus.routers.jobs import create_job
        from hive.bus.schemas import JobCreate

        tpl = json.loads(tpl_json)
        tpl["submitter_urn"] = sub_urn
        created = await create_job(JobCreate(**tpl))
        jid = created["id"] if isinstance(created, dict) else created
        async with hive_repo().connection() as tx:
            await hive_repo().mark_schedule_fired(tx, sched_id=sid, now=NOW, next_at=NOW + interval)
            await tx.commit()
            async with tx.execute("SELECT kind, description FROM jobs WHERE id=?", (jid,)) as cur:
                kind, desc = await cur.fetchone()
        self.assertEqual(kind, "sched:daily")
        self.assertEqual(desc, "nightly")


if __name__ == "__main__":
    unittest.main()
