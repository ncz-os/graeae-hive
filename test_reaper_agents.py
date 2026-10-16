"""Reaper agent-lifecycle + retention repo methods (queue health).

Drives the HiveMindRepository methods the reaper calls, against a real (temp)
sqlite DB — so the SQL predicates (heartbeat windows, in-flight-claim safety,
retention windows) are exercised, not just the pure filter.
"""

from __future__ import annotations

import os
import tempfile
import time
import unittest

os.environ.setdefault("HIVE_BACKEND", "sqlite")
os.environ["AGENT_BUS_DB"] = os.path.join(tempfile.mkdtemp(prefix="hive-reaper-"), "t.db")

from hive.persistence.sqlite import SqliteHiveRepository  # noqa: E402

NOW = time.time()


def _agent(urn, status, hb_age):
    return dict(urn=urn, kind="zeroclaw", host="h", session_id="s", pid=1, capabilities="[]",
                version="v", started_at=NOW - 10_000, last_heartbeat=NOW - hb_age, status=status,
                metadata="{}", runtime="zeroclaw", model="m", provider="p", autonomy_level="a",
                cost_tier="B", current_load="0", auth_method="api", plan_cap_usd=0.0,
                plan_period_used_usd=0.0, subscription_pools="[]")


def _job(jid, status, claimed_by=None, claimed_at=None, started_at=None):
    return dict(id=jid, submitter_urn="urn:agent:opencode:h:1", kind="feat:x", description="d",
                priority=0, status=status, claimed_by=claimed_by, claimed_at=claimed_at,
                started_at=started_at or (NOW - 10), ended_at=None, result=None, retry_count=0,
                max_retries=2)


class AgentLifecycleReaperTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # a fresh DB per test (no cross-test contamination)
        self._dir = tempfile.mkdtemp(prefix="hive-reaper-")
        self.repo = SqliteHiveRepository(os.path.join(self._dir, "t.db"))
        await self.repo.init()

    def _dbpath(self):
        return self.repo.db_path

    async def _insert(self, table, row: dict):
        cols = ", ".join(row)
        ph = ", ".join("?" for _ in row)
        async with self.repo.connection() as tx:
            await tx.execute(f"INSERT INTO {table} ({cols}) VALUES ({ph})", tuple(row.values()))
            await tx.commit()

    async def test_stale_vs_dead_by_heartbeat_window(self):
        await self._insert("agents", _agent("u-fresh", "online", 10))      # < 90s
        await self._insert("agents", _agent("u-stale", "online", 120))     # 90..300
        await self._insert("agents", _agent("u-dead", "online", 400))      # > 300
        await self._insert("agents", _agent("u-offline", "offline", 500))  # already offline
        repo = self.repo
        async with repo.connection() as tx:
            stale = await repo.select_stale_agent_urns(tx, stale_cutoff=NOW - 90, offline_cutoff=NOW - 300)
            dead = await repo.select_dead_agent_urns(tx, offline_cutoff=NOW - 300)
        self.assertEqual(stale, ["u-stale"])
        self.assertNotIn("u-offline", dead, "already-offline agents are not 'dead' candidates")
        self.assertIn("u-dead", dead)
        self.assertNotIn("u-fresh", dead)

    async def test_mark_agents_stale_and_offline(self):
        await self._insert("agents", _agent("u1", "online", 120))
        repo = self.repo
        async with repo.connection() as tx:
            await repo.mark_agents_stale(tx, ["u1"])
            await tx.commit()
            status = await self._agent_status(tx, "u1")
            self.assertEqual(status, "stale")
            await repo.mark_agent_offline(tx, "u1")
            await tx.commit()
            self.assertEqual(await self._agent_status(tx, "u1"), "offline")

    async def _agent_status(self, tx, urn):
        async with tx.execute("SELECT status FROM agents WHERE urn=?", (urn,)) as cur:
            return (await cur.fetchone())[0]

    async def test_release_unstarted_jobs_only_offered_or_claimed(self):
        await self._insert("jobs", _job("j-offered", "offered", "u1", NOW - 50))
        await self._insert("jobs", _job("j-claimed", "claimed", "u1", NOW - 50))
        await self._insert("jobs", _job("j-running", "running", "u1", NOW - 50))
        repo = self.repo
        async with repo.connection() as tx:
            released = await repo.release_unstarted_jobs_for_agent(tx, "u1")
            await tx.commit()
        self.assertEqual(sorted(released), ["j-claimed", "j-offered"])
        async with repo.connection() as tx:
            self.assertEqual(await self._job_status(tx, "j-offered"), "queued")
            self.assertEqual(await self._job_status(tx, "j-claimed"), "queued")
            self.assertEqual(await self._job_status(tx, "j-running"), "running",
                             "running work must NOT be released (would discard a live result)")

    async def _job_status(self, tx, jid):
        async with tx.execute("SELECT status FROM jobs WHERE id=?", (jid,)) as cur:
            return (await cur.fetchone())[0]

    async def test_purge_protects_agents_with_inflight_claim(self):
        await self._insert("agents", _agent("u-idle", "offline", 30000))     # old + offline, no claim
        await self._insert("agents", _agent("u-busy", "offline", 30000))     # old + offline, HAS a claim
        await self._insert("jobs", _job("j1", "running", "u-busy", NOW - 50))
        repo = self.repo
        async with repo.connection() as tx:
            purged = await repo.purge_offline_agents(tx, cutoff=NOW - 21600)
            await tx.commit()
        self.assertEqual(purged, 1)
        async with repo.connection() as tx:
            async with tx.execute("SELECT urn FROM agents ORDER BY urn") as cur:
                left = [r[0] for r in await cur.fetchall()]
        self.assertEqual(left, ["u-busy"], "an agent holding an in-flight claim must survive the purge")

    async def test_retention_selects_only_terminal_and_old(self):
        await self._insert("jobs", _job("old-done", "done"))
        await self._insert("jobs", _job("new-done", "done"))
        await self._insert("jobs", _job("active", "running"))
        repo = self.repo
        async with repo.connection() as tx:
            await tx.execute("UPDATE jobs SET ended_at=? WHERE id=?", (NOW - 30 * 86400, "old-done"))
            await tx.execute("UPDATE jobs SET ended_at=? WHERE id=?", (NOW - 60, "new-done"))
            await tx.commit()
            ids = await repo.select_sweepable_terminal_job_ids(
                tx, statuses=["done", "failed", "cancelled"], cutoff=NOW - 7 * 86400
            )
        self.assertEqual(ids, ["old-done"], "only terminal jobs past the retention window are swept")


if __name__ == "__main__":
    unittest.main()
