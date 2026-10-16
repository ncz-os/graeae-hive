"""Reaper sweeps — stale-queued cancel, event purge, test-junk retention."""

from __future__ import annotations

import os
import tempfile
import time
import unittest

os.environ.setdefault("HIVE_BACKEND", "sqlite")
os.environ["AGENT_BUS_DB"] = os.path.join(tempfile.mkdtemp(prefix="hive-sweep-"), "t.db")

from hive.persistence.sqlite import SqliteHiveRepository  # noqa: E402

NOW = time.time()


def _job(jid, status, started_at, description="d"):
    return dict(id=jid, submitter_urn="urn:agent:opencode:h:1", kind="feat:x", description=description,
                priority=0, status=status, claimed_by=None, claimed_at=None, started_at=started_at,
                ended_at=None, result=None, retry_count=0, max_retries=2)


class ReaperSweepsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._dir = tempfile.mkdtemp(prefix="hive-sweep-")
        self.repo = SqliteHiveRepository(os.path.join(self._dir, "t.db"))
        await self.repo.init()

    async def _insert_job(self, row):
        cols = ", ".join(row)
        ph = ", ".join("?" for _ in row)
        async with self.repo.connection() as tx:
            await tx.execute(f"INSERT INTO jobs ({cols}) VALUES ({ph})", tuple(row.values()))
            await tx.commit()

    async def test_stale_queued_cancel_only_old_queued(self):
        old = NOW - 8 * 86400
        await self._insert_job(_job("old-queued", "queued", old))
        await self._insert_job(_job("recent-queued", "queued", NOW - 60))
        await self._insert_job(_job("old-running", "running", old))
        repo = self.repo
        cutoff = NOW - 7 * 86400
        async with repo.connection() as tx:
            ids = await repo.select_stale_queued_job_ids(tx, cutoff=cutoff)
        self.assertEqual(ids, ["old-queued"], "only queued jobs untouched >7d are cancelled")
        async with repo.connection() as tx:
            await repo.cancel_jobs(tx, ids, ended_at=NOW)
            await tx.commit()
            async with tx.execute("SELECT status, ended_at FROM jobs WHERE id=?", ("old-queued",)) as cur:
                status, ended = await cur.fetchone()
        self.assertEqual(status, "cancelled")
        self.assertEqual(ended, NOW)

    async def test_purge_events_before(self):
        repo = self.repo
        async with repo.connection() as tx:
            await tx.execute("INSERT INTO events (ts, kind, payload) VALUES (?,?,?)", (NOW - 1000, "old", "{}"))
            await tx.execute("INSERT INTO events (ts, kind, payload) VALUES (?,?,?)", (NOW - 10, "new", "{}"))
            await tx.commit()
            await repo.purge_events_before(tx, cutoff=NOW - 100)
            await tx.commit()
            async with tx.execute("SELECT kind FROM events") as cur:
                left = [r[0] for r in await cur.fetchall()]
        self.assertEqual(left, ["new"], "events older than the retention cutoff are purged")

    async def test_sweepable_test_junk_only_terminal_pattern_old(self):
        old = NOW - 3 * 86400
        await self._insert_job(_job("junk-done", "done", old, description="hive deploy e2e run"))
        await self._insert_job(_job("junk-active", "running", old, description="hive deploy e2e run"))
        await self._insert_job(_job("real-done", "done", old, description="ship the thing"))
        repo = self.repo
        async with repo.connection() as tx:
            await tx.execute("UPDATE jobs SET ended_at=? WHERE id=?", (old, "junk-done"))
            await tx.execute("UPDATE jobs SET ended_at=? WHERE id=?", (old, "real-done"))
            await tx.commit()
            ids = await repo.select_sweepable_test_job_ids(
                tx, statuses=["done", "failed", "cancelled"], patterns=("e2e",), cutoff=NOW - 86400
            )
        self.assertEqual(ids, ["junk-done"],
                         "only terminal jobs whose description matches a test pattern are swept")


if __name__ == "__main__":
    unittest.main()
