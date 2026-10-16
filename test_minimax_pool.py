"""Regression: the claim path must populate the columns the MiniMax pool reads.

The pool occupancy query counts jobs by claimed_provider. If the claim UPDATE
did not write claimed_provider/claimed_model, the pool snapshot would always be
empty and the cap would never engage. This asserts the round trip: claim a
MiniMax job, see the columns set, then confirm the cap actually blocks the 6th.
"""

import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import agent_bus
from hive.bus.routers import jobs as hive_jobs_router
from hive.persistence.sqlite import SqliteHiveRepository

MINIMAX_WORKER = (
    "INSERT INTO agents (urn, kind, host, session_id, capabilities, started_at, "
    "last_heartbeat, status, runtime, cost_tier, model, provider) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)
MINIMAX_JOB = (
    "INSERT INTO jobs (id, submitter_urn, kind, priority, status, started_at) "
    "VALUES (?, 'urn:submitter', 'feat:edit', 5, 'queued', ?)"
)


class MiniMaxPoolWritePathTests(unittest.IsolatedAsyncioTestCase):
    async def _seed_minimax_worker(self, db, urn, host, now):
        await db.execute(
            MINIMAX_WORKER,
            (urn, "zeroclaw", host, "session", "[]", now, now, "online", "zeroclaw", "B", "minimax-m3", "minimax"),
        )

    async def test_claim_populates_pool_columns_then_cap_blocks(self):
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "mx-write.db"))
            await repo.init(agent_bus.SCHEMA)
            now = time.time()
            first = "urn:agent:zeroclaw:w0:s1"
            async with repo.connection() as db:
                await self._seed_minimax_worker(db, first, "w0", now)
                await db.execute(MINIMAX_JOB, ("job-a", now))
                await db.commit()

            with mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo), \
                    mock.patch.object(hive_jobs_router, "emit_event", mock.AsyncMock()):
                claimed = await hive_jobs_router.dequeue_next_job(first)

            # the claim must have written the provider the pool query filters on
            self.assertEqual(claimed.get("id"), "job-a")
            async with repo.connection() as db:
                async with db.execute(
                    "SELECT claimed_provider, claimed_model FROM jobs WHERE id='job-a'"
                ) as cur:
                    prov, model = await cur.fetchone()
            self.assertEqual(prov, "minimax")
            self.assertEqual(model, "minimax-m3")

            # fill the pool to the global cap with 4 more active sessions
            async with repo.connection() as db:
                for i in range(1, 5):
                    urn = f"urn:agent:zeroclaw:w{i}:s1"
                    await self._seed_minimax_worker(db, urn, f"w{i}", now)
                    await db.execute(
                        "INSERT INTO jobs (id, submitter_urn, kind, priority, status, claimed_by, "
                        "claimed_provider, claimed_model, claim_lease_expires_at, started_at) "
                        "VALUES (?, 'urn:submitter', 'feat:edit', 5, 'running', ?, 'minimax', "
                        "'minimax-m3', ?, ?)",
                        (f"job-{i}", urn, now + 3600, now),
                    )
                # the 6th worker + a queued job
                sixth = "urn:agent:zeroclaw:w6:s1"
                await self._seed_minimax_worker(db, sixth, "w6", now)
                await db.execute(MINIMAX_JOB, ("job-6", now))
                await db.commit()

            with mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo), \
                    mock.patch.object(hive_jobs_router, "emit_event", mock.AsyncMock()):
                denied = await hive_jobs_router.dequeue_next_job(sixth)
            self.assertEqual(denied.status_code, 204)
            self.assertEqual(denied.headers.get("X-Hive-Claim-Result"), "minimax_pool_saturated")


if __name__ == "__main__":
    unittest.main()
