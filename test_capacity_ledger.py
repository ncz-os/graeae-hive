"""Capacity ledger persistence — idempotent ABC storage (Phase 5)."""

from __future__ import annotations

import os
import tempfile
import unittest

os.environ.setdefault("HIVE_BACKEND", "sqlite")

from hive.persistence.sqlite import SqliteHiveRepository  # noqa: E402


class CapacityLedgerPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._dir = tempfile.mkdtemp(prefix="hive-cap-")
        self.repo = SqliteHiveRepository(os.path.join(self._dir, "t.db"))
        await self.repo.init()

    async def _row(self, service):
        async with self.repo.connection() as tx:
            for r in await self.repo.capacity_state_load(tx):
                if r[0] == service:
                    return r
        return None

    async def test_set_upsert_is_idempotent(self):
        # saving the SAME state twice must not double anything (SET, not increment)
        for _ in range(3):
            async with self.repo.connection() as tx:
                await self.repo.capacity_state_save(
                    tx, service="minimax", in_flight=2, tokens_used=1000, spent_usd=1.5,
                    limited_until=0.0, window_start=0.0, updated_at=10.0,
                )
                await tx.commit()
        row = await self._row("minimax")
        self.assertEqual(row[1:4], (2, 1000, 1.5), "re-saving the same state is a no-op")

    async def test_conflicting_keys_in_one_tx_apply_once(self):
        # first insert builds the row; second save updates the same key
        async with self.repo.connection() as tx:
            await self.repo.capacity_state_save(tx, service="minimax", in_flight=1, updated_at=1.0)
            await self.repo.capacity_state_save(tx, service="minimax", in_flight=4, updated_at=2.0)
            await tx.commit()
        row = await self._row("minimax")
        self.assertEqual(row[1], 4)
        async with self.repo.connection() as tx:
            rows = await self.repo.capacity_state_load(tx)
        self.assertEqual(len([r for r in rows if r[0] == "minimax"]), 1, "one row per service")

    async def test_fold_usage_is_idempotent(self):
        repo = self.repo
        async with repo.connection() as tx:
            first = await repo.capacity_fold_usage(tx, dedup_key="job-1", service="minimax", tokens=100, cost_usd=0.5, now=1.0)
            replay = await repo.capacity_fold_usage(tx, dedup_key="job-1", service="minimax", tokens=100, cost_usd=0.5, now=2.0)
            await tx.commit()
        self.assertTrue(first)
        self.assertFalse(replay, "the same dedup_key must not fold twice")
        row = await self._row("minimax")
        self.assertEqual(row[2], 100, "tokens counted once")
        self.assertAlmostEqual(row[3], 0.5, "cost counted once")

    async def test_fold_usage_distinct_keys_accumulate(self):
        repo = self.repo
        async with repo.connection() as tx:
            await repo.capacity_fold_usage(tx, dedup_key="job-a", service="minimax", tokens=10, cost_usd=0.1, now=1.0)
            await repo.capacity_fold_usage(tx, dedup_key="job-b", service="minimax", tokens=20, cost_usd=0.2, now=2.0)
            await tx.commit()
        row = await self._row("minimax")
        self.assertEqual(row[2], 30)
        self.assertAlmostEqual(row[3], 0.3)


if __name__ == "__main__":
    unittest.main()
