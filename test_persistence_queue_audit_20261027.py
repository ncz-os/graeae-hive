"""Production repository paths with deterministic driver contracts (no network)."""

import sys
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from hive.persistence.mysql import MySqlHiveRepository
from hive.persistence.postgres import PostgresHiveRepository


class PersistenceQueueAuditTests(unittest.IsolatedAsyncioTestCase):
    def postgres_driver(self, tag="UPDATE 0"):
        transaction = SimpleNamespace(start=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock())
        raw = SimpleNamespace(
            transaction=Mock(return_value=transaction),
            execute=AsyncMock(return_value=tag),
            fetch=AsyncMock(return_value=[]),
            close=AsyncMock(),
        )
        return raw, transaction, SimpleNamespace(connect=AsyncMock(return_value=raw))

    async def test_postgres_cas_preserves_zero_and_actual_rowcount(self):
        for tag, count in (("UPDATE 0", 0), ("UPDATE 1", 1), ("DELETE 2", 2)):
            raw, transaction, driver = self.postgres_driver(tag)
            repo = PostgresHiveRepository("postgres://unused")
            with patch.dict(sys.modules, asyncpg=driver):
                async with repo.connection() as tx:
                    self.assertIs(tx._repo, repo)
                    actual = await repo.patch_job_status(tx, "job", sets={"status": "done"}, expected_status="running")
                    self.assertEqual(actual, count)
                    await tx.commit()
            transaction.start.assert_awaited_once()
            transaction.commit.assert_awaited_once()
            transaction.rollback.assert_not_awaited()
            raw.close.assert_awaited_once()

    async def test_postgres_failure_rolls_back_without_replaying_statement(self):
        raw, transaction, driver = self.postgres_driver()
        raw.execute.side_effect = RuntimeError("database failure")
        repo = PostgresHiveRepository("postgres://unused")
        with patch.dict(sys.modules, asyncpg=driver):
            with self.assertRaisesRegex(RuntimeError, "database failure"):
                async with repo.connection() as tx:
                    await repo.patch_job_status(tx, "job", sets={"status": "done"})
        raw.execute.assert_awaited_once()
        raw.fetch.assert_not_awaited()
        transaction.rollback.assert_awaited_once()
        transaction.commit.assert_not_awaited()

    async def test_postgres_uncommitted_work_rolled_back_and_new_work_starts_transaction(self):
        raw, transaction, driver = self.postgres_driver("UPDATE 1")
        repo = PostgresHiveRepository("postgres://unused")
        with patch.dict(sys.modules, asyncpg=driver):
            async with repo.connection() as tx:
                await repo.patch_job_status(tx, "one", sets={"status": "done"})
                await tx.commit()
                await repo.patch_job_status(tx, "two", sets={"status": "done"})
        self.assertEqual(transaction.start.await_count, 2)
        transaction.commit.assert_awaited_once()
        transaction.rollback.assert_awaited_once()

    async def test_mysql_cas_reads_rowcount_after_execution_and_disables_autocommit(self):
        for count in (0, 1):
            cursor = SimpleNamespace(rowcount=-1)

            async def execute(*args, cursor=cursor, count=count):
                cursor.rowcount = count

            cursor.execute = AsyncMock(side_effect=execute)
            raw = SimpleNamespace(
                cursor=AsyncMock(return_value=cursor),
                commit=AsyncMock(),
                rollback=AsyncMock(),
                close=Mock(),
            )
            driver = SimpleNamespace(connect=AsyncMock(return_value=raw))
            repo = MySqlHiveRepository("mysql://unused/hive")
            with patch.dict(sys.modules, aiomysql=driver):
                async with repo.connection() as tx:
                    self.assertIs(tx._repo, repo)
                    actual = await repo.patch_job_status(tx, "job", sets={"status": "done"}, expected_status="running")
                    self.assertEqual(actual, count)
                    await tx.commit()
            self.assertFalse(driver.connect.call_args.kwargs["autocommit"])
            raw.commit.assert_awaited_once()
            raw.rollback.assert_awaited_once()
            raw.close.assert_called_once()

    async def test_mysql_failed_transaction_rolled_back_on_exit(self):
        raw = SimpleNamespace(rollback=AsyncMock(), close=Mock())
        driver = SimpleNamespace(connect=AsyncMock(return_value=raw))
        with patch.dict(sys.modules, aiomysql=driver):
            with self.assertRaisesRegex(RuntimeError, "abort"):
                async with MySqlHiveRepository("mysql://unused/hive").connection():
                    raise RuntimeError("abort")
        raw.rollback.assert_awaited_once()
        raw.close.assert_called_once()


class DependencyTransactionAuditTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import tempfile
        from pathlib import Path

        from hive.persistence.sqlite import SqliteHiveRepository

        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.repo = SqliteHiveRepository(str(Path(self.directory.name) / "audit.db"))
        await self.repo.init()

    async def insert_job(self, tx, job_id, deps=None):
        import json

        await tx.execute(
            "INSERT INTO jobs (id, submitter_urn, kind, status, started_at, depends_on) "
            "VALUES (?, 'submitter', 'test', 'queued', 1, ?)",
            (job_id, json.dumps(deps) if deps else None),
        )

    async def test_release_events_do_not_commit_terminal_transition_on_failure(self):
        from hive.bus.deps import emit_event, release_dependencies

        async with self.repo.connection() as tx:
            await self.insert_job(tx, "dep")
            await self.insert_job(tx, "child", ["dep"])
            await tx.commit()
        with self.assertRaisesRegex(RuntimeError, "abort"):
            async with self.repo.connection() as tx:
                await self.repo.patch_job_status(tx, "dep", sets={"status": "failed"})
                await release_dependencies(tx, "dep", "failed", emit_event=emit_event)
                raise RuntimeError("abort")
        async with self.repo.connection() as tx:
            async with tx.execute("SELECT status FROM jobs WHERE id='dep'") as cursor:
                self.assertEqual((await cursor.fetchone())[0], "queued")
            async with tx.execute("SELECT depends_on FROM jobs WHERE id='child'") as cursor:
                self.assertEqual((await cursor.fetchone())[0], '["dep"]')
            self.assertEqual(await self.repo.tail_events(tx), [])

    async def test_claim_missing_dep_event_rolls_back_when_other_dep_blocks(self):
        from hive.persistence.sqlite import DependencyNotReady

        async with self.repo.connection() as tx:
            await self.insert_job(tx, "pending")
            await self.insert_job(tx, "child", ["missing", "pending"])
            await tx.commit()
        with self.assertRaises(DependencyNotReady):
            async with self.repo.connection() as tx:
                await self.repo.claim_job(
                    tx,
                    job_id="child",
                    agent_urn="worker",
                    now=10,
                    lease_expires_at=20,
                    runtime="test",
                    model="test",
                    provider="local",
                    cost_tier="A",
                )
        async with self.repo.connection() as tx:
            async with tx.execute("SELECT depends_on FROM jobs WHERE id='child'") as cursor:
                self.assertEqual((await cursor.fetchone())[0], '["missing", "pending"]')
            self.assertEqual(await self.repo.tail_events(tx), [])
