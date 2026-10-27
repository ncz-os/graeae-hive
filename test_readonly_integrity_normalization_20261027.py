"""A read-only job with no artifact must not fail on the default-branch guard.

2026-10-27: the deployed worker's delivery-integrity check reads the branch the
job's workspace was checked out on as if it were a delivered ref, so every
review: job that (correctly) produced no commits died as
delivery_integrity_check_failed with "delivery landed on default branch
'master'". The bus now normalizes exactly that contradiction: read-only kind,
failed, no commits, no push, default-branch as the only complaint.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")
os.environ.setdefault("HIVE_BUS_ADMIN_TOKEN", "test-admin-token")

import agent_bus
from hive.bus.routers import jobs as hive_jobs_router
from hive.bus.schemas import JobUpdate
from hive.persistence import sqlite as sqlite_mod


class ReadonlyIntegrityNormalizationTests(unittest.IsolatedAsyncioTestCase):
    async def _repo(self):
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        repo = sqlite_mod.SqliteHiveRepository(str(Path(td.name) / "ro-int.db"))
        await repo.init(agent_bus.SCHEMA)
        return repo

    async def _mk(self, repo, job_id, kind="review:zoder-standalone-profile-r3f"):
        now = time.time()
        async with repo.connection() as db:
            await db.execute(
                "INSERT INTO jobs (id, submitter_urn, kind, status, started_at, claimed_by, "
                "claimed_at, max_retries) VALUES (?, 'urn:submitter', ?, 'running', ?, "
                "'urn:agent:zeroclaw:host:x', ?, 2)",
                (job_id, kind, now, now),
            )
            await db.commit()

    async def _patch(self, repo, job_id, result, status="failed"):
        with (
            mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo),
            mock.patch.object(hive_jobs_router, "connect_db", repo.connection),
        ):
            return await hive_jobs_router.update_job(
                job_id,
                JobUpdate(status=status, claimed_by="urn:agent:zeroclaw:host:x", result=result),
            )

    async def _row(self, repo, job_id):
        async with repo.connection() as db:
            async with db.execute("SELECT status, result FROM jobs WHERE id=?", (job_id,)) as cur:
                return await cur.fetchone()

    async def test_readonly_default_branch_false_positive_is_normalized(self):
        repo = await self._repo()
        await self._mk(repo, "rev-1")
        result = {
            "failure_reason": "delivery_integrity_check_failed",
            "delivery_integrity_errors": ["delivery landed on default branch 'master' (refs/heads/master)"],
            "commits": [],
            "codex_delivery": {"push": {"status": "skipped"}},
            "delivery": {"verdict": "request_changes"},
        }
        await self._patch(repo, "rev-1", result)
        row = await self._row(repo, "rev-1")
        self.assertEqual(row[0], "done", row)
        stored = json.loads(row[1])
        self.assertEqual(stored.get("delivery_normalized"), "readonly-no-artifact-default-branch")

    async def test_code_job_and_artifact_cases_still_fail(self):
        repo = await self._repo()
        await self._mk(repo, "code-1", kind="fix:something")
        await self._patch(repo, "code-1", {
            "failure_reason": "delivery_integrity_check_failed",
            "delivery_integrity_errors": ["delivery landed on default branch 'master' (refs/heads/master)"],
            "commits": [],
        })
        row = await self._row(repo, "code-1")
        self.assertNotEqual(row[0], "done", "a code job must not be normalized to done")

        await self._mk(repo, "rev-2")
        await self._patch(repo, "rev-2", {
            "failure_reason": "delivery_integrity_check_failed",
            "delivery_integrity_errors": ["integrity contradiction: evidence commits=['abc']"],
            "commits": ["abc"],
        })
        row = await self._row(repo, "rev-2")
        self.assertNotEqual(row[0], "done", "a real integrity contradiction must not be normalized")
