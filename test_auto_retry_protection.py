"""Auto-restore must preserve operator/protection/routing refusals in stored results."""

import json
from unittest import mock

from fastapi import HTTPException

from test_dependency_release_20261027 import _RepoCase, hive_jobs_router


class AutoRetryProtectionTests(_RepoCase):
    async def test_stored_protection_and_holds_cannot_auto_retry(self):
        for result in (
            {"failure_reason": "protection_violation"},
            {"error": "protection violation: tracked source"},
            {"worker_error": "host_declines_kind"},
            {"stderr": "released_by_host: intentional"},
            {"failure_reason": "operator_hold"},
            {"needs_operator": True},
            {"retryable": False},
            {"tail": "provider idle timeout; protection_violation"},
            {"error": "no_workspace_for_repo: missing"},
        ):
            with self.subTest(result=result):
                repo = await self._repo()
                await self._mk(repo, "job", status="failed", max_retries=3)
                async with repo.connection() as db:
                    await db.execute("UPDATE jobs SET kind='fix:x', result=? WHERE id='job'", (json.dumps(result),))
                    await db.commit()
                with mock.patch.object(hive_jobs_router, "hive_repo", lambda repo=repo: repo):
                    with self.assertRaises(HTTPException) as error:
                        await hive_jobs_router.auto_retry_job("job")
                    self.assertEqual(error.exception.status_code, 409)
                async with repo.connection() as db:
                    async with db.execute("SELECT status,retry_count,result FROM jobs WHERE id='job'") as cur:
                        row = await cur.fetchone()
                self.assertEqual(row[0:2], ("failed", 0))
                self.assertEqual(json.loads(row[2]), result)

    async def test_retryable_failure_still_requeues(self):
        repo = await self._repo()
        await self._mk(repo, "job", status="failed", max_retries=3)
        async with repo.connection() as db:
            await db.execute(
                "UPDATE jobs SET kind='fix:x', result=? WHERE id='job'", ('{"error":"temporary network failure"}',)
            )
            await db.commit()
        with mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo):
            result = await hive_jobs_router.auto_retry_job("job")
        self.assertEqual(result["retry_count"], 1)
        self.assertEqual((await self._row(repo, "job"))[0], "queued")
