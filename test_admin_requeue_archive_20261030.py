"""FB51 step 1: an admin requeue archives the result before clearing it.

The bug: ``POST /v1/admin/jobs/requeue`` wrote ``result=NULL`` through
``patch_job_status``, so the ``hive_reviews`` evidence a reviewer had recorded
on the row was destroyed with no way to read it back.

The fix lives in the repository, not the endpoint: one operation
(``archive_and_requeue_job``) appends the previous non-null payload to an
append-only ``job_result_history`` table, then requeues the row, in the caller's
transaction. These tests pin the behaviour through the real sqlite backend and
the real endpoint function.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")

import agent_bus
from hive.bus.routers import admin as admin_router
from hive.persistence.sqlite import SqliteHiveRepository

ACTOR = "urn:agent:opencode:operator:1"
REVIEWS = {"verdict": "approve", "reviewers": [{"model": "nemotron-3-ultra", "verdict": "approve"}]}


class RequeueArchiveTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = SqliteHiveRepository(str(Path(self.temp.name) / "archive.db"))
        await self.repo.init(agent_bus.SCHEMA)

    async def _job(self, job_id="job-review", status="failed", result=None, retry_count=1, max_retries=2):
        async with self.repo.connection() as tx:
            await tx.execute(
                "INSERT INTO jobs (id, submitter_urn, kind, status, started_at, claimed_by, "
                "retry_count, max_retries, result) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    job_id,
                    "urn:submitter",
                    "fix:test",
                    status,
                    1.0,
                    "urn:agent:zeroclaw:worker:1",
                    retry_count,
                    max_retries,
                    json.dumps(result) if result is not None else None,
                ),
            )
            await tx.commit()
        return job_id

    async def _requeue(self, *job_ids, **request):
        with mock.patch.object(admin_router, "hive_repo", lambda: self.repo):
            return await admin_router.admin_requeue_jobs(
                agent_bus.RequeueRequest(job_ids=list(job_ids), **request), actor=ACTOR
            )

    async def _history(self, job_id):
        async with self.repo.connection() as tx:
            return await self.repo.list_job_result_history(tx, job_id=job_id, limit=50)

    async def _row(self, job_id):
        """The job as named columns (the raw row's positional order is schema-dependent)."""
        async with self.repo.connection() as tx:
            _total, rows = await self.repo.list_jobs(tx, limit=1000)
        return next(r for r in rows if r["id"] == job_id)

    async def test_requeue_keeps_the_review_evidence_it_clears(self):
        await self._job(result={"exit_code": 1, "hive_reviews": REVIEWS})
        result = await self._requeue("job-review")
        self.assertEqual(result["ok"], ["job-review"])

        row = await self._row("job-review")
        self.assertEqual(row["status"], "queued")
        self.assertIsNone(row["result"], "the current result must be cleared on requeue")

        history = await self._history("job-review")
        self.assertEqual(len(history), 1)
        entry = history[0]
        self.assertEqual(entry["attempt"], 2, "attempt number is retry_count + 1")
        self.assertEqual(entry["prior_status"], "failed")
        self.assertEqual(entry["actor"], ACTOR)
        self.assertEqual(entry["reason"], "manual-requeue")
        self.assertEqual(entry["action"], "requeue")
        self.assertEqual(entry["payload"]["hive_reviews"], REVIEWS)

    async def test_empty_result_adds_no_history_entry(self):
        await self._job("job-empty", status="failed", result=None)
        result = await self._requeue("job-empty")
        self.assertEqual(result["ok"], ["job-empty"])
        self.assertEqual(await self._history("job-empty"), [])

    async def test_blank_and_empty_json_results_add_no_history_entry(self):
        for index, payload in enumerate(("", "{}", "[]")):
            job_id = f"job-blank-{index}"
            async with self.repo.connection() as tx:
                await tx.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, status, started_at, result) "
                    "VALUES (?, 'urn:submitter', 'fix:test', 'failed', 1.0, ?)",
                    (job_id, payload),
                )
                await tx.commit()
            result = await self._requeue(job_id)
            self.assertEqual(result["ok"], [job_id], payload)
            self.assertEqual(await self._history(job_id), [], payload)

    async def test_history_endpoint_lists_newest_first_without_writing(self):
        await self._job("job-endpoint", result={"exit_code": 1, "hive_reviews": REVIEWS})
        async with self.repo.connection() as tx:
            await self.repo.archive_and_requeue_job(
                tx, job_id="job-endpoint", actor="urn:first", reason="first", archived_at=10.0
            )
            await tx.commit()
        async with self.repo.connection() as tx:
            await self.repo.archive_and_requeue_job(
                tx, job_id="job-endpoint", actor="urn:second", reason="second", archived_at=20.0
            )
            await tx.commit()

        with mock.patch.object(admin_router, "hive_repo", lambda: self.repo):
            body = await admin_router.admin_job_result_history("job-endpoint", limit=50)
        self.assertEqual(body["count"], 1, "only the first archive had a payload to store")
        self.assertEqual(body["history"][0]["actor"], "urn:first")

        # Read-only: calling it again returns the same rows.
        with mock.patch.object(admin_router, "hive_repo", lambda: self.repo):
            again = await admin_router.admin_job_result_history("job-endpoint", limit=50)
        self.assertEqual(again, body)

    async def test_history_endpoint_rejects_a_nonsense_limit(self):
        from fastapi import HTTPException

        with mock.patch.object(admin_router, "hive_repo", lambda: self.repo):
            with self.assertRaises(HTTPException) as ctx:
                await admin_router.admin_job_result_history("job-review", limit=0)
        self.assertEqual(ctx.exception.status_code, 422)

    async def test_lost_compare_and_set_leaves_no_history_row(self):
        await self._job("job-race", result={"exit_code": 1})
        async with self.repo.connection() as tx:
            changed = await self.repo.archive_and_requeue_job(
                tx,
                job_id="job-race",
                actor=ACTOR,
                reason="race",
                archived_at=5.0,
                expected_status="cancelled",  # the row is 'failed'
            )
            await tx.commit()
        self.assertEqual(changed, 0)
        self.assertEqual(await self._history("job-race"), [])

    async def test_the_operation_resets_retry_and_claim_state(self):
        await self._job("job-reset", status="cancelled", result={"exit_code": 0}, retry_count=2, max_retries=5)
        async with self.repo.connection() as tx:
            changed = await self.repo.archive_and_requeue_job(
                tx, job_id="job-reset", actor=ACTOR, reason="reset", archived_at=7.0
            )
            await tx.commit()
        self.assertEqual(changed, 1)
        row = await self._row("job-reset")
        self.assertEqual(row["status"], "queued")
        self.assertIsNone(row["claimed_by"], "claimed_by must be cleared")
        self.assertEqual(row["retry_count"], 0, "retry_count resets to 0")
        for column in ("claim_lease_expires_at", "claimed_at", "ended_at"):
            self.assertIsNone(row[column], column)


if __name__ == "__main__":
    unittest.main()
