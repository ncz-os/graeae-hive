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
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from fastapi import HTTPException

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


    async def _audit(self, job_id):
        async with self.repo.connection() as tx:
            async with tx.execute(
                "SELECT actor_urn, old_status, new_status, patch FROM job_audit_log WHERE job_id=?",
                (job_id,),
            ) as cur:
                return [tuple(row) for row in await cur.fetchall()]

    async def _reclassify(self, *job_ids, **request):
        with mock.patch.object(admin_router, "hive_repo", lambda: self.repo):
            return await admin_router.admin_reclassify_jobs(
                agent_bus.ReclassifyRequest(job_ids=list(job_ids), **request), actor=ACTOR
            )

    async def test_dead_letter_reclassify_without_override_is_refused_whole(self):
        await self._job("job-dlq", status="dead-letter", result={"exit_code": 1, "hive_reviews": REVIEWS})
        with self.assertRaises(HTTPException) as refused:
            await self._reclassify("job-dlq", status="queued")
        self.assertEqual(refused.exception.status_code, 422)

        row = await self._row("job-dlq")
        self.assertEqual(row["status"], "dead-letter")
        self.assertEqual(row["result"], {"exit_code": 1, "hive_reviews": REVIEWS})
        self.assertEqual(await self._history("job-dlq"), [])
        self.assertEqual(await self._audit("job-dlq"), [])

    async def test_dead_letter_reclassify_with_override_archives_and_audits(self):
        payload = {"exit_code": 1, "hive_reviews": REVIEWS}
        await self._job("job-dlq-override", status="dead-letter", result=payload, retry_count=1, max_retries=2)
        result = await self._reclassify(
            "job-dlq-override",
            status="queued",
            override_max_retries=True,
            override_reason="operator: reopen after fixing head branch",
        )
        self.assertEqual(result["ok"], ["job-dlq-override"])

        row = await self._row("job-dlq-override")
        self.assertEqual(row["status"], "queued")
        self.assertIsNone(row["result"])
        self.assertEqual(row["retry_count"], 0)

        (entry,) = await self._history("job-dlq-override")
        self.assertEqual(entry["action"], "reclassify")
        self.assertEqual(entry["prior_status"], "dead-letter")
        self.assertEqual(entry["actor"], ACTOR)
        self.assertEqual(entry["reason"], "operator: reopen after fixing head branch")
        self.assertEqual(entry["payload"]["hive_reviews"], REVIEWS)

        (audit,) = await self._audit("job-dlq-override")
        actor, old_status, new_status, patch = audit
        self.assertEqual((actor, old_status, new_status), (ACTOR, "dead-letter", "queued"))
        detail = json.loads(patch)
        self.assertEqual(detail["override"], "dead-letter")
        self.assertEqual(detail["reason"], "operator: reopen after fixing head branch")
        self.assertEqual((detail["max_retries"], detail["retry_count"]), (2, 1))


    async def test_repeated_requeues_append_two_history_entries(self):
        await self._job("job-repeat", result={"exit_code": 1}, retry_count=1, max_retries=2)
        async with self.repo.connection() as tx:
            changed = await self.repo.archive_and_requeue_job(
                tx, job_id="job-repeat", actor=ACTOR, reason="first", archived_at=10.0
            )
            await tx.commit()
        self.assertEqual(changed, 1)
        async with self.repo.connection() as tx:
            await tx.execute(
                "UPDATE jobs SET status='failed', result=?, retry_count=1 WHERE id=?",
                (json.dumps({"exit_code": 2}), "job-repeat"),
            )
            await tx.commit()
        async with self.repo.connection() as tx:
            changed = await self.repo.archive_and_requeue_job(
                tx, job_id="job-repeat", actor=ACTOR, reason="second", archived_at=20.0
            )
            await tx.commit()
        self.assertEqual(changed, 1)

        history = await self._history("job-repeat")
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["payload"], {"exit_code": 2})
        self.assertEqual(history[0]["reason"], "second")
        self.assertEqual(history[1]["payload"], {"exit_code": 1})
        self.assertEqual(history[1]["reason"], "first")

    async def test_history_write_failure_leaves_current_result_untouched(self):
        payload = {"exit_code": 1}
        await self._job("job-rollback", result=payload)
        async with self.repo.connection() as tx:
            await tx.execute(
                "INSERT INTO job_result_history "
                "(id, job_id, attempt, prior_status, archived_at, actor_urn, reason, action, payload) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("fb51-collision", "job-rollback", 2, "failed", 1.0, ACTOR, "seeded", "requeue", "{}"),
            )
            await tx.commit()
        fixed = SimpleNamespace(hex="fb51-collision")
        with mock.patch("hive.persistence.sqlite.uuid.uuid4", return_value=fixed):
            with self.assertRaises(sqlite3.IntegrityError):
                async with self.repo.connection() as tx:
                    await self.repo.archive_and_requeue_job(
                        tx, job_id="job-rollback", actor=ACTOR, reason="rollback", archived_at=2.0
                    )
                    await tx.commit()

        row = await self._row("job-rollback")
        self.assertEqual(row["status"], "failed")
        self.assertEqual(row["result"], payload)
        history = await self._history("job-rollback")
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["reason"], "seeded")

    async def test_concurrent_requeue_writes_no_duplicate_history(self):
        payload = {"exit_code": 1}
        await self._job("job-race-endpoint", result=payload)
        original = self.repo.patch_job_status

        async def interleave(tx, job_id, **kwargs):
            await tx.execute("UPDATE jobs SET status='done' WHERE id=?", (job_id,))
            return await original(tx, job_id, **kwargs)

        with mock.patch.object(self.repo, "patch_job_status", side_effect=interleave):
            result = await self._requeue("job-race-endpoint")
        self.assertEqual(result["skipped_conflict"], ["job-race-endpoint"])
        row = await self._row("job-race-endpoint")
        self.assertEqual(row["status"], "done")
        self.assertEqual(row["result"], payload)
        self.assertEqual(await self._history("job-race-endpoint"), [])


if __name__ == "__main__":
    unittest.main()
