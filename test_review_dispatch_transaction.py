"""Real SQLite transactions for immediate review dispatch and rollback."""

import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import agent_bus
from hive.bus.domain.review_dispatch import dispatch_needs_review
from hive.bus.routers import jobs
from hive.bus.schemas import JobUpdate
from hive.persistence.sqlite import SqliteHiveRepository
from queue_logic import dedup_key


class ReviewDispatchTransaction(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.repo = SqliteHiveRepository(str(Path(self.tmp.name) / "review.db"))
        await self.repo.init(agent_bus.SCHEMA)
        async with self.repo.connection() as tx:
            await self.repo.submit_job(
                tx,
                id="source",
                submitter_urn="urn:submitter",
                kind="fix:thing",
                status="running",
                repo="owner/repo",
                head_branch="fix/thing",
                base_branch="main",
                started_at=time.time(),
            )
            await tx.commit()

    async def rows(self):
        async with self.repo.connection() as tx:
            _, rows = await self.repo.list_jobs(tx, limit=100)
        return rows

    async def test_production_patch_dispatches_once_with_head_and_family_unpinned(self):
        with patch.object(jobs, "hive_repo", return_value=self.repo):
            await jobs.update_job("source", JobUpdate(status="needs-review", result={"head_sha": "a" * 40}))
        rows = await self.rows()
        child = next(row for row in rows if row["parent_job_id"] == "source")
        parent = next(row for row in rows if row["id"] == "source")
        self.assertEqual(parent["result"]["auto_review_job_id"], child["id"])
        self.assertGreaterEqual(child["priority"], 100)
        self.assertFalse(child["eligible_kinds"])
        self.assertFalse(child["preferred_models"])
        self.assertEqual(child["max_cost_tier"], "C")
        self.assertEqual(child["head_branch"], "fix/thing")
        self.assertIn("a" * 40, child["description"])
        async with self.repo.connection() as tx:
            same = await dispatch_needs_review(tx, self.repo, "source", now=time.time())
            await tx.commit()
        self.assertEqual(same, child["id"])
        self.assertEqual(len(await self.rows()), 2)

    async def test_rollback_does_not_leave_child_or_parent_marker(self):
        with self.assertRaisesRegex(RuntimeError, "abort"):
            async with self.repo.connection() as tx:
                await self.repo.patch_job_status(tx, "source", sets={"status": "needs-review"})
                await dispatch_needs_review(tx, self.repo, "source", now=time.time())
                raise RuntimeError("abort")
        rows = await self.rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "running")
        self.assertFalse((rows[0]["result"] or {}).get("auto_review_job_id"))

    async def test_reconciliation_winner_is_reused(self):
        async with self.repo.connection() as tx:
            await self.repo.patch_job_status(tx, "source", sets={"status": "needs-review"})
            await self.repo.submit_job(
                tx,
                id="doctor-child",
                submitter_urn="urn:doctor",
                kind="review:thing",
                parent_job_id="source",
                started_at=time.time(),
                dedup_hash=dedup_key(idempotency_key="needs-review-followup:source"),
            )
            same = await dispatch_needs_review(tx, self.repo, "source", now=time.time())
            await tx.commit()
        self.assertEqual(same, "doctor-child")
        self.assertEqual(len(await self.rows()), 2)

    async def test_protected_lane_preserved(self):
        with patch.dict("os.environ", {"DLQ_PROTECT_KIND_PREFIXES": "fix:"}):
            async with self.repo.connection() as tx:
                await self.repo.patch_job_status(tx, "source", sets={"status": "needs-review"})
                result = await dispatch_needs_review(tx, self.repo, "source", now=time.time())
                await tx.commit()
        self.assertIsNone(result)
        self.assertEqual(len(await self.rows()), 1)

    async def test_generic_failure_does_not_hide_nested_protected_reason(self):
        async with self.repo.connection() as tx:
            await self.repo.patch_job_status(
                tx,
                "source",
                sets={
                    "status": "needs-review",
                    "result": json.dumps(
                        {"failure_reason": "delivery_failed", "error": {"reason": "dlq_protected:other-owner"}}
                    ),
                },
            )
            result = await dispatch_needs_review(tx, self.repo, "source", now=time.time())
            await tx.commit()
        self.assertIsNone(result)
        self.assertEqual(len(await self.rows()), 1)

    async def test_child_verdict_completes_only_approved_matching_artifact(self):
        for verdict, head, artifact, expected in [
            ("approve", "a" * 40, True, "done"),
            ("request-changes", "a" * 40, True, "needs-review"),
            ("approve", "b" * 40, True, "needs-review"),
            ("approve", "a" * 40, False, "needs-review"),
        ]:
            with self.subTest(verdict=verdict, head=head, artifact=artifact):
                proof = {
                    "head_sha": "a" * 40,
                    "commits": ["a" * 40],
                    "files_changed": ["code.py"],
                    "worktree_clean": artifact,
                }
                async with self.repo.connection() as tx:
                    await tx.execute("DELETE FROM jobs WHERE parent_job_id='source'")
                    await self.repo.patch_job_status(tx, "source", sets={"status": "running", "result": None})
                    await tx.commit()
                with patch.object(jobs, "hive_repo", return_value=self.repo):
                    await jobs.update_job("source", JobUpdate(status="needs-review", result=proof))
                    rows = await self.rows()
                    child = next(row for row in rows if row["parent_job_id"] == "source")
                    child_proof = {
                        "verdict": {"verdict": verdict, "head": head},
                        "deliverable": "Reviewed exact commit.",
                    }
                    async with self.repo.connection() as tx:
                        await self.repo.patch_job_status(tx, child["id"], sets={"status": "running"})
                        await tx.commit()
                    await jobs.update_job(child["id"], JobUpdate(status="done", result=child_proof))
                parent = next(row for row in await self.rows() if row["id"] == "source")
                self.assertEqual(parent["status"], expected)
                self.assertEqual(parent["result"]["review_evidence"]["verdict"]["verdict"], verdict)
                self.assertEqual(parent["result"]["head_sha"], "a" * 40)

    async def test_corrected_child_verdict_can_unblock_parent(self):
        proof = {"head_sha": "a" * 40, "commits": ["a" * 40], "files_changed": ["code.py"], "worktree_clean": True}
        with patch.object(jobs, "hive_repo", return_value=self.repo):
            await jobs.update_job("source", JobUpdate(status="needs-review", result=proof))
            child = next(row for row in await self.rows() if row["parent_job_id"] == "source")
            async with self.repo.connection() as tx:
                await self.repo.patch_job_status(tx, child["id"], sets={"status": "running"})
                await tx.commit()
            for verdict in ("request-changes", "approve"):
                await jobs.update_job(
                    child["id"],
                    JobUpdate(
                        status="done",
                        result={"verdict": {"verdict": verdict, "head_sha": "a" * 40}, "deliverable": "Reviewed"},
                    ),
                )
                parent = next(row for row in await self.rows() if row["id"] == "source")
                self.assertEqual(parent["status"], "done" if verdict == "approve" else "needs-review")
