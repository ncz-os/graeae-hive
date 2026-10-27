"""Real router/repository regression coverage for mutation guards and CAS."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from fastapi import FastAPI

from hive.bus import deps
from hive.bus.routers import admin, schedules
from hive.bus.schemas import ReclassifyRequest, RequeueRequest
from hive.persistence.sqlite import SqliteHiveRepository


class AdminScheduleAuditTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.repo = SqliteHiveRepository(str(Path(self.directory.name) / "test.db"))
        await self.repo.init()
        async with self.repo.connection() as tx:
            await tx.execute(
                "INSERT INTO jobs (id,submitter_urn,kind,status,started_at,max_retries) "
                "VALUES ('job','actor','build:audit','failed',1,2)"
            )
            await tx.execute(
                "INSERT INTO agents (urn,kind,host,session_id,started_at,last_heartbeat,status,runtime) "
                "VALUES ('actor','codex','test','s',1,1,'online','codex-orchestrator')"
            )
            await tx.commit()
        self.patch_repo = patch.object(admin, "hive_repo", return_value=self.repo)
        self.patch_repo.start()
        self.addCleanup(self.patch_repo.stop)

    async def test_requeue_does_not_overwrite_completion_after_preread(self):
        original = self.repo.patch_job_status

        async def interleave(tx, job_id, **kwargs):
            await tx.execute("UPDATE jobs SET status='done', result='winner' WHERE id=?", (job_id,))
            return await original(tx, job_id, **kwargs)

        with patch.object(self.repo, "patch_job_status", side_effect=interleave):
            result = await admin.admin_requeue_jobs(RequeueRequest(job_ids=["job"]), actor="actor")
        self.assertEqual(result["ok"], [])
        self.assertEqual(result["skipped_conflict"], ["job"])
        async with self.repo.connection() as tx:
            async with tx.execute("SELECT status,result FROM jobs WHERE id='job'") as cursor:
                self.assertEqual(tuple(await cursor.fetchone()), ("done", "winner"))

    async def test_reclassify_does_not_dispatch_after_lost_cas(self):
        original = self.repo.patch_job_status

        async def interleave(tx, job_id, **kwargs):
            await tx.execute("UPDATE jobs SET status='done' WHERE id=?", (job_id,))
            return await original(tx, job_id, **kwargs)

        with patch.object(self.repo, "patch_job_status", side_effect=interleave):
            result = await admin.admin_reclassify_jobs(
                ReclassifyRequest(job_ids=["job"], status="needs-review"), actor="actor"
            )
        self.assertEqual(result["ok"], [])
        self.assertEqual(result["skipped_conflict"], ["job"])
        async with self.repo.connection() as tx:
            total, children = await self.repo.list_jobs(tx, parent_job_id="job")
            self.assertEqual(total, 0)
            self.assertEqual(children, [])

    async def test_reclassify_dispatches_exactly_one_review_before_commit(self):
        request = ReclassifyRequest(job_ids=["job"], status="needs-review", reason="review")
        await admin.admin_reclassify_jobs(request, actor="actor")
        await admin.admin_reclassify_jobs(request, actor="actor")
        async with self.repo.connection() as tx:
            _, children = await self.repo.list_jobs(tx, parent_job_id="job")
            self.assertEqual(len(children), 1)
            self.assertEqual(children[0]["kind"], "review:build:audit")
            async with tx.execute("SELECT result FROM jobs WHERE id='job'") as cursor:
                marker = json.loads((await cursor.fetchone())[0])
            self.assertEqual(marker["auto_review_job_id"], children[0]["id"])

    async def test_reclassify_dispatch_failure_rolls_back_transition(self):
        async def fail(*args, **kwargs):
            raise RuntimeError("dispatch failed")

        with patch.object(admin, "dispatch_needs_review", side_effect=fail):
            with self.assertRaisesRegex(RuntimeError, "dispatch failed"):
                await admin.admin_reclassify_jobs(
                    ReclassifyRequest(job_ids=["job"], status="needs-review"), actor="actor"
                )
        async with self.repo.connection() as tx:
            async with tx.execute("SELECT status FROM jobs WHERE id='job'") as cursor:
                self.assertEqual((await cursor.fetchone())[0], "failed")

    async def test_schedule_mutations_require_actor_and_token(self):
        app = FastAPI()
        app.include_router(schedules.router)
        with patch.object(deps, "_HIVE_REPO", self.repo), patch.dict("os.environ", HIVE_SHARED_SECRET="secret"):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                for method, path in (
                    ("POST", "/v1/schedules"),
                    ("PATCH", "/v1/schedules/s"),
                    ("DELETE", "/v1/schedules/s"),
                ):
                    response = await client.request(method, path, json={})
                    self.assertEqual(response.status_code, 401)
                response = await client.patch(
                    "/v1/schedules/s?enabled=false", headers={"X-Hive-Actor": "actor", "X-Hive-Token": "wrong"}
                )
                self.assertEqual(response.status_code, 401)
                headers = {"X-Hive-Actor": "actor", "X-Hive-Token": "secret"}
                response = await client.post(
                    "/v1/schedules",
                    headers=headers,
                    json={
                        "name": "audit",
                        "interval_seconds": 60,
                        "job_template": {"submitter_urn": "actor", "kind": "build:audit", "description": "audit"},
                    },
                )
                self.assertEqual(response.status_code, 200, response.text)
                sid = response.json()["id"]
                response = await client.patch(f"/v1/schedules/{sid}?enabled=false", headers=headers)
                self.assertEqual(response.status_code, 200)
                response = await client.delete(f"/v1/schedules/{sid}", headers=headers)
                self.assertEqual(response.status_code, 200)
                response = await client.delete(f"/v1/schedules/{sid}", headers=headers)
                self.assertEqual(response.status_code, 404)
