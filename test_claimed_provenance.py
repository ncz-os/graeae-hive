"""Model-provenance write path: PATCH /v1/jobs/{id} claimed_* fields.

Workers on the provenance branch report the model they ACTUALLY ran via
JobUpdate.claimed_model/claimed_provider/claimed_runtime/claimed_cost_tier.
The bus used to drop those keys (pydantic ignores extras), so the job row kept
the agent's REGISTRATION model (e.g. 'zoder') forever. These tests pin:

  * a claimant's PATCH persists and is visible on GET /v1/jobs/{id} and the list
  * a non-claimant can NOT set provenance
  * an explicit None never clobbers an already-recorded value
  * a done update whose result carries models.author records the real model
"""

import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")
os.environ.setdefault("HIVE_BUS_ADMIN_TOKEN", "test-admin-token")
os.environ.setdefault("AGENT_BUS_DB", str(Path(tempfile.gettempdir()) / "hive-provenance-tests.db"))

import agent_bus
from hive.bus.routers import jobs as hive_jobs_router
from hive.bus.schemas import JobUpdate
from hive.persistence.sqlite import SqliteHiveRepository

CLAIMANT = "urn:agent:zeroclaw:typhon:s1"


class ClaimedProvenanceTests(unittest.IsolatedAsyncioTestCase):
    async def _repo(self, name):
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        repo = SqliteHiveRepository(str(Path(td.name) / name))
        await repo.init(agent_bus.SCHEMA)
        return repo

    async def _claimed_job(self, repo, job_id="job-prov", model="zoder"):
        now = time.time()
        async with repo.connection() as db:
            await db.execute(
                "INSERT INTO jobs (id, submitter_urn, kind, priority, status, claimed_by, "
                "claimed_model, claimed_runtime, claimed_provider, claimed_cost_tier, started_at) "
                "VALUES (?, 'urn:submitter', 'research', 5, 'running', ?, ?, 'zeroclaw', ?, 'B', ?)",
                (job_id, CLAIMANT, model, "local", now),
            )
            await db.commit()
        return job_id

    async def _patch(self, repo, job_id, **kw):
        with (
            mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo),
            mock.patch.object(hive_jobs_router, "emit_event", mock.AsyncMock()),
            mock.patch("hive.bus.deps.hive_repo", lambda: repo),
        ):
            return await hive_jobs_router.update_job(job_id, JobUpdate(**kw))

    async def _get(self, repo, job_id):
        with mock.patch("hive.bus.deps.hive_repo", lambda: repo):
            return await hive_jobs_router.get_job(job_id)

    async def _list(self, repo):
        with mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo):
            return await hive_jobs_router.list_jobs(limit=50)

    async def test_patch_persists_and_is_returned_by_get_and_list(self):
        repo = await self._repo("persist.db")
        job_id = await self._claimed_job(repo)
        await self._patch(
            repo,
            job_id,
            status="running",
            claimed_by=CLAIMANT,
            claimed_model="claude-opus-4-7",
            claimed_provider="anthropic",
            claimed_runtime="claude-code",
            claimed_cost_tier="C",
        )
        got = await self._get(repo, job_id)
        self.assertEqual(got["claimed_model"], "claude-opus-4-7")
        self.assertEqual(got["claimed_provider"], "anthropic")
        self.assertEqual(got["claimed_runtime"], "claude-code")
        self.assertEqual(got["claimed_cost_tier"], "C")
        listed = await self._list(repo)
        row = next(j for j in listed["jobs"] if j["id"] == job_id)
        self.assertEqual(row["claimed_model"], "claude-opus-4-7")
        self.assertEqual(row["claimed_runtime"], "claude-code")

    async def test_non_claimant_cannot_set_provenance(self):
        from fastapi import HTTPException

        repo = await self._repo("nonclaimant.db")
        job_id = await self._claimed_job(repo)
        for kw in (
            # a stranger attempting the update outright
            {"claimed_by": "urn:agent:zeroclaw:other:s9", "claimed_model": "stolen-model"},
            # and an anonymous caller that omits claimed_by entirely
            {"claimed_model": "stolen-model"},
        ):
            with self.assertRaises(HTTPException) as ctx:
                await self._patch(repo, job_id, status="running", **kw)
            self.assertIn(ctx.exception.status_code, (403, 422))
        got = await self._get(repo, job_id)
        self.assertEqual(got["claimed_model"], "zoder")

    async def test_none_does_not_clobber(self):
        repo = await self._repo("none.db")
        job_id = await self._claimed_job(repo)
        await self._patch(
            repo,
            job_id,
            status="running",
            claimed_by=CLAIMANT,
            claimed_model="kimi-k2.6",
        )
        # Explicit None plus blank strings must leave the row untouched.
        await self._patch(
            repo,
            job_id,
            status="running",
            claimed_by=CLAIMANT,
            claimed_model=None,
            claimed_provider="   ",
        )
        got = await self._get(repo, job_id)
        self.assertEqual(got["claimed_model"], "kimi-k2.6")
        self.assertEqual(got["claimed_provider"], "local")

    async def test_done_records_result_models_author(self):
        repo = await self._repo("done.db")
        job_id = await self._claimed_job(repo)
        await self._patch(
            repo,
            job_id,
            status="done",
            claimed_by=CLAIMANT,
            result={
                "exit_code": 0,
                "models": {"author": "grok-4.3", "reviewers": ["deepseek-v4-pro"]},
            },
        )
        got = await self._get(repo, job_id)
        self.assertEqual(got["claimed_model"], "grok-4.3")

    async def test_done_explicit_claimed_model_wins_over_result_author(self):
        repo = await self._repo("done-explicit.db")
        job_id = await self._claimed_job(repo)
        await self._patch(
            repo,
            job_id,
            status="done",
            claimed_by=CLAIMANT,
            claimed_model="minimax-m3",
            result={"exit_code": 0, "models": {"author": "grok-4.3"}},
        )
        got = await self._get(repo, job_id)
        self.assertEqual(got["claimed_model"], "minimax-m3")


if __name__ == "__main__":
    unittest.main()