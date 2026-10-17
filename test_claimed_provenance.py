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

    async def test_unclaimed_job_cannot_receive_provenance(self):
        """No current claimant == nobody may stamp provenance, even by
        self-declaring a claimed_by on a claimed/running status."""
        from fastapi import HTTPException

        repo = await self._repo("unclaimed.db")
        now = time.time()
        async with repo.connection() as db:
            await db.execute(
                "INSERT INTO jobs (id, submitter_urn, kind, priority, status, started_at) "
                "VALUES ('job-open', 'urn:submitter', 'research', 5, 'queued', ?)",
                (now,),
            )
            await db.commit()
        # queued -> claimed is a claim bypass (409) regardless; assert either
        # way that no provenance lands on the row.
        with self.assertRaises(HTTPException) as ctx:
            await self._patch(
                repo,
                "job-open",
                status="claimed",
                claimed_by="urn:agent:zeroclaw:typhon:s1",
                claimed_model="sneaky-model",
            )
        self.assertIn(ctx.exception.status_code, (403, 409))
        async with repo.connection() as db:
            async with db.execute("SELECT claimed_model FROM jobs WHERE id='job-open'") as cur:
                self.assertIsNone((await cur.fetchone())[0])

    async def test_malformed_result_models_is_ignored(self):
        """`result['models']` is worker-supplied; a non-dict must not crash."""
        repo = await self._repo("malformed.db")
        for models in ("grok-4.3", ["grok-4.3"], None, 17, {"author": ["not-a-str"]}, {"author": "   "}):
            job_id = await self._claimed_job(repo, f"job-{abs(hash(repr(models)))}")
            await self._patch(
                repo,
                job_id,
                status="done",
                claimed_by=CLAIMANT,
                result={"exit_code": 0, "models": models},
            )
            got = await self._get(repo, job_id)
            # No author could be extracted -> the registration model stands.
            self.assertEqual(got["claimed_model"], "zoder", models)

    async def test_provenance_reaches_the_update_sql(self):
        """The provenance fields must land in the UPDATE that patch_job_status
        executes, not just in the in-memory `sets` map."""
        repo = await self._repo("updatesql.db")
        job_id = await self._claimed_job(repo, "job-sql")
        seen: list[dict] = []
        original = repo.patch_job_status

        async def spy(tx, jid, *, sets, expected_status=None):
            seen.append(dict(sets))
            return await original(tx, jid, sets=sets, expected_status=expected_status)

        with (
            mock.patch.object(repo, "patch_job_status", spy),
            mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo),
            mock.patch.object(hive_jobs_router, "emit_event", mock.AsyncMock()),
            mock.patch("hive.bus.deps.hive_repo", lambda: repo),
        ):
            await hive_jobs_router.update_job(
                job_id,
                JobUpdate(
                    status="running",
                    claimed_by=CLAIMANT,
                    claimed_model="claude-opus-4-7",
                    claimed_provider="anthropic",
                ),
            )
        self.assertTrue(seen, "patch_job_status was never called")
        self.assertEqual(seen[-1]["claimed_model"], "claude-opus-4-7")
        self.assertEqual(seen[-1]["claimed_provider"], "anthropic")
        # And the row really changed on disk.
        got = await self._get(repo, job_id)
        self.assertEqual(got["claimed_model"], "claude-opus-4-7")

    async def test_none_is_never_emitted_into_the_update_sql(self):
        """An absent provenance value must not appear in the UPDATE at all
        (emitting NULL would clobber the recorded model)."""
        repo = await self._repo("nonull.db")
        job_id = await self._claimed_job(repo, "job-nonull", model="kimi-k2.6")
        seen: list[dict] = []
        original = repo.patch_job_status

        async def spy(tx, jid, *, sets, expected_status=None):
            seen.append(dict(sets))
            return await original(tx, jid, sets=sets, expected_status=expected_status)

        with (
            mock.patch.object(repo, "patch_job_status", spy),
            mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo),
            mock.patch.object(hive_jobs_router, "emit_event", mock.AsyncMock()),
            mock.patch("hive.bus.deps.hive_repo", lambda: repo),
        ):
            await hive_jobs_router.update_job(job_id, JobUpdate(status="running", claimed_by=CLAIMANT))
        for col in ("claimed_model", "claimed_provider", "claimed_runtime", "claimed_cost_tier"):
            self.assertNotIn(col, seen[-1])
        got = await self._get(repo, job_id)
        self.assertEqual(got["claimed_model"], "kimi-k2.6")

    async def test_provenance_columns_exist_after_init_on_a_legacy_db(self):
        """A pre-provenance SQLite DB is migrated forward by init(): the four
        claimed_* columns are ALTERed in, existing rows survive, and GET works."""
        import sqlite3

        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        path = Path(td.name) / "legacy.db"
        con = sqlite3.connect(path)
        con.executescript(agent_bus.SCHEMA)
        for col in ("claimed_model", "claimed_provider", "claimed_runtime", "claimed_cost_tier"):
            con.execute(f"ALTER TABLE jobs DROP COLUMN {col}")
        con.execute(
            "INSERT INTO jobs (id, submitter_urn, kind, status, started_at, claimed_by) "
            "VALUES ('old-job', 'urn:submitter', 'research', 'done', 1.0, 'urn:agent:zeroclaw:x:s1')"
        )
        con.commit()
        con.close()

        repo = SqliteHiveRepository(str(path))
        await repo.init(agent_bus.SCHEMA)
        async with repo.connection() as db:
            async with db.execute("PRAGMA table_info(jobs)") as cur:
                cols = {r[1] for r in await cur.fetchall()}
        for col in ("claimed_model", "claimed_provider", "claimed_runtime", "claimed_cost_tier"):
            self.assertIn(col, cols)
        got = await self._get(repo, "old-job")
        self.assertEqual(got["id"], "old-job")
        self.assertIsNone(got["claimed_model"])

    async def test_oracle_translation_carries_the_provenance_columns(self):
        """The Oracle backend inherits patch_job_status and rewrites its SQL;
        the provenance columns must survive translation as bound params."""
        from hive.persistence.oracle import _translate_sql

        sql = (
            "UPDATE jobs SET status=?, last_update_at=?, result=?, claimed_model=?, "
            "claimed_provider=?, claimed_runtime=?, claimed_cost_tier=? WHERE id=? AND status=?"
        )
        translated, params = _translate_sql(
            sql,
            ["done", 1.0, "{}", "grok-4.3", "xai", "claude-code", "B", "job-1", "running"],
        )
        for col in ("claimed_model", "claimed_provider", "claimed_runtime", "claimed_cost_tier"):
            # present in the SET list and rewritten to an Oracle bind marker,
            # i.e. the value is BOUND, never inlined into the SQL text.
            self.assertRegex(translated, rf"{col}=:\d")
            self.assertNotIn("grok-4.3", translated)
        self.assertIn("grok-4.3", params)
        # Oracle's required-column contract must list all four.
        from hive.persistence.oracle import _ORACLE_REQUIRED_JOB_COLUMNS

        for col in ("CLAIMED_MODEL", "CLAIMED_PROVIDER", "CLAIMED_RUNTIME", "CLAIMED_COST_TIER"):
            self.assertIn(col, _ORACLE_REQUIRED_JOB_COLUMNS)

    async def test_normalize_preserves_models_key_on_done(self):
        """normalize_result_payload must not strip result['models']."""
        from hive.bus.domain.jobs import normalize_result_payload

        out = normalize_result_payload("done", {"models": {"author": "grok-4.3", "reviewers": []}})
        self.assertEqual(out["models"]["author"], "grok-4.3")
        self.assertEqual(out["commits"], [])

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