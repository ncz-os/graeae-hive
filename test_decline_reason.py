"""Tests for persisting the release/decline reason on job rows
(graeae:dead-letter-explains-itself 2026-10-27).

Production incident fixed here: a job submitted with base_branch=master against
a repo whose default branch is `main` is claimed, probed, and released with
reason `invalid_base_branch`; the bus counted it as a job-level decline and
dead-lettered it. The row then showed status=dead-letter, claimed_by=null,
retry_count=3 and NO reason anywhere in GET /v1/jobs — the operator had to read
worker source to find out what happened.

The fix:
  * persist last_decline_reason / last_decline_at / last_declined_by on EVERY
    release (any worker_error string, not just the counted decline prefixes);
  * the dead-letter transition carries the reason (belt-and-braces re-set);
  * GET /v1/jobs and GET /v1/jobs/{id} return the three new fields;
  * the migration is additive (ALTER TABLE ADD COLUMN) so older rows without
    the columns still deserialise (fields are None).
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("HIVE_BACKEND", "sqlite")

from starlette.testclient import TestClient  # noqa: E402

from hive.bus.app import app  # noqa: E402
from hive.bus.deps import hive_repo  # noqa: E402

WORKER = "urn:agent:zeroclaw:cerberus:aaaa"
ORCH = "urn:agent:claude-code:pythia:bbbb"

# (id, submitter, kind, desc, priority, status, claimed_by, claimed_at, started_at)
_SEED_COLS = (
    "id, submitter_urn, kind, description, priority, "
    "status, claimed_by, claimed_at, started_at, retry_count"
)


def _fresh_repo_path() -> str:
    return str(Path(tempfile.mkdtemp(prefix="hive-dl-")) / "t.db")


def _new_repo() -> None:
    """Point the process-wide repo cache at a fresh, initialised DB."""
    repo = hive_repo()
    repo.db_path = _fresh_repo_path()
    asyncio.run(repo.init())


async def _seed_claimed(job_id: str, claimed_by: str, status: str = "claimed",
                        kind: str = "build:zoder", retry_count: int = 0,
                        decline_count: int = 0):
    repo = hive_repo()
    async with repo.connection() as db:
        await db.execute(
            "INSERT INTO jobs (id, submitter_urn, kind, description, priority, "
            "status, claimed_by, claimed_at, started_at, retry_count, "
            "max_retries, decline_count, retry_backoff_until) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (job_id, ORCH, kind, "d", 0, status, claimed_by, 100.0, 100.0,
             retry_count, 2, decline_count, 0),
        )
        await db.commit()


async def _row(job_id: str):
    repo = hive_repo()
    async with repo.connection() as db:
        async with db.execute(
            "SELECT status, last_decline_reason, last_decline_at, last_declined_by, "
            "claimed_by, retry_count, decline_count FROM jobs WHERE id=?",
            (job_id,),
        ) as cur:
            return await cur.fetchone()


class DeclineReasonPersistenceTests(unittest.IsolatedAsyncioTestCase):
    """Release + dead-letter persistence, through the bus API (sqlite)."""

    def setUp(self):
        _new_repo()
        self.client = TestClient(app)

    def tearDown(self):
        hive_repo().db_path = _fresh_repo_path()

    # ── a release with invalid_base_branch records the reason ──
    def test_release_records_reason_and_get_returns_it(self):
        asyncio.run(_seed_claimed("job-dl-1", WORKER))
        jid = "job-dl-1"
        # worker releases with the invalid_base_branch reason
        r = self.client.patch(
            f"/v1/jobs/{jid}",
            json={
                "status": "queued",
                "claimed_by": WORKER,
                "result": {"worker_error": "invalid_base_branch", "via": "hive_worker"},
            },
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertFalse(r.json().get("dead_lettered"))

        # GET /v1/jobs surfaces the reason
        jobs = {j["id"]: j for j in self.client.get("/v1/jobs", params={"limit": 10}).json()["jobs"]}
        j = jobs[jid]
        self.assertEqual(j["last_decline_reason"], "invalid_base_branch")
        self.assertIsNotNone(j["last_decline_at"])
        self.assertEqual(j["last_declined_by"], WORKER)

        # GET /v1/jobs/{id} surfaces it too
        r3 = self.client.get(f"/v1/jobs/{jid}")
        self.assertEqual(r3.json()["last_decline_reason"], "invalid_base_branch")
        self.assertEqual(r3.json()["last_declined_by"], WORKER)

        # the row itself (not just the API view) carries it
        row = asyncio.run(_row(jid))
        self.assertEqual(row[0], "queued")
        self.assertEqual(row[1], "invalid_base_branch")
        self.assertEqual(row[3], WORKER)

    # ── a counted decline reaching the threshold dead-letters WITH the reason ──
    def test_reason_survives_dead_letter(self):
        # seed with decline_count=2 + retry_count=2: the NEXT decline
        # (3rd) reaches MAX_DECLINE_REQUEUES and dead-letters the job in ONE
        # release — the production shape (retry_count=3, dead-letter).
        asyncio.run(_seed_claimed("job-dl-2", WORKER, retry_count=2, decline_count=2))
        jid = "job-dl-2"
        last = self.client.patch(
            f"/v1/jobs/{jid}",
            json={
                "status": "queued",
                "claimed_by": WORKER,
                "result": {"worker_error": "invalid_base_branch:master"},
            },
        )
        self.assertEqual(last.status_code, 200, last.text)
        self.assertTrue(last.json().get("dead_lettered"), last.text)
        # the dead-letter row carries the reason
        j = self.client.get(f"/v1/jobs/{jid}").json()
        self.assertEqual(j["status"], "dead-letter")
        self.assertEqual(j["last_decline_reason"], "invalid_base_branch:master")
        self.assertIsNotNone(j["last_decline_at"])
        self.assertEqual(j["last_declined_by"], WORKER)
        row = asyncio.run(_row(jid))
        self.assertEqual(row[0], "dead-letter")
        self.assertEqual(row[1], "invalid_base_branch:master")
        self.assertEqual(row[5], 3)  # retry_count=3, as observed in production

    # ── a non-counted release reason still lands on the row ──
    #    (any worker_error is recorded; only DECLINE_REASON_PREFIXES count)
    def test_noncounted_release_reason_still_recorded(self):
        asyncio.run(_seed_claimed("job-dl-4", WORKER))
        jid = "job-dl-4"
        r = self.client.patch(
            f"/v1/jobs/{jid}",
            json={
                "status": "queued",
                "claimed_by": WORKER,
                "result": {"worker_error": "host_declines_kind:clawpi"},
            },
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.assertFalse(r.json().get("dead_lettered"))
        row = asyncio.run(_row(jid))
        self.assertEqual(row[0], "queued")
        self.assertEqual(row[1], "host_declines_kind:clawpi")
        self.assertEqual(row[3], WORKER)
        self.assertEqual(row[6], 0)  # decline_count untouched (skip release)

    # ── a release without worker_error still stamps time + actor ──
    def test_plain_release_records_actor_and_time(self):
        asyncio.run(_seed_claimed("job-dl-3", WORKER))
        jid = "job-dl-3"
        r = self.client.patch(
            f"/v1/jobs/{jid}",
            json={"status": "queued", "claimed_by": WORKER},
        )
        self.assertEqual(r.status_code, 200, r.text)
        row = asyncio.run(_row(jid))
        self.assertEqual(row[0], "queued")
        self.assertIsNone(row[1])  # no reason string on a plain release
        self.assertIsNotNone(row[2])  # but the time is recorded
        self.assertEqual(row[3], WORKER)

    # ── an older row without the new columns still deserialises ──
    def test_older_row_without_new_columns_deserialises(self):
        jid = "job-old"
        repo = hive_repo()

        async def seed_old():
            async with repo.connection() as db:
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, description, priority, "
                    "status, started_at, max_retries, retry_count) "
                    "VALUES (?,?,?,?,?, ?,?,2,0)",
                    (jid, ORCH, "build:zoder", "d", 0, "queued", 100.0),
                )
                await db.commit()

        asyncio.run(seed_old())
        j = self.client.get(f"/v1/jobs/{jid}").json()
        self.assertEqual(j["last_decline_reason"], None)
        self.assertEqual(j["last_decline_at"], None)
        self.assertEqual(j["last_declined_by"], None)
        jobs = self.client.get("/v1/jobs", params={"limit": 10}).json()["jobs"]
        self.assertTrue(any(x["id"] == jid and x["last_decline_reason"] is None for x in jobs))


class MigrationAdditiveTests(unittest.IsolatedAsyncioTestCase):
    """The live-DB migration adds the three columns without a table rebuild."""

    async def asyncSetUp(self):
        import aiosqlite

        self._dir = tempfile.mkdtemp(prefix="hive-dl-mig-")
        self.db_path = os.path.join(self._dir, "old.db")
        # create a PRE-migration jobs table (no last_decline_* columns) with the
        # full pre-2026-10-27 column set so repo.init() only has to add the three
        # new ones (the real live-DB shape before the migration).
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "CREATE TABLE jobs ("
                "id TEXT PRIMARY KEY, submitter_urn TEXT NOT NULL, parent_job_id TEXT, "
                "kind TEXT NOT NULL, description TEXT, priority INTEGER NOT NULL DEFAULT 0, "
                "deadline REAL, required_capabilities TEXT, eligible_kinds TEXT, "
                "eligible_hosts TEXT, site TEXT, repo TEXT, head_branch TEXT, "
                "pr_number INTEGER, project TEXT, "
                "status TEXT NOT NULL CHECK(status IN ('queued','done','failed')), "
                "claimed_by TEXT, claimed_at REAL, started_at REAL NOT NULL, "
                "ended_at REAL, result TEXT, required_autonomy TEXT, "
                "max_cost_tier TEXT, preferred_providers TEXT, preferred_models TEXT, "
                "claimed_runtime TEXT, claimed_model TEXT, claimed_provider TEXT, "
                "claimed_cost_tier TEXT, tokens_in INTEGER, tokens_out INTEGER, "
                "estimated_cost_usd REAL, mnemos_refs TEXT, result_mnemos_id TEXT, "
                "required_resources TEXT, claimed_host_caps TEXT, tags TEXT, "
                "depends_on TEXT, retry_count INTEGER NOT NULL DEFAULT 0, "
                "max_retries INTEGER NOT NULL DEFAULT 2, retry_backoff_until REAL, "
                "last_update_at REAL, claim_lease_expires_at REAL, "
                "decline_count INTEGER NOT NULL DEFAULT 0)"
            )
            await db.execute(
                "INSERT INTO jobs (id, submitter_urn, kind, description, priority, "
                "status, started_at) VALUES ('old-row','s','k','d',0,'queued',1.0)"
            )
            await db.commit()

    async def test_migration_adds_columns(self):
        from hive.persistence.sqlite import SqliteHiveRepository

        repo = SqliteHiveRepository(self.db_path)
        await repo.init()
        async with repo.connection() as db:
            async with db.execute("PRAGMA table_info(jobs)") as cur:
                cols = {r[1] for r in await cur.fetchall()}
        self.assertIn("last_decline_reason", cols)
        self.assertIn("last_decline_at", cols)
        self.assertIn("last_declined_by", cols)
        # the old row is still there, with NULLs on the new columns
        async with repo.connection() as db:
            async with db.execute(
                "SELECT status, last_decline_reason, last_decline_at, last_declined_by "
                "FROM jobs WHERE id='old-row'"
            ) as cur:
                row = await cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "queued")
        self.assertIsNone(row[1])
        self.assertIsNone(row[2])
        self.assertIsNone(row[3])


if __name__ == "__main__":
    unittest.main()
