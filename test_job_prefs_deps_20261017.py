"""Fault 2026-10-17: preferred_models / preferred_providers / depends_on.

POST /v1/jobs accepted ``preferred_models`` and ``depends_on`` (JobCreate) but
GET /v1/jobs and GET /v1/jobs/{id} never returned them (no JobOut fields), and
hive_worker.py's escalation_agent never read ``preferred_models`` — so model
preferences were silently ignored and dependency ordering could not be
verified from the API. These tests pin the whole chain:

  * persistence round-trip on the sqlite backend (insert -> SELECT -> response)
  * the response model declares the fields (response fields / serializers)
    and previously-written NULL rows still serialize
  * the claim path never hands out a job whose depends_on jobs are not done
    (both the batch dequeue and the targeted claim, plus the in-transaction
    re-check behind them)

The worker half (preferred_models -> starting rung) lives with hive_worker.py
on the deploy ref and is tested there.
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
os.environ.setdefault("AGENT_BUS_DB", str(Path(tempfile.gettempdir()) / "hive-prefs-deps-tests.db"))

import agent_bus
from hive.bus.routers import jobs as hive_jobs_router
from hive.bus.schemas import JobCreate, JobOut
from hive.persistence.sqlite import DependencyNotReady, SqliteHiveRepository

HERE = Path(__file__).resolve().parent
class JobOutResponseModelTests(unittest.TestCase):
    def test_response_model_declares_preference_and_dependency_fields(self):
        fields = JobOut.model_fields
        for name in ("preferred_providers", "preferred_models", "depends_on"):
            self.assertIn(name, fields)
            self.assertIsNone(fields[name].default)

    def test_response_model_serializes_values_and_nulls(self):
        out = JobOut(
            id="j1",
            submitter_urn="urn:submitter",
            kind="research",
            status="queued",
            preferred_models=["qwen38", "MiniMax-M3"],
            preferred_providers=["local"],
            depends_on=["a", "b"],
        )
        dumped = out.model_dump()
        self.assertEqual(dumped["preferred_models"], ["qwen38", "MiniMax-M3"])
        self.assertEqual(dumped["preferred_providers"], ["local"])
        self.assertEqual(dumped["depends_on"], ["a", "b"])
        # A legacy row (NULL columns) must still validate.
        legacy = JobOut(id="j2", submitter_urn="urn:submitter", kind="build", status="queued")
        self.assertIsNone(legacy.model_dump()["preferred_models"])

    def test_job_create_still_accepts_the_fields(self):
        req = JobCreate(
            submitter_urn="urn:submitter",
            kind="build",
            preferred_models=["deepseek-flash"],
            depends_on=["dep"],
        )
        self.assertEqual(req.preferred_models, ["deepseek-flash"])
        self.assertEqual(req.depends_on, ["dep"])


class _RepoCase(unittest.IsolatedAsyncioTestCase):
    async def _repo(self, name="hive.db"):
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        repo = SqliteHiveRepository(str(Path(td.name) / name))
        await repo.init(agent_bus.SCHEMA)
        return repo

    async def _insert(
        self,
        repo,
        job_id,
        *,
        status="queued",
        deps=None,
        prefs=None,
        providers=None,
        priority=0,
        kind="build",
        eligible_kinds=None,
    ):
        now = time.time()
        async with repo.connection() as db:
            await db.execute(
                "INSERT INTO jobs (id, submitter_urn, kind, priority, status, started_at, "
                "preferred_models, preferred_providers, depends_on, eligible_kinds) "
                "VALUES (?, 'urn:submitter', ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    job_id,
                    kind,
                    priority,
                    status,
                    now,
                    json.dumps(prefs) if prefs else None,
                    json.dumps(providers) if providers else None,
                    json.dumps(deps) if deps else None,
                    json.dumps(eligible_kinds) if eligible_kinds else None,
                ),
            )
            await db.commit()


class PreferencePersistenceTests(_RepoCase):
    async def test_submit_job_persists_and_list_job_returns_preferences(self):
        repo = await self._repo()
        async with repo.connection() as tx:
            await repo.submit_job(
                tx,
                id="j-prefs",
                submitter_urn="urn:submitter",
                kind="build",
                started_at=1.0,
                preferred_providers_json=json.dumps(["local", "deepseek"]),
                preferred_models_json=json.dumps(["qwen38", "MiniMax-M3"]),
                depends_on_json=json.dumps(["dep-1"]),
            )
            await tx.commit()

        async with repo.connection() as tx:
            total, rows = await repo.list_jobs(tx, limit=10)
        self.assertEqual(total, 1)
        row = rows[0]
        self.assertEqual(row["id"], "j-prefs")
        self.assertEqual(row["preferred_providers"], ["local", "deepseek"])
        self.assertEqual(row["preferred_models"], ["qwen38", "MiniMax-M3"])
        self.assertEqual(row["depends_on"], ["dep-1"])
        # The response model accepts the serialized row unchanged.
        out = JobOut(**row)
        self.assertEqual(out.preferred_models, ["qwen38", "MiniMax-M3"])

    async def test_get_job_returns_preferences(self):
        repo = await self._repo()
        await self._insert(repo, "j-get", prefs=["deepseek-flash"], providers=["deepseek"], deps=["d"])
        with mock.patch("hive.bus.deps.hive_repo", lambda: repo):
            payload = await hive_jobs_router.get_job("j-get")
        self.assertEqual(payload["preferred_models"], ["deepseek-flash"])
        self.assertEqual(payload["preferred_providers"], ["deepseek"])
        self.assertEqual(payload["depends_on"], ["d"])
        JobOut(**payload)

    async def test_list_jobs_endpoint_returns_preferences(self):
        repo = await self._repo()
        await self._insert(repo, "j-list", deps=["dep-1"])
        with mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo):
            payload = await hive_jobs_router.list_jobs(limit=10)
        self.assertEqual(payload["count"], 1)
        job = payload["jobs"][0]
        self.assertEqual(job["depends_on"], ["dep-1"])
        self.assertIsNone(job["preferred_models"])
        JobOut(**job)

    async def test_list_jobs_status_filter_returns_only_matching_rows(self):
        # Regression: the status predicate was appended after ORDER BY/LIMIT,
        # so `GET /v1/jobs?status=...` raised a sqlite syntax error.
        repo = await self._repo()
        await self._insert(repo, "j-queued", status="queued")
        await self._insert(repo, "j-done", status="done")
        with mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo):
            payload = await hive_jobs_router.list_jobs(status="done", limit=10)
        self.assertEqual(payload["total"], 1)
        self.assertEqual([j["id"] for j in payload["jobs"]], ["j-done"])

    async def test_oracle_migration_creates_preference_columns(self):
        from hive.persistence.oracle import _ORACLE_JOB_ADDITIONS, _ORACLE_REQUIRED_JOB_COLUMNS

        for column in ("PREFERRED_MODELS", "PREFERRED_PROVIDERS", "DEPENDS_ON"):
            self.assertIn(column, _ORACLE_REQUIRED_JOB_COLUMNS)
            self.assertIn(column, _ORACLE_JOB_ADDITIONS)


def _stub_claim(blocked_ids):
    async def _claim(tx, **kw):
        return 0 if kw["job_id"] in blocked_ids else 1

    return _claim


class DependencyGatingTests(_RepoCase):
    async def _agent(self, repo, urn="urn:agent:zeroclaw:host:session"):
        now = time.time()
        async with repo.connection() as db:
            await db.execute(
                "INSERT INTO agents (urn, kind, host, session_id, capabilities, started_at, "
                "last_heartbeat, status, runtime, cost_tier) "
                "VALUES (?, 'zeroclaw', 'host', 'session', '[]', ?, ?, 'online', 'zeroclaw', 'B')",
                (urn, now, now),
            )
            await db.commit()
        return urn

    async def test_claim_job_refuses_when_dependency_not_done(self):
        repo = await self._repo()
        urn = await self._agent(repo)
        await self._insert(repo, "dep", status="queued")
        await self._insert(repo, "blocked", deps=["dep"], prefs=["qwen38"])
        now = time.time()
        async with repo.connection() as tx:
            with self.assertRaises(DependencyNotReady):
                await repo.claim_job(
                    tx,
                    job_id="blocked",
                    agent_urn=urn,
                    now=now,
                    lease_expires_at=now + 60,
                    runtime="zeroclaw",
                    model="qwen38",
                    provider="local",
                    cost_tier="B",
                )
        # Still queued: nothing was handed out.
        async with repo.connection() as tx:
            async with tx.execute("SELECT status FROM jobs WHERE id='blocked'") as cur:
                self.assertEqual((await cur.fetchone())[0], "queued")
            self.assertFalse(await repo.dependencies_satisfied(tx, "blocked"))

    async def test_claim_job_allows_dependency_once_done(self):
        repo = await self._repo()
        urn = await self._agent(repo)
        await self._insert(repo, "dep", status="done")
        await self._insert(repo, "ready", deps=["dep"])
        now = time.time()
        async with repo.connection() as tx:
            self.assertTrue(await repo.dependencies_satisfied(tx, "ready"))
            rc = await repo.claim_job(
                tx,
                job_id="ready",
                agent_urn=urn,
                now=now,
                lease_expires_at=now + 60,
                runtime="zeroclaw",
                model="qwen38",
                provider="local",
                cost_tier="B",
            )
            await tx.commit()
        self.assertEqual(rc, 1)

    async def test_repeated_dependency_id_does_not_block_forever(self):
        repo = await self._repo()
        urn = await self._agent(repo)
        await self._insert(repo, "dep", status="done")
        await self._insert(repo, "dup", deps=["dep", "dep"])
        now = time.time()
        async with repo.connection() as tx:
            self.assertTrue(await repo.dependencies_satisfied(tx, "dup"))
            rc = await repo.claim_job(
                tx,
                job_id="dup",
                agent_urn=urn,
                now=now,
                lease_expires_at=now + 60,
                runtime="zeroclaw",
                model="qwen38",
                provider="local",
                cost_tier="B",
            )
            await tx.commit()
        self.assertEqual(rc, 1)

    async def test_malformed_dependencies_fail_closed(self):
        repo = await self._repo()
        rows = (("bad-json", "{not json"), ("not-list", '{"a": 1}'))
        for job_id, _raw in rows:
            await self._insert(repo, job_id)
        async with repo.connection() as db:
            for job_id, raw in rows:
                await db.execute("UPDATE jobs SET depends_on=? WHERE id=?", (raw, job_id))
            await db.commit()
        async with repo.connection() as tx:
            self.assertFalse(await repo.dependencies_satisfied(tx, "bad-json"))
            self.assertFalse(await repo.dependencies_satisfied(tx, "not-list"))

    async def test_dependencies_satisfied_treats_null_and_empty_as_open(self):
        repo = await self._repo()
        await self._insert(repo, "no-deps")
        async with repo.connection() as tx:
            self.assertTrue(await repo.dependencies_satisfied(tx, "no-deps"))
            self.assertTrue(await repo.dependencies_satisfied(tx, "missing-row"))

    async def test_dependency_gate_applies_on_the_fallback_claim_path(self):
        repo = await self._repo()
        urn = await self._agent(repo)
        await self._insert(repo, "dep", status="queued", kind="dependency", eligible_kinds=["other"])
        await self._insert(repo, "blocked", deps=["dep"])
        now = time.time()
        async with repo.connection() as tx:
            claimed = await repo.claim_next_job(
                tx,
                agent_urn=urn,
                now=now,
                active_statuses={"online", "idle"},
                claim_lease_seconds=60,
                host_denylist=set(),
                match_capabilities=lambda labels, caps: type("M", (), {"eligible": True})(),
                kind_aliases=lambda kind, runtime: {kind},
                json_list=lambda value: json.loads(value) if value else [],
                cost_tiers=["A", "B", "C"],
                throttle_headroom=0.9,
            )
        self.assertIsNone(claimed)

    async def test_batch_dequeue_skips_blocked_job(self):
        repo = await self._repo()
        urn = await self._agent(repo)
        # PRIORITY order = scan order: "blocked" is highest priority and
        # "ready" is lowest, so if the gate were missing the claim would return
        # "blocked". The dependency itself is a kind with no eligible claimer,
        # and the path that can reach it (queued_candidates -> claim_job) is
        # stubbed out so it can never be claimed in passing — the SELECT gate
        # under test is the only thing between "blocked" and the agent.
        await self._insert(repo, "dep", status="queued", kind="dependency", eligible_kinds=["other"])
        await self._insert(repo, "blocked", deps=["dep"], priority=10)
        await self._insert(repo, "ready", priority=-1)
        blocked_ids = {"dep", "blocked"}
        with (
            mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo),
            mock.patch.object(hive_jobs_router, "emit_event", mock.AsyncMock()),
            # Keep the gate as the only filter: eligibility/cost-tier policy is
            # covered elsewhere and would otherwise mask it.
            mock.patch.object(hive_jobs_router, "job_agent_eligible", lambda job, agent: (True, "")),
            mock.patch.object(repo, "claim_job", side_effect=_stub_claim(blocked_ids)),
        ):
            claimed = await hive_jobs_router.dequeue_next_job(urn)
        self.assertEqual(claimed["id"], "ready")

    async def test_fallback_claim_payload_carries_prefs_and_attempt(self):
        repo = await self._repo()
        urn = await self._agent(repo)
        # No preference filters: that legacy path filters models by the AGENT's
        # registered model, which is its own (unrelated) concern.
        await self._insert(repo, "pref-job", priority=5)
        now = time.time()
        async with repo.connection() as tx:
            claimed = await repo.claim_next_job(
                tx,
                agent_urn=urn,
                now=now,
                active_statuses={"online", "idle"},
                claim_lease_seconds=60,
                host_denylist=set(),
                match_capabilities=lambda labels, caps: type("M", (), {"eligible": True})(),
                kind_aliases=lambda kind, runtime: {kind},
                json_list=lambda value: json.loads(value) if value else [],
                cost_tiers=["A", "B", "C"],
                throttle_headroom=0.9,
            )
        self.assertEqual(claimed["id"], "pref-job")
        self.assertEqual(claimed["preferred_models"], [])
        self.assertEqual(claimed["preferred_providers"], [])
        self.assertEqual(claimed["depends_on"], [])
        self.assertEqual(claimed["retry_count"], 0)

    async def test_targeted_claim_returns_409_for_unmet_dependency(self):
        repo = await self._repo()
        urn = await self._agent(repo)
        await self._insert(repo, "dep", status="queued")
        await self._insert(repo, "blocked", deps=["dep"])
        from fastapi import HTTPException

        with (
            mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo),
            mock.patch.object(hive_jobs_router, "emit_event", mock.AsyncMock()),
        ):
            with self.assertRaises(HTTPException) as ctx:
                await hive_jobs_router.claim_job("blocked", urn)
        self.assertEqual(ctx.exception.status_code, 409)


if __name__ == "__main__":
    unittest.main()
