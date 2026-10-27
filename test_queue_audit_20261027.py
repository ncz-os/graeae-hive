"""Queue incident regressions through production routes and SQLite transactions."""

import json
import time
from unittest import mock

from fastapi import HTTPException
from pydantic import ValidationError

from hive.bus.schemas import JobCreate, JobUpdate
from test_dependency_release_20261027 import _RepoCase, hive_jobs_router


class QueueAuditTests(_RepoCase):
    async def _submit(self, repository, **fields):
        with (
            mock.patch.object(hive_jobs_router, "hive_repo", return_value=repository),
            mock.patch.object(hive_jobs_router, "require_orchestrator_submitter", new=mock.AsyncMock()),
        ):
            return await hive_jobs_router.create_job(
                JobCreate(submitter_urn="urn:submitter", kind="fix:queue", **fields)
            )

    async def test_zero_budget_requires_reason_and_persists_opt_in(self):
        repo = await self._repo()
        await self._agent(repo)
        for fields in ({"max_retries": 0}, {"max_retries": 0, "zero_retry_reason": " "}, {"max_retries": -1}):
            with self.assertRaises(ValidationError):
                await self._submit(repo, **fields)
        created = await self._submit(repo, max_retries=0, zero_retry_reason="operator one-shot")
        async with repo.connection() as tx:
            row = await repo.job_retry_context(tx, created["id"])
        self.assertEqual(row[2], 0)
        self.assertEqual(json.loads(row[4])["zero_retry_reason"], "operator one-shot")

    async def test_submit_rejects_host_capability_intersection_and_down_engine(self):
        repo = await self._repo()
        await self._agent(repo)
        for fields in ({"eligible_hosts": ["absent"]}, {"required_capabilities": ["hard:absent"]}):
            with self.assertRaises(HTTPException) as exc:
                await self._submit(repo, **fields)
            self.assertEqual(exc.exception.status_code, 422)
        async with repo.connection() as tx:
            await tx.execute("UPDATE agents SET metadata=?", (json.dumps({"engine_ready": False}),))
            await tx.commit()
        with self.assertRaises(HTTPException):
            await self._submit(repo)

    async def test_submit_refuses_existing_output_branch(self):
        repo = await self._repo()
        await self._agent(repo)
        with (
            mock.patch.object(hive_jobs_router._ba_mod, "resolve_remote_url", return_value="ssh://origin/repo"),
            mock.patch.object(
                hive_jobs_router,
                "branch_admission",
                return_value={"ok": True, "state": "head_present", "warnings": [], "branch_created_from": None},
            ),
        ):
            with self.assertRaises(HTTPException) as exc:
                await self._submit(repo, repo="example", head_branch="published")
        self.assertIn("output_branch_exists", exc.exception.detail)

    async def test_submit_reprobes_cached_absent_output_ref(self):
        repo = await self._repo()
        await self._agent(repo)
        ba = hive_jobs_router._ba_mod
        url = "ssh://origin/repo"
        ba.clear_probe_cache()
        ba._PROBE_CACHE[(url, "refs/heads/published")] = (time.time(), "absent")
        ba._PROBE_CACHE[(url, "refs/heads/main")] = (time.time(), "present")
        published = mock.Mock(returncode=0, stdout="abc\trefs/heads/published\n")
        admission = ba.branch_admission
        try:
            with (
                mock.patch.object(ba, "resolve_remote_url", return_value=url),
                mock.patch.object(ba.subprocess, "run", return_value=published) as probe,
                mock.patch.object(
                    hive_jobs_router, "branch_admission",
                    side_effect=lambda **kw: admission(**kw, resolver=lambda _: url),
                ),
            ):
                with self.assertRaises(HTTPException) as exc:
                    await self._submit(repo, repo="example", head_branch="published", base_branch="main")
            self.assertIn("output_branch_exists", exc.exception.detail)
            self.assertEqual(probe.call_count, 1)
        finally:
            ba.clear_probe_cache()

    async def test_infra_requeues_preserves_evidence_excludes_failed_host_and_parks_at_budget(self):
        for budget, expected_count, parked in [(2, 1, False), (0, 0, True)]:
            repo = await self._repo()
            urn = "urn:agent:zeroclaw:host:deprelease"
            await self._agent(repo, urn)
            await self._mk(repo, "job", status="claimed", max_retries=budget)
            async with repo.connection() as tx:
                await tx.execute("UPDATE jobs SET claimed_by=? WHERE id='job'", (urn,))
                await tx.commit()
            with mock.patch.object(hive_jobs_router, "hive_repo", return_value=repo):
                await hive_jobs_router.update_job(
                    "job",
                    JobUpdate(
                        status="failed",
                        claimed_by=urn,
                        result={
                            "failure_reason": "no commits on top of base",
                            "error": "[zoder] daemon.sock is unreachable",
                        },
                    ),
                )
            async with repo.connection() as tx:
                row = await repo.job_retry_context(tx, "job")
                self.assertFalse(await hive_jobs_router._claim_health(tx, "job", urn))
                await self._agent_other(tx)
                self.assertEqual(
                    await hive_jobs_router._claim_health(tx, "job", "urn:agent:zeroclaw:other:1"), not parked
                )
            self.assertEqual(row[0], "queued")
            self.assertEqual(row[3], expected_count)
            result = json.loads(row[4])
            self.assertEqual(result["failure_class"], "engine_unavailable")
            self.assertEqual(result["infrastructure_parked"], parked)
            self.assertIn("daemon.sock", result["error"])
            # Drive both production claim routes after removing only backoff.
            async with repo.connection() as tx:
                await tx.execute("UPDATE jobs SET retry_backoff_until=NULL WHERE id='job'")
                await tx.commit()
            await self._agent(repo, "urn:agent:zeroclaw:other:1")
            with mock.patch.object(hive_jobs_router, "hive_repo", return_value=repo):
                with self.assertRaises(HTTPException):
                    await hive_jobs_router.claim_job("job", by=urn)
                if parked:
                    with self.assertRaises(HTTPException):
                        await hive_jobs_router.claim_job("job", by="urn:agent:zeroclaw:other:1")
                else:
                    await hive_jobs_router.claim_job("job", by="urn:agent:zeroclaw:other:1")

    async def test_daemon_lost_preflight_is_routed_as_infrastructure(self):
        repo = await self._repo()
        urn = "urn:agent:zeroclaw:host:preflight"
        await self._agent(repo, urn)
        await self._mk(repo, "preflight", status="claimed", max_retries=2)
        async with repo.connection() as tx:
            await tx.execute("UPDATE jobs SET claimed_by=? WHERE id='preflight'", (urn,))
            await tx.commit()
        with mock.patch.object(hive_jobs_router, "hive_repo", return_value=repo):
            await hive_jobs_router.update_job("preflight", JobUpdate(
                status="failed", claimed_by=urn,
                result={"failure_reason": "daemon_lost", "error": "preflight denied", "review_iterations": 0},
            ))
        async with repo.connection() as tx:
            row = await repo.job_retry_context(tx, "preflight")
        self.assertEqual(row[0], "queued")
        self.assertEqual(json.loads(row[4])["failure_class"], "engine_unavailable")
        self.assertEqual(json.loads(row[4])["review_iterations"], 0)

    async def _agent_other(self, tx):
        await tx.execute(
            "INSERT INTO agents (urn,kind,host,session_id,started_at,last_heartbeat,status) VALUES ('urn:agent:zeroclaw:other:1','zeroclaw','other','1',0,0,'online')"
        )

    async def test_host_routing_release_does_not_spend_work_retry_budget(self):
        repo = await self._repo()
        urn = "urn:agent:zeroclaw:host:deprelease"
        await self._mk(repo, "job", status="claimed", max_retries=2)
        async with repo.connection() as tx:
            await tx.execute("UPDATE jobs SET claimed_by=? WHERE id='job'", (urn,))
            await tx.commit()
        with mock.patch.object(hive_jobs_router, "hive_repo", return_value=repo):
            await hive_jobs_router.update_job(
                "job", JobUpdate(status="queued", claimed_by=urn, result={"worker_error": "host_declines_kind"})
            )
        async with repo.connection() as tx:
            row = await repo.job_retry_context(tx, "job")
        self.assertEqual(row[3], 0)

    async def test_submit_dedup_includes_artifact_and_retry_contract(self):
        repo = await self._repo()
        await self._agent(repo)
        with (
            mock.patch.object(hive_jobs_router._ba_mod, "resolve_remote_url", return_value="ssh://origin/repo"),
            mock.patch.object(
                hive_jobs_router,
                "branch_admission",
                return_value={
                    "ok": True,
                    "state": "head_creatable_from_base",
                    "warnings": [],
                    "branch_created_from": "main",
                },
            ),
        ):
            first = await self._submit(repo, repo="example", head_branch="first", base_branch="main")
            second = await self._submit(repo, repo="example", head_branch="second", base_branch="main")
            third = await self._submit(repo, repo="example", head_branch="first", base_branch="main", max_retries=3)
        self.assertEqual(len({first["id"], second["id"], third["id"]}), 3)

    async def test_work_failures_and_explicit_refusals_are_not_infrastructure_retries(self):
        for report in (
            {"error": "session-cancel acknowledged; task aborted by user"},
            {"error": "daemon.sock connected successfully; tests failed"},
            {"error": "provider_idle_timeout", "retryable": False},
            {"error": "daemon.sock is unreachable", "failure_reason": "protection_violation"},
        ):
            repo = await self._repo()
            await self._mk(repo, "job", status="claimed", max_retries=2)
            with mock.patch.object(hive_jobs_router, "hive_repo", return_value=repo):
                await hive_jobs_router.update_job(
                    "job", JobUpdate(status="failed", claimed_by="urn:worker", result=report)
                )
            async with repo.connection() as tx:
                row = await repo.job_retry_context(tx, "job")
            self.assertEqual(row[0], "failed", report)
            self.assertNotIn("failure_class", json.loads(row[4]))
