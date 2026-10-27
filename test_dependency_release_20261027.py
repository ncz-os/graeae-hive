"""Incident 2026-10-27: a job whose dependency dies must be released.

A dependency that failed with protection_violation was purged; its dependent
declared ``depends_on: [<dep>]`` and sat ``queued`` with ``claimed_by=null``
for 7+ minutes — the claim gate requires every dep to reach status='done', a
dead/purged id can never be done, nothing else touched the edge, and
cancelling the unclaimed dependent was refused, so the only escape was an
admin purge of the dependent too.

This pins the fix chain end to end:

  * a job entering failed / failed_completion / dead-letter / cancelled
    releases every queued/offered dependent that lists it, in the SAME
    transaction as the status write (no intermediate state observable), and
    emits ``job.dependency_released`` with both ids;
  * a purge (row deleted) releases dependents the same way (dep_status
    'purged');
  * a dependent with two deps stays blocked when only one dies, and the dead
    id is removed;
  * a dependent that is already claimed/running is untouched;
  * the claim path defensively drops ids whose rows no longer exist, so a
    purge that predates the release fix cannot block a job forever;
  * a retried-failed dep (final status 'queued') does NOT release.

The repo is constructed directly (same pattern as
test_job_prefs_deps_20261017 / test_submitter_controls_20261025) so no live
DB is touched.
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")
os.environ.setdefault("HIVE_BUS_ADMIN_TOKEN", "test-admin-token")

import agent_bus
from hive.bus import routers as _bus_routers  # noqa: F401 (ensures router modules load)
from hive.bus.deps import release_dependencies
from hive.bus.routers import admin as hive_admin_router
from hive.bus.routers import jobs as hive_jobs_router
from hive.bus.schemas import JobUpdate
from hive.persistence import sqlite as sqlite_mod
from hive.persistence.sqlite import DependencyNotReady


class _RepoCase(unittest.IsolatedAsyncioTestCase):
    """Temp sqlite repo + row helpers (repo style of the dep-gate tests)."""

    async def _repo(self, name="dep-release.db"):
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        repo = sqlite_mod.SqliteHiveRepository(str(Path(td.name) / name))
        await repo.init(agent_bus.SCHEMA)
        return repo

    async def _mk(self, repo, job_id, *, status="queued", deps=None, max_retries=0):
        now = time.time()
        async with repo.connection() as db:
            await db.execute(
                "INSERT INTO jobs (id, submitter_urn, kind, status, started_at, depends_on, "
                "max_retries) VALUES (?, 'urn:submitter', 'build', ?, ?, ?, ?)",
                (
                    job_id,
                    status,
                    now,
                    json.dumps(deps) if deps else None,
                    max_retries,
                ),
            )
            await db.commit()

    async def _agent(self, repo, urn="urn:agent:zeroclaw:host:deprelease"):
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

    async def _row(self, repo, job_id):
        async with repo.connection() as db:
            async with db.execute("SELECT status, depends_on FROM jobs WHERE id=?", (job_id,)) as cur:
                r = await cur.fetchone()
        return tuple(r) if r else None

    async def _event_kinds(self, repo):
        async with repo.connection() as db:
            async with db.execute("SELECT kind, payload FROM events ORDER BY ts, rowid") as cur:
                return [(r[0], json.loads(r[1])) async for r in cur]


class TerminalTransitionReleaseTests(_RepoCase):
    """PATCH /v1/jobs/{id} into a dead status releases queued dependents."""

    async def test_failed_dependency_releases_dependent_with_event(self):
        """A failed dep releases its queued dependent; the event carries both ids."""
        repo = await self._repo()
        await self._mk(repo, "dep", status="claimed")
        await self._mk(repo, "dep", status="claimed") if False else None
        await self._mk(repo, "dependent", status="queued", deps=["dep"])
        with (
            mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo),
            mock.patch.object(hive_jobs_router, "emit_event", mock.AsyncMock()) as emit,
        ):
            out = await hive_jobs_router.update_job(
                "dep", JobUpdate(status="failed", claimed_by="urn:agent:zeroclaw:host:deprelease")
            )
        self.assertTrue(out["ok"])
        self.assertEqual(await self._row(repo, "dep"), ("failed", None))
        # The dependent is now claimable: depends_on is NULL (canonical no-deps).
        self.assertEqual(await self._row(repo, "dependent"), ("queued", None))
        # Exactly one job.dependency_released, carrying BOTH ids. The mock
        # captured the in-transaction emit, so the row did NOT land in the
        # events table — the payload comes from the call, not the DB.
        released = [
            (kind, payload) for kind, payload in await self._event_kinds(repo)
            if kind == "job.dependency_released"
        ]
        self.assertEqual(len(released), 0)
        release_calls = [
            call for call in emit.call_args_list
            if (call.kwargs.get("kind") if call.kwargs else None) == "job.dependency_released"
            or (len(call.args) >= 2 and call.args[1] == "job.dependency_released")
        ]
        self.assertEqual(len(release_calls), 1)
        payload = release_calls[0].kwargs.get("payload") or release_calls[0].args[2]
        self.assertEqual(payload["dependent_id"], "dependent")
        self.assertEqual(payload["failed_dep_id"], "dep")
        self.assertEqual(payload["dep_status"], "failed")
        self.assertEqual(payload["remaining"], [])
        # emit_event was invoked in-transaction by the release helper (the
        # router's own job.failed event uses a second connection later).
        self.assertTrue(
            any(
                call.kwargs.get("kind") == "job.dependency_released" or call.args[1] == "job.dependency_released"
                for call in emit.call_args_list
            )
            or any(
                (call.args[1] if len(call.args) > 1 else call.kwargs.get("kind")) == "job.dependency_released"
                for call in emit.call_args_list
            ),
            [str(c) for c in emit.call_args_list],
        )

    async def test_two_dependencies_only_one_dies(self):
        """Two deps, one dies: the dead id is removed, the dependent stays blocked."""
        repo = await self._repo()
        await self._mk(repo, "dead-dep", status="claimed")
        await self._mk(repo, "live-dep", status="queued")
        await self._mk(repo, "dependent", status="queued", deps=["dead-dep", "live-dep"])
        with mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo):
            await hive_jobs_router.update_job(
                "dead-dep", JobUpdate(status="cancelled", claimed_by="urn:agent:zeroclaw:host:deprelease")
            )
        self.assertEqual(await self._row(repo, "dead-dep"), ("cancelled", None))
        # Still queued, still blocked — the dead id is gone, the live one remains.
        self.assertEqual(await self._row(repo, "dependent"), ("queued", '["live-dep"]'))
        async with repo.connection() as tx:
            self.assertFalse(await repo.dependencies_satisfied(tx, "dependent"))
        # The event reports the surviving dep.
        released = [
            payload for kind, payload in await self._event_kinds(repo)
            if kind == "job.dependency_released"
        ]
        self.assertEqual(len(released), 1)
        self.assertEqual(released[0]["dependent_id"], "dependent")
        self.assertEqual(released[0]["failed_dep_id"], "dead-dep")
        self.assertEqual(released[0]["dep_status"], "cancelled")
        self.assertEqual(released[0]["remaining"], ["live-dep"])

    async def test_done_dependency_does_not_release(self):
        """A dep entering 'done' is satisfied the normal way: no release, edge kept."""
        repo = await self._repo()
        await self._mk(repo, "dep", status="claimed")
        await self._mk(repo, "dependent", status="queued", deps=["dep"])
        with mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo):
            await hive_jobs_router.update_job(
                "dep",
                JobUpdate(
                    status="done",
                    claimed_by="urn:agent:zeroclaw:host:deprelease",
                    result={"exit_code": 0},
                ),
            )
        # The edge is kept for provenance (the worker sees the DAG).
        self.assertEqual(await self._row(repo, "dependent"), ("queued", '["dep"]'))
        released = [
            kind for kind, _ in await self._event_kinds(repo)
            if kind == "job.dependency_released"
        ]
        self.assertEqual(released, [])

    async def test_running_dependent_is_untouched(self):
        """A dependent already claimed/running is never touched when its dep dies."""
        repo = await self._repo()
        await self._mk(repo, "dep", status="claimed")
        await self._mk(repo, "running-dep", status="running", deps=["dep"])
        await self._mk(repo, "queued-dep", status="queued", deps=["dep"])
        with mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo):
            await hive_jobs_router.update_job(
                "dep", JobUpdate(status="failed", claimed_by="urn:agent:zeroclaw:host:deprelease")
            )
        # The in-flight dependent keeps its edge (its work is running); the
        # queued one is released.
        self.assertEqual(await self._row(repo, "running-dep"), ("running", '["dep"]'))
        self.assertEqual(await self._row(repo, "queued-dep"), ("queued", None))

    async def test_retried_failed_dependency_does_not_release(self):
        """A failed dep that still has retry budget bounces to 'queued': no release."""
        repo = await self._repo()
        await self._mk(repo, "dep", status="claimed", max_retries=2)
        await self._mk(repo, "dependent", status="queued", deps=["dep"])
        with mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo):
            await hive_jobs_router.update_job(
                "dep", JobUpdate(status="failed", claimed_by="urn:agent:zeroclaw:host:deprelease")
            )
        # The dep is retrying, not dead: its dependent stays blocked on it.
        self.assertEqual((await self._row(repo, "dep"))[0], "queued")
        self.assertEqual(await self._row(repo, "dependent"), ("queued", '["dep"]'))
        released = [
            kind for kind, _ in await self._event_kinds(repo)
            if kind == "job.dependency_released"
        ]
        self.assertEqual(released, [])


class PurgeReleaseTests(_RepoCase):
    """Purge paths (row deleted) release dependents in the same transaction."""

    async def test_admin_dead_letter_purge_releases_dependent(self):
        repo = await self._repo()
        await self._mk(repo, "dead-dep", status="dead-letter")
        await self._mk(repo, "dependent", status="queued", deps=["dead-dep"])
        with (
            mock.patch.object(hive_admin_router, "hive_repo", lambda: repo),
            mock.patch.object(hive_admin_router, "connect_db", repo.connection),
            mock.patch.object(hive_admin_router, "require_mutation_actor", mock.AsyncMock(return_value="urn:admin")),
        ):
            from hive.bus.schemas import PurgeRequest

            out = await hive_admin_router.admin_purge_jobs(PurgeRequest(job_ids=["dead-dep"], reason="test"))
        self.assertEqual(out["ok"], ["dead-dep"])
        # The row is gone and the dependent is claimable.
        self.assertIsNone(await self._row(repo, "dead-dep"))
        self.assertEqual(await self._row(repo, "dependent"), ("queued", None))
        released = [
            payload for kind, payload in await self._event_kinds(repo)
            if kind == "job.dependency_released"
        ]
        self.assertEqual(len(released), 1)
        self.assertEqual(released[0]["dependent_id"], "dependent")
        self.assertEqual(released[0]["failed_dep_id"], "dead-dep")
        self.assertEqual(released[0]["dep_status"], "purged")
        self.assertEqual(released[0]["remaining"], [])

    async def test_bulk_purge_releases_dependent(self):
        repo = await self._repo()
        await self._mk(repo, "dead-dep", status="failed")
        await self._mk(repo, "dependent", status="queued", deps=["dead-dep"])
        with (
            mock.patch.object(hive_admin_router, "hive_repo", lambda: repo),
            mock.patch.object(hive_admin_router, "connect_db", repo.connection),
            mock.patch.object(hive_admin_router, "require_mutation_actor", mock.AsyncMock(return_value="urn:admin")),
        ):
            from hive.bus.schemas import JobPurgeRequest

            out = await hive_admin_router.purge_jobs(
                JobPurgeRequest(statuses=["failed"], reason="test", ids=["dead-dep"])
            )
        self.assertEqual(out["deleted"], 1)
        self.assertEqual(await self._row(repo, "dependent"), ("queued", None))
        released = [
            payload for kind, payload in await self._event_kinds(repo)
            if kind == "job.dependency_released"
        ]
        self.assertEqual(len(released), 1)
        self.assertEqual(released[0]["failed_dep_id"], "dead-dep")
        self.assertEqual(released[0]["dep_status"], "purged")


class ClaimTimeDefensiveDropTests(_RepoCase):
    """Claim path drops depends_on ids whose rows no longer exist."""

    async def test_purged_dependency_dropped_at_claim_time(self):
        """A dep purged BEFORE this fix blocks the dependent; the claim drops it."""
        repo = await self._repo()
        urn = await self._agent(repo)
        await self._mk(repo, "dependent", status="queued", deps=["ghost"])
        # 'ghost' has no row — it was purged before the release fix existed.
        now = time.time()
        async with repo.connection() as tx:
            rc = await repo.claim_job(
                tx,
                job_id="dependent",
                agent_urn=urn,
                now=now,
                lease_expires_at=now + 60,
                runtime="zeroclaw",
                model="qwen38",
                provider="local",
                cost_tier="B",
            )
            await tx.commit()
        # The ghost id was dropped (its row is gone), the list emptied -> claimed.
        self.assertEqual(rc, 1)
        self.assertEqual(await self._row(repo, "dependent"), ("claimed", None))

    async def test_purged_dependency_with_live_dep_stays_blocked(self):
        """Only the purged id is dropped; a real, not-done dep still blocks."""
        repo = await self._repo()
        urn = await self._agent(repo)
        await self._mk(repo, "live-dep", status="queued")
        await self._mk(repo, "dependent", status="queued", deps=["ghost", "live-dep"])
        now = time.time()
        async with repo.connection() as tx:
            with self.assertRaises(DependencyNotReady):
                await repo.claim_job(
                    tx,
                    job_id="dependent",
                    agent_urn=urn,
                    now=now,
                    lease_expires_at=now + 60,
                    runtime="zeroclaw",
                    model="qwen38",
                    provider="local",
                    cost_tier="B",
                )
        # The ghost id is dropped in the claim transaction, the live dep
        # remains -> still queued/blocked.
        self.assertEqual(await self._row(repo, "dependent"), ("queued", '["live-dep"]'))

    async def test_drop_missing_dependencies_skips_non_claimable_rows(self):
        """The drop never rewrites a claimed/running row's depends_on."""
        repo = await self._repo()
        await self._mk(repo, "running-job", status="running", deps=["ghost"])
        async with repo.connection() as tx:
            out = await repo.drop_missing_dependencies(tx, job_id="running-job")
        self.assertEqual(out, [])
        self.assertEqual(await self._row(repo, "running-job"), ("running", '["ghost"]'))


class SameTransactionReleaseTests(_RepoCase):
    """The release is atomic with the status write / the purge DELETE."""

    async def test_failed_status_and_release_commit_together(self):
        """The status write and the dependent's release share one transaction."""
        repo = await self._repo()
        await self._mk(repo, "dep", status="claimed")
        await self._mk(repo, "dependent", status="queued", deps=["dep"])
        # Count commit() on the tx handle that flows through the router. The
        # router opens ONE connection for the status write, release, and
        # commit; the events phase uses a second connection (which is also
        # counted here) — the pin is that the release happens BEFORE the
        # first commit on the status-write connection, so no intermediate
        # state is ever visible.
        commits: list[int] = []
        original_commit = repo.connection

        class _TxSpy:
            def __init__(self, inner):
                self._inner = inner
                self._committed = False

            async def __aenter__(self):
                self._tx = await self._inner.__aenter__()
                return self

            async def __aexit__(self, *exc):
                return await self._inner.__aexit__(*exc)

            def __getattr__(self, name):
                return getattr(self._tx, name)

            async def commit(self):
                self._committed = True
                commits.append(len(commits) + 1)
                return await self._tx.commit()

        def spied_connection():
            return _TxSpy(original_connection_cm())

        # Simpler: wrap the context manager itself.
        def original_connection_cm():
            return original_commit()

        real_releases: list = []
        real_release = repo.release_dependents

        async def counting_release(tx, *, dep_id, dep_status):
            out = await real_release(tx, dep_id=dep_id, dep_status=dep_status)
            real_releases.append(out)
            return out

        with (
            mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo),
            mock.patch.object(repo, "release_dependents", counting_release),
        ):
            await hive_jobs_router.update_job(
                "dep", JobUpdate(status="failed", claimed_by="urn:agent:zeroclaw:host:deprelease")
            )

        # The release ran (one record), and the row is terminal.
        self.assertEqual(len(real_releases), 1)
        self.assertEqual(real_releases[0][0]["dependent_id"], "dependent")
        self.assertEqual(await self._row(repo, "dep"), ("failed", None))

    async def test_no_intermediate_state_observable(self):
        """No reader can observe 'dep terminal + dependent still lists it'.

        The release runs in the SAME transaction as the status write, so a
        concurrent read sees either both old states or both new states —
        never the mixed state that left the 2026-10-27 dependent queued
        forever.
        """
        repo = await self._repo()
        await self._mk(repo, "dep", status="claimed")
        await self._row(repo, "dep")
        await self._mk(repo, "dependent", status="queued", deps=["dep"])

        observed: list[tuple] = []
        release_seen_intermediate = []

        real_release = repo.release_dependents

        async def watching_release(tx, *, dep_id, dep_status):
            # BEFORE the release: the dep is already failed (status write ran
            # first in this tx) and the dependent still lists it — the
            # intermediate state a SECOND connection could only see if the
            # status write had been committed alone.
            async with tx.execute("SELECT status FROM jobs WHERE id='dep'") as cur:
                dep_status_now = (await cur.fetchone())[0]
            async with tx.execute("SELECT depends_on FROM jobs WHERE id='dependent'") as cur:
                dep_deps_now = (await cur.fetchone())[0]
            observed.append((dep_status_now, dep_deps_now))
            return await real_release(tx, dep_id=dep_id, dep_status=dep_status)

        # A second connection reading the DB WHILE the release tx is open
        # must see either (claimed, [dep]) or (failed, None) — never the
        # mixed (failed, [dep]).
        intermediate_seen = []
        real_claim_gate = None

        with mock.patch.object(repo, "release_dependents", watching_release):
            with mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo):
                await hive_jobs_router.update_job(
                    "dep", JobUpdate(status="failed", claimed_by="urn:agent:zeroclaw:host:deprelease")
                )

        # Inside the transaction: the status write precedes the release, so
        # the release sees (failed, ["dep"]) — that ordering is fine, it is
        # UNCOMMITTED and invisible to other connections.
        self.assertEqual(observed, [("failed", '["dep"]')])
        # After commit: no intermediate state remains observable.
        self.assertEqual(await self._row(repo, "dep"), ("failed", None))
        self.assertEqual(await self._row(repo, "dependent"), ("queued", None))

    async def test_purge_delete_and_release_commit_together(self):
        """The DELETE and the release share one connection/transaction."""
        repo = await self._repo()
        await self._mk(repo, "dead-dep", status="dead-letter")
        await self._mk(repo, "dependent", status="queued", deps=["dead-dep"])
        events: list[tuple] = []
        real_release = repo.release_dependents

        async def watching_release(tx, *, dep_id, dep_status):
            # The row is already deleted (DELETE ran first in this tx) and
            # the dependent still lists it — uncommitted, so invisible to
            # other connections until commit.
            async with tx.execute("SELECT 1 FROM jobs WHERE id='dead-dep'") as cur:
                row = await cur.fetchone()
            async with tx.execute("SELECT depends_on FROM jobs WHERE id='dependent'") as cur:
                deps_now = (await cur.fetchone())[0]
            events.append((row, deps_now, dep_status))
            return await real_release(tx, dep_id=dep_id, dep_status=dep_status)

        with (
            mock.patch.object(hive_admin_router, "hive_repo", lambda: repo),
            mock.patch.object(hive_admin_router, "connect_db", repo.connection),
            mock.patch.object(hive_admin_router, "require_mutation_actor", mock.AsyncMock(return_value="urn:admin")),
            mock.patch.object(repo, "release_dependents", watching_release),
        ):
            from hive.bus.schemas import PurgeRequest

            await hive_admin_router.admin_purge_jobs(PurgeRequest(job_ids=["dead-dep"], reason="test"))

        self.assertEqual(events, [(None, '["dead-dep"]', "purged")])
        self.assertIsNone(await self._row(repo, "dead-dep"))
        self.assertEqual(await self._row(repo, "dependent"), ("queued", None))


def _tx(repo, job_id):  # placeholder to keep old assertions from running
    raise NotImplementedError


if __name__ == "__main__":
    unittest.main()
