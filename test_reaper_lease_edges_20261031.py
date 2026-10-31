"""FB55: reaper and lease edge cases — extends FB37.

Four scenarios, each stated as an invariant in the test docstring, all with a
fake clock (fixed constants, no sleeps, no threads):

1. renew-at-exact-expiry: a renew PATCH landing at ``now == lease_expires_at``
   succeeds (renew has no lease precondition) and the reaper scan at that same
   instant finds nothing; with the scan first and the renew second, the requeue
   compare-and-set loses and the job stays claimed on the fresh lease.
2. backward clock: the pure selector never reaps on negative durations, but a
   renew executed while the wall clock is behind writes an already-expired
   lease, so the reaper immediately requeues a live worker's job (FB55-1).
3. heartbeat-without-renew: agent liveness never extends a lease — the reaper
   takes the job while the agent row stays online.
4. mass orphans: one pass requeues 60 mixed-cause orphans exactly once.

Repo style follows ``test_dependency_release_20261027.py`` (direct
``SqliteHiveRepository`` on a temp file; the router's ``hive_repo`` and
``emit_event`` patched to the temp repo / a mock). No new dependencies.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")
os.environ.setdefault("HIVE_BUS_ADMIN_TOKEN", "test-admin-token")

import agent_bus  # noqa: E402
from hive.bus.domain.reaper import select_orphan_claims  # noqa: E402
from hive.bus.routers import jobs as hive_jobs_router  # noqa: E402
from hive.bus.schemas import JobUpdate  # noqa: E402
from hive.persistence import sqlite as sqlite_mod  # noqa: E402

T0 = 1_700_000_000.0
LEASE = 1800.0
STALE_AFTER = 1800.0
HARD_MAX = 14400.0


def _sel(cands, now, **kw):
    base = dict(stale_after=STALE_AFTER, hard_max_age=HARD_MAX)
    base.update(kw)
    return select_orphan_claims(cands, now=now, **base)


class _RepoCase(unittest.IsolatedAsyncioTestCase):
    async def _repo(self, name="lease-edge.db"):
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        repo = sqlite_mod.SqliteHiveRepository(str(Path(td.name) / name))
        await repo.init(agent_bus.SCHEMA)
        return repo

    async def _agent(self, repo, urn="urn:agent:zeroclaw:host:lease55", status="online", hb=None):
        now = hb if hb is not None else T0
        async with repo.connection() as db:
            await db.execute(
                "INSERT INTO agents (urn, kind, host, session_id, capabilities, started_at, "
                "last_heartbeat, status, runtime, cost_tier) "
                "VALUES (?, 'zeroclaw', 'host', 'session', '[]', ?, ?, ?, 'zeroclaw', 'B')",
                (urn, now, now, status),
            )
            await db.commit()
        return urn

    async def _claim_row(self, repo, job_id, urn, *, claimed_at=T0, lease_off=LEASE, updated=None):
        lease = claimed_at + lease_off if lease_off is not None else None
        async with repo.connection() as db:
            await db.execute(
                "INSERT INTO jobs (id, submitter_urn, kind, status, started_at, claimed_by, "
                "claimed_at, last_update_at, claim_lease_expires_at) "
                "VALUES (?, 'urn:submitter', 'build', 'claimed', ?, ?, ?, ?, ?)",
                (job_id, claimed_at, urn, claimed_at, updated if updated is not None else claimed_at, lease),
            )
            await db.commit()

    async def _job(self, repo, job_id):
        async with repo.connection() as tx:
            async with tx.execute(
                "SELECT status, claimed_by, claimed_at, last_update_at, claim_lease_expires_at FROM jobs WHERE id=?",
                (job_id,),
            ) as cur:
                return await cur.fetchone()

    def _router_ctx(self, repo):
        return (
            mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo),
            mock.patch.object(hive_jobs_router, "emit_event", mock.AsyncMock()),
        )

    async def _renew(self, repo, job_id, urn, *, now, status="claimed"):
        """A real renew PATCH through the router at faked wall-clock ``now``."""
        with mock.patch("time.time", return_value=now):
            patches = self._router_ctx(repo)
            patches[0].start()
            patches[1].start()
            try:
                return await hive_jobs_router.update_job(job_id, JobUpdate(status=status, claimed_by=urn))
            finally:
                patches[1].stop()
                patches[0].stop()

    async def _reap_pass(self, repo, now):
        """The app reaper's select→filter→CAS loop at ``now``; returns (requeued, events)."""
        default_cutoff = now - STALE_AFTER
        hard_cutoff = now - HARD_MAX
        async with repo.connection() as db:
            candidates = await repo.select_orphan_claim_candidates(
                db, now=now, hard_cutoff=hard_cutoff, default_cutoff=default_cutoff
            )
        orphans = _sel([c[:6] for c in candidates], now)
        snapshots = {c[0]: c for c in candidates}
        requeued, events = [], []
        async with repo.connection() as db:
            for job_id, claimer in orphans:
                if await repo.requeue_orphan_job(db, snapshots[job_id]):
                    requeued.append(job_id)
                    events.append({"id": job_id, "prior_claimer": claimer, "reason": "worker_offline_or_stale_claim"})
            await db.commit()
        return requeued, events


class RenewAtExactExpiryTests(_RepoCase):
    async def test_renew_at_exact_expiry_extends_lease_and_scan_finds_nothing(self):
        """Invariant: renew has no lease precondition — a PATCH at now == lease
        extends the lease, and the reaper evaluated at that same instant flags
        nothing (lease_exp <= now is false once the renew moved it forward)."""
        repo = await self._repo()
        urn = await self._agent(repo)
        await self._claim_row(repo, "j", urn)
        out = await self._renew(repo, "j", urn, now=T0 + LEASE)
        self.assertTrue(out.get("ok"), out)
        row = await self._job(repo, "j")
        self.assertEqual(row[0], "claimed")
        self.assertAlmostEqual(row[4], T0 + 2 * LEASE)
        requeued, _ = await self._reap_pass(repo, T0 + LEASE)
        self.assertEqual(requeued, [])

    async def test_scan_first_then_renew_loses_the_requeue_cas(self):
        """Invariant: when the reaper snapshot predates a renew, the requeue
        compare-and-set loses (row changed under it) and the job stays claimed
        on the fresh lease — the CAS, not ordering luck, resolves the race."""
        repo = await self._repo()
        urn = await self._agent(repo)
        await self._claim_row(repo, "j", urn)
        now = T0 + LEASE
        # Reaper snapshot at the expiry instant: flags the orphan.
        async with repo.connection() as db:
            candidates = await repo.select_orphan_claim_candidates(
                db, now=now, hard_cutoff=now - HARD_MAX, default_cutoff=now - STALE_AFTER
            )
        self.assertEqual(_sel([c[:6] for c in candidates], now), [("j", urn)])
        snapshot = candidates[0]
        # Renew lands before the requeue write.
        await self._renew(repo, "j", urn, now=now)
        async with repo.connection() as db:
            rc = await repo.requeue_orphan_job(db, snapshot)
            await db.commit()
        self.assertEqual(rc, 0)
        row = await self._job(repo, "j")
        self.assertEqual(row[0], "claimed")
        self.assertAlmostEqual(row[4], now + LEASE)


class BackwardClockTests(_RepoCase):
    def test_pure_selector_never_reaps_on_negative_durations(self):
        """Invariant: with now behind every timestamp, all three rules evaluate
        negative-vs-ttl and flag nothing — a backward jump cannot mass-reap by itself."""
        cands = [
            ("j1", "build", "urn:a", T0, T0, T0 + LEASE),
            ("j2", "build", "urn:a", T0, T0 - 100, T0 + LEASE),
            ("j3", "build", "urn:a", None, None, None),
        ]
        # The phantom (all-NULL) row is timelessly stale — latest=0 — and stays
        # flagged; that is pre-existing intended behaviour, not clock-related.
        self.assertEqual(_sel(cands, T0 - 3600), [("j3", "urn:a")])

    async def test_renew_under_backward_clock_writes_instantly_expired_lease(self):
        """Invariant (FB55-1, surprising): renew trusts the wall clock, so a
        renew executed while the clock is behind writes lease = past + 1800,
        which is already expired against the true clock — the reaper then
        requeues a live worker's job. No monotonic guard exists."""
        repo = await self._repo()
        urn = await self._agent(repo)
        await self._claim_row(repo, "j", urn)
        await self._renew(repo, "j", urn, now=T0 - 3600)
        row = await self._job(repo, "j")
        self.assertAlmostEqual(row[4], T0 - 3600 + LEASE)
        self.assertLess(row[4], T0)  # already expired against the true clock
        requeued, _ = await self._reap_pass(repo, T0 + 1)
        self.assertEqual(requeued, ["j"])


class HeartbeatWithoutRenewTests(_RepoCase):
    async def test_live_heartbeat_does_not_extend_the_lease(self):
        """Invariant: heartbeats touch only agents.last_heartbeat — a worker
        that heartbeats but never PATCHes loses its job at lease expiry while
        its agent row stays online. Liveness and lease are independent by design."""
        repo = await self._repo()
        urn = await self._agent(repo)
        await self._claim_row(repo, "j", urn)
        async with repo.connection() as db:
            await repo.heartbeat_agent(db, urn=urn, ts=T0 + LEASE - 10, status="online")
            await db.commit()
        requeued, events = await self._reap_pass(repo, T0 + LEASE + 1)
        self.assertEqual(requeued, ["j"])
        self.assertEqual(len(events), 1)
        row = await self._job(repo, "j")
        self.assertEqual(row[0], "queued")
        self.assertIsNone(row[1])
        async with repo.connection() as db:
            async with db.execute("SELECT status FROM agents WHERE urn=?", (urn,)) as cur:
                self.assertEqual((await cur.fetchone())[0], "online")

    async def test_progress_patch_keeps_reaper_at_bay_past_stale_ttl(self):
        """Invariant: steady PATCH progress (last_update_at fresh, lease renewed)
        defeats the kind-TTL rule — only the un-refreshable hard cap still applies."""
        repo = await self._repo()
        urn = await self._agent(repo)
        await self._claim_row(repo, "j", urn, claimed_at=T0)
        await self._renew(repo, "j", urn, now=T0 + STALE_AFTER + 60, status="running")
        requeued, _ = await self._reap_pass(repo, T0 + STALE_AFTER + 61)
        self.assertEqual(requeued, [])


class MassOrphanTests(_RepoCase):
    async def test_sixty_mixed_cause_orphans_requeue_exactly_once(self):
        """Invariant: one reaper pass requeues every orphan exactly once across
        all four causes (lease, staleness, hard cap, phantom); a second pass
        finds zero — no loss, no double-requeue."""
        repo = await self._repo()
        now = T0 + HARD_MAX + 100
        for i in range(60):
            urn = await self._agent(repo, urn=f"urn:agent:zeroclaw:host:w{i:02d}", status="offline")
            cause = i % 4
            if cause == 0:  # lease expired
                await self._claim_row(repo, f"j{i:02d}", urn, claimed_at=now - 100, lease_off=-10, updated=now - 50)
            elif cause == 1:  # stale, lease fresh
                await self._claim_row(
                    repo, f"j{i:02d}", urn, claimed_at=now - 100, lease_off=HARD_MAX, updated=now - STALE_AFTER - 1
                )
            elif cause == 2:  # hard cap, everything else fresh
                await self._claim_row(
                    repo, f"j{i:02d}", urn, claimed_at=now - HARD_MAX - 1, lease_off=HARD_MAX, updated=now - 1
                )
            else:  # phantom: NULL claimed_at
                async with repo.connection() as db:
                    await db.execute(
                        "INSERT INTO jobs (id, submitter_urn, kind, status, started_at, "
                        "claimed_by, claimed_at, last_update_at, claim_lease_expires_at) "
                        "VALUES (?, 'urn:submitter', 'build', 'claimed', ?, ?, NULL, NULL, NULL)",
                        (f"j{i:02d}", now - 10, urn),
                    )
                    await db.commit()
        requeued, events = await self._reap_pass(repo, now)
        self.assertEqual(len(requeued), 60)
        self.assertEqual(sorted(requeued), sorted(f"j{i:02d}" for i in range(60)))
        self.assertEqual(len(events), 60)
        requeued2, _ = await self._reap_pass(repo, now)
        self.assertEqual(requeued2, [])


if __name__ == "__main__":
    unittest.main()
