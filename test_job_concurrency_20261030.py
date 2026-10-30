"""FB37: concurrency for claim and lease on the in-process bus.

Every scenario runs against a temporary SQLite bus through the real ASGI app,
with several asyncio tasks hitting it at once and a fake clock for leases.

Covered races:

* N workers claiming M jobs: no job is double-claimed and none is lost;
* an expired lease racing a renewal (and the reaper's selector seeing it);
* completion racing cancellation on the same claimed row;
* two doctors reclassifying the same row at once.

The deterministic checks run once; ``test_slow_races_repeat`` repeats all four
200 times and is marked ``slow``.
"""

import asyncio
import os
import tempfile
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest

import agent_bus
from hive.bus import deps
from hive.bus.domain.reaper import select_orphan_claims
from hive.bus.routers import admin as admin_router
from hive.bus.routers import jobs as jobs_router

LEASE = jobs_router.CLAIM_LEASE_SECONDS
SLOW_REPEATS = 200


class Bus:
    """A temporary SQLite bus with a fake clock, driven from one event loop."""

    def __init__(self):
        self.temp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {
            "AGENT_BUS_DB": str(Path(self.temp.name) / "bus.db"),
            "HIVE_BACKEND": "sqlite",
            "MNEMOS_URL": "",
            "MNEMOS_TOKEN": "",
            "HIVE_SHARED_SECRET": "",
            "HIVE_BUS_ADMIN_TOKEN": "",
        })
        self.env.start()
        self.repo_patch = patch.object(deps, "_HIVE_REPO", None)
        self.repo_patch.start()
        self.clock = 1800000000.0
        self.time_patch = patch.object(jobs_router, "time", SimpleNamespace(time=lambda: self.clock))
        self.time_patch.start()
        asyncio.run(deps.hive_repo().init())

    def close(self):
        self.time_patch.stop()
        self.repo_patch.stop()
        self.env.stop()
        self.temp.cleanup()

    def run(self, scenario):
        async def main():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://test"
            ) as client:
                return await scenario(client)

        return asyncio.run(main())

    def advance(self, seconds):
        self.clock += seconds

    @staticmethod
    async def register(client, kind, capabilities):
        response = await client.post("/v1/agents/register", json={
            "kind": kind, "host": "conc", "runtime": kind,
            "capabilities": capabilities, "cost_tier": "B", "auth_method": "api",
        })
        assert response.status_code == 200, response.text
        return response.json()["urn"]

    @staticmethod
    async def submit(client, submitter, kind="e2e:conc"):
        # The bus coalesces an identical (kind, description) resubmission onto
        # the live row, so a fixture that reused one description would create
        # ONE job and then see legitimate 204s. Every submission here is a
        # distinct job.
        Bus.submitted += 1
        response = await client.post("/v1/jobs", json={
            "kind": kind, "description": f"concurrency-{Bus.submitted}",
            "submitter_urn": submitter, "max_retries": 2,
        }, headers={"X-Hive-Actor": submitter})
        assert response.status_code in (200, 201), response.text
        body = response.json()
        assert "coalesced" not in body, body
        return body["id"]

    submitted = 0

    @staticmethod
    async def jobs(client):
        response = await client.get("/v1/jobs?limit=100")
        assert response.status_code == 200, response.text
        return response.json()["jobs"]

    @staticmethod
    async def row(client, jid):
        response = await client.get(f"/v1/jobs/{jid}")
        assert response.status_code == 200, response.text
        return response.json()

    @staticmethod
    async def claim_next(client, urn):
        response = await client.post(
            f"/v1/jobs/next?agent_urn={urn}", headers={"X-Hive-Actor": urn}
        )
        assert response.status_code in (200, 204), response.text
        return response.status_code, (response.json()["id"] if response.status_code == 200 else None)

    @staticmethod
    async def patch(client, jid, actor, status, result=None):
        body = {"status": status, "claimed_by": actor}
        if result is not None:
            body["result"] = result
        response = await client.patch(f"/v1/jobs/{jid}", json=body, headers={"X-Hive-Actor": actor})
        return response.status_code


def test_parallel_claims_never_double_claim_or_lose_a_job():
    """Every queued job is claimed exactly once when workers drain the queue."""
    bus = Bus()
    try:
        async def scenario(client):
            submitter = await bus.register(client, "opencode", ["code", "orchestrate"])
            workers = [await bus.register(client, "zeroclaw", ["code"]) for _ in range(8)]
            jobs = [await bus.submit(client, submitter) for _ in range(20)]

            claimed: dict[str, str] = {}

            async def drain(urn):
                mine = []
                while True:
                    code, jid = await bus.claim_next(client, urn)
                    if code == 204:
                        return mine
                    mine.append(jid)
                    claimed[jid] = urn

            results = await asyncio.gather(*[drain(urn) for urn in workers])
            all_claimed = [jid for mine in results for jid in mine]

            assert len(all_claimed) == len(set(all_claimed)), "a job was claimed twice"
            assert set(all_claimed) == set(jobs), "a queued job was lost"
            rows = {job["id"]: job for job in await bus.jobs(client)}
            for jid in jobs:
                row = rows[jid]
                assert row["status"] == "claimed", row
                assert row["claimed_by"] == claimed[jid], row
                assert row["claim_lease_expires_at"] == bus.clock + LEASE, row

        bus.run(scenario)
    finally:
        bus.close()


def test_expired_lease_racing_a_renewal():
    """A renewal racing the reaper's requeue leaves one consistent row."""
    bus = Bus()
    try:
        async def scenario(client):
            submitter = await bus.register(client, "opencode", ["code", "orchestrate"])
            worker = await bus.register(client, "zeroclaw", ["code"])
            jid = await bus.submit(client, submitter)
            assert (await bus.claim_next(client, worker))[0] == 200
            bus.advance(LEASE + 1)

            rows = await bus.jobs(client)
            candidate = [(jid, rows[0]["kind"], rows[0]["claimed_by"], rows[0]["claimed_at"],
                          rows[0]["last_update_at"], rows[0]["claim_lease_expires_at"])]
            selected = select_orphan_claims(candidate, now=bus.clock, stale_after=10**9, hard_max_age=10**9)
            assert [item[0] for item in selected] == [jid], "the reaper must see the expired lease"

            renew, requeue = await asyncio.gather(
                bus.patch(client, jid, worker, "claimed"),
                client.post("/v1/admin/jobs/requeue", json={"job_ids": [jid]},
                            headers={"X-Hive-Actor": submitter}),
            )
            assert renew in (200, 409), renew
            assert requeue.status_code == 200, requeue.text
            assert jid in requeue.json()["ok"], requeue.json()

            row = await bus.row(client, jid)
            assert row["status"] == "queued", row
            assert row["claimed_by"] is None, row
            assert row["claim_lease_expires_at"] is None, row
            if renew == 409:
                # The renewal lost the race: it must not have refreshed anything.
                assert row["claim_lease_expires_at"] is None, row

        bus.run(scenario)
    finally:
        bus.close()


def test_completion_racing_cancellation():
    """A completion and a cancellation leave one coherent terminal row.

    ``docs/JOB_LIFECYCLE.md`` documents ``done | cancelled`` as a legal
    retirement, so the two writes may both apply (in some order); what must
    never happen is a torn row: a terminal status that still holds a lease, a
    500, or a status that is neither of the two targets.
    """
    bus = Bus()
    try:
        async def scenario(client):
            submitter = await bus.register(client, "opencode", ["code", "orchestrate"])
            worker = await bus.register(client, "zeroclaw", ["code"])
            jid = await bus.submit(client, submitter)
            assert (await bus.claim_next(client, worker))[0] == 200

            done, cancelled = await asyncio.gather(
                bus.patch(client, jid, worker, "done", {"note": "concurrency"}),
                bus.patch(client, jid, worker, "cancelled"),
            )
            assert {done, cancelled} <= {200, 409}, (done, cancelled)
            assert 200 in (done, cancelled), (done, cancelled)

            row = await bus.row(client, jid)
            assert row["status"] in {"done", "cancelled"}, row
            assert row["claim_lease_expires_at"] is None, row
            if done == 200 and cancelled == 409:
                assert row["status"] == "done", row
            if cancelled == 200 and done == 409:
                assert row["status"] == "cancelled", row

        bus.run(scenario)
    finally:
        bus.close()


def test_two_doctors_reclassify_with_stale_reads():
    """Two doctors that both read the same status: one CAS wins.

    The reclassify loop reads the row (`_budget_row`) and then compare-and-sets
    on that status. Without a barrier the two requests simply run one after the
    other and both legitimately apply. The barrier holds both reads open so the
    second UPDATE is guaranteed to carry a stale ``expected_status``; that
    request must report the job as a conflict, not as applied.
    """
    bus = Bus()
    try:
        async def scenario(client):
            submitter = await bus.register(client, "opencode", ["code", "orchestrate"])
            jid = await bus.submit(client, submitter)

            reads_ready = asyncio.Barrier(2)
            original = admin_router._budget_row

            async def gated_read(tx, job_id):
                row = await original(tx, job_id)
                await reads_ready.wait()
                return row

            async def reclassify(status):
                response = await client.post(
                    "/v1/admin/jobs/reclassify",
                    json={"job_ids": [jid], "status": status},
                    headers={"X-Hive-Actor": submitter},
                )
                assert response.status_code == 200, response.text
                return status, response.json()

            with patch.object(admin_router, "_budget_row", gated_read):
                outcomes = await asyncio.gather(reclassify("needs-review"), reclassify("cancelled"))

            winners = [status for status, body in outcomes if jid in body["ok"]]
            losers = [body for status, body in outcomes if jid not in body["ok"]]
            assert len(winners) == 1, outcomes
            assert len(losers) == 1, outcomes
            assert losers[0]["skipped_conflict"] == [jid], losers[0]

            row = await bus.row(client, jid)
            assert row["status"] == winners[0], row
            assert row["claimed_by"] is None, row

        bus.run(scenario)
    finally:
        bus.close()


def test_sequential_reclassifications_each_report_truthfully():
    """Two doctors, no barrier: every reported ``ok`` is a transition that ran."""
    bus = Bus()
    try:
        async def scenario(client):
            submitter = await bus.register(client, "opencode", ["code", "orchestrate"])
            jid = await bus.submit(client, submitter)

            async def reclassify(status):
                response = await client.post(
                    "/v1/admin/jobs/reclassify",
                    json={"job_ids": [jid], "status": status},
                    headers={"X-Hive-Actor": submitter},
                )
                assert response.status_code == 200, response.text
                return status, response.json()

            outcomes = await asyncio.gather(reclassify("needs-review"), reclassify("cancelled"))
            applied = [status for status, body in outcomes if jid in body["ok"]]
            conflicts = [body for status, body in outcomes if jid in body["skipped_conflict"]]
            assert len(applied) + len(conflicts) == 2, outcomes
            assert applied, "one of the two must land"

            row = await bus.row(client, jid)
            # Whatever order the two ran in, the row carries the result of one of
            # the transitions the bus reported as applied -- never a third value.
            assert row["status"] in set(applied), (row["status"], outcomes)
            if len(applied) == 1:
                assert row["status"] == applied[0], (row["status"], outcomes)

        bus.run(scenario)
    finally:
        bus.close()


@pytest.mark.slow
def test_slow_races_repeat():
    """Each race repeated 200 times, reporting the first inconsistent row."""
    bus = Bus()
    try:
        async def parallel(client, submitter, workers, size):
            jobs = [await bus.submit(client, submitter) for _ in range(size)]
            seen: Counter = Counter()

            async def drain(urn):
                while True:
                    code, jid = await bus.claim_next(client, urn)
                    if code == 204:
                        return
                    seen[jid] += 1

            await asyncio.gather(*[drain(urn) for urn in workers])
            # Rows left queued by earlier iterations are legitimately claimable,
            # so the check is that THIS iteration's jobs were all taken, exactly
            # once, rather than that nothing else was.
            missing = set(jobs) - set(seen)
            assert not missing, f"unclaimed jobs: {sorted(missing)}"
            assert all(count == 1 for count in seen.values()), seen

        async def renew_vs_reaper(client, submitter, worker):
            jid = await bus.submit(client, submitter)
            assert (await bus.claim_next(client, worker))[0] == 200
            bus.advance(LEASE + 1)
            renew, requeue = await asyncio.gather(
                bus.patch(client, jid, worker, "claimed"),
                client.post("/v1/admin/jobs/requeue", json={"job_ids": [jid]},
                            headers={"X-Hive-Actor": submitter}),
            )
            assert renew in (200, 409) and requeue.status_code == 200
            row = await bus.row(client, jid)
            assert row["status"] == "queued" and row["claimed_by"] is None, row
            # Retire the requeued row so later iterations do not drain it.
            assert await bus.patch(client, jid, submitter, "cancelled") == 200

        async def complete_vs_cancel(client, submitter, worker):
            jid = await bus.submit(client, submitter)
            assert (await bus.claim_next(client, worker))[0] == 200
            done, cancelled = await asyncio.gather(
                bus.patch(client, jid, worker, "done", {"note": "loop"}),
                bus.patch(client, jid, worker, "cancelled"),
            )
            assert {done, cancelled} <= {200, 409} and 200 in (done, cancelled), (done, cancelled)
            row = await bus.row(client, jid)
            assert row["status"] in {"done", "cancelled"}, row
            assert row["claim_lease_expires_at"] is None, row

        async def doctors(client, submitter):
            jid = await bus.submit(client, submitter)
            reads_ready = asyncio.Barrier(2)
            original = admin_router._budget_row

            async def gated_read(tx, job_id):
                row = await original(tx, job_id)
                await reads_ready.wait()
                return row

            async def reclassify(status):
                response = await client.post(
                    "/v1/admin/jobs/reclassify",
                    json={"job_ids": [jid], "status": status},
                    headers={"X-Hive-Actor": submitter},
                )
                assert response.status_code == 200, response.text
                return status, response.json()

            with patch.object(admin_router, "_budget_row", gated_read):
                outcomes = await asyncio.gather(reclassify("needs-review"), reclassify("cancelled"))
            winners = [s for s, body in outcomes if jid in body["ok"]]
            assert len(winners) == 1, outcomes
            row = await bus.row(client, jid)
            assert row["status"] == winners[0], row

        async def scenario(client):
            submitter = await bus.register(client, "opencode", ["code", "orchestrate"])
            workers = [await bus.register(client, "zeroclaw", ["code"]) for _ in range(8)]
            for _ in range(SLOW_REPEATS):
                await parallel(client, submitter, workers, 8)
                await renew_vs_reaper(client, submitter, workers[0])
                await complete_vs_cancel(client, submitter, workers[0])
                await doctors(client, submitter)

        bus.run(scenario)
    finally:
        bus.close()
