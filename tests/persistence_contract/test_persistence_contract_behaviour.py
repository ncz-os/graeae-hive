"""FB38: the persistence contract, run for real against every configured backend.

One behavioural suite, parameterised over the supported backends. sqlite (the
reference dialect every other backend subclasses) always runs; oracle, postgres,
mysql and db2 run when their DSN is set and are SKIPPED otherwise.

The point is not sqlite's behaviour for its own sake: it is that each of these
assertions is written against the ``HiveMindRepository`` ABC, so the day another
backend is reachable these same tests say whether its SQL translations still
mean what the bus expects.
"""

from __future__ import annotations

import json

import contract_support as cs
import pytest


def _job_row(repo, job_id):
    """The job as the ABC exposes it in a listing (named columns, not a Row)."""
    import asyncio

    async def _read():
        async with repo.connection() as tx:
            _total, rows = await repo.list_jobs(tx, limit=1000)
            return next((r for r in rows if r["id"] == job_id), None)

    return asyncio.run(_read())


def test_init_is_idempotent_and_connection_yields_a_transaction(backend):
    """init() twice on the same repository is a no-op, not a failure."""
    backend.run(backend.repo.init())

    async def _probe():
        async with backend.repo.connection() as tx:
            await tx.execute("SELECT 1")
            await tx.commit()
        return True

    assert backend.run(_probe())


def test_agent_register_heartbeat_and_metadata_patch(backend):
    urn = f"urn:agent:zeroclaw:contract:{backend.uid()}"
    now = 1_800_000_000.0

    async def _scenario():
        async with backend.repo.connection() as tx:
            await backend.repo.register_agent(tx, **cs.agent_fields(urn, "s1", now))
            before = await backend.repo.require_agent(tx, urn=urn)
            await backend.repo.heartbeat_agent(
                tx, urn=urn, ts=now + 5, status="idle", metadata_patch={"load": 2}
            )
            rows = await backend.repo.list_agents(tx, include_offline=True)
            await tx.commit()
        return before, next(r for r in rows if r["urn"] == urn)

    before, after = backend.run(_scenario())
    assert before is not None and before[0] == "zeroclaw"
    assert after["status"] == "idle"
    assert after["metadata"]["load"] == 2
    # The patch MERGES: the register-time metadata key survives.
    assert after["metadata"]["suite"] == "persistence_contract"


def test_submit_claim_patch_cycle_and_compare_and_set(backend):
    uid = backend.uid()
    urn = f"urn:agent:zeroclaw:contract:{uid}"
    now = 1_800_000_000.0

    async def _scenario():
        async with backend.repo.connection() as tx:
            await backend.repo.register_agent(tx, **cs.agent_fields(urn, uid, now))
            await backend.repo.submit_job(tx, **cs.job_fields(f"job-{uid}", urn, now))
            await tx.commit()
            claimed = await backend.repo.claim_job(
                tx,
                job_id=f"job-{uid}",
                agent_urn=urn,
                now=now,
                lease_expires_at=now + 60,
                runtime="zeroclaw",
                model="contract-model",
                provider="contract-provider",
                cost_tier="B",
            )
            # A second claim of the same row must lose the compare-and-set.
            again = await backend.repo.claim_job(
                tx,
                job_id=f"job-{uid}",
                agent_urn=urn,
                now=now,
                lease_expires_at=now + 60,
                runtime="zeroclaw",
                model="contract-model",
                provider="contract-provider",
                cost_tier="B",
            )
            stale = await backend.repo.patch_job_status(
                tx, f"job-{uid}", sets={"status": "running"}, expected_status="queued"
            )
            ok = await backend.repo.patch_job_status(
                tx,
                f"job-{uid}",
                sets={"status": "done", "ended_at": now},
                expected_status="claimed",
            )
            await tx.commit()
        return claimed, again, stale, ok

    claimed, again, stale, ok = backend.run(_scenario())
    assert claimed == 1
    assert again == 0, "a claimed row must not be claimable twice"
    assert stale == 0, "a stale expected_status must not match"
    assert ok == 1
    row = _job_row(backend.repo, f"job-{uid}")
    assert row["status"] == "done"
    assert row["claimed_by"] == urn


def test_claim_refuses_an_unsatisfied_dependency(backend):
    from hive.persistence.sqlite import DependencyNotReady

    uid = backend.uid()
    urn = f"urn:agent:zeroclaw:contract:{uid}"
    now = 1_800_000_000.0
    dep_id, job_id = f"dep-{uid}", f"job-{uid}"

    async def _scenario():
        async with backend.repo.connection() as tx:
            await backend.repo.register_agent(tx, **cs.agent_fields(urn, uid, now))
            await backend.repo.submit_job(tx, **cs.job_fields(dep_id, urn, now))
            await backend.repo.submit_job(
                tx,
                **cs.job_fields(job_id, urn, now, depends_on_json=json.dumps([dep_id])),
            )
            await tx.commit()
            satisfied = await backend.repo.dependencies_satisfied(tx, job_id)
            blocked = await backend.repo.jobs_with_unsatisfied_deps(tx, ids=[job_id])
            with pytest.raises(DependencyNotReady):
                await backend.repo.claim_job(
                    tx,
                    job_id=job_id,
                    agent_urn=urn,
                    now=now,
                    lease_expires_at=now + 60,
                    runtime="zeroclaw",
                    model="m",
                    provider="p",
                    cost_tier="B",
                )
            await tx.commit()
        return satisfied, blocked

    satisfied, blocked = backend.run(_scenario())
    assert satisfied is False
    assert blocked == {job_id}
    assert _job_row(backend.repo, job_id)["status"] == "queued"


def test_dedup_index_and_lookup_agree(backend):
    uid = backend.uid()
    urn = f"urn:agent:zeroclaw:contract:{uid}"
    now = 1_800_000_000.0
    job_id = f"job-{uid}"

    async def _scenario():
        async with backend.repo.connection() as tx:
            await backend.repo.submit_job(
                tx, **cs.job_fields(job_id, urn, now, dedup_hash=f"dedup-{uid}")
            )
            await tx.commit()
            found = await backend.repo.find_active_dedup_job(tx, dedup_hash=f"dedup-{uid}")
            missing = await backend.repo.find_active_dedup_job(tx, dedup_hash=f"nope-{uid}")
        return found, missing

    found, missing = backend.run(_scenario())
    assert found is not None and found[0] == job_id
    assert missing is None


def test_events_are_append_only_and_cursor_readable(backend):
    uid = backend.uid()
    urn = f"urn:agent:zeroclaw:contract:{uid}"
    now = 1_800_000_000.0

    async def _scenario():
        async with backend.repo.connection() as tx:
            for i in range(3):
                await backend.repo.emit_event(
                    tx, ts=now + i, kind="contract.event", payload_json=json.dumps({"i": i}), agent_urn=urn
                )
            await tx.commit()
            first = await backend.repo.tail_events(tx, limit=2)
            rest = await backend.repo.tail_events(tx, since_id=first[-1]["id"], limit=10)
        return first, rest

    first, rest = backend.run(_scenario())
    assert [e["payload"]["i"] for e in first] == [0, 1]
    assert [e["payload"]["i"] for e in rest] == [2]


def test_messages_post_and_filter(backend):
    uid = backend.uid()
    urn = f"urn:agent:zeroclaw:contract:{uid}"
    now = 1_800_000_000.0

    async def _scenario():
        async with backend.repo.connection() as tx:
            await backend.repo.post_message(
                tx,
                id=f"msg-{uid}",
                from_urn=urn,
                to_urn=None,
                in_reply_to=None,
                topic="orchestrator.coordination",
                payload_json=json.dumps({"status": "contract"}),
                ts=now,
            )
            matching = await backend.repo.list_messages(tx, topic="orchestrator.*", limit=10)
            other = await backend.repo.list_messages(tx, topic="nothing.*", limit=10)
        return matching, other

    matching, other = backend.run(_scenario())
    assert any(m["id"] == f"msg-{uid}" for m in matching)
    assert other == []


def test_result_cache_store_lookup_and_hit(backend):
    uid = backend.uid()
    now = 1_800_000_000.0
    key = f"cache-{uid}"

    async def _scenario():
        async with backend.repo.connection() as tx:
            await backend.repo.cache_store(
                tx,
                cache_key=key,
                result_json=json.dumps({"exit_code": 0}),
                source_job_id=f"job-{uid}",
                model="m",
                provider="p",
                cached_at=now,
            )
            hit = await backend.repo.cache_lookup(tx, cache_key=key, cutoff=0.0)
            expired = await backend.repo.cache_lookup(tx, cache_key=key, cutoff=now + 1)
            await backend.repo.cache_record_hit(tx, cache_key=key, cost_saved_usd=0.5, ts=now)
            after = await backend.repo.cache_lookup(tx, cache_key=key, cutoff=0.0)
            # Storing the same key again UPSERTS instead of failing.
            await backend.repo.cache_store(
                tx,
                cache_key=key,
                result_json=json.dumps({"exit_code": 1}),
                source_job_id=f"job-{uid}",
                model="m",
                provider="p",
                cached_at=now + 2,
            )
            replaced = await backend.repo.cache_lookup(tx, cache_key=key, cutoff=0.0)
        return hit, expired, after, replaced

    hit, expired, after, replaced = backend.run(_scenario())
    assert hit["result"] == {"exit_code": 0}
    assert expired is None
    assert after["hit_count"] == hit["hit_count"] + 1
    assert after["cost_saved_usd"] == hit["cost_saved_usd"] + 0.5
    assert replaced["result"] == {"exit_code": 1}, "cache_store must upsert, not raise"


def test_worker_kind_stats_accumulate(backend):
    uid = backend.uid()
    urn = f"urn:agent:zeroclaw:contract:{uid}"
    now = 1_800_000_000.0

    async def _scenario():
        async with backend.repo.connection() as tx:
            for _ in range(2):
                await backend.repo.upsert_worker_kind_stats(
                    tx,
                    column="success_count",
                    urn=urn,
                    kind="e2e:contract",
                    tokens_in=10,
                    tokens_out=5,
                    cost_usd=0.01,
                    duration_sec=2.0,
                    last_run=now,
                )
            rows = await backend.repo.worker_stats_rows(tx, kind="e2e:contract", include_system=False, top_n=10)
        return rows

    rows = backend.run(_scenario())
    mine = next(r for r in rows if r["urn"] == urn)
    assert mine["success_count"] == 2
    assert mine["total_tokens_in"] == 20


def test_schedule_create_due_and_fire(backend):
    uid = backend.uid()
    urn = f"urn:agent:zeroclaw:contract:{uid}"
    now = 1_800_000_000.0
    sched_id = f"sched-{uid}"

    async def _scenario():
        async with backend.repo.connection() as tx:
            await backend.repo.create_schedule(
                tx,
                id=sched_id,
                name="contract schedule",
                created_by_urn=urn,
                interval_seconds=3600,
                job_template_json=json.dumps({"kind": "e2e:contract"}),
                enabled=1,
                next_fire_at=now - 1,
                created_at=now,
            )
            due = await backend.repo.select_due_schedules(tx, now=now)
            await backend.repo.mark_schedule_fired(tx, sched_id=sched_id, now=now, next_at=now + 3600)
            not_due = await backend.repo.select_due_schedules(tx, now=now)
            listed = await backend.repo.list_schedules(tx)
        return due, not_due, listed

    due, not_due, listed = backend.run(_scenario())
    assert any(row[0] == sched_id for row in due)
    assert not any(row[0] == sched_id for row in not_due)
    mine = next(s for s in listed if s["id"] == sched_id)
    assert mine["fire_count"] == 1


def test_capacity_state_is_a_keyed_set_and_folds_are_idempotent(backend):
    uid = backend.uid()
    now = 1_800_000_000.0
    service = f"contract-{uid}"

    async def _scenario():
        async with backend.repo.connection() as tx:
            await backend.repo.capacity_state_save(
                tx, service=service, in_flight=2, tokens_used=100, spent_usd=1.5, updated_at=now
            )
            await backend.repo.capacity_state_load(tx)
            # SET semantics: saving the same numbers again is a no-op, not a sum.
            await backend.repo.capacity_state_save(
                tx, service=service, in_flight=2, tokens_used=100, spent_usd=1.5, updated_at=now
            )
            after_save = {r[0]: r for r in await backend.repo.capacity_state_load(tx)}
            first = await backend.repo.capacity_fold_usage(
                tx, dedup_key=f"fold-{uid}", service=service, tokens=5, cost_usd=0.1, now=now
            )
            replay = await backend.repo.capacity_fold_usage(
                tx, dedup_key=f"fold-{uid}", service=service, tokens=5, cost_usd=0.1, now=now
            )
            after_fold = {r[0]: r for r in await backend.repo.capacity_state_load(tx)}
        return after_save[service], first, replay, after_fold[service]

    saved, first, replay, folded = backend.run(_scenario())
    assert (saved[1], saved[2], saved[3]) == (2, 100, 1.5)
    assert (first, replay) == (True, False), "a replayed fold must be a no-op"
    assert (folded[2], folded[3]) == (105, 1.6)


def test_reaper_selects_and_orphan_requeue_releases_the_row(backend):
    uid = backend.uid()
    urn = f"urn:agent:zeroclaw:contract:{uid}"
    now = 1_800_000_000.0
    job_id = f"job-{uid}"

    async def _scenario():
        async with backend.repo.connection() as tx:
            await backend.repo.register_agent(tx, **cs.agent_fields(urn, uid, now))
            await backend.repo.submit_job(tx, **cs.job_fields(job_id, urn, now))
            await backend.repo.claim_job(
                tx,
                job_id=job_id,
                agent_urn=urn,
                now=now,
                lease_expires_at=now + 10,
                runtime="zeroclaw",
                model="m",
                provider="p",
                cost_tier="B",
            )
            await backend.repo.mark_agent_offline(tx, urn)
            await tx.commit()
            candidates = await backend.repo.select_orphan_claim_candidates(
                tx, now=now + 100, hard_cutoff=now + 100, default_cutoff=now + 100
            )
            mine = next(c for c in candidates if c[0] == job_id)
            released = await backend.repo.requeue_orphan_job(tx, mine)
            await backend.repo.mark_agents_stale(tx, [urn])
            await backend.repo.release_unstarted_jobs_for_agent(tx, urn)
            await backend.repo.cancel_jobs(tx, ids=[job_id], ended_at=now + 200)
            await tx.commit()
        return released

    released = backend.run(_scenario())
    assert released == 1
    row = _job_row(backend.repo, job_id)
    assert row["status"] == "cancelled"
    assert row["claimed_by"] is None


@pytest.mark.xfail(
    strict=True,
    reason=(
        "FB38-FINDING-1: the ABC declares usage_ledger_insert but no backend's DDL "
        "creates usage_ledger, so the reference schema raises 'no such table' and the "
        "bus drops the write (hive/bus/... logs 'usage_ledger write skipped')"
    ),
)
def test_usage_ledger_insert_is_executable_on_every_backend(backend):
    """The ABC declares ``usage_ledger_insert``; every backend must be able to run it."""

    async def _scenario():
        async with backend.repo.connection() as tx:
            await backend.repo.usage_ledger_insert(
                tx,
                provider="contract",
                model="contract-model",
                task_kind="e2e:contract",
                tokens_in=1,
                tokens_out=2,
                est_cost_usd=0.01,
                outcome="ok",
            )
            await tx.commit()

    backend.run(_scenario())
