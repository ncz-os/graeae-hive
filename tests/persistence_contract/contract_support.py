"""Shared support for the persistence contract suite.

Two things live here:

* ``run_workload`` — one async scenario that drives a broad slice of the
  ``HiveMindRepository`` ABC (agents, jobs, claim/CAS, events, messages, cache,
  stats, schedules, capacity ledger, reapers). It is the single definition of
  "the behaviour every backend must agree on" and it is also the SQL sample the
  dialect check translates.
* the SQL-dialect detector — ``capture_sql`` records every statement the
  reference backend emits, ``translate`` runs it through a backend's adapter,
  and ``find_violations`` reports constructs that only sqlite understands.
"""

from __future__ import annotations

import json
import re

# --------------------------------------------------------------------------- #
# field builders (the exact kwarg names the ABC implementations read)
# --------------------------------------------------------------------------- #


def agent_fields(urn: str, uid: str, now: float) -> dict:
    return {
        "urn": urn,
        "kind": "zeroclaw",
        "runtime": "zeroclaw",
        "model": "contract-model",
        "provider": "contract-provider",
        "cost_tier": "B",
        "autonomy_level": "interactive",
        "auth_method": "api",
        "plan_cap_usd": 10.0,
        "subscription_pools_json": None,
        "host": "contract",
        "session_id": uid,
        "pid": 4242,
        "capabilities_json": json.dumps(["code"]),
        "version": "contract-1",
        "started_at": now,
        "last_heartbeat": now,
        "metadata_json": json.dumps({"suite": "persistence_contract"}),
    }


def job_fields(job_id: str, submitter: str, now: float, **extra) -> dict:
    fields = {
        "id": job_id,
        "submitter_urn": submitter,
        "kind": "e2e:contract",
        "description": "persistence contract",
        "priority": 1,
        "started_at": now,
    }
    fields.update(extra)
    return fields


# --------------------------------------------------------------------------- #
# the behavioural workload
# --------------------------------------------------------------------------- #


async def run_workload(repo, uid: str) -> dict:
    """Drive a broad slice of the ABC; return the ids it created."""
    now = 1_800_000_000.0
    urn = f"urn:agent:zeroclaw:contract:{uid}"
    job_id = f"job-{uid}"
    dep_id = f"dep-{uid}"
    sched_id = f"sched-{uid}"
    cache_key = f"cache-{uid}"
    msg_id = f"msg-{uid}"

    async with repo.connection() as tx:
        await repo.register_agent(tx, **agent_fields(urn, uid, now))
        await repo.emit_event(tx, ts=now, kind="job.queued", payload_json="{}", agent_urn=urn)
        await repo.heartbeat_agent(tx, urn=urn, ts=now, status="idle", metadata_patch={"load": 0})
        await repo.require_agent(tx, urn=urn)
        await repo.list_agents(tx, include_offline=True)
        await repo.dedup_agent_instance(tx, host="contract", kind="zeroclaw", instance=uid, keep_urn=urn)
        await repo.agent_spend_add(tx, urn=urn, delta=0.25)
        await repo.agent_claim_context(tx, urn)
        await repo.active_agent_models(tx)

        await repo.submit_job(tx, **job_fields(dep_id, urn, now))
        await repo.submit_job(
            tx,
            **job_fields(job_id, urn, now, dedup_hash=f"dedup-{uid}"),
        )
        await repo.find_active_dedup_job(tx, dedup_hash=f"dedup-{uid}")
        await repo.jobs_exist(tx, ids=[job_id])
        await repo.job_update_context(tx, job_id)
        await repo.job_claim_context(tx, job_id)
        await repo.job_retry_context(tx, job_id)
        await repo.job_release_context(tx, job_id)
        await repo.job_stats_context(tx, job_id)
        await repo.queued_candidates(tx, agent_urn=urn, now=now, limit=10)
        await repo.dependencies_satisfied(tx, job_id)
        await repo.jobs_with_unsatisfied_deps(tx, ids=[job_id])
        await repo.completed_job_ids(tx, ids=[dep_id])
        await repo.claim_job(
            tx,
            job_id=job_id,
            agent_urn=urn,
            now=now,
            lease_expires_at=now + 60.0,
            runtime="zeroclaw",
            model="contract-model",
            provider="contract-provider",
            cost_tier="B",
        )
        await repo.patch_job_status(
            tx,
            job_id,
            sets={"status": "done", "ended_at": now, "last_update_at": now},
            expected_status="claimed",
        )
        await repo.get_job(tx, job_id=job_id)
        await repo.list_jobs(tx, limit=10)
        await repo.list_jobs(tx, status="queued", agent_urn=urn, since=0.0, reason_prefix="no_repo")
        await repo.insert_job_audit(
            tx,
            job_id=job_id,
            ts=now,
            actor_urn=urn,
            old_status="claimed",
            new_status="done",
            old_claimed_by=urn,
            new_claimed_by=None,
            patch_json="{}",
        )
        await repo.select_stale_agent_urns(tx, stale_cutoff=now + 1, offline_cutoff=0.0)
        await repo.select_dead_agent_urns(tx, offline_cutoff=0.0)
        await repo.select_orphan_claim_candidates(tx, now=now, hard_cutoff=0.0, default_cutoff=0.0)
        await repo.select_stale_queued_job_ids(tx, cutoff=now + 1)
        await repo.cancel_jobs(tx, ids=[dep_id], ended_at=now)
        await repo.purge_events_before(tx, cutoff=now + 1)
        await repo.select_sweepable_terminal_job_ids(tx, statuses=["done"], cutoff=now + 1)
        await repo.select_sweepable_test_job_ids(tx, statuses=["done"], patterns=("e2e:%",), cutoff=now + 1)
        await tx.commit()

    async with repo.connection() as tx:
        await repo.post_message(
            tx,
            id=msg_id,
            from_urn=urn,
            to_urn=None,
            in_reply_to=None,
            topic="orchestrator.coordination",
            payload_json=json.dumps({"status": "contract"}),
            ts=now,
        )
        await repo.list_messages(tx, topic="orchestrator.*", limit=5)
        await repo.cache_store(
            tx,
            cache_key=cache_key,
            result_json=json.dumps({"exit_code": 0}),
            source_job_id=job_id,
            model="contract-model",
            provider="contract-provider",
            cached_at=now,
        )
        await repo.cache_lookup(tx, cache_key=cache_key, cutoff=0.0)
        await repo.cache_record_hit(tx, cache_key=cache_key, cost_saved_usd=0.5, ts=now)
        await repo.cache_stats(tx, top_n=5)
        await repo.upsert_worker_kind_stats(
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
        await repo.worker_stats_rows(tx, kind=None, include_system=False, top_n=5)
        await repo.create_schedule(
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
        await repo.list_schedules(tx)
        await repo.select_due_schedules(tx, now=now)
        await repo.mark_schedule_fired(tx, sched_id=sched_id, now=now, next_at=now + 3600)
        await repo.capacity_state_save(
            tx, service="contract", in_flight=1, tokens_used=10, spent_usd=0.5, updated_at=now
        )
        await repo.capacity_state_load(tx)
        await repo.capacity_fold_usage(tx, dedup_key=f"fold-{uid}", service="contract", tokens=5, cost_usd=0.1, now=now)
        await repo.tail_events(tx, since_id=0, limit=10)
        await tx.commit()

    return {"urn": urn, "job_id": job_id, "dep_id": dep_id, "cache_key": cache_key, "sched_id": sched_id}


# --------------------------------------------------------------------------- #
# SQL capture + dialect detection
# --------------------------------------------------------------------------- #


class RecordingTransaction:
    """A transaction proxy that records every statement, then delegates."""

    def __init__(self, tx, sink: list[str]) -> None:
        self._tx = tx
        self.statements = sink

    # The drivers' execute/executemany return a cursor-like object that is BOTH
    # awaitable and an async context manager, so the proxy records and hands the
    # real object straight back instead of wrapping it.
    def execute(self, sql, params=None):
        self.statements.append((sql, () if params is None else tuple(params)))
        return self._tx.execute(sql, () if params is None else params)

    def executemany(self, sql, seq_of_params=None):
        rows = list(seq_of_params or [])
        self.statements.append((sql, tuple(rows[0]) if rows else ()))
        return self._tx.executemany(sql, rows)

    async def executescript(self, script):
        self.statements.append((script, ()))
        return await self._tx.executescript(script)

    async def commit(self):
        return await self._tx.commit()

    async def rollback(self):
        return await self._tx.rollback()

    def __getattr__(self, name):
        return getattr(self._tx, name)


class RecordingRepository:
    """Wrap a repository so every ``connection()`` hands out a recorder."""

    def __init__(self, repo) -> None:
        self._repo = repo
        self.statements: list[tuple[str, tuple]] = []

    def connection(self):
        recorder = RecordingTransaction
        sink = self.statements

        class _Ctx:
            async def __aenter__(self_inner):
                self_inner._cm = self._repo.connection()
                tx = await self_inner._cm.__aenter__()
                self_inner.tx = recorder(tx, sink)
                return self_inner.tx

            async def __aexit__(self_inner, *exc):
                return await self_inner._cm.__aexit__(*exc)

        return _Ctx()

    def __getattr__(self, name):
        return getattr(self._repo, name)


def capture_sql(repo, uid: str) -> list[tuple[str, tuple]]:
    """Every ``(sql, params)`` the reference repository emits for the workload.

    The bound params matter: db2's adapter resolves ``LIMIT ?`` from the value,
    so translating with empty params would fail for the wrong reason.
    """
    recording = RecordingRepository(repo)
    import asyncio

    asyncio.run(run_workload(recording, uid))
    return list(recording.statements)


#: Constructs no non-sqlite backend understands. Each maps to the reason.
FORBIDDEN = {
    "json_extract": "sqlite's json_extract(); the shared SQL must use JSON_VALUE",
    "INSERT OR ": "sqlite's INSERT OR REPLACE/IGNORE upsert spelling",
    "AUTOINCREMENT": "sqlite identity syntax; other backends use sequences/identity",
    "PRAGMA ": "sqlite-only pragma",
    "datetime(": "sqlite date function; other backends need their own expression",
    "strftime(": "sqlite date function",
    "sqlite_master": "sqlite catalog table",
}

#: Extra constructs a particular backend cannot express, even though postgres or
#: the adapter's own rewrite may. ``ON CONFLICT``/``excluded.`` are native
#: postgres, so only the backends whose upsert is a MERGE flag them.
FORBIDDEN_BY_BACKEND = {
    "mysql": {"ON CONFLICT", "excluded."},
    "oracle": {"ON CONFLICT", "excluded."},
    "db2": {"ON CONFLICT", "excluded."},
}


def _strip_literals(sql: str) -> str:
    """Drop string literals so a '?' inside 'text' is not read as a placeholder."""
    out = []
    in_single = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'":
            if in_single and i + 1 < len(sql) and sql[i + 1] == "'":
                i += 2
                continue
            in_single = not in_single
            out.append(" ")
        elif not in_single:
            out.append(ch)
        i += 1
    return "".join(out)


def translate(backend: str, sql: str, params=()):
    """Run ``sql`` through the backend's own adapter (None = not translatable)."""
    if backend == "postgres":
        from hive.persistence.dialect import to_numbered

        return to_numbered(sql), params
    if backend == "mysql":
        from hive.persistence.dialect import mysql_upsert, to_format

        return mysql_upsert(to_format(sql)), params
    if backend == "oracle":
        from hive.persistence.oracle import _translate_sql

        return _translate_sql(sql, params)
    if backend == "db2":
        from hive.persistence.db2 import _adapt_sql

        return _adapt_sql(sql, params)
    raise AssertionError(f"no translation adapter for {backend!r}")


#: The placeholder style each backend must END UP with (None = same as sqlite).
PLACEHOLDER = {"postgres": "$", "mysql": "%s", "oracle": ":", "db2": "?"}


UNIXEPOCH_RE = re.compile(r"datetime\(\s*(\w+)\s*,\s*'unixepoch'\s*\)", re.IGNORECASE)


def dialect_gaps(sql: str) -> set[str]:
    """The sqlite-only date expressions a backend's adapter did NOT rewrite.

    Oracle rewrites ``datetime(col,'unixepoch')`` to TO_CHAR/NUMTODSINTERVAL, so
    it reports none; postgres, mysql and db2 leave the expression in place.
    """
    return {f"datetime({m.group(1)})" for m in UNIXEPOCH_RE.finditer(sql)}


#: ``IS <bind>``: sqlite accepts ``x IS ?`` as a null-safe equality; every
#: other engine only allows NULL/TRUE/FALSE/UNKNOWN/DISTINCT FROM after IS
#: (Oracle: ORA-00908, the 2026-11-01 reclassify 500).
IS_BIND_RE = re.compile(r"\bIS\s+(?:NOT\s+)?(?:\?|:\d+|\$\d+|%s)", re.IGNORECASE)

#: Engines whose ``jobs.result`` is a LOB: a LOB cannot be an operand of
#: ``=``/``<>``/``IS [NOT] DISTINCT FROM`` (Oracle ORA-00932, Db2 SQL0401N).
LOB_RESULT_BACKENDS = {"oracle", "db2"}
_RESULT_COMPARE_RE = re.compile(r"\bresult\s*(?:=|<>|!=|<=>)|\bresult\s+IS\s+(?:NOT\s+)?DISTINCT\s+FROM", re.IGNORECASE)

#: Null-safe operators that exist only in some engines.
NULL_SAFE_FORBIDDEN = {
    "postgres": ("<=>",),
    "db2": ("<=>",),
    "oracle": ("<=>", "DISTINCT FROM"),
    "mysql": ("DISTINCT FROM",),
}


def predicate_violations(backend: str, sql: str) -> list[str]:
    """Comparison-grammar problems in a translated statement's predicates."""
    problems = []
    body = _strip_literals(sql)
    if backend != "sqlite" and IS_BIND_RE.search(body):
        problems.append(f"IS <bind>: sqlite-only null-safe compare, {backend} cannot parse it")
    for token in NULL_SAFE_FORBIDDEN.get(backend, ()):
        if token.lower() in body.lower():
            problems.append(f"{token}: no such null-safe operator on {backend}")
    if backend in LOB_RESULT_BACKENDS:
        # Only the predicate side: ``SET result=?`` is an assignment, not a compare.
        parts = re.split(r"\bWHERE\b", body, maxsplit=1, flags=re.IGNORECASE)
        if len(parts) == 2 and _RESULT_COMPARE_RE.search(parts[1]):
            problems.append(f"result compared in SQL: a LOB on {backend} cannot be compared")
    return problems


def find_violations(backend: str, sql: str) -> list[str]:
    """Backend-specific problems in ``sql`` (already translated, or raw)."""
    problems = predicate_violations(backend, sql)
    body = _strip_literals(sql)
    lowered = body.lower()
    tokens = dict(FORBIDDEN)
    for token in FORBIDDEN_BY_BACKEND.get(backend, ()):
        tokens[token] = f"not expressible on {backend} (its upsert/lock form differs)"
    for token, why in tokens.items():
        if token.lower() in lowered:
            problems.append(f"{token}: {why}")
    style = PLACEHOLDER.get(backend)
    if style and style != "?" and "?" in body.rstrip():
        # to_format escapes a literal % as %%, so a surviving '?' is a real bind.
        problems.append(f"? placeholder survived the {style} conversion")
    return problems
