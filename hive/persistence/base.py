"""Backend-neutral Hive Mind persistence interfaces.

Follows the MNEMOS/EPIMONE convention (see
docs/analysis-mnemos-reuse-20261015.md): every storage method takes the
caller's ``Transaction`` handle as its first argument, so multi-step writes stay
atomic on one connection:

    async with repo.connection() as tx:
        await repo.patch_job_status(tx, job_id, sets={...})

Backends own their dialect; callers touch only this ABC.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol, runtime_checkable

Row = tuple[Any, ...]


@runtime_checkable
class Transaction(Protocol):
    """Backend-neutral transaction/connection handle."""

    async def execute(self, sql: str, params: Any = ...) -> Any: ...

    async def executemany(self, sql: str, seq_of_params: Any) -> Any: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...


class HiveMindRepository(ABC):
    """Storage facade used by the Hive Mind bus.

    Backend-neutral: the factory selects a backend (sqlite/oracle/db2) from the
    environment; the bus depends only on this ABC. Every method takes a ``tx``
    (see module docstring) except lifecycle (``init``/``close``/``connection``).
    """

    @abstractmethod
    async def init(self, schema: str | None = None) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    def connection(self) -> AbstractAsyncContextManager[Transaction]: ...

    # ---- events ----
    @abstractmethod
    async def emit_event(
        self,
        tx: Transaction,
        *,
        ts: float,
        kind: str,
        payload_json: str,
        agent_urn: str | None,
    ) -> None: ...

    @abstractmethod
    async def tail_events(
        self, tx: Transaction, *, since_id: int | None = None, limit: int = 100
    ) -> list[dict[str, Any]]: ...

    # ---- agents ----
    @abstractmethod
    async def register_agent(self, tx: Transaction, **fields: Any) -> None: ...

    @abstractmethod
    async def heartbeat_agent(
        self,
        tx: Transaction,
        *,
        urn: str,
        ts: float,
        status: str,
        metadata_json: str | None = None,
        metadata_patch: dict[str, Any] | None = None,
    ) -> int: ...

    @abstractmethod
    async def require_agent(self, tx: Transaction, *, urn: str) -> Row | None: ...

    @abstractmethod
    async def list_agents(self, tx: Transaction, **filters: Any) -> list[dict[str, Any]]: ...

    @abstractmethod
    async def dedup_agent_instance(
        self, tx: Transaction, *, host: str, kind: str, instance: str, keep_urn: str
    ) -> list[str]: ...

    # ---- jobs ----

    #: Whether ``list_jobs(reason_prefix=...)`` is expressed IN the backend's
    #: own SQL (case-insensitive prefix match on the job's result JSON, over
    #: the deploy-line reason fields). sqlite/postgres/mysql can; oracle and
    #: db2 cannot through their adapters, so the bus falls back to a Python
    #: filter and reports the pre-filter ``total``.
    list_jobs_sql_reason: bool = False

    @abstractmethod
    async def submit_job(self, tx: Transaction, **fields: Any) -> None: ...

    @abstractmethod
    async def claim_next_job(
        self,
        tx: Transaction,
        *,
        agent_urn: str,
        now: float,
        active_statuses: set[str],
        claim_lease_seconds: float,
        host_denylist: set[str],
        match_capabilities: Any,
        kind_aliases: Any,
        json_list: Any,
        cost_tiers: list[str],
        throttle_headroom: float,
    ) -> dict[str, Any] | None: ...

    @abstractmethod
    async def patch_job_status(
        self,
        tx: Transaction,
        job_id: str,
        *,
        sets: Mapping[str, Any],
        expected_status: str | None = None,
    ) -> int: ...

    @abstractmethod
    async def archive_and_requeue_job(
        self,
        tx: Transaction,
        *,
        job_id: str,
        actor: str,
        reason: str | None,
        archived_at: float,
        expected_status: str | None = None,
        action: str = "requeue",
    ) -> int:
        """Archive the job's result payload, then requeue the row, in ONE transaction.

        The only operation that clears ``result``: the previous admin requeue
        wrote ``result=NULL`` directly and destroyed the review evidence stored
        there (FB51). An empty result adds no history entry; a lost
        compare-and-set returns 0 and leaves no history row behind.
        """
        ...

    @abstractmethod
    async def list_job_result_history(
        self, tx: Transaction, *, job_id: str, limit: int = 50
    ) -> list[dict[str, Any]]:
        """The archived result payloads for one job, newest first (read-only)."""
        ...

    @abstractmethod
    async def get_job(self, tx: Transaction, *, job_id: str) -> Row | None: ...

    @abstractmethod
    async def list_jobs(self, tx: Transaction, **filters: Any) -> tuple[int, list[dict[str, Any]]]: ...

    @abstractmethod
    async def find_active_dedup_job(self, tx: Transaction, *, dedup_hash: str) -> Row | None: ...

    @abstractmethod
    async def jobs_exist(self, tx: Transaction, *, ids: Sequence[str]) -> set[str]: ...

    @abstractmethod
    async def agent_kind_runtimes(self, tx: Transaction) -> list[Row]: ...

    # ---- dequeue storage primitives (algorithm stays in the router) ----
    @abstractmethod
    async def agent_claim_context(self, tx: Transaction, urn: str) -> Row | None: ...

    @abstractmethod
    async def pool_occupancy(self, tx: Transaction, *, provider: str, model: str, now: float) -> list[Row]: ...

    @abstractmethod
    async def queued_candidates(
        self,
        tx: Transaction,
        *,
        agent_urn: str,
        now: float,
        limit: int,
        unsatisfied_dep_job_ids: set[str] | None = None,
    ) -> list[Row]:
        """The dequeue candidate scan (queued, plus this agent's own offered).

        ``unsatisfied_dep_job_ids`` is an ADVISORY hint (job ids whose
        ``depends_on`` are not all done) that bounds the scan; the binding
        dependency gate is the in-transaction re-check inside
        :meth:`claim_job`, which applies even when the hint is omitted.
        """
        ...

    @abstractmethod
    async def jobs_with_unsatisfied_deps(self, tx: Transaction, *, ids: Sequence[str]) -> set[str]:
        """Of ``ids``, which jobs have a ``depends_on`` entry not all ``done``?

        Fail-closed on the depends_on COLUMN: malformed JSON or a non-array
        counts as unsatisfied, and a reference to a job id that does not exist
        counts as unsatisfied (such a dep can never reach done). Only ids that
        exist in the jobs table are evaluated; an id with no row produces no
        row (callers pass candidate ids just read from the same table).
        """
        ...

    @abstractmethod
    async def completed_job_ids(self, tx: Transaction, *, ids: Sequence[str]) -> set[str]: ...

    @abstractmethod
    async def dependencies_satisfied(self, tx: Transaction, job_id: str) -> bool:
        """True when every ``depends_on`` job of ``job_id`` is status='done'."""

    @abstractmethod
    async def claim_job(
        self,
        tx: Transaction,
        *,
        job_id: str,
        agent_urn: str,
        now: float,
        lease_expires_at: float,
        runtime: str,
        model: str,
        provider: str,
        cost_tier: str,
    ) -> int: ...

    @abstractmethod
    async def job_update_context(self, tx: Transaction, job_id: str) -> Row | None:
        """PATCH pre-read: status/claim/provenance/retry/decline/kind/repo/result/submitter.

        ``submitter_urn`` is the last column so the PATCH handler can authorize a
        submitter (or orchestrator-capable agent) cancellation of its own job
        without a second query (defect 2026-10-25).
        """
        ...

    @abstractmethod
    async def job_stats_context(self, tx: Transaction, job_id: str) -> Row | None: ...

    @abstractmethod
    async def agent_spend_add(self, tx: Transaction, *, urn: str, delta: float) -> None: ...

    @abstractmethod
    async def usage_ledger_insert(self, tx: Transaction, **fields: Any) -> None: ...

    @abstractmethod
    async def insert_job_audit(self, tx: Transaction, **fields: Any) -> None: ...

    @abstractmethod
    async def job_claim_context(self, tx: Transaction, job_id: str) -> Row | None: ...

    @abstractmethod
    async def job_retry_context(self, tx: Transaction, job_id: str) -> Row | None: ...

    @abstractmethod
    async def job_release_context(self, tx: Transaction, job_id: str) -> Row | None: ...

    @abstractmethod
    async def active_orchestrator_urn(self, tx: Transaction) -> str | None: ...

    @abstractmethod
    async def active_agent_models(self, tx: Transaction) -> list[dict[str, Any]]:
        """``[{"model", "provider"}]`` for every non-offline agent.

        Feeds submit-time ``preferred_models`` admission (which model names
        some registered worker can actually serve). Deliberately narrow: two
        columns, no JSON parsing, so the check stays cheap on the hot submit
        path and every backend can answer it with one indexed SELECT.
        """
        ...

    # ---- stats reads ----
    @abstractmethod
    async def worker_stats_rows(
        self, tx: Transaction, *, kind: str | None, include_system: bool, top_n: int
    ) -> list[dict[str, Any]]: ...

    @abstractmethod
    async def cache_stats(self, tx: Transaction, *, top_n: int) -> dict[str, Any]: ...

    @abstractmethod
    async def cost_stats_rows(self, tx: Transaction, *, group_by: str, cutoff: float) -> list[dict[str, Any]]: ...

    # ---- messages ----
    @abstractmethod
    async def post_message(self, tx: Transaction, **fields: Any) -> None: ...

    @abstractmethod
    async def list_messages(self, tx: Transaction, **filters: Any) -> list[dict[str, Any]]: ...

    # ---- result cache ----
    @abstractmethod
    async def cache_lookup(self, tx: Transaction, *, cache_key: str, cutoff: float) -> dict[str, Any] | None: ...

    @abstractmethod
    async def cache_store(self, tx: Transaction, **fields: Any) -> None: ...

    @abstractmethod
    async def cache_record_hit(self, tx: Transaction, *, cache_key: str, cost_saved_usd: float, ts: float) -> None: ...

    # ---- stats + schedules ----
    @abstractmethod
    async def upsert_worker_kind_stats(self, tx: Transaction, **fields: Any) -> None: ...

    @abstractmethod
    async def create_schedule(self, tx: Transaction, **fields: Any) -> None: ...

    @abstractmethod
    async def list_schedules(self, tx: Transaction) -> list[dict[str, Any]]: ...

    # ---- reaper: agent lifecycle maintenance ----
    @abstractmethod
    async def select_stale_agent_urns(
        self, tx: Transaction, *, stale_cutoff: float, offline_cutoff: float
    ) -> list[str]: ...

    @abstractmethod
    async def mark_agents_stale(self, tx: Transaction, urns: list[str]) -> None: ...

    @abstractmethod
    async def select_dead_agent_urns(self, tx: Transaction, *, offline_cutoff: float) -> list[str]: ...

    @abstractmethod
    async def release_unstarted_jobs_for_agent(self, tx: Transaction, urn: str) -> list[str]: ...

    @abstractmethod
    async def mark_agent_offline(self, tx: Transaction, urn: str) -> None: ...

    @abstractmethod
    async def purge_offline_agents(self, tx: Transaction, *, cutoff: float) -> int: ...

    # ---- reaper: job lifecycle maintenance ----
    @abstractmethod
    async def select_orphan_claim_candidates(
        self, tx: Transaction, *, now: float, hard_cutoff: float, default_cutoff: float
    ) -> list[tuple]: ...

    @abstractmethod
    async def requeue_orphan_job(self, tx: Transaction, candidate: tuple) -> int: ...

    @abstractmethod
    async def select_due_schedules(self, tx: Transaction, *, now: float) -> list[tuple]: ...

    @abstractmethod
    async def mark_schedule_fired(self, tx: Transaction, *, sched_id: str, now: float, next_at: float) -> None: ...

    @abstractmethod
    async def select_stale_queued_job_ids(self, tx: Transaction, *, cutoff: float) -> list[str]: ...

    @abstractmethod
    async def cancel_jobs(self, tx: Transaction, ids: list[str], *, ended_at: float) -> None: ...

    @abstractmethod
    async def purge_events_before(self, tx: Transaction, *, cutoff: float) -> None: ...

    @abstractmethod
    async def select_sweepable_test_job_ids(
        self, tx: Transaction, *, statuses: list[str], patterns: tuple[str, ...], cutoff: float
    ) -> list[str]: ...

    @abstractmethod
    async def select_sweepable_terminal_job_ids(
        self, tx: Transaction, *, statuses: list[str], cutoff: float
    ) -> list[str]: ...

    # ---- capacity ledger (Phase 5) ----
    @abstractmethod
    async def capacity_state_load(self, tx: Transaction) -> list[Row]: ...

    @abstractmethod
    async def capacity_state_save(self, tx: Transaction, **fields: Any) -> None: ...

    @abstractmethod
    async def capacity_fold_usage(
        self, tx: Transaction, *, dedup_key: str, service: str, tokens: int, cost_usd: float, now: float
    ) -> bool: ...
