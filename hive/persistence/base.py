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
    async def patch_job_status(self, tx: Transaction, job_id: str, *, sets: Mapping[str, Any]) -> int: ...

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
    async def pool_occupancy(
        self, tx: Transaction, *, provider: str, model: str, now: float
    ) -> list[Row]: ...

    @abstractmethod
    async def queued_candidates(
        self, tx: Transaction, *, agent_urn: str, now: float, limit: int
    ) -> list[Row]: ...

    @abstractmethod
    async def completed_job_ids(self, tx: Transaction, *, ids: Sequence[str]) -> set[str]: ...

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
