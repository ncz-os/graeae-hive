"""Request/response schemas (Phase 1a extraction from agent_bus.py)."""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator, model_validator


class AgentRegister(BaseModel):
    # OPEN REGISTRATION + RUNTIME→KIND ENFORCEMENT (per Kimi advisory 2026-05-23).
    # Any agent registers; identity is recorded for transparency. Kind must align
    # with runtime (prevents opencode-misregistering-as-claude). Soft fields
    # below default to "unknown" if not declared.
    #
    #   runtime          = TOOL/ORCHESTRATOR (claude-code, opencode, codex, hermes, zeroclaw, openclaw, ic-engine, mnemos, human)
    #   model            = LLM (claude-opus-4-7, grok-4.3, kimi-k2.6, deepseek-v4-pro, gpt-5.5, gemini-3.1-pro, ...)
    #   provider         = INFERENCE HOST (anthropic, xai, ngc, openai, deepseek, together, groq, gemini, local-llamacpp, ...)
    #   kind             = URN routing segment — MUST be in RUNTIME_KIND_MAP[runtime] if runtime known
    #   autonomy_level   = autonomous / confirm-risky / interactive / unknown
    runtime: str = Field("unknown", pattern=r"^[a-z][a-z0-9-]{0,31}$")
    model: str = "unknown"
    provider: str = "unknown"
    host: str
    kind: str | None = Field(
        None,
        pattern=r"^[a-z][a-z0-9-]{0,31}$",
        description="URN routing segment — defaults to runtime",
    )
    autonomy_level: str = Field("unknown", description="autonomous / confirm-risky / interactive / unknown")
    auth_method: str = Field(
        "unknown",
        description="subscription (Max plan), api (pay-per-token), free, unknown",
    )
    plan_cap_usd: float | None = None  # monthly cap; defaults from DEFAULT_PLAN_CAPS by auth_method
    pid: int | None = None
    capabilities: list[str] | None = None
    version: str | None = None
    metadata: dict | None = None
    subscription_pools: list[str] | None = Field(default_factory=list)
    # Stable per-worker-instance identity for registration dedup (2026-06-15).
    # Workers run as systemd units `zeroclaw-wss-worker@N` / `zc-worker@N` with a
    # %i identity that is STABLE across restarts (unlike pid/session_id). When a
    # worker restarts and re-registers with the same (host, kind, instance), the
    # bus REPLACES its prior row instead of minting a new uuid4 URN -> ends the
    # dashboard duplicate-registration/inflation. OPTIONAL: agents that don't send
    # `instance` (claude/codex/system/mnemos/old workers) keep the original
    # mint-a-new-URN behavior, so this is fully backward-compatible and never
    # breaks an existing agent kind. Bounded pattern: prevents pathological keys.
    instance: str | None = Field(
        None,
        pattern=r"^[A-Za-z0-9._:@-]{1,64}$",
        description="stable per-worker-instance id for register dedup",
    )


class AgentHeartbeat(BaseModel):
    urn: str
    status: str = "online"
    metadata: dict | None = None


class JobCreate(BaseModel):
    submitter_urn: str  # who is asking (user OR delegating agent)
    parent_job_id: str | None = None
    kind: str  # work type (code-edit/research/review/build/etc)
    description: str | None = None
    priority: int = 0  # higher = more urgent (default 0)
    deadline: float | None = None  # unix ts, optional SLA
    required_capabilities: list[str] | None = None  # worker must have ALL of these
    eligible_kinds: list[str] | None = None  # restrict to agent kinds; null = any
    eligible_hosts: list[str] | None = None  # restrict to agent hosts (e.g. ["cixmini"]); null = any
    pin_reason: str | None = None  # why eligible_hosts is set; omitting it makes the bus warn
    target_workspace: str | None = (
        None  # GRAEAE permanent-fix: bus-side first-class workspace (replaces worker KIND_WORKSPACE_MAP guessing)
    )
    # #10 FIX (review 2026-05-23): project tag — separate from worker capabilities.
    # 'riskyeats'/'investorclaw' are PROJECTS, not capabilities. Workers don't gain/lose
    # the ability to do work because of a project label; the label is for filter+routing.
    project: str | None = None
    max_retries: int = Field(2, ge=0, le=100, strict=True)
    zero_retry_reason: str | None = Field(None, max_length=2000)

    @model_validator(mode="after")
    def require_zero_retry_reason(self):
        if self.max_retries == 0 and not (self.zero_retry_reason or "").strip():
            raise ValueError("max_retries=0 requires a nonblank zero_retry_reason")
        return self

    # COST DISCIPLINE (per CLAUDE.md llm-usage-policy):
    max_cost_tier: str = "B"  # adaptive tier cap: A=local/NGC first (free), B=Groq/xAI overflow (paid), C=reserve. Default B=Tier A preferred, Tier B fallback when Tier A overloaded.
    preferred_providers: list[str] | None = None  # ranked preference (first match wins among tier-eligible)
    preferred_models: list[str] | None = None  # ranked model preference
    # MNEMOS provenance:
    mnemos_refs: list[str] | None = None  # mem_XXX ids — context/handoffs/related work the worker should consult
    # DAG support:
    depends_on: list[str] | None = None  # job ids that must be status='done' before this job is dequeueable
    idempotency_key: str | None = None  # forced-rerun escape: a unique value is never coalesced
    # PR-fix routing (operator 2026-06-17): forge + repo + branch/PR to check out.
    site: str | None = None  # git_sites.json key (github/gitlab-ncz/codeberg/internal)
    repo: str | None = None  # owner/name path on that site
    head_branch: str | None = None  # branch / PR head to check out
    # base_branch (PR-fix-extension 2026-10-21): optional upstream branch the
    # worker CLONES when `head_branch` doesn't exist upstream. The worker
    # creates `head_branch` locally off `base_branch`, mirroring how
    # deploy/hive-canonical-20261003 agent_bus.py carries the same key — see
    # `hive_worker.py`'s `job.get("base_branch")` reader on 4d4fc85. Optional
    # and nullable: existing callers that omit it stay backward-compatible;
    # the live HIVE_JOBS row carries NULL on pre-existing rows (the migration
    # only adds the column, never back-fills).
    base_branch: str | None = None
    pr_number: int | None = None


class JobOut(BaseModel):
    """The job response model (GET /v1/jobs, GET /v1/jobs/{id}).

    PREFERRED_* / DEPENDS_ON ROUND-TRIP FIX (fault 2026-10-17): JobCreate
    accepted ``preferred_models``/``preferred_providers``/``depends_on`` and the
    repository persisted them, but the read serializers never returned them —
    submitters' model preferences looked silently ignored and dependency
    ordering could not be verified from the API. They are DECLARED here (not
    just added to a dict) so a future serializer change that drops a field
    fails a test instead of silently regressing.

    Optional with a ``None`` default: rows written before this fix carry NULL,
    and ``preferred_*``/``depends_on`` are stored/returned as JSON arrays.
    """

    id: str
    submitter_urn: str
    parent_job_id: str | None = None
    kind: str
    description: str | None = None
    priority: int = 0
    status: str
    claimed_by: str | None = None
    started_at: float | None = None
    ended_at: float | None = None
    result: dict | None = None
    estimated_cost_usd: float | None = None
    required_capabilities: list[str] | None = None
    eligible_kinds: list[str] | None = None
    eligible_hosts: list[str] | None = None
    project: str | None = None
    max_cost_tier: str | None = None
    preferred_providers: list[str] | None = None
    preferred_models: list[str] | None = None
    depends_on: list[str] | None = None
    mnemos_refs: list[str] | None = None
    site: str | None = None
    repo: str | None = None
    head_branch: str | None = None
    base_branch: str | None = None
    # Submit-time branch admission (2026-10-25): the base_branch the worker
    # will create head_branch FROM, set only when head_branch was absent on
    # origin and base_branch existed there. NULL = nothing to create.
    branch_created_from: str | None = None
    pr_number: int | None = None
    max_retries: int | None = None
    retry_count: int | None = None
    claimed_at: float | None = None
    claim_lease_expires_at: float | None = None
    last_update_at: float | None = None
    claimed_model: str | None = None
    claimed_provider: str | None = None
    claimed_runtime: str | None = None
    claimed_cost_tier: str | None = None


class JobUpdate(BaseModel):
    status: str
    result: dict | None = None
    claimed_by: str | None = None
    # SUBMITTER CANCELLATION (2026-10-25). A claimed job could only be updated
    # by its claimant, so a submitter could not cancel or supersede its own job
    # (an orchestrator had to force-DELETE the row and kill the worker). These
    # two fields are the ONLY extra input that path accepts:
    #
    #   reason       -- why the job is being stopped; recorded on the audit row
    #                   and in the job.cancelled event, so the claimant's
    #                   operator can see who stopped it and why.
    #   cancel_actor -- the urn claiming the cancellation. It is an identity
    #                   CLAIM, authorized against the job's submitter_urn or,
    #                   for an agent registered with the 'orchestrator'
    #                   capability, cross-job authority. It never widens what a
    #                   caller may write: any other field in the same request
    #                   is refused for a non-claimant cancel.
    #
    # Both are optional and NULL for every existing caller (workers send
    # neither), so this is backward-compatible.
    reason: str | None = Field(None, max_length=2000)
    cancel_actor: str | None = Field(None, max_length=255)
    tokens_in: int | None = None  # workers SHOULD report token usage on done/failed for cost audit
    tokens_out: int | None = None
    result_mnemos_id: str | None = None  # mem_XXX id where worker stored the outcome — closes provenance loop
    # Routing re-assignment (queued/offered jobs only — use status=queued to re-route)
    eligible_kinds: list[str] | None = None
    eligible_hosts: list[str] | None = None
    # MODEL PROVENANCE (2026-10-16). Workers report the runtime/model/provider/
    # cost-tier they ACTUALLY used when an attempt starts and on lease renewal.
    # Previously these keys were silently dropped (pydantic ignores extras), so
    # the bus kept showing the agent's registration model (e.g. 'zoder')
    # forever. Optional + strip + bounded: unset/None never clobbers the row.
    claimed_model: str | None = Field(None, max_length=128)
    claimed_provider: str | None = Field(None, max_length=128)
    claimed_runtime: str | None = Field(None, max_length=128)
    claimed_cost_tier: str | None = Field(None, max_length=128)

    @field_validator("reason", "cancel_actor", mode="before")
    @classmethod
    def _strip_cancel_fields(cls, v):
        if isinstance(v, str):
            v = v.strip()
            return v or None
        return v

    @field_validator(
        "claimed_model",
        "claimed_provider",
        "claimed_runtime",
        "claimed_cost_tier",
        mode="before",
    )
    @classmethod
    def _strip_provenance(cls, v):
        if isinstance(v, str):
            v = v.strip()
            return v or None
        return v


class ScheduleCreate(BaseModel):
    name: str
    interval_seconds: int = Field(
        ...,
        ge=60,
        le=86400 * 30,
        description="60s minimum (avoid hot-loop); 30d maximum",
    )
    job_template: dict  # full JobCreate body that will be submitted each tick
    enabled: bool = True


class MessagePublish(BaseModel):
    from_urn: str
    to_urn: str | None = None
    in_reply_to: str | None = None
    topic: str
    payload: dict


class RequeueRequest(BaseModel):
    job_ids: list[str]
    reason: str = "manual-requeue"
    # Operator override (2026-10-26): a job with no retry budget left (max_retries 0,
    # or retry_count >= max_retries) is only requeued when the caller sets this AND
    # gives a non-empty reason. Every override is logged and emitted as an event.
    override_max_retries: bool = False
    override_reason: str | None = None


class PurgeRequest(BaseModel):
    job_ids: list[str]
    reason: str = "doctor-dlq-remove"


class JobPurgeRequest(BaseModel):
    kind_prefix: str | None = None
    status: str | None = None
    statuses: list[str] | None = None
    description_contains: str | None = None
    submitter_urn: str | None = None
    older_than_seconds: float | None = None
    ids: list[str] | None = None
    include_active: bool = False
    reason: str = "housekeeping-purge"


class ReclassifyRequest(BaseModel):
    job_ids: list[str]
    status: str
    reason: str = "doctor-dlq-reclassify"
    # Same operator override as RequeueRequest; only consulted for status="queued".
    override_max_retries: bool = False
    override_reason: str | None = None
