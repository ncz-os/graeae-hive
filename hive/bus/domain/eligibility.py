"""Eligibility + routing-kind domain (Phase 1a extraction from agent_bus.py).

Worker authorization predicates: which agent may take which job, by kind
affinity, capability match, host scope, cost tier, and provider-as-capability
gate. Pure functions over dicts — no FastAPI, no DB.
"""

from __future__ import annotations

import os
from typing import Any

import queue_logic
from hive.core.models import COST_TIERS

RUNTIME_KIND_MAP: dict[str, set[str]] = {
    "claude-code": {"claude", "claude-code"},
    "claude-cli": {"claude", "claude-code"},
    "opencode": {"opencode"},  # opencode CAN run any model — kind=opencode
    "opencode-cli": {"opencode"},
    "codex": {"codex"},
    "codex-cli": {"codex"},
    "hermes": {"hermes"},
    "zeroclaw": {"zeroclaw"},  # zeroclaw is its own kind; does not alias codex/opencode/claude
    "openclaw": {"openclaw"},
    "ic-engine": {"ic-engine"},
    "mnemos": {"mnemos"},
    "human": {"human"},
    "claude": {"claude"},
    "system": {"system"},  # fleet hosts (ARGOS/TYPHON/HYDRA/MEDUSA/CERBERUS/PROTEUS/cixmini)
    "doctor": {"doctor"},  # PYTHIA zeroclaw doctor — triage authority + DB access
    "dream-walker": {"dream-walker"},
    "unknown": {"unknown"},
}
# INVARIANT (locked 2026-10-17 by
# test_alias_table_does_not_bridge_doctor_to_executor_kinds and
# test_runtime_kind_map_invariant_fires_when_executor_aliases_doctor):
# "doctor" MUST NOT appear in any executor runtime's alias set. If a future
# edit adds it (e.g. for a "doctor-zc" runtime that runs codex dispatches),
# a normal worker job with eligible_kinds=["zeroclaw"] would alias through
# to a doctor-kind agent -- exactly the live claim/release churn this
# module fixes (reproduced 2026-10-17, fix:singularity-shell-about-graphic).
# Doctor scope is enforced at _doctor_scope_allowed / job_agent_eligible,
# NOT via the alias table; the alias table must keep doctor isolated so the
# alias-set match in job_agent_eligible's non-doctor path cannot re-introduce
# the leak. A violating source edit raises at import time so the bus cannot
# come up with a bridged alias table.
_DOCTOR_EXECUTOR_RUNTIMES = frozenset({
    "zeroclaw", "codex", "codex-cli", "opencode", "opencode-cli",
    "claude", "claude-code", "claude-cli",
})
for _rt in _DOCTOR_EXECUTOR_RUNTIMES:
    if "doctor" in RUNTIME_KIND_MAP.get(_rt, set()):
        raise RuntimeError(
            f"RUNTIME_KIND_MAP invariant violated: executor runtime {_rt!r} "
            "aliases include 'doctor' -- this would re-leak normal worker "
            "jobs to a doctor agent (live incident 2026-10-17, "
            "fix:singularity-shell-about-graphic). Move 'doctor' out of "
            "executor alias sets; doctor scope is enforced in "
            "_doctor_scope_allowed, not via the alias table."
        )
del _rt


NARROW_HOSTS = {"cixmini", "bigpi", "clawpi", "zeropi"}


NARROW_ALLOWLIST = ("cixmini-os:", "ncz-os-", "fleet-infra:")


_HOLD_HOSTS = {
    "codex-manual-rescue",
    "rescue-guard-hold",
    "completion-repair-hold",
    "zcgate-release-sink",
}


KIND_KIND_AFFINITY: dict[str, list[str]] = {
    # Dream-walker analytics jobs are handled by the zeropi dream-walker
    # runtime. They do not need a git workspace and must not be claimed by
    # generic code workers that fail preflight with no_workspace_for_kind.
    "dream-walker:": ["dream-walker"],
}


KIND_WORKSPACE_CAPABILITY: dict[str, str] = {
    # Linux-compile jobs (rust cargo etc) -> only workers advertising linux-compile
    # (ULTRA in-VM compile worker). Keeps compile OFF the macOS native authoring
    # workers (which would fail to build linux targets). 2026-06-12.
    "build:zeroclaw": "hard:linux-compile",
    "zeroclaw:": "hard:linux-compile",
    "ncz-os-zeroclaw:": "hard:linux-compile",
    # zoder (ncz-os rust workspace, sibling to zeroclaw) -> linux-compile hosts only
    "zoder:": "hard:linux-compile",
    "ncz-os-zoder:": "hard:linux-compile",
    "compile:": "hard:linux-compile",
    # mnemos rust hot-path jobs -> rust+CI hosts only (cerberus/hydra/achilles).
    # Most mnemos work has NO such requirement and stays claimable by any host;
    # tag a job build:mnemos-rust / mnemos-rust: / compile:mnemos to gate it. 2026-06-21.
    "build:mnemos-rust": "hard:linux-compile",
    "mnemos-rust:": "hard:linux-compile",
    "compile:mnemos": "hard:linux-compile",
    "test:provider-bench-1-readme-fix-deepseek-pro": "workspace-riskybiz",
    "test:provider-bench-2-typo-deepseek-pro": "workspace-riskybiz",
    "test:provider-bench-": "workspace-riskybiz",
    "triage:test:provider-bench-": "workspace-riskybiz",
    "riskybiz:p2-per-page-last-updated-and-sitemap": "workspace-riskybiz",
    "riskybiz:": "workspace-riskybiz",
}


HEAVY_REPO_KIND_PREFIXES = ("zeroclaw:", "ncz-os-zeroclaw:")


# ── Doctor scope (2026-10-17) ──────────────────────────────────────────────
# The doctor registers with kind "doctor". The bus matcher is kind-filtered
# only by eligible_kinds, so a normal worker job (eligible_kinds
# ["zeroclaw"], or the wildcard "*") matched the doctor too -- reproduced live
# 2026-10-17: the doctor claimed fix:singularity-shell-about-graphic and had to
# release it back (claim/release churn). The doctor is a triage brain, not an
# executor, so scope it here at the source instead.
DOCTOR_KIND = "doctor"
DOCTOR_JOB_KIND_PREFIXES = ("triage:", "doctor:")
# The doctor's own codex dispatches are round-tripped by the zero-triage path
# (zeroclaw_doctor.py consumes them), not executed by the triage brain itself.
DOCTOR_JOB_KIND_EXCLUDE_PREFIXES = ("doctor:codex-fix:",)


THROTTLE_HEADROOM = 0.85  # at >=85% of plan cap, prefer non-subscription workers for tier-B/C jobs


LLM_CAP_GATE = os.environ.get("HIVE_LLM_CAP_GATE", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)


def _norm_str(value: Any) -> str:
    return value.lower() if isinstance(value, str) else ""


def _norm_str_set(values: Any) -> set[str]:
    return {_norm_str(v) for v in (values or []) if _norm_str(v)}


def agent_kind_aliases(kind: str, runtime: str | None) -> set[str]:
    kind = _norm_str(kind)
    runtime = _norm_str(runtime)
    aliases = {kind}
    if runtime:
        aliases.add(runtime)
        aliases.update(RUNTIME_KIND_MAP.get(runtime, set()))
    for rt, kinds in RUNTIME_KIND_MAP.items():
        if kind in kinds:
            aliases.add(rt)
            aliases.update(kinds)
    # ALL-EXECUTORS-ELIGIBLE union REMOVED 2026-09-15. It existed so a job
    # marked for one executor kind (claude/codex/zeroclaw/opencode) was
    # claimable by any of them, on the premise that "they differ only at
    # DISPATCH (zeroclaw routes per-model by complexity via KNEMON)" -- true
    # under the old zc-gateway/zeroclaw_wss_worker_v2.py architecture, where
    # KNEMON picked the real model server-side regardless of which agent
    # claimed the job. hive_worker.py (the fleet-wide replacement worker,
    # deployed to every host this session) does NOT call KNEMON at all --
    # WORKER_KIND directly selects the execution tool (`zoder loop` for
    # zeroclaw, `codex exec` for codex). With the union still active, a
    # codex-kind worker could claim a job with eligible_kinds=["zeroclaw"]
    # and literally run it via `codex exec` instead of zoder -- reproduced
    # live 2026-09-15 (TYDEUS's codex worker claimed + ran a zeroclaw smoke
    # test job). Kind is strict again; a job's eligible_kinds now means what
    # it says.
    # Doctor isolation (2026-10-17, source fix for live incident
    # fix:singularity-shell-about-graphic): a doctor agent must never carry
    # executor aliases. The kind/runtime/alias-set path above is purely
    # data-driven from RUNTIME_KIND_MAP -- if a future edit ever made
    # "doctor" an alias of an executor runtime, aliases would silently pick
    # it up. Drop executor aliases for doctor agents so the alias-set match
    # in job_agent_eligible cannot bridge the two worlds. The actual
    # doctor-scope matching lives in _doctor_scope_allowed /
    # job_agent_eligible's doctor branch; this strip is defence-in-depth so
    # the alias set alone cannot trigger an executor-eligible_kinds match.
    if kind == DOCTOR_KIND or runtime == DOCTOR_KIND:
        aliases -= _DOCTOR_EXECUTOR_ALIASES
    aliases.discard("")
    return aliases


# Aliases that, if present in agent_kind_aliases for a doctor agent, would
# let a normal worker job with that exact eligible_kinds entry match the
# doctor by alias-set intersection (job_agent_eligible's non-doctor path).
# We strip them for any doctor agent so the alias-set alone can never
# bridge doctor<->executor. Locked at import time by the RUNTIME_KIND_MAP
# invariant check above.
_DOCTOR_EXECUTOR_ALIASES = frozenset({
    "zeroclaw", "codex", "codex-cli", "opencode", "opencode-cli",
    "claude", "claude-code", "claude-cli",
})


def workspace_capability_for_kind(kind: str) -> str | None:
    kind = _norm_str(kind)
    for prefix, capability in KIND_WORKSPACE_CAPABILITY.items():
        if kind.startswith(prefix):
            return capability
    return None


def kind_affinity_for_kind(kind: str) -> list[str] | None:
    kind = _norm_str(kind)
    for prefix, kinds in KIND_KIND_AFFINITY.items():
        if kind.startswith(prefix):
            return kinds
    return None


def heavy_repo_required_for_kind(kind: str) -> bool:
    kind = _norm_str(kind)
    return any(kind.startswith(prefix) for prefix in HEAVY_REPO_KIND_PREFIXES)


def agent_is_doctor(agent: dict[str, Any]) -> bool:
    """True when this agent is a triage doctor (kind or runtime == 'doctor')."""
    for key in ("kind", "runtime"):
        if _norm_str(agent.get(key)) == DOCTOR_KIND:
            return True
    return False


def _doctor_scope_allowed(job: dict[str, Any], kinds: set[str]) -> tuple[bool, str]:
    """Doctor agents may only take triage:* / doctor:* work.

    Restriction (2026-10-17 source fix, live incident
    fix:singularity-shell-about-graphic, eligible_kinds=['zeroclaw']):
    doctor agents are triage brains, not executors. The bus matcher used to
    offer them any normal worker job because job_agent_eligible filtered on
    eligible_kinds alone. We now enforce the doctor scope explicitly here:

      1. Explicit opt-in: a job whose eligible_kinds lists 'doctor' is in
         scope regardless of its kind prefix -- UNLESS the kind is on
         DOCTOR_JOB_KIND_EXCLUDE_PREFIXES, which is always excluded even
         with an explicit opt-in (see rule 5 below).
      2. Otherwise the job kind must start with one of DOCTOR_JOB_KIND_PREFIXES
         ("triage:", "doctor:").
      3. The wildcard "*" in eligible_kinds is NEVER an opt-in for a doctor
         (that is exactly what leaked worker jobs to the doctor in the first
         place). It is treated as if it were not present.
      4. Executor aliases in eligible_kinds ("zeroclaw", "codex", ...) are
         NEVER an opt-in for a doctor -- they describe executor capabilities,
         not doctor ones. They are stripped from the kinds set before the
         opt-in check, so a job listing ['zeroclaw'] cannot claim to opt in
         just because the doctor happens to be one of many kinds.
      5. DOCTOR_JOB_KIND_EXCLUDE_PREFIXES ("doctor:codex-fix:") is ALWAYS
         excluded -- even with an explicit 'doctor' opt-in. Those dispatches
         are submitted by the doctor itself (action_dispatch_codex_fix,
         eligible_kinds=['zeroclaw']) and round-tripped by zero-triage
         (zeroclaw_doctor.py consumes them via /v1/jobs/next); the doctor
         dispatches them but never executes one. The exclude check therefore
         runs BEFORE the opt-in check, so a 'doctor:codex-fix:*' job with
         eligible_kinds=['doctor'] is rejected, not opted in.
    """
    j_kind = _norm_str(job.get("kind"))

    # The exclude check ALWAYS runs -- before the opt-in check -- so a job
    # whose kind is 'doctor:codex-fix:*' (a doctor-dispatched codex-fix job)
    # cannot be claimed by the doctor regardless of whether eligible_kinds
    # lists 'doctor'. Those jobs are submitted with eligible_kinds=['zeroclaw']
    # and consumed by the zeroclaw worker via the zero-triage round trip;
    # the doctor dispatching them is the only legitimate path and it goes
    # straight to the worker -- the doctor never executes one itself.
    if j_kind.startswith(DOCTOR_JOB_KIND_EXCLUDE_PREFIXES):
        return (
            False,
            f"doctor out of scope for excluded kind {j_kind!r} (codex-fix "
            "dispatch -- round-tripped by zero-triage, not executed by the "
            "doctor; the exclude applies even with an explicit 'doctor' "
            "opt-in in eligible_kinds)",
        )

    # Strip executor aliases and the wildcard from the kinds set: none of
    # those are opt-ins for a doctor agent. Only a literal "doctor" entry
    # in eligible_kinds is. This is the line that closes the
    # fix:singularity-shell-about-graphic leak: a job whose only eligible
    # kind is "zeroclaw" (or "*") cannot pass the opt-in check below.
    opt_in_kinds = kinds - DOCTOR_OPT_IN_EXCLUDE_ALIASES

    if DOCTOR_KIND in opt_in_kinds:
        return True, "eligible"
    if j_kind.startswith(DOCTOR_JOB_KIND_PREFIXES):
        return True, "eligible"
    return (
        False,
        f"doctor is out of scope for kind {j_kind!r} (eligible only for "
        f"{list(DOCTOR_JOB_KIND_PREFIXES)} kinds, or jobs explicitly listing "
        f"'doctor' in eligible_kinds; '*' and executor aliases are not opt-ins)",
    )


# Aliases in a job's eligible_kinds that are NEVER an opt-in for a doctor
# agent. The wildcard "*" is the most important (any-worker match); the
# executor aliases are defensive: even if a future job author wrote
# eligible_kinds=["zeroclaw", "doctor"] to mean "either the zeroclaw worker
# or the doctor", the doctor must look at the job kind to decide scope,
# not at the alias. With these stripped, the only path to opt in is a
# literal "doctor" entry alongside a job kind that the doctor owns.
DOCTOR_OPT_IN_EXCLUDE_ALIASES = frozenset(
    {"*", "zeroclaw", "codex", "codex-cli", "opencode", "opencode-cli",
     "claude", "claude-code", "claude-cli", "openclaw", "ic-engine",
     "hermes", "mnemos", "human", "system", "dream-walker", "unknown"}
)


def job_agent_eligible(job: dict[str, Any], agent: dict[str, Any]) -> tuple[bool, str]:
    """Decide whether `agent` may claim `job`.

    For non-doctor agents, the matching path is the legacy "filter on
    eligible_kinds + kind_affinity + capabilities + cost tier" flow.

    For doctor agents, the doctor scope (triage:* / doctor:* minus
    doctor:codex-fix:*, or jobs that explicitly opt in with 'doctor' in
    eligible_kinds) is the AUTHORITY. _doctor_scope_allowed runs the
    same logic against the job's kind and eligible_kinds, and rejects
    the doctor when '*' or any executor alias is the only opt-in. Once a
    job has cleared doctor scope, the rest of the function falls through
    normally so a triage job's required_capabilities, eligible_hosts, and
    cost_tier are still honoured.
    """
    agent_host = _norm_str(agent.get("host"))
    agent_caps = _norm_str_set(agent.get("capabilities"))
    eligible_aliases = _norm_str_set(agent.get("eligible_aliases"))
    j_kind = _norm_str(job.get("kind"))
    kinds = _norm_str_set(job.get("eligible_kinds"))

    # ── Doctor scope branch (2026-10-17 source fix) ─────────────────────────
    # The doctor is a triage brain, not an executor. We check its scope
    # before the kind_affinity / executor-alias gates below: a doctor
    # agent whose kind/runtime is 'doctor' must never pass the function
    # because it happens to alias-match ['zeroclaw'] or ['*'].
    if agent_is_doctor(agent):
        doctor_ok, doctor_reason = _doctor_scope_allowed(job, kinds)
        if not doctor_ok:
            # Clear, auditable reason -- the bus endpoint surfaces this
            # to the worker so the doctor can log + skip instead of
            # claim/release-churning.
            return False, doctor_reason
        # In scope for the doctor: continue with capability / host / cost
        # tier checks below; skip the executor-eligible_kinds match
        # (which would reject {'doctor'} on a triage:* job that does not
        # list zeroclaw/codex/etc in eligible_kinds).
    else:
        # Non-doctor path: legacy executor matching. '*' short-circuits
        # the eligible_kinds gate; otherwise the agent's alias set must
        # intersect the job's eligible_kinds.
        if kinds and "*" not in kinds and not kinds.intersection(eligible_aliases):
            return (
                False,
                f"agent kind aliases={sorted(eligible_aliases)!r} not in eligible_kinds={sorted(kinds)}",
            )

    if agent_host in NARROW_HOSTS and not j_kind.startswith(NARROW_ALLOWLIST):
        return False, f"narrow host {agent_host} not allowed for kind {j_kind}"

    kind_affinity = kind_affinity_for_kind(j_kind)
    if kind_affinity and not _norm_str_set(kind_affinity).intersection(eligible_aliases):
        return (
            False,
            f"agent kind aliases={sorted(eligible_aliases)!r} do not satisfy kind affinity={kind_affinity!r}",
        )

    hosts = _norm_str_set(job.get("eligible_hosts"))
    hosts = {h for h in hosts if h not in _HOLD_HOSTS}
    if hosts and "*" not in hosts and agent_host not in hosts:
        return False, f"agent host={agent_host!r} not in eligible_hosts={sorted(hosts)}"

    labels = [_norm_str(c) for c in (job.get("required_capabilities") or []) if _norm_str(c)]
    if heavy_repo_required_for_kind(j_kind) and "heavy-repo" not in labels:
        labels.append("heavy-repo")
    _wcap = workspace_capability_for_kind(j_kind)  # kind->capability (compile jobs -> linux-compile)
    if _wcap and _wcap not in labels:
        labels.append(_wcap)
    if labels:
        cap_match = queue_logic.match(labels, agent_caps)
        if not cap_match.eligible:
            return (
                False,
                f"agent does not satisfy required_capabilities: {cap_match.reason}",
            )

    # Provider-as-capability gate (flag-gated; Phase 2). Executor jobs need a
    # live LLM; a host advertising no llm-* provider is excluded here -- this
    # is what replaces the manual HIVE_HOST_DENYLIST for dead-provider hosts.
    if LLM_CAP_GATE:
        _needs_llm = j_kind.startswith("build:") or workspace_capability_for_kind(j_kind) is not None
        if _needs_llm and not any(c.startswith("llm-") for c in agent_caps):
            return (
                False,
                f"agent has no live llm-* provider capability (kind {j_kind} needs an LLM)",
            )

    job_max_tier = (job.get("max_cost_tier") or "B").upper()
    agent_tier = (agent.get("cost_tier") or "C").upper()
    if job_max_tier not in COST_TIERS:
        return False, f"job max_cost_tier is invalid: {job_max_tier!r}"
    if agent_tier not in COST_TIERS:
        agent_tier = "C"
    if COST_TIERS.index(agent_tier) > COST_TIERS.index(job_max_tier):
        return (
            False,
            f"agent cost_tier={agent_tier!r} exceeds job max_cost_tier={job_max_tier!r}",
        )

    if agent.get("subscription_throttled") and job_max_tier != "A":
        return (
            False,
            f"subscription agent throttled (>= {THROTTLE_HEADROOM * 100:.0f}% MTD); cannot claim tier-{job_max_tier} jobs",  # noqa: UP031
        )

    return True, "eligible"
