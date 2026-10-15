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
    aliases.discard("")
    return aliases


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


def job_agent_preference_score(job: dict[str, Any], agent: dict[str, Any]) -> int:
    score = 0
    agent_provider = _norm_str(agent.get("provider"))
    agent_model = _norm_str(agent.get("model"))
    provs = [_norm_str(p) for p in (job.get("preferred_providers") or []) if _norm_str(p)]
    models = [_norm_str(m) for m in (job.get("preferred_models") or []) if _norm_str(m)]
    if agent_provider in provs:
        score += 1000 - provs.index(agent_provider)
    if agent_model in models:
        score += 1000 - models.index(agent_model)
    return score


def job_agent_eligible(job: dict[str, Any], agent: dict[str, Any]) -> tuple[bool, str]:
    agent_host = _norm_str(agent.get("host"))
    agent_caps = _norm_str_set(agent.get("capabilities"))
    eligible_aliases = _norm_str_set(agent.get("eligible_aliases"))
    j_kind = _norm_str(job.get("kind"))

    if agent_host in NARROW_HOSTS and not j_kind.startswith(NARROW_ALLOWLIST):
        return False, f"narrow host {agent_host} not allowed for kind {j_kind}"

    kind_affinity = kind_affinity_for_kind(j_kind)
    if kind_affinity and not _norm_str_set(kind_affinity).intersection(eligible_aliases):
        return (
            False,
            f"agent kind aliases={sorted(eligible_aliases)!r} do not satisfy kind affinity={kind_affinity!r}",
        )

    kinds = _norm_str_set(job.get("eligible_kinds"))
    if kinds and "*" not in kinds and not kinds.intersection(eligible_aliases):
        return (
            False,
            f"agent kind aliases={sorted(eligible_aliases)!r} not in eligible_kinds={sorted(kinds)}",
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
                "agent has no live llm-* provider capability (kind %s needs an LLM)" % j_kind,
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
