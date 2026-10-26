#!/usr/bin/env python3
"""
GRAEAE Hive Mind — PYTHIA zeroclaw doctor worker.

Registers as kind=doctor + runtime=doctor (RUNTIME_KIND_MAP entry added 2026-05-26).
Claims triage:* jobs from the hive queue and processes them by invoking
the local Codex CLI tool through ChatGPT subscription auth.

Doctor authority (GRAEAE-validated consultation 2026-05-26):
  • Read hive bus HTTP API to gather failed-job context.
  • Run codex exec with structured prompt requesting JSON action.
  • Execute allowlisted playbook actions: restart_service | resubmit_jobs |
    cancel_jobs | dispatch_codex_fix | no_action | escalate.
  • PATCH triage job to done with summary; auto-clean failed-queue entries.

Constraints:
  • One in-flight triage job per process.
  • No raw shell from LLM output — LLM emits a JSON action, doctor maps to
    a hardcoded playbook function.
  • Allowlisted services + hosts for restart actions.
  • Anthropic FORBIDDEN as agentic provider (CLAUDE.md directive #5).
  • Together AI FORBIDDEN for doctor (too expensive per hive rules).

Env:
  HIVE_URL                   http://192.0.2.67:5005
  ZEROCLAW_BIN               /usr/local/bin/zeroclaw
  DOCTOR_AGENT_ALIAS         hive_doctor (must exist in ~/.zeroclaw/config.toml)
  AGENT_HOST                 PYTHIA
  POLL_INTERVAL              30 seconds idle wait
  HEARTBEAT_INTERVAL         15 seconds
  ZEROCLAW_TIMEOUT           900 seconds per LLM call
  MAX_SSH_PER_HOUR           3
  MIN_HOST_COOLDOWN          1800 seconds
  DRY_RUN                    "1" to disable real actions (default 0)
  DOCTOR_TRIAGE_MODEL        diagnose/classify phase model (default cerberus-reviewer)
  DOCTOR_REVIEW_MODEL        adversarial-review phase model (default cerberus-reviewer)
  DOCTOR_FIX_MODEL           in-doctor fix phase model (default qwen38)
  DOCTOR_OPENWEIGHT_BASE_URL OpenAI-compatible base for the triage/review phases
                             and unknown phases (default CERBERUS
                             http://192.0.2.96:8080/v1)
  DOCTOR_FIX_BASE_URL        base for the fix phase (default TYDEUS
                             http://192.0.2.73:8006/v1)
  DOCTOR_OPENWEIGHT_MODEL    model id for the open-weight path (default gpt-oss-20b)
  DOCTOR_OPENWEIGHT_API_KEY  key for open-weight path (falls back to config.toml deepseek)
"""

from __future__ import annotations

import json
import hashlib
import logging
import os
import re
import shlex
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections import deque

# SOP phase 3 (2026-10-22): durable safety state and the destructive-action rails.
# Both are stdlib-only siblings, importable without the hive package, so the
# container still runs this module standalone.
from hive.workers import doctor_gates
from hive.workers.doctor_state import DurableStateStore, state_store_from_env
from hive.workers import doctor_v2

HIVE_URL = os.environ.get("HIVE_URL", "http://192.0.2.67:5005")
# C1: bus transport auth bearer token (env-sourced). The doctor SUBMITS jobs
# (dispatch_codex_fix), so this token must satisfy the bus admin tier if one is
# configured (HIVE_BUS_ADMIN_TOKEN); otherwise worker-tier auth suffices.
HIVE_BUS_TOKEN = os.environ.get("HIVE_BUS_TOKEN", "")
HIVE_BUS_ADMIN_TOKEN = os.environ.get("HIVE_BUS_ADMIN_TOKEN", "")
ZEROCLAW_BIN = os.environ.get("ZEROCLAW_BIN", "/usr/local/bin/zeroclaw")
DOCTOR_AGENT_ALIAS = os.environ.get("DOCTOR_AGENT_ALIAS", "hive_doctor")
AGENT_HOST = os.environ.get("AGENT_HOST", socket.gethostname().split(".")[0])
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "30"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "15"))
ZEROCLAW_TIMEOUT = int(os.environ.get("ZEROCLAW_TIMEOUT", "900"))
MAX_SSH_PER_HOUR = int(os.environ.get("MAX_SSH_PER_HOUR", "3"))
# The window the SSH rate cap is measured over. Named so the durable rate limiter
# and the in-memory deque cannot drift onto different windows.
SSH_RATE_WINDOW_SEC = float(os.environ.get("SSH_RATE_WINDOW_SEC", "3600"))
MIN_HOST_COOLDOWN = int(os.environ.get("MIN_HOST_COOLDOWN", "1800"))
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"
# Loop-detection: per-job-id (re)fail counter. If the same job_id keeps
# coming back to the doctor (because we released it and some broken worker
# re-failed it), we escalate instead of releasing it again.
_seen_job_attempts: dict[str, list[float]] = {}
LOOP_WINDOW_SEC = float(os.environ.get("LOOP_WINDOW_SEC", "1800"))  # 30 min
LOOP_ESCALATE_THRESHOLD = int(os.environ.get("LOOP_ESCALATE_THRESHOLD", "3"))

# ── Per-phase model selection (2026-06-02) ──
# The doctor runs in phases; each phase picks its model independently.
#   triage  = diagnose/classify a failure cluster -> local open-weight reviewer
#   review  = adversarial review/verify of a fix  -> local open-weight reviewer
#   fix     = implement a code fix                -> local coder; the
#             implementation is normally DISPATCHED to zoder workers via
#             action_dispatch_codex_fix(eligible_kinds=["zeroclaw"]); this knob
#             only governs any in-doctor model call labelled "fix".
# Value is a model id sent to the OpenAI-compatible path
# (DOCTOR_<PHASE>_BASE_URL / DOCTOR_OPENWEIGHT_*). A codex/gpt alias is a
# config error (Codex never runs unattended) and uses DOCTOR_OPENWEIGHT_MODEL.
# LOCAL brain (operator 2026-10-15): the orchestrator reasons on on-prem models.
# Defaults (2026-10-26, matching the live PYTHIA quadlet env): triage + review
# use the CERBERUS role id ``cerberus-reviewer``; the in-doctor fix phase uses
# TYDEUS ``qwen38`` on its own base (DOCTOR_FIX_BASE_URL); the open-weight
# fallback model id is ``gpt-oss-20b`` (a CERBERUS alias).
# HYDRA (.78) / ACHILLES (.11) expose ROCm llama-server — usable as a LIGHT
# orchestration model via DOCTOR_TRIAGE_BASE_URL / DOCTOR_OPENWEIGHT_BASE_URL.
# NEVER the retired :4100.
DOCTOR_TRIAGE_MODEL = os.environ.get("DOCTOR_TRIAGE_MODEL", "cerberus-reviewer").strip().lower()
DOCTOR_REVIEW_MODEL = os.environ.get("DOCTOR_REVIEW_MODEL", "cerberus-reviewer").strip().lower()
DOCTOR_FIX_MODEL = os.environ.get("DOCTOR_FIX_MODEL", "qwen38").strip().lower()
_PHASE_MODELS = {
    "triage": DOCTOR_TRIAGE_MODEL,
    "review": DOCTOR_REVIEW_MODEL,
    "fix": DOCTOR_FIX_MODEL,
}
# OpenAI-compatible local endpoint: CERBERUS llama-server (the durable brain).
DOCTOR_OPENWEIGHT_BASE_URL = os.environ.get("DOCTOR_OPENWEIGHT_BASE_URL", "http://192.0.2.96:8080/v1").rstrip("/")
DOCTOR_OPENWEIGHT_MODEL = os.environ.get("DOCTOR_OPENWEIGHT_MODEL", "gpt-oss-20b")
DOCTOR_OPENWEIGHT_API_KEY = os.environ.get("DOCTOR_OPENWEIGHT_API_KEY", "")
DOCTOR_OPENWEIGHT_TIMEOUT = int(os.environ.get("DOCTOR_OPENWEIGHT_TIMEOUT", "120"))
# Per-phase bases. Triage and review default to CERBERUS (DOCTOR_OPENWEIGHT_BASE_URL);
# the fix phase defaults to TYDEUS, where qwen38 is served — a phase model id
# is sent to its phase base, so the two must agree. Override any phase to a
# LIGHT model on HYDRA/ACHILLES (e.g. DOCTOR_TRIAGE_BASE_URL=http://192.0.2.78:8080/v1).
DOCTOR_FIX_DEFAULT_BASE_URL = "http://192.0.2.73:8006/v1"
DOCTOR_TRIAGE_BASE_URL = os.environ.get("DOCTOR_TRIAGE_BASE_URL", DOCTOR_OPENWEIGHT_BASE_URL).rstrip("/")
DOCTOR_REVIEW_BASE_URL = os.environ.get("DOCTOR_REVIEW_BASE_URL", DOCTOR_OPENWEIGHT_BASE_URL).rstrip("/")
DOCTOR_FIX_BASE_URL = os.environ.get("DOCTOR_FIX_BASE_URL", DOCTOR_FIX_DEFAULT_BASE_URL).rstrip("/")
_PHASE_BASE_URLS = {
    "triage": DOCTOR_TRIAGE_BASE_URL,
    "review": DOCTOR_REVIEW_BASE_URL,
    "fix": DOCTOR_FIX_BASE_URL,
}

# ── Failover chain (2026-10-17) ──
# HYDRA (.78) and ACHILLES (.11) are BUILD hosts: their arm-orchestrator UNLOADS
# the model while compiling, so whichever phase base points at one of them is
# routinely down. DOCTOR_FALLBACK_BASE_URLS is a comma list tried in order after
# the phase base, with DOCTOR_FALLBACK_MODELS a parallel list of model ids (the
# CERBERUS rung serves a different model than the ROCm rungs, so the base and
# its model must travel together). LOCAL ONLY: every default is an on-prem
# llama-server; there is deliberately no remote/paid rung.
DOCTOR_FALLBACK_BASE_URLS: list[str] = [
    u.strip().rstrip("/")
    for u in os.environ.get(
        "DOCTOR_FALLBACK_BASE_URLS",
        "http://192.0.2.78:8080/v1,http://192.0.2.11:8080/v1,http://192.0.2.96:8080/v1",
    ).split(",")
    if u.strip()
]
DOCTOR_FALLBACK_MODELS: list[str] = [
    m.strip()
    for m in os.environ.get("DOCTOR_FALLBACK_MODELS", "gemma4-e2b,gemma4-e2b,gemma4-31b").split(",")
    if m.strip()
]

# Doctor v2 runs beside the existing doctor first.  Enforcing it is an
# explicit operator switch after the side-by-side report has been reviewed.
DOCTOR_V2_MODE = os.environ.get("DOCTOR_V2_MODE", "shadow").strip().lower()
DOCTOR_V2_RULES_PATH = os.environ.get(
    "DOCTOR_V2_RULES_PATH", os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "docs", "doctor_v2_rules.toml")
)
try:
    DOCTOR_V2_RULES = doctor_v2.load_rules(DOCTOR_V2_RULES_PATH)
except (OSError, RuntimeError, ValueError):
    DOCTOR_V2_RULES = []


def _phase_model(phase: str) -> str:
    """The model id the operator configured for this phase.

    DOCTOR_<PHASE>_MODEL, falling back to the doctor-wide open-weight model for
    an unknown phase. A Codex/GPT alias can never be returned (Codex never runs
    unattended — operator rule 2026-10-10), and neither can an empty value."""
    alias = (_PHASE_MODELS.get(phase) or "").strip()
    if not alias or _is_codex_alias(alias):
        return DOCTOR_OPENWEIGHT_MODEL
    return alias


def _fallback_rungs(phase_base: str, phase_model: str | None = None) -> list[tuple[str, str]]:
    """(base_url, model_id) rungs to try after the phase base.

    One rung per DOCTOR_FALLBACK_BASE_URLS entry, excluding the phase base
    itself. DOCTOR_FALLBACK_MODELS is the parallel model list; the two lists may
    differ in length in either direction:
      - more bases than models  -> the trailing bases reuse ``phase_model``
        (never an index into a shorter list),
      - more models than bases  -> the extra models are unused.
    Duplicate bases are collapsed. Nothing here can raise IndexError."""
    default_model = (phase_model or "").strip() or DOCTOR_OPENWEIGHT_MODEL
    rungs: list[tuple[str, str]] = []
    seen = {phase_base.rstrip("/")}
    for idx, raw_base in enumerate(DOCTOR_FALLBACK_BASE_URLS):
        base = (raw_base or "").strip().rstrip("/")
        if not base or base in seen:
            continue
        seen.add(base)
        model = (DOCTOR_FALLBACK_MODELS[idx] or "").strip() if idx < len(DOCTOR_FALLBACK_MODELS) else ""
        rungs.append((base, model or default_model))
    return rungs


# Why a single endpoint attempt failed. Only transport errors, HTTP 5xx and an
# empty completion are failover-eligible; a 4xx (bad model id, bad request) would
# fail identically everywhere and must surface instead of being masked.
_FAILOVER_ELIGIBLE_REASONS = ("transport", "http_5xx", "empty")


def _classify_attempt_failure(reason: str) -> str:
    """Classify a failed invocation reason for failover eligibility."""
    text = (reason or "").lower()
    m = re.search(r"\bhttp (\d{3})\b", text)
    if m:
        return "http_5xx" if int(m.group(1)) >= 500 else "http_4xx"
    if "empty response" in text:
        return "empty"
    if "call failed at " in text or "call failed:" in text:
        return "transport"
    return "other"


# Self-triage scanner config
SCAN_INTERVAL = float(os.environ.get("SCAN_INTERVAL", "300"))  # seconds between failed-cluster scans
CLUSTER_THRESHOLD = int(os.environ.get("CLUSTER_THRESHOLD", "1"))  # min failures of same base_kind to trigger a triage
CLUSTER_WINDOW_SEC = float(os.environ.get("CLUSTER_WINDOW_SEC", "21600"))  # only look at failures in last 6h
# To avoid re-triaging the same cluster repeatedly, we remember kinds we've already
# auto-triaged within this rolling window:
_recent_auto_triage: dict[str, float] = {}
AUTO_TRIAGE_COOLDOWN = float(os.environ.get("AUTO_TRIAGE_COOLDOWN", "3600"))  # 1h per base_kind

# ── Dead-letter queue (DLQ) remediation ─────────────────────────────────────
# Restored 2026-10-17: the Phase-2 refactor dropped the DLQ sweep that the
# pre-refactor doctor had (commit 5e720f8), so dead-lettered jobs accumulated
# with no automatic remediation.
DLQ_SCAN_INTERVAL = float(os.environ.get("DLQ_SCAN_INTERVAL", str(SCAN_INTERVAL)))
DLQ_APPLY = os.environ.get("DLQ_APPLY", "0") == "1"
DLQ_MAX_PER_CYCLE = int(os.environ.get("DLQ_MAX_PER_CYCLE", "20"))
# The WORK one dead-letter cycle may do, counted in ATTEMPTS rather than in actions.
# `DLQ_MAX_PER_CYCLE` is a budget for progress: a refusal for want of an approval
# does not spend it, because a job the doctor was not allowed to act on is still a
# job the operator has to hear about. That left the work itself unbounded — every
# refused job in the queue was classified, attempted and broadcast on every cycle,
# which is one pass over the whole dead-letter queue per cycle for a queue that
# cannot drain. This bounds the pass. It is at least `DLQ_MAX_PER_CYCLE`, so the
# action budget still binds first whenever the doctor is actually making progress.
DLQ_MAX_ATTEMPTS_PER_CYCLE = max(DLQ_MAX_PER_CYCLE, int(os.environ.get("DLQ_MAX_ATTEMPTS_PER_CYCLE", "50")))
# Once a dead-letter job has a logged decision, don't re-decide it every cycle.
_dlq_seen: dict[str, float] = {}
DLQ_SEEN_TTL = float(os.environ.get("DLQ_SEEN_TTL", "21600"))  # 6h
NEEDS_REVIEW_SCAN_INTERVAL = float(os.environ.get("NEEDS_REVIEW_SCAN_INTERVAL", "3600"))
NEEDS_REVIEW_OLDER_HOURS = float(os.environ.get("NEEDS_REVIEW_OLDER_HOURS", "24"))
MAX_BULK_RESUBMITS_PER_ACTION = 20
DOCTOR_MAX_SAME_SIGNATURE_RESUBMITS = doctor_v2.max_same_signature_resubmits()
SAME_SIGNATURE_RETRY_TTL = 30 * 24 * 3600


def _max_same_signature_resubmits() -> int:
    return doctor_v2.max_same_signature_resubmits()
# Owner-protected dead-letter jobs (2026-10-17): another orchestrator asked that
# its jobs be reported, not remediated. A dead-letter job whose kind starts with
# one of these prefixes, or whose project is listed, is logged and left alone
# (no purge, requeue or reclassify). Comma-separated env lists.
DLQ_PROTECT_KIND_PREFIXES = tuple(
    p.strip() for p in os.environ.get("DLQ_PROTECT_KIND_PREFIXES", "").split(",") if p.strip()
)
DLQ_PROTECT_PROJECTS = frozenset(p.strip() for p in os.environ.get("DLQ_PROTECT_PROJECTS", "").split(",") if p.strip())


def dlq_protected(job: dict) -> bool:
    """Whether a dead-letter job belongs to an owner who handles it themselves."""
    kind = str(job.get("kind") or "")
    if DLQ_PROTECT_KIND_PREFIXES and kind.startswith(DLQ_PROTECT_KIND_PREFIXES):
        return True
    return str(job.get("project") or "") in DLQ_PROTECT_PROJECTS


# Job-level "permanently unschedulable" decline reasons -> remove. Mirrors
# hive.bus.domain.jobs.DECLINE_REASON_PREFIXES (kept literal: this module is
# stdlib-only so it can run in the container without the hive package).
DLQ_UNSCHEDULABLE_PREFIXES = (
    "no_workspace_for_kind",
    "no_workspace_for_repo",
    "no_repo_mapping",
    "malformed_job_id",
    "invalid_head_branch",
    "invalid_base_branch",
    "branch_not_found",
)
# Transient / infra failure signatures -> safe to fix & resubmit.
DLQ_TRANSIENT_RE = re.compile(
    r"timed?\s*out|timeout|rate.?limit|\b429\b|too many requests|"
    r"oom|out of memory|killed|signal\s*9|\b137\b|"
    r"connection (reset|refused|aborted)|broken pipe|"
    r"temporar|unavailable|\b50[0-9]\b|bad gateway|gateway time",
    re.IGNORECASE,
)

# ── Token-mismatch auto-fix (2026-05-27) ──
# When a worker fails with "no token for host" because its gateway-tokens.json
# 127.0.0.1 entry drifted away from the gateway's paired_tokens, the doctor
# auto-dispatches a codex fix without burning an LLM call.
# Rate-limit: max 3 fixes per host per hour to avoid loops.
_token_fix_attempts: dict[str, list[float]] = {}
MAX_TOKEN_FIX_PER_HOST = int(os.environ.get("MAX_TOKEN_FIX_PER_HOST", "3"))
TOKEN_FIX_WINDOW_SEC = float(os.environ.get("TOKEN_FIX_WINDOW_SEC", "3600"))  # 1 hour
# SECURITY (review C2/H3): no baked-in default. The prior literal
# ('fleet-lan-token-2026-05-26') was a committed shared LAN secret embedded into
# generated gateway-token fix scripts. It is now env-only and compromised via git
# history — rotate the gateway paired_tokens on every fleet host and purge it from
# history. When unset, token-mismatch auto-fix escalates instead of emitting an
# empty/placeholder token into gateway config.
FLEET_LAN_TOKEN = os.environ.get("FLEET_LAN_TOKEN", "")
# Known failure signatures → automatic dispatch (no LLM needed)
KNOWN_FAILURE_SIGNATURES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"no token for host", re.IGNORECASE), "token_mismatch"),
    (re.compile(r"pair gateway first via POST /pair", re.IGNORECASE), "token_mismatch"),
    (re.compile(r"gateway.*unauthorized.*token", re.IGNORECASE), "token_mismatch"),
]
# Full fleet including Mac hosts (ULTRA, STUDIO) and sshpass hosts (MEDUSA, HYDRA)
ALL_FLEET_HOSTS: dict[str, dict] = {
    "ultra": {"ip": "192.0.2.60", "os": "macos", "auth": "passwordless", "user": "jasonperlow"},
    "studio": {"ip": "192.0.2.10", "os": "macos", "auth": "passwordless", "user": "jasonperlow"},
    "medusa": {"ip": "192.0.2.64", "os": "linux", "auth": "sshpass", "user": "jasonperlow"},
    "hydra": {"ip": "192.0.2.78", "os": "linux", "auth": "sshpass", "user": "jasonperlow"},
    "cerberus": {"ip": "192.0.2.96", "os": "linux", "auth": "passwordless", "user": "jasonperlow"},
    "proteus": {"ip": "192.0.2.25", "os": "linux", "auth": "passwordless", "user": "jasonperlow"},
    "bigpi": {"ip": "192.0.2.65", "os": "linux", "auth": "passwordless", "user": "jasonperlow"},
    "clawpi": {"ip": "192.0.2.54", "os": "linux", "auth": "passwordless", "user": "jasonperlow"},
    "zeropi": {"ip": "192.0.2.56", "os": "linux", "auth": "passwordless", "user": "jasonperlow"},
    "typhon": {"ip": "192.0.2.61", "os": "linux", "auth": "passwordless", "user": "jasonperlow"},
}

log = logging.getLogger("doctor")

_running = True
_urn: str = ""
_session_id = str(uuid.uuid4())

# Rate-limit state for SSH restart actions. Both moved to the durable store
# (2026-10-22): a restart of the pod used to reset the SSH rate limiter, so the
# first post-restart restart action always looked like the first one.
_ssh_actions_log: deque = deque()  # write-only mirror; the cap reads the durable store
_host_last_action: dict[str, float] = {}  # legacy mirror, kept for the tests that read it

# Allowlisted services for restart_service action
ALLOWED_SERVICE_RE = re.compile(
    r"^(zeroclaw-worker(@[a-z0-9-]+)?|zeroclaw-fanout|zeroclaw-doctor|"
    r"hive-triage|graeae-system-watcher)\.service$"
)

# Fleet hosts eligible for SSH restart actions (NOT PYTHIA itself, NOT ARGOS).
# DEPRECATED 2026-10-22: `doctor_gates.RESTART_TARGETS` is the one table now, and
# it also holds the connection spec and cixmini. This alias stays so the existing
# imports keep working, and `action_restart_service` no longer reads it.
FLEET_HOSTS = {
    name: entry.get("address", "") for name, entry in doctor_gates.RESTART_TARGETS.items() if entry.get("address")
}
# Hosts the doctor must NEVER restart — production-critical only.
# (User directive 2026-05-26: cixmini is one of our most powerful systems —
# DO NOT blacklist it. Fix config-side bugs instead.) Now the one frozenset in
# `doctor_gates`, which is also the one the decision SOP reads.
RESTART_BLOCKED_HOSTS = set(doctor_gates.RESTART_BLOCKED_HOSTS)
SSH_TIMEOUT = 15

# ───────────────────────── Durable safety state ─────────────────────────
# Built once, at import. `doctor_state.state_store_from_env` picks the backend the
# environment asks for; auto/file use a persistent local file. The module-level dicts above
# and below stay as mirrors for the tests and for anything that reads them
# directly, but every *enforcement* path goes through the store.
STATE: DurableStateStore = state_store_from_env()
APPROVALS = doctor_gates.ApprovalGate(STATE)

# Key prefixes, named once so the TTLs in `COUNTER_TTLS` and the keys the doctor
# writes cannot drift apart.
KEY_LOOP = "loop"
KEY_COOLDOWN = "cooldown"
KEY_DLQ = "dlq"
KEY_SSH = "ssh"
KEY_HOST_COOLDOWN = "host_cooldown"
KEY_TOKEN_FIX = "token_fix"


def reset_safety_state(state: DurableStateStore | None = None) -> None:
    """Clear every safety counter: the durable store AND the in-memory mirrors.

    Two callers, one function, because the two halves have to move together. A
    test that clears only the module dicts leaves the durable loop count in place
    and the next test escalates a job it never touched; an operator who has just
    changed a limit deliberately needs the same single call.

    NOT reachable from the job loop: nothing in `process_triage_job`,
    `handle_dead_letter_queue` or the SSH path calls this. Clearing a safety limit
    is a human action.

    One `clear()` rather than a `forget()` per key: the store's keys are the
    durable half and there can be hundreds of them, so per-key forgetting is a
    per-key round-trip to the backend (and, on a file backend, a full rewrite of
    the state file each time).
    """
    target = STATE if state is None else state
    target.clear()
    _seen_job_attempts.clear()
    _recent_auto_triage.clear()
    _dlq_seen.clear()
    _ssh_actions_log.clear()
    _host_last_action.clear()


def loop_attempts(job_id: str, window: float | None = None, at: float | None = None) -> list[float]:
    """Times the doctor has been handed this job inside the window.

    Durable, so a restart cannot give a job a fresh loop count. The in-memory
    `_seen_job_attempts` mirror is kept in step for the callers that read it.
    """
    window = LOOP_WINDOW_SEC if window is None else float(window)
    stamps = STATE.touch(f"{KEY_LOOP}:{job_id}", ttl=window, at=at)
    _seen_job_attempts.setdefault(job_id, [])
    _seen_job_attempts[job_id] = list(stamps)
    return stamps


def record_ssh_action(host: str, at: float | None = None) -> None:
    """Record one SSH restart against the durable rate limiter and the cooldown.

    Durable, so a restart of the pod does not hand the doctor a fresh SSH budget.

    The mirror is trimmed HERE rather than on read: `_ssh_rate_ok` no longer
    prunes it, so a deque nothing reads must not be the only thing bounding its
    growth, and a mirror trimmed on write is the one a reader can trust to be the
    window it claims.
    """
    stamp = STATE.mark(f"{KEY_SSH}:{host}", ttl=SSH_RATE_WINDOW_SEC, at=at)
    STATE.touch(f"{KEY_SSH}:all", ttl=SSH_RATE_WINDOW_SEC, at=at)
    STATE.mark(f"{KEY_HOST_COOLDOWN}:{host}", ttl=MIN_HOST_COOLDOWN, at=at)
    _ssh_actions_log.append(stamp)
    # `clear`/`extend` rather than a slice assignment: the mirror is a deque in
    # production and a list in some tests, and neither should care which.
    # Trimmed against `stamp`, which is the STORE's clock for this record and the
    # same clock `seen()` and `timestamps()` window against. The wall clock is not
    # used here: a caller that backdates `at` (a replay, a test) would otherwise
    # have every entry it just recorded trimmed away as "old".
    _live = [entry for entry in _ssh_actions_log if stamp - entry <= SSH_RATE_WINDOW_SEC]
    _ssh_actions_log.clear()
    _ssh_actions_log.extend(_live)
    _host_last_action[host] = stamp


def ssh_actions_in_window(host: str, window: float) -> int:
    """How many SSH actions this host has seen inside `window`, durably."""
    return len(STATE.timestamps(f"{KEY_SSH}:{host}", ttl=window))


def ssh_actions_total(window: float) -> int:
    """How many SSH actions across the fleet inside `window`, durably."""
    return len(STATE.timestamps(f"{KEY_SSH}:all", ttl=window))


def host_in_cooldown(host: str, now: float | None = None) -> float:
    """Seconds since the last SSH action on this host, or ``inf`` if never."""
    last = STATE.last_mark(f"{KEY_HOST_COOLDOWN}:{host}")
    if last is None:
        return float("inf")
    return max(0.0, (time.time() if now is None else now) - last)


def cooldown_active(key: str, ttl: float) -> bool:
    """Whether a cooldown is in force, durably."""
    return STATE.seen(key, ttl=ttl)


def note_triage(kind: str, ttl: float | None = None, at: float | None = None) -> float:
    """Start the per-kind re-triage cooldown."""
    ttl = AUTO_TRIAGE_COOLDOWN if ttl is None else float(ttl)
    stamp = STATE.mark(f"{KEY_COOLDOWN}:{kind}", ttl=ttl, at=at)
    _recent_auto_triage[kind] = stamp
    return stamp


def dlq_decided(job_id: str, ttl: float | None = None) -> bool:
    """Whether a dead-letter job has already been decided this cycle window.

    A READ. Recording is [`dlq_record_decision`]'s job, deliberately, because the
    two must not happen at the same moment: a combined "check and mark" marks a
    job the moment it is LOOKED at, so a job the per-cycle cap cuts off before it
    is reached is recorded as decided and then never decided again for the whole
    seen window. The base commit marked at the end of the loop, which is the order
    that matters, and this keeps it.
    """
    ttl = DLQ_SEEN_TTL if ttl is None else float(ttl)
    return STATE.seen(f"{KEY_DLQ}:{job_id}", ttl=ttl)


def dlq_record_decision(job_id: str, ttl: float | None = None, at: float | None = None) -> float:
    """Record that a dead-letter job has been DECIDED, durably.

    Called after the decision has been carried out — or after the job is recorded as
    one its owner handles — and never before. A refusal from the approval gate IS a
    decision (the doctor decided not to act), so that is recorded too; what must
    never be recorded is a job the doctor never reached.
    """
    ttl = DLQ_SEEN_TTL if ttl is None else float(ttl)
    stamp = STATE.mark(f"{KEY_DLQ}:{job_id}", ttl=ttl, at=at)
    _dlq_seen[job_id] = stamp
    return stamp


def _signal(signum, frame):
    global _running
    log.info("signal %s — shutting down after current job", signum)
    _running = False


# ───────────────────────── HTTP helpers ─────────────────────────
def _http(method: str, path: str, body: dict | None = None, timeout: float = 15.0) -> tuple[int, dict | None]:
    url = f"{HIVE_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    headers = {"content-type": "application/json"}
    registering_orchestrator = (
        path == "/v1/agents/register"
        and isinstance(body, dict)
        and str(body.get("runtime") or "").lower()
        in {
            "claude-code",
            "claude-cli",
            "claude",
            "human",
            "mnemos",
            "doctor",
            "dream-walker",
            "unknown",
        }
    )
    token = HIVE_BUS_ADMIN_TOKEN if path.startswith("/v1/admin/") or registering_orchestrator else HIVE_BUS_TOKEN
    if token:
        headers["authorization"] = f"Bearer {token}"
    # MANDATORY: mutating routes require X-Hive-Actor naming a registered, ACTIVE
    # agent (hive/bus/deps.require_mutation_actor). Without it every DLQ apply
    # 401s and the job is then marked "seen" (never retried). The doctor's own
    # registered URN qualifies.
    if _urn and method.upper() in {"POST", "PATCH", "PUT", "DELETE"} and path != "/v1/agents/register":
        headers["X-Hive-Actor"] = _urn
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, None
    except Exception as e:
        log.warning("http %s %s: %s", method, path, e)
        return 0, None


# ───────────────────────── Registration ─────────────────────────
def register() -> str:
    global _urn
    body = {
        "host": AGENT_HOST,
        "kind": "doctor",
        "runtime": "doctor",
        # Per-phase models (2026-06-02): triage model is the doctor's primary
        # diagnose model; review/fix models surfaced in metadata.phase_models.
        "model": DOCTOR_TRIAGE_MODEL,
        # Codex never runs unattended (operator 2026-10-10): a codex/gpt alias
        # is a config error and the doctor registers as its local open-weight
        # provider, matching what invoke_doctor_agent actually calls.
        "provider": "openai-compatible",
        "autonomy_level": "autonomous",
        "auth_method": "subscription",
        "capabilities": [
            "triage",
            "diagnose",
            "fix-agents",
            "fix-code",
            "dispatch-codex",
            "resubmit-jobs",
            "restart-services",
            "bus-http-read",
            "bus-http-write",
        ],
        "version": "zeroclaw-doctor/1.0",
        "metadata": {
            "agent_alias": DOCTOR_AGENT_ALIAS,
            "session_id": _session_id,
            "dry_run": DRY_RUN,
            "daemon": "zeroclaw_doctor.py",
            "phase_models": dict(_PHASE_MODELS),
        },
    }
    for attempt in range(1, 11):
        code, resp = _http("POST", "/v1/agents/register", body)
        if code in (200, 201) and resp and resp.get("urn"):
            _urn = resp["urn"]
            log.info("registered urn=%s alias=%s", _urn, DOCTOR_AGENT_ALIAS)
            return _urn
        log.warning("register attempt %d failed code=%s resp=%s", attempt, code, str(resp)[:200])
        time.sleep(min(30, 5 * attempt))
    log.error("could not register doctor after 10 attempts — exiting")
    sys.exit(1)


# Codes that mean "the bus does not know this URN (any more) — re-register".
# 404 is the historic answer; the bus now returns 409 for an unknown agent URN
# (so workers self-heal, 2026-10-16). Both are self-heal signals, not errors.
REREGISTER_CODES = (404, 409)


def heartbeat():
    code, _ = _http("POST", "/v1/agents/heartbeat", {"urn": _urn, "status": "online"})
    if code in REREGISTER_CODES:
        # Without this a ghost doctor (URN reaped after a stall, or bus restored)
        # gets a 409 on every heartbeat and spins forever: BLOCKING-3 in
        # docs/audit-3-fixes-20261016.md. 404 stays handled for older buses.
        log.warning("heartbeat %s (agent not registered) — re-registering", code)
        register()


# ───────────────────────── Job claim/patch ─────────────────────────
# CLAIM SCOPE (2026-10-17): the doctor only owns doctor/triage work. It used to
# claim whatever /v1/jobs/next offered (which includes any job whose
# eligible_kinds lists 'doctor'), then failed it in 0s because process_triage_job
# is not an executor — observed as fix:singularity-shell-about-graphic being
# failed instantly.
DOCTOR_CLAIM_KIND_PREFIXES = ("triage:", "doctor:")
# Its own dispatch kind (action_dispatch_codex_fix) is worker work, never the
# doctor's to claim.
DOCTOR_CLAIM_KIND_EXCLUDE_PREFIXES = ("doctor:codex-fix:",)


def is_doctor_claimable(kind: str) -> bool:
    """True only for triage:<base_kind> and doctor:* jobs (minus its own fix
    dispatches). A normal worker job must never be claimed by the doctor."""
    k = (kind or "").strip()
    if not k:
        return False
    if any(k.startswith(p) for p in DOCTOR_CLAIM_KIND_EXCLUDE_PREFIXES):
        return False
    return any(k.startswith(p) for p in DOCTOR_CLAIM_KIND_PREFIXES)


def release_out_of_scope_job(job: dict) -> str:
    """Put a wrongly-claimed job straight back in the queue.

    Released with worker_error 'released_by_host' — a skip/decline signal, NOT
    a failure: the bus keeps it queued (no failed status, no dead-letter
    counting) and the matcher routes it to a host that can execute it."""
    job_id = (job or {}).get("id") or ""
    kind = (job or {}).get("kind") or ""
    body = {
        "status": "queued",
        "claimed_by": _urn,
        "result": {"worker_error": "released_by_host", "kind": kind},
    }
    code, resp = _http("PATCH", f"/v1/jobs/{job_id}", body)
    if code in (200, 201, 204):
        log.warning("released out-of-scope job %s kind=%s back to queue", job_id[:12], kind)
        return f"released {job_id[:12]} ({kind}) to queue (out of doctor scope)"
    log.error("could not release out-of-scope job %s kind=%s code=%s resp=%s", job_id[:12], kind, code, str(resp)[:200])
    return f"failed to release out-of-scope job {job_id[:12]} code={code}"


def claim_next_job() -> dict | None:
    code, resp = _http("POST", f"/v1/jobs/next?agent_urn={_urn}")
    if code == 204:
        return None
    if code in (200, 201) and resp:
        job = resp if isinstance(resp, dict) else None
        if job is not None and not is_doctor_claimable(job.get("kind", "")):
            # The bus claim endpoint cannot be kind-filtered per agent, so the
            # scope is enforced here: hand the job straight back, unfailed.
            release_out_of_scope_job(job)
            return None
        return job
    if code in REREGISTER_CODES:
        # Expected re-register signal (unknown URN / not online-idle), not an
        # error: the heartbeat loop heals it. Logging here warned every poll
        # (ADVISORY-2 in docs/audit-3-fixes-20261016.md).
        log.debug("claim_next %s — awaiting re-register", code)
        return None
    if code != 0:
        log.warning("claim_next %s: %s", code, str(resp)[:200])
    return None


def patch_job(job_id: str, status: str, result: dict | None = None):
    # Bus requires `claimed_by` to match the current claimant URN — using
    # any other field name (e.g. `agent_urn`) yields 403 in the patch_job
    # ownership check at agent_bus.py:1690.
    body: dict = {"status": status, "claimed_by": _urn}
    if result is not None:
        body["result"] = result
    _http("PATCH", f"/v1/jobs/{job_id}", body)


# ───────────────────────── Failure context (HTTP API) ─────────────────────────
# Use the bus HTTP API for job context so the doctor stays behind the same
# queue contract as the rest of the fleet.
def fetch_failed_jobs_for_kind(base_kind: str, limit: int = 10) -> list[dict]:
    """Recent failed jobs whose kind starts with base_kind, newest first."""
    code, resp = _http("GET", "/v1/jobs?status=failed&limit=200", timeout=15.0)
    if code != 200 or not resp:
        log.warning("fetch_failed_jobs http=%s", code)
        return []
    jobs = resp.get("jobs", []) or []
    # match kind prefix OR exact base_kind after stripping [tag] suffix
    strip_re = re.compile(r"\s*\[.*?\]\s*$")
    matched = [
        j
        for j in jobs
        if (j.get("kind") or "").startswith(base_kind) or strip_re.sub("", j.get("kind", "")).strip() == base_kind
    ]
    matched.sort(key=lambda j: j.get("ended_at") or 0, reverse=True)
    return matched[:limit]


def fetch_agent_registry_summary() -> dict:
    """Snapshot of agents grouped by kind / host / status via /v1/agents."""
    out = {"by_kind": {}, "by_host": {}, "by_status": {}}
    code, resp = _http("GET", "/v1/agents", timeout=10.0)
    if code != 200 or not resp:
        log.warning("fetch_agents http=%s", code)
        return out
    for a in resp.get("agents", []) or []:
        for col, key in (("kind", "by_kind"), ("host", "by_host"), ("status", "by_status")):
            v = a.get(col) or "(null)"
            out[key][v] = out[key].get(v, 0) + 1
    return out


# ───────────────────────── Doctor LLM prompt ─────────────────────────
DOCTOR_SYSTEM = """You diagnose Hive job failures. Return one JSON object with
"action_type" (release_to_queue, no_action, or escalate), "confidence" (0..1),
"reason" (diagnosis), "evidence_quote" (a verbatim substring of the supplied
failure evidence), "max_retries" (default 2, never above the configured
same-signature retry cap), and optional "release_eligible_kinds" drawn only
from zeroclaw, opencode, zoder, claude. Release requires confidence at least 0.7;
if no supplied line supports a transient diagnosis, do not release.
Do not call tools. Purges, restarts and fix tasks require deterministic decisions
and exact operator approvals; they are not model-selectable actions.
"""
DOCTOR_SYSTEM += "\nDiagnostic hosts: " + "|".join(doctor_gates.restart_target_names())


def build_doctor_prompt(job: dict, base_kind: str, failed: list[dict], registry: dict, failure_count_hint: int) -> str:
    msg = [
        "TRIAGE REQUEST",
        f"triage_job_id: {job.get('id', '')}",
        f"triage_kind: {job.get('kind', '')}",
        f"base_kind: {base_kind}",
        f"failure_count_hint: {failure_count_hint}",
        "",
        "TRIAGE DESCRIPTION:",
        (job.get("description") or "")[:6000],
        "",
        f"RECENT FAILED JOBS OF base_kind={base_kind!r} ({len(failed)} shown):",
    ]
    for fj in failed[:8]:
        result_preview = ""
        if fj.get("result"):
            try:
                rd = json.loads(fj["result"]) if isinstance(fj["result"], str) else fj["result"]
                txt = " | ".join(str(rd.get(key) or "")[-4096:] for key in
                                  ("failure_reason", "error", "tail", "stderr", "stdout") if rd.get(key))
                exit_code = rd.get("exit_code")
                result_preview = f"exit={exit_code} retry_count={fj.get('retry_count', rd.get('retry_count', 0))} | {txt[-16384:]}"
            except Exception:
                result_preview = str(fj["result"])[:400]
        msg.append(
            f"  - id={fj.get('id', '')[:12]} kind={fj.get('kind', '')} "
            f"claimed_by={fj.get('claimed_by', '')} "
            f"provider={fj.get('claimed_provider', '')} "
            f"model={fj.get('claimed_model', '')}"
        )
        if result_preview:
            msg.append(f"    result: {result_preview}")
        msg.append(f"    host={fj.get('host') or fj.get('claimed_host') or ''} head_branch={fj.get('head_branch') or ''}")
    msg.extend(
        [
            "EVIDENCE RULE: A release_to_queue decision MUST include evidence_quote exactly as supplied below.",
            "An invented or absent quote is invalid and will be escalated.",
            "",
            "AGENT REGISTRY SUMMARY:",
            f"  by_kind: {json.dumps(registry.get('by_kind', {}))}",
            f"  by_host: {json.dumps(registry.get('by_host', {}))}",
            f"  by_status: {json.dumps(registry.get('by_status', {}))}",
            "",
            "Respond with the JSON action object only.",
        ]
    )
    return "\n".join(msg)


DOCTOR_GATEWAY_HOST = os.environ.get("GATEWAY_HOST", "127.0.0.1").strip()
DOCTOR_GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", "42617"))
DOCTOR_GATEWAY_TIMEOUT = int(os.environ.get("DOCTOR_GATEWAY_TIMEOUT", "180"))


def _invoke_gateway(prompt: str) -> tuple[bool, str]:
    """DEPRECATED / UNREACHABLE FROM THE DOCTOR LOOP (operator rule 2026-10-10).

    Codex never runs unattended: invoke_doctor_agent() no longer calls this for
    any alias — "gateway"/"codex"/"gpt" are treated as a config error and the
    open-weight chain is used instead. Kept only so an operator can post a
    manual, attended prompt to the local gateway.

    Reason via the LOCAL zeroclaw gateway codex chain — the SAME path the
    workers use. The gateway KNEMON-routes POST /webhook to codex/gpt over
    ChatGPT-OAuth ($0, no API key, not the codex CLI subprocess). This is the
    doctor's DEFAULT reasoning path per operator doctrine 2026-06-03
    (codex/OpenAI-OAuth first; open-weight only on a genuine gateway outage)."""
    full = DOCTOR_SYSTEM + "\n\n" + prompt
    url = f"http://{DOCTOR_GATEWAY_HOST}:{DOCTOR_GATEWAY_PORT}/webhook"
    data = json.dumps({"message": full}).encode()
    req = urllib.request.Request(url, data=data, method="POST", headers={"content-type": "application/json"})
    log.info("invoking gateway codex path %s prompt_chars=%d", url, len(full))
    try:
        with urllib.request.urlopen(req, timeout=DOCTOR_GATEWAY_TIMEOUT) as r:
            resp = json.loads(r.read())
        text = (resp.get("response") or resp.get("message") or "").strip()
        if text:
            return True, text
        return False, f"gateway empty response: {str(resp)[:300]}"
    except urllib.error.HTTPError as e:
        return False, f"gateway HTTP {e.code}: {e.read()[:300]!r}"
    except Exception as e:
        return False, f"gateway call failed: {e}"


def _gateway_failure_allows_fallback(reason: str) -> bool:
    """Only genuine gateway transport outages may fall back to open-weight."""
    text = (reason or "").lower()
    if re.search(r"gateway http (502|503|504)\b", text):
        return True
    if not text.startswith("gateway call failed:"):
        return False
    outage_markers = (
        "connection refused",
        "connection reset",
        "connection aborted",
        "timed out",
        "timeout",
        "network is unreachable",
        "no route to host",
        "temporarily unavailable",
    )
    return any(marker in text for marker in outage_markers)


def _openweight_api_key() -> str:
    """Resolve the open-weight API key: env override, else the NGC LiteLLM
    proxy key from /etc/nclawzero/ngc-review-env (the default endpoint is the
    .4 NGC gateway). No deepseek fallback — review-only per operator 2026-06-07."""
    if DOCTOR_OPENWEIGHT_API_KEY:
        return DOCTOR_OPENWEIGHT_API_KEY
    for envfile in ("/etc/nclawzero/ngc-review-env", "/etc/nclawzero/agent-env"):
        try:
            for line in open(envfile):
                m = re.match(r"\s*(?:export\s+)?NGC_LITELLM_PROXY_KEY=(.*)", line)
                if m:
                    return m.group(1).strip().strip('"').strip("'")
        except OSError:
            continue
    return ""


def _invoke_openweight(
    prompt: str,
    model_alias: str,
    *,
    base_url: str | None = None,
    model: str | None = None,
) -> tuple[bool, str]:
    """Run a non-codex phase via an OpenAI-compatible chat endpoint.

    Defaults to the LOCAL brain (CERBERUS, DOCTOR_OPENWEIGHT_MODEL) — pass
    ``base_url``/``model`` for a specific phase. Anthropic + Together remain
    FORBIDDEN per CLAUDE.md. Local endpoints accept a placeholder auth, so a
    missing key is only fatal when a base is explicitly remote."""
    base = (base_url or DOCTOR_OPENWEIGHT_BASE_URL).rstrip("/")
    model_id = model or DOCTOR_OPENWEIGHT_MODEL
    api_key = _openweight_api_key()
    body = {
        "model": model_id,
        "messages": [
            {"role": "system", "content": DOCTOR_SYSTEM},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.0,
        "stream": False,
    }
    url = f"{base}/chat/completions"
    data = json.dumps(body).encode()
    headers = {"content-type": "application/json"}
    if api_key:
        headers["authorization"] = f"Bearer {api_key}"
    else:
        log.warning("no open-weight API key; calling %s unauthenticated (ok for local)", base)
    req = urllib.request.Request(url, data=data, method="POST", headers=headers)
    log.info("invoking model=%s at %s (alias=%s, prompt_chars=%d)", model_id, base, model_alias, len(prompt))
    try:
        with urllib.request.urlopen(req, timeout=DOCTOR_OPENWEIGHT_TIMEOUT) as r:
            resp = json.loads(r.read())
        text = (resp.get("choices") or [{}])[0].get("message", {}).get("content", "")
        if text and text.strip():
            return True, text.strip()
        return False, f"model {model_id} empty response: {str(resp)[:300]}"
    except urllib.error.HTTPError as e:
        return False, f"model {model_id} HTTP {e.code}: {e.read()[:300]!r}"
    except Exception as e:
        return False, f"model {model_id} call failed at {base}: {e}"


_CODEX_ALIASES = ("gateway", "codex", "openai", "gpt")
# Emitted once per process: an operator who (re)introduces a Codex alias in
# DOCTOR_<PHASE>_MODEL gets a loud, single config-error line — not a silent
# per-job gateway call.
_codex_alias_warned = False


def _is_codex_alias(model_alias: str) -> bool:
    alias = (model_alias or "").strip().lower()
    return alias in _CODEX_ALIASES or alias.startswith("gpt-")


def invoke_doctor_agent(prompt: str, phase: str = "triage") -> tuple[bool, str]:
    """Run the doctor LLM for a given PHASE using its configured model.

    Phase model/base come from DOCTOR_<PHASE>_MODEL / DOCTOR_<PHASE>_BASE_URL
    (see _PHASE_MODELS / _PHASE_BASE_URLS) and default to a LOCAL llama-server.

    Failover (2026-10-17): the phase base is tried first, then every
    DOCTOR_FALLBACK_BASE_URLS rung that differs from it, each with its parallel
    DOCTOR_FALLBACK_MODELS id. Only transport errors, HTTP 5xx and an empty
    completion move to the next rung — a 4xx is surfaced immediately. The
    endpoint that answered is logged (and named in the returned text on
    failover).

    Codex/GPT aliases are a CONFIG ERROR (operator rule 2026-10-10: Codex never
    runs unattended). They log once and use the open-weight chain; the local
    gateway is never called automatically. Remote/paid providers are never a
    fallback rung — every rung is an on-prem llama-server."""
    global _codex_alias_warned
    model_alias = _PHASE_MODELS.get(phase, DOCTOR_TRIAGE_MODEL)
    if _is_codex_alias(model_alias):
        if not _codex_alias_warned:
            _codex_alias_warned = True
            log.error(
                "CONFIG ERROR: DOCTOR_%s_MODEL=%r is a Codex/GPT alias, which never "
                "runs unattended (operator rule 2026-10-10); using the open-weight "
                "chain instead",
                phase.upper(),
                model_alias,
            )
        model_alias = DOCTOR_OPENWEIGHT_MODEL

    phase_model = _phase_model(phase)
    phase_base = _PHASE_BASE_URLS.get(phase, DOCTOR_OPENWEIGHT_BASE_URL).rstrip("/")

    # Try the phase base first, then the local failover rungs.
    chain: list[tuple[str, str]] = [(phase_base, phase_model)]
    chain.extend(_fallback_rungs(phase_base, phase_model))

    last_base, last_reason = phase_base, ""
    for idx, (base, model) in enumerate(chain):
        # Pass the model ONLY as the keyword: _invoke_openweight's second
        # positional parameter is the alias label, not the model id.
        ok, text = _invoke_openweight(prompt, model_alias, base_url=base, model=model)
        if ok and text.strip():
            if idx == 0:
                log.info("doctor phase=%s answered by %s model=%s", phase, base, model)
            else:
                log.warning(
                    "doctor phase=%s FAILOVER: %s (model=%s) answered after %s failed (%s)",
                    phase,
                    base,
                    model,
                    last_base,
                    str(last_reason)[:160],
                )
                text = (
                    f"[doctor failover: {base} model={model} after {last_base} "
                    f"failed: {str(last_reason)[:160]}]\n{text}"
                )
            return True, text

        last_base, last_reason = base, text
        kind = _classify_attempt_failure(text)
        if kind not in _FAILOVER_ELIGIBLE_REASONS:
            log.warning(
                "doctor phase=%s endpoint %s failed non-retryably (%s): %s",
                phase,
                base,
                kind,
                str(text)[:200],
            )
            break
        log.warning(
            "doctor phase=%s endpoint %s failed (%s) — trying next local endpoint",
            phase,
            base,
            kind,
        )

    return False, (f"model ({model_alias}) failed on every local endpoint; last {last_base}: {last_reason}")


_CODE_FENCE_RE = re.compile(r"```(?:json)?\s*([\s\S]*?)```", re.IGNORECASE)
_JSON_RE = re.compile(r"\{[\s\S]*\}", re.MULTILINE)


def parse_doctor_response(text: str) -> dict | None:
    """Extract the JSON action object from the LLM output. Tolerates markdown
    code fences and prose wrappers."""
    if not text:
        return None
    # First try: whole text is JSON (response_format=json_object case)
    try:
        obj = json.loads(text.strip())
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    # Second: strip markdown fences
    for cand in _CODE_FENCE_RE.findall(text):
        try:
            obj = json.loads(cand.strip())
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue
    # Third: largest braced span, prefer those with action_type
    candidates = _JSON_RE.findall(text)
    best = None
    for cand in candidates:
        try:
            obj = json.loads(cand)
            if not isinstance(obj, dict):
                continue
            if "action_type" in obj:
                return obj
            best = best or obj
        except Exception:
            continue
    return best


# ───────────────────────── Playbook actions ─────────────────────────
def _ssh_rate_ok(host: str) -> bool:
    """The SSH rate cap and the per-host cooldown, read from DURABLE state.

    2026-10-22: both counters lived in module dicts, so restarting the pod gave
    the doctor a fresh SSH budget on every host. They are now timestamps in the
    state store; the deque/dict mirrors are updated alongside so anything still
    reading them sees the same thing, and the deque is trimmed on WRITE
    (`record_ssh_action`) because nothing here reads it.
    """
    now = time.time()
    if not STATE.loaded_ok or not STATE.persist_ok:
        # A rate limiter that cannot record what it has already allowed is not a
        # limiter: after a restart it would hand out a fresh budget. Fail closed
        # until the store can write again. The remedy is the store's, not this
        # function's, so the message says what failed rather than what to retry.
        log.warning("SSH rate limiter unavailable (safety state is not persisted) — skipping")
        return False
    # Read the cap from the DURABLE count, not from the deque. The deque is a
    # write-only mirror (nothing reads it; it exists so the pre-existing attribute
    # names keep working), so re-syncing it here would cost a second store read on
    # every check to keep a value nobody consults up to date — and trimming it here
    # would be the only thing bounding its growth, which is why it is trimmed on
    # write instead.
    total = ssh_actions_total(SSH_RATE_WINDOW_SEC)
    if total >= MAX_SSH_PER_HOUR:
        log.warning("SSH rate cap %d/h reached (durable: %d) — skipping", MAX_SSH_PER_HOUR, total)
        return False
    since = host_in_cooldown(host, now)
    if since < MIN_HOST_COOLDOWN:
        log.warning("host %s cooldown %ds remaining (durable) — skipping", host, int(MIN_HOST_COOLDOWN - since))
        return False
    return True


def _record_ssh_action(host: str):
    record_ssh_action(host)


def action_restart_service(target_host: str | None, service: str | None) -> str:
    if not target_host or not service:
        return "restart_service missing target_host or service"
    if not ALLOWED_SERVICE_RE.match(service):
        return f"service {service!r} not in allowlist"
    # One table, blocklist first, then the allowlist, then the entry's own
    # configuration. 2026-10-22 this read FLEET_HOSTS before the cixmini branch,
    # so the whole cixmini path was unreachable and every cixmini restart answered
    # `unknown host 'cixmini'`. See `doctor_gates.resolve_restart_target`.
    target, refusal = doctor_gates.resolve_restart_target(target_host)
    if target is None:
        return refusal
    host = target["host"]
    ip = target["address"]
    if not _ssh_rate_ok(host):
        return f"rate-limited on {host}"
    # A restart kills a running service on a fleet host: it waits for an operator
    # like every other destructive action, and the approval is scoped to THIS host
    # and THIS service.
    blocked = APPROVALS.require("restart_service", host=host, service=service)
    if blocked:
        log.warning("restart refused on %s: %s", host, blocked)
        return f"REFUSED: {blocked}"
    if DRY_RUN:
        _record_ssh_action(host)
        return f"DRY_RUN: would restart {service} on {host} ({ip})"
    # Reserve before the effect. A timeout may still have restarted the unit;
    # every attempted restart consumes budget, including an interrupted process.
    _record_ssh_action(host)
    if not STATE.persist_ok:
        return "REFUSED: restart budget could not be persisted"
    sudo_password = os.environ.get("DOCTOR_SUDO_PASSWORD")
    if target["auth"] == "password":
        cixmini_password = os.environ.get("CIXMINI_SSH_PASSWORD", "")
        cmd = [
            "sshpass",
            "-p",
            cixmini_password,
            "ssh",
            "-o",
            "PubkeyAuthentication=no",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "ConnectTimeout=10",
            f"{target['user']}@{ip}",
            f"printf '%s\\n' {shlex.quote(cixmini_password)} | sudo -S systemctl restart {service}",
        ]
    else:
        restart_cmd = f"sudo -n systemctl restart {service}"
        if sudo_password:
            restart_cmd = (
                f"{restart_cmd} 2>/dev/null || "
                f"printf '%s\\n' {shlex.quote(sudo_password)} | sudo -S systemctl restart {service}"
            )
        cmd = [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "ConnectTimeout=10",
            f"{target['user']}@{ip}",
            restart_cmd,
        ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=SSH_TIMEOUT)
        if r.returncode == 0:
            return f"restarted {service} on {host}"
        return f"restart FAILED on {host}: {r.stderr[-200:]}"
    except subprocess.TimeoutExpired:
        return f"SSH timeout to {host}"


def action_resubmit_jobs(base_kind: str | None, max_resubmits: int = MAX_BULK_RESUBMITS_PER_ACTION) -> str:
    if not base_kind:
        return "resubmit_jobs missing base_kind"
    code, resp = _http("GET", "/v1/jobs?status=failed&limit=200")
    if code != 200 or not resp:
        return f"could not fetch failed jobs (code={code})"
    failed = resp.get("jobs", [])
    strip_re = re.compile(r"\s*\[.*?\]\s*$")
    target_ids = [
        j["id"] for j in failed if strip_re.sub("", j.get("kind", "")).strip() == base_kind and not dlq_protected(j)
    ][: max(0, min(MAX_BULK_RESUBMITS_PER_ACTION, int(max_resubmits)))]
    if not target_ids:
        return f"no failed jobs of base_kind={base_kind}"
    if DRY_RUN:
        return f"DRY_RUN: would requeue {len(target_ids)} jobs of {base_kind}"
    code2, resp2 = _http(
        "POST",
        "/v1/admin/jobs/requeue",
        {
            "job_ids": target_ids,
            "reason": f"doctor-resubmit:{base_kind}",
        },
    )
    return f"requeued {len(target_ids)} jobs of {base_kind} (api={code2})"


def _children_of(parent_ids: list[str], limit: int = 200) -> list[str]:
    """Ids of jobs whose parent_job_id is one of `parent_ids`.

    Uses the bus's parent_job_id filter (one query per parent) rather than
    pulling the whole jobs table. Best-effort: an unreachable bus returns no
    children, which leaves the purge exactly as it was before this check.
    """
    found: list[str] = []
    for pid in parent_ids[:20]:
        code, resp = _http("GET", f"/v1/jobs?parent_job_id={pid}&limit={int(limit)}", timeout=10.0)
        if code != 200 or not resp:
            continue
        for child in resp.get("jobs", []) or []:
            cid = child.get("id")
            if cid and cid not in found and not dlq_protected(child):
                found.append(cid)
    return found


def action_cancel_jobs(base_kind: str | None, max_cancels: int = 20) -> str:
    if not base_kind:
        return "cancel_jobs missing base_kind"
    code, resp = _http("GET", "/v1/jobs?status=failed&limit=200")
    if code != 200 or not resp:
        return f"could not fetch failed jobs (code={code})"
    failed = resp.get("jobs", [])
    strip_re = re.compile(r"\s*\[.*?\]\s*$")
    target_ids = [
        j["id"] for j in failed if strip_re.sub("", j.get("kind", "")).strip() == base_kind and not dlq_protected(j)
    ][: max(0, min(20, int(max_cancels)))]
    if not target_ids:
        return f"no failed jobs of base_kind={base_kind}"
    if DRY_RUN:
        return f"DRY_RUN: would cancel {len(target_ids)} jobs of {base_kind}"
    # A purge deletes the parent row outright, so its children (parent_job_id)
    # would be orphaned in the queue with nothing to account for them. Pull
    # every child of a purged parent into the same purge. Children are purged
    # with the same status filter, so an in-flight or historical child of a
    # failed parent is left in place rather than deleted out from under a worker.
    child_ids = _children_of(target_ids)
    purge_ids = target_ids + [cid for cid in child_ids if cid not in target_ids]
    # 2026-10-22: this is a HARD DELETE of up to 20 jobs and every child job, and
    # it used to happen at whatever confidence the model claimed. It now waits for
    # an operator, and the approval is scoped to THIS id set — approving one
    # cancel never approves the next.
    blocked = APPROVALS.require("cancel_jobs", base_kind=base_kind, job_ids=sorted(purge_ids))
    if blocked:
        log.warning("cancel refused for %s: %s", base_kind, blocked)
        return f"REFUSED: {blocked}"
    # Failed jobs are terminal and cannot be PATCHed, so a cancel removes them
    # from the queue with the bus purge (failed status only, explicit ids).
    # This is a HARD DELETE (hive.bus.routers.admin.purge_jobs -> DELETE), not
    # a failed->cancelled transition: the row and its audit log are gone. The
    # ids stay on record in this triage job's result and the broadcast.
    # `doctor_gates.purge_request` owns the endpoint/payload pairing, so the
    # `ids` key can only ever go to the route that reads `ids`.
    path, body = doctor_gates.purge_request("cancel_failed", purge_ids, f"doctor-cancel:{base_kind}")
    code2, resp2 = _http("POST", path, body)
    outcome = doctor_gates.purge_result("cancel_failed", resp2, purge_ids)
    broadcast_doctor_message(
        "doctor.cancel",
        {
            "host": AGENT_HOST,
            "base_kind": base_kind,
            "job_ids": purge_ids,
            "reason": "doctor-determined-structural-failure",
            "api": code2,
            "removed": outcome["removed"],
            "skipped": outcome.get("skipped", []),
        },
    )
    # Report the CONFIRMED count against the requested count. `removed` counts only
    # ids the route named as deleted, so a partial delete reads as a partial
    # delete rather than as a success.
    summary = (
        f"cancelled {outcome['removed']}/{len(purge_ids)} "
        f"failed jobs of {base_kind} (api={code2}): {', '.join(purge_ids)}"
    )
    if outcome["partial"]:
        how_many = len(outcome.get("skipped", []))
        # `attributed` means the bus confirmed a COUNT and named no ids, so the ids
        # are not knowable; print the count and say so rather than list jobs the
        # response never identified.
        detail = (
            f"{how_many} job(s) not deleted (the bus named no ids, so which is not reported)"
            if outcome.get("attributed")
            else f"{how_many} job(s) not deleted: {', '.join(outcome.get('skipped', []))}"
        )
        summary += f"; PARTIAL: {detail}"
    if code2 != 200:
        summary += f"; api did not confirm the purge (status={code2})"
    return summary


# Fleet harness kinds (hive/harness/base.py:46) minus non-workers. Codex runs
# only on explicit request, never as a default target, and 'doctor' must never
# be here: a doctor-eligible job would be re-claimed by this same pool and loop.
# This is an ALLOWLIST of worker kinds, so an LLM-supplied job kind
# ("fix:hive-one") or "doctor" cannot leave the job queued with nothing able
# to claim it.
DEFAULT_RELEASE_KINDS = ["zeroclaw", "opencode"]
RELEASABLE_KINDS = frozenset({"zeroclaw", "opencode", "zoder", "claude"})

# Failures no retry can fix: the submitter has to create the branch or map the
# repo first. Escalated without an LLM call.
# NOTE: no_repo_mapping is NOT here. It is deployment-dependent (the worker's
# REPO_MAP vs the bus catalog), so it is classified per job below via
# hive.bus.domain.jobs.is_unresolvable_release -- escalating every occurrence
# would kill jobs a stale worker merely declined (audit BLOCKING-1).
_UNSCHEDULABLE_RE = re.compile(
    r"Remote branch \S+ not found|couldn't find remote ref"
    r"|unknown repo|could not resolve repo",
    re.IGNORECASE,
)


def _repo_catalog_filter():
    """Loader for hive.bus.domain.jobs.repo_in_catalog (patchable in tests).

    The doctor is otherwise stdlib-only, so the hive import is deferred: the
    signature phase only pays for it when a failed result mentions
    no_repo_mapping. An unimportable catalog falls back to repo_in_catalog's
    own fail-open behaviour (unknown repo -> might be present -> retry)."""
    try:
        from hive.bus.domain.jobs import repo_in_catalog
    except Exception:  # noqa: BLE001 - catalog unavailable: never kill the job
        return lambda repo: True
    return repo_in_catalog


repo_in_catalog = _repo_catalog_filter()


def _is_unschedulable(failed: list[dict]) -> str | None:
    """The unschedulable reason in `failed`, or None.

    no_repo_mapping is counted ONLY when the job's repo is genuinely absent
    from the bus catalog. A catalog-present repo means a STALE WORKER: the job
    must be requeued/routed, never escalated (hive.bus.domain.jobs:135-157)."""
    for fj in failed[:8]:
        result = fj.get("result") or ""
        if isinstance(result, dict):
            try:
                result = json.dumps(result)
            except Exception:
                result = str(result)
        text = str(result)[:3000]
        match = _UNSCHEDULABLE_RE.search(text)
        if match:
            return match.group(0)
        if re.search(r"no_repo_mapping", text, re.IGNORECASE):
            if not repo_in_catalog(fj.get("repo")):
                return "no_repo_mapping"
    return None


def _release_job(job_id: str, release_eligible_kinds: list | None) -> tuple[str, bool]:
    """Release `job_id` to the queue. Returns (summary, bus_accepted).

    The boolean is the AUTHORITY: main() skips the terminal PATCH only when the
    bus actually accepted the requeue. A rejected PATCH (e.g. 404: the job is
    gone or already terminal) must report False so the caller still terminates
    the job instead of leaving it in 'running' until its lease expires."""
    if release_eligible_kinds and isinstance(release_eligible_kinds, list):
        # Only real worker kinds: an LLM-supplied job kind or "doctor" would
        # leave the job queued with nothing able to claim it.
        new_kinds = [k for k in release_eligible_kinds if k in RELEASABLE_KINDS]
    else:
        new_kinds = []
    # If LLM didn't give us anything usable, broaden to all workers.
    if not new_kinds:
        new_kinds = list(DEFAULT_RELEASE_KINDS)
    if DRY_RUN:
        return f"DRY_RUN: would release {job_id[:12]} with eligible_kinds={new_kinds}", True
    # Bus contract: eligible_kinds can only be updated on queued/offered jobs.
    # Our job is currently 'running' (we patched it on claim), so we must
    # release in TWO steps: (1) status running→queued (clears claimed_by),
    # (2) PATCH eligible_kinds on the now-queued job.
    code1, resp1 = _http("PATCH", f"/v1/jobs/{job_id}", {"status": "queued", "claimed_by": _urn})
    if code1 not in (200, 201, 204):
        return f"release step 1 FAILED code={code1} resp={str(resp1)[:200]}", False
    code2, resp2 = _http("PATCH", f"/v1/jobs/{job_id}", {"status": "queued", "eligible_kinds": new_kinds})
    if code2 in (200, 201, 204):
        return f"released {job_id[:12]} → queued, eligible_kinds={new_kinds}", True
    # Step 1 already succeeded so the job IS released; the kind retag just failed.
    return (f"released {job_id[:12]} → queued (eligible_kinds retag failed code={code2} resp={str(resp2)[:200]})"), True


def action_release_to_queue(job_id: str, release_eligible_kinds: list | None) -> str:
    """Return the claimed job back to the regular queue so other workers
    (NOT doctor) can retry it. Implements the user's directive:
    'once doctor grabs a fail, it needs to move it into its queue until it
    releases it back into the regular queue'.

    CRITICAL: release MUST exclude 'doctor' from eligible_kinds, otherwise
    the doctor pool will re-claim the job and trigger a loop (caught by
    loop detection but burns LLM calls and human attention)."""
    return _release_job(job_id, release_eligible_kinds)[0]


def apply_release(
    job_id: str, is_explicit_triage: bool, base_kind: str, release_eligible_kinds: list | None
) -> tuple[str, bool]:
    """Carry out a release_to_queue decision. Returns (summary, released).

    On a triage:<kind> wrapper, "release" means re-run the ORIGINAL failed
    job(s) of that kind; releasing the wrapper itself would leave the failed
    jobs untouched and park the wrapper in the queue. Only a job the doctor
    claimed directly is handed back to the regular queue.

    ``released`` is True ONLY when the job actually left the doctor's hands
    (the bus accepted the requeue). main() skips the terminal PATCH when it is
    True, so a failed release must report False or the job hangs in 'running'
    until its claim lease expires and the cycle repeats."""
    if is_explicit_triage:
        # The wrapper is answered by the triage result itself; the failed jobs
        # were requeued, so the wrapper is finished -- never "released".
        text = action_resubmit_jobs(base_kind)
        return text, False
    return _release_job(job_id, release_eligible_kinds)


def action_dispatch_codex_fix(codex_task: str | None, parent_job_id: str) -> str:
    """Submit a codex sub-job for code-level fix. Requires registered orchestrator submitter."""
    if not codex_task:
        return "dispatch_codex_fix missing codex_task"
    if len(codex_task) > 4000:
        codex_task = codex_task[:4000]
    blocked = APPROVALS.require("dispatch_codex_fix", task=codex_task, parent_job_id=parent_job_id)
    if blocked:
        return f"REFUSED: {blocked}"
    body = {
        "kind": f"doctor:codex-fix:{int(time.time())}",
        "description": codex_task,
        "submitter_urn": _urn,
        "parent_job_id": parent_job_id,
        # codex-first (operator 2026-06-03): the fix runs on a codex-capable
        # zeroclaw worker (ChatGPT-OAuth, NO API key). hard:codex gates it to
        # codex workers only AND stops the doctor (no codex cap) from re-claiming
        # its own dispatch — the self-loop that thrashed deepseek.
        "eligible_kinds": ["zeroclaw"],
        "max_cost_tier": "C",
        "priority": 80,
        # 2026-10-17: Codex is never started unattended (operator 2026-10-10).
        # Route the fix to a zoder worker on the escalation ladder instead;
        # hard:verified-git-delivery also keeps the doctor (no such cap) and the
        # minos codex-runtime workers from claiming it.
        "required_capabilities": ["hard:verified-git-delivery", "route:flash"],
    }
    if DRY_RUN:
        return f"DRY_RUN: would submit codex sub-job ({len(codex_task)} chars)"
    code, resp = _http("POST", "/v1/jobs", body)
    if code in (200, 201) and resp:
        return f"codex sub-job submitted id={resp.get('id', '?')[:12]}"
    return f"codex submit FAILED code={code} resp={str(resp)[:200]}"


# ── Token-mismatch auto-fix (2026-05-27) ──
def _extract_host_from_job(job: dict, failed_jobs: list[dict]) -> str:
    """Extract offending hostname from URN patterns in claimed_by or kind fields.
    URN format: urn:agent:<kind>:<host>:<session_id>"""
    # Check the triage job itself
    for field in ("claimed_by", "submitter_urn"):
        val = job.get(field) or ""
        m = re.search(r"urn:agent:\w+:(\w+)", val)
        if m:
            return m.group(1).lower()
    # Check recent failed jobs
    for fj in failed_jobs[:10]:
        for field in ("claimed_by", "submitter_urn"):
            val = fj.get(field) or ""
            m = re.search(r"urn:agent:\w+:(\w+)", val)
            if m:
                return m.group(1).lower()
    # Try extracting from kind (e.g. "zeroclaw[ULTRA]")
    kind = job.get("kind") or ""
    m = re.search(r"\[(\w+)\]", kind)
    if m:
        return m.group(1).lower()
    return ""


def _check_token_fix_rate_limit(host: str) -> bool:
    """True if we're under the rate limit for this host. False = blocked."""
    now = time.time()
    attempts = _token_fix_attempts.setdefault(host.lower(), [])
    attempts[:] = [t for t in attempts if (now - t) <= TOKEN_FIX_WINDOW_SEC]
    return len(attempts) < MAX_TOKEN_FIX_PER_HOST


def _record_token_fix_attempt(host: str):
    now = time.time()
    _token_fix_attempts.setdefault(host.lower(), []).append(now)


def _build_token_fix_codex_task(offending_host: str) -> str:
    """Build an imperative codex task to fix gateway token mismatch on a host.
    Includes OS detection, auth method selection, and health-check verification."""
    host_info = ALL_FLEET_HOSTS.get(offending_host.lower(), {})
    ip = host_info.get("ip", offending_host)
    os_type = host_info.get("os", "linux")
    auth = host_info.get("auth", "passwordless")
    user = host_info.get("user", "jasonperlow")
    host_upper = offending_host.upper()

    # Build SSH command prefix based on auth method
    if auth == "sshpass":
        password_var = f"{offending_host.upper()}_SSH_PASSWORD"
        ssh_prefix = (
            f'PASSWORD="${{{password_var}:-$DOCTOR_SSH_PASSWORD}}"; '
            f'if [ -n "$PASSWORD" ]; then '
            f'sshpass -p "$PASSWORD" ssh -o PubkeyAuthentication=no '
            f"-o StrictHostKeyChecking=no -o ConnectTimeout=10 {user}@{ip}; "
            f"else ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 {user}@{ip}; fi"
        )
    else:
        ssh_prefix = f"ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 {user}@{ip}"

    # Build restart commands based on OS
    if os_type == "macos":
        restart_gateway = (
            f"launchctl bootout gui/$(id -u {user}) "
            f"~/Library/LaunchAgents/com.zeroclaw.gateway.plist 2>/dev/null || true; "
            f"sleep 1; "
            f"launchctl bootstrap gui/$(id -u {user}) "
            f"~/Library/LaunchAgents/com.zeroclaw.gateway.plist"
        )
        restart_worker = (
            "pkill -f zeroclaw-wss-worker 2>/dev/null || true; sleep 1; nohup ~/zeroclaw-wss-worker &>/dev/null &"
        )
        restart_cmd = f"{restart_gateway}; {restart_worker}"
    else:
        restart_cmd = "sudo systemctl restart zeroclaw-gateway; for i in 1 2 3 4; do sudo systemctl restart zeroclaw-wss-worker@$i; done"

    return f"""FLEET INFRA FIX: Gateway token mismatch on {offending_host} ({ip}, {os_type} via {auth})

DIAGNOSIS: Worker on {offending_host} is failing with 'no token for host' / 'pair gateway first via POST /pair'.
The gateway-tokens.json 127.0.0.1 entry has drifted from the gateway's paired_tokens in config.toml.

FLEET LAN TOKEN: taken from $FLEET_LAN_TOKEN on the executing host.
This job description is persisted in the jobs table and the event log, so the
token value is deliberately NOT written here -- export it in the shell that
runs the steps below:  export FLEET_LAN_TOKEN=<value from ~/.api_keys_master.json>

FIX STEPS — execute in order via SSH to {offending_host} ({ip}):

STEP 1 — SSH to host:
  {ssh_prefix}

STEP 2 — Fix gateway-tokens.json (MUST have both 127.0.0.1 AND hostname entries):
  cat > /tmp/gateway-tokens-fix.json << 'TOKENS_EOF'
{{
  "127.0.0.1": "'"$FLEET_LAN_TOKEN"'",
  "{host_upper}": "'"$FLEET_LAN_TOKEN"'"
}}
TOKENS_EOF
  # Merge with existing entries (preserve other host tokens already in file)
  python3 -c "
import json, sys
with open('/tmp/gateway-tokens-fix.json') as f:
    new = json.load(f)
try:
    with open(os.path.expanduser('~/.zeroclaw/gateway-tokens.json')) as f:
        old = json.load(f)
except Exception:
    old = {{}}
old.update(new)
with open(os.path.expanduser('~/.zeroclaw/gateway-tokens.json'), 'w') as f:
    json.dump(old, f, indent=2)
print('gateway-tokens.json updated')
"
  # Verify: cat ~/.zeroclaw/gateway-tokens.json

STEP 3 — Fix config.toml [gateway] paired_tokens:
  python3 -c "
import sys, os
path = os.path.expanduser('~/.zeroclaw/config.toml')
with open(path) as f:
    content = f.read()
# Ensure [gateway] section exists with paired_tokens
if '[gateway]' not in content:
    content += '\\n[gateway]\\npaired_tokens = [\\'' + os.environ['FLEET_LAN_TOKEN'] + '\\']\\n'
elif 'paired_tokens' not in content.split('[gateway]')[1].split('[')[0] if '[gateway]' in content else '':
    content = content.replace('[gateway]', '[gateway]\\npaired_tokens = [\\'' + os.environ['FLEET_LAN_TOKEN'] + '\\']')
else:
    import re
    content = re.sub(
        r'paired_tokens\\s*=\\s*\\[.*?\\]',
        'paired_tokens = [\\'' + os.environ['FLEET_LAN_TOKEN'] + '\\']',
        content
    )
with open(path, 'w') as f:
    f.write(content)
print('config.toml updated')
"

STEP 4 — Restart services:
  {restart_cmd}

STEP 5 — Verify health (CRITICAL — must return paired:true):
  for attempt in 1 2 3 4 5; do
    result=$(curl -s -m 5 http://127.0.0.1:42617/health 2>/dev/null)
    if echo "$result" | grep -q '"paired":true'; then
      echo "HEALTH CHECK PASSED: $result"
      break
    fi
    echo "attempt $attempt: paired not yet true, waiting..."
    sleep 2
  done
  if ! echo "$result" | grep -q '"paired":true'; then
    echo "CRITICAL: health check FAILED after fix — escalate to human"
    exit 1
  fi

CONSTRAINTS (READ CAREFULLY):
- DO NOT touch ~/.codex/auth.json or ~/.api_keys_master.json under ANY circumstance.
- Use sshpass with PubkeyAuthentication=no for MEDUSA and HYDRA.
- Mac hosts (ULTRA, STUDIO) use launchctl + nohup, NOT systemctl.
- Linux hosts use systemctl.
- If ANY step fails, report the exact error and escalate."""


def _match_known_signature(job: dict, base_kind: str, failed: list[dict]) -> dict | None:
    """Check if the triage job or its associated failures match a known,
    auto-fixable signature. Returns an action dict (bypassing the LLM)
    or None if no signature matches.

    Current signatures:
      - token_mismatch: 'no token for host' / 'pair gateway first via POST /pair'
    """
    # Build searchable text from job description + failed-job results.
    # A doctor-authored job must NOT be judged on its own stored failure: a
    # doctor:* job that failed with a transient error would match its own
    # "ConnectionClosed" and release itself, skipping the terminal PATCH.
    own_failures = [
        fj
        for fj in failed[:8]
        if not (
            str(fj.get("kind") or "").startswith(DOCTOR_CLAIM_KIND_PREFIXES)
            or (_urn and str(fj.get("claimed_by") or "") == _urn)
        )
    ]
    search_text = (job.get("description") or "")[:10000]
    for fj in own_failures:
        result = fj.get("result") or ""
        if isinstance(result, dict):
            try:
                result = json.dumps(result)
            except Exception:
                result = str(result)
        search_text += " " + str(result)[:3000]

    unsched = _is_unschedulable(own_failures)
    if unsched:
        return {
            "action_type": "escalate",
            "confidence": 1.0,
            "reason": f"unschedulable ({unsched}): the submitter must "
            "create the branch or map the repo; a retry cannot fix it; no LLM",
        }

    # Transient gateway/session error (ws-400 / ConnectionClosed) — e.g. a worker
    # restart severed the WSS mid-job. Release to queue WITHOUT burning codex-cli.
    _transient = (
        "ws_exception" in search_text
        or "ConnectionClosed" in search_text
        or "reason_phrase='Bad Request'" in search_text
    )
    _structural = any(
        x in search_text for x in ("Traceback", "SyntaxError", "no_workspace", "clone_failed", "no_zeroclaw_handler")
    )
    if _transient and not _structural:
        return {
            "action_type": "release_to_queue",
            "confidence": 0.9,
            "reason": "transient ws/gateway error (ws-400/ConnectionClosed) — retry; no LLM",
        }

    for pattern, sig_name in KNOWN_FAILURE_SIGNATURES:
        if not pattern.search(search_text):
            continue

        offending_host = _extract_host_from_job(job, failed)
        if not offending_host:
            log.info("signature matched '%s' but could not extract host — falling through to LLM diagnosis", sig_name)
            return None

        # Check rate limit
        if not _check_token_fix_rate_limit(offending_host):
            count = len(_token_fix_attempts.get(offending_host.lower(), []))
            log.warning(
                "token-fix rate cap reached for %s (%d in %ds) — escalating",
                offending_host,
                count,
                int(TOKEN_FIX_WINDOW_SEC),
            )
            return {
                "action_type": "escalate",
                "confidence": 1.0,
                "reason": (
                    f"token-fix rate-limit: {count} fixes dispatched for "
                    f"{offending_host} in last {int(TOKEN_FIX_WINDOW_SEC / 3600)}h"
                ),
            }

        # Check host is known
        if offending_host.lower() not in ALL_FLEET_HOSTS:
            log.warning("token-fix: host %s not in ALL_FLEET_HOSTS — falling through to LLM", offending_host)
            return None

        # SECURITY (review C2/H3): fail closed without a configured LAN token. The
        # codex fix rewrites gateway-tokens.json / config.toml paired_tokens; with
        # no FLEET_LAN_TOKEN we would bake an empty token and lock the gateway out.
        if not FLEET_LAN_TOKEN:
            log.warning(
                "token-fix: FLEET_LAN_TOKEN unset — escalating instead of "
                "dispatching a fix that would write an empty token"
            )
            return {
                "action_type": "escalate",
                "confidence": 1.0,
                "reason": (
                    f"token-mismatch on {offending_host} but FLEET_LAN_TOKEN is not configured; cannot safely auto-fix"
                ),
            }

        _record_token_fix_attempt(offending_host)
        log.info(
            "AUTO-DETECT signature=%s host=%s — bypassing LLM, dispatching codex fix directly", sig_name, offending_host
        )

        return {
            "action_type": "dispatch_codex_fix",
            "confidence": 0.95,
            "reason": (
                f"auto-detected signature '{sig_name}': 'no token for host' gateway-auth failure on {offending_host}"
            ),
            "codex_task": _build_token_fix_codex_task(offending_host),
            "target_host": offending_host,
            "signature": sig_name,
            "auto_detected": True,
        }

    return None


# ───────────────────────── Failed-cluster scanner (self-triage) ─────────────────────────
def _base_kind(kind: str) -> str:
    """Strip [tag] suffix to canonicalize the kind."""
    return re.sub(r"\s*\[.*?\]\s*$", "", kind or "").strip()


def scan_for_failure_clusters() -> list[dict]:
    """Inspect /v1/jobs?status=failed; return clusters of N+ failures of same base_kind
    within CLUSTER_WINDOW_SEC, that haven't been auto-triaged within AUTO_TRIAGE_COOLDOWN.
    Returns list of {base_kind, count, sample_job_id}."""
    code, resp = _http("GET", "/v1/jobs?status=failed&limit=500", timeout=20.0)
    if code != 200 or not resp:
        return []
    cutoff = time.time() - CLUSTER_WINDOW_SEC
    counts: dict[str, list[dict]] = {}
    for j in resp.get("jobs", []) or []:
        if (j.get("ended_at") or 0) < cutoff:
            continue
        bk = _base_kind(j.get("kind", ""))
        if not bk or bk.startswith("triage:") or bk.startswith("doctor:"):
            continue
        # Owners who handle their own failures (DLQ_PROTECT_*) are left alone
        # here too, not only in the dead-letter sweep.
        if dlq_protected(j):
            continue
        counts.setdefault(bk, []).append(j)
    clusters = []
    for bk, jobs in counts.items():
        if len(jobs) < CLUSTER_THRESHOLD:
            continue
        # Durable since 2026-10-22: the cooldown lived in a module dict, so a pod
        # restart let every kind be re-triaged immediately.
        if cooldown_active(f"{KEY_COOLDOWN}:{bk}", AUTO_TRIAGE_COOLDOWN):
            continue
        # Also skip if an active triage:<bk> already exists in queued/running
        if _active_triage_exists(bk):
            continue
        clusters.append(
            {
                "base_kind": bk,
                "count": len(jobs),
                "sample_job_id": jobs[0].get("id", ""),
            }
        )
    return clusters


def _active_triage_exists(base_kind: str) -> bool:
    """True if a triage:<base_kind> job is currently queued or running."""
    for status in ("queued", "running"):
        code, resp = _http("GET", f"/v1/jobs?status={status}&limit=200", timeout=10.0)
        if code != 200 or not resp:
            continue
        target = f"triage:{base_kind}"
        for j in resp.get("jobs", []) or []:
            if (j.get("kind") or "").startswith(target):
                # Also check it includes "doctor" in eligible_kinds OR is unrestricted
                ek = j.get("eligible_kinds") or []
                if not ek or "doctor" in ek:
                    return True
    return False


def submit_auto_triage(base_kind: str, count: int, sample_job_id: str) -> str | None:
    """Submit a triage:<base_kind> job targeting eligible_kinds=['doctor']."""
    body = {
        "kind": f"triage:{base_kind}",
        "description": (
            f"HIVE AUTO-TRIAGE — {count} fails of base_kind={base_kind} in last "
            f"{int(CLUSTER_WINDOW_SEC / 3600)}h.\n"
            f"Sample failed job id: {sample_job_id}\n\n"
            f"Doctor: analyze failure pattern, diagnose root cause, dispatch fix "
            f"(codex sub-job, service restart, or resubmit) and clear the failed cluster."
        ),
        "submitter_urn": _urn,
        "priority": 90,
        "eligible_kinds": ["doctor"],
        "max_cost_tier": "B",
        "required_capabilities": ["triage"],
    }
    if DRY_RUN:
        log.info("DRY_RUN: would auto-submit triage:%s (count=%d)", base_kind, count)
        return None
    note_triage(base_kind)
    if not STATE.loaded_ok or not STATE.persist_ok:
        log.warning("auto-triage refused: cooldown could not be persisted")
        return None
    code, resp = _http("POST", "/v1/jobs", body, timeout=15.0)
    if code in (200, 201) and resp:
        jid = resp.get("id", "")
        log.info("auto-triage submitted id=%s base_kind=%s count=%d", jid[:12], base_kind, count)
        return jid
    log.warning("auto-triage submit failed code=%s base_kind=%s resp=%s", code, base_kind, str(resp)[:200])
    return None


def broadcast_doctor_message(topic: str, payload: dict):
    """Leave a message on the hive bus so other Claudes/agents can see what
    the doctor is doing. Non-blocking; failures logged but ignored.

    topic examples:
      'doctor.scan'    — completed a cluster scan
      'doctor.triage'  — taking action on a triage job
      'doctor.fix'     — dispatched a codex fix
      'doctor.resubmit' — resubmitted jobs
    """
    body = {
        "from_urn": _urn,
        "to_urn": None,  # broadcast
        "topic": topic,
        "payload": payload,
    }
    code, _ = _http("POST", "/v1/messages", body, timeout=5.0)
    if code not in (200, 201, 204):
        log.debug("broadcast %s failed code=%s", topic, code)


# ───────────────────────── Triage loop ─────────────────────────
def execute_sop_decision(job: dict) -> dict:
    """Interpret the versioned decision envelope; never ask a model to execute prose.

    Payload approval flags carry no authority. Destructive helpers require their
    own exact approval in the local store, including delegated fix tasks.
    """
    try:
        envelope = json.loads(job.get("description") or "")
        if not isinstance(envelope, dict) or envelope.get("version") != 1:
            raise ValueError("unsupported decision envelope")
        action = envelope["action"]
        original = envelope["job_id"]
        if not isinstance(action, dict) or not isinstance(original, str):
            raise ValueError("missing action or original job id")
        kind = action.get("action")
        if kind in {"release_to_queue", "dispatch_codex_fix"} and not original:
            raise ValueError("missing original job id")
        if kind == "resubmit_jobs":
            text = action_resubmit_jobs(action["base_kind"], action["max"])
        elif kind == "cancel_jobs":
            text = action_cancel_jobs(action["base_kind"], action["max"])
        elif kind == "restart_service":
            text = action_restart_service(action["host"], action["service"])
        elif kind == "dispatch_codex_fix":
            text = action_dispatch_codex_fix(action["task"], original)
        elif kind in {"release_to_queue", "dlq_remove"}:
            target = original if kind == "release_to_queue" else action["job_id"]
            from urllib.parse import quote

            code, current = _http("GET", f"/v1/jobs/{quote(target, safe='')}")
            if code != 200 or not isinstance(current, dict) or dlq_protected(current):
                raise ValueError("original job unavailable or protected")
            if kind == "release_to_queue":
                text = action_release_to_queue(target, action["release_eligible_kinds"])
            else:
                text = remediate_dead_letter(current, "remove", "sop-decision")
        elif kind == "no_action":
            text = "no action"
        else:
            raise ValueError("unknown executor action")
        return {"exit_code": int(text.startswith("REFUSED")), "stdout": text, "action": action}
    except (ValueError, TypeError, KeyError) as error:
        return {"exit_code": 1, "stdout": f"REFUSED: invalid SOP decision ({error})"}


def _doctor_v2_decision(job: dict, base_kind: str, failed: list[dict]) -> doctor_v2.Decision | None:
    """Return a deterministic v2 decision from the failed-job evidence.

    The current doctor remains the shadow comparator and executor until the
    operator sets DOCTOR_V2_MODE=enforce.  Prior triage results are included
    only as evidence; the v2 module never writes queue state itself.
    """
    if not DOCTOR_V2_RULES:
        return None
    target = failed[0] if failed else job
    prior = []
    for item in failed:
        result = doctor_v2._result(item)
        if isinstance(result.get("signature"), str):
            prior.append({"signature": result["signature"], "action": result.get("action")})
    snapshot = doctor_v2.collect_evidence(target, prior_actions=prior)
    retry_key = _same_signature_retry_key(base_kind, snapshot.signature)
    prior_resubmits = STATE.timestamps(retry_key, SAME_SIGNATURE_RETRY_TTL)
    prior.extend({"signature": snapshot.signature, "action": "release_to_queue"} for _ in prior_resubmits)
    snapshot = doctor_v2.collect_evidence(target, prior_actions=prior)
    decision = doctor_v2.decide_rules(snapshot, DOCTOR_V2_RULES, retry_cap=_max_same_signature_resubmits())
    if decision:
        decision = doctor_v2.Decision(
            decision.action, decision.rule_id,
            f"{decision.reason}; quote={decision.evidence_quote!r}",
            decision.signature, decision.max_retries, decision.retry_scope,
            decision.evidence_quote,
        )
        log.info("doctor v2 mode=%s base_kind=%s rule=%s action=%s signature=%s", DOCTOR_V2_MODE, base_kind,
                 decision.rule_id, decision.action, decision.signature[:12])
    return decision


def _same_signature_retry_key(base_kind: str, signature: str) -> str:
    digest = hashlib.sha256(base_kind.encode("utf-8", "replace")).hexdigest()[:16]
    return f"failures:doctor_v2_resubmit:{digest}:{signature}"


def _record_signature_resubmit(base_kind: str, signature: str) -> None:
    """Persist one successful resubmission against its failure signature."""
    STATE.touch(_same_signature_retry_key(base_kind, signature), SAME_SIGNATURE_RETRY_TTL)


def process_triage_job(job: dict) -> dict:
    """Process either an explicit triage:<base_kind> job OR any other job whose
    eligible_kinds includes 'doctor'.  The user's directive (2026-05-26):
    'ALL FAILS ARE DOCTOR ELIGIBLE' — so the doctor must be able to diagnose
    any failure, not only triage:* wrappers."""
    job_id = job.get("id", "")
    kind = job.get("kind", "")
    desc = job.get("description", "") or ""

    if dlq_protected(job):
        return {"exit_code": 0, "stdout": "protected lane: escalated", "action": {"action_type": "escalate"}}
    if kind == "doctor:execute":
        if not STATE.loaded_ok or not STATE.persist_ok:
            return {"exit_code": 1, "stdout": "REFUSED: safety state unavailable"}
        return execute_sop_decision(job)

    is_explicit_triage = kind.startswith("triage:")
    base_kind = kind[len("triage:") :] if is_explicit_triage else _base_kind(kind)

    m = re.search(r"(\d+)\s+fail", desc, re.IGNORECASE)
    failure_count = int(m.group(1)) if m else 0

    # Loop detection — if we've already cycled this job through the doctor
    # multiple times in the last 30min, escalate without burning another LLM
    # call.  This prevents broken workers (e.g. cixmini with empty config)
    # from creating an infinite release→fail→release cycle.
    now = time.time()
    # Durable since 2026-10-22: the loop counter lived in a module dict, so a pod
    # restart reset it and a job that had already looped three times looked new.
    history = loop_attempts(job_id, window=LOOP_WINDOW_SEC, at=now)
    if not STATE.loaded_ok or not STATE.persist_ok:
        return {"exit_code": 1, "stdout": "REFUSED: safety state unavailable"}
    if len(history) >= LOOP_ESCALATE_THRESHOLD:
        log.warning(
            "LOOP-DETECT job=%s seen %d times in %ds — escalating", job_id[:12], len(history), int(LOOP_WINDOW_SEC)
        )
        return {
            "exit_code": 0,
            "stdout": (
                f"action=escalate; conf=1.00; reason=loop-detect "
                f"({len(history)} doctor-cycles in {int(LOOP_WINDOW_SEC)}s); "
                f"likely a broken worker is reclaiming. Human review required."
            ),
            "action": {"action_type": "escalate", "confidence": 1.0, "reason": f"loop-detect {len(history)} attempts"},
            "base_kind": base_kind,
            "loop_detected": True,
        }

    log.info(
        "processing %s job=%s base_kind=%s failures=%d attempt=%d",
        "TRIAGE" if is_explicit_triage else "DIRECT",
        job_id[:12],
        base_kind,
        failure_count,
        len(history),
    )

    failed = fetch_failed_jobs_for_kind(base_kind, limit=10)
    registry = fetch_agent_registry_summary()

    # Deterministic rules are evaluated before any model call.  In shadow mode
    # they are logged and returned as comparison metadata while the established
    # doctor continues to act; enforce mode uses only the fixed action set.
    v2 = _doctor_v2_decision(job, base_kind, failed)
    if v2 and DOCTOR_V2_MODE == "enforce":
        released = False
        text = "escalated to human"
        if v2.action == "release_to_queue":
            text, released = apply_release(job_id, is_explicit_triage, base_kind, None)
            if released or re.search(r"requeued [1-9]\d* jobs .*\(api=20[01]\)", text):
                _record_signature_resubmit(base_kind, v2.signature)
        note = v2.reason if v2.action == "escalate" else None
        return {
            "exit_code": 0, "stdout": f"action={v2.action}; rule={v2.rule_id}; {text}",
            "action": {"action_type": v2.action, "rule_id": v2.rule_id, "reason": v2.reason,
                       "evidence_quote": v2.evidence_quote, "signature": v2.signature,
                       **({"orchestrator_note": note} if note else {})},
            "base_kind": base_kind, "released_to_queue": released,
            "needs_review": v2.action == "escalate",
            "v2": {"rule_id": v2.rule_id, "signature": v2.signature, "model": None},
        }

    # ── Check known failure signatures BEFORE LLM call ──
    # If we match a known auto-fixable pattern (e.g. token mismatch),
    # dispatch the codex fix directly — no LLM tokens burned.
    known_action = _match_known_signature(job, base_kind, failed)
    if known_action:
        a = known_action.get("action_type", "no_action")
        reason = known_action.get("reason", "")
        log.info("doctor decision (SIGNATURE-MATCH): action=%s reason=%s", a, reason)
        parts = [f"action={a}", f"conf={known_action.get('confidence', 1.0):.2f}", f"reason={reason}"]
        released = False
        if a == "dispatch_codex_fix":
            parts.append(action_dispatch_codex_fix(known_action.get("codex_task"), job_id))
            broadcast_doctor_message(
                "doctor.fix.token_mismatch",
                {
                    "host": AGENT_HOST,
                    "job_id": job_id,
                    "target_host": known_action.get("target_host"),
                    "signature": known_action.get("signature"),
                    "base_kind": base_kind,
                },
            )
            # Re-run the failed work now the offending host is fixed (the
            # original jobs for a triage wrapper, else this job itself).
            text, released = apply_release(
                job_id, is_explicit_triage, base_kind, known_action.get("release_eligible_kinds")
            )
            parts.append(text)
        elif a == "release_to_queue":
            text, released = apply_release(
                job_id, is_explicit_triage, base_kind, known_action.get("release_eligible_kinds")
            )
            parts.append(text)
        elif a == "escalate":
            parts.append("escalated to human")
            broadcast_doctor_message(
                "doctor.escalate.token_mismatch",
                {
                    "host": AGENT_HOST,
                    "job_id": job_id,
                    "reason": reason,
                    "base_kind": base_kind,
                },
            )
        else:
            parts.append(f"unknown auto-action {a!r}")
        return {
            "exit_code": 0,
            "stdout": "; ".join(parts),
            "action": known_action,
            "base_kind": base_kind,
            "failed_count_seen": len(failed),
            "auto_detected": True,
            "released_to_queue": released,
        }

    prompt = build_doctor_prompt(job, base_kind, failed, registry, failure_count)

    # Diagnose = triage phase (cheap open-weight by default). The doctor emits a
    # JSON action; any code FIX is dispatched to open-weight workers, and
    # adversarial REVIEW of that fix runs on codex (DOCTOR_REVIEW_MODEL).
    ok, text = invoke_doctor_agent(prompt, phase="triage")
    if not ok:
        return {
            "exit_code": 2,
            "stdout": "doctor agent failed",
            "stderr": text[:2000],
            "action_type": "escalate",
            "reason": "doctor LLM call failed",
        }

    action = parse_doctor_response(text)
    if not action:
        return {
            "exit_code": 3,
            "stdout": "could not parse doctor response",
            "raw": text[-2000:],
            "action_type": "escalate",
        }

    # Residual release decisions must cite the original evidence and obey the
    # same durable per-signature retry ceiling as deterministic rules.
    residual_evidence = doctor_v2.collect_evidence(failed[0] if failed else job)
    retry_key = _same_signature_retry_key(base_kind, residual_evidence.signature)
    used_retries = len(STATE.timestamps(retry_key, SAME_SIGNATURE_RETRY_TTL))
    if "evidence_quote" in action or "quote" in action or action.get("action_type") == "release_to_queue":
        residual = doctor_v2.validate_llm_decision(
            action, residual_evidence,
            rule_cap=_max_same_signature_resubmits(), used_retries=used_retries,
        )
        if residual.action == "escalate" and action.get("action_type") != "escalate":
            action = {"action_type": "escalate", "reason": residual.reason,
                      "evidence_quote": residual.evidence_quote, "signature": residual.signature,
                      "orchestrator_note": f"{residual.reason}; manual review required",
                      "needs_review": True}

    a = action.get("action_type", "no_action")
    try:
        conf = float(action.get("confidence", 0.0) or 0.0)
    except (ValueError, TypeError):
        conf = 0.0
    reason = action.get("reason", "")
    if a not in {"release_to_queue", "no_action", "escalate"} or (a == "release_to_queue" and not 0.7 <= conf <= 1.0):
        action = {"action_type": "escalate", "reason": reason, "proposed_action": a}
        a = "escalate"
    log.info("doctor decision: action=%s conf=%.2f reason=%s", a, conf, reason)

    parts = [f"action={a}", f"conf={conf:.2f}", f"reason={reason}"]
    released = False  # whether we put the job back to queued (changes return shape)

    if a == "release_to_queue":
        rk = action.get("release_eligible_kinds")
        rk_clean = [k for k in rk if isinstance(k, str) and k] if isinstance(rk, list) else None
        text, released = apply_release(job_id, is_explicit_triage, base_kind, rk_clean)
        parts.append(text)
        if released or re.search(r"requeued [1-9]\d* jobs .*\(api=20[01]\)", text):
            _record_signature_resubmit(base_kind, residual_evidence.signature)
    elif a == "no_action":
        parts.append("no action — triage cleared")
    elif a == "escalate":
        parts.append("escalated to human")
    else:
        parts.append(f"unknown action_type={a!r} — treated as escalate")

    return {
        "exit_code": 0,
        "stdout": "; ".join(parts),
        "action": action,
        "base_kind": base_kind,
        "failed_count_seen": len(failed),
        "released_to_queue": released,
        "needs_review": bool(action.get("needs_review")),
        "v2_shadow": ({"rule_id": v2.rule_id, "action": v2.action, "signature": v2.signature,
                        "old_action": a, "agree": v2.action == a}
                       if v2 else None),
    }


# ───────────────────────── Main loop ─────────────────────────
# ───────────────────── dead-letter queue remediation ─────────────────────
def fetch_dead_letter_jobs(limit: int = 200) -> list[dict]:
    """Jobs in the terminal dead-letter queue, newest first."""
    code, resp = _http("GET", f"/v1/jobs?status=dead-letter&limit={int(limit)}", timeout=15.0)
    if code != 200 or not resp:
        log.warning("fetch_dead_letter http=%s", code)
        return []
    jobs = resp.get("jobs", []) or []
    jobs.sort(key=lambda j: j.get("ended_at") or 0, reverse=True)
    return jobs


def _job_result_dict(job: dict) -> dict:
    """Best-effort parse of a job's result field (may be dict or JSON string)."""
    r = job.get("result")
    if isinstance(r, dict):
        return r
    if isinstance(r, str) and r.strip():
        try:
            d = json.loads(r)
            return d if isinstance(d, dict) else {"raw": r}
        except Exception:
            return {"raw": r}
    return {}


def classify_dead_letter(job: dict) -> tuple[str, str]:
    """Decide remediation for one dead-letter job.

    Returns ``(decision, reason)`` with decision in {resubmit, remove, needs-review}.
    """
    res = _job_result_dict(job)
    decline_reason = str(res.get("last_decline_reason") or "").strip()
    if decline_reason.startswith(DLQ_UNSCHEDULABLE_PREFIXES):
        return "remove", f"unschedulable:{decline_reason}"
    blob = " ".join(str(res.get(k) or "") for k in ("error", "note", "stderr", "stdout", "last_decline_reason", "raw"))
    blob += " " + json.dumps(res.get("last_decline_result") or "")
    if DLQ_TRANSIENT_RE.search(blob):
        return "resubmit", "transient-infra-failure"
    return "needs-review", "unclassified"


def remediate_dead_letter(job: dict, decision: str, reason: str) -> str:
    """Carry out a DLQ decision. No-op (logs intent only) unless DLQ_APPLY=1."""
    if dlq_protected(job):
        return "REFUSED: protected lane"
    if not STATE.loaded_ok or not STATE.persist_ok:
        return "REFUSED: safety state unavailable"
    jid = job.get("id", "")
    if not DLQ_APPLY:
        return f"DLQ_DRY_RUN: would {decision} {jid[:12]} ({reason})"
    if decision == "resubmit":
        code, resp = _http(
            "POST", "/v1/admin/jobs/requeue", {"job_ids": [jid], "reason": f"doctor-dlq-resubmit:{reason}"}
        )
        if code in (200, 201):
            return f"resubmitted {jid[:12]} via admin requeue"
        return f"resubmit FAILED {jid[:12]} code={code} resp={str(resp)[:160]}"
    if decision == "remove":
        # A DLQ remove is a hard delete too, so it waits for an operator like
        # every other destructive action. Scoped to THIS job id.
        blocked = APPROVALS.require("dlq_remove", job_ids=[jid])
        if blocked:
            log.warning("dlq remove refused for %s: %s", jid[:12], blocked)
            return f"REFUSED: {blocked}"
        path, body = doctor_gates.purge_request("dlq_remove", [jid], f"doctor-dlq-remove:{reason}")
        code, resp = _http("POST", path, body)
        if code == 404:
            return f"remove PENDING {jid[:12]}: {path} not on bus yet"
        if code not in (200, 201, 204):
            return f"remove FAILED {jid[:12]} code={code} resp={str(resp)[:160]}"
        # 2026-10-22: this reported `removed <id> via admin purge` for ANY 2xx,
        # including the case where the bus returned ok=[] and put the job in
        # `skipped_not_dead_letter` — i.e. nothing was deleted at all. The result
        # is now read from the route's own shape and says what happened.
        outcome = doctor_gates.purge_result("dlq_remove", resp, [jid])
        if outcome["removed"]:
            return f"removed {jid[:12]} via admin purge"
        if jid in outcome.get("not_found", []):
            return f"remove NO-OP {jid[:12]}: the bus has no such job"
        if jid in outcome.get("skipped", []):
            return f"remove REFUSED {jid[:12]}: not in dead-letter, nothing deleted"
        return f"remove UNCONFIRMED {jid[:12]}: {path} returned 2xx naming no deleted id resp={str(resp)[:120]}"
    code, resp = _http(
        "POST",
        "/v1/admin/jobs/reclassify",
        {"job_ids": [jid], "status": "needs-review", "reason": f"doctor-dlq-needs-review:{reason}"},
    )
    if code in (200, 201, 204):
        return f"re-tagged {jid[:12]} -> needs-review"
    if code == 404:
        return f"needs-review PENDING {jid[:12]}: /v1/admin/jobs/reclassify not on bus yet"
    return f"needs-review FAILED {jid[:12]} code={code} resp={str(resp)[:160]}"


def handle_dead_letter_queue() -> dict:
    """Scan the dead-letter queue, classify each not-yet-seen job, and remediate
    (dry-run unless DLQ_APPLY=1). Broadcasts a doctor.dlq message per decision."""
    now = time.time()
    # The DLQ-seen set is durable since 2026-10-22: it lived in a module dict, so a
    # restart re-decided every dead-letter job the doctor had already decided. The
    # dict below is now only the pre-existing in-process mirror — `dlq_decided()`
    # answers from the store and nothing reads the mirror — so it is trimmed and
    # kept in step, not rehydrated from the store on every cycle.
    for k in [k for k, t in _dlq_seen.items() if now - t > DLQ_SEEN_TTL]:
        _dlq_seen.pop(k, None)
    jobs = fetch_dead_letter_jobs()
    summary = {"scanned": len(jobs), "resubmit": 0, "remove": 0, "needs-review": 0, "skipped_seen": 0}
    acted = 0
    # Two counters, because a cycle has two things to bound: how much it GETS DONE
    # (`acted`) and how much work it DOES (`attempts`). Counting only actions left a
    # DLQ full of refused jobs unbounded — `acted` never advanced, so one cycle
    # classified, attempted and broadcast every job in the queue, forever.
    attempts = 0
    for index, job in enumerate(jobs):
        jid = job.get("id", "")
        if not jid:
            continue
        if dlq_decided(jid, ttl=DLQ_SEEN_TTL):
            summary["skipped_seen"] += 1
            continue
        if dlq_protected(job):
            # Its owner's job: REPORTED, never remediated — which is why it is not
            # recorded as decided. A protected job produces no decision, so
            # recording it would mean the owner stops hearing about it after one
            # cycle, for the whole seen window. The base commit recorded it, which
            # made "reported" mean "reported once".
            summary["protected"] = summary.get("protected", 0) + 1
            log.info(
                "dlq %s kind=%s project=%s -> left for its owner (protected)",
                jid[:12],
                job.get("kind", "?"),
                job.get("project") or "-",
            )
            continue
        if acted >= DLQ_MAX_PER_CYCLE or attempts >= DLQ_MAX_ATTEMPTS_PER_CYCLE:
            # The cap, not a decision: BREAK WITHOUT RECORDING, so the job is
            # decided on a later cycle instead of being marked decided and then
            # never reached again for the whole seen window. `deferred` counts
            # EVERY job this cycle did not reach, so the summary carries the size of
            # the backlog behind the cap rather than the fact that one was hit.
            summary["deferred"] = summary.get("deferred", 0) + (len(jobs) - index)
            break
        attempts += 1
        decision, reason = classify_dead_letter(job)
        outcome = remediate_dead_letter(job, decision, reason)
        summary[decision] = summary.get(decision, 0) + 1
        # Only a real action spends the cycle's budget. Counting refusals against
        # `DLQ_MAX_PER_CYCLE` lets twenty unapproved purges crowd out every job
        # behind them, so the cap measures progress rather than attempts.
        if not outcome.startswith("REFUSED:"):
            acted += 1
        # Recorded AFTER the work, which is the whole ordering — and NOT recorded when
        # the work was REFUSED for want of an approval. A refusal is not a decision:
        # the doctor wanted to act and was not allowed to, and the operator's
        # approval is a future event, so recording it would hide the job from every
        # later cycle for the whole seen window and the operator would approve
        # something that never came back round.
        #
        # A `DLQ_DRY_RUN` IS a decision — the doctor classified the job and chose an
        # action; DLQ_APPLY=0 is the configured behaviour, not a refusal — so it is
        # recorded, which is what stops every cycle re-deciding the same dead-letter
        # jobs when the apply switch is off.
        if not outcome.startswith("REFUSED:"):
            dlq_record_decision(jid, at=now)
        log.info("dlq %s kind=%s -> %s (%s): %s", jid[:12], job.get("kind", "?"), decision, reason, outcome)
        broadcast_doctor_message(
            "doctor.dlq",
            {
                "host": AGENT_HOST,
                "job_id": jid,
                "kind": job.get("kind"),
                "decision": decision,
                "reason": reason,
                "apply": DLQ_APPLY,
                "outcome": outcome,
            },
        )
    return summary


def fetch_needs_review_jobs(limit: int = 200) -> list[dict]:
    """Fetch review backlog for the report-only sweep."""
    code, response = _http("GET", f"/v1/jobs?status=needs-review&limit={int(limit)}", timeout=15.0)
    if code != 200 or not response:
        log.warning("fetch_needs_review http=%s", code)
        return []
    return list(response.get("jobs", []) or [])


def report_needs_review_sweep() -> dict:
    """Classify old needs-review jobs without cancelling, deleting, or retagging."""
    rows = doctor_v2.sweep_needs_review(fetch_needs_review_jobs(), older_than_hours=NEEDS_REVIEW_OLDER_HOURS)
    for row in rows:
        log.info("needs-review report-only: %s", row)
        broadcast_doctor_message("doctor.needs_review", {"host": AGENT_HOST, **row})
    return {"scanned": len(rows), "rows": rows}


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [doctor] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    signal.signal(signal.SIGTERM, _signal)
    signal.signal(signal.SIGINT, _signal)
    log.info("starting on %s url=%s alias=%s dry_run=%s", AGENT_HOST, HIVE_URL, DOCTOR_AGENT_ALIAS, DRY_RUN)
    register()
    last_hb = time.time()
    last_scan = 0.0
    last_dlq = 0.0
    last_needs_review = 0.0

    while _running:
        try:
            now = time.time()
            if now - last_hb >= HEARTBEAT_INTERVAL:
                heartbeat()
                last_hb = now

            # Periodic dead-letter-queue remediation (dry-run unless DLQ_APPLY=1).
            if now - last_dlq >= DLQ_SCAN_INTERVAL:
                last_dlq = now
                try:
                    s = handle_dead_letter_queue()
                    log.info("dlq scan: %s (apply=%s)", s, DLQ_APPLY)
                except Exception as e:
                    log.warning("dlq scan error: %s", e)

            if now - last_needs_review >= NEEDS_REVIEW_SCAN_INTERVAL:
                last_needs_review = now
                try:
                    log.info("needs-review sweep: %s", report_needs_review_sweep())
                except Exception as e:
                    log.warning("needs-review sweep error: %s", e)

            # Periodic failed-cluster scan — emit triage:* jobs targeting doctor.
            if now - last_scan >= SCAN_INTERVAL:
                last_scan = now
                try:
                    clusters = scan_for_failure_clusters()
                    if clusters:
                        log.info("scan: %d failed-clusters above threshold", len(clusters))
                        for c in clusters[:5]:  # cap to avoid burst-submit
                            submit_auto_triage(c["base_kind"], c["count"], c["sample_job_id"])
                        broadcast_doctor_message(
                            "doctor.scan",
                            {
                                "host": AGENT_HOST,
                                "clusters_found": len(clusters),
                                "auto_triaged": min(5, len(clusters)),
                            },
                        )
                except Exception as e:
                    log.warning("scan error: %s", e)

            job = claim_next_job()
            if not job:
                time.sleep(POLL_INTERVAL)
                continue

            job_id = job.get("id", "?")
            kind = job.get("kind", "?")
            log.info("claimed job %s kind=%s", job_id[:12], kind)
            patch_job(job_id, "running")
            broadcast_doctor_message(
                "doctor.triage",
                {
                    "host": AGENT_HOST,
                    "job_id": job_id,
                    "kind": kind,
                    "stage": "claimed",
                },
            )

            t0 = time.time()
            try:
                result = process_triage_job(job)
                dur = int(time.time() - t0)
                result.setdefault("duration_sec", dur)
                if result.get("released_to_queue"):
                    # Doctor already patched job back to queued via the action.
                    # Don't overwrite with done/failed; just log + broadcast.
                    status = "released"
                elif result.get("needs_review"):
                    status = "needs-review"
                    patch_job(job_id, status, result)
                else:
                    # Covers a release that reported False because the bus
                    # rejected it: without this PATCH the job would sit in
                    # 'running' until its claim lease expired and loop.
                    status = "done" if result.get("exit_code", 1) == 0 else "failed"
                    patch_job(job_id, status, result)
                log.info("job %s → %s in %ds: %s", job_id[:12], status, dur, result.get("stdout", "")[:200])
                broadcast_doctor_message(
                    "doctor.triage",
                    {
                        "host": AGENT_HOST,
                        "job_id": job_id,
                        "kind": kind,
                        "stage": "complete",
                        "status": status,
                        "action_type": (result.get("action") or {}).get("action_type"),
                        "summary": result.get("stdout", "")[:240],
                        "duration_sec": dur,
                    },
                )
            except Exception as e:
                log.exception("process_triage_job crashed")
                patch_job(
                    job_id,
                    "failed",
                    {
                        "exit_code": 99,
                        "error": f"{type(e).__name__}: {e}",
                    },
                )

        except KeyboardInterrupt:
            break
        except Exception as e:
            log.exception("loop error: %s", e)
            time.sleep(POLL_INTERVAL)

    log.info("clean shutdown")


if __name__ == "__main__":
    main()
