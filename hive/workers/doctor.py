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
  HIVE_URL                   bus base URL. NO DEFAULT: unset disables the doctor's
                             bus features with one log line (see docs/ENVIRONMENT.md)
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
                             and unknown phases. NO DEFAULT: unset disables model
                             calls (set it to the CERBERUS llama-server base)
  DOCTOR_FIX_BASE_URL        base for the fix phase. NO DEFAULT: set it to the
                             TYDEUS base that serves qwen38
  DOCTOR_OPENWEIGHT_MODEL    model id for the open-weight path (default gpt-oss-20b)
  DOCTOR_OPENWEIGHT_API_KEY  key for open-weight path (falls back to config.toml deepseek)
"""

from __future__ import annotations

import hashlib
import io
import json
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
from typing import Any

# Fleet addresses come from operator config (HIVE_FLEET_INVENTORY), never source.
from hive import review_queue, review_record, review_verdict
from hive.core import fleet_inventory

# SOP phase 3 (2026-10-22): durable safety state and the destructive-action rails.
# Both are stdlib-only siblings, importable without the hive package, so the
# container still runs this module standalone.
from hive.review_queue import pick_reviewers
from hive.workers import doctor_decline_paths as decline_paths
from hive.workers import (
    doctor_gates,
    doctor_head_match,
    doctor_models,
    doctor_remediation,
    doctor_rules,
    doctor_v2,
    loop_outcome,
    remediation,
    retention,
)
from hive.workers.doctor_state import DurableStateStore, state_store_from_env

#: ``approved["basis"]`` that marks an approve signed at a VERDICT-ONLY
#: descendant of the delivered head (see hive/workers/doctor_head_match.py).
BASIS_VERDICT_ONLY = doctor_head_match.VERDICT_ONLY

HIVE_URL = os.environ.get("HIVE_URL", "")
# No baked-in bus address (operator rule 2026-10-29): a literal address in source
# goes stale without anybody noticing. Unset means this doctor cannot reach the
# bus, so every bus-backed feature is disabled with one log line rather than
# crash-looping on a relative URL.
if not HIVE_URL:
    fleet_inventory.warn_once(
        "doctor.HIVE_URL",
        "HIVE_URL is not set: this doctor cannot reach the bus, so claims, "
        "heartbeats and job updates are disabled (set HIVE_URL in its service env)",
    )
# C1: bus transport auth bearer token (env-sourced). The doctor SUBMITS jobs
# (dispatch_codex_fix), so this token must satisfy the bus admin tier if one is
# configured (HIVE_BUS_ADMIN_TOKEN); otherwise worker-tier auth suffices.
HIVE_BUS_TOKEN = os.environ.get("HIVE_BUS_TOKEN", "")
HIVE_BUS_ADMIN_TOKEN = os.environ.get("HIVE_BUS_ADMIN_TOKEN", "")
ZEROCLAW_BIN = os.environ.get("ZEROCLAW_BIN", "/usr/local/bin/zeroclaw")
DOCTOR_AGENT_ALIAS = os.environ.get("DOCTOR_AGENT_ALIAS", "hive_doctor")

# The module logger must exist BEFORE the first ``_env_int`` call below. Those
# calls run at import time and their warning branches read ``log``; a later
# binding meant a malformed env var raised ``NameError: name 'log' is not
# defined`` at import -- the exact crash loop ``_env_int`` exists to prevent
# (review 2026-10-31, high). This is the ONLY binding: ``getLogger`` is
# idempotent, so a second assignment further down was dead duplication.
log = logging.getLogger("doctor")


def _env_int(name: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    """``os.environ[name]`` as an int, defensively: default, bounds, one warning.

    A bare ``int(os.environ.get(...))`` at module scope raises on a typo
    (``HIVE_DOCTOR_REVIEW_SUBMIT_MAX=three``) and takes the WHOLE doctor down at
    import, which is a crash loop, not a config error. An unset or unparsable
    value falls back to the default with a logged warning; ``minimum``/
    ``maximum`` clamp a real value (the warning names it when they do), and a
    clamped ``minimum=0`` cap is how "never submit" is spelled (review
    2026-10-31, high).
    """
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        log.warning("doctor: %s=%r is not an integer; using the default %d", name, raw, default)
        return default
    if minimum is not None and value < minimum:
        log.warning("doctor: %s=%r is below the minimum %d; using %d", name, raw, minimum, minimum)
        return minimum
    if maximum is not None and value > maximum:
        log.warning("doctor: %s=%r is above the maximum %d; using %d", name, raw, maximum, maximum)
        return maximum
    return value


AGENT_HOST = os.environ.get("AGENT_HOST", socket.gethostname().split(".")[0])
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "30"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "15"))
ZEROCLAW_TIMEOUT = _env_int("ZEROCLAW_TIMEOUT", 900)
MAX_SSH_PER_HOUR = _env_int("MAX_SSH_PER_HOUR", 3, minimum=0)
# The window the SSH rate cap is measured over. Named so the durable rate limiter
# and the in-memory deque cannot drift onto different windows.
SSH_RATE_WINDOW_SEC = float(os.environ.get("SSH_RATE_WINDOW_SEC", "3600"))
MIN_HOST_COOLDOWN = _env_int("MIN_HOST_COOLDOWN", 1800)
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"
# Loop-detection: per-job-id (re)fail counter. If the same job_id keeps
# coming back to the doctor (because we released it and some broken worker
# re-failed it), we escalate instead of releasing it again.
_seen_job_attempts: dict[str, list[float]] = {}
LOOP_WINDOW_SEC = float(os.environ.get("LOOP_WINDOW_SEC", "1800"))  # 30 min
LOOP_ESCALATE_THRESHOLD = _env_int("LOOP_ESCALATE_THRESHOLD", 3, minimum=0)

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
DOCTOR_OPENWEIGHT_BASE_URL = os.environ.get("DOCTOR_OPENWEIGHT_BASE_URL", "").rstrip("/")
DOCTOR_OPENWEIGHT_MODEL = os.environ.get("DOCTOR_OPENWEIGHT_MODEL", "gpt-oss-20b")
DOCTOR_OPENWEIGHT_API_KEY = os.environ.get("DOCTOR_OPENWEIGHT_API_KEY", "")
DOCTOR_OPENWEIGHT_TIMEOUT = _env_int("DOCTOR_OPENWEIGHT_TIMEOUT", 120, minimum=1)
# Per-phase bases. Triage and review default to CERBERUS (DOCTOR_OPENWEIGHT_BASE_URL);
# the fix phase defaults to TYDEUS, where qwen38 is served — a phase model id
# is sent to its phase base, so the two must agree. Override any phase to a
# LIGHT model on HYDRA/ACHILLES (e.g. DOCTOR_TRIAGE_BASE_URL=<hydra>:8080/v1).
# No baked-in address for either phase: DOCTOR_FIX_BASE_URL must say where qwen38
# is served. Unset disables the fix phase's model call with one log line.
DOCTOR_FIX_DEFAULT_BASE_URL = ""
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
        "",
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
if DOCTOR_V2_MODE not in {"shadow", "enforce"}:
    # Only "enforce" enforces; anything else (a typo like "enforced") runs shadow.
    logging.getLogger("doctor").warning(
        "DOCTOR_V2_MODE=%r is not 'shadow' or 'enforce'; running v2 in shadow mode", DOCTOR_V2_MODE
    )
DOCTOR_V2_RULES_PATH = os.environ.get(
    "DOCTOR_V2_RULES_PATH",
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "docs", "doctor_v2_rules.toml"),
)
try:
    DOCTOR_V2_RULES = doctor_v2.load_rules(DOCTOR_V2_RULES_PATH)
except (OSError, RuntimeError, ValueError) as _rules_error:
    # v2 is then off (shadow and enforce alike) and the LLM path decides; say so,
    # or DOCTOR_V2_MODE=enforce with a bad rules path looks like it is enforcing.
    logging.getLogger("doctor").warning(
        "doctor v2 rules not loaded from %s (%s); v2 decisions are disabled", DOCTOR_V2_RULES_PATH, _rules_error
    )
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


# ── Terminal-job retention (operator 2026-10-27) ──
# The queue must never hold a backlog of dead jobs ("we should never have a
# queue of 20+ dead jobs"). The doctor sweeps terminal rows on a bounded
# interval; the pure planner + snapshot helpers live in hive.workers.retention.
RETENTION_DRY_RUN = os.environ.get("RETENTION_DRY_RUN", "0") == "1"
RETENTION_DEAD_TTL_SEC = float(os.environ.get("RETENTION_DEAD_TTL_SEC", "3600"))  # dead rows older than 1 h
RETENTION_TRIAGE_TTL_SEC = float(
    os.environ.get("RETENTION_TRIAGE_TTL_SEC", "600")
)  # triage work orders older than 10 min
RETENTION_MAX_DEAD = _env_int("RETENTION_MAX_DEAD", 10, minimum=0)  # hard cap on dead (failed+dead-letter+cancelled)
RETENTION_SWEEP_INTERVAL = float(os.environ.get("RETENTION_SWEEP_INTERVAL", "600"))  # seconds between retention sweeps

# Self-triage scanner config
SCAN_INTERVAL = float(os.environ.get("SCAN_INTERVAL", "300"))  # seconds between failed-cluster scans
CLUSTER_THRESHOLD = _env_int("CLUSTER_THRESHOLD", 1, minimum=0)  # min failures of same base_kind to trigger a triage
CLUSTER_WINDOW_SEC = float(os.environ.get("CLUSTER_WINDOW_SEC", "21600"))  # only look at failures in last 6h
# To avoid re-triaging the same cluster repeatedly, we remember kinds we've already
# auto-triaged within this rolling window:
_recent_auto_triage: dict[str, float] = {}
AUTO_TRIAGE_COOLDOWN = float(os.environ.get("AUTO_TRIAGE_COOLDOWN", "3600"))  # 1h per base_kind
TRIAGE_SEEN_TTL_SEC = float(os.environ.get("TRIAGE_SEEN_TTL_SEC", str(30 * 86400)))  # 30 days

# ── Dead-letter queue (DLQ) remediation ─────────────────────────────────────
# Restored 2026-10-17: the Phase-2 refactor dropped the DLQ sweep that the
# pre-refactor doctor had (commit 5e720f8), so dead-lettered jobs accumulated
# with no automatic remediation.
DLQ_SCAN_INTERVAL = float(os.environ.get("DLQ_SCAN_INTERVAL", str(SCAN_INTERVAL)))
DLQ_APPLY = os.environ.get("DLQ_APPLY", "0") == "1"
DLQ_MAX_PER_CYCLE = _env_int("DLQ_MAX_PER_CYCLE", 20, minimum=0)
# The WORK one dead-letter cycle may do, counted in ATTEMPTS rather than in actions.
# `DLQ_MAX_PER_CYCLE` is a budget for progress: a refusal for want of an approval
# does not spend it, because a job the doctor was not allowed to act on is still a
# job the operator has to hear about. That left the work itself unbounded — every
# refused job in the queue was classified, attempted and broadcast on every cycle,
# which is one pass over the whole dead-letter queue per cycle for a queue that
# cannot drain. This bounds the pass. It is at least `DLQ_MAX_PER_CYCLE`, so the
# action budget still binds first whenever the doctor is actually making progress.
DLQ_MAX_ATTEMPTS_PER_CYCLE = max(DLQ_MAX_PER_CYCLE, _env_int("DLQ_MAX_ATTEMPTS_PER_CYCLE", 50, minimum=0))
# Once a dead-letter job has a logged decision, don't re-decide it every cycle.
_dlq_seen: dict[str, float] = {}
DLQ_SEEN_TTL = float(os.environ.get("DLQ_SEEN_TTL", "21600"))  # 6h
NEEDS_REVIEW_SCAN_INTERVAL = float(os.environ.get("NEEDS_REVIEW_SCAN_INTERVAL", "3600"))
NEEDS_REVIEW_OLDER_HOURS = float(os.environ.get("NEEDS_REVIEW_OLDER_HOURS", "24"))


def _env_seconds(name: str, default: float) -> float:
    """A float from the environment; a blank or malformed value is the default."""
    try:
        return float(os.environ.get(name, "").strip() or default)
    except ValueError:
        return default


# A needs-review row nobody has touched for this long, and for which the doctor has
# no automatic action, is announced ONCE on doctor.needs_review_stuck (and again only
# when its state changes). Zero or negative disables the message.
NEEDS_REVIEW_ESCALATE_SECONDS = _env_seconds("DOCTOR_NEEDS_REVIEW_ESCALATE_SECONDS", 1800.0)
# A first sweep over a long backlog must not flood the channel; the rest follow on
# the next sweeps (a row is only marked once its message went out).
NEEDS_REVIEW_ESCALATE_MAX_PER_SWEEP = 10

# ── Verdict-completion recovery for codex_delivery_failed rows ──────────────
# The codex delivery path can hold a row in needs-review with
# failure_reason ``codex_delivery_failed`` (e.g. "produced no workspace edits")
# even though the review itself SUCCEEDED: the structured verdict JSON is
# sitting in the row's own ``result.tail``. That completion is recoverable by
# pure parsing — no retry, no re-run. ``DOCTOR_VERDICT_COMPLETION_DRY_RUN=1``
# makes the pass log-and-return instead of PATCHing (the durable completion
# marker is still recorded, matching the DLQ dry-run semantics).
VERDICT_COMPLETION_DRY_RUN = os.environ.get("DOCTOR_VERDICT_COMPLETION_DRY_RUN", "0") == "1"
# Key prefix for the durable "completion was attempted/completed for this
# job" record in STATE. One attempt per row per process: a second pass in the
# same process sees the marker and stops; a new process re-attempts at most
# once (its own record starts empty), which is the intended semantics.
KEY_VERDICT_COMPLETION = "verdict_completion"
# The completion is a one-time recovery of a past delivery: the marker only has
# to outlive the doctor process, not the row. The prefix is listed in
# doctor_state.COUNTER_TTLS so its records carry this exact window (an
# unlisted prefix would be pruned at the store's default TTL).
VERDICT_COMPLETION_TTL = 24 * 3600
# A FAILED PATCH must not burn the whole 24h window: it leaves a short backoff
# marker so the next sweep retries (and a restart retries almost immediately),
# while two sweeps within this window do not hammer a bus that is down.
# A distinct key avoids the TTL ambiguity of `seen()` — the success marker and
# the backoff marker are independent records.
VERDICT_COMPLETION_RETRY_TTL = 600  # 10 min
KEY_VERDICT_COMPLETION_RETRY = "verdict_completion_retry"
ORPHAN_RESUBMIT_TTL_SEC = 600
STALE_QUEUE_SEC = 900
MAX_BULK_RESUBMITS_PER_ACTION = 20
DOCTOR_MAX_SAME_SIGNATURE_RESUBMITS = doctor_v2.max_same_signature_resubmits()
SAME_SIGNATURE_RETRY_TTL = 30 * 24 * 3600

# ── protection_violation remediation (operator 2026-10-27) ─────────────────
# A worker that modified another session's checkout fails with
# failure_reason=protection_violation ("driver modified another session's
# checkout: <path>"). The attempt delivered nothing, so the doctor handles the
# row deterministically — no LLM decision in the path: purge the empty failed
# row and re-dispatch the same work once on the remaining hosts, excluding the
# offending one. The cap and the markers are durable (STATE), so a restart
# never re-purges or re-dispatches the same original job id.
VIOLATION_MAX_RETRIES = _env_int("VIOLATION_MAX_RETRIES", 2, minimum=0)
PROTECTION_RETRY_TTL_SEC = 30 * 86400.0
PROTECTION_ERROR_RE = re.compile(r"driver modified another session's checkout:\s*(\S+)")

# ── needs-review follow-up (operator 2026-10-27) ───────────────────────────
# A needs-review row with no review performed (no result.review_evidence and no
# child review:* job) used to sit there forever — the triage that produced it
# had escalated into it with an unusable decision. The sweep now submits ONE
# read-only review job per such row, oldest first, capped per sweep so a
# big backlog cannot flood the queue. A row that HAS a review verdict is left
# for a human/orchestrator, and a needs-review row is never auto-purged:
# if it carries commits, those are delivered work.
REVIEW_FOLLOWUP_MAX_PER_SWEEP = _env_int("REVIEW_FOLLOWUP_MAX_PER_SWEEP", 3, minimum=0)
REVIEW_FOLLOWUP_TTL_SEC = 30 * 86400.0
REVIEW_FOLLOWUP_KIND_PREFIX = "review:"
REVIEW_FOLLOWUP_ELIGIBLE_KINDS = []  # ANY reviewer family; scheduler owns capability matching
REVIEW_FOLLOWUP_PREFERRED_MODELS = []
REVIEW_FOLLOWUP_MAX_COST_TIER = "C"
REVIEW_FOLLOWUP_PRIORITY = 100


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


def retry_budget_exhausted(job: dict) -> bool:
    """True when the doctor must never requeue this job on its own (operator 2026-10-26).

    A job submitted with ``max_retries`` 0, or whose ``retry_count`` has reached
    ``max_retries``, is not the doctor's to re-run: the submitter said how many
    attempts it gets. The doctor reports it (needs-review) instead. An unreadable
    budget fails closed -- every job record the bus returns carries both fields, so
    a record without them is not one the doctor can safely act on."""
    max_retries = job.get("max_retries")
    retry_count = job.get("retry_count")
    if isinstance(max_retries, bool) or isinstance(retry_count, bool):
        return True
    try:
        max_retries = int(max_retries)
        retry_count = 0 if retry_count is None else int(retry_count)
    except (TypeError, ValueError):
        return True
    return max_retries <= 0 or retry_count >= max_retries


def _retry_budget_note(job: dict) -> str:
    return f"max_retries={job.get('max_retries')} retry_count={job.get('retry_count')}"


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
MAX_TOKEN_FIX_PER_HOST = _env_int("MAX_TOKEN_FIX_PER_HOST", 3, minimum=0)
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
# The full fleet — Mac hosts, Linux hosts and the password-auth ones — read from
# the operator's inventory (HIVE_FLEET_INVENTORY, see hive/core/fleet_inventory.py).
# There is no in-source table any more: the entries this replaced carried stale
# addresses (ULTRA, MEDUSA and PROTEUS were all wrong) and every clone of the repo
# carried them too. Empty inventory = no known host = the host-addressed actions
# refuse by name instead of dialling a guess.


def all_fleet_hosts() -> dict[str, dict]:
    """``host -> {ip, os, auth, user}`` from the fleet inventory.

    The legacy ``ip`` key is kept because every caller and test reads it; it is
    the inventory's ``address``. Read per call rather than at import so a test (or
    an operator) that sets the inventory sees it take effect without a reload.
    """
    return {
        name: {"ip": entry["address"], "os": entry["os"], "auth": entry["auth"], "user": entry["user"]}
        for name, entry in fleet_inventory.load_inventory().items()
    }


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
KEY_PROTECTION = "protection"
KEY_REVIEW = "review"


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


def triage_seen_key(base_kind: str) -> str:
    """Durable marker: the newest failure this base_kind was already triaged for."""
    return f"{KEY_COOLDOWN}:triage_seen:{base_kind}"


def triaged_through(base_kind: str, newest_failure: float) -> bool:
    """True when a triage already covers every failure up to ``newest_failure``.

    2026-10-27 (operator): the 1 h cooldown alone re-triaged the same old
    failures every hour, because a failed row stays in ``status=failed`` until
    somebody deletes it. The marker records the moment the triage was submitted,
    which is always at or after the newest failure it covered, so a second scan
    of unchanged failures is a no-op and a genuinely NEW failure (ended after
    the marker) still gets one triage.
    """
    seen = STATE.last_mark(triage_seen_key(base_kind))
    return seen is not None and seen >= float(newest_failure or 0)


def note_triage_seen(base_kind: str, at: float | None = None) -> float:
    """Record that every failure up to now for ``base_kind`` has been triaged."""
    return STATE.mark(triage_seen_key(base_kind), ttl=TRIAGE_SEEN_TTL_SEC, at=at)


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
    if not HIVE_URL:
        # Bus disabled (see the module-level warning): report "no answer" so every
        # caller takes its existing unreachable-bus path instead of raising.
        fleet_inventory.warn_once(
            "doctor.http.disabled",
            "bus call skipped: HIVE_URL is not set (the doctor's bus features are disabled)",
            level=logging.DEBUG,
        )
        return 0, None
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


def _emit_light_model_alert() -> dict:
    """Broadcast ``doctor.light_model_needed`` from the heartbeat, ONCE.

    The ledger decides when a crossing happened (``CallLedger._alerted_reason``
    de-duplicates per hour, and a quiet hour re-arms it); the heartbeat is the
    one place that can broadcast without a model call issuing a second request.
    """
    try:
        summary = _model_call_ledger().counter()
    except Exception as exc:  # noqa: BLE001 -- telemetry never fails a heartbeat
        log.warning("doctor model-call counter unavailable: %s", exc)
        return {}
    if summary.get("alert"):
        log.warning(
            "doctor %s reason=%s calls=%s slot_taken=%s",
            doctor_rules.ALERT_TOPIC,
            summary.get("alert_reason"),
            summary.get("calls_last_hour"),
            summary.get("slot_taken_last_hour"),
        )
        try:
            broadcast_doctor_message(
                doctor_rules.ALERT_TOPIC,
                {
                    "host": AGENT_HOST,
                    "reason": summary.get("alert_reason"),
                    "calls_last_hour": summary.get("calls_last_hour"),
                    "slot_taken_last_hour": summary.get("slot_taken_last_hour"),
                },
            )
        except Exception as exc:  # noqa: BLE001 -- telemetry never fails a heartbeat
            log.warning("doctor %s broadcast failed: %s", doctor_rules.ALERT_TOPIC, exc)
    return summary


def heartbeat():
    # Alert telemetry is best effort: a bus outage is exactly when an alert
    # fires, and it must never cost the doctor its heartbeat. `_emit_...`
    # already isolates what it can; this is the belt for the rest.
    try:
        _emit_light_model_alert()
    except Exception as exc:  # noqa: BLE001 -- never let telemetry fail a heartbeat
        log.warning("doctor alert broadcast failed (heartbeat continues): %s", exc)
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
    code, response = _http("PATCH", f"/v1/jobs/{job_id}", body)
    if status == "needs-review" and code in (200, 201, 204):
        # Fetch the authoritative row: result alone lacks repo/branch context.
        try:
            rows = fetch_needs_review_jobs(limit=1000)
            row = next((r for r in rows if r.get("id") == job_id), None)
            if row is None:
                raise RuntimeError("review row absent from reconciliation listing")
            ensure_review_followup(row)
        except Exception as exc:
            log.error("immediate review dispatch failed job=%s: %s", job_id, exc)
            broadcast_doctor_message(
                "doctor.sweep_error", {"sweep": "immediate-review", "job_id": job_id, "error": str(exc)}
            )
    return code, response


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

#: The residue prompt's system text: short, and it names ONLY the classes.
#: The doctor asks a model for one enum value and a short reason, nothing else
#: (operator 2026-10-31) -- so the schema, the grammar and the prompt agree.
RESIDUE_SYSTEM = (
    "You classify ONE failed Hive job row. Answer with one JSON object and nothing else: "
    '{"action_type": <one of no_action | release_to_queue | escalate>, "confidence": <0..1>, '
    '"reason": <at most 140 characters>}. '
    "release_to_queue = the failure is transient and rerunning the SAME work is safe. "
    "no_action = the row needs nothing from the doctor. "
    "escalate = everything else, including anything you cannot quote from the evidence. "
    "Do not call tools; purges, restarts and code fixes are never yours to choose."
)


def build_residue_prompt(row: dict, cls: doctor_rules.RowClass | None = None) -> str:
    """The SHORT prompt for the only model-classifiable part of triage.

    The old prompt asked for evidence quotes, retry scopes and release kinds; it
    now asks for one enum value and a short reason, and carries a bounded slice
    of the evidence instead of a transcript."""
    result = row.get("result")
    if not isinstance(result, str):
        result = json.dumps(result or {})
    return "\n".join(
        [
            "TRIAGE RESIDUE",
            f"job_id: {str(row.get('id') or '')[:12]}",
            f"kind: {row.get('kind') or ''}",
            f"failure_reason: {str(row.get('failure_reason') or '')[:500]}",
            f"result (truncated): {result[:2000]}",
            f"rule_out: {cls.rule_id if cls else 'none'}; decision classes: {' | '.join(doctor_rules.CLASSES)}",
        ]
    )


def _classify_triage_state(row: dict, base_kind: str = "") -> dict:
    """The deterministic classification of one triage row (operator 2026-10-31).

    Called from ``process_triage_job`` at the ONE point a model used to be
    dialled. ``doctor_rules.classify_row`` answers from the row's shape; the
    only residue it cannot name (``ambiguous``) is asked of a model with a
    CONSTRAINED reply, and a model that cannot produce one is escalated rather
    than obeyed. Either way the return value is one of ``CLASSES``."""
    cls = doctor_rules.classify_row(row)
    if cls is not None and not cls.ambiguous:
        log.info("doctor row=%s classified rule=%s action=%s", str(row.get("id", ""))[:12], cls.rule_id, cls.action)
        return doctor_rules.as_action(cls)
    if cls is None:
        # A never-act class is owned by another caller; do not invent a
        # second decision for it.
        return doctor_rules.as_action(None)

    # The residue: evidence no rule names. ONE constrained call, and the next
    # rung when the reply does not parse (the chain and the legacy loop both
    # retry); a final non-answer escalates, it never releases.
    prompt = build_residue_prompt(row, cls)
    started = time.monotonic()
    ok, text = invoke_doctor_agent(prompt, phase="triage")
    latency_ms = (time.monotonic() - started) * 1000.0
    if not ok:
        _note_doctor_model_call("triage", "unavailable", latency_ms)
        return {
            "action_type": "escalate",
            "confidence": 1.0,
            "reason": "residue model unavailable; escalated",
            "rule_id": "residue:model_unavailable",
            "needs_review": True,
            "model_attempted": True,
            "raw": text,
        }
    # ``parse_constrained`` takes the REPLY TEXT (it is the same parser the
    # schema tests exercise), so the constraint is judged on what the model
    # actually sent -- not on a re-serialisation of it.
    parsed_ok, decision = doctor_rules.parse_constrained(text)
    _note_doctor_model_call("triage", "ok" if parsed_ok else "unusable", latency_ms)
    if not parsed_ok:
        return {
            "action_type": "escalate",
            "confidence": 1.0,
            "reason": f"residue model reply rejected ({decision.get('error')}); escalated",
            "rule_id": "residue:rejected",
            "needs_review": True,
            "model_attempted": True,
            "raw": text,
        }
    # Keep the model's own evidence quote: the residual validator below checks
    # it verbatim against the row, and the schema deliberately drops it.
    raw_reply = parse_doctor_response(text) or {}
    quote = raw_reply.get("evidence_quote") or raw_reply.get("quote")
    if quote:
        decision["evidence_quote"] = str(quote)
    decision["rule_id"] = f"residue:model:{decision['action_type']}"
    decision["needs_review"] = decision["action_type"] == "escalate"
    return decision


DOCTOR_GATEWAY_HOST = os.environ.get("GATEWAY_HOST", "127.0.0.1").strip()
DOCTOR_GATEWAY_PORT = _env_int("GATEWAY_PORT", 42617, minimum=1, maximum=65535)
DOCTOR_GATEWAY_TIMEOUT = _env_int("DOCTOR_GATEWAY_TIMEOUT", 180, minimum=1)


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
    urlopen=None,
    structured: bool = False,
) -> tuple[bool, str]:
    """Run a non-codex phase via an OpenAI-compatible chat endpoint.

    Defaults to the LOCAL brain (CERBERUS, DOCTOR_OPENWEIGHT_MODEL) — pass
    ``base_url``/``model`` for a specific phase. Anthropic + Together remain
    FORBIDDEN per CLAUDE.md. Local endpoints accept a placeholder auth, so a
    missing key is only fatal when a base is explicitly remote.

    ``structured=True`` constrains the reply (operator 2026-10-31): the request
    carries ``response_format`` (honoured by every OpenAI-compatible rung) AND
    ``json_schema``/``grammar`` (what llama-server and vLLM compile into a
    grammar), so a small or overloaded model cannot answer outside the
    enumerated classes."""
    base = (base_url or DOCTOR_OPENWEIGHT_BASE_URL).rstrip("/")
    model_id = model or DOCTOR_OPENWEIGHT_MODEL
    if not base:
        # No address in source, no default base: this phase is unconfigured. Say so
        # once and return a refusal the caller records, rather than POSTing to a
        # relative URL and reporting a transport error nobody can act on.
        fleet_inventory.warn_once(
            "doctor.base_url.disabled",
            "no model base URL is configured (DOCTOR_OPENWEIGHT_BASE_URL / "
            "DOCTOR_<PHASE>_BASE_URL): doctor model calls are disabled",
        )
        return False, f"model {model_id} disabled: no base URL configured (set DOCTOR_OPENWEIGHT_BASE_URL)"
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
    if structured:
        body.update(doctor_rules.structured_request_fields())
    url = f"{base}/chat/completions"
    data = json.dumps(body).encode()
    headers = {"content-type": "application/json"}
    if api_key:
        headers["authorization"] = f"Bearer {api_key}"
    else:
        log.warning("no open-weight API key; calling %s unauthenticated (ok for local)", base)
    req = urllib.request.Request(url, data=data, method="POST", headers=headers)
    log.info("invoking model=%s at %s (alias=%s, prompt_chars=%d)", model_id, base, model_alias, len(prompt))
    opener = urlopen or urllib.request.urlopen
    try:
        with opener(req, timeout=DOCTOR_OPENWEIGHT_TIMEOUT) as r:
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


def _chain():
    """The slot-aware chain for this call, or ``None`` for the legacy path.

    A thin seam so tests (and a future caller) can inject a chain without
    patching the module that defines it: ``chain_from_env`` reads
    DOCTOR_MODEL_CHAIN and returns None when unset, which is the default."""
    chain = doctor_models.chain_from_env(
        # Count the doctor's own calls per rung; a rung at its cap is skipped,
        # not dialled (the "free slot found taken" evidence the alert reads).
        urlopen=_ledger_urlopen(),
        clock=_model_chain_clock,
    )
    return chain


#: Re-exported so a caller/test can name the chain factory from this module.
chain_from_env = doctor_models.chain_from_env


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
    # Slot-aware model chain (operator 2026-10-31): a DOCTOR_MODEL_CHAIN file
    # opts in. Unset means `chain_from_env` returns None and the legacy
    # single-base-per-phase chain below runs exactly as before.
    # `is not None` is load-bearing: tests patch chain_from_env with a falsy
    # sentinel to assert the legacy path consults no chain at all.
    model_chain = _chain()
    if model_chain is not None:
        # A structured reply that does not parse is a failure of THAT rung, and
        # the next rung must be tried (operator 2026-10-31: "reject and retry on
        # the next rung when it does not parse"). The chain already walks its
        # rungs inside one call; this loop covers a chain that returns a
        # rejection, and it is bounded so a broken chain cannot spin.
        max_rounds = int(getattr(doctor_models, "DEFAULT_MAX_ATTEMPTS", 5))
        tried: list[str] = []
        last = ""
        for _round in range(max(1, int(max_rounds))):
            outcome = model_chain.call(
                phase,
                system=DOCTOR_SYSTEM,
                prompt=prompt,
                structured=phase != "fix",
                timeout=DOCTOR_OPENWEIGHT_TIMEOUT,
            )
            tried.extend(outcome.attempts)
            if outcome.ok:
                log.info(
                    "doctor phase=%s answered by chain rung=%s attempts=%s",
                    phase,
                    outcome.rung,
                    outcome.attempts,
                )
                return True, outcome.text
            last = outcome.text
            if not str(outcome.text).startswith("unusable reply"):
                break
        return False, f"model chain failed for phase {phase}; tried {tried}: {last}"

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
        # Constrained output (operator 2026-10-31): a triage/review reply must
        # parse into the enumerated classes, so every rung inherits the schema.
        structured = phase != "fix"
        started = time.monotonic()
        ok, text = _invoke_openweight(prompt, model_alias, base_url=base, model=model, structured=structured)
        latency_ms = (time.monotonic() - started) * 1000.0
        # A reply that arrived but does not parse into the classes is a failure
        # of THIS rung; the next rung is tried in the same call.
        parsed_ok = True
        if ok and text.strip() and structured:
            parsed_ok = _reply_satisfies_schema(text)
        _record_model_call(
            rung=model or model_alias or base,
            phase=phase,
            latency_ms=latency_ms,
            outcome=("ok" if ok and parsed_ok else "unusable" if ok else _classify_attempt_failure(text)),
            skipped_because=None if ok else str(text)[:200],
        )
        if ok and text.strip() and parsed_ok:
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


def _reply_satisfies_schema(text: str) -> bool:
    """Is this legacy reply usable, or must the next rung be tried?

    The legacy chain predates the constraint and its bytes/behaviour must not
    change when ``DOCTOR_MODEL_CHAIN`` is unset: prose is still returned to the
    caller, exactly as before. Only a MALFORMED framed reply -- an action
    object that was started and did not close or did not parse -- is a failure
    of this rung, so a truncated/garbled answer rolls to the next rung instead
    of being handed to the residue validator as if it were a diagnosis.

    The chain path is the strict one: it passes ``structured=True`` and judges
    replies with ``doctor_models._acceptable`` against ``doctor_rules.CLASSES``.
    """
    stripped = (text or "").strip()
    if not stripped:
        return False
    if "{" not in stripped and "`" not in stripped:
        return True  # plain prose: legacy-legal, and the caller escalates on it
    return parse_doctor_response(stripped) is not None


def _model_chain_clock() -> float:
    """The chain's clock, patched by tests; the ledger stamps with it too."""
    return time.time()


def _ledger_urlopen():
    """Wrap urlopen so every probe that finds a rung BUSY is evidence.

    The chain probes a rung before choosing it; a second probe on a rung whose
    cached reading (a few seconds old) said "busy" is the doctor finding the
    free slot TAKEN (the chain's re-probe). Recording that here is the only
    place it is observable without a slot-level contract.
    """
    real = urllib.request.urlopen

    # The wrapper must be transparent: it forwards every positional argument
    # and keeps urlopen's own timeout default, so a caller that leaves the
    # timeout out does not silently get a 0 s one (and a positional argument
    # does not become a TypeError).
    def opener(request, *args, **kwargs):
        busy = False
        try:
            with real(request, *args, **kwargs) as response:
                body = response.read(1 << 20)
                status = getattr(response, "status", _REPLAY_OK_STATUS)
        except Exception:
            # A probe that cannot be read is not proof of capacity; it is not
            # evidence of a taken slot either.
            raise
        try:
            text = body.decode("utf-8", "replace") if isinstance(body, bytes) else str(body)
            if "/slots" in getattr(request, "full_url", ""):
                data = json.loads(text)
                slots = data.get("slots") if isinstance(data, dict) else data
                busy = _slots_report_full(slots)
        except (ValueError, AttributeError):
            busy = False
        if busy:
            _record_model_call(
                rung="slot-probe",
                phase="probe",
                latency_ms=0.0,
                outcome="busy",
                skipped_because="free slot found taken",
                slot_taken=True,
            )

        class _Replay(io.BytesIO):
            pass

        replay = _Replay(body)
        replay.status = status
        return replay

    return opener


def _slots_report_full(slots: Any) -> bool:
    """Is this llama-server ``/slots`` body every-slot-busy?

    An EMPTY list is UNKNOWN, not busy: llama-server reports every configured
    slot, so a body with no slots measures nothing (the server may be between
    a restart and its allocation) -- and ``all()`` over nothing is vacuously
    true, which turned "nothing to read" into "the free slot was taken". Only
    a non-empty list of processing slots is evidence. The chain's
    ``_llama_busy`` reads the same body the same way (0 busy, hence free).
    """
    if not isinstance(slots, list) or not slots:
        return False
    return all(isinstance(slot, dict) and slot.get("is_processing") for slot in slots)


#: What a replayed response reports when the real one carried no status.
_REPLAY_OK_STATUS = 200


def _model_call_ledger() -> doctor_rules.CallLedger:
    """The process-wide ledger (one JSONL file, like the safety state)."""
    global _MODEL_CALL_LEDGER
    if _MODEL_CALL_LEDGER is None:
        _MODEL_CALL_LEDGER = doctor_rules.CallLedger(clock=_model_chain_clock)
    return _MODEL_CALL_LEDGER


_MODEL_CALL_LEDGER: doctor_rules.CallLedger | None = None


def _record_model_call(**entry) -> dict:
    """One ledger line per rung; returns the counter (and the ONCE-alert flag).

    The alert is NOT broadcast from here: a model call must never issue a
    second network request (an outage is exactly when it fires). The caller --
    the doctor heartbeat, where a broadcast is expected -- reads
    ``alert`` and emits ``doctor.light_model_needed``.

    ``doctor.light_model_needed`` is emitted when more than 30 doctor model
    calls happen in an hour, or the doctor's free slot is found taken more than
    3 times in an hour (operator rule 2026-10-31) -- that is the evidence the
    restoration rule reads before a dedicated light model is put back."""
    try:
        summary = _model_call_ledger().record(**entry)
    except Exception as exc:  # noqa: BLE001 -- telemetry never fails a triage
        log.warning("doctor model-call ledger unavailable: %s", exc)
        return {}
    if summary.get("alert"):
        log.warning(
            "doctor %s reason=%s calls=%s slot_taken=%s",
            doctor_rules.ALERT_TOPIC,
            summary.get("alert_reason"),
            summary.get("calls_last_hour"),
            summary.get("slot_taken_last_hour"),
        )
    return summary


def _note_doctor_model_call(phase: str, outcome: str, latency_ms: float = 0.0, rung: str = "") -> dict:
    """Thin, patchable seam for call sites outside ``invoke_doctor_agent``."""
    return _record_model_call(rung=rung, phase=phase, latency_ms=latency_ms, outcome=outcome)


def doctor_model_call_counter() -> dict:
    """The counter the doctor status and the weekend triage line expose."""
    try:
        return _model_call_ledger().counter()
    except Exception:  # noqa: BLE001 -- a status read never raises
        return doctor_rules.model_call_counter([], now=time.time())


def doctor_agent_status() -> dict:
    """The extra health payload the doctor status carries (heartbeat metadata)."""
    return {
        "model_calls": doctor_model_call_counter(),
        "doctor_state_loaded": bool(STATE.loaded_ok),
    }


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


def action_resubmit_jobs(
    base_kind: str | None,
    max_resubmits: int = MAX_BULK_RESUBMITS_PER_ACTION,
    exclude_ids: set[str] | None = None,
) -> str:
    if not base_kind:
        return "resubmit_jobs missing base_kind"
    code, resp = _http("GET", "/v1/jobs?status=failed&limit=200")
    if code != 200 or not resp:
        return f"could not fetch failed jobs (code={code})"
    failed = resp.get("jobs", [])
    strip_re = re.compile(r"\s*\[.*?\]\s*$")
    candidates = [
        j
        for j in failed
        if strip_re.sub("", j.get("kind", "")).strip() == base_kind
        and not dlq_protected(j)
        and str(j.get("id") or "") not in (exclude_ids or set())
    ]

    # A job whose retry budget is spent (max_retries 0, or retry_count >= max_retries)
    # is never requeued by the doctor: it is reported for review, nothing else.
    # A replacement this doctor submitted is never resubmitted again: its own
    # exhausted budget would otherwise mint another replacement forever
    # (cross-family review 2026-10-27: "permanently failing jobs generate an
    # unbounded chain of new jobs"). One generation per original, then a report.
    def _as_dict(value) -> dict:
        """job['result'] is worker-mutable and may be a JSON string or absent."""
        if isinstance(value, dict):
            return value
        if isinstance(value, str) and value.strip():
            try:
                decoded = json.loads(value)
                return decoded if isinstance(decoded, dict) else {}
            except ValueError:
                return {}
        return {}

    def _already_a_replacement(job: dict) -> bool:
        """True when this row is a child, i.e. someone already re-dispatched it.

        The signal is the DURABLE parent_job_id column, not a field inside
        result: the worker overwrites result on failure, so a marker stored
        there disappears exactly when the guard is needed. Conservative by
        design - a review child (parent = its triage row) is also left alone,
        because re-dispatching review children is the dispatcher's job.
        """
        return bool(job.get("parent_job_id")) or bool(
            _as_dict(job.get("result")).get("resubmitted_from") or _as_dict(job.get("result")).get("resubmitted_as")
        )

    held, eligible, chain = [], [], []
    for j in candidates:
        if not retry_budget_exhausted(j):
            # A replacement this doctor minted still has its granted budget, so
            # it is a normal job again: requeue it like any other. Only a
            # BUDGET-SPENT replacement is the end of the line.
            eligible.append(j)
        elif _already_a_replacement(j):
            chain.append(j)
        else:
            held.append(j)
    target_ids = [j["id"] for j in eligible][: max(0, min(MAX_BULK_RESUBMITS_PER_ACTION, int(max_resubmits)))]
    held_note = ""
    if held:
        held_note = f"; held {len(held)} with no retry budget (needs-review): " + ", ".join(
            f"{j.get('id', '?')[:12]} ({_retry_budget_note(j)})" for j in held[:10]
        )
        log.warning("resubmit %s: not requeueing %d job(s) with no retry budget%s", base_kind, len(held), held_note)
        broadcast_doctor_message(
            "doctor.needs-review",
            {
                "host": AGENT_HOST,
                "base_kind": base_kind,
                "reason": "retry-budget-exhausted",
                "job_ids": [j.get("id") for j in held],
            },
        )
    if not target_ids:
        if held:
            # 2026-10-27 (operator: "why is the doctor not acting on these?"):
            # a failed row whose retry budget is spent could only ever be
            # REPORTED, so every such failure became a triage row that nobody
            # could action - the pile-up the operator kept seeing. The work is
            # still real, so resubmit it as a NEW job with a real budget instead
            # of holding it: same kind, description, repo, branches and
            # eligibility, parented to the original so the lineage is visible.
            if DRY_RUN:
                return f"DRY_RUN: would resubmit {len(held)} budget-spent job(s) of {base_kind} as new{held_note}"
            made, failed_submit, unmarked = [], [], []
            cap = max(0, min(MAX_BULK_RESUBMITS_PER_ACTION, int(max_resubmits)))
            for j in held[:cap]:
                payload = {
                    "kind": j.get("kind"),
                    "description": j.get("description") or "",
                    "priority": j.get("priority") or 0,
                    "project": j.get("project"),
                    "repo": j.get("repo"),
                    "head_branch": j.get("head_branch"),
                    "base_branch": j.get("base_branch"),
                    "eligible_kinds": j.get("eligible_kinds"),
                    "eligible_hosts": json.loads(j["eligible_hosts"])
                    if isinstance(j.get("eligible_hosts"), str)
                    else j.get("eligible_hosts"),
                    "max_retries": 2,
                    "parent_job_id": j.get("id"),
                    "idempotency_key": f"doctor-resubmit-{j.get('id')}",
                }
                payload = {k: v for k, v in payload.items() if v not in (None, "", [])}
                payload["result"] = {
                    "resubmitted_from": j.get("id"),
                    "resubmitted_reason": "retry-budget-exhausted",
                }
                code3, resp3 = _http("POST", "/v1/jobs", payload)
                if code3 not in (200, 201):
                    # The original's branch usually EXISTS after its first
                    # attempt, and the bus refuses a commit-producing job whose
                    # output ref is already published - correctly. A resubmit
                    # therefore needs a fresh ref: derive one deterministically
                    # from the original id so a second pass cannot collide with
                    # itself either.
                    short = str(j.get("id") or "")[:8]
                    derived = f"{(payload.get('head_branch') or 'resubmit')[:40]}-resub-{short}"
                    payload["head_branch"] = derived
                    payload["idempotency_key"] = f"doctor-resubmit-{j.get('id')}-{short}"
                    log.warning(
                        "resubmit %s: %s refused on the original ref (http=%s); retrying on %s",
                        base_kind,
                        j.get("id"),
                        code3,
                        derived,
                    )
                    code3, resp3 = _http("POST", "/v1/jobs", payload)
                if code3 in (200, 201) and isinstance(resp3, dict) and resp3.get("id"):
                    made.append(resp3["id"])
                    # Point the ORIGINAL at its replacement so the next sweep
                    # does not select it again and lean on server-side
                    # idempotency alone (cross-family review 2026-10-27).
                    patch_code, _patch_resp = _http(
                        "PATCH",
                        f"/v1/jobs/{j.get('id')}",
                        {
                            "claimed_by": j.get("claimed_by"),
                            "result": {
                                **_as_dict(j.get("result")),
                                "resubmitted_as": resp3["id"],
                                "resubmitted_reason": "retry-budget-exhausted",
                            },
                        },
                    )
                    if patch_code not in (200, 201):
                        # Not fatal: the durable parent_job_id on the child and
                        # the submit idempotency key both still prevent a second
                        # generation. It must be VISIBLE though.
                        log.warning(
                            "resubmit %s: could not mark %s as resubmitted (http=%s)",
                            base_kind,
                            j.get("id"),
                            patch_code,
                        )
                        unmarked.append(j.get("id"))
                else:
                    failed_submit.append(j.get("id"))
            broadcast_doctor_message(
                "doctor.resubmitted",
                {
                    "host": AGENT_HOST,
                    "base_kind": base_kind,
                    "reason": "retry-budget-exhausted",
                    "submitted": made,
                    "failed": failed_submit,
                    "unmarked_originals": unmarked,
                },
            )
            log.warning(
                "resubmit %s: submitted %d new job(s) for budget-spent failures%s",
                base_kind,
                len(made),
                held_note,
            )
            chain_note = f"; {len(chain)} replacement(s) left alone (one generation only)" if chain else ""
            if made:
                return f"resubmitted {len(made)} budget-spent job(s) of {base_kind} as new{held_note}{chain_note}"
            return f"could not resubmit budget-spent jobs of {base_kind}{held_note}{chain_note}"
        if chain:
            return (
                f"no resubmit: every budget-spent job of {base_kind} is already a replacement "
                f"({len(chain)} left alone, one generation only)"
            )
        return f"no failed jobs of base_kind={base_kind}"
    if DRY_RUN:
        return f"DRY_RUN: would requeue {len(target_ids)} jobs of {base_kind}{held_note}"
    code2, resp2 = _http(
        "POST",
        "/v1/admin/jobs/requeue",
        {
            "job_ids": target_ids,
            "reason": f"doctor-resubmit:{base_kind}",
        },
    )
    return f"requeued {len(target_ids)} jobs of {base_kind} (api={code2}){held_note}"


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


def _unknown_signature_decision(job: dict, reason: str) -> dict | None:
    """The deterministic unknown-signature decision, or None when there is no
    failure to classify.

    Called from `process_triage_job` at exactly one point — where the doctor
    classifies a failure it does not know, i.e. after the v2 rules and the
    known-signature map both miss. The decision comes from
    `remediation.unknown_policy`, which classifies what no
    `remediation_manifest.SIGNATURE_HANDLERS` row claims, and every durable
    counter it keeps lives in STATE (the signature table, the per-signature+job
    retry counter), never in a module dict — a pod restart must not reset them.

    Branches:
      a. delivered — commits already on origin: no requeue, no release; the
         triage job is finished and its result says so;
      b. retried — one requeue via the normal release path (doctor excluded,
         so another eligible host claims it), tagged ``unknown_signature``;
      c. needs-review — parked for a human, signature and raw reason recorded.

    The never-act classes (host_declines_kind / released_by_host /
    protection_violation) are claimed by manifest rows, so this function
    returns None for them: they stay report-only and are never requeued.
    """
    if not (reason or "").strip():
        return None
    signature = remediation.failure_signature(reason)
    row = remediation.handler_for_signature(signature)
    if row is not None:
        # A manifest row claims this shape: the known handler owns it, and the
        # unknown policy — and this function — stay out of its way.
        return None

    decision = remediation.unknown_policy(
        STATE,
        job,
        reason,
        commits_reachable_from_origin=False,  # the doctor never proves delivery on its own
    )
    signature = decision["signature"]

    if decision["action"] == "retried":
        # One retry on a different eligible host: the normal release path keeps
        # 'doctor' out of eligible_kinds, so the requeued work lands on a
        # different worker. The signature is tagged on the job result.
        text, released = apply_release(
            job["id"], job.get("kind", "").startswith("triage:"), _base_kind(job.get("kind") or ""), None
        )
        if not released:
            log.warning("unknown_signature %s: release rejected; retry not queued", signature)
        return {
            "action_type": "release_to_queue",
            "confidence": 0.9,
            "reason": decision["result"]["status_note"],
            "signature": signature,
            "unknown_signature": signature,
            "unknown_signature_retry": True,
            "released_to_queue": released,
            "release_text": text,
        }

    # delivered and needs-review both terminate the triage job with the
    # policy's result; needs-review additionally parks it for a human.
    if decision["action"] == "delivered":
        return {
            "action_type": "no_action",
            "confidence": 1.0,
            "reason": decision["result"]["status_note"],
            "signature": signature,
            "unknown_signature": signature,
            "result": dict(decision["result"]),
        }
    return {
        "action_type": "escalate",
        "confidence": 1.0,
        "reason": decision["result"]["status_note"],
        "signature": signature,
        "unknown_signature": signature,
        "needs_review": True,
        "needs_operator": True,
        "result": dict(decision["result"]),
    }


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


# The agent kinds that claim and run jobs; the sweeps below share this set.
_WORKER_KINDS = ("zeroclaw", "codex", "opencode")


def _online_worker_kinds() -> set[str] | None:
    """Kinds of the worker agents online now, or None when the bus cannot say."""
    code, resp = _http("GET", "/v1/agents?status=online&limit=500", timeout=15.0)
    agents = resp.get("agents") if code == 200 and isinstance(resp, dict) else None
    if not isinstance(agents, list):
        return None
    return {
        str(a.get("kind") or "") for a in agents if isinstance(a, dict) and str(a.get("kind") or "") in _WORKER_KINDS
    }


def _kinds_claimable(job: dict, live_kinds: set[str]) -> bool:
    """False when the row names eligible kinds and none has an online worker."""
    kinds = job.get("eligible_kinds")
    if isinstance(kinds, str):
        try:
            kinds = json.loads(kinds)
        except ValueError:
            return False  # corrupt eligibility: leave the row for a human, never loop it
    if not kinds:
        return True  # unset eligibility is claimable by any kind
    if not isinstance(kinds, list):
        return False  # malformed eligibility: same as corrupt, never loop it
    return bool({str(k) for k in kinds} & live_kinds)


LOOP_REQUEUE_TTL_SEC = 7 * 24 * 3600
LOOP_PARK_TTL_SEC = 30 * 24 * 3600


def _memoize(fn):
    """Call ``fn`` at most once per sweep pass (a health probe, not per row)."""
    box: list = []

    def wrapper():
        if not box:
            box.append(fn())
        return box[0]

    return wrapper


def _loop_requeue_count(job_id: str) -> int:
    """How many automatic requeues this job has had (durable, one mark per requeue)."""
    return sum(
        1
        for n in range(1, loop_outcome.MAX_AUTO_REQUEUES + 1)
        if STATE.seen(f"loop_requeue:{job_id}:{n}", LOOP_REQUEUE_TTL_SEC)
    )


def _loop_requeue_record(job_id: str) -> None:
    n = _loop_requeue_count(job_id) + 1
    STATE.mark(f"loop_requeue:{job_id}:{n}", ttl=LOOP_REQUEUE_TTL_SEC)


def _park_for_review(job: dict, cls: str, detail: str = "") -> None:
    """Move a row that automation cannot fix to needs-review, once, with the cause."""
    jid = str(job.get("id") or "")
    if not jid or STATE.seen(f"loop_park:{jid}", LOOP_PARK_TTL_SEC):
        return
    reason = f"loop-outcome:{cls}{detail}"
    if DRY_RUN:
        log.info("DRY_RUN: would park %s as needs-review (%s)", jid[:12], reason)
        return
    code, _resp = _http(
        "POST",
        "/v1/admin/jobs/reclassify",
        {"job_ids": [jid], "status": "needs-review", "reason": reason},
        timeout=15.0,
    )
    if code in (200, 201):
        STATE.mark(f"loop_park:{jid}", ttl=LOOP_PARK_TTL_SEC)
        broadcast_doctor_message(
            "doctor.needs-review",
            {"host": AGENT_HOST, "job_ids": [jid], "reason": reason},
        )
        log.info("parked %s as needs-review (%s)", jid[:12], reason)
    else:
        log.warning("could not park %s as needs-review (http=%s)", jid[:12], code)


def sweep_orphan_failures(max_bases: int = 5) -> dict:
    """Resubmit failed rows that nothing else will ever look at.

    2026-10-27/28 (operator, twice: "triage queue has a loop_unresolved"): a
    failed row reaches the resubmit path only through a ``triage:`` wrapper, and
    the wrapper is suppressed for a base_kind already triaged inside the marker
    window. A failure that appears after that window, or a wrapper that was
    never created, therefore sat in ``failed`` forever with nobody acting on it -
    which is exactly the triage pile-up the operator kept seeing.

    This sweep closes that loop deterministically, without the LLM: for every
    distinct base_kind among failed rows that have no parent and are not
    protected, call the resubmit action (idempotent per original id, one
    generation only). Bounded per pass and marked per base_kind so a pass cannot
    spin.
    """
    code, resp = _http("GET", "/v1/jobs?status=failed&limit=200", timeout=15.0)
    if code != 200 or not resp:
        log.warning("orphan-failure sweep: could not list failed rows (http=%s)", code)
        return {"scanned": 0, "error": f"http={code}"}
    failed = resp.get("jobs", []) or []
    # A row the infrastructure classifier PARKED sits in status=queued, not
    # failed, so a sweep that only reads failed rows never sees it - and with
    # max_retries 0 the bus will never claim it again. That is what the operator
    # reported as "P76 in the queue is not being claimed". Read the parked rows
    # too and treat them as work to re-dispatch.
    pcode, presp = _http("GET", "/v1/jobs?status=queued&limit=200", timeout=15.0)
    parked = []
    if pcode == 200 and presp:
        for j in presp.get("jobs", []) or []:
            res = j.get("result") or {}
            if res.get("infrastructure_parked"):
                parked.append(j)
    failed = failed + parked
    # 2026-10-29 (operator: "Codex is at quota for several days, so jobs should
    # not go to codex"): a row only some worker KIND can take (eligible_kinds
    # ["codex"]) was requeued every pass into a pool with no live worker of that
    # kind, or into one that fails it at once. Skip rows whose eligible kinds have
    # no online agent; they resume on their own when such a worker comes back.
    live_kinds = _online_worker_kinds()
    # 2026-10-29 (operator: "if it knows the failure outcome then the doctor should
    # be able to act on it better and we shouldn't be filling up the queue"): act
    # on WHY the loop failed, not only THAT it did. Rows whose cause is outside the
    # job (reviewer down) wait for the cause to clear instead of burning a retry;
    # rows that need a human are parked once; the rest requeue as before.
    reviewer_probe = _memoize(loop_outcome.reviewer_healthy)
    excluded: set[str] = set()
    requeue_marks: list[str] = []
    outcome_notes: dict[str, str] = {}
    class_counts: dict[str, int] = {}
    bases: dict[str, int] = {}
    base_ids: dict[str, list[str]] = {}
    for j in failed:
        if j.get("parent_job_id") or dlq_protected(j):
            continue
        if live_kinds is not None and not _kinds_claimable(j, live_kinds):
            continue
        jid = str(j.get("id") or "")
        cls = loop_outcome.classify(j)
        class_counts[cls] = class_counts.get(cls, 0) + 1
        local_only = cls == loop_outcome.KILLED_BY_SIGNAL and loop_outcome.has_local_only_commits(j)
        verdict = loop_outcome.decide(cls, _loop_requeue_count(jid), reviewer_probe, local_commits_only=local_only)
        if cls != loop_outcome.UNKNOWN:
            outcome_notes[jid[:12]] = f"{cls}->{verdict}"
        if verdict in ("hold", "skip"):
            excluded.add(jid)
            continue
        if verdict == "needs_review":
            excluded.add(jid)
            detail = ":local_commits_only" if local_only else ""
            if cls == loop_outcome.EMPTY_DIFF:
                detail = ": " + loop_outcome.EMPTY_DIFF_REASON
            _park_for_review(j, cls, detail)
            continue
        if verdict == "requeue":
            requeue_marks.append(jid)
        kind = re.sub(r"\s*\[.*?\]\s*$", "", str(j.get("kind") or "")).strip()
        if not kind:
            continue
        bases[kind] = bases.get(kind, 0) + 1
        base_ids.setdefault(kind, []).append(jid)
    acted, skipped = {}, []
    for base_kind in sorted(bases, key=lambda k: -bases[k])[: max(0, int(max_bases))]:
        # 2026-10-29: this is a COOLDOWN, so ask `seen(key, ttl)`. `last_mark()` returns when the key was
        # last marked and never expires: with `is not None` the first requeue of a base_kind suppressed every
        # later failure of that kind (operator: the triage backlog was never serviced).
        # 2026-10-29 (operator: "triage queue is ballooning, why isn't the doctor addressing"):
        # the cooldown was per base_kind, so ONE requeue held back every other failure of that
        # kind for the whole window. Key it on the SET of failed rows instead: the same rows
        # inside the window are still skipped (no requeue loop), but a row that failed since
        # the last pass changes the set and is acted on at once.
        set_key = (
            "orphan_resubmit:"
            + base_kind
            + ":"
            + hashlib.sha1(",".join(sorted(base_ids.get(base_kind, []))).encode()).hexdigest()[:12]
        )
        if STATE.seen(set_key, ORPHAN_RESUBMIT_TTL_SEC):
            skipped.append(base_kind)
            continue
        try:
            text = action_resubmit_jobs(base_kind, exclude_ids=excluded)
        except Exception as exc:  # noqa: BLE001 - a sweep must not kill the loop
            text = f"error: {type(exc).__name__}: {exc}"
        acted[base_kind] = text
        # Mark ONLY a definitive outcome. A refusal, a transport failure or an
        # exception must leave the base_kind unmarked so the next pass retries;
        # marking those is what made a real failure look "handled" for the TTL
        # (2026-10-28: the first sweep marked after a refused submit).
        # action_resubmit_jobs reports a bus requeue as "requeued N jobs of ...
        # (api=<code>)": leaving that unmarked re-ran the same requeue every pass
        # (2026-10-29, build:codex requeued once a minute). Only a 200/201 is a
        # requeue that happened; any other code failed and must be retried, the
        # same test the triage release path applies to this text.
        if (
            text.startswith(("resubmitted ", "no failed jobs"))
            or re.match(r"requeued \d+ jobs .*\(api=20[01]\)", text)
            or "already a replacement" in text
        ):
            STATE.mark(set_key, ttl=ORPHAN_RESUBMIT_TTL_SEC)
            for jid in requeue_marks:
                _loop_requeue_record(jid)
    if acted:
        log.info("orphan-failure sweep: %s", acted)
    if outcome_notes:
        log.info("orphan-failure sweep outcomes: %s", outcome_notes)
    if class_counts:
        log.info("orphan-failure sweep class_counts: %s", class_counts)
    return {
        "scanned": len(failed),
        "bases": len(bases),
        "acted": acted,
        "skipped": skipped,
        "outcomes": outcome_notes,
        "class_counts": class_counts,
    }


def sweep_unclaimable_queued(max_rows: int = 3) -> dict:
    """Re-dispatch a queued row that no worker can ever claim.

    2026-10-28 (orchestrator coordination on the bus): a build job pinned to
    ``eligible_hosts: ["hydra"]`` sat queued for hours. HYDRA runs no zeroclaw
    worker unit at all, so no agent could ever match it - and a queued row is
    invisible to every other sweep, which is why it looked like "the claim path
    is stalled". The claim path was fine; the eligibility was unsatisfiable.

    For each queued, unclaimed, non-parked row older than STALE_QUEUE_SEC whose
    eligible hosts contain no host with an active worker agent, submit a
    replacement with the host restriction removed (a runner is chosen by the
    claim gate), parented to the original, and mark the row so one pass cannot
    submit it twice.
    """
    code, resp = _http("GET", "/v1/jobs?status=queued&limit=200", timeout=15.0)
    if code != 200 or not resp:
        log.warning("unclaimable-queued sweep: could not list queued rows (http=%s)", code)
        return {"scanned": 0, "error": f"http={code}"}
    acode, aresp = _http("GET", "/v1/agents?status=online&limit=500", timeout=15.0)
    if acode != 200 or not aresp:
        log.warning("unclaimable-queued sweep: could not list agents (http=%s)", acode)
        return {"scanned": 0, "error": f"agents http={acode}"}
    worker_hosts = {
        str(a.get("host") or "").lower()
        for a in (aresp.get("agents") or [])
        if str(a.get("kind") or "") in _WORKER_KINDS
    }
    now = time.time()
    acted, skipped = {}, []
    for j in resp.get("jobs", []) or []:
        if len(acted) >= max(0, int(max_rows)):
            break
        res = j.get("result") or {}
        if j.get("claimed_by") or res.get("infrastructure_parked") or dlq_protected(j):
            continue
        if now - float(j.get("started_at") or now) < STALE_QUEUE_SEC:
            continue
        hosts = j.get("eligible_hosts")
        if isinstance(hosts, str):
            try:
                hosts = json.loads(hosts)
            except ValueError:
                hosts = None
        if not hosts:
            continue  # unset eligibility is claimable by anyone
        if {str(h).lower() for h in hosts} & worker_hosts:
            continue  # a live worker host can still take it
        key = "unclaimable_queued:" + str(j.get("id"))
        if STATE.last_mark(key) is not None:
            skipped.append(j.get("id"))
            continue
        payload = {
            "kind": j.get("kind"),
            "description": (j.get("description") or "")
            + "\n\nRE-DISPATCHED (doctor 2026-10-28): the original was pinned to "
            + ",".join(str(h) for h in hosts)
            + ", which runs no worker, so nothing could ever claim it. Host restriction removed.",
            "priority": j.get("priority") or 0,
            "project": j.get("project"),
            "repo": j.get("repo"),
            "head_branch": j.get("head_branch"),
            "base_branch": j.get("base_branch"),
            "eligible_kinds": j.get("eligible_kinds"),
            "max_retries": 2,
            "parent_job_id": j.get("id"),
            "idempotency_key": f"doctor-unclaimable-{j.get('id')}",
            "result": {"resubmitted_from": j.get("id"), "resubmitted_reason": "unclaimable_eligible_hosts"},
        }
        payload = {k: v for k, v in payload.items() if v not in (None, "", [])}
        code3, resp3 = _http("POST", "/v1/jobs", payload)
        if code3 in (200, 201) and isinstance(resp3, dict) and resp3.get("id"):
            acted[j.get("id")] = resp3["id"]
            STATE.mark(key, ttl=ORPHAN_RESUBMIT_TTL_SEC)
        else:
            acted[j.get("id")] = f"submit http={code3}"
    if acted:
        log.warning("unclaimable-queued sweep: %s", acted)
    return {"scanned": len(resp.get("jobs", []) or []), "acting": acted, "skipped": skipped}


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
        # IDEMPOTENCY (F2 audit 2026-10-29): the kind used to embed
        # int(time.time()), so two dispatches for one parent produced two fix
        # jobs. Kind and idempotency_key are now both derived from the parent id:
        # the bus dedups on queue_logic.dedup_key(kind, description,
        # idempotency_key), so a replayed dispatch coalesces onto the first job.
        # The kind keeps the `doctor:codex-fix:` prefix that
        # DOCTOR_CLAIM_KIND_EXCLUDE_PREFIXES relies on (the doctor must never
        # claim its own dispatch).
        "kind": f"doctor:codex-fix:{parent_job_id}",
        "description": codex_task,
        "submitter_urn": _urn,
        "parent_job_id": parent_job_id,
        "idempotency_key": f"doctor-codex-fix:{parent_job_id}",
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
    host_info = all_fleet_hosts().get(offending_host.lower(), {})
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
        if offending_host.lower() not in all_fleet_hosts():
            log.warning(
                "token-fix: host %s has no address in the fleet inventory — falling through to LLM",
                offending_host,
            )
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
    if code != 200 or not isinstance(resp, dict) or not isinstance(resp.get("jobs"), list):
        broadcast_doctor_message("doctor.sweep_error", {"sweep": "failed-clusters", "http_status": code})
        raise RuntimeError("failed-clusters incomplete listing http=" + str(code))
    cutoff = time.time() - CLUSTER_WINDOW_SEC
    counts: dict[str, list[dict]] = {}
    for j in resp.get("jobs", []) or []:
        if (j.get("ended_at") or 0) < cutoff:
            continue
        # protection_violation is handled deterministically, here and now — no
        # LLM triage in the path. It must not also become a triage cluster.
        if is_protection_violation(j):
            handle_protection_violation(j)
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
        # A cluster whose every failure has a known loop outcome is handled
        # deterministically by the orphan sweep; an LLM triage job for it only
        # fills the queue (events are not fetched here: result/text only).
        if loop_outcome.cluster_is_infrastructure(
            [loop_outcome.classify(j, events_fetcher=lambda _id: None) for j in jobs]
        ):
            continue
        # Durable since 2026-10-22: the cooldown lived in a module dict, so a pod
        # restart let every kind be re-triaged immediately.
        if cooldown_active(f"{KEY_COOLDOWN}:{bk}", AUTO_TRIAGE_COOLDOWN):
            continue
        # 2026-10-27: a completed triage must not be repeated for the SAME
        # failures. Only a failure that ended after the last triage re-arms it.
        newest_failure = max((j.get("ended_at") or 0) for j in jobs)
        if triaged_through(bk, newest_failure):
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
    note_triage_seen(base_kind)
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


# ───────────────────────── Terminal-job retention sweep ─────────────────────────
def _fetch_retention_rows() -> list[dict] | None:
    """Fetch every row the sweep may look at: all terminal statuses plus
    ``done`` (for ``triage:*`` work orders). Returns None when ANY listing is
    incomplete (a failed sweep must never purge on a partial picture)."""
    jobs: list[dict] = []
    for status in (*retention.DEAD_STATUSES, "done"):
        code, resp = _http("GET", f"/v1/jobs?status={status}&limit=1000", timeout=20.0)
        if code != 200 or not isinstance(resp, dict) or not isinstance(resp.get("jobs"), list):
            log.warning("retention sweep: listing %s failed code=%s — skipping sweep", status, code)
            return None
        jobs.extend(resp.get("jobs", []) or [])
    return jobs


def run_retention_sweep() -> dict:
    """One terminal-job retention sweep: fetch → plan → re-read → snapshot →
    purge → verify → announce.

    Operator invariant: after a sweep ``count(failed+dead-letter+cancelled) <=
    RETENTION_MAX_DEAD``. Protected rows (active statuses, non-triage ``done``)
    are never touched; a snapshot is written BEFORE any deletion and a failed
    snapshot aborts the sweep (no purge without a snapshot).

    Returns the report dict — including a report when nothing was planned or
    purged, so the invariant's state is observable on EVERY sweep (the recount
    is read-only, so dry-run and no-op sweeps verify the invariant against the
    live bus too). An error report with scanned=None marks an incomplete
    listing whose queue state could not be assessed. ``RETENTION_DRY_RUN=1``
    plans and reports without calling the purge endpoint."""
    rows = _fetch_retention_rows()
    if rows is None:
        broadcast_doctor_message("doctor.sweep_error", {"sweep": "retention", "error": "incomplete listing"})
        return {"error": "incomplete listing", "scanned": None, "purged": 0}
    plan = retention.plan_retention_sweep(
        rows,
        dead_ttl=RETENTION_DEAD_TTL_SEC,
        triage_ttl=RETENTION_TRIAGE_TTL_SEC,
        max_dead=RETENTION_MAX_DEAD,
    )
    purge_ids: list[str] = plan["purge_ids"]

    # Re-read each candidate immediately before deleting: never purge a job
    # another code path may still be writing (row vanished or moved active).
    to_purge: list[str] = []
    skipped = 0
    for jid in purge_ids:
        code, row = _http("GET", f"/v1/jobs/{jid}", timeout=15.0)
        if code != 200 or not row:
            skipped += 1
            continue
        if row.get("status") in retention.PROTECTED_ACTIVE_STATUSES:
            skipped += 1
            continue
        to_purge.append(jid)
    entries = [e for e in plan["entries"] if e.get("id") in set(to_purge)]

    # Snapshot BEFORE delete — a failed audit write aborts the sweep.
    snap_path: str | None = None
    if entries and not RETENTION_DRY_RUN:
        p = retention.write_retention_snapshot(retention.default_snapshot_dir(), entries)
        if p is not None:
            snap_path = str(p)

    purged = 0
    if to_purge and not RETENTION_DRY_RUN:
        # ALL candidates go in ONE explicit-ids purge call (the operator's
        # "oldest first" ordering is the plan order — split calls would lose it
        # and the purge endpoint's own active guard is the second line).
        code, resp = _http(
            "POST", "/v1/jobs/purge", {"ids": to_purge, "reason": "doctor-retention-sweep"}, timeout=30.0
        )
        purged = int((resp or {}).get("deleted", 0) or 0)
        if code not in (200, 201, 204):
            log.warning("retention sweep: purge failed code=%s resp=%s", code, str(resp)[:200])

    ages = [e.get("age_sec") for e in entries if isinstance(e.get("age_sec"), (int, float))]
    report = {
        "dry_run": RETENTION_DRY_RUN,
        "would_purge": len(purge_ids) if RETENTION_DRY_RUN else 0,
        "purged": 0 if RETENTION_DRY_RUN else purged,
        "skipped": skipped,
        "counts_by_reason": {},
        "dead_after_sweep": None,
        "invariant_satisfied": False,
        "snapshot_file": snap_path,
        "oldest_purged_sec": None,
        "newest_purged_sec": None,
    }
    for r in plan["reasons"].values():
        report["counts_by_reason"][r] = report["counts_by_reason"].get(r, 0) + 1
    report["counts_by_reason"] = {k: v for k, v in report["counts_by_reason"].items() if v}
    if ages:
        report["oldest_purged_sec"] = max(ages)
        report["newest_purged_sec"] = min(ages)

    # Post-sweep recount on the LIVE bus: a row skipped because it vanished or
    # moved active between listing and re-read is already out of the queue, so
    # the recount is the truth for the invariant (not the pre-sweep planning
    # set). Read-only, so it runs on EVERY sweep — dry-run and no-op included
    # — because the invariant must be verifiable after every sweep regardless
    # of whether any jobs were deleted. (The recount is skipped only when the
    # initial listing failed, in which case this function already returned
    # None — no partial view, no false invariant claim.)
    counts_by_status: dict[str, int] = {}
    recount = _fetch_retention_rows()
    if recount is not None:
        for j in recount:
            st = j.get("status") or "(null)"
            counts_by_status[st] = counts_by_status.get(st, 0) + 1
        report["dead_after_sweep"] = sum(1 for j in recount if j.get("status") in retention.DEAD_STATUSES)
        report["invariant_satisfied"] = report["dead_after_sweep"] <= RETENTION_MAX_DEAD
    report["counts_by_status"] = counts_by_status

    log.info(
        "retention sweep: %s planned=%d purged=%d skipped=%d dead_after=%s invariant=%s",
        "dry-run" if RETENTION_DRY_RUN else "sweep",
        len(purge_ids),
        report["purged"],
        skipped,
        report["dead_after_sweep"],
        report["invariant_satisfied"],
    )

    broadcast_doctor_message(
        "doctor.retention",
        {
            "host": AGENT_HOST,
            "dry_run": RETENTION_DRY_RUN,
            "purged": report["purged"],
            "skipped": skipped,
            "counts_by_reason": report["counts_by_reason"],
            "counts_by_status": report["counts_by_status"],
            "dead_after_sweep": report["dead_after_sweep"],
            "invariant_satisfied": report["invariant_satisfied"],
            "max_dead": RETENTION_MAX_DEAD,
            "oldest_purged_sec": report["oldest_purged_sec"],
            "newest_purged_sec": report["newest_purged_sec"],
            "snapshot_file": snap_path,
        },
    )
    return report


def _job_result_dict(job: dict) -> dict:
    """The job's result as a dict (the bus stores it as a JSON string on some
    paths). A JSON string that does not parse to a dict is preserved as
    ``{"raw": <text>}`` so downstream text search (e.g. the DLQ classifier)
    still sees it; {} otherwise."""
    res = job.get("result")
    if isinstance(res, str):
        if not res.strip():
            return {}
        try:
            res = json.loads(res)
        except Exception:
            return {"raw": res}
    return res if isinstance(res, dict) else {}


# ───────────────────────── protection_violation remediation ─────────────────
# (operator 2026-10-27) A worker that modified another session's checkout fails
# with failure_reason=protection_violation; the attempt delivered nothing. The
# doctor handles the row deterministically — no LLM decision in the path:
# purge the empty failed row and re-dispatch the same work excluding the
# offending host, capped by VIOLATION_MAX_RETRIES per original job id, tracked
# durably in STATE so a restart never re-purges or re-dispatches.

PROTECTION_FAILURE_REASON = "protection_violation"


def is_protection_violation(job: dict) -> bool:
    """Detection predicate, row-level, no model call.

    True when ``failure_reason == "protection_violation"`` anywhere on the row
    (top-level or in result) OR any error-ish text on the row contains the
    token. The 2026-10-27 incident row carried it both ways:
    result.failure_reason and result.error."""
    res = _job_result_dict(job)
    for value in (job.get("failure_reason"), res.get("failure_reason")):
        if str(value or "").strip().lower() == PROTECTION_FAILURE_REASON:
            return True
    blob = " ".join(str(job.get(k) or "") for k in ("error", "failure_reason"))
    blob += " " + " ".join(str(res.get(k) or "") for k in ("error", "note", "stderr", "stdout", "failure_reason"))
    return PROTECTION_FAILURE_REASON in blob.lower()


def protection_offending_path(job: dict) -> str:
    """The checkout path named in "driver modified another session's checkout: <path>", or ''."""
    res = _job_result_dict(job)
    for text in (job.get("error"), res.get("error"), res.get("stderr"), res.get("stdout"), res.get("note")):
        text = str(text or "")
        m = PROTECTION_ERROR_RE.search(text)
        if m:
            return m.group(1)
    return ""


def protection_offending_host(job: dict) -> str:
    """Map the violation to a host: the host encoded in the claimant's URN
    (urn:agent:<kind>:<host>:<session>), or ''. When the row names no claimant
    (claimed_by/submitter_urn empty or absent), the host is unparseable and the
    caller must not re-dispatch — it quarantines and flags needs_operator."""
    for urn in (job.get("claimed_by"), job.get("submitter_urn")):
        urn = str(urn or "")
        if not urn:
            continue
        m = re.search(r"urn:agent:[^:]+:([^:/]+)", urn)
        if m:
            return m.group(1).lower()
    return ""


def _host_from_registry(host: str, registry: dict | None) -> dict:
    """The live agent row that declares this host, from the registry snapshot."""
    for agent in (registry or {}).get("agents") or []:
        if str(agent.get("host") or "").lower() == host.lower():
            return agent
    return {}


def protection_violation_retried(job_id: str) -> bool:
    """Whether this original job id was already re-dispatched, durably.

    A READ. Recording is done only after a successful submit, deliberately, for
    the same reason as the DLQ seen-set: marking a job the moment it is looked
    at would hide it from every later sweep for the whole window when the
    submit failed and the work still needs a retry."""
    return STATE.seen(f"{KEY_PROTECTION}:retried:{job_id}", ttl=PROTECTION_RETRY_TTL_SEC)


def protection_violation_record_retry(job_id: str, at: float | None = None) -> None:
    """Persist that this original job id has now been re-dispatched once."""
    STATE.mark(f"{KEY_PROTECTION}:retried:{job_id}", ttl=PROTECTION_RETRY_TTL_SEC, at=at)


def _violation_retry_idempotency_key(job_id: str) -> str:
    """Stable per original job id: the bus refuses a second submit with the same
    key, so the durable marker and the bus agree on "handled once"."""
    return f"protection-violation-retry:{job_id}"


def _build_retry_spec(job: dict, offending_host: str, retry_note: str) -> dict:
    """The copy of a failed job's spec for one re-dispatch.

    Kind gets a -retry suffix; the description carries a one-line note naming the
    violation and the excluded host; repo/head_branch/base_branch/project/
    priority/eligible_kinds/preferred_models/max_cost_tier are copied. eligible_hosts
    is the original set minus the offending host — when that becomes empty, ALL
    non-offending build hosts (the fleet build lanes minus the offender), so the
    work still runs and is never pinned to a host that has already proven the
    violation. Returns {} when nothing can be copied (no kind)."""
    kind = str(job.get("kind") or "").strip()
    if not kind:
        return {}
    desc = str(job.get("description") or "").strip()
    body = {
        "kind": f"{kind}-retry",
        "description": (desc + f"\n\n[doctor re-dispatch] {retry_note}").strip(),
        "priority": job.get("priority") or 0,
        "repo": job.get("repo"),
        "head_branch": job.get("head_branch"),
        "base_branch": job.get("base_branch"),
        "project": job.get("project"),
        "eligible_kinds": list(job.get("eligible_kinds") or []),
        "preferred_models": list(job.get("preferred_models") or []),
        "max_cost_tier": job.get("max_cost_tier"),
        "idempotency_key": _violation_retry_idempotency_key(job.get("id") or ""),
    }
    hosts = [str(h) for h in (job.get("eligible_hosts") or []) if str(h).strip()]
    offenders = {offending_host.lower()}
    remaining = [h for h in hosts if h.lower() not in offenders]
    if not remaining:
        # The original set was empty, or it only named the offending host:
        # fall back to every non-offending build host in the live registry.
        remaining = [h for h in _all_build_hosts() if h not in offenders]
    body["eligible_hosts"] = remaining
    return body


def submit_job_spec(spec: dict) -> tuple[int, dict | None]:
    """Submit one job via the bus as the doctor (the doctor's registered URN
    satisfies the orchestrator-submitter tier)."""
    body = {
        k: v for k, v in spec.items() if v is not None or k in ("repo", "head_branch", "base_branch", "max_cost_tier")
    }
    body["submitter_urn"] = _urn or "urn:agent:doctor:pythia:unknown"
    if not body.get("eligible_kinds"):
        body.pop("eligible_kinds", None)
    return _http("POST", "/v1/jobs", body)


def _all_build_hosts() -> list[str]:
    """Every host in the live registry, for the empty-eligible_hosts fallback."""
    out = set()
    code, resp = _http("GET", "/v1/agents", timeout=10.0)
    if code == 200 and resp:
        for agent in resp.get("agents", []) or []:
            h = str(agent.get("host") or "").strip().lower()
            if h:
                out.add(h)
    return sorted(out)


def _cancel_failed_job(job_id: str) -> str:
    """The fallback when the purge route refuses: mark the row cancelled."""
    code, resp = _http("PATCH", f"/v1/jobs/{job_id}", {"status": "cancelled"})
    if code in (200, 201, 204):
        return f"marked cancelled {job_id[:12]} (purge refused, fallback)"
    return f"cancel FAILED {job_id[:12]} code={code} resp={str(resp)[:160]}"


def handle_protection_violation(job: dict) -> str:
    """Carry out the deterministic protection-violation remediation for one row.

    A row that DID deliver commits is left entirely alone — no purge, no
    re-dispatch (the commits are real work somebody must look at) — and is
    flagged needs_operator. Otherwise: (1) purge the empty failed row via the
    admin purge path (or mark cancelled if refused); (2) re-dispatch the same
    work once, excluding the offending host, at most VIOLATION_MAX_RETRIES
    times per original job id, tracked durably. Each step is recorded in the
    row's result/reason and the doctor.protection_violation message. No LLM
    decision anywhere in this path. Returns a one-line outcome string."""
    jid = str(job.get("id") or "")
    if not jid:
        return "no job id"
    result = _job_result_dict(job)
    commits = result.get("commits") or []
    if commits:
        # Delivered work: do not purge, do not re-dispatch. The row's commits
        # are real and somebody must look at them.
        steps = ["SKIPPED entirely: row carries commits (delivered work)"]
        reason = "protection_violation; " + "; ".join(steps)
        try:
            _http(
                "PATCH",
                f"/v1/jobs/{jid}",
                {
                    "status": job.get("status") or "failed",
                    "result": {**result, "doctor_reason": reason, "needs_operator": True},
                },
            )
        except Exception:
            log.warning("protection violation: could not annotate row %s", jid[:12])
        broadcast_doctor_message(
            "doctor.protection_violation",
            {
                "host": AGENT_HOST,
                "job_id": jid,
                "kind": job.get("kind"),
                "offending_host": protection_offending_host(job) or None,
                "offending_path": protection_offending_path(job) or None,
                "steps": steps,
                "needs_operator": True,
            },
        )
        return reason

    steps: list[str] = []
    needs_operator = False
    if not STATE.loaded_ok or not STATE.persist_ok:
        steps.append("purge REFUSED: safety state unavailable")
        needs_operator = True
    else:
        path, body = doctor_gates.purge_request("dlq_remove", [jid], "doctor-protection-violation")
        code, resp = _http("POST", path, body)
        outcome = doctor_gates.purge_result("dlq_remove", resp, [jid]) if code == 200 else None
        if outcome is not None and outcome["removed"]:
            steps.append(f"purged {jid[:12]} via admin purge")
        else:
            cancel = _cancel_failed_job(jid)
            steps.append(f"purge refused code={code}; {cancel}")

    offending_host = protection_offending_host(job)
    offending_path = protection_offending_path(job)
    note = f"protection_violation on original {jid[:12]}" + (
        f" (checkout {offending_path} on {offending_host})" if offending_host else " (offending host unparseable)"
    )

    re_dispatched = False
    if offending_host:
        # The durable marker counts how many times this original job id has
        # ALREADY been re-dispatched. Re-dispatch while that count is strictly
        # below the cap, then record this attempt — so a cap of 2 permits up to
        # two re-dispatches (one per failed attempt) across restarts.
        used = 1 if protection_violation_retried(jid) else 0
        if used >= VIOLATION_MAX_RETRIES:
            steps.append(f"re-dispatch SKIPPED: {used}/{VIOLATION_MAX_RETRIES} retries already used")
            needs_operator = True
        else:
            spec = _build_retry_spec(job, offending_host, note)
            code, resp = submit_job_spec(spec)
            new_id = (resp or {}).get("id") if isinstance(resp, dict) else None
            if code in (200, 201) and new_id:
                protection_violation_record_retry(jid)
                re_dispatched = True
                steps.append(
                    f"re-dispatched {new_id[:12]} excluding host={offending_host} (retry {used + 1}/{VIOLATION_MAX_RETRIES})"
                )
            else:
                steps.append(f"re-dispatch FAILED code={code} resp={str(resp)[:160]}")
                needs_operator = True
    else:
        steps.append("re-dispatch SKIPPED: offending host unparseable")
        needs_operator = True

    reason = "protection_violation; " + "; ".join(steps)
    try:
        _http(
            "PATCH",
            f"/v1/jobs/{jid}",
            {
                "status": job.get("status") or "failed",
                "result": {**result, "doctor_reason": reason, "needs_operator": needs_operator},
            },
        )
    except Exception:
        log.warning("protection violation: could not annotate row %s", jid[:12])
    payload = {
        "host": AGENT_HOST,
        "job_id": jid,
        "kind": job.get("kind"),
        "offending_host": offending_host or None,
        "offending_path": offending_path or None,
        "steps": steps,
        "needs_operator": needs_operator,
    }
    if re_dispatched:
        payload["re_dispatched"] = True
    broadcast_doctor_message("doctor.protection_violation", payload)
    return reason


# ───────────────────────── needs-review follow-up ───────────────────────────
# (operator 2026-10-27) A needs-review row with no review performed (no
# result.review_evidence and no child review:* job) must get a review
# immediately. The sweep submits ONE read-only review job per such row,
# oldest first, capped by REVIEW_FOLLOWUP_MAX_PER_SWEEP. A row with a review
# verdict is left for a human/orchestrator. A needs-review row is NEVER
# auto-purged: if it carries commits, those are delivered work.


def review_followup_row_has_verdict(job: dict) -> bool:
    """True when a verdict exists or a review submission is already recorded.

    A submission marker prevents duplicate work; it does not prove completion.
    """
    res = _job_result_dict(job)
    if res.get("review_evidence") or res.get("verdict"):
        return True
    return bool(str(res.get("auto_review_job_id") or "").strip())


def awaiting_review_entry(job: dict) -> dict | None:
    """The row's ``result.review_state`` (review queue) block, or None.

    Operator rule 2026-10-31: a loop that finished authoring without a review
    is QUEUED work, not failed. There is no separate queue table: the review
    queue IS the needs-review listing, and ``review_state.state ==
    'awaiting_review'`` marks where a row is inside it. This helper is how the
    sweep tells "waiting for a reviewer" from "waiting for a human".
    """
    return review_queue.review_state({"result": _job_result_dict(job)})


def is_awaiting_review(job: dict) -> bool:
    """True while the review queue still owes this row a review."""
    return awaiting_review_entry(job) is not None


def _awaiting_review_state_key(job: dict) -> str:
    """Which of the dashboard buckets an awaiting_review row belongs in."""
    state = awaiting_review_entry(job) or {}
    if state.get("approved"):
        return "approved"
    if state.get("request_changes"):
        return "request_changes"
    if review_queue.is_parked(state):
        return "parked"
    if _review_queue_exhausted(state):
        return "exhausted"
    if review_queue.in_backoff(state):
        return "backoff"
    return "queued"


def needs_review_queue_counts(jobs: list[dict], result_key: str = "needs_review_result") -> dict:
    """Counts for the dashboard/report: queued, parked, exhausted and unreviewed rows."""
    counts = {
        "total": 0,
        "queued": 0,
        "backoff": 0,
        "parked": 0,
        "exhausted": 0,
        "approved": 0,
        "request_changes": 0,
        "unreviewed": 0,
    }
    for job in jobs:
        if not isinstance(job, dict):
            continue
        counts["total"] += 1
        if is_awaiting_review(job):
            counts[_awaiting_review_state_key(job)] += 1
        else:
            counts["unreviewed"] += 1
    counts["awaiting_review"] = counts["queued"] + counts["backoff"] + counts["parked"] + counts["exhausted"]
    counts[result_key] = dict(counts)
    return counts


def _needs_review_child_review_jobs(job: dict) -> list[dict]:
    """Child jobs of this row whose kind starts with review: — a review that was
    already dispatched for it."""
    jid = str(job.get("id") or "")
    code, resp = _http("GET", f"/v1/jobs?parent_job_id={jid}&limit=50", timeout=10.0)
    if code != 200 or not isinstance(resp, dict) or not isinstance(resp.get("jobs"), list):
        raise RuntimeError(f"review child lookup failed job={jid} http={code}")
    return [j for j in resp["jobs"] if str(j.get("kind") or "").startswith(REVIEW_FOLLOWUP_KIND_PREFIX)]


def _chain_job(jid: str) -> dict | None:
    """One row for the review-chain walk: the row, None when the bus no longer
    has it (404), and an exception when the bus cannot answer."""
    from urllib.parse import quote

    code, row = _http("GET", f"/v1/jobs/{quote(str(jid), safe='')}", timeout=10.0)
    if code == 404:
        return None
    if code != 200 or not isinstance(row, dict):
        raise RuntimeError(f"review chain lookup failed job={jid} http={code}")
    return row


def _review_child_refusal(job: dict) -> str:
    """Why no review child may be created for ``job`` (``""`` when one may).

    A review job is never itself reviewed and no review chain runs deeper than
    ``review_queue.MAX_REVIEW_CHAIN_HOPS`` hops from its work row (PYTHIA
    2026-11-01: a ten-deep review-of-review chain, one new row per sweep).
    Every refusal is logged."""
    reason = review_queue.review_chain_refusal(job, _chain_job)
    if reason:
        log.warning(
            "refused review child for %s (kind %s, parent %s): %s",
            str(job.get("id") or "")[:12],
            job.get("kind"),
            str(job.get("parent_job_id") or "")[:12] or "-",
            reason,
        )
    return reason


def _submit_review_followup(job: dict, *, rereview_reason: str = "") -> tuple[str, dict | None, str]:
    """Submit ONE review job for a needs-review row. Returns (job_id, spec, note).

    Any reviewer family may claim this high-priority read-only job. Preserve
    the branch and exact delivered SHA separately; stable idempotency closes
    the submit/marker crash window. No commits expected (read-only).

    ``rereview_reason`` marks a re-review of a row whose existing evidence does not
    cover the delivered head: the idempotency key then carries the head, so the
    bus can never hand back the earlier review for it and there is at most one
    re-review per (row, head).

    Refused (nothing submitted) for a review-kind row or a chain past the depth
    guard: see :func:`_review_child_refusal`."""
    refused = _review_child_refusal(job)
    if refused:
        return "", None, f"refused review child: {refused}"
    jid = str(job.get("id") or "")
    res = _job_result_dict(job)
    kind = str(job.get("kind") or "").strip()
    if kind.startswith("review:"):
        base_kind = kind[len("review:") :]
    elif kind.startswith("triage:"):
        base_kind = kind[len("triage:") :]
    else:
        base_kind = _base_kind(kind)
    date = time.strftime("%Y%m%d", time.gmtime())
    review_kind = f"review:{base_kind}-{date}" if base_kind else f"review:{date}"
    head = job.get("head_branch") or ""
    head_sha = res.get("head_sha") or job.get("head_sha") or ""
    base = job.get("base_branch") or ""
    desc = (
        "READ-ONLY review. Do not modify any file, branch, or commit. "
        f"Review the branch {head or '(see job)'} at HEAD {head_sha or '(resolve and report)'} against base {base or '(default)'} "
        "and return a structured verdict JSON with a 'verdict' field "
        "(approve | request-changes | blocked), a 'head_sha' field containing the exact reviewed commit, and a 'reason' field. "
        f"Reviewed job id: {jid}. No commits expected."
    ).strip()
    if rereview_reason:
        desc += f" This is a re-review: {rereview_reason}."
    key = f"needs-review-followup:{jid}"
    if rereview_reason:
        key += f":{head_sha[:12] or 'nohead'}"
    spec = {
        "kind": review_kind,
        "description": desc,
        "repo": job.get("repo"),
        "head_branch": head or None,
        "base_branch": base or None,
        "eligible_kinds": list(REVIEW_FOLLOWUP_ELIGIBLE_KINDS),
        "preferred_models": list(REVIEW_FOLLOWUP_PREFERRED_MODELS),
        "max_cost_tier": REVIEW_FOLLOWUP_MAX_COST_TIER,
        "priority": max(REVIEW_FOLLOWUP_PRIORITY, job.get("priority") or 0),
        "project": job.get("project"),
        "parent_job_id": jid,
        "idempotency_key": key,
    }
    code, resp = submit_job_spec(spec)
    new_id = (resp or {}).get("id") if isinstance(resp, dict) else None
    if code in (200, 201) and new_id:
        return new_id, spec, f"submitted {review_kind}"
    return "", spec, f"submit FAILED code={code} resp={str(resp)[:160]}"


def _record_auto_review(job: dict, review_job_id: str, *, rereview_reason: str = "") -> None:
    """Record the submission on the original row (result.auto_review_job_id +
    doctor_reason) AND keep the durable marker, so a second sweep (or a
    restarted doctor) does not submit a second review for the same row.

    A re-review also records ``result.re_review_head`` (the head it covers) and a
    per-head marker: the cap of one re-review per (row, head), durable on the row
    and in STATE."""
    jid = str(job.get("id") or "")
    STATE.mark(f"{KEY_REVIEW}:submitted:{jid}", ttl=REVIEW_FOLLOWUP_TTL_SEC)
    extra: dict = {}
    reason = f"auto review submitted {review_job_id[:12]}"
    if rereview_reason:
        head = review_verdict.delivered_head(job, _job_result_dict(job)) or ""
        STATE.mark(_rereview_key(jid, head), ttl=REVIEW_FOLLOWUP_TTL_SEC)
        extra["re_review_head"] = head
        reason = f"re-review: {rereview_reason}; {reason}"
    try:
        res = _job_result_dict(job)
        body = {
            "status": "needs-review",
            "result": {**res, "auto_review_job_id": review_job_id, "doctor_reason": reason, **extra},
        }
        if job.get("claimed_by"):
            body["claimed_by"] = job["claimed_by"]
        code, _ = _http("PATCH", f"/v1/jobs/{jid}", body)
        if code not in (200, 201, 204):
            raise RuntimeError(f"annotation failed http={code}")
        if not STATE.persist_ok:
            raise RuntimeError("durable review marker persistence failed")
    except Exception as exc:
        log.warning("review followup: could not annotate row %s: %s", jid[:12], exc)
        broadcast_doctor_message("doctor.sweep_error", {"sweep": "review-annotation", "job_id": jid, "error": str(exc)})


def _rereview_key(job_id: str, head: str) -> str:
    return f"{KEY_REVIEW}:rereview:{job_id}:{head[:12] or 'nohead'}"


_REVIEW_CHILD_TERMINAL = frozenset({"done", "failed", "cancelled", "dead-letter", "needs-review"})


def _commit_shas(result: dict) -> list[str]:
    """``result.commits`` as a list of shas, oldest first (dict entries use ``sha``)."""
    commits = result.get("commits")
    out = []
    for entry in commits if isinstance(commits, list) else []:
        sha = entry.get("sha") if isinstance(entry, dict) else entry
        if isinstance(sha, str) and sha.strip():
            out.append(sha.strip())
    return out


def _final_round_had_no_reviewer(job: dict, result: dict) -> bool:
    """True when the loop's final round ended with no reviewer and did not resolve.

    ``loop_result.teardown.final_verdict`` (or, without a teardown verdict, the last
    entry of ``loop_result.verdicts``) is ``reviewer_unavailable`` and
    ``teardown.resolved`` is not true. An empty diff is reported by the loop with the
    same word but is not an outage (nothing existed to review): ``loop_outcome``
    tells the two apart, and the empty-diff row is left alone.
    """
    loop = result.get("loop_result")
    if not isinstance(loop, dict):
        return False
    teardown = loop.get("teardown") if isinstance(loop.get("teardown"), dict) else {}
    verdicts = loop.get("verdicts") if isinstance(loop.get("verdicts"), list) else []
    last = verdicts[-1] if verdicts else None
    if isinstance(last, dict):
        last = last.get("verdict")
    unavailable = loop_outcome.REVIEWER_UNAVAILABLE
    if unavailable not in (
        review_verdict.normalize_verdict(teardown.get("final_verdict")),
        review_verdict.normalize_verdict(last),
    ):
        return False
    if teardown.get("resolved") is True:
        return False
    try:
        return loop_outcome.classify(job, events_fetcher=lambda _job_id: None) != loop_outcome.EMPTY_DIFF
    except Exception:  # noqa: BLE001 - classification is advisory
        return True


def review_evidence_gap(job: dict, result: dict | None = None) -> str | None:
    """Why the review evidence on this row does not cover its CURRENT delivered head.

    None means "nothing to say": there is no evidence, or it covers the delivered
    head (or the bus recorded that the review job itself advanced the branch, which
    is a human's call, see docs/NEEDS_REVIEW_POLICY.md). Otherwise a short reason,
    for the cases where evidence from an earlier loop round must not count as a
    review of what was finally delivered:

    * the evidence names a reviewed head that differs from ``result.head_sha``;
    * the loop's final round had no reviewer (``reviewer_unavailable``, unresolved);
    * the reviewer asked for changes and the row has commits newer than the
      reviewed head.

    A reviewed head equal to the delivered head (or ``head_matches: true`` from the
    bus's reconcile) is proof the head was reviewed and ends the inquiry.
    """
    result = _job_result_dict(job) if result is None else result
    evidence = result.get("review_evidence")
    if not evidence:  # no evidence: the row is simply unreviewed, which the caller handles
        return None
    evidence = evidence if isinstance(evidence, dict) else {}
    delivered = review_verdict.delivered_head(job, result)
    heads = review_verdict.reviewed_heads(result)
    covers = bool(delivered) and any(review_verdict.same_sha(h, delivered) for h in heads)
    if evidence.get("head_matches") is True or covers:
        return None
    if evidence.get("child_advanced_branch") is True:
        return None
    if delivered and heads:
        return (
            f"evidence is for an older head ({review_verdict.short_sha(heads[0])}, "
            f"delivered {review_verdict.short_sha(delivered)})"
        )
    if _final_round_had_no_reviewer(job, result):
        return "final round had no reviewer (reviewer_unavailable, unresolved)"
    if review_verdict.row_verdict(result) == "request_changes" and heads:
        commits = _commit_shas(result)
        at = [i for i, sha in enumerate(commits) if any(review_verdict.same_sha(sha, h) for h in heads)]
        if at and max(at) < len(commits) - 1:
            return f"reviewer asked for changes and {len(commits) - 1 - max(at)} newer commit(s) exist"
    return None


def review_followup_submitted(job_id: str) -> bool:
    """Durable marker: the sweep already submitted a review for this row."""
    return STATE.seen(f"{KEY_REVIEW}:submitted:{job_id}", ttl=REVIEW_FOLLOWUP_TTL_SEC)


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
            if kind == "release_to_queue" and retry_budget_exhausted(current):
                text = f"needs-review {target[:12]}: no retry budget ({_retry_budget_note(current)}), not released"
                log.warning("sop release refused: %s", text)
            elif kind == "release_to_queue":
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
    prior_resubmits = _signature_resubmits(base_kind, snapshot.signature)
    prior.extend({"signature": snapshot.signature, "action": "release_to_queue"} for _ in prior_resubmits)
    snapshot = doctor_v2.collect_evidence(target, prior_actions=prior)
    decision = doctor_v2.decide_rules(snapshot, DOCTOR_V2_RULES, retry_cap=_max_same_signature_resubmits())
    if decision:
        decision = doctor_v2.Decision(
            decision.action,
            decision.rule_id,
            f"{decision.reason}; quote={decision.evidence_quote!r}",
            decision.signature,
            decision.max_retries,
            decision.retry_scope,
            decision.evidence_quote,
        )
        log.info(
            "doctor v2 mode=%s base_kind=%s rule=%s action=%s signature=%s",
            DOCTOR_V2_MODE,
            base_kind,
            decision.rule_id,
            decision.action,
            decision.signature[:12],
        )
    return decision


def _same_signature_retry_key(base_kind: str, signature: str) -> str:
    # Its own prefix, listed in COUNTER_TTLS at SAME_SIGNATURE_RETRY_TTL. Under the
    # old `failures:` prefix the store pruned these records on the 1-day
    # `failures` window at every unrelated write, so the 30-day cap was a 1-day cap.
    digest = hashlib.sha256(base_kind.encode("utf-8", "replace")).hexdigest()[:16]
    return f"v2_resubmit:{digest}:{signature}"


def _legacy_same_signature_retry_key(base_kind: str, signature: str) -> str:
    """The pre-2026-10-26 key; still read so a deploy does not reset live counts."""
    digest = hashlib.sha256(base_kind.encode("utf-8", "replace")).hexdigest()[:16]
    return f"failures:doctor_v2_resubmit:{digest}:{signature}"


def _signature_resubmits(base_kind: str, signature: str) -> list[float]:
    """Every recorded resubmission of this signature inside the retry window."""
    stamps = STATE.timestamps(_same_signature_retry_key(base_kind, signature), SAME_SIGNATURE_RETRY_TTL)
    legacy = STATE.timestamps(_legacy_same_signature_retry_key(base_kind, signature), SAME_SIGNATURE_RETRY_TTL)
    return sorted(set(stamps) | set(legacy))


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

    # ── Deterministic decline paths (2026-10-27) ──
    # The decline vocabulary is decided by table, not by prompt: every reason
    # string in docs/DETERMINISTIC-PATHS.md resolves to a bounded action BEFORE
    # any LLM call, and no invoke_doctor_agent call can follow a classified row.
    # The main failure-matrix rules (integrity / dirty worktree / no-commits /
    # SIGHUP / push_failed / model_mismatch / review-not-approved /
    # loop_unresolved) are owned by the in-flight failure-matrix job; this
    # block owns only the decline rows, in a clearly separated function so the
    # two can be reconciled when that branch lands.
    decline = _handle_decline_job(job, base_kind)
    if decline is not None:
        return decline

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
            "exit_code": 0,
            "stdout": f"action={v2.action}; rule={v2.rule_id}; {text}",
            "action": {
                "action_type": v2.action,
                "rule_id": v2.rule_id,
                "reason": v2.reason,
                "evidence_quote": v2.evidence_quote,
                "signature": v2.signature,
                **({"orchestrator_note": note} if note else {}),
            },
            "base_kind": base_kind,
            "released_to_queue": released,
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
            dispatch_text = action_dispatch_codex_fix(known_action.get("codex_task"), job_id)
            parts.append(dispatch_text)
            if dispatch_text.startswith(("REFUSED", "codex submit FAILED", "dispatch_codex_fix missing")):
                # The fix was NOT dispatched (approval gate, submit failure or
                # no task): do not announce it and do not re-run the failed
                # work against a host that was never fixed. Leave it for a human.
                broadcast_doctor_message(
                    "doctor.escalate.token_mismatch",
                    {
                        "host": AGENT_HOST,
                        "job_id": job_id,
                        "reason": f"fix not dispatched: {dispatch_text[:200]}",
                        "base_kind": base_kind,
                    },
                )
                parts.append("fix not dispatched; escalated to human, no release")
            else:
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

    # ── Unknown-signature policy — the single point where the doctor
    # classifies a failure it does not know (the v2 rules and the
    # known-signature map both missed above). Deterministic code, no LLM:
    # verify delivery, retry once on another eligible host, or park for a
    # human. The durable signature table and retry counters live in STATE,
    # so a restart does not reset them. Never-act classes (host_declines_kind
    # / released_by_host / protection_violation) are report-only: their
    # manifest rows keep them out of the policy.
    _first_failed = failed[0] if failed else {}
    _first_result = _first_failed.get("result")
    if not isinstance(_first_result, str):
        _first_result = ""
    unknown_action = _unknown_signature_decision(
        job,
        str(_first_failed.get("failure_reason") or _first_result or "").strip(),
    )
    if unknown_action:
        a = unknown_action.get("action_type", "no_action")
        released = bool(unknown_action.get("released_to_queue", False))
        release_text = unknown_action.get("release_text", "")
        parts = [
            f"action={a}",
            f"conf={unknown_action.get('confidence', 1.0):.2f}",
            f"unknown_signature={unknown_action.get('unknown_signature', '')}",
            f"reason={unknown_action.get('reason', '')}",
        ]
        if a == "release_to_queue" and release_text:
            parts.append(release_text)
        log.info("doctor decision (UNKNOWN-SIGNATURE): action=%s signature=%s", a, unknown_action.get("signature", "?"))
        return {
            "exit_code": 0,
            "stdout": "; ".join(parts),
            "action": unknown_action,
            "base_kind": base_kind,
            "failed_count_seen": len(failed),
            "auto_detected": True,
            "released_to_queue": released,
            "needs_review": unknown_action.get("needs_review", False),
            "result": unknown_action.get("result"),
        }

    # ── The residue: everything above answered from the row. What is left is
    # ambiguous, and that is the ONLY reason a model is dialled (operator
    # 2026-10-31). ``_classify_triage_state`` asks one CONSTRAINED question --
    # one enum value and a short reason -- and rejects/retries down the chain
    # when the reply does not parse; when no rung parses, the row escalates.
    # Deployment-dependent worker_error (no_repo_mapping) is NOT a row the
    # table may answer for: whether it is unschedulable depends on the catalog,
    # which only this process can read. A genuinely absent repo escalates with
    # no model call; a catalog-present one falls through to the ordinary path.
    _unsched = _is_unschedulable(failed or [job])
    if _unsched:
        return {
            "exit_code": 0,
            "stdout": f"action=escalate; reason=unschedulable ({_unsched}); no model call",
            "action": {
                "action_type": "escalate",
                "confidence": 1.0,
                "reason": f"unschedulable ({_unsched}): the submitter must create the branch or map the repo",
            },
            "base_kind": base_kind,
            "needs_review": True,
            "doctor_model_calls": doctor_model_call_counter(),
        }

    residue_row = _classify_triage_state(failed[0] if failed else job)
    # ``parse_constrained`` bounds a model reply to the decision schema, which
    # deliberately drops extras -- including ``evidence_quote``. Carry the raw
    # quote through so the residual validator below can still verify it against
    # the row's own evidence (the quote gate is not weakened by the schema).
    action = {**residue_row, **(residue_row.get("model_action") or {})}

    if residue_row.get("action_type") == "escalate" and residue_row.get("model_attempted"):
        return {
            "exit_code": 3,
            "stdout": "could not parse doctor response on any rung",
            "raw": str(residue_row.get("raw") or "")[-2000:],
            "action_type": "escalate",
            "base_kind": base_kind,
            "needs_review": True,
            "action": {"action_type": "escalate", "reason": residue_row.get("reason") or ""},
            "doctor_model_calls": doctor_model_call_counter(),
        }

    # Residual release decisions must cite the original evidence and obey the
    # same durable per-signature retry ceiling as deterministic rules. A TABLE
    # decision is not residual: it names its own rule, so it needs no evidence
    # quote -- the quote gate exists for free-form model diagnoses.
    residual_evidence = doctor_v2.collect_evidence(failed[0] if failed else job)
    used_retries = len(_signature_resubmits(base_kind, residual_evidence.signature))
    from_table = bool(
        residue_row.get("rule_id", "").startswith(("retryable_", "residue:no_action", "loop:", "empty_result"))
    )
    if not from_table and (
        "evidence_quote" in action or "quote" in action or action.get("action_type") == "release_to_queue"
    ):
        residual = doctor_v2.validate_llm_decision(
            action,
            residual_evidence,
            rule_cap=_max_same_signature_resubmits(),
            used_retries=used_retries,
        )
        if residual.action == "escalate" and action.get("action_type") != "escalate":
            action = {
                "action_type": "escalate",
                "reason": residual.reason,
                "evidence_quote": residual.evidence_quote,
                "signature": residual.signature,
                "orchestrator_note": f"{residual.reason}; manual review required",
                "needs_review": True,
            }

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
        "v2_shadow": (
            {
                "rule_id": v2.rule_id,
                "action": v2.action,
                "signature": v2.signature,
                "old_action": a,
                "agree": v2.action == a,
            }
            if v2
            else None
        ),
        # The doctor call counter rides on the result so the weekend triage
        # line and the status can read it without a second source of truth.
        "doctor_model_calls": doctor_model_call_counter(),
    }


# ───────────────────────── Main loop ─────────────────────────
# ───────────────────── dead-letter queue remediation ─────────────────────
# Scalar keys that never ride on a decline-resubmit JobCreate: identity,
# terminal state, and the decline evidence of the original row.
_RESUBMIT_BODY_EXCLUDES = (
    "id",
    "status",
    "result",
    "created_at",
    "ended_at",
    "worker_error",
    "last_decline_reason",
)


def _resubmit_job_once(job: dict, fields: dict, note: str) -> dict:
    """Resubmit one declined job exactly once with `fields` patched in.

    A second resubmit of the same job is impossible by construction: the
    durable key `v2_resubmit:decline:<job_id>` is checked AND recorded in one
    step, under the 30-day signature window, so a doctor restart or a second
    triage pass over the same row cannot re-release it.
    """
    jid = job.get("id", "")
    kind_base = _base_kind(str(job.get("kind") or ""))
    # A resubmit of a declined job is a FRESH submit, not a release of the
    # original row: the original stays dead-letter/failed as evidence and the
    # new row carries the corrected field. The durable key here is what keeps
    # this to exactly one per job across restarts and triage cycles.
    #
    # `already` is read through the same store the handler runs on; the
    # `decline:` signature is the job id, not a failure signature, so the
    # 30-day window applies without the per-kind digest.
    decline_key = f"decline:{jid}"
    already = len(_signature_resubmits(kind_base, decline_key))
    # One resubmit per declined job: the durable key above was written on the
    # first pass, so a second triage cycle (or a doctor restart) finds it and
    # holds — no second submit, ever.
    if already:
        return {
            "exit_code": 0,
            "stdout": f"action=resubmit; held {jid[:12]}: already resubmitted once ({note})",
            "action": {"action_type": "resubmit", "reason": note, "resubmit_body": None, "already_resubmitted": True},
            "base_kind": kind_base,
            "released_to_queue": False,
            "needs_review": False,
        }
    _record_signature_resubmit(kind_base, f"decline:{jid}")
    # The resubmit is a fresh JobCreate: keep the scalar work fields, patch in
    # the corrected ones, and drop identity / terminal state / decline evidence
    # — those ride on the result of the ORIGINAL row, not the new one.
    body = {
        k: v
        for k, v in job.items()
        if isinstance(v, (str, int, float)) and v is not None and k not in _RESUBMIT_BODY_EXCLUDES
    }
    body.update(fields)
    # DRY_RUN is the doctor-wide submit switch; the DLQ switch is unrelated to
    # triage-path resubmits and must not double-gate them.
    if DRY_RUN:
        text = f"DRY_RUN: would resubmit {jid[:12]} ({note})"
    else:
        code, resp = _http("POST", "/v1/jobs", body)
        if code in (200, 201) and resp:
            text = f"resubmitted {jid[:12]} -> {str(resp.get('id', ''))[:12]} ({note})"
        else:
            text = f"resubmit FAILED {jid[:12]} code={code} resp={str(resp)[:160]}"
    return {
        "exit_code": 0,
        "stdout": f"action=resubmit; {text}",
        "action": {"action_type": "resubmit", "reason": note, "resubmit_body": body, "already_resubmitted": False},
        "base_kind": kind_base,
        "released_to_queue": False,
        "needs_review": False,
    }


def _handle_decline_job(job: dict, base_kind: str) -> dict | None:
    """The doctor's deterministic decline-vocabulary handler (2026-10-27).

    Returns a process_triage_job-shaped result for the decline rows the
    failure-matrix rules do not own, or None when no decline reason is on
    record (the row falls through to the v2 rules and the LLM path). The
    routing signals never increment the bus decline counter — that is the
    bus classifier's job, covered in test_decline_reasons.py — and this
    handler never acts on them either: a routing signal is report-only. It
    is recorded in the durable telemetry tally (one sighting per signature,
    windowed) and logged, and nothing else: no needs-review, no escalation,
    no resubmit, no retry-budget consumption, no matter how often the same
    signature arrives.

    Module-level names used here (all defined in this module):
        log                      logging.getLogger("doctor")          line ~392
        STATE                    state_store_from_env()                line ~429
        AGENT_HOST               socket.gethostname()                  line ~82
        decline_paths            hive.workers.doctor_decline_paths     line ~71
        broadcast_doctor_message _http POST /v1/messages              line ~1949
        _base_kind               kind -> base_kind                    line ~1848
        _signature_resubmits     STATE.timestamps wrapper             line ~2212
        _record_signature_resubmit STATE.touch wrapper                line ~2218
        decline_paths.INVALID_HEAD_BRANCH_PREFIX
        decline_paths.resubmit_body_for_invalid_head
    """
    reason = decline_paths._reason(job)
    if not reason:
        # No decline reason on record: the row is not a decline at all, so the
        # decline table must not decide it — fall through to the v2 rules and,
        # only if none of those own it, the LLM path. A decline REASON is what
        # triggers this handler; an empty one is a non-event, not a no_action
        # verdict on the job.
        return None
    decision = decline_paths.classify_decline_reason(reason, job)
    # ── host routing signals: never counted, never acted, logged once per sig ──
    if decision.action == "no_action" and not decision.reason.startswith("unowned"):
        # host_declines_kind / released_by_host: a legitimate "not me" from the
        # matcher. Never counted (the bus keeps it out of decline_count — pinned
        # in test_decline_reasons.py) and never acted on: the sighting goes
        # into the durable telemetry tally so a repeated pattern is visible,
        # and the row is handed back to the queue's own routing. The tally
        # NEVER changes this branch's action and NEVER consumes a retry
        # budget — the signal is a report, at any count.
        count = decline_paths.routing_signal_seen(STATE, job, reason)
        log.info(
            "routing signal %s: job=%s count=%d in %dh (telemetry only, never escalated)",
            reason,
            job.get("id", "")[:12],
            count,
            int(decline_paths.ROUTING_SIGNAL_WINDOW_SEC // 3600),
        )
        return {
            "exit_code": 0,
            "stdout": f"action=no_action; reason={decision.reason}; routing count={count}",
            "action": {"action_type": "no_action", "reason": decision.reason, "routing_count": count},
            "base_kind": base_kind,
            "released_to_queue": False,
            "needs_review": False,
        }
    if decision.action == "no_action" and decision.reason.startswith("unowned"):
        # A reason no table owns must surface, not vanish: the registry is a
        # contract and an unknown row is a missing row, so it is an operator
        # question, not an LLM question.
        return {
            "exit_code": 0,
            "stdout": f"action=needs-review; reason={decision.reason}",
            "action": {"action_type": "needs_review", "reason": decision.reason, "needs_review": True},
            "base_kind": base_kind,
            "released_to_queue": False,
            "needs_review": True,
            "needs_review_result": {
                "doctor_needs_review": True,
                "needs_operator": True,
                "reason": decision.reason,
            },
        }
    if decision.action == "no_action_log":
        # no_workspace_for_kind:<kind>: same report-only contract as the host
        # signals — the sighting goes into the durable telemetry tally and the
        # row is handed back to the queue's own routing. The tally NEVER
        # changes this branch's action and NEVER consumes a retry budget.
        count = decline_paths.routing_signal_seen(STATE, job, reason)
        log.info(
            "routing signal %s: count=%d in %dh (telemetry only, never escalated, logged once per signature)",
            reason,
            count,
            int(decline_paths.ROUTING_SIGNAL_WINDOW_SEC // 3600),
        )
        return {
            "exit_code": 0,
            "stdout": f"action=no_action; reason={decision.reason}; routing count={count}",
            "action": {"action_type": "no_action", "reason": decision.reason, "routing_count": count},
            "base_kind": base_kind,
            "released_to_queue": False,
            "needs_review": False,
        }
    if decision.action == "resubmit":
        if reason.startswith(decline_paths.INVALID_HEAD_BRANCH_PREFIX):
            fields = decline_paths.resubmit_body_for_invalid_head(job)
            note = f"invalid_head_branch -> sanitized {fields['head_branch']!r}"
            if fields.get("branch_created_from"):
                note += f" (kept branch_created_from {fields['branch_created_from']!r})"
        else:
            fields = dict(decision.resubmit_fields)
            note = decision.reason
        # The resubmit NEVER carries a target_workspace — unset means the bus
        # resolves the workspace itself, which is exactly what the original
        # decline was missing.
        fields = {k: v for k, v in fields.items() if v is not None}
        return _resubmit_job_once(job, fields, note)
    if decision.action == "needs_review":
        # Needs-review is its OWN action type, not a flavor of escalate: the
        # terminal loop reads it to tag the row status=needs-review (the
        # operator owns it from here), and nothing downstream may mistake it
        # for a plain LLM escalation and re-triage it.
        result = {
            "doctor_needs_review": True,
            "needs_operator": decision.needs_operator,
            "reason": decision.reason,
        }
        if decision.message_topic:
            broadcast_doctor_message(
                decision.message_topic,
                {
                    "host": AGENT_HOST,
                    "job_id": job.get("id", ""),
                    **decision.message_payload,
                },
            )
        return {
            "exit_code": 0,
            "stdout": f"action=needs-review; reason={decision.reason}",
            "action": {"action_type": "needs_review", "reason": decision.reason, "needs_review": True},
            "base_kind": base_kind,
            "released_to_queue": False,
            "needs_review": True,
            "needs_review_result": result,
        }
    if decision.action == "log_only":
        # A protected row: the owner of that lane handles it. The doctor's only
        # move is to say so — no requeue, no resubmit, no escalation, and
        # crucially NO LLM call, so a protected row can never be "fixed" into
        # the owner's hands or retried against a host that declined it.
        log.info(
            "dlq_protected lane job=%s reason=%s -> log only, left for its owner",
            job.get("id", "")[:12],
            reason,
        )
        return {
            "exit_code": 0,
            "stdout": f"action=log_only; reason={decision.reason}",
            "action": {"action_type": "no_action", "reason": decision.reason, "log_only": True},
            "base_kind": base_kind,
            "released_to_queue": False,
            "needs_review": False,
        }
    return None


def fetch_dead_letter_jobs(limit: int = 200) -> list[dict]:
    """Jobs in the terminal dead-letter queue, newest first."""
    code, resp = _http("GET", f"/v1/jobs?status=dead-letter&limit={int(limit)}", timeout=15.0)
    if code != 200 or not isinstance(resp, dict) or not isinstance(resp.get("jobs"), list):
        broadcast_doctor_message("doctor.sweep_error", {"sweep": "fetch_dead_letter", "http_status": code})
        raise RuntimeError("fetch_dead_letter incomplete listing http=" + str(code))
    jobs = resp.get("jobs", []) or []
    jobs.sort(key=lambda j: j.get("ended_at") or 0, reverse=True)
    return jobs


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
        if retry_budget_exhausted(job):
            return "needs-review", f"retry-budget-exhausted ({_retry_budget_note(job)})"
        return "resubmit", "transient-infra-failure"
    return "needs-review", "unclassified"


def remediate_dead_letter(job: dict, decision: str, reason: str) -> str:
    """Carry out a DLQ decision. No-op (logs intent only) unless DLQ_APPLY=1."""
    if dlq_protected(job):
        return "REFUSED: protected lane"
    if not STATE.loaded_ok or not STATE.persist_ok:
        return "REFUSED: safety state unavailable"
    jid = job.get("id", "")
    if decision == "resubmit" and retry_budget_exhausted(job):
        # Second line of defence behind classify_dead_letter: whatever decided
        # "resubmit", a job with no retry budget is only tagged for review.
        decision, reason = "needs-review", f"retry-budget-exhausted ({_retry_budget_note(job)})"
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
        try:
            ensure_review_followup({**job, "status": "needs-review"})
        except Exception as exc:
            log.error("immediate DLQ review dispatch failed job=%s: %s", jid, exc)
            broadcast_doctor_message(
                "doctor.sweep_error", {"sweep": "immediate-review", "job_id": jid, "error": str(exc)}
            )
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
        # protection_violation is a deterministic path: handled here, not by the
        # classifier. It is not recorded as a DLQ decision: it IS handled, and the
        # durable protection markers own the dedupe. (A protection-violation
        # worker never lands commits, so the row is safe to purge and the work
        # is re-dispatched once on the remaining hosts — no LLM decision.)
        if is_protection_violation(job):
            handle_protection_violation(job)
            summary["protection_violation"] = summary.get("protection_violation", 0) + 1
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


def fetch_needs_review_jobs(limit: int = 1000) -> list[dict]:
    """Fetch review backlog for the report-only sweep."""
    code, response = _http("GET", f"/v1/jobs?status=needs-review&limit={int(limit)}", timeout=15.0)
    if code != 200 or not isinstance(response, dict) or not isinstance(response.get("jobs"), list):
        broadcast_doctor_message("doctor.sweep_error", {"sweep": "fetch_needs_review", "http_status": code})
        raise RuntimeError("fetch_needs_review incomplete listing http=" + str(code))
    rows = list(response["jobs"])
    total = response.get("total", len(rows))
    if isinstance(total, int) and total > len(rows):
        log.error("needs-review backlog truncated: fetched=%s total=%s; API has no pagination", len(rows), total)
        broadcast_doctor_message(
            "doctor.sweep_error",
            {
                "sweep": "needs-review",
                "error": "backlog truncated; API has no pagination",
                "scanned": len(rows),
                "total": total,
            },
        )
    return rows


def _row_result_dict(job: dict) -> dict:
    """The row's ``result`` as a dict, tolerating a JSON string column."""
    value = (job or {}).get("result")
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _is_verdict_completion_candidate(job: dict) -> bool:
    """Whether the row is a stuck codex-delivery row the verdict pass may heal.

    The row must be needs-review with ``result.failure_reason ==
    codex_delivery_failed`` (or an error that says the driver produced no
    workspace edits — the same delivery failure described from the other
    side). Read-only review rows fail this way even though their verdict is
    already present in the tail; code-kind rows that genuinely produced
    nothing are not completed here (their tail has no verdict JSON).
    """
    if (job or {}).get("status") != "needs-review" or review_followup_protected(job):
        return False
    result = _row_result_dict(job)
    failure = str(result.get("failure_reason") or "").strip()
    error = str(result.get("error") or "").strip()
    return failure == "codex_delivery_failed" or "produced no workspace edits" in error.lower()


def extract_verdict_from_tail(tail: str) -> dict | None:
    """The LAST JSON object with a ``verdict`` key in a driver tail.

    Tolerates leading prose (log lines before the JSON) and trailing noise
    (the driver's final "N tokens used" line). Parses the whole stream first,
    then walks every ``{`` with ``JSONDecoder.raw_decode`` and keeps the LAST
    object that actually carries a ``verdict`` key — not merely the last
    ``{``, which inside pretty-printed output is some nested findings object.
    Returns ``None`` when there is no such object (malformed JSON included);
    never raises.
    """
    text = str(tail or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict) and "verdict" in parsed:
            return parsed
    except (TypeError, ValueError):
        pass
    decoder = json.JSONDecoder()
    last: dict | None = None
    idx = text.find("{")
    while idx != -1:
        try:
            obj, end = decoder.raw_decode(text[idx:])
        except ValueError:
            idx = text.find("{", idx + 1)
            continue
        if isinstance(obj, dict) and "verdict" in obj:
            last = obj
        idx = text.find("{", idx + max(end, 1))
    return last


def build_verdict_completion_result(job: dict, verdict_obj: dict, finished_at: float) -> dict:
    """The exact result payload for a verdict-completed ``done`` PATCH.

    ``summary`` reports the verdict's model/verdict/confidence/head;
    ``deliverable`` is the parsed JSON object itself; ``head_sha``/``commits``
    are carried ONLY when the verdict's ``head`` is a 40/64-char hex SHA; the
    other fields are fixed for a read-only review (no workspace edits by
    definition).
    """
    v = verdict_obj if isinstance(verdict_obj, dict) else {}
    head = v.get("head")
    head_sha = None
    commits: list[str] = []
    if isinstance(head, str) and re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", head.lower()):
        head_sha = head.lower()
        commits = [head_sha]
    result: dict = {
        "summary": {
            "model": v.get("model"),
            "verdict": v.get("verdict"),
            "confidence": v.get("confidence"),
            "head": head,
        },
        "deliverable": verdict_obj,
        "files_changed": ["(none: read-only review)"],
        "worktree_clean": True,
        "verdict": v,
        "verdict_json": json.dumps(verdict_obj, sort_keys=True),
        "delivery_note": (
            "verdict extracted from result.tail by the doctor verdict-completion "
            "pass; the run was read-only and produced no workspace edits"
        ),
        "finished_at": finished_at,
    }
    if head_sha is not None:
        result["head_sha"] = head_sha
        result["commits"] = commits
    return result


def complete_verdict_job(job: dict, *, now: float | None = None) -> tuple[bool, str]:
    """Complete ONE stuck needs-review row whose tail already holds the verdict.

    Returns ``(completed, detail)``. ``completed`` is True when the row was
    PATCHed (or, in dry-run, was eligible and the marker recorded). Never
    purges: no parseable verdict means no action at all, and a row that already
    has ``result.verdict`` is left alone (idempotent).

    The durable completion marker (full ``VERDICT_COMPLETION_TTL``) is written
    only AFTER the bus accepted the PATCH; a failed PATCH leaves only the short
    ``VERDICT_COMPLETION_RETRY_TTL`` backoff, so a transient bus error cannot
    block recovery across restarts for the whole TTL.
    """
    jid = (job or {}).get("id", "")
    now = now if now is not None else time.time()
    if not _is_verdict_completion_candidate(job):
        return False, "not a codex_delivery_failed needs-review row"
    result = _row_result_dict(job)
    if result.get("verdict") is not None:
        # Already completed by an earlier pass (or by the worker itself):
        # the completion marker is what stops a second attempt in-process.
        return False, "already has result.verdict"
    tail = result.get("tail")
    if not isinstance(tail, str) or not tail.strip():
        return False, "no result.tail to parse"
    verdict_obj = extract_verdict_from_tail(tail)
    if not isinstance(verdict_obj, dict):
        return False, "no parseable verdict object in tail"
    claimed_by = job.get("claimed_by")
    body: dict = {
        "status": "done",
        "result": build_verdict_completion_result(job, verdict_obj, now),
    }
    if claimed_by:
        # The bus ownership check (PATCH /v1/jobs) requires claimed_by to match
        # the current claimant; an unclaimed row carries no claimed_by and is
        # completed as-is.
        body["claimed_by"] = claimed_by
    if VERDICT_COMPLETION_DRY_RUN:
        # Dry-run is a decision, not a bus outcome: the marker is still
        # recorded (matching the DLQ dry-run semantics) so the row is not
        # re-announced on every pass.
        log.info("verdict-completion DRY_RUN: would PATCH %s -> done", jid[:12])
        STATE.mark(KEY_VERDICT_COMPLETION + ":" + jid, VERDICT_COMPLETION_TTL, at=now)
        return True, "dry-run"
    code, resp = _http("PATCH", f"/v1/jobs/{jid}", body, timeout=15.0)
    if code in (200, 201, 204):
        # Durable SUCCESS marker, written only after the bus accepted the
        # completion: this is what stops a second attempt in this process and
        # any later process for the full TTL.
        STATE.mark(KEY_VERDICT_COMPLETION + ":" + jid, VERDICT_COMPLETION_TTL, at=now)
        broadcast_doctor_message(
            "doctor.verdict_completed",
            {
                "host": AGENT_HOST,
                "job_id": jid,
                "kind": job.get("kind"),
                "head_sha": body["result"].get("head_sha"),
                "verdict": (body["result"].get("verdict") or {}).get("verdict"),
            },
        )
        log.info("verdict-completed %s kind=%s", jid[:12], job.get("kind"))
        return True, f"patched {code}"
    log.warning(
        "verdict-completion PATCH %s failed code=%s resp=%s",
        jid[:12],
        code,
        str(resp)[:200],
    )
    # A failed PATCH leaves only a SHORT backoff marker under a SEPARATE key —
    # never the success marker — so the next sweep (in this process or a fresh
    # one) retries instead of skipping the row for the whole TTL.
    STATE.mark(KEY_VERDICT_COMPLETION_RETRY + ":" + jid, VERDICT_COMPLETION_RETRY_TTL, at=now)
    return False, f"patch failed code={code}"


def complete_stuck_verdict_jobs(jobs: list[dict]) -> dict:
    """One needs-review pass over the verdict-completion rows.

    A row whose durable completion marker is still live is skipped without any
    further work (idempotency across passes and across restarts). The marker
    itself is written by ``complete_verdict_job`` only after the bus accepted
    the PATCH; a failed PATCH leaves only the short retry backoff, so the next
    sweep retries. The report-only classification sweep keeps running over the
    same rows regardless.
    """
    now = time.time()
    summary = {"scanned": len(jobs or []), "completed": 0, "skipped": 0, "no_action": 0}
    for job in jobs or []:
        jid = (job or {}).get("id", "")
        if not jid:
            continue
        key = KEY_VERDICT_COMPLETION + ":" + jid
        if STATE.seen(key, ttl=VERDICT_COMPLETION_TTL):
            summary["skipped"] += 1
            continue
        # A previous attempt failed and its backoff has not lapsed: skip for
        # now, the next sweep (or a fresh process) will retry.
        if STATE.seen(KEY_VERDICT_COMPLETION_RETRY + ":" + jid, ttl=VERDICT_COMPLETION_RETRY_TTL):
            summary["skipped"] += 1
            continue
        if not _is_verdict_completion_candidate(job):
            continue
        if _row_result_dict(job).get("verdict") is not None:
            # Idempotent: already carries its verdict; one durable record keeps
            # this process from re-attempting it, no bus traffic at all.
            STATE.mark(key, VERDICT_COMPLETION_TTL, at=now)
            summary["skipped"] += 1
            continue
        completed, detail = complete_verdict_job(job, now=now)
        if completed:
            summary["completed"] += 1
        else:
            summary["no_action"] += 1
        log.info("verdict-completion %s -> %s (%s)", jid[:12], "done" if completed else "no action", detail)
    return summary


# ── needs-review evidence (docs/NEEDS_REVIEW_POLICY.md, step 1: report only) ──
# Every function here is read-only and fail-soft: an evidence source that is
# missing, slow or wrong yields "unknown", never an exception and never a write.
UNKNOWN = "unknown"
_SUPERSEDING_STATUSES = ("running", "done")


def branch_merged_into_base(job: dict) -> bool | None:
    """Whether the row's head branch is already merged into its base branch.

    True/False when the answer is certain, None when it cannot be known. The
    repo is resolved through the catalog (``resolve_remote_url``: unambiguous
    ids only, validated URL). A bare repo on a local path answers with
    ``merge-base --is-ancestor``; any other remote can only prove the trivial
    case (head sha == base sha) through ``git ls-remote``, otherwise None.
    """
    try:
        from hive.bus.domain import branch_admission as ba

        head = str(job.get("head_branch") or "")
        base = str(job.get("base_branch") or "")
        if not (ba.valid_branch_name(head) and ba.valid_branch_name(base)):
            return None
        url = ba.resolve_remote_url(job.get("repo"))
        if not url:
            return None
        env = ba._probe_env()
        if url.startswith("/"):
            proc = subprocess.run(
                ["git", f"--git-dir={url}", "merge-base", "--is-ancestor", f"refs/heads/{head}", f"refs/heads/{base}"],
                capture_output=True,
                text=True,
                env=env,
                timeout=20,
                check=False,
                shell=False,
            )
            return {0: True, 1: False}.get(proc.returncode)
        shas = {}
        for name in (head, base):
            proc = subprocess.run(
                ["git", "ls-remote", "--heads", url, f"refs/heads/{name}"],
                capture_output=True,
                text=True,
                env=env,
                timeout=20,
                check=False,
                shell=False,
            )
            line = (proc.stdout or "").split()
            if proc.returncode != 0 or len(line) < 2:
                return None
            shas[name] = line[0]
        return True if shas[head] == shas[base] else None
    except Exception:  # noqa: BLE001 - evidence is best effort
        return None


def _make_branch_jobs_fetcher():
    """A once-per-sweep lister of jobs (any status) for ``head_branch`` lookups.

    The API has no head_branch filter, so one unfiltered listing is fetched
    lazily and reused for every row. Raises on a failed fetch (the caller maps
    that to "unknown").
    """
    cache: list[list[dict]] = []

    def fetch(job: dict) -> list[dict]:
        if not cache:
            code, response = _http("GET", "/v1/jobs?limit=1000&include_result=false", timeout=15.0)
            if code != 200 or not isinstance(response, dict) or not isinstance(response.get("jobs"), list):
                raise RuntimeError(f"job listing unavailable http={code}")
            cache.append(list(response["jobs"]))
        branch = job.get("head_branch")
        return [j for j in cache[0] if j.get("head_branch") == branch]

    return fetch


def _as_float(value) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def needs_review_superseder(job: dict, branch_jobs: list[dict]) -> str | None:
    """Id of the newest job on the same head branch (same repo when both name
    one) created after ``job`` that is running or done; None when there is none."""
    created = _as_float(job.get("created_at"))
    if created is None:
        return None
    newer = [
        j
        for j in branch_jobs
        if j.get("id")
        and str(j.get("id")) != str(job.get("id"))
        and j.get("head_branch") == job.get("head_branch")
        and j.get("status") in _SUPERSEDING_STATUSES
        and (_as_float(j.get("created_at")) or 0.0) > created
        and not (job.get("repo") and j.get("repo") and job.get("repo") != j.get("repo"))
    ]
    if not newer:
        return None
    newer.sort(key=lambda j: ((_as_float(j.get("created_at")) or 0.0), str(j.get("id"))))
    return str(newer[-1]["id"])


def gather_needs_review_evidence(job: dict, *, merged_fn, branch_jobs_fn, state=None, now: float | None = None) -> dict:
    """Read-only evidence for one needs-review row; any field may be "unknown"."""
    state = STATE if state is None else state
    now = time.time() if now is None else now
    jid = str(job.get("id") or "")
    head = job.get("head_branch")
    evidence: dict = {"job_id": jid, "head_branch": head, "base_branch": job.get("base_branch")}
    if not head:
        evidence["merged"] = False
        evidence["superseded_by"] = None
    else:
        try:
            merged = merged_fn(job)
            evidence["merged"] = merged if isinstance(merged, bool) else UNKNOWN
        except Exception:  # noqa: BLE001
            evidence["merged"] = UNKNOWN
        try:
            evidence["superseded_by"] = needs_review_superseder(job, list(branch_jobs_fn(job)))
        except Exception:  # noqa: BLE001
            evidence["superseded_by"] = UNKNOWN
    try:
        parked_at = state.last_mark(f"loop_park:{jid}") if jid else None
    except Exception:  # noqa: BLE001
        parked_at = UNKNOWN
    if parked_at == UNKNOWN:
        evidence["doctor_parked"] = UNKNOWN
        evidence["parked_age_sec"] = UNKNOWN
    else:
        evidence["doctor_parked"] = parked_at is not None
        evidence["parked_age_sec"] = max(0.0, now - float(parked_at)) if parked_at is not None else None
    return evidence


def needs_review_suggestion(evidence: dict) -> str:
    """The suggested (never executed) action for one row's evidence."""
    if evidence.get("merged") is True:
        return "merged: safe to cancel"
    superseded_by = evidence.get("superseded_by")
    if superseded_by not in (None, UNKNOWN):
        return f"superseded by {superseded_by}: safe to cancel"
    if not evidence.get("head_branch"):
        return "needs a human: row has no head branch to check"
    gaps = [name for name in ("merged", "superseded_by") if evidence.get(name) == UNKNOWN]
    if gaps:
        return "needs a human: evidence unknown (" + ", ".join(gaps) + ")"
    reason = "not merged and not superseded"
    if evidence.get("doctor_parked") is True:
        age = evidence.get("parked_age_sec")
        reason += (
            f"; parked by the doctor {int(age // 3600)}h ago" if isinstance(age, float) else "; parked by the doctor"
        )
    return f"needs a human: {reason}"


def needs_review_evidence_fn(*, merged_fn=None, branch_jobs_fn=None):
    """Bind the evidence sources (looked up at call time, so tests can inject)."""
    merged_fn = merged_fn or branch_merged_into_base
    branch_jobs_fn = branch_jobs_fn or _make_branch_jobs_fetcher()
    now = time.time()
    return lambda job: gather_needs_review_evidence(job, merged_fn=merged_fn, branch_jobs_fn=branch_jobs_fn, now=now)


_NEEDS_REVIEW_LOGGED: dict[str, tuple] = {}


def reset_needs_review_report_state() -> None:
    """Forget what the report-only sweep already logged (tests; a restart does it too)."""
    _NEEDS_REVIEW_LOGGED.clear()


def _log_report_row_once(row: dict) -> bool:
    """Log a report-only row only when its (classification, evidence summary) changed.

    The same row used to be logged on every scan for as long as it existed. The
    first sight of a row, and every change to what the line says, still log.
    """
    jid = str(row.get("job_id") or "")
    state = (row.get("classification"), row.get("evidence_summary"))
    if _NEEDS_REVIEW_LOGGED.get(jid) == state:
        return False
    _NEEDS_REVIEW_LOGGED[jid] = state
    log.info("needs-review report-only: %s", row)
    return True


def _row_touched_at(job: dict) -> float | None:
    """When the row last changed, as far as the listing says (None: unknown)."""
    stamps = [_as_float(job.get(k)) for k in ("updated_at", "last_update_at", "ended_at", "created_at")]
    stamps = [s for s in stamps if s]
    return max(stamps) if stamps else None


def review_followup_blocker(job: dict) -> str | None:
    """Why the review follow-up will NOT act on this needs-review row (None: it will).

    Read-only and without a bus call, so it can say "stuck" without a listing per
    row: it mirrors the order of ``ensure_review_followup``. A non-None reason is
    the reason a human is needed.
    """
    if str(job.get("kind") or "").startswith("triage:"):
        return "a triage report: it needs a human, not a code review"
    if is_awaiting_review(job):
        # Queued work, not a row a human must chase: the review queue retries
        # with backoff (see review_queue_stuck_owned).
        kind = _awaiting_review_state_key(job)
        if kind == "parked":
            return "awaiting review in the review queue (parked: no cross-family reviewer)"
        if kind == "exhausted":
            return "awaiting review in the review queue (review attempts exhausted)"
        return "awaiting review in the review queue (queued for the next cross-family reviewer)"
    result = _job_result_dict(job)
    if result.get("verdict"):
        return "the row already carries a verdict"
    jid = str(job.get("id") or "")
    if STATE.seen(KEY_VERDICT_COMPLETION + ":" + jid, ttl=VERDICT_COMPLETION_TTL):
        return "verdict completion was already attempted"
    if review_followup_submitted(jid):
        return "a review was already submitted and has not produced a verdict"
    if not result.get("review_evidence"):
        if result.get("auto_review_job_id"):
            return "a review job is recorded and has not produced a verdict"
        return None
    gap = review_evidence_gap(job, result)
    if gap is None:
        return "its review evidence covers the delivered head"
    head = review_verdict.delivered_head(job, result) or ""
    if result.get("re_review_head") == head or STATE.seen(_rereview_key(jid, head), ttl=REVIEW_FOLLOWUP_TTL_SEC):
        return f"already re-reviewed for this head ({gap})"
    return None


def escalate_stuck_needs_review(jobs: list[dict], *, evidence_fn, now: float | None = None) -> list[str]:
    """ONE ``doctor.needs_review_stuck`` message per row that is stuck, until it changes.

    Stuck: needs-review, not a triage report or protected lane, untouched for more than
    ``DOCTOR_NEEDS_REVIEW_ESCALATE_SECONDS``, and the review follow-up has nothing
    to do for it. The message names the row, its classification and the reason. The
    (classification, evidence, blocker) the message was sent for is kept durably
    (``review:stuck:<id>:<state hash>``), so the next sweep, and a restarted doctor, stay quiet
    until that state changes. Protected lanes and routing signals are never
    announced. Returns the ids announced.
    """
    if NEEDS_REVIEW_ESCALATE_SECONDS <= 0:
        return []
    now = time.time() if now is None else now
    classified = {
        str(r.get("job_id")): r
        for r in doctor_v2.sweep_needs_review(
            jobs, now=now, older_than_hours=0, summarize=review_verdict.evidence_summary
        )
    }
    sent: list[str] = []
    for job in jobs:
        jid = str(job.get("id") or "")
        row = classified.get(jid)
        if not jid or row is None or review_followup_protected(job):
            continue
        if str(job.get("kind") or "").startswith("triage:"):
            continue  # a triage report is meant to wait for a human; one message each would be noise
        exhausted = False
        if is_awaiting_review(job) and not review_queue.is_resolved(awaiting_review_entry(job)):
            # Operator rule 2026-10-31: an awaiting-review row is QUEUED work for
            # the review queue (it retries with backoff), not a row a human must
            # chase -- until its attempts are spent. Then only the owner can act,
            # and the row is escalated to them once like any other stuck row.
            if review_queue_stuck_owned(job, now=now):
                continue
            exhausted = True
        touched = _row_touched_at(job)
        if touched is None or now - touched <= NEEDS_REVIEW_ESCALATE_SECONDS:
            continue
        if exhausted:
            entry = awaiting_review_entry(job) or {}
            blocker = (
                f"cross-family review attempts exhausted ({int(entry.get('attempt') or 0)} tried, "
                f"last failure {entry.get('last_failure') or 'unknown'}): waiting for the owner"
            )
        else:
            blocker = review_followup_blocker(job)
        if blocker is None:
            continue
        state = "|".join([str(row["classification"]), str(row["evidence_summary"]), blocker])
        key = f"{KEY_REVIEW}:stuck:{jid}:{hashlib.sha1(state.encode()).hexdigest()[:12]}"
        if STATE.seen(key, ttl=REVIEW_FOLLOWUP_TTL_SEC):
            continue
        if len(sent) >= NEEDS_REVIEW_ESCALATE_MAX_PER_SWEEP:
            break
        suggestion = needs_review_suggestion(evidence_fn(job))
        broadcast_doctor_message(
            "doctor.needs_review_stuck",
            {
                "host": AGENT_HOST,
                "job_id": jid,
                "kind": job.get("kind"),
                "classification": row["classification"],
                "reason": f"{suggestion}; no automatic action: {blocker}",
                "evidence_summary": row["evidence_summary"],
                "idle_seconds": int(now - touched),
            },
        )
        STATE.mark(key, ttl=REVIEW_FOLLOWUP_TTL_SEC)
        sent.append(jid)
    return sent


def report_needs_review_sweep() -> dict:
    """Classify old needs-review jobs without cancelling, deleting, or retagging,
    and complete the stuck ``codex_delivery_failed`` rows whose verdict JSON is
    already in ``result.tail`` (one ``doctor.verdict_completed`` message per
    row, dry-run via ``DOCTOR_VERDICT_COMPLETION_DRY_RUN=1``)."""
    jobs = fetch_needs_review_jobs()
    # The review queue IS this listing: awaiting_review rows are advanced
    # BEFORE the report so the row it prints is the state the sweep just left
    # (submitted / parked / approved / backoff), never a stale one.
    try:
        # The sweep records each outcome on the row object it was handed, so
        # the report below reads the post-sweep state without a second fetch.
        review_queue_summary = run_review_queue_sweep(rows=jobs)
    except Exception as exc:  # noqa: BLE001 -- the report still has to run
        log.error("review queue sweep failed: %s", exc)
        broadcast_doctor_message("doctor.sweep_error", {"sweep": "review-queue", "error": str(exc)})
        review_queue_summary = {"errors": 1}
    rows = doctor_v2.sweep_needs_review(
        jobs, older_than_hours=NEEDS_REVIEW_OLDER_HOURS, summarize=review_verdict.evidence_summary
    )
    # Step 1 of docs/NEEDS_REVIEW_POLICY.md: evidence and a suggested action per
    # row, report only. The only write below is the once-per-state stuck message
    # (escalate_stuck_needs_review), which marks STATE after it was broadcast.
    by_id = {str(j.get("id")): j for j in jobs if j.get("id")}
    rows = [dict(row) for row in rows]
    evidence_fn = needs_review_evidence_fn()
    for row in rows:
        evidence = evidence_fn(by_id.get(str(row.get("job_id")), {"id": row.get("job_id")}))
        row["evidence"] = evidence
        row["suggested_action"] = needs_review_suggestion(evidence)
    for row in rows:
        job = by_id.get(str(row.get("job_id")))
        if job is not None:
            # The review queue's view of the row (reason, next_attempt_at,
            # attempts) and "not stale while the queue still owns it".
            _gate_review_queue_stuck(row, job, None)
        _log_report_row_once(row)
        broadcast_doctor_message("doctor.needs_review", {"host": AGENT_HOST, **row})
    seen_ids = {str(r.get("job_id")) for r in rows}
    for jid in [k for k in _NEEDS_REVIEW_LOGGED if k not in seen_ids]:
        del _NEEDS_REVIEW_LOGGED[jid]  # a row that left the report is logged afresh if it returns
    verdicts = complete_stuck_verdict_jobs(jobs)
    log.info("needs-review verdict completion: %s", verdicts)
    escalated = escalate_stuck_needs_review(jobs, evidence_fn=evidence_fn)
    return {
        "scanned": len(rows),
        "rows": rows,
        "verdict_completion": verdicts,
        "escalated": escalated,
        "review_queue": review_queue_summary,
    }


def run_needs_review_sweep() -> dict:
    """One combined needs-review sweep: the report-only classification plus the
    review follow-up.

    The report half is unchanged (classify old rows, never cancel/delete/retag).
    The follow-up half submits ONE read-only review job for every row that
    has no review performed yet (no result.review_evidence and no child
    review:* job), oldest first, capped by REVIEW_FOLLOWUP_MAX_PER_SWEEP. A row
    that already has a verdict is left for a human/orchestrator, and a
    needs-review row is never auto-purged — a row with commits carries
    delivered work, and a row without commits is still reviewable."""
    report = report_needs_review_sweep()
    followup = run_review_followup_sweep()
    report["review_followup"] = followup
    # ``report["review_queue"]`` is the review-queue half (awaiting_review rows
    # advanced on this same listing); the follow-up half only sees rows the
    # queue does not own.
    return report


def run_review_followup_sweep() -> dict:
    """Submit review jobs for needs-review rows that have no review performed."""
    rows = fetch_needs_review_jobs(limit=1000)
    # Oldest first: the per-sweep cap must drain the longest-stuck rows first.
    rows.sort(key=lambda j: j.get("ended_at") or j.get("started_at") or j.get("last_update_at") or 0)
    summary = {"scanned": len(rows), "submitted": 0, "skipped_verdict": 0, "skipped_marker": 0, "deferred": 0}
    for job in rows:
        jid = str(job.get("id") or "")
        if not jid:
            continue
        try:
            outcome = ensure_review_followup(job, allow_submit=summary["submitted"] < REVIEW_FOLLOWUP_MAX_PER_SWEEP)
            summary[outcome] = summary.get(outcome, 0) + 1
        except Exception as exc:
            summary["errors"] = summary.get("errors", 0) + 1
            log.error("review followup failed job=%s: %s", jid, exc)
            broadcast_doctor_message(
                "doctor.sweep_error", {"sweep": "review-followup", "job_id": jid, "error": str(exc)}
            )
    return summary


# ── Remediation sweep (the doctor FIXES rows, it does not only report) ──────


def fetch_remediable_rows(limit: int = 300) -> list[dict]:
    """The failed / needs-review / dead-letter rows the remediation sweep reads.

    A failed listing is a HARD error, not an empty list: an unreadable bus must
    never read as "nothing left to fix"."""
    rows: list[dict] = []
    for status in doctor_remediation.SCAN_STATUSES:
        code, resp = _http("GET", f"/v1/jobs?status={status}&limit={int(limit)}", timeout=20.0)
        if code != 200 or not isinstance(resp, dict) or not isinstance(resp.get("jobs"), list):
            broadcast_doctor_message(
                "doctor.sweep_error", {"sweep": "remediation", "status": status, "http_status": code}
            )
            raise RuntimeError(f"remediation listing incomplete status={status} http={code}")
        rows.extend(j for j in resp["jobs"] if isinstance(j, dict))
    return rows


def _remediate_apply_fields(jid: str, changes: dict, reason: str) -> tuple[int, Any]:
    """Write the changed fields onto the row (the audit trail rides along).

    The route takes a field repair only as part of a status move
    (``ReclassifyRequest.status`` is required), and merges it only when the row
    stays out of the queue. ``needs-review`` is the doctor's own holding state:
    the real status move that follows (requeue or hand-over) is a separate call.
    """
    body = {
        "job_ids": [jid],
        "status": "needs-review",
        "fields": {name: change.get("after") for name, change in changes.items()},
    }
    return _http("POST", "/v1/admin/jobs/reclassify", {**body, "reason": f"doctor-remediate:{reason}"}, timeout=20.0)


def _remediate_reclassify(jid: str, status: str, reason: str, override: bool, override_reason: str) -> tuple[int, Any]:
    """Move the row through the EXISTING admin reclassify path."""
    body: dict[str, Any] = {"job_ids": [jid], "status": status, "reason": f"doctor-remediate:{reason}"}
    if override:
        body["override_max_retries"] = True
        body["override_reason"] = override_reason
    return _http("POST", "/v1/admin/jobs/reclassify", body, timeout=20.0)


def _remediate_escalate(job: dict, why: str) -> None:
    """Escalate a row past the attempt cap (never a verdict, never a merge)."""
    jid = str(job.get("id") or "")
    broadcast_doctor_message("doctor.needs_review_stuck", {"host": AGENT_HOST, "job_ids": [jid], "reason": why})
    log.warning("remediation escalated %s: %s", jid[:12], why)


def run_remediation_sweep(*, dry_run: bool | None = None, now: float | None = None) -> dict:
    """Classify and FIX the triage backlog: one thin call site for the module.

    All the decision logic lives in :mod:`hive.workers.doctor_remediation` (so
    the sibling review-adoption branch merges cleanly); this function only wires
    the doctor's bus access and safety state to it. Disabled by
    ``HIVE_DOCTOR_REMEDIATE=0``."""
    if not doctor_remediation.REMEDIATE_ENABLED:
        return {"enabled": False, "applied": 0, "reason": "HIVE_DOCTOR_REMEDIATE=0"}
    # The durable store is checked BEFORE the bus is read: every guard in the
    # module is durable, so without the store a sweep would re-fix the same row
    # forever -- and a refused sweep must not have listed the board first.
    if not STATE.loaded_ok or not STATE.persist_ok:
        raise RuntimeError("remediation refused: durable state unavailable")
    rows = fetch_remediable_rows()
    roster = review_queue.load_roster()
    if not roster:
        # The classifier would read an empty roster as "no route serves this
        # model" and switch rows onto a route that does not exist. On a host
        # where the roster file is missing that is every row, so an unreadable
        # roster is a refused sweep, not an empty one (same rule as the store).
        raise RuntimeError("remediation refused: reviewer roster unavailable")
    summary = doctor_remediation.sweep(
        rows,
        state=STATE,
        apply_fields=_remediate_apply_fields,
        reclassify=_remediate_reclassify,
        escalate=_remediate_escalate,
        advance_review=lambda job: ensure_review_followup({**job, "status": "needs-review"}),
        # The real reviewer roster and the doctor's own route-health view: a
        # capped/down route is only visible through the probe, so leaving both
        # unset would have the sweep classify rows against a roster nobody
        # checked and switch them to a model that cannot serve them.
        roster=roster,
        health=_review_route_health(),
        dry_run=doctor_remediation.DRY_RUN if dry_run is None else dry_run,
        now=now,
        # Only for the review_job escalation note: is the reviewed work row settled?
        job_lookup=_chain_job,
    )
    summary["enabled"] = True
    # The review-driven half: request_changes -> continuation job -> re-review
    # (operator 2026-11-01). Same kill switch and store; the global DRY_RUN
    # also holds it back, because submitting a job is a write.
    try:
        summary["continuations"] = run_continuation_sweep(
            rows,
            roster=roster,
            dry_run=bool(DRY_RUN) or (doctor_remediation.DRY_RUN if dry_run is None else dry_run),
            now=now,
        )
    except Exception as exc:  # noqa: BLE001 -- the field-fix half already ran
        summary["continuations"] = {"errors": 1, "error": str(exc)}
        log.error("continuation sweep failed: %s", exc)
    broadcast_doctor_message("doctor.remediation", {"host": AGENT_HOST, **summary})
    return summary


def _continuation_verdict_only(row: dict, child: dict, tip: str, reviewed: str) -> bool:
    """Whether ``reviewed`` is a verdict-only descendant of ``tip``, proven on disk."""
    return classify_review_head(row, tip, reviewed, child=child).verdict == doctor_head_match.VERDICT_ONLY


def run_continuation_sweep(
    rows: list[dict], *, roster: list[dict] | None = None, dry_run: bool = False, now: float | None = None
) -> dict:
    """Wire :func:`doctor_remediation.continuation_sweep` to the bus.

    Children are this row's ``review:`` jobs; the continuation is submitted as
    the doctor; the escalation is the remediation one (``doctor.needs_review_stuck``).
    Refused, before any read, when the durable store is not durable: this pass
    CREATES jobs, and every guard on it is a durable record."""
    if not STATE.loaded_ok or not STATE.persist_ok:
        raise RuntimeError("continuation refused: durable state unavailable")
    return doctor_remediation.continuation_sweep(
        rows,
        state=STATE,
        children=_needs_review_child_review_jobs,
        submit=submit_job_spec,
        escalate=_remediate_escalate,
        roster=roster,
        now=now,
        dry_run=dry_run,
        head_is_verdict_only=_continuation_verdict_only,
        # The parent-chain read for a row whose lineage record and key are lost.
        job_lookup=_chain_job,
        # A NON-BLOCKING finding becomes its own row (operator 2026-11-01): it
        # never re-opens the grind loop and never blocks a merge.
        add_followup=submit_job_spec,
        # A fix commit that deletes or weakens tests fails its round (never
        # merged) and escalates once.
        integrity=doctor_remediation.test_integrity,
    )


def review_followup_protected(job: dict) -> bool:
    """Owner protection and routing-only signals apply to all review mutations."""
    return dlq_protected(job) or decline_paths.review_is_never_act(job)


# ── lane B / RiskyEats: never the doctor's rows to review (2026-10-31) ──────
# The operator's lane-B work (RiskyEats, and the test/ refactor/ fix/ docs/
# perf/ ops/ feat/ branch families) has its own owner, as do two owner-wait
# chains whose jobs the operator is holding. The review queue must not adopt,
# submit for, or touch them: their verdict is the owner's.
LANE_B_JOB_ID_PREFIXES = ("01a18ab8-0a4f", "01a18a8d-9ffa")
LANE_B_BRANCH_PREFIXES = ("test/", "refactor/", "fix/", "docs/", "perf/", "ops/", "feat/")
LANE_B_PROJECTS = frozenset({"riskyeats"})


def lane_b_row(job: dict) -> bool:
    """Whether this row belongs to lane B / RiskyEats or a lane-B owner-wait chain."""
    if not isinstance(job, dict):
        return False
    jid = str(job.get("id") or "").strip().lower()
    if jid.startswith(LANE_B_JOB_ID_PREFIXES):
        return True
    if str(job.get("project") or "").strip().lower() in LANE_B_PROJECTS:
        return True
    repo = str(job.get("repo") or "").strip().lower()
    if repo.rsplit("/", 1)[-1] in LANE_B_PROJECTS:
        return True
    head = str(job.get("head_branch") or "").strip().lower()
    return head.startswith(LANE_B_BRANCH_PREFIXES)


def ensure_review_followup(job: dict, *, allow_submit: bool = True) -> str:
    """Shared immediate/reconciliation path; never mutate protected lanes."""
    jid = str(job.get("id") or "")
    if not jid or job.get("status") != "needs-review":
        return "skipped_status"
    # Triage rows are remediation reports, not branches for code review.
    if str(job.get("kind") or "").startswith("triage:"):
        return "skipped_triage"
    # A review job is never itself reviewed (PYTHIA 2026-11-01).
    if review_queue.is_review_kind(job):
        return "skipped_review_job"
    if review_followup_protected(job):
        return "skipped_protected"
    if not STATE.loaded_ok or not STATE.persist_ok:
        raise RuntimeError("review dispatch refused: durable state unavailable")
    result = _row_result_dict(job)
    if result.get("verdict"):
        return "skipped_verdict"
    if review_queue.review_state(job) is not None:
        # The review queue owns this row (cross-family pairing, backoff, park):
        # dispatching the generic family-unpinned follow-up here would submit a
        # SECOND review under a different idempotency key.
        return "skipped_review_queue"
    children = _needs_review_child_review_jobs(job)
    gap = review_evidence_gap(job, result)
    for child in children:
        if result.get("auto_review_job_id") and result["auto_review_job_id"] != child.get("id"):
            continue
        proof = _row_result_dict(child)
        # The bus's own normalisation: the legacy dict ``result.verdict`` and the
        # worker's ``result.reviewer.verdict`` are the same review.
        verdict = review_verdict.child_verdict(proof)
        if child.get("status") != "done" or verdict is None:
            continue
        previous = result.get("review_evidence")
        if (
            isinstance(previous, dict)
            and previous.get("review_job_id") == child.get("id")
            and previous.get("verdict") == verdict
            and previous.get("artifact_valid") is True
            and previous.get("head_matches") is True
        ):
            return "skipped_verdict"
        # The bus already recorded exactly this review and it does not cover the
        # delivered head: replaying it would change nothing, on every sweep, and
        # the evidence gap below would never be reached.
        if (
            gap is not None
            and isinstance(previous, dict)
            and previous.get("review_job_id") == child.get("id")
            and previous.get("verdict") == verdict
        ):
            continue
        # Replay the child completion through the bus's transactional verifier.
        # The doctor never manufactures parent artifact proof or approval.
        body = {"status": "done", "result": proof}
        if child.get("claimed_by"):
            body["claimed_by"] = child["claimed_by"]
        code, _ = _http("PATCH", f"/v1/jobs/{child['id']}", body)
        if code not in (200, 201, 204):
            raise RuntimeError(f"review verdict replay failed http={code}")
        return "replayed_verdict"
    if review_followup_submitted(jid) or STATE.seen(KEY_VERDICT_COMPLETION + ":" + jid, ttl=VERDICT_COMPLETION_TTL):
        return "skipped_marker"
    if children or result.get("auto_review_job_id") or result.get("review_evidence"):
        # Evidence only counts as "reviewed" when it applies to the head that was
        # delivered. An evidence gap earns ONE re-review per (row, head): never while
        # a review is in flight, never twice for the same head, and never past the
        # markers/caps above.
        if gap is None or any(c.get("status") not in _REVIEW_CHILD_TERMINAL for c in children):
            return "skipped_verdict"
        head = review_verdict.delivered_head(job, result) or ""
        if result.get("re_review_head") == head or STATE.seen(_rereview_key(jid, head), ttl=REVIEW_FOLLOWUP_TTL_SEC):
            return "skipped_marker"
    if not allow_submit:
        return "deferred"
    if _review_child_refusal(job):
        return "skipped_review_chain"
    rereview_reason = gap or ""
    review_id, spec, note = _submit_review_followup(job, rereview_reason=rereview_reason)
    if not review_id:
        raise RuntimeError(note)
    _record_auto_review(job, review_id, rereview_reason=rereview_reason)
    payload = {
        "host": AGENT_HOST,
        "job_id": jid,
        "auto_review_job_id": review_id,
        "review_kind": spec["kind"],
        "note": note,
    }
    if rereview_reason:
        payload["re_review"] = rereview_reason
    broadcast_doctor_message("doctor.needs_review", payload)
    return "submitted"


# ── review queue inside the triage queue (operator 2026-10-31) ─────────────
# The review queue IS the triage queue: the Hive job rows in needs-review this
# sweep already reads. A row whose loop authored work but got no reviewer (the
# endpoint was busy, timed out, answered prose, or returned 503) is a
# needs-review row carrying result.review_state.state='awaiting_review' -- the
# worker puts it there instead of failing it. This sweep consumes those rows:
# it picks a CROSS-FAMILY reviewer from hive/core/reviewer-roster.json for the
# author's family, submits ONE read-only review job naming that reviewer,
# records the attempt on the same row, and on a structured APPROVE hands the
# row to the merge path, on request_changes back to the author path, and on a
# reviewer failure schedules the next attempt with exponential backoff bounded
# by a per-route concurrency cap. A row with no cross-family reviewer available
# stays awaiting_review (parked): never self-reviewed, never failed.

# ── route health ─────────────────────────────────────────────────────────────
# ``REVIEW_QUEUE_ROUTE_HEALTH`` is the doctor's view of each reviewer route by
# roster route id (``{"cerberus-gpt-oss": False}``): a route a HOST has marked
# down (or that failed its last probe) must not receive more work. It is
# process state the doctor's health loop fills; an UNLISTED route is unknown,
# not down -- a provider route has no local probe and rate limits are "busy",
# which the load/concurrency cap already models. Rows are matched on the route
# id, the model id and every role id the route serves.
REVIEW_QUEUE_ROUTE_HEALTH: dict = {}
# Override for the per-route load reader (a callable returning one); None uses
# the cached 30 s llama-server ``/slots`` / vLLM metrics probe over the roster.
REVIEW_QUEUE_ROUTE_LOAD = None
# Codex (operator 2026-10-31): the plan-usage probe the roster's quota guard
# reads. None means "no readable share", which the gate treats as fail-closed
# (``codex_usage_unknown``); a host with a probe overrides it with a callable
# returning ``{"remaining_pct": float}``. The doctor NEVER logs in, refreshes or
# reads the OAuth token for this: PYTHIA holds the master token and the confined
# codex worker spends it.
CODEX_REVIEW_USAGE = None


def _codex_worker_online() -> bool | None:
    """Whether a ``codex``-kind worker is online (``None``: the bus cannot say)."""
    kinds = _online_worker_kinds()
    if kinds is None:
        # Bus unreachable: not "offline", just unknown. The gate fails closed
        # either way, but the reason it reports must be the true one.
        return None
    return review_queue.CODEX_WORKER_KIND in kinds


def review_queue_codex_gate() -> dict:
    """The doctor's Codex gate: config switch + online worker + usage guard."""
    return review_queue.codex_gate(
        worker_online=_codex_worker_online(),
        usage=CODEX_REVIEW_USAGE,
    )


def _review_lane(job: dict, result: dict, roster: list[dict]) -> str:
    """The row's review lane: hard for architecture/security-sensitive work."""
    paths = result.get("files_changed") if isinstance(result.get("files_changed"), list) else []
    return review_queue.review_lane(
        repo=job.get("repo"),
        paths=[str(p) for p in paths if str(p).strip()],
        kind=str(job.get("kind") or ""),
    )


# ── adopting legacy rows (operator 2026-10-31) ──────────────────────────────
# The deployed worker line does not write ``result.review_state``, so every one
# of its needs-review rows was invisible to the queue above (production logged
# ``scanned=0 submitted=0`` while thirteen rows sat unapproved). With this
# switch ON (the default, operator 2026-10-31) the sweep ADOPTS such a row --
# builds its review_state from the row, status untouched -- and reviews it like
# any other queue row. ``HIVE_DOCTOR_ADOPT_DRY_RUN`` only logs what it would
# submit; the per-sweep cap bounds how many review jobs one pass sends.
HIVE_DOCTOR_ADOPT_LEGACY_REVIEWS = os.environ.get("HIVE_DOCTOR_ADOPT_LEGACY_REVIEWS", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
HIVE_DOCTOR_REVIEW_SUBMIT_MAX = _env_int("HIVE_DOCTOR_REVIEW_SUBMIT_MAX", 3, minimum=0)
HIVE_DOCTOR_ADOPT_DRY_RUN = os.environ.get("HIVE_DOCTOR_ADOPT_DRY_RUN", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)


def _review_route_health():
    """``health(route) -> bool`` for ``pick_reviewers``."""

    def healthy(route: dict) -> bool:
        for key in (route.get("id"), route.get("model_id"), route.get("role_id"), *(route.get("roles") or [])):
            if key and str(key) in REVIEW_QUEUE_ROUTE_HEALTH:
                return bool(REVIEW_QUEUE_ROUTE_HEALTH[str(key)])
        return True

    return healthy


def review_queue_load_reader():
    """The cached (30 s) per-route load reader ``pick_reviewers`` takes."""
    override = REVIEW_QUEUE_ROUTE_LOAD
    if override is None:
        return review_queue.route_load_fn()
    if callable(override):
        return lambda route: override(route) if override.__code__.co_argcount else override()
    return lambda route: override


def _review_children_by_key(job: dict, idempotency_key: str) -> list[dict]:
    """The row's review children that hold ``idempotency_key`` ([] when none).

    Read through the same child listing the verdict read uses; a failed
    listing is not an answer and yields nothing here (the caller then reports
    the submit failure it already has).
    """
    try:
        children = _needs_review_child_review_jobs(job)
    except RuntimeError:
        return []
    return [child for child in children if str(child.get("idempotency_key") or "") == idempotency_key]


def _same_model(a: Any, b: Any) -> bool:
    return str(a or "").strip().lower() == str(b or "").strip().lower()


def _author_model(job: dict, result: dict) -> str:
    """The author model of a row: result.review_state, models, or the claim."""
    state = review_queue.review_state(job) or {}
    for candidate in (
        state.get("author"),
        (result.get("models") or {}).get("author") if isinstance(result.get("models"), dict) else None,
        result.get("author_model"),
        job.get("claimed_model"),
    ):
        if str(candidate or "").strip():
            return str(candidate).strip()
    return ""


def _review_head(job: dict, result: dict) -> str:
    return review_verdict.delivered_head(job, result) or str(result.get("head_sha") or job.get("head_sha") or "")


def _excluded_families(author: str, *others: Any) -> set:
    """Every family a reviewer may not have: the author's and each co-author's."""
    families = {review_queue.family_of(m) for m in [author, *others] if str(m or "").strip()}
    families.discard("")
    return families


def interpret_review_outcome(
    raw: Any, *, head_sha: str | None = None, author_families: set | frozenset | None = None
) -> dict:
    """Read a review job's output as a structured verdict -- or refuse it.

    Only a structured JSON verdict naming ``reviewers[].model`` counts
    (operator 2026-10-31). Prose, an empty body, a truncated fragment, a
    503-style error object or JSON without a reviewer list is NOT an approval:
    every one raises ValueError with the failure kind the sweep records on the
    row (``review_prose_only``, ``review_empty``, ``review_truncated``,
    ``review_http_503``, ``review_non_json``).

    Only CROSS-family reviewers count: an entry whose model is in one of
    ``author_families`` is the author grading itself and is ignored; a verdict
    with no cross-family entry left is ``review_same_family``. An approve must
    name the delivered head: a different head is ``review_head_mismatch``, a
    verdict (or a row) that names no head is ``review_head_missing``.
    """
    payload = raw
    if isinstance(payload, str):
        text = payload.strip()
        if not text:
            raise ValueError("review_empty")
        if text[:1] not in "{[":
            # Text that never started a JSON document is prose, whatever its
            # last character; only a cut-off JSON document is "truncated".
            raise ValueError("review_prose_only")
        try:
            payload = json.loads(text)
        except ValueError:
            raise ValueError("review_truncated" if text[-1:] not in "}]" else "review_non_json") from None
    if isinstance(payload, dict) and isinstance(payload.get("result"), dict):
        payload = payload["result"]  # a raw job row: read its result
    if not isinstance(payload, dict):
        raise ValueError("review_non_json")
    if payload.get("http_status") == 503 or payload.get("status_code") == 503:
        raise ValueError("review_http_503")
    entries = payload.get("reviewers")
    if not isinstance(entries, list) or not entries:
        raise ValueError("review_no_reviewers")
    reviewers, verdicts = [], []
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("review_no_reviewers")
        model = str(entry.get("model") or entry.get("model_id") or "").strip()
        verdict = review_verdict.normalize_verdict(entry.get("verdict"))
        if not model or not verdict:
            raise ValueError("review_no_reviewers")
        reviewers.append(model)
        verdicts.append(verdict)
    blocked = {f for f in (author_families or ()) if f}
    cross = [(m, v) for m, v in zip(reviewers, verdicts, strict=True) if review_queue.family_of(m) not in blocked]
    if not cross:
        raise ValueError("review_same_family")
    unreviewed = False
    reviewers = [m for m, _ in cross]
    verdicts = [v for _, v in cross]
    reviewing = verdicts
    reviewed_head = str(payload.get("head_sha") or "").strip()
    if "approve" in reviewing and "request_changes" not in reviewing:
        if not head_sha or not reviewed_head:
            raise ValueError("review_head_missing")
        if not review_verdict.same_sha(reviewed_head, head_sha):
            raise ValueError("review_head_mismatch")
        kind = "approve"
    # ``normalize_verdict`` returns ``request_changes`` (``-`` read as ``_``),
    # so the comparison is against the normalized spelling, not the prose one.
    elif (
        "request_changes" in reviewing or review_verdict.normalize_verdict(payload.get("verdict")) == "request_changes"
    ):
        kind = "request_changes"
    elif reviewing and all(v == "blocked" for v in reviewing):
        kind = "reviewer_failure"
    else:
        raise ValueError("review_undecided")
    return {
        "kind": kind,
        "reviewers": reviewers,
        "head_sha": reviewed_head or head_sha or "",
        "verdicts": verdicts,
        "unreviewed": unreviewed,
    }


def _descent_workspaces(job: dict, approved: dict) -> list[str]:
    """The checkouts that may hold ``approved_sha``: the row's, its in-flight child's, the APPROVING child's.

    Recording the approval clears ``review_job_id`` (the row is settled), so
    the in-flight lookup finds nothing afterwards; the approving child is the
    one ``approved["review_job_id"]`` names, read from the row's child listing
    (its recorded workdir) or by its id under the workspace root.
    """
    repos = [_head_branch_workspace(job), _child_workspace(_head_match_child(job) or {})]
    approver = str(approved.get("review_job_id") or "").strip()
    if approver:
        listed = None
        try:
            listed = next((c for c in _needs_review_child_review_jobs(job) if str(c.get("id") or "") == approver), None)
        except RuntimeError:
            listed = None
        repos.append(_child_workspace(listed or {"id": approver}))
    seen: list[str] = []
    for repo in repos:
        if repo and repo not in seen:
            seen.append(repo)
    return seen


def _descent_proof(job: dict, approved: dict) -> str:
    """Re-prove a recorded verdict-only descent against git: ``proven`` | ``refused`` | ``not_proven``.

    ``approved["basis"]`` is the PROOF-BEARING field: only the classifier writes
    it, and only after it has diffed ``T..R`` against git and found nothing but
    verdict artifacts. It is still never taken on trust: a workspace holding
    both commits must answer now, and every workspace consulted must agree (a
    code change in the descent -- a hand-edited row, a force-push rewriting it
    -- refuses the approve). ``verdict_only_descendant`` alone is a flag any
    writer can set and is not proof at all.

    No workspace holding both commits is ``not_proven``: head-match is an
    approval gate and fails CLOSED. A row whose checkout aged out is re-proved
    (or re-reviewed at its tip) -- it is never merged on a record alone.
    """
    if str(approved.get("basis") or "") != BASIS_VERDICT_ONLY:
        return "refused"
    tip = str(approved.get("head_sha") or "").strip()
    reviewed = str(approved.get("approved_sha") or "").strip()
    if not tip or not reviewed:
        return "refused"
    checked = False
    for repo in _descent_workspaces(job, approved):
        if repo and doctor_head_match.checkout_for(repo, tip, reviewed):
            checked = True
            if not doctor_head_match.verdict_only_between(repo, tip, reviewed):
                return "refused"
    if checked:
        return "proven"
    log.info(
        "head-match: no workspace holds %s..%s; the recorded verdict-only basis for %s is not proven",
        str(tip)[:12],
        str(reviewed)[:12],
        str(job.get("id") or "")[:12],
    )
    return "not_proven"


def _verdict_only_descendant_approval(job: dict, approved: dict) -> bool:
    """Whether ``approved`` names a REVIEWED commit PROVEN to descend from the delivered head by a verdict only."""
    return _descent_proof(job, approved) == "proven"


def merge_handoff(job: dict) -> str:
    """The merge path's verdict on a row: approved rows are ready to merge.

    ``doctor.review_approved`` names the row and the approving reviewers; the
    merge train (``hive/workers/merge_train.py``) consumes needs-review rows,
    so an approved row stays exactly where it was -- in the triage queue.
    """
    state = review_queue.review_state(job) or {}
    if not state:
        # ``review_state()`` only admits the IN-FLIGHT shape (``awaiting_review``);
        # a row the head-match rule already cleared carries ``approved_mergeable``
        # and would otherwise read as "never reviewed" -- the other half of the
        # critical the r2 review found (the doctor cleared the row, the merge
        # pass still refused to act). Read the block directly.
        raw = _row_result_dict(job).get("review_state")
        state = raw if isinstance(raw, dict) else {}
    approved = state.get("approved")
    if not isinstance(approved, dict) or not approved.get("reviewers"):
        approved = state.get("approved_mergeable")
    if not isinstance(approved, dict) or not approved.get("reviewers"):
        return "needs_review"
    if approved.get("head_sha") and not review_verdict.same_sha(
        approved["head_sha"], _review_head(job, _row_result_dict(job))
    ):
        return "stale_approval"
    # ONE descent proof per decision: asking git twice lets the workspace change
    # between the questions, so "refused" could be reported as "not proven" (or
    # the reverse). It is only asked when the reviewer signed a different commit.
    proof = (
        _descent_proof(job, approved)
        if approved.get("approved_sha") and not review_verdict.same_sha(approved["approved_sha"], approved["head_sha"])
        else "proven"
    )
    if proof != "proven":
        if proof == "not_proven":
            # A verdict-only basis no workspace can re-prove: not stale (nothing
            # says the head moved) and NOT mergeable -- the gate fails closed.
            return "approval_not_proven"
        # An approval recorded at a verdict-only DESCENDANT covers the delivered
        # head: ``approved.head_sha`` is that head (T) and ``approved_sha`` is the
        # commit the reviewer signed (R, T an ancestor of R, R..T diff touching
        # only verdict artifacts -- the classifier proved both before recording
        # it). Demanding approved_sha == head_sha here made EVERY such approval
        # read stale, which is the critical the r2 review found: the doctor
        # cleared the row and the merge pass still refused to act.
        #
        # ``approved.head_sha`` was checked against the CURRENT delivered head
        # just above, so a row rewritten since the approve (a new tip) is still
        # stale; the descendant exemption cannot hide that.
        return "stale_approval"
    if review_followup_protected(job):
        return "protected"
    return "ready_to_merge"


def _review_queue_rows(rows: list[dict]) -> list[dict]:
    """The rows the queue owns, oldest-owed first (adopted rows go last).

    A row with a complete review_state is advanced exactly as before. A LEGACY
    row (one the older worker line left without review_state) is admitted too:
    it is owed a review, and the operator's point is that the queue must do the
    reviewing (2026-10-31). Adopted rows are ordered LAST so a backlog of rows
    the queue already owns drains first, and so a single legacy backlog cannot
    starve them.
    """

    def _owed(row: dict) -> tuple:
        state = review_queue.usable_review_state(row)
        if state is not None:
            return (0, state.get("next_attempt_at") or 0)
        if _adoptable_row(row):
            return (1, row.get("ended_at") or row.get("started_at") or row.get("last_update_at") or 0)
        return (2, 0)

    ordered = [row for row in rows if review_queue.review_state(row) is not None or _adoptable_row(row)]
    return sorted(ordered, key=_owed)


def _adoptable_row(job: dict) -> bool:
    """True when this sweep must ACT on the row: adopt it, or advance it.

    A row still carrying a complete ``review_state`` is already the queue's
    (adopted by an earlier sweep, or written by the newer worker line): it is
    not adopted again, but its in-flight review, backoff, approve and retry
    bookkeeping still have to be advanced, so it stays in the pass.
    """
    if _adoption_for(job) is not None:
        return True
    return _review_state_row(job) is not None


def _review_state_row(job: dict) -> dict | None:
    """The complete review_state of a needs-review row this sweep may advance."""
    if not HIVE_DOCTOR_ADOPT_LEGACY_REVIEWS:
        return None
    if str(job.get("status") or "") != "needs-review":
        return None
    if str(job.get("kind") or "").startswith("triage:"):
        return None
    # A review job carrying a review_state (the remediation sweep wrote one on
    # PYTHIA 2026-11-01) is still never advanced: that is the review-of-review.
    if review_queue.is_review_kind(job):
        return None
    if lane_b_row(job):
        return None
    return review_queue.usable_review_state(job)


def _adoption_for(job: dict) -> dict | None:
    """The review_state a legacy row would be adopted with, or None.

    Lane B / RiskyEats rows and the lane-B owner-wait chains are never adopted
    (operator 2026-10-31): another owner is already waiting on them.
    """
    if not HIVE_DOCTOR_ADOPT_LEGACY_REVIEWS:
        return None
    if str(job.get("status") or "") != "needs-review":
        return None
    if str(job.get("kind") or "").startswith("triage:"):
        return None
    # A review job is never adopted: adopting it queues a review OF the review.
    if review_queue.is_review_kind(job):
        return None
    if lane_b_row(job):
        return None
    return review_queue.adoption_review_state(job)


def _in_flight_review_job_id(job: dict, result: dict) -> str:
    """The row's live review child id, or "" -- the guard before route picking.

    Two sources, both recorded on the row rather than in review_state: the
    worker's own ``result.auto_review_job_id`` and any ``review:`` child the
    bus still lists as non-terminal. A row whose review is already running is
    never submitted again (operator 2026-10-31: no duplicate review jobs).
    """
    recorded = str(result.get("auto_review_job_id") or "").strip()
    if recorded:
        return recorded
    try:
        children = _needs_review_child_review_jobs(job)
    except RuntimeError:
        # An unreadable listing is not "no review": the caller falls through to
        # its own in-flight handling, which reports the error.
        return ""
    for child in children:
        if str(child.get("status") or "") not in _REVIEW_CHILD_TERMINAL:
            return str(child.get("id") or "").strip()
    return ""


def _review_is_in_flight(job: dict) -> bool:
    """Whether a review child for this row is still running (never re-adopt)."""
    try:
        children = _needs_review_child_review_jobs(job)
    except RuntimeError:
        # Cannot prove the row is unclaimed: treat it as in flight so a sweep
        # never submits a second review on an unreadable listing.
        return True
    return any(str(c.get("status") or "") not in _REVIEW_CHILD_TERMINAL for c in children)


def adopt_legacy_review_row(job: dict, *, now: float | None = None) -> bool:
    """Give a legacy needs-review row the review_state the queue advances.

    The row's STATUS is not touched (still needs-review: the queue IS triage);
    only ``result.review_state`` is added, built from what the row already
    records. Returns True when the row now carries a state it did not before.
    """
    if review_queue.usable_review_state(job) is not None:
        return False
    if _review_is_in_flight(job):
        # A review child is already running: the row is not legacy, it is busy.
        return False
    state = _adoption_for(job)
    if state is None:
        return False
    result = _row_result_dict(job)
    result["review_state"] = state
    job["result"] = result
    return True


def _review_child_outcome(child: dict, *, head_sha: str = "", author_families: set | None = None) -> dict | None:
    """Read one child review job's output; None while it has not finished.

    ``head_sha`` is the SOURCE row's delivered head and ``author_families`` the
    families that may not review it. Neither is read from the child: the
    child's own ``head_sha`` is whatever the reviewer claims it reviewed, so
    checking a verdict against it would accept a review of any commit.
    """
    status = str(child.get("status") or "")
    if status not in _REVIEW_CHILD_TERMINAL:
        return None
    proof = _row_result_dict(child)
    if proof.get("review_overreach") or proof.get("failure_reason") == "review_overreach":
        # The worker refused this review as overreach: whatever verdict it also
        # carries covers the reviewer's own edits, never the author's tip. It is
        # never parsed as an approve (a failed child WITH a verdict otherwise is).
        return {"kind": "reviewer_failure", "failure": "review_overreach", "reviewers": []}
    sources = []
    for key in ("review_evidence", "verdict"):
        if proof.get(key):
            sources.append(proof[key])
    if proof.get("reviewers"):
        # A review job whose verdict IS its result (reviewers[].model plus the
        # reviewed head), with no prose beside it. Without this the finished
        # review read as empty and the row was retried instead of resolved.
        sources.append(proof)
    tail = proof.get("tail")
    if isinstance(tail, str) and tail.strip():
        sources.append(tail)
    if result_text := proof.get("stdout"):
        sources.append(result_text) if isinstance(result_text, str) else None
    if not sources:
        return {"kind": "reviewer_failure", "failure": "review_empty", "reviewers": []}
    if status != "done" and not proof.get("verdict"):
        return {
            "kind": "reviewer_failure",
            "failure": "review_http_503" if "503" in str(tail) else "review_failed",
            "reviewers": [],
        }
    failure = None
    for candidate in sources:
        try:
            return interpret_review_outcome(candidate, head_sha=head_sha, author_families=author_families)
        except ValueError as exc:
            failure = str(exc)
    if failure == "review_head_mismatch":
        # A structured approve for a head OTHER than the delivered one. It is
        # not an approval of this head -- but the verdict itself is real, so it
        # is returned for the caller's verdict-descendant rule to judge
        # (verdict-only descendant / newer work / overreach). Judging it here
        # would close the bug the other way round: the exact-head rule was the
        # defect (production 2026-10-31), so the shape must survive to the
        # classifier rather than collapse into "reviewer_failure".
        # The source that NAMES the head wins: a parallel evidence block may
        # carry the same verdict without one.
        structured = [
            verdict
            for verdict in (_structured_verdict(c, author_families=author_families) for c in sources)
            if verdict is not None and verdict.get("kind") == "approve"
        ]
        for verdict in sorted(structured, key=lambda v: not v.get("head_sha")):
            if verdict.get("head_sha"):
                return verdict
        if structured:
            return structured[0]
    return {"kind": "reviewer_failure", "failure": failure or "review_no_reviewers", "reviewers": []}


def _structured_verdict(raw: Any, *, author_families: set | frozenset | None = None) -> dict | None:
    """A structured verdict read WITHOUT the head check (cross-family still enforced).

    ``{"kind": "approve" | "request_changes" | "reviewer_failure", "reviewers",
    "head_sha", "verdicts"}``, or None when ``raw`` is not a usable structured
    verdict. The reviewed head is whatever the payload names ('' when none).
    """
    payload = raw
    if isinstance(payload, str):
        text = payload.strip()
        if not text or text[:1] not in "{[":
            return None
        try:
            payload = json.loads(text)
        except ValueError:
            return None
    if isinstance(payload, dict) and isinstance(payload.get("result"), dict):
        payload = payload["result"]
    if not isinstance(payload, dict):
        return None
    entries = payload.get("reviewers")
    if not isinstance(entries, list) or not entries:
        return None
    blocked = {f for f in (author_families or ()) if f}
    cross = []
    for entry in entries:
        if not isinstance(entry, dict):
            return None
        model = str(entry.get("model") or entry.get("model_id") or "").strip()
        verdict = review_verdict.normalize_verdict(entry.get("verdict"))
        if not model or not verdict:
            return None
        if review_queue.family_of(model) in blocked:
            continue
        cross.append((model, verdict))
    if not cross:
        return None
    verdicts = [verdict for _, verdict in cross]
    if "approve" in verdicts and "request_changes" not in verdicts:
        kind = "approve"
    elif "request_changes" in verdicts or review_verdict.normalize_verdict(payload.get("verdict")) == "request_changes":
        kind = "request_changes"
    elif all(v == "blocked" for v in verdicts):
        kind = "reviewer_failure"
    else:
        return None
    return {
        "kind": kind,
        "reviewers": [model for model, _ in cross],
        "head_sha": str(payload.get("head_sha") or "").strip(),
        "verdicts": verdicts,
    }


def _tried_entry(entry: dict, failure: str, now: float) -> dict:
    return {"model_id": entry.get("model_id"), "family": entry.get("family"), "failure": failure, "at": now}


def _patch_review_state(job: dict, state: dict, *, verdict: dict | None = None) -> bool:
    """Record the queue state on the SAME row (still needs-review: the queue IS triage)."""
    jid = str(job.get("id") or "")
    res = _row_result_dict(job)
    body_result = {**res, "review_state": state}
    body: dict = {"status": "needs-review", "result": body_result}
    if verdict is not None:
        body_result["verdict"] = verdict
        body_result["review_evidence"] = {
            **(body_result.get("review_evidence") if isinstance(body_result.get("review_evidence"), dict) else {}),
            **verdict,
            "review_job_id": state.get("review_job_id"),
        }
        review_record.apply(
            body_result, launched=verdict.get("reviewers", [""])[0] if verdict.get("reviewers") else None
        )
    if job.get("claimed_by"):
        body["claimed_by"] = job["claimed_by"]
    code, _ = _http("PATCH", f"/v1/jobs/{jid}", body)
    if code not in (200, 201, 204):
        broadcast_doctor_message(
            "doctor.sweep_error", {"sweep": "review-queue", "job_id": jid, "error": f"annotation failed http={code}"}
        )
        return False
    # Mirror the accepted state onto the in-memory row, so the report built from
    # THIS listing prints what the queue just left -- without a second GET of
    # the same rows (the report is read-only: exactly one listing fetch).
    if isinstance(job, dict):
        job["result"] = body_result
        job["status"] = "needs-review"
        if body.get("claimed_by"):
            job["claimed_by"] = body["claimed_by"]
    return True


def _resume_state(state: dict) -> dict:
    """Clear a parked reason before a new attempt (a retry is possible again).

    Both park reasons are TRANSIENT: no cross-family reviewer right now, or the
    attempts were spent. A fresh attempt that fails again re-parks the row with
    the same or a higher count, so the state never claims a park it has left.
    """
    out = dict(state)
    if out.get("parked_reason") in (
        "in_backoff_or_parked_not_supported",
        "no_cross_family_reviewer",
        "attempts_exhausted_owner_wait",
    ):
        out.pop("parked_reason", None)
    return out


def _pick_routes(
    author: str,
    job: dict | None,
    roster: list[dict],
    health,
    load,
    co_authors: list[str],
    *,
    lane: str = review_queue.LANE_BULK,
) -> list[dict]:
    """The cross-family pick, saturated routes last, never a blocked family.

    Every Codex pick is behind the roster gate (``review_queue_codex_gate``:
    config switch, online codex worker, plan usage, cooldown marker), so no
    cron or hook can start a Codex review without it; an UNSET
    ``CODEX_REVIEW_USAGE`` probe makes the gate refuse (fail closed).
    """
    job = job or {}
    cohort = _excluded_families(author, *co_authors)
    picked = pick_reviewers(
        author,
        roster,
        health=health,
        load=load,
        co_authors=[m for m in co_authors if str(m or "").strip()],
        repo=job.get("repo"),
        author_routes=[str(job.get("claimed_agent") or "")] if job.get("claimed_agent") else None,
        lane=lane,
        codex_worker_online=_codex_worker_online(),
        codex_usage=CODEX_REVIEW_USAGE,
    )
    return [route for route in _sort_routes_by_family_load(picked, load) if route["family"] not in cohort]


def _review_kind_for(base_kind: str) -> str:
    date = time.strftime("%Y%m%d", time.gmtime())
    return f"review:{base_kind}-{date}" if base_kind else f"review:{date}"


def _review_ref_slug(branch: str | None) -> str:
    """The verdict-artifact branch part (``review_queue.review_ref_slug``, shared with the worker)."""
    return review_queue.review_ref_slug(branch)


def _review_queue_submit(job: dict, state: dict, reviewers: list[dict], *, now: float) -> tuple[str, dict]:
    """Submit ONE read-only review job naming the cross-family pick(s).

    Raises (nothing submitted) for a review-kind row or a chain past the depth
    guard: see :func:`_review_child_refusal`."""
    refused = _review_child_refusal(job)
    if refused:
        raise RuntimeError(f"refused review child: {refused}")
    jid = str(job.get("id") or "")
    res = _row_result_dict(job)
    kind = str(job.get("kind") or "").strip()
    base_kind = (
        kind[len("review:") :]
        if kind.startswith("review:")
        else (kind[len("triage:") :] if kind.startswith("triage:") else _base_kind(kind))
    )
    date = time.strftime("%Y%m%d", time.gmtime())
    review_kind = f"review:{base_kind}-{date}" if base_kind else f"review:{date}"
    head = job.get("head_branch") or ""
    # The reviewed head: the delivered commit; a row that recorded none is
    # reviewed against the branch tip (the job says so in its description).
    head_sha = _review_head(job, res) or "unrecorded"
    base = job.get("base_branch") or ""
    author = state.get("author") or ""
    names = [r["model_id"] for r in reviewers]
    desc = (
        "READ-ONLY review. Do not modify any file, branch, or commit. "
        f"Review the branch {head or '(see job)'} at HEAD {head_sha or '(resolve and report)'} against base {base or '(default)'} "
        "with a reviewer from a DIFFERENT model family than the author "
        f"(author {author or 'unknown'}, family {state.get('author_family') or 'unknown'}; "
        f"review with {', '.join(names)}). "
        "Return a structured verdict JSON with a 'reviewers' list of {model, verdict} entries "
        "(verdict: approve | request-changes | blocked), a 'head_sha' field containing the exact reviewed commit, "
        f"and a 'reason' field. Prose is not a verdict. Reviewed job id: {jid}. "
        # The verdict artifact goes to its OWN ref, never onto the author's branch
        # (production 2026-10-31: four rows would not close because the review job
        # committed its verdict json onto the author branch). The ref is keyed by
        # the REVIEW job's own id, which does not exist until this submit
        # returns: the worker resolves it and appends the exact ref to this
        # brief (``job_delivery_brief``). No placeholder is ever sent -- a
        # literal ``<job id>`` was followed verbatim, or "resolved" to the
        # reviewed job's id printed just above.
        # Both names come from ``review_queue`` -- the same helpers the worker's
        # delivery brief and overreach guard use -- so the two never disagree.
        "The ONLY write a review job makes is its verdict artifact "
        f"{review_queue.review_verdict_paths(head, None)[0]}; push it to the "
        f"SEPARATE ref {review_queue.review_ref_prefix(head)}(this review job's own id), exactly as spelled in "
        "this job's delivery policy below (never the author branch). "
        f"Never commit, push or amend anything on the author branch {head or '(see job)'}: "
        "any other file changed there is review overreach and the review is refused. No commits expected."
    )
    attempt = int(state.get("attempt") or 0)
    codex = review_queue.is_codex_route(reviewers[0]) if reviewers else False
    spec = {
        "kind": review_kind,
        "description": desc,
        "repo": job.get("repo"),
        "head_branch": head or None,
        "base_branch": base or None,
        "eligible_kinds": list(REVIEW_FOLLOWUP_ELIGIBLE_KINDS),
        "preferred_models": names[:1],
        "max_cost_tier": REVIEW_FOLLOWUP_MAX_COST_TIER,
        "priority": max(REVIEW_FOLLOWUP_PRIORITY, job.get("priority") or 0),
        "project": job.get("project"),
        "parent_job_id": jid,
        "idempotency_key": f"review-queue:{jid}:{attempt}:{head_sha[:12] or 'nohead'}",
    }
    if codex:
        # A Codex review is a normal Hive job (the same read-only ``review:``
        # kind every reviewer gets, so the read-only completion gate and the
        # child listing keep working) that ONLY the confined local codex
        # worker may claim (workspace-write, one at a time): never zoder, EIH
        # or any cloud route.
        spec["eligible_kinds"] = [review_queue.CODEX_WORKER_KIND]
        spec["max_concurrency"] = 1
        spec["review_route"] = reviewers[0]["id"]
        spec["review_lane"] = "codex"
    code, resp = submit_job_spec(spec)
    new_id = (resp or {}).get("id") if isinstance(resp, dict) else None
    if code in (200, 201) and new_id:
        return str(new_id), spec
    # A submit that failed AFTER the bus already holds this exact review
    # (the idempotency key names it, so a retry can never duplicate it) must
    # still record the child it created -- otherwise this row resubmits
    # forever while the child sits in its own retry budget.
    for candidate in _review_children_by_key(job, spec["idempotency_key"]):
        if str(candidate.get("idempotency_key") or "") != spec["idempotency_key"]:
            continue
        if str(candidate.get("status") or "") in _REVIEW_CHILD_TERMINAL:
            break
        return str(candidate["id"]), spec
    raise RuntimeError(f"review submit failed code={code} resp={str(resp)[:160]}")


# ── an approve at a verdict-only descendant covers the delivered head ───────
# Operator report 2026-10-31 (the doctor's exact-head rule): four production
# rows (01a19270 hv/bl-ct-dialect, 1007c484 hv/bl-ct-system, 01a1922a
# hv/zf-failure-contract-r3, 01a18fe6 hv/zc-master-prompt) had a review child
# with a structured approve from cerberus-reviewer and still sat needs-review
# with "approved, but the review job advanced the branch". The review job had
# committed its verdict artifact onto the AUTHOR's branch, so the head it
# approved was the author tip plus one json file. The classifier and its git
# questions live in hive/workers/doctor_head_match.py; this is the call site:
#
#   verdict_only    -> record approved_mergeable with the approved sha (status
#                      stays needs-review: the MERGE PASS acts, the doctor never
#                      merges);
#   code_descendant -> "approval covers newer work not on the head branch": push
#                      it only as a pure fast-forward from the review child's own
#                      workspace (the rescue path), else report the exact sha and
#                      a one-line action, and re-review at the tip;
#   overreach       -> a review that changed code on the author branch: its
#                      approval is NOT counted (review_overreach) and another
#                      reviewer is asked for the tip;
#   not_descendant  -> unchanged: head-exact, never an approval.
#
# Everything is still per-row, still bounded by one per-sweep cap, and
# HIVE_DOCTOR_VERDICT_DESCENDANT (default on) turns the descendant rule off.
HIVE_DOCTOR_VERDICT_DESCENDANT = doctor_head_match.env_switch("HIVE_DOCTOR_VERDICT_DESCENDANT", default=True)
HIVE_DOCTOR_HEAD_MATCH_DRY_RUN = doctor_head_match.env_switch("HIVE_DOCTOR_HEAD_MATCH_DRY_RUN", default=False)
HIVE_DOCTOR_HEAD_MATCH_MAX = _env_int("HIVE_DOCTOR_HEAD_MATCH_MAX", 25, minimum=0)

#: Every outcome a sweep can record per row (the phase-0 and phase-1 keys that
#: are merged into one report; ``scanned``/``adopted``/``submitted``/dry-run are
#: phase facts, not row outcomes).
_PER_ROW_OUTCOMES = frozenset(
    {
        "awaited",
        "deferred",
        "capacity",
        "parked",
        "exhausted",
        "approved",
        "request_changes",
        "review_failed",
        "resolved",
        "capped",
        "approved_mergeable",
        "review_overreach",
        "approval_not_on_head",
        "awaiting_review_after_rescue",
        "head_match_previewed",
        "head_match_capped",
    }
)

#: The sweep outcome (and summary key) per non-covering verdict. ``overreach``
#: and a plain non-descendant head share "the approval is not counted"; the
#: code-descendant case names what an operator does about it.
_HEAD_MATCH_OUTCOMES = {
    doctor_head_match.OVERREACH: "review_overreach",
    doctor_head_match.CODE_DESCENDANT: "approval_not_on_head",
    doctor_head_match.NOT_DESCENDANT: "approval_not_on_head",
    doctor_head_match.NOT_PROVEN: "approval_not_on_head",
}

#: The per-sweep head-match budget: rows past the cap wait unchanged for the
#: next pass. Reset by ``run_review_queue_sweep``; a direct per-row call (a
#: test, a replay) is never capped.
_HEAD_MATCH_BUDGET = {"left": None, "capped": 0}


def reset_head_match_budget(limit: int | None = None) -> None:
    """Start a sweep's head-match budget (``None``: uncapped)."""
    _HEAD_MATCH_BUDGET["left"] = None if limit is None else int(limit)
    _HEAD_MATCH_BUDGET["capped"] = 0


def head_match_budget_capped() -> int:
    """How many rows the CURRENT pass skipped because the cap was spent."""
    return int(_HEAD_MATCH_BUDGET.get("capped") or 0)


def _head_match_budget_left() -> bool:
    """Spend one unit of the pass's head-match budget.

    A refusal is tallied here and ONLY here: the per-row caller has not looked
    at the row yet, so it returns the row UNCHANGED (not ``head_match_capped``)
    and ``run_review_queue_sweep`` records one cap for it. Counting both would
    report two capped rows for one budget.
    """
    left = _HEAD_MATCH_BUDGET.get("left")
    if left is None:
        return True
    if left <= 0:
        _HEAD_MATCH_BUDGET["capped"] = int(_HEAD_MATCH_BUDGET.get("capped") or 0) + 1
        return False
    _HEAD_MATCH_BUDGET["left"] = left - 1
    return True


#: The workspace a row's review child ran in, by kind (the worker's layout).
_WORKSPACE_ROOT = os.environ.get("HIVE_WORKSPACE_ROOT", os.path.expanduser("~/hive-workspaces"))

#: A worker workspace directory is named by its job id, a uuid.
_WORKSPACE_ID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


def _child_workspace(child: dict) -> str | None:
    """The review child's own checkout -- the only place its commits are known to exist.

    The worker clones into ``<HIVE_WORKSPACE_ROOT>/<job id>``; a child that
    recorded its ``workdir`` wins. Never the head branch's clone: a rescue must
    push a commit the child actually holds. The workspace SURVIVES the child's
    terminal state (the worker preserves it), so a finished review's commits are
    still on disk when the sweep reads them.
    """
    for container in (child, child.get("result") if isinstance(child.get("result"), dict) else {}):
        for key in ("workdir", "workspace", "workspace_preserved"):
            path = str(container.get(key) or "").strip()
            if path and os.path.isdir(path):
                return path
    jid = str(child.get("id") or "").strip()
    # The id fallback is a GUESS from an id: only a worker workspace name (a
    # uuid) directly under the configured root, after symlinks are resolved.
    # ``../x`` or an absolute id (which ``os.path.join`` honours verbatim)
    # would otherwise point the classifier -- and a rescue push -- at any
    # checkout on the host.
    if not jid or not _WORKSPACE_ROOT or not _WORKSPACE_ID_RE.fullmatch(jid):
        return None
    root = os.path.realpath(str(_WORKSPACE_ROOT))
    path = os.path.realpath(os.path.join(root, jid))
    if os.path.dirname(path) != root or os.path.basename(path) != jid:
        return None
    return path if os.path.isdir(path) else None


def classify_review_head(
    job: dict, head_sha: str, reviewed: str, *, child: dict | None = None
) -> doctor_head_match.HeadMatch:
    """Classify an approval at ``reviewed`` against the row's delivered ``head_sha``.

    The workspace that answers is the review CHILD's own (it holds the reviewed
    commit); a row with no reachable workspace falls back to the head branch's
    clone, which can only ever see the delivered tip. A verdict that gained
    neither workspace answers ``not_descendant``: head-exact, which is exactly
    the pre-fix rule. ``child`` is the listing row the sweep already read.
    """
    reviewed = str(reviewed or "").strip()
    head_sha = str(head_sha or "").strip()
    if not reviewed or not head_sha:
        return doctor_head_match.HeadMatch(doctor_head_match.NOT_DESCENDANT, reviewed, head_sha, reason="no head")
    child = child if child is not None else (_head_match_child(job) or {})
    pushed = child.get("result") if isinstance(child.get("result"), dict) else {}
    # ``checkout_for`` refuses a workspace that does not hold BOTH commits: the
    # classifier diffs ``tip..reviewed`` and a tip-only clone would answer
    # ``not_descendant`` for every review -- the exact-head bug this fixes.
    for repo in (_child_workspace(child) if child else None, _head_branch_workspace(job)):
        if repo and doctor_head_match.checkout_for(repo, head_sha, reviewed):
            return doctor_head_match.classify_descendant(
                repo,
                head_sha,
                reviewed,
                pushed_files=pushed.get("files_changed"),
                pushed_commits=pushed.get("commits"),
            )
    return doctor_head_match.HeadMatch(
        doctor_head_match.NOT_DESCENDANT,
        reviewed,
        head_sha,
        reason=f"reviewed head {doctor_head_match._shown(reviewed)} is not in any reachable workspace",
    )


def _head_branch_workspace(job: dict) -> str | None:
    """The row's own checkout (the delivered tip), under the CONFIGURED root.

    Only ``HIVE_WORKSPACE_ROOT`` -- never a hardcoded ``~/hive-workspaces``
    fallback. The path is guessed from the job id, so falling back to $HOME
    would let a test (or a host with a differently-rooted fleet) grade a
    stranger's checkout: a review id that happened to exist under the operator's
    real workspace directory would be classified against live data.
    """
    jid = str(job.get("id") or "").strip()
    if not jid or not _WORKSPACE_ROOT:
        return None
    root = os.path.realpath(str(_WORKSPACE_ROOT))
    path = os.path.realpath(os.path.join(root, jid))
    # A job id is an id, never a path: one that escapes the root (``..`` or an
    # absolute id, which ``os.path.join`` would honour verbatim) names no
    # workspace here.
    if os.path.dirname(path) != root:
        return None
    return path if os.path.isdir(path) else None


def _review_head_mismatch(outcome: dict, head: str) -> bool:
    """The child answered an approve/request-changes for a head OTHER than the delivered one."""
    reviewed = str((outcome or {}).get("head_sha") or "").strip()
    return bool(reviewed) and not review_verdict.same_sha(reviewed, head or "")


def _head_match_enabled(state: dict) -> bool:
    """Whether the descendant rule may judge this row (switch on, row not already resolved)."""
    if not HIVE_DOCTOR_VERDICT_DESCENDANT:
        return False
    # A row holding an approval (mergeable, or the legacy ``approved`` block) is
    # resolved: a later child of the SAME row can never replace that approval.
    return not state.get("approved") and not state.get("approved_mergeable")


def _head_match_evidence(result: doctor_head_match.HeadMatch, *, action: str, **extra) -> dict:
    """The one ``review_head_match`` record kept on the row (stable, idempotent)."""
    record = {**result.as_dict(), "action": action}
    record.update({k: v for k, v in extra.items() if v is not None})
    if action:
        record["action"] = " ".join(str(action).split())
    return record


def _head_match_path(
    job: dict,
    state: dict,
    child: dict,
    head: str,
    *,
    review_job_id: str,
    now: float,
    allow_work: bool = True,
    child_outcome: dict | None = None,
) -> tuple[str, dict, None]:
    """Judge an approve for a head other than the delivered one (the bug's four shapes).

    ``verdict_only`` -> ``approved_mergeable`` with the approved sha (status
    unchanged: the merge pass acts). ``overreach`` -> not counted, one retry
    at the tip. ``code_descendant`` -> rescue by pure fast-forward when the
    review child's workspace holds it, else report the exact sha and the
    one-line action, and re-review. ``not_descendant`` -> unchanged, head-exact.

    ``allow_work=False`` (the per-sweep cap is spent) returns ``head_match_capped``
    and writes nothing: the row is read again on the next pass.
    """
    reviewed = str((child_outcome or {}).get("head_sha") or "")
    result = classify_review_head(job, head, reviewed, child=child)
    if not allow_work:
        # The pass's budget is spent: the row is left EXACTLY as it was (its
        # state object, so nothing is re-PATCHed) and judged next sweep. The
        # budget itself counted the skip; the caller returns the outcome.
        return "head_match_capped", state, None
    if result.ok_for_tip and result.verdict == doctor_head_match.VERDICT_ONLY:
        if HIVE_DOCTOR_HEAD_MATCH_DRY_RUN:
            return "head_match_previewed", _record_head_match_preview(job, state, result, now=now), None
        return _approved_mergeable(job, state, result, child_outcome or {}, now=now)
    rescue = rescue_reviewed_head(job, reviewed, head, child=child) if result.newer_work else {}
    if rescue.get("action") in ("pushed", "would_push"):
        # The head branch now carries the reviewed commit: the row is re-reviewed
        # at the NEW tip next pass. Nothing is approved and no attempt is spent.
        record_state = _record_head_match_preview(
            job, state, result, now=now, action=_head_match_action(result, rescue), rescued=rescue
        )
        return "awaiting_review_after_rescue", record_state, None
    return _head_match_failure(job, state, result, now=now, rescue=rescue, reason=result.verdict)


def _record_head_match_preview(
    job: dict,
    state: dict,
    result: doctor_head_match.HeadMatch,
    *,
    now: float,
    action: str | None = None,
    rescued: dict | None = None,
) -> dict:
    """Write (or dry-run log) the head-match record without changing the row's disposition.

    ``rescued`` marks the fast-forward path: the child FINISHED (its slot is
    free, the row is re-reviewed at the new tip), so ``review_job_id`` is
    cleared. A dry run writes nothing at all.
    """
    action = action or (
        "merge pass may merge the delivered head" if result.ok_for_tip else _head_match_action(result, rescued or {})
    )
    record = _head_match_evidence(
        result,
        action=action,
        approved_sha=result.reviewed if result.ok_for_tip else None,
        rescued_sha=result.reviewed if rescued else None,
    )
    record["at"] = now
    if HIVE_DOCTOR_HEAD_MATCH_DRY_RUN:
        log.info(
            "DRY_RUN head-match: job=%s verdict=%s reviewed=%s tip=%s action=%s",
            str(job.get("id") or "")[:12],
            result.verdict,
            result.reviewed[:12],
            result.tip[:12],
            record["action"],
        )
        return {**state, "review_head_match": record}
    # ``at`` is bookkeeping, not evidence: two records that describe the same
    # git state are the SAME record, so a second pass over the row writes
    # nothing (idempotency is what keeps a sweep from re-PATCHing every row).
    if _same_head_match(state.get("review_head_match"), record):
        return state
    new_state = {
        **state,
        "review_head_match": record,
        "updated_at": now,
    }
    if result.ok_for_tip or rescued:
        # The review is settled (or its commits were fast-forwarded): the child
        # is closed out so the next pass reviews the new tip, not this child.
        new_state["review_job_id"] = None
    _patch_review_state(job, new_state)
    return new_state


def _same_head_match(recorded: Any, record: dict) -> bool:
    """Whether ``recorded`` already says exactly what ``record`` says (bar ``at``)."""
    if not isinstance(recorded, dict):
        return False
    left = {k: v for k, v in recorded.items() if k != "at"}
    right = {k: v for k, v in record.items() if k != "at"}
    return left == right


def _head_match_child(job: dict) -> dict | None:
    """The row's in-flight review child, from the listing the sweep already read."""
    child_id = str(
        (review_queue.review_state(job) or {}).get("review_job_id")
        or _row_result_dict(job).get("auto_review_job_id")
        or ""
    )
    if not child_id:
        return None
    try:
        for child in _needs_review_child_review_jobs(job):
            if str(child.get("id")) == child_id:
                return child
    except RuntimeError:
        return None
    return None


def rescue_reviewed_head(job: dict, reviewed: str, tip: str, *, child: dict | None = None) -> dict:
    """Fast-forward the head branch to ``reviewed`` from the review child's workspace.

    Only a pure fast-forward the CHILD holds (``git push <sha>:refs/heads/<branch>``
    -- never a merge, never a force). Any other shape is refused and reported.
    """
    child = child if child is not None else (_head_match_child(job) or {})
    repo = _child_workspace(child)
    branch = job.get("head_branch")
    if not repo or not os.path.isdir(repo):
        return {"action": "unavailable", "sha": reviewed, "old": tip, "reason": "no review workspace"}
    return doctor_head_match.rescue_ff(repo, tip, reviewed, branch=branch, dry_run=HIVE_DOCTOR_HEAD_MATCH_DRY_RUN)


def _head_match_action(result: doctor_head_match.HeadMatch, rescue: dict) -> str:
    """ONE line an operator (or the next pass) can act on."""
    short = doctor_head_match._shown
    if result.verdict == doctor_head_match.CODE_DESCENDANT:
        if rescue.get("action") in ("pushed", "would_push"):
            verb = "fast-forwarded" if rescue["action"] == "pushed" else "would fast-forward"
            return f"{verb} the head branch to {short(result.reviewed)}; re-review the new tip"
        return f"reviewed {short(result.reviewed)} is not on the head branch: {doctor_head_match.Actions.RESCUE_OR_CONFIRM}"
    if result.verdict == doctor_head_match.OVERREACH:
        return doctor_head_match.Actions.RE_REVIEW
    return doctor_head_match.Actions.RE_REVIEW_EXACT


def _head_match_failure(
    job: dict,
    state: dict,
    result: doctor_head_match.HeadMatch,
    *,
    now: float,
    rescue: dict | None = None,
    reason: str | None = None,
) -> tuple[str, dict, None]:
    """Record a non-covering approval and schedule the next review attempt.

    The approval is NOT counted: no ``approved`` block, the row stays
    ``awaiting_review`` and either a review is asked for the tip (overreach) or
    the new tip (code descendant). Idempotent: an unchanged verdict on the same
    head is not re-PATCHed.
    """
    record = {
        **result.as_dict(),
        "action": _head_match_action(result, rescue or {}),
        "at": now,
    }
    if rescue:
        record["rescue"] = rescue.get("action")
        if rescue.get("action") in ("pushed", "would_push"):
            record["rescued_sha"] = result.reviewed
    outcome = _HEAD_MATCH_OUTCOMES.get(result.verdict, "approval_not_on_head")
    # ``reason`` is the verdict ('overreach'/'code_descendant'/'not_descendant');
    # the row records the FAILURE kind, which is prefixed like the other ones.
    failure = reason or result.verdict
    if failure == doctor_head_match.OVERREACH:
        failure = "review_overreach"
    if failure == doctor_head_match.NOT_DESCENDANT:
        # A head that is not the delivered one and does not descend from it is
        # the plain head-exact refusal, and it keeps the name every operator
        # alert and dashboard already keys on. The descendant rule only ever
        # renames the cases it actually judges.
        failure = "review_head_mismatch"
    if _same_head_match(state.get("review_head_match"), record) and state.get("last_failure") == failure:
        return outcome, state, None
    tried = list(state.get("tried") or [])
    new_state = {
        **state,
        "review_job_id": None,
        # Explicit ``None`` on the row the rule judged: "this approval is NOT
        # mergeable" is a recorded fact, distinct from a row never judged.
        "approved_mergeable": None,
        "review_head_match": record,
        "last_failure": failure,
        "next_attempt_at": now + review_queue.backoff_seconds(int(state.get("attempt") or 0)),
        "updated_at": now,
    }
    _patch_review_state(job, new_state)
    broadcast_doctor_message(
        "doctor.review_approval_not_counted",
        {
            "host": AGENT_HOST,
            "job_id": job.get("id"),
            "verdict": result.verdict,
            "reviewed": result.reviewed,
            "head": result.tip,
            "action": record["action"],
            "tried": len(tried),
        },
    )
    return outcome, new_state, None


def _approved_mergeable(
    job: dict, state: dict, result: doctor_head_match.HeadMatch, outcome: dict, *, now: float
) -> tuple[str, dict, None]:
    """Record ``approved_mergeable`` with the approved sha; the merge pass acts.

    The row's STATUS stays needs-review (the review queue IS the triage queue):
    only the merge pass merges, never the doctor. ``approved.head_sha`` stays
    the DELIVERED tip and ``approved.approved_sha`` is the commit the reviewer
    signed, so the merge pass can prove one covers the other.
    """
    approved = {
        "reviewers": list(outcome.get("reviewers") or []),
        "head_sha": result.tip,
        "approved_sha": result.reviewed,
        "basis": result.verdict,
        "review_job_id": state.get("review_job_id"),
        "at": now,
    }
    if result.verdict == doctor_head_match.VERDICT_ONLY and result.reviewed != result.tip:
        approved["verdict_only_descendant"] = True
    record = {**result.as_dict(), "action": "merge pass may merge the delivered head", "at": now}
    new_state = {
        **state,
        "state": doctor_head_match.APPROVED_MERGEABLE,
        "review_job_id": None,
        "approved": approved,
        # The merge pass reads THIS key; ``approved`` is the legacy spelling that
        # makes the row resolvable to the queue (``is_resolved``). Both name the
        # same commit: the delivered tip, plus the sha the reviewer signed.
        "approved_mergeable": approved,
        "review_head_match": record,
        "last_failure": None,
        "next_attempt_at": None,
        "updated_at": now,
    }
    _patch_review_state(
        job,
        new_state,
        verdict={
            "reviewers": [{"model": m, "verdict": "approve"} for m in outcome.get("reviewers") or []],
            "head_sha": result.tip,
            "approved_sha": result.reviewed,
            "basis": result.verdict,
        },
    )
    broadcast_doctor_message(
        "doctor.review_approved",
        {
            "host": AGENT_HOST,
            "job_id": job.get("id"),
            "reviewers": outcome.get("reviewers"),
            "review_job_id": approved["review_job_id"],
            "head_sha": result.tip,
            "approved_sha": result.reviewed,
            "state": doctor_head_match.APPROVED_MERGEABLE,
            "merge": merge_handoff({**job, "result": {**_row_result_dict(job), "review_state": new_state}}),
        },
    )
    return "approved_mergeable", new_state, None


def process_awaiting_review_row(
    job: dict,
    *,
    now: float | None = None,
    health=None,
    load=None,
    roster: list[dict] | None = None,
    allow_submit: bool = True,
) -> tuple[str, dict | None, dict | None]:
    """Advance ONE awaiting_review row. Returns (outcome, new_state, verdict).

    Outcomes: ``skipped_status``/``skipped_triage``/``skipped_protected``,
    ``deferred`` (backoff not elapsed), ``awaited`` (a review job is in
    flight), ``submitted``, ``parked`` (no cross-family reviewer),
    ``exhausted`` (attempt budget spent: the row waits for an owner),
    ``approved``, ``request_changes`` and ``review_failed``.

    A LEGACY needs-review row (no complete review_state) is adopted first: the
    state is built from the row and recorded on it, status untouched. So the
    queue reviews what the older worker line left behind, not just what the new
    line tags (operator 2026-10-31). ``allow_submit=False`` spends no attempt:
    the row waits for the next sweep (the per-sweep submit cap).

    The row never leaves needs-review: there is no separate review status.
    """
    now = time.time() if now is None else float(now)
    jid = str(job.get("id") or "")
    if not jid or job.get("status") != "needs-review":
        return "skipped_status", None, None
    if str(job.get("kind") or "").startswith("triage:"):
        return "skipped_triage", None, None
    # A review job is never itself reviewed, whatever review_state it carries
    # (PYTHIA 2026-11-01: the review-of-review chain).
    if review_queue.is_review_kind(job):
        return "skipped_review_job", None, None
    state = review_queue.usable_review_state(job)
    if state is None:
        adopt_legacy_review_row(job, now=now)
        state = review_queue.usable_review_state(job)
    if state is None:
        return "skipped_status", None, None
    if review_queue.is_resolved(state):
        return "resolved", state, None
    if review_followup_protected(job):
        return "skipped_protected", None, None
    result = _row_result_dict(job)
    head = _review_head(job, result)
    # A submitted review in flight: read its outcome, never submit a second one.
    in_flight = str(state.get("review_job_id") or "")
    if not in_flight:
        # An ADOPTED row can also carry the earlier worker's own review child
        # (``result.auto_review_job_id`` / a live review: child) with no
        # ``review_job_id`` in its state. Submitting again here would hand the
        # same job to two reviewers, so the live child is adopted as in-flight.
        in_flight = _in_flight_review_job_id(job, result)
    if in_flight:
        try:
            children = [c for c in _needs_review_child_review_jobs(job) if str(c.get("id")) == in_flight]
        except RuntimeError as exc:
            # An unreadable listing is not "the child is gone": it is not an
            # answer either, so the row waits and the sweep names the error.
            broadcast_doctor_message("doctor.sweep_error", {"sweep": "review-queue", "job_id": jid, "error": str(exc)})
            return "awaited", state, None
        child = next((c for c in children if str(c.get("id")) == in_flight), None)
        if child is not None:
            outcome = _review_child_outcome(
                child,
                head_sha=head,
                author_families=_excluded_families(
                    state.get("author") or _author_model(job, result), *(state.get("co_authors") or [])
                ),
            )
            if outcome is None:
                return "awaited", state, None
            if outcome["kind"] == "approve" and _review_head_mismatch(outcome, head):
                # THE BUG (production 2026-10-31): the approve names a head other
                # than the delivered tip. Before this, the exact-head rule turned
                # it into review_failed and the four rows never cleared. The
                # verdict-descendant rule judges it from the child's commits on
                # disk; with the switch off it is the old refusal, unchanged.
                if _head_match_enabled(state):
                    # ONE unit of the pass's budget, spent here and passed in:
                    # a plain exact-head approve never reaches this line, and a
                    # capped row leaves the state byte-identical (the sweep
                    # counts it exactly once).
                    allow_work = _head_match_budget_left()
                    held_outcome, held_state, _ = _head_match_path(
                        job,
                        state,
                        child,
                        head,
                        review_job_id=in_flight,
                        now=now,
                        allow_work=allow_work,
                        child_outcome=outcome,
                    )
                    if not allow_work:
                        # ``_head_match_path`` was refused: leave the row as it
                        # was and let the sweep record the cap.
                        return "head_match_capped", held_state, None
                    return held_outcome, held_state, None
                # Switch off: the pre-fix exact-head verdict, unchanged.
                failure = "review_head_mismatch"
                tried = list(state.get("tried") or [])
                catalog = roster or review_queue.load_roster()
                for model in outcome.get("reviewers") or []:
                    entry = next((r for r in catalog if _same_model(r.get("model_id"), model)), None)
                    tried.append(_tried_entry(entry or {"model_id": model}, failure, now))
                return _schedule_retry(job, state, tried, failure, now=now, review_job_id="")
            if outcome["kind"] == "approve":
                approved = {
                    "reviewers": outcome["reviewers"],
                    "head_sha": head,
                    "review_job_id": in_flight,
                    "at": now,
                }
                new_state = {**state, "review_job_id": None, "approved": approved, "updated_at": now}
                _patch_review_state(
                    job,
                    new_state,
                    verdict={
                        "reviewers": [{"model": m, "verdict": "approve"} for m in outcome["reviewers"]],
                        "head_sha": head,
                    },
                )
                broadcast_doctor_message(
                    "doctor.review_approved",
                    {
                        "host": AGENT_HOST,
                        "job_id": jid,
                        "reviewers": outcome["reviewers"],
                        "review_job_id": in_flight,
                        "merge": merge_handoff({**job, "result": {**result, "review_state": new_state}}),
                    },
                )
                return "approved", new_state, outcome
            if outcome["kind"] == "request_changes":
                changes = {
                    "reviewers": outcome["reviewers"],
                    "head_sha": outcome["head_sha"] or head,
                    "review_job_id": in_flight,
                    "at": now,
                }
                new_state = {**state, "review_job_id": None, "request_changes": changes, "updated_at": now}
                _patch_review_state(
                    job,
                    new_state,
                    verdict={
                        "reviewers": [{"model": m, "verdict": "request-changes"} for m in outcome["reviewers"]],
                        "head_sha": outcome["head_sha"] or head,
                    },
                )
                broadcast_doctor_message(
                    "doctor.review_request_changes",
                    {
                        "host": AGENT_HOST,
                        "job_id": jid,
                        "reviewers": outcome["reviewers"],
                        "review_job_id": in_flight,
                        "head_sha": outcome["head_sha"] or head,
                    },
                )
                return "request_changes", new_state, outcome
            failure = outcome.get("failure") or "review_no_reviewers"
            tried = list(state.get("tried") or [])
            catalog = roster or review_queue.load_roster()
            # The reviewer that failed, from this attempt only: ``tried`` is
            # grown below, so re-reading it here would record the same failure
            # twice (and inflate the backoff step).
            failing = list(outcome.get("reviewers") or [])
            for model in failing:
                entry = next((r for r in catalog if _same_model(r.get("model_id"), model)), None)
                tried.append(_tried_entry(entry or {"model_id": model}, failure, now))
            if not failing:
                # The reviewer route this attempt ran on, by the attempt record.
                launched = [
                    r
                    for r in catalog
                    if r["id"] == state.get("review_route") or r["model_id"] == state.get("review_model")
                ]
                if launched:
                    tried.append(_tried_entry(launched[0], failure, now))
            # The child answered (badly): clear it so the next attempt submits a new one.
            return _schedule_retry(job, state, tried, failure, now=now, review_job_id="")
        # The child is gone from the listing: fall through to a new attempt.
    if review_queue.attempts_remain(state) and review_queue.in_backoff(state, now=now):
        return "deferred", state, None
    if not review_queue.attempts_remain(state):
        return _park_exhausted(job, state, now)
    author = state.get("author") or _author_model(job, result)
    models = result.get("models") if isinstance(result.get("models"), dict) else {}
    co_authors = [
        *(models.get("reviewers") if isinstance(models.get("reviewers"), list) else []),
        *state.get("co_authors", []),
    ]
    roster = list(roster if roster is not None else review_queue.load_roster())
    health = health or _review_route_health()
    load = load or review_queue_load_reader()
    co_authors = [str(m) for m in co_authors if str(m or "").strip()]
    lane = _review_lane(job, result, roster)
    picked = _ordered_routes(_pick_routes(author, job, roster, health, load, co_authors, lane=lane), state)
    if not picked and _pick_routes(author, job, roster, health, lambda _route: 0, co_authors, lane=lane):
        # A cross-family reviewer EXISTS but every one is at its concurrency
        # cap (in-flight reviews included). That is not a park and costs no
        # attempt: the row waits unchanged for the next sweep.
        return "capacity", state, None
    if not picked:
        # Same count, more attempts spent: a parked row still ages toward the
        # owner instead of sitting at attempt 0 forever.
        attempts = int(state.get("attempt") or 0)
        new_state = {
            **_resume_state(state),
            "attempt": min(attempts + 1, int(state.get("max_attempts") or review_queue.MAX_REVIEW_ATTEMPTS)),
            "parked_reason": "no_cross_family_reviewer",
            # A retry is scheduled for a row that can still take one; a park
            # reason only survives while another pick is worth trying.
            "next_attempt_at": (
                now + review_queue.backoff_seconds(int(state.get("attempt") or 0))
                if review_queue.attempts_remain(state)
                else None
            ),
            "author": author,
            "author_family": review_queue.family_of(author),
            "updated_at": now,
        }
        _patch_review_state(job, new_state)
        broadcast_doctor_message(
            "doctor.review_queue_parked",
            {
                "host": AGENT_HOST,
                "job_id": jid,
                "author": author,
                "author_family": new_state["author_family"],
                "reason": "no_cross_family_reviewer",
            },
        )
        return "parked", new_state, None
    if not allow_submit:
        # The per-sweep submit cap is spent. Nothing is written: the row is
        # unchanged and the next sweep picks it up (no attempt is spent).
        return "capped", state, None
    if _review_child_refusal(job):
        # Past the review-chain depth guard (or the chain is unreadable): no
        # review child, no attempt spent, nothing written.
        return "skipped_review_chain", state, None
    if HIVE_DOCTOR_ADOPT_DRY_RUN:
        _record_dry_run_submission(job, {**state, "author": author}, picked, now=now)
        return "previewed", {**state, "author": author}, None
    try:
        review_id, spec = _review_queue_submit(job, {**state, "author": author}, picked, now=now)
    except RuntimeError as exc:
        broadcast_doctor_message("doctor.sweep_error", {"sweep": "review-queue", "job_id": jid, "error": str(exc)})
        return _schedule_retry(
            job, state, list(state.get("tried") or []), "review_submit_failed", now=now, review_job_id=""
        )
    tried = list(state.get("tried") or []) + [_tried_entry(picked[0], "review_submitted", now)]
    new_state = {
        **_resume_state(state),
        "author": author,
        "author_family": review_queue.family_of(author),
        "tried": tried,
        "attempt": int(state.get("attempt") or 0) + 1,
        "review_job_id": review_id,
        "review_kind": spec["kind"],
        "review_route": picked[0]["id"],
        "review_model": picked[0]["model_id"],
        "next_attempt_at": None,
        "last_failure": None,
        "updated_at": now,
    }
    _patch_review_state(job, new_state)
    broadcast_doctor_message(
        "doctor.review_queue",
        {
            "host": AGENT_HOST,
            "job_id": jid,
            "review_job_id": review_id,
            "reviewers": [picked[0]["model_id"]],
            "author_family": new_state["author_family"],
        },
    )
    return "submitted", new_state, None


def _ordered_routes(routes: list[dict], state: dict) -> list[dict]:
    """Put the cross-family routes the row's own ``tried[]`` names at the back.

    The roster pairing is unchanged (``pick_reviewers`` already excluded the
    author's family and every saturated or unhealthy route). What this adds is
    least-tried-first, so a row that already spent attempts on the CERBERUS
    endpoint tries TYDEUS qwen38, MiniMax M3, Muse, DeepSeek or the EIH route
    before going back to the one that just failed.
    """
    tried = {
        str(entry.get("model_id") or "").strip().lower()
        for entry in state.get("tried") or []
        if isinstance(entry, dict)
    }
    if not tried:
        return list(routes)
    # Tried routes last, and the roster order kept explicitly among the rest
    # (no reliance on sort stability).
    return [
        route
        for _, route in sorted(
            enumerate(routes),
            key=lambda item: (str(item[1].get("model_id") or "").strip().lower() in tried, item[0]),
        )
    ]


def _record_dry_run_submission(job: dict, state: dict, picked: list[dict], *, now: float) -> None:
    """Log what a real sweep would submit, writing nothing (``HIVE_DOCTOR_ADOPT_DRY_RUN``)."""
    log.info(
        "DRY_RUN: would submit review for %s at %s: reviewers=%s author=%s (%s)",
        str(job.get("id") or "")[:12],
        (_review_head(job, _row_result_dict(job)) or "unrecorded")[:12],
        ",".join(str(route.get("model_id") or "") for route in picked[:2]),
        state.get("author") or "unknown",
        state.get("author_family") or "unknown",
    )


def _sort_routes_by_family_load(routes: list[dict], load) -> list[dict]:
    """Least-loaded first WITHIN a priority tier; a lower tier is always later.

    The roster's ``flags.priority`` (EIH Nemotron is -10: 39 s for a one-word
    reply) and location class come first, so load can reorder routes of the
    same tier but never lift a slow route above a usable faster one. Ties keep
    ``pick_reviewers``' deterministic order.
    """

    def family_load(route):
        try:
            value = load(route)
        except Exception:  # noqa: BLE001
            return 1e9
        return value if review_queue.finite_load(value) else 1e9

    def key(item):
        index, route = item
        priority = int((route.get("flags") or {}).get("priority") or 0)
        location = {"local": 0, "provider": 1, "eih": 2}.get(str(route.get("location")), 3)
        return (-priority, location == "eih", family_load(route), index)

    return [route for _, route in sorted(enumerate(routes), key=key)]


def _schedule_retry(
    job: dict, state: dict, tried: list[dict], failure: str, *, now: float, review_job_id: str
) -> tuple[str, dict, None]:
    """Record a failed review attempt and schedule the next one.

    ``attempt`` is the SUBMISSION budget (it is spent when a review is sent, in
    the submit path, not here), so a failure records the failure and the
    backoff without spending a second attempt.
    """
    attempt = int(state.get("attempt") or 0)
    if attempt >= int(state.get("max_attempts") or review_queue.MAX_REVIEW_ATTEMPTS):
        return _park_exhausted(job, {**state, "tried": tried, "last_failure": failure, "review_job_id": None}, now)
    new_state = {
        **_resume_state(state),
        "tried": tried,
        "last_failure": failure,
        # The failed child answered (badly) or was never created: it is not in
        # flight any more, so the next sweep after the backoff submits a NEW one.
        "review_job_id": None,
        "claimed_failure": failure,
        "next_attempt_at": now + review_queue.backoff_seconds(attempt),
        "updated_at": now,
    }
    _patch_review_state(job, new_state)
    broadcast_doctor_message(
        "doctor.review_retry",
        {
            "host": AGENT_HOST,
            "job_id": job.get("id"),
            "failure": failure,
            "attempt": attempt,
            "next_attempt_at": new_state["next_attempt_at"],
        },
    )
    return "review_failed", new_state, None


def _park_exhausted(job: dict, state: dict, now: float) -> tuple[str, dict, None]:
    """Attempts spent: stay needs-review, with the owner-wait reason visible.

    Recorded once. A row already parked for its owner is left exactly as it
    is: rewriting it on every sweep would be a PATCH (and a broadcast) per row
    per pass for rows nobody can act on but the owner -- whom the needs-review
    escalation names.
    """
    if state.get("parked_reason") == "attempts_exhausted_owner_wait" and not state.get("review_job_id"):
        return "exhausted", state, None
    new_state = {
        **_resume_state(state),
        "parked_reason": "attempts_exhausted_owner_wait",
        "next_attempt_at": None,
        "updated_at": now,
    }
    _patch_review_state(job, new_state)
    broadcast_doctor_message(
        "doctor.review_queue_exhausted",
        {
            "host": AGENT_HOST,
            "job_id": job.get("id"),
            "attempts": state.get("attempt"),
            "last_failure": state.get("last_failure"),
        },
    )
    return "exhausted", new_state, None


def run_review_queue_sweep(*, rows: list[dict] | None = None, now: float | None = None, **kwargs) -> dict:
    """The review-queue half of the needs-review sweep (one pass).

    Rows come from the SAME needs-review listing the triage report reads --
    there is no separate queue. Outcomes are counted per row so one bad row
    cannot stop the sweep; a listing failure is raised, never read as empty.

    Phase 0 ADOPTS the legacy rows in the listing (the older worker line never
    wrote ``result.review_state``, so without this the queue scanned 0 of them
    in production), then ONE pass advances every row the queue owns -- adopted
    and tagged alike -- under the per-sweep submit cap.
    """
    now = time.time() if now is None else float(now)
    # The head-match (verdict-descendant) budget for THIS pass: bounded work per
    # sweep, and a row past it is simply read again next time. Reset BEFORE
    # phase 0 -- that phase advances rows through the same per-row path and
    # spends the same budget.
    reset_head_match_budget(int(HIVE_DOCTOR_HEAD_MATCH_MAX) if HIVE_DOCTOR_VERDICT_DESCENDANT else 0)
    caller_rows = rows is not None
    if rows is None:
        rows = fetch_needs_review_jobs(limit=1000)
    adoption = submit_adopted_reviews(rows=rows, now=now, **{k: v for k, v in kwargs.items() if k != "load"})
    handled = adoption.pop("_handled_rows", None)
    # The caller's own list (a test, a replay) is judged exactly as passed. The
    # full queue gate is for rows FETCHED from the bus, which carry every
    # needs-review job whatever its shape; a caller's list still has to be rows
    # the queue can advance.
    rows = [row for row in rows if review_queue.review_state(row)] if caller_rows else _review_queue_rows(rows)
    summary = {
        "scanned": len(rows),
        "adopted": adoption.get("adopted", 0),
        "submitted": adoption.get("submitted", 0),
        "would_submit": adoption.get("would_submit", 0),
        "dry_run": adoption.get("dry_run", False),
        "awaited": 0,
        "deferred": 0,
        "capacity": 0,
        "parked": 0,
        "exhausted": 0,
        "approved": 0,
        "request_changes": 0,
        "review_failed": 0,
        "resolved": 0,
        "capped": 0,
        "approved_mergeable": 0,
        "review_overreach": 0,
        "approval_not_on_head": 0,
        "awaiting_review_after_rescue": 0,
        "head_match_previewed": 0,
        "head_match_capped": 0,
    }
    # Phase 0 already ADVANCED every row it touched (it calls the same per-row
    # advance), so its outcomes are this pass's outcomes: leaving them out would
    # report "approved 0" for a pass that approved rows (the head-match and
    # overreach counts included).
    for key, value in adoption.items():
        if not key.startswith("_") and key in _PER_ROW_OUTCOMES and isinstance(value, int):
            summary[key] = summary.get(key, 0) + value
    if adoption.get("errors"):
        summary["errors"] = adoption["errors"]
    # Per-route concurrency cap across the whole pass: reviews already in
    # flight on the bus plus the ones this pass submits count against each
    # route's ``concurrency`` on top of the probed server load (a queued review
    # is not yet visible to the probe). Counting a running review twice errs
    # on the side of the cap; exceeding it is what overloaded CERBERUS.
    in_flight: dict[str, int] = {}
    for job in rows:
        state = review_queue.review_state(job) or {}
        if state.get("review_job_id") and state.get("review_route") and not review_queue.is_resolved(state):
            in_flight[str(state["review_route"])] = in_flight.get(str(state["review_route"]), 0) + 1
    base_load = kwargs.pop("load", None) or review_queue_load_reader()

    def load(route: dict):
        value = base_load(route)
        if not review_queue.finite_load(value):
            return value
        return value + in_flight.get(str(route.get("id")), 0)

    # The per-sweep submit cap is shared with the adoption phase; the review
    # jobs that phase already sent count against it.
    budget = max(0, int(HIVE_DOCTOR_REVIEW_SUBMIT_MAX) - summary["submitted"])
    if HIVE_DOCTOR_ADOPT_DRY_RUN:
        budget = 0
    for job in rows:
        if handled and id(job) in handled:
            continue  # phase 0 advanced (or capped) this legacy row already
        before = review_queue.review_state(job) or {}
        held = str(before.get("review_route") or "") if before.get("review_job_id") else ""
        try:
            outcome, new_state, _ = process_awaiting_review_row(
                job, now=now, load=load, allow_submit=budget > 0, **kwargs
            )
        except Exception as exc:  # noqa: BLE001 -- one row must not stop the sweep
            summary["errors"] = summary.get("errors", 0) + 1
            log.error("review queue failed job=%s: %s", str(job.get("id"))[:12], exc)
            broadcast_doctor_message(
                "doctor.sweep_error", {"sweep": "review-queue", "job_id": job.get("id"), "error": str(exc)}
            )
            continue
        if held and outcome != "awaited" and in_flight.get(held):
            in_flight[held] -= 1  # the child finished: its slot is free
        if outcome == "submitted" and new_state and new_state.get("review_route"):
            route_id = str(new_state["review_route"])
            in_flight[route_id] = in_flight.get(route_id, 0) + 1
            budget -= 1
        summary[outcome] = summary.get(outcome, 0) + 1
    # The budget's refusals are the rows this pass could not judge: phase 0's
    # rows were capped through the same budget, so the tally is the pass's.
    summary["head_match_capped"] = max(summary["head_match_capped"], head_match_budget_capped())
    return summary


def submit_adopted_reviews(
    *,
    rows: list[dict] | None = None,
    now: float | None = None,
    roster: list[dict] | None = None,
    health=None,
    load=None,
) -> dict:
    """Adopt the legacy needs-review rows and review them (one capped pass).

    This is the operator's point made real (2026-10-31): the triage queue is
    supposed to be REVIEWING. Rows the older worker line left without
    ``result.review_state`` are adopted and advanced exactly like queue rows --
    cross-family reviewer from ``hive/core/reviewer-roster.json``, retry
    backoff, owner-wait at the attempt cap -- with:

    * the row's STATUS untouched (still needs-review: the queue IS triage);
    * at most ``HIVE_DOCTOR_REVIEW_SUBMIT_MAX`` review jobs per pass;
    * ``HIVE_DOCTOR_ADOPT_DRY_RUN`` logging what it would submit, writing
      nothing;
    * lane B / RiskyEats rows and the lane-B owner-wait chains never touched.

    The doctor neither merges nor cancels anything here: a structured approve
    is RECORDED on the row (``review_state.approved`` with the reviewers named)
    for the operator-merge pass.
    """
    now = time.time() if now is None else float(now)
    summary = {
        "scanned": 0,
        "adopted": 0,
        "would_submit": 0,
        "submitted": 0,
        "awaited": 0,
        "deferred": 0,
        "capacity": 0,
        "capped": 0,
        "parked": 0,
        "exhausted": 0,
        "approved": 0,
        "request_changes": 0,
        "review_failed": 0,
        "resolved": 0,
        "skipped_status": 0,
        "skipped_triage": 0,
        "skipped_protected": 0,
        "previewed": 0,
        "dry_run": HIVE_DOCTOR_ADOPT_DRY_RUN,
    }
    if not HIVE_DOCTOR_ADOPT_LEGACY_REVIEWS:
        # A disabled pass keeps the SAME report shape, all zero: the sweep
        # reads ``adopted``/``submitted``/``would_submit``/``_handled_rows``
        # off this summary unconditionally, so dropping them (or leaving the
        # caller to default a missing key) would make a switched-off phase
        # indistinguishable from a broken one.
        summary["disabled"] = True
        summary["_handled_rows"] = set()
        return summary
    if rows is None:
        rows = fetch_needs_review_jobs(limit=1000)
    # Every row this phase ADOPTS is claimed here, so the phase-1 pass over the
    # same listing cannot touch it a second time (it is already in flight).
    adoptable = {id(row) for row in rows if _adoptable_row(row)}
    claimed: set[int] = set()
    summary["_handled_rows"] = claimed
    summary["scanned"] = len(adoptable)
    # A dry run spends no real submit but previews up to the same cap; a real
    # pass allows one submission only while budget is left (never negative).
    budget = max(0, int(HIVE_DOCTOR_REVIEW_SUBMIT_MAX))
    for row in list(rows):
        jid = str(row.get("id") or "")
        if id(row) not in adoptable:
            continue
        claimed.add(id(row))
        try:
            adopted = adopt_legacy_review_row(row, now=now)
            if (
                adopted
                and not HIVE_DOCTOR_ADOPT_DRY_RUN
                and not _patch_review_state(row, review_queue.usable_review_state(row) or {})
            ):
                summary["errors"] = summary.get("errors", 0) + 1
                continue
            if adopted:
                summary["adopted"] += 1
                state = review_queue.usable_review_state(row) or {}
                log.info(
                    "adopted legacy needs-review row %s (%s, author %s/%s)",
                    jid[:12],
                    state.get("adopted_from"),
                    state.get("author") or "unknown",
                    state.get("author_family") or "unknown",
                )
            outcome, _state, _verdict = process_awaiting_review_row(
                row,
                now=now,
                health=health,
                load=load,
                roster=roster,
                allow_submit=budget > 0,
            )
        except Exception as exc:  # noqa: BLE001 -- one row must not stop the sweep
            summary["errors"] = summary.get("errors", 0) + 1
            log.error("legacy review adoption failed job=%s: %s", jid[:12], exc)
            broadcast_doctor_message(
                "doctor.sweep_error", {"sweep": "adopt-legacy-reviews", "job_id": jid, "error": str(exc)}
            )
            continue
        if outcome == "previewed":
            summary["would_submit"] += 1
        if outcome in ("submitted", "previewed"):
            budget -= 1
        summary[outcome] = summary.get(outcome, 0) + 1
    if summary["adopted"] and not HIVE_DOCTOR_ADOPT_DRY_RUN:
        broadcast_doctor_message(
            "doctor.review_adopted",
            {
                "host": AGENT_HOST,
                "adopted": summary["adopted"],
                "submitted": summary["submitted"],
                "would_submit": summary["would_submit"],
            },
        )
    return summary


def _review_queue_exhausted(state: dict | None) -> bool:
    """Attempts spent and nothing in flight: only the owner can move the row."""
    if not state or review_queue.is_resolved(state) or state.get("review_job_id"):
        return False
    return state.get("parked_reason") == "attempts_exhausted_owner_wait" or not review_queue.attempts_remain(state)


def review_queue_triage_row(job: dict, now: float | None = None) -> dict | None:
    """The triage view of an awaiting_review row (reason, next_attempt_at, states)."""
    state = review_queue.review_state(job)
    if state is None:
        return None
    now = time.time() if now is None else float(now)
    reason = review_queue.awaiting_review_reason(job)
    if state.get("approved"):
        action = f"approved by {', '.join(state['approved'].get('reviewers') or [])}; hand to the merge path"
    elif state.get("request_changes"):
        action = f"changes requested by {', '.join(state['request_changes'].get('reviewers') or [])}; back to the author path"
    elif review_queue.is_parked(state):
        action = f"awaiting cross-family review: parked, no cross-family reviewer is available ({reason})"
    elif state.get("review_job_id"):
        action = f"awaiting cross-family review: review job {state['review_job_id'][:12]} in flight"
    elif _review_queue_exhausted(state):
        # No attempt budget left (whether or not a sweep already recorded it).
        action = f"review attempts exhausted: the owner must review or re-queue this row ({reason})"
    elif review_queue.in_backoff(state, now=now):
        action = f"awaiting cross-family review: next attempt scheduled ({reason})"
    else:
        action = f"awaiting cross-family review ({reason})"
    exhausted = _review_queue_exhausted(state)
    view = {
        "review_state": review_queue.AWAITING_REVIEW,
        "review_reason": reason,
        "next_attempt_at": state.get("next_attempt_at"),
        "review_attempt": int(state.get("attempt") or 0),
        "review_parked": review_queue.is_parked(state),
        "review_exhausted": exhausted,
        "review_in_backoff": review_queue.in_backoff(state, now=now),
        "suggested_action": action,
        # Attempts remain (backoff not elapsed, a review in flight, or a parked
        # pick to retry): NOT stale, never escalated while the queue owns it.
        # Spent attempts with nothing in flight: stale, and the owner's move.
        "stale": exhausted,
    }
    if not review_queue.is_resolved(state):
        view["classification"] = "stale" if exhausted else review_queue.AWAITING_REVIEW
        attempts = (
            f"attempt {view['review_attempt']}/{int(state.get('max_attempts') or review_queue.MAX_REVIEW_ATTEMPTS)}"
        )
        view["orchestrator_note"] = (
            f"report-only needs-review sweep: review attempts exhausted ({attempts}), waiting for the owner"
            if exhausted
            else f"report-only needs-review sweep: awaiting cross-family review ({attempts})"
        )
    return view


def review_queue_stuck_owned(job: dict, now: float | None = None) -> bool:
    """Whether the review queue can still act on this row (so it is not stuck).

    Attempts remain (backoff running, a review child in flight, or a parked
    pick to retry) => the row ages inside the queue, never against the owner.
    """
    state = review_queue.review_state(job)
    if state is None or review_queue.is_resolved(state):
        return False
    return not _review_queue_exhausted(state)


def _gate_review_queue_stuck(row: dict, job: dict, now: float | None) -> None:
    """A triage row must not suggest an action for a row the queue still owns."""
    view = review_queue_triage_row(job, now=now)
    if view is None:
        return
    if "orchestrator_note" in view and row.get("evidence_summary"):
        view["orchestrator_note"] += f"; evidence={row['evidence_summary']}"
    row.update({k: v for k, v in view.items() if k != "suggested_action"})
    row["suggested_action"] = view["suggested_action"]


def remediation_startup_line() -> str:
    """The startup line for the remediation sweep: on/off, preview, cap.

    Read through the module (never cached here) so one line answers the three
    questions an operator has at start: is the doctor going to FIX rows, does
    that only preview, and how many rows can one pass touch."""
    state = "enabled" if doctor_remediation.REMEDIATE_ENABLED else "disabled"
    return (
        f"remediation {state} dry_run={doctor_remediation.DRY_RUN} "
        f"max_per_sweep={doctor_remediation.MAX_REMEDIATIONS_PER_SWEEP} "
        f"max_attempts_per_row={doctor_remediation.MAX_AUTO_REMEDIATIONS}"
    )


def doctor_loop_body(last: dict) -> None:
    """One pass of the loop's periodic (interval-gated) work.

    ``last`` is the loop's schedule (``last_hb``, ``last_scan``, ``last_dlq``,
    ``last_needs_review``, ``last_retention``): each interval block stamps its
    own key in place, so the cadence lives in the loop and not in this function.
    Extracted from ``main`` (operator 2026-11-01) so the ORDER of the sweeps is
    testable -- the remediation sweep had shipped without a call site and only a
    test of this body could have caught it.
    """
    now = time.time()
    if now - last["last_hb"] >= HEARTBEAT_INTERVAL:
        heartbeat()
        last["last_hb"] = now

    # Periodic dead-letter-queue remediation (dry-run unless DLQ_APPLY=1).
    if now - last["last_dlq"] >= DLQ_SCAN_INTERVAL:
        last["last_dlq"] = now
        try:
            s = handle_dead_letter_queue()
            log.info("dlq scan: %s (apply=%s)", s, DLQ_APPLY)
        except Exception as e:
            log.warning("dlq scan error: %s", e)

    if now - last["last_needs_review"] >= NEEDS_REVIEW_SCAN_INTERVAL:
        last["last_needs_review"] = now
        try:
            log.info("needs-review sweep: %s", run_needs_review_sweep())
        except Exception as e:
            log.warning("needs-review sweep error: %s", e)
        # The wrapper path is not the only way a failure must be acted
        # on: a failed row whose base_kind was already triaged inside the
        # marker window would otherwise sit forever (operator: "triage
        # queue has a loop_unresolved").
        try:
            log.info("orphan-failure sweep: %s", sweep_orphan_failures())
        except Exception as e:
            log.warning("orphan-failure sweep error: %s", e)
        try:
            log.info("unclaimable-queued sweep: %s", sweep_unclaimable_queued())
        except Exception as e:
            log.warning("unclaimable-queued sweep error: %s", e)
        # The doctor FIXES the backlog it just classified (operator 2026-10-31).
        # The kill switch (HIVE_DOCTOR_REMEDIATE=0) and the dry-run flag are
        # honoured inside run_remediation_sweep, so the sweep is always called
        # and reports which of the two applied; its own try/except keeps a
        # failed listing or an unavailable durable store from stopping the loop.
        try:
            log.info("remediation sweep: %s", run_remediation_sweep())
        except Exception as e:
            log.warning("remediation sweep error: %s", e)

    # Periodic failed-cluster scan — emit triage:* jobs targeting doctor.
    if now - last["last_scan"] >= SCAN_INTERVAL:
        last["last_scan"] = now
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

    # Periodic terminal-job retention sweep (bounded interval — never
    # every tick): keeps the dead-job backlog at or under the cap.
    if now - last["last_retention"] >= RETENTION_SWEEP_INTERVAL:
        last["last_retention"] = now
        try:
            run_retention_sweep()
        except Exception as e:
            log.warning("retention sweep error: %s", e)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [doctor] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    signal.signal(signal.SIGTERM, _signal)
    signal.signal(signal.SIGINT, _signal)
    if not HIVE_URL:
        # Fail closed, once: a doctor with no bus cannot claim, heartbeat or
        # report, so spinning here would be a silent crash loop (and a log flood).
        log.error(
            "HIVE_URL is not set: this doctor has no bus to serve, refusing to start "
            "(set HIVE_URL in the service environment — see docs/ENVIRONMENT.md)"
        )
        return 2
    log.info("starting on %s url=%s alias=%s dry_run=%s", AGENT_HOST, HIVE_URL, DOCTOR_AGENT_ALIAS, DRY_RUN)
    log.info("%s", remediation_startup_line())
    register()
    last = {
        "last_hb": time.time(),
        "last_scan": 0.0,
        "last_retention": 0.0,
        "last_dlq": 0.0,
        "last_needs_review": 0.0,
    }

    while _running:
        try:
            doctor_loop_body(last)

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
