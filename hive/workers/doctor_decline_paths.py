"""Deterministic decline-vocabulary paths (2026-10-27).

A clearly separated classifier for the decline reasons the doctor must decide
WITHOUT an LLM call, so the decision for every reason string the worker emits
is a testable function rather than a prompt. The main failure-matrix job owns
the execution-failure rows (integrity / dirty worktree / no-commits /
not_task_branch / SIGHUP / push_failed / model_mismatch / review-not-approved /
loop_unresolved); this module owns the DECLINE vocabulary none of those cover:

    no_workspace_for_repo:<repo>      -> canonical-id resubmit or needs-review
    no_workspace_for_kind:<kind>      -> routing signal; escalate at 3 in 6 h
    host_declines_kind:<host>         -> routing signal; never counted
    released_by_host                  -> routing signal; never counted
    invalid_head_branch               -> one resubmit with a sanitized branch
    attribution / gate-fail-closed    -> NEVER retried; needs-review + message
    dlq_protected rows                -> never acted; log only

The handler functions build a bounded decision; they make no bus calls
themselves. The executor (the doctor's triage loop) owns the HTTP and the
approval gates, so a test proves the decision without a live bus.

The contract is docs/DETERMINISTIC-PATHS.md: one table row per reason, and a
row that names a test which does not exist fails the suite.
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable

# Durable-key prefix for the routing-signal tally: one counter per signature,
# pruned at the routing-signal window.
KEY_ROUTING_SIGNAL = "routing_signal"

# Escalate a routing signature once this many of the SAME signature have
# arrived inside the window.
ROUTING_SIGNAL_ESCALATE_AT = 3
ROUTING_SIGNAL_WINDOW_SEC = 6 * 3600

# Sanitized-branch ceiling (the kind slug eats most of the headroom).
_MAX_BRANCH_CHARS = 60

# ── Reason-shape prefixes (kept literal: the doctor is stdlib-only) ──────────
NO_WORKSPACE_FOR_REPO_PREFIX = "no_workspace_for_repo:"
NO_WORKSPACE_FOR_KIND_PREFIX = "no_workspace_for_kind:"
HOST_DECLINES_KIND_PREFIX = "host_declines_kind"
RELEASED_BY_HOST = "released_by_host"
INVALID_HEAD_BRANCH_PREFIX = "invalid_head_branch"
ATTRIBUTION_REASONS = ("attribution_check_failed", "ai_attribution_trailer", "gate_fail_closed")
ATTRIBUTION_EVIDENCE_RE = re.compile(
    r"attribution|co-?authored[-\s]by|generated[-\s]by|assisted[-\s]by|written[-\s]with", re.IGNORECASE
)
DLQ_PROTECTED = "dlq_protected"


def _result(job: dict[str, Any]) -> dict[str, Any]:
    value = job.get("result")
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            import json

            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {"raw": value}
        except (TypeError, ValueError):
            return {"raw": value}
    return {}


def _reason(job: dict[str, Any]) -> str:
    """The decline reason on record for one job, or "" when there is none.

    A decline row is what the DECLINE classifier owns: worker_error (the
    worker's release), last_decline_reason (the bus's thrash-guard record), or
    failure_reason (a terminal failure the doctor picked up). A plain failure
    row — an error string with no decline reason — is not a decline, and the
    v2 rules plus the LLM path keep deciding it, so this returns "" and the
    handler falls through rather than swallowing it.
    """
    res = _result(job)
    for key in ("worker_error", "last_decline_reason"):
        value = res.get(key) or job.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    failure = res.get("failure_reason") or job.get("failure_reason")
    if isinstance(failure, str) and failure.strip() in ATTRIBUTION_REASONS:
        return failure.strip()
    return ""


@dataclass(frozen=True)
class DeclineDecision:
    """The doctor's deterministic decision for one declined job.

    action is one of:
        no_action       routing signal — the bus requeues, the doctor does nothing
        no_action_log   same, but the first sighting of this signature is logged
        resubmit        resubmit once with the patched fields
        needs_review    hand to an operator (never requeue, never resubmit)
        log_only        protected row — never acted, only logged
    """

    action: str
    reason: str
    resubmit_fields: dict[str, Any] = field(default_factory=dict)
    needs_operator: bool = False
    message_topic: str | None = None
    message_payload: dict[str, Any] = field(default_factory=dict)


# ────────────────────────── classifier ──────────────────────────────────────

def is_routing_signal_reason(reason: str) -> bool:
    """The legitimate "not me" signals a matcher emits while it routes a job
    to a capable host. These must NEVER count toward the decline threshold
    (the bus already keeps them out of `is_unresolvable_release`) and the
    doctor must never act on them either."""
    r = (reason or "").strip()
    if not r:
        return False
    return r == RELEASED_BY_HOST or r.startswith(HOST_DECLINES_KIND_PREFIX)


def is_dlq_protected_reason(job: dict[str, Any] | None = None, reason: str = "") -> bool:
    """Whether a declined row is owned by another operator's protected lane.

    The reason string is the first answer; the job's kind/project is the
    second (the same rule `doctor.dlq_protected` enforces for dead-letter
    rows). Either one makes the row log-only: never acted, never requeued,
    never sent to an LLM."""
    r = (reason or _reason(job or {})).strip()
    if r == DLQ_PROTECTED or r.startswith(DLQ_PROTECTED + ":"):
        return True
    job = job or {}
    kind = str(job.get("kind") or "")
    prefixes = tuple(
        p.strip() for p in os.environ.get("DLQ_PROTECT_KIND_PREFIXES", "").split(",") if p.strip()
    )
    if prefixes and kind.startswith(prefixes):
        return True
    protected_projects = frozenset(
        p.strip() for p in os.environ.get("DLQ_PROTECT_PROJECTS", "").split(",") if p.strip()
    )
    return str(job.get("project") or "") in protected_projects


def _attribution_evidence(job: dict[str, Any]) -> str:
    """The violating commit / author, straight from the worker's result."""
    res = _result(job)
    attribution = res.get("attribution")
    parts: list[str] = []
    if isinstance(attribution, dict):
        for key in ("reason", "error"):
            value = attribution.get(key)
            if isinstance(value, str) and value.strip():
                parts.append(value.strip())
        for key in ("commits", "violation_commits"):
            value = attribution.get(key)
            if isinstance(value, list):
                parts.extend(str(v) for v in value if isinstance(v, str))
    for key in ("commits", "commit"):
        value = res.get(key)
        if isinstance(value, str) and value.strip() and not parts:
            parts.append(value.strip())
    if not parts:
        for key in ("error", "failure_reason", "note", "worker_error", "last_decline_reason"):
            value = res.get(key)
            if isinstance(value, str) and ATTRIBUTION_EVIDENCE_RE.search(value):
                parts.append(value.strip())
                break
    return "; ".join(dict.fromkeys(p for p in parts if p))[:500]


def classify_decline_reason(reason: str, job: dict[str, Any] | None = None) -> DeclineDecision:
    """Pure decision for one decline reason. No bus calls, no state writes —
    the executor carries those, so this table is the single place a reason's
    meaning lives and every row is unit-testable."""
    job = job or {}
    r = (reason or "").strip()
    if not r:
        return DeclineDecision("no_action", "empty decline reason")

    # ── dlq_protected rows: never acted, log only ──
    # The owner of this lane handles it themselves; the doctor reports it and
    # does nothing else. Checked FIRST so a protected row can never be
    # resubmitted or escalated by any other rule.
    if is_dlq_protected_reason(job, r):
        return DeclineDecision(
            "log_only",
            f"dlq_protected lane {r}: owner handles it; doctor logs only, never acts",
        )

    # ── host routing signals: never counted, never acted ──
    if r == RELEASED_BY_HOST or r.startswith(HOST_DECLINES_KIND_PREFIX):
        return DeclineDecision(
            "no_action",
            f"routing signal {r}: not a defect, the matcher routes to a capable host",
        )

    # ── attribution / gate-fail-closed: NEVER retry ──
    if r in ATTRIBUTION_REASONS:
        evidence = _attribution_evidence(job)
        return DeclineDecision(
            "needs_review",
            f"attribution violation is never retried (operator rule: no AI attribution "
            f"outside singularityos-lab); violating commit/author: {evidence or 'not on record'}",
            needs_operator=True,
            message_topic="doctor.attribution_violation",
            message_payload={
                "job_id": job.get("id", ""),
                "kind": job.get("kind", ""),
                "reason": r,
                "violating": evidence,
            },
        )

    # ── no_workspace_for_repo:<repo>: canonical id or operator ──
    if r.startswith(NO_WORKSPACE_FOR_REPO_PREFIX):
        repo_name = r[len(NO_WORKSPACE_FOR_REPO_PREFIX) :].strip()
        lookup = None
        try:
            from hive import repos as repo_registry

            lookup = repo_registry.find_exact(repo_name)
        except Exception:  # noqa: BLE001 — catalog unavailable: hand to an operator
            lookup = None
        if lookup is not None:
            canonical = str(lookup.get("id") or repo_name).strip()
            return DeclineDecision(
                "resubmit",
                f"repo {repo_name!r} resolved to catalog id {canonical!r}; resubmit once, workspace left unset",
                resubmit_fields={
                    "repo": canonical,
                    "target_workspace": None,  # left unset: the bus resolves it
                },
            )
        return DeclineDecision(
            "needs_review",
            f"repo {repo_name!r} is not in the bus catalog (GET /v1/repos); operator to supply the mapping",
            needs_operator=True,
        )

    # ── no_workspace_for_kind:<kind>: routing signal, escalated on a pattern ──
    if r.startswith(NO_WORKSPACE_FOR_KIND_PREFIX):
        kind = r[len(NO_WORKSPACE_FOR_KIND_PREFIX) :].strip() or "<unknown>"
        return DeclineDecision(
            "no_action_log",
            f"routing signal {r}: no workspace mapped for kind {kind!r}; logged once per signature",
            message_topic=None,
        )

    # ── invalid_head_branch: one resubmit with a sanitized branch ──
    if r == INVALID_HEAD_BRANCH_PREFIX or r.startswith(INVALID_HEAD_BRANCH_PREFIX + ":"):
        return DeclineDecision("resubmit", "sanitized head branch; resubmit once", resubmit_fields={})

    return DeclineDecision("no_action", f"unowned decline reason {r!r}; the registry must name a handler")


# ────────────────────────── sanitization ────────────────────────────────────

def sanitize_head_branch(kind: str, head_branch: str) -> str:
    """A resubmittable head branch: the kind's slug prefixed, no leading '-',
    lowercase, at most 60 chars. A leading '-' can never survive to git."""
    slug = str(kind or "").strip().replace(":", "-").lower()
    name = str(head_branch or "").lstrip("-").lower()
    # '/' is a valid ref character and carries no meaning for a branch the
    # worker creates locally; flatten it so the sanitized name stays one token.
    name = name.replace("/", "-")
    name = re.sub(r"[^a-z0-9._-]", "-", name)
    if slug:
        candidate = f"{slug}-{name}" if name else slug
    else:
        candidate = name or "fix"
    if not candidate:
        candidate = "fix"
    candidate = candidate[:_MAX_BRANCH_CHARS]
    # Truncation can leave a trailing '-' or '.'; git accepts neither at a
    # ref's end.
    return candidate.rstrip(".-") or "fix"


def resubmit_body_for_invalid_head(job: dict[str, Any]) -> dict[str, Any]:
    """The PATCHed fields for the one invalid_head_branch resubmit.

    `branch_created_from` is kept when the job already has it: the worker will
    create the sanitized head FROM that recorded base, so the resubmit must not
    re-derive admission. `target_workspace` stays unset — the bus resolves the
    workspace itself.
    """
    kind = str(job.get("kind") or "")
    head = str(job.get("head_branch") or "")
    fields: dict[str, Any] = {"head_branch": sanitize_head_branch(kind, head)}
    if job.get("branch_created_from"):
        fields["branch_created_from"] = job["branch_created_from"]
    return fields


# ────────────────── routing-signal tally (durable key) ──────────────────────

def routing_signal_signature(job: dict[str, Any] | None = None, reason: str = "") -> str:
    """The per-signature key: one counter per (base_kind, reason)."""
    job = job or {}
    r = (reason or _reason(job)).strip()
    base_kind = str(job.get("base_kind") or job.get("kind") or "").strip()
    return f"{base_kind}|{r}"[:190]


def routing_signal_seen(
    store,
    job: dict[str, Any] | None = None,
    reason: str = "",
    *,
    now: float | None = None,
    window_sec: float = ROUTING_SIGNAL_WINDOW_SEC,
    escalate_at: int = ROUTING_SIGNAL_ESCALATE_AT,
) -> tuple[int, bool]:
    """Record one routing-signal sighting under its durable signature key and
    return (count_in_window, escalated_now).

    The counter is the doctor's OWN durable store (the same one that holds the
    loop and SSH limits): it is a pattern detector, not a decline counter. A
    routing signal NEVER increments the bus decline_count, whatever this says.
    """
    signature = routing_signal_signature(job, reason)
    key = f"{KEY_ROUTING_SIGNAL}:{signature}"
    at = float(now) if now is not None else time.time()
    stamps = store.touch(key, float(window_sec), at=at)
    count = len([s for s in stamps if at - float(s) <= window_sec])
    escalated = count >= int(escalate_at)
    return count, escalated


def routing_signal_escalation(
    job: dict[str, Any] | None = None,
    reason: str = "",
    *,
    count: int,
    window_sec: float = ROUTING_SIGNAL_WINDOW_SEC,
) -> DeclineDecision:
    """The needs-review decision once a routing signature crosses its threshold."""
    return DeclineDecision(
        "needs_review",
        (
            f"routing signal {reason!r} arrived {count}x in {int(window_sec // 3600)}h "
            f"(threshold {ROUTING_SIGNAL_ESCALATE_AT}); pattern needs an operator"
        ),
        needs_operator=True,
        message_topic="doctor.routing_signal_escalation",
        message_payload={
            "job_id": (job or {}).get("id", ""),
            "signature": routing_signal_signature(job, reason),
            "count": count,
            "window_hours": int(window_sec // 3600),
        },
    )
