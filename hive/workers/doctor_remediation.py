"""Doctor remediation — the doctor FIXES jobs, it does not only report them.

Operator 2026-10-31: *"the doctor has to review everything in the triage queue,
that is the point of the doctor. it needs to actually fix jobs to alter the
fields to make them run correctly and get completed jobs reviewed if the auto
reviewer is busy."*

This module owns that second half. The doctor's sweeps already CLASSIFY rows
(``doctor.py`` report/follow-up, ``remediation.py`` unknown signatures,
``doctor_v2.py`` rules); this one changes the FIELDS that made a row
unrunnable and re-enters it through the existing admin reclassify path
(``POST /v1/admin/jobs/reclassify``), recording every change with its
before/after values and a reason.

Scope, and what it must never do:

* it never changes a review VERDICT, never approves anything, never merges and
  never deletes a branch;
* it never acts on a row another lane owns (``is_lane_b_owned``) — RiskyEats,
  the ``test/ refactor/ fix/ docs/ perf/ ops/ feat/`` branch lanes and the two
  chains the sibling review-adoption job owns;
* it acts on a row at most ``MAX_AUTO_REMEDIATIONS`` (2) times, durably, and
  then escalates with ``doctor.needs_review_stuck``;
* it is idempotent across sweeps: a row whose fields are already correct is a
  no-op, and a row it already re-entered carries its own marker.

Causes (one classifier row each — see :func:`classify`):

======================  ==============================================  ===========
cause                   meaning                                         field fix
======================  ==============================================  ===========
``unresolvable_ref``    a review/verify job names a baseline or head    resolve the
                        that is NOT a commit (a 36-char composite        real head
                        ``head_sha[:28] + workspace_uuid[-8:]`` named    from the
                        no commit; ``git merge-base --is-ancestor``      pushed
                        exited 128 and aborted the delivery check)      branch
``missing_branch``      ``base_branch``/``head_branch`` absent, or the   set from the
                        base branch does not exist on ARGONAS; a head   repo registry;
                        branch missing on the remote is pushed from     never requeued
                        the preserved workspace HEAD                    onto a missing
                                                                        head
``host_pinned``         ``eligible_hosts`` pinned to the host that      clear the pin
                        already failed the job
``infra_retries``       retry budget spent by infrastructure causes     requeue with
                        only (reviewer_unavailable, exit 143, provider  override
                        cooldown, 503, quota)                           retries
``unavailable_model``   ``preferred_models``/``max_cost_tier`` names    switch to a
                        a route that cannot serve the job               healthy
                                                                        roster route
``stranded_wip``        the workspace still holds unpushed commits      push and mark
``waiting_review``      finished work whose review failed because the   hand to the
                        auto reviewer was busy                          review queue
``review_job``          a needs-review row that IS a review job (kind   none: a
                        ``review:``) -- never itself reviewed           review job is
                        (PYTHIA 2026-11-01: a ten-deep review-of-        never
                        review chain)                                   reviewed; at
                                                                        most one
                                                                        escalation
                                                                        note
======================  ==============================================  ===========
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from hive import delivery_ref, review_queue

log = logging.getLogger("hive.doctor.remediation")

# ── Tunables (env-overridable) ──────────────────────────────────────────────

#: Rows one sweep may remediate. A cap, not a queue limit: the next sweep
#: picks up where this one stopped, and no single pass can rewrite the board.
MAX_REMEDIATIONS_PER_SWEEP = int(os.environ.get("HIVE_DOCTOR_REMEDIATE_MAX_PER_SWEEP", "10"))

#: Automatic remediations per job id before the row is escalated to a human
#: (``doctor.needs_review_stuck``). Two is the operator's number.
MAX_AUTO_REMEDIATIONS = int(os.environ.get("HIVE_DOCTOR_REMEDIATE_MAX_ATTEMPTS", "2"))

#: The kill switch: on by default (operator rule 2026-10-31).
REMEDIATE_ENABLED = os.environ.get("HIVE_DOCTOR_REMEDIATE", "1") == "1"

#: When on, every action is logged/returned and nothing is written.
DRY_RUN = os.environ.get("HIVE_DOCTOR_REMEDIATE_DRY_RUN", os.environ.get("DRY_RUN", "0")) == "1"

#: Durable keys (same store as the doctor's other safety state).
_KEY_MARK = "remediation"
#: A remediation marker outlives an operator's attention: the row is not
#: re-fixed for 30 days unless its fields change again.
MARK_TTL_SEC = float(os.environ.get("HIVE_DOCTOR_REMEDIATE_TTL", str(30 * 86400)))

#: Rows past the attempt cap are escalated ONCE and then left alone.
ESCALATE_TTL_SEC = float(os.environ.get("HIVE_DOCTOR_REMEDIATE_ESCALATE_TTL", str(7 * 86400)))

# ── Cause names ─────────────────────────────────────────────────────────────

UNRESOLVABLE_REF = "unresolvable_ref"
MISSING_BRANCH = "missing_branch"
HOST_PINNED = "host_pinned"
INFRA_RETRIES = "infra_retries"
UNAVAILABLE_MODEL = "unavailable_model"
STRANDED_WIP = "stranded_wip"
WAITING_REVIEW = "waiting_review"
#: A review job in triage. Never handed to the review queue: reviewing it
#: mints a review of the review, which ends the same way (live 2026-11-01:
#: ten deep, one new row per sweep). No field fix and no status move.
REVIEW_JOB = "review_job"

CAUSES = (
    UNRESOLVABLE_REF,
    MISSING_BRANCH,
    HOST_PINNED,
    INFRA_RETRIES,
    UNAVAILABLE_MODEL,
    STRANDED_WIP,
    WAITING_REVIEW,
    REVIEW_JOB,
)

#: Causes whose plan hands the row to the review queue. A review job never
#: gets one (see :data:`REVIEW_JOB`).
_REVIEW_HANDOVER_CAUSES = frozenset({WAITING_REVIEW, STRANDED_WIP})

#: Work-row statuses that mean nobody is waiting on a review of it any more.
_SETTLED_STATUSES = frozenset({"done", "cancelled"})

#: Results of :func:`remediate_one`.
APPLIED = "applied"
DRY_WOULD_APPLY = "would_apply"
NO_CHANGE = "no_change"
SKIPPED = "skipped"
ESCALATED = "escalated"
CAPPED = "capped"
FAILED = "failed"

#: Statuses this module reads. A done/queued/claimed row is not its business.
SCAN_STATUSES = ("failed", "needs-review", "dead-letter")

# ── Lane B: rows another lane owns (never touched) ──────────────────────────

#: The sibling job ``hv/gh-adopt-legacy-reviews-20261031`` (running now) adopts
#: legacy needs-review rows and submits cross-family review jobs. It owns these
#: rows; duplicating its work here would mint competing reviews.
LANE_B_PROJECTS = frozenset({"riskyeats"})
LANE_B_BRANCH_PREFIXES = ("test/", "refactor/", "fix/", "docs/", "perf/", "ops/", "feat/")
LANE_B_CHAIN_PREFIXES = ("01a18ab8-0a4f", "01a18a8d-9ffa")


def is_lane_b_owned(job: dict[str, Any]) -> bool:
    """Whether another lane owns this row, so the doctor must not touch it.

    Project ``riskyeats``, any of the listed branch prefixes on
    ``head_branch``/``base_branch``, or a job id inside one of the two chains
    (the row itself or its ``parent_job_id``)."""
    if str(job.get("project") or "").strip().lower() in LANE_B_PROJECTS:
        return True
    for bfield in ("head_branch", "base_branch"):
        branch = str(job.get(bfield) or "").strip()
        if branch.startswith(LANE_B_BRANCH_PREFIXES):
            return True
    for jfield in ("id", "parent_job_id"):
        ident = str(job.get(jfield) or "").strip()
        if ident.startswith(LANE_B_CHAIN_PREFIXES):
            return True
    return False


# ── Row shape helpers (the row shapes are the bus's, not invented) ──────────


def result_dict(job: dict[str, Any]) -> dict[str, Any]:
    """``job['result']`` as a dict, PRESERVING a payload it cannot merge into.

    The column is free-form TEXT. A value that does not decode to an object is
    kept under ``raw`` (the key ``doctor._job_result_dict`` and the loop
    finaliser already use) instead of being read as ``{}``: a discarded payload
    was invisible to classification, to ``_fields_unchanged`` and to the review
    hand-over, so the next patch would drop the evidence. An empty/null value is
    still ``{}`` -- there is nothing to preserve."""
    value = job.get("result")
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        if not value.strip():
            return {}
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            return {"raw": value}
        if decoded is None:
            # The text is the JSON literal ``null``: there is no payload to
            # preserve and no text to search, so this is empty -- the same
            # answer ``doctor._job_result_dict`` gives (see the parity test).
            return {}
        return decoded if isinstance(decoded, dict) else {"raw": value}
    if value is None:
        return {}
    # A list/number/bool stored on the column: keep its text, not its type, so
    # the preserved value is JSON-safe on the next write.
    return {"raw": json.dumps(value)}


def failure_blob(job: dict[str, Any]) -> str:
    """Every failure-ish field of a row, lowercased, for signature matching."""
    res = result_dict(job)
    parts = [
        res.get("error"),
        res.get("failure_reason"),
        res.get("worker_error"),
        res.get("last_decline_reason"),
        res.get("verification_error"),
        res.get("error_detail"),
        res.get("summary"),
        res.get("raw"),
        job.get("error"),
    ]
    return " ".join(str(p) for p in parts if p).lower()


# ── The composite baseline shape (cause a) ─────────────────────────────────

#: Positive evidence that the row failed because a ref did not RESOLVE: git's
#: own words (``git merge-base --is-ancestor`` exits 128 with one of these),
#: or the repair note this module leaves behind.
_UNRESOLVABLE_RE = re.compile(
    r"exit(ed)?\s*128|bad object|unknown revision|not a valid object|"
    r"unresolvable|not a resolvable commit|ambiguous argument|needed a single revision",
    re.IGNORECASE,
)

#: Fields that hold a baseline/verify ARGUMENT. ``head_sha``/``commits`` are
#: the delivered evidence (the WORKER writes them); the baseline the loop's
#: ``--verify-delivery`` gate is given lives in these.
BASELINE_FIELDS = ("baseline", "baseline_sha", "base_sha", "verify_baseline")


def composite_baseline_fields(job: dict[str, Any]) -> list[str]:
    """Field names carrying an unresolvable baseline on a row whose evidence
    says a ref did not resolve (empty when none).

    The composite shape is one such value; another is a 36-char hex span that
    is not a commit. Both are reported only when the row's own failure text is
    git's "that is not a revision" answer, so a field name colliding with a
    harmless value never triggers a rewrite."""
    if not _UNRESOLVABLE_RE.search(failure_blob(job)):
        return []
    res = result_dict(job)
    found = []
    for holder_name, holder in (("result", res), ("job", job)):
        for key in BASELINE_FIELDS:
            # The result carries the value the verify step actually read; the job
            # column is the fallback when the result has no such key at all.
            if holder_name == "job" and key in res:
                continue
            value = holder.get(key)
            text = str(value or "").strip().lower()
            if not text:
                continue
            if (
                delivery_ref.is_composite(text)
                or (re.fullmatch(r"[0-9a-f]{36}", text) and not delivery_ref.is_commit(text))
                or (text.startswith("composite:"))
            ):
                # Qualify with the holder the value was ACTUALLY found in. A
                # bare key resolves against the job columns, so a value read off
                # the result must say ``result.<key>`` and a value on the job
                # column must stay bare: qualifying it as ``result.`` would
                # record a "before" of None and patch a place the row does not
                # keep that value.
                found.append(f"result.{key}" if holder_name == "result" else key)
    return found


def repair_head(
    job: dict[str, Any],
    *,
    ref_exists: Callable[[str], bool] | None = None,
    resolve_ref: Callable[[str], str] | None = None,
) -> str:
    """The real head sha to repair an unresolvable baseline with.

    Order: the delivered head (``result.head_sha``, then the newest of
    ``result.commits``) when it is a commit; otherwise the resolver's answer
    for the pushed ``head_branch`` (injected — the sweep passes a git-backed
    one). Empty when nothing can be proven, so the row is escalated rather than
    pointed at a guess."""
    res = result_dict(job)
    candidates = [res.get("head_sha"), job.get("head_sha")]
    commits = res.get("commits") or []
    if isinstance(commits, list):
        for entry in reversed(commits):
            candidates.append(entry.get("sha") if isinstance(entry, dict) else entry)
    for candidate in candidates:
        text = str(candidate or "").strip().lower()
        if delivery_ref.is_commit(text, ref_exists=ref_exists):
            return text
    branch = str(job.get("head_branch") or "").strip()
    if branch and ref_exists is not None:
        ref = f"refs/heads/{branch}"
        # The resolver is a first-class argument; a ``resolve`` attribute on the
        # predicate is honoured only as a compatibility shim for older callers.
        resolved = resolve_ref if callable(resolve_ref) else getattr(ref_exists, "resolve", None)
        try:
            if ref_exists(ref) and callable(resolved):
                text = str(resolved(ref) or "").strip().lower()
                if delivery_ref.is_commit(text):
                    return text
        except Exception:  # noqa: BLE001 -- an unreadable resolver is not an answer
            return ""
    return ""


# ── Branch shape (cause b) ─────────────────────────────────────────────────


def _valid_branch(name: Any) -> bool:
    """A branch name git would accept, and none of the sentinel values a row
    may carry when nobody filled the field in."""
    text = str(name or "").strip()
    if not text or text.lower() in {"none", "null", "nil", "unknown", "default", "-"}:
        return False
    if text.startswith("-") or text.endswith("/") or " " in text:
        return False
    if ".." in text or "@{" in text or text.endswith(".lock"):
        return False
    return bool(re.match(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$", text))


def missing_branch_fields(job: dict[str, Any]) -> list[str]:
    """The branch fields that are absent/invalid on this row."""
    out = []
    if not _valid_branch(job.get("base_branch")):
        out.append("base_branch")
    if not _valid_branch(job.get("head_branch")):
        out.append("head_branch")
    return out


# ── Host pin (cause c) ─────────────────────────────────────────────────────


def host_pin_fields(job: dict[str, Any]) -> list[str]:
    """Whether ``eligible_hosts`` pins a row no eligible host can be offered.

    The bus never re-offers a failed job to the host that failed it, so the pin
    is the reason the row cannot run ONLY when EVERY eligible host has already
    failed it (``host_failures``, plus the host named in ``claimed_by`` --
    ``urn:agent:<kind>:<host>:<session>`` -- which the bus also refuses). One
    failed host in a two-host pin does not strand the row: the other host is
    still offered it, and clearing that pin would widen a deliberate pin for no
    reason. Hence ``all``, never ``any`` (cross-family review, 2026-10-31)."""
    hosts = [str(h).strip().lower() for h in (job.get("eligible_hosts") or []) if str(h).strip()]
    if not hosts:
        return []
    failed = {str(h).strip().lower() for h in (job.get("host_failures") or {})}
    claimed = str(job.get("claimed_by") or "")
    m = re.search(r"urn:agent:[^:]+:([^:/]+)", claimed)
    if m:
        failed.add(m.group(1).lower())
    if not failed:
        return []
    if all(h in failed for h in hosts):
        return ["eligible_hosts"]
    return []


# ── Retry budget (cause d) ─────────────────────────────────────────────────


def retry_budget_spent(job: dict[str, Any]) -> bool:
    """Same rule as the doctor: no budget left (max 0, or count >= max)."""
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


#: Infrastructure causes, each an evidence shape found in the row's own text.
INFRA_PATTERNS: tuple[tuple[str, re.Pattern], ...] = (
    ("provider_cooldown", re.compile(r"provider[_ -]?cooldown|cooldown active")),
    ("quota", re.compile(r"\bquota\b|resource[_ -]?exhausted|insufficient[_ -]?quota")),
    ("http_503", re.compile(r"\b503\b|service unavailable|bad gateway|gateway time")),
    ("restart_killed", re.compile(r"exit(ed)?\s*143|sigterm|killed by restart|terminated by supervisor")),
    ("reviewer_unavailable", re.compile(r"reviewer[_ -]?unavailable")),
    ("timeout", re.compile(r"\btimed?\s*out\b|\btimeout\b")),
)

#: The generic loop-teardown reason. Alone it says the loop did not finish, NOT
#: why: a reviewer that answered 503 and a test run that failed both end here,
#: so ``loop_unresolved`` is classified by the failure EVIDENCE below it rather
#: than treated as a logic failure by itself.
LOOP_UNRESOLVED_RE = re.compile(r"\bloop_unresolved\b(?!_work_rescued)")

#: A logic failure is never retried on the doctor's say-so. ``loop_unresolved``
#: is deliberately NOT in this list: it is the teardown's catch-all reason.
LOGIC_FAILURE_RE = re.compile(
    r"push_failed|no_commits|dirty_worktree|"
    r"not_task_branch|codex_model_mismatch|attribution|integrity|"
    r"tests?[_ -]?fail|check_failed|assertion",
)


def infra_cause(job: dict[str, Any]) -> str | None:
    """The infrastructure cause on record, or None when it is not one.

    Logic failures win over infra text: a row that also says its tests failed
    is not an infrastructure casualty."""
    blob = failure_blob(job)
    if LOGIC_FAILURE_RE.search(blob):
        return None
    if LOOP_UNRESOLVED_RE.search(blob) and not any(p.search(blob) for _n, p in INFRA_PATTERNS):
        # The loop stopped without any infrastructure evidence: the submitter
        # decides whether that work gets another run.
        return None
    for name, pattern in INFRA_PATTERNS:
        if pattern.search(blob):
            return name
    return None


# ── Model / tier (cause e) ─────────────────────────────────────────────────

#: Cost-class ordering, cheapest first. A job's ``max_cost_tier`` is its cost
#: CEILING: an alternative is only ever at or below it.
COST_CLASSES = ("A", "B", "C")


def _tier_index(value: Any) -> int | None:
    text = str(value or "").strip().upper()
    return COST_CLASSES.index(text) if text in COST_CLASSES else None


def roster_route_for(model: str, *, roster: list[dict] | None = None) -> dict | None:
    """The roster route a model/workload name resolves to (route id, model id,
    a listed role id, or a case-insensitive model id)."""
    wanted = str(model or "").strip()
    if not wanted:
        return None
    low = wanted.lower()
    for route in roster if roster is not None else review_queue.load_roster():
        names = {
            str(route.get("id") or "").lower(),
            str(route.get("model_id") or "").lower(),
            *(str(r).lower() for r in (route.get("roles") or [])),
        }
        if route.get("role_id"):
            names.add(str(route["role_id"]).lower())
        if low in names:
            return route
    return None


def model_problem(
    job: dict[str, Any],
    *,
    roster: list[dict] | None = None,
    health: Callable[[dict], bool] | None = None,
    capped: set[str] | None = None,
) -> str | None:
    """Why the row's model/tier fields cannot serve it, or None.

    ``unavailable_model`` (no roster route serves a preferred model),
    ``capped_model`` (the route is at its spend cap / marked capped by the
    caller) or ``tier_too_low`` (``max_cost_tier`` cannot pay for the only
    route that could serve the row)."""
    routes = roster if roster is not None else review_queue.load_roster()
    health = health or (lambda _r: True)
    capped = {str(c).lower() for c in (capped or set())}
    preferred = [str(m) for m in (job.get("preferred_models") or []) if str(m).strip()]
    for model in preferred:
        route = roster_route_for(model, roster=routes)
        if route is None:
            return "unavailable_model"
        if str(route.get("id") or "").lower() in capped or str(route.get("model_id") or "").lower() in capped:
            return "capped_model"
        # A probe that RAISES is not proof of health (``healthy_alternative``
        # treats it the same way); letting the exception escape would abort the
        # whole classify/sweep instead of classifying the row.
        try:
            healthy = health(route)
        except Exception:  # noqa: BLE001 -- an unreadable health check is not proof of health
            healthy = False
        if not healthy:
            return "capped_model"
    tier = _tier_index(job.get("max_cost_tier"))
    if tier is not None and not any(
        _tier_index(r.get("cost_class")) is not None and _tier_index(r.get("cost_class")) <= tier for r in routes
    ):
        return "tier_too_low"
    return None


def healthy_alternative(
    job: dict[str, Any],
    *,
    roster: list[dict] | None = None,
    health: Callable[[dict], bool] | None = None,
    capped: set[str] | None = None,
) -> dict | None:
    """The cheapest healthy roster route at or below the row's cost ceiling.

    Never the author's own route (a job does not run on the model it just
    proved cannot serve it) and never a forbidden family."""
    routes = roster if roster is not None else review_queue.load_roster()
    health = health or (lambda _r: True)
    capped = {str(c).lower() for c in (capped or set())}
    tier = _tier_index(job.get("max_cost_tier"))
    current = {str(m).lower() for m in (job.get("preferred_models") or [])}
    usable = []
    for route in routes:
        if str(route.get("id") or "").lower() in capped or str(route.get("model_id") or "").lower() in capped:
            continue
        if str(route.get("id") or "").lower() in current or str(route.get("model_id") or "").lower() in current:
            continue
        if str(route.get("family") or "") in review_queue.FORBIDDEN_FAMILIES:
            continue
        rank = _tier_index(route.get("cost_class"))
        # ``max_cost_tier`` is a ceiling: a route whose cost_class cannot be
        # read cannot be PROVEN to sit under it, so it is never chosen (the
        # previous fallback ranked it below every known class and let it win).
        if tier is not None and (rank is None or rank > tier):
            continue
        try:
            if not health(route):
                continue
        except Exception:  # noqa: BLE001 -- an unreadable health check is not proof of health
            continue
        usable.append((rank if rank is not None else len(COST_CLASSES), str(route.get("id") or ""), route))
    usable.sort(key=lambda item: (item[0], item[1]))
    return usable[0][2] if usable else None


# ── Stranded WIP (cause f) ─────────────────────────────────────────────────


def stranded_workspace(job: dict[str, Any]) -> str:
    """The workspace path a row recorded as preserved, or ``""``.

    The ROW is the evidence, not this process's filesystem: the doctor may run
    on a different host from the worker that preserved the workspace, so a
    local ``isdir`` probe would silently mark live WIP unfixable. The path
    shape is validated instead (absolute, no traversal) and the resucer itself
    refuses a path it cannot read."""
    res = result_dict(job)
    for holder in (res, job):
        for key in ("workspace_preserved", "workspace", "workspace_path"):
            text = str(holder.get(key) or "").strip()
            if not text or text.lower() in {"none", "null", "nil", "false"}:
                continue
            if not text.startswith("/") or ".." in text.split("/"):
                continue
            return text
    return ""


# ── Finished work (cause g) ────────────────────────────────────────────────


def author_model(job: dict[str, Any]) -> str:
    """The AUTHOR route of a row — the model whose family a reviewer must differ from.

    Same precedence the doctor uses: an existing ``result.review_state``, then
    ``result.models.author`` / ``result.author_model``, then the claim. The job
    KIND is never an author: ``fix:hive-review-queue-20261031`` names the task,
    not a model, and treating it as one would exclude nobody from reviewing."""
    state = review_queue.review_state(job) or {}
    res = result_dict(job)
    models = res.get("models") if isinstance(res.get("models"), dict) else {}
    for candidate in (
        state.get("author"),
        models.get("author"),
        res.get("author_model"),
        job.get("claimed_model"),
    ):
        if str(candidate or "").strip():
            return str(candidate).strip()
    # A row scheduled onto a model list was run as its first preference when no
    # claim recorded the author: that is the best evidence of who wrote it, and
    # an unknown author excludes nobody from reviewing.
    for preferred in job.get("preferred_models") or ():
        if str(preferred or "").strip():
            return str(preferred).strip()
    return ""


def review_busy_failure(job: dict[str, Any]) -> str | None:
    """The reviewer-busy cause on a delivered row, or None.

    Evidence is the worker's own structured record: ``result.review_state``
    with ``last_failure == reviewer_unavailable``, or a failure reason in the
    loop-teardown vocabulary. Prose-only, empty, truncated and 503 output are
    the same class: the reviewer could not answer."""
    state = review_queue.review_state(job) or {}
    if state and str(state.get("last_failure") or "").strip() == "reviewer_unavailable":
        return "reviewer_unavailable"
    blob = failure_blob(job)
    for name in ("reviewer_unavailable", "loop_unresolved"):
        if name in blob:
            return name
    return None


def delivery_verified(job: dict[str, Any]) -> bool:
    """Whether the row's own delivery check proved the work reached the branch.

    An explicit ``remote_verified: false`` is a NO; an absent field is also a
    no: ``waiting_review`` is only for work whose delivery is on record."""
    res = result_dict(job)
    value = res.get("remote_verified", job.get("remote_verified"))
    if isinstance(value, str):
        return value.strip().lower() in {"true", "yes", "1"}
    return value is True


def delivered_head(job: dict[str, Any]) -> str:
    """The delivered commit on a row: ``result.head_sha``, else the head commit."""
    res = result_dict(job)
    # Normalise each holder SEPARATELY: ``res.get(...) or job.get(...)`` picks the
    # result's placeholder over a valid job value, because the raw ``"none"``
    # string is truthy. The result wins only when it actually holds a head.
    head = _head_text(res.get("head_sha")) or _head_text(job.get("head_sha"))
    if head:
        return head
    commits = res.get("commits") or []
    if isinstance(commits, list) and commits:
        last = commits[-1]
        return _head_text(last.get("sha") if isinstance(last, dict) else last)
    return ""


def _head_text(value: Any) -> str:
    """A head sha as lowercase text, or ``""`` for no head.

    ``str(None)`` is the truthy string ``"none"``, so a row with no head used
    to satisfy every ``if delivered_head(job)`` guard and be handed to the
    review queue with a head of ``"none"``. The placeholder spellings a JSON
    round-trip leaves behind read as no head too."""
    if value is None:
        return ""
    text = str(value).strip().lower()
    return "" if text in {"", "none", "null"} else text


# ── Classification ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Cause:
    """One classified cause for one row.

    ``name`` must be one of :data:`CAUSES`: the plan table in :func:`plan_for`
    and the audit records switch on it, so an invented name would classify a
    row into a branch that does not exist. ``detail`` is free text (it rides
    into the audit reason) and is not a cause name — e.g. ``capped_model`` and
    ``tier_too_low`` are model_problem DETAILS under ``unavailable_model``."""

    name: str
    detail: str = ""
    fields: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.name not in CAUSES:
            raise ValueError(f"unknown cause name {self.name!r}; expected one of {CAUSES}")


def row_has_verdict(job: dict[str, Any]) -> bool:
    """Whether the row already carries a review verdict.

    A verdict — whatever it says — is a decision already made about this work.
    Remediation never re-reviews or overturns one, so it is a no-op on such a
    row. ``has_verdict`` lets a caller supply a stricter check (e.g. one that
    also consults the review queue); this is the floor every caller gets."""
    res = result_dict(job)
    evidence = res.get("review_evidence") or job.get("review_evidence")
    if isinstance(evidence, dict) and evidence.get("verdict"):
        return True
    if isinstance(evidence, str) and evidence.strip():
        return True
    return bool(str(res.get("verdict") or job.get("verdict") or "").strip())


def classify(
    job: dict[str, Any],
    *,
    has_verdict: Callable[[dict], bool] | None = None,
    roster: list[dict] | None = None,
    health: Callable[[dict], bool] | None = None,
    capped: set[str] | None = None,
) -> Cause | None:
    """The FIRST cause the row has, in fix-priority order, or None.

    Order matters: a row must be pointed at a real commit before its budget
    matters, and a row whose review was lost is only handed over once its
    delivery is proven. A row with no recognised cause is a no-op."""
    verdict_fn = has_verdict or row_has_verdict
    if composite_baseline_fields(job):
        return Cause(UNRESOLVABLE_REF, "composite_baseline", tuple(composite_baseline_fields(job)))
    status = str(job.get("status") or "")
    # (b) only for a row whose branch fields are genuinely absent: a needs-review
    # row keeps the branch it delivered.
    fields = missing_branch_fields(job)
    if fields and status in ("failed", "dead-letter"):
        return Cause(MISSING_BRANCH, "branch_field:" + ",".join(fields), tuple(fields))
    pins = host_pin_fields(job)
    if pins:
        return Cause(HOST_PINNED, "eligible_hosts_pin", tuple(pins))
    # (g) before (d): FINISHED work is finished whether or not a re-run budget is
    # left. The proof is the row's own delivery check (``remote_verified``): the
    # commits are on the branch, only the review never ran, so spending a retry
    # on it would re-run work already done. A row whose delivery was never
    # verified has no such proof and is an infrastructure retry instead.
    # A review job is never itself reviewed: the causes that HAND a row to the
    # review queue (g, f) are skipped for it, and a review job with no field
    # fix to make is classified ``review_job`` (no action) instead of healthy,
    # so the audit names why the sweep left it alone.
    review_job = review_queue.is_review_kind(job)
    busy = review_busy_failure(job)
    if busy and not review_job and delivered_head(job) and delivery_verified(job) and not verdict_fn(job):
        return Cause(WAITING_REVIEW, busy)
    if retry_budget_spent(job) and not verdict_fn(job):
        # A row that already carries a review verdict is not retryable at all:
        # re-running it would mint a second, competing review of work that has
        # been judged. The verdict is a human (or review-queue) decision.
        infra = infra_cause(job)
        if infra:
            return Cause(INFRA_RETRIES, infra, ("max_retries",))
    if status in ("failed", "dead-letter") and stranded_workspace(job) and not review_job:
        return Cause(STRANDED_WIP, "workspace_preserved")
    # The HEALTH probe and the caller's cap set must reach the classifier: an
    # unavailable route is only visible through the probe, and reading it here
    # without them would classify a row against a roster nobody checked.
    problem = model_problem(
        job,
        roster=roster,
        health=health,
        capped={str(c).lower() for c in (job.get("capped_routes") or [])} | {str(c).lower() for c in (capped or ())},
    )
    if problem:
        return Cause(UNAVAILABLE_MODEL, problem, ("preferred_models", "max_cost_tier"))
    if review_job and status == "needs-review":
        return Cause(REVIEW_JOB, "needs-review")
    return None


# ── Markers, attempts and escalation (durable) ─────────────────────────────


def mark_key(job_id: str, cause: Cause | None = None, head: str = "") -> str:
    """The durable "already fixed for this shape" key for one row.

    The cause AND the head are in the key, so a row whose fields move again
    (a new head, a new cause) is a new remediation — and a re-read of the SAME
    row across sweeps hits the same key and is a no-op."""
    base = f"{_KEY_MARK}:{job_id}"
    if cause is None:
        return base
    tail = f":{head[:12]}" if head else ""
    return f"{base}:{cause.name}:{cause.detail}{tail}"


def _attempt_key(job_id: str) -> str:
    return f"{_KEY_MARK}:attempts:{job_id}"


def escalation_key(job_id: str) -> str:
    return f"{_KEY_MARK}:escalated:{job_id}"


def attempts(state: Any, job_id: str) -> int:
    """How many automatic remediations this row has had (durable)."""
    value = state.value(_attempt_key(job_id), 0)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def record_attempt(state: Any, job_id: str, at: float | None = None) -> int:
    """Count one remediation for this row and return the new total."""
    total = attempts(state, job_id) + 1
    state.set_value(_attempt_key(job_id), total, ttl=MARK_TTL_SEC)
    return total


def _marked_within(state: Any, key: str, ttl: float, now: float | None) -> bool:
    """Whether ``key`` was marked within ``ttl`` of ``now``.

    The MARKER carries its own stamp (``{"at": <module clock>}``) because the
    durable store records retention on ITS clock: the sweep runs on an injected
    clock, and a marker written with that clock's ``at`` would look decades old
    to the store and be pruned on the next write. The store therefore keeps the
    marker for its own TTL window, while freshness — the thing idempotency turns
    on — is judged here, on the clock that wrote it.

    A marker with no stamp (written by an older build) falls back to the store's
    own so it is never treated as infinitely fresh."""
    raw = state.value(key, None)
    if isinstance(raw, dict) and "at" in raw:
        stamped = raw.get("at")
    elif hasattr(state, "last_mark"):
        stamped = state.last_mark(key)
    else:
        stamped = raw if isinstance(raw, (int, float)) else None
    if stamped is None:
        return False
    # ``now`` unset must mean the REAL clock, never the marker's own stamp: a
    # marker compared against itself has an age of 0 and reads as fresh
    # forever. The marker carries its own stamp because it was written on the
    # module clock; only its AGE is judged here.
    reference = time.time() if now is None else float(now)
    try:
        age = reference - float(stamped)
    except (TypeError, ValueError):
        return False
    # A stamp in the FUTURE is not a recent fix: it is a clock that moved
    # backwards (or a forged stamp), and accepting it would suppress this
    # row's remediation until the wall clock caught up. Same rule as
    # ``recent_audit``: only a non-negative age inside the TTL counts.
    return 0 <= age <= ttl


def already_fixed(state: Any, key: str, *, now: float | None = None) -> bool:
    """Whether ``key`` was marked inside the module's TTL."""
    return _marked_within(state, key, MARK_TTL_SEC, now)


def mark_fixed(state: Any, key: str, *, at: float | None = None) -> None:
    """Record ``key`` as fixed, stamping the module clock it happened on."""
    stamp = time.time() if at is None else float(at)
    state.set_value(key, {"at": stamp}, ttl=MARK_TTL_SEC)


# ── The "second copy" of mutable fields on the row (``result.remediation``) ─
# The admin reclassify route archives and CLEARS the result when it requeues a
# row (FB51), so the audit trail also lives in the row's own reclassification
# reason — which the bus keeps — and in these keys on the CURRENT result.

AUDIT_KEY = "remediation"


def audit_events(job: dict[str, Any]) -> list[dict[str, Any]]:
    """The remediation audit trail recorded on the row (newest last)."""
    res = result_dict(job)
    block = res.get(AUDIT_KEY)
    if isinstance(block, dict):
        events = block.get("events")
        if isinstance(events, list):
            return [e for e in events if isinstance(e, dict)]
    if isinstance(block, list):
        return [e for e in block if isinstance(e, dict)]
    return []


def _audit_entry(cause: Cause, changes: dict[str, Any], reason: str, applied_at: float, action: str) -> dict:
    """One audit event: before/after per field plus the reason.

    Every entry is JSON-safe (it is written into the row's result and could be
    the only record of the change once the bus archives the old result)."""
    before = {name: change.get("before") for name, change in changes.items()}
    after = {name: change.get("after") for name, change in changes.items()}
    return {
        "at": float(applied_at),
        "cause": cause.name,
        "detail": cause.detail,
        "action": action,
        "reason": reason,
        "before": before,
        "after": after,
    }


# ── Field-fix plans (pure: testable without a bus) ─────────────────────────


@dataclass(frozen=True)
class Plan:
    """One remediation plan: the field changes to write and where the row goes.

    ``changes`` is ``{field: {"before": ..., "after": ...}}`` on the row's own
    result (the reclassify route takes no field patch), so the operator sees
    the exact edit. ``requeue`` says the row must go back to ``queued`` (which
    is what spends a retry); ``needs_review`` hands it to the review queue
    instead. ``reason`` is the audit reason, ``detail`` the human one-liner."""

    cause: Cause
    changes: dict[str, dict[str, Any]] = field(default_factory=dict)
    requeue: bool = False
    needs_review: bool = False
    override: bool = False
    override_reason: str = ""
    reason: str = ""
    detail: str = ""
    #: Push the preserved workspace HEAD to ``head_branch`` before the status
    #: move (a head branch missing on the remote; stranded WIP always does).
    push_workspace: bool = False

    @property
    def changed(self) -> bool:
        return bool(self.changes)


def _change(before: Any, after: Any) -> dict[str, Any]:
    return {"before": before, "after": after}


def plan_for(
    job: dict[str, Any],
    cause: Cause,
    *,
    default_branch: str | None,
    branch_exists: Callable[[str], bool] | None = None,
    roster: list[dict] | None = None,
    health: Callable[[dict], bool] | None = None,
    capped: set[str] | None = None,
    ref_exists: Callable[[str], bool] | None = None,
    resolve_ref: Callable[[str], str] | None = None,
    now: float | None = None,
) -> Plan | None:
    """The field fix for one classified cause, or None when it cannot apply.

    None means "escalate": a cause with no provable fix is never papered over
    with a guess (a branch nobody proved exists, a model nobody serves).

    A review job never gets a plan that hands it to the review queue, whatever
    cause a caller names: that is how a review of a review is minted."""
    if cause.name == REVIEW_JOB:
        return None
    if cause.name in _REVIEW_HANDOVER_CAUSES and review_queue.is_review_kind(job):
        log.warning(
            "remediation: refused to hand review job %s (%s) to the review queue",
            str(job.get("id") or "")[:12],
            job.get("kind"),
        )
        return None
    if cause.name == UNRESOLVABLE_REF:
        head = repair_head(job, ref_exists=ref_exists, resolve_ref=resolve_ref)
        if not head:
            return None
        changes = {}
        for field_name in cause.fields:
            # ``result.<key>`` lives on the result dict; a bare ``<key>`` is a job
            # column (base_branch, head_branch, ...). Reading the wrong holder
            # yields a None "before" and patches the wrong place.
            holder = result_dict(job) if field_name.startswith("result.") else job
            key = field_name.split(".", 1)[-1]
            # The plan is keyed by the row's own field name: the qualifier is
            # how the CAUSE points at the holder, not part of the edit.
            changes[key] = _change(holder.get(key), head)
        # ONE key for the head: ``head_sha``, the row field the worker writes and
        # ``delivered_head`` reads. The executor takes ``{field: {before,
        # after}}`` and cannot parse a ``result.`` prefix, so the write must
        # never be qualified -- and the readers (``_fields_unchanged``, the
        # ``remediate_one`` harvest) assume that same name.
        changes["head_sha"] = _change(result_dict(job).get("head_sha"), head)
        return Plan(
            cause=cause,
            changes=changes,
            requeue=True,
            reason=f"unresolvable_baseline:{','.join(cause.fields)}->resolved_head:{head[:12]}",
            detail=f"replaced {','.join(cause.fields)} with the real head {head[:12]}",
        )
    if cause.name == MISSING_BRANCH:
        base = str(default_branch or "").strip()
        if not base or not _valid_branch(base):
            return None
        if branch_exists is not None:
            try:
                if not branch_exists(base):
                    return None
            except Exception:  # noqa: BLE001 -- an unreadable probe is not proof
                return None
        changes = {}
        for field_name in cause.fields:
            current = job.get(field_name)
            if field_name == "base_branch":
                changes[field_name] = _change(current, base)
            else:
                # A missing head branch on a failed job: the repo's default is
                # the only branch proven to exist, and the worker checks out a
                # fresh branch from it per the delivery policy.
                changes[field_name] = _change(current, base if not _valid_branch(current) else current)
        # A head branch that is well-formed but ABSENT on the remote is as
        # missing as an empty one: requeueing onto it re-fails the same way.
        # Repair it from the preserved workspace HEAD (pushed before the
        # requeue), or decline -- an unreadable probe is not proof either.
        head = str(job.get("head_branch") or "").strip()
        push = False
        if "head_branch" not in cause.fields and branch_exists is not None:
            try:
                head_present = bool(branch_exists(head))
            except Exception:  # noqa: BLE001 -- an unreadable probe is not proof
                return None
            if not head_present:
                if not stranded_workspace(job):
                    return None
                push = True
                changes["workspace_pushed"] = _change(False, True)
        return Plan(
            cause=cause,
            changes=changes,
            requeue=True,
            push_workspace=push,
            reason=f"missing_branch:{','.join(cause.fields)}->base_branch:{base}"
            + (f";head_branch:{head}->pushed_from_workspace" if push else ""),
            detail=f"set {','.join(cause.fields)} from the repo registry ({base}, verified on ARGONAS)"
            + (f"; pushed the preserved workspace HEAD to the missing {head}" if push else ""),
        )
    if cause.name == HOST_PINNED:
        hosts = list(job.get("eligible_hosts") or [])
        return Plan(
            cause=cause,
            changes={"eligible_hosts": _change(hosts, [])},
            requeue=True,
            reason=f"host_pinned:{','.join(str(h) for h in hosts)}->unpinned",
            detail=f"cleared the eligible_hosts pin ({', '.join(str(h) for h in hosts)} already failed this job)",
        )
    if cause.name == INFRA_RETRIES:
        max_retries = job.get("max_retries")
        try:
            raised = max(int(max_retries or 0), 1)
        except (TypeError, ValueError):
            raised = 1
        reason = f"infra_retry:{cause.detail}"
        return Plan(
            cause=cause,
            changes={"max_retries": _change(max_retries, raised)},
            requeue=True,
            override=True,
            override_reason=reason,
            reason=reason,
            detail=f"requeued with override_max_retries ({cause.detail})",
        )
    if cause.name == UNAVAILABLE_MODEL:
        route = healthy_alternative(job, roster=roster, health=health, capped=capped)
        if route is None:
            return None
        model = str(route.get("model_id") or route.get("id"))
        changes = {"preferred_models": _change(list(job.get("preferred_models") or []), [model])}
        current_tier = job.get("max_cost_tier")
        new_tier = route.get("cost_class")
        if (
            new_tier
            and _tier_index(new_tier) is not None
            and (_tier_index(current_tier) is None or _tier_index(current_tier) < _tier_index(new_tier))
        ):
            changes["max_cost_tier"] = _change(current_tier, new_tier)
        return Plan(
            cause=cause,
            changes=changes,
            requeue=True,
            reason=f"unavailable_model:{cause.detail}->{model}",
            detail=f"switched preferred_models to the healthy route {route.get('id')} ({model})",
        )
    if cause.name == STRANDED_WIP:
        workspace = stranded_workspace(job)
        branch = str(job.get("head_branch") or "").strip()
        if not workspace or not _valid_branch(branch):
            return None
        return Plan(
            cause=cause,
            changes={"workspace_pushed": _change(False, True)},
            needs_review=True,
            reason=f"stranded_wip:{branch}",
            detail=f"pushed the preserved workspace {workspace} to {branch}; handing the row to review",
        )
    if cause.name == WAITING_REVIEW:
        head = delivered_head(job)
        state = review_queue.review_state(job) or {}
        if state:
            # The worker already left the row in the review queue's shape; only
            # its status needs to follow.
            return Plan(
                cause=cause,
                changes={},
                needs_review=True,
                reason=f"review_busy:{cause.detail}",
                detail=f"handed to the review queue ({cause.detail})",
            )
        new_state = review_queue.make_review_state(
            author_model(job),
            [],
            last_failure=cause.detail,
            now=now,
        )
        return Plan(
            cause=cause,
            changes={"review_state": _change(None, new_state)},
            needs_review=True,
            reason=f"review_busy:{cause.detail}",
            detail=f"marked awaiting_review and handed to the review queue ({cause.detail}, head {head[:12]})",
        )
    return None


# ── Executors (injected side effects) ──────────────────────────────────────


def _fields_unchanged(job: dict[str, Any], plan: Plan) -> bool:
    """Whether every planned field already holds its target value.

    A row whose fields are correct is a no-op even if its marker expired: this
    is the idempotency's second line of defence, and it is what makes a second
    sweep over an already-fixed row cost nothing."""
    if not plan.changes:
        # A status-only move: idempotent when the status is already right.
        return str(job.get("status") or "") == "needs-review"
    if plan.override:
        # The infra-retry plan only lifts ``max_retries`` to at least 1 (usually
        # leaving it as it is): the budget is spent and the OVERRIDE is the fix,
        # so "the field already holds its value" is not evidence of a fix.
        return False
    for field_name, change in plan.changes.items():
        key = field_name.split(".", 1)[-1]
        if field_name.startswith("result."):
            # ``result.<key>`` is the result dict, full stop.
            current = result_dict(job).get(key)
        elif key in result_dict(job):
            # The executor writes every bare row field onto the RESULT
            # (``plan_for``: "changes is {field: {before, after}} on the row's
            # own result"), so a result copy of the key is the one this plan
            # just wrote -- and the one that decides whether the write landed.
            #
            # It must be checked BEFORE the job column: a row fixed once can
            # still carry a stale copy of the same name on its column (the
            # legacy shape keeps ``baseline`` there), and reading that stale
            # copy reported a fixed row as unfixed -- so every sweep re-fixed
            # it until the 2-attempt cap escalated a healthy row.
            current = result_dict(job).get(key)
        elif key in job:
            # Not on the result: ``base_branch``/``head_branch``/... live on
            # the job column, and the plan's target for them is written there.
            current = job.get(key)
        else:
            current = None
        target = change.get("after")
        if current != target:
            return False
    return True


def recent_audit(job: dict[str, Any], cause: Cause, *, within: float = 3600.0, now: float | None = None) -> bool:
    """Whether the row already carries an audit event for this cause/head pair."""
    now = time.time() if now is None else float(now)
    for event in audit_events(job):
        if event.get("cause") != cause.name or event.get("detail") != cause.detail:
            continue
        try:
            age = now - float(event.get("at") or 0)
        except (TypeError, ValueError):
            continue
        # ``now - at < within`` is also true for every stamp in the FUTURE: a
        # clock-skewed (or forged) stamp would block the row's own fix for as
        # long as it sat ahead of the clock. A future stamp is not evidence of
        # a recent fix, so it is treated as stale.
        if 0 <= age < within:
            return True
    return False


@dataclass
class Outcome:
    """What :func:`remediate_one` did with one row (one structured log line)."""

    job_id: str
    status: str
    cause: str = ""
    detail: str = ""
    reason: str = ""
    changes: dict[str, Any] = field(default_factory=dict)
    attempts: int = 0
    error: str = ""

    def as_log(self) -> str:
        return json.dumps(
            {
                "event": "doctor.remediation",
                "job_id": self.job_id,
                "status": self.status,
                "cause": self.cause,
                "detail": self.detail,
                "reason": self.reason,
                "changes": self.changes,
                "attempts": self.attempts,
                "error": self.error or None,
            },
            sort_keys=True,
        )


def _handover_result(job: dict[str, Any], plan: Plan) -> dict[str, Any]:
    """The result dict the review hand-over hands over: the row's own result
    plus every planned ``after`` value.

    Production ``plan_for`` emits BARE row field names (``review_state``,
    ``head_sha``, ``baseline``), not ``result.<key>`` qualified ones; each is
    written straight onto the result dict. A qualified ``result.<key>`` key is
    nonetheless unpacked to its ``<key>`` before it is merged, so a future plan
    that spells the holder explicitly cannot leak a literal
    ``"result.review_state"`` key into the payload: ``doctor._remediate_apply_fields``
    posts ``{name: after}`` verbatim, and ``review_queue.review_state`` reads
    ``result["review_state"]``, not ``result["result.review_state"]``. The bare
    dialect is exercised end to end by
    ``test_r6_the_production_plan_key_is_bare_and_reaches_the_review_queue``;
    the qualified branch by ``test_r6_a_qualified_result_key_is_unpacked_into_the_handover``."""
    merged = dict(result_dict(job))
    for name, change in plan.changes.items():
        text = str(name)
        key = text.split(".", 1)[1] if text.startswith("result.") else text
        if key:
            merged[key] = change.get("after")
    return merged


#: The per-row lists ``POST /v1/admin/jobs/reclassify`` files a row under when
#: it did NOT move it (``hive/bus/routers/admin.py``).
_RECLASSIFY_REFUSALS = ("not_found", "skipped_done", "skipped_retry_budget", "skipped_conflict")


def _refusal(resp: Any, jid: str) -> str:
    """The refusal list the reclassify response filed ``jid`` under, or ``""``.

    Only an explicit refusal counts: a body without per-row lists is judged by
    its HTTP code alone."""
    if not isinstance(resp, dict):
        return ""
    for name in _RECLASSIFY_REFUSALS:
        listed = resp.get(name)
        if isinstance(listed, list) and jid in listed:
            return name
    ok = resp.get("ok")
    if isinstance(ok, list) and jid not in ok and any(isinstance(resp.get(n), list) for n in _RECLASSIFY_REFUSALS):
        return "not_ok"
    return ""


def _settled_work_parent(job: dict[str, Any], job_lookup: Callable[[str], dict | None] | None) -> dict | None:
    """The work row a review job reviewed, when that row is already settled.

    None when there is no lookup, no parent, the parent is unreadable, is itself
    a review job, or is still open: in each case no note is owed."""
    parent_id = str(job.get("parent_job_id") or "").strip()
    if not parent_id or job_lookup is None:
        return None
    try:
        parent = job_lookup(parent_id)
    except Exception:  # noqa: BLE001 -- an unreadable parent is no evidence it settled
        return None
    if not isinstance(parent, dict) or review_queue.is_review_kind(parent):
        return None
    if str(parent.get("status") or "") in _SETTLED_STATUSES:
        return parent
    return None


def _review_job_outcome(
    job: dict[str, Any],
    cause: Cause,
    *,
    state: Any,
    escalate: Callable[[dict, str], None],
    job_lookup: Callable[[str], dict | None] | None,
    now: float,
    dry_run: bool = False,
) -> Outcome:
    """A review job in triage: no field change, no status move, no hand-over.

    At most ONE escalation note per :data:`ESCALATE_TTL_SEC`, and only when the
    work row it reviewed is already settled (nothing is waiting on this review
    any more, so a human should close it)."""
    jid = str(job.get("id") or "")
    if _marked_within(state, escalation_key(jid), ESCALATE_TTL_SEC, now):
        return Outcome(job_id=jid, status=NO_CHANGE, cause=cause.name, detail=cause.detail, reason="review_job")
    parent = _settled_work_parent(job, job_lookup)
    if parent is None:
        return Outcome(job_id=jid, status=NO_CHANGE, cause=cause.name, detail=cause.detail, reason="review_job")
    why = (
        f"review job {jid} ({job.get('kind')}) is in needs-review but its work row "
        f"{parent.get('id')} is already {parent.get('status')}; a review job is never itself reviewed"
    )
    if dry_run:
        # A dry-run sweep must not broadcast, nor spend the once-per-TTL marker
        # the live sweep needs.
        return Outcome(job_id=jid, status=NO_CHANGE, cause=cause.name, detail=cause.detail, reason="review_job_dry_run")
    escalate(job, why)
    state.set_value(escalation_key(jid), {"at": now}, ttl=ESCALATE_TTL_SEC)
    return Outcome(job_id=jid, status=ESCALATED, cause=cause.name, detail=cause.detail, reason="review_job_settled")


def remediate_one(
    job: dict[str, Any],
    *,
    state: Any,
    apply_fields: Callable[[str, dict[str, dict[str, Any]], str], tuple[int, Any]],
    reclassify: Callable[[str, str, str, bool, str], tuple[int, Any]],
    escalate: Callable[[dict, str], None],
    advance_review: Callable[[dict], Any] | None = None,
    rescue_workspace: Callable[[dict, str], tuple[bool, str]] | None = None,
    default_branch: Callable[[dict], str | None] | None = None,
    branch_exists: Callable[[str], bool] | None = None,
    ref_exists: Callable[[str], bool] | None = None,
    resolve_ref: Callable[[str], str] | None = None,
    roster: list[dict] | None = None,
    health: Callable[[dict], bool] | None = None,
    capped: set[str] | None = None,
    now: float | None = None,
    dry_run: bool = False,
    job_capped: Any = None,
    has_verdict: Callable[[dict], bool] | None = None,
    job_lookup: Callable[[str], dict | None] | None = None,
) -> Outcome:
    """Classify and FIX one row. Never approves, merges or deletes.

    The three side effects are injected so the whole decision is testable
    without a bus: ``apply_fields`` writes the result fields on the row,
    ``reclassify`` performs the admin transition and ``escalate`` posts
    ``doctor.needs_review_stuck``. The attempt counter and the per-(row, cause,
    head) marker are durable, so two sweeps — or a restarted doctor — act once.

    ``job_lookup(job_id)`` (optional) reads another row; it is used only to see
    whether the work row a ``review_job`` reviewed is already settled, for the
    one escalation note such a row may get.
    """
    jid = str(job.get("id") or "")
    # ``job_capped`` is the same caller-supplied set under its older name;
    # accepted so both call sites read naturally and neither silently drops it.
    capped = {str(c).lower() for c in (capped or ())} | {str(c).lower() for c in (job_capped or ())}
    now = time.time() if now is None else float(now)
    if not jid:
        return Outcome(job_id="", status=SKIPPED, reason="no_job_id")
    if is_lane_b_owned(job):
        return Outcome(job_id=jid, status=SKIPPED, reason="lane_b_owned")
    if str(job.get("status") or "") not in SCAN_STATUSES:
        return Outcome(job_id=jid, status=SKIPPED, reason="status_out_of_scope")
    cause = classify(
        job,
        has_verdict=has_verdict,
        roster=roster,
        health=health,
        capped={str(c).lower() for c in (job.get("capped_routes") or [])} | capped,
    )
    if cause is None:
        return Outcome(job_id=jid, status=NO_CHANGE, reason="healthy_row")
    if cause.name == REVIEW_JOB:
        return _review_job_outcome(
            job, cause, state=state, escalate=escalate, job_lookup=job_lookup, now=now, dry_run=dry_run
        )
    spent = attempts(state, jid)
    if spent >= MAX_AUTO_REMEDIATIONS:
        if not _marked_within(state, escalation_key(jid), ESCALATE_TTL_SEC, now):
            escalate(job, f"auto-remediation cap reached ({spent}/{MAX_AUTO_REMEDIATIONS}) for cause {cause.name}")
            state.set_value(escalation_key(jid), {"at": now}, ttl=ESCALATE_TTL_SEC)
            return Outcome(job_id=jid, status=ESCALATED, cause=cause.name, detail=cause.detail, attempts=spent)
        return Outcome(
            job_id=jid, status=SKIPPED, cause=cause.name, detail=cause.detail, reason="cap_reached", attempts=spent
        )
    plan = plan_for(
        job,
        cause,
        default_branch=default_branch(job) if default_branch else None,
        branch_exists=branch_exists,
        roster=roster,
        health=health,
        capped={str(c).lower() for c in (job.get("capped_routes") or [])} | capped,
        ref_exists=ref_exists,
        resolve_ref=resolve_ref,
        now=now,
    )
    if plan is None:
        return Outcome(job_id=jid, status=NO_CHANGE, cause=cause.name, detail=cause.detail, reason="no_provable_fix")
    key = mark_key(jid, cause, delivered_head(job))
    if already_fixed(state, key, now=now) or recent_audit(job, cause, now=now) or _fields_unchanged(job, plan):
        return Outcome(job_id=jid, status=NO_CHANGE, cause=cause.name, detail=cause.detail, reason="already_fixed")
    event = _audit_entry(cause, plan.changes, plan.reason, now, "requeue" if plan.requeue else "needs_review")
    if dry_run:
        # ``changes`` on a dry-run outcome is the SHAPE the row would receive
        # (the same mapping ``apply_fields`` gets), not the audit before/after
        # wrapper: a dry run is the preview of the write, so it must read like
        # the write. Live outcomes keep the audit entry's shape.
        applied = {name: change.get("after") for name, change in plan.changes.items()}
        return Outcome(
            job_id=jid,
            status=DRY_WOULD_APPLY,
            cause=cause.name,
            detail=plan.detail,
            reason=plan.reason,
            changes=applied,
            attempts=spent,
        )
    # Side effect 1 (stranded WIP, or a head branch missing on the remote):
    # push the preserved workspace BEFORE anything is written. The plan records
    # ``workspace_pushed: True``; written ahead of a push that then failed (or
    # had no rescuer), that claim made ``_fields_unchanged`` read the row as
    # already fixed on every later sweep, so it was never pushed or moved again.
    if cause.name == STRANDED_WIP or plan.push_workspace:
        if rescue_workspace is None:
            return Outcome(
                job_id=jid,
                status=FAILED,
                cause=cause.name,
                detail=plan.detail,
                error="no_workspace_rescuer",
                attempts=spent,
            )
        pushed, note = rescue_workspace(job, str(job.get("head_branch") or ""))
        if not pushed:
            return Outcome(
                job_id=jid,
                status=FAILED,
                cause=cause.name,
                detail=plan.detail,
                reason=plan.reason,
                error=note[:200],
                attempts=spent,
            )
    # Side effect 2: the field changes on the row's result (audit trail included).
    code, resp = apply_fields(jid, plan.changes, plan.reason)
    if code not in (200, 201, 204):
        return Outcome(
            job_id=jid,
            status=FAILED,
            cause=cause.name,
            detail=plan.detail,
            reason=plan.reason,
            error=f"field_patch_http={code}",
            attempts=spent,
        )
    # Side effect 3: the status move, through the existing admin reclassify.
    target = "queued" if plan.requeue else "needs-review"
    code, resp = reclassify(jid, target, plan.reason, plan.override, plan.override_reason)
    if code not in (200, 201, 204):
        return Outcome(
            job_id=jid,
            status=FAILED,
            cause=cause.name,
            detail=plan.detail,
            reason=plan.reason,
            error=f"reclassify_http={code}",
            attempts=spent,
        )
    refused = _refusal(resp, jid)
    if refused:
        # The admin route answers 200 for a batch it partly REFUSED (no retry
        # budget without an override, a concurrent status change): the row
        # did not move, so this is not a fix and spends no attempt.
        return Outcome(
            job_id=jid,
            status=FAILED,
            cause=cause.name,
            detail=plan.detail,
            reason=plan.reason,
            error=f"reclassify_refused={refused}",
            attempts=spent,
        )
    total = record_attempt(state, jid, at=now)
    mark_fixed(state, key, at=now)
    if plan.needs_review and advance_review is not None:
        try:
            advance_review(
                {
                    **job,
                    "status": "needs-review",
                    # EVERY planned change, not just the bare row fields: the
                    # plan for a review hand-over writes the result field
                    # ``review_state`` (the block the review queue consumes), so
                    # filtering the ``result.*`` keys handed the queue a row
                    # whose state the fix had never delivered -- the remediation
                    # landed on the row and the review queue never saw it.
                    # ``_handover_result`` also unpacks a qualified
                    # ``result.<key>`` into its holder, so the hand-over can
                    # never produce a literal ``"result.review_state"`` key.
                    "result": _handover_result(job, plan),
                }
            )
        except Exception as exc:  # noqa: BLE001 -- the hand-over is retried by the queue sweep
            log.warning("remediation: review hand-over failed job=%s: %s", jid[:12], exc)
    return Outcome(
        job_id=jid,
        status=APPLIED,
        cause=cause.name,
        detail=plan.detail,
        reason=plan.reason,
        changes={k: v for k, v in event["after"].items() if k != "review_state"},
        attempts=total,
    )


# ── The sweep ──────────────────────────────────────────────────────────────


def _would_act(row: dict[str, Any], *, state: Any) -> bool:
    """Whether the sweep would attempt a fix on ``row`` (cheap, no side effects).

    Used only past the per-sweep cap, to tell a row that is being DEFERRED from
    one the sweep would never have touched anyway."""
    if is_lane_b_owned(row):
        return False
    if str(row.get("status") or "") not in SCAN_STATUSES:
        return False
    jid = str(row.get("id") or "")
    if jid and attempts(state, jid) >= MAX_AUTO_REMEDIATIONS:
        return False
    cause = classify(row)
    return cause is not None and cause.name != REVIEW_JOB


def sweep(
    rows: list[dict[str, Any]],
    *,
    state: Any,
    apply_fields: Callable[[str, dict[str, dict[str, Any]], str], tuple[int, Any]],
    reclassify: Callable[[str, str, str, bool, str], tuple[int, Any]],
    escalate: Callable[[dict, str], None],
    advance_review: Callable[[dict], Any] | None = None,
    rescue_workspace: Callable[[dict, str], tuple[bool, str]] | None = None,
    default_branch: Callable[[dict], str | None] | None = None,
    branch_exists: Callable[[str], bool] | None = None,
    ref_exists: Callable[[str], bool] | None = None,
    resolve_ref: Callable[[str], str] | None = None,
    roster: list[dict] | None = None,
    health: Callable[[dict], bool] | None = None,
    capped: set[str] | None = None,
    now: float | None = None,
    dry_run: bool = False,
    max_per_sweep: int | None = None,
    job_capped: Any = None,
    job_lookup: Callable[[str], dict | None] | None = None,
) -> dict[str, Any]:
    """Remediate every fixable row in ``rows``, capped.

    Oldest first (the longest-stuck row drains first) and at most
    ``max_per_sweep`` APPLIED rows per pass; the rest are counted ``deferred``
    and picked up by the next sweep. One bad row never stops the pass."""
    now = time.time() if now is None else float(now)
    cap = MAX_REMEDIATIONS_PER_SWEEP if max_per_sweep is None else int(max_per_sweep)
    ordered = sorted(
        [r for r in rows or [] if isinstance(r, dict)],
        key=lambda r: (
            r.get("ended_at") or r.get("last_update_at") or r.get("started_at") or 0,
            str(r.get("id") or ""),
        ),
    )
    summary: dict[str, Any] = {
        "scanned": len(ordered),
        APPLIED: 0,
        "dry_run": bool(dry_run),
        "capped": cap,
    }
    #: Rows already handled in THIS pass. A caller that lists the same row twice
    #: must not remediate it twice: the durable marker covers the next sweep, and
    #: this covers the rest of this one.
    handled: set[str] = set()
    for row in ordered:
        row_id = str(row.get("id") or "")
        if row_id and row_id in handled:
            continue
        if row_id:
            handled.add(row_id)
        if summary[APPLIED] >= cap:
            # Only a row the sweep WOULD have acted on is deferred; a healthy or
            # out-of-scope row is counted by its own outcome, cap or no cap.
            if _would_act(row, state=state):
                summary["deferred"] = summary.get("deferred", 0) + 1
            else:
                summary["skipped"] = summary.get("skipped", 0) + 1
            continue
        try:
            outcome = remediate_one(
                row,
                state=state,
                apply_fields=apply_fields,
                reclassify=reclassify,
                escalate=escalate,
                advance_review=advance_review,
                rescue_workspace=rescue_workspace,
                default_branch=default_branch,
                branch_exists=branch_exists,
                ref_exists=ref_exists,
                resolve_ref=resolve_ref,
                roster=roster,
                health=health,
                capped=capped,
                now=now,
                dry_run=dry_run,
                job_capped=job_capped,
                job_lookup=job_lookup,
            )
        except Exception as exc:  # noqa: BLE001 -- one row must not stop the sweep
            summary["errors"] = summary.get("errors", 0) + 1
            log.error("remediation failed job=%s: %s", str(row.get("id"))[:12], exc)
            continue
        # One counter per outcome, and ``applied`` IS the APPLIED counter: it is
        # what the cap is measured against, so counting it twice would let the
        # cap through at half the configured size.
        if outcome.status == FOLLOW_UP and outcome.attempts:
            # One row can record SEVERAL non-blocking findings: the count is
            # the findings written, not the rows that wrote them.
            summary[FOLLOW_UP] = summary.get(FOLLOW_UP, 0) + int(outcome.attempts)
        else:
            summary[outcome.status] = summary.get(outcome.status, 0) + 1
        summary.setdefault("causes", {})
        if outcome.cause:
            summary["causes"][outcome.cause] = summary["causes"].get(outcome.cause, 0) + 1
        log.info(outcome.as_log())
    return summary


# ── Continuation rounds: GRIND UNTIL GREEN (operator 2026-11-01, amended) ───
# The orchestrator ran this loop by hand: a cross-family review asked for
# changes, a fix job carried the findings onto the branch, and the new head
# went back to the review queue like any other delivery. The doctor now runs
# it, BOUNDED, with alternating fix-in-place reviewers.
#
# GREEN is objective: the merged-tree suite is clean AND the import preflight
# is clean AND a STRONG reviewer family returns a structured APPROVE on the
# exact head. Only high/critical findings block; medium/low are recorded as a
# follow-up row and never re-open the loop. Each round reviews the DELTA since
# the last reviewed head plus whether the prior blocking findings were
# resolved. Hard limits (rounds, spend, hourly brake, stall by fingerprint)
# each escalate ONCE with the evidence and stop.
#
# Conflicts and merged-tree FAILURES are NOT repaired here: they belong to the
# merge-pass tool (fleet-ops). Only the review-driven path lives in this module.

#: Hard cap on rounds per work row (``HIVE_DOCTOR_GRIND_MAX_ROUNDS``). Past it
#: the row is escalated ONCE with its round count, findings and spend.
GRIND_MAX_ROUNDS_DEFAULT = 6


def _env_count(name: str, default: int) -> int:
    """A non-negative count from the environment; an unreadable value is the default."""
    try:
        return max(int(str(os.environ.get(name, "")).strip()), 0)
    except ValueError:
        return default


GRIND_MAX_ROUNDS = _env_count("HIVE_DOCTOR_GRIND_MAX_ROUNDS", GRIND_MAX_ROUNDS_DEFAULT)

#: Where the pre-2026-11-01 build read the old two-round rule; unused by the
#: grind loop, kept only so an operator env file that still sets it is not a
#: silent no-op (``docs/DOCTOR_REMEDIATION.md``).
_CONTINUATION_MAX_ROUNDS_ENV = "HIVE_DOCTOR_CONTINUATION_MAX_ROUNDS"

#: New continuation jobs one sweep may submit (a dry run previews as many).
CONTINUATION_MAX_PER_SWEEP = int(os.environ.get("HIVE_DOCTOR_CONTINUATION_MAX_PER_SWEEP", "3"))


#: The global brake: continuation jobs the doctor may create per rolling hour
#: across ALL rows. Counted in its own durable record, so it holds when every
#: per-row record (lineage, rounds, markers) has been lost. ``0`` creates none.
CONTINUE_MAX_PER_HOUR = _env_count("HIVE_DOCTOR_CONTINUE_MAX_PER_HOUR", 6)
HOURLY_WINDOW_SEC = 3600.0

#: Outcomes of the continuation guards. ``refused``: the doctor will not create
#: a job for this row (lineage unknown, store not durable); ``throttled``: the
#: hourly brake is reached.
REFUSED = "refused"
THROTTLED = "throttled"
LINEAGE_UNKNOWN = "lineage_unknown"
DURABLE_STATE_UNAVAILABLE = "durable_state_unavailable"

#: The standing instruction every continuation brief carries, verbatim.
CONTINUATION_INSTRUCTION = (
    "verify every finding against source first; failing test first when it holds; "
    "pinning test and a note when it does not; never change correct code to satisfy a "
    "disproved claim; push your final HEAD and confirm with git ls-remote"
)

#: ``idempotency_key`` prefix of a continuation job: ``<prefix>:<root>:r<N>:<tip12>``.
CONTINUATION_KEY_PREFIX = "doctor-continuation"
_CONTINUATION_KEY_RE = re.compile(r"^doctor-continuation:(?P<root>[^:]+):r(?P<round>\d+):")
_ROUND_SUFFIX_RE = re.compile(r"-r\d+$")

#: ``idempotency_key`` prefix of a GRIND job (operator 2026-11-01): the fix or
#: the follow-up review of one round, ``<prefix>:<root>:<fix|review>:r<N>:<tip12>``.
GRIND_KEY_PREFIX = "doctor-grind"
#: ``idempotency_key`` prefix of a follow-up row for a NON-BLOCKING finding:
#: ``<prefix>:<work row>:<fingerprint digest>:<reviewed head12>``.
FOLLOW_UP_KEY_PREFIX = "doctor:followup"

#: Severity order for picking the BEST request_changes review (most severe first).
_SEVERITY_RANK = {"critical": 0, "blocker": 0, "high": 1, "medium": 2, "moderate": 2, "low": 3, "info": 4, "nit": 5}

_APPROVE_WORDS = frozenset({"approve", "approved", "pass", "lgtm"})
_CHANGES_WORDS = frozenset({"request_changes", "changes_requested", "reject", "fail", "blocked"})

#: Severities that BLOCK green and re-open the loop. Every other severity is
#: recorded as a follow-up row and never blocks (operator 2026-11-01).
BLOCKING_SEVERITIES = frozenset({"critical", "blocker", "high"})

#: The STRONG reviewer families (operator 2026-11-01): muse (meta), deepseek,
#: Codex review-only per its ladder (openai) and opus. A local route
#: (cerberus-reviewer/gpt-oss-20b, qwen38, gemma, nemotron) may review and
#: request changes, but its approve never counts toward green. ``gpt-6-astra``
#: is the codex AUTHORIZATION model: never a reviewer, never green.
STRONG_REVIEW_FAMILIES = ("deepseek", "meta", "openai", "opus")
STRONG_NEVER_MODELS = frozenset({"gpt-6-astra"})
#: Opus is a model, not something the family table names: it answers
#: ``model:claude-opus-...`` (or ``anthropic``), neither of which is a family
#: the operator's strong list can be spelled with. It is recognised here, and
#: ONLY here, so the strong list stays the operator list.
_OPUS_RE = re.compile(r"claude[-_.]?opus|^opus", re.IGNORECASE)
#: The codex ladder is review-only: ``codex_job_allowed`` decides the rest.
CODEX_STRONG_ROUTE = review_queue.CODEX_ROUTE_ID

#: Per-row spend cap (USD) across the grind loop. 0/negative disables the cap;
#: an unreadable value is the default.
GRIND_MAX_SPEND_DEFAULT = 12.0


def _env_float(name: str, default: float) -> float:
    """A float from the environment; an unreadable value is the default."""
    try:
        return float(str(os.environ.get(name, "")).strip())
    except ValueError:
        return default


GRIND_MAX_SPEND_USD = _env_float("HIVE_DOCTOR_GRIND_MAX_SPEND_USD", GRIND_MAX_SPEND_DEFAULT)
#: Lines a fix round may add before it is rejected (``HIVE_DOCTOR_GRIND_DIFF_BUDGET``).
GRIND_DIFF_BUDGET = _env_count("HIVE_DOCTOR_GRIND_DIFF_BUDGET", 400)

#: Outcomes of the grind loop beyond the field-fix ones.
GREEN = "green"  # green proved on the head: nothing left to fix
STALLED = "stalled"  # the same finding fingerprint in two rounds
REJECTED = "rejected"  # a fix commit failed the test-integrity guard
FOLLOW_UP = "follow_up"  # a non-blocking finding recorded as its own row
FIX = "fix"  # a blocking finding handed to a fix job
REVIEW = "review"  # the follow-up review of that fix (a different family)
#: Why a row is not green (``green_verdict``'s ``why``).
SUITE_NOT_CLEAN = "suite_not_clean"
PREFLIGHT_NOT_CLEAN = "import_preflight_not_clean"
GATES_UNKNOWN = "gates_unknown"
NO_STRONG_APPROVE = "no_strong_approve"


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ("" if value is None else str(value).strip())


def _finding_location(finding: dict[str, Any]) -> str:
    """``path:line`` (or ``path``) of a finding, '' when it names no place.

    Three producer spellings: a ``location`` string, a ``location`` object
    (``path``/``file`` + ``line``) and top-level ``file``/``path`` + ``line``."""
    loc = finding.get("location")
    if isinstance(loc, str) and loc.strip():
        return loc.strip()
    holder = loc if isinstance(loc, dict) else finding
    path = _text(holder.get("path") or holder.get("file"))
    if not path:
        return ""
    line = _text(holder.get("line") or holder.get("start_line"))
    return f"{path}:{line}" if line else path


def _normalise_finding(raw: Any) -> dict[str, str] | None:
    """One located finding as ``{severity, location, title, body}`` -- or None.

    Values are carried VERBATIM (stripped only): the brief quotes them. A
    finding with no location, or with neither a title nor a body, is not
    actionable and is dropped."""
    if not isinstance(raw, dict):
        return None
    location = _finding_location(raw)
    title = _text(raw.get("title") or raw.get("summary"))
    body = _text(
        raw.get("body") or raw.get("detail") or raw.get("description") or raw.get("message") or raw.get("note")
    )
    if not location or not (title or body):
        return None
    severity = _text(raw.get("severity") or raw.get("level")).lower() or "unspecified"
    return {"severity": severity, "location": location, "title": title, "body": body}


def _finding_lists(result: Any, depth: int = 0) -> list[list]:
    """Every list of findings a review result carries, in a stable order.

    Read where the producers write them: the result itself, the legacy
    ``verdict`` dict, the worker's ``reviewer`` record, ``review_evidence`` (and
    its per-reviewer entries -- an APPROVING reviewer's notes are not requested
    changes), the loop log's last iteration, and a JSON ``tail``/``stdout``."""
    if not isinstance(result, dict) or depth > 1:
        return []
    out: list[list] = []

    def add(value: Any) -> None:
        if isinstance(value, list):
            out.append(value)

    def from_reviewers(entries: Any) -> None:
        for entry in entries if isinstance(entries, list) else []:
            if not isinstance(entry, dict):
                continue
            word = _text(entry.get("verdict")).lower().replace("-", "_")
            if word in _APPROVE_WORDS:
                continue
            add(entry.get("findings"))

    add(result.get("findings"))
    from_reviewers(result.get("reviewers"))
    for key in ("verdict", "reviewer", "review_evidence"):
        holder = result.get(key)
        if not isinstance(holder, dict):
            continue
        add(holder.get("findings"))
        from_reviewers(holder.get("reviewers"))
        inner = holder.get("verdict")
        if isinstance(inner, dict):
            add(inner.get("findings"))
    iterations = result.get("log") or result.get("iterations")
    if isinstance(iterations, list) and iterations and isinstance(iterations[-1], dict):
        add(iterations[-1].get("findings") or iterations[-1].get("unresolved_findings"))
    for key in ("tail", "stdout"):
        text = result.get(key)
        if isinstance(text, str) and text.strip()[:1] == "{":
            try:
                parsed = json.loads(text)
            except ValueError:
                continue
            out.extend(_finding_lists(parsed, depth + 1))
    return out


def located_findings(result: Any) -> list[dict[str, str]]:
    """The located findings of one review result, normalised and de-duplicated."""
    found: list[dict[str, str]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for entries in _finding_lists(result):
        for raw in entries:
            finding = _normalise_finding(raw)
            if finding is None:
                continue
            key = (finding["severity"], finding["location"], finding["title"], finding["body"])
            if key not in seen:
                seen.add(key)
                found.append(finding)
    return found


def blocking_findings(findings: list[dict[str, str]]) -> list[dict[str, str]]:
    """The findings that BLOCK green: ``high`` and ``critical`` only.

    Operator 2026-11-01: "Only high and critical findings block; medium/low
    findings are recorded as a follow-up row and never re-open the loop." An
    unlabelled severity (``unspecified``) is not a claim of severity, so it is
    recorded and does not block; it never reads as high."""
    return [f for f in findings if str(f.get("severity") or "").strip().lower() in BLOCKING_SEVERITIES]


def finding_fingerprint(finding: dict[str, str]) -> str:
    """A stable identity for one finding: its location and its normalized title.

    Severity is NOT part of it: a reviewer that re-raises the same defect at a
    lower severity has still not fixed it, and the stall detector must see one
    fingerprint, not two."""
    location = str(finding.get("location") or "").strip().lower()
    title = " ".join(str(finding.get("title") or finding.get("body") or "").lower().split())
    return f"{location}|{title}"


def finding_fingerprints(findings: list[dict[str, str]]) -> list[str]:
    """The fingerprints of ``findings``, de-duplicated, in order."""
    out: list[str] = []
    for finding in findings:
        fingerprint = finding_fingerprint(finding)
        if fingerprint not in out:
            out.append(fingerprint)
    return out


def is_strong_reviewer(model: Any, *, job: dict | None = None) -> bool:
    """Whether ``model`` is a STRONG reviewer family (operator 2026-11-01).

    Strong: muse (``meta``), ``deepseek``, Codex on its own ladder (review-only,
    ``openai``) and ``opus``. Local routes -- ``gpt-oss-20b`` on CERBERUS,
    ``qwen38``, gemma, nemotron -- may review but never count; ``gpt-6-astra``
    never counts, authorized or not (it is the codex authorization model, not
    a reviewer). An unknown model is its own family and is not strong."""
    name = str(model or "").strip()
    if not name or name.lower() in STRONG_NEVER_MODELS:
        return False
    if _OPUS_RE.search(name):
        return True
    if name.lower() == CODEX_STRONG_ROUTE or name.lower().startswith("codex"):
        # Review-only per the ladder: the ladder model, not the authorization
        # model and not an operator-authorized one.
        return review_queue.codex_job_allowed(None, name, job=job or {})
    family = review_queue.family_of(name)
    if family not in STRONG_REVIEW_FAMILIES:
        return False
    if family == "openai":
        # An OPENAI-family local route (cerberus gpt-oss-20b, or any other
        # ``gpt-oss`` build) is a LOCAL reviewer: it reviews, it never greens.
        return name.lower().startswith(("gpt-6", "codex")) and review_queue.codex_job_allowed(None, name)
    return True


def fixer_families(models: list[str]) -> list[str]:
    """The families of reviewers that REQUESTED CHANGES (branch authors of fixes).

    A reviewer that asked for changes on a head caused a fix commit to be
    written for it: it is a FIXER, and its approve of the resulting head can
    never carry the row to green."""
    return sorted({review_queue.family_of(m) for m in models if review_queue.family_of(m)})


def _review_models(review: dict[str, Any], res: dict[str, Any]) -> list[str]:
    """The reviewer models of one review row (verdict record first, then the claim).

    The CLAIM always rides along when it names a model: a row run by a LOCAL
    route must never be re-read as strong because its verdict record named only
    a provider ladder model, and a local reviewer must never count as a fixer's
    cross-family partner (operator 2026-11-01)."""
    from hive import review_record

    names: list[str] = []
    reviewer = res.get("reviewer")
    if isinstance(reviewer, dict):
        for name in [reviewer.get("model"), *(reviewer.get("models") or [])]:
            text = review_record._named(name)
            if text and text not in names:
                names.append(text)
    for holder in (res.get("review_evidence"), res):
        for name in review_record.evidence_reviewers(holder):
            if name not in names:
                names.append(name)
    verdict = res.get("verdict")
    if isinstance(verdict, dict):
        for name in review_record.evidence_reviewers(verdict):
            if name not in names:
                names.append(name)
    if not names:
        claimed = review_record._named(review.get("claimed_model"))
        if claimed:
            names.append(claimed)
    elif review.get("claimed_model"):
        # The CLAIM rides along: a row run by a LOCAL route must never be
        # re-read as strong because its verdict record named only a provider
        # ladder model, and a local reviewer must never count as a fixer's
        # cross-family partner (operator 2026-11-01).
        claimed = review_record._named(review.get("claimed_model"))
        if claimed and claimed not in names:
            names.append(claimed)
    return names


def _review_word_and_head(res: dict[str, Any]) -> tuple[str, str]:
    """``(verdict, reviewed head)`` of one review result ('' when unknown)."""
    from hive import review_verdict

    read = review_verdict.child_verdict(res)
    word = review_verdict.normalize_verdict(read.get("verdict")) if read else ""
    head = _text(read.get("head") or read.get("head_sha")) if read else ""
    if not word:
        evidence = res.get("review_evidence") if isinstance(res.get("review_evidence"), dict) else {}
        words = [
            review_verdict.normalize_verdict(e.get("verdict"))
            for e in evidence.get("reviewers") or []
            if isinstance(e, dict) and e.get("verdict")
        ]
        if any(w in _CHANGES_WORDS for w in words):
            word = "request_changes"
        elif words and all(w in _APPROVE_WORDS for w in words):
            word = "approve"
    if not head:
        heads = review_verdict.reviewed_heads(res)
        head = (
            heads[0]
            if heads
            else _text(res.get("remote_sha") if res.get("remote_verified") else "") or _text(res.get("head_sha"))
        )
    if word in _APPROVE_WORDS:
        word = "approve"
    elif word in _CHANGES_WORDS:
        word = "request_changes"
    return word, head


def read_review(
    row: dict[str, Any],
    review: dict[str, Any],
    *,
    tip: str,
    head_is_verdict_only: Callable[[dict, dict, str, str], bool] | None = None,
) -> dict[str, Any] | None:
    """One NON-AUTHOR review of ``row`` on its current tip, read -- or None.

    None for: an overreaching review (its verdict covers the reviewer's own
    edits), no verdict, an author whose family is unknown (non-author cannot be
    proven), a reviewer in the author's family, and a head that is neither the
    tip nor -- proven by ``head_is_verdict_only`` -- a verdict-only descendant
    of it (live 2026-11-01: the reviewer reports the commit that adds its
    verdict artifact). An unproven descendant is NOT the tip."""
    res = result_dict(review)
    if res.get("review_overreach") or res.get("failure_reason") == "review_overreach":
        return None
    word, head = _review_word_and_head(res)
    if word not in ("approve", "request_changes"):
        return None
    author = author_model(row)
    author_family = review_queue.family_of(author) if author else ""
    if not author_family:
        return None
    models = _review_models(review, res)
    families = sorted({review_queue.family_of(m) for m in models if review_queue.family_of(m)})
    if not families:
        return None
    # An APPROVE from the author's own family proves nothing -- the author is
    # not an independent reviewer of its own commit. A REQUEST_CHANGES from it
    # still names a real defect and must drive a fix: the family that raised it
    # is then excluded from BOTH the fixer and the reviewer rotation.
    if word == "approve" and author_family in families:
        return None
    from hive import review_verdict

    if not head or not tip:
        return None
    if review_verdict.same_sha(head, tip):
        basis = "exact"
    elif head_is_verdict_only is not None and head_is_verdict_only(row, review, tip, head):
        basis = "verdict_only"
    else:
        return None
    findings = located_findings(res) if word == "request_changes" else []
    return {
        "verdict": word,
        "reviewers": models,
        "families": families,
        # A structured APPROVE from a strong family is what green needs; a
        # local route may approve and never counts (operator 2026-11-01). Any
        # named reviewer model that is strong makes the approve strong: the
        # claim rides along with the verdict record and either may be the
        # strong one.
        "strong": any(is_strong_reviewer(m) for m in models) if word == "approve" else False,
        "head": head,
        "basis": basis,
        "findings": findings,
        "blocking": blocking_findings(findings),
        "fingerprints": finding_fingerprints(findings),
        "fixers": fixer_families(models),
        "review_job_id": str(review.get("id") or ""),
        "ended_at": review.get("ended_at") or 0,
    }


def _review_rank(review: dict[str, Any]) -> tuple:
    findings = review.get("findings") or []
    worst = min((_SEVERITY_RANK.get(f["severity"], 9) for f in findings), default=9)
    try:
        newest = -float(review.get("ended_at") or 0)
    except (TypeError, ValueError):
        newest = 0.0
    return (worst, -len(findings), newest, review.get("review_job_id") or "")


def best_review(
    row: dict[str, Any],
    reviews: list[dict[str, Any]],
    *,
    head_is_verdict_only: Callable[[dict, dict, str, str], bool] | None = None,
) -> dict[str, Any] | None:
    """The BEST non-author review of the row's current tip, or None.

    An approve outranks every request_changes (there is nothing to continue).
    Among request_changes reviews with located findings the most severe wins,
    then the most findings, then the newest. The families of EVERY
    request_changes review on the tip ride along (``requested_by``): the
    ladder must avoid all of them, not just the best one's."""
    tip = delivered_head(row)
    readings = [
        r
        for r in (read_review(row, review, tip=tip, head_is_verdict_only=head_is_verdict_only) for review in reviews)
        if r is not None
    ]
    if any(r["verdict"] == "approve" for r in readings):
        return None
    changes = [r for r in readings if r["verdict"] == "request_changes" and r["findings"]]
    if not changes:
        return None
    best = dict(sorted(changes, key=_review_rank)[0])
    best["requested_by"] = sorted({f for r in readings if r["verdict"] == "request_changes" for f in r["families"]})
    return best


def green_verdict(reviews: list[dict[str, Any]]) -> dict[str, Any]:
    """Whether a row is GREEN from its reviews, and why not when it is not.

    GREEN is objective (operator 2026-11-01): the merged-tree suite is clean,
    the import preflight is clean, and a STRONG reviewer family returns a
    structured APPROVE on the exact head. A strong approve whose gates were
    never run is not green: absence of proof is not proof. A strong approve
    from a FIXER family is not green either -- a fixer never approves its own
    commit. The newest strong approve wins when several qualify."""
    reasons: list[str] = []
    for review in reviews or []:
        if not isinstance(review, dict) or review.get("verdict") != "approve":
            continue
        why = green_blocker(review)
        if why:
            reasons.append(why)
            continue
        return {"green": True, "why": "", "review": review}
    why = reasons[0] if reasons else NO_STRONG_APPROVE
    return {"green": False, "why": why, "review": None}


def green_blocker(review: dict[str, Any]) -> str:
    """Why one approving review is not GREEN: '' when it is."""
    if not review.get("strong"):
        return NO_STRONG_APPROVE
    gates = review.get("gates")
    if not isinstance(gates, dict) or "suite" not in gates or "import_preflight" not in gates:
        return GATES_UNKNOWN
    if suite_text(gates.get("suite")) is None:
        return SUITE_NOT_CLEAN
    if not _preflight_clean(gates.get("import_preflight")):
        return PREFLIGHT_NOT_CLEAN
    return ""


#: The shape a gate result has: ``{"clean": bool, "detail": str}``.
def _gate(clean: bool, detail: str = "") -> dict[str, Any]:
    return {"clean": bool(clean), "detail": str(detail or "")}


_PASSED_RE = re.compile(r"(?P<count>\d+)\s+passed")
_BAD_SUMMARY_RE = re.compile(r"\b(\d+)\s+(failed|error|errors|skipped|xfailed|xpassed|deselected|warnings?)\b")
_NO_TESTS_RE = re.compile(r"no tests ran|collected 0 items|collecting\s*\.\.\.\s*$", re.IGNORECASE)


def suite_gate(run: dict[str, Any] | None) -> dict[str, Any]:
    """The merged-tree suite gate from a finished run.

    ``run`` is ``{"returncode": int, "summary": str}`` -- what the runner
    printed. Clean means exit 0, at least one test PASSED, and nothing failed,
    errored, was skipped/xfailed or was deselected: a skipped test is a test
    that did not run, and a grind that accepts one can hide a real failure
    behind ``pytest.skip``."""
    if not isinstance(run, dict):
        return _gate(False, "no run")
    code = run.get("returncode")
    if code != 0:
        return _gate(False, f"returncode={code}")
    summary = str(run.get("summary") or "").strip()
    if not summary:
        return _gate(False, "no summary")
    if _NO_TESTS_RE.search(summary):
        return _gate(False, "no tests ran")
    bad = _BAD_SUMMARY_RE.search(summary)
    if bad:
        return _gate(False, summary)
    if not _PASSED_RE.search(summary):
        return _gate(False, summary)
    return _gate(True, summary)


def suite_text(value: Any) -> str | None:
    """The suite summary ``value`` when it reads CLEAN, else None.

    Accepts a gate dict (``{"clean", "detail"}``), the word ``clean``/``ok``,
    or the runner's own summary line (``"812 passed in 41.20s"``). Anything
    that does not read clean -- a failure, an error, a skip, no run at all --
    is None, so a caller can never treat "unreadable" as "clean"."""
    if isinstance(value, dict):
        gate = suite_gate(value)
        return gate["detail"] if gate["clean"] else None
    text = str(value or "").strip()
    if not text:
        return None
    if text.lower() in {"clean", "ok", "pass", "passed"}:
        return text
    gate = suite_gate({"returncode": 0, "summary": text})
    return gate["detail"] if gate["clean"] else None


def _preflight_clean(value: Any) -> bool:
    """Whether an import-preflight result reads clean. Fails closed."""
    if isinstance(value, dict):
        return value.get("clean") is True
    text = str(value or "").strip().lower()
    return text in {"clean", "ok", "pass", "passed"}


def import_preflight(path: str | Path, *, base: str, head: str, timeout: float = 300.0) -> dict[str, Any]:
    """Compile every Python file the head changes against ``base``.

    The floor under "import preflight clean": a file that does not even
    COMPILE cannot be imported, and a suite that never imports it (an
    unreachable module, a missing guard) would otherwise call the tree clean.
    Fails closed: an unreadable base/head or a git failure is NOT clean. The
    detail is bounded and carries paths only, never file contents."""
    import subprocess

    root = str(path)
    try:
        names = subprocess.run(
            ["git", "-C", root, "diff", "--name-only", "--diff-filter=d", f"{base}..{head}", "--", "*.py"],
            capture_output=True,
            text=True,
            check=True,
            timeout=timeout,
        ).stdout
    except (subprocess.SubprocessError, OSError) as exc:
        return _gate(False, f"preflight cannot read the diff: {type(exc).__name__}")
    failed: list[str] = []
    for name in [line.strip() for line in names.splitlines() if line.strip()]:
        try:
            source = subprocess.run(
                ["git", "-C", root, "show", f"{head}:{name}"],
                capture_output=True,
                check=True,
                timeout=timeout,
            ).stdout
        except (subprocess.SubprocessError, OSError) as exc:
            failed.append(f"{name}: unreadable ({type(exc).__name__})")
            continue
        try:
            compile(source, name, "exec")
        except SyntaxError as exc:
            failed.append(f"{name}: SyntaxError line {exc.lineno}")
    if failed:
        return _gate(False, "; ".join(failed)[:500])
    return _gate(True, f"{len(names.splitlines())} file(s) compiled")


#: A test file by name: ``test_*.py``, ``*_test.py``, or under ``tests/``.
_TEST_FILE_RE = re.compile(r"(^|/)(test_[^/]*|[^/]*_test)\.py$|(^|/)tests/")


def _is_test_file(name: str) -> bool:
    return bool(_TEST_FILE_RE.search(str(name or "")))


#: Assertion mentions (for the weak/removed assertion guard).
_ASSERT_RE = re.compile(r"^\s*(assert\b|self\.assert\w+|pytest\.raises\b|self\.fail\b)", re.MULTILINE)
#: Skips, xfails and unconditional deselects a fix must never ADD.
_SKIP_RE = re.compile(
    r"pytest\.skip|pytest\.xfail|@pytest\.mark\.(skip|xfail)|\bskip_test\b|unittest\.skip|\bxfail\b",
    re.IGNORECASE,
)
_HUNK_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+\d+(?:,\d+)? @@", re.MULTILINE)


def _not_ok(why: str, **extra: Any) -> dict[str, Any]:
    return {"ok": False, "why": why, "lines": 0, **extra}


def test_integrity(
    path: str | Path, *, base: str, head: str, budget: int | None = None, timeout: float = 300.0
) -> dict[str, Any]:
    """The test-integrity guard on ONE fix commit range.

    A fix commit that DELETES a test file, removes or weakens an assertion,
    ADDS a skip/xfail, or grows the diff past ``budget`` lines is rejected: the
    round fails, nothing merges and the operator is told. Reject, never repair
    -- the guard is what stops a fixer from turning a red suite green by
    editing the tests. Fails closed: an unreadable diff is a rejection."""
    import subprocess

    limit = GRIND_DIFF_BUDGET if budget is None else int(budget)
    root = str(path)

    def git(*args: str) -> str:
        out = subprocess.run(
            ["git", "-C", root, *args], capture_output=True, text=True, check=True, timeout=timeout
        ).stdout
        return out

    try:
        name_status = git("diff", "--name-status", "-M", f"{base}..{head}")
        numstat = git("diff", "--numstat", f"{base}..{head}")
    except (subprocess.SubprocessError, OSError) as exc:
        return _not_ok(f"cannot read the fix diff: {type(exc).__name__}")
    deleted = [line.split("\t", 1)[1] for line in name_status.splitlines() if line.startswith(("D\t", "D "))]
    for name in deleted:
        if _is_test_file(name):
            return _not_ok(f"deleted: {name}")
    lines = 0
    for row in numstat.splitlines():
        parts = row.split("\t")
        if len(parts) < 3:
            continue
        try:
            lines += int(parts[0]) + int(parts[1])
        except ValueError:  # a binary file: "-\t-\tpath"
            continue
    if lines > limit:
        return _not_ok(f"over the diff budget: {lines} > {limit}", lines=lines)
    try:
        diff = git("diff", "-U0", "-M", f"{base}..{head}")
    except (subprocess.SubprocessError, OSError) as exc:
        return _not_ok(f"cannot read the fix patch: {type(exc).__name__}")
    current = ""
    added_asserts = removed_asserts = 0
    for line in diff.splitlines():
        if line.startswith("+++ "):
            current = line[4:].strip()
            if current.startswith("b/"):
                current = current[2:]
            added_asserts = removed_asserts = 0
            continue
        if line.startswith(("--- ", "@@", "diff ", "index ", "new file", "similarity", "rename", "deleted file")):
            continue
        if not line.startswith(("+", "-")):
            continue
        body = line[1:]
        if line.startswith("+"):
            if _SKIP_RE.search(body):
                return _not_ok(f"skip added: {current}")
            if _ASSERT_RE.search(body):
                added_asserts += 1
                continue
        elif _ASSERT_RE.search(body):
            removed_asserts += 1
            continue
        # Only a test file's non-assert LINE removal counts, and only when no
        # assertion replaced it: a refactor that keeps the coverage is not a
        # weakening.
        if (
            _is_test_file(current)
            and line.startswith("-")
            and body.strip()
            and not body.lstrip().startswith("#")
            and removed_asserts > added_asserts
        ):
            return _not_ok(f"assertion removed: {current}", lines=lines)
    if removed_asserts > added_asserts:
        return _not_ok(f"assertion removed: {current}", lines=lines)
    return {"ok": True, "why": "", "lines": lines}


def green_marker_key(job_id: str, head: str) -> str:
    """The durable GREEN record for one (work row, head)."""
    return f"{_KEY_MARK}:grind:green:{job_id}:{str(head or '')[:12]}"


def green_marker(state: Any, job_id: str) -> dict[str, Any] | None:
    """The GREEN record of ``job_id`` (newest head), or None."""
    keys = getattr(state, "keys", None)
    if not callable(keys):
        return None
    for key in reversed(keys(f"{_KEY_MARK}:grind:green:{job_id}:")):
        value = state.value(key, None)
        if isinstance(value, dict) and value.get("head"):
            return value
    return None


def record_spend(state: Any, job_id: str, usd: float, *, now: float | None = None) -> float:
    """Add ``usd`` to the row's grind spend and return the total."""
    key = f"{_KEY_MARK}:grind:spend:{job_id}"
    spent = 0.0
    raw = state.value(key, None)
    if isinstance(raw, dict):
        try:
            spent = float(raw.get("usd") or 0.0)
        except (TypeError, ValueError):
            spent = 0.0
    total = max(spent, 0.0) + max(float(usd or 0.0), 0.0)
    stamp = time.time() if now is None else float(now)
    state.set_value(key, {"usd": round(total, 6), "at": stamp}, ttl=MARK_TTL_SEC)
    return total


def grind_spend(state: Any, job_id: str) -> float:
    """What the row's grind has spent so far (USD)."""
    raw = state.value(f"{_KEY_MARK}:grind:spend:{job_id}", None)
    try:
        return max(float((raw or {}).get("usd") or 0.0), 0.0) if isinstance(raw, dict) else 0.0
    except (TypeError, ValueError):
        return 0.0


def _grind_key(job_id: str) -> str:
    return f"{_KEY_MARK}:grind:state:{job_id}"


def grind_state(row: dict[str, Any], state: Any) -> dict[str, Any]:
    """The row's durable grind state: rounds, reviewed heads, fingerprints, fixers."""
    raw = state.value(_grind_key(str(row.get("id") or "")), None)
    base = {
        "rounds": 0,
        "lineage_round": 0,
        "reviewed": [],
        "unresolved": [],
        "fingerprints": [],
        "fixers": [],
        "fix_pending": None,
        "rejections": 0,
    }
    if isinstance(raw, dict):
        for name, default in base.items():
            value = raw.get(name, default)
            base[name] = value if isinstance(value, type(default)) or default is None else default
    return base


def save_grind_state(job_id: str, state: Any, data: dict[str, Any], *, now: float) -> None:
    """Persist the grind state (durable: every guard on the loop reads it)."""
    state.set_value(_grind_key(job_id), {**data, "at": now}, ttl=MARK_TTL_SEC)


def continuation_lineage(row: dict[str, Any], state: Any = None) -> tuple[str, int]:
    """``(root work row id, round)`` of a row: ``(own id, 0)`` for an original.

    The doctor records each continuation it submits (durable, keyed on the new
    job id) and also spells it in the job's ``idempotency_key``; either is
    enough, so a row whose key the bus dropped still counts its rounds."""
    jid = str(row.get("id") or "")
    if state is not None and jid:
        recorded = state.value(_lineage_key(jid), None)
        if isinstance(recorded, dict) and recorded.get("root"):
            try:
                return str(recorded["root"]), int(recorded.get("round") or 0)
            except (TypeError, ValueError):
                pass
    match = _CONTINUATION_KEY_RE.match(str(row.get("idempotency_key") or ""))
    if match:
        return match.group("root"), int(match.group("round"))
    return jid, 0


def _lineage_key(job_id: str) -> str:
    return f"{_KEY_MARK}:continuation:lineage:{job_id}"


def _rounds_key(root: str) -> str:
    return f"{_KEY_MARK}:continuation:rounds:{root}"


def continuation_escalation_key(root: str) -> str:
    return f"{_KEY_MARK}:continuation:escalated:{root}"


#: The hourly brake's own durable record: ``{"stamps": [module-clock seconds]}``.
HOURLY_KEY = f"{_KEY_MARK}:continuation_hourly"


def lineage_unknown_key(job_id: str) -> str:
    return f"{_KEY_MARK}:continuation_lineage_unknown:{job_id}"


def state_durable(state: Any) -> bool:
    """Whether the store is durable: loaded AND persisting. Missing flags are a no."""
    try:
        return bool(state.loaded_ok) and bool(state.persist_ok)
    except Exception:  # noqa: BLE001 -- an unreadable store is not a durable one
        return False


def _hourly_stamps(state: Any, now: float) -> list[float]:
    """Creation stamps still inside the hour. A stamp in the FUTURE (a clock that
    moved backwards) still counts: the brake never loosens on a clock jump."""
    raw = state.value(HOURLY_KEY, None)
    stamps = raw.get("stamps") if isinstance(raw, dict) else None
    out = []
    for stamp in stamps if isinstance(stamps, list) else []:
        try:
            value = float(stamp)
        except (TypeError, ValueError):
            continue
        if now - value <= HOURLY_WINDOW_SEC:
            out.append(value)
    return out


def continuations_last_hour(state: Any, now: float | None = None) -> int:
    """Continuation jobs the doctor created (or tried to) in the last hour."""
    return len(_hourly_stamps(state, time.time() if now is None else float(now)))


def _reserve_hourly(state: Any, now: float) -> None:
    """Count one creation BEFORE submitting: a submit that timed out may still
    have created the job, so a failure is counted too (fail closed)."""
    stamps = _hourly_stamps(state, now) + [now]
    state.set_value(HOURLY_KEY, {"stamps": stamps}, ttl=max(MARK_TTL_SEC, 2 * HOURLY_WINDOW_SEC))


#: The first line of every continuation brief (see :func:`continuation_brief`).
_BRIEF_MARKER_RE = re.compile(r"\bContinuation round \d+ of \d+ for work job\b")
_BRIEF_FIELDS = ("description", "brief", "prompt", "task")


class LineageLookupError(RuntimeError):
    """The parent chain could not be read: lineage is neither proven nor disproven."""


def _own_continuation_markers(row: dict[str, Any]) -> str:
    """The continuation marker the row itself carries, '' when none."""
    if str(row.get("idempotency_key") or "").startswith(CONTINUATION_KEY_PREFIX + ":"):
        return "idempotency_key"
    res = result_dict(row)
    for holder in (row, res):
        for name in _BRIEF_FIELDS:
            text = holder.get(name)
            if isinstance(text, str) and (_BRIEF_MARKER_RE.search(text) or CONTINUATION_INSTRUCTION in text):
                return "brief_marker"
    if _ROUND_SUFFIX_RE.search(str(row.get("head_branch") or "").strip()):
        return "round_branch"
    return ""


def _doctor_continued(state: Any, job_id: str) -> bool:
    """Whether the store shows the doctor continued ``job_id`` or it is a continuation."""
    if state is None or not job_id:
        return False
    if isinstance(state.value(_lineage_key(job_id), None), dict):
        return True
    if state.value(_rounds_key(job_id), None):
        return True
    keys = getattr(state, "keys", None)
    return bool(callable(keys) and keys(f"{_KEY_MARK}:continuation:{job_id}:"))


def continuation_evidence(
    row: dict[str, Any],
    state: Any = None,
    *,
    job_lookup: Callable[[str], dict | None] | None = None,
    max_hops: int = 8,
) -> str:
    """Why ``row`` looks like a continuation, '' when nothing says so.

    Read independently of the lineage record and of a well-formed key, so a row
    that lost both is still recognised: a ``doctor-continuation:`` key prefix,
    the brief's marker in its description, a ``-rN`` head branch, a parent the
    store shows the doctor continued, or (with ``job_lookup``) a parent chain in
    which a work row's head is this row's base or carries a marker itself.
    Raises :class:`LineageLookupError` when the chain cannot be read."""
    own = _own_continuation_markers(row)
    if own:
        return own
    current = row
    seen = {str(row.get("id") or "")}
    for _hop in range(max_hops):
        pid = str(current.get("parent_job_id") or "").strip()
        if not pid or pid in seen:
            return ""
        seen.add(pid)
        if _doctor_continued(state, pid):
            return "parent_continued"
        if job_lookup is None:
            return ""
        try:
            parent = job_lookup(pid)
        except Exception as exc:  # noqa: BLE001 -- unreadable is not "no parent"
            raise LineageLookupError(f"parent {pid} unreadable: {exc}") from exc
        if not isinstance(parent, dict):
            return ""
        if review_queue.is_review_kind(parent) or str(parent.get("kind") or "").startswith(("review:", "triage:")):
            return ""
        base = str(current.get("base_branch") or "").strip()
        if base and base == str(parent.get("head_branch") or "").strip():
            return "parent_chain"
        if _own_continuation_markers(parent):
            return "parent_chain"
        current = parent
    return ""


def _lineage_recorded(row: dict[str, Any], state: Any) -> bool:
    """Whether :func:`continuation_lineage` reads a real record (not the fallback)."""
    jid = str(row.get("id") or "")
    recorded = state.value(_lineage_key(jid), None) if jid else None
    if isinstance(recorded, dict) and recorded.get("root"):
        try:
            int(recorded.get("round") or 0)
            return True
        except (TypeError, ValueError):
            pass
    return bool(_CONTINUATION_KEY_RE.match(str(row.get("idempotency_key") or "")))


def continuation_key(job_id: str, tip: str, round_no: int) -> str:
    """The durable "already continued" marker for one (row, tip, round)."""
    return f"{_KEY_MARK}:continuation:{job_id}:{str(tip or '')[:12] or 'nohead'}:r{int(round_no)}"


def rounds_spent(row: dict[str, Any], state: Any) -> tuple[str, int]:
    """``(root, rounds already submitted for the lineage)``."""
    root, own = continuation_lineage(row, state)
    try:
        recorded = int(state.value(_rounds_key(root), 0) or 0)
    except (TypeError, ValueError):
        recorded = 0
    return root, max(own, recorded)


def continuation_branch(row: dict[str, Any], round_no: int, state: Any = None) -> str:
    """``<slug>-rN``: the slug is the head branch without a previous ``-rN``
    (stripped only from a row that IS a continuation)."""
    head = str(row.get("head_branch") or "").strip()
    _root, own = continuation_lineage(row, state)
    slug = _ROUND_SUFFIX_RE.sub("", head) if own else head
    return f"{slug}-r{int(round_no)}"


def _hard_lane_routes(roster: list[dict]) -> list[dict]:
    flags = lambda r: r.get("flags") if isinstance(r.get("flags"), dict) else {}  # noqa: E731
    hard = [r for r in roster if review_queue._int_flag(flags(r).get("hard_lane_priority")) > 0]
    return sorted(hard, key=lambda r: review_queue._bulk_key(r, review_queue.LANE_HARD))


def _author_family(row: dict[str, Any]) -> str:
    author = author_model(row)
    return review_queue.family_of(author) if author else ""


def fixer_pick(
    row: dict[str, Any],
    review: dict[str, Any],
    *,
    fixers: list[str] | None = None,
    round_no: int = 1,
    roster: list[dict] | None = None,
) -> dict[str, Any] | None:
    """The model a FIX round runs on (in place, on the row's own branch).

    Alternation (operator 2026-11-01): the fixer's family differs from the
    branch author's, from every reviewer that requested changes, and from every
    family that already fixed this row. Round 1 prefers the author's own
    route (the author knows its own code and is not a reviewer), later rounds
    take the strongest hard-lane route outside every one of those families.
    ``{"model", "route"}`` or None (the row escalates: no cross-family fixer)."""
    blocked = {f for f in [*(review.get("requested_by") or []), *(review.get("families") or []), *(fixers or [])] if f}
    blocked |= set(review_queue.FORBIDDEN_FAMILIES) | set(review_queue.NEVER_REVIEW_FAMILIES)
    author_family = _author_family(row)
    # Round 1 fixes in the AUTHOR's own family: the author knows its own code
    # and is not a reviewer, so it is never "a fixer approving its own commit"
    # -- the REVIEWER of the next round is what must differ from the fixer.
    if int(round_no) <= 1:
        author = author_model(row)
        if not author or not author_family:
            return None
        if author_family in blocked:
            return None
        route = roster_route_for(author, roster=roster) if roster else None
        return {"model": author, "route": route}
    # Later rounds take the strongest hard-lane route outside every family that
    # reviewed, fixed, or authored this row: the fixer must be cross-family
    # from the reviewer, and a fixer never approves its own commit.
    for route in _hard_lane_routes(list(roster if roster is not None else review_queue.load_roster())):
        family = str(route.get("family") or "")
        if not family or family in blocked or family == author_family:
            continue
        return {"model": str(route.get("model_id") or ""), "route": route}
    return None


def ladder_pick(
    row: dict[str, Any], review: dict[str, Any], round_no: int, *, roster: list[dict] | None = None
) -> dict[str, Any] | None:
    """The model a continuation round runs on, or None (the row escalates).

    Round 1: the AUTHOR's own model -- same family as the author, which must
    not be a family that requested changes. Round 2+: the strongest hard-lane
    route (``hard_lane_priority``, ``review_queue``'s hard-lane order) whose
    family requested no changes and is not forbidden. ``{"model", "route"}``."""
    blocked = {f for f in [*(review.get("requested_by") or []), *(review.get("families") or [])] if f}
    if int(round_no) <= 1:
        author = author_model(row)
        family = review_queue.family_of(author) if author else ""
        if not family or family in blocked or family in review_queue.FORBIDDEN_FAMILIES:
            return None
        route = roster_route_for(author, roster=roster) if roster else None
        return {"model": author, "route": route}
    for route in _hard_lane_routes(list(roster if roster is not None else review_queue.load_roster())):
        family = str(route.get("family") or "")
        if not family or family in blocked or family in review_queue.FORBIDDEN_FAMILIES:
            continue
        return {"model": str(route.get("model_id") or ""), "route": route}
    return None


def _reviewer_pick(
    row: dict[str, Any], review: dict[str, Any], *, fixers: list[str] | None, roster: list[dict] | None
) -> dict[str, Any] | None:
    """The model a FOLLOW-UP REVIEW round runs on: a STRONG family only.

    The reviewer's family differs from the author's, from the fixers' and from
    every family that requested changes (it is the next, independent round).
    Only a strong family can carry a row to green, so a round that no strong
    route can review does not run: the row escalates instead of burning a
    round on a reviewer whose approve would not count."""
    # ``fixers`` holds MODEL ids, not families: resolve them, or a fixer is
    # offered its own commit to approve (which could never count).
    fixer_families = [review_queue.family_of(str(m)) for m in (fixers or []) if m]
    blocked = {f for f in [*(review.get("requested_by") or []), *(review.get("families") or []), *fixer_families] if f}
    blocked |= set(review_queue.FORBIDDEN_FAMILIES)
    author_family = _author_family(row)
    routes = list(roster if roster is not None else review_queue.load_roster())
    # Hard-lane routes first (the strongest), then any other STRONG route: a
    # reviewer only has to be strong and cross-family, not hard-lane.
    hard = _hard_lane_routes(routes)
    ordered = hard + [r for r in routes if r not in hard]
    for route in ordered:
        model = str(route.get("model_id") or "")
        family = str(route.get("family") or "")
        if not family or family in blocked or family == author_family:
            continue
        if not is_strong_reviewer(model):
            continue
        return {"model": model, "route": route}
    return None


def _job_route(pick: dict[str, Any]) -> tuple[str, dict | None]:
    """``(model, route)`` from a pick (a bare model string is a pick too)."""
    if isinstance(pick, str):
        return pick, None
    route = pick.get("route") if isinstance(pick.get("route"), dict) else None
    return str(pick.get("model") or ""), route


def _ceil_tier(row: dict[str, Any], route: dict | None) -> str | None:
    """The row's cost ceiling, raised to the route's class when that is higher."""
    tier = str(row.get("max_cost_tier") or "").strip().upper()
    route_tier = str((route or {}).get("cost_class") or "").strip().upper()
    if route_tier in COST_CLASSES and (
        tier not in COST_CLASSES or COST_CLASSES.index(route_tier) > COST_CLASSES.index(tier)
    ):
        tier = route_tier
    return tier or None


def _confine_to_codex(spec: dict[str, Any], route: dict | None) -> dict[str, Any]:
    """A codex route's job is claimable by the local codex worker only."""
    if route and review_queue.is_codex_route(route):
        spec["eligible_kinds"] = [review_queue.CODEX_WORKER_KIND]
        spec["max_concurrency"] = 1
    return spec


def continuation_brief(row: dict[str, Any], review: dict[str, Any], round_no: int, *, base: str, head: str) -> str:
    """The continuation job's description: context, findings verbatim, the rule."""
    tip = delivered_head(row) or "(unrecorded)"
    lines = [
        f"Continuation round {int(round_no)} of {GRIND_MAX_ROUNDS} for work job {row.get('id')} "
        f"({row.get('kind') or 'work'}) in {row.get('repo') or '(see job)'}.",
        f"Start from branch {base} at commit {tip}; create the NEW branch {head} from it, commit your fixes "
        f"there and push {head}. Never push to {base}.",
        f"A cross-family review ({', '.join(review.get('reviewers') or []) or 'reviewer'}; "
        f"families {', '.join(review.get('families') or []) or 'unknown'}) requested changes on {review.get('head') or tip}"
        f"{' (a verdict-only descendant of the tip)' if review.get('basis') == 'verdict_only' else ''}. "
        "Its findings, verbatim:",
        "",
    ]
    for index, finding in enumerate(review.get("findings") or [], start=1):
        lines.append(f"{index}. [{finding['severity']}] {finding['location']} -- {finding['title']}")
        if finding.get("body"):
            lines.append(f"   {finding['body']}")
    lines += ["", f"Standing instruction: {CONTINUATION_INSTRUCTION}."]
    return "\n".join(lines)


def fix_brief(row: dict[str, Any], review: dict[str, Any], round_no: int, *, head: str) -> str:
    """The FIX job's description: the blocking findings verbatim, the mandatory rule.

    Verify before fixing is not advice here, it is the instruction: the fixer
    first reproduces each finding against source with a failing test; when it
    cannot, it adds a pinning test and a note and the finding is closed as
    disproved, and correct code is never changed to satisfy a disproved claim.
    The test-integrity guard rejects a fix that deletes or weakens a test."""
    tip = delivered_head(row) or "(unrecorded)"
    blocking = review.get("blocking") or []
    lines = [
        f"Fix round {int(round_no)} of {GRIND_MAX_ROUNDS} for work job {row.get('id')} "
        f"({row.get('kind') or 'work'}) in {row.get('repo') or '(see job)'}.",
        f"Fix IN PLACE on branch {head} at commit {tip}: commit the fix as a continuation commit on {head} "
        f"and push {head}. Do not open a new branch and never push to {row.get('base_branch') or '(the base)'}.",
        f"A cross-family review ({', '.join(review.get('reviewers') or []) or 'reviewer'}; families "
        f"{', '.join(review.get('families') or []) or 'unknown'}) found {len(blocking)} BLOCKING finding(s) "
        f"on {review.get('head') or tip}. Only these are in scope; medium and low findings are "
        "recorded for follow-up and must not be chased here:",
        "",
    ]
    for index, finding in enumerate(blocking, start=1):
        lines.append(f"{index}. [{finding['severity']}] {finding['location']} -- {finding['title']}")
        if finding.get("body"):
            lines.append(f"   {finding['body']}")
    lines += [
        "",
        f"Standing instruction: {CONTINUATION_INSTRUCTION}.",
        "Do not delete or weaken an existing assertion or test, and never add a skip or xfail: "
        f"such a commit is rejected and the round fails (diff budget +{GRIND_DIFF_BUDGET} lines).",
    ]
    return "\n".join(lines)


def review_brief(row: dict[str, Any], review: dict[str, Any], *, head: str, gate: dict[str, Any]) -> str:
    """The follow-up REVIEW job's description: the DELTA and the prior findings.

    Each round reviews the DELTA since the last reviewed head plus whether the
    prior blocking findings were resolved -- not a fresh full review."""
    prior = review.get("blocking") or []
    last = review.get("head") or delivered_head(row) or "(unrecorded)"
    lines = [
        "READ-ONLY review. Do not modify any file, branch, or commit. No commits expected.",
        f"Grind round review for work job {row.get('id')} on branch {head or '(see job)'} at HEAD {last}.",
        f"Review the DELTA since the last reviewed head {last} and whether each prior blocking finding "
        "was resolved; do not repeat a fresh full review. Return a structured verdict JSON with a "
        "'verdict' field (approve | request_changes | blocked), a 'head_sha' field with the exact reviewed "
        "commit, a 'reason' and located findings.",
        "",
        "Prior blocking findings:",
    ]
    if prior:
        for index, finding in enumerate(prior, start=1):
            lines.append(f"{index}. [{finding['severity']}] {finding['location']} -- {finding['title']}")
    else:
        lines.append("(none: this is the first round)")
    lines += [
        "",
        f"This round's objective gates: suite={gate.get('suite')}, import_preflight={gate.get('import_preflight')}.",
        f"Reviewed job id: {row.get('id')}.",
    ]
    return "\n".join(lines)


def follow_up_spec(
    row: dict[str, Any], finding: dict[str, str], review: dict[str, Any], *, state: Any = None
) -> dict[str, Any]:
    """One follow-up row for a NON-BLOCKING finding (operator 2026-11-01).

    ``parent_job_id`` is the WORK row, never a review job: the finding rides
    with the work it came from, it never re-opens the grind loop and it never
    blocks a merge. The key is per (row, finding fingerprint, reviewed head) so
    the same finding is written once."""
    if review_queue.is_review_kind(row) or str(row.get("kind") or "").startswith(("review:", "triage:")):
        raise ValueError(f"refused follow-up: {row.get('id')} is not a work row")
    jid = str(row.get("id") or "")
    # The key carries the SEVERITY, which the stall fingerprint deliberately
    # does not: a medium and a low raised at the same place are two rows to
    # write, while a re-raised finding in the stall detector is still one.
    fingerprint = f"{finding_fingerprint(finding)}|{str(finding.get('severity') or '').strip().lower()}"
    digest = hashlib.sha1(fingerprint.encode("utf-8")).hexdigest()[:12]
    base_kind = str(row.get("kind") or "work")
    base_kind = re.sub(r"^(review|triage):", "", base_kind)
    head = str(review.get("head") or delivered_head(row) or "")
    lines = [
        f"FOLLOW-UP (non-blocking) from a cross-family review of work job {jid} on {head or '(unrecorded)'}.",
        f"[{finding['severity']}] {finding['location']} -- {finding['title']}",
    ]
    if finding.get("body"):
        lines.append(finding["body"])
    lines += [
        "",
        "This finding is medium/low: it did NOT block the merge and is recorded here so it is not "
        "lost. Fix it on its own branch; do not touch the reviewed branch for it.",
    ]
    return {
        "kind": f"followup:{base_kind}",
        "description": "\n".join(lines),
        "repo": row.get("repo"),
        "project": row.get("project"),
        "base_branch": row.get("base_branch"),
        "priority": 0,
        "parent_job_id": jid,
        "idempotency_key": (
            f"{FOLLOW_UP_KEY_PREFIX}:{jid}:{digest}:{head[:12] or 'nohead'}"
            f":{str(review.get('review_job_id') or '')[:12]}"
        ),
    }


def grind_spec(
    row: dict[str, Any],
    review: dict[str, Any],
    round_no: int,
    pick: dict[str, Any] | str,
    *,
    state: Any = None,
    kind: str = FIX,
    gate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """The fix (in place) or follow-up-review job for ``row``'s next round.

    A FIX job commits on the row's OWN branch (``head_branch``): the fix is a
    continuation commit on the branch under review, never a new branch, so the
    next round sees the delta. A follow-up REVIEW job is read-only on that
    branch. Raises ValueError for a review row: the parent is always the WORK
    row, never a review job."""
    if review_queue.is_review_kind(row) or str(row.get("kind") or "").startswith(("review:", "triage:")):
        raise ValueError(f"refused continuation: {row.get('id')} is not a work row")
    model, route = _job_route(pick)
    head = str(row.get("head_branch") or "").strip()
    if not head:
        raise ValueError(f"refused continuation: {row.get('id')} has no head_branch")
    root, _own = continuation_lineage(row, state)
    tip = delivered_head(row)
    is_fix = kind == FIX
    if is_fix:
        description = fix_brief(row, review, round_no, head=head)
    else:
        description = review_brief(row, review, head=head, gate=gate or {})
    spec: dict[str, Any] = {
        "kind": row.get("kind"),
        "description": description,
        "repo": row.get("repo"),
        "project": row.get("project"),
        "head_branch": head,
        # In place: the job checks the row's OWN branch out and commits onto
        # it, so the base of that work is the same branch (the worker only
        # consults base_branch when head_branch is absent upstream).
        "base_branch": head if is_fix else str(row.get("base_branch") or head),
        "eligible_kinds": list(row.get("eligible_kinds") or []),
        "preferred_models": [model],
        "max_cost_tier": _ceil_tier(row, route),
        "priority": row.get("priority") or 0,
        "parent_job_id": str(row.get("id") or ""),
        "idempotency_key": f"{GRIND_KEY_PREFIX}:{root}:{kind}:r{int(round_no)}:{(tip or 'nohead')[:12]}",
    }
    if not is_fix:
        spec["kind"] = f"review:{re.sub(r'^(review|triage):', '', str(row.get('kind') or 'work'))}"
    return _confine_to_codex(spec, route)


def continuation_spec(
    row: dict[str, Any], review: dict[str, Any], round_no: int, pick: dict[str, Any] | str, *, state: Any = None
) -> dict[str, Any]:
    """The continuation job for ``row`` (a NEW ``-rN`` branch cut from its head).

    Kept for the ``missing_branch``/``stranded_wip`` style repair path; the
    grind loop fixes IN PLACE and uses :func:`grind_spec`."""
    if review_queue.is_review_kind(row) or str(row.get("kind") or "").startswith(("review:", "triage:")):
        raise ValueError(f"refused continuation: {row.get('id')} is not a work row")
    model, route = _job_route(pick)
    base = str(row.get("head_branch") or "").strip()
    if not base:
        raise ValueError(f"refused continuation: {row.get('id')} has no head_branch")
    head = continuation_branch(row, round_no, state)
    root, _own = continuation_lineage(row, state)
    tip = delivered_head(row)
    spec: dict[str, Any] = {
        "kind": row.get("kind"),
        "description": continuation_brief(row, review, round_no, base=base, head=head),
        "repo": row.get("repo"),
        "project": row.get("project"),
        "head_branch": head,
        "base_branch": base,
        "eligible_kinds": list(row.get("eligible_kinds") or []),
        "preferred_models": [model],
        "max_cost_tier": _ceil_tier(row, route),
        "priority": row.get("priority") or 0,
        "parent_job_id": str(row.get("id") or ""),
        "idempotency_key": f"{CONTINUATION_KEY_PREFIX}:{root}:r{int(round_no)}:{(tip or 'nohead')[:12]}",
    }
    return _confine_to_codex(spec, route)


def _continuation_candidate(row: dict[str, Any]) -> bool:
    """Cheap, side-effect free: a needs-review WORK row the doctor may continue."""
    if not isinstance(row, dict) or str(row.get("status") or "") != "needs-review":
        return False
    if is_lane_b_owned(row) or review_queue.is_review_kind(row):
        return False
    kind = str(row.get("kind") or "")
    return not kind.startswith(("review:", "triage:")) and bool(str(row.get("head_branch") or "").strip())


def continue_one(
    row: dict[str, Any],
    *,
    state: Any,
    children: Callable[[dict], list[dict]],
    submit: Callable[[dict], tuple[int, Any]],
    escalate: Callable[[dict, str], None],
    roster: list[dict] | None = None,
    now: float | None = None,
    dry_run: bool = False,
    allow_submit: bool = True,
    head_is_verdict_only: Callable[[dict, dict, str, str], bool] | None = None,
    job_lookup: Callable[[str], dict | None] | None = None,
    gates: Callable[[str], dict[str, Any] | None] | None = None,
    integrity: Callable[..., dict[str, Any]] | None = None,
    add_followup: Callable[[dict], tuple[int, Any]] | None = None,
) -> tuple[Outcome, dict | None]:
    """Continue ONE row, or say why not. Returns ``(outcome, spec or None)``.

    Operator 2026-11-01: doctor reviews GRIND UNTIL GREEN with ALTERNATING
    FIX-IN-PLACE reviewers, under hard limits. This is the bounded loop that
    replaces "two automatic rounds then escalate":

    1. GREEN first: a STRONG-family structured APPROVE on the exact head, with
       the merged-tree suite and import preflight clean, ends the row and is
       recorded durably (``green_marker``).
    2. Non-blocking findings (medium/low) are recorded as follow-up rows
       (``parent_job_id`` = this work row) and never re-open the loop.
    3. A blocking finding (high/critical) gets a FIX job IN PLACE: a
       continuation commit on the row's own branch, from a family that is
       neither the author's nor a requesting reviewer's.
    4. A fix commit that fails the test-integrity guard fails the round and
       escalates once -- it is never merged.
    5. The same finding fingerprint surviving a fix is a stall: escalate once.
    6. ``GRIND_MAX_ROUNDS`` and the per-row spend cap escalate ONCE and stop.
    """
    now = time.time() if now is None else float(now)
    jid = str(row.get("id") or "")
    if not _continuation_candidate(row):
        return Outcome(jid, SKIPPED, "continuation", "not a needs-review work row"), None
    tip = delivered_head(row)
    gs = grind_state(row, state)
    # A row that already IS round N of a grind seeds its round count from its
    # lineage: the cap must hold for a row whose grind state was never written
    # (a restarted doctor, a dropped record), not just for one it watched.
    # ``rounds`` counts grind rounds already ATTEMPTED for this row. A row that
    # IS continuation round N was produced by the escalation round that
    # preceded it, so N-1 grind rounds have run: the next one is round N.
    # The root itself (round 0) starts at zero.
    _root, lineage_round = continuation_lineage(row, state)
    lineage_spent = max(int(lineage_round or 0) - 1, 0)
    if lineage_spent > int(gs.get("rounds") or 0):
        gs["rounds"] = lineage_spent
    # The row's own lineage round is what the CAP counts: continuation round N
    # is the Nth round of this grind, whether or not its grind state survived.
    gs["lineage_round"] = max(int(lineage_round or 0), int(gs.get("lineage_round") or 0))
    # ── 1. GREEN is objective: gates clean AND a strong approve on the head ──
    marker = green_marker(state, jid)
    if marker is not None and str(marker.get("head") or "") == tip:
        return Outcome(jid, NO_CHANGE, "continuation", f"green already recorded on {tip[:12]}"), None
    reviews = children(row)
    readings = [
        r
        for r in (read_review(row, review, tip=tip, head_is_verdict_only=head_is_verdict_only) for review in reviews)
        if r is not None
    ]
    if gates is not None:
        # A FIXER never approves its own commit: an approve from a family that
        # fixed this row is not green, however clean the gates are.
        fixer_families_now = {review_queue.family_of(str(m)) for m in (gs.get("fixers") or []) if m}
        for reading in readings:
            if reading.get("verdict") != "approve":
                continue
            if fixer_families_now & set(reading.get("families") or []):
                continue
            candidate = {**reading, "gates": gates(tip)}
            if green_blocker(candidate):
                continue
            if dry_run:
                return Outcome(jid, DRY_WOULD_APPLY, "continuation", f"would be green on {tip[:12]}"), None
            state.set_value(
                green_marker_key(jid, tip),
                {"head": tip, "round": int(gs.get("rounds") or 0), "at": now},
                ttl=MARK_TTL_SEC,
            )
            return Outcome(jid, GREEN, "continuation", f"green on {tip[:12]}"), None
    # ── 2. Only a BLOCKING finding continues the loop ─────────────────────────
    # A FIXER never reviews its own commit: drop any review whose families
    # include a family that fixed this row (operator 2026-11-01).
    fixer_families = {review_queue.family_of(str(m)) for m in (gs.get("fixers") or []) if m}
    if fixer_families:
        readings = [r for r in readings if not (set(r.get("families") or []) & fixer_families)]
    # ``best_review`` re-reads what it is given, so it must see ONLY the
    # surviving reviews: passing the raw ``reviews`` would put a fixer's back.
    # Review job ids are not unique in a live bus replay, so the pairing is
    # by family rather than by id.
    pool = reviews
    if fixer_families:
        pool = [
            review
            for review in reviews
            if not (
                set(
                    review_queue.family_of(m)
                    for m in _review_models(review, result_dict(review))
                    if review_queue.family_of(m)
                )
                & fixer_families
            )
        ]
    review = best_review(row, pool, head_is_verdict_only=head_is_verdict_only)
    # Findings come from EVERY review on the tip, not just the best one: a
    # medium from one reviewer and a low from another are two follow-up rows
    # (operator 2026-11-01), and a blocking finding anywhere still blocks.
    findings = [f for r in readings for f in (r.get("findings") or [])]
    blocking = blocking_findings(findings)
    soft = [f for f in findings if f not in blocking]
    recorded = 0
    if soft and add_followup is not None:
        # One follow-up row per (review, finding): two reviewers raising the
        # same soft finding are two rows, and a finding is written once.
        for reading in readings:
            for finding in reading.get("findings") or []:
                if finding in blocking:
                    continue
                spec = follow_up_spec(row, finding, reading, state=state)
                if dry_run or already_fixed(state, spec["idempotency_key"], now=now):
                    continue
                code, _resp = add_followup(spec)
                if code in (200, 201):
                    mark_fixed(state, spec["idempotency_key"], at=now)
                    recorded += 1
    if review is None or not blocking:
        if recorded:
            return (
                Outcome(
                    jid, FOLLOW_UP, "continuation", f"{recorded} non-blocking finding(s) recorded", attempts=recorded
                ),
                None,
            )
        return Outcome(jid, NO_CHANGE, "continuation", "no blocking finding on the tip"), None
    if not dry_run and recorded:
        log.info("continuation: job=%s recorded %d non-blocking finding(s)", jid[:12], recorded)
    if not _lineage_recorded(row, state):
        # Guard 1 (DeepSeek HIGH 2026-11-01): with the lineage record AND the
        # key both gone the fallback reads round 0, and a row that already IS a
        # continuation would start a fresh round-1 chain on every loss. A row
        # that shows any continuation marker is refused instead.
        refused = _refuse_unknown_lineage(row, state, escalate, now=now, dry_run=dry_run, job_lookup=job_lookup)
        if refused is not None:
            return refused, None
    # Idempotency is per (row, tip, ROUND): a second sweep on the same tip is
    # the same round and creates nothing, while the round after a fix (a NEW
    # tip) is a different round and must run.
    next_round = int(gs.get("rounds") or 0) + 1
    if already_fixed(state, continuation_key(jid, tip, next_round), now=now):
        return Outcome(jid, NO_CHANGE, "continuation", "already continued on this tip"), None
    # ── 3. A fix already committed on this head must pass test integrity ──────
    pending = gs.get("fix_pending")
    pending_head = str((pending or {}).get("head") or "") if isinstance(pending, dict) else ""
    if pending_head and integrity is not None:
        base_head = pending_head if pending_head != tip else str((pending or {}).get("base") or pending_head)
        check = integrity(str(row.get("repo") or ""), base=base_head, head=tip)
        if not (isinstance(check, dict) and check.get("ok")):
            why = (
                f"continuation: test_integrity -- the fix on {tip[:12]} changed tests it must not "
                f"({(check or {}).get('why') or 'unreadable'}); the round failed and is not merged"
            )
            key = continuation_escalation_key(jid)
            if dry_run or _marked_within(state, key, ESCALATE_TTL_SEC, now):
                return Outcome(jid, REJECTED, "continuation", "test_integrity", reason=why), None
            escalate(row, why)
            state.set_value(key, {"at": now}, ttl=MARK_TTL_SEC)
            return Outcome(jid, REJECTED, "continuation", "test_integrity", reason=why), None
    # ── 4. Stall: the same fingerprint survived a fix ─────────────────────────
    prints = finding_fingerprints(blocking)
    prior = [str(p) for p in (gs.get("fingerprints") or [])]
    if prints and set(prints) <= set(prior):
        why = (
            f"continuation: stall -- the same finding fingerprint survived the fix on {tip[:12]} "
            f"(round {int(gs.get('rounds') or 0)}): {', '.join(sorted(set(prints))[:3])}"
        )
        key = continuation_escalation_key(jid)
        if dry_run or _marked_within(state, key, ESCALATE_TTL_SEC, now):
            return Outcome(jid, STALLED, "continuation", "stall", reason=why), None
        escalate(row, why)
        state.set_value(key, {"at": now}, ttl=MARK_TTL_SEC)
        return Outcome(jid, STALLED, "continuation", "stall", reason=why), None
    # ── 5. Hard limits: rounds, then spend -- escalate ONCE and stop ──────────
    spent = int(gs.get("rounds") or 0)
    limit = _grind_limit_reason(jid, state, max(spent, int(gs.get("lineage_round") or 0)), now)
    if limit is not None:
        why = limit
        key = continuation_escalation_key(jid)
        if dry_run or _marked_within(state, key, ESCALATE_TTL_SEC, now):
            return Outcome(jid, ESCALATED, "continuation", "limit reached", reason=why), None
        escalate(row, why)
        state.set_value(key, {"at": now}, ttl=MARK_TTL_SEC)
        return Outcome(jid, ESCALATED, "continuation", "limit reached", reason=why), None
    # ── 6. The next round: a fixer, or the review of that fix ─────────────────
    fixers = [str(f) for f in (gs.get("fixers") or [])]
    round_no = spent + 1
    if not pending_head:
        pick = fixer_pick(row, review, fixers=fixers, round_no=round_no, roster=roster)
        kind, missing = FIX, "no cross-family fixer"
    else:
        pick = _reviewer_pick(row, review, fixers=fixers, roster=roster)
        kind, missing = REVIEW, "no strong reviewer family outside the fixer's"
    if pick is None:
        why = f"continuation: {missing} for round {round_no} on {tip[:12]}"
        key = continuation_escalation_key(jid)
        if dry_run or _marked_within(state, key, ESCALATE_TTL_SEC, now):
            return Outcome(jid, SKIPPED, "continuation", "escalated already", reason=why), None
        escalate(row, why)
        state.set_value(key, {"at": now}, ttl=MARK_TTL_SEC)
        return Outcome(jid, ESCALATED, "continuation", missing, reason=why), None
    if not allow_submit:
        return Outcome(jid, "deferred", "continuation", f"per-sweep cap ({CONTINUATION_MAX_PER_SWEEP})"), None
    # Guard 2: the global hourly brake, independent of every per-row record.
    if continuations_last_hour(state, now) >= CONTINUE_MAX_PER_HOUR:
        return Outcome(jid, THROTTLED, "continuation", f"hourly brake ({CONTINUE_MAX_PER_HOUR} per hour)"), None
    spec = grind_spec(row, review, round_no, pick, state=state, kind=kind)
    spec.update(_confine_to_codex(spec, _job_route(pick)[1]))
    model = _job_route(pick)[0]
    detail = f"round {round_no} {kind} on {model}: {spec['head_branch']}"
    if dry_run:
        return Outcome(jid, DRY_WOULD_APPLY, "continuation", detail), spec
    # Guard 3: nothing is created unless the store is durable, before AND after
    # the brake's own write -- a counter that did not persist is no brake.
    if not state_durable(state):
        return Outcome(jid, REFUSED, "continuation", DURABLE_STATE_UNAVAILABLE), None
    _reserve_hourly(state, now)
    if not state_durable(state):
        return Outcome(jid, REFUSED, "continuation", DURABLE_STATE_UNAVAILABLE), None
    code, resp = submit(spec)
    new_id = resp.get("id") if isinstance(resp, dict) else None
    if code not in (200, 201) or not new_id:
        return Outcome(jid, FAILED, "continuation", detail, error=f"submit http={code}"), spec
    mark_fixed(state, continuation_key(jid, tip, round_no), at=now)
    state.set_value(_lineage_key(str(new_id)), {"root": _grind_root(row, state), "round": round_no}, ttl=MARK_TTL_SEC)
    # Rounds ATTEMPTED. The round just submitted is round ``round_no``; the
    # next sweep asks for ``round_no + 1``.
    gs["rounds"] = round_no
    # ``rounds`` is rounds ATTEMPTED; the next sweep asks for ``rounds + 1``.
    gs["fingerprints"] = sorted(set(prints) | set(prior))
    if kind == FIX:
        gs["fix_pending"] = {"head": tip, "round": round_no}
        gs["fixers"] = sorted({*fixers, model})
    else:
        gs["reviewed"] = sorted({*[str(h) for h in (gs.get("reviewed") or [])], tip})
        gs["fix_pending"] = None
    save_grind_state(jid, state, gs, now=now)
    return Outcome(jid, APPLIED, "continuation", f"{detail} (job {str(new_id)[:12]})", attempts=round_no), spec


def _grind_root(row: dict[str, Any], state: Any) -> str:
    """The root work row of ``row`` (itself when it is not a continuation)."""
    return continuation_lineage(row, state)[0]


def _grind_limit_reason(job_id: str, state: Any, spent: int, now: float) -> str | None:
    """Why the row's grind must stop and escalate, or None while it may run."""
    if spent >= GRIND_MAX_ROUNDS:
        return (
            f"continuation: grind cap -- {spent} round(s) spent (max {GRIND_MAX_ROUNDS}, "
            f"HIVE_DOCTOR_GRIND_MAX_ROUNDS); escalation with the evidence"
        )
    used = grind_spend(state, job_id)
    if used >= GRIND_MAX_SPEND_USD:
        return f"continuation: spend cap -- ${used:.2f} spent (max ${GRIND_MAX_SPEND_USD:.2f}); escalation"
    return None


def _refuse_unknown_lineage(
    row: dict[str, Any],
    state: Any,
    escalate: Callable[[dict, str], None],
    *,
    now: float,
    dry_run: bool,
    job_lookup: Callable[[str], dict | None] | None,
) -> Outcome | None:
    """The refusal for a continuation whose lineage is lost, or None for a root.

    Escalated (and logged) once per ``ESCALATE_TTL_SEC``; an unreadable parent
    chain creates nothing and escalates nothing (the next sweep reads again)."""
    jid = str(row.get("id") or "")
    try:
        evidence = continuation_evidence(row, state, job_lookup=job_lookup)
    except LineageLookupError as exc:
        return Outcome(jid, REFUSED, "continuation", "lineage_lookup_failed", error=str(exc))
    if not evidence:
        return None
    why = (
        f"continuation: {LINEAGE_UNKNOWN} -- {jid[:12]} is a continuation ({evidence}) but neither its "
        "lineage record nor its idempotency key survives; refusing to start a new round-1 chain"
    )
    key = lineage_unknown_key(jid)
    if dry_run or _marked_within(state, key, ESCALATE_TTL_SEC, now):
        return Outcome(jid, REFUSED, "continuation", LINEAGE_UNKNOWN, reason=why)
    escalate(row, why)
    state.set_value(key, {"at": now}, ttl=MARK_TTL_SEC)
    log.warning(why)
    return Outcome(jid, REFUSED, "continuation", LINEAGE_UNKNOWN, reason=why)


def continuation_sweep(
    rows: list[dict[str, Any]],
    *,
    state: Any,
    children: Callable[[dict], list[dict]],
    submit: Callable[[dict], tuple[int, Any]],
    escalate: Callable[[dict, str], None],
    roster: list[dict] | None = None,
    now: float | None = None,
    dry_run: bool | None = None,
    enabled: bool | None = None,
    max_per_sweep: int | None = None,
    head_is_verdict_only: Callable[[dict, dict, str, str], bool] | None = None,
    job_lookup: Callable[[str], dict | None] | None = None,
    gates: Callable[[str], dict[str, Any] | None] | None = None,
    integrity: Callable[..., dict[str, Any]] | None = None,
    add_followup: Callable[[dict], tuple[int, Any]] | None = None,
) -> dict[str, Any]:
    """One pass of the continuation loop over ``rows`` (oldest first, capped).

    ``HIVE_DOCTOR_REMEDIATE=0`` (``enabled``) stops it before any read; a dry
    run submits, marks and escalates nothing and lists what it would submit.
    A store that is not durable (``loaded_ok``/``persist_ok``) refuses the pass
    before any read, dry run or not. One bad row never stops the pass."""
    enabled = REMEDIATE_ENABLED if enabled is None else bool(enabled)
    dry_run = DRY_RUN if dry_run is None else bool(dry_run)
    cap = CONTINUATION_MAX_PER_SWEEP if max_per_sweep is None else int(max_per_sweep)
    summary: dict[str, Any] = {"enabled": enabled, "dry_run": dry_run, "capped": cap, APPLIED: 0, "scanned": 0}
    if not enabled:
        return summary
    if not state_durable(state):
        # Every guard on this path is a durable record; without the store the
        # doctor would re-create the same continuations on every sweep.
        summary["refused"] = DURABLE_STATE_UNAVAILABLE
        log.error("continuation refused: durable state unavailable (loaded_ok/persist_ok false)")
        return summary
    now = time.time() if now is None else float(now)
    ordered = sorted(
        [r for r in rows or [] if _continuation_candidate(r)],
        key=lambda r: (r.get("ended_at") or r.get("last_update_at") or r.get("started_at") or 0, str(r.get("id"))),
    )
    summary["scanned"] = len(ordered)
    used = 0
    handled: set[str] = set()
    for row in ordered:
        jid = str(row.get("id") or "")
        if jid in handled:
            continue
        handled.add(jid)
        try:
            outcome, spec = continue_one(
                row,
                state=state,
                children=children,
                submit=submit,
                escalate=escalate,
                roster=roster,
                now=now,
                dry_run=dry_run,
                allow_submit=used < cap,
                head_is_verdict_only=head_is_verdict_only,
                job_lookup=job_lookup,
                gates=gates,
                integrity=integrity,
                add_followup=add_followup,
            )
        except Exception as exc:  # noqa: BLE001 -- one row must not stop the sweep
            summary["errors"] = summary.get("errors", 0) + 1
            log.error("continuation failed job=%s: %s", jid[:12], exc)
            continue
        if spec is not None and outcome.status in (APPLIED, DRY_WOULD_APPLY, FAILED):
            used += 1
        if outcome.status == DRY_WOULD_APPLY and spec is not None:
            summary.setdefault("would_submit", []).append(
                {k: spec.get(k) for k in ("parent_job_id", "head_branch", "base_branch", "preferred_models")}
            )
        if outcome.status == FOLLOW_UP and outcome.attempts:
            # One row can record SEVERAL non-blocking findings: the count is
            # the findings written, not the rows that wrote them.
            summary[FOLLOW_UP] = summary.get(FOLLOW_UP, 0) + int(outcome.attempts)
        else:
            summary[outcome.status] = summary.get(outcome.status, 0) + 1
        if outcome.status == REFUSED and outcome.detail == LINEAGE_UNKNOWN:
            # Logged once, with its escalation (``_refuse_unknown_lineage``);
            # a refusal repeated every sweep is not news.
            log.debug(outcome.as_log())
        else:
            log.info(outcome.as_log())
    if summary.get(THROTTLED):
        log.warning(
            "continuation: hourly brake reached (%d per hour, HIVE_DOCTOR_CONTINUE_MAX_PER_HOUR); %d row(s) skipped",
            CONTINUE_MAX_PER_HOUR,
            summary[THROTTLED],
        )
    return summary
