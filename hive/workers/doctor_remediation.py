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

import json
import logging
import os
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
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
        return _review_job_outcome(job, cause, state=state, escalate=escalate, job_lookup=job_lookup, now=now)
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
        summary[outcome.status] = summary.get(outcome.status, 0) + 1
        summary.setdefault("causes", {})
        if outcome.cause:
            summary["causes"][outcome.cause] = summary["causes"].get(outcome.cause, 0) + 1
        log.info(outcome.as_log())
    return summary
