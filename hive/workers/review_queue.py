"""Durable review queue (operator rule 2026-10-31).

The rule is two sentences: *always have an alternate review protocol or a
review queue*, and *always pair reviewer and author from different model
families*.

Before this module a loop whose reviewer endpoint was busy ended
``reviewer_unavailable`` and the job FAILED -- on the night of 2026-10-30 all
8 slots of the CERBERUS ``gpt-oss-20b`` endpoint were busy and about 20 loops
failed for no reason of their own. A ticket that cannot be reviewed yet is not
failed work: the branch exists, it is pushed, and it needs a reviewer.

This module owns the state machine (pure functions, no I/O except the cached
load probe):

``queued``      awaiting a cross-family reviewer; ``next_attempt_at`` says when
``submitted``   a review job was dispatched to ``route_id``
``approved``    a STRUCTURED verdict naming ``reviewers[].model`` approved
``parked``      no cross-family reviewer is available -> a human's call
``exhausted``   ``max_attempts`` dispatches produced no usable verdict

The queue is durable because it lives in the row's own ``result``
(``result.awaiting_review``), alongside ``status``: the bus has no
``awaiting-review`` status, and a row the doctor already understands
(``needs-review``) keeps every existing guard (never auto-purged, protected
lanes, supersession). ``result.awaiting_review.state`` is what tells the
doctor -- and the dashboard -- that the row is QUEUED work, not stale.

A prose-only, empty, truncated, 503 or non-JSON review is NEVER an approval:
only a structured JSON object with ``reviewers[].model`` counting does.
"""

from __future__ import annotations

import json
import random
import re
import threading
import time
import urllib.request
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from hive.workers import reviewer_roster as rr

#: The result key that marks a row as queued for review.
FIELD = "awaiting_review"
STATE_QUEUED = "queued"
STATE_SUBMITTED = "submitted"
STATE_APPROVED = "approved"
STATE_PARKED = "parked"
STATE_EXHAUSTED = "exhausted"
STATES = frozenset({STATE_QUEUED, STATE_SUBMITTED, STATE_APPROVED, STATE_PARKED, STATE_EXHAUSTED})
#: States the doctor treats as queued work (a sweep must not call them stale).
ACTIVE_STATES = frozenset({STATE_QUEUED, STATE_SUBMITTED, STATE_PARKED})

DEFAULT_BASE_SECONDS = 30.0
DEFAULT_MAX_SECONDS = 900.0
DEFAULT_JITTER = 0.2
DEFAULT_MAX_ATTEMPTS = 6
#: The load probe's cache lifetime (operator: cheap, cached for 30 s).
LOAD_CACHE_SECONDS = 30.0
LOAD_TIMEOUT_SECONDS = 2.0

_PLACEHOLDERS = frozenset({"", "auto", "unknown", "reviewer", "none", "null"})


# ---------------------------------------------------------------------------
# (4a) record an unreviewed delivery as awaiting-review, never failed
# ---------------------------------------------------------------------------


def _named_reviewer(value: Any) -> bool:
    text = str(value or "").strip()
    return bool(text) and text.lower() not in _PLACEHOLDERS and not text.startswith("(")


def _evidence_has_named_reviewer(result: Mapping) -> bool:
    """True when the row already carries a review from a NAMED model."""
    evidence = result.get("review_evidence")
    if (
        isinstance(evidence, Mapping)
        and evidence.get("reviewers")
        and any(isinstance(e, Mapping) and _named_reviewer(e.get("model")) for e in evidence["reviewers"])
    ):
        return True
    reviewer = result.get("reviewer")
    return isinstance(reviewer, Mapping) and _named_reviewer(reviewer.get("model"))


class AwaitingReview:
    """How the worker should record a delivery that needs a review."""

    __slots__ = ("status", "failure_reason", "result")

    def __init__(self, status: str, failure_reason: str | None, result: dict):
        self.status = status
        self.failure_reason = failure_reason
        self.result = result


def awaiting_review_record(
    *,
    status: str,
    failure_reason: str | None,
    result: dict,
    has_commits: bool,
    pushed: bool,
    branch: str | None = None,
    author_model: str | None = None,
    now: float | None = None,
) -> AwaitingReview | None:
    """The rewrite that turns a reviewer outage into queued work, or None.

    Applies when, and only when, the delivery COMMITTED and PUSHED work and
    the row carries no review from a named model. A ``done`` row that already
    carries a named review is left untouched (never downgraded). Without
    commits there is nothing to review, and without a push a reviewer could
    not see the head -- both return None.
    """
    if not has_commits or not pushed:
        return None
    result = result if isinstance(result, dict) else {}
    if _evidence_has_named_reviewer(result):
        return None
    head = str(result.get("remote_sha") or result.get("head_sha") or "").strip()
    entry = {
        "state": STATE_QUEUED,
        "head": head,
        "head_branch": branch or result.get("head_branch"),
        "author_model": author_model,
        "author_family": rr.family_of(author_model) if author_model else None,
        "attempts": 0,
        "next_attempt_at": float(now or 0.0),
        "last_error": "reviewer_unavailable"
        if failure_reason in (None, "", "reviewer_unavailable")
        else str(failure_reason),
    }
    queued = dict(result)
    queued[FIELD] = entry
    queued.pop("failure_reason", None)
    return AwaitingReview("needs-review", None, queued)


# ---------------------------------------------------------------------------
# (4b) only a structured verdict naming a reviewer counts
# ---------------------------------------------------------------------------


def parse_structured_verdict(payload: Any) -> tuple[dict | None, str | None]:
    """``(verdict, failure_reason)`` for a review payload; the reason is None on success.

    Success means a JSON OBJECT with a verdict and at least one
    ``reviewers[]`` entry naming a model (not a placeholder). Everything else
    is refused with one of ``empty``, ``prose_only``, ``truncated``,
    ``non_json`` or ``no_named_reviewer`` -- and a refused payload is NEVER an
    approval, whatever words it contains.
    """
    if payload is None:
        return None, "empty"
    if isinstance(payload, Mapping):
        data: Any = dict(payload)
    else:
        text = str(payload).strip()
        if not text:
            return None, "empty"
        if not text.startswith(("{", "[")):
            return None, "prose_only"
        try:
            data = json.loads(text)
        except (ValueError, RecursionError):
            # A JSON-shaped payload that does not parse is truncated or
            # malformed; either way it is not a verdict.
            return None, "truncated" if text.startswith("{") else "non_json"
    if not isinstance(data, dict):
        return None, "non_json"
    if not str(data.get("verdict") or "").strip():
        return None, "non_json"
    reviewers = data.get("reviewers")
    named = [
        r
        for r in (reviewers if isinstance(reviewers, list) else [])
        if isinstance(r, Mapping) and _named_reviewer(r.get("model"))
    ]
    if not named:
        return None, "no_named_reviewer"
    return data, None


def review_counts_as_approval(payload: Any, author_family: str | None = None) -> bool:
    """True only for a structured approve from a named, CROSS-FAMILY reviewer."""
    verdict, reason = parse_structured_verdict(payload)
    if verdict is None or reason is not None:
        return False
    if str(verdict.get("verdict") or "").strip().lower().replace("-", "_") != "approve":
        return False
    families = {str(r.get("family") or rr.family_of(r.get("model"))).strip().lower() for r in verdict["reviewers"]}
    if author_family:
        fam = str(author_family).strip().lower()
        if fam in families or not any(rr.families_pair(f, fam) for f in families):
            return False
    return True


# ---------------------------------------------------------------------------
# (4c) backoff + per-route concurrency
# ---------------------------------------------------------------------------


def retry_delay(
    attempt: int,
    *,
    base: float = DEFAULT_BASE_SECONDS,
    cap: float = DEFAULT_MAX_SECONDS,
    jitter: float = DEFAULT_JITTER,
    rand: Callable[[], float] = random.random,
) -> float:
    """Exponential backoff for the ``attempt``-th retry (0-based), capped.

    ``base * 2**attempt`` rounded down at ``cap``, then scaled by a symmetric
    jitter in ``[1 - jitter, 1 + jitter]``. Monotonic for ``jitter=0`` so a
    schedule is reproducible in tests.
    """
    raw = min(float(base) * (2 ** max(0, int(attempt))), float(cap))
    if jitter <= 0:
        return raw
    factor = 1.0 + (rand() * 2.0 - 1.0) * float(jitter)
    return min(float(cap), raw * factor)


class RouteInflight:
    """How many review calls are in flight per ENDPOINT, capped by the roster.

    Routes sharing a ``base_url`` share one budget (the CERBERUS reviewer and
    narrator role ids front the same 8-slot llama-server), so the cap is
    ``route_capacity`` -- never more than the configured concurrency to one
    route.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[str, int] = {}
        # A submission handed to the bus keeps its slot until the route's
        # count for this queue is reconciled (the review job finished); the
        # ``hold`` value is the count AT submission, so subtraction can never
        # collide with a slot another caller took in the meantime.
        self._holds: dict[str, int] = {}

    @staticmethod
    def _key(route: rr.Route) -> str:
        return route.base_url or route.id

    def count(self, route: rr.Route) -> int:
        return self._counts.get(self._key(route), 0)

    def acquire(
        self, route: rr.Route, roster: rr.Roster | Mapping[str, rr.Route] | Iterable[rr.Route] | None = None
    ) -> bool:
        key = self._key(route)
        capacity = rr.route_capacity(route, roster)
        with self._lock:
            current = self._counts.get(key, 0)
            if current >= capacity:
                return False
            self._counts[key] = current + 1
            return True

    def hold(self, route: rr.Route) -> None:
        """Reserve this route against the next sweep.

        An asynchronous review keeps running after the consumer submits it, so
        the in-flight count must survive ``drain`` returning; otherwise the next
        sweep would hand every slot out again. The hold remembers the endpoint
        count at submission and is reconciled (``reset``) or given back
        (``release_hold``) once the review job is terminal.
        """
        key = self._key(route)
        with self._lock:
            self._holds[key] = self._counts.get(key, 0)

    def release_hold(self, route: rr.Route) -> None:
        """Give back the slots this queue still holds for ``route``'s endpoint."""
        key = self._key(route)
        with self._lock:
            held = self._holds.pop(key, None)
            if held:
                self._counts[key] = max(0, self._counts.get(key, 0) - held)
            elif self._counts.get(key, 0):
                self._counts[key] = 0
            self._counts.pop(key, None)

    def reset(self) -> None:
        """Drop every count and hold (used when the queue state is reloaded)."""
        with self._lock:
            self._counts.clear()
            self._holds.clear()

    def release(self, route: rr.Route) -> None:
        key = self._key(route)
        with self._lock:
            current = self._counts.get(key, 0)
            if current <= 1:
                self._counts.pop(key, None)
            else:
                self._counts[key] = current - 1


# ---------------------------------------------------------------------------
# (5) load awareness: llama-server /slots, vLLM /metrics, cached 30 s
# ---------------------------------------------------------------------------


def _default_http(url: str, timeout: float) -> Any:
    with urllib.request.urlopen(url, timeout=timeout) as response:  # noqa: S310 - LAN/inventory URL
        return _RawResponse(response.read(), getattr(response, "status", 200))


class _RawResponse:
    def __init__(self, body: bytes, status: int):
        self.body = body
        self.status = status

    def json(self) -> Any:
        return json.loads(self.body.decode("utf-8", "replace"))

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", "replace")


_VLLM_RUNNING_RE = re.compile(r"^vllm:num_requests_running\S*\s+([0-9.]+)", re.MULTILINE)


class LoadProbe:
    """Cached load reader for one route (cheap; cached for ``LOAD_CACHE_SECONDS``)."""

    def __init__(self, http: Callable[[str, float], Any] | None = None, clock: Callable[[], float] | None = None):
        self._http = http or _default_http
        self._clock = clock or time.time
        self._lock = threading.Lock()
        self._cache: dict[str, tuple[float, int | None, str]] = {}

    def _base(self, url: str) -> str:
        base = url.rstrip("/")
        if base.endswith("/v1"):
            base = base[: -len("/v1")]
        return base

    def route_kind(self, route: rr.Route) -> str:
        kind = self._read(route)[2]
        return kind

    def _read(self, route: rr.Route) -> tuple[float, int | None, str]:
        now = float(self._clock())
        with self._lock:
            cached = self._cache.get(route.id)
        # A cache entry lives for LOAD_CACHE_SECONDS and expires STRICTLY after
        # that: at exactly 30 s the probe asks again (a slot freed at the
        # boundary must be visible, not hidden behind a stale count).
        if cached is not None and now - cached[0] <= LOAD_CACHE_SECONDS:
            return cached
        value, kind = self._fetch(route)
        with self._lock:
            self._cache[route.id] = (now, value, kind)
        return now, value, kind

    def _fetch(self, route: rr.Route) -> tuple[int | None, str]:
        if route.location != "local" or not route.base_url:
            return None, "remote"
        base = self._base(route.base_url)
        # 1. llama-server /slots: count the slots that are processing.
        try:
            payload = self._http(f"{base}/slots", LOAD_TIMEOUT_SECONDS).json()
            if isinstance(payload, list):
                busy = 0
                for slot in payload:
                    if not isinstance(slot, dict):
                        continue
                    if slot.get("is_processing") is True or str(slot.get("state") or "").lower() in {
                        "processing",
                        "generating",
                    }:
                        busy += 1
                return busy, "llama_slots"
        except Exception:  # noqa: BLE001 - fall through to metrics; unknown is not zero
            pass
        # 2. vLLM /metrics: num_requests_running.
        try:
            response = self._http(f"{base}/metrics", LOAD_TIMEOUT_SECONDS)
            text = response.text if hasattr(response, "text") else str(response)
            match = _VLLM_RUNNING_RE.search(text)
            if match:
                return int(float(match.group(1))), "vllm_metrics"
        except Exception:  # noqa: BLE001
            pass
        return None, "unknown"

    def load(self, route: rr.Route) -> int | None:
        """In-flight requests on ``route``'s endpoint, or None when unknowable."""
        return self._read(route)[1]

    def loads(self, routes: Iterable[rr.Route]) -> dict[str, int]:
        """``{route id: load}`` for every route whose load could be read."""
        out: dict[str, int] = {}
        for route in routes:
            value = self.load(route)
            if value is not None:
                out[route.id] = value
        return out


# ---------------------------------------------------------------------------
# (4d) the queue state machine
# ---------------------------------------------------------------------------


def entry_of(job: Mapping | None) -> dict | None:
    """The row's ``result.awaiting_review`` entry, or None.

    Accepts the whole job (``{"result": {...}}``) or, for callers that already
    unwrapped it (the doctor's ``_job_result_dict``), the result mapping itself.
    """
    if not isinstance(job, Mapping):
        return None
    result = job.get("result")
    holder = result if isinstance(result, Mapping) else job
    entry = holder.get(FIELD)
    return entry if isinstance(entry, dict) else None


def is_awaiting_review(job: Mapping | None) -> bool:
    """True for a row the queue still owes a review (queued/submitted/parked)."""
    entry = entry_of(job)
    return bool(entry) and str(entry.get("state") or "").lower() in ACTIVE_STATES


def mark_attempt_failed(
    job: dict,
    error: str,
    *,
    now: float,
    base: float = DEFAULT_BASE_SECONDS,
    cap: float = DEFAULT_MAX_SECONDS,
    jitter: float = DEFAULT_JITTER,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    rand: Callable[[], float] = random.random,
) -> dict:
    """Record a failed dispatch/parse attempt and schedule the next try."""
    result = job.setdefault("result", {})
    entry = dict(result.get(FIELD) or {})
    attempts = int(entry.get("attempts") or 0) + 1
    entry["attempts"] = attempts
    entry["last_error"] = str(error)[:200]
    if attempts >= max_attempts:
        entry["state"] = STATE_EXHAUSTED
    else:
        entry["state"] = STATE_QUEUED
        entry["next_attempt_at"] = float(now) + retry_delay(attempts - 1, base=base, cap=cap, jitter=jitter, rand=rand)
    result[FIELD] = entry
    return entry


def mark_submitted(job: dict, route: rr.Route | str, review_job_id: str, *, now: float | None = None) -> dict:
    """Record that a review job was dispatched for this row."""
    result = job.setdefault("result", {})
    entry = dict(result.get(FIELD) or {})
    entry["state"] = STATE_SUBMITTED
    entry["route_id"] = route if isinstance(route, str) else route.id
    entry["reviewer_model"] = None
    entry["review_job_id"] = review_job_id
    entry["submitted_at"] = float(now or 0.0)
    entry.pop("last_error", None)
    result[FIELD] = entry
    return entry


def mark_parked(job: dict, reason: str = "no_cross_family_reviewer") -> dict:
    """No cross-family reviewer is available: park the row for a human."""
    result = job.setdefault("result", {})
    entry = dict(result.get(FIELD) or {})
    entry["state"] = STATE_PARKED
    entry["last_error"] = reason
    result[FIELD] = entry
    return entry


def apply_verdict(
    job: dict,
    payload: Any,
    *,
    author_family: str | None = None,
    now: float = 0.0,
    base: float = DEFAULT_BASE_SECONDS,
    cap: float = DEFAULT_MAX_SECONDS,
    jitter: float = DEFAULT_JITTER,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    rand: Callable[[], float] = random.random,
) -> dict:
    """Apply a review payload to the queue entry.

    A structured verdict (any verdict word) names the reviewer and completes
    the queue entry -- ``approved`` only for an approve from a cross-family
    reviewer. Anything else (prose, empty, truncated, 503, no named reviewer)
    never counts: the attempt is recorded as FAILED and retried with backoff.
    """
    result = job.setdefault("result", {})
    entry = dict(result.get(FIELD) or {})
    fam = author_family or entry.get("author_family")
    verdict, reason = parse_structured_verdict(payload)
    if verdict is not None:
        first = verdict["reviewers"][0]
        entry["reviewer_model"] = str(first.get("model"))
        entry["reviewer_family"] = str(first.get("family") or rr.family_of(first.get("model")))
        entry["verdict"] = str(verdict.get("verdict") or "").strip().lower()
        entry["state"] = STATE_APPROVED if review_counts_as_approval(verdict, fam) else STATE_PARKED
        if entry["state"] == STATE_PARKED and not review_counts_as_approval(verdict, fam):
            entry["last_error"] = "same_family_or_non_approve_verdict"
        else:
            entry.pop("last_error", None)
        result[FIELD] = entry
        return entry
    return mark_attempt_failed(
        job,
        reason or "no_verdict",
        now=now,
        base=base,
        cap=cap,
        jitter=jitter,
        max_attempts=max_attempts,
        rand=rand,
    )


def drain(
    rows: Iterable[dict],
    *,
    roster: rr.Roster | None = None,
    health: Mapping[str, str] | None = None,
    load: Mapping[str, int] | None = None,
    now: float,
    submit: Callable[[dict, rr.Route, str], str | None],
    inflight: RouteInflight | None = None,
    max_per_sweep: int | None = None,
) -> dict:
    """Submit queued rows to the next cross-family reviewer (mutates them in place).

    Rows whose ``next_attempt_at`` has not arrived are ``deferred``; a row with
    no cross-family route is ``parked`` (never self-reviewed); a route at its
    cap is skipped and the row takes the next one or waits. ``submit(row,
    route, head)`` returns the review job id, or a falsy value when the bus
    refused -- that counts as a failed attempt (recorded, backed off).
    """
    roster = roster if roster is not None else rr.load_roster()
    inflight = inflight if inflight is not None else RouteInflight()
    summary = {"scanned": 0, "submitted": 0, "parked": 0, "deferred": 0, "failed": 0, "skipped": 0}
    for row in rows:
        entry = entry_of(row)
        if entry is None or str(entry.get("state") or "").lower() not in ACTIVE_STATES:
            summary["skipped"] += 1
            continue
        summary["scanned"] += 1
        if str(entry.get("state") or "").lower() == STATE_PARKED:
            summary["skipped"] += 1
            continue
        if max_per_sweep is not None and summary["submitted"] >= max_per_sweep:
            summary["deferred"] += 1
            continue
        if float(entry.get("next_attempt_at") or 0.0) > float(now):
            summary["deferred"] += 1
            continue
        head = str(entry.get("head") or "").strip()
        candidates = rr.pick_reviewers(
            str(entry.get("author_model") or ""),
            roster=roster,
            health=health,
            load=load,
            author_family=entry.get("author_family"),
            repo=row.get("repo"),
        )
        picked = None
        for route in candidates:
            if inflight.acquire(route, roster):
                picked = route
                break
        if picked is None:
            if not candidates:
                mark_parked(row)
                summary["parked"] += 1
            else:
                summary["deferred"] += 1
            continue
        try:
            review_job_id = submit(row, picked, head)
        except Exception as exc:  # noqa: BLE001 - one row must not stop the queue
            inflight.release(picked)
            mark_attempt_failed(row, f"submit_error: {exc}", now=now)
            summary["failed"] += 1
            continue
        if review_job_id:
            mark_submitted(row, picked, str(review_job_id), now=now)
            # The review runs on the route after this sweep returns: keep the
            # slot until it is terminal so the next sweep cannot oversubscribe.
            inflight.hold(picked)
            summary["submitted"] += 1
        else:
            inflight.release(picked)
            mark_attempt_failed(row, "submit_refused", now=now)
            summary["failed"] += 1
    return summary
