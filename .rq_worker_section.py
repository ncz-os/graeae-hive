# ── review queue inside the triage queue: the WORKER side (operator 2026-10-31)
# The review queue IS the triage queue: the Hive job rows in needs-review the
# doctor's needs-review sweep already reads. When a loop finishes authoring and
# its reviewer cannot answer (busy endpoint, timeout, prose-only, empty,
# truncated or 503 output) the worker must NOT fail the job: it pushes the
# branch and leaves the row needs-review with
# result.review_state.state='awaiting_review', naming the author, its family,
# the tried reviewers and their failure kinds. The doctor's sweep then pairs
# the row with a CROSS-FAMILY reviewer from hive/core/reviewer-roster.json.
#
# The family table, the roster and the pairing function live in
# hive/review_queue.py: ONE data file and one implementation shared by the
# worker and the bus lines. These are thin seams over it.


def review_queue_roster() -> list:
    """The reviewer roster (every reviewer-capable route), from the one data file."""
    return review_queue.load_roster()


def review_queue_reviewers(
    author_model: str,
    *,
    health=None,
    load=None,
    co_authors: list | None = None,
    repo: str | None = None,
    author_routes: list | None = None,
) -> list:
    """Ordered CROSS-FAMILY reviewers for ``author_model``; ``[]`` means PARK.

    An empty list is a verdict, not an error: with no cross-family reviewer
    available the row is parked awaiting_review -- never self-reviewed.
    """
    return review_queue.pick_reviewers(
        author_model,
        review_queue_roster(),
        health=health,
        load=load,
        co_authors=co_authors,
        repo=repo,
        author_routes=author_routes,
    )


#: Error substrings that name WHY a reviewer route could not answer. Ordered
#: most-specific first; the first match wins.
_REVIEWER_FAILURE_PATTERNS = (
    ("review_http_503", ("answered 503", "http 503", " 503")),
    ("review_truncated", ("truncat", "max_tokens", "finish_reason: length")),
    ("review_timeout", ("timed out", "timeout")),
    ("review_prose_only", ("prose", "not a verdict", "non-json", "no json")),
    ("review_empty", ("empty", "no review chunks")),
    ("review_busy", ("busy", "no review chunks", "all healthy endpoints failed")),
)


def reviewer_failure_kind(evidence: dict | None, *, author_model: str = "", head_sha: str | None = None) -> str | None:
    """The structured failure kind of a reviewer attempt, or None on success.

    None means the reviewer produced a STRUCTURED verdict for this head: only a
    JSON ``reviewers`` list naming its model(s) counts. A prose-only, empty,
    truncated, 503 or non-JSON review is a failure kind, NEVER an approval.
    """
    payload = evidence if isinstance(evidence, dict) else {}
    if review_queue.has_structured_verdict(payload, head_sha=head_sha):
        return None
    haystack = " ".join(
        str(payload.get(key) or "") for key in ("error", "reason", "verdict", "detail", "message")
    ).lower()
    for kind, needles in _REVIEWER_FAILURE_PATTERNS:
        if any(needle in haystack for needle in needles):
            return kind
    # No structured verdict and no recognisable cause: the safe reading of a
    # reviewer that answered nothing usable is a busy route, which retries.
    return "review_busy"


def awaiting_review_state(
    author_model: str,
    tried: list | None = None,
    *,
    attempt: int = 0,
    next_attempt_at: float | None = None,
    now: float | None = None,
    author_family: str | None = None,
    parked_reason: str | None = None,
    last_failure: str | None = None,
) -> dict:
    """The ``result.review_state`` block the worker writes instead of failing."""
    return review_queue.make_review_state(
        author_model,
        tried,
        attempt=attempt,
        next_attempt_at=next_attempt_at,
        now=now,
        author_family=author_family,
        parked_reason=parked_reason,
        last_failure=last_failure,
    )


def awaiting_review_disposition(
    loop_teardown: str | None, *, exit_code: int, ready: bool, cancelled: bool
) -> tuple[str, str | None]:
    """Row disposition for a loop that got no reviewer: needs-review, not failed."""
    return review_queue.awaiting_review_status(
        loop_teardown, exit_code=exit_code, ready=ready, cancelled=cancelled
    )


def review_resume_delay(attempt: int) -> float:
    """Delay before review attempt ``attempt+1`` (15 min doubling, capped at 2 h)."""
    return review_queue.backoff_seconds(attempt)


