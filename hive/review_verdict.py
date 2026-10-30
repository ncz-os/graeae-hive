"""Read a review verdict, and the commit it covers, from a job result.

Stdlib only, no bus or worker imports: the bus (``hive/bus/domain/review_dispatch.py``)
and the doctor (``hive/workers/doctor.py``) both read review results and must read
them the same way. Two producer shapes exist for a finished review job:

* legacy: a top-level ``result.verdict`` dict with a string ``verdict`` (and a
  ``head`` or ``head_sha`` naming the reviewed commit);
* worker: ``result.verdict`` is null and the verdict sits at ``result.reviewer.verdict``
  (``hive/review_record.py``), next to the shas of the commit the job left the
  branch at (``remote_sha`` when ``remote_verified`` is true, else ``head_sha``).

A needs-review ROW carries the same facts in a third place, ``result.review_evidence``
(written by the worker's loop, or by the bus when it reconciles a review child), so
the row helpers below look there too.
"""

from __future__ import annotations

SHORT_SHA = 7
_MIN_PREFIX = 7


def text_value(value) -> str | None:
    """``value`` stripped, or None when it is not a non-blank string."""
    return value.strip() if isinstance(value, str) and value.strip() else None


def normalize_verdict(value) -> str:
    """Lower-case verdict with ``-`` read as ``_`` (``request-changes`` == ``request_changes``)."""
    return (text_value(value) or "").lower().replace("-", "_")


def short_sha(sha) -> str:
    return (text_value(sha) or "")[:SHORT_SHA]


def same_sha(left, right) -> bool:
    """True when two shas name the same commit (an abbreviation of 7+ chars counts)."""
    a, b = (text_value(left) or "").lower(), (text_value(right) or "").lower()
    if not a or not b:
        return False
    if a == b:
        return True
    shorter = min(len(a), len(b))
    return shorter >= _MIN_PREFIX and a[:shorter] == b[:shorter]


def delivered_head(job: dict, result: dict) -> str | None:
    """The head the row delivered: ``result.head_sha``, else the row's own."""
    return text_value(result.get("head_sha")) or text_value(job.get("head_sha"))


def reviewed_heads(result: dict) -> list[str]:
    """Every commit the row's review evidence says was reviewed (may be empty).

    Fields read: ``review_evidence.reviewed_head`` / ``head_sha`` / ``head`` (the bus's
    reconcile record and a worker verdict), ``review_evidence.verdict.head`` /
    ``head_sha`` (the normalised child verdict), and ``reviewer.head`` /
    ``head_sha`` / ``reviewed_head``. ``delivered_head`` and ``approved_unmerged_tip``
    are not read: the first is the row's own head and the second is a tip the review
    job pushed, neither is "the commit that was reviewed".
    """
    heads: list[str] = []

    def add(value) -> None:
        text = text_value(value)
        if text and text not in heads:
            heads.append(text)

    evidence = result.get("review_evidence")
    if isinstance(evidence, dict):
        for key in ("reviewed_head", "head_sha", "head"):
            add(evidence.get(key))
        verdict = evidence.get("verdict")
        if isinstance(verdict, dict):
            add(verdict.get("head"))
            add(verdict.get("head_sha"))
    reviewer = result.get("reviewer")
    if isinstance(reviewer, dict):
        for key in ("head", "head_sha", "reviewed_head"):
            add(reviewer.get(key))
    return heads


def row_verdict(result: dict) -> str:
    """The reviewer's verdict on the row, normalised ('' when none is recorded)."""
    reviewer = result.get("reviewer")
    if isinstance(reviewer, dict):
        text = normalize_verdict(reviewer.get("verdict"))
        if text:
            return text
    evidence = result.get("review_evidence")
    if isinstance(evidence, dict):
        verdict = evidence.get("verdict")
        text = normalize_verdict(verdict.get("verdict") if isinstance(verdict, dict) else verdict)
        if text:
            return text
        for entry in (evidence.get("first"), *(evidence.get("reviewers") or [])):
            text = normalize_verdict(entry.get("verdict")) if isinstance(entry, dict) else ""
            if text:
                return text
    return ""
