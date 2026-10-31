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


def child_verdict(child_result: dict) -> dict | None:
    """The review child's verdict as ``{"verdict": str, "head": str | None}``, or None.

    Two producer shapes exist. The legacy one is a top-level ``result.verdict``
    dict with a string ``verdict`` (returned unchanged). The real worker result
    has ``result.verdict`` null and the verdict at ``result.reviewer.verdict``
    (``hive/review_record.py``), next to the shas of the commit the review job
    left the branch at: ``    remote_sha`` when ``remote_verified`` is true (what
    the remote really holds), else ``head_sha``. Anything else is unusable.

    Never raises: a non-dict root (prose, a truncated write, a bare list)
    is unusable, not an error.
    """
    if not isinstance(child_result, dict):
        return None
    verdict = child_result.get("verdict")
    if isinstance(verdict, dict) and text_value(verdict.get("verdict")):
        return verdict
    reviewer = child_result.get("reviewer")
    text = text_value(reviewer.get("verdict")) if isinstance(reviewer, dict) else None
    if not text:
        return None
    if text.lower() == "approve" and not named_model(reviewer.get("model")):
        # FB40: a worker approve without a named reviewer is not approval —
        # the reconcile gate must not complete code work on a model-less claim.
        return None
    remote = text_value(child_result.get("remote_sha")) if child_result.get("remote_verified") is True else None
    return {"verdict": text.lower(), "head": remote or text_value(child_result.get("head_sha"))}


_PLACEHOLDER_MODELS = frozenset({"", "auto", "unknown", "reviewer", "none", "null"})


def named_model(value) -> str:
    """A reviewer model name, or "" when the value is blank or a placeholder.

    Mirrors ``hive.review_record._named`` (kept local so this module stays
    dependency-free): bare/placeholder names never identify a reviewer.
    """
    text = text_value(value)
    if not text or text.lower() in _PLACEHOLDER_MODELS or text.startswith("("):
        return ""
    return text


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
    res_head = text_value(result.get("head_sha")) if isinstance(result, dict) else None
    job_head = text_value(job.get("head_sha")) if isinstance(job, dict) else None
    return res_head or job_head


def reviewed_heads(result: dict) -> list[str]:
    """Every commit the row's review evidence says was reviewed (may be empty).

    Fields read: ``review_evidence.reviewed_head`` / ``head_sha`` / ``head`` (the bus's
    reconcile record and a worker verdict), ``review_evidence.verdict.head`` /
    ``head_sha`` (the normalised child verdict), and ``reviewer.head`` /
    ``head_sha`` / ``reviewed_head``. ``delivered_head`` and ``approved_unmerged_tip``
    are not read: the first is the row's own head and the second is a tip the review
    job pushed, neither is "the commit that was reviewed".

    Never raises: a non-dict root carries no reviewed heads.
    """
    if not isinstance(result, dict):
        return []
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
    """The reviewer's verdict on the row, normalised ('' when none is recorded).

    Never raises: a non-dict root records no verdict. An ``approve`` surfaces
    only from a slot that also names the approving model (FB40): a bare
    verdict string or a model-less reviewer entry is not approval.
    """
    if not isinstance(result, dict):
        return ""
    reviewer = result.get("reviewer")
    if isinstance(reviewer, dict):
        text = normalize_verdict(reviewer.get("verdict"))
        if text and (text != "approve" or named_model(reviewer.get("model"))):
            return text
    evidence = result.get("review_evidence")
    if isinstance(evidence, dict):
        verdict = evidence.get("verdict")
        if isinstance(verdict, dict):
            text = normalize_verdict(verdict.get("verdict"))
            if text and (text != "approve" or named_model(verdict.get("model"))):
                return text
        else:
            text = normalize_verdict(verdict)
            if text and text != "approve":
                return text
        for entry in (evidence.get("first"), *(evidence.get("reviewers") or [])):
            if not isinstance(entry, dict):
                continue
            text = normalize_verdict(entry.get("verdict"))
            if text and (text != "approve" or named_model(entry.get("model"))):
                return text
    return ""


def evidence_summary(job: dict, result: dict) -> str:
    """One short line of what the row actually carries, ``"none"`` only when nothing.

    ``<failure_reason>, review=<verdict>@<reviewed sha7 or ?> head=<delivered sha7>,
    final_round=reviewer_unavailable``. The review part is printed whenever the row
    has review evidence of any kind, so a line that says ``evidence=none`` is true.

    Never raises: non-dict inputs summarise as ``"none"``.
    """
    if not isinstance(job, dict):
        job = {}
    if not isinstance(result, dict):
        result = {}
    parts: list[str] = []
    reason = text_value(job.get("failure_reason")) or text_value(result.get("failure_reason"))
    if reason:
        parts.append(reason)
    verdict = row_verdict(result)
    heads = reviewed_heads(result)
    evidence = result.get("review_evidence")
    if verdict or heads or evidence:
        review = f"review={verdict or 'unknown'}@{short_sha(heads[0]) if heads else '?'}"
        delivered = delivered_head(job, result)
        if delivered:
            review += f" head={short_sha(delivered)}"
        parts.append(review)
        loop = result.get("loop_result")
        teardown = loop.get("teardown") if isinstance(loop, dict) else None
        final = normalize_verdict(teardown.get("final_verdict")) if isinstance(teardown, dict) else ""
        if final == "reviewer_unavailable":
            parts.append("final_round=reviewer_unavailable")
    return ", ".join(parts) or "none"
