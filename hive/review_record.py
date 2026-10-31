"""One reviewer record per job (2026-10-26).

For one job the bus row could name three different reviewers: the worker's
``result.review_evidence`` (from the structured zoder verdict),
``result.models.reviewers`` (from the host ledger, which also holds other jobs'
calls in the same window) and ``result.hive_reviews`` (the pre-dispatch brief
review by the hive-reviewer timer). A dispatcher reading the row could not tell
which model reviewed the delivered diff.

The source of truth is the structured zoder verdict: ``reviewers[].model`` of
``result.review_evidence``, in order, the first entry being the reviewer of
record. Without a structured verdict the record falls back to the model the
worker launched the review with, marked ``source: "launched_route"``. Every
reviewer field on the row is derived from that one record:

* ``result.reviewer``                    the record itself (new key)
* ``result.models.reviewers``            its model list; a different list (ledger,
                                         role names) is kept as ``models.observed_reviewers``
* ``result.review_evidence``             unchanged, plus ``reviewer_model`` and
                                         ``reviewer_source``
* ``result.hive_reviews``                one ``stage: "delivery"`` entry; the
                                         hive-reviewer's brief reviews are a
                                         different stage (``stage`` absent or
                                         ``"brief"``) and are left untouched

``claimed_model`` is the AUTHOR route of the job, not a reviewer field.

Pure functions, stdlib only: the same file ships on the worker and the bus lines.
"""

from __future__ import annotations

from typing import Any

SOURCE_VERDICT = "zoder_verdict"
SOURCE_LAUNCHED = "launched_route"
DELIVERY_STAGE = "delivery"
_PLACEHOLDERS = frozenset({"", "auto", "unknown", "reviewer", "none", "null"})


def _named(name: Any) -> str:
    text = str(name or "").strip()
    if text.lower() in _PLACEHOLDERS or text.startswith("("):
        return ""
    return text


def _label(entry: Any) -> str:
    if not isinstance(entry, dict):
        return ""
    return _named(entry.get("model")) or _named(entry.get("role"))


def evidence_reviewers(evidence: Any) -> list[str]:
    """Reviewer models of a structured verdict, in order, de-duplicated."""
    names: list[str] = []
    entries = evidence.get("reviewers") if isinstance(evidence, dict) else None
    for entry in entries if isinstance(entries, list) else []:
        label = _label(entry)
        if label and label not in names:
            names.append(label)
    return names


def canonical_reviewer(evidence: Any, launched: Any = None) -> dict:
    """The reviewer record: from the verdict, else the launched route, else empty."""
    names = evidence_reviewers(evidence)
    if names:
        entries = evidence.get("reviewers") if isinstance(evidence, dict) else None
        entries = entries or []
        first = next((e for e in entries if _label(e) == names[0]), {})
        verdict = str(first.get("verdict") or "").strip().lower() or None
        return {
            "model": names[0],
            "models": names,
            "verdict": verdict,
            "source": SOURCE_VERDICT,
        }
    route = _named(launched)
    if route:
        return {
            "model": route,
            "models": [route],
            "verdict": None,
            "source": SOURCE_LAUNCHED,
        }
    return {"model": None, "models": [], "verdict": None, "source": None}


def _delivery_entries(result: dict) -> list[dict]:
    if not isinstance(result, dict):
        return []
    reviews = result.get("hive_reviews")
    return [
        r
        for r in (reviews if isinstance(reviews, list) else [])
        if isinstance(r, dict) and r.get("stage") == DELIVERY_STAGE
    ]


def reviewer_fields(result: Any) -> dict[str, str | None]:
    """The reviewer each field of a result names (None when the field is absent)."""
    if not isinstance(result, dict):
        return {}
    fields: dict[str, str | None] = {}
    rec = result.get("reviewer")
    fields["reviewer"] = None
    if isinstance(rec, dict):
        fields["reviewer"] = _named(rec.get("model")) or None
    names = evidence_reviewers(result.get("review_evidence"))
    fields["review_evidence"] = names[0] if names else None
    models = result.get("models")
    listed = models.get("reviewers") if isinstance(models, dict) else None
    listed = [_named(n) for n in listed] if isinstance(listed, list) else []
    listed = [n for n in listed if n]
    fields["models"] = listed[0] if listed else None
    delivery = _delivery_entries(result)
    fields["hive_reviews"] = None
    if delivery:
        fields["hive_reviews"] = _named(delivery[-1].get("model")) or None
    return fields


def disagreements(result: Any) -> list[str]:
    """Human-readable disagreements between the reviewer fields that are present."""
    fields = {k: v for k, v in reviewer_fields(result).items() if v}
    if len(set(fields.values())) <= 1:
        return []
    return [f"{k}={v}" for k, v in sorted(fields.items())]


def apply(result: dict, launched: Any = None, *, hive_reviews: bool = False) -> dict:
    """Derive every reviewer field of ``result`` from the one record. Returns it.

    Never mutates a nested dict in place (``result["models"]`` may be shared with
    the caller); replaced containers are fresh copies. ``hive_reviews=True`` also
    writes the delivery entry -- only the bus does that, because an older bus
    replaces the stored ``hive_reviews`` with whatever a worker sends.
    """
    if not isinstance(result, dict):
        return {"model": None, "models": [], "verdict": None, "source": None}
    evidence = result.get("review_evidence")
    if launched is None:
        prior = result.get("reviewer")
        if isinstance(prior, dict) and prior.get("source") == SOURCE_LAUNCHED:
            launched = prior.get("model")
    record = canonical_reviewer(evidence, launched)
    result["reviewer"] = dict(record)

    models = result.get("models")
    models = dict(models) if isinstance(models, dict) else {}
    old = models.get("reviewers")
    if isinstance(old, list) and old and list(old) != record["models"]:
        models.setdefault("observed_reviewers", list(old))
    models["reviewers"] = list(record["models"])
    models["reviewers_source"] = record["source"]
    result["models"] = models

    if isinstance(evidence, dict):
        evidence = dict(evidence)
        evidence["reviewer_model"] = record["model"]
        evidence["reviewer_source"] = record["source"]
        result["review_evidence"] = evidence

    if hive_reviews:
        reviews = result.get("hive_reviews")
        kept = [
            r
            for r in (reviews if isinstance(reviews, list) else [])
            if not (isinstance(r, dict) and r.get("stage") == DELIVERY_STAGE)
        ]
        if record["model"]:
            kept.append(
                {
                    "stage": DELIVERY_STAGE,
                    "model": record["model"],
                    "verdict": record["verdict"],
                    "source": record["source"],
                }
            )
        if kept or isinstance(reviews, list):
            result["hive_reviews"] = kept
    return record
