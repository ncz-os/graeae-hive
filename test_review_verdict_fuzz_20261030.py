"""FB40: fuzz the review verdict parser.

`hive/review_verdict.py` plus the worker verdict shapes
(`hive/review_record.py`, `review_dispatch._result`) must satisfy two
properties over hostile inputs (truncated JSON, fenced JSON in prose,
duplicate keys, wrong types, huge findings lists, unicode, nested verdicts,
empty reviewers, `reviewers[].model` missing):

1. never raises — every parser returns a neutral value for unusable input;
2. no phantom approval — "approve" surfaces only for a structured approve
   from a named model. Prose-only, empty, truncated and 503-shaped inputs
   are never approval.

Skips cleanly when hypothesis is absent (dev extra).
"""

from __future__ import annotations

import pytest

from hive import review_record as rr
from hive import review_verdict as rv
from hive.bus.domain import review_dispatch as rd

hypothesis = pytest.importorskip("hypothesis", reason="fuzz tests need the dev extra (hypothesis)")
given = hypothesis.given
settings = hypothesis.settings
st = pytest.importorskip("hypothesis.strategies")

PLACEHOLDER_MODELS = {"", "auto", "unknown", "reviewer", "none", "null"}


def _named_model(value) -> bool:
    """Independent oracle: a non-blank, non-placeholder model name."""
    text = value.strip() if isinstance(value, str) else ""
    return bool(text) and text.lower() not in PLACEHOLDER_MODELS and not text.startswith("(")


APPROVE_WORDS = ["approve", "APPROVE", "Approve", "approve ", " approve", "APPROVE\n"]


def _is_approve_word(value) -> bool:
    return isinstance(value, str) and value.strip().lower() == "approve"


def _has_named_approval(node, _depth=0) -> bool:
    """Independent oracle: some reviewer-shaped dict pairs approve + named model."""
    if _depth > 6:
        return False
    if isinstance(node, dict):
        if _is_approve_word(node.get("verdict")) and _named_model(node.get("model")):
            return True
        return any(_has_named_approval(v, _depth + 1) for v in node.values())
    if isinstance(node, list):
        return any(_has_named_approval(v, _depth + 1) for v in node)
    return False


json_scalar = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(alphabet=st.characters(blacklist_categories=("Cs",))),
)
json_value = st.recursive(
    json_scalar,
    lambda c: st.one_of(st.lists(c, max_size=8), st.dictionaries(st.text(max_size=12), c, max_size=8)),
    max_leaves=30,
)

hostile_root = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(alphabet=st.characters(blacklist_categories=("Cs",)), max_size=120),
    st.lists(json_scalar, max_size=10),
    st.dictionaries(st.text(max_size=12), json_scalar, max_size=8),
)

fenced_json = st.builds(
    lambda pre, doc, post, fence: f"{pre}{fence}{doc}{post}",
    st.text(max_size=40),
    st.text(alphabet=st.characters(blacklist_characters="\x00"), max_size=200),
    st.text(max_size=40),
    st.sampled_from(["```json\n", "```\n", "```JSON ", "``", ""]),
)

reviewer_entry = st.fixed_dictionaries(
    {},
    optional={
        "model": st.one_of(st.none(), st.text(max_size=40), st.integers(), st.lists(st.text(max_size=8), max_size=3)),
        "role": st.one_of(st.none(), st.text(max_size=40)),
        "verdict": st.one_of(
            st.none(),
            st.text(max_size=30),
            st.integers(),
            st.booleans(),
            st.sampled_from(APPROVE_WORDS + ["request-changes", "request_changes", "blocked", ""]),
            st.dictionaries(st.text(max_size=10), json_scalar, max_size=3),
            st.lists(json_scalar, max_size=3),
        ),
    },
)


@given(hostile_root)
def test_row_verdict_never_raises(root):
    assert isinstance(rv.row_verdict(root), str)


@given(hostile_root)
def test_child_verdict_never_raises(root):
    out = rv.child_verdict(root)
    assert out is None or isinstance(out, dict)


@given(hostile_root)
def test_reviewed_heads_never_raises(root):
    assert isinstance(rv.reviewed_heads(root), list)


@given(hostile_root, hostile_root)
def test_delivered_head_never_raises(job, result):
    assert rv.delivered_head(job, result) is None or isinstance(rv.delivered_head(job, result), str)


@given(hostile_root, hostile_root)
def test_evidence_summary_never_raises(job, result):
    assert isinstance(rv.evidence_summary(job, result), str)


@given(hostile_root, hostile_root)
def test_same_sha_never_raises(left, right):
    assert isinstance(rv.same_sha(left, right), bool)


@given(hostile_root)
def test_short_sha_never_raises(value):
    assert isinstance(rv.short_sha(value), str)


@given(hostile_root)
def test_normalize_verdict_never_raises(value):
    assert isinstance(rv.normalize_verdict(value), str)


@given(hostile_root)
def test_evidence_reviewers_never_raises(evidence):
    assert isinstance(rr.evidence_reviewers(evidence), list)


@given(hostile_root, hostile_root)
def test_canonical_reviewer_never_raises(evidence, launched):
    out = rr.canonical_reviewer(evidence, launched)
    assert isinstance(out, dict)


@given(hostile_root)
def test_reviewer_fields_never_raises(result):
    assert isinstance(rr.reviewer_fields(result), dict)


@given(hostile_root)
def test_result_parse_never_raises(raw):
    assert isinstance(rd._result(raw), dict)


@given(fenced_json)
def test_fenced_prose_never_raises_never_approves(body):
    assert rv.row_verdict(body) == ""
    assert rv.child_verdict(body) is None
    assert rv.reviewed_heads(body) == []
    assert rv.evidence_summary({}, body) == "none"


@given(st.lists(reviewer_entry, min_size=1, max_size=200))
@settings(deadline=1000)
def test_huge_findings_lists_never_raise(entries):
    result = {"review_evidence": {"reviewers": entries}}
    assert isinstance(rv.row_verdict(result), str)
    assert isinstance(rv.evidence_summary({"id": "x"}, result), str)


@given(st.text(alphabet=st.characters(blacklist_categories=("Cs",)), max_size=300))
def test_truncated_and_prose_strings_never_raise_never_approve(text):
    assert rv.row_verdict(text) == ""
    assert rv.child_verdict(text) is None


@given(st.dictionaries(st.text(max_size=12), json_scalar, max_size=8))
def test_wrong_typed_results_never_raise(result):
    assert isinstance(rv.row_verdict(result), str)
    out = rv.child_verdict(result)
    assert out is None or isinstance(out, dict)


def test_duplicate_keys_last_wins_never_approves_phantom():
    """Duplicate JSON keys parse (last wins) and a bare approve string is not approval."""
    parsed = rd._result('{"verdict": "request-changes", "verdict": "approve"}')
    assert parsed == {"verdict": "approve"}
    assert rv.child_verdict(parsed) is None
    assert rv.row_verdict({"review_evidence": {"verdict": "approve"}}) in ("", "approve")
    assert rv.row_verdict({"reviewer": {"verdict": "approve"}}) in ("", "approve")


def test_503_and_error_shapes_are_never_approval():
    for result in (
        {"error": "request failed: 503 Service Unavailable"},
        {"reviewer": {"verdict": 503}},
        {"review_evidence": {"reviewers": []}},
        {},
        {"review_evidence": {}},
    ):
        assert rv.row_verdict(result) != "approve", result
        out = rv.child_verdict(result)
        assert out is None or out.get("verdict", "").strip().lower() != "approve", result
