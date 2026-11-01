"""Round 6: the findings a cross-family review (muse-spark-1.3-contributor)
returned against the r5 remediation work (2026-10-31).

Each test below is one finding, verified against the source first:

* HIGH — the review hand-over dropped ``result.*`` changes. ``remediate_one``
  excluded them from the result of the row it handed to
  ``advance_review`` while ``plan_for(WAITING_REVIEW)`` writes exactly one
  such field (``review_state``). The fix must not filter: a field fix that
  lands on the result IS the remediation, and the hand-over sees it.

* MEDIUM — an invalid-JSON ``result`` was discarded. The bus already preserved
  it (the admin field patch parks a non-object payload under
  ``previous_result``), but on the ROW every reader in this module returned
  ``{}`` — so the preserved text was invisible to classification, to the
  "fields unchanged" check and to the review hand-over, and the next patch
  would drop it again. The fix: keep the raw text under a result key, the way
  the doctor (``_job_result_dict``) and the loop finaliser (``_result_dict``)
  already do.

* MEDIUM — host-pin handling triggered on any overlap instead of a full pin.
  ``host_pin_fields`` used ``any(h in failed for h in hosts)``, but the bus
  refuses only the hosts that failed the row, so with
  ``eligible_hosts=[a, b]`` and only ``a`` failed the row is still offered to
  ``b`` and the pin strands nothing. The rule is now ``all(...)``: a pin is
  cleared only when EVERY eligible host already failed. The single-host
  fixture (the observed shape) still classifies exactly as before; the
  ``test_r6_overlap_pin_*`` tests pin both sides, so a regression back to
  ``any`` fails them.

The fixtures are the real r5 row shapes (``doctor_remediation_rows.json``,
sanitised from the 2026-10-29/30 triage evidence); side effects are injected,
so each assertion is about the decision and the exact payload.

The FIRST structured reviewer run also raised "missing required keyword
argument ``rescue_workspace`` in ``test_r6_high_the_handover_carries_a_result
_field_for_any_cause``". DISPROVED: ``rescue_workspace`` is an optional keyword
(``Callable | None = None``) on ``remediate_one``, the call omits it
deliberately (that test never reaches the stranded-WIP branch), and the test
passes. No change made.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hive.workers import doctor_remediation as dr
from hive.workers.doctor_state import DurableStateStore, MemoryBackend

FIXTURE = Path(__file__).parent / "tests" / "fixtures" / "doctor_remediation_rows.json"
REAL_HEAD = "4b8c1e0a7d92f36b58a0c1d4e7f9b2a3c6d8e0f1"
ROSTER = [
    {"id": "tydeus-qwen38", "model_id": "qwen38", "family": "alibaba", "cost_class": "A", "concurrency": 4},
    {"id": "cerberus-gemma", "model_id": "gemma4-31b", "family": "google", "cost_class": "A", "concurrency": 2},
    {"id": "cerberus-gpt-oss", "model_id": "gpt-oss-20b", "family": "openai", "cost_class": "A", "concurrency": 8},
]


def row(name: str, **overrides) -> dict:
    data = json.loads(FIXTURE.read_text())
    base = json.loads(json.dumps(data[name]))
    result_overrides = overrides.pop("result", None)
    base.update(overrides)
    if result_overrides:
        base["result"] = {**base.get("result", {}), **result_overrides}
    return base


@pytest.fixture
def state():
    return DurableStateStore(MemoryBackend(), refresh_secs=0.0)


class Fake:
    """The injected side effects, recorded."""

    def __init__(self):
        self.fields: list[tuple[str, dict, str]] = []
        self.reclassify: list[tuple[str, str, str, bool, str]] = []
        self.escalated: list[tuple[str, str]] = []
        self.reviewed: list[dict] = []

    def apply_fields(self, jid, changes, reason):
        self.fields.append((jid, changes, reason))
        return 200, {"ok": [jid]}

    def reclassify_to(self, jid, status, reason, override, override_reason):
        self.reclassify.append((jid, status, reason, override, override_reason))
        return 200, {"ok": [jid]}

    def escalate(self, job, why):
        self.escalated.append((str(job.get("id")), why))

    def advance_review(self, job):
        self.reviewed.append(job)

    def run(self, job, state, **kw):
        kw.setdefault("default_branch", lambda _j: "deploy/integrate-20261017")
        kw.setdefault("branch_exists", lambda _b: True)
        kw.setdefault("ref_exists", lambda _r: True)
        kw.setdefault("roster", ROSTER)
        return dr.remediate_one(
            job,
            state=state,
            apply_fields=self.apply_fields,
            reclassify=self.reclassify_to,
            escalate=self.escalate,
            advance_review=self.advance_review,
            rescue_workspace=lambda _job, _branch: (True, "pushed"),
            **kw,
        )


# ── HIGH: the review hand-over must carry result.* changes ─────────────────


def test_r6_high_the_review_handover_carries_the_planned_result_fields(state):
    """``WAITING_REVIEW`` writes ``review_state`` — a ``result.*`` field — and
    the hand-over must see it. Filtering ``result.*`` out handed the review
    queue a row without the state the plan had just written."""
    fake = Fake()
    job = row("waiting_review")
    outcome = fake.run(job, state)

    assert outcome.status == dr.APPLIED
    handled = fake.reviewed[0]
    planned = fake.fields[0][1]["review_state"]["after"]
    assert handled["result"]["review_state"] == planned, "the planned result field reaches the hand-over"
    assert handled["result"]["review_state"]["state"] == "awaiting_review"


def test_r6_high_the_handover_carries_a_result_field_for_any_cause(state):
    """Generalised: a plan may change a result field for ANY cause, and the
    hand-over is built from the row, so every planned ``after`` must land —
    the bare row fields AND the result fields, in one dict."""
    job = row("waiting_review")
    plan = dr.Plan(
        cause=dr.Cause(dr.WAITING_REVIEW, "test", ()),
        changes={
            "review_state": dr._change(None, {"state": "awaiting_review"}),
            "head_sha": dr._change(None, REAL_HEAD),
        },
        needs_review=True,
        reason="review_busy:test",
        detail="test",
    )
    fake = Fake()
    state_store = DurableStateStore(MemoryBackend(), refresh_secs=0.0)
    captured = {}

    def advance_review(row_after):
        captured.update(row_after)

    original_plan_for = dr.plan_for
    dr.plan_for = lambda *a, **kw: plan
    try:
        dr.remediate_one(
            job,
            state=state_store,
            apply_fields=lambda jid, changes, reason: (200, {}),
            reclassify=lambda jid, status, reason, override, override_reason: (200, {}),
            escalate=fake.escalate,
            advance_review=advance_review,
            roster=ROSTER,
        )
    finally:
        dr.plan_for = original_plan_for

    assert captured["status"] == "needs-review"
    assert captured["result"]["review_state"] == {"state": "awaiting_review"}
    assert captured["result"]["head_sha"] == REAL_HEAD


# ── MEDIUM: an invalid-JSON result is preserved, not discarded ─────────────


def test_r6_medium_invalid_json_result_is_preserved_as_raw_text():
    """The row's result is free-form TEXT. A payload that is not JSON at all
    used to read as ``{}`` on every path of this module: the preserved text
    was dropped from the fix, and the hand-over would have written a result
    that no longer carries it."""
    job = {"id": "01a18c91-bad", "status": "needs-review", "result": "review said: approved? no, prose"}
    assert dr.result_dict(job) == {"raw": "review said: approved? no, prose"}


def test_r6_medium_a_bare_string_result_is_preserved_too():
    job = {"id": "01a18c91-bad", "status": "needs-review", "result": '"just-a-string"'}
    assert dr.result_dict(job) == {"raw": '"just-a-string"'}


def test_r6_medium_a_json_list_result_is_preserved_with_its_text():
    job = {"id": "01a18c91-bad", "status": "needs-review", "result": "[1, 2, 3]"}
    assert dr.result_dict(job) == {"raw": "[1, 2, 3]"}


def test_r6_medium_the_handover_keeps_the_preserved_text(state):
    """End to end: a row whose result is unparseable text and whose plan writes
    a result field hands the review queue a row that still carries the text
    (under the same ``raw`` key the rest of the doctor uses)."""
    fake = Fake()
    job = row("waiting_review")
    # The reviewer's prose landed on the column instead of a JSON object: the
    # row has a REAL failure and delivery record, and an unparseable payload.
    job["result"] = (
        '{"error": "reviewer_unavailable: no review chunks", "head_sha": "a71f3c9d2e60b48f15c7d0a3e9b24f6817c5d0e3"'
    )
    plan = dr.Plan(
        cause=dr.Cause(dr.WAITING_REVIEW, "test", ()),
        changes={"review_state": dr._change(None, {"state": "awaiting_review"})},
        needs_review=True,
        reason="review_busy:test",
        detail="test",
    )

    def advance_review(row_after):
        fake.reviewed.append(row_after)

    original_plan_for = dr.plan_for
    dr.plan_for = lambda *a, **kw: plan
    try:
        fake.run(job, state)
    finally:
        dr.plan_for = original_plan_for

    handled = fake.reviewed[0]
    assert handled["result"]["raw"] == job["result"], "the unparseable payload is preserved under result.raw"
    assert handled["result"]["review_state"] == {"state": "awaiting_review"}


def test_r6_medium_a_dict_result_is_untouched():
    """No behavioural change for the normal shape (the fixtures are all dicts)."""
    job = {"id": "01a18c91-ok", "status": "needs-review", "result": {"verdict": "approve"}}
    assert dr.result_dict(job) == {"verdict": "approve"}


# ── MEDIUM (DISPROVED): a pin is cleared only on a FULL pin ────────────────


def test_r6_overlap_pin_is_disproved_a_partial_pin_is_never_cleared(state):
    """PINNING. ``eligible_hosts=[cerberus, tydeus]`` with only ``cerberus``
    failed: the bus can still offer the work to ``tydeus``, so there is no pin
    to clear. (This fails if the rule ever regresses to ``any()``.)"""
    job = row("host_pinned", eligible_hosts=["cerberus", "tydeus"], host_failures={"cerberus": 2})
    assert dr.host_pin_fields(job) == []
    fake = Fake()
    outcome = fake.run(job, state)
    assert outcome.status == dr.NO_CHANGE
    assert fake.fields == []


def test_r6_overlap_pin_is_disproved_a_claim_is_not_a_failed_host(state):
    """PINNING. ``claimed_by`` names the host that RAN the row. When it is one
    of several eligible hosts and that host has no failure on record, the claim
    says nothing about the others: the pin is not full, so nothing is cleared.
    (This fails if the claim-alone rule is dropped back to a blanket add.)"""
    job = row(
        "host_pinned",
        eligible_hosts=["cerberus", "tydeus"],
        host_failures={},
        claimed_by="urn:agent:zeroclaw:cerberus:2",
    )
    assert dr.host_pin_fields(job) == []
    fake = Fake()
    assert fake.run(job, state).status == dr.NO_CHANGE
    assert fake.fields == []


def test_r6_overlap_pin_is_disproved_an_unchanged_scope_clears_on_a_full_pin(state):
    """PINNING (the positive side). Only when EVERY eligible host has failed
    the row is the pin the reason it cannot run — the single-host fixture."""
    job = row("host_pinned", eligible_hosts=["cerberus"], host_failures={"Cerberus": 1})
    assert dr.host_pin_fields(job) == ["eligible_hosts"]
    assert dr.classify(job).name == dr.HOST_PINNED
    fake = Fake()
    assert fake.run(job, state).status == dr.APPLIED
    assert fake.fields[0][1]["eligible_hosts"] == {"before": ["cerberus"], "after": []}


def test_r6_overlap_pin_is_disproved_a_multi_host_full_pin_is_cleared(state):
    """A pin where BOTH hosts failed is still a pin: all eligible hosts failed,
    so the bus cannot re-offer the row to any of them."""
    job = row("host_pinned", eligible_hosts=["cerberus", "tydeus"], host_failures={"cerberus": 1, "tydeus": 2})
    assert dr.host_pin_fields(job) == ["eligible_hosts"]
    assert dr.classify(job).name == dr.HOST_PINNED


# ── HIGH, end to end on the REAL bus: the hand-over must carry review_state ─


WAITING_ROW = (
    '{"id": "01a18c91-9b02-7caa-b4e6-1f2a3b05d75a", "kind": "fix:hive-review-queue-20261031", '
    '"status": "failed", "repo": "graeae-hive", "project": "graeae-hive", '
    '"head_branch": "hv/gh-review-queue-20261031", "base_branch": "deploy/integrate-20261017", '
    '"eligible_kinds": ["zeroclaw"], "eligible_hosts": [], "preferred_models": ["qwen38"], '
    '"max_cost_tier": "A", "max_retries": 2, "retry_count": 2, "priority": 0, '
    '"claimed_by": "urn:agent:zeroclaw:cerberus:2", "ended_at": 1793524500.0, '
    '"result": {"via": "hive_worker", "failure_reason": "loop_unresolved", '
    '"error": "reviewer_unavailable: no review chunks from the configured endpoint", '
    '"head_sha": "a71f3c9d2e60b48f15c7d0a3e9b24f6817c5d0e3", "remote_verified": true, '
    '"worktree_clean": true, "tracked_worktree_clean": true}}'
)


def test_r6_high_the_row_the_sample_hands_over_is_readable_by_the_review_queue():
    """The real row shape, fixed and handed over: the sample the remediation
    passes to ``advance_review`` must be the row the review queue can pick up
    (``review_queue.review_state`` reads ``result.review_state``). With the
    hand-over filtered to bare fields, the queue saw a row with no state."""
    from hive import review_queue

    job = row("waiting_review")
    assert dr.classify(job).name == dr.WAITING_REVIEW
    plan = dr.plan_for(
        job,
        dr.Cause(dr.WAITING_REVIEW, "reviewer_unavailable: no review chunks from the configured endpoint", ()),
        default_branch="deploy/integrate-20261017",
        roster=ROSTER,
    )
    assert plan is not None
    # The sample ``remediate_one`` builds for ``advance_review``. Until round 6
    # it carried only the plan's BARE fields: ``review_state`` was one of the
    # ``result.*`` keys the hand-over excluded, so the row the review queue was
    # told about had no state to consume -- the fix never reached the queue.
    # (The assertion below is on the module's own text because the previous
    # round's test file pins that filter's intent; here it is the defect.)
    excluded = 'k.startswith("result.")'
    import inspect

    src = inspect.getsource(dr.remediate_one)
    assert excluded not in src, "the hand-over must not drop result.* changes"

    handed = {
        **job,
        "status": "needs-review",
        "result": {**dr.result_dict(job), **{k: v["after"] for k, v in plan.changes.items()}},
    }
    assert review_queue.review_state(handed) is not None, "the hand-over must carry the state the plan wrote"
    assert review_queue.review_state(handed)["state"] == "awaiting_review"


def test_r6_the_executor_never_sees_a_nested_result_change(state):
    """DISPROVED (second structured reviewer run). The claim was that removing
    the hand-over filter also sent ``result.*`` changes to ``apply_fields``,
    whose API takes top-level fields only. It does not: ``apply_fields``
    receives ``plan.changes``, and the hand-over sample is a separate dict
    built for ``advance_review``. Pinned so the two can never be conflated."""
    fake = Fake()
    job = row("waiting_review")
    fake.run(job, state)

    assert fake.fields, "the run must have written the plan"
    for _jid, changes, _reason in fake.fields:
        assert all(not name.startswith("result.") for name in changes), changes
        assert set(changes) <= set(job) | {"review_state", "head_sha"}, changes
    assert fake.fields[0][1]["review_state"]["after"]["state"] == "awaiting_review"


def _run_with_plan(job, state, plan):
    """Remediate ``job`` with a fixed plan (so the hand-over shape is exercised)."""
    captured = {}

    def advance_review(row_after):
        captured["row"] = row_after

    original_plan_for = dr.plan_for
    dr.plan_for = lambda *a, **kw: plan
    try:
        fake = Fake()
        fake.advance_review = advance_review
        outcome = fake.run(job, state)
    finally:
        dr.plan_for = original_plan_for
    return outcome, captured.get("row")


def test_r6_a_qualified_result_key_is_unpacked_into_the_handover(state):
    """Third reviewer run: if a plan ever names ``result.<key>``, the hand-over
    must unpack it into the result. It must NEVER write a literal
    ``"result.review_state"`` key -- the review queue reads
    ``result.review_state``, not ``result["result.review_state"]``."""
    job = row("waiting_review")
    plan = dr.Plan(
        cause=dr.Cause(dr.WAITING_REVIEW, "test", ()),
        changes={"result.review_state": dr._change(None, {"state": "awaiting_review"})},
        needs_review=True,
        reason="review_busy:test",
        detail="test",
    )
    outcome, handled = _run_with_plan(job, state, plan)

    assert outcome.status == dr.APPLIED
    assert "result.review_state" not in handled["result"], handled["result"]
    assert handled["result"]["review_state"] == {"state": "awaiting_review"}


def test_r6_the_production_plan_key_is_bare_and_reaches_the_review_queue(state):
    """The production shape, end to end (no patching of ``plan_for``): the
    ``WAITING_REVIEW`` plan writes the BARE key ``review_state``, it lands on
    the handed-over row's result, and ``review_queue.review_state`` reads it."""
    from hive import review_queue

    job = row("waiting_review")
    outcome = Fake().run(job, state)
    assert outcome.status == dr.APPLIED

    plan = dr.plan_for(
        row("waiting_review"),
        dr.Cause(dr.WAITING_REVIEW, "reviewer_unavailable", ()),
        default_branch="deploy/integrate-20261017",
        roster=ROSTER,
    )
    assert set(plan.changes) == {"review_state"}, plan.changes
    assert not any(name.startswith("result.") for name in plan.changes)

    handed = dr._handover_result(row("waiting_review"), plan)
    assert review_queue.review_state({"result": handed}) is not None
    assert review_queue.review_state({"result": handed})["state"] == "awaiting_review"


# ── The qualified result.* shape: one level of holder, never a nested copy ──


def test_r6_plan_for_never_emits_a_qualified_result_key():
    """The production plan is BARE-keyed by construction.

    ``plan_for(UNRESOLVABLE_REF)`` starts from cause fields that ARE qualified
    (``composite_baseline`` carries ``result.baseline``/``result.verify_baseline``
    -- the observed 36-char composite shape) and de-qualifies each to the row
    field the executor writes. ``_remediate_apply_fields`` posts ``plan.changes``
    keys VERBATIM to the admin reclassify API, so a surviving ``result.``
    qualifier would post a field name the row does not have. A reviewer claimed
    the production plan emits ``result.review_state``; it does not, and this
    pins that against the real row shape instead of asserting it in prose."""
    cause = dr.classify(row("composite_baseline"))
    assert cause.name == dr.UNRESOLVABLE_REF, cause
    assert any(f.startswith("result.") for f in cause.fields), cause.fields

    plan = dr.plan_for(
        row("composite_baseline"),
        cause,
        default_branch="deploy/integrate-20261017",
        roster=ROSTER,
        ref_exists=lambda ref: str(ref).startswith("refs/heads/"),
        resolve_ref=lambda ref: "a" * 40,
    )
    assert plan is not None
    qualified = [name for name in plan.changes if name.startswith("result.")]
    assert not qualified, f"plan.changes must be bare row fields, got {qualified}"


def test_r6_a_qualified_key_never_becomes_a_nested_result_dict(state):
    """A hypothetical qualified plan key is flattened to its holder, not nested.

    The consumer boundary is one holder deep: ``review_queue.review_state``
    reads ``result["review_state"]`` and ``doctor._remediate_apply_fields``
    posts each key verbatim. Nesting a qualified key would produce
    ``result["result"]["review_state"]``, which NO reader consumes -- so both a
    literal dotted key AND a nested dict are wrong. The qualifier is syntax for
    pointing at the holder, not part of the field name."""
    result = dr._handover_result(
        row("waiting_review"),
        dr.Plan(
            cause=dr.Cause(dr.WAITING_REVIEW, "test", ()),
            changes={
                "result.review_state": dr._change(None, {"state": "awaiting_review"}),
                "result.head_sha": dr._change(None, "b" * 40),
            },
            needs_review=True,
            reason="review_busy:test",
            detail="test",
        ),
    )
    # No literal dotted key survives...
    assert "result.review_state" not in result, result
    # ...and no nested holder is created either: the source row carries no
    # ``result`` key of its own, so any ``result`` here could only be the
    # wrongly-nested form.
    assert "result" not in result, result
    # Both qualified changes landed as direct result keys.
    assert result["review_state"] == {"state": "awaiting_review"}, result
    assert result["head_sha"] == "b" * 40, result
    # The pre-existing result keys are preserved alongside them.
    assert result["via"] == "hive_worker", result

    # ...and the consumer the hand-over exists for reads it.
    from hive import review_queue

    assert review_queue.review_state({"result": result})["state"] == "awaiting_review"


def test_r6_a_json_null_result_reads_empty_not_as_raw_text():
    """A result whose text is the JSON literal ``null`` is EMPTY, not evidence.

    ``'{"raw": "null"}'`` would make the string ``null`` searchable as if the
    row had failed with it, and it diverged from ``doctor._job_result_dict``
    (which returns ``{}``). Where the two agree, they must agree exactly."""
    from hive.workers import doctor

    for value in (None, "", "null"):
        assert dr.result_dict({"result": value}) == {}, value
        assert dr.result_dict({"result": value}) == doctor._job_result_dict({"result": value}), value


def test_r6_result_dict_matches_the_doctor_except_where_preservation_differs():
    """Parity with ``doctor._job_result_dict``, with ONE deliberate difference.

    The doctor reads a result for search and may drop a non-object; this module
    must PRESERVE it (the medium finding). Everything that parses to an object,
    or to nothing, must agree exactly -- a drift there is a bug."""
    from hive.workers import doctor

    for value in (None, "", "null", "{}", '{"a": 1}', "not json", "  "):
        assert dr.result_dict({"result": value}) == doctor._job_result_dict({"result": value}), value

    # The deliberate difference: a JSON list is real evidence here, empty there.
    assert dr.result_dict({"result": "[1, 2, 3]"}) == {"raw": "[1, 2, 3]"}
    assert doctor._job_result_dict({"result": "[1, 2, 3]"}) == {}
