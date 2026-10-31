"""FB61: one focused unit test per doctor-v2 rule (positive, negative, boundary).

Rules live in ``docs/doctor_v2_rules.toml`` and are data, not code: the
"documented id and docstring" requirement is pinned by
``test_every_rule_has_id_note_and_valid_shape`` (unique ordered ids,
non-empty note per rule, valid action/scope, and a docstring on every
public function exercised here).

Conventions: ``ev()`` builds evidence with an explicit failure reason and
tail text. Positive = the named rule fires; negative = a near-miss skips
it; boundary = the retry/ordering edge. ``decide_rules`` is first-match, so
several tests also pin which rule wins an overlap.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from hive.workers import doctor_v2
from hive.workers.doctor_v2 import (
    bs_audit_signal,
    collect_evidence,
    decide_rules,
    default_evidence_summary,
    load_rules,
    residual_decision,
    shadow_compare,
    sweep_needs_review,
    validate_llm_decision,
)

RULES = load_rules(Path(__file__).parent / "docs" / "doctor_v2_rules.toml")
BY_ID = {rule["id"]: rule for rule in RULES}
ALLOWED_ACTIONS = {"release_to_queue", "escalate", "needs_review", "no_action"}


def ev(reason, text, *, host="CERBERUS", kind="build:x", prior=(), head="fix/x"):
    """Evidence with caller-chosen reason/tail; kind is non-BS by default."""
    return collect_evidence(
        {
            "id": "j1",
            "kind": kind,
            "failure_reason": reason,
            "head_branch": head,
            "host": host,
            "result": {"failure_reason": reason, "tail": text},
        },
        prior_actions=prior,
    )


def decide(reason, text, **kw):
    item = ev(reason, text, **kw)
    return item, decide_rules(item, RULES)


# ── rule table documentation ──────────────────────────────────────────────
def test_every_rule_has_id_note_and_valid_shape():
    """Every rule has a documented id (R1..R10 in order) with a note, a
    valid action/scope, and every public function used here has a docstring."""
    assert [rule["id"] for rule in RULES] == [f"R{i}" for i in range(1, 11)]
    for rule in RULES:
        assert rule["id"] and rule["action"] in ALLOWED_ACTIONS
        assert isinstance(rule.get("note"), str) and rule["note"].strip()
        assert rule.get("retry_scope") in {"same_host", "other_host"}
        assert int(rule.get("max_retries", 0)) >= 0
    from hive.workers.doctor_v2 import evidence_lines, shadow_compare  # noqa: E402

    for func in (
        collect_evidence,
        decide_rules,
        validate_llm_decision,
        bs_audit_signal,
        sweep_needs_review,
        residual_decision,
        default_evidence_summary,
        load_rules,
        shadow_compare,
        evidence_lines,
    ):
        assert func.__doc__, func.__name__


# ── R1: bad branch/clone target ───────────────────────────────────────────
def test_r1_positive_on_reason():
    item, decision = decide("clone_failed", "some other text")
    assert decision.rule_id == "R1"
    assert decision.action == "escalate" and decision.max_retries == 0


def test_r1_positive_on_evidence_only_and_case_insensitive():
    item, decision = decide("CLONE_FAILED", "Remote branch fix/x not found")
    assert decision.rule_id == "R1"


def test_r1_negative_unrelated():
    item, decision = decide("build_failed", "tests passed slowly")
    assert decision is None or decision.rule_id != "R1"


# ── R2: provider transport ────────────────────────────────────────────────
@pytest.mark.parametrize(
    "reason", ["provider 429", "HTTP 503", "dns lookup failed", "connection timeout after 30s", "502 bad gateway"]
)
def test_r2_positive(reason):
    item, decision = decide(reason, "irrelevant tail")
    assert decision.rule_id == "R2"
    assert decision.action == "release_to_queue"
    assert decision.retry_scope == "other_host"


def test_r2_negative_timeout_without_connect():
    # "timed out" alone does not match connect.*timeout.
    item, decision = decide("worker timed out", "process timed out waiting")
    assert decision is None or decision.rule_id != "R2"


def test_r2_boundary_rule_max_capped_by_env():
    # Rule asks for 3, env cap defaults to 2.
    item, decision = decide("provider 503", "HTTP 503")
    assert decision.max_retries == 2


def test_r2_boundary_second_same_signature_still_releases():
    item = ev("provider 503", "HTTP 503")
    one = ev("provider 503", "HTTP 503", prior=({"signature": item.signature},))
    assert decide_rules(one, RULES).action == "release_to_queue"


def test_r1_beats_r2_on_overlap():
    # Failure matches both R1 (clone_failed) and R2 (503): first match wins.
    item, decision = decide("clone_failed 503", "Remote branch fix/x not found")
    assert decision.rule_id == "R1"


# ── R3: delivery evidence failures ────────────────────────────────────────
@pytest.mark.parametrize(
    "reason", ["codex_delivery_failed", "delivery_integrity_check_failed", "completion_evidence_missing"]
)
def test_r3_positive_all_reasons(reason):
    item, decision = decide(reason, "anything at all")
    assert decision.rule_id == "R3"
    assert (decision.action, decision.max_retries) == ("escalate", 0)


def test_r3_negative_text_only_does_not_trigger():
    item, decision = decide("build_failed", "verdict: delivery failed badly")
    assert decision is None or decision.rule_id != "R3"


# ── R4: unhandled exception ───────────────────────────────────────────────
def test_r4_positive_releases_once_same_host():
    item, decision = decide("unhandled_exception", "Traceback blah")
    assert decision.rule_id == "R4"
    assert decision.action == "release_to_queue"
    assert decision.retry_scope == "same_host"
    assert decision.max_retries == 1


def test_r4_negative_handled_exception_text():
    item, decision = decide("build_failed", "an exception was handled and retried ok")
    # "exception" in the tail alone is not the unhandled_exception reason.
    assert decision is None or decision.rule_id != "R4"


def test_r4_boundary_one_prior_still_releases_two_escalate():
    item = ev("unhandled_exception", "Traceback blah")
    one = ev("unhandled_exception", "Traceback blah", prior=({"signature": item.signature},))
    two = ev(
        "unhandled_exception", "Traceback blah", prior=({"signature": item.signature}, {"signature": item.signature})
    )
    assert decide_rules(one, RULES).action == "release_to_queue"
    escalated = decide_rules(two, RULES)
    assert escalated.action == "escalate"
    assert escalated.reason == "same signature reached configured retry cap"


# ── R5: insufficient evidence ─────────────────────────────────────────────
def test_r5_positive_truly_empty_evidence():
    # Non-mandatory kind (no bs_audit line), no keys, no error: text is "".
    item = collect_evidence({"id": "j9", "kind": "ops:x", "head_branch": "ops/y"})
    assert item.evidence_text == ""
    decision = decide_rules(item, RULES)
    assert decision.rule_id == "R5"
    assert (decision.action, decision.max_retries) == ("escalate", 0)


def test_r5_negative_single_line_of_evidence_skips():
    item = collect_evidence({"id": "j9", "kind": "ops:x", "head_branch": "ops/y", "result": {"tail": "one line"}})
    assert item.evidence_text == "one line"
    decision = decide_rules(item, RULES)
    assert decision is None or decision.rule_id != "R5"


# ── R6: zoder infra fault (match_all) ─────────────────────────────────────
def test_r6_positive_needs_both_halves():
    item, decision = decide("loop_unresolved", "session-cancel during review")
    assert decision.rule_id == "R6"
    assert decision.retry_scope == "other_host"
    assert decision.max_retries == 1


def test_r6_negative_reason_only():
    item, decision = decide("loop_unresolved", "all checks green, nothing wrong")
    assert decision is None or decision.rule_id not in {"R6", "R7"}


def test_r6_negative_evidence_only():
    item, decision = decide("build_failed", "daemon.sock is unreachable, sorry")
    assert decision is None or decision.rule_id != "R6"


def test_r2_beats_r6_on_overlap():
    # 503 reason fires R2 before R6 is even considered.
    item, decision = decide("provider 503", "session-cancel during review")
    assert decision.rule_id == "R2"


# ── R7: failing tests in loop record (match_all) ──────────────────────────
def test_r7_positive():
    item, decision = decide("loop_unresolved", "pytest failed tests: test_x")
    assert decision.rule_id == "R7"
    assert (decision.action, decision.max_retries) == ("escalate", 0)


def test_r7_negative_tests_without_loop_reason():
    item, decision = decide("build_failed", "pytest failed tests: test_x")
    assert decision is None or decision.rule_id != "R7"


def test_r6_r7_ordering_infra_checked_before_tests():
    # Evidence matching both R6 and R7 regexes still lands on R6.
    item, decision = decide("loop_unresolved", "session-cancel; pytest failed: test_x")
    assert decision.rule_id == "R6"


def test_loop_unresolved_green_matches_no_rule():
    item, decision = decide("loop_unresolved", "all checks green, nothing wrong")
    assert decide_rules(item, RULES) is None


# ── R8: host fault ────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "tail",
    [
        "OOM-killed worker 42",
        "ENOSPC on /data",
        "disk full, cannot write",
        "lost lease mid-run",
        "worker restart detected",
    ],
)
def test_r8_positive(tail):
    item, decision = decide("host hiccup", tail)
    assert decision.rule_id == "R8"
    assert decision.action == "release_to_queue"
    assert decision.retry_scope == "other_host"
    assert decision.max_retries == 2


def test_r8_negative_ordinary_error():
    item, decision = decide("build_failed", "exit code 1, see log above")
    assert decision is None or decision.rule_id != "R8"


# ── R9: budget exceeded (any-match, host-terminal) ────────────────────────
def test_r9_positive_reason_only():
    item, decision = decide("budget_exceeded", "usage ledger rejected the row")
    assert decision.rule_id == "R9"


def test_r9_positive_evidence_only():
    item, decision = decide("engine blew up", "Budget exceeded: $25 of $25 Day limit")
    assert decision.rule_id == "R9"


def test_r9_boundary_reason_carries_host_terminal_note():
    item = ev("budget_exceeded", "Budget exceeded: $25 of $25 Day limit")
    decision = decide_rules(item, RULES)
    assert decision.action == "escalate" and decision.max_retries == 0
    assert "host=CERBERUS" in decision.reason
    assert "one escalation per host/day" in decision.reason
    assert decision.evidence_quote in item.evidence_text


def test_r9_negative_other_money_text():
    item, decision = decide("build_failed", "cost estimate $25 looks fine")
    assert decision is None or decision.rule_id != "R9"


# ── R10: bs_audit ─────────────────────────────────────────────────────────
def test_r10_positive_confab():
    item = collect_evidence(
        {"id": "j1", "kind": "fix:x", "result": {"output": "As an AI I don't have the ability to edit files"}}
    )
    assert "bs_audit: BS:confab+nocommit" in item.evidence_text
    decision = decide_rules(item, RULES)
    assert decision.rule_id == "R10"
    assert decision.action == "escalate"


def test_r10_negative_committed_work_skips():
    item = collect_evidence({"id": "j1", "kind": "fix:x", "result": {"output": "done", "commits": ["abc"]}})
    decision = decide_rules(item, RULES)
    assert decision is None or decision.rule_id != "R10"


# ── retry guard and env ───────────────────────────────────────────────────
def test_retry_cap_zero_forces_escalate_on_release_rules():
    item = ev("provider 503", "HTTP 503")
    decision = decide_rules(item, RULES, retry_cap=0)
    assert decision.action == "escalate"
    assert decision.reason == "same signature reached configured retry cap"


def test_invalid_env_falls_back_to_default_two(monkeypatch):
    monkeypatch.setenv("DOCTOR_MAX_SAME_SIGNATURE_RESUBMITS", "not-a-number")
    assert doctor_v2.max_same_signature_resubmits() == 2


def test_negative_env_clamps_to_zero(monkeypatch):
    monkeypatch.setenv("DOCTOR_MAX_SAME_SIGNATURE_RESUBMITS", "-5")
    assert doctor_v2.max_same_signature_resubmits() == 0


def test_unmatched_rule_shape_returns_none():
    assert decide_rules(ev("nope", "nothing matches here"), []) is None


# ── validate_llm_decision branches ────────────────────────────────────────
def test_llm_rejects_unknown_action():
    item = ev("unknown", "real evidence line")
    decision = validate_llm_decision({"action": "delete_everything", "evidence_quote": "real evidence line"}, item)
    assert (decision.action, decision.rule_id) == ("escalate", "llm")


def test_llm_rejects_missing_quote():
    item = ev("unknown", "real evidence line")
    decision = validate_llm_decision({"action": "release_to_queue"}, item)
    assert decision.action == "escalate"


def test_llm_release_with_zero_permitted_escalates():
    item = ev("unknown", "real evidence line")
    decision = validate_llm_decision(
        {"action": "release_to_queue", "evidence_quote": "real evidence line", "max_retries": 0}, item
    )
    assert decision.action == "escalate"
    assert decision.reason == "same signature reached configured retry cap"


def test_llm_used_retries_consume_the_cap():
    item = ev("unknown", "real evidence line")
    decision = validate_llm_decision(
        {"action": "release_to_queue", "evidence_quote": "real evidence line", "max_retries": 2},
        item,
        rule_cap=2,
        used_retries=1,
    )
    assert (decision.action, decision.max_retries) == ("release_to_queue", 1)


# ── residual_decision ─────────────────────────────────────────────────────
def test_residual_model_failure_escalates_with_latency():
    item = ev("unknown", "real evidence line")

    def boom(_payload):
        raise RuntimeError("model down")

    decision = residual_decision(item, boom)
    assert decision.action == "escalate"
    assert "residual model failure" in decision.reason
    assert decision.latency_ms is not None and decision.latency_ms >= 0


def test_residual_ok_model_passes_through():
    item = ev("unknown", "real evidence line")

    def ok(_payload):
        return {"action": "needs_review", "reason": "look", "evidence_quote": "real evidence line", "model": "m"}

    decision = residual_decision(item, ok)
    assert (decision.action, decision.model) == ("needs_review", "m")


# ── bs_audit / error class / summary / shadow / sweep ─────────────────────
@pytest.mark.parametrize(
    ("job", "expected"),
    [
        ({"kind": "riskyeats", "result": {}}, "BS:empty+nocommit"),
        ({"kind": "code", "result": {"output": "x" * 100}}, "BS:nocommit-mandatory"),
        ({"kind": "code", "result": {"output": "done", "commits": ["a"]}}, "ok-committed"),
        ({"kind": "ops", "result": {"output": "done"}}, "ok-noncommit"),
    ],
)
def test_bs_audit_branches(job, expected):
    assert bs_audit_signal(job) == expected


def test_error_class_branches():
    assert doctor_v2._error_class("HTTP 503 rate limit, dns fail") == "provider_transport"
    assert doctor_v2._error_class("OOM-killed, ENOSPC disk full") == "host_fault"
    assert doctor_v2._error_class("session-cancel lost; daemon.sock is unreachable") == "zoder_infra"
    assert doctor_v2._error_class("Traceback unhandled exception") == "exception"
    assert doctor_v2._error_class("something odd happened") == "generic"


def test_shadow_compare_agree_and_disagree():
    assert shadow_compare({"action": "escalate"}, FakeDecision("escalate"))["agree"] is True
    assert shadow_compare({"action": "escalate"}, FakeDecision("release_to_queue"))["agree"] is False
    assert shadow_compare({}, None) == {"old_action": "unknown", "new_action": "llm", "agree": False}


class FakeDecision:
    def __init__(self, action):
        self.action = action


def test_sweep_boundaries_and_report_only():
    jobs = [
        {"id": "recent", "kind": "build:x", "status": "needs-review", "updated_at": 99999, "head_branch": "fix/a"},
        {"id": "done-row", "kind": "build:x", "status": "done", "updated_at": 1},
        {"id": "stale", "kind": "feat:y", "status": "needs-review", "updated_at": 1, "head_branch": "fix/y"},
        {"id": "actionable", "kind": "feat:z", "status": "needs-review", "updated_at": 1},
    ]
    rows = sweep_needs_review(jobs, now=100000, older_than_hours=1)
    by_id = {row["job_id"]: row["classification"] for row in rows}
    assert "recent" not in by_id  # inside the window: excluded
    assert "done-row" not in by_id  # not needs-review: excluded
    assert by_id["stale"] == "stale"  # old, no head branch
    assert by_id["actionable"] == "actionable"
    assert all(job["status"] in {"needs-review", "done"} for job in jobs)  # untouched


def test_default_evidence_summary_variants():
    assert default_evidence_summary({"failure_reason": "boom"}, {}) == "boom"
    assert default_evidence_summary({}, {"reviewer": {"verdict": "CHANGES"}}) == "review=changes"
    assert default_evidence_summary({}, {"review_evidence": "x"}) == "review_evidence"
    assert default_evidence_summary({}, {}) == "none"


def test_line_matches_nothing_without_regexes():
    item = ev("unknown", "some text")
    assert doctor_v2._matches({}, item) is False
    assert doctor_v2._matches({"failure_reason_regex": "zzz"}, item) is False


def test_line_helper_first_line_only():
    assert doctor_v2._line("  first\nsecond  ") == "first"
    assert doctor_v2._line("") == ""
    assert doctor_v2._line(None) == ""


def test_whitespace_error_is_stripped_to_empty_evidence():
    # collect_evidence strips the error, so a whitespace-only error leaves
    # truly empty evidence and R5 (insufficient evidence) fires.
    item = collect_evidence({"id": "j", "kind": "ops", "result": {"error": "   "}})
    assert item.error == ""
    assert item.evidence_text == ""
    assert decide_rules(item, RULES).rule_id == "R5"


def test_load_rules_without_tomllib_raises(monkeypatch):
    monkeypatch.setattr(doctor_v2, "tomllib", None)
    with pytest.raises(RuntimeError):
        load_rules(Path(__file__).parent / "docs" / "doctor_v2_rules.toml")


def test_sweep_handles_string_results():
    jobs = [
        {
            "id": "s",
            "kind": "feat:q",
            "status": "needs-review",
            "updated_at": 1,
            "result": '{"failure_reason": "boom"}',
            "head_branch": "fix/q",
        },
        {
            "id": "bad",
            "kind": "feat:q",
            "status": "needs-review",
            "updated_at": 1,
            "result": "not json",
            "head_branch": "fix/q",
        },
    ]
    rows = sweep_needs_review(jobs, now=100000, older_than_hours=1)
    by_id = {row["job_id"]: row["evidence_summary"] for row in rows}
    assert by_id["s"] == "boom"
    assert by_id["bad"] == "none"


def test_collect_evidence_result_string_and_retry_count():
    item = collect_evidence({"id": "j", "kind": "ops", "result": '{"failure_reason": "x"}', "retry_count": 3})
    assert item.failure_reason == "x"
    assert item.retry_count == 3
    raw = collect_evidence({"id": "j", "kind": "ops", "result": "not json"})
    assert raw.result == {"raw": "not json"}
    assert (
        collect_evidence({"id": "j", "kind": "ops"}).signature == collect_evidence({"id": "j", "kind": "ops"}).signature
    )  # stable
