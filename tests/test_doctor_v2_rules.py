from pathlib import Path

from hive.workers.doctor_v2 import (
    bs_audit_signal,
    collect_evidence,
    decide_rules,
    load_rules,
    normalize,
    validate_llm_decision,
    shadow_report,
    sweep_needs_review,
)


RULES = load_rules(Path(__file__).parents[1] / "docs" / "doctor_v2_rules.toml")


def evidence(reason, text, *, host="CERBERUS", prior=()):
    return collect_evidence({"id": "j1", "kind": "build:x", "failure_reason": reason,
                             "head_branch": "fix/x", "host": host,
                             "result": {"failure_reason": reason, "tail": text}},
                            prior_actions=prior)


def test_rule_table_covers_initial_rules_and_is_deterministic():
    cases = [
        ("clone_failed", "Remote branch fix/x not found", "R1"),
        ("provider 503", "HTTP 503", "R2"),
        ("codex_delivery_failed", "verdict: delivery failed", "R3"),
        ("unhandled_exception", "Traceback exception", "R4"),
        ("loop_unresolved", "session-cancel timeout", "R6"),
        ("loop_unresolved", "pytest failed tests: test_x", "R7"),
        ("host", "OOM-killed worker", "R8"),
        ("engine", "Budget exceeded: $25 of $25 Day limit", "R9"),
    ]
    for reason, text, rule_id in cases:
        item = evidence(reason, text)
        first = decide_rules(item, RULES)
        second = decide_rules(item, RULES)
        assert first and first.rule_id == rule_id
        assert first == second
        assert first.signature == item.signature


def test_same_signature_is_never_requeued_more_than_twice():
    item = evidence("provider 503", "HTTP 503", prior=({"signature": "x"},))
    # Use the actual signature as the prior lineage record.
    item = evidence("provider 503", "HTTP 503")
    repeated = evidence("provider 503", "HTTP 503", prior=({"signature": item.signature}, {"signature": item.signature}))
    decision = decide_rules(repeated, RULES)
    assert decision.action == "escalate"
    assert decision.reason == "same signature reached global retry cap"


def test_r9_is_host_terminal_and_evidence_quote_is_verbatim():
    item = evidence("engine", "Budget exceeded: $25 of $25 Day limit")
    decision = decide_rules(item, RULES)
    assert decision.action == "escalate"
    assert "CERBERUS" in decision.reason
    assert decision.evidence_quote in item.evidence_text


def test_llm_quote_must_be_a_substring():
    item = evidence("unknown", "real evidence line")
    bad = validate_llm_decision({"action": "release_to_queue", "evidence_quote": "invented"}, item)
    assert bad.action == "escalate"
    good = validate_llm_decision({"action": "release_to_queue", "evidence_quote": "real evidence line", "max_retries": 99}, item)
    assert good.action == "release_to_queue"
    assert good.max_retries == 2


def test_normalization_removes_volatile_parts():
    assert normalize("id deadbeef1234 at /var/lib/hive/42 2026-10-25T12:00:00Z") == "id <hex> at <path> <time>"


def test_bs_audit_is_a_rule_input_and_sweep_is_report_only():
    assert bs_audit_signal({"kind": "fix:x", "description": "implement auth", "result": {"output": "done"}}) == "BS:empty+nocommit"
    jobs = [
        {"id": "old", "kind": "build:x", "status": "needs-review", "updated_at": 1, "head_branch": "fix/x"},
        {"id": "new", "kind": "build:x", "status": "done", "updated_at": 2},
    ]
    rows = sweep_needs_review(jobs, now=100000, older_than_hours=1)
    assert rows[0]["classification"] == "superseded"
    assert jobs[0]["status"] == "needs-review"


def test_shadow_report_counts_both_outcomes():
    assert shadow_report([{"agree": True}, {"agree": False}, {"agree": False}]) == {
        "total": 3, "agreement": 1, "disagreement": 2
    }
