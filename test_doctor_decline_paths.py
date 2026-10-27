"""Deterministic decline-vocabulary paths (2026-10-27).

One test per registry row the doctor owns (docs/DETERMINISTIC-PATHS.md):
the decision, that no invoke_doctor_agent call can follow it, and — for the
routing-signal rows — that the bus decline counter is never incremented.
No live bus: every HTTP surface is monkeypatched.
"""

from __future__ import annotations

import json

import pytest

from hive.bus.domain.jobs import is_unresolvable_release
from hive.workers import doctor
from hive.workers import doctor_decline_paths as dp

CATALOG_REPOS = (
    {
        "id": "graeae-hive",
        "display_name": "GRAEAE Hive",
        "aliases": ["hive", "hive-hive"],
    },
    {
        "id": "mnemos",
        "display_name": "MNEMOS",
        "aliases": [],
    },
)


@pytest.fixture
def calls(monkeypatch):
    """Every surface that could hide a model call or a bus write, captured."""
    seen = {"llm": 0, "http": [], "messages": [], "resubmit_posted": []}

    def fake_llm(prompt, phase="triage"):
        seen["llm"] += 1
        return True, json.dumps({"action_type": "release_to_queue", "confidence": 0.9, "evidence_quote": "x"})

    def fake_http(method, path, body=None, timeout=0):
        seen["http"].append((method, path, body))
        if method == "GET" and path.startswith("/v1/jobs?status=failed"):
            return 200, {"jobs": []}
        if method == "GET" and path.startswith("/v1/jobs?status=dead-letter"):
            return 200, {"jobs": []}
        if method == "POST" and path == "/v1/jobs":
            seen["resubmit_posted"].append(body)
            return 201, {"id": "new-job-0001"}
        return 200, {}

    def fake_broadcast(topic, payload):
        seen["messages"].append((topic, payload))

    monkeypatch.setattr(doctor, "invoke_doctor_agent", fake_llm)
    monkeypatch.setattr(doctor, "_http", fake_http)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", fake_broadcast)
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "fetch_agent_registry_summary", lambda: {})
    return seen


def _job(kind, **extra):
    base = {"id": "job-0001", "kind": kind, "description": "", "base_kind": kind}
    base.update(extra)
    return base


def _repo_fake(monkeypatch, capture=None):
    """A fake catalog lookup + /v1/repos answer.

    When `capture` (a dict) is passed, every HTTP call is recorded in it:
    capture["http"] is a list of (method, path, body) and capture["resubmit_posted"]
    is a list of the POST /v1/jobs bodies, so a test can assert the resubmit
    was actually submitted exactly once with the canonical id.
    """
    def fake_http(method, path, body=None, timeout=0):
        if capture is not None:
            capture["http"].append((method, path, body))
        if method == "GET" and path == "/v1/repos":
            return 200, {"count": len(CATALOG_REPOS), "repos": CATALOG_REPOS}
        if method == "POST" and path == "/v1/jobs" and capture is not None:
            capture["resubmit_posted"].append(body)
            return 201, {"id": "new-job-0001"}
        return 200, {}

    monkeypatch.setattr(doctor, "_http", fake_http)

    def fake_find_exact(repo_id):
        key = (repo_id or "").strip().lower()
        for entry in CATALOG_REPOS:
            names = {entry["id"].lower()} | {a.lower() for a in entry.get("aliases") or []}
            if key in names:
                return entry
        return None

    import hive.repos

    monkeypatch.setattr(hive.repos, "find_exact", fake_find_exact)


# ── no_workspace_for_repo:<repo> ────────────────────────────────────────────

def test_no_workspace_for_repo_alias_resubmits_with_canonical_id(calls, monkeypatch):
    """An alias resolves to the catalog id; the resubmit carries the canonical
    id and leaves target_workspace unset."""
    _repo_fake(monkeypatch, capture=calls)
    job = _job("fix:hive-one", result={"worker_error": "no_workspace_for_repo:hive", "repo": "hive"})
    out = doctor._handle_decline_job(job, "fix:hive-one")
    assert out is not None
    assert out["action"]["action_type"] == "resubmit"
    assert calls["llm"] == 0, "a classified decline row must never reach the LLM"
    body = out["action"]["resubmit_body"]
    assert body["repo"] == "graeae-hive", "the resubmit must use the canonical catalog id"
    assert "target_workspace" not in body, "target_workspace stays unset"
    # The resubmit was actually submitted — exactly once, with the canonical id.
    assert calls["resubmit_posted"] and calls["resubmit_posted"][0]["repo"] == "graeae-hive"
    assert "target_workspace" not in calls["resubmit_posted"][0]
    # The durable resubmit key says this job will be resubmitted at most once.
    assert len(doctor._signature_resubmits("fix:hive-one", "decline:job-0001")) == 1
    # Second pass over the same row: held, no new submit.
    out2 = doctor._handle_decline_job(job, "fix:hive-one")
    assert out2["action"]["already_resubmitted"] is True
    assert len(calls["resubmit_posted"]) == 1


def test_no_workspace_for_repo_absent_is_needs_review_with_repo_named(calls, monkeypatch):
    """A repo the catalog does not hold is an operator question, never a
    resubmit, and the repo name rides on the reason."""
    import hive.repos

    monkeypatch.setattr(hive.repos, "find_exact", lambda repo_id: None)
    job = _job("fix:hive-one", result={"worker_error": "no_workspace_for_repo:zzz-not-a-repo"})
    out = doctor._handle_decline_job(job, "fix:hive-one")
    assert out["action"]["action_type"] == "needs_review"
    assert out["needs_review"] is True
    assert "zzz-not-a-repo" in out["action"]["reason"], "the repo name must be in the reason"
    assert calls["llm"] == 0
    assert calls["http"] == [], "no resubmit of an unresolvable repo"
    assert out["needs_review_result"]["needs_operator"] is True


def test_no_workspace_for_repo_exact_id_resubmits(calls, monkeypatch):
    _repo_fake(monkeypatch)
    job = _job("fix:hive-one", result={"worker_error": "no_workspace_for_repo:mnemos"})
    out = doctor._handle_decline_job(job, "fix:hive-one")
    assert out["action"]["action_type"] == "resubmit"
    assert out["action"]["resubmit_body"]["repo"] == "mnemos"
    assert calls["llm"] == 0


# ── no_workspace_for_kind:<kind> ────────────────────────────────────────────

def test_no_workspace_for_kind_is_a_routing_signal_logged_once(calls):
    job = _job("fix:hive-one", result={"worker_error": "no_workspace_for_kind:code"})
    out = doctor._handle_decline_job(job, "fix:hive-one")
    assert out["action"]["action_type"] == "no_action"
    assert out["action"].get("routing_count") == 1
    assert out["needs_review"] is False
    assert calls["llm"] == 0
    assert calls["http"] == [], "a routing signal is logged, not requeued or dead-lettered by the doctor"
    # Documented split: the BUS still counts the kind reason toward its own
    # decline threshold (test_decline_reasons.py pins that); the DOCTOR never
    # acts on it and never dead-letters it — that is what this test pins.
    assert is_unresolvable_release("no_workspace_for_kind:code") is True


def test_no_workspace_for_kind_escalates_at_three_in_six_hours(calls, monkeypatch):
    from hive.workers.doctor_state import DurableStateStore, MemoryBackend

    clock = {"now": 1_000_000.0}
    store = DurableStateStore(MemoryBackend(), refresh_secs=0.0, time_fn=lambda: clock["now"])
    monkeypatch.setattr(doctor, "STATE", store)
    job = _job("fix:hive-one", result={"worker_error": "no_workspace_for_kind:code"})
    out1 = doctor._handle_decline_job(job, "fix:hive-one")
    assert out1["action"]["action_type"] == "no_action"
    assert out1["action"]["routing_count"] == 1
    clock["now"] += 3600
    out2 = doctor._handle_decline_job(job, "fix:hive-one")
    assert out2["action"]["routing_count"] == 2
    assert out2["action"]["action_type"] == "no_action"
    clock["now"] += 3600
    out3 = doctor._handle_decline_job(job, "fix:hive-one")
    assert out3["action"]["routing_count"] == 3
    assert out3["action"]["action_type"] == "needs_review"
    assert out3["needs_review"] is True
    assert "3" in out3["action"]["reason"]
    # The escalation is a durable-key decision: the count survived all three
    # passes under the same signature.
    key = f"routing_signal:fix:hive-one|no_workspace_for_kind:code"
    assert len(doctor.STATE.timestamps(key, dp.ROUTING_SIGNAL_WINDOW_SEC)) == 3


def test_no_workspace_for_kind_below_threshold_is_not_needs_review(calls):
    job = _job("fix:hive-one", result={"worker_error": "no_workspace_for_kind:code"})
    out = doctor._handle_decline_job(job, "fix:hive-one")
    assert out["needs_review"] is False


# ── host routing signals ────────────────────────────────────────────────────

def test_host_declines_kind_is_never_counted_or_acted(calls):
    for reason in ("host_declines_kind:ultra", "host_declines_kind", "released_by_host"):
        job = _job("fix:hive-one", result={"worker_error": reason})
        out = doctor._handle_decline_job(job, "fix:hive-one")
        assert out["action"]["action_type"] == "no_action", reason
        assert out["needs_review"] is False, reason
        assert out["action"].get("routing_count") == 1, reason
        assert is_unresolvable_release(reason) is False, f"{reason!r} must stay OUT of the decline count"
    assert calls["llm"] == 0, "a routing signal must never reach the LLM"
    assert calls["http"] == [], "a routing signal is logged, not requeued or dead-lettered"
    # Logged ONCE per signature: three different reasons, three signatures,
    # one sighting each.
    assert len(doctor.STATE.keys(dp.KEY_ROUTING_SIGNAL)) == 3


def test_host_declines_kind_logs_once_per_signature(calls):
    """Log-once semantics: a second sighting of the SAME signature does not
    log a fresh entry, but a DIFFERENT signature does."""
    job = _job("fix:hive-one", result={"worker_error": "host_declines_kind:ultra"})
    out1 = doctor._handle_decline_job(job, "fix:hive-one")
    assert out1["action"]["routing_count"] == 1
    out2 = doctor._handle_decline_job(job, "fix:hive-one")
    assert out2["action"]["routing_count"] == 2
    other = _job("fix:hive-one", result={"worker_error": "host_declines_kind:hydra"})
    out3 = doctor._handle_decline_job(other, "fix:hive-one")
    assert out3["action"]["routing_count"] == 1, "a different signature starts its own tally"
    # The bus decline counter stays at zero for all of them.
    assert is_unresolvable_release("host_declines_kind:ultra") is False
    assert is_unresolvable_release("host_declines_kind:hydra") is False


# ── invalid_head_branch ─────────────────────────────────────────────────────

def test_invalid_head_branch_resubmits_sanitized_once(calls):
    job = _job(
        "fix:hive-one",
        head_branch="-fix/weird/name",
        base_branch="main",
        result={"worker_error": "invalid_head_branch:fix/weird/name"},
    )
    out = doctor._handle_decline_job(job, "fix:hive-one")
    assert out["action"]["action_type"] == "resubmit"
    branch = out["action"]["resubmit_body"]["head_branch"]
    assert not branch.startswith("-"), "a leading '-' can never reach git"
    assert branch == branch.lower()
    assert len(branch) <= 60
    assert branch.startswith("fix-hive-one"), "the kind's slug must prefix the sanitized branch"
    assert calls["llm"] == 0
    # A second pass over the same row cannot resubmit again.
    out2 = doctor._handle_decline_job(job, "fix:hive-one")
    assert out2["action"]["already_resubmitted"] is True


def test_invalid_head_branch_keeps_branch_created_from(calls):
    job = _job(
        "fix:hive-one",
        head_branch="-fix/x",
        branch_created_from="release/2.0",
        result={"worker_error": "invalid_head_branch"},
    )
    out = doctor._handle_decline_job(job, "fix:hive-one")
    body = out["action"]["resubmit_body"]
    assert body["branch_created_from"] == "release/2.0", "an existing branch_created_from must be kept"
    assert body["head_branch"].startswith("fix-hive-one")


def test_invalid_head_branch_without_created_from_sets_none(calls):
    job = _job("fix:hive-one", head_branch="-fix/x", result={"worker_error": "invalid_head_branch"})
    out = doctor._handle_decline_job(job, "fix:hive-one")
    assert "branch_created_from" not in out["action"]["resubmit_body"]
    assert out["action"]["resubmit_body"]["head_branch"] == "fix-hive-one-fix-x"


# ── attribution / gate-fail-closed ──────────────────────────────────────────

def test_attribution_violation_is_never_retried_and_sends_one_message(calls):
    job = _job(
        "fix:hive-one",
        result={
            "failure_reason": "ai_attribution_trailer",
            "attribution": {
                "status": "violation",
                "reason": "Co-authored-by: Claude <noreply@example.com> in commit 1234abcd",
                "commits": ["1234abcd"],
            },
        },
    )
    out = doctor._handle_decline_job(job, "fix:hive-one")
    assert out["action"]["action_type"] == "needs_review"
    assert out["needs_review"] is True
    assert out["needs_review_result"]["needs_operator"] is True
    assert "1234abcd" in out["action"]["reason"], "the violating commit must be in the result"
    topics = [t for t, _ in calls["messages"]]
    assert topics.count("doctor.attribution_violation") == 1, "exactly one doctor.attribution_violation message"
    assert calls["llm"] == 0, "an attribution violation is never retried, hence never model-diagnosed"
    assert calls["http"] == [], "no resubmit of an attribution violation"


def test_attribution_check_failed_also_never_retried(calls):
    job = _job("fix:hive-one", result={"failure_reason": "attribution_check_failed", "error": "attribution git check failed: timeout"})
    out = doctor._handle_decline_job(job, "fix:hive-one")
    assert out["action"]["action_type"] == "needs_review"
    assert out["needs_review_result"]["needs_operator"] is True
    assert calls["llm"] == 0


# ── dlq_protected rows ──────────────────────────────────────────────────────

def test_dlq_protected_reason_is_log_only_and_never_llm(calls, monkeypatch):
    """A row whose decline reason is `dlq_protected` is logged and left alone:
    no action, no resubmit, no LLM call."""
    job = _job("fix:hive-one", result={"worker_error": "dlq_protected"})
    out = doctor._handle_decline_job(job, "fix:hive-one")
    assert out is not None
    assert out["action"]["action_type"] == "no_action"
    assert out["action"].get("log_only") is True
    assert out["needs_review"] is False
    assert calls["llm"] == 0, "a protected row must never reach the LLM"
    assert calls["http"] == [], "a protected row is never requeued or resubmitted"


def test_dlq_protected_kind_is_log_only_and_never_llm(calls, monkeypatch):
    """The same row protected by its KIND prefix (not the reason string) is
    also log-only: the classifier checks the job, not just the reason."""
    import importlib

    monkeypatch.setenv("DLQ_PROTECT_KIND_PREFIXES", "ops:critical")
    reloaded = importlib.reload(doctor)
    try:
        monkeypatch.setattr(reloaded, "broadcast_doctor_message", lambda *a, **k: None)
        job = _job("ops:critical:thing", result={"worker_error": "no_workspace_for_repo:whatever"})
        assert reloaded.dlq_protected(job) is True
        # The triage path refuses protected rows before any classification.
        out = reloaded.process_triage_job(job)
        assert out["action"]["action_type"] == "escalate"
        assert "protected" in out["stdout"]
        assert calls["llm"] == 0
        assert calls["http"] == []
    finally:
        importlib.reload(doctor)


# ── end-to-end: the triage loop classifies before any model call ───────────

def test_process_triage_job_never_calls_the_llm_for_a_classified_decline(calls, monkeypatch):
    job = _job(
        "fix:hive-one",
        description="1 fail",
        result={"worker_error": "no_workspace_for_repo:hive"},
        head_branch="fix/hive-one",
    )
    _repo_fake(monkeypatch, capture=calls)
    out = doctor.process_triage_job(job)
    assert calls["llm"] == 0
    assert out["action"]["action_type"] == "resubmit"


def test_unowned_decline_reason_escalates_instead_of_vanishing(calls):
    job = _job("fix:hive-one", result={"worker_error": "some_new_reason_seen_in_the_wild"})
    out = doctor._handle_decline_job(job, "fix:hive-one")
    assert out["action"]["action_type"] == "needs_review"
    assert out["needs_review"] is True
    assert out["needs_review_result"]["needs_operator"] is True
    assert calls["llm"] == 0


def test_no_decline_reason_falls_through_to_the_llm_path(calls):
    """A job without a decline reason must NOT be touched by the decline table."""
    job = _job("fix:hive-one", result={"error": "HTTP 503"})
    assert doctor._handle_decline_job(job, "fix:hive-one") is None


# ── the bus-side counter: routing signals never increment it ────────────────

def test_bus_classifier_keeps_host_signals_out_of_the_decline_count():
    """The existing bus behaviour, pinned here because the registry names it:
    host_declines_kind / released_by_host never increment decline_count, while
    the job-level reasons still do."""
    assert is_unresolvable_release("host_declines_kind:ultra") is False
    assert is_unresolvable_release("host_declines_kind") is False
    assert is_unresolvable_release("released_by_host") is False
    assert is_unresolvable_release("invalid_head_branch") is True
    assert is_unresolvable_release("invalid_head_branch:fix/x") is True


@pytest.mark.parametrize(
    "reason",
    [
        "no_workspace_for_repo:graeae-hive",
        "no_workspace_for_repo:zzz-not-a-repo",
        "no_workspace_for_kind:code",
        "host_declines_kind:ultra",
        "released_by_host",
        "invalid_head_branch",
        "attribution_check_failed",
        "ai_attribution_trailer",
        "gate_fail_closed",
    ],
)
def test_classifier_returns_a_bounded_decision_for_every_registry_row(calls, reason):
    decision = dp.classify_decline_reason(reason, _job("fix:hive-one", result={"worker_error": reason}))
    assert decision.action in {"no_action", "no_action_log", "resubmit", "needs_review", "log_only"}
    assert decision.reason
