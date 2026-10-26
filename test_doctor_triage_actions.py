"""What the doctor does with a triage decision (2026-10-17).

A live triage of a failed job released the triage:<kind> wrapper itself
back to the queue with eligible_kinds set to the job's own kind: nothing
could claim it, and the original failed job was never retried.
"""

import json

import pytest

from hive.workers import doctor, doctor_gates


@pytest.fixture
def calls(monkeypatch):
    seen = {"resubmit": [], "release": [], "http": [], "llm": 0}
    monkeypatch.setattr(doctor, "_seen_job_attempts", {})
    monkeypatch.setattr(doctor, "fetch_agent_registry_summary", lambda: {})
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(
        doctor, "action_resubmit_jobs", lambda bk, n=20: seen["resubmit"].append(bk) or f"requeued {bk}"
    )
    monkeypatch.setattr(
        doctor,
        "_release_job",
        lambda jid, kinds: seen["release"].append((jid, kinds)) or (f"released {jid}", True),
    )
    return seen


def _llm_decides(monkeypatch, calls, action, failed=()):
    monkeypatch.setattr(doctor, "fetch_failed_jobs_for_kind", lambda bk, limit=10: list(failed))

    def fake_llm(prompt, phase="triage"):
        calls["llm"] += 1
        return True, json.dumps(action)

    monkeypatch.setattr(doctor, "invoke_doctor_agent", fake_llm)


def test_release_on_a_triage_wrapper_requeues_the_original_failed_jobs(monkeypatch, calls):
    _llm_decides(
        monkeypatch,
        calls,
        {"action_type": "release_to_queue", "confidence": 0.9, "evidence_quote": "HTTP 503",
         "release_eligible_kinds": ["fix:hive-one"]},
    )
    out = doctor.process_triage_job({"id": "t1", "kind": "triage:fix:hive-one", "description": "",
                                     "failure_reason": "provider 503", "error": "HTTP 503"})
    assert calls["resubmit"] == ["fix:hive-one"]
    assert calls["release"] == []
    assert out["released_to_queue"] is False


def test_release_on_a_directly_claimed_job_hands_that_job_back(monkeypatch, calls):
    _llm_decides(monkeypatch, calls, {"action_type": "release_to_queue", "confidence": 0.9,
                                      "evidence_quote": "HTTP 503"})
    out = doctor.process_triage_job({"id": "j1", "kind": "fix:hive-one", "description": "",
                                     "failure_reason": "provider 503", "error": "HTTP 503"})
    assert calls["release"] == [("j1", None)]
    assert out["released_to_queue"] is True


def test_missing_branch_escalates_without_an_llm_call(monkeypatch, calls):
    failed = [
        {
            "id": "f1",
            "kind": "fix:hive-one",
            "result": {"error": "clone_failed: fatal: Remote branch fix/x not found in upstream origin"},
        }
    ]
    _llm_decides(monkeypatch, calls, {"action_type": "release_to_queue"}, failed)
    out = doctor.process_triage_job({"id": "t1", "kind": "triage:fix:hive-one", "description": ""})
    assert calls["llm"] == 0
    assert out["action"]["action_type"] == "escalate"
    assert calls["resubmit"] == [] and calls["release"] == []


def test_release_kinds_are_real_worker_kinds_and_never_default_to_codex(monkeypatch):
    sent = []
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "_http", lambda m, path, body=None, timeout=0: sent.append(body) or (200, {}))
    doctor.action_release_to_queue("j1", ["fix:hive-one", "doctor", "zeroclaw"])
    assert sent[-1]["eligible_kinds"] == ["zeroclaw"]
    doctor.action_release_to_queue("j2", ["fix:hive-one"])
    assert sent[-1]["eligible_kinds"] == ["zeroclaw", "opencode"]


# ── reviewer round: no_repo_mapping is deployment-dependent ──────────────
def test_no_repo_mapping_on_a_catalog_present_repo_is_not_unschedulable(monkeypatch, calls):
    """A stale worker's no_repo_mapping must NOT be escalated (BLOCKING-1)."""
    monkeypatch.setattr(doctor, "repo_in_catalog", lambda repo: True)
    failed = [
        {"id": "f1", "kind": "fix:hive-one", "repo": "graeae-hive", "result": {"worker_error": "no_repo_mapping"}},
    ]
    _llm_decides(monkeypatch, calls, {"action_type": "release_to_queue", "confidence": 0.9,
                                      "evidence_quote": "no_repo_mapping"}, failed)
    out = doctor.process_triage_job({"id": "t1", "kind": "triage:fix:hive-one", "description": ""})
    assert calls["llm"] == 1, "catalog-present repo must fall through, not escalate"
    assert out["action"]["action_type"] == "release_to_queue"
    assert calls["resubmit"] == ["fix:hive-one"]


def test_no_repo_mapping_on_a_catalog_absent_repo_is_unschedulable(monkeypatch, calls):
    monkeypatch.setattr(doctor, "repo_in_catalog", lambda repo: False)
    failed = [
        {
            "id": "f1",
            "kind": "fix:hive-one",
            "repo": "definitely-absent-zzz",
            "result": {"worker_error": "no_repo_mapping"},
        },
    ]
    _llm_decides(monkeypatch, calls, {"action_type": "release_to_queue"}, failed)
    out = doctor.process_triage_job({"id": "t1", "kind": "triage:fix:hive-one", "description": ""})
    assert calls["llm"] == 0
    assert out["action"]["action_type"] == "escalate"
    assert calls["resubmit"] == [] and calls["release"] == []


def test_retry_cap_escalates_to_needs_review_with_orchestrator_note(monkeypatch, calls):
    monkeypatch.setattr(doctor, "DOCTOR_V2_MODE", "enforce")
    monkeypatch.setattr(doctor, "DOCTOR_V2_RULES", [{
        "id": "R2", "failure_reason_regex": "503", "action": "release_to_queue",
        "max_retries": 3, "retry_scope": "other_host", "note": "provider retry",
    }])
    monkeypatch.setattr(doctor, "_max_same_signature_resubmits", lambda: 0)
    monkeypatch.setattr(doctor, "fetch_failed_jobs_for_kind", lambda *a, **k: [])
    result = doctor.process_triage_job({"id": "t-cap", "kind": "triage:fix:hive-one",
                                        "failure_reason": "provider 503", "error": "HTTP 503"})
    assert result["action"]["action_type"] == "escalate"
    assert result["needs_review"] is True
    assert result["action"]["orchestrator_note"]
    assert calls["resubmit"] == [] and calls["release"] == []


def test_missing_branch_still_escalates_without_the_repo_check(monkeypatch, calls):
    monkeypatch.setattr(doctor, "repo_in_catalog", lambda repo: True)
    failed = [
        {
            "id": "f1",
            "kind": "fix:hive-one",
            "result": {"error": "clone_failed: fatal: Remote branch fix/x not found in upstream origin"},
        },
    ]
    _llm_decides(monkeypatch, calls, {"action_type": "release_to_queue"}, failed)
    out = doctor.process_triage_job({"id": "t1", "kind": "triage:fix:hive-one", "description": ""})
    assert calls["llm"] == 0
    assert out["action"]["action_type"] == "escalate"


# ── reviewer round: release reports actual HTTP outcome ──────────────────
def test_apply_release_reports_failure_when_the_patch_fails(monkeypatch):
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "_http", lambda m, path, body=None, timeout=0: (404, {"detail": "job not found"}))
    text, released = doctor.apply_release("j1", False, "fix:hive-one", None)
    assert released is False, "a failed release must not claim the job was requeued"
    assert "FAILED" in text


def test_apply_release_reports_success_when_the_patch_succeeds(monkeypatch):
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "_http", lambda m, path, body=None, timeout=0: (200, {}))
    text, released = doctor.apply_release("j1", False, "fix:hive-one", None)
    assert released is True


# ── reviewer round: doctor must not release itself on its own failure ────
def test_doctor_kind_never_self_releases_on_its_own_failure(monkeypatch, calls):
    """A doctor:* job reading its own stored result must not see a transient
    signature and release itself (that skipped the terminal PATCH)."""
    self_failed = {
        "id": "d1",
        "kind": "doctor:wakeup:[fix:hive-one]",
        "result": {"exit_code": 2, "stdout": "doctor agent failed", "stderr": "ws_exception: ConnectionClosed"},
    }
    _llm_decides(monkeypatch, calls, {"action_type": "no_action", "confidence": 0.9}, [self_failed])
    out = doctor.process_triage_job({"id": "d1", "kind": "doctor:wakeup:[fix:hive-one]", "description": "wakeup"})
    assert out["action"]["action_type"] == "no_action"
    assert calls["release"] == []
    assert out["released_to_queue"] is False


# ── reviewer round: fleet harness kinds are releasable ──────────────────
def test_release_kinds_include_zoder_and_claude(monkeypatch):
    sent = []
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "_http", lambda m, path, body=None, timeout=0: sent.append(body) or (200, {}))
    doctor.action_release_to_queue("j1", ["zoder", "claude", "doctor", "fix:hive-one"])
    assert sent[-1]["eligible_kinds"] == ["zoder", "claude"]


def test_release_kinds_never_include_doctor_even_if_only_doctor_is_offered(monkeypatch):
    sent = []
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "_http", lambda m, path, body=None, timeout=0: sent.append(body) or (200, {}))
    doctor.action_release_to_queue("j1", ["doctor"])
    assert "doctor" not in sent[-1]["eligible_kinds"]
    assert sent[-1]["eligible_kinds"] == doctor.DEFAULT_RELEASE_KINDS


def _approve_cancel(base_kind, job_ids):
    """Record the operator approval `action_cancel_jobs` now requires.

    A purge is a hard delete of up to 20 jobs and every child job, so it waits for
    a human (2026-10-22). The approval is scoped to the exact id set, which is why
    this is a helper and not a blanket `auto_approve=True`: the tests below still
    prove that a DIFFERENT id set would be refused.
    """
    digest = doctor_gates.action_digest("cancel_jobs", base_kind=base_kind, job_ids=sorted(job_ids))
    doctor.APPROVALS.record_approval("cancel_jobs", digest)


def test_cancel_removes_the_failed_jobs(monkeypatch):
    posted = []
    failed = {"jobs": [{"id": "a", "kind": "fix:hive-one"}, {"id": "b", "kind": "fix:other"}]}

    def fake_http(method, path, body=None, timeout=0):
        if method == "GET":
            if path.startswith("/v1/jobs?parent_job_id="):
                return 200, {"jobs": []}
            return 200, failed
        posted.append((path, body))
        return 200, {"deleted": len(body["ids"]), "matched_ids": body["ids"]}

    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "_http", fake_http)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    _approve_cancel("fix:hive-one", ["a"])
    out = doctor.action_cancel_jobs("fix:hive-one")
    assert posted == [("/v1/jobs/purge", {"ids": ["a"], "status": "failed", "reason": "doctor-cancel:fix:hive-one"})]
    assert out.startswith("cancelled 1/1")


def test_cancel_without_an_approval_deletes_nothing(monkeypatch):
    """The same call with no approval recorded. A purge used to happen at any
    confidence; now it refuses and names the digest an approval needs."""
    posted = []
    failed = {"jobs": [{"id": "a", "kind": "fix:hive-one"}]}

    def fake_http(method, path, body=None, timeout=0):
        if method == "GET":
            if path.startswith("/v1/jobs?parent_job_id="):
                return 200, {"jobs": []}
            return 200, failed
        posted.append((path, body))
        return 200, {"deleted": 0, "matched_ids": []}

    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "_http", fake_http)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    out = doctor.action_cancel_jobs("fix:hive-one")
    assert posted == [], f"nothing may be purged without an approval, got {posted}"
    assert out.startswith("REFUSED"), out


def test_cancel_also_cancels_child_jobs_of_the_purged_parents(monkeypatch):
    """Purging a parent must not orphan its children in the queue."""
    failed = {"jobs": [{"id": "a", "kind": "fix:hive-one"}]}
    children = {
        "jobs": [
            {"id": "child1", "kind": "fix:hive-one", "parent_job_id": "a"},
            {"id": "child2", "kind": "fix:hive-one", "parent_job_id": "zzz"},
        ]
    }
    calls = []

    def fake_http(method, path, body=None, timeout=0):
        if method == "GET":
            if path.startswith("/v1/jobs?parent_job_id="):
                pid = path.split("parent_job_id=")[1].split("&")[0]
                return 200, {"jobs": [c for c in children["jobs"] if c["parent_job_id"] == pid]}
            return 200, failed
        calls.append((path, body))
        return 200, {"deleted": 1, "matched_ids": body["ids"]}

    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "_http", fake_http)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    # The approval covers the PARENT AND ITS CHILDREN, so it is recorded for the
    # id set the executor is about to purge — not for the parent's alone.
    _approve_cancel("fix:hive-one", ["a", "child1"])
    out = doctor.action_cancel_jobs("fix:hive-one")
    # Only child1 descends from a purged parent; child2 is left alone.
    purge = [c for c in calls if c[0] == "/v1/jobs/purge"]
    assert purge and purge[0][1]["ids"] == ["a", "child1"]
    assert "child1" in out and "child2" not in out
