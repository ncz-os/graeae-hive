"""What the doctor does with a triage decision (2026-10-17).

A live triage of a failed job released the triage:<kind> wrapper itself
back to the queue with eligible_kinds set to the job's own kind: nothing
could claim it, and the original failed job was never retried.
"""

import json

import pytest

from hive.workers import doctor


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
        doctor, "action_release_to_queue", lambda jid, kinds: seen["release"].append((jid, kinds)) or f"released {jid}"
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
        {"action_type": "release_to_queue", "confidence": 0.9, "release_eligible_kinds": ["fix:hive-one"]},
    )
    out = doctor.process_triage_job({"id": "t1", "kind": "triage:fix:hive-one", "description": ""})
    assert calls["resubmit"] == ["fix:hive-one"]
    assert calls["release"] == []
    assert out["released_to_queue"] is False


def test_release_on_a_directly_claimed_job_hands_that_job_back(monkeypatch, calls):
    _llm_decides(monkeypatch, calls, {"action_type": "release_to_queue", "confidence": 0.9})
    out = doctor.process_triage_job({"id": "j1", "kind": "fix:hive-one", "description": ""})
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


def test_cancel_removes_the_failed_jobs(monkeypatch):
    posted = []
    failed = {"jobs": [{"id": "a", "kind": "fix:hive-one"}, {"id": "b", "kind": "fix:other"}]}

    def fake_http(method, path, body=None, timeout=0):
        if method == "GET":
            return 200, failed
        posted.append((path, body))
        return 200, {"deleted": len(body["ids"])}

    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "_http", fake_http)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    out = doctor.action_cancel_jobs("fix:hive-one")
    assert posted == [("/v1/jobs/purge", {"ids": ["a"], "status": "failed", "reason": "doctor-cancel:fix:hive-one"})]
    assert out.startswith("cancelled 1/1")
