"""The doctor handles protection_violation and needs-review rows immediately.

Two live gaps (2026-10-27), both fixed here:

  1. ``build:blackhole-flow-20261027`` failed with
     ``failure_reason=protection_violation`` ("driver modified another
     session's checkout: ..."). The attempt delivered nothing (commits=[]),
     but the row sat failed and the triage the doctor created for it
     escalated with an unusable LLM decision and stopped. The doctor must now
     handle the row deterministically — no LLM decision in the path: purge the
     empty failed row and re-dispatch the same work once, excluding the
     offending host, capped durably per original job id.

  2. A needs-review row with no review performed (no result.review_evidence
     and no child review:* job) used to sit there forever. The needs-review
     sweep now submits ONE read-only codex review job per such row, oldest
     first, capped per sweep. A row with a verdict is left for a human, and a
     needs-review row is never auto-purged (commits are delivered work).

All bus traffic is mocked at doctor._http; nothing touches a socket.
"""

import time

import pytest

from hive.workers import doctor
from hive.workers.doctor_state import DurableStateStore, FileBackend, MemoryBackend

NOW = 1_761_600_000.0  # fixed clock, so the TTL windows never race the test


def _store(tmp_path=None):
    """A fresh in-memory store (or file-backed when a path is given)."""
    if tmp_path is None:
        return DurableStateStore(MemoryBackend(), refresh_secs=0.0)
    return DurableStateStore(FileBackend(str(tmp_path / "state.json")), refresh_secs=0.0)


_NO_CLAIMANT = object()


def _violation_job(jid="orig-1", kind="build:blackhole-flow-20261027", host="ultra",
                   claimed_by=_NO_CLAIMANT, commits=None, **extra):
    error = "driver modified another session's checkout: /home/jasonperlow/Projects/p4/cix-installer"
    if claimed_by is _NO_CLAIMANT:
        claimed_by = f"urn:agent:codex:{host}:sess-1"
    return {
        "id": jid,
        "kind": kind,
        "status": "failed",
        "result": {"failure_reason": "protection_violation", "error": error, "commits": commits or []},
        "claimed_by": claimed_by,
        "repo": "graeae/graeae-hive",
        "head_branch": "fix/x",
        "base_branch": "master",
        "project": "graeae",
        "priority": 5,
        "eligible_kinds": ["codex"],
        "preferred_models": ["gpt-6-luna"],
        "max_cost_tier": "A",
        "eligible_hosts": ["ultra", "hydra", "achilles"],
        **extra,
    }


# ═══════════════════════════════ PART A ═══════════════════════════════


def test_detection_from_failure_reason(monkeypatch):
    monkeypatch.setattr(doctor, "STATE", _store())
    assert doctor.is_protection_violation(
        {"result": {"failure_reason": "protection_violation"}}
    )


def test_detection_from_error_string(monkeypatch):
    monkeypatch.setattr(doctor, "STATE", _store())
    assert doctor.is_protection_violation(
        {"result": {"error": "driver modified another session's checkout: /x (protection_violation)"}}
    )
    # The token must actually be present, not just a similar string.
    assert not doctor.is_protection_violation({"result": {"error": "driver modified its own checkout"}})


def test_host_parsed_from_claimed_by_and_path(monkeypatch):
    monkeypatch.setattr(doctor, "STATE", _store())
    job = _violation_job(host="ULTRA")
    assert doctor.protection_offending_host(job) == "ultra"
    assert doctor.protection_offending_path(job).endswith("cix-installer")


def test_purge_once_and_redispatch_with_host_excluded(monkeypatch):
    calls = []
    state = _store()

    def _http(method, path, body=None, **kw):
        calls.append((method, path, body))
        if method == "POST" and path == "/v1/admin/jobs/purge":
            return 200, {"ok": [body["job_ids"][0]]}
        if method == "POST" and path == "/v1/jobs":
            if "retry" in (body or {}).get("kind", ""):
                return 201, {"id": "new-retry-job"}
            return 201, {}
        if method == "PATCH":
            return 200, {}
        return 200, {}

    monkeypatch.setattr(doctor, "STATE", state)
    monkeypatch.setattr(doctor, "_urn", "urn:agent:doctor:pythia:s")
    monkeypatch.setattr(doctor, "_http", _http)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    doctor.handle_protection_violation(_violation_job())

    purges = [c for c in calls if c[0] == "POST" and c[1] == "/v1/admin/jobs/purge"]
    submits = [c for c in calls if c[0] == "POST" and c[1] == "/v1/jobs"]
    assert len(purges) == 1, "the empty failed row is purged exactly once"
    assert len(submits) == 1
    spec = submits[0][2]
    assert spec["kind"] == "build:blackhole-flow-20261027-retry"
    assert spec["eligible_hosts"] == ["hydra", "achilles"], "the offending host is excluded"
    assert "protection_violation" in spec["description"] and "ultra" in spec["description"]
    assert spec["idempotency_key"] == "protection-violation-retry:orig-1"
    assert spec["repo"] == "graeae/graeae-hive" and spec["head_branch"] == "fix/x"
    assert spec["max_cost_tier"] == "A" and spec["eligible_kinds"] == ["codex"]
    # The durable marker is set only after a successful submit.
    assert doctor.protection_violation_retried("orig-1")


def test_max_retries_cap_enforced_across_restart(tmp_path, monkeypatch):
    """A restart must not re-purge or re-dispatch past the durable cap."""
    state = _store(tmp_path)
    monkeypatch.setattr(doctor, "STATE", state)
    monkeypatch.setattr(doctor, "_urn", "urn:agent:doctor:pythia:s")
    monkeypatch.setattr(doctor, "VIOLATION_MAX_RETRIES", 1)

    def _http1(method, path, body=None, **kw):
        if method == "POST" and path == "/v1/admin/jobs/purge":
            return 200, {"ok": [body["job_ids"][0]]}
        if method == "POST" and path == "/v1/jobs":
            return 201, {"id": "n1"}
        return 200, {}

    monkeypatch.setattr(doctor, "_http", _http1)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    doctor.handle_protection_violation(_violation_job())

    # Simulate a restart: a brand-new store over the same file backend.
    state2 = DurableStateStore(FileBackend(str(tmp_path / "state.json")), refresh_secs=0.0)
    monkeypatch.setattr(doctor, "STATE", state2)
    calls = []

    def _http2(method, path, body=None, **kw):
        calls.append((method, path, body))
        if method == "POST" and path == "/v1/admin/jobs/purge":
            return 200, {"ok": [body["job_ids"][0]]}
        if method == "POST" and path == "/v1/jobs":
            return 201, {"id": "n2"}
        return 200, {}

    monkeypatch.setattr(doctor, "_http", _http2)
    doctor.handle_protection_violation(_violation_job())
    assert not [c for c in calls if c[0] == "POST" and c[1] == "/v1/jobs"], (
        "the durable cap (1) was reached, so a restart must not re-dispatch"
    )


def test_unparseable_host_no_redispatch_and_needs_operator(monkeypatch):
    calls = []
    msgs = []

    def _http(method, path, body=None, **kw):
        calls.append((method, path, body))
        if method == "POST" and path == "/v1/admin/jobs/purge":
            return 200, {"ok": [body["job_ids"][0]]}
        return 200, {}

    monkeypatch.setattr(doctor, "STATE", _store())
    monkeypatch.setattr(doctor, "_urn", "urn:agent:doctor:pythia:s")
    monkeypatch.setattr(doctor, "_http", _http)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda topic, payload: msgs.append((topic, payload)))
    job = _violation_job(claimed_by=None)
    reason = doctor.handle_protection_violation(job)
    assert not [c for c in calls if c[0] == "POST" and c[1] == "/v1/jobs"], "no re-dispatch"
    topic, payload = msgs[-1]
    assert topic == "doctor.protection_violation" and payload["needs_operator"] is True
    assert "unparseable" in reason


def test_job_with_commits_is_never_purged_or_redispatched(monkeypatch):
    calls = []

    def _http(method, path, body=None, **kw):
        calls.append((method, path, body))
        return 200, {}

    monkeypatch.setattr(doctor, "STATE", _store())
    monkeypatch.setattr(doctor, "_urn", "urn:agent:doctor:pythia:s")
    monkeypatch.setattr(doctor, "_http", _http)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    job = _violation_job(commits=["abc123"])
    doctor.handle_protection_violation(job)
    assert not [c for c in calls if c[0] == "POST" and c[1] == "/v1/admin/jobs/purge"], "delivered work is not purged"
    assert not [c for c in calls if c[0] == "POST" and c[1] == "/v1/jobs"], "delivered work is not re-dispatched"


# ═══════════════════════════════ PART B ═══════════════════════════════


def _nr(jid="nr-1", kind="fix:hive-one", age=60, commits=None, **extra):
    return {
        "id": jid,
        "kind": kind,
        "status": "needs-review",
        "result": {"commits": commits or []},
        "ended_at": NOW - age,
        "repo": "graeae/graeae-hive",
        "head_branch": "fix/one",
        "base_branch": "master",
        **extra,
    }


def _wire_sweep(monkeypatch, rows, submitted_ids=None):
    """Mock the bus for the needs-review sweep. Returns a list of submitted specs."""
    submitted = []
    counter = {"n": 0}
    submitted_ids = submitted_ids or []

    def _http(method, path, body=None, **kw):
        if method == "GET" and path.startswith("/v1/jobs?status=needs-review"):
            return 200, {"jobs": rows}
        if method == "GET" and "parent_job_id=" in path:
            return 200, {"jobs": []}
        if method == "POST" and path == "/v1/jobs":
            counter["n"] += 1
            submitted.append(body)
            kind = body.get("kind") or ""
            return 201, {"id": f"review-{counter['n']}" if kind.startswith("review:") else f"other-{counter['n']}"}
        if method == "PATCH":
            return 200, {}
        return 200, {}

    monkeypatch.setattr(doctor, "STATE", _store())
    monkeypatch.setattr(doctor, "_urn", "urn:agent:doctor:pythia:s")
    monkeypatch.setattr(doctor, "_http", _http)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    return submitted


def test_row_with_commits_and_no_review_gets_exactly_one(monkeypatch):
    submitted = _wire_sweep(monkeypatch, [_nr()])
    doctor.run_review_followup_sweep()
    assert len(submitted) == 1
    spec = submitted[0]
    assert spec["eligible_kinds"] == ["codex"]
    assert spec["preferred_models"] == ["gpt-6-luna"]
    assert spec["max_cost_tier"] == "A"
    assert spec["head_branch"] == "fix/one" and spec["base_branch"] == "master"
    assert "READ-ONLY" in spec["description"] and "nr-1" in spec["description"]
    assert spec["kind"].startswith("review:fix:hive-one-codex-")
    assert spec["parent_job_id"] == "nr-1"


def test_second_sweep_does_not_duplicate(monkeypatch):
    rows = [_nr()]
    submitted = _wire_sweep(monkeypatch, rows)
    doctor.run_review_followup_sweep()
    # The first sweep recorded the submission on the row (auto_review_job_id).
    rows[0]["result"]["auto_review_job_id"] = "review-1"
    doctor.run_review_followup_sweep()
    assert len(submitted) == 1, "the durable marker + row field stop a second submit"


def test_cap_honoured_oldest_first(monkeypatch):
    rows = [_nr("newest", age=10), _nr("oldest", age=3600), _nr("mid", age=600), _nr("oldest2", age=1800)]
    submitted = _wire_sweep(monkeypatch, rows)
    doctor.run_review_followup_sweep()
    assert len(submitted) == doctor.REVIEW_FOLLOWUP_MAX_PER_SWEEP
    # Oldest first: the three longest-stuck rows get the reviews.
    assert [s["parent_job_id"] for s in submitted] == ["oldest", "oldest2", "mid"]


def test_row_with_existing_review_verdict_is_untouched(monkeypatch):
    row = _nr("nr-verdict", result={"review_evidence": {"verdict": "approve"}})
    submitted = _wire_sweep(monkeypatch, [row])
    doctor.run_review_followup_sweep()
    assert submitted == []


def test_row_without_commits_is_not_purged_and_is_reviewable(monkeypatch):
    # No purge call exists in the sweep at all: a needs-review row is never
    # auto-purged, whether or not it carries commits. It is still reviewed.
    row = _nr("nr-nocommits", commits=[])
    submitted = _wire_sweep(monkeypatch, [row])
    doctor.run_review_followup_sweep()
    assert len(submitted) == 1
    assert submitted[0]["parent_job_id"] == "nr-nocommits"
