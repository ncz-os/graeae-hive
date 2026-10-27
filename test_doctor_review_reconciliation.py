"""Production doctor entry points: routing, restart dedupe and failed scans."""

import pytest

from hive.workers import doctor
from hive.workers.doctor_state import DurableStateStore, FileBackend


@pytest.fixture
def bus(monkeypatch, tmp_path):
    row = {
        "id": "source",
        "kind": "fix:thing",
        "status": "needs-review",
        "repo": "owner/repo",
        "head_branch": "fix/thing",
        "base_branch": "main",
        "result": {"head_sha": "a" * 40, "commits": ["a" * 40]},
    }
    calls, messages = [], []
    state_path = str(tmp_path / "state.json")
    monkeypatch.setattr(doctor, "STATE", DurableStateStore(FileBackend(state_path), refresh_secs=0))
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda topic, body: messages.append((topic, body)))

    def http(method, path, body=None, **kwargs):
        calls.append((method, path, body))
        if method == "GET":
            return 200, {"jobs": [] if "parent_job_id=" in path else [row]}
        if method == "POST" and path == "/v1/jobs":
            return 201, {"id": "review-child"}
        return 200, {}

    monkeypatch.setattr(doctor, "_http", http)
    return row, calls, messages, state_path, http


def test_immediate_patch_and_restart_reconciliation_share_marker(bus, monkeypatch):
    row, calls, _, state_path, _ = bus
    doctor.patch_job(row["id"], "needs-review", row["result"])
    monkeypatch.setattr(doctor, "STATE", DurableStateStore(FileBackend(state_path), refresh_secs=0))
    report = doctor.run_review_followup_sweep()
    submits = [c[2] for c in calls if c[:2] == ("POST", "/v1/jobs")]
    assert len(submits) == 1
    spec = submits[0]
    assert not spec.get("eligible_kinds") and not spec.get("preferred_models")
    assert spec["max_cost_tier"] == "C" and spec["priority"] >= 100
    assert spec["head_branch"] == "fix/thing"
    assert "a" * 40 in spec["description"]
    assert spec["idempotency_key"] == "needs-review-followup:source"
    assert report["skipped_marker"] == 1
    assert not any("purge" in c[1] for c in calls)


def test_triage_needs_review_is_not_dispatched_by_doctor(bus):
    row, calls, _, _, _ = bus
    row["kind"] = "triage:fix:thing"
    doctor.patch_job(row["id"], "needs-review", {"failure_reason": "failed"})
    assert doctor.run_review_followup_sweep()["skipped_triage"] == 1
    assert not any(c[:2] == ("POST", "/v1/jobs") for c in calls)


def test_child_listing_error_cannot_dispatch(bus, monkeypatch):
    _, calls, messages, _, http = bus

    def failed(method, path, *args, **kwargs):
        if "parent_job_id=" in path:
            return 503, {"error": "unavailable"}
        return http(method, path, *args, **kwargs)

    monkeypatch.setattr(doctor, "_http", failed)
    report = doctor.run_review_followup_sweep()
    assert report["errors"] == 1 and report["submitted"] == 0
    assert not any(c[:2] == ("POST", "/v1/jobs") for c in calls)
    assert messages[-1][0] == "doctor.sweep_error"


@pytest.mark.parametrize(
    "fetch", [doctor.fetch_needs_review_jobs, doctor.fetch_dead_letter_jobs, doctor.scan_for_failure_clusters]
)
@pytest.mark.parametrize("response", [(503, {}), (200, {}), (200, {"jobs": "broken"})])
def test_listing_error_is_not_empty_queue(bus, monkeypatch, fetch, response):
    monkeypatch.setattr(doctor, "_http", lambda *a, **kw: response)
    with pytest.raises(RuntimeError, match="incomplete listing"):
        fetch()
    assert bus[2][-1][0] == "doctor.sweep_error"


@pytest.mark.parametrize("result", [{"verdict": {"verdict": "approve"}}, {"worker_error": "released_by_host"}])
def test_existing_verdict_and_never_act_not_dispatched(bus, result):
    row, calls, _, _, _ = bus
    row["result"] = result
    doctor.run_review_followup_sweep()
    assert not any(c[0] in {"PATCH", "POST"} for c in calls)


def test_protected_lane_not_dispatched_or_verdict_completed(bus, monkeypatch):
    row, calls, _, _, _ = bus
    monkeypatch.setattr(doctor, "DLQ_PROTECT_PROJECTS", frozenset({"owner-only"}))
    row["project"] = "owner-only"
    row["result"] = {"failure_reason": "codex_delivery_failed", "tail": '{"verdict":"approve"}'}
    doctor.run_needs_review_sweep()
    assert not any(c[0] in {"PATCH", "POST"} for c in calls)


def test_failed_submit_retries_same_idempotency_key(bus, monkeypatch):
    row, calls, _, _, http = bus
    specs = []

    def lost_response(method, path, body=None, **kwargs):
        if (method, path) == ("POST", "/v1/jobs"):
            specs.append(body)
            return 0, None
        return http(method, path, body, **kwargs)

    monkeypatch.setattr(doctor, "_http", lost_response)
    doctor.run_review_followup_sweep()
    doctor.run_review_followup_sweep()
    assert len(specs) == 2
    assert specs[0]["idempotency_key"] == specs[1]["idempotency_key"]
    assert not doctor.review_followup_submitted(row["id"])


def test_retention_fetch_error_visible_without_purge(bus, monkeypatch):
    calls = []
    monkeypatch.setattr(doctor, "_http", lambda *a, **kw: calls.append(a) or (503, {}))
    report = doctor.run_retention_sweep()
    assert report["error"] == "incomplete listing"
    assert report["scanned"] is None and report["purged"] == 0
    assert bus[2][-1][0] == "doctor.sweep_error"
    assert all(c[0] == "GET" for c in calls)


def test_generic_failure_does_not_hide_nested_routing_signal(bus):
    row, calls, _, _, _ = bus
    row["result"] = {"failure_reason": "delivery_failed", "error": {"reason": "released_by_host"}}
    doctor.run_needs_review_sweep()
    assert not any(c[0] in {"PATCH", "POST"} for c in calls)


def test_completed_child_replayed_even_with_submission_marker(bus, monkeypatch):
    row, calls, _, _, http = bus
    row["result"]["auto_review_job_id"] = "review-child"
    doctor.STATE.mark("review:submitted:source", ttl=doctor.REVIEW_FOLLOWUP_TTL_SEC)
    proof = {"verdict": {"verdict": "approve", "head": "a" * 40}, "deliverable": "Review complete"}

    def completed(method, path, body=None, **kwargs):
        if "parent_job_id=" in path:
            return 200, {"jobs": [{"id": "review-child", "kind": "review:thing", "status": "done", "result": proof}]}
        return http(method, path, body, **kwargs)

    monkeypatch.setattr(doctor, "_http", completed)
    summary = doctor.run_review_followup_sweep()
    assert summary["replayed_verdict"] == 1
    assert [(c[1], c[2]) for c in calls if c[0] == "PATCH"] == [
        ("/v1/jobs/review-child", {"status": "done", "result": proof})
    ]
    assert not any(c[:2] == ("POST", "/v1/jobs") for c in calls)


def test_truncated_backlog_is_explicit_and_uses_api_maximum(bus, monkeypatch):
    row, calls, messages, _, http = bus

    def truncated(method, path, body=None, **kwargs):
        calls.append((method, path, body))
        return 200, {"jobs": [row], "total": 1001}

    monkeypatch.setattr(doctor, "_http", truncated)
    assert doctor.fetch_needs_review_jobs() == [row]
    assert "limit=1000" in calls[0][1]
    assert messages[-1][1]["total"] == 1001
    assert "truncated" in messages[-1][1]["error"]
