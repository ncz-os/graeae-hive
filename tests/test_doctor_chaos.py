"""Chaos tests for the doctor's failed-row sweep and resubmit paths.

Every bus call goes through ``doctor._http``, which each test replaces with a
scripted fake. The state store is in memory and the reviewer probe is stubbed,
so nothing touches the network, the home directory or real time.
"""

import pytest

from hive.workers import doctor, loop_outcome
from hive.workers.doctor_state import DurableStateStore, MemoryBackend


def failed_row(job_id, **extra):
    row = {
        "id": job_id,
        "kind": "build:x",
        "status": "failed",
        "max_retries": 2,
        "retry_count": 0,
        "result": {"failure_reason": "provider 503"},
        "eligible_kinds": ["build"],
        "eligible_hosts": None,
    }
    row.update(extra)
    return row


class FakeBus:
    """Scripted ``_http``: ``handler(method, path, body) -> (status, body)``."""

    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    def __call__(self, method, path, body=None, timeout=15.0):
        self.calls.append((method, path, body))
        return self.handler(method, path, body)

    def posts(self, path):
        return [c for c in self.calls if c[0] == "POST" and c[1] == path]


def jobs_handler(failed, queued=(), requeue_status=200):
    """Serve the failed and queued listings and answer the requeue route."""

    def handler(method, path, body):
        if method == "GET" and path.startswith("/v1/jobs?status=failed"):
            return 200, {"jobs": list(failed)}
        if method == "GET" and path.startswith("/v1/jobs?status=queued"):
            return 200, {"jobs": list(queued)}
        if method == "POST" and path == "/v1/admin/jobs/requeue":
            return requeue_status, {"ok": requeue_status == 200}
        return 404, None

    return handler


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    monkeypatch.setattr(doctor, "STATE", DurableStateStore(MemoryBackend(), refresh_secs=0.0))
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "_online_worker_kinds", lambda: None)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    monkeypatch.setattr(doctor, "_park_for_review", lambda *a, **k: None)
    monkeypatch.setattr(loop_outcome, "reviewer_healthy", lambda *a, **k: True)
    monkeypatch.setattr(loop_outcome, "classify", lambda job, *a, **k: loop_outcome.UNKNOWN)


def install(monkeypatch, handler):
    bus = FakeBus(handler)
    monkeypatch.setattr(doctor, "_http", bus)
    return bus


# ── bus 5xx and transport timeouts ─────────────────────────────────────


def test_5xx_on_failed_listing_is_a_clean_no_op(monkeypatch):
    bus = install(monkeypatch, lambda m, p, b: (503, None))
    result = doctor.sweep_orphan_failures()
    assert result == {"scanned": 0, "error": "http=503"}
    assert bus.posts("/v1/admin/jobs/requeue") == []


def test_timeout_on_requeue_is_not_marked_so_next_pass_retries(monkeypatch):
    bus = install(monkeypatch, jobs_handler([failed_row("job-1")], requeue_status=0))
    first = doctor.sweep_orphan_failures()
    doctor.sweep_orphan_failures()
    assert "(api=0)" in first["acted"]["build:x"]
    # A transport failure is not a definitive outcome, so the second pass retries.
    assert len(bus.posts("/v1/admin/jobs/requeue")) == 2


def test_5xx_on_requeue_is_retried_not_counted_as_handled(monkeypatch):
    bus = install(monkeypatch, jobs_handler([failed_row("job-1")], requeue_status=503))
    doctor.sweep_orphan_failures()
    doctor.sweep_orphan_failures()
    assert len(bus.posts("/v1/admin/jobs/requeue")) == 2


def test_successful_requeue_is_marked_and_not_repeated(monkeypatch):
    bus = install(monkeypatch, jobs_handler([failed_row("job-1")], requeue_status=200))
    doctor.sweep_orphan_failures()
    second = doctor.sweep_orphan_failures()
    assert len(bus.posts("/v1/admin/jobs/requeue")) == 1
    assert second["skipped"] == ["build:x"]


def test_jobs_field_null_does_not_crash(monkeypatch):
    install(monkeypatch, lambda m, p, b: (200, {"jobs": None}))
    assert doctor.sweep_orphan_failures()["scanned"] == 0


def test_truncated_result_string_does_not_crash_resubmit(monkeypatch):
    spent = failed_row("job-1", retry_count=2, result='{"failure_reason": "prov')
    install(monkeypatch, jobs_handler([spent]))
    assert isinstance(doctor.action_resubmit_jobs("build:x"), str)


def test_requeue_refused_with_409_is_one_attempt_per_pass(monkeypatch):
    bus = install(monkeypatch, jobs_handler([failed_row("job-1")], requeue_status=409))
    text = doctor.action_resubmit_jobs("build:x")
    assert text == "requeued 1 jobs of build:x (api=409)"
    assert len(bus.posts("/v1/admin/jobs/requeue")) == 1


def test_child_row_is_never_requeued_by_the_sweep(monkeypatch):
    child = failed_row("job-child", parent_job_id="job-parent")
    bus = install(monkeypatch, jobs_handler([child]))
    result = doctor.sweep_orphan_failures()
    assert result["bases"] == 0
    assert bus.posts("/v1/admin/jobs/requeue") == []


def test_budget_spent_row_gets_one_replacement_across_two_passes(monkeypatch):
    spent = failed_row("job-9", retry_count=2, max_retries=2)

    def handler(method, path, body):
        if method == "GET" and path.startswith("/v1/jobs?status=failed"):
            # The original stays listed as failed, as on the real bus.
            return 200, {"jobs": [spent]}
        if method == "POST" and path == "/v1/jobs":
            return 201, {"id": "job-new"}
        if method == "PATCH" and path == "/v1/jobs/job-9":
            # The real bus persists the marker, so the next listing shows it.
            spent["result"] = {**spent["result"], **body["result"]}
            return 200, {}
        return 404, None

    bus = install(monkeypatch, handler)
    first = doctor.action_resubmit_jobs("build:x")
    second = doctor.action_resubmit_jobs("build:x")
    assert first.startswith("resubmitted 1 budget-spent job(s)")
    assert len(bus.posts("/v1/jobs")) == 1, "a replacement was minted twice"
    assert "left alone" in second or "no resubmit" in second


def test_reviewer_unavailable_holds_the_row_without_requeue(monkeypatch):
    monkeypatch.setattr(loop_outcome, "reviewer_healthy", lambda *a, **k: False)
    monkeypatch.setattr(
        loop_outcome, "classify", lambda job, *a, **k: loop_outcome.REVIEWER_UNAVAILABLE
    )
    bus = install(monkeypatch, jobs_handler([failed_row("job-1")]))
    result = doctor.sweep_orphan_failures()
    assert result["outcomes"] == {"job-1": "reviewer_unavailable->hold"}
    assert bus.posts("/v1/admin/jobs/requeue") == []
