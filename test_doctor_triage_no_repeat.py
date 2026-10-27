"""2026-10-27: a triage is not repeated for the SAME failed rows.

Operator directive: "we cannot let dead jobs sit there ... we should never have a
queue of 20+ dead jobs", and the doctor must not re-triage an already-triaged
failure every hour. These tests pin the durable marker that makes the scan a
no-op until a genuinely newer failure appears.
"""

import time

from hive.workers import doctor


def _job(i: int, ended: float, kind: str = "build:example") -> dict:
    return {
        "id": f"job{i}",
        "kind": kind,
        "status": "failed",
        "ended_at": ended,
        "last_update_at": ended,
    }


def _wire(monkeypatch, jobs: list[dict]) -> list[dict]:
    """Fake the bus: the failed list is fixed, every status list is empty."""
    submitted: list[dict] = []

    def fake_http(method, path, body=None, timeout=15.0):
        if method == "GET" and path.startswith("/v1/jobs?status=failed"):
            return 200, {"jobs": jobs}
        if method == "GET" and path.startswith("/v1/jobs?status="):
            return 200, {"jobs": []}
        if method == "POST" and path == "/v1/jobs":
            submitted.append(body or {})
            return 200, {"id": f"triage-{len(submitted)}"}
        return 404, None

    monkeypatch.setattr(doctor, "_http", fake_http)
    monkeypatch.setattr(doctor, "CLUSTER_THRESHOLD", 1)
    return submitted


def test_same_failures_are_not_retriaged(monkeypatch):
    jobs = [_job(1, time.time() - 100.0), _job(2, time.time() - 90.0)]
    submitted = _wire(monkeypatch, jobs)

    clusters = doctor.scan_for_failure_clusters()
    assert len(clusters) == 1, "a fresh failure cluster must be offered once"
    doctor.submit_auto_triage(clusters[0]["base_kind"], clusters[0]["count"], clusters[0]["sample_job_id"])
    assert len(submitted) == 1

    # Same two failed rows, unchanged: nothing new to triage.
    assert doctor.scan_for_failure_clusters() == []
    assert len(submitted) == 1, "no second triage for the same failures"


def test_new_failure_rearms_the_scan(monkeypatch):
    jobs = [_job(1, time.time() - 100.0)]
    submitted = _wire(monkeypatch, jobs)

    clusters = doctor.scan_for_failure_clusters()
    assert len(clusters) == 1
    doctor.submit_auto_triage(clusters[0]["base_kind"], clusters[0]["count"], clusters[0]["sample_job_id"])
    assert len(submitted) == 1

    # A genuinely new failure arrives AFTER the marker. The 1 h cooldown still
    # applies (unchanged design), so within the hour it is not re-triaged.
    jobs.append(_job(2, time.time() + 5.0))
    assert doctor.scan_for_failure_clusters() == []

    # Once the cooldown has elapsed, the same new failure is triaged once - and
    # then the marker covers it again.
    doctor.STATE.mark(f"{doctor.KEY_COOLDOWN}:build:example", ttl=doctor.AUTO_TRIAGE_COOLDOWN, at=time.time() - 7200)
    clusters = doctor.scan_for_failure_clusters()
    assert len(clusters) == 1, "after the cooldown a new failure is triaged"
    assert clusters[0]["count"] == 2
    doctor.submit_auto_triage(clusters[0]["base_kind"], clusters[0]["count"], clusters[0]["sample_job_id"])
    assert doctor.scan_for_failure_clusters() == []


def test_marker_is_durable_state_not_memory(monkeypatch):
    jobs = [_job(1, time.time() - 100.0)]
    _wire(monkeypatch, jobs)
    clusters = doctor.scan_for_failure_clusters()
    doctor.submit_auto_triage(clusters[0]["base_kind"], clusters[0]["count"], clusters[0]["sample_job_id"])

    key = doctor.triage_seen_key(clusters[0]["base_kind"])
    assert doctor.STATE.last_mark(key) is not None, "marker must live in the durable store"
    assert doctor.triaged_through(clusters[0]["base_kind"], time.time() - 100.0) is True
    assert doctor.triaged_through(clusters[0]["base_kind"], time.time() + 60.0) is False
