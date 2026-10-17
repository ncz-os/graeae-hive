"""Owner-protected dead-letter jobs are reported, never remediated (2026-10-17)."""

from hive.workers import doctor


def _dl_job(jid, kind, project=None):
    return {
        "id": jid,
        "kind": kind,
        "project": project,
        "result": {"dead_letter": True, "last_decline_reason": "no_repo_mapping"},
    }


def test_protected_by_kind_prefix_and_project(monkeypatch):
    monkeypatch.setattr(doctor, "DLQ_PROTECT_KIND_PREFIXES", ("p3-", "build:p3-"))
    monkeypatch.setattr(doctor, "DLQ_PROTECT_PROJECTS", frozenset({"ncz-preview3-20261017"}))
    assert doctor.dlq_protected(_dl_job("a", "p3-image-verify"))
    assert doctor.dlq_protected(_dl_job("b", "build:p3-sky1-r290-stage"))
    assert doctor.dlq_protected(_dl_job("c", "fix:other", "ncz-preview3-20261017"))
    assert not doctor.dlq_protected(_dl_job("d", "fix:hive-x", "graeae"))


def test_empty_lists_protect_nothing(monkeypatch):
    monkeypatch.setattr(doctor, "DLQ_PROTECT_KIND_PREFIXES", ())
    monkeypatch.setattr(doctor, "DLQ_PROTECT_PROJECTS", frozenset())
    assert not doctor.dlq_protected(_dl_job("a", "p3-image-verify"))


def test_sweep_leaves_protected_jobs_alone(monkeypatch):
    monkeypatch.setattr(doctor, "DLQ_PROTECT_KIND_PREFIXES", ("p3-",))
    monkeypatch.setattr(doctor, "DLQ_PROTECT_PROJECTS", frozenset())
    monkeypatch.setattr(doctor, "_dlq_seen", {})
    jobs = [_dl_job("job-p3", "p3-image-verify"), _dl_job("job-other", "fix:hive-x")]
    monkeypatch.setattr(doctor, "fetch_dead_letter_jobs", lambda limit=200: jobs)
    remediated = []
    monkeypatch.setattr(
        doctor, "remediate_dead_letter", lambda job, d, r: remediated.append(job["id"]) or "ok"
    )
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    summary = doctor.handle_dead_letter_queue()
    assert remediated == ["job-other"]
    assert summary["protected"] == 1
