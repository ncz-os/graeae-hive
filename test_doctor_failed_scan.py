"""The doctor triages failed jobs one by one (2026-10-17).

Hive job kinds are unique per task, so a cluster threshold above 1 meant the
failed-job scan never fired. Each recent failed job of an unprotected owner
must produce a cluster; protected owners and triage/doctor jobs must not.
"""

import time

from hive.workers import doctor


def _failed(jid, kind, project=None, age=60):
    return {"id": jid, "kind": kind, "project": project, "status": "failed", "ended_at": time.time() - age}


def _scan(monkeypatch, jobs, threshold=1):
    monkeypatch.setattr(doctor, "CLUSTER_THRESHOLD", threshold)
    monkeypatch.setattr(doctor, "_recent_auto_triage", {})
    monkeypatch.setattr(doctor, "_active_triage_exists", lambda bk: False)
    monkeypatch.setattr(doctor, "_http", lambda *a, **k: (200, {"jobs": jobs}))
    monkeypatch.setattr(doctor, "DLQ_PROTECT_KIND_PREFIXES", ("p3-", "build:p3-"))
    monkeypatch.setattr(doctor, "DLQ_PROTECT_PROJECTS", frozenset({"ncz-preview3-20261017"}))
    return {c["base_kind"] for c in doctor.scan_for_failure_clusters()}


def test_every_recent_unique_failure_is_triaged(monkeypatch):
    jobs = [_failed("a", "fix:hive-one"), _failed("b", "fix:hive-two")]
    assert _scan(monkeypatch, jobs) == {"fix:hive-one", "fix:hive-two"}


def test_threshold_above_one_never_fires_on_unique_kinds(monkeypatch):
    jobs = [_failed("a", "fix:hive-one"), _failed("b", "fix:hive-two")]
    assert _scan(monkeypatch, jobs, threshold=5) == set()


def test_protected_owners_and_old_or_meta_jobs_are_skipped(monkeypatch):
    jobs = [
        _failed("p", "p3-image-verify"),
        _failed("q", "fix:other", project="ncz-preview3-20261017"),
        _failed("t", "triage:fix:hive-one"),
        _failed("o", "fix:hive-old", age=doctor.CLUSTER_WINDOW_SEC + 60),
        _failed("k", "fix:hive-keep"),
    ]
    assert _scan(monkeypatch, jobs) == {"fix:hive-keep"}
