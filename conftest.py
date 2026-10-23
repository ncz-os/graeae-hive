"""Test-wide fixtures for the graeae-hive-mind package.

The doctor's safety state is **process-global by design** — the loop counter, the
per-kind cooldown, the DLQ-seen set and the SSH rate limiter are one store, and
that is the whole point of moving them out of memory (see
`hive/workers/doctor_state.py`). The cost is that it leaks between tests: a test
which leaves three loop attempts behind makes the next test escalate a job it
never touched, which is a confusing failure in an unrelated file.

So every test gets a FRESH memory-backed store, swapped in for the duration and
swapped back afterwards.

It does not call `doctor.reset_safety_state()`, which would be the obvious way to
do this and is the wrong one: that function writes through to the configured
backend, so running the suite on a host where `DOCTOR_STATE_PATH` points at the
live doctor's state file would erase the real safety limits of a real doctor. The
store is replaced, never emptied — clearing the dicts alone would leave the durable
half behind, which is the bug the module exists to remove.
"""

import pytest


@pytest.fixture(autouse=True)
def _doctor_starts_with_no_safety_history(monkeypatch, tmp_path):
    """Swap in an empty in-memory safety store for the duration of one test.

    The module-level MIRRORS the store keeps in step (`_seen_job_attempts`,
    `_recent_auto_triage`, `_dlq_seen`, `_ssh_actions_log`, `_host_last_action`) are
    cleared too. They are process-global for the same reason the store is, and a
    mirror that survives a test is a loop count the next test escalates on.
    """
    # A fixture can reload doctor after this store swap. Keep that constructor
    # in scratch too, so a reloaded module cannot write a configured live file.
    monkeypatch.setenv("DOCTOR_STATE_BACKEND", "file")
    monkeypatch.setenv("DOCTOR_STATE_PATH", str(tmp_path / "runtime-state.json"))
    from hive.workers import doctor
    from hive.workers.doctor_state import DurableStateStore, MemoryBackend

    monkeypatch.setattr(doctor, "STATE", DurableStateStore(MemoryBackend(), refresh_secs=0.0))
    # The gate reads the same store, and the approval it holds is what a test's
    # refusal depends on, so the two are swapped together.
    monkeypatch.setattr(doctor, "APPROVALS", doctor.doctor_gates.ApprovalGate(doctor.STATE, gate_enabled=True))
    mirrors = (
        doctor._seen_job_attempts,
        doctor._recent_auto_triage,
        doctor._dlq_seen,
        doctor._ssh_actions_log,
        doctor._host_last_action,
    )
    for mirror in mirrors:
        mirror.clear()
    yield
    # Cleared again on the way out: the mirrors are process-global and the
    # production code writes them, so a test that left entries behind would leak
    # them into whatever runs next.
    for mirror in mirrors:
        mirror.clear()
