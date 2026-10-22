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
def _doctor_starts_with_no_safety_history(monkeypatch):
    """Swap in an empty in-memory safety store for the duration of one test."""
    from hive.workers import doctor
    from hive.workers.doctor_state import DurableStateStore, MemoryBackend

    monkeypatch.setattr(doctor, "STATE", DurableStateStore(MemoryBackend(), refresh_secs=0.0))
    # The gate reads the same store, and the approval it holds is what a test's
    # refusal depends on, so the two are swapped together.
    monkeypatch.setattr(doctor, "APPROVALS", doctor.doctor_gates.ApprovalGate(doctor.STATE, gate_enabled=True))
    yield
