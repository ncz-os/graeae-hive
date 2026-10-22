"""Test-wide fixtures for the graeae-hive-mind package.

The doctor's safety state is **process-global by design** — the loop counter, the
per-kind cooldown, the DLQ-seen set and the SSH rate limiter are one store, and
that is the whole point of moving them out of memory (see
`hive/workers/doctor_state.py`). The cost is that it leaks between tests: a test
which leaves three loop attempts behind makes the next test escalate a job it
never touched, which is a confusing failure in an unrelated file.

So every test starts with the counters cleared, through the doctor's own
`reset_safety_state()` — the one function that clears the durable store and the
in-memory mirrors together. Clearing only the dicts would leave the durable half
behind, which is precisely the bug this module exists to remove.
"""

import pytest


@pytest.fixture(autouse=True)
def _doctor_starts_with_no_safety_history():
    from hive.workers import doctor

    doctor.reset_safety_state()
    yield
    doctor.reset_safety_state()
