"""The fleet gateway token must never reach a persisted job description.

`_build_token_fix_codex_task()` produces the description of a remediation job.
That description is stored in the jobs table AND the event log, both readable by
any bus client, so interpolating $FLEET_LAN_TOKEN into it published a
fleet-wide gateway credential to every worker that could query the bus.

The remediation still has to work, so the generated script references the
variable and lets the executing host resolve it -- the value is supplied by the
operator's shell, not carried in a database row.
"""

import importlib
import os

import pytest

SENTINEL = "fleet-token-SENTINEL-do-not-leak-9f3a2b"


@pytest.fixture
def doctor(monkeypatch):
    """Import the module with a known token in the environment."""
    monkeypatch.setenv("FLEET_LAN_TOKEN", SENTINEL)
    import zeroclaw_doctor
    return importlib.reload(zeroclaw_doctor)


def test_token_value_never_appears_in_the_job_description(doctor):
    assert doctor.FLEET_LAN_TOKEN == SENTINEL, "precondition: module read the env var"
    task = doctor._build_token_fix_codex_task("TESTHOST")
    assert SENTINEL not in task, (
        "the gateway token value reached a job description that is persisted in "
        "the jobs table and event log"
    )


def test_the_remediation_still_resolves_the_token_at_run_time(doctor):
    """Redaction must not turn the fix into a no-op that writes an empty token."""
    task = doctor._build_token_fix_codex_task("TESTHOST")
    assert "FLEET_LAN_TOKEN" in task, "the script must still reference the variable"
    # both the shell heredoc and the embedded python must read it, not bake it
    assert '"$FLEET_LAN_TOKEN"' in task, "shell steps must expand the env var"
    assert "os.environ['FLEET_LAN_TOKEN']" in task, "embedded python must read the env var"


def test_no_token_when_the_variable_is_unset(monkeypatch):
    """An unset token must not silently produce a description containing ''."""
    monkeypatch.delenv("FLEET_LAN_TOKEN", raising=False)
    import zeroclaw_doctor
    d = importlib.reload(zeroclaw_doctor)
    task = d._build_token_fix_codex_task("TESTHOST")
    assert "FLEET_LAN_TOKEN" in task
