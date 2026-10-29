"""One codex-fix dispatch per parent job — a replay must not mint a second fix.

F2 idempotency audit (2026-10-29): `action_dispatch_codex_fix` built its kind from
`int(time.time())` and sent no `idempotency_key`, so two dispatches for one parent
produced two fix jobs. `run_job`'s always-terminal except path can reach it twice
for one row. The bus dedups on `queue_logic.dedup_key(kind, description,
idempotency_key)`, so both are now derived from the parent id.

The operator approval gate is single-use, which is itself an idempotency layer:
re-running the action requires a fresh grant. These tests record a fresh grant per
call so the ASSERTS are about the submit payload (what the bus sees), not about
the gate.
"""

from __future__ import annotations

import pytest

from hive.workers import doctor, doctor_gates


def _grant(kind: str, **parts) -> None:
    """Record a fresh scoped operator approval for exactly this action."""
    doctor.APPROVALS.record_approval(kind, doctor_gates.action_digest(kind, **parts), by="operator:test")


@pytest.fixture
def submits(monkeypatch):
    """Capture POST /v1/jobs bodies; answer like the bus."""
    calls: list[dict] = []

    def _http(method, path, body=None, **_kw):
        if method == "POST" and path == "/v1/jobs":
            calls.append(body)
            return 201, {"id": f"fix-{len(calls)}"}
        return 200, {}

    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "_http", _http)
    return calls


def test_two_dispatches_for_one_parent_are_the_same_submit(submits):
    task, parent = "repair the gateway token", "job-parent-1"

    _grant("dispatch_codex_fix", task=task, parent_job_id=parent)
    first = doctor.action_dispatch_codex_fix(task, parent)
    _grant("dispatch_codex_fix", task=task, parent_job_id=parent)
    second = doctor.action_dispatch_codex_fix(task, parent)

    assert first.startswith("codex sub-job submitted") and second.startswith("codex sub-job submitted")
    assert len(submits) == 2, "each approved dispatch still reaches the bus"
    assert submits[0]["kind"] == submits[1]["kind"], "a stable kind is what makes the dedup hash stable"
    assert submits[0]["idempotency_key"] == submits[1]["idempotency_key"]
    assert submits[0]["idempotency_key"] == f"doctor-codex-fix:{parent}"


def test_the_kind_stays_excluded_from_doctor_claiming(submits):
    """The doctor must never claim its own dispatch (self-loop)."""
    _grant("dispatch_codex_fix", task="t", parent_job_id="p")
    doctor.action_dispatch_codex_fix("t", "p")
    kind = submits[0]["kind"]
    assert kind.startswith("doctor:codex-fix:")
    assert doctor.is_doctor_claimable(kind) is False


def test_two_different_parents_get_different_keys(submits):
    for parent in ("p1", "p2"):
        _grant("dispatch_codex_fix", task="t", parent_job_id=parent)
        doctor.action_dispatch_codex_fix("t", parent)
    assert submits[0]["idempotency_key"] != submits[1]["idempotency_key"]


def test_an_unapproved_dispatch_still_submits_nothing(submits):
    text = doctor.action_dispatch_codex_fix("repair", "unapproved-parent")
    assert text.startswith("REFUSED:")
    assert submits == []
