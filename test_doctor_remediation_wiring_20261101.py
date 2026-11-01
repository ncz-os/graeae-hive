"""The doctor's loop CALLS the remediation sweep (operator 2026-11-01).

The remediation module and ``doctor.run_remediation_sweep`` shipped to PYTHIA,
but nothing called the sweep: the main loop ran the DLQ scan, the needs-review
sweep, the orphan-failure sweep and the unclaimable-queued sweep, and never the
one that FIXES the rows. Live log evidence: ``needs-review report-only`` lines
and zero ``remediation`` lines, so the doctor only ever reported the backlog it
was built to drain.

These tests drive the loop's per-interval body -- :func:`doctor.doctor_loop_body`,
extracted from ``main`` for exactly this reason -- with the sweeps patched, so
the order, the cadence, the isolation and the log line are pinned without
running a live loop:

* the sweep runs once per needs-review interval, AFTER ``run_needs_review_sweep``
  (whose classification it reads);
* it runs in its own try/except: a raising sweep logs ``remediation sweep error``
  and the rest of the body still runs, so the loop is never killed;
* its result is logged as ``remediation sweep: %s``;
* ``HIVE_DOCTOR_REMEDIATE=0`` still reaches the sweep (the kill switch lives
  inside ``run_remediation_sweep``) and nothing inside the remediation module is
  reached -- the disabled sweep is idempotent and should be cheap;
* the startup line says whether remediation is enabled and dry-run.

They fail on the base: there is no ``doctor_loop_body``, ``main`` never
mentions ``run_remediation_sweep``, and the startup line does not name
remediation at all.

The kill-switch case is the one that fails on the base for a DIFFERENT reason:
the base ``run_remediation_sweep`` fetches the rows and the roster BEFORE the
durable-state guard, so a disabled sweep still reads the bus. It is a live-sweep
defect, not a wiring one, and it is fixed in the same change (report step 3).
"""

from __future__ import annotations

import os

os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")

import pytest

from hive.workers import doctor
from hive.workers import doctor_remediation as dr


class Recorder:
    """A patched sweep that records the call order and returns a summary."""

    def __init__(self, calls: list, name: str, result=None, raises: Exception | None = None):
        self.calls = calls
        self.name = name
        self.result = {} if result is None else result
        self.raises = raises
        self.count = 0

    def __call__(self, *args, **kwargs):
        self.count += 1
        self.calls.append(self.name)
        if self.raises is not None:
            raise self.raises
        return self.result


class LoopClock:
    """A scripted clock: each ``time()`` read advances by ``step`` seconds."""

    def __init__(self, start: float = 10_000.0, step: float = 1.0):
        self.value = start
        self.step = step

    def __call__(self) -> float:
        self.value += self.step
        return self.value


def _last() -> dict:
    """The loop's schedule, all intervals expired (one pass runs every block)."""
    return {
        "last_hb": 0.0,
        "last_scan": 0.0,
        "last_retention": 0.0,
        "last_dlq": 0.0,
        "last_needs_review": 0.0,
    }


def _body(monkeypatch, *, last, calls, sweeps=None, clock=None):
    """Patch every side effect of one loop-body pass and return the recorders."""
    sweeps = sweeps or {}
    recorders = {
        name: Recorder(
            calls,
            name,
            result=(sweeps.get(name) or {}).get("result"),
            raises=(sweeps.get(name) or {}).get("raises"),
        )
        for name in (
            "handle_dead_letter_queue",
            "run_needs_review_sweep",
            "sweep_orphan_failures",
            "sweep_unclaimable_queued",
            "run_remediation_sweep",
            "run_retention_sweep",
            "scan_for_failure_clusters",
        )
    }
    for name, recorder in recorders.items():
        monkeypatch.setattr(doctor, name, recorder)
    stub = lambda *a, **k: calls.append("side_effect") or {}  # noqa: E731
    monkeypatch.setattr(doctor, "heartbeat", stub)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", stub)
    monkeypatch.setattr(doctor, "submit_auto_triage", stub)
    if clock is not None:
        monkeypatch.setattr(doctor.time, "time", clock)
    return recorders


def test_the_loop_body_runs_the_remediation_sweep_after_the_needs_review_sweep(monkeypatch):
    """The wiring itself: the sweep is called, once, after the report-only
    needs-review sweep (which it depends on having classified the rows)."""
    calls: list = []
    last = _last()
    _body(monkeypatch, last=last, calls=calls)
    doctor.doctor_loop_body(last)

    assert calls.count("run_remediation_sweep") == 1, calls
    assert calls.index("run_needs_review_sweep") < calls.index("run_remediation_sweep"), calls


def test_the_sweep_runs_once_per_needs_review_interval_not_once_per_tick(monkeypatch):
    """The loop ticks every POLL_INTERVAL; the remediation sweep is a scan, so
    a second pass inside the interval must NOT run it again."""
    calls: list = []
    clock = LoopClock(step=1.0)
    last = _last()
    _body(monkeypatch, last=last, calls=calls, clock=clock)
    doctor.doctor_loop_body(last)
    assert calls.count("run_remediation_sweep") == 1

    # A second pass a couple of seconds later: still inside the interval.
    doctor.doctor_loop_body(last)
    assert calls.count("run_remediation_sweep") == 1, "the sweep is a scan, not a tick"
    assert calls.count("run_needs_review_sweep") == 1

    # Once the interval has elapsed the sweep runs again.
    clock.value += doctor.NEEDS_REVIEW_SCAN_INTERVAL + 1
    doctor.doctor_loop_body(last)
    assert calls.count("run_remediation_sweep") == 2
    assert calls.count("run_needs_review_sweep") == 2


def test_a_raising_sweep_logs_its_own_error_and_never_kills_the_loop(monkeypatch, caplog):
    """An unreadable bus (``fetch_remediable_rows`` raises on a failed listing)
    or a durable-state refusal must not take the doctor down with it: the
    sweep's exception is caught, named ``remediation sweep error``, and the
    rest of the interval block still runs."""
    calls: list = []
    last = _last()
    caplog.set_level("INFO", logger="doctor")
    _body(
        monkeypatch,
        last=last,
        calls=calls,
        sweeps={"run_remediation_sweep": {"raises": RuntimeError("bus down")}},
    )
    doctor.doctor_loop_body(last)
    assert calls.count("run_remediation_sweep") == 1, calls
    # Everything scheduled AFTER the remediation sweep still ran.
    assert "scan_for_failure_clusters" in calls, calls
    assert "run_retention_sweep" in calls, calls
    errors = [r.getMessage() for r in caplog.records if "remediation sweep error" in r.getMessage()]
    assert errors, [r.getMessage() for r in caplog.records]
    assert "bus down" in errors[0]


def test_the_sweep_result_is_logged(monkeypatch, caplog):
    """The operator reads the live log: the summary must appear as
    ``remediation sweep: <summary>``, the same shape as the sibling sweeps."""
    calls: list = []
    last = _last()
    summary = {"enabled": True, "scanned": 7, "applied": 2, "dry_run": False}
    caplog.set_level("INFO", logger="doctor")
    _body(
        monkeypatch,
        last=last,
        calls=calls,
        sweeps={"run_remediation_sweep": {"result": summary}},
    )
    doctor.doctor_loop_body(last)
    lines = [r.getMessage() for r in caplog.records]
    assert any(line.startswith("remediation sweep: ") and "'applied': 2" in line for line in lines), lines


def test_the_kill_switch_stops_the_module_before_any_bus_or_state_read(monkeypatch):
    """``HIVE_DOCTOR_REMEDIATE=0`` is honoured inside ``run_remediation_sweep``
    (the call site always calls it), and the disabled sweep must be CHEAP: no
    listing, no roster, no durable-state requirement."""
    calls: list = []
    monkeypatch.setattr(dr, "REMEDIATE_ENABLED", False)
    monkeypatch.setattr(
        doctor,
        "fetch_remediable_rows",
        lambda *a, **k: calls.append("fetch_remediable_rows") or [],
    )
    monkeypatch.setattr(
        doctor,
        "review_queue",
        type("RQ", (), {"load_roster": staticmethod(lambda: calls.append("load_roster") or [])}),
    )
    out = doctor.run_remediation_sweep()
    assert out["enabled"] is False
    assert out["applied"] == 0
    assert calls == [], f"a disabled sweep must not read the bus or the roster: {calls}"


def test_the_startup_line_says_whether_remediation_is_enabled_and_dry_run():
    """An operator reading the doctor's first log line must be able to tell
    whether it is going to FIX rows and whether that is a preview."""
    text = doctor.remediation_startup_line()
    assert "remediation" in text.lower()
    assert "enabled" in text.lower()
    assert "dry_run" in text.lower() or "dry-run" in text.lower()


@pytest.mark.parametrize("enabled,dry", [(True, False), (False, True)])
def test_the_startup_line_reflects_the_module_flags(monkeypatch, enabled, dry):
    monkeypatch.setattr(dr, "REMEDIATE_ENABLED", enabled)
    monkeypatch.setattr(dr, "DRY_RUN", dry)
    text = doctor.remediation_startup_line()
    expected = "enabled" if enabled else "disabled"
    assert text.startswith(f"remediation {expected}"), text
    assert ("dry_run=True" in text) if dry else ("dry_run=False" in text), text
    # The cap the sweep enforces is named, so the line also answers "how many".
    assert str(dr.MAX_REMEDIATIONS_PER_SWEEP) in text
