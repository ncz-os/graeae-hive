"""The continuation loop fails CLOSED (DeepSeek HIGH, 2026-11-01).

The continuation pass makes the doctor create jobs by itself, and an earlier
remediation runaway the same day built a ten-deep review chain. Three guards
stand between a lost record and a runaway, each tested here:

1. ``lineage_unknown`` -- a row whose durable lineage record AND
   ``doctor-continuation:`` idempotency key are both gone, but which still
   shows it IS a continuation (the brief's marker in its description, a
   ``-rN`` head branch, or a parent chain the doctor continued), is refused:
   no new round-1 chain, one log line, one escalation per TTL. A genuinely new
   root (no marker anywhere) still gets round 1.
2. A global hourly brake (``HIVE_DOCTOR_CONTINUE_MAX_PER_HOUR``, default 6)
   on continuation jobs across ALL rows, counted durably and independently of
   every per-row record.
3. No continuation is created unless the doctor's state store is durable
   (``loaded_ok`` and ``persist_ok``), the same rule ``run_remediation_sweep``
   already applies.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from pathlib import Path

os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")

import pytest

from hive.workers import doctor_remediation as dr
from hive.workers.doctor_state import DurableStateStore, MemoryBackend
from test_doctor_continuation_rounds_20261101 import (  # noqa: F401 -- table_only is an autouse fixture
    NOW,
    ROSTER,
    TIP,
    Fake,
    child,
    continuation_row,
    one_rc,
    quiet_work_row,
    rc_on,
    table_only,
    work_row,
)

ROOT = Path(__file__).parent


@pytest.fixture
def state():
    return DurableStateStore(MemoryBackend(), refresh_secs=0.0)


def orphaned(row: dict, **overrides) -> dict:
    """A continuation whose idempotency key the bus dropped (and whose lineage
    record is absent from the store the test hands in)."""
    row = json.loads(json.dumps(row))
    row.pop("idempotency_key", None)
    row.update(overrides)
    return row


def brief_only_row() -> dict:
    """Marker evidence only: the continuation brief in the description; the
    head branch has no ``-rN`` and there is no parent."""
    root = work_row()
    review = {"reviewers": ["gpt-oss-20b"], "families": ["openai"], "head": TIP, "findings": []}
    row = continuation_row(1)
    row = orphaned(
        row,
        head_branch="hv/fo-merge-pass-followup",
        parent_job_id=None,
        description=dr.continuation_brief(root, review, 1, base=root["head_branch"], head="hv/x-r1"),
    )
    return row


def branch_only_row() -> dict:
    """Marker evidence only: the ``-rN`` head branch (no brief, no parent)."""
    return orphaned(continuation_row(1), parent_job_id=None, description="fix things")


def parent_only_row() -> dict:
    """Marker evidence only: the parent chain (base == the parent's head)."""
    root = work_row()
    return orphaned(
        continuation_row(1),
        head_branch="hv/fo-merge-pass-followup",
        base_branch=root["head_branch"],
        parent_job_id=root["id"],
        description="fix things",
    )


def sweep_one(fake: Fake, row: dict, state, **kw) -> dict:
    return fake.sweep([row], state, **kw)


# ── guard 1: lineage_unknown ────────────────────────────────────────────────


@pytest.mark.parametrize(
    "make_row, lookup",
    [
        (lambda: orphaned(continuation_row(1)), False),
        (brief_only_row, False),
        (branch_only_row, False),
        (parent_only_row, True),
    ],
    ids=["all_markers", "brief_marker", "rN_branch", "parent_chain"],
)
def test_a_continuation_with_no_lineage_record_or_key_is_refused(state, caplog, make_row, lookup):
    """Without the guard each of these became round 1 again on the author's
    family: a fresh chain from a row that already WAS a round."""
    row = make_row()
    assert dr.continuation_lineage(row, state) == (row["id"], 0), "precondition: lineage really is lost"
    fake = Fake({row["id"]: [rc_on(row, "rc_meta")]})
    root = work_row()
    kw = {"job_lookup": lambda jid: root if jid == root["id"] else None} if lookup else {}
    caplog.set_level(logging.INFO, logger="hive.doctor.remediation")
    first = sweep_one(fake, row, state, **kw)
    second = sweep_one(fake, row, state, now=NOW + 60, **kw)
    assert fake.specs == [], fake.specs
    assert first.get(dr.REFUSED) == 1 and second.get(dr.REFUSED) == 1, (first, second)
    assert first.get(dr.APPLIED) == 0 and second.get(dr.APPLIED) == 0
    assert len(fake.escalated) == 1, fake.escalated
    assert fake.escalated[0][0] == row["id"] and dr.LINEAGE_UNKNOWN in fake.escalated[0][1]
    loud = [r for r in caplog.records if r.levelno >= logging.INFO and dr.LINEAGE_UNKNOWN in r.getMessage()]
    assert len(loud) == 1, [r.getMessage() for r in loud]


def test_lineage_unknown_escalates_again_only_after_the_ttl(state):
    row = branch_only_row()
    fake = Fake({row["id"]: [rc_on(row, "rc_meta")]})
    sweep_one(fake, row, state)
    sweep_one(fake, row, state, now=NOW + dr.ESCALATE_TTL_SEC - 1)
    assert len(fake.escalated) == 1
    sweep_one(fake, row, state, now=NOW + dr.ESCALATE_TTL_SEC + 1)
    assert len(fake.escalated) == 2
    assert fake.specs == []


def test_lineage_unknown_dry_run_escalates_nothing(state):
    row = branch_only_row()
    fake = Fake({row["id"]: [rc_on(row, "rc_meta")]})
    summary = sweep_one(fake, row, state, dry_run=True)
    assert fake.specs == [] and fake.escalated == []
    assert summary.get(dr.REFUSED) == 1, summary


def test_a_parent_the_doctor_continued_is_evidence_without_a_lookup(state):
    """The store still holds the PARENT's continuation marker: the child work
    row is that continuation, even with no bus lookup wired in."""
    root = work_row()
    dr.mark_fixed(state, dr.continuation_key(root["id"], TIP, 1), at=NOW - 60)
    row = orphaned(
        continuation_row(1),
        head_branch="hv/fo-merge-pass-followup",
        base_branch="somewhere-else",
        parent_job_id=root["id"],
        description="",
    )
    fake = Fake({row["id"]: [rc_on(row, "rc_meta")]})
    summary = sweep_one(fake, row, state)
    assert fake.specs == [] and summary.get(dr.REFUSED) == 1, summary


def test_a_failed_parent_lookup_creates_nothing_and_escalates_nothing(state):
    root = work_row()
    row = parent_only_row()
    fake = Fake({row["id"]: [rc_on(row, "rc_meta")]})

    def broken(_jid):
        raise RuntimeError("bus down")

    summary = sweep_one(fake, row, state, job_lookup=broken)
    assert fake.specs == [] and fake.escalated == [], summary
    assert summary.get(dr.APPLIED) == 0
    assert root["id"]


def test_the_recorded_lineage_still_wins_over_the_markers(state):
    """Not a regression: a continuation whose lineage record survived gets round 2."""
    row = orphaned(continuation_row(1))
    state.set_value(dr._lineage_key(row["id"]), {"root": work_row()["id"], "round": 1}, ttl=dr.MARK_TTL_SEC)
    fake = Fake({row["id"]: [rc_on(row, "rc_meta")]})
    sweep_one(fake, row, state)
    assert [s["head_branch"] for s in fake.specs] == ["hv/fo-merge-pass-20261031-r2"]


# ── guard 1b: a genuinely new root is still round 1 ─────────────────────────


def test_a_genuinely_new_root_still_gets_round_one(state):
    row = work_row()
    assert dr.continuation_evidence(row, state) == ""
    fake = one_rc(row)
    summary = sweep_one(fake, row, state)
    assert summary.get(dr.REFUSED, 0) == 0 and summary[dr.APPLIED] == 1, summary
    assert fake.specs[0]["head_branch"] == "hv/fo-merge-pass-20261031-r1"
    assert fake.escalated == []


def test_a_new_root_with_an_unrelated_parent_still_gets_round_one(state):
    """A parent_job_id alone is not a continuation: the parent's head is not
    this row's base and the doctor never continued it."""
    parent = work_row(id="01a194cb-0000-7000-8000-00000000beef", head_branch="hv/other-work")
    row = work_row(parent_job_id=parent["id"])
    fake = one_rc(row)
    sweep_one(fake, row, state, job_lookup=lambda jid: parent if jid == parent["id"] else None)
    assert [s["head_branch"] for s in fake.specs] == ["hv/fo-merge-pass-20261031-r1"]


# ── guard 2: the global hourly brake ────────────────────────────────────────


def many_rows(n: int, start: int = 0) -> tuple[list[dict], dict]:
    rows, kids = [], {}
    for i in range(start, start + n):
        r = work_row(id=f"01a194cb-79b3-7c10-9e2a-3b4c5d6e{i:04d}", ended_at=1793544000.0 + i)
        rows.append(r)
        kids[r["id"]] = [child("rc_openai", parent_job_id=r["id"])]
    return rows, kids


def test_the_hourly_brake_defaults_to_six():
    assert dr.CONTINUE_MAX_PER_HOUR == 6


@pytest.mark.parametrize("value, expected", [("2", 2), ("0", 0), ("-3", 0), ("junk", 6), ("", 6)])
def test_the_hourly_brake_reads_its_env(value, expected):
    env = {**os.environ, "HIVE_DOCTOR_CONTINUE_MAX_PER_HOUR": value}
    out = subprocess.run(
        [sys.executable, "-c", "from hive.workers import doctor_remediation as d; print(d.CONTINUE_MAX_PER_HOUR)"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout.strip() == str(expected)


def test_the_hourly_brake_caps_creation_across_all_rows(state, caplog):
    rows, kids = many_rows(10)
    fake = Fake(kids)
    caplog.set_level(logging.WARNING, logger="hive.doctor.remediation")
    summary = fake.sweep(rows, state, max_per_sweep=100)
    assert len(fake.specs) == 6, summary
    assert summary.get(dr.THROTTLED) == 4, summary
    assert any("per hour" in r.getMessage() for r in caplog.records)


def test_the_hourly_brake_spans_sweeps_and_reopens_after_an_hour(state):
    rows, kids = many_rows(10)
    fake = Fake(kids)
    fake.sweep(rows[:3], state)
    fake.sweep(rows[3:6], state, now=NOW + 600)
    third = fake.sweep(rows[6:9], state, now=NOW + 1200)
    assert len(fake.specs) == 6 and third.get(dr.THROTTLED) == 3, third
    fake.sweep(rows[6:9], state, now=NOW + 3601)
    assert len(fake.specs) == 9


def test_the_hourly_brake_is_independent_of_per_row_state(state):
    """Every per-row record lost: the brake still holds, because its counter is
    its own durable record and is read by a fresh store on the same backend."""
    backend = MemoryBackend()
    store = DurableStateStore(backend, refresh_secs=0.0)
    rows, kids = many_rows(6)
    fake = Fake(kids)
    fake.sweep(rows, store, max_per_sweep=100)
    assert len(fake.specs) == 6
    for key in store.keys(dr._KEY_MARK + ":"):
        if key != dr.HOURLY_KEY:
            store.forget(key)
    restarted = DurableStateStore(backend, refresh_secs=0.0)
    more, more_kids = many_rows(3, start=50)
    fake.children_of.update(more_kids)
    summary = fake.sweep(more, restarted, now=NOW + 60, max_per_sweep=100)
    assert len(fake.specs) == 6, summary
    assert summary.get(dr.THROTTLED) == 3, summary


def test_a_failed_submit_still_counts_against_the_hourly_brake(state, monkeypatch):
    """A bus that timed out may still have created the job: fail closed."""
    monkeypatch.setattr(dr, "CONTINUE_MAX_PER_HOUR", 1)
    row = work_row()
    fake = one_rc(row)
    fake.fail = True
    fake.sweep([row], state)
    fake.fail = False
    summary = fake.sweep([row], state, now=NOW + 60)
    assert len(fake.specs) == 1 and summary.get(dr.THROTTLED) == 1, summary


def test_the_hourly_brake_does_not_count_dry_runs(state, monkeypatch):
    monkeypatch.setattr(dr, "CONTINUE_MAX_PER_HOUR", 1)
    row = work_row()
    fake = one_rc(row)
    fake.sweep([row], state, dry_run=True)
    fake.sweep([row], state, dry_run=True)
    fake.sweep([row], state)
    assert len(fake.specs) == 1


def test_a_zero_hourly_brake_creates_nothing(state, monkeypatch):
    monkeypatch.setattr(dr, "CONTINUE_MAX_PER_HOUR", 0)
    row = work_row()
    fake = one_rc(row)
    summary = fake.sweep([row], state)
    assert fake.specs == [] and summary.get(dr.THROTTLED) == 1, summary


# ── guard 3: a non-durable store creates nothing ────────────────────────────


class RefusingBackend(MemoryBackend):
    """Loads fine, refuses every save: ``persist_ok`` goes False on a write."""

    name = "file"

    def save(self, namespace, records):
        return False


class UnreadableBackend(MemoryBackend):
    name = "file"

    def load(self, namespace):
        raise OSError("state file unreadable")


def not_persisting() -> DurableStateStore:
    store = DurableStateStore(RefusingBackend(), refresh_secs=0.0)
    store.set_value("probe", 1, ttl=60)
    assert store.loaded_ok and not store.persist_ok
    return store


def not_loaded() -> DurableStateStore:
    store = DurableStateStore(UnreadableBackend(), refresh_secs=0.0)
    assert not store.loaded_ok
    return store


@pytest.mark.parametrize("make_store", [not_persisting, not_loaded], ids=["persist_failed", "load_failed"])
def test_a_non_durable_store_creates_no_continuation(make_store):
    store = make_store()
    row = work_row()
    fake = one_rc(row)
    summary = fake.sweep([row], store)
    assert fake.specs == [] and fake.escalated == [] and fake.lookups == [], summary
    assert summary.get("refused") == "durable_state_unavailable", summary


def test_a_store_that_stops_persisting_mid_sweep_stops_creating(monkeypatch):
    """The brake's own write must land before a job is submitted."""
    backend = MemoryBackend()
    store = DurableStateStore(backend, refresh_secs=0.0)
    rows, kids = many_rows(3)
    fake = Fake(kids)
    real_submit = fake.submit

    def submit_then_break(spec):
        out = real_submit(spec)
        monkeypatch.setattr(backend, "save", lambda *_a, **_k: False)
        return out

    fake.submit = submit_then_break
    fake.sweep(rows, store, max_per_sweep=100)
    assert len(fake.specs) == 1, [s["parent_job_id"] for s in fake.specs]


def test_run_continuation_sweep_refuses_a_non_durable_store(monkeypatch):
    from hive.workers import doctor

    monkeypatch.setattr(dr, "REMEDIATE_ENABLED", True)
    monkeypatch.setattr(doctor, "STATE", not_persisting())
    posts: list = []

    def http(method, path, body=None, **_kw):
        if method == "POST":
            posts.append(body)
        return 200, {"jobs": []}

    monkeypatch.setattr(doctor, "_http", http)
    with pytest.raises(RuntimeError, match="durable state"):
        doctor.run_continuation_sweep([quiet_work_row()], roster=ROSTER, now=NOW)
    assert posts == []
