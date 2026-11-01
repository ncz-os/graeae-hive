"""The grind-until-green loop's GATES are real evidence, not a test double (2026-11-01).

``continue_one`` only calls a row GREEN when a strong, non-fixer approve on
the exact tip comes with a clean merged-tree suite and a clean import
preflight. Until now nothing in production supplied those gates, so no row
could ever be GREEN. This file covers the production half:

* ``GateProvider``: per (row, exact tip) the gates come from a durable gate
  record keyed by repo + base + tip; absent, stale or malformed is None --
  never clean;
* ``verify_spec`` / the verify request: a ``verify:build:zeroclaw`` Hive job
  (eligible kind zeroclaw, no host pin) that merges the base into the tip and
  runs the suite and the import preflight; one durable marker per (row, tip),
  capped per sweep and per hour, honouring the kill switch and DRY_RUN, never
  for a lane-B row and never for a review job;
* ``read_verify_result``: the verify job's result JSON copied into the gate
  record (only for the exact repo/base/tip it was asked about);
* the sweep: strong approve + no record -> verify once; clean -> GREEN and the
  parent row is recorded ``approved_mergeable``; dirty -> the fix path; stale
  -> ignored; a fixer-family approve -> ignored; the doctor never merges.
"""

from __future__ import annotations

import json
import os

os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")

import pytest

from hive.workers import doctor_remediation as dr
from hive.workers.doctor_state import DurableStateStore, MemoryBackend
from test_doctor_continuation_rounds_20261101 import (  # noqa: F401 -- table_only is an autouse fixture
    NOW,
    ROSTER,
    TIP,
    child,
    table_only,
    work_row,
)
from test_doctor_grind_until_green_20261101 import approve_by, rc_with

REPO = "graeae-hive"
BASE = "deploy/integrate-20261017"
OTHER_TIP = "7" * 40


@pytest.fixture
def state():
    return DurableStateStore(MemoryBackend(), refresh_secs=0.0)


def verify_result(tip: str = TIP, *, returncode: int = 0, summary: str = "812 passed in 41.20s", clean=True) -> dict:
    return {
        "returncode": returncode,
        "summary": summary,
        "import_preflight": {"clean": clean, "detail": "3 file(s) compiled" if clean else "x.py: SyntaxError line 4"},
        "base": BASE,
        "tip": tip,
    }


def verify_job(result: dict | None = None, *, status: str = "done", jid: str = "01a1d000-0000-7000-8000-000000000001"):
    return {
        "id": jid,
        "kind": dr.VERIFY_KIND,
        "status": status,
        "repo": REPO,
        "result": verify_result() if result is None else result,
    }


def record(state, tip: str = TIP, **kw) -> dict:
    job = verify_job(verify_result(tip, **kw))
    rec = dr.read_verify_result(job, repo=REPO, base=BASE, tip=tip)
    assert rec is not None
    dr.save_gate_record(state, rec, now=NOW)
    return rec


# ── the provider ────────────────────────────────────────────────────────────


def test_no_gate_record_is_unknown_never_clean(state):
    provider = dr.GateProvider(state)
    assert provider.for_row(work_row(), TIP) is None
    # Without a row there is no repo/base to key on: unknown.
    assert provider(TIP) is None


def test_a_clean_record_for_the_exact_tip_is_returned_in_green_shape(state):
    record(state)
    gates = dr.GateProvider(state).for_row(work_row(), TIP)
    assert set(gates) == {"suite", "import_preflight"}
    assert gates["suite"]["clean"] is True
    assert gates["import_preflight"] == {"clean": True, "detail": "3 file(s) compiled"}
    assert dr.green_blocker({"verdict": "approve", "strong": True, "gates": gates}) == ""


def test_a_record_for_another_tip_is_stale(state):
    record(state, OTHER_TIP)
    assert dr.GateProvider(state).for_row(work_row(), TIP) is None


def test_a_record_for_another_base_is_stale(state):
    record(state)
    row = work_row(base_branch="main")
    assert dr.GateProvider(state).for_row(row, TIP) is None


def test_a_record_for_another_repo_is_stale(state):
    record(state)
    row = work_row(repo="fleet-ops")
    assert dr.GateProvider(state).for_row(row, TIP) is None


@pytest.mark.parametrize(
    "mangle",
    [
        lambda r: r.pop("suite"),
        lambda r: r.pop("import_preflight"),
        lambda r: r.__setitem__("suite", {"clean": "yes", "detail": ""}),
        lambda r: r.__setitem__("import_preflight", "clean"),
        lambda r: r.__setitem__("tip", OTHER_TIP),
        lambda r: r.__setitem__("base", "main"),
        lambda r: r.__setitem__("repo", "fleet-ops"),
    ],
)
def test_a_malformed_record_is_unknown(state, mangle):
    rec = record(state)
    mangle(rec)
    state.set_value(dr.gate_record_key(REPO, BASE, TIP), rec, ttl=dr.MARK_TTL_SEC)
    assert dr.GateProvider(state).for_row(work_row(), TIP) is None


def test_a_non_dict_record_is_unknown(state):
    state.set_value(dr.gate_record_key(REPO, BASE, TIP), "clean", ttl=dr.MARK_TTL_SEC)
    assert dr.GateProvider(state).for_row(work_row(), TIP) is None


def test_a_dirty_record_is_returned_dirty(state):
    record(state, returncode=1, summary="3 failed, 809 passed")
    gates = dr.GateProvider(state).for_row(work_row(), TIP)
    assert gates["suite"]["clean"] is False
    assert dr.green_blocker({"verdict": "approve", "strong": True, "gates": gates}) == dr.SUITE_NOT_CLEAN


# ── the reader ──────────────────────────────────────────────────────────────


def test_the_reader_copies_a_done_jobs_result():
    rec = dr.read_verify_result(verify_job(), repo=REPO, base=BASE, tip=TIP)
    assert rec["tip"] == TIP and rec["base"] == BASE and rec["repo"] == REPO
    assert rec["suite"] == {"clean": True, "detail": "812 passed in 41.20s"}
    assert rec["import_preflight"]["clean"] is True
    assert rec["job_id"] == verify_job()["id"]


def test_the_reader_finds_the_result_json_in_the_tail():
    """The worker never parses a model's JSON into the result: the last JSON
    object in ``result.tail`` that names the verify fields is the evidence."""
    tail = "running suite...\n" + json.dumps(verify_result()) + "\n12034 tokens used\n"
    job = verify_job({"tail": tail, "exit_code": 0, "failure_reason": "completion_evidence_missing"})
    job["status"] = "needs-review"
    rec = dr.read_verify_result(job, repo=REPO, base=BASE, tip=TIP)
    assert rec is not None and rec["suite"]["clean"] is True


@pytest.mark.parametrize(
    "result",
    [
        {**verify_result(), "tip": OTHER_TIP},
        {**verify_result(), "base": "main"},
        {**verify_result(), "returncode": True},
        {**verify_result(), "returncode": "0"},
        {k: v for k, v in verify_result().items() if k != "summary"},
        {k: v for k, v in verify_result().items() if k != "import_preflight"},
        {**verify_result(), "import_preflight": {"clean": "true", "detail": ""}},
        {"tail": "no json here"},
        {},
    ],
)
def test_the_reader_refuses_a_malformed_or_foreign_result(result):
    assert dr.read_verify_result(verify_job(result), repo=REPO, base=BASE, tip=TIP) is None


@pytest.mark.parametrize("status", ["queued", "claimed", "running"])
def test_the_reader_ignores_an_unsettled_job(status):
    assert dr.read_verify_result(verify_job(status=status), repo=REPO, base=BASE, tip=TIP) is None


def test_the_reader_ignores_a_job_of_another_kind():
    job = verify_job()
    job["kind"] = "build:zeroclaw"
    assert dr.read_verify_result(job, repo=REPO, base=BASE, tip=TIP) is None


def test_the_provider_ingests_the_requested_verify_job(state):
    """Marker (row, tip) -> verify job id -> done result -> gate record."""
    row = work_row()
    job = verify_job()
    state.set_value(dr.verify_marker_key(row["id"], TIP), {"job_id": job["id"], "at": NOW}, ttl=dr.MARK_TTL_SEC)
    provider = dr.GateProvider(state, job_lookup=lambda jid: job if jid == job["id"] else None)
    gates = provider.for_row(row, TIP)
    assert gates is not None and gates["suite"]["clean"] is True
    # Copied durably: a second provider with no bus access reads the record.
    assert dr.GateProvider(state).for_row(row, TIP) == gates


def test_the_provider_survives_an_unreadable_bus(state):
    row = work_row()
    state.set_value(dr.verify_marker_key(row["id"], TIP), {"job_id": "x", "at": NOW}, ttl=dr.MARK_TTL_SEC)

    def boom(_jid):
        raise RuntimeError("bus down")

    assert dr.GateProvider(state, job_lookup=boom).for_row(row, TIP) is None


# ── the verify request spec ─────────────────────────────────────────────────


def test_the_verify_spec_is_a_zeroclaw_job_with_no_host_pin():
    row = work_row()
    spec = dr.verify_spec(row, TIP)
    assert spec["kind"] == "verify:build:zeroclaw" == dr.VERIFY_KIND
    assert spec["eligible_kinds"] == ["zeroclaw"]
    assert not spec.get("eligible_hosts")
    assert spec["repo"] == REPO
    assert spec["parent_job_id"] == row["id"]
    # The tip branch is what the worker clones; no head_branch -> no output branch.
    assert spec["base_branch"] == row["head_branch"]
    assert not spec.get("head_branch")
    assert spec["idempotency_key"] == f"{dr.VERIFY_KEY_PREFIX}:{row['id']}:{TIP[:12]}"
    for needle in (TIP, BASE, row["head_branch"], "returncode", "import_preflight"):
        assert needle in spec["description"]
    assert len(spec["description"]) < 2000


def test_the_verify_brief_is_a_module_constant():
    assert isinstance(dr.VERIFY_BRIEF, str) and "{tip}" in dr.VERIFY_BRIEF and "{base}" in dr.VERIFY_BRIEF


def test_a_review_row_never_gets_a_verify_spec():
    row = work_row(kind="review:build:zeroclaw")
    with pytest.raises(ValueError):
        dr.verify_spec(row, TIP)


def test_a_verify_row_is_recognised():
    assert dr.is_verify_kind({"kind": dr.VERIFY_KIND})
    assert not dr.is_verify_kind({"kind": "build:zeroclaw"})


@pytest.mark.parametrize("status", ["failed", "dead-letter"])
def test_a_verify_row_is_never_field_fixed_or_requeued(status):
    """It has no head_branch on purpose: the missing_branch fix must not touch it."""
    job = verify_job({"failure_reason": "loop_unresolved"}, status=status)
    job["base_branch"] = "hv/fo-merge-pass-20261031"
    assert dr.missing_branch_fields(job) == ["head_branch"]
    assert dr.classify(job) is None


def test_a_verify_row_is_never_continued(state):
    job = verify_job(status="needs-review")
    job["head_branch"] = "hv/fo-merge-pass-20261031"
    assert not dr._continuation_candidate(job)


# ── the sweep ───────────────────────────────────────────────────────────────


class Gated:
    """The sweep with the PRODUCTION gate provider and recorded side effects."""

    def __init__(self, state, children: dict[str, list[dict]], *, verify_fail: bool = False, mark_ok: bool = True):
        self.state = state
        self.children_of = children
        self.specs: list[dict] = []
        self.verifies: list[dict] = []
        self.escalated: list[tuple[str, str]] = []
        self.mergeable: list[tuple[str, dict]] = []
        self.jobs: dict[str, dict] = {}
        self.verify_fail = verify_fail
        self.mark_ok = mark_ok

    def children(self, row):
        return list(self.children_of.get(str(row.get("id")), []))

    def submit(self, spec):
        self.specs.append(spec)
        return 201, {"id": f"01a1b000-0000-7000-8000-{len(self.specs):012d}"}

    def request_verify(self, spec):
        self.verifies.append(spec)
        if self.verify_fail:
            return 503, {"detail": "bus unavailable"}
        return 201, {"id": f"01a1d000-0000-7000-8000-{len(self.verifies):012d}"}

    def escalate(self, job, why):
        self.escalated.append((str(job.get("id")), why))

    def mark_mergeable(self, row, green):
        self.mergeable.append((str(row.get("id")), green))
        return self.mark_ok

    def sweep(self, rows, **kw):
        kw.setdefault("roster", ROSTER)
        kw.setdefault("now", NOW)
        kw.setdefault("enabled", True)
        kw.setdefault("dry_run", False)
        return dr.continuation_sweep(
            rows,
            state=self.state,
            children=self.children,
            submit=self.submit,
            escalate=self.escalate,
            gates=dr.GateProvider(self.state, job_lookup=self.jobs.get),
            request_verify=self.request_verify,
            mark_mergeable=self.mark_mergeable,
            **kw,
        )


def strong(row: dict) -> dict[str, list[dict]]:
    return {row["id"]: [approve_by(row, "muse-spark-1.3")]}


def test_a_strong_approve_with_no_record_requests_verify_once(state):
    row = work_row()
    g = Gated(state, strong(row))
    summary = g.sweep([row])
    assert summary.get(dr.VERIFY_REQUESTED) == 1, summary
    assert len(g.verifies) == 1 and g.specs == [] and g.mergeable == []
    assert g.verifies[0]["kind"] == dr.VERIFY_KIND
    assert dr.green_marker(state, row["id"]) is None
    # Idempotent: the next sweep on the same tip requests nothing new.
    summary = g.sweep([row])
    assert len(g.verifies) == 1, summary
    assert summary.get(dr.VERIFY_PENDING) == 1, summary


def test_a_failed_verify_submit_leaves_no_marker(state):
    row = work_row()
    g = Gated(state, strong(row), verify_fail=True)
    summary = g.sweep([row])
    assert summary.get(dr.FAILED) == 1, summary
    assert state.value(dr.verify_marker_key(row["id"], TIP), None) is None


def test_a_local_approve_never_requests_verify(state):
    row = work_row()
    g = Gated(state, {row["id"]: [child("approve_google")]})
    g.sweep([row])
    assert g.verifies == []


def test_a_fixer_family_approve_is_ignored(state):
    row = work_row()
    state.set_value(dr._grind_key(row["id"]), {"rounds": 1, "fixers": ["muse-spark-1.3"]}, ttl=dr.MARK_TTL_SEC)
    record(state)
    g = Gated(state, strong(row))
    summary = g.sweep([row])
    assert g.verifies == [] and g.mergeable == [], summary
    assert summary.get(dr.GREEN) is None
    assert dr.green_marker(state, row["id"]) is None


def test_a_clean_record_is_green_and_marks_the_row_approved_mergeable(state):
    row = work_row()
    record(state)
    g = Gated(state, strong(row))
    summary = g.sweep([row])
    assert summary.get(dr.GREEN) == 1, summary
    assert g.verifies == [] and g.specs == []
    assert dr.green_marker(state, row["id"])["head"] == TIP
    [(jid, green)] = g.mergeable
    assert jid == row["id"]
    assert green["head"] == TIP
    assert green["reviewers"] == ["muse-spark-1.3"]
    assert green["gates"]["suite"]["clean"] is True
    # Recorded once: the next sweep sees the GREEN marker.
    g.sweep([row])
    assert len(g.mergeable) == 1


def test_a_mergeable_patch_that_fails_is_retried_next_sweep(state):
    row = work_row()
    record(state)
    g = Gated(state, strong(row), mark_ok=False)
    summary = g.sweep([row])
    assert summary.get(dr.GREEN) is None, summary
    assert dr.green_marker(state, row["id"]) is None
    g.mark_ok = True
    summary = g.sweep([row])
    assert summary.get(dr.GREEN) == 1, summary
    assert len(g.mergeable) == 2


def test_the_requested_verify_result_turns_the_row_green(state):
    """End to end: request -> verify job done -> next sweep reads it -> GREEN."""
    row = work_row()
    g = Gated(state, strong(row))
    g.sweep([row])
    jid = "01a1d000-0000-7000-8000-000000000001"
    g.jobs[jid] = verify_job(jid=jid)
    summary = g.sweep([row])
    assert summary.get(dr.GREEN) == 1, summary
    assert len(g.verifies) == 1 and len(g.mergeable) == 1


@pytest.mark.parametrize(
    ("kw", "blocker"),
    [({"returncode": 1, "summary": "2 failed, 810 passed"}, "suite"), ({"clean": False}, "import_preflight")],
)
def test_a_dirty_record_goes_down_the_fix_path(state, kw, blocker):
    row = work_row()
    record(state, **kw)
    g = Gated(state, strong(row))
    summary = g.sweep([row])
    assert summary.get(dr.APPLIED) == 1, summary
    assert g.mergeable == [] and g.verifies == []
    [spec] = g.specs
    assert ":fix:" in spec["idempotency_key"]
    assert spec["head_branch"] == row["head_branch"]
    assert spec["parent_job_id"] == row["id"]
    assert blocker in spec["description"]
    assert dr.green_marker(state, row["id"]) is None


def test_a_stale_record_is_ignored_and_the_tip_is_verified(state):
    row = work_row()
    record(state, OTHER_TIP)
    g = Gated(state, strong(row))
    summary = g.sweep([row])
    assert summary.get(dr.GREEN) is None, summary
    assert g.mergeable == []
    assert [v["idempotency_key"] for v in g.verifies] == [f"{dr.VERIFY_KEY_PREFIX}:{row['id']}:{TIP[:12]}"]


def test_a_blocking_request_changes_still_gets_its_fix(state):
    row = work_row()
    g = Gated(state, {row["id"]: [rc_with(row, "high")]})
    summary = g.sweep([row])
    assert summary.get(dr.APPLIED) == 1, summary
    assert g.verifies == []


def test_dry_run_requests_nothing_and_marks_nothing(state):
    row = work_row()
    g = Gated(state, strong(row))
    summary = g.sweep([row], dry_run=True)
    assert g.verifies == [] and g.mergeable == []
    assert state.value(dr.verify_marker_key(row["id"], TIP), None) is None
    assert summary.get(dr.DRY_WOULD_APPLY) == 1, summary


def test_dry_run_on_a_clean_record_marks_nothing(state):
    row = work_row()
    record(state)
    g = Gated(state, strong(row))
    g.sweep([row], dry_run=True)
    assert g.mergeable == [] and dr.green_marker(state, row["id"]) is None


def test_the_kill_switch_requests_nothing(state):
    row = work_row()
    g = Gated(state, strong(row))
    g.sweep([row], enabled=False)
    assert g.verifies == []


def test_a_lane_b_row_never_gets_a_verify(state):
    row = work_row(project="riskyeats")
    g = Gated(state, strong(row))
    g.sweep([row])
    assert g.verifies == []


def test_a_review_row_never_gets_a_verify(state):
    row = work_row(kind="review:build:zeroclaw")
    g = Gated(state, strong(row))
    g.sweep([row])
    assert g.verifies == []


def _rows(n: int) -> list[dict]:
    rows = []
    for i in range(n):
        rows.append(work_row(id=f"01a194cb-79b3-7c10-9e2a-3b4c5d6e7f{i:02d}", ended_at=NOW - 1000 + i))
    return rows


def test_verify_requests_are_capped_per_sweep(state, monkeypatch):
    monkeypatch.setattr(dr, "VERIFY_MAX_PER_SWEEP", 2)
    rows = _rows(4)
    g = Gated(state, {r["id"]: [approve_by(r, "muse-spark-1.3")] for r in rows})
    summary = g.sweep(rows)
    assert len(g.verifies) == 2, summary
    assert summary.get("deferred") == 2, summary


def test_verify_requests_are_capped_per_hour(state, monkeypatch):
    monkeypatch.setattr(dr, "VERIFY_MAX_PER_SWEEP", 10)
    monkeypatch.setattr(dr, "VERIFY_MAX_PER_HOUR", 3)
    rows = _rows(5)
    g = Gated(state, {r["id"]: [approve_by(r, "muse-spark-1.3")] for r in rows})
    summary = g.sweep(rows)
    assert len(g.verifies) == 3, summary
    assert summary.get(dr.THROTTLED) == 2, summary


def test_without_a_verify_hook_the_sweep_behaves_as_before(state):
    """No ``request_verify``: an unknown gate is just not green."""
    row = work_row()
    g = Gated(state, strong(row))
    summary = dr.continuation_sweep(
        [row],
        state=state,
        children=g.children,
        submit=g.submit,
        escalate=g.escalate,
        gates=dr.GateProvider(state),
        roster=ROSTER,
        now=NOW,
        enabled=True,
        dry_run=False,
    )
    assert summary.get(dr.GREEN) is None and summary.get(dr.VERIFY_REQUESTED) is None, summary


# ── the doctor wiring ───────────────────────────────────────────────────────


def test_run_continuation_sweep_passes_the_production_provider(monkeypatch):
    from hive.workers import doctor

    seen: dict = {}

    def fake_sweep(rows, **kw):
        seen.update(kw)
        return {}

    monkeypatch.setattr(doctor, "STATE", DurableStateStore(MemoryBackend(), refresh_secs=0))
    monkeypatch.setattr(dr, "continuation_sweep", fake_sweep)
    doctor.run_continuation_sweep([], roster=ROSTER, dry_run=False, now=NOW)
    assert isinstance(seen["gates"], dr.GateProvider)
    assert seen["gates"].state is doctor.STATE
    assert seen["gates"].job_lookup is doctor._chain_job
    assert seen["request_verify"] is doctor.submit_job_spec
    assert seen["mark_mergeable"] is doctor.record_green_mergeable


def test_record_green_mergeable_patches_review_state_and_never_merges(monkeypatch):
    from hive.workers import doctor

    calls: list[tuple] = []

    def http(method, path, body=None, **_kw):
        calls.append((method, path, body))
        return 200, {}

    monkeypatch.setattr(doctor, "_http", http)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *_a, **_k: None)
    row = work_row()
    gates = {"suite": {"clean": True, "detail": "812 passed"}, "import_preflight": {"clean": True, "detail": ""}}
    green = {
        "head": TIP,
        "reviewers": ["muse-spark-1.3"],
        "families": ["meta"],
        "review_job_id": "01a1e000-0000-7000-8000-000000000001",
        "basis": "exact",
        "gates": gates,
    }
    assert doctor.record_green_mergeable(row, green) is True
    [(method, path, body)] = calls
    assert method == "PATCH" and path == f"/v1/jobs/{row['id']}"
    # Still needs-review: only the merge pass merges, never the doctor.
    assert body["status"] == "needs-review"
    rs = body["result"]["review_state"]
    assert rs["state"] == "approved_mergeable"
    assert rs["reviewer_model"] == "muse-spark-1.3"
    assert rs["reviewed_sha"] == TIP
    assert rs["approved_mergeable"]["head_sha"] == TIP
    assert rs["gates"] == gates
    # The row's own evidence survives the PATCH.
    assert body["result"]["head_sha"] == TIP
    assert not any(m == "POST" and "merge" in p for m, p, _b in calls)


def test_record_green_mergeable_reports_a_failed_patch(monkeypatch):
    from hive.workers import doctor

    monkeypatch.setattr(doctor, "_http", lambda *_a, **_k: (503, None))
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *_a, **_k: None)
    green = {"head": TIP, "reviewers": ["muse-spark-1.3"], "gates": {}}
    assert doctor.record_green_mergeable(work_row(), green) is False


def test_a_verify_row_is_never_handed_to_review(monkeypatch):
    from hive.workers import doctor

    monkeypatch.setattr(doctor, "STATE", DurableStateStore(MemoryBackend(), refresh_secs=0))

    def no_bus(*_a, **_k):
        raise AssertionError("a verify row must be skipped before any bus call")

    monkeypatch.setattr(doctor, "_http", no_bus)
    job = verify_job(status="needs-review")
    assert doctor.ensure_review_followup(job) == "skipped_verify_job"


def test_record_green_mergeable_refuses_an_empty_head_or_unclean_gates(monkeypatch):
    """DeepSeek 2026-11-01: the recorder must fail closed on a malformed GREEN."""
    from hive.workers import doctor

    patched = []
    monkeypatch.setattr(doctor, "_patch_review_state", lambda row, state: patched.append(state) or True)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    clean = {"suite": {"clean": True, "detail": "812 passed"}, "import_preflight": {"clean": True, "detail": ""}}
    row = {"id": "01a1b000-0000-7000-8000-000000000001", "result": {}}
    good = {"head": "a" * 40, "reviewers": ["muse-spark-1.3"], "gates": clean}
    assert doctor.record_green_mergeable(row, dict(good)) is True
    assert len(patched) == 1
    for bad in (
        {**good, "head": ""},
        {**good, "head": None},
        {**good, "gates": None},
        {**good, "gates": {}},
        {**good, "gates": {"suite": {"clean": True}}},
        {**good, "gates": {"suite": {"clean": False}, "import_preflight": {"clean": True}}},
        {**good, "gates": {"suite": {"clean": True}, "import_preflight": {"clean": "yes"}}},
    ):
        assert doctor.record_green_mergeable(row, bad) is False, bad
    assert len(patched) == 1
