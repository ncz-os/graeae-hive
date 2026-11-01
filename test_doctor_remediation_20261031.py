"""The doctor FIXES jobs, it does not only report them (operator 2026-10-31).

*"the doctor has to review everything in the triage queue, that is the point of
the doctor. it needs to actually fix jobs to alter the fields to make them run
correctly and get completed jobs reviewed if the auto reviewer is busy."*

`hive/workers/doctor_remediation.py` owns that: it classifies the cause of a
failed / needs-review / dead-letter row, changes the FIELD that made the row
unrunnable and re-enters it through the existing admin reclassify path, with an
audit event per change. One test per cause, on the real row SHAPES from the
2026-10-29/30 triage evidence (sanitised — see
``tests/fixtures/doctor_remediation_rows.json``), plus idempotency, the caps,
the lane-B exclusion, the no-op on a healthy row and the fake clock.

The side effects (field patch, reclassify, escalation) are injected, so every
assertion below is about the DECISION and the exact field change — never about a
live bus.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from hive import delivery_ref
from hive.workers import doctor_remediation as dr
from hive.workers.doctor_state import DurableStateStore, MemoryBackend

FIXTURE = Path(__file__).parent / "tests" / "fixtures" / "doctor_remediation_rows.json"

#: A commit that exists in the fixtures' pretend repository.
REAL_HEAD = "4b8c1e0a7d92f36b58a0c1d4e7f9b2a3c6d8e0f1"
ROSTER = [
    {"id": "tydeus-qwen38", "model_id": "qwen38", "family": "alibaba", "cost_class": "A", "concurrency": 4},
    {"id": "cerberus-gemma", "model_id": "gemma4-31b", "family": "google", "cost_class": "A", "concurrency": 2},
    {"id": "cerberus-gpt-oss", "model_id": "gpt-oss-20b", "family": "openai", "cost_class": "A", "concurrency": 8},
    {"id": "minimax-m3", "model_id": "MiniMax-M3", "family": "minimax", "cost_class": "B", "concurrency": 2},
    {"id": "deepseek-flash", "model_id": "deepseek-flash", "family": "deepseek", "cost_class": "B", "concurrency": 2},
]


#: The fixture writes preserved workspaces under ``{workspace_root}``; each test
#: roots it at its own ``tmp_path`` (pushed by the autouse fixture below).
WORKSPACE_PLACEHOLDER = "{workspace_root}"
_WORKSPACE_ROOT: list[str] = []


@pytest.fixture(autouse=True)
def _workspace_root(tmp_path):
    _WORKSPACE_ROOT.append(str(tmp_path / "workspaces"))
    yield
    _WORKSPACE_ROOT.pop()


def row(name: str, **overrides) -> dict:
    """One fixture row (a deep copy), with ``result`` overrides merged."""
    text = FIXTURE.read_text()
    if _WORKSPACE_ROOT:
        text = text.replace(WORKSPACE_PLACEHOLDER, json.dumps(_WORKSPACE_ROOT[-1])[1:-1])
    data = json.loads(text)
    base = json.loads(json.dumps(data[name]))
    result_overrides = overrides.pop("result", None)
    base.update(overrides)
    if result_overrides:
        base["result"] = {**base.get("result", {}), **result_overrides}
    return base


@pytest.fixture
def state():
    """A fresh durable store per test (same type the doctor runs with)."""
    return DurableStateStore(MemoryBackend(), refresh_secs=0.0)


class Fake:
    """The three injected side effects, recorded."""

    def __init__(self):
        self.fields: list[tuple[str, dict, str]] = []
        self.reclassify: list[tuple[str, str, str, bool, str]] = []
        self.escalated: list[tuple[str, str]] = []
        self.reviewed: list[dict] = []
        self.rescued: list[tuple[str, str]] = []

    def apply_fields(self, jid, changes, reason):
        self.fields.append((jid, changes, reason))
        return 200, {"ok": [jid]}

    def reclassify_to(self, jid, status, reason, override, override_reason):
        self.reclassify.append((jid, status, reason, override, override_reason))
        return 200, {"ok": [jid]}

    def escalate(self, job, why):
        self.escalated.append((str(job.get("id")), why))

    def advance_review(self, job):
        self.reviewed.append(job)

    def rescue_workspace(self, job, branch):
        self.rescued.append((str(job.get("id")), branch))
        return True, "pushed"

    def run(self, job, state, **kw):
        kw.setdefault("default_branch", lambda _j: "deploy/integrate-20261017")
        kw.setdefault("branch_exists", lambda _b: True)
        kw.setdefault("ref_exists", lambda _r: True)
        kw.setdefault("roster", ROSTER)
        return dr.remediate_one(
            job,
            state=state,
            apply_fields=self.apply_fields,
            reclassify=self.reclassify_to,
            escalate=self.escalate,
            advance_review=self.advance_review,
            rescue_workspace=self.rescue_workspace,
            **kw,
        )


# ── (a) unresolvable baseline: the 36-char composite ───────────────────────


def test_a_composite_baseline_is_repaired_with_the_real_head(state):
    """The observed defect: ``head_sha[:28] + workspace_uuid[-8:]`` named no
    commit, ``git merge-base --is-ancestor`` exited 128 and the delivery check
    aborted. The repair resolves the real head from the pushed branch."""
    fake = Fake()
    job = row("composite_baseline")
    composite = job["result"]["baseline"]
    assert len(composite) == 36, "the fixture must carry the real 36-char shape"
    assert dr.classify(job).name == dr.UNRESOLVABLE_REF

    outcome = fake.run(
        job,
        state,
        ref_exists=lambda ref: ref in (REAL_HEAD, "refs/heads/" + job["head_branch"]),
        resolve_ref=lambda ref: REAL_HEAD,
    )

    assert outcome.status == dr.APPLIED
    jid, changes, reason = fake.fields[0]
    assert changes["baseline"]["before"] == composite
    assert changes["baseline"]["after"] == REAL_HEAD
    assert changes["verify_baseline"]["after"] == REAL_HEAD
    # One key for the head, and it is a ROW field name: the executor takes
    # ``{field: {before, after}}`` and cannot parse a ``result.`` prefix.
    assert changes["head_sha"]["after"] == REAL_HEAD
    assert "result.head_sha" not in changes
    assert "exited 128" in reason or "unresolvable_baseline" in reason
    assert fake.reclassify[0][0] == jid and fake.reclassify[0][1] == "queued"
    # The audit trail is on the plan's reason AND in the durable attempt count.
    assert dr.attempts(state, jid) == 1


def test_a_composite_baseline_whose_head_cannot_be_resolved_escalates_nothing(state):
    """No proven head means NO rewrite: the row is left for a human, not
    pointed at a guess."""
    fake = Fake()
    job = row("composite_baseline")
    outcome = fake.run(job, state, ref_exists=lambda _ref: False)
    assert outcome.status == dr.NO_CHANGE
    assert outcome.reason == "no_provable_fix"
    assert fake.fields == [] and fake.reclassify == []


def test_the_composite_cause_names_the_result_holder_it_reads():
    """The baseline lives on the row's RESULT, so the cause must name it as
    ``result.<key>``. A bare key resolves against the job columns and reads
    None: the repair would then record a "before" of None and patch nothing
    the verify step actually looks at."""
    job = row("composite_baseline")
    cause = dr.classify(job)
    assert cause.name == dr.UNRESOLVABLE_REF
    assert cause.fields, "the fixture must carry at least one bad baseline"
    for field_name in cause.fields:
        assert field_name.startswith("result."), f"{field_name} is not holder-qualified"
        key = field_name.split(".", 1)[-1]
        assert job["result"].get(key) == job["result"]["baseline"]

    plan = dr.plan_for(
        job,
        cause,
        default_branch="deploy/integrate-20261017",
        ref_exists=lambda _ref: True,
        resolve_ref=lambda _ref: REAL_HEAD,
    )
    # The PLAN is keyed by the row's own field name, and every "before" is the
    # real value the verify step read (never None).
    assert plan.changes["baseline"]["before"] == job["result"]["baseline"]
    assert plan.changes["verify_baseline"]["before"] == job["result"]["verify_baseline"]


def test_a_baseline_held_on_the_job_column_is_named_without_the_result_prefix():
    """The older row shape keeps the bad baseline on the job column. The cause
    must name it BARE (so it reads the column that holds it), and the plan's
    "before" must be the real value, not None from the wrong holder."""
    job = row("composite_baseline")
    bad = job["result"].pop("baseline")
    job["result"].pop("verify_baseline")
    job["baseline"] = bad
    assert dr.classify(job).name == dr.UNRESOLVABLE_REF

    cause = dr.classify(job)
    assert cause.fields == ("baseline",), cause.fields
    plan = dr.plan_for(
        job,
        cause,
        default_branch="deploy/integrate-20261017",
        ref_exists=lambda _ref: True,
        resolve_ref=lambda _ref: REAL_HEAD,
    )
    assert plan.changes["baseline"]["before"] == bad
    assert plan.changes["baseline"]["after"] == REAL_HEAD


def test_the_module_exposes_every_symbol_the_doctor_call_site_uses():
    """The thin call site in ``doctor.py`` reaches into this module by name:
    the scan statuses, the on/off switch, the dry-run flag, the sweep itself,
    and ``time`` for the fake clock. A rename here breaks the doctor at run
    time, not at import, so the contract is pinned explicitly."""
    assert dr.SCAN_STATUSES == ("failed", "needs-review", "dead-letter")
    assert isinstance(dr.REMEDIATE_ENABLED, bool)
    assert isinstance(dr.DRY_RUN, bool)
    assert callable(dr.sweep)
    # ``time`` is the injected-clock source every helper reads.
    assert dr.time is time


def test_the_audit_entry_exposes_top_level_before_and_after():
    """``remediate_one`` reads ``event["after"]`` to report what it wrote; the
    per-field before/after lives one level down. Both must be present."""
    cause = dr.Cause(dr.UNRESOLVABLE_REF, "composite_baseline", ("result.baseline",))
    event = dr._audit_entry(
        cause,
        {"result.baseline": {"before": "bad", "after": "good"}},
        "why",
        1234.0,
        "requeue",
    )
    assert event["after"] == {"result.baseline": "good"}
    assert event["before"] == {"result.baseline": "bad"}
    assert event["cause"] == dr.UNRESOLVABLE_REF
    assert event["at"] == 1234.0
    # JSON-safe: the entry is persisted on the row.
    json.dumps(event)


def test_fixture_paths_are_portable(tmp_path):
    """Muse review finding (2026-10-31, low): the fixture carried a real
    operator home path (``/home/<operator>/hive-workspaces/<uuid>``), which
    leaks one machine's layout into a committed fixture and makes the rows fail
    to describe the shape they claim to. Every preserved workspace in the
    fixture is an absolute path (``stranded_workspace`` requires one) that is
    NOT under an operator home, and the same value read under a ``tmp_path``
    root still classifies as stranded WIP -- the row is data, not this host's
    filesystem."""
    data = json.loads(FIXTURE.read_text())
    preserved = [
        payload["result"]["workspace_preserved"]
        for payload in data.values()
        if isinstance(payload, dict)
        and isinstance(payload.get("result"), dict)
        and payload["result"].get("workspace_preserved")
    ]
    assert preserved, "the fixture must still carry at least one preserved workspace"
    for path in preserved:
        # Round 7: no absolute path at all is committed -- not an operator home
        # and not a shared system directory either. The placeholder is rooted
        # at this test's ``tmp_path`` by ``row()``.
        assert path.startswith(WORKSPACE_PLACEHOLDER + "/"), f"{path} must be rooted at the placeholder"
        assert ".." not in path.split("/"), f"{path} must not traverse"
    raw = FIXTURE.read_text()
    for absolute in ("/home/", "/var/", "/tmp/", "/srv/", "/opt/"):
        assert absolute not in raw, f"the fixture names a host path ({absolute})"
    # The row ``row()`` hands a test is absolute (``stranded_workspace``
    # requires that) and lives under THIS test's tmp_path.
    job = row("stranded_wip")
    path = job["result"]["workspace_preserved"]
    assert Path(path).is_absolute()
    assert Path(path).is_relative_to(tmp_path), path
    assert dr.classify(job).name == dr.STRANDED_WIP
    assert dr.stranded_workspace(job) == path


def test_every_classified_cause_name_is_one_the_plan_table_knows():
    """``plan_for`` and the audit records switch on ``cause.name``. A name
    outside CAUSES would fall through to "no plan" and look like a healthy
    row, so the dataclass refuses one at construction."""
    with pytest.raises(ValueError, match="unknown cause name"):
        dr.Cause("capped_model")  # a model_problem DETAIL, not a cause name
    # Every fixture row classifies to a name the plan table knows.
    for name, payload in json.loads(FIXTURE.read_text()).items():
        if not isinstance(payload, dict):
            continue
        cause = dr.classify(payload)
        if cause is not None:
            assert cause.name in dr.CAUSES, f"{name} classified as {cause.name}"


def test_a_capped_route_is_classified_as_unavailable_model():
    """The capped/tier reasons are DETAILS under ONE cause name, so the plan
    table handles them; they must never surface as their own cause."""
    job = row("unavailable_model")
    cause = dr.classify(job)
    assert cause.name == dr.UNAVAILABLE_MODEL
    assert cause.detail in {"unavailable_model", "capped_model", "tier_too_low"}
    assert dr.healthy_alternative(job) is not None, "a capped row needs a healthy alternative"


def test_the_sweep_cap_reads_the_applied_counter_it_seeds():
    """The per-sweep cap is enforced against the same key the summary seeds and
    the outcome increments. A literal read against a renamed seed lets the cap
    through at the wrong size."""
    from hive.workers.doctor_state import DurableStateStore, MemoryBackend

    fake = Fake()
    rows = [row("host_pinned", id=f"01a18c66-7b02-7c44-a01e-58d2f3b9c8{i:02d}") for i in range(4)]
    summary = dr.sweep(
        rows,
        state=DurableStateStore(MemoryBackend(), refresh_secs=0.0),
        apply_fields=fake.apply_fields,
        reclassify=fake.reclassify_to,
        escalate=fake.escalate,
        default_branch=lambda _j: "deploy/integrate-20261017",
        branch_exists=lambda _b: True,
        roster=ROSTER,
        now=600.0,
        max_per_sweep=1,
    )
    assert dr.APPLIED == "applied"
    assert summary[dr.APPLIED] == 1
    assert len(fake.reclassify) == 1


def test_the_builder_never_composes_a_boundary_from_a_prefix_and_a_uuid():
    """The cause of (a): the value was BUILT. ``build_boundary`` returns the
    commit alone, or nothing — never the 36-char composite."""
    head = "9c1f4e0a7b2d6c88f13a05be7d41c2e3b68d1a4b"
    workspace = "01a18c47-2f31-7d20-8c1e-4d0a5c9b6e11"
    assert delivery_ref.build_boundary(workspace, head) == head
    assert delivery_ref.build_boundary(workspace, "") == ""
    # The exact shape the audit found, and the reason it is not a commit.
    composite = head[:28] + workspace.replace("-", "")[-8:]
    assert len(composite) == 36
    assert delivery_ref.is_composite(composite)
    assert not delivery_ref.is_commit(composite)
    assert delivery_ref.is_commit(head)


# ── (b) missing / wrong branch fields ──────────────────────────────────────


def test_b_missing_base_branch_is_set_from_the_registry_after_it_is_verified(state):
    fake = Fake()
    job = row("missing_base")
    seen = []

    def branch_exists(name):
        seen.append(name)
        return name in ("deploy/integrate-20261017", job["head_branch"])

    outcome = fake.run(job, state, branch_exists=branch_exists)

    assert outcome.status == dr.APPLIED
    # Round 7: the head branch the row keeps is verified too (a well-formed head
    # that is absent on the remote is as missing as an empty one).
    assert seen == ["deploy/integrate-20261017", job["head_branch"]], (
        "both branches must be verified before the requeue"
    )
    changes = fake.fields[0][1]
    assert changes["base_branch"] == {"before": "", "after": "deploy/integrate-20261017"}
    assert fake.reclassify[0][1] == "queued"


def test_b_a_branch_that_does_not_exist_on_argonas_is_not_written(state):
    fake = Fake()
    outcome = fake.run(row("missing_base"), state, branch_exists=lambda _b: False)
    assert outcome.status == dr.NO_CHANGE
    assert fake.fields == []


# ── (c) eligible_hosts pins the host that failed the job ───────────────────


def test_c_a_pin_to_the_host_that_failed_the_job_is_cleared(state):
    fake = Fake()
    job = row("host_pinned")
    assert dr.classify(job).name == dr.HOST_PINNED
    outcome = fake.run(job, state)
    assert outcome.status == dr.APPLIED
    assert fake.fields[0][1]["eligible_hosts"] == {"before": ["cerberus"], "after": []}
    assert fake.reclassify[0][1] == "queued"


# ── (d) retry budget spent by infrastructure only ──────────────────────────


def test_d_infrastructure_budget_exhaustion_requeues_with_an_override(state):
    fake = Fake()
    job = row("infra_retries")
    assert dr.classify(job).name == dr.INFRA_RETRIES
    outcome = fake.run(job, state)
    assert outcome.status == dr.APPLIED
    jid, status, reason, override, override_reason = fake.reclassify[0]
    assert status == "queued"
    assert override is True and override_reason.startswith("infra_retry:")
    assert fake.fields[0][1]["max_retries"] == {"before": 2, "after": 2}


def test_d_killed_by_restart_exit_143_is_infrastructure(state):
    job = row("killed_by_restart")
    assert dr.infra_cause(job) == "restart_killed"
    assert dr.classify(job).name == dr.INFRA_RETRIES
    fake = Fake()
    fake.run(job, state)
    assert fake.reclassify[0][3] is True


def test_d_a_logic_failure_never_gets_a_retry_override(state):
    """Budget spent by failing tests is the submitter's decision, not the
    doctor's: no override, no requeue."""
    fake = Fake()
    job = row("logic_failure_exhausted")
    assert dr.infra_cause(job) is None
    assert dr.classify(job) is None
    outcome = fake.run(job, state)
    assert outcome.status == dr.NO_CHANGE
    assert fake.reclassify == [] and fake.fields == []


# ── (e) preferred model / tier that cannot serve the job ───────────────────


def test_e_an_unavailable_model_switches_to_a_healthy_roster_route(state):
    fake = Fake()
    job = row("unavailable_model")
    assert dr.classify(job).name == dr.UNAVAILABLE_MODEL
    outcome = fake.run(job, state)
    assert outcome.status == dr.APPLIED
    changes = fake.fields[0][1]
    assert changes["preferred_models"]["before"] == ["gpt-5.3-codex-spark"]
    picked = changes["preferred_models"]["after"]
    assert len(picked) == 1 and picked[0] in {r["model_id"] for r in ROSTER}
    assert max_cost_tier_ok(job["max_cost_tier"], picked[0]), "never above the row's cost ceiling"


def max_cost_tier_ok(tier, model):
    rank = {"A": 0, "B": 1, "C": 2}
    route = next(r for r in ROSTER if model in (r["model_id"], r["id"]))
    return rank[route["cost_class"]] <= rank[tier]


def test_e_a_capped_only_roster_leaves_the_row_alone(state):
    fake = Fake()
    capped = {r["id"] for r in ROSTER if r["cost_class"] == "A"}
    outcome = fake.run(row("unavailable_model"), state, capped=capped, job_capped=capped)
    # With every cheap route capped and the row's ceiling at B, the B routes
    # remain: a fix is still provable. With everything capped it is not.
    assert outcome.status in (dr.APPLIED, dr.NO_CHANGE)
    all_capped = {r["id"] for r in ROSTER}
    fake2 = Fake()
    job2 = row("unavailable_model", capped_routes=sorted(all_capped))
    assert fake2.run(job2, state).status == dr.NO_CHANGE
    assert fake2.fields == []


# ── (f) stranded worker WIP ────────────────────────────────────────────────


def test_f_stranded_wip_is_pushed_and_handed_to_review(state):
    fake = Fake()
    job = row("stranded_wip")
    assert dr.classify(job).name == dr.STRANDED_WIP
    outcome = fake.run(job, state)
    assert outcome.status == dr.APPLIED
    assert fake.rescued and fake.rescued[0][1] == job["head_branch"], "the workspace HEAD is pushed"
    assert fake.reclassify[0][1] == "needs-review"
    assert fake.fields[0][1]["workspace_pushed"] == {"before": False, "after": True}


def test_f_a_failed_push_does_not_mark_the_row_reviewable(state):
    fake = Fake()
    fake.rescue_workspace = lambda _job, _branch: (False, "push rejected: no upstream")
    outcome = fake.run(row("stranded_wip"), state)
    assert outcome.status == dr.FAILED
    assert fake.reclassify == [], "a row whose work did not reach the branch is not handed to review"


# ── (g) finished work whose reviewer was busy ──────────────────────────────


def test_g_delivered_work_goes_to_the_review_queue_instead_of_failing(state):
    fake = Fake()
    job = row("waiting_review")
    assert dr.classify(job).name == dr.WAITING_REVIEW
    outcome = fake.run(job, state)
    assert outcome.status == dr.APPLIED
    jid, status, reason, _override, _override_reason = fake.reclassify[0]
    assert status == "needs-review", "finished work is never re-run: it is reviewed"
    assert reason.startswith("review_busy:")
    assert fake.reviewed and fake.reviewed[0]["id"] == jid
    changes = fake.fields[0][1]
    assert changes["review_state"]["after"]["state"] == "awaiting_review"
    assert changes["review_state"]["after"]["author_family"] == "alibaba"


def test_g_a_row_that_already_carries_a_verdict_is_not_re_reviewed(state):
    fake = Fake()
    job = row("healthy_done")
    cause = dr.classify(job, has_verdict=lambda j: bool(j["result"].get("review_evidence")))
    assert cause is None
    assert fake.run(job, state).status == dr.NO_CHANGE


# ── idempotency across sweeps ──────────────────────────────────────────────


def test_a_second_sweep_over_the_same_row_is_a_no_op(state):
    fake = Fake()
    job = row("host_pinned")
    assert fake.run(job, state).status == dr.APPLIED
    # The second sweep runs AFTER the first, on the same clock the marker was
    # stamped with: a "now" in the past is not a later sweep, and since round 2
    # a future-dated stamp is not fresh either (the marker's age must be >= 0).
    assert fake.run(job, state, now=time.time() + 60).status == dr.NO_CHANGE
    assert len(fake.fields) == 1 and len(fake.reclassify) == 1


def test_the_same_cause_at_a_new_head_is_a_new_remediation(state):
    """A re-run that moves the head is a NEW fact: the marker is per (cause,
    head), so the fix applies again instead of being suppressed forever."""
    fake = Fake()
    job = row("waiting_review")
    assert fake.run(job, state, now=1000.0).status == dr.APPLIED
    moved = row("waiting_review", result={"head_sha": "c" * 40, "commits": ["c" * 40]})
    assert fake.run(moved, state, now=2000.0).status == dr.APPLIED
    assert len(fake.reclassify) == 2


def test_two_sweeps_in_one_pass_act_once(dr_module_fresh_state=None):  # noqa: ATE001 - unused fixture
    """The in-pass guard: the SAME row listed twice is remediated once."""
    state = DurableStateStore(MemoryBackend(), refresh_secs=0.0)
    fake = Fake()
    job = row("host_pinned")
    summary = dr.sweep(
        [job, dict(job)],
        state=state,
        apply_fields=fake.apply_fields,
        reclassify=fake.reclassify_to,
        escalate=fake.escalate,
        default_branch=lambda _j: "deploy/integrate-20261017",
        branch_exists=lambda _b: True,
        roster=ROSTER,
        now=100.0,
    )
    assert summary["applied"] == 1
    assert len(fake.reclassify) == 1


# ── caps ───────────────────────────────────────────────────────────────────


def test_the_attempt_cap_escalates_once_and_then_stops(state):
    fake = Fake()
    job = row("host_pinned")
    assert fake.run(job, state, now=1.0).status == dr.APPLIED
    # The same fields again (the requeue did not take): a second remediation.
    second = fake.run(job, state, now=2.0)
    assert second.status == dr.NO_CHANGE  # marker for this cause/head is live
    state.forget(dr.mark_key(job["id"], dr.Cause(dr.HOST_PINNED, "eligible_hosts_pin")))
    assert fake.run(job, state, now=3.0).status == dr.APPLIED
    assert dr.attempts(state, job["id"]) == 2
    state.forget(dr.mark_key(job["id"], dr.Cause(dr.HOST_PINNED, "eligible_hosts_pin")))
    capped = fake.run(job, state, now=4.0)
    assert capped.status == dr.ESCALATED
    assert fake.escalated and fake.escalated[0][0] == job["id"]
    # Escalated once: the next sweep neither escalates again nor acts.
    state.forget(dr.mark_key(job["id"], dr.Cause(dr.HOST_PINNED, "eligible_hosts_pin")))
    again = fake.run(job, state, now=5.0)
    assert again.status == dr.SKIPPED and again.reason == "cap_reached"
    assert len(fake.escalated) == 1


def test_the_per_sweep_cap_bounds_one_pass_and_the_rest_defer(state):
    fake = Fake()
    rows = [row("host_pinned", id=f"01a18c66-7b02-7c44-a01e-58d2f3b9c6{i:02d}") for i in range(5)]
    summary = dr.sweep(
        rows,
        state=state,
        apply_fields=fake.apply_fields,
        reclassify=fake.reclassify_to,
        escalate=fake.escalate,
        default_branch=lambda _j: "deploy/integrate-20261017",
        branch_exists=lambda _b: True,
        roster=ROSTER,
        now=500.0,
        max_per_sweep=2,
    )
    assert summary["applied"] == 2
    assert summary["deferred"] == 3
    assert len(fake.reclassify) == 2


# ── lane B is another lane's ───────────────────────────────────────────────


@pytest.mark.parametrize(
    "lane_field",
    [
        {"project": "riskyeats"},
        {"head_branch": "fix/doctor-thing"},
        {"head_branch": "feat/review-queue"},
        {"base_branch": "test/harness"},
        {"id": "01a18ab8-0a4f-7c11-9d22-3e4f5a6b7c8d"},
        {"parent_job_id": "01a18a8d-9ffa-7b22-8c33-4d5e6f7a8b9c"},
    ],
)
def test_lane_b_rows_are_never_touched(state, lane_field):
    fake = Fake()
    job = row("host_pinned", **lane_field)
    assert dr.is_lane_b_owned(job)
    outcome = fake.run(job, state)
    assert outcome.status == dr.SKIPPED and outcome.reason == "lane_b_owned"
    assert fake.fields == [] and fake.reclassify == [] and fake.escalated == []


# ── no-op on a healthy row ─────────────────────────────────────────────────


def test_a_healthy_row_is_a_no_op(state):
    fake = Fake()
    healthy = {
        "id": "01a18ca0-0000-7000-8000-000000000001",
        "kind": "fix:thing",
        "status": "failed",
        "repo": "graeae-hive",
        "project": "graeae-hive",
        "head_branch": "hv/some-work-20261031",
        "base_branch": "deploy/integrate-20261017",
        "eligible_hosts": [],
        "preferred_models": ["qwen38"],
        "max_cost_tier": "A",
        "max_retries": 2,
        "retry_count": 1,
        "result": {"failure_reason": "push_failed", "error": "remote rejected (non-fast-forward)"},
    }
    outcome = fake.run(healthy, state)
    assert outcome.status == dr.NO_CHANGE and outcome.reason == "healthy_row"
    assert fake.fields == [] and fake.reclassify == [] and fake.escalated == []


# ── dry run ────────────────────────────────────────────────────────────────


def test_dry_run_reports_the_fix_and_writes_nothing(state):
    fake = Fake()
    outcome = fake.run(row("host_pinned"), state, dry_run=True)
    assert outcome.status == dr.DRY_WOULD_APPLY
    assert outcome.changes == {"eligible_hosts": []}
    assert fake.fields == [] and fake.reclassify == [] and dr.attempts(state, row("host_pinned")["id"]) == 0


def test_a_dry_run_sweep_keeps_the_flag_and_the_would_apply_counter_apart(state):
    """Review 2026-10-31 (high): the summary carried the boolean flag under
    ``"dry_run"`` AND counted the would-apply outcome under the same key, so a
    dry-run sweep turned ``True`` into ``2``. The flag stays a bool and the
    counter has its own name."""
    fake = Fake()
    rows = [row("host_pinned", id=f"01a18c66-7b02-7c44-a01e-58d2f3b9c6{i:02d}") for i in range(3)]
    summary = dr.sweep(
        rows,
        state=state,
        apply_fields=fake.apply_fields,
        reclassify=fake.reclassify_to,
        escalate=fake.escalate,
        default_branch=lambda _j: "deploy/integrate-20261017",
        branch_exists=lambda _b: True,
        roster=ROSTER,
        now=500.0,
        dry_run=True,
    )
    assert summary["dry_run"] is True
    assert summary[dr.DRY_WOULD_APPLY] == 3
    assert dr.DRY_WOULD_APPLY != "dry_run"
    assert fake.fields == [] and fake.reclassify == []


# ── the fake clock ─────────────────────────────────────────────────────────


def test_the_marker_expiry_is_measured_on_the_injected_clock():
    """The idempotency marker is written at ``now`` and read under the module
    TTL; a clock past the TTL releases it. Every time-dependent path takes the
    clock as an argument, so no test sleeps."""
    store = DurableStateStore(MemoryBackend(), refresh_secs=0.0)
    key = dr.mark_key("job-1", dr.Cause(dr.HOST_PINNED, "eligible_hosts_pin"))
    stamp = time.time()
    dr.mark_fixed(store, key, at=stamp)
    dr.record_attempt(store, "job-1", at=stamp)
    assert dr.attempts(store, "job-1") == 1
    # Stamped on the real clock (a marker is only ever judged on the clock that
    # wrote it), so now=None reads it as fresh; past the TTL it does not.
    assert dr.already_fixed(store, key) is True
    assert dr.already_fixed(store, key, now=stamp + dr.MARK_TTL_SEC + 1) is False
    assert dr.MARK_TTL_SEC > 0


# ── Review round 2 (muse-spark-1.3-contributor, different family) ──────────
# Each test below reproduces one reported finding against the source and fails
# on the previous round's code. The findings are graded by the behaviour the
# row actually gets, not by the wording of the report.


def test_r2_a_a_raising_health_probe_reads_as_an_unusable_route():
    """medium: model_problem does not handle health-check exceptions.

    A health probe that RAISES is not proof of health. ``healthy_alternative``
    already treats a raising probe as unusable; ``model_problem`` called it
    bare, so the exception escaped. Drive both readers of the probe with the
    same raising probe: model_problem classifies the route it could not read,
    and the plan step that follows does not crash on it."""
    job = row("unavailable_model")
    roster = [*ROSTER, {"id": "spark", "model_id": "gpt-5.3-codex-spark", "family": "openai", "cost_class": "B"}]

    def unhealthy(_route):
        raise TimeoutError("roster probe timed out")

    problem = dr.model_problem(job, roster=roster, health=unhealthy)
    assert problem is not None, "a raising probe must not read as a healthy route"
    assert problem in {"capped_model", "unavailable_model"}, f"got {problem!r}"

    # The row is unrecoverable through model_problem; plan_for must then decline
    # to guess and escalate, not raise. On the unfixed code this call raises.
    cause = dr.Cause(dr.UNAVAILABLE_MODEL, problem, ("preferred_models",))
    plan = dr.plan_for(job, cause, default_branch=None, roster=roster, health=unhealthy)
    assert plan is None or plan.changes, plan


def test_r2_b_an_unknown_cost_class_never_outranks_the_ceiling():
    """medium: healthy_alternative allows an unknown cost_class above the ceiling.

    ``max_cost_tier`` is a CEILING. A route whose ``cost_class`` cannot be read
    cannot be proven to sit under it, so it must be rejected — the previous
    sort silently ranked it BELOW every known class (``len(COST_CLASSES)``)
    and could pick it over a cheaper known route, breaking the ceiling."""
    roster = [
        # Unknown/malformed cost class on the "cheapest" route by sort order.
        {"id": "mystery-route", "model_id": "mystery-1", "family": "other", "cost_class": "?", "concurrency": 1},
        # A known route that is ABOVE the ceiling and must never be chosen.
        {"id": "pricey", "model_id": "pricey-1", "family": "acme", "cost_class": "C", "concurrency": 1},
    ]
    job = row("unavailable_model", max_cost_tier="A", preferred_models=["gpt-5.3-codex-spark"])
    picked = dr.healthy_alternative(job, roster=roster)
    assert picked is None, f"an unreadable cost_class cannot be proven under the ceiling, got {picked}"

    # With a KNOWN route under the ceiling available, the unknown one must lose.
    roster.append({"id": "cerberus-gemma", "model_id": "gemma4-31b", "family": "google", "cost_class": "A"})
    picked = dr.healthy_alternative(job, roster=roster)
    assert picked is not None and picked["id"] == "cerberus-gemma", f"got {picked}"


def test_r2_c_delivered_head_never_returns_the_string_none():
    """high: delivered_head stringifies None to the string 'none'.

    ``str(None)`` is the truthy string ``"none"``, so a row with NO head
    satisfied ``if busy and delivered_head(job)`` in ``classify`` and was
    handed to the review queue with a head of ``"none"``."""
    job = row("waiting_review")
    job["result"] = {**job["result"], "head_sha": None, "commits": []}
    job.pop("head_sha", None)
    assert dr.delivered_head(job) == "", f"a missing head must be empty, got {dr.delivered_head(job)!r}"

    # The same for a literal "none"/"null" string and a whitespace value.
    for empty in ("none", "null", "  ", None):
        job2 = row("waiting_review")
        job2["result"] = {**job2["result"], "head_sha": empty, "commits": []}
        job2.pop("head_sha", None)
        assert dr.delivered_head(job2) == "", f"{empty!r} must read as no head"


def test_r2_d_marker_freshness_is_judged_on_the_given_clock():
    """high: _marked_within with now=None always returns True.

    With ``now`` unset the previous code compared the marker against ITSELF
    (``reference = stamped``), so the age was always 0 and every marker read
    as fresh forever. An unstamped reference must fall back to the real clock
    and an old marker must expire."""
    store = DurableStateStore(MemoryBackend(), refresh_secs=0.0)
    dr.mark_fixed(store, "k", at=1000.0)
    # Judged on a real clock far past the TTL, an old marker is NOT fresh.
    assert dr.already_fixed(store, "k", now=1000.0) is True
    assert dr.already_fixed(store, "k", now=1000.0 + dr.MARK_TTL_SEC + 1) is False
    # now=None must read the real clock, not the marker's own stamp.
    assert dr._marked_within(store, "k", 1.0, None) is False


def test_r2_e_a_missing_result_head_sha_is_not_a_delivered_head():
    """high: a missing result.head_sha is reported as unchanged.

    ``_fields_unchanged`` skipped ``result.head_sha`` whenever the row had no
    head at all, so a failed row whose field fix IS the head was read as
    already-fixed after a crash between the field write and the status move."""
    job = row("composite_baseline")
    assert not (job.get("result") or {}).get("head_sha")
    cause = dr.Cause(dr.UNRESOLVABLE_REF, "composite_baseline", ("result.baseline",))
    plan = dr.Plan(
        cause=cause,
        changes={"head_sha": dr._change(None, REAL_HEAD)},
        requeue=True,
        reason="test",
        detail="test",
    )
    assert dr._fields_unchanged(job, plan) is False


def test_r2_f_a_future_audit_event_is_not_recent():
    """low: future audit events are treated as recent.

    ``now - at < within`` is true for every ``at`` in the future, so a row
    carrying a clock-skewed (or forged) future stamp blocked its own fix
    forever. A stamp in the future is not evidence of a recent fix."""
    job = row("host_pinned")
    cause = dr.Cause(dr.HOST_PINNED, "eligible_hosts_pin", ("eligible_hosts",))
    future = 10_000.0
    job["result"] = {
        **job["result"],
        "remediation": {"events": [{"cause": cause.name, "detail": cause.detail, "at": future}]},
    }
    assert dr.recent_audit(job, cause, within=3600.0, now=future - 7200.0) is False


def test_r2_g_the_sweeps_health_probe_reaches_the_classifier():
    """The sweep takes a ``health`` probe; classify must SEE it.

    ``sweep(health=...)`` was plumbed into ``plan_for`` but ``classify`` called
    ``model_problem`` with the default (always-healthy) probe, so a route the
    sweep KNOWS is capped still classified as runnable and the row was handed a
    healthy substitute -- masking the outage -- or a plain requeue. A row whose
    only route is capped must classify as the model problem it is."""
    store = DurableStateStore(MemoryBackend(), refresh_secs=0.0)
    job = row("unavailable_model", preferred_models=["gemma4-31b"], max_cost_tier="A")
    fake = Fake()

    summary = dr.sweep(
        [job],
        state=store,
        apply_fields=fake.apply_fields,
        reclassify=fake.reclassify_to,
        escalate=fake.escalate,
        roster=ROSTER,
        health=lambda _route: False,
        now=1000.0,
    )
    # The classifier must report the model problem rather than treat the row as
    # healthy and switch it to a substitute route.
    assert summary.get("causes", {}).get(dr.UNAVAILABLE_MODEL) == 1, summary
    assert not any("test" in str(r[2]) for r in fake.reclassify), fake.reclassify
    assert all("preferred_models" not in changes for _jid, changes, _reason in fake.fields), fake.fields


def test_r2_h_a_placeholder_result_head_does_not_mask_a_valid_job_head():
    """Review round 2, finding 1: the fallback is chosen before normalisation.

    ``res.get("head_sha") or job.get("head_sha")`` picks the result's
    placeholder because the raw ``"none"`` string is truthy, and only then
    normalises it -- so a row that DOES have a usable job-level head reported
    no delivered head. Each holder must be normalised separately."""
    job = row("waiting_review")
    job["result"] = {**job["result"], "head_sha": "none", "commits": []}
    job["head_sha"] = "abc123def456"
    assert dr.delivered_head(job) == "abc123def456", dr.delivered_head(job)

    # And the result still wins when it holds a REAL head.
    job["result"]["head_sha"] = "fff000111222"
    assert dr.delivered_head(job) == "fff000111222"


def test_r2_i_a_future_dated_marker_is_not_fresh():
    """Review round 2, finding 2: a future-dated marker reads as fresh.

    ``reference - stamped <= ttl`` is true for every stamp ahead of the clock,
    so after a backwards clock adjustment the durable marker suppressed this
    row's remediation until the wall clock caught up. Only a non-negative age
    inside the TTL counts -- the rule ``recent_audit`` already applies.

    The stamps are near the REAL clock because the durable store prunes on its
    own clock: a 1970-era stamp is collected at write, which would make this
    test pass for the wrong reason."""
    store = DurableStateStore(MemoryBackend(), refresh_secs=0.0)
    now = time.time()
    dr.mark_fixed(store, "k", at=now - 10)
    assert dr._marked_within(store, "k", 60.0, now) is True, "a 10s-old marker is fresh"
    dr.mark_fixed(store, "k", at=now + 1000)
    assert dr._marked_within(store, "k", 60.0, now) is False, "a stamp 1000s in the future is not fresh"


# ── Review round 5: one change-dict key for the head ───────────────────────
#
# The plan's ``changes`` mapping is handed to ``apply_fields(jid, changes,
# reason)``, whose contract is ``{field: {"before": ..., "after": ...}}`` where
# the FIELD NAME is the row's own field on the job's ``result``. The baseline
# branch qualified the holder (``result.baseline`` because the value was READ
# from the result) so the sweep could tell the two shapes apart, but the
# executor was handed that qualified name -- ``fields: {"result.baseline":
# <sha>}`` -- and then the same mapping carried ``head_sha`` bare. One write,
# two dialects: the executor cannot tell a row field from a holder qualifier,
# so the head is the only key that names a field, and ``result.`` is a prefix
# the row does not have. The plan is keyed by ROW FIELD NAME (the same
# convention ``_fields_unchanged`` and the harvest in ``remediate_one``
# already assume); the holder qualifier belongs to the CAUSE (``Cause.fields``,
# which is what ``composite_baseline_fields`` reports and what the tests
# assert), never to the write.


class ApplyFieldsSpy:
    """``apply_fields`` as the doctor's call site implements it: every key is
    a field name on the job's ``result`` (``_remediate_apply_fields`` builds
    ``fields: {name: after for name, change in changes.items()}``).

    It applies each write to a copy of the row and records the names it
    could not place, so a qualified/unqualified key clash shows up the way it
    would on the bus: the write lands somewhere the verifier never reads."""

    def __init__(self):
        self.writes: list[tuple[str, dict]] = []
        self.unplaced: list[str] = []
        self.rows: dict[str, dict] = {}

    def apply_fields(self, jid, changes, reason):
        before = self.rows.get(jid, {"result": {}})
        result = dict(before.get("result") or {})
        for name, change in changes.items():
            if not name or not name.replace("_", "").isalnum() or name.isdigit():
                self.unplaced.append(name)
                continue
            result[name] = change.get("after")
        self.rows[jid] = {**before, "result": result}
        self.writes.append((jid, {name: change.get("after") for name, change in changes.items()}))
        return 200, {"ok": [jid]}


def test_r5_the_plan_keys_every_field_it_writes_by_row_field_name():
    """The head sha is written under ONE key, and it is a row field name.

    Round 4 wrote ``head_sha`` alongside ``result.<key>`` (and skipped only
    ``result.head_sha``), so a single change dict carried both dialects: the
    executor received ``{"result.baseline": ..., "head_sha": ...}`` and had to
    guess which is a field and which is a holder. The writer, the reader and
    the audit entry must all name the SAME key."""
    job = row("composite_baseline")
    cause = dr.classify(job)
    assert cause.name == dr.UNRESOLVABLE_REF
    # The CAUSE still names the holder it read (that is how the row shapes are
    # told apart); only the WRITE is unqualified.
    assert all(name.startswith("result.") for name in cause.fields), cause.fields

    plan = dr.plan_for(
        job,
        cause,
        default_branch="deploy/integrate-20261017",
        ref_exists=lambda _ref: True,
        resolve_ref=lambda _ref: REAL_HEAD,
    )
    assert plan.changes, "the composite baseline must produce a plan"
    for name in plan.changes:
        assert not name.startswith("result."), f"{name!r} is a holder qualifier, not a row field"
        assert "." not in name, f"{name!r} is not a row field name"
    # Exactly one head key, named ``head_sha`` -- the same key ``delivered_head``
    # reads and the worker writes.
    assert "head_sha" in plan.changes
    assert plan.changes["head_sha"]["after"] == REAL_HEAD
    # The row's observed shape carries an EMPTY result.head_sha; the repair
    # records that as the "before" (never None from the job column, which has
    # no such key at all).
    assert plan.changes["head_sha"]["before"] == ""

    # And the writer/reader pair agree: applying the plan puts the head where
    # ``delivered_head`` (and therefore the review dispatch) looks for it.
    spy = ApplyFieldsSpy()
    code, _ = spy.apply_fields(job["id"], plan.changes, plan.reason)
    assert code == 200
    assert spy.unplaced == [], spy.unplaced
    # Every claimed change actually landed on the row's result.
    for name, change in plan.changes.items():
        assert spy.rows[job["id"]]["result"].get(name) == change["after"], name
    assert dr.delivered_head(spy.rows[job["id"]]) == REAL_HEAD

    # A second sweep over the row as written is a no-op: the reader sees what
    # the writer wrote, so idempotency is not a property of the marker alone.
    # (The job column outranks the result on this row shape — the fixture
    # carries ``head_sha: ""`` there — so ``job["result"]`` is overlaid on the
    # row before the reader looks, exactly like a bus row read back.)
    reread = {**job, "result": spy.rows[job["id"]]["result"]}
    assert dr._fields_unchanged(reread, plan) is True


def test_r5_the_job_column_baseline_writes_the_same_row_field_key():
    """The older shape keeps the bad baseline on the job column; the fix must
    still write the row field name ``baseline`` (no qualifier) and the one
    head key, so both shapes produce the same dict."""
    job = row("composite_baseline")
    bad = job["result"].pop("baseline")
    job["result"].pop("verify_baseline")
    job["baseline"] = bad
    cause = dr.classify(job)
    assert cause.fields == ("baseline",), cause.fields
    plan = dr.plan_for(
        job,
        cause,
        default_branch="deploy/integrate-20261017",
        ref_exists=lambda _r: True,
        resolve_ref=lambda _r: REAL_HEAD,
    )
    assert set(plan.changes) == {"baseline", "head_sha"}, sorted(plan.changes)
    assert plan.changes["baseline"] == {"before": bad, "after": REAL_HEAD}
    assert plan.changes["head_sha"] == {"before": "", "after": REAL_HEAD}


def test_r5_a_live_repair_reports_the_one_head_key_it_wrote(state):
    """The APPLIED outcome reports what was written (``head_sha``, not
    ``result.head_sha``), and the write the executor receives is that same
    mapping -- there is no second, differently-spelled head key anywhere in
    the path from plan to row."""
    fake = Fake()
    spy = ApplyFieldsSpy()
    job = row("composite_baseline")

    def apply_fields(jid, changes, reason):
        fake.fields.append((jid, changes, reason))
        return spy.apply_fields(jid, changes, reason)

    outcome = dr.remediate_one(
        job,
        state=state,
        apply_fields=apply_fields,
        reclassify=fake.reclassify_to,
        escalate=fake.escalate,
        default_branch=lambda _j: "deploy/integrate-20261017",
        branch_exists=lambda _b: True,
        ref_exists=lambda _r: True,
        resolve_ref=lambda _r: REAL_HEAD,
        roster=ROSTER,
        now=1000.0,
    )
    assert outcome.status == dr.APPLIED, outcome
    assert outcome.changes.get("head_sha") == REAL_HEAD
    assert not any(name.startswith("result.") for name in outcome.changes), outcome.changes
    # The row now holds the repaired head where the verifier reads it.
    assert dr.delivered_head(spy.rows[job["id"]]) == REAL_HEAD
    # The audit trail recorded the same field name under before/after.
    assert fake.fields[0][1]["head_sha"]["after"] == REAL_HEAD


# ── Review round 6: a field present on BOTH holders ────────────────────────
#
# The executor writes every planned field onto the row's ``result`` (that is
# the contract `plan_for` documents: "``changes`` is ``{field: {before,
# after}}`` on the row's own result"). A row can ALSO carry a stale copy of the
# same name on its job column -- the legacy row shape keeps ``baseline`` on the
# column, and a row that was fixed once can end up with both. Resolving a bare
# key by "is it on the job dict?" then reads the STALE copy and reports the row
# as unfixed, so every sweep re-remediates it until the 2-attempt cap escalates
# a row whose fields are already correct.
#
# The resolution must follow where the executor WRITES: a bare key is a row
# field, which the executor puts on the result. The job column is the fallback
# only for fields the row does not keep on the result at all (``base_branch``).


def test_r6_a_result_owned_field_beats_a_stale_job_column_copy():
    """The row IS fixed (result holds the target) while a stale job-column copy
    still holds the old value. The reader must say unchanged, not "fix me
    again" -- otherwise the row spends its whole remediation budget re-doing
    work that already landed."""
    job = {
        "id": "01a18c47-2f31",
        "status": "failed",
        "baseline": "9c1f4e0a7b2d6c88f13a05be7d415c9b6e11",  # stale column copy
        "result": {"baseline": REAL_HEAD},
    }
    plan = dr.Plan(
        cause=dr.Cause(dr.UNRESOLVABLE_REF, "composite_baseline", ("baseline",)),
        changes={"baseline": dr._change("9c1f4e0a7b2d6c88f13a05be7d415c9b6e11", REAL_HEAD)},
        requeue=True,
        reason="test",
        detail="test",
    )
    assert dr._fields_unchanged(job, plan) is True


def test_r6_a_column_owned_field_still_resolves_to_the_column():
    """PINNING test (passes before and after the fix): ``base_branch`` is a real
    job COLUMN and this row has no result copy, so the column holds the plan's
    target and the row reads unchanged. It guards the fix against
    over-correction -- preferring the result must not lose the column path."""
    job = {
        "id": "01a18c48-1234",
        "status": "failed",
        "base_branch": "deploy/integrate-20261017",
        "result": {"error": "missing base"},
    }
    plan = dr.Plan(
        cause=dr.Cause(dr.MISSING_BRANCH, "missing_branch", ("base_branch",)),
        changes={"base_branch": dr._change("", "deploy/integrate-20261017")},
        requeue=True,
        reason="test",
        detail="test",
    )
    assert dr._fields_unchanged(job, plan) is True


def test_r6_an_unfixed_result_owned_field_is_not_reported_as_unchanged():
    """PINNING test (passes before and after the fix): the guard must not become
    a blanket "always unchanged" -- a result-held field still holding the OLD
    value is genuinely unfixed, and the fix must keep saying so."""
    job = {"id": "01a18c49-5678", "status": "failed", "result": {"baseline": "old-bad-value"}}
    plan = dr.Plan(
        cause=dr.Cause(dr.UNRESOLVABLE_REF, "composite_baseline", ("baseline",)),
        changes={"baseline": dr._change("old-bad-value", REAL_HEAD)},
        requeue=True,
        reason="test",
        detail="test",
    )
    assert dr._fields_unchanged(job, plan) is False


def test_r6_the_head_key_can_never_collide_with_a_cause_field():
    """DISPROVED finding: "the cause loop can write ``head_sha``, so the
    following ``changes["head_sha"] = ...`` silently overwrites it".

    ``cause.fields`` for this cause comes only from ``composite_baseline_fields``,
    which iterates ``BASELINE_FIELDS``; ``head_sha`` is not one of them, so the
    loop can never produce that key and there is nothing to overwrite. This
    pins the invariant so a future addition to ``BASELINE_FIELDS`` cannot
    reintroduce the ambiguity silently."""
    assert "head_sha" not in dr.BASELINE_FIELDS

    job = row("composite_baseline")
    cause = dr.classify(job)
    assert cause.name == dr.UNRESOLVABLE_REF
    bare = {name.split(".", 1)[-1] for name in cause.fields}
    assert "head_sha" not in bare, f"a cause field now collides with the head key: {cause.fields}"

    plan = dr.plan_for(
        job,
        cause,
        default_branch="deploy/integrate-20261017",
        ref_exists=lambda _r: True,
        resolve_ref=lambda _r: REAL_HEAD,
    )
    # Exactly one head entry, whose "before" comes from the result (where the
    # worker writes it) -- not a merged or overwritten value.
    assert plan.changes["head_sha"]["after"] == REAL_HEAD
    assert plan.changes["head_sha"]["before"] == job["result"]["head_sha"]


# ── Review round 7: claims about the writer/reader pair ────────────────────
#
# Round 7's review claimed (a) the repair writes `head_sha` while the reclassify
# path reads `head_sha_composite`, and (b) `_fields_unchanged` tests
# `key in result_dict` before `startswith("result.")`, so a bare `base_branch`
# reads the result. Both were reproduced against the source and DISPROVED:
#   (a) no `head_sha_composite` exists anywhere in the tree, and the writer and
#       reader agree on `head_sha` (proved end to end below);
#   (b) the code already tests `startswith("result.")` FIRST.
# These pins hold both invariants so a later edit cannot silently reintroduce
# the ambiguity the review described.


def test_r7_the_written_head_field_is_the_one_the_reader_and_delivered_head_use():
    """DISPROVED: "plan_for writes ``head_sha`` but the reader reads
    ``result.head_sha``". There is ONE key. The executor writes a bare field
    onto the row's result, the reader finds it there, and ``delivered_head``
    reads the same name -- so a write is never reported as unapplied."""
    job = row("composite_baseline")
    plan = dr.plan_for(
        job,
        dr.classify(job),
        default_branch="deploy/integrate-20261017",
        ref_exists=lambda _r: True,
        resolve_ref=lambda _r: REAL_HEAD,
    )
    assert "head_sha" in plan.changes
    assert not any("." in name for name in plan.changes), plan.changes

    # Simulate the executor: every planned field lands on the row's result.
    written = json.loads(json.dumps(job))
    for name, change in plan.changes.items():
        written["result"][name] = change["after"]

    assert dr.delivered_head(written) == REAL_HEAD
    assert dr._fields_unchanged(written, plan) is True


def test_r7_no_code_path_reads_a_qualified_head_key():
    """DISPROVED: "the admin reclassify path reads ``head_sha_composite``".
    No such name exists, and no executable line writes or reads a
    ``result.head_sha`` patch key -- that spelling survives only in prose that
    names the holder."""
    import inspect

    src = inspect.getsource(dr)
    code = "\n".join(line for line in src.splitlines() if not line.lstrip().startswith("#"))
    # Drop docstrings: the holder name legitimately appears in prose.
    code_without_docstrings = "\n".join(line for line in code.splitlines() if '"""' not in line and "``" not in line)
    assert "head_sha_composite" not in src
    assert '["result.head_sha"]' not in code_without_docstrings
    assert '"result.head_sha"' not in code_without_docstrings


def test_r7_a_qualified_key_is_resolved_to_its_holder_before_any_fallback():
    """PINNING: the ``result.`` branch is tested FIRST. A ``result.<key>`` patch
    must never fall through to the job column, even when the same bare key also
    exists there with a different value."""
    job = {
        "id": "01a18c4a-0001",
        "status": "failed",
        "baseline": "job-column-value",  # a DIFFERENT value on the column
        "result": {"baseline": REAL_HEAD},
    }
    plan = dr.Plan(
        cause=dr.Cause(dr.UNRESOLVABLE_REF, "composite_baseline", ("result.baseline",)),
        changes={"result.baseline": dr._change("old", REAL_HEAD)},
        requeue=True,
        reason="test",
        detail="test",
    )
    # The qualified key reads the result (the target), never the column.
    assert dr._fields_unchanged(job, plan) is True
