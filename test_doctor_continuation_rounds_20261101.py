"""The doctor runs the request_changes -> continuation -> re-review loop (2026-11-01).

Until now the orchestrator ran this loop by hand: a cross-family review asked
for changes, someone wrote a CONTINUATION job carrying the findings, and the
continuation's head was reviewed again. ``doctor_remediation`` now does it:

* a needs-review WORK row qualifies when its BEST non-author review on the
  current tip is ``request_changes`` with located findings and it has had
  fewer than ``CONTINUATION_MAX_ROUNDS`` (2) automatic rounds;
* it gets ONE continuation job: same repo, base = the row's ``head_branch``,
  head = ``<slug>-rN``, ``parent_job_id`` = the work row (never a review job),
  a brief carrying the findings verbatim plus the standing instruction;
* the model ladder: round 1 the author's own family (never a reviewer's),
  round 2 the strongest hard-lane route (``hard_lane_priority``) outside the
  reviewers' families;
* one durable marker per (row, tip, round); after two rounds the row is
  escalated ONCE (``doctor.needs_review_stuck``) and then left alone;
* ``HIVE_DOCTOR_REMEDIATE=0`` and DRY_RUN are honoured, at most
  ``CONTINUATION_MAX_PER_SWEEP`` (3) new continuations per sweep, lane-B rows
  never touched.

The rows are the fleet's own shapes: ``tests/fixtures/doctor_remediation_rows.json``
(``waiting_review``), the live 2026-11-01 work row of
``tests/fixtures/doctor_review_chain_rows.json`` and the review children in
``tests/fixtures/doctor_continuation_rows.json`` (two families requesting
changes, a verdict-only head advance).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")

import pytest

from hive import review_queue as rq
from hive.workers import doctor_remediation as dr
from hive.workers.doctor_state import DurableStateStore, MemoryBackend

FIXTURES = Path(__file__).parent / "tests" / "fixtures"
TIP = "5d2e8a0c7b41f9e36a1d0c5b8e7f2a4c9d6b3e10"
VERDICT_ONLY_HEAD = "9e1b7c3a5f20d48e6c1a0b9f8e7d6c5b4a392817"
NOW = 1793547000.0
STANDING = (
    "verify every finding against source first; failing test first when it holds; "
    "pinning test and a note when it does not; never change correct code to satisfy a "
    "disproved claim; push your final HEAD and confirm with git ls-remote"
)
#: The repo's own roster (hive/core/reviewer-roster.json): the hard lane is the
#: codex route, the only one with a ``hard_lane_priority``.
ROSTER = rq.load_roster()


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def child(name: str, **overrides) -> dict:
    base = json.loads(json.dumps(_load("doctor_continuation_rows.json")[name]))
    result = overrides.pop("result", None)
    base.update(overrides)
    if result:
        base["result"] = {**base["result"], **result}
    return base


def work_row(**overrides) -> dict:
    """The live 2026-11-01 work row, in needs-review with the queue's request_changes."""
    base = json.loads(json.dumps(_load("doctor_review_chain_rows.json")["work_root"]))
    base["result"]["review_state"] = {
        **rq.make_review_state("qwen38", [], now=NOW - 600),
        "request_changes": {
            "reviewers": ["gpt-oss-20b"],
            "head_sha": TIP,
            "review_job_id": child("rc_openai")["id"],
            "at": NOW - 300,
        },
    }
    result = overrides.pop("result", None)
    base.update(overrides)
    if result:
        base["result"] = {**base["result"], **result}
    return base


def remediation_row() -> dict:
    """The ``waiting_review`` remediation fixture, as a needs-review work row."""
    base = json.loads(json.dumps(_load("doctor_remediation_rows.json")["waiting_review"]))
    base["status"] = "needs-review"
    base["claimed_model"] = "qwen38"
    return base


@pytest.fixture(autouse=True)
def table_only(monkeypatch):
    """Family answers from the data files only (no deployed resolver modules)."""
    monkeypatch.setitem(sys.modules, "provenance", None)
    monkeypatch.setitem(sys.modules, "hive.llm_roles", None)
    rq._family_of_cached.cache_clear()
    yield
    rq._family_of_cached.cache_clear()


@pytest.fixture
def state():
    return DurableStateStore(MemoryBackend(), refresh_secs=0.0)


class Fake:
    """The sweep's injected side effects, recorded."""

    def __init__(self, children: dict[str, list[dict]] | None = None, *, fail: bool = False):
        self.children_of = children or {}
        self.specs: list[dict] = []
        self.escalated: list[tuple[str, str]] = []
        self.fail = fail
        self.lookups: list[str] = []

    def children(self, row):
        self.lookups.append(str(row.get("id")))
        return list(self.children_of.get(str(row.get("id")), []))

    def submit(self, spec):
        self.specs.append(spec)
        if self.fail:
            return 503, {"detail": "bus unavailable"}
        return 201, {"id": f"01a1b000-0000-7000-8000-{len(self.specs):012d}"}

    def escalate(self, job, why):
        self.escalated.append((str(job.get("id")), why))

    def sweep(self, rows, state, **kw):
        kw.setdefault("roster", ROSTER)
        kw.setdefault("now", NOW)
        kw.setdefault("enabled", True)
        kw.setdefault("dry_run", False)
        return dr.continuation_sweep(
            rows,
            state=state,
            children=self.children,
            submit=self.submit,
            escalate=self.escalate,
            **kw,
        )


def one_rc(row: dict, *kids: dict) -> Fake:
    return Fake({row["id"]: list(kids) or [child("rc_openai")]})


# ── eligibility ─────────────────────────────────────────────────────────────


def test_a_request_changes_with_located_findings_gets_one_continuation(state):
    row = work_row()
    fake = one_rc(row)
    summary = fake.sweep([row], state)
    assert summary[dr.APPLIED] == 1, summary
    assert len(fake.specs) == 1
    spec = fake.specs[0]
    assert spec["repo"] == "graeae-hive"
    assert spec["base_branch"] == "hv/fo-merge-pass-20261031"
    assert spec["head_branch"] == "hv/fo-merge-pass-20261031-r1"
    assert spec["parent_job_id"] == row["id"]
    assert spec["kind"] == row["kind"]


def test_the_remediation_fixture_row_qualifies_too(state):
    row = remediation_row()
    rc = child("rc_openai", parent_job_id=row["id"])
    rc["result"]["head_sha"] = rc["result"]["remote_sha"] = row["result"]["head_sha"]
    fake = one_rc(row, rc)
    fake.sweep([row], state)
    assert [s["head_branch"] for s in fake.specs] == ["hv/gh-review-queue-20261031-r1"]
    assert fake.specs[0]["base_branch"] == "hv/gh-review-queue-20261031"


def test_an_approve_on_the_tip_outranks_a_request_changes(state):
    """BEST review: one cross-family approve of the tip means nothing to fix."""
    row = work_row()
    fake = one_rc(row, child("rc_openai"), child("approve_google"))
    summary = fake.sweep([row], state)
    assert fake.specs == [] and summary[dr.APPLIED] == 0, summary


def test_findings_without_a_location_do_not_qualify(state):
    row = work_row()
    rc = child("rc_openai")
    rc["result"]["reviewer"]["findings"] = [{"severity": "high", "title": "vague", "body": "somewhere"}]
    fake = one_rc(row, rc)
    fake.sweep([row], state)
    assert fake.specs == []


def test_a_request_changes_on_a_stale_head_does_not_qualify(state):
    row = work_row()
    rc = child("rc_openai")
    rc["result"]["head_sha"] = rc["result"]["remote_sha"] = "1" * 40
    fake = one_rc(row, rc)
    fake.sweep([row], state, head_is_verdict_only=lambda *_a: False)
    assert fake.specs == []


def test_a_verdict_only_head_advance_counts_as_the_current_tip(state):
    """Live 2026-11-01 shape: the reviewer reports a verdict-only descendant."""
    row = work_row()
    seen: list[tuple[str, str]] = []

    def verdict_only(_row, _child, tip, reviewed):
        seen.append((tip, reviewed))
        return tip == TIP and reviewed == VERDICT_ONLY_HEAD

    fake = one_rc(row, child("rc_verdict_only"))
    fake.sweep([row], state, head_is_verdict_only=verdict_only)
    assert seen == [(TIP, VERDICT_ONLY_HEAD)]
    assert len(fake.specs) == 1
    assert "deploy/hive_worker.py:7225" in fake.specs[0]["description"]


def test_a_verdict_only_head_advance_is_not_assumed_without_proof(state):
    row = work_row()
    fake = one_rc(row, child("rc_verdict_only"))
    fake.sweep([row], state)  # no verdict-only judge: an unproven descendant is not the tip
    assert fake.specs == []


def test_a_review_from_the_authors_own_family_is_not_counted(state):
    row = work_row()
    rc = child("rc_openai", claimed_model="qwen38")
    rc["result"]["reviewer"]["model"] = "qwen38"
    rc["result"]["reviewer"]["models"] = ["qwen38"]
    fake = one_rc(row, rc)
    fake.sweep([row], state)
    assert fake.specs == []


def test_an_overreaching_review_is_never_read(state):
    row = work_row()
    rc = child("rc_openai", result={"review_overreach": "review_overreach: changed x"})
    fake = one_rc(row, rc)
    fake.sweep([row], state)
    assert fake.specs == []


# ── the brief ───────────────────────────────────────────────────────────────


def test_the_brief_carries_every_located_finding_verbatim_and_the_standing_instruction(state):
    row = work_row()
    fake = one_rc(row, child("rc_openai"), child("rc_meta"))
    fake.sweep([row], state)
    desc = fake.specs[0]["description"]
    assert STANDING in desc
    assert dr.CONTINUATION_INSTRUCTION in desc
    # The best review is meta's (a critical finding): its findings verbatim.
    for needle in (
        "critical",
        "hive/review_queue.py:830",
        "hard-lane sort ignores the codex gate",
        "A route whose gate is closed still sorts first and the pick returns nothing usable.",
        "medium",
        "docs/REVIEW_QUEUE.md:12",
        "doc names the wrong default",
        "The documented cap is 5; the code reads 3.",
    ):
        assert needle in desc, needle
    assert TIP in desc


def test_located_findings_normalise_the_three_location_shapes():
    found = dr.located_findings(child("rc_meta")["result"]) + dr.located_findings(child("rc_openai")["result"])
    assert [(f["severity"], f["location"]) for f in found] == [
        ("critical", "hive/review_queue.py:830"),
        ("medium", "docs/REVIEW_QUEUE.md:12"),
        ("high", "hive/workers/doctor.py:5401"),
    ]
    assert found[2]["title"] == "merge pass reads a stale tip"
    assert found[2]["body"].startswith("The tip is read before the fetch")


# ── the ladder ──────────────────────────────────────────────────────────────


def test_round_one_uses_the_authors_family_and_never_a_reviewers(state):
    """Two families requested changes (openai, meta): round 1 is the author's qwen38."""
    row = work_row()
    fake = one_rc(row, child("rc_openai"), child("rc_meta"))
    fake.sweep([row], state)
    spec = fake.specs[0]
    assert spec["preferred_models"] == ["qwen38"]
    assert rq.family_of(spec["preferred_models"][0]) not in {"openai", "meta"}


def test_round_one_refuses_an_author_in_a_reviewers_family():
    row = work_row(claimed_model="gpt-oss-20b")
    row["result"]["review_state"]["author"] = "gpt-oss-20b"
    review = {"families": ["openai"], "reviewers": ["gpt-6-luna"]}
    assert dr.ladder_pick(row, review, 1, roster=ROSTER) is None


def continuation_row(round_no: int, **overrides) -> dict:
    """A continuation the doctor submitted earlier, now itself in needs-review."""
    root = work_row()
    slug = root["head_branch"]
    head = f"{slug}-r{round_no}"
    base = slug if round_no == 1 else f"{slug}-r{round_no - 1}"
    tip = f"{round_no}" * 40
    row = work_row(
        id=f"01a1b111-0000-7000-8000-00000000000{round_no}",
        head_branch=head,
        base_branch=base,
        parent_job_id=root["id"],
        claimed_model="qwen38",
        idempotency_key=f"doctor-continuation:{root['id']}:r{round_no}:{TIP[:12]}",
    )
    row["result"]["head_sha"] = tip
    row["result"]["commits"] = [tip]
    row.update(overrides)
    return row


def rc_on(row: dict, name: str = "rc_meta") -> dict:
    rc = child(name, parent_job_id=row["id"], head_branch=row["head_branch"])
    tip = row["result"]["head_sha"]
    rc["result"]["head_sha"] = tip
    if "remote_sha" in rc["result"]:
        rc["result"]["remote_sha"] = tip
    if "review_evidence" in rc["result"]:
        rc["result"]["review_evidence"]["reviewed_head"] = tip
    return rc


def test_round_two_goes_to_the_hard_lane_outside_the_reviewers_families(state):
    row = continuation_row(1)
    fake = Fake({row["id"]: [rc_on(row, "rc_meta")]})
    fake.sweep([row], state)
    spec = fake.specs[0]
    hard = [r for r in ROSTER if (r.get("flags") or {}).get("hard_lane_priority")]
    assert hard, "the roster has no hard-lane route"
    assert spec["preferred_models"] == [hard[0]["model_id"]]
    assert spec["eligible_kinds"] == [rq.CODEX_WORKER_KIND]
    assert spec["head_branch"] == "hv/fo-merge-pass-20261031-r2"
    assert spec["base_branch"] == "hv/fo-merge-pass-20261031-r1"
    assert spec["parent_job_id"] == row["id"]
    assert ":r2:" in spec["idempotency_key"]


def test_round_two_with_no_hard_lane_route_outside_the_reviewers_escalates_once(state):
    """openai requested changes; the only hard-lane route is openai's codex."""
    row = continuation_row(1)
    fake = Fake({row["id"]: [rc_on(row, "rc_openai")]})
    fake.sweep([row], state)
    fake.sweep([row], state)
    assert fake.specs == []
    assert len(fake.escalated) == 1 and fake.escalated[0][0] == row["id"]


# ── idempotency, cap, escalation ───────────────────────────────────────────


def test_a_second_sweep_over_the_same_row_and_tip_is_a_no_op(state):
    row = work_row()
    fake = one_rc(row)
    fake.sweep([row], state)
    second = fake.sweep([row], state, now=NOW + 3600)
    assert len(fake.specs) == 1, fake.specs
    assert second[dr.APPLIED] == 0
    assert state.value(dr.continuation_key(row["id"], TIP, 1), None) is not None


def test_the_same_row_listed_twice_in_one_sweep_is_continued_once(state):
    row = work_row()
    fake = one_rc(row)
    fake.sweep([row, json.loads(json.dumps(row))], state)
    assert len(fake.specs) == 1


def test_the_idempotency_key_names_the_root_round_and_tip(state):
    row = work_row()
    fake = one_rc(row)
    fake.sweep([row], state)
    assert fake.specs[0]["idempotency_key"] == f"doctor-continuation:{row['id']}:r1:{TIP[:12]}"


def test_after_two_rounds_the_row_is_escalated_once_and_left_alone(state):
    row = continuation_row(2)
    fake = Fake({row["id"]: [rc_on(row, "rc_meta")]})
    first = fake.sweep([row], state)
    fake.sweep([row], state, now=NOW + 60)
    assert fake.specs == []
    assert fake.escalated == [(row["id"], fake.escalated[0][1])]
    assert "round" in fake.escalated[0][1]
    assert first.get(dr.ESCALATED) == 1, first


def test_the_round_count_is_durable_even_when_the_key_is_lost(state):
    """Round 2 submitted from a row whose lineage the store knows."""
    row = work_row()
    fake = one_rc(row)
    fake.sweep([row], state)
    new_id = "01a1b000-0000-7000-8000-000000000001"
    r1 = continuation_row(1, id=new_id)
    r1.pop("idempotency_key")
    fake.children_of[new_id] = [rc_on(r1, "rc_meta")]
    fake.sweep([r1], state)
    assert fake.specs[-1]["head_branch"] == "hv/fo-merge-pass-20261031-r2"
    assert ":r2:" in fake.specs[-1]["idempotency_key"]


def test_a_failed_submit_writes_no_marker_and_is_retried(state):
    row = work_row()
    fake = one_rc(row)
    fake.fail = True
    summary = fake.sweep([row], state)
    assert summary.get(dr.FAILED) == 1, summary
    fake.fail = False
    fake.sweep([row], state)
    assert len(fake.specs) == 2


def test_at_most_three_new_continuations_per_sweep(state):
    assert dr.CONTINUATION_MAX_PER_SWEEP == 3
    rows, kids = [], {}
    for i in range(5):
        r = work_row(id=f"01a194cb-79b3-7c10-9e2a-3b4c5d6e7f1{i}", ended_at=1793544000.0 + i)
        rows.append(r)
        kids[r["id"]] = [child("rc_openai", parent_job_id=r["id"])]
    fake = Fake(kids)
    summary = fake.sweep(rows, state)
    assert len(fake.specs) == 3
    assert summary["deferred"] == 2, summary
    assert [s["parent_job_id"] for s in fake.specs] == [r["id"] for r in rows[:3]]


# ── switches and guards ─────────────────────────────────────────────────────


def test_the_kill_switch_stops_everything(state, monkeypatch):
    monkeypatch.setattr(dr, "REMEDIATE_ENABLED", False)
    row = work_row()
    fake = one_rc(row)
    summary = dr.continuation_sweep(
        [row], state=state, children=fake.children, submit=fake.submit, escalate=fake.escalate, roster=ROSTER, now=NOW
    )
    assert summary["enabled"] is False
    assert fake.specs == [] and fake.lookups == []


def test_dry_run_submits_nothing_and_marks_nothing(state):
    row = work_row()
    fake = one_rc(row)
    summary = fake.sweep([row], state, dry_run=True)
    assert fake.specs == []
    assert summary.get(dr.DRY_WOULD_APPLY) == 1, summary
    assert summary["would_submit"][0]["head_branch"] == "hv/fo-merge-pass-20261031-r1"
    fake.sweep([row], state)
    assert len(fake.specs) == 1


def test_dry_run_never_escalates(state):
    row = continuation_row(2)
    fake = Fake({row["id"]: [rc_on(row, "rc_meta")]})
    fake.sweep([row], state, dry_run=True)
    assert fake.escalated == []


def test_a_lane_b_row_is_never_touched(state):
    row = work_row(head_branch="fix/fo-merge-pass-20261031")
    fake = one_rc(row)
    fake.sweep([row], state)
    assert fake.specs == [] and fake.lookups == []


def test_a_review_job_is_never_continued(state):
    """The never-review-a-review guard: a review row is not a work row."""
    row = work_row(kind="review:build:zeroclaw-20261101")
    fake = one_rc(row)
    fake.sweep([row], state)
    assert fake.specs == []
    with pytest.raises(ValueError):
        dr.continuation_spec(row, {"findings": [], "reviewers": [], "families": [], "head": TIP}, 1, "qwen38")


def test_only_needs_review_rows_are_read(state):
    row = work_row(status="failed")
    fake = one_rc(row)
    fake.sweep([row], state)
    assert fake.specs == [] and fake.lookups == []


def test_one_bad_children_listing_does_not_stop_the_sweep(state):
    good = work_row()
    bad = work_row(id="01a194cb-79b3-7c10-9e2a-3b4c5d6e7f99", ended_at=1.0)
    fake = one_rc(good)

    def children(row):
        if row["id"] == bad["id"]:
            raise RuntimeError("review child lookup failed")
        return [child("rc_openai")]

    summary = dr.continuation_sweep(
        [bad, good],
        state=state,
        children=children,
        submit=fake.submit,
        escalate=fake.escalate,
        roster=ROSTER,
        now=NOW,
        enabled=True,
        dry_run=False,
    )
    assert summary.get("errors") == 1 and len(fake.specs) == 1, summary


# ── the doctor's wiring ─────────────────────────────────────────────────────


def quiet_work_row() -> dict:
    """The work row with no failure signal, so the field-fix sweep has no cause
    on it and every POST seen here is the continuation pass's own."""
    row = work_row()
    row["result"].pop("failure_reason", None)
    row["result"].pop("error", None)
    return row


def test_run_remediation_sweep_runs_the_continuation_pass(monkeypatch):
    from hive.workers import doctor

    monkeypatch.setattr(dr, "REMEDIATE_ENABLED", True)
    monkeypatch.setattr(doctor, "STATE", DurableStateStore(MemoryBackend(), refresh_secs=0))
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *_a, **_k: None)
    monkeypatch.setattr(doctor, "_review_route_health", lambda: lambda _r: True)
    row = quiet_work_row()
    jobs = {row["id"]: row, child("rc_openai")["id"]: child("rc_openai")}
    posts: list[dict] = []

    def http(method, path, body=None, **_kw):
        if method == "GET" and "parent_job_id=" in path:
            parent = path.split("parent_job_id=", 1)[1].split("&", 1)[0]
            return 200, {"jobs": [j for j in jobs.values() if j.get("parent_job_id") == parent]}
        if method == "GET" and path.startswith("/v1/jobs?status="):
            status = path.split("status=", 1)[1].split("&", 1)[0]
            return 200, {"jobs": [j for j in jobs.values() if j.get("status") == status]}
        if method == "POST" and path == "/v1/jobs":
            posts.append(body)
            return 201, {"id": "01a1b000-0000-7000-8000-00000000abcd"}
        return 200, {}

    monkeypatch.setattr(doctor, "_http", http)
    summary = doctor.run_remediation_sweep(dry_run=False, now=NOW)
    assert summary["continuations"][dr.APPLIED] == 1, summary
    assert [p["head_branch"] for p in posts] == ["hv/fo-merge-pass-20261031-r1"]
    assert posts[0]["parent_job_id"] == row["id"]
    assert posts[0]["submitter_urn"]


def test_run_remediation_sweep_honours_the_global_dry_run(monkeypatch):
    from hive.workers import doctor

    monkeypatch.setattr(dr, "REMEDIATE_ENABLED", True)
    monkeypatch.setattr(doctor, "DRY_RUN", True)
    monkeypatch.setattr(doctor, "STATE", DurableStateStore(MemoryBackend(), refresh_secs=0))
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *_a, **_k: None)
    monkeypatch.setattr(doctor, "_review_route_health", lambda: lambda _r: True)
    row = quiet_work_row()
    jobs = {row["id"]: row, child("rc_openai")["id"]: child("rc_openai")}
    posts: list = []

    def http(method, path, body=None, **_kw):
        if method == "GET" and "parent_job_id=" in path:
            return 200, {"jobs": [j for j in jobs.values() if j.get("parent_job_id") == row["id"]]}
        if method == "GET" and path.startswith("/v1/jobs?status="):
            status = path.split("status=", 1)[1].split("&", 1)[0]
            return 200, {"jobs": [j for j in jobs.values() if j.get("status") == status]}
        if method == "POST" and path == "/v1/jobs":
            posts.append(body)
        return 200, {}

    monkeypatch.setattr(doctor, "_http", http)
    summary = doctor.run_remediation_sweep(dry_run=False, now=NOW)
    assert posts == []
    assert summary["continuations"].get(dr.DRY_WOULD_APPLY) == 1, summary


def test_a_row_with_no_delivered_head_never_crashes_the_escalation_message(state):
    """Muse 2026-11-01 claimed ``tip[:12]`` raises on a headless row; delivered_head returns ''."""
    row = continuation_row(2)
    rc = rc_on(row, "rc_meta")
    row["result"] = {}
    row.pop("head_sha", None)
    assert dr.delivered_head(row) == ""
    fake = Fake({row["id"]: [rc]})
    fake.sweep([row], state)  # must not raise
    assert fake.specs == []
