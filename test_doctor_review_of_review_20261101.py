"""A review job is never itself reviewed (PYTHIA incident 2026-11-01 15:14 UTC).

Once the Oracle field-patch fix let the remediation sweep write, it produced a
runaway REVIEW-OF-REVIEW chain on ``hv/fo-merge-pass-20261031``: a row of kind
``review:build:zeroclaw-20261101`` ending in needs-review was classified
``waiting_review`` (``review_busy:loop_unresolved``), given a ``review_state``
(awaiting_review) and handed over; the review queue then advanced that row like
any other and submitted a review CHILD for the review job, which ended
needs-review, and so on -- ten deep, about one new row per sweep, eating
reviewer slots and filling the triage queue.

Three layers let it through, and each is pinned here:

* ``doctor_remediation`` classified a review job as finished work waiting for a
  review (it is now ``review_job``: no action, at most an escalation note when
  the work row it reviewed is already settled);
* the review-queue / follow-up submit paths would create a review child under a
  review-kind parent (now refused, with a log line);
* the legacy-review adoption adopted review-kind rows too (now refused).

Plus a kind-agnostic depth guard: no review chain deeper than
``review_queue.MAX_REVIEW_CHAIN_HOPS`` (2) hops from a work row.

The rows are the incident's shapes (``tests/fixtures/doctor_review_chain_rows.json``).
Everything is hermetic: the bus, the doctor state and the route probes are test
doubles.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")

import pytest

from hive import review_queue as rq
from hive.workers import doctor_remediation as dr
from hive.workers.doctor_state import DurableStateStore, MemoryBackend

FIXTURE = Path(__file__).parent / "tests" / "fixtures" / "doctor_review_chain_rows.json"
ROSTER = [
    {"id": "tydeus-qwen38", "model_id": "qwen38", "family": "alibaba", "cost_class": "A", "concurrency": 4},
    {"id": "cerberus-gemma", "model_id": "gemma4-31b", "family": "google", "cost_class": "A", "concurrency": 2},
    {"id": "cerberus-gpt-oss", "model_id": "gpt-oss-20b", "family": "openai", "cost_class": "A", "concurrency": 8},
]
NOW = 1793546000.0


def row(name: str, **overrides) -> dict:
    data = json.loads(FIXTURE.read_text())
    base = json.loads(json.dumps(data[name]))
    result_overrides = overrides.pop("result", None)
    base.update(overrides)
    if result_overrides:
        base["result"] = {**base.get("result", {}), **result_overrides}
    return base


def with_review_state(job: dict) -> dict:
    """The row as the remediation sweep LEFT it: awaiting_review, queue-owned."""
    job = json.loads(json.dumps(job))
    job["result"]["review_state"] = rq.make_review_state(
        str(job.get("claimed_model") or ""), [], last_failure="loop_unresolved", now=NOW
    )
    return job


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


# ── layer (a): doctor_remediation ───────────────────────────────────────────


class Fake:
    """The remediation module's injected side effects, recorded."""

    def __init__(self, jobs: dict[str, dict] | None = None):
        self.fields: list = []
        self.reclassify: list = []
        self.escalated: list = []
        self.reviewed: list = []
        self.jobs = jobs or {}

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

    def lookup(self, jid):
        return self.jobs.get(jid)

    def kwargs(self, **kw):
        kw.setdefault("default_branch", lambda _j: "deploy/integrate-20261017")
        kw.setdefault("branch_exists", lambda _b: True)
        kw.setdefault("ref_exists", lambda _r: True)
        kw.setdefault("roster", ROSTER)
        return dict(
            apply_fields=self.apply_fields,
            reclassify=self.reclassify_to,
            escalate=self.escalate,
            advance_review=self.advance_review,
            **kw,
        )

    def touched(self) -> bool:
        return bool(self.fields or self.reclassify or self.reviewed)


def test_the_incident_review_row_is_a_review_job_not_waiting_review():
    """The exact live shape: review-kind, needs-review, loop_unresolved, the
    delivered head verified -- every signal ``waiting_review`` reads."""
    job = row("review_child")
    cause = dr.classify(job)
    assert cause is not None and cause.name == dr.REVIEW_JOB, cause
    assert cause.name != dr.WAITING_REVIEW


def test_the_work_row_itself_still_waits_for_review():
    """The guard is on the review job only: the work row it reviewed is still
    finished work whose reviewer was busy."""
    assert dr.classify(row("work_root")).name == dr.WAITING_REVIEW


def test_plan_for_never_hands_a_review_job_to_the_review_queue():
    """Belt and braces: even a caller that forces the waiting_review cause onto
    a review job gets no plan (no awaiting_review marking, no field patch)."""
    job = row("review_child")
    for cause in (dr.Cause(dr.WAITING_REVIEW, "loop_unresolved"), dr.Cause(dr.STRANDED_WIP, "workspace_preserved")):
        assert dr.plan_for(job, cause, default_branch="deploy/integrate-20261017") is None
    assert dr.plan_for(job, dr.Cause(dr.REVIEW_JOB, "needs-review"), default_branch="main") is None


def test_remediate_one_does_nothing_to_a_review_job(state):
    fake = Fake()
    outcome = dr.remediate_one(row("review_child"), state=state, now=NOW, **fake.kwargs())
    assert outcome.status == dr.NO_CHANGE
    assert outcome.cause == dr.REVIEW_JOB
    assert not fake.touched(), (fake.fields, fake.reclassify, fake.reviewed)
    assert fake.escalated == []
    assert dr.attempts(state, row("review_child")["id"]) == 0


def test_the_chain_sweep_submits_nothing(state):
    """Parent review child -> grandchild review child: one sweep, then a second
    (the cadence that grew the chain one row per minute). Nothing is patched,
    reclassified, handed over or escalated."""
    fake = Fake(jobs={r["id"]: r for r in (row("work_root"), row("review_child"), row("review_grandchild"))})
    chain = [row("review_child"), row("review_grandchild")]
    for tick in range(2):
        summary = dr.sweep(chain, state=state, now=NOW + 60 * tick, job_lookup=fake.lookup, **fake.kwargs())
        assert summary[dr.APPLIED] == 0, summary
        assert summary.get("causes", {}).get(dr.WAITING_REVIEW, 0) == 0, summary
    assert not fake.touched(), (fake.fields, fake.reclassify, fake.reviewed)
    assert fake.escalated == []


def test_a_review_job_is_not_deferred_past_the_cap(state):
    """Past the per-sweep cap a review job is not counted as work the sweep
    owes: it is never the sweep's to act on."""
    fake = Fake()
    summary = dr.sweep([row("review_child")], state=state, now=NOW, max_per_sweep=0, **fake.kwargs())
    assert summary.get("deferred", 0) == 0, summary


def test_a_review_job_whose_work_row_is_settled_gets_one_escalation_note(state):
    """At most a note: the work row is done, so the dangling review job needs a
    human to close it. Once per TTL, never a field change."""
    work = row("work_root", status="done")
    fake = Fake(jobs={work["id"]: work})
    job = row("review_child")
    first = dr.remediate_one(job, state=state, now=NOW, job_lookup=fake.lookup, **fake.kwargs())
    second = dr.remediate_one(job, state=state, now=NOW + 60, job_lookup=fake.lookup, **fake.kwargs())
    assert first.status == dr.ESCALATED and first.cause == dr.REVIEW_JOB
    assert second.status == dr.NO_CHANGE
    assert len(fake.escalated) == 1 and fake.escalated[0][0] == job["id"]
    assert work["id"] in fake.escalated[0][1]
    assert not fake.touched()


def test_a_dry_run_sends_no_escalation_note_and_records_no_marker(state):
    """Muse r56grr: the review_job escalation ignored dry_run -- a dry-run sweep
    broadcast a real note and wrote the once-per-TTL marker, so the live sweep
    that followed stayed silent."""
    work = row("work_root", status="done")
    fake = Fake(jobs={work["id"]: work})
    job = row("review_child")
    dry = dr.remediate_one(job, state=state, now=NOW, job_lookup=fake.lookup, dry_run=True, **fake.kwargs())
    assert dry.status == dr.NO_CHANGE and fake.escalated == [] and not fake.touched()
    live = dr.remediate_one(job, state=state, now=NOW + 60, job_lookup=fake.lookup, **fake.kwargs())
    assert live.status == dr.ESCALATED and len(fake.escalated) == 1


def test_an_unreadable_parent_lookup_is_no_note_and_no_action(state):
    def broken(_jid):
        raise RuntimeError("bus down")

    fake = Fake()
    outcome = dr.remediate_one(row("review_child"), state=state, now=NOW, job_lookup=broken, **fake.kwargs())
    assert outcome.status == dr.NO_CHANGE and outcome.cause == dr.REVIEW_JOB
    assert fake.escalated == [] and not fake.touched()


def test_a_failed_review_job_can_still_be_requeued_for_a_field_fix(state):
    """The guard is on REVIEWING a review job, not on fixing one: an
    unresolvable baseline on a review job is still repaired and requeued (the
    review re-runs; nobody reviews it)."""
    job = row(
        "review_child",
        status="failed",
        result={
            "error": "`git merge-base --is-ancestor` exited 128 (bad object)",
            "baseline": "9c1f4e0a7b2d6c88f13a05be7d415c9b6e11",
        },
    )
    assert dr.classify(job).name == dr.UNRESOLVABLE_REF


# ── review_queue: the pure guards ───────────────────────────────────────────


def test_review_kind_detection():
    assert rq.is_review_kind(row("review_child"))
    assert rq.is_review_kind({"kind": "  review:x"})
    assert not rq.is_review_kind(row("work_root"))
    assert not rq.is_review_kind({"kind": "fix:review-queue"})
    assert not rq.is_review_kind({})


def test_needs_adoption_refuses_a_review_job():
    adopt, _reason = rq.needs_adoption(row("review_child"))
    assert adopt is False
    assert rq.adoption_review_state(row("review_child")) is None
    assert rq.needs_adoption(row("work_root"))[0] is True


def _lookup(rows):
    table = {r["id"]: r for r in rows}
    return lambda jid: table.get(jid)


def test_chain_refusal_for_a_review_kind_parent():
    assert rq.review_chain_refusal(row("review_child"), _lookup([])) == rq.REFUSE_REVIEW_OF_REVIEW


def test_chain_refusal_depth_guard_is_kind_agnostic():
    """No review chain deeper than 2 hops from the work row, whatever the
    kinds say: a review of a row 1 hop down is hop 2 (allowed), a review of a
    row 2 hops down would be hop 3 (refused)."""
    root = {"id": "w", "kind": "build:x", "parent_job_id": None}
    hop1 = {"id": "h1", "kind": "fix:x", "parent_job_id": "w"}
    hop2 = {"id": "h2", "kind": "fix:y", "parent_job_id": "h1"}
    lookup = _lookup([root, hop1, hop2])
    assert rq.MAX_REVIEW_CHAIN_HOPS == 2
    assert rq.review_chain_refusal(root, lookup) == ""
    assert rq.review_chain_refusal(hop1, lookup) == ""
    assert rq.review_chain_refusal(hop2, lookup) == rq.REFUSE_CHAIN_TOO_DEEP


def test_chain_refusal_fails_closed_on_an_unreadable_or_cyclic_chain():
    def broken(_jid):
        raise RuntimeError("bus down")

    assert rq.review_chain_refusal({"id": "a", "kind": "fix:x", "parent_job_id": "p"}, broken) == (
        rq.REFUSE_CHAIN_UNREADABLE
    )
    a = {"id": "a", "kind": "fix:x", "parent_job_id": "b"}
    b = {"id": "b", "kind": "fix:x", "parent_job_id": "a"}
    assert rq.review_chain_refusal(a, _lookup([a, b])) != ""


def test_a_purged_parent_ends_the_walk():
    """A parent the bus no longer has (retention purge) is the root, not an
    error: the row is reviewed like any work row."""
    assert rq.review_chain_refusal({"id": "a", "kind": "fix:x", "parent_job_id": "gone"}, _lookup([])) == ""


# ── layers (b) and (c): the doctor's submit and adoption paths ─────────────


@pytest.fixture
def bus(monkeypatch):
    """The doctor's bus, state and probes as doubles; records every call."""
    from hive.workers import doctor

    monkeypatch.setattr(doctor, "STATE", DurableStateStore(MemoryBackend(), refresh_secs=0))
    monkeypatch.setattr(doctor, "REVIEW_QUEUE_ROUTE_HEALTH", {})
    monkeypatch.setattr(doctor, "REVIEW_QUEUE_ROUTE_LOAD", lambda: 0)
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_ADOPT_LEGACY_REVIEWS", True)
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_REVIEW_SUBMIT_MAX", 3)
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_ADOPT_DRY_RUN", False)
    messages: list = []
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda topic, body: messages.append((topic, body)))

    class Bus:
        def __init__(self):
            self.calls: list = []
            self.jobs: dict[str, dict] = {}
            self.messages = messages
            self.posted = 0

        def add(self, *rows):
            for r in rows:
                self.jobs[r["id"]] = r

        def http(self, method, path, body=None, **kwargs):
            self.calls.append((method, path, body))
            if method == "GET" and "parent_job_id=" in path:
                parent = path.split("parent_job_id=", 1)[1].split("&", 1)[0]
                return 200, {"jobs": [j for j in self.jobs.values() if j.get("parent_job_id") == parent]}
            if method == "GET" and path.startswith("/v1/jobs?status="):
                status = path.split("status=", 1)[1].split("&", 1)[0]
                return 200, {"jobs": [j for j in self.jobs.values() if j.get("status") == status]}
            if method == "GET" and path.startswith("/v1/jobs/"):
                jid = path.rsplit("/", 1)[-1]
                return (200, self.jobs[jid]) if jid in self.jobs else (404, {"detail": "not found"})
            if method == "POST" and path == "/v1/jobs":
                self.posted += 1
                new_id = f"review-{self.posted}"
                self.jobs[new_id] = {"id": new_id, "status": "queued", **(body or {})}
                return 201, {"id": new_id}
            if method == "POST" and path == "/v1/admin/jobs/reclassify":
                return 200, {"ok": list((body or {}).get("job_ids") or [])}
            return 200, {}

        def posts(self):
            return [c[2] for c in self.calls if c[:2] == ("POST", "/v1/jobs")]

        def writes(self):
            return [c for c in self.calls if c[0] in ("POST", "PATCH")]

    fake = Bus()
    monkeypatch.setattr(doctor, "_http", fake.http)
    return fake


def test_adoption_never_adopts_a_review_job(bus):
    from hive.workers import doctor

    job = row("review_child")
    bus.add(row("work_root", status="done"), job)
    assert doctor.adopt_legacy_review_row(job, now=NOW) is False
    assert rq.review_state(job) is None
    summary = doctor.submit_adopted_reviews(rows=[job], now=NOW, roster=ROSTER)
    assert summary["adopted"] == 0 and summary["submitted"] == 0, summary
    assert bus.writes() == []


def test_the_review_queue_never_advances_a_review_job_it_was_handed(bus):
    """The chain's engine: the review row carried the review_state the
    remediation wrote, so the queue advanced it and submitted a review OF it."""
    from hive.workers import doctor

    child = with_review_state(row("review_child"))
    grandchild = with_review_state(row("review_grandchild"))
    bus.add(row("work_root", status="done"), child, grandchild)
    outcome, _state, _verdict = doctor.process_awaiting_review_row(child, now=NOW, roster=ROSTER)
    assert outcome == "skipped_review_job"
    summary = doctor.submit_adopted_reviews(rows=[child, grandchild], now=NOW, roster=ROSTER)
    assert summary["submitted"] == 0, summary
    summary = doctor.run_review_queue_sweep(rows=[child, grandchild], now=NOW, roster=ROSTER)
    assert summary.get("submitted", 0) == 0, summary
    assert bus.posts() == []


def test_the_generic_followup_never_reviews_a_review_job(bus):
    from hive.workers import doctor

    job = row("review_child")
    bus.add(row("work_root", status="done"), job)
    assert doctor.ensure_review_followup(job) == "skipped_review_job"
    assert bus.posts() == []


def test_the_submit_functions_refuse_a_review_kind_parent_with_a_log_line(bus, caplog):
    """The last line of defence, whatever called them."""
    from hive.workers import doctor

    job = row("review_child")
    bus.add(row("work_root", status="done"), job)
    state = rq.make_review_state("gpt-oss-20b", [], now=NOW)
    with caplog.at_level(logging.WARNING, logger=doctor.log.name):
        with pytest.raises(RuntimeError, match="review_of_review"):
            doctor._review_queue_submit(job, state, [ROSTER[0]], now=NOW)
        review_id, _spec, note = doctor._submit_review_followup(job)
    assert review_id == "" and "review_of_review" in note
    assert bus.posts() == []
    assert sum("refused review child" in r.getMessage() for r in caplog.records) == 2, caplog.text


def test_the_depth_guard_stops_a_chain_whatever_the_kinds_say(bus):
    """A row two hops below its work row, of a NON-review kind, still gets no
    review child (it would be hop 3); one hop below still does."""
    from hive.workers import doctor

    root = row("work_root", status="done")
    hop1 = row("work_root", id="01a1a111-0000-7000-8000-000000000001", kind="fix:x", parent_job_id=root["id"])
    hop2 = row("work_root", id="01a1a222-0000-7000-8000-000000000002", kind="fix:y", parent_job_id=hop1["id"])
    hop1["head_branch"] = "hv/hop1-20261101"
    hop2["head_branch"] = "hv/hop2-20261101"
    bus.add(root, hop1, hop2)
    summary = doctor.submit_adopted_reviews(rows=[hop2], now=NOW, roster=ROSTER)
    assert summary["submitted"] == 0, summary
    assert bus.posts() == []
    summary = doctor.submit_adopted_reviews(rows=[hop1], now=NOW, roster=ROSTER)
    assert summary["submitted"] == 1, summary
    assert [p["parent_job_id"] for p in bus.posts()] == [hop1["id"]]


def test_run_remediation_sweep_over_the_chain_writes_nothing(bus, monkeypatch):
    """The doctor's wiring end to end: listing -> remediation -> hand-over.
    No field patch, no reclassify, no review child."""
    from hive.workers import doctor

    monkeypatch.setattr(dr, "REMEDIATE_ENABLED", True)
    monkeypatch.setattr(doctor.review_queue, "load_roster", lambda *a, **k: list(ROSTER))
    monkeypatch.setattr(doctor, "_review_route_health", lambda: lambda _r: True)
    bus.add(row("work_root", status="done"), row("review_child"), row("review_grandchild"))
    summary = doctor.run_remediation_sweep(dry_run=False, now=NOW)
    assert summary[dr.APPLIED] == 0, summary
    assert summary.get("causes", {}).get(dr.WAITING_REVIEW, 0) == 0, summary
    assert [c for c in bus.writes()] == [], bus.writes()
