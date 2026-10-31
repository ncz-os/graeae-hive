"""The doctor's review queue must genuinely REVIEW the triage queue (2026-10-31).

Operator: *"the triage queue is supposed to be reviewing; that is the point of
the review enhancements."* Production proved otherwise: every doctor sweep logged
``review_queue scanned=0 submitted=0`` while thirteen needs-review rows sat
unapproved. The queue only scanned rows that ALREADY carried
``result.review_state``, and only the NEW worker code writes that block -- the
deployed workers still run the older ``deploy/hive-canonical`` line, so their
rows were invisible to the review queue and the needs-review sweep stayed
report-only.

An ADOPTED row is a legacy needs-review row: it has a pushed ``head_branch``,
its ``result`` carries no structured approve for the CURRENT head
(``reviewer_unavailable``, request-changes at an older head, a review that did
not approve, prose-only or empty evidence, or no reviewer at all), and no review
job is in flight for it. The adoption writes the ``review_state`` block the queue
needs WITHOUT changing the row's status; the same sweep then pairs the row with a
cross-family reviewer from ``hive/core/reviewer-roster.json`` and submits ONE
normal Hive review job.

Covers, per the operator's brief: adoption of each legacy shape, no adoption
when a review is in flight or an approve exists for the head, pairing (an
author from the alibaba family never gets a qwen reviewer), the per-sweep cap,
lane B / RiskyEats exclusion, fake-clock backoff, and idempotency across sweeps.

Everything is hermetic: the bus, the doctor state and the route probes are test
doubles.
"""

import sys

import pytest

from hive import review_queue as rq
from hive.review_queue import family_of, load_roster, make_review_state, review_state

HEAD = "a" * 40
NEW_HEAD = "a1" * 20
OLD_HEAD = "b" * 40

#: The lane-B owner-wait chains (operator 2026-10-31): their rows are NOT the
#: doctor's to review.
LANE_B_JOB_IDS = ("01a18ab8-0a4f7d1e-9c1a-4a2b-8e3f-6d5c4b3a2f10", "01a18a8d-9ffa0b2c-7d6e-4f5a-9b8c-1a2b3c4d5e6f")


@pytest.fixture(autouse=True)
def table_only(monkeypatch):
    """Family answers from the data files only (no deployed resolver modules)."""
    monkeypatch.setitem(sys.modules, "provenance", None)
    monkeypatch.setitem(sys.modules, "hive.llm_roles", None)
    rq._family_of_cached.cache_clear()
    yield
    rq._family_of_cached.cache_clear()


@pytest.fixture
def bus(monkeypatch):
    """The doctor's bus, state and probes as doubles; records every call."""
    from hive.workers import doctor
    from hive.workers.doctor_state import DurableStateStore, MemoryBackend

    monkeypatch.setattr(doctor, "STATE", DurableStateStore(MemoryBackend(), refresh_secs=0))
    monkeypatch.setattr(doctor, "REVIEW_QUEUE_ROUTE_HEALTH", {})
    monkeypatch.setattr(doctor, "REVIEW_QUEUE_ROUTE_LOAD", lambda: 0)
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_ADOPT_LEGACY_REVIEWS", True)
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_REVIEW_SUBMIT_MAX", 3)
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_ADOPT_DRY_RUN", False)
    messages = []
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda topic, body: messages.append((topic, body)))

    class Bus:
        def __init__(self):
            self.calls = []
            self.children = {}
            self.messages = messages
            self.rows = {}
            self.posted = 0

        def http(self, method, path, body=None, **kwargs):
            self.calls.append((method, path, body))
            if method == "GET" and "parent_job_id=" in path:
                parent = path.split("parent_job_id=", 1)[1].split("&", 1)[0]
                return 200, {"jobs": [c for c in self.children.values() if c.get("parent_job_id") == parent]}
            if method == "POST" and path == "/v1/jobs":
                self.posted += 1
                new_id = f"review-{self.posted}"
                self.children[new_id] = {
                    "id": new_id,
                    "kind": body.get("kind"),
                    "status": "queued",
                    "parent_job_id": body.get("parent_job_id"),
                    "idempotency_key": body.get("idempotency_key"),
                    "preferred_models": body.get("preferred_models"),
                }
                return 201, {"id": new_id}
            if method == "PATCH":
                jid = path.rsplit("/", 1)[-1]
                self.rows.setdefault(jid, {}).update(body or {})
                return 200, {}
            return 200, {}

        def posts(self):
            return [c[2] for c in self.calls if c[:2] == ("POST", "/v1/jobs")]

        def patches(self, jid=None):
            return [c[2] for c in self.calls if c[0] == "PATCH" and (jid is None or c[1].endswith("/" + jid))]

        def finish(self, child_id, result, status="done"):
            self.children[child_id].update(status=status, result=result)

    fake = Bus()
    monkeypatch.setattr(doctor, "_http", fake.http)
    return fake


def _legacy_row(jid="legacy-1", *, author="qwen38", result=None, **extra):
    """A row as the OLDER worker line leaves it: no result.review_state."""
    base = {
        "id": jid,
        "kind": "fix:thing",
        "status": "needs-review",
        "repo": "graeae-hive",
        "project": "graeae-hive",
        # NOT a lane-B branch family: fix/ test/ refactor/ docs/ perf/ ops/
        # feat/ are the operator's lane B and are never adopted (see below).
        "head_branch": f"hv/{jid}",
        "base_branch": "main",
        "claimed_model": author,
        "head_sha": HEAD,
        "result": {
            "head_sha": HEAD,
            "commits": [HEAD],
            "review_evidence": {
                "error": "POST :8080/v1/chat/completions answered 503",
                "verdict": "reviewer_unavailable",
            },
        },
    }
    if result is not None:
        base["result"] = result
    base.update(extra)
    return base


# ── adoption helpers ────────────────────────────────────────────────────────


def test_adopt_legacy_row_builds_review_state_without_changing_status(bus):
    from hive.workers import doctor

    row = _legacy_row()
    assert review_state(row) is None  # nothing the queue could see before
    assert doctor.adopt_legacy_review_row(row, now=1000.0) is True
    state = review_state(row)
    assert state is not None
    assert state["state"] == rq.AWAITING_REVIEW
    assert state["author"] == "qwen38" and state["author_family"] == "alibaba"
    assert state["attempt"] == 0 and state["max_attempts"] == rq.MAX_REVIEW_ATTEMPTS
    assert "legacy" in str(state.get("adopted_from") or "")
    assert row["status"] == "needs-review", "adoption must not change the row status"


def test_adopt_legacy_row_infers_author_from_result_and_claimed_model(bus):
    from hive.workers import doctor

    # No claimed_model: the author comes from result.models.author.
    row = _legacy_row(result={"head_sha": HEAD, "models": {"author": "MiniMax-M3"}})
    row.pop("claimed_model")
    assert doctor.adopt_legacy_review_row(row, now=1000.0) is True
    assert review_state(row)["author_family"] == "minimax"

    # A row that names no author at all is still adopted: pick_reviewers parks
    # it (never self-review) but the queue must SEE it.
    bare = _legacy_row(result={"head_sha": HEAD})
    bare.pop("claimed_model")
    assert doctor.adopt_legacy_review_row(bare, now=1000.0) is True
    assert review_state(bare)["author"] == ""


def test_adopt_legacy_row_seeds_tried_from_recorded_review_rounds(bus):
    from hive.workers import doctor

    row = _legacy_row(
        result={
            "head_sha": HEAD,
            "reviewer": {"model": "gpt-oss-20b", "verdict": None},
            "loop_result": {
                "verdicts": [
                    {"verdict": "reviewer_unavailable", "model": "gemma4-31b"},
                    {"verdict": "reviewer_unavailable"},
                ]
            },
        }
    )
    assert doctor.adopt_legacy_review_row(row, now=1000.0) is True
    tried = review_state(row)["tried"]
    models = [t["model_id"] for t in tried]
    assert "gpt-oss-20b" in models and "gemma4-31b" in models
    assert all(t["failure"] for t in tried), "every seeded attempt names its failure"


@pytest.mark.parametrize(
    "result",
    [
        # reviewer_unavailable from the loop
        {"head_sha": HEAD, "loop_result": {"teardown": {"final_verdict": "reviewer_unavailable", "resolved": False}}},
        # request-changes at an OLDER head
        {
            "head_sha": NEW_HEAD,
            "commits": [OLD_HEAD, NEW_HEAD],
            "review_evidence": {
                "head_sha": OLD_HEAD,
                "reviewers": [{"model": "gpt-oss-20b", "verdict": "request-changes"}],
            },
        },
        # a review that did not approve (blocked)
        {
            "head_sha": HEAD,
            "review_evidence": {"head_sha": HEAD, "reviewers": [{"model": "gemma4-31b", "verdict": "blocked"}]},
        },
        # no reviewer at all: empty result
        {"head_sha": HEAD},
        # prose-only evidence: no reviewers list, no head
        {"head_sha": HEAD, "review_evidence": "The reviewer said this looks mostly fine, ship it."},
    ],
)
def test_each_legacy_shape_is_adopted(bus, result):
    from hive.workers import doctor

    row = _legacy_row(result=result)
    assert doctor.adopt_legacy_review_row(row, now=1000.0) is True
    assert review_state(row) is not None


@pytest.mark.parametrize(
    "result",
    [
        # a structured approve for the CURRENT head: nothing to review
        {
            "head_sha": NEW_HEAD,
            "commits": [NEW_HEAD],
            "review_evidence": {"head_sha": NEW_HEAD, "reviewers": [{"model": "gpt-oss-20b", "verdict": "approve"}]},
        },
        # the same approve with a SHORT sha
        {
            "head_sha": NEW_HEAD,
            "review_evidence": {
                "head_sha": NEW_HEAD[:12],
                "reviewers": [{"model": "gemma4-31b", "verdict": "approve"}],
            },
        },
    ],
)
def test_no_adoption_when_an_approve_exists_for_the_head(bus, result):
    from hive.workers import doctor

    row = _legacy_row(result=result)
    assert doctor.adopt_legacy_review_row(row, now=1000.0) is False
    assert review_state(row) is None


def test_no_adoption_when_a_review_is_in_flight(bus):
    from hive.workers import doctor

    # A live review child for the row: never a second review.
    row = _legacy_row()
    bus.children["review-live"] = {
        "id": "review-live",
        "kind": "review:fix-1",
        "status": "running",
        "parent_job_id": row["id"],
    }
    assert doctor.adopt_legacy_review_row(row, now=1000.0) is False
    assert review_state(row) is None

    # A row already in the queue is never re-adopted.
    queued = _legacy_row(jid="legacy-q")
    queued["result"]["review_state"] = make_review_state("qwen38", [], now=0)
    assert doctor.adopt_legacy_review_row(queued, now=1000.0) is False


def test_no_adoption_without_a_pushed_head_branch(bus):
    from hive.workers import doctor

    for row in (_legacy_row(head_branch=""), _legacy_row(head_branch=None)):
        assert doctor.adopt_legacy_review_row(row, now=1000.0) is False
        assert review_state(row) is None


# ── lane B / RiskyEats exclusion ────────────────────────────────────────────


@pytest.mark.parametrize(
    "overrides",
    [
        {"project": "riskyeats"},
        {"repo": "riskyeats"},
        {"head_branch": "test/lane-b"},
        {"head_branch": "refactor/thing"},
        {"head_branch": "fix/thing"},
        {"head_branch": "docs/thing"},
        {"head_branch": "perf/thing"},
        {"head_branch": "ops/thing"},
        {"head_branch": "feat/thing"},
        {"id": LANE_B_JOB_IDS[0]},
        {"id": LANE_B_JOB_IDS[1]},
    ],
)
def test_lane_b_and_riskyeats_rows_are_never_adopted(bus, overrides):
    from hive.workers import doctor

    row = _legacy_row(**overrides)
    assert doctor.adopt_legacy_review_row(row, now=1000.0) is False
    assert review_state(row) is None


def test_lane_b_prefixes_are_specific_not_substring_matches():
    from hive.workers import doctor

    assert doctor.lane_b_row({"head_branch": "fixit/thing", "project": "graeae-hive"}) is False
    assert doctor.lane_b_row({"project": "riskyeats", "head_branch": "main"}) is True


# ── status is never changed by the doctor ───────────────────────────────────


def test_adoption_patches_only_result_and_keeps_the_status(bus):
    from hive.workers import doctor

    row = _legacy_row()
    summary = doctor.submit_adopted_reviews(rows=[row], now=1000.0)
    assert summary["adopted"] == 1
    assert summary["submitted"] == 1
    patches = bus.patches(row["id"])
    assert patches, "the adopted state must be recorded on the row"
    assert all(p.get("status") == "needs-review" for p in patches), "the doctor never changes a row's status"


# ── pairing ─────────────────────────────────────────────────────────────────


def test_adopted_row_gets_a_cross_family_reviewer_never_the_author_family(bus):
    from hive.workers import doctor

    row = _legacy_row(author="qwen38")
    doctor.submit_adopted_reviews(rows=[row], now=1000.0)
    posts = bus.posts()
    assert len(posts) == 1
    preferred = posts[0]["preferred_models"]
    assert preferred and family_of(preferred[0]) != "alibaba"
    assert posts[0]["parent_job_id"] == row["id"]
    assert posts[0]["kind"].startswith("review:")
    assert review_state(row)["review_job_id"] == "review-1"


def test_adopted_row_never_gets_the_author_route(bus, monkeypatch):
    from hive.workers import doctor

    # Every reviewer-capable route from other families is unavailable: the row
    # PARKS (never self-reviews).
    monkeypatch.setattr(
        doctor,
        "REVIEW_QUEUE_ROUTE_LOAD",
        lambda *_: 999,
    )
    row = _legacy_row(author="qwen38")
    summary = doctor.submit_adopted_reviews(rows=[row], now=1000.0)
    assert bus.posts() == []
    # Saturated routes are not absent routes: the row WAITS for a reviewer,
    # spending neither an attempt nor a self-review.
    assert summary["submitted"] == 0
    state = review_state(row)
    assert state["author_family"] == "alibaba"
    assert state["attempt"] == 0


# ── the per-sweep cap ───────────────────────────────────────────────────────


def test_per_sweep_cap_limits_review_submissions(bus):
    from hive.workers import doctor

    rows = [_legacy_row(jid=f"legacy-{i}") for i in range(6)]
    summary = doctor.submit_adopted_reviews(rows=rows, now=1000.0)
    assert summary["submitted"] == doctor.HIVE_DOCTOR_REVIEW_SUBMIT_MAX
    assert len(bus.posts()) == 3


def test_cap_is_configurable(bus, monkeypatch):
    from hive.workers import doctor

    monkeypatch.setattr(doctor, "HIVE_DOCTOR_REVIEW_SUBMIT_MAX", 1)
    rows = [_legacy_row(jid=f"legacy-{i}") for i in range(4)]
    doctor.submit_adopted_reviews(rows=rows, now=1000.0)
    assert len(bus.posts()) == 1


# ── the switch and the dry run ───────────────────────────────────────────────


def test_switch_off_returns_one_report_shape(bus, monkeypatch):
    """A disabled pass reports the SAME fields, all zero, with ``disabled``.

    ``run_review_queue_sweep`` reads ``adopted``/``submitted``/``would_submit``/
    ``_handled_rows`` off this summary unconditionally. A disabled pass that
    dropped those keys would raise, or would report a missing key as a broken
    sweep; instead it reports a complete, zeroed pass (review 2026-10-31).
    """
    from hive.workers import doctor

    row = _legacy_row()
    summary = doctor.submit_adopted_reviews(rows=[_legacy_row(jid="probe")], now=1000.0)
    assert summary["submitted"] == 1
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_ADOPT_LEGACY_REVIEWS", False)
    bus.calls.clear()
    off = doctor.submit_adopted_reviews(rows=[row], now=1000.0)
    assert off["disabled"] is True
    # Same keys, plus the ``disabled`` marker a live pass does not carry.
    assert set(summary) | {"disabled"} == set(off), set(summary) ^ set(off)
    assert all(off[k] == 0 for k in ("scanned", "adopted", "submitted", "would_submit", "previewed", "capped"))
    assert off["_handled_rows"] == set()
    assert bus.posts() == []

    # And the whole sweep tolerates the disabled phase.
    monkeypatch.setattr(doctor, "fetch_needs_review_jobs", lambda limit=1000: [row])
    report = doctor.report_needs_review_sweep()["review_queue"]
    assert report["submitted"] == 0 and report["adopted"] == 0


def test_switch_off_submits_nothing(bus, monkeypatch):
    from hive.workers import doctor

    monkeypatch.setattr(doctor, "HIVE_DOCTOR_ADOPT_LEGACY_REVIEWS", False)
    row = _legacy_row()
    summary = doctor.submit_adopted_reviews(rows=[row], now=1000.0)
    assert summary["adopted"] == 0 and summary["submitted"] == 0
    assert bus.posts() == []
    assert review_state(row) is None


def test_dry_run_logs_what_it_would_submit_and_writes_nothing(bus, monkeypatch, caplog):
    from hive.workers import doctor

    monkeypatch.setattr(doctor, "HIVE_DOCTOR_ADOPT_DRY_RUN", True)
    row = _legacy_row()
    with caplog.at_level("INFO"):
        summary = doctor.submit_adopted_reviews(rows=[row], now=1000.0)
    assert summary["dry_run"] is True
    assert summary["adopted"] == 1 and summary["would_submit"] == 1
    assert bus.posts() == []
    assert bus.patches(row["id"]) == []
    assert any("would submit" in rec.getMessage() for rec in caplog.records)


def test_dry_run_previews_never_exceed_the_per_sweep_cap(bus, monkeypatch, caplog):
    """A dry run previews at most the cap and writes nothing (review 2026-10-31).

    "Logs what it would submit" must mean the SAME list a real sweep would
    submit: a dry run that previews one row per scanner is a fiction that
    misleads the operator reading the log. The share of the per-sweep budget is
    held under ONE floor so the two phases can never spend the cap twice.
    """
    from hive.workers import doctor

    monkeypatch.setattr(doctor, "HIVE_DOCTOR_ADOPT_DRY_RUN", True)
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_REVIEW_SUBMIT_MAX", 2)
    rows = [_legacy_row(jid=f"legacy-{i}") for i in range(5)]
    with caplog.at_level("INFO"):
        summary = doctor.submit_adopted_reviews(rows=[row for row in rows], now=1000.0)
    assert summary["adopted"] == 5
    assert summary["would_submit"] == 2, summary
    assert summary["previewed"] == 2 and summary["capped"] == 3
    assert len([r for r in caplog.records if "would submit" in r.getMessage()]) == 2
    assert bus.posts() == []
    assert [p for p in bus.patches() if "result" in p] == []

    # The whole sweep (adoption phase + review phase) still previews only 2.
    monkeypatch.setattr(doctor, "fetch_needs_review_jobs", lambda limit=1000: [dict(r) for r in rows])
    report = doctor.report_needs_review_sweep()["review_queue"]
    assert report["would_submit"] == 2, report
    assert bus.posts() == []


# ── backoff on a fake clock ─────────────────────────────────────────────────


def test_reviewer_failure_parks_the_adopted_row_in_backoff_on_a_fake_clock(bus):
    from hive.workers import doctor

    row = _legacy_row()
    doctor.submit_adopted_reviews(rows=[row], now=1000.0)
    child = "review-1"
    bus.finish(child, {"review_evidence": "this looks fine to me"}, status="done")

    # Prose is never an approval: the attempt fails and a retry is scheduled.
    summary = doctor.submit_adopted_reviews(rows=[row], now=1060.0)
    assert summary["review_failed"] == 1 and summary["approved"] == 0
    assert len(bus.posts()) == 1
    state = review_state(row)
    assert state["last_failure"] == "review_prose_only"
    assert not state.get("approved")
    # Backoff is keyed off the attempt already spent, from the observing clock.
    assert state["next_attempt_at"] == 1060.0 + rq.backoff_seconds(1)
    assert rq.in_backoff(state, now=1060.0) is True
    assert rq.in_backoff(state, now=state["next_attempt_at"] + 1) is False

    # Inside the backoff nothing is submitted again.
    assert doctor.submit_adopted_reviews(rows=[row], now=1060.0)["submitted"] == 0
    assert len(bus.posts()) == 1

    # After the backoff elapses the sweep submits the next attempt.
    summary = doctor.submit_adopted_reviews(rows=[row], now=state["next_attempt_at"] + 1)
    assert summary["submitted"] == 1
    assert len(bus.posts()) == 2
    assert review_state(row)["attempt"] == 2


def test_structured_approve_for_the_head_is_recorded_named_reviewer(bus):
    from hive.workers import doctor

    row = _legacy_row()
    doctor.submit_adopted_reviews(rows=[row], now=1000.0)
    bus.finish(
        "review-1",
        {"reviewers": [{"model": "gemma4-31b", "verdict": "approve"}], "head_sha": HEAD},
    )
    summary = doctor.submit_adopted_reviews(rows=[row], now=1001.0)
    assert summary["approved"] == 1
    state = review_state(row)
    assert state["approved"]["reviewers"] == ["gemma4-31b"]
    assert state["approved"]["head_sha"] == HEAD
    # Recorded on the row, status untouched: the operator-merge pass acts.
    assert bus.patches(row["id"])[-1]["status"] == "needs-review"
    assert any(topic == "doctor.review_approved" for topic, _ in bus.messages)


def test_prose_or_503_is_never_an_approval(bus):
    from hive.workers import doctor

    for payload in ("Approved! Ship it.", {"http_status": 503, "error": "overloaded"}, ""):
        bus.calls.clear()
        bus.children.clear()
        bus.posted = 0
        row = _legacy_row(jid="legacy-x")
        doctor.submit_adopted_reviews(rows=[row], now=1000.0)
        bus.finish("review-1", payload, status="done")
        summary = doctor.submit_adopted_reviews(rows=[row], now=1001.0)
        assert summary["approved"] == 0
        assert not review_state(row).get("approved")


def test_an_approve_of_an_older_head_is_not_an_approval(bus):
    from hive.workers import doctor

    row = _legacy_row(result={"head_sha": NEW_HEAD, "commits": [OLD_HEAD, NEW_HEAD]})
    doctor.submit_adopted_reviews(rows=[row], now=1000.0)
    bus.finish("review-1", {"reviewers": [{"model": "gemma4-31b", "verdict": "approve"}], "head_sha": OLD_HEAD})
    summary = doctor.submit_adopted_reviews(rows=[row], now=1001.0)
    assert summary["approved"] == 0
    assert not review_state(row).get("approved")


# ── idempotency ─────────────────────────────────────────────────────────────


def test_idempotent_across_sweeps_while_the_review_is_in_flight(bus):
    from hive.workers import doctor

    row = _legacy_row()
    doctor.submit_adopted_reviews(rows=[row], now=1000.0)
    assert len(bus.posts()) == 1
    for tick in range(1, 4):
        summary = doctor.submit_adopted_reviews(rows=[row], now=1000.0 + tick)
        assert summary["awaited"] == 1
    assert len(bus.posts()) == 1, "a second sweep must never duplicate the review job"


def test_a_row_already_holding_a_review_job_id_is_never_submitted_again(bus):
    """The adoption phase and the sweep must not both submit for one row.

    A row reaches ``process_awaiting_review_row`` either freshly adopted in this
    pass or already carrying ``review_state.review_job_id`` from an earlier one.
    The guard has to sit BEFORE route picking, so no second review job is ever
    created -- otherwise one job is reviewed twice and a reviewer slot is lost.
    """
    from hive.workers import doctor

    row = _legacy_row()
    row["result"]["review_state"] = {
        **make_review_state("qwen38", [], now=0.0),
        "review_job_id": "review-inherited",
        "review_route": "cerberus-gemma",
        "review_model": "gemma4-31b",
        "attempt": 1,
    }
    bus.children["review-inherited"] = {
        "id": "review-inherited",
        "kind": "review:fix:thing-20261031",
        "status": "running",
        "parent_job_id": row["id"],
    }

    outcome, state, _verdict = doctor.process_awaiting_review_row(row, now=1000.0)
    assert outcome == "awaited"
    assert state["review_job_id"] == "review-inherited"
    assert bus.posts() == [], "an in-flight review is never submitted a second time"

    # The sweep over the same row agrees and submits nothing either.
    summary = doctor.submit_adopted_reviews(rows=[row], now=1001.0)
    assert summary["submitted"] == 0 and summary["awaited"] == 1
    assert bus.posts() == []


def test_a_live_autoreview_child_on_a_legacy_row_is_not_resubmitted(bus):
    """The older worker line records its review in ``result.auto_review_job_id``.

    A legacy row (no review_state) whose recorded review child is still running
    is already being reviewed: adopting it and submitting again would review one
    job twice. Once that child is terminal the row is adoptable again.
    """
    from hive.workers import doctor

    row = _legacy_row()
    row["result"]["auto_review_job_id"] = "review-live"
    bus.children["review-live"] = {
        "id": "review-live",
        "kind": "review:fix:thing-20261031",
        "status": "running",
        "parent_job_id": row["id"],
    }
    assert doctor.adopt_legacy_review_row(row, now=1000.0) is False
    assert review_state(row) is None
    assert doctor.submit_adopted_reviews(rows=[row], now=1000.0)["submitted"] == 0
    assert bus.posts() == []

    # The child finished without a usable verdict: the row is legacy again.
    bus.finish(
        "review-live",
        {"review_evidence": {"head_sha": HEAD, "reviewers": [{"model": "gemma4-31b", "verdict": "blocked"}]}},
    )
    assert doctor.adopt_legacy_review_row(row, now=1000.0) is True


def test_idempotent_after_a_completed_review_for_the_head(bus):
    from hive.workers import doctor

    row = _legacy_row()
    doctor.submit_adopted_reviews(rows=[row], now=1000.0)
    bus.finish("review-1", {"reviewers": [{"model": "gemma4-31b", "verdict": "approve"}], "head_sha": HEAD})
    doctor.submit_adopted_reviews(rows=[row], now=1001.0)
    # A later sweep over the same (now approved) row submits nothing new.
    doctor.submit_adopted_reviews(rows=[row], now=1002.0)
    assert len(bus.posts()) == 1


def test_a_new_head_earns_a_new_review(bus):
    from hive.workers import doctor

    row = _legacy_row()
    doctor.submit_adopted_reviews(rows=[row], now=1000.0)
    bus.finish("review-1", {"reviewers": [{"model": "gemma4-31b", "verdict": "approve"}], "head_sha": HEAD})
    doctor.submit_adopted_reviews(rows=[row], now=1001.0)
    assert review_state(row)["approved"]["head_sha"] == HEAD
    # The author pushed a newer head: the old approval no longer counts.
    row["result"] = {**row["result"], "head_sha": NEW_HEAD, "commits": [HEAD, NEW_HEAD]}
    row["result"].pop("review_state", None)
    assert doctor.adopt_legacy_review_row(row, now=1002.0) is True
    doctor.submit_adopted_reviews(rows=[row], now=1002.0)
    assert len(bus.posts()) == 2


def test_whole_needs_review_sweep_adopts_and_submits(bus, monkeypatch):
    from hive.workers import doctor

    rows = [_legacy_row(), _legacy_row(jid="legacy-2", author="gpt-oss-20b")]
    monkeypatch.setattr(doctor, "fetch_needs_review_jobs", lambda limit=1000: [dict(r) for r in rows])
    report = doctor.report_needs_review_sweep()
    half = report["review_queue"]
    assert half["adopted"] == 2 and half["submitted"] == 2
    for spec in bus.posts():
        assert family_of(spec["preferred_models"][0]) not in {"alibaba", "openai"}


def test_roster_still_provides_every_family_the_brief_names():
    """TYDEUS qwen38 / Muse / M3 / DeepSeek / CERBERUS, per the roster flags."""
    families = {route["family"] for route in load_roster()}
    assert {"alibaba", "meta", "minimax", "deepseek", "openai", "google"} <= families


def test_ordered_routes_put_tried_routes_last_and_keep_the_roster_order():
    """Least-tried-first must never reshuffle the untried routes among themselves."""
    from hive.workers import doctor

    routes = [{"model_id": m} for m in ("a", "b", "c", "d")]
    assert [r["model_id"] for r in doctor._ordered_routes(routes, {})] == ["a", "b", "c", "d"]
    assert [r["model_id"] for r in doctor._ordered_routes(routes, {"tried": [{"model_id": "B"}]})] == [
        "a",
        "c",
        "d",
        "b",
    ]
    assert [
        r["model_id"] for r in doctor._ordered_routes(routes, {"tried": [{"model_id": "a"}, {"model_id": "c"}]})
    ] == [
        "b",
        "d",
        "a",
        "c",
    ]


def test_a_non_terminal_autoreview_child_blocks_a_second_submission(bus):
    """A recorded child that is still running means the row is already reviewed."""
    from hive.workers import doctor

    row = _legacy_row()
    row["result"]["auto_review_job_id"] = "review-live"
    bus.children["review-live"] = {
        "id": "review-live",
        "kind": "review:fix:thing-20261031",
        "status": "running",
        "parent_job_id": row["id"],
    }
    summary = doctor.submit_adopted_reviews(rows=[row], now=1000.0)
    assert summary["submitted"] == 0
    assert bus.posts() == [], "a running review child is never reviewed a second time"


def test_signature_accepts_the_new_keyword_and_a_default_call(bus):
    """Regression (review 2026-10-31): ``allow_submit`` must be OPTIONAL.

    ``run_review_queue_sweep`` and ``submit_adopted_reviews`` pass
    ``allow_submit=`` by keyword. If the parameter is missing from the
    definition every call would raise ``TypeError`` and the review queue would
    stop reviewing the triage queue -- the exact outage this task fixes. So the
    keyword must be accepted AND a bare call (no ``allow_submit``) must still
    advance the row by submitting a review.
    """
    import inspect

    from hive.workers import doctor

    params = inspect.signature(doctor.process_awaiting_review_row).parameters
    assert "allow_submit" in params
    assert params["allow_submit"].default is True

    row = _legacy_row()
    # Called with no allow_submit at all: the sweep's default path.
    outcome, state, _verdict = doctor.process_awaiting_review_row(row, now=1000.0, roster=load_roster())
    assert outcome == "submitted", outcome
    assert state and state.get("review_job_id") == "review-1"
    assert len(bus.posts()) == 1

    # And explicitly False spends no attempt: the row just waits.
    other = _legacy_row(jid="legacy-capped")
    outcome, state, _verdict = doctor.process_awaiting_review_row(
        other, now=1000.0, allow_submit=False, roster=load_roster()
    )
    assert outcome == "capped", outcome  # the per-sweep cap spent, no attempt spent
    assert len(bus.posts()) == 1
