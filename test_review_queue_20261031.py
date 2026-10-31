"""Review queue with a cross-family reviewer roster (operator rule 2026-10-31).

A loop whose reviewer endpoint is busy ended ``reviewer_unavailable`` and
FAILED (all 8 slots of the CERBERUS gpt-oss-20b endpoint busy -> about 20
loops failed in one night). The rule: always have an alternate review protocol
or a review queue, add every local and provider model to the roster, and ALWAYS
pair reviewer and author from different model families.

These tests pin the five pieces:

1. ONE model-family table (data file) -- every roster model has exactly one
   family, and the documented rules hold;
2. the reviewer roster (data file) next to it -- never MiMo, contributor tiers
   only on operator-approved repos, EIH Nemotron slow and lowest priority;
3. ``pick_reviewers`` -- cross-family only, deterministic, empty (parked) when
   no cross-family reviewer is available, never a self-review;
4. the review queue IS the triage queue: no separate table or status. The
   worker leaves a needs-review row with ``review_state='awaiting_review'``
   and the doctor's needs-review sweep consumes it;
5. load awareness: cached route load keeps a saturated endpoint out of the
   pick, and the per-route cap is never exceeded;
6. triage visibility: an ``awaiting_review`` row is a needs-review row with its
   reason and ``next_attempt_at`` visible, and the doctor does not call it
   stale while attempts remain.
"""

import json
import time

import pytest

from hive.review_queue import (
    APPROVED_REPOS,
    FAMILY_TABLE_PATH,
    ROSTER_PATH,
    awaiting_review_reason,
    backoff_seconds,
    family_of,
    load_family_table,
    load_roster,
    make_review_state,
    pick_reviewers,
    review_state,
    route_load_fn,
    triage_reason,
)

# ── 1. the model family table ────────────────────────────────────────────────


def test_family_table_maps_the_documented_rules():
    exact = {
        "qwen38": "alibaba",
        "Qwen3.8-27B": "alibaba",
        "Qwen2.5-Coder-32B-Instruct": "alibaba",
        "gemma4-31b": "google",
        "gemma4-26b-a4b": "google",
        "gpt-oss-20b": "openai",
        "gpt-6-luna": "openai",
        "codex": "openai",
        "nemotron-3-ultra-550b-a55b": "nvidia",
        "nemotron35": "nvidia",
        "MiniMax-M3": "minimax",
        "MiniMax-M3.1-Flash-Preview": "minimax",
        "deepseek-flash": "deepseek",
        "deepseek-v4-pro": "deepseek",
        "muse-spark-1.3": "meta",
        "mimo-v2.6-flash": "xiaomi",
        "mimo-v2.6-pro": "xiaomi",
        "stepfun-3.5": "stepfun",
        "claude-sonnet-4-6": "anthropic",
    }
    for model, family in exact.items():
        assert family_of(model) == family, model


def test_unknown_model_is_its_own_family_and_matches_nothing():
    assert family_of("widget-9000") == "model:widget-9000"
    assert family_of("widget-9000") == family_of("WIDGET-9000")
    assert family_of("widget-9000") != family_of("widget-9001")
    assert family_of("") == ""


def test_every_roster_model_has_exactly_one_family():
    """One source of truth: each roster entry names the family the table gives.

    A family that were listed twice (two patterns claiming one model) would
    make ``family_of`` order-dependent; the loader refuses such a table, and
    every roster route must agree with the table's answer for its model.
    """
    table = load_family_table()
    for model, entry in table["exact"].items():
        assert isinstance(entry["family"], str) and entry["family"], model
    for entry in load_roster():
        assert entry["family"] == family_of(entry["model_id"]), entry["model_id"]


def test_family_table_and_roster_are_data_files_next_to_each_other():
    assert FAMILY_TABLE_PATH.is_file() and ROSTER_PATH.is_file()
    assert FAMILY_TABLE_PATH.parent == ROSTER_PATH.parent
    data = json.loads(FAMILY_TABLE_PATH.read_text())
    assert data["schema_version"] == 1
    # Every rule in the operator's list is represented by a pattern.
    families = {entry["family"] for entry in data["exact"].values()}
    for wanted in ("alibaba", "google", "openai", "nvidia", "minimax", "deepseek", "meta", "xiaomi", "stepfun"):
        assert wanted in families, wanted


# ── 2. the reviewer roster ──────────────────────────────────────────────────


def test_roster_lists_every_reviewer_capable_route():
    by_model = {e["model_id"]: e for e in load_roster()}
    expected = {
        "qwen38": ("tydeus", "local"),
        "gemma4-31b": ("cerberus", "local"),
        "gpt-oss-20b": ("cerberus", "local"),
        "MiniMax-M3": (None, "provider"),
        "MiniMax-M3.1-Flash-Preview": (None, "provider"),
        "deepseek-flash": (None, "provider"),
        "muse-spark-1.3": (None, "provider"),
        "nvidia/nemotron-3-ultra-550b-a55b": (None, "eih"),
    }
    assert expected.keys() <= by_model.keys()
    for model, (host, location) in expected.items():
        entry = by_model[model]
        assert entry["location"] == location, model
        assert entry.get("host") == host, model
        for field in ("provider_id", "model_id", "family", "location", "concurrency", "cost_class", "flags"):
            assert field in entry, (model, field)
        assert isinstance(entry["concurrency"], int) and entry["concurrency"] >= 1


def test_roster_flags_encode_the_operator_rules():
    by_model = {e["model_id"]: e for e in load_roster()}
    # MiMo is never a reviewer -- not even listed as reviewer-capable.
    assert not any(e["family"] == "xiaomi" or e["model_id"].lower().startswith("mimo") for e in by_model.values())
    # StepFun and Meta are contributor tiers, operator-approved repos only.
    for tier_model in ("stepfun-3.5", "muse-spark-1.3"):
        entry = by_model[tier_model]
        assert "contributor_tier" in entry["flags"], tier_model
        assert set(entry["flags"]["contributor_tier"]["repos"]) == set(APPROVED_REPOS)
    # EIH Nemotron is allowed but slow: lowest priority for bulk.
    assert "slow" in by_model["nvidia/nemotron-3-ultra-550b-a55b"]["flags"]
    assert by_model["nvidia/nemotron-3-ultra-550b-a55b"]["flags"]["priority"] < 0


def test_roster_role_ids_resolve_to_their_backing_model():
    """CERBERUS serves BOTH cerberus-reviewer and cerberus-narrator as gpt-oss-20b."""
    roles = {}
    for entry in load_roster():
        for role in [*(entry.get("roles") or []), entry.get("role_id")]:
            if role:
                roles[role] = entry["model_id"]
    assert roles["cerberus-reviewer"] == "gpt-oss-20b"
    assert roles["cerberus-narrator"] == "gpt-oss-20b"


# ── 3. pairing ──────────────────────────────────────────────────────────────


def _healthy(entry):
    return True


def test_pick_reviewers_prefers_a_different_family_and_is_deterministic():
    first = pick_reviewers("qwen38", health=_healthy, load=lambda route: 0)
    second = pick_reviewers("qwen38", health=_healthy, load=lambda route: 0)
    assert first and [r["model_id"] for r in first] == [r["model_id"] for r in second]
    assert all(r["family"] != "alibaba" for r in first)
    # Bulk order puts the slow EIH route last.
    assert first[-1]["model_id"] == "nvidia/nemotron-3-ultra-550b-a55b"


@pytest.mark.parametrize(
    "author",
    [
        "qwen38",
        "gemma4-31b",
        "gpt-oss-20b",
        "MiniMax-M3",
        "deepseek-flash",
        "muse-spark-1.3",
        "nemotron-3-ultra-550b-a55b",
    ],
)
def test_no_same_family_pair_is_ever_returned(author):
    picked = pick_reviewers(author, health=_healthy, load=lambda route: 0, repo="graeae-hive")
    author_family = family_of(author)
    assert picked, author
    assert all(r["family"] != author_family for r in picked)


def test_every_author_family_against_the_whole_roster_is_cross_family_only():
    """The full matrix: for EVERY family the roster knows, and every model in it,
    no returned reviewer shares the author's family (or a co-author's)."""
    roster = load_roster()
    families = sorted({entry["family"] for entry in roster})
    assert len(families) >= 7
    for family in families:
        for author in [e["model_id"] for e in roster if e["family"] == family]:
            picked = pick_reviewers(author, roster, health=_healthy, load=lambda route: 0, repo="graeae-hive")
            assert picked, author
            assert {r["family"] for r in picked}.isdisjoint({family}), author
            # And nothing outside the roster can sneak in.
            assert {r["id"] for r in picked} <= {e["id"] for e in roster}


def test_co_author_families_are_excluded_too():
    picked = pick_reviewers(
        "qwen38", co_authors=["gemma4-31b"], health=_healthy, load=lambda route: 0, repo="graeae-hive"
    )
    assert all(r["family"] not in {"alibaba", "google"} for r in picked)
    assert "gemma4-31b" not in {r["model_id"] for r in picked}


def test_unhealthy_and_saturated_routes_are_skipped():
    healthy = lambda route: route["model_id"] != "gpt-oss-20b"  # noqa: E731
    load = lambda route: route["concurrency"] if route["model_id"] == "MiniMax-M3" else 0  # noqa: E731
    picked = [r["model_id"] for r in pick_reviewers("qwen38", health=healthy, load=load, repo="graeae-hive")]
    assert "gpt-oss-20b" not in picked
    assert "MiniMax-M3" not in picked


def test_no_cross_family_reviewer_returns_empty_never_self_review():
    """Every route the author's family or unhealthy -> PARK, never self-review."""
    everything = load_roster()
    picked = pick_reviewers(
        "qwen38",
        roster=everything,
        health=lambda route: route["location"] == "local" and route["family"] == "alibaba",
        load=lambda route: 0,
        repo="graeae-hive",
    )
    assert picked == []


def test_contributor_tiers_only_on_approved_repos():
    for repo, allowed in (
        ("graeae-hive", True),
        ("fleet-ops", True),
        ("tokenomics", True),
        ("ncz-screensavers", True),
        ("zoder", True),
        ("some-other-repo", False),
    ):
        picked = {r["model_id"] for r in pick_reviewers("qwen38", health=_healthy, load=lambda route: 0, repo=repo)}
        assert ("stepfun-3.5" in picked) is allowed, repo
        assert ("muse-spark-1.3" in picked) is allowed, repo


# ── 4/5/6. the review queue inside the triage queue ─────────────────────────


@pytest.fixture
def bus(monkeypatch, tmp_path):
    """A doctor whose bus, state and route health are all local test doubles."""
    from hive.workers import doctor
    from hive.workers.doctor_state import DurableStateStore, MemoryBackend

    doctor.STATE = DurableStateStore(MemoryBackend(), refresh_secs=0)
    calls, messages, rows = [], [], {}
    rows["source"] = {
        "id": "source",
        "kind": "fix:thing",
        "status": "needs-review",
        "repo": "graeae-hive",
        "head_branch": "fix/thing",
        "base_branch": "main",
        "result": {
            "head_sha": "a" * 40,
            "commits": ["a" * 40],
            "review_state": make_review_state("qwen38", [], attempt=0, now=0),
        },
    }
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda topic, body: messages.append((topic, body)))
    monkeypatch.setattr(doctor, "REVIEW_QUEUE_ROUTE_HEALTH", {})
    monkeypatch.setattr(doctor, "REVIEW_QUEUE_ROUTE_LOAD", lambda: 0)

    def http(method, path, body=None, **kwargs):
        calls.append((method, path, body))
        if method == "GET" and "parent_job_id=" in path:
            return 200, {"jobs": []}
        if method == "GET" and path.startswith("/v1/jobs?"):
            return 200, {"jobs": list(rows.values()), "total": len(rows)}
        if method == "POST" and path == "/v1/jobs":
            return 201, {"id": "review-child"}
        return 200, {}

    monkeypatch.setattr(doctor, "_http", http)
    return rows, calls, messages, monkeypatch


def _review_state_of(rows):
    return review_state(rows["source"])


def _submits(calls):
    return [c[2] for c in calls if c[:2] == ("POST", "/v1/jobs")]


def test_awaiting_review_with_budget_submits_one_review(bus):
    from hive.workers import doctor

    rows, calls, _, _ = bus
    summary = doctor.run_review_queue_sweep(rows=list(rows.values()))
    assert summary["submitted"] == 1
    spec = _submits(calls)[0]
    assert spec["parent_job_id"] == "source"
    assert spec["preferred_models"], "the cross-family pick is named on the review job"
    assert family_of(spec["preferred_models"][0]) != "alibaba"
    # The row stays in needs-review, with its queue state visible.
    patched = [c[2] for c in calls if c[0] == "PATCH"]
    assert patched and patched[-1]["status"] == "needs-review"
    state = patched[-1]["result"]["review_state"]
    assert state["state"] == "awaiting_review"
    assert state["tried"][-1]["model_id"] == spec["preferred_models"][0]


def test_awaiting_review_never_self_reviews(bus):
    from hive.review_queue import make_review_state as state
    from hive.workers import doctor

    rows, calls, _, monkeypatch = bus
    rows["source"]["result"] = {
        "head_sha": "a" * 40,
        "commits": ["a" * 40],
        "review_state": state("qwen38", [], attempt=0, now=0),
    }
    # Only the author's family is healthy: nothing cross-family exists.
    monkeypatch.setattr(
        doctor,
        "REVIEW_QUEUE_ROUTE_HEALTH",
        {route["id"]: route["family"] == "alibaba" for route in load_roster()},
    )
    summary = doctor.run_review_queue_sweep(rows=list(rows.values()))
    assert summary["submitted"] == 0
    assert summary["parked"] == 1
    assert not _submits(calls)
    patched = [c[2] for c in calls if c[0] == "PATCH"]
    assert patched[-1]["result"]["review_state"]["parked_reason"] == "no_cross_family_reviewer"


def test_prose_only_review_is_never_an_approval(bus):
    from hive.workers import doctor

    rows, calls, _, monkeypatch = bus
    with pytest.raises(ValueError):
        doctor.interpret_review_outcome("Looks great to me! Ship it.")
    rows["source"]["result"]["review_state"]["tried"] = [{"model_id": "gemma4-31b", "failure": "review_busy"}]
    outcome = doctor.interpret_review_outcome(
        {"reviewers": [{"model": "gemma4-31b", "verdict": "approve"}], "head_sha": "a" * 40}, head_sha="a" * 40
    )
    assert outcome["kind"] == "approve"
    assert outcome["reviewers"] == ["gemma4-31b"]


def test_sweep_consumes_review_child_verdicts(bus):
    from hive.review_queue import make_review_state as state
    from hive.workers import doctor

    rows, calls, messages, monkeypatch = bus
    rows["source"]["result"]["review_state"] = {
        **state("qwen38", [{"model_id": "gemma4-31b", "failure": "review_busy"}], attempt=1, now=0),
        "review_job_id": "review-child",
    }
    children = {
        "review-child": {
            "id": "review-child",
            "kind": "review:fix-20261031",
            "status": "done",
            "parent_job_id": "source",
            "result": {"verdict": {"reviewers": [{"model": "gemma4-31b", "verdict": "approve"}], "head_sha": "a" * 40}},
        }
    }

    def http(method, path, body=None, **kwargs):
        calls.append((method, path, body))
        if method == "GET" and "parent_job_id=" in path:
            return 200, {"jobs": list(children.values())}
        if method == "GET" and path.startswith("/v1/jobs?"):
            return 200, {"jobs": list(rows.values()), "total": 1}
        return 200, {}

    monkeypatch.setattr(doctor, "_http", http)
    summary = doctor.run_review_queue_sweep(rows=list(rows.values()))
    assert summary["approved"] == 1
    assert any(topic == "doctor.review_approved" for topic, _ in messages)
    patched = [c[2] for c in calls if c[0] == "PATCH"]
    assert patched[-1]["status"] == "needs-review"
    merged = patched[-1]["result"]["review_state"]["approved"]
    assert merged["reviewers"] == ["gemma4-31b"]
    # Handed to the merge path, on the same row.
    handoff = {**rows["source"], "result": patched[-1]["result"]}
    assert doctor.merge_handoff(handoff) == "ready_to_merge"


def test_request_changes_goes_back_to_the_author(bus):
    from hive.review_queue import make_review_state as state
    from hive.workers import doctor

    rows, calls, messages, monkeypatch = bus
    rows["source"]["result"]["review_state"] = {
        **state("qwen38", [{"model_id": "gemma4-31b", "failure": "review_busy"}], attempt=1, now=0),
        "review_job_id": "review-child",
    }
    child = {
        "id": "review-child",
        "kind": "review:fix-20261031",
        "status": "done",
        "parent_job_id": "source",
        "result": {
            "verdict": {"reviewers": [{"model": "gemma4-31b", "verdict": "request-changes"}], "head_sha": "b" * 40}
        },
    }

    def http(method, path, body=None, **kwargs):
        calls.append((method, path, body))
        if method == "GET" and "parent_job_id=" in path:
            return 200, {"jobs": [child]}
        return 200, {}

    monkeypatch.setattr(doctor, "_http", http)
    summary = doctor.run_review_queue_sweep(rows=list(rows.values()))
    assert summary["request_changes"] == 1
    assert any(topic == "doctor.review_request_changes" for topic, _ in messages)
    patched = [c[2] for c in calls if c[0] == "PATCH"]
    assert patched[-1]["result"]["review_state"]["request_changes"]["head_sha"] == "b" * 40


def test_reviewer_failure_schedules_exponential_backoff(bus):
    """A busy/unavailable reviewer schedules the NEXT attempt with doubling delay."""
    from hive.review_queue import make_review_state as state
    from hive.workers import doctor

    rows, calls, _, monkeypatch = bus

    def no_reviewer(*a, **k):
        return []

    monkeypatch.setattr(doctor, "pick_reviewers", no_reviewer)
    rows["source"]["result"]["review_state"] = state("qwen38", [], attempt=0, now=0)
    doctor.run_review_queue_sweep(rows=list(rows.values()))
    nxt = [c[2] for c in calls if c[0] == "PATCH"][-1]["result"]["review_state"]["next_attempt_at"]
    calls.clear()
    rows["source"]["result"]["review_state"] = state(
        "qwen38",
        [{"model_id": "gemma4-31b", "failure": "review_busy"}],
        attempt=1,
        next_attempt_at=nxt,
        now=0,
    )
    # A row still inside its backoff window waits: no second submission.
    second = doctor.run_review_queue_sweep(rows=list(rows.values()), now=nxt - 1)
    assert second["deferred"] == 1 and second["submitted"] == 0
    # Once the window passes the next attempt runs and its delay doubles.
    calls.clear()
    third = doctor.run_review_queue_sweep(rows=list(rows.values()), now=nxt + 1)
    later = [c[2] for c in calls if c[0] == "PATCH"][-1]["result"]["review_state"]["next_attempt_at"]
    assert third["parked"] == 1
    # The delay doubles per attempt (15 min, 30 min, ...) and is bounded.
    assert later - (nxt + 1) == backoff_seconds(1) == 2 * backoff_seconds(0)
    assert later > nxt


def test_attempts_exhausted_stays_needs_review_with_owner_wait_reason(bus):
    from hive.review_queue import MAX_REVIEW_ATTEMPTS
    from hive.review_queue import make_review_state as state
    from hive.workers import doctor

    rows, calls, _, monkeypatch = bus

    def no_reviewer(*a, **k):
        return []

    monkeypatch.setattr(doctor, "pick_reviewers", no_reviewer)
    rows["source"]["result"]["review_state"] = state(
        "qwen38", [{"model_id": "gemma4-31b", "failure": "review_busy"}], attempt=MAX_REVIEW_ATTEMPTS, now=0
    )
    summary = doctor.run_review_queue_sweep(rows=list(rows.values()))
    assert summary["exhausted"] == 1 and summary["submitted"] == 0
    # Never failed, never deleted: still a needs-review row.
    assert not any(c[0] == "DELETE" for c in calls)
    patched = [c[2] for c in calls if c[0] == "PATCH"]
    assert patched[-1]["status"] == "needs-review"
    assert "owner" in patched[-1]["result"]["review_state"]["parked_reason"]


def test_plain_json_verdict_without_reviewers_is_not_an_approval():
    from hive.workers import doctor

    with pytest.raises(ValueError):
        doctor.interpret_review_outcome({"verdict": "approve"})
    with pytest.raises(ValueError):
        doctor.interpret_review_outcome('{"verdict": "approve", "reviewers": "gemma4-31b"}')


def test_structured_verdict_shape_is_accepted():
    from hive.workers import doctor

    out = doctor.interpret_review_outcome(
        {
            "reviewers": [{"model": "gemma4-31b", "verdict": "approve"}],
            "head_sha": "a" * 40,
        },
        head_sha="a" * 40,
    )
    assert out["kind"] == "approve" and out["reviewers"] == ["gemma4-31b"]


def test_structured_verdict_for_a_different_head_is_not_an_approval():
    from hive.workers import doctor

    with pytest.raises(ValueError):
        doctor.interpret_review_outcome(
            {"reviewers": [{"model": "gemma4-31b", "verdict": "approve"}], "head_sha": "b" * 40},
            head_sha="a" * 40,
        )


def test_reviewer_unavailable_row_is_awaiting_review_not_failed():
    from hive.review_queue import awaiting_review_status

    status, reason = awaiting_review_status(
        loop_teardown="reviewer_unavailable", exit_code=1, ready=True, cancelled=False
    )
    assert status == "needs-review" and reason == "reviewer_unavailable"
    status, reason = awaiting_review_status(
        loop_teardown="reviewer_unavailable", exit_code=1, ready=False, cancelled=False
    )
    assert status == "needs-review"
    # An author failure is not a reviewer outage.
    assert (
        awaiting_review_status(loop_teardown="author_failed", exit_code=1, ready=True, cancelled=False)[0] == "failed"
    )


def test_awaiting_review_row_is_visible_in_triage_and_not_stale(bus):
    from hive.workers import doctor

    rows, calls, messages, _ = bus
    rows["source"]["result"]["review_state"] = make_review_state(
        "qwen38",
        [{"model_id": "gpt-oss-20b", "failure": "review_busy"}],
        attempt=1,
        next_attempt_at=time.time() + 900,
        last_failure="reviewer_unavailable",
        now=0,
    )
    report = doctor.report_needs_review_sweep()
    assert report["scanned"] == 1
    row = report["rows"][0]
    assert row["job_id"] == "source"
    assert row["review_state"] == "awaiting_review"
    assert row["next_attempt_at"] is not None
    assert "awaiting cross-family review" in row["suggested_action"]
    assert not row["stale"]
    reason = awaiting_review_reason(rows["source"])
    assert "reviewer unavailable" in reason or "reviewer_unavailable" in reason
    assert triage_reason(rows["source"]).startswith("review-queue:")


def test_doctor_does_not_call_an_awaiting_row_stale_while_attempts_remain(bus):
    from hive.workers import doctor

    rows, calls, messages, _ = bus
    rows["source"]["result"]["review_state"] = make_review_state(
        "qwen38", [], attempt=1, next_attempt_at=time.time() + 900, now=0
    )
    doctor.report_needs_review_sweep()
    assert not any(topic == "doctor.needs_review_stuck" for topic, _ in messages)


def test_exhausted_awaiting_review_row_waits_for_an_owner(bus):
    from hive.workers import doctor

    rows, calls, messages, _ = bus
    rows["source"]["result"]["review_state"] = make_review_state(
        "qwen38", [{"model_id": "gemma4-31b", "failure": "review_busy"}], attempt=99, now=0
    )
    report = doctor.report_needs_review_sweep()
    assert "owner" in report["rows"][0]["suggested_action"]
    summary = doctor.run_review_queue_sweep(rows=list(rows.values()))
    assert summary["exhausted"] == 1 and summary["submitted"] == 0


# ── 5. load awareness ──────────────────────────────────────────────────────


class _FakeResponse:
    def __init__(self, body):
        self.body = body.encode()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self, *a):
        return self.body


def test_route_load_reads_llama_slots_with_a_30s_cache(monkeypatch):
    from hive import review_queue as rq

    monkeypatch.setattr(rq, "_ROSTER_CACHE", {"key": None, "routes": ()})
    local = [e for e in rq.load_roster() if e["location"] == "local" and e["model_id"] == "gpt-oss-20b"][0]
    body = json.dumps([{"id": 0, "is_processing": True}, {"id": 1, "is_processing": False}])
    fetches = []

    def fake_urlopen(url, timeout=0):
        fetches.append(url)
        return _FakeResponse(body)

    clock = {"t": 1000.0}
    fn = rq.route_load_fn(urlopen=fake_urlopen, clock=lambda: clock["t"])
    assert fn(local) == 1
    assert fn(local) == 1
    assert all(url.endswith("/slots") for url in fetches)
    assert len(fetches) == 1
    clock["t"] += 31
    fn(local)
    assert len(fetches) == 2


def test_route_load_reads_vllm_metrics_for_the_tydeus_server():
    from hive import review_queue as rq

    route = [e for e in rq.load_roster() if e["server"] == "vllm"][0]
    metrics = 'vllm:num_requests_running{model_name="q"} 3\nvllm:num_requests_waiting{model_name="q"} 2\n'
    fetches = []

    def fake_urlopen(url, timeout=0):
        fetches.append(url)
        if url.endswith("/metrics"):
            return _FakeResponse(metrics)
        return _FakeResponse("not found")

    assert rq.route_load_fn(urlopen=fake_urlopen, clock=lambda: 1.0)(route) == 5
    assert fetches == [route["base_url"].rstrip("/") + "/metrics"]


def test_route_load_of_an_unreachable_route_is_a_failure_not_zero(monkeypatch):
    from hive import review_queue as rq

    local = [e for e in rq.load_roster() if e["location"] == "local"][0]

    def boom(url, timeout=0):
        raise OSError("connection refused")

    fn = rq.route_load_fn(urlopen=boom, clock=lambda: 1.0)
    assert fn(local) is rq.LOAD_UNKNOWN


def test_route_load_returns_none_off_the_load_path():
    from hive import review_queue as rq

    provider = [e for e in rq.load_roster() if e["location"] == "provider"][0]
    assert route_load_fn(urlopen=lambda *a, **k: None, clock=lambda: 1.0)(provider) == 0


# ── 6b. dashboard counts (regression: the queue half must be counted) ────────


def test_doctor_counts_awaiting_review_rows_as_queued_work():
    from hive.workers import doctor

    queued = {
        "id": "q1",
        "status": "needs-review",
        "result": {"review_state": make_review_state("qwen38", [], attempt=0, now=0)},
    }
    parked = {
        "id": "q2",
        "status": "needs-review",
        "result": {"review_state": make_review_state("qwen38", [], parked_reason="no_cross_family_reviewer", now=0)},
    }
    plain = {"id": "plain", "status": "needs-review", "result": {}}
    counts = doctor.needs_review_queue_counts([queued, parked, plain])
    assert counts["total"] == 3
    assert counts["queued"] == 1
    assert counts["parked"] == 1
    assert counts["unreviewed"] == 1
    assert counts["awaiting_review"] == 2
    # An exhausted row is waiting for an OWNER, not hidden inside "queued".
    tired = {
        "id": "q3",
        "status": "needs-review",
        "result": {"review_state": make_review_state("qwen38", [], attempt=99, now=0)},
    }
    counts = doctor.needs_review_queue_counts([tired])
    assert counts["exhausted"] == 1
    assert counts["queued"] == 0
    assert counts["awaiting_review"] == 1


def test_doctor_followup_blocker_names_the_queue():
    from hive.workers import doctor

    row = {
        "id": "waiting",
        "kind": "code",
        "status": "needs-review",
        "result": {"review_state": make_review_state("qwen38", [], attempt=1, now=0)},
    }
    blocker = doctor.review_followup_blocker(row)
    assert blocker is not None
    # It names the queue, and specifically NOT "no review was submitted".
    assert "queue" in blocker
