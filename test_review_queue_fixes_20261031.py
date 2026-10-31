"""Review-queue defects found in review of hv/gh-review-queue-20261031.

Each test here pins one defect the branch shipped with (every one failed on the
branch head before its fix):

* the family table left real model names outside every family (lower-cased
  ``minimax-m3.1-flash-preview``, ``MiniMax-M4``, the ``minimax_flash`` alias,
  the ``cerberus-reviewer`` role id), so an author with one of those names was
  "its own family" and could be paired with a SAME-family reviewer;
* ``pick_reviewers`` with no known author returned every route instead of
  parking the row;
* a review child that APPROVED was accepted even when the approving reviewer
  was the author's own family, when it reviewed a different head, or when it
  named no head at all;
* a failed (prose-only) review child stayed recorded as in flight, so every
  later sweep re-read the same dead child: no new attempt, the attempt count
  never moved and the row could never reach owner-wait;
* nothing capped how many reviews one sweep sent to a route;
* the EIH Nemotron route (39 s for a one-word reply) sorted ahead of a local
  route that had a single request running;
* the load probe asked ``<base>/v1/slots`` and ``<base>/v1/metrics`` (both live
  at the server root) and read a hardcoded LAN host:port from the roster;
* a ``result`` stored as JSON text (a bus dialect) hid the row from the queue;
* the needs-review report still classified an awaiting row ``stale`` while
  attempts remained, and an exhausted row was rewritten on every sweep and
  never escalated to its owner.

Everything is hermetic: the bus, the doctor state and the route probes are
test doubles, and the optional role map / provenance resolvers are removed so
the family answers come from the data files alone.
"""

import json
import sys

import pytest

from hive import review_queue as rq
from hive.review_queue import (
    MAX_REVIEW_ATTEMPTS,
    backoff_seconds,
    family_of,
    load_family_table,
    load_roster,
    make_review_state,
    pick_reviewers,
    review_state,
)

HEAD = "a" * 40
OTHER_HEAD = "b" * 40


@pytest.fixture(autouse=True)
def table_only(monkeypatch):
    """Family answers from the data files only (no deployed resolver modules)."""
    monkeypatch.setitem(sys.modules, "provenance", None)
    monkeypatch.setitem(sys.modules, "hive.llm_roles", None)
    rq._family_of_cached.cache_clear()
    yield
    rq._family_of_cached.cache_clear()


def _patterns_by_family():
    import fnmatch

    table = load_family_table()["exact"]
    out = {}
    for entry in table.values():
        out.setdefault(entry["family"], set()).update(entry["patterns"])

    def families_matching(name):
        low = name.lower()
        return {fam for fam, pats in out.items() if any(fnmatch.fnmatchcase(low, p) for p in pats)}

    return families_matching


# ── the family table ─────────────────────────────────────────────────────────


def test_every_roster_model_matches_exactly_one_family_pattern():
    """Not just "the roster agrees with family_of": each roster model, in any
    letter case, is claimed by the patterns of exactly ONE family."""
    families_matching = _patterns_by_family()
    for route in load_roster():
        for spelling in (route["model_id"], route["model_id"].lower(), route["model_id"].upper()):
            assert families_matching(spelling) == {route["family"]}, spelling
            assert family_of(spelling) == route["family"], spelling


def test_every_roster_route_and_role_name_resolves_to_its_route_family():
    """A row may name the route id or a fleet role id instead of the model."""
    for route in load_roster():
        names = [route["id"], route.get("role_id"), *(route.get("roles") or [])]
        for name in [n for n in names if n]:
            assert family_of(name) == route["family"], name


@pytest.mark.parametrize(
    ("model", "family"),
    [
        ("minimax-m3.1-flash-preview", "minimax"),
        ("MiniMax-M3.1-Flash", "minimax"),
        ("MiniMax-M4", "minimax"),
        ("minimax_flash", "minimax"),
        ("minimax", "minimax"),
        ("cerberus-reviewer", "openai"),
        ("cerberus-narrator", "openai"),
        ("gpt-5.1-codex", "openai"),
        ("GEMMA4-31B", "google"),
        ("Nemotron-3-Super", "nvidia"),
        ("DeepSeek-V4-Pro", "deepseek"),
        ("StepFun-3.5", "stepfun"),
        ("Muse-Spark-1.3", "meta"),
        ("MiMo-V2.6-Pro", "xiaomi"),
        ("Claude-Opus-4", "anthropic"),
    ],
)
def test_family_of_resolves_aliases_case_and_newer_versions(model, family):
    assert family_of(model) == family


def test_unknown_model_stays_its_own_family():
    assert family_of("widget-9000") == "model:widget-9000"


# ── pairing ─────────────────────────────────────────────────────────────────

#: Every author spelling the fleet uses, with the family it really belongs to.
#: Pinned, not ``family_of(author)``: a name the table failed to resolve would
#: otherwise be "its own family" and the check would pass vacuously.
_AUTHORS = {
    "qwen38": "alibaba",
    "Qwen3-Coder-30B": "alibaba",
    "gemma4-31b": "google",
    "gpt-oss-20b": "openai",
    "codex": "openai",
    "cerberus-reviewer": "openai",
    "cerberus-narrator": "openai",
    "tydeus-qwen38": "alibaba",
    "MiniMax-M3": "minimax",
    "minimax-m3.1-flash-preview": "minimax",
    "minimax_flash": "minimax",
    "MiniMax-M4": "minimax",
    "deepseek-flash": "deepseek",
    "muse-spark-1.3": "meta",
    "stepfun-3.5": "stepfun",
    "nvidia/nemotron-3-ultra-550b-a55b": "nvidia",
    "mimo-v2.6-flash": "xiaomi",
    "claude-sonnet-4-6": "anthropic",
    "widget-9000": "model:widget-9000",
}


@pytest.mark.parametrize("author", sorted(_AUTHORS))
@pytest.mark.parametrize("repo", [None, "graeae-hive", "some-other-repo"])
def test_pick_reviewers_never_returns_a_same_family_route(author, repo):
    author_family = _AUTHORS[author]
    assert family_of(author) == author_family
    picked = pick_reviewers(author, health=lambda r: True, load=lambda r: 0, repo=repo)
    for route in picked:
        assert family_of(route["model_id"]) != author_family, (author, route["id"])
        assert route["family"] != author_family, (author, route["id"])
        assert not route["model_id"].lower().startswith("mimo")


@pytest.mark.parametrize("author", ["", "   ", None])
def test_pick_reviewers_without_an_author_parks(author):
    """No author family means no cross-family guarantee: PARK, never guess."""
    assert pick_reviewers(author, health=lambda r: True, load=lambda r: 0, repo="graeae-hive") == []


def test_pick_reviewers_with_no_cross_family_route_is_empty():
    same = [r for r in load_roster() if r["family"] == "minimax"]
    assert pick_reviewers("minimax_flash", same, health=lambda r: True, load=lambda r: 0) == []


# ── load probe ──────────────────────────────────────────────────────────────


class _Resp:
    def __init__(self, body):
        self.body = body.encode()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self, *a):
        return self.body


@pytest.fixture
def inventory(monkeypatch, tmp_path):
    """Host addresses from the operator's fleet inventory (TEST-NET only)."""
    path = tmp_path / "fleet.json"
    path.write_text(
        json.dumps(
            {"schema_version": 1, "hosts": {"cerberus": {"address": "192.0.2.96"}, "tydeus": {"address": "192.0.2.73"}}}
        )
    )
    monkeypatch.setenv("HIVE_FLEET_INVENTORY", str(path))
    return path


def test_roster_carries_no_hardcoded_local_endpoint():
    for route in load_roster():
        if route["location"] == "local":
            assert not route.get("base_url"), route["id"]
            assert route.get("host") and isinstance(route.get("port"), int), route["id"]


def test_load_probe_reads_llama_slots_at_the_server_root(inventory):
    route = next(r for r in load_roster() if r["model_id"] == "gpt-oss-20b")
    fetched = []

    def urlopen(url, timeout=0):
        fetched.append(url)
        return _Resp(json.dumps([{"is_processing": True}, {"is_processing": True}, {"is_processing": False}]))

    assert rq.route_load_fn(urlopen=urlopen, clock=lambda: 1.0)(route) == 2
    assert fetched == [f"http://192.0.2.96:{route['port']}/slots"]


def test_load_probe_reads_vllm_metrics_at_the_server_root(inventory):
    route = next(r for r in load_roster() if r.get("server") == "vllm")
    fetched = []

    def urlopen(url, timeout=0):
        fetched.append(url)
        return _Resp("vllm:num_requests_running 1\nvllm:num_requests_waiting 1\n")

    assert rq.route_load_fn(urlopen=urlopen, clock=lambda: 1.0)(route) == 2
    assert fetched == [f"http://192.0.2.73:{route['port']}/metrics"]


def test_load_probe_without_an_inventory_address_is_unknown(monkeypatch):
    monkeypatch.delenv("HIVE_FLEET_INVENTORY", raising=False)
    route = next(r for r in load_roster() if r["location"] == "local")

    fetched = []

    def urlopen(url, timeout=0):  # must not be called: the probe swallows raises
        fetched.append(url)
        raise OSError("unreachable")

    assert rq.route_load_fn(urlopen=urlopen, clock=lambda: 1.0)(route) is rq.LOAD_UNKNOWN
    assert fetched == [], "probed an address that did not come from the inventory"


# ── row state ───────────────────────────────────────────────────────────────


def test_review_state_reads_a_json_text_result():
    state = make_review_state("qwen38", [], now=0)
    assert review_state({"result": json.dumps({"review_state": state})}) == state
    assert review_state({"result": "not json"}) is None


# ── the doctor sweep ────────────────────────────────────────────────────────


@pytest.fixture
def bus(monkeypatch):
    from hive.workers import doctor
    from hive.workers.doctor_state import DurableStateStore, MemoryBackend

    monkeypatch.setattr(doctor, "STATE", DurableStateStore(MemoryBackend(), refresh_secs=0))
    monkeypatch.setattr(doctor, "REVIEW_QUEUE_ROUTE_HEALTH", {})
    monkeypatch.setattr(doctor, "REVIEW_QUEUE_ROUTE_LOAD", lambda: 0)
    messages = []
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda topic, body: messages.append((topic, body)))

    class Bus:
        def __init__(self):
            self.calls = []
            self.children = {}
            self.messages = messages
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
            return 200, {}

        def patches(self, jid=None):
            return [c[2] for c in self.calls if c[0] == "PATCH" and (jid is None or c[1].endswith("/" + jid))]

        def posts(self):
            return [c[2] for c in self.calls if c[:2] == ("POST", "/v1/jobs")]

        def finish(self, child_id, result, status="done"):
            self.children[child_id].update(status=status, result=result)

    fake = Bus()
    monkeypatch.setattr(doctor, "_http", fake.http)
    return fake


def _row(jid="source", *, author="qwen38", state=None, **extra):
    return {
        "id": jid,
        "kind": "fix:thing",
        "status": "needs-review",
        "repo": "graeae-hive",
        "head_branch": f"fix/{jid}",
        "base_branch": "main",
        "result": {
            "head_sha": HEAD,
            "commits": [HEAD],
            "review_state": state if state is not None else make_review_state(author, [], attempt=0, now=0),
        },
        **extra,
    }


def _in_flight(child_id="review-child", route="cerberus-gemma", model="gemma4-31b", attempt=1):
    return {
        **make_review_state("qwen38", [{"model_id": model, "failure": "review_submitted"}], attempt=attempt, now=0),
        "review_job_id": child_id,
        "review_route": route,
        "review_model": model,
    }


def _with_child(bus, verdict_result, status="done", child_id="review-child"):
    bus.children[child_id] = {
        "id": child_id,
        "kind": "review:fix-20261031",
        "status": status,
        "parent_job_id": "source",
        "result": verdict_result,
    }


def test_sweep_reads_a_json_text_result_row(bus):
    from hive.workers import doctor

    row = _row()
    row["result"] = json.dumps(row["result"])
    summary = doctor.run_review_queue_sweep(rows=[row], now=1000.0)
    assert summary["submitted"] == 1
    assert family_of(bus.posts()[0]["preferred_models"][0]) != "alibaba"


def test_same_family_approval_is_not_an_approval(bus):
    from hive.workers import doctor

    row = _row(state=_in_flight())
    _with_child(bus, {"verdict": {"reviewers": [{"model": "qwen38", "verdict": "approve"}], "head_sha": HEAD}})
    summary = doctor.run_review_queue_sweep(rows=[row], now=1000.0)
    assert summary["approved"] == 0
    state = review_state(row)
    assert not state.get("approved")
    assert state["last_failure"] == "review_same_family"
    assert not any(topic == "doctor.review_approved" for topic, _ in bus.messages)


def test_only_the_cross_family_reviewers_verdict_counts(bus):
    from hive.workers import doctor

    row = _row(state=_in_flight())
    _with_child(
        bus,
        {
            "verdict": {
                "reviewers": [
                    {"model": "Qwen3-Coder-30B", "verdict": "approve"},
                    {"model": "gemma4-31b", "verdict": "request-changes"},
                ],
                "head_sha": HEAD,
            }
        },
    )
    summary = doctor.run_review_queue_sweep(rows=[row], now=1000.0)
    assert summary["request_changes"] == 1 and summary["approved"] == 0
    assert review_state(row)["request_changes"]["reviewers"] == ["gemma4-31b"]


def test_approval_of_a_different_head_is_not_an_approval(bus):
    from hive.workers import doctor

    row = _row(state=_in_flight())
    _with_child(
        bus,
        {
            "head_sha": OTHER_HEAD,
            "verdict": {"reviewers": [{"model": "gemma4-31b", "verdict": "approve"}], "head_sha": OTHER_HEAD},
        },
    )
    summary = doctor.run_review_queue_sweep(rows=[row], now=1000.0)
    assert summary["approved"] == 0
    assert review_state(row)["last_failure"] == "review_head_mismatch"


def test_approval_that_names_no_head_is_not_an_approval(bus):
    from hive.workers import doctor

    row = _row(state=_in_flight())
    _with_child(bus, {"verdict": {"reviewers": [{"model": "gemma4-31b", "verdict": "approve"}]}})
    summary = doctor.run_review_queue_sweep(rows=[row], now=1000.0)
    assert summary["approved"] == 0
    assert not review_state(row).get("approved")


def test_cross_family_approval_of_the_delivered_head_goes_to_the_merge_path(bus):
    from hive.workers import doctor

    row = _row(state=_in_flight())
    _with_child(bus, {"verdict": {"reviewers": [{"model": "gemma4-31b", "verdict": "approve"}], "head_sha": HEAD}})
    summary = doctor.run_review_queue_sweep(rows=[row], now=1000.0)
    assert summary["approved"] == 1
    assert row["status"] == "needs-review"
    assert doctor.merge_handoff(row) == "ready_to_merge"
    approved = [body for topic, body in bus.messages if topic == "doctor.review_approved"]
    assert approved and approved[0]["merge"] == "ready_to_merge"


def test_prose_only_review_retries_with_a_new_child_and_attempt_plus_one(bus):
    from hive.workers import doctor

    row = _row(state=_in_flight(attempt=1))
    _with_child(bus, {"tail": "Looks good to me, ship it."})
    first = doctor.run_review_queue_sweep(rows=[row], now=1000.0)
    assert first["review_failed"] == 1 and first["approved"] == 0
    state = review_state(row)
    assert state["state"] == "awaiting_review" and row["status"] == "needs-review"
    assert state["last_failure"] == "review_prose_only"
    assert state["attempt"] == 1
    assert not state.get("review_job_id"), "the dead child must not stay in flight"
    assert state["next_attempt_at"] == 1000.0 + backoff_seconds(1)
    # Once the backoff elapses a NEW review is submitted: attempt 2.
    second = doctor.run_review_queue_sweep(rows=[row], now=state["next_attempt_at"] + 1)
    assert second["submitted"] == 1
    assert review_state(row)["attempt"] == 2
    assert review_state(row)["review_job_id"] == "review-1"


def test_repeated_failures_exhaust_to_owner_wait_and_then_stay_quiet(bus):
    from hive.workers import doctor

    row = _row()
    now = 1000.0
    for attempt in range(1, MAX_REVIEW_ATTEMPTS + 1):
        assert doctor.run_review_queue_sweep(rows=[row], now=now)["submitted"] == 1
        state = review_state(row)
        assert state["attempt"] == attempt
        bus.finish(state["review_job_id"], {"tail": ""})
        now += 1
        doctor.run_review_queue_sweep(rows=[row], now=now)
        now = (review_state(row).get("next_attempt_at") or now) + 1
    state = review_state(row)
    assert state["parked_reason"] == "attempts_exhausted_owner_wait"
    assert row["status"] == "needs-review"
    assert len(bus.posts()) == MAX_REVIEW_ATTEMPTS
    # Further sweeps neither resubmit nor rewrite the row (no PATCH storm).
    before = len(bus.calls)
    again = doctor.run_review_queue_sweep(rows=[row], now=now + 10_000)
    assert again["exhausted"] == 1
    assert not [c for c in bus.calls[before:] if c[0] in ("PATCH", "POST")]


def test_one_sweep_never_exceeds_a_routes_concurrency(bus):
    from hive.workers import doctor

    gemma = [r for r in load_roster() if r["id"] == "cerberus-gemma"]
    assert gemma and gemma[0]["concurrency"] == 2
    rows = [_row(f"row{i}") for i in range(5)]
    summary = doctor.run_review_queue_sweep(rows=rows, now=1000.0, roster=gemma, load=lambda r: 0)
    assert summary["submitted"] == 2
    # The rows over the cap wait unchanged: no attempt spent, no park.
    waiting = [r for r in rows if not review_state(r).get("review_job_id")]
    assert len(waiting) == 3
    for r in waiting:
        assert review_state(r)["attempt"] == 0 and not review_state(r).get("parked_reason")


def test_in_flight_reviews_count_against_the_cap(bus):
    from hive.workers import doctor

    gemma = [r for r in load_roster() if r["id"] == "cerberus-gemma"]
    busy = _row("source", state=_in_flight())
    _with_child(bus, {}, status="running")
    fresh = [_row(f"row{i}") for i in range(3)]
    summary = doctor.run_review_queue_sweep(rows=[busy, *fresh], now=1000.0, roster=gemma, load=lambda r: 0)
    assert summary["awaited"] == 1
    assert summary["submitted"] == 1


def test_whole_roster_sweep_respects_every_routes_cap(bus):
    from hive.workers import doctor

    rows = [_row(f"row{i}") for i in range(40)]
    doctor.run_review_queue_sweep(rows=rows, now=1000.0, load=lambda r: 0)
    caps = {r["id"]: r["concurrency"] for r in load_roster()}
    sent = {}
    for r in rows:
        route = review_state(r).get("review_route")
        if route and review_state(r).get("review_job_id"):
            sent[route] = sent.get(route, 0) + 1
    assert sent, "something was submitted"
    for route, n in sent.items():
        assert n <= caps[route], (route, n)
        assert family_of(next(x["model_id"] for x in load_roster() if x["id"] == route)) != "alibaba"


def test_slow_eih_route_sorts_last_even_when_local_routes_are_busy():
    from hive.workers import doctor

    roster = load_roster()
    picked = doctor._pick_routes(
        "qwen38",
        {"repo": "graeae-hive"},
        roster,
        lambda r: True,
        lambda r: 1 if r["location"] == "local" else 0,
        [],
    )
    assert picked[-1]["id"] == "eih-nemotron"


# ── triage classification and escalation ───────────────────────────────────


def test_report_does_not_classify_an_awaiting_row_stale_while_attempts_remain(bus):
    from hive.workers import doctor

    row = _row(state=make_review_state("qwen38", [], attempt=1, next_attempt_at=4e12, now=0), updated_at=1.0)
    report_rows = _report(doctor, bus, [row])
    assert report_rows[0]["classification"] == "awaiting_review"
    assert report_rows[0]["stale"] is False
    assert "stale" not in report_rows[0]["orchestrator_note"]


def test_report_lets_an_exhausted_row_be_stale_and_names_the_owner(bus):
    from hive.workers import doctor

    exhausted = {
        **make_review_state("qwen38", [], attempt=MAX_REVIEW_ATTEMPTS, now=0),
        "parked_reason": "attempts_exhausted_owner_wait",
    }
    report_rows = _report(doctor, bus, [_row(state=exhausted, updated_at=1.0)])
    assert report_rows[0]["classification"] == "stale"
    assert report_rows[0]["stale"] is True
    assert "owner" in report_rows[0]["suggested_action"]


def _report(doctor, bus, rows):
    def http(method, path, body=None, **kwargs):
        if method == "GET" and path.startswith("/v1/jobs?"):
            return 200, {"jobs": rows, "total": len(rows)}
        return bus.http(method, path, body, **kwargs)

    doctor._http = http  # restored by the bus fixture's monkeypatch
    return doctor.report_needs_review_sweep()["rows"]


def _escalate(doctor, row, now):
    return doctor.escalate_stuck_needs_review([row], evidence_fn=lambda job: {"head_branch": "x"}, now=now)


def test_awaiting_row_with_attempts_left_is_never_escalated(bus):
    from hive.workers import doctor

    now = 1_000_000.0
    row = _row(state=make_review_state("qwen38", [], attempt=1, now=0), updated_at=now - 10 * 86400)
    assert _escalate(doctor, row, now) == []


def test_exhausted_awaiting_row_is_escalated_to_its_owner(bus):
    from hive.workers import doctor

    now = 1_000_000.0
    exhausted = {
        **make_review_state("qwen38", [], attempt=MAX_REVIEW_ATTEMPTS, now=0),
        "parked_reason": "attempts_exhausted_owner_wait",
    }
    row = _row(state=exhausted, updated_at=now - 10 * 86400)
    assert _escalate(doctor, row, now) == ["source"]
    stuck = [body for topic, body in bus.messages if topic == "doctor.needs_review_stuck"]
    assert stuck and "exhausted" in stuck[0]["reason"]
