"""Durable review queue (operator rule 2026-10-31).

Today a loop whose reviewer endpoint is busy ends ``reviewer_unavailable`` and
FAILS: all 8 slots of the CERBERUS gpt-oss-20b endpoint were busy and about 20
loops failed in one night. The queue replaces that failure with a state:

* authoring finishes, the reviewer is unavailable (busy, timeout, prose-only,
  empty, truncated) -> the branch is pushed and the row is recorded as
  ``awaiting-review`` (a ``result`` field plus a status the doctor understands),
  NOT ``failed``;
* a consumer (``hive/workers/review_queue.py``) turns queued rows into review
  submissions for the next CROSS-FAMILY route, with retry, exponential backoff
  and a per-route concurrency cap;
* a prose-only, empty, truncated, 503 or non-JSON review is NEVER an approval —
  only a structured JSON verdict naming ``reviewers[].model`` counts;
* the doctor's needs-review sweep treats an awaiting-review row as QUEUED work,
  not stale.

Every test drives the pure helpers; the few that touch the worker import it the
same way ``tests/test_worker_build_busy.py`` does (the deploy sidecars are
stubbed because this checkout does not ship them).
"""

from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest

from hive.workers import review_queue as rq
from hive.workers import reviewer_roster as rr

ROOT = Path(__file__).resolve().parent


@pytest.fixture
def worker(monkeypatch):
    budget = types.ModuleType("hive_budget")
    budget.FAILURE_REASON = "budget_exceeded"
    provenance = types.ModuleType("provenance")
    provenance.STATIC_AGENT_MODELS = {}
    provenance.KNOWN_COST_TIERS = set()
    monkeypatch.setitem(sys.modules, "hive_budget", budget)
    monkeypatch.setitem(sys.modules, "provenance", provenance)
    for name in ("codex_delivery", "llm_roles", "push_policy", "review_evidence"):
        module = types.ModuleType("hive." + name)
        if name == "codex_delivery":
            module.fleet_git_env = lambda *a, **kw: {}
        if name == "push_policy":
            module.argonas_url_for_slug = lambda slug: slug
        monkeypatch.setitem(sys.modules, "hive." + name, module)
    spec = importlib.util.spec_from_file_location("terra_worker", ROOT / "deploy" / "hive_worker.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ── (4a) the awaiting-review recording rule ─────────────────────────────────


@pytest.mark.parametrize(
    "failure_reason",
    [
        "reviewer_unavailable",
        "loop_unresolved_work_rescued",
        "review_unverified",
        "",
    ],
)
def test_unreviewed_delivery_is_recorded_as_awaiting_review_not_failed(worker, failure_reason):
    """A delivery with commits and NO review is queued, never failed."""

    result = {
        "review_evidence": {"final_verdict": "reviewer_unavailable", "reviewers": []},
        "loop_result": {"teardown": {"final_verdict": "reviewer_unavailable", "resolved": False}},
        "head_sha": "a" * 40,
        "remote_sha": "a" * 40,
        "remote_verified": True,
    }
    verdict = rq.awaiting_review_record(
        status="failed" if failure_reason == "reviewer_unavailable" else "needs-review",
        failure_reason=failure_reason,
        result=result,
        has_commits=True,
        pushed=True,
    )
    assert verdict is not None
    assert verdict.status == "needs-review"
    assert verdict.result["awaiting_review"]["state"] == "queued"
    assert verdict.result["awaiting_review"]["head"] == "a" * 40
    assert verdict.failure_reason in (None, "reviewer_unavailable")


def test_no_review_needed_without_commits():
    assert (
        rq.awaiting_review_record(
            status="failed",
            failure_reason="reviewer_unavailable",
            result={"head_sha": "b" * 40},
            has_commits=False,
            pushed=False,
        )
        is None
    )


def test_unpushed_branch_is_not_queued():
    """The queue exists for DELIVERED work: an unpushed head cannot be reviewed."""
    assert (
        rq.awaiting_review_record(
            status="failed",
            failure_reason="reviewer_unavailable",
            result={"head_sha": "c" * 40},
            has_commits=True,
            pushed=False,
        )
        is None
    )


def test_approved_delivery_is_never_downgraded():
    result = {
        "review_evidence": {"reviewers": [{"model": "gemma4-31b", "verdict": "approve"}]},
        "reviewer": {"model": "gemma4-31b", "verdict": "approve"},
        "head_sha": "d" * 40,
    }
    assert (
        rq.awaiting_review_record(status="done", failure_reason=None, result=result, has_commits=True, pushed=True)
        is None
    )


# ── (4b) only a structured verdict counts ───────────────────────────────────


STRUCTURED = {
    "verdict": "approve",
    "reviewers": [{"model": "gemma4-31b", "family": "google", "verdict": "approve"}],
    "head_sha": "e" * 40,
}


@pytest.mark.parametrize(
    "payload",
    [
        # prose only
        "Looks good to me, approved!",
        "",
        None,
        "   \n",
        # truncated JSON
        '{"verdict": "approve", "reviewers": [{"model": "gemma4-31b"',
        # 503 / HTML error page
        "<html><body>503 Service Unavailable</body></html>",
        '{"error": "503 Service Unavailable"}',
        # JSON but no named reviewer
        '{"verdict": "approve", "reviewers": []}',
        '{"verdict": "approve", "reviewers": [{"model": "(auto)"}]}',
        '{"verdict": "approve"}',
        # a bare string, not a structured object
        '"approve"',
    ],
)
def test_prose_empty_truncated_or_nonjson_is_never_an_approval(payload):
    verdict, reason = rq.parse_structured_verdict(payload)
    assert verdict is None
    assert reason in {"prose_only", "empty", "truncated", "non_json", "no_named_reviewer"}


def test_structured_verdict_with_named_model_is_the_only_approval():
    verdict, reason = rq.parse_structured_verdict(json.dumps(STRUCTURED))
    assert reason is None
    assert verdict["verdict"] == "approve"
    assert verdict["reviewers"][0]["model"] == "gemma4-31b"
    # ... and it is fed straight to the queue's completion rule.
    assert rq.review_counts_as_approval(STRUCTURED)


def test_same_family_approval_does_not_count():
    """Cross-family pairing is the rule: an approval from the author's family is
    a misconfiguration, and the queue refuses to complete on it."""
    same = {
        "verdict": "approve",
        "reviewers": [{"model": "qwen38", "family": "alibaba", "verdict": "approve"}],
        "head_sha": "f" * 40,
    }
    assert not rq.review_counts_as_approval(same, author_family="alibaba")
    cross = {
        "verdict": "approve",
        "reviewers": [{"model": "gemma4-31b", "family": "google", "verdict": "approve"}],
        "head_sha": "f" * 40,
    }
    assert rq.review_counts_as_approval(cross, author_family="alibaba")


# ── (4c) backoff + concurrency caps ─────────────────────────────────────────


def test_exponential_backoff_is_monotonic_and_capped():
    delays = [rq.retry_delay(attempt, base=30.0, cap=900.0, jitter=0.0) for attempt in range(8)]
    assert delays[0] < delays[1] < delays[2] < delays[3]
    assert max(delays) <= 900.0
    assert delays[-1] == 900.0


def test_backoff_jitter_stays_within_bounds():
    for _ in range(50):
        value = rq.retry_delay(2, base=30.0, cap=900.0, jitter=0.2)
        assert 30.0 * 4 * 0.8 <= value <= 30.0 * 4 * 1.2


def test_route_concurrency_cap_is_enforced():
    roster = rr.load_roster()
    route = next(r for r in roster if r.id == "cerberus-gemma4-31b")
    inflight = rq.RouteInflight()
    for _ in range(route.concurrency):
        assert inflight.acquire(route) is True
    assert inflight.acquire(route) is False
    inflight.release(route)
    assert inflight.acquire(route) is True
    # A shared endpoint (reviewer + narrator on CERBERUS) is one budget.
    narrator = next(r for r in roster if r.id == "cerberus-gpt-oss-20b-narrator")
    shared = rq.RouteInflight()
    for _ in range(route.concurrency):
        assert shared.acquire(route, roster)
    assert not shared.acquire(narrator, roster)
    # Alone (no roster to consult) a route only knows its own limit.
    solo = rq.RouteInflight()
    for _ in range(rr.route_capacity(route, roster)):
        assert solo.acquire(route)


# ── (4d) the queue consumer ─────────────────────────────────────────────────


def _row(job_id: str, *, family: str = "alibaba", model: str = "qwen38", attempts: int = 0, ready_at=0.0):
    return {
        "id": job_id,
        "status": "needs-review",
        "result": {
            "awaiting_review": {
                "state": "queued",
                "head": "1" * 40,
                "branch": f"hv/{job_id}",
                "author_model": model,
                "author_family": family,
                "attempts": attempts,
                "next_attempt_at": ready_at,
            }
        },
    }


class _Plan:
    """Fake bus: records submissions, hands back review job ids."""

    def __init__(self, roster, health=None, load=None, now=1000.0):
        self.roster = roster
        self.health = health or {}
        self.load = load or {}
        self.now = now
        self.submitted = []

    def submit(self, row, route, head):
        self.submitted.append((row["id"], route.id, head))
        return f"review-{len(self.submitted)}"


def test_consumer_submits_to_the_next_cross_family_route():
    roster = rr.load_roster()
    plan = _Plan(roster)
    rows = [_row("j1"), _row("j2")]
    out = rq.drain(
        rows,
        roster=roster,
        health=plan.health,
        load=plan.load,
        now=plan.now,
        submit=plan.submit,
        inflight=rq.RouteInflight(),
    )
    assert [s[1] for s in plan.submitted] == ["cerberus-gemma4-31b", "cerberus-gemma4-31b"]
    assert out["submitted"] == 2
    assert all(row["result"]["awaiting_review"]["state"] == "submitted" for row in rows)


def test_consumer_parks_a_row_with_no_cross_family_reviewer():
    roster = rr.load_roster()
    cross = [r for r in rr.reviewer_routes(roster) if r.family != "alibaba"]
    plan = _Plan(roster, health={r.id: "down" for r in cross})
    rows = [_row("j3")]
    out = rq.drain(
        rows,
        roster=roster,
        health=plan.health,
        load=plan.load,
        now=plan.now,
        submit=plan.submit,
        inflight=rq.RouteInflight(),
    )
    assert out["parked"] == 1
    assert rows[0]["result"]["awaiting_review"]["state"] == "parked"
    assert rows[0]["result"]["awaiting_review"]["last_error"] == "no_cross_family_reviewer"
    assert plan.submitted == []
    # Parked by the consumer, the row is still needs-review for the doctor.
    assert rows[0]["status"] == "needs-review"


def test_consumer_respects_the_route_cap_across_rows():
    roster = rr.load_roster()
    gemma = next(r for r in roster if r.id == "cerberus-gemma4-31b")
    plan = _Plan(roster)
    rows = [_row(f"k{i}") for i in range(gemma.concurrency + 3)]
    out = rq.drain(
        rows,
        roster=roster,
        health=plan.health,
        load=plan.load,
        now=plan.now,
        submit=plan.submit,
        inflight=rq.RouteInflight(),
    )
    # The shared CERBERUS endpoint takes at most gemma's budget, whatever the
    # row count; the rest spill to other cross-family routes (or wait).
    cap = rr.route_capacity(gemma, roster)
    cerberus = [s for s in plan.submitted if s[1] == "cerberus-gemma4-31b"]
    assert len(cerberus) == min(gemma.concurrency, cap)
    assert out["submitted"] >= gemma.concurrency
    assert out["submitted"] + out["deferred"] + out["parked"] == len(rows)
    assert len({s[1] for s in plan.submitted}) > 1


def test_backoff_defers_rows_that_are_not_due():
    roster = rr.load_roster()
    plan = _Plan(roster, now=1000.0)
    rows = [_row("later", attempts=3, ready_at=5000.0)]
    out = rq.drain(
        rows,
        roster=roster,
        health=plan.health,
        load=plan.load,
        now=plan.now,
        submit=plan.submit,
        inflight=rq.RouteInflight(),
    )
    assert out["deferred"] == 1
    assert plan.submitted == []
    assert rows[0]["result"]["awaiting_review"]["state"] == "queued"


def test_a_failed_attempt_schedules_the_next_try_with_backoff():
    row = _row("retry")
    entry = rq.mark_attempt_failed(row, "busy", now=1000.0, base=30.0, cap=900.0, jitter=0.0)
    assert entry["attempts"] == 1
    assert entry["last_error"] == "busy"
    assert entry["state"] == "queued"
    assert entry["next_attempt_at"] == 1030.0
    entry = rq.mark_attempt_failed(row, "busy", now=2000.0, base=30.0, cap=900.0, jitter=0.0)
    assert entry["attempts"] == 2
    assert entry["next_attempt_at"] == 2060.0


def test_exhausted_retries_leave_the_row_for_a_human():
    row = _row("tired", attempts=rq.DEFAULT_MAX_ATTEMPTS)
    entry = rq.mark_attempt_failed(row, "busy", now=1000.0, max_attempts=rq.DEFAULT_MAX_ATTEMPTS)
    assert entry["state"] == "exhausted"


def test_structured_approval_completes_the_queue_entry():
    row = _row("approved")
    entry = rq.apply_verdict(row, STRUCTURED, author_family="alibaba")
    assert entry["state"] == "approved"
    assert entry["reviewer_model"] == "gemma4-31b"


def test_prose_verdict_never_completes_and_schedules_a_retry():
    row = _row("prose")
    verdict, reason = rq.parse_structured_verdict("Everything looks fine, ship it")
    assert verdict is None
    entry = rq.apply_verdict(row, "Everything looks fine, ship it", author_family="alibaba", now=1000.0, jitter=0.0)
    assert entry["state"] == "queued"
    assert entry["attempts"] == 1
    assert reason == "prose_only"


# ── (5) load awareness ──────────────────────────────────────────────────────


class _Resp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status = status

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload

    @property
    def text(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload if isinstance(self._payload, str) else json.dumps(self._payload)


#: Sentinel for "this endpoint answers, but not with JSON" (503 / HTML).
_DOWN = object()


class _Down:
    """A response whose body is not JSON (a 503 HTML page, a plain-text error)."""

    status = 503

    def json(self):
        raise ValueError("not json")

    @property
    def text(self):
        return "<html><body>503 Service Unavailable</body></html>"


class _FakeHTTP:
    def __init__(self, routes, strict=False):
        self.routes = routes
        self.calls = []
        self.strict = strict

    def __call__(self, url, timeout):
        self.calls.append(url)
        for prefix, payload in self.routes.items():
            if url.startswith(prefix):
                if payload is _DOWN:
                    return _Down()
                return _Resp(payload)
        if self.strict:
            raise OSError(f"unstubbed URL {url}")
        # A route with nothing stubbed answers, but not with JSON.
        return _Down()


def test_llama_server_slots_are_counted_as_load():
    route = next(r for r in rr.load_roster() if r.id == "cerberus-gpt-oss-20b-reviewer")
    http = _FakeHTTP(
        {
            "http://192.0.2.96:8080/slots": [
                {"id": 0, "is_processing": True},
                {"id": 1, "is_processing": False},
                {"id": 2, "state": "processing"},
                {"id": 3, "is_processing": False},
            ]
        }
    )
    probe = rq.LoadProbe(http=http, clock=lambda: 0.0)
    assert probe.load(route) == 2
    assert probe.route_kind(route) == "llama_slots"


def test_vllm_metrics_fall_back_to_running_requests():
    route = rr.Route(
        id="tydeus-vllm",
        provider="local",
        model="qwen38",
        family="alibaba",
        location="local",
        concurrency=4,
        cost_class="A",
        base_url="http://192.0.2.73:8000/v1",
        served_as="qwen38",
        flags=(),
    )
    body = 'vllm:num_requests_running{model="qwen38"} 3.0\nvllm:num_requests_waiting 0.0\n'
    http = _FakeHTTP(
        {
            f"{route.base_url.removesuffix('/v1')}/metrics": body,
            f"{route.base_url.removesuffix('/v1')}/slots": _DOWN,
        }
    )
    probe = rq.LoadProbe(http=http, clock=lambda: 0.0)
    assert probe.load(route) == 3
    assert probe.route_kind(route) == "vllm_metrics"


def test_load_probe_is_cached_for_thirty_seconds():
    route = next(r for r in rr.load_roster() if r.id == "cerberus-gpt-oss-20b-reviewer")
    now = [0.0]
    endpoint = route.base_url.removesuffix("/v1")
    http = _FakeHTTP({f"{endpoint}/slots": [{"is_processing": True}, {"is_processing": False}]})
    probe = rq.LoadProbe(http=http, clock=lambda: now[0])
    assert probe.load(route) == 1
    for when in (1.0, 2.0, 29.0):
        now[0] = when
        assert probe.load(route) == 1
    # Strictly after 30 s (not at the boundary) the entry is stale and the
    # probe asks again; a slot freed in between must be visible.
    now[0] = 30.0
    assert len(http.calls) == 1
    now[0] = 30.001
    assert probe.load(route) == 1
    assert len(http.calls) == 2


def test_slot_freed_after_the_cache_window_is_seen():
    route = next(r for r in rr.load_roster() if r.id == "cerberus-gpt-oss-20b-reviewer")
    now = [0.0]
    endpoint = route.base_url.removesuffix("/v1")
    http = _FakeHTTP({f"{endpoint}/slots": [{"is_processing": True}, {"is_processing": True}]})
    probe = rq.LoadProbe(http=http, clock=lambda: now[0])
    assert probe.load(route) == 2
    http.routes[f"{endpoint}/slots"] = [{"is_processing": True}, {"is_processing": False}]
    now[0] = 30.0
    assert probe.load(route) == 2  # still cached at the boundary
    now[0] = 31.0
    assert probe.load(route) == 1  # the freed slot is visible again


def test_probe_failure_is_treated_as_unknown_not_as_zero_load():
    route = next(r for r in rr.load_roster() if r.id == "cerberus-gpt-oss-20b-reviewer")
    probe = rq.LoadProbe(http=_FakeHTTP({}), clock=lambda: 0.0)
    assert probe.load(route) is None


# ── (6) doctor integration ──────────────────────────────────────────────────


def _doctor():
    from hive.workers import doctor

    return doctor


def test_doctor_counts_awaiting_review_rows_as_queued_work():
    doctor = _doctor()
    rows = [
        _row("q1"),
        {**_row("q2"), "result": {"awaiting_review": {"state": "parked", "head": "1" * 40}}},
        {"id": "plain", "status": "needs-review", "result": {}},
    ]
    counts = doctor.needs_review_queue_counts(rows)
    assert counts["queued"] == 1
    assert counts["parked"] == 1
    assert counts["unreviewed"] == 1
    assert counts["total"] == 3


def test_doctor_does_not_call_an_awaiting_review_row_stale():
    doctor = _doctor()
    row = _row("waiting")
    row["kind"] = "code"
    assert doctor.review_followup_blocker(row) is not None
    # The blocker names the queue, and specifically NOT "no review was submitted".
    blocker = doctor.review_followup_blocker(row)
    assert "queue" in blocker


def test_doctor_never_escalates_a_queued_row_as_stuck():
    doctor = _doctor()
    row = _row("escalate")
    row.update({"kind": "code", "updated_at": 0.0})
    sent = doctor.escalate_stuck_needs_review([row], evidence_fn=lambda job: {"head_branch": "hv/x"}, now=10**7)
    assert sent == []
