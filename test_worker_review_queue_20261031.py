"""Worker side of the review queue (operator rule 2026-10-31).

A job whose author work is done but whose reviewer could not answer -- all 8
slots of the CERBERUS gpt-oss-20b endpoint busy, a timeout, prose-only or empty
output, truncation, 503 -- used to end ``reviewer_unavailable`` and FAIL. The
worker now delivers the branch and leaves the row ``needs-review`` with
``result.review_state.state='awaiting_review'`` so the doctor's needs-review
sweep can pair it with a CROSS-FAMILY reviewer later.

These tests drive the worker's own seams (the deployment is absent here, so
nothing leaves the machine): the reviewer pick from the roster, the reviewer
failure classification, and the disposition the finalizer reports.
"""

import importlib.util
import sys
import types

import pytest

from hive.review_queue import FAMILY_TABLE_PATH, ROSTER_PATH, backoff_seconds, family_of


@pytest.fixture
def worker(monkeypatch):
    """The tracked worker module, with its deployed sidecars stubbed.

    ``hive/codex_delivery.py``, ``hive/llm_roles.py``, ``hive/push_policy.py``,
    ``hive/review_evidence.py``, ``hive_budget.py`` and ``provenance.py`` ship
    with the deployed runtime and are absent from this checkout; only those
    import-time dependencies are stubbed (never the review-queue code).
    """
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
        if name == "llm_roles":
            module.is_role_id = lambda value: False
            module.family = lambda value: None
            module.backing_model = lambda value: None
        if name == "push_policy":
            module.argonas_url_for_secret = lambda *a, **kw: None
            module.argonas_url_for_slug = lambda slug: slug
        monkeypatch.setitem(sys.modules, "hive." + name, module)
    root = __import__("pathlib").Path(__file__).resolve().parent
    spec = importlib.util.spec_from_file_location("review_queue_worker", root / "deploy" / "hive_worker.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_family_table_and_roster_are_reachable_from_the_worker_line():
    """The worker shares ONE data file with the doctor: no second copy."""
    assert FAMILY_TABLE_PATH.is_file() and ROSTER_PATH.is_file()
    assert family_of("gpt-oss-20b") == "openai"


def test_worker_reviewer_pick_is_cross_family(worker):
    picked = worker.review_queue_reviewers("qwen38")
    assert picked, "the qwen38 author has cross-family reviewers"
    assert all(family_of(route["model_id"]) != "alibaba" for route in picked)
    # The busy CERBERUS endpoint is skippable: a saturated route drops out.
    cerberus = [r for r in worker.review_queue_roster() if r["model_id"] == "gpt-oss-20b"]
    assert cerberus
    saturated = worker.review_queue_reviewers("qwen38", load=lambda route: route["concurrency"])
    assert all(r["model_id"] != "gpt-oss-20b" for r in saturated)


def test_worker_pick_never_self_reviews_and_can_be_empty(worker, monkeypatch):
    only_self = [r for r in worker.review_queue_roster() if r["family"] == family_of("qwen38")]
    monkeypatch.setattr(worker, "review_queue_roster", lambda: only_self)
    assert worker.review_queue_reviewers("qwen38") == []


@pytest.mark.parametrize(
    ("evidence", "author", "expected"),
    [
        ({"verdict": "reviewer_unavailable", "error": "no review chunks were generated"}, "qwen38", "review_busy"),
        ({"error": "POST :8080/v1/chat/completions answered 503"}, "qwen38", "review_http_503"),
        ({"error": "review timed out after 900s"}, "qwen38", "review_timeout"),
        ({"reviewers": [{"model": "gemma4-31b", "verdict": "approve"}], "head_sha": "a" * 40}, "qwen38", None),
        (None, "qwen38", "review_busy"),
    ],
)
def test_reviewer_failure_kind_is_structured(worker, evidence, author, expected):
    assert worker.reviewer_failure_kind(evidence, author_model=author, head_sha="a" * 40) == expected


def test_awaiting_review_state_names_author_family_and_tried_reviewers(worker):
    state = worker.awaiting_review_state(
        "qwen38",
        [{"model_id": "gpt-oss-20b", "failure": "review_busy"}],
        attempt=1,
        next_attempt_at=1234.0,
        now=1000.0,
    )
    assert state["state"] == "awaiting_review"
    assert state["author"] == "qwen38" and state["author_family"] == "alibaba"
    assert state["tried"][0]["model_id"] == "gpt-oss-20b"
    assert state["attempt"] == 1 and state["next_attempt_at"] == 1234.0


def test_reviewer_outage_disposition_is_needs_review_not_failed(worker):
    status, reason = worker.awaiting_review_disposition(
        loop_teardown="reviewer_unavailable", exit_code=1, ready=True, cancelled=False
    )
    assert (status, reason) == ("needs-review", "reviewer_unavailable")
    # A cancel or an author failure keeps its own disposition: no queue.
    assert worker.awaiting_review_disposition(
        loop_teardown="reviewer_unavailable", exit_code=1, ready=True, cancelled=True
    )[0] == "failed"
    assert worker.awaiting_review_disposition(
        loop_teardown="author_failed", exit_code=1, ready=True, cancelled=False
    )[0] == "failed"


def test_resume_delay_doubles_and_is_bounded(worker):
    from hive import review_queue

    assert worker.review_resume_delay(0) == backoff_seconds(0)
    assert worker.review_resume_delay(1) == 2 * backoff_seconds(0)
    assert worker.review_resume_delay(99) == review_queue.MAX_BACKOFF_SEC


def test_no_stray_splice_scaffolding_with_unresolved_names():
    """No ``.rq_*.py`` splice fragment may survive in the repo root.

    The worker seams were developed as root-level ``.rq_*`` fragments that a
    splice step pasted into ``deploy/hive_worker.py``. ``99d56c4`` removed
    ``.rq_insert.py`` and ``.rq_section.py`` but left
    ``.rq_worker_section.py`` behind: a dead, unpackaged copy of the worker
    seams that uses ``review_queue`` without importing it (ruff F821) and that
    had drifted from the live code (it classified the CERBERUS-busy ``no review
    chunks`` signature as ``review_empty`` instead of ``review_busy``). A file
    nothing imports cannot regress a run, but an unimported duplicate of live
    logic is a trap for the next reader, so its absence is asserted here.
    """
    from pathlib import Path

    root = Path(__file__).resolve().parent
    strays = sorted(p.name for p in root.glob(".rq_*.py"))
    assert strays == [], f"stray review-queue splice scaffolding: {strays}"


def test_live_worker_classifies_busy_before_empty(worker):
    """The CERBERUS-busy signature is a retryable BUSY route, not an empty review.

    ``no review chunks`` means the loop never got a slot at all (the 8-slot
    gpt-oss-20b endpoint), so it must classify ``review_busy``: the row stays
    awaiting_review and retries rather than being parked as an empty review.
    """
    kind = worker.reviewer_failure_kind({"error": "all healthy endpoints failed: no review chunks"}, head_sha="a" * 40)
    assert kind == "review_busy"
