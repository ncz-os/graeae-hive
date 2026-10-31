"""Reviewer roster + model-family table (operator rule 2026-10-31).

The operator rule is two sentences: *always have an alternate review protocol or
a review queue*, and *always pair reviewer and author from different model
families* — with a roster that lists EVERY local and provider model.

These tests pin the three artefacts that make the rule enforceable:

1. the FAMILY table (``hive/workers/model_families.json``): one source of
   truth, one family per roster model, and the operator's explicit mapping —
   qwen38/Qwen* = alibaba, gemma* = google, gpt-oss*/gpt-6*/codex = openai,
   nemotron* = nvidia, MiniMax-M3/M3.1 = minimax, deepseek* = deepseek,
   muse-spark* = meta, mimo* = xiaomi, stepfun* = stepfun, claude* = anthropic,
   and an unknown model is its OWN family (so it never pairs with anything);
2. the ROSTER (``hive/workers/reviewer_roster.json``): every
   reviewer-capable route with provider, model id, family, location,
   concurrency limit, cost class and flags — including the operator's
   exclusions (MiMo is never a reviewer; StepFun and Meta contributor tiers
   only for approved repos; EIH Nemotron is allowed but marked slow);
3. ``pick_reviewers``: an ordered, deterministic list of cross-family
   reviewers; empty (=> the job is PARKED) when no cross-family route is
   healthy, never a self-review.

No network, no bus: the health/load inputs to ``pick_reviewers`` are the
injected mappings.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hive.workers import reviewer_roster as rr

ROOT = Path(__file__).resolve().parent
ROSTER_PATH = ROOT / "hive" / "workers" / "reviewer_roster.json"
FAMILIES_PATH = ROOT / "hive" / "workers" / "model_families.json"

# The operator's family table, written out INDEPENDENTLY of the data file so a
# silent edit to the table fails this test instead of agreeing with itself.
OAUTH_FAMILIES = {
    "qwen38": "alibaba",
    "qwen3-coder": "alibaba",
    "gemma4-31b": "google",
    "gemma4-26b-a4b": "google",
    "gpt-oss-20b": "openai",
    "gpt-oss-120b": "openai",
    "gpt-6": "openai",
    "codex": "openai",
    "nvidia/nemotron-3-ultra-550b-a55b": "nvidia",
    "nemotron35": "nvidia",
    "MiniMax-M3": "minimax",
    "MiniMax-M3.1-Flash-Preview": "minimax",
    "MiniMax-M3.1-Flash": "minimax",
    "deepseek-flash": "deepseek",
    "deepseek-v4-pro": "deepseek",
    "muse-spark-1.3": "meta",
    "mimo-v2.6-flash": "xiaomi",
    "stepfun-3": "stepfun",
    "claude-sonnet-4-7": "anthropic",
}


# ── (1) family table ────────────────────────────────────────────────────────


@pytest.mark.parametrize("model,family", sorted(OAUTH_FAMILIES.items()))
def test_operator_family_mapping(model, family):
    assert rr.family_of(model) == family


def test_unknown_model_is_its_own_family_and_never_pairs():
    fam = rr.family_of("totally-unknown-model-9000")
    assert fam.startswith("unknown:")
    # Its own family: two different unknown models are different families, and
    # an unknown model does not even match itself's route (never pairs).
    assert rr.family_of("another-mystery") != fam
    assert not rr.families_pair("totally-unknown-model-9000", "totally-unknown-model-9000")


def test_every_roster_model_has_exactly_one_family():
    roster = rr.load_roster()
    assert roster, "roster must not be empty"
    families = rr.load_families()
    # One source of truth: every roster entry carries the SAME family the table
    # gives for its model id, and the table names every roster model.
    for route in roster:
        assert route.family == rr.family_of(route.model), route.id
        assert route.model not in families or families[route.model] == route.family
    table = json.loads(FAMILIES_PATH.read_text())
    for model, family in table["families"].items():
        if model.startswith("*"):
            continue
        assert isinstance(family, str) and family and not family.startswith("unknown:")
    # And the roster agrees with the table for every enumerated exact model.
    for model in (r.model for r in roster):
        assert model in table["families"] or any(
            model.lower().startswith(p.rstrip("*").lower()) for p in table["families"] if p.endswith("*")
        ), f"{model} has no family rule"


def test_family_table_is_free_of_duplicate_patterns():
    table = json.loads(FAMILIES_PATH.read_text())["families"]
    lowered = [k.lower() for k in table]
    assert len(lowered) == len(set(lowered)), "a pattern is listed twice"
    table_by_lower = {k.lower(): (k, v) for k, v in table.items()}
    for pattern in table:
        if not pattern.endswith("*"):
            continue
        pattern_family = table_by_lower[pattern.lower()][1]
        stem = pattern[:-1].lower()
        # Only ENUMERATED model ids that the wildcard covers are a duplicate.
        # Wildcards sharing a prefix ("mimo*" and "mimo-v2*") are fine as long
        # as they agree on the family. Look the literals up case-insensitively.
        clashes = [k for k in table_by_lower if k != pattern.lower() and k.startswith(stem) and "*" not in k]
        # "mimo*" naturally covers "mimo-v2.6-flash"; what must not happen is
        # two DIFFERENT wildcard stems covering the same explicit model.
        if clashes:
            assert ", ".join(sorted({table_by_lower[c][1] for c in clashes})) == pattern_family or all(
                table_by_lower[c][1] == pattern_family for c in clashes
            ), f"{pattern} disagrees with explicit {clashes}"


# ── (2) reviewer roster ─────────────────────────────────────────────────────


REQUIRED_ROUTE_IDS = {
    "tydeus-qwen38",
    "cerberus-gemma4-31b",
    "cerberus-gpt-oss-20b-reviewer",
    "cerberus-gpt-oss-20b-narrator",
    "provider-minimax-m3",
    "provider-minimax-m3-1-flash",
    "provider-deepseek-flash",
    "provider-muse-spark-1-3",
    "eih-nemotron",
}


def test_roster_lists_every_reviewer_capable_route():
    ids = {r.id for r in rr.load_roster()}
    missing = REQUIRED_ROUTE_IDS - ids
    assert not missing, f"roster is missing {sorted(missing)}"


def test_roster_route_fields_are_complete_and_real():
    seen = set()
    for route in rr.load_roster():
        assert route.id not in seen
        seen.add(route.id)
        assert route.provider, route.id
        assert route.model, route.id
        assert route.family == rr.family_of(route.model), route.id
        assert route.location in {"local", "provider", "eih"}, route.id
        assert isinstance(route.concurrency, int) and route.concurrency >= 1, route.id
        assert route.cost_class in {"A", "B", "C"}, route.id
        assert route.base_url.startswith(("http://", "https://")), route.id
    # The fleet's real routes (addresses are data, not source constants).
    by_id = {r.id: r for r in rr.load_roster()}
    assert by_id["tydeus-qwen38"].base_url == "http://192.0.2.73:8006/v1"
    for rid in ("cerberus-gemma4-31b", "cerberus-gpt-oss-20b-reviewer", "cerberus-gpt-oss-20b-narrator"):
        assert by_id[rid].base_url == "http://192.0.2.96:8080/v1"
    assert by_id["eih-nemotron"].model == "nvidia/nemotron-3-ultra-550b-a55b"
    assert by_id["provider-deepseek-flash"].model == "deepseek-flash"
    assert by_id["provider-minimax-m3"].model == "MiniMax-M3"
    assert by_id["provider-minimax-m3-1-flash"].model == "MiniMax-M3.1-Flash-Preview"


def test_cerberus_reviewer_and_narrator_share_the_endpoint_but_are_distinct_routes():
    by_id = {r.id: r for r in rr.load_roster()}
    a = by_id["cerberus-gpt-oss-20b-reviewer"]
    b = by_id["cerberus-gpt-oss-20b-narrator"]
    assert a.base_url == b.base_url
    assert a.served_as == "cerberus-reviewer"
    assert b.served_as == "cerberus-narrator"
    # The single effective limit for that endpoint is the reviewer's, not the sum:
    # both slots front the same 8-slot llama-server.
    assert rr.route_capacity(a, {b.id: b}) == a.concurrency


def test_mimo_is_never_a_reviewer():
    roster = rr.load_roster()
    mimo = [r for r in roster if r.family == "xiaomi"]
    assert mimo, "MiMo must still be LISTED (it is a known model), just not reviewer-capable"
    for route in mimo:
        assert route.reviewer is False, route.id
        assert route.id not in {r.id for r in rr.reviewer_routes(roster)}


def test_stepfun_and_meta_require_an_approved_repo():
    roster = rr.reviewer_routes(rr.load_roster())
    for route in roster:
        if route.family in {"stepfun", "meta"}:
            assert route.approved_repos, route.id
            for repo in ("graeae-hive", "fleet-ops", "tokenomics", "ncz-screensavers", "zoder"):
                assert repo in route.approved_repos, (route.id, repo)
    meta = next(r for r in rr.reviewer_routes(rr.load_roster()) if r.family == "meta")
    step = next(r for r in rr.reviewer_routes(rr.load_roster()) if r.family == "stepfun")
    assert rr.route_allowed_for_repo(meta, "graeae-hive")
    assert not rr.route_allowed_for_repo(meta, "some-other-repo")
    assert rr.route_allowed_for_repo(step, "fleet-ops")
    assert not rr.route_allowed_for_repo(step, None)


def test_eih_nemotron_is_allowed_but_marked_slow_and_lowest_priority():
    nemo = next(r for r in rr.load_roster() if r.id == "eih-nemotron")
    assert nemo.reviewer is True
    assert nemo.slow is True
    # Measured: 39 s for a one-word reply.
    assert nemo.latency_ms >= 39_000
    assert nemo.priority_rank > 0


# ── (3) pick_reviewers ──────────────────────────────────────────────────────

AUTHOR_FAMILIES = sorted(
    {r.family for r in rr.reviewer_routes(rr.load_roster())} | {"alibaba", "anthropic", "unknown:zzz"}
)


@pytest.mark.parametrize("author_family", AUTHOR_FAMILIES)
def test_no_same_family_pair_is_ever_returned(author_family):
    picked = rr.pick_reviewers("author-model", roster=rr.load_roster(), health={}, load={}, author_family=author_family)
    assert picked, f"the shipped roster must offer a cross-family reviewer for {author_family}"
    for route in picked:
        assert route.family != author_family, (author_family, route.id)


def test_pick_reviewers_is_deterministic_and_ordered():
    roster = rr.load_roster()
    # Non-approved repo: the contributor tiers drop out, so the ordering below
    # is the plain local -> provider -> eih ladder.
    first = rr.pick_reviewers("qwen38", roster=roster, health={}, load={}, repo="some-other-repo")
    second = rr.pick_reviewers("qwen38", roster=roster, health={}, load={}, repo="some-other-repo")
    assert [r.id for r in first] == [r.id for r in second]
    # The local route comes first (cheapest class + measured fast); EIH Nemotron
    # (slow) is last.
    ids = [r.id for r in first]
    assert ids[0] == "cerberus-gemma4-31b"
    assert ids[-1] == "eih-nemotron"


def test_pick_reviewers_skips_unhealthy_and_saturated_routes():
    roster = rr.load_roster()
    health = {"cerberus-gemma4-31b": "down"}
    load = {"cerberus-gpt-oss-20b-reviewer": 8}  # all 8 slots busy
    picked = rr.pick_reviewers("qwen38", roster=roster, health=health, load=load)
    ids = {r.id for r in picked}
    assert "cerberus-gemma4-31b" not in ids
    assert "cerberus-gpt-oss-20b-reviewer" not in ids
    # The narrator slot shares that saturated endpoint, so it is skipped too.
    assert "cerberus-gpt-oss-20b-narrator" not in ids
    assert picked, "provider routes remain"


def test_pick_reviewers_returns_empty_rather_than_self_review():
    roster = rr.load_roster()
    cross = [r for r in rr.reviewer_routes(roster) if r.family != "google"]
    health = {r.id: "down" for r in cross}
    assert rr.pick_reviewers("gemma4-31b", roster=roster, health=health, load={}) == []
    # And an author whose model id IS the only healthy route is never its own reviewer.
    only = rr.load_roster()[0]
    assert rr.pick_reviewers(only.model, roster=[only], health={}, load={}) == []


def test_pick_reviewers_honours_co_author_families():
    roster = rr.load_roster()
    picked = rr.pick_reviewers(
        "qwen38",
        roster=roster,
        health={},
        load={},
        co_author_models=["gemma4-31b"],
    )
    assert "google" not in {r.family for r in picked}


def test_pick_reviewers_honours_approved_repo_flags():
    roster = rr.load_roster()
    picked = rr.pick_reviewers(
        "qwen38", roster=roster, health={}, load={}, repo="some-other-repo", include_expensive=True
    )
    assert "meta" not in {r.family for r in picked}
    assert "stepfun" not in {r.family for r in picked}
    # Approved repo + include_expensive may surface them.
    wide = rr.pick_reviewers("qwen38", roster=roster, health={}, load={}, repo="graeae-hive", include_expensive=True)
    assert "meta" in {r.family for r in wide}


def test_pick_reviewers_bulk_mode_de_prioritises_slow_routes():
    roster = rr.load_roster()
    bulk = rr.pick_reviewers("qwen38", roster=roster, health={}, load={}, bulk=True)
    assert [r.id for r in bulk][-1] == "eih-nemotron"
