"""Codex in the review roster (operator rule 2026-10-31).

*"codex needs to be part of our review system."* Codex is a reviewer ROUTE in
the roster: family **openai**, the same family as gpt-oss, so an OpenAI author
(``gpt-oss-20b``, ``gpt-6-*``, ``codex``) is NEVER reviewed by Codex or
gpt-oss, and Codex-authored work still gets a NON-openai structured review.

The fleet rules the roster data and the code encode:

1. Codex runs ONLY through the confined local Hive Codex worker (``kind:
   codex``, workspace-write limited to the job workspace, the CERBERUS
   ``hive-worker-codex.service``); never via EIH, never via Azure/AWS/GCP/OCI
   or the retired :4100 gateway. PYTHIA holds the master OAuth token: nothing
   here logs in, refreshes or reads it.
2. Model ladder ``gpt-6-luna`` -> ``gpt-6.1-terra`` -> ``gpt-6-sol``.
   ``gpt-6-astra`` only on an explicit operator authorization recorded in the
   job -- never auto-selected, never pinned.
3. Concurrency cap 1 and a quota guard: the roster reads Codex plan usage
   (injected as a fake usage source in these tests) and skips Codex when the
   remaining share is under 30 percent or a cooldown marker exists.
4. Priority: lower than local and DeepSeek/M3 reviewers for BULK review,
   higher for the HARD lane (architecture, or security-sensitive changes:
   security, auth, redaction, budget code).
5. The review job is a normal Hive job of ``kind: codex`` with the SAME
   structured JSON verdict contract as every other reviewer
   (``reviewers[].model`` names the model that ran, e.g. ``gpt-6-luna``);
   prose-only or empty output is never an approval.
6. The doctor never starts a Codex review from a cron or a hook without the
   roster gate, the quota guard and ``HIVE_REVIEW_CODEX_ENABLED`` (default on
   only when the codex worker is online).
"""

import importlib
import json
import os

import pytest

from hive.review_queue import (
    ROSTER_PATH,
    codex_gate,
    codex_job_allowed,
    codex_ladder,
    codex_review_available,
    family_of,
    load_roster,
    pick_reviewers,
)

CODEX_ROUTE_ID = "codex-worker"


def _roster_entry(route_id=CODEX_ROUTE_ID):
    return next((e for e in load_roster() if e["id"] == route_id), None)


def _healthy(entry):
    return True


def _idle(route):
    return 0


# ── 1. the roster entry ──────────────────────────────────────────────────────


def test_codex_is_a_roster_route_of_family_openai():
    entry = _roster_entry()
    assert entry is not None, "Codex must be in hive/core/reviewer-roster.json"
    assert entry["family"] == "openai"
    assert family_of(entry["model_id"]) == "openai"
    # NEVER an EIH route, never a provider API route: the confined local worker.
    # ``worker`` (not ``local``): the route is driven by a worker, so it has no
    # slot endpoint to probe -- calling it ``local`` made the shared load probe
    # read LOAD_UNKNOWN and silently retire the route from every pick.
    assert entry["location"] == "worker"
    assert "port" not in entry
    assert entry["kind"] == "codex"
    assert entry["worker"] == "hive-worker-codex.service"
    assert entry["host"] == "cerberus"
    assert entry["concurrency"] == 1
    assert entry["flags"]["codex"]["sandbox"] == "workspace-write"
    assert entry["flags"]["codex"]["auth_token_holder"] == "pythia"


def test_codex_route_is_not_an_eih_or_cloud_api_route():
    for key in ("base_url",):
        assert not _roster_entry().get(key)
    eih = [e for e in load_roster() if e.get("location") == "eih"]
    assert CODEX_ROUTE_ID not in {e["id"] for e in eih}
    text = json.dumps(_roster_entry()).lower()
    for forbidden in ("azure", "aws", "gcp", "oci", ":4100", "integrate.api.nvidia"):
        assert forbidden not in text, forbidden


def test_codex_ladder_is_data_on_the_route():
    entry = _roster_entry()
    assert codex_ladder(entry) == ["gpt-6-luna", "gpt-6.1-terra", "gpt-6-sol"]
    # astra exists but is NOT on the automatic ladder.
    flags = entry["flags"]["codex"]
    assert flags["authorization_model"] == "gpt-6-astra"
    assert "gpt-6-astra" not in codex_ladder(entry)


def test_family_table_answers_for_every_codex_model():
    for model in ("gpt-6-luna", "gpt-6.1-terra", "gpt-6-sol", "gpt-6-astra", "codex", "codex-worker"):
        assert family_of(model) == "openai", model


# ── 2. pairing: an openai author never gets Codex ───────────────────────────


@pytest.mark.parametrize("author", ["gpt-oss-20b", "gpt-6-luna", "gpt-6.1-terra", "codex", "gpt-6-sol"])
def test_openai_author_never_gets_codex(author):
    picked = pick_reviewers(author, health=_healthy, load=_idle, repo="graeae-hive")
    assert picked, author
    assert all(route["family"] != "openai" for route in picked), author
    assert CODEX_ROUTE_ID not in {route["id"] for route in picked}


def test_codex_authored_work_gets_a_non_openai_review():
    picked = pick_reviewers("gpt-6-luna", health=_healthy, load=_idle, repo="graeae-hive")
    assert picked
    assert all(route["family"] != "openai" for route in picked)


def test_codex_is_never_paired_with_gpt_oss_either_way():
    """gpt-oss is an openai AUTHOR and Codex an openai REVIEWER: never paired."""
    picked = pick_reviewers("gpt-oss-20b", health=_healthy, load=_idle, repo="graeae-hive")
    families = {route["family"] for route in picked}
    assert "openai" not in families
    # And the codex route may not review a gpt-oss row.
    assert CODEX_ROUTE_ID not in {route["id"] for route in picked}


# ── 3. the gate: worker + config switch + quota ─────────────────────────────


def test_codex_disabled_when_the_worker_is_not_online():
    gate = codex_gate(
        entry=_roster_entry(),
        worker_online=False,
        env={"HIVE_REVIEW_CODEX_ENABLED": "1"},
        usage=lambda: {"remaining_pct": 90.0},
        now=1000.0,
    )
    assert gate["allowed"] is False
    assert gate["reason"] == "codex_worker_offline"


def test_codex_config_switch_defaults_on_when_worker_online():
    gate = codex_gate(
        entry=_roster_entry(), worker_online=True, env={}, usage=lambda: {"remaining_pct": 90.0}, now=1000.0
    )
    assert gate["allowed"] is True, gate


def test_codex_config_switch_off_refuses_even_when_online():
    for value in ("0", "false", "no", "off"):
        gate = codex_gate(
            entry=_roster_entry(),
            worker_online=True,
            env={"HIVE_REVIEW_CODEX_ENABLED": value},
            usage=lambda: {"remaining_pct": 90.0},
            now=1000.0,
        )
        assert gate["allowed"] is False, value
        assert gate["reason"] == "codex_disabled"


def test_codex_is_skipped_below_the_usage_ceiling():
    gate = codex_gate(
        entry=_roster_entry(),
        worker_online=True,
        env={},
        usage=lambda: {"remaining_pct": 29.0},
        now=1000.0,
    )
    assert gate["allowed"] is False
    assert gate["reason"] == "codex_quota_low"
    # At or above 30 percent it is allowed again.
    gate = codex_gate(
        entry=_roster_entry(), worker_online=True, env={}, usage=lambda: {"remaining_pct": 30.0}, now=1000.0
    )
    assert gate["allowed"] is True, gate


def test_codex_is_skipped_while_a_cooldown_marker_is_live(tmp_path, monkeypatch):
    monkeypatch.setenv("HIVE_WORKER_STATE_DIR", str(tmp_path))
    from hive.review_queue import write_codex_cooldown

    write_codex_cooldown(reason="usage limit reached", now=1000.0, ttl_secs=3600.0)
    gate = codex_gate(
        entry=_roster_entry(), worker_online=True, env={}, usage=lambda: {"remaining_pct": 90.0}, now=1000.0
    )
    assert gate["allowed"] is False
    assert gate["reason"] == "codex_cooldown"
    # Once the marker expires the route is usable again.
    gate = codex_gate(
        entry=_roster_entry(), worker_online=True, env={}, usage=lambda: {"remaining_pct": 90.0}, now=1000.0 + 3601
    )
    assert gate["allowed"] is True, gate


def test_codex_is_skipped_when_usage_cannot_be_read():
    """An unknown remaining share fails CLOSED -- never guess headroom."""
    gate = codex_gate(entry=_roster_entry(), worker_online=True, env={}, usage=lambda: None, now=1000.0)
    assert gate["allowed"] is False
    assert gate["reason"] == "codex_usage_unknown"


def test_codex_concurrency_cap_is_one():
    entry = _roster_entry()
    healthy = lambda route: route["id"] != CODEX_ROUTE_ID  # noqa: E731
    occupancy = {CODEX_ROUTE_ID: 1}

    def load(route):
        return occupancy.get(route["id"], 0)

    picked = pick_reviewers(
        "qwen38",
        health=healthy,
        load=load,
        repo="graeae-hive",
        codex_worker_online=True,
        codex_usage=lambda: {"remaining_pct": 90.0},
    )
    assert CODEX_ROUTE_ID not in {route["id"] for route in picked}
    assert entry["concurrency"] == 1


# ── 4. priority: bulk vs hard lane ──────────────────────────────────────────


def test_codex_is_not_the_bulk_reviewer():
    picked = pick_reviewers(
        "qwen38",
        health=_healthy,
        load=_idle,
        repo="graeae-hive",
        codex_worker_online=True,
        codex_usage=lambda: {"remaining_pct": 90.0},
    )
    ids = [route["id"] for route in picked]
    assert CODEX_ROUTE_ID in ids, "the usable Codex route is still an option"
    assert ids.index(CODEX_ROUTE_ID) > 0
    # Local and DeepSeek/M3 reviewers come first for bulk review.
    first = picked[0]
    assert first["family"] in {"alibaba", "google", "minimax", "deepseek"} or first["location"] == "local"


def test_codex_leads_the_hard_lane():
    picked = pick_reviewers(
        "qwen38",
        health=_healthy,
        load=_idle,
        repo="graeae-hive",
        lane="hard",
        codex_worker_online=True,
        codex_usage=lambda: {"remaining_pct": 90.0},
    )
    assert picked, "a hard-lane pick exists"
    assert picked[0]["id"] == CODEX_ROUTE_ID


def test_hard_lane_detects_security_sensitive_changes():
    from hive.review_queue import review_lane

    assert review_lane(repo="graeae-hive", paths=["hive/bus/auth.py"]) == "hard"
    assert review_lane(repo="graeae-hive", paths=["hive/core/secrets.py"]) == "hard"
    assert review_lane(repo="graeae-hive", paths=["hive/redaction.py"]) == "hard"
    assert review_lane(repo="graeae-hive", paths=["hive/budget.py"]) == "hard"
    assert review_lane(repo="graeae-hive", paths=["docs/ARCHITECTURE.md"]) == "hard"
    assert review_lane(repo="graeae-hive", paths=["hive/review_queue.py"]) == "bulk"


def test_hard_lane_still_never_self_reviews():
    picked = pick_reviewers(
        "gpt-6-luna",
        health=_healthy,
        load=_idle,
        repo="graeae-hive",
        lane="hard",
        codex_worker_online=True,
        codex_usage=lambda: {"remaining_pct": 90.0},
    )
    assert picked
    assert picked[0]["id"] != CODEX_ROUTE_ID


# ── 5. the review job is a kind:codex job with the common verdict contract ──


@pytest.fixture
def bus(monkeypatch, tmp_path):
    from hive.review_queue import make_review_state
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
        if method == "GET" and "/v1/agents" in path:
            return 200, {"agents": [{"kind": "codex", "status": "online", "host": "cerberus"}]}
        if method == "GET" and path.startswith("/v1/jobs?"):
            return 200, {"jobs": list(rows.values()), "total": len(rows)}
        if method == "POST" and path == "/v1/jobs":
            return 201, {"id": "review-child"}
        return 200, {}

    monkeypatch.setattr(doctor, "_http", http)
    return rows, calls, messages, monkeypatch


def _submits(calls):
    return [c[2] for c in calls if c[:2] == ("POST", "/v1/jobs")]


def _codex_only(doctor, monkeypatch):
    """Only the Codex route is healthy: any auto dispatch MUST be Codex-gated."""
    route_ids = {route["id"] for route in load_roster()}
    monkeypatch.setattr(doctor, "REVIEW_QUEUE_ROUTE_HEALTH", {rid: rid == CODEX_ROUTE_ID for rid in route_ids})


def test_codex_review_is_a_normal_codex_kind_job(bus):
    from hive.workers import doctor

    rows, calls, _, monkeypatch = bus
    _codex_only(doctor, monkeypatch)
    monkeypatch.setattr(doctor, "CODEX_REVIEW_USAGE", lambda: {"remaining_pct": 80.0})
    summary = doctor.run_review_queue_sweep(rows=list(rows.values()))
    assert summary["submitted"] == 1, summary
    spec = _submits(calls)[0]
    assert spec["kind"].startswith("review:")
    assert spec["eligible_kinds"] == ["codex"]
    assert spec["preferred_models"] == ["gpt-6-luna"]
    assert spec["max_concurrency"] == 1
    # The same structured verdict contract as every other reviewer.
    assert "reviewers" in spec["description"] and "head_sha" in spec["description"]


def test_doctor_never_auto_dispatches_codex_when_the_switch_is_off(bus):
    from hive.workers import doctor

    rows, calls, _, monkeypatch = bus
    _codex_only(doctor, monkeypatch)
    monkeypatch.setenv("HIVE_REVIEW_CODEX_ENABLED", "0")
    summary = doctor.run_review_queue_sweep(rows=list(rows.values()))
    assert summary["submitted"] == 0
    assert not _submits(calls)
    assert summary["parked"] == 1


def test_doctor_never_auto_dispatches_codex_without_an_online_worker(bus):
    from hive.workers import doctor

    rows, calls, _, monkeypatch = bus
    _codex_only(doctor, monkeypatch)

    def http(method, path, body=None, **kwargs):
        calls.append((method, path, body))
        if method == "GET" and "parent_job_id=" in path:
            return 200, {"jobs": []}
        if method == "GET" and "/v1/agents" in path:
            return 200, {"agents": []}
        if method == "GET" and path.startswith("/v1/jobs?"):
            return 200, {"jobs": list(rows.values()), "total": 1}
        return 200, {}

    monkeypatch.setattr(doctor, "_http", http)
    summary = doctor.run_review_queue_sweep(rows=list(rows.values()))
    assert summary["submitted"] == 0
    assert not _submits(calls)


def test_doctor_never_auto_dispatches_codex_on_a_low_quota(bus):
    from hive.workers import doctor

    rows, calls, _, monkeypatch = bus
    _codex_only(doctor, monkeypatch)
    monkeypatch.setattr(doctor, "CODEX_REVIEW_USAGE", lambda: {"remaining_pct": 12.0})
    summary = doctor.run_review_queue_sweep(rows=list(rows.values()))
    assert summary["submitted"] == 0
    assert not _submits(calls)


def test_a_codex_verdict_names_the_model_and_binds_the_head(bus):
    from hive.workers import doctor

    rows, calls, messages, monkeypatch = bus
    rows["source"]["result"]["review_state"] = {
        **rows["source"]["result"]["review_state"],
        "review_job_id": "review-child",
        "attempt": 1,
    }
    child = {
        "id": "review-child",
        "kind": "review:fix-20261031",
        "status": "done",
        "parent_job_id": "source",
        "result": {"verdict": {"reviewers": [{"model": "gpt-6-luna", "verdict": "approve"}], "head_sha": "a" * 40}},
    }

    def http(method, path, body=None, **kwargs):
        calls.append((method, path, body))
        if method == "GET" and "parent_job_id=" in path:
            return 200, {"jobs": [child]}
        if method == "GET" and path.startswith("/v1/jobs?"):
            return 200, {"jobs": list(rows.values()), "total": 1}
        return 200, {}

    monkeypatch.setattr(doctor, "_http", http)
    summary = doctor.run_review_queue_sweep(rows=list(rows.values()))
    assert summary["approved"] == 1
    assert any(topic == "doctor.review_approved" for topic, _ in messages)


def test_prose_only_codex_output_is_never_an_approval(bus):
    from hive.workers import doctor

    with pytest.raises(ValueError):
        doctor.interpret_review_outcome("Codex looked at it and it seems fine.")
    with pytest.raises(ValueError):
        doctor.interpret_review_outcome("")
    # A JSON verdict that does not name its model is not a verdict either.
    with pytest.raises(ValueError):
        doctor.interpret_review_outcome({"verdict": "approve", "head_sha": "a" * 40}, head_sha="a" * 40)


# ── 6. gpt-6-astra needs explicit operator authorization ────────────────────


def test_astra_is_never_auto_selected_or_pinned():
    assert codex_ladder(_roster_entry()) == ["gpt-6-luna", "gpt-6.1-terra", "gpt-6-sol"]
    assert codex_review_available(_roster_entry(), usage={"remaining_pct": 90.0})["model"] == "gpt-6-luna"


def test_astra_requires_an_operator_authorization_recorded_in_the_job():
    entry = _roster_entry()
    assert codex_job_allowed(entry, "gpt-6-luna", job={}) is True
    assert codex_job_allowed(entry, "gpt-6-sol", job={}) is True
    assert codex_job_allowed(entry, "gpt-6-astra", job={}) is False
    assert codex_job_allowed(entry, "gpt-6-astra", job={"codex_model_authorization": "gpt-6-astra"}) is True
    assert codex_job_allowed(entry, "gpt-6-astra", job={"codex_model_authorization": "gpt-6-luna"}) is False
    # Anything outside the ladder is refused outright.
    assert codex_job_allowed(entry, "gpt-5.3-codex-spark", job={}) is False


# ── 7. no token anywhere ────────────────────────────────────────────────────


def test_nothing_in_the_codex_path_touches_an_oauth_token(tmp_path, monkeypatch):
    """PYTHIA holds the master token: this repo never logs in, refreshes or reads it."""
    monkeypatch.setenv("HIVE_WORKER_STATE_DIR", str(tmp_path))
    from hive import review_queue as rq

    source = importlib.import_module("hive.review_queue").__file__
    with open(source, encoding="utf-8") as fh:
        text = fh.read().lower()
    for forbidden in ("codex login", "auth.json", "refresh_token", "access_token", "chatgpt-oauth", "bearer "):
        assert forbidden not in text, forbidden
    # The gate only reads config, worker presence, usage and the cooldown marker.
    assert rq.CODEX_COOLDOWN_PROVIDER == "codex"
    assert os.path.isdir(tmp_path)


def test_roster_file_has_no_token_fields():
    text = ROSTER_PATH.read_text().lower()
    for forbidden in ('token":', "api_key", "access_token", "refresh_token"):
        assert forbidden not in text, forbidden


# ── 7. the route is REACHABLE through the production seams ──────────────────
#
# Regression (found on review of the first Codex attempt, 2026-10-31): the
# roster row said ``location: local`` with no ``port``, so the SHARED load probe
# tried to read a slot endpoint that does not exist, got LOAD_UNKNOWN back, and
# ``_available`` read that as "saturated". Every codex test injected a fake
# ``load=_idle``, so the suite stayed green while the route could never be
# picked in production. These tests use the real probe and the real pick.


def _real_probe_roster():
    """The roster WITHOUT an injected health/load override."""
    return load_roster()


def test_codex_is_not_a_slot_serving_local_route():
    """A worker-driven route has no HTTP slot endpoint to probe."""
    from hive import review_queue as rq

    entry = _roster_entry()
    assert entry["location"] == "worker", "codex is driven by a worker, not an HTTP server"
    assert "port" not in entry, "there is no port: nothing in codex is reached over HTTP"
    assert rq.is_codex_route(entry)


def test_real_load_probe_reports_codex_as_idle_not_unknown():
    """The production probe must not retire the route it cannot probe."""
    from hive import review_queue as rq

    entry = _roster_entry()
    fetched = []

    def urlopen(url, timeout=0):  # pragma: no cover - must never be called
        fetched.append(url)
        raise AssertionError(f"the codex route must not be probed: {url}")

    assert rq.route_load_fn(urlopen=urlopen, clock=lambda: 1.0)(entry) == 0
    assert fetched == [], "no slot/metrics request is made for the codex route"


def test_codex_is_reachable_with_the_production_load_probe():
    """End to end: the REAL probe + the roster's real flags pick Codex."""
    from hive import review_queue as rq

    picked = pick_reviewers(
        "qwen38",
        roster=_real_probe_roster(),
        health=_healthy,
        load=rq.route_load_fn(urlopen=lambda *a, **k: None, clock=lambda: 1.0),
        repo="graeae-hive",
        codex_worker_online=True,
        codex_usage=lambda: {"remaining_pct": 90.0},
    )
    assert CODEX_ROUTE_ID in [route["id"] for route in picked]


def test_codex_is_not_mistaken_for_an_eih_route():
    """Only a genuine EIH route sorts into the EIH tier."""
    from hive.workers import doctor

    entry = _roster_entry()
    key = doctor._sort_routes_by_family_load.__doc__  # exists: the sort seam
    assert key is not None
    ordered = doctor._sort_routes_by_family_load([entry, _roster_entry("eih-nemotron")], lambda _r: 0)
    assert [route["id"] for route in ordered][-1] == "eih-nemotron"


# ── 8. the gate must not confuse "unknown" with "offline" ───────────────────


def test_an_unreadable_worker_list_fails_closed_as_unknown():
    """The bus being down is not proof that the worker is offline."""
    answer = codex_gate(worker_online=None, env={}, usage=lambda: {"remaining_pct": 90.0})
    assert answer["allowed"] is False
    assert answer["reason"] == "codex_worker_unknown"


def test_an_offline_worker_still_reports_offline():
    answer = codex_gate(worker_online=False, env={}, usage=lambda: {"remaining_pct": 90.0})
    assert answer["allowed"] is False
    assert answer["reason"] == "codex_worker_offline"


def test_pick_reviewers_does_not_read_unknown_worker_state_as_online():
    """No caller may omit the worker view and get Codex anyway."""
    picked = pick_reviewers(
        "qwen38",
        health=_healthy,
        load=_idle,
        repo="graeae-hive",
        codex_usage=lambda: {"remaining_pct": 90.0},
    )
    assert CODEX_ROUTE_ID not in [route["id"] for route in picked]


def test_the_doctor_passes_an_unknown_worker_view_as_none(monkeypatch):
    """``_online_worker_kinds() -> None`` must reach the gate as None."""
    from hive.workers import doctor

    monkeypatch.setattr(doctor, "_online_worker_kinds", lambda: None)
    assert doctor._codex_worker_online() is None
    assert doctor.review_queue_codex_gate()["reason"] == "codex_worker_unknown"


# ── 9. the job names the model it will actually run ─────────────────────────


def test_the_codex_job_names_the_model_that_runs(bus):
    """``preferred_models`` must name the route's model, not an arbitrary one.

    The codex worker fails loudly (``codex_model_mismatch``) when a job names a
    ``gpt-*`` model the host does not serve, so the submitted spec and the
    picked route have to agree: the job asks for the model the roster would run,
    and that model is on the ladder (never the authorization-only rung).
    """
    from hive.workers import doctor

    rows, calls, _, monkeypatch = bus
    _codex_only(doctor, monkeypatch)
    monkeypatch.setattr(doctor, "CODEX_REVIEW_USAGE", lambda: {"remaining_pct": 80.0})
    assert doctor.run_review_queue_sweep(rows=list(rows.values()))["submitted"] == 1
    spec = _submits(calls)[0]
    names = spec["preferred_models"]
    assert names == [codex_review_available()["model"]]
    assert names[0] in codex_ladder()
    assert names[0] != _roster_entry()["flags"]["codex"]["authorization_model"]
    # A normal Hive job: the same read-only review kind, restricted to codex.
    assert spec["kind"].startswith("review:")
    assert spec["eligible_kinds"] == ["codex"]


# ── 10. the quota guard never raises and never fails open ───────────────────


@pytest.mark.parametrize("value", [None, "abc", "", [], {}, object(), float("nan")])
def test_an_unreadable_share_is_unknown_not_a_crash(value):
    """A garbage usage probe must refuse, not raise (the gate runs in the sweep)."""
    answer = codex_gate(worker_online=True, env={}, usage=lambda: {"remaining_pct": value})
    assert answer["allowed"] is False
    assert answer["reason"] == "codex_usage_unknown"


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), "inf"])
def test_an_infinite_share_is_never_an_approval(value):
    """``inf >= 30`` is True: an infinite share must not open the gate."""
    answer = codex_gate(worker_online=True, env={}, usage=lambda: {"remaining_pct": value})
    assert answer["allowed"] is False
    assert answer["reason"] == "codex_usage_unknown"


@pytest.mark.parametrize("value", [True, False])
def test_a_boolean_share_is_not_a_percentage(value):
    """``True`` is an int in Python; it is not a reading of the plan share."""
    answer = codex_gate(worker_online=True, env={}, usage=lambda: {"remaining_pct": value})
    assert answer["allowed"] is False
    assert answer["reason"] == "codex_usage_unknown"


@pytest.mark.parametrize("value", ["45", " 45 ", "45.5"])
def test_a_numeric_string_share_is_read_as_its_number(value):
    """A probe that reports its share as text is read, not discarded."""
    answer = codex_gate(worker_online=True, env={}, usage=lambda: {"remaining_pct": value})
    assert answer["allowed"] is True, answer
    assert answer["reason"] == ""


def test_a_share_exactly_at_the_ceiling_is_allowed():
    """30 % remaining is the boundary: ``< 30`` refuses, 30 does not."""
    answer = codex_gate(worker_online=True, env={}, usage=lambda: {"remaining_pct": 30.0})
    assert answer["allowed"] is True
    just_under = codex_gate(worker_online=True, env={}, usage=lambda: {"remaining_pct": 29.999})
    assert just_under["allowed"] is False
    assert just_under["reason"] == "codex_quota_low"


def test_a_usage_probe_that_raises_is_unknown_not_a_crash():
    def boom():
        raise RuntimeError("usage endpoint down")

    answer = codex_gate(worker_online=True, env={}, usage=boom)
    assert answer["allowed"] is False
    assert answer["reason"] == "codex_usage_unknown"


def test_a_bare_float_probe_is_unknown_not_a_crash():
    """The probe contract is a dict; a bare float is unreadable, not a crash."""
    answer = codex_gate(worker_online=True, env={}, usage=lambda: 90.0)
    assert answer["allowed"] is False
    assert answer["reason"] == "codex_usage_unknown"


# ── 11. the symbols the codex path needs, and a route without flags ─────────


def test_the_codex_path_has_every_symbol_it_references():
    """Pin the names the gate/pick/submit path resolves, so a rename fails here.

    The reviewer of an earlier attempt reported these as missing imports; they
    are definitions in the two modules that own them. This test is what keeps
    that claim false: if any were removed, this fails before the sweep does.
    """
    from pathlib import Path

    from hive import review_queue as rq
    from hive.workers import doctor

    assert rq.Path is Path
    assert callable(rq.load_roster) and rq.load_roster()
    assert rq.CODEX_WORKER_KIND == "codex"
    assert rq.LANE_BULK == "bulk" and rq.LANE_HARD == "hard"
    # The doctor owns the usage probe hook and reads the constants off the module.
    assert hasattr(doctor, "CODEX_REVIEW_USAGE")
    assert doctor.review_queue.CODEX_WORKER_KIND == rq.CODEX_WORKER_KIND
    assert doctor.review_queue.LANE_BULK == rq.LANE_BULK


def test_a_route_without_flags_sorts_by_defaults():
    """``_bulk_key`` sees hand-built routes too: no flags is defaults, not KeyError."""
    from hive import review_queue as rq

    bare = {"id": "bare", "location": "local"}
    assert rq._bulk_key(bare) == (0, 0, "bare")
    assert rq._bulk_key(bare, rq.LANE_HARD) == (0, 0, "bare")
    assert rq._bulk_key({"id": "n", "location": "local", "flags": None}) == (0, 0, "n")


def test_pick_reviewers_orders_a_flagless_route_without_raising():
    from hive import review_queue as rq

    roster = [
        # A hand-built route with NO flags block at all.
        {"id": "bare", "model_id": "qwen38", "family": "alibaba", "location": "local", "concurrency": 1},
        # The real codex row (its flags block carries the ladder the gate reads).
        _roster_entry(),
    ]
    # A deepseek author is outside both families, so neither route is excluded
    # by pairing and the assertion is about ORDER, not filtering.
    picked = rq.pick_reviewers(
        "deepseek-flash",
        roster=roster,
        health=_healthy,
        load=_idle,
        repo="graeae-hive",
        codex_worker_online=True,
        codex_usage=lambda: {"remaining_pct": 90.0},
    )
    # The flagless route sorts on its defaults (priority 0) ahead of Codex (-5).
    assert [route["id"] for route in picked] == ["bare", "codex-worker"]


# ── 12. a malformed roster flag is the default, not a crash ────────────────
#
# ``load_roster`` does not type-check ``flags``: they are operator data. A
# typo (``"priority": "high"``) used to raise ValueError from ``int()`` inside
# the sort, taking the whole sweep down instead of parking the row. A flag the
# roster cannot express now means the default, which is what a MISSING flag
# already meant.


@pytest.mark.parametrize(
    "value,expected", [(None, 0), ("high", 0), ([], 0), ({}, 0), (5, 5), (-3, -3), ("7", 7), (True, 1)]
)
def test_a_flag_that_is_not_a_number_is_the_default(value, expected):
    from hive import review_queue as rq

    assert rq._int_flag(value) == expected


def test_a_malformed_priority_does_not_break_the_bulk_sort():
    from hive import review_queue as rq

    typo = {"id": "typo", "flags": {"priority": "high"}}
    assert rq._bulk_key(typo) == (0, 3, "typo")


def test_a_malformed_priority_does_not_break_the_hard_lane():
    from hive import review_queue as rq

    row = {"id": "hard", "flags": {"hard_lane_priority": 20, "priority": "high"}}
    assert rq._bulk_key(row, rq.LANE_HARD) == (-20, 0, "hard")


def test_a_malformed_priority_does_not_break_pick_reviewers():
    """The pick must order a typo'd route, not raise out of the sweep."""
    from hive import review_queue as rq

    roster = [
        {
            "id": "typo",
            "model_id": "qwen38",
            "family": "alibaba",
            "location": "local",
            "concurrency": 1,
            "flags": {"priority": "high"},
        },
        _roster_entry(),
    ]
    picked = rq.pick_reviewers(
        "deepseek-flash",
        roster=roster,
        health=_healthy,
        load=_idle,
        repo="graeae-hive",
        codex_worker_online=True,
        codex_usage=lambda: {"remaining_pct": 90.0},
    )
    assert [route["id"] for route in picked] == ["typo", "codex-worker"]


def test_a_valid_roster_entry_sorts_exactly_as_before():
    """The hardening must not move a route that was already well-formed."""
    from hive import review_queue as rq

    entry = _roster_entry()
    assert rq._bulk_key(entry) == (5, 3, "codex-worker")
    assert rq._bulk_key(entry, rq.LANE_HARD) == (-20, 5, "codex-worker")
