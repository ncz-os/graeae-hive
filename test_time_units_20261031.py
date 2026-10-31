"""FB43: fake-clock boundary tests for every time window in `hive/`.

The audit these tests pin is `docs/TIME_HANDLING.md`. Each test freezes the one
clock its subject reads and asserts BOTH sides of the boundary — the instant
inside the window and the instant just past it — because "expires after 30
minutes" is only a contract if 29:59.999 still holds and 30:00.001 does not.

Clocks frozen here:

* the bus claim lease (`CLAIM_LEASE_SECONDS`) and the orphan reaper's three
  cutoffs (explicit lease, un-refreshable hard cap, kind-specific staleness);
* the agent heartbeat reaper (stale 90 s, offline 300 s, purge 6 h);
* the doctor's host cooldown (1800 s) and per-kind re-triage cooldown (3600 s),
  driven through a `DurableStateStore` with an injected `time_fn`;
* the needs-review escalation clock (`DOCTOR_NEEDS_REVIEW_ESCALATE_SECONDS`);
* the provider windows: DeepSeek's UTC peak hours and the MiniMax 5-hour quota
  read cache.

Two real defects this audit found are pinned as strict xfails at the bottom, so
a fix flips them to XPASS (a failure) instead of passing silently.
"""

import asyncio
import calendar
import contextlib
import os
import tempfile
import time
import types
from pathlib import Path
from unittest import mock

import httpx
import pytest

import agent_bus
from hive.bus import app as bus_app
from hive.bus import deps
from hive.bus.domain.reaper import select_orphan_claims
from hive.bus.routers import jobs as jobs_router
from hive.review_queue import make_review_state

LEASE = jobs_router.CLAIM_LEASE_SECONDS
STALE_AFTER = bus_app.CLAIM_STALE_AFTER
HARD_MAX_AGE = bus_app.CLAIM_HARD_MAX_AGE
EPS = 0.001


# ── a temporary bus, one event loop ─────────────────────────────────────────


@contextlib.contextmanager
def bus():
    """A throwaway SQLite bus driven through the real ASGI app."""
    with tempfile.TemporaryDirectory() as tmp:
        env = {
            "AGENT_BUS_DB": str(Path(tmp) / "bus.db"),
            "HIVE_BACKEND": "sqlite",
            "MNEMOS_URL": "",
            "MNEMOS_TOKEN": "",
            "HIVE_SHARED_SECRET": "",
            "HIVE_BUS_ADMIN_TOKEN": "",
        }
        with mock.patch.dict(os.environ, env), mock.patch.object(deps, "_HIVE_REPO", None):
            asyncio.run(deps.hive_repo().init())
            yield


def run(scenario):
    async def main():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://test"
        ) as client:
            return await scenario(client)

    return asyncio.run(main())


async def register(client, kind="zeroclaw", capabilities=("code",)):
    response = await client.post(
        "/v1/agents/register",
        json={
            "kind": kind,
            "host": "clock",
            "runtime": kind,
            "capabilities": list(capabilities),
            "cost_tier": "B",
            "auth_method": "api",
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["urn"]


_submitted = 0


async def submit(client, submitter, kind="e2e:clock"):
    global _submitted
    _submitted += 1  # the bus coalesces identical (kind, description) resubmissions
    response = await client.post(
        "/v1/jobs",
        json={"kind": kind, "description": f"clock-{_submitted}", "submitter_urn": submitter, "max_retries": 2},
        headers={"X-Hive-Actor": submitter},
    )
    assert response.status_code in (200, 201), response.text
    return response.json()["id"]


# ── the claim lease is written in epoch SECONDS ─────────────────────────────


def test_claim_lease_is_epoch_seconds_for_exactly_the_constant():
    """A claimed row's timestamps are epoch seconds and its lease is +LEASE."""
    with bus():

        async def scenario(client):
            submitter = await register(client, "opencode", ["code", "orchestrate"])
            worker = await register(client)
            jid = await submit(client, submitter)
            claimed_at_wall = time.time()
            response = await client.post(f"/v1/jobs/next?agent_urn={worker}", headers={"X-Hive-Actor": worker})
            assert response.status_code == 200, response.text

            row = (await client.get(f"/v1/jobs/{jid}")).json()
            claimed_at = row["claimed_at"]
            lease = row["claim_lease_expires_at"]
            # Seconds, not milliseconds or micros: a ms value would be ~1e12.
            assert 1e9 < claimed_at < 1e10, row
            assert abs(claimed_at - claimed_at_wall) < 60, (
                "claim time must be the wall clock, not a monotonic or local clock"
            )
            assert lease - claimed_at == pytest.approx(LEASE), (
                "the lease window is the constant, measured from the claim"
            )
            assert row["last_update_at"] == pytest.approx(claimed_at), row

        run(scenario)


# ── orphan reaper: every cutoff, both sides ─────────────────────────────────


NOW = 1_800_000_000.0


def _candidate(*, lease=None, claimed_at=None, last_update=None, kind="e2e:x"):
    return ("job-1", kind, "worker-1", claimed_at, last_update, lease)


def _select(candidate, now=NOW):
    return select_orphan_claims(
        [candidate],
        now=now,
        stale_after=STALE_AFTER,
        hard_max_age=HARD_MAX_AGE,
        stale_by_kind=bus_app.CLAIM_STALE_AFTER_BY_KIND,
        hard_by_kind=bus_app.CLAIM_HARD_MAX_AGE_BY_KIND,
    )


def test_lease_expiry_boundary_is_inclusive():
    """`lease_expires_at <= now` reaps: the lease is dead AT its expiry instant."""
    fresh = _candidate(lease=NOW + EPS, claimed_at=NOW - 1, last_update=NOW - 1)
    assert _select(fresh) == []
    assert _select(_candidate(lease=NOW, claimed_at=NOW - 1, last_update=NOW - 1)) == [("job-1", "worker-1")]


def test_hard_cap_boundary_is_inclusive_and_unaffected_by_progress():
    """The 4 h cap is measured from claimed_at, so fresh PATCHes do not save a claim."""
    assert _select(_candidate(claimed_at=NOW - HARD_MAX_AGE + EPS, last_update=NOW - 1)) == []
    reaped = _select(_candidate(claimed_at=NOW - HARD_MAX_AGE, last_update=NOW - 1))
    assert reaped == [("job-1", "worker-1")]


def test_staleness_boundary_uses_last_progress_or_the_claim():
    """30 min without progress reaps; 29:59.999 does not; a missing update falls back to claimed_at."""
    assert _select(_candidate(claimed_at=NOW - 60, last_update=NOW - STALE_AFTER + EPS)) == []
    assert _select(_candidate(claimed_at=NOW - 60, last_update=NOW - STALE_AFTER)) == [("job-1", "worker-1")]
    # last_update_at NULL: staleness is measured from the first claim.
    assert _select(_candidate(claimed_at=NOW - STALE_AFTER, last_update=None)) == [("job-1", "worker-1")]
    assert _select(_candidate(claimed_at=NOW - STALE_AFTER + EPS, last_update=None)) == []


def test_kind_overrides_are_the_longer_window_and_only_for_that_kind():
    """`migration` gets 2 h of staleness and 6 h of hard cap; other kinds keep the defaults."""
    slow_migration = _candidate(kind="migration", claimed_at=NOW - 3600, last_update=NOW - 3600)
    assert _select(slow_migration) == [], "an hour without progress is inside a migration's 2 h window"
    stale_migration = _candidate(kind="migration", claimed_at=NOW - 100, last_update=NOW - 7200)
    assert _select(stale_migration) == [("job-1", "worker-1")], "at 2 h without progress the migration is stale too"
    short_kind = _candidate(kind="e2e:x", claimed_at=NOW - 100, last_update=NOW - STALE_AFTER)
    assert _select(short_kind) == [("job-1", "worker-1")], "the same age IS stale for a kind without an override"
    hard_migration = _candidate(kind="migration", claimed_at=NOW - 21600, last_update=NOW - 1)
    assert _select(hard_migration) == [("job-1", "worker-1")], "the 6 h hard cap still applies"
    assert _select(_candidate(kind="migration", claimed_at=NOW - 21600 + EPS, last_update=NOW - 1)) == []


def test_a_corrupt_claim_with_no_claimed_at_is_still_reapable():
    """A row whose claim timestamps are all NULL must not be un-reapable."""
    assert _select(_candidate(claimed_at=None, last_update=None)) == [("job-1", "worker-1")]


# ── agent heartbeat reaper ───────────────────────────────────────────────────


def _reap_agent(scenario):
    with bus():

        async def wrapped(client):
            async with bus_app.connect_db() as db:
                return await scenario(client, db)

        return run(wrapped)


HEARTBEAT = 1_800_000_000.0


async def _pin_heartbeat(db, urn, last_heartbeat):
    await db.execute("UPDATE agents SET last_heartbeat=? WHERE urn=?", (last_heartbeat, urn))
    await db.commit()


async def _agent_status(db, urn):
    async with db.execute("SELECT status FROM agents WHERE urn=?", (urn,)) as cur:
        row = await cur.fetchone()
    return row[0] if row else None


def test_heartbeat_stale_and_offline_boundaries():
    """90 s without a heartbeat is stale; 300 s is offline; one second earlier is neither."""
    assert bus_app.HEARTBEAT_STALE_AFTER == 90.0
    assert bus_app.agent_offline_after() == 300.0

    async def scenario(client, db):
        urn = await register(client)
        await _pin_heartbeat(db, urn, HEARTBEAT)

        await bus_app.reap_agent_heartbeats(db, HEARTBEAT + 90)
        assert await _agent_status(db, urn) == "online", "exactly 90 s is still online (the predicate is strict <)"

        await bus_app.reap_agent_heartbeats(db, HEARTBEAT + 90 + EPS)
        assert await _agent_status(db, urn) == "stale"

        await bus_app.reap_agent_heartbeats(db, HEARTBEAT + 300)
        assert await _agent_status(db, urn) == "stale", "exactly 300 s is not yet offline"

        await bus_app.reap_agent_heartbeats(db, HEARTBEAT + 300 + EPS)
        assert await _agent_status(db, urn) == "offline"

    _reap_agent(scenario)


def test_offline_agent_purge_boundary():
    """An agent offline for the purge window is deleted AT the cutoff, not before."""
    assert bus_app.AGENT_PURGE_AFTER == 21600.0

    async def scenario(client, db):
        urn = await register(client)
        await _pin_heartbeat(db, urn, HEARTBEAT)
        await bus_app.reap_agent_heartbeats(db, HEARTBEAT + 301)
        assert await _agent_status(db, urn) == "offline"

        repo = deps.hive_repo()
        assert await repo.purge_offline_agents(db, cutoff=HEARTBEAT) == 0, "last_heartbeat < cutoff is strict"
        assert await repo.purge_offline_agents(db, cutoff=HEARTBEAT + EPS) == 1
        assert await _agent_status(db, urn) is None

    _reap_agent(scenario)


# ── the doctor's cooldowns ───────────────────────────────────────────────────


class Clock:
    """A settable clock, callable so it can be a store's ``time_fn``."""

    def __init__(self, at=1_800_000_000.0):
        self.at = float(at)

    def __call__(self):
        return self.at

    def advance(self, seconds):
        self.at += seconds
        return self.at


def _doctor_with_clock(monkeypatch, clock):
    """Swap in a memory-backed store whose clock is ours, and return the module."""
    from hive.workers import doctor
    from hive.workers.doctor_state import DurableStateStore, MemoryBackend

    monkeypatch.setattr(doctor, "STATE", DurableStateStore(MemoryBackend(), time_fn=clock))
    return doctor


def test_host_cooldown_boundary(monkeypatch):
    """A host that saw an SSH action is skipped for 1800 s and allowed at 1800 s."""
    clock = Clock()
    doctor = _doctor_with_clock(monkeypatch, clock)
    cooldown = doctor.MIN_HOST_COOLDOWN

    assert doctor.host_in_cooldown("host-a", now=clock.at) == float("inf"), "never touched: not in cooldown"

    doctor.record_ssh_action("host-a")
    assert doctor.host_in_cooldown("host-a", now=clock.at) == 0.0

    clock.advance(cooldown - 1)
    since = doctor.host_in_cooldown("host-a", now=clock.at)
    assert since == pytest.approx(cooldown - 1)
    assert since < cooldown, "one second early is still inside the cooldown"

    clock.advance(1)
    assert doctor.host_in_cooldown("host-a", now=clock.at) == pytest.approx(cooldown)
    assert doctor.host_in_cooldown("host-a", now=clock.at) >= cooldown, "at exactly 1800 s the gate opens"


def test_triage_cooldown_boundary_is_inclusive(monkeypatch):
    """The per-kind re-triage cooldown admits its own expiry instant, per `STATE.seen`."""
    clock = Clock()
    doctor = _doctor_with_clock(monkeypatch, clock)
    ttl = doctor.AUTO_TRIAGE_COOLDOWN

    doctor.note_triage("build")
    key = f"{doctor.KEY_COOLDOWN}:build"
    assert doctor.cooldown_active(key, ttl) is True

    clock.advance(ttl - 1)
    assert doctor.cooldown_active(key, ttl) is True

    clock.advance(1)  # exactly ttl
    assert doctor.cooldown_active(key, ttl) is True, "`seen` treats now - at <= ttl as inside the window"

    clock.advance(1)
    assert doctor.cooldown_active(key, ttl) is False

    assert doctor.cooldown_active("cooldown:never-marked", ttl) is False
    assert doctor.cooldown_active(key, 0) is False, "a zero window admits nothing"


# ── needs-review escalation ─────────────────────────────────────────────────


def _stuck_row(jid="source", *, updated_at, exhausted=True):
    state = make_review_state(
        "qwen38",
        [],
        attempt=doctor_max_attempts() if exhausted else 1,
        now=0,
        parked_reason="attempts_exhausted_owner_wait" if exhausted else None,
    )
    return {
        "id": jid,
        "kind": "fix:thing",
        "status": "needs-review",
        "repo": "graeae-hive",
        "head_branch": f"fix/{jid}",
        "base_branch": "main",
        "updated_at": updated_at,
        "created_at": 0,
        "result": {"head_sha": "0" * 40, "commits": ["0" * 40], "review_state": state},
    }


def doctor_max_attempts():
    from hive import review_queue

    return review_queue.MAX_REVIEW_ATTEMPTS


def test_needs_review_escalation_boundary(monkeypatch):
    """Escalated only when idle for MORE than the threshold, and only once per state."""
    from hive.workers import doctor

    assert doctor.NEEDS_REVIEW_ESCALATE_SECONDS == 1800.0
    clock = Clock()
    _doctor_with_clock(monkeypatch, clock)
    sent = []
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda topic, body: sent.append((topic, body)))

    now = 1_000_000.0
    at_threshold = _stuck_row(updated_at=now - doctor.NEEDS_REVIEW_ESCALATE_SECONDS)
    assert (
        doctor.escalate_stuck_needs_review([at_threshold], evidence_fn=lambda job: {"head_branch": "x"}, now=now) == []
    )

    past_threshold = _stuck_row(updated_at=now - doctor.NEEDS_REVIEW_ESCALATE_SECONDS - 1)
    assert doctor.escalate_stuck_needs_review(
        [past_threshold], evidence_fn=lambda job: {"head_branch": "x"}, now=now
    ) == ["source"]
    assert sent and sent[0][0] == "doctor.needs_review_stuck"

    sent.clear()
    assert (
        doctor.escalate_stuck_needs_review([past_threshold], evidence_fn=lambda job: {"head_branch": "x"}, now=now + 60)
        == []
    ), "the same (classification, evidence, blocker) must not be announced twice"
    assert sent == []


# ── provider windows ────────────────────────────────────────────────────────


def _worker(monkeypatch):
    """Load `deploy/hive_worker.py` with its absent sidecars stubbed (as FB42 does)."""
    import importlib.util
    import sys

    for name, attrs in {
        "hive_budget": {"FAILURE_REASON": "budget_exceeded"},
        "provenance": {"STATIC_AGENT_MODELS": {}, "KNOWN_COST_TIERS": set()},
        "hive.codex_delivery": {"fleet_git_env": lambda *a, **kw: {}},
        "hive.llm_roles": {},
        "hive.push_policy": {"argonas_url_for_slug": lambda slug: slug},
        "hive.review_evidence": {},
    }.items():
        module = types.ModuleType(name)
        for key, value in attrs.items():
            setattr(module, key, value)
        monkeypatch.setitem(sys.modules, name, module)

    path = Path(__file__).resolve().parent / "deploy" / "hive_worker.py"
    spec = importlib.util.spec_from_file_location("time_audit_worker", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _utc(year, month, day, hour, minute=0):
    """A UTC epoch WITHOUT reading the local zone: `time.gmtime` is the contract."""
    return float(calendar.timegm((year, month, day, hour, minute, 0, 0, 0, 0)))


@pytest.mark.parametrize(
    "hour,expected",
    [
        (0, False),
        (1, True),  # 01:00-04:00 UTC is peak
        (3, True),
        (4, False),  # half-open, so 04:00 is off-peak
        (5, False),
        (6, True),  # 06:00-10:00 UTC is peak
        (9, True),
        (10, False),
        (23, False),
    ],
)
def test_deepseek_peak_boundaries_in_utc(monkeypatch, hour, expected):
    """Wednesday 2026-10-28: peak is [01:00,04:00) ∪ [06:00,10:00) in UTC."""
    worker = _worker(monkeypatch)
    assert worker.deepseek_peak(time.gmtime(_utc(2026, 10, 28, hour))) is expected


def test_deepseek_peak_is_off_at_the_weekend_in_utc(monkeypatch):
    """Saturday and Sunday are off-peak at every hour, decided by the UTC weekday."""
    worker = _worker(monkeypatch)
    for hour in (2, 7):
        assert worker.deepseek_peak(time.gmtime(_utc(2026, 10, 31, hour))) is False, "2026-10-31 is a Saturday"
        assert worker.deepseek_peak(time.gmtime(_utc(2026, 11, 1, hour))) is False, "2026-11-01 is a Sunday"
        assert worker.deepseek_peak(time.gmtime(_utc(2026, 10, 30, hour))) is True, "2026-10-30 is a Friday"


def test_bus_time_multiplier_agrees_with_the_worker_rule_in_utc():
    """`capacity.time_multiplier` takes an EPOCH and must match `deepseek_peak` hour for hour."""
    from hive.bus import capacity

    wednesday = _utc(2026, 10, 28, 0)
    for hour in range(24):
        expected = 2.0 if (1 <= hour < 4 or 6 <= hour < 10) else 1.0
        assert capacity.time_multiplier("deepseek", now=wednesday + hour * 3600) == expected, hour
    saturday = _utc(2026, 10, 31, 2)
    assert capacity.time_multiplier("deepseek", now=saturday) == 1.0
    assert capacity.time_multiplier("minimax", now=wednesday + 2 * 3600) == 1.0, "only DeepSeek prices by the hour"


def test_limited_until_expiry_boundary():
    """A service limited until T is denied before T and admitted AT T (`until > now`)."""
    from hive.bus.capacity import CapacityLedger, ServiceCapacity

    ledger = CapacityLedger()
    ledger.register(ServiceCapacity(service="minimax", slots=1000))
    until = NOW + 60
    ledger.limited_until["minimax"] = until

    assert ledger.headroom("minimax", now=until - EPS)[0] is False
    ok, reason = ledger.headroom("minimax", now=until)
    assert ok is True, reason
    assert ledger.headroom("never-declared", now=until)[0] is False


def test_minimax_window_cache_ttl_boundaries(monkeypatch):
    """The quota read is cached for MINIMAX_QUOTA_TTL and refetched at exactly that age."""
    worker = _worker(monkeypatch)
    ttl = worker.MINIMAX_QUOTA_TTL
    assert ttl == 600.0

    reads = []
    clock = Clock()
    monkeypatch.setitem(worker._mimo_cache, "quota", None)
    monkeypatch.setattr(worker, "_fetch_minimax_interval_pct", lambda: reads.append(1) or 10.0)
    monkeypatch.setattr(worker, "MINIMAX_LOW_PCT", 25.0)

    assert worker.minimax_window_low(now=clock.at) is True
    assert len(reads) == 1

    clock.advance(ttl - 1)
    assert worker.minimax_window_low(now=clock.at) is True
    assert len(reads) == 1, "inside the TTL the cached reading is used"

    clock.advance(1)  # exactly ttl: the check is `< ttl`, so this refetches
    assert worker.minimax_window_low(now=clock.at) is True
    assert len(reads) == 2


def test_minimax_unknown_reading_is_not_low_and_is_cached_shortly(monkeypatch):
    """A failed read counts as NOT low and is retried after the error TTL, not the success TTL."""
    worker = _worker(monkeypatch)
    assert worker.MINIMAX_QUOTA_ERROR_TTL == 120.0

    reads = []
    clock = Clock()
    monkeypatch.setitem(worker._mimo_cache, "quota", None)
    monkeypatch.setattr(worker, "_fetch_minimax_interval_pct", lambda: reads.append(1) or None)

    assert worker.minimax_window_low(now=clock.at) is False, "unknown must not trigger the MiMo substitution"
    clock.advance(worker.MINIMAX_QUOTA_ERROR_TTL - 1)
    assert worker.minimax_window_low(now=clock.at) is False
    assert len(reads) == 1
    clock.advance(1)
    assert worker.minimax_window_low(now=clock.at) is False
    assert len(reads) == 2


# ── findings: unit mismatches this audit could not fix without a product call ─


@pytest.mark.xfail(
    strict=True,
    reason="FB43-FINDING-1: doctor_v2.sweep_needs_review does `cutoff = (now or time.time())`, so now=0.0 "
    "(a legitimate fake-clock instant) is replaced by the wall clock. Fix: `time.time() if now is None else now`.",
)
def test_sweep_needs_review_honours_epoch_zero(monkeypatch):
    from hive.workers import doctor_v2

    # Relative to epoch 0 this row is only 10 s old, so a 100 h sweep must NOT report
    # it; substituting the wall clock instead makes it ~100 h old and reports it.
    row = {"id": "z", "kind": "fix:z", "status": "needs-review", "updated_at": -10.0, "created_at": -20.0}
    assert doctor_v2.sweep_needs_review([row], now=0.0, older_than_hours=100) == []


@pytest.mark.xfail(
    strict=True,
    reason="FB43-FINDING-2: deploy/hive_worker.escalation_agent passes an epoch float from _epoch() into "
    "deepseek_peak(), which reads now_utc.tm_hour on a struct_time. Fix: pass time.gmtime(_epoch(now_utc)).",
)
def test_escalation_agent_accepts_an_epoch(monkeypatch):
    worker = _worker(monkeypatch)
    monkeypatch.setattr(worker, "MINIMAX_LOW_PCT", 25.0)
    job = {"id": "j", "required_capabilities": ["route:flash"]}
    assert isinstance(worker.escalation_agent(job, now_utc=_utc(2026, 10, 28, 2)), str)
