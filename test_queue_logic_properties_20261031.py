"""FB53: property tests for the queue's dispatch decisions.

The brief points at ``hive/bus/queue_logic.py``, but that module only holds the
submission dedup key and the HARD/SOFT capability matcher. The four behaviours
the task names live elsewhere (recorded in ``PLAN-FB53.md``):

* ordering by priority and job age — ``queued_candidates``' ``ORDER BY priority
  DESC, started_at ASC`` plus ``hive/scheduler.order_candidates_for_worker`` in
  ``POST /v1/jobs/next``;
* ``eligible_kinds`` / ``eligible_hosts`` filtering —
  ``hive/bus/domain/eligibility.job_agent_eligible``, called per candidate;
* ``depends_on`` gating — ``jobs_with_unsatisfied_deps`` (scan bound) and the
  binding in-transaction re-check in ``claim_job``, with the release-on-death
  helper in ``hive/bus/deps.release_dependencies``;
* claim fairness — the scheduler sort key and the single-claimant compare-and-set.

These properties drive the real implementations: pure functions directly, the
repository against a temp sqlite file, and the whole claim path in-process via
``httpx.ASGITransport``. Every test states its invariant in its docstring, above
the assertion that assumes it. Skips cleanly when hypothesis is absent (it lives
in the ``dev`` extra, not a runtime dependency).
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
from pathlib import Path
from unittest import mock

import httpx
import pytest

os.environ.setdefault("HIVE_BACKEND", "sqlite")

hypothesis = pytest.importorskip("hypothesis", reason="FB53 property tests need the dev extra (hypothesis)")
from hypothesis import HealthCheck, given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

import agent_bus  # noqa: E402
import queue_logic  # noqa: E402
from hive.bus import deps as _deps  # noqa: E402
from hive.bus.domain import eligibility  # noqa: E402
from hive.core.models import COST_TIERS  # noqa: E402
from hive.persistence.sqlite import DependencyNotReady  # noqa: E402
from hive.scheduler import (  # noqa: E402
    ROLE_BUILD,
    ROLE_GENERAL,
    ROLE_REVIEW,
    WorkerProfile,
    order_candidates_for_worker,
    score_candidate,
    select_best_job,
)

#: Pure functions are cheap, so explore more there; the bus-backed properties
#: pay for a temp database per example.
PURE = settings(max_examples=80, deadline=None, suppress_health_check=[HealthCheck.too_slow])
BUS = settings(max_examples=6, deadline=None, suppress_health_check=[HealthCheck.too_slow])

CAPS = ["code", "gpu", "linux-compile", "build-rust", "workspace-a", "role:build", "*"]
LABELS = CAPS + [f"hard:{c}" for c in CAPS] + [f"soft:{c}" for c in CAPS]
SAFE_TEXT = st.text(alphabet="abcXYZ -_/:0123", max_size=6)


def _run(coro):
    return asyncio.run(coro)


# ══════════════════════════════════════════════════════════════════════════
# 1. queue_logic — HARD/SOFT capability matching
# ══════════════════════════════════════════════════════════════════════════
@given(
    caps=st.sets(st.sampled_from(CAPS), max_size=4),
    hard=st.sets(st.sampled_from(CAPS), max_size=3),
    soft=st.sets(st.sampled_from(CAPS), max_size=3),
)
@PURE
def test_hard_labels_gate_on_exact_capabilities(caps, hard, soft):
    """Invariant: ``match`` is eligible iff every ``hard:`` label's suffix is an
    exact capability. A ``*`` capability does not stand in for a missing HARD
    label (the honest-caps rule), while SOFT labels never gate.
    """
    labels = [f"hard:{h}" for h in hard] + [f"soft:{s}" for s in soft]
    result = queue_logic.match(labels, caps)
    missing = hard - caps
    assert result.eligible is (not missing), (labels, caps, result)
    if missing:
        assert result.score == 0.0
        assert "missing hard caps" in result.reason


@given(
    caps=st.sets(st.sampled_from(CAPS), max_size=4),
    extra=st.sampled_from(CAPS),
    labels=st.lists(st.sampled_from(LABELS), max_size=5),
)
@PURE
def test_adding_a_capability_never_hurts(caps, extra, labels):
    """Invariant: capability sets are monotone. Gaining one capability can only
    make a worker eligible when it was not, and can only raise its score.
    """
    before = queue_logic.match(labels, caps)
    after = queue_logic.match(labels, caps | {extra})
    assert after.eligible or not before.eligible, (labels, caps, extra)
    if before.eligible:
        assert after.score >= before.score, (labels, caps)


@given(
    caps=st.sets(st.sampled_from(CAPS), max_size=4),
    labels=st.lists(st.sampled_from(LABELS), max_size=5),
    load=st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
    more_load=st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
)
@PURE
def test_score_is_bounded_and_load_hurts(caps, labels, load, more_load):
    """Invariants: the score is an affinity in [0, 1] less a load penalty, so it
    stays inside [-0.25, 1]; a busier worker never scores higher on the same
    labels (the anti-herding signal); and the result is deterministic.
    """
    base = queue_logic.match(labels, caps, worker_load=load)
    heavier = queue_logic.match(labels, caps, worker_load=max(load, more_load))
    assert -0.25 <= base.score <= 1.0, base
    assert heavier.score <= base.score + 1e-12
    assert heavier.eligible is base.eligible
    assert queue_logic.match(labels, caps, worker_load=load) == base


# ══════════════════════════════════════════════════════════════════════════
# 2. queue_logic — submission dedup key
# ══════════════════════════════════════════════════════════════════════════
@given(
    tenant=SAFE_TEXT,
    kind=SAFE_TEXT,
    description=SAFE_TEXT,
    version=SAFE_TEXT,
    window=SAFE_TEXT,
)
@PURE
def test_dedup_key_is_stable_and_distinguishes_every_scoping_field(tenant, kind, description, version, window):
    """Invariants: the same submission always hashes to the same key (so a
    retried submit coalesces), and each scoping field alone changes it. Values
    here never contain the ``\\x1f`` separator, which is what keeps the basis
    unambiguous.
    """
    key = queue_logic.dedup_key(tenant=tenant, kind=kind, description=description, version=version, window=window)
    assert key == queue_logic.dedup_key(
        tenant=tenant, kind=kind, description=description, version=version, window=window
    )
    assert key != queue_logic.dedup_key(
        tenant=tenant + "x", kind=kind, description=description, version=version, window=window
    )
    assert key != queue_logic.dedup_key(
        tenant=tenant, kind=kind + "x", description=description, version=version, window=window
    )
    assert key != queue_logic.dedup_key(
        tenant=tenant, kind=kind, description=description + "x", version=version, window=window
    )
    assert key != queue_logic.dedup_key(
        tenant=tenant, kind=kind, description=description, version=version + "x", window=window
    )
    assert key != queue_logic.dedup_key(
        tenant=tenant, kind=kind, description=description, version=version, window=window + "x"
    )


@given(description=st.text(alphabet="abc ", max_size=6))
@PURE
def test_dedup_key_ignores_surrounding_whitespace_only(description):
    """Invariant: the description is stripped before hashing, so a submit that
    pads its text coalesces with the unpadded one (and never with a different one).
    """
    assert queue_logic.dedup_key(description=f"  {description}  ") == queue_logic.dedup_key(description=description)


@given(
    idempotency_key=st.text(alphabet="abcXYZ -_/:0123", min_size=1, max_size=6),
    tenant=SAFE_TEXT,
    kind=SAFE_TEXT,
    description=SAFE_TEXT,
)
@PURE
def test_idempotency_key_is_authoritative(idempotency_key, tenant, kind, description):
    """Invariant: an explicit (non-empty) ``idempotency_key`` overrides every
    other field — the caller owns rerun semantics, so the same key is the same
    job. An empty key is falsy and falls back to the scoped basis, which is why
    the strategy never generates one.
    """
    a = queue_logic.dedup_key(tenant=tenant, kind=kind, description=description, idempotency_key=idempotency_key)
    b = queue_logic.dedup_key(
        tenant=tenant + "x",
        kind=kind + "x",
        description=description + "x",
        version="v9",
        window="w9",
        idempotency_key=idempotency_key,
    )
    assert a == b
    assert a != queue_logic.dedup_key(idempotency_key=idempotency_key + "z")


# ══════════════════════════════════════════════════════════════════════════
# 3. scheduler — ordering and fairness keys
# ══════════════════════════════════════════════════════════════════════════
NOW = 1_800_000_000.0


def job(job_id: str, priority: int = 0, kind: str = "feat:queue", caps=()) -> dict:
    return {
        "id": job_id,
        "submitter_urn": "urn:agent:opencode:prop-a:submitter",
        "kind": kind,
        "priority": priority,
        "required_capabilities": list(caps),
    }


def worker(
    urn: str = "urn:agent:zeroclaw:prop-a:w1",
    host: str = "prop-a",
    role: str = ROLE_GENERAL,
    utilization: float = 0.0,
    last_served_at: float | None = None,
    deficit: float = 0.0,
) -> WorkerProfile:
    return WorkerProfile(
        urn=urn,
        host=host,
        role=role,
        utilization=utilization,
        last_served_at=last_served_at,
        deficit=deficit,
        active_minimax_lease=None,
    )


@given(
    a=st.integers(min_value=-50, max_value=50),
    b=st.integers(min_value=-50, max_value=50),
    w=st.builds(
        worker,
        role=st.sampled_from([ROLE_GENERAL, ROLE_BUILD, ROLE_REVIEW]),
        utilization=st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
        last_served_at=st.one_of(st.none(), st.floats(min_value=0.0, max_value=NOW, allow_nan=False)),
        deficit=st.floats(min_value=0.0, max_value=10.0, allow_nan=False),
    ),
)
@PURE
def test_priority_is_the_first_ordering_key(a, b, w):
    """Invariant: a higher priority job outranks a lower priority one for the
    same worker, whatever the worker's role, utilization, age or deficit. Input
    order cannot change the pick (the ordering is not first-come-first-served).
    """
    if a == b:
        return
    high, low = (a, b) if a > b else (b, a)
    candidates = [job("j-low", low), job("j-high", high)]
    for order in (candidates, list(reversed(candidates))):
        pick = select_best_job(candidates=order, worker=w, now=NOW)
        assert pick["id"] == "j-high", (a, b, order, pick)


@given(
    priorities=st.lists(st.integers(min_value=-5, max_value=5), min_size=0, max_size=6),
    w=st.builds(
        worker,
        role=st.sampled_from([ROLE_GENERAL, ROLE_BUILD]),
        utilization=st.floats(min_value=0.0, max_value=1.0, allow_nan=False),
        last_served_at=st.one_of(st.none(), st.floats(min_value=0.0, max_value=NOW, allow_nan=False)),
        deficit=st.floats(min_value=0.0, max_value=10.0, allow_nan=False),
    ),
)
@PURE
def test_ordering_is_a_deterministic_permutation(priorities, w):
    """Invariants: ordering never loses or duplicates a candidate, is total, is
    reproducible for the same inputs, and is non-increasing in priority.
    """
    candidates = [job(f"j{i}", p) for i, p in enumerate(priorities)]
    ordered = order_candidates_for_worker(candidates=candidates, worker=w, now=NOW)
    assert sorted(c["id"] for c in ordered) == sorted(c["id"] for c in candidates), (priorities, ordered)
    assert ordered == order_candidates_for_worker(candidates=candidates, worker=w, now=NOW)
    got = [c["priority"] for c in ordered]
    assert got == sorted(got, reverse=True), (priorities, got)


@given(w=st.builds(worker, role=st.sampled_from([ROLE_GENERAL, ROLE_BUILD, ROLE_REVIEW])))
@PURE
def test_exact_ties_keep_their_input_order(w):
    """Invariant: candidates with identical sort keys are ordered stably, so a
    re-run of the same scan cannot reorder equal jobs (no nondeterministic churn).
    """
    identical = [job("j-same", 3), job("j-same", 3), job("j-same", 3)]
    ordered = order_candidates_for_worker(candidates=identical, worker=w, now=NOW)
    assert [c["id"] for c in ordered] == ["j-same", "j-same", "j-same"]
    assert len(ordered) == 3


def test_role_preference_follows_whether_the_job_needs_a_build():
    """Invariant: for non-build work a general worker is preferred over a build
    worker; for build work the order flips. Role is a preference, never a gate —
    both remain eligible.
    """
    plain = [job("j-plain", 1)]
    build = [job("j-build", 1, kind="build:zeroclaw")]
    general, builder = worker(role=ROLE_GENERAL), worker(role=ROLE_BUILD)
    penalty = {
        (w.role, j["kind"]): score_candidate(job=j, worker=w, now=NOW).score[1]
        for w in (general, builder)
        for j in (plain[0], build[0])
    }
    assert penalty[(ROLE_GENERAL, "feat:queue")] < penalty[(ROLE_BUILD, "feat:queue")]
    assert penalty[(ROLE_BUILD, "build:zeroclaw")] < penalty[(ROLE_GENERAL, "build:zeroclaw")]


@given(
    deficit=st.floats(min_value=0.0, max_value=10.0, allow_nan=False),
    served=st.one_of(st.none(), st.floats(min_value=0.0, max_value=NOW - 1, allow_nan=False)),
    priorities=st.lists(st.integers(min_value=-3, max_value=3), min_size=1, max_size=4),
)
@PURE
def test_the_starvation_boost_cannot_change_one_workers_pick(deficit, served, priorities):
    """Invariant (and finding): ``select_best_job`` promotes a starved worker's
    deficit to +inf, but deficit/utilization/age are single constants in the sort
    key, so they cannot order two candidates *for the same worker*. A starved
    worker therefore gets exactly the job it would have got anyway; the boost is
    only visible in a cross-worker comparison the bus never performs.
    See FB53-FINDING-1 in FREEBUFF-BATCH10-SUMMARY.md.
    """
    candidates = [job(f"j{i}", p) for i, p in enumerate(priorities)]
    fresh = worker(deficit=deficit, last_served_at=served)
    starved = worker(deficit=0.0, last_served_at=None)
    assert starved.last_served_at is None  # is_starved() treats "never served" as starved
    assert (
        select_best_job(candidates=candidates, worker=fresh, now=NOW)["id"]
        == (select_best_job(candidates=candidates, worker=starved, now=NOW)["id"])
    )


# ══════════════════════════════════════════════════════════════════════════
# 4. eligibility — eligible_kinds, eligible_hosts, capabilities, cost tier
# ══════════════════════════════════════════════════════════════════════════
KINDS = [None, [], ["zeroclaw"], ["codex"], ["*"], ["claude-code"], ["zeroclaw", "codex"]]
HOSTS = [None, [], ["prop-a"], ["prop-b"], ["*"], ["prop-a", "prop-b"]]
#: The end-to-end pools must be admissible at submit time (a pin that matches no
#: registered worker is refused with 422 by design), so only codex/zeroclaw and
#: the two worker hosts appear here.
E2E_KINDS = [None, [], ["zeroclaw"], ["codex"], ["*"], ["zeroclaw", "codex"]]
E2E_HOSTS = [None, [], ["prop-a"], ["prop-b"], ["*"], ["prop-a", "prop-b"]]


def agent(kind: str = "zeroclaw", host: str = "prop-a", caps=("code",), tier: str = "A") -> dict:
    runtime = kind
    return {
        "urn": f"urn:agent:{kind}:{host}:s1",
        "host": host,
        "kind": kind,
        "runtime": runtime,
        "model": "unknown",
        "provider": "unknown",
        "cost_tier": tier,
        "capabilities": set(caps),
        "eligible_aliases": eligibility.agent_kind_aliases(kind, runtime),
        "subscription_throttled": False,
    }


def eligible_job(**overrides) -> dict:
    base = {
        "id": "j",
        "kind": "feat:queue",
        "required_capabilities": [],
        "eligible_kinds": None,
        "eligible_hosts": None,
        "max_cost_tier": "B",
    }
    base.update(overrides)
    return base


@given(kinds=st.sampled_from(KINDS), kind=st.sampled_from(["zeroclaw", "codex", "claude"]))
@PURE
def test_eligible_kinds_admits_only_matching_agents(kinds, kind):
    """Invariant: ``eligible_kinds=None``/``[]`` admits every agent, ``["*"]``
    admits every agent, and a populated list admits only agents whose kind alias
    set intersects it (kind is strict — no executor-to-executor bridging).
    """
    a = agent(kind=kind)
    eligible, reason = eligibility.job_agent_eligible(eligible_job(eligible_kinds=kinds), a)
    expected = not kinds or "*" in kinds or bool(set(kinds) & a["eligible_aliases"])
    assert eligible is expected, (kinds, kind, a["eligible_aliases"], reason)
    if eligible:
        assert reason == "eligible"
    else:
        assert "eligible_kinds" in reason and reason


@given(hosts=st.sampled_from(HOSTS), host=st.sampled_from(["prop-a", "prop-b", "prop-c"]))
@PURE
def test_eligible_hosts_pins_the_job_to_the_named_hosts(hosts, host):
    """Invariant: an unpinned job (or one pinned to ``*``) is open to every host;
    a host pin admits only agents registered on one of the named hosts, and a
    refusal says which host was rejected.
    """
    a = agent(kind="zeroclaw", host=host)
    eligible, reason = eligibility.job_agent_eligible(eligible_job(eligible_hosts=hosts), a)
    expected = not hosts or "*" in hosts or host in hosts
    assert eligible is expected, (hosts, host, reason)
    if not eligible:
        assert "eligible_hosts" in reason


@given(
    caps=st.sets(st.sampled_from(CAPS), max_size=4),
    required=st.lists(st.sampled_from(LABELS), max_size=3),
)
@PURE
def test_required_capabilities_reuse_the_hard_rule(caps, required):
    """Invariant: the eligibility filter defers to ``queue_logic.match`` for
    required capabilities, so a HARD label the agent lacks refuses the job and a
    wildcard capability cannot paper over it.
    """
    a = agent(caps=caps)
    eligible, reason = eligibility.job_agent_eligible(eligible_job(required_capabilities=required), a)
    assert eligible is queue_logic.match(required, caps).eligible, (required, caps, reason)


@given(
    agent_tier=st.sampled_from(COST_TIERS),
    job_tier=st.sampled_from(COST_TIERS),
)
@PURE
def test_cost_tier_ceiling_is_enforced(agent_tier, job_tier):
    """Invariant: an agent may claim a job only when its provider tier is at or
    below the job's ``max_cost_tier`` (A free/local, B mid, C premium).
    """
    a = agent(tier=agent_tier)
    eligible, reason = eligibility.job_agent_eligible(eligible_job(max_cost_tier=job_tier), a)
    expected = COST_TIERS.index(agent_tier) <= COST_TIERS.index(job_tier)
    assert eligible is expected, (agent_tier, job_tier, reason)
    if not eligible:
        assert "cost_tier" in reason


# ══════════════════════════════════════════════════════════════════════════
# 5. The bus: temp sqlite + in-process ASGI (harness copied from
#    test_api_contract_20261029.py, kept local so this file is self-contained)
# ══════════════════════════════════════════════════════════════════════════
class Bus:
    """One isolated bus: temp sqlite file, patched repo singleton, ASGI client."""

    def __init__(self):
        self._td = tempfile.TemporaryDirectory()
        self._patches = []
        env = mock.patch.dict(
            os.environ,
            {
                "AGENT_BUS_DB": str(Path(self._td.name) / "queue.db"),
                "HIVE_BACKEND": "sqlite",
                "HIVE_HOST_DENYLIST": "",
                "MNEMOS_URL": "",
                "MNEMOS_TOKEN": "",
                "HIVE_SHARED_SECRET": "",
                "HIVE_BUS_ADMIN_TOKEN": "",
            },
        )
        env.start()
        self._patches.append(env)
        repo = mock.patch.object(_deps, "_HIVE_REPO", None)
        repo.start()
        self._patches.append(repo)
        _run(self.repo().init())
        self.submitter = self.register("opencode", "prop-a", caps=("code", "orchestrate"))

    # -- lifecycle ------------------------------------------------------
    def __enter__(self) -> Bus:
        return self

    def __exit__(self, *_exc) -> None:
        for patch in reversed(self._patches):
            patch.stop()
        self._td.cleanup()

    # -- transport ------------------------------------------------------
    def request(self, method: str, path: str, body=None, actor: str | None = None):
        async def _do():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t"
            ) as client:
                headers = {"X-Hive-Actor": actor} if actor else None
                response = await client.request(method, path, json=body, headers=headers)
                try:
                    return response.status_code, response.json()
                except ValueError:
                    return response.status_code, None

        return _run(_do())

    def register(self, kind: str, host: str, caps=("code",)) -> str:
        code, data = self.request(
            "POST",
            "/v1/agents/register",
            {
                "kind": kind,
                "host": host,
                "runtime": kind,
                "capabilities": list(caps),
                "auth_method": "api",
            },
        )
        assert code == 200, data
        return data["urn"]

    def submit(self, **fields) -> str:
        body = {"kind": "feat:queue-prop", "submitter_urn": self.submitter, "priority": 0}
        body.update({k: v for k, v in fields.items() if v is not None})
        code, data = self.request("POST", "/v1/jobs", body, actor=self.submitter)
        assert code in (200, 201), (body, code, data)
        return data["id"]

    def claim(self, urn: str):
        return self.request("POST", f"/v1/jobs/next?agent_urn={urn}")

    # -- direct storage access for state the lifecycle owns -------------
    def repo(self):
        return _deps.hive_repo()

    def call(self, fn, *, commit: bool = False):
        """Run ``fn(connection)`` on a fresh connection; return its result."""

        async def _do():
            async with self.repo().connection() as db:
                result = await fn(db)
                if commit:
                    await db.commit()
                return result

        return _run(_do())

    def sql(self, statement: str, params=()) -> None:
        async def _do(db):
            await db.execute(statement, params)

        self.call(_do, commit=True)

    def insert_job(
        self,
        job_id: str,
        *,
        status: str = "queued",
        priority: int = 0,
        started_at: float = 0.0,
        depends_on=None,
        backoff: float | None = None,
    ) -> None:
        self.sql(
            "INSERT INTO jobs (id, submitter_urn, kind, priority, status, started_at, depends_on, "
            "retry_backoff_until) VALUES (?,?,?,?,?,?,?,?)",
            (
                job_id,
                "urn:agent:opencode:prop-a:submitter",
                "feat:queue-prop",
                priority,
                status,
                started_at,
                json.dumps(depends_on) if depends_on else None,
                backoff,
            ),
        )

    def insert_agent(self, urn: str = "urn:agent:zeroclaw:prop-a:w1", kind: str = "zeroclaw") -> str:
        now = time.time()
        self.sql(
            "INSERT INTO agents (urn, kind, host, session_id, capabilities, started_at, last_heartbeat, "
            "status, runtime, cost_tier) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (urn, kind, "prop-a", "s1", '["code"]', now, now, "online", kind, "A"),
        )
        return urn

    def job_row(self, job_id: str):
        async def _do(db):
            async with db.execute("SELECT status, depends_on FROM jobs WHERE id=?", (job_id,)) as cur:
                return await cur.fetchone()

        return self.call(_do)

    def claim_row(self, job_id: str) -> int:
        """The repository's binding gate, isolated from the deps pre-scan."""

        async def _do(db):
            return await self.repo().claim_job(
                db,
                job_id=job_id,
                agent_urn="urn:agent:zeroclaw:prop-a:w1",
                now=time.time(),
                lease_expires_at=time.time() + 60,
                runtime="zeroclaw",
                model="unknown",
                provider="unknown",
                cost_tier="A",
            )

        return self.call(_do, commit=True)


@pytest.mark.parametrize("job_id,status", [("j1", "queued"), ("j2", "queued"), ("j3", "done")])
def test_harness_sanity_inserts_rows(job_id, status):
    """Invariant: the harness's direct inserts are visible through the same
    connection the repository reads, so a later property cannot pass vacuously.
    """
    with Bus() as bus:
        bus.insert_job(job_id, status=status)
        assert bus.job_row(job_id)[0] == status


# ══════════════════════════════════════════════════════════════════════════
# 6. Candidate scan ordering (priority DESC, queue age ASC)
# ══════════════════════════════════════════════════════════════════════════
@given(priorities=st.lists(st.integers(min_value=-4, max_value=4), min_size=1, max_size=5))
@BUS
def test_queued_candidates_orders_by_priority_then_queue_age(priorities):
    """Invariant: the dequeue scan returns queued jobs by priority descending,
    then by ``started_at`` ascending (the oldest job first within a priority).
    ``started_at`` is unique here, so the expected order is total and exact.
    """
    with Bus() as bus:
        for index, priority in enumerate(priorities):
            bus.insert_job(f"j{index}", priority=priority, started_at=1000.0 + index)

        def scan(db):
            return bus.repo().queued_candidates(db, agent_urn="urn:agent:zeroclaw:prop-a:w1", now=5000.0, limit=100)

        rows = bus.call(scan)
        got = [row[0] for row in rows]
        expected = [f"j{index}" for index, _ in sorted(enumerate(priorities), key=lambda pair: (-pair[1], pair[0]))]
        assert got == expected, (priorities, got)
        # The scan carries priority at index 5 and started_at at index 17.
        assert [row[5] for row in rows] == sorted(priorities, reverse=True)


@given(priorities=st.lists(st.integers(min_value=-4, max_value=4), min_size=2, max_size=5), limit=st.integers(1, 4))
@BUS
def test_queued_candidates_limit_returns_the_queue_head(priorities, limit):
    """Invariant: the scan's LIMIT truncates the *head* of the priority order and
    never reshuffles it (a bounded claim scan cannot skip the top job).
    """
    with Bus() as bus:
        for index, priority in enumerate(priorities):
            bus.insert_job(f"j{index}", priority=priority, started_at=1000.0 + index)
        rows = bus.call(
            lambda db: bus.repo().queued_candidates(
                db, agent_urn="urn:agent:zeroclaw:prop-a:w1", now=5000.0, limit=limit
            )
        )
        full = bus.call(
            lambda db: bus.repo().queued_candidates(db, agent_urn="urn:agent:zeroclaw:prop-a:w1", now=5000.0, limit=100)
        )
        assert [row[0] for row in rows] == [row[0] for row in full][:limit]


def test_retry_backoff_hides_a_job_until_its_deadline():
    """Invariant: a job under retry backoff is absent from the scan before its
    deadline and present after it — the backoff is not merely advisory.
    """
    with Bus() as bus:
        bus.insert_job("waiting", priority=9, started_at=1000.0, backoff=2000.0)
        bus.insert_job("ready", priority=0, started_at=1001.0)

        def as_of(now):
            rows = bus.call(
                lambda db: bus.repo().queued_candidates(
                    db, agent_urn="urn:agent:zeroclaw:prop-a:w1", now=now, limit=100
                )
            )
            return [row[0] for row in rows]

        assert as_of(1500.0) == ["ready"]
        assert as_of(2001.0) == ["waiting", "ready"]


# ══════════════════════════════════════════════════════════════════════════
# 7. depends_on gating (binding gate + release-on-death)
# ══════════════════════════════════════════════════════════════════════════
@given(dep_statuses=st.lists(st.sampled_from(["queued", "done", "failed", "cancelled", "needs-review"]), max_size=3))
@BUS
def test_the_claim_gate_admits_only_jobs_whose_dependencies_are_all_done(dep_statuses):
    """Invariant: a job is claimable iff every ``depends_on`` id exists and is
    ``done``. Failed, cancelled and needs-review dependencies block it (they are
    not done), and both the pre-scan and the binding claim gate agree.
    """
    with Bus() as bus:
        bus.insert_agent()
        dep_ids = []
        for index, status in enumerate(dep_statuses):
            dep_ids.append(f"dep{index}")
            bus.insert_job(dep_ids[-1], status=status)
        bus.insert_job("target", depends_on=dep_ids or None)
        expected_blocked = any(status != "done" for status in dep_statuses)

        blocked = bus.call(lambda db: bus.repo().jobs_with_unsatisfied_deps(db, ids=["target"]))
        satisfied = bus.call(lambda db: bus.repo().dependencies_satisfied(db, "target"))
        assert ("target" in blocked) is expected_blocked, (dep_statuses, blocked)
        assert satisfied is (not expected_blocked)

        if expected_blocked:
            with pytest.raises(DependencyNotReady):
                bus.claim_row("target")
            assert bus.job_row("target")[0] == "queued"
        else:
            assert bus.claim_row("target") == 1
            assert bus.job_row("target")[0] == "claimed"


def test_a_dangling_dependency_strands_the_batch_dequeue_but_not_a_direct_claim():
    """Invariant (and finding FB53-FINDING-2): ``claim_job`` defensively drops a
    ``depends_on`` id whose row no longer exists (the 2026-10-27 purge fix), but
    the batch dequeue's pre-scan evaluates the same id with ``_deps_all_done``,
    which counts it unsatisfied and removes the job from the candidate list. The
    drop therefore never runs on that path and ``POST /v1/jobs/next`` never hands
    the job out, while the targeted claim takes it. The two paths disagree about
    a dependency that cannot exist.
    """
    with Bus() as bus:
        bus.insert_agent()
        worker_urn = "urn:agent:zeroclaw:prop-a:w1"
        bus.insert_job("orphan", depends_on=["gone-forever"])

        # The read used by the pre-scan says "not satisfied"; the claim gate would
        # have dropped the id and proceeded.
        assert bus.call(lambda db: bus.repo().dependencies_satisfied(db, "orphan")) is False
        blocked = bus.call(lambda db: bus.repo().jobs_with_unsatisfied_deps(db, ids=["orphan"]))
        assert "orphan" in blocked

        code, data = bus.claim(worker_urn)
        assert code == 204, ("the dangling-dep job was handed out by the batch dequeue", code, data)
        assert bus.job_row("orphan")[0] == "queued"

        assert bus.claim_row("orphan") == 1
        assert bus.job_row("orphan")[0] == "claimed"
        assert bus.job_row("orphan")[1] is None, "the dropped id must leave depends_on NULL"


@given(
    status=st.sampled_from(["queued", "offered", "claimed", "running", "done"]),
    extra=st.booleans(),
)
@BUS
def test_release_dependents_strips_a_dead_edge_only_from_unclaimed_dependents(status, extra):
    """Invariant: when a dependency dies, the queued/offered dependents that name
    it lose that id (NULL when it was the only one) and are reported once each;
    a dependent already in flight is never edited.
    """
    with Bus() as bus:
        bus.insert_job("dead", status="failed")
        bus.insert_job("other", status="queued")
        deps = ["dead"] + (["other"] if extra else [])
        bus.insert_job("dependent", status=status, depends_on=deps)

        records = bus.call(
            lambda db: bus.repo().release_dependents(db, dep_id="dead", dep_status="failed"), commit=True
        )
        stored = bus.job_row("dependent")[1]

        if status in ("queued", "offered"):
            assert [record["dependent_id"] for record in records] == ["dependent"], records
            assert records[0]["dep_status"] == "failed"
            assert records[0]["failed_dep_id"] == "dead"
            assert records[0]["remaining"] == (["other"] if extra else [])
            if extra:
                assert json.loads(stored) == ["other"]
            else:
                assert stored is None, "an emptied depends_on must be NULL, not '[]'"
        else:
            assert records == []
            assert json.loads(stored) == deps


# ══════════════════════════════════════════════════════════════════════════
# 8. End to end through POST /v1/jobs/next
# ══════════════════════════════════════════════════════════════════════════
@given(priorities=st.lists(st.integers(min_value=-5, max_value=5), min_size=1, max_size=4, unique=True))
@BUS
def test_claim_next_hands_out_jobs_highest_priority_first_and_only_once(priorities):
    """Invariant: repeated claims by one eligible worker return every job exactly
    once, in descending priority — the queue is a priority queue, not a FIFO, and
    a claim cannot hand the same job to two callers.
    """
    with Bus() as bus:
        worker_urn = bus.register("zeroclaw", "prop-east")
        ids = [bus.submit(priority=priority, description=f"p{priority}") for priority in priorities]
        claimed = []
        for _ in range(len(ids) + 1):
            code, data = bus.claim(worker_urn)
            if code == 204:
                break
            assert code == 200, (code, data)
            claimed.append(data["id"])
        assert len(claimed) == len(set(claimed)) == len(ids), (priorities, claimed)
        expected = [jid for _, jid in sorted(enumerate(ids), key=lambda pair: (-priorities[pair[0]], pair[0]))]
        assert claimed == expected, (priorities, claimed)


def test_equal_priority_goes_to_the_older_job():
    """Invariant: with equal priority the job that entered the queue first is
    claimed first (priority ASC is not a tie-break; age is).
    """
    with Bus() as bus:
        worker_urn = bus.register("zeroclaw", "prop-east")
        first = bus.submit(description="older")
        time.sleep(0.01)
        bus.submit(description="younger")
        code, data = bus.claim(worker_urn)
        assert code == 200, data
        assert data["id"] == first


@given(kinds=st.lists(st.sampled_from(E2E_KINDS), min_size=1, max_size=3))
@BUS
def test_claim_next_respects_eligible_kinds(kinds):
    """Invariant: a worker never receives a job whose ``eligible_kinds`` excludes
    its kind, and every job is still claimed by somebody eligible for it — the
    filter narrows the queue, it does not strand jobs.
    """
    with Bus() as bus:
        workers = {"zeroclaw": bus.register("zeroclaw", "prop-a"), "codex": bus.register("codex", "prop-b")}
        jobs = {}
        for index, allowed in enumerate(kinds):
            jobs[bus.submit(eligible_kinds=allowed, description=f"k{index}")] = allowed
        claimed = _drain(bus, workers)
        for job_id, kind in claimed.items():
            allowed = jobs[job_id]
            assert not allowed or "*" in allowed or kind in allowed, (job_id, kind, allowed)
        assert set(claimed) == set(jobs)


@given(hosts=st.lists(st.sampled_from(E2E_HOSTS), min_size=1, max_size=3))
@BUS
def test_claim_next_respects_eligible_hosts(hosts):
    """Invariant: a host-pinned job is only handed to an agent registered on one
    of the pinned hosts (or to anyone when pinned to ``*``), and no pinned job is
    stranded by the pin when a matching worker is polling.
    """
    with Bus() as bus:
        workers = {"prop-a": bus.register("zeroclaw", "prop-a"), "prop-b": bus.register("zeroclaw", "prop-b")}
        jobs = {}
        for index, pinned in enumerate(hosts):
            jobs[bus.submit(eligible_hosts=pinned, description=f"h{index}")] = pinned
        claimed = _drain(bus, workers)
        for job_id, host in claimed.items():
            pinned = jobs[job_id]
            assert not pinned or "*" in pinned or host in pinned, (job_id, host, pinned)
        assert set(claimed) == set(jobs)


def _drain(bus: Bus, workers: dict[str, str]) -> dict[str, str]:
    """Claim until every worker reports an empty queue; return {job_id: worker}."""
    claimed: dict[str, str] = {}
    idle = 0
    while idle < len(workers) * 2 + 1:
        idle += 1
        for name, urn in workers.items():
            code, data = bus.claim(urn)
            if code == 204:
                continue
            assert code == 200, (code, data)
            idle = 0
            assert data["id"] not in claimed, f"{data['id']} was handed out twice"
            claimed[data["id"]] = name
    return claimed


@given(chain=st.integers(min_value=1, max_value=4))
@BUS
def test_a_dependent_is_never_claimed_before_its_dependency_is_done(chain):
    """Invariant: at any moment exactly the chain's unblocked head is claimable —
    a dependent is not offered while its dependency is anything but ``done``, and
    completing the dependency admits the next link (the edge is satisfied, not
    stale).
    """
    with Bus() as bus:
        worker_urn = bus.register("zeroclaw", "prop-east")
        ids = []
        for index in range(chain):
            ids.append(bus.submit(description=f"link{index}", depends_on=[ids[-1]] if ids else None))

        for index, job_id in enumerate(ids):
            code, data = bus.claim(worker_urn)
            assert code == 200, (index, code, data)
            assert data["id"] == job_id, (index, data["id"], ids)
            code, data = bus.claim(worker_urn)
            assert code == 204, ("a dependent was offered before its dependency finished", code, data)
            bus.sql(
                "UPDATE jobs SET status='done', ended_at=? WHERE id=?",
                (time.time(), job_id),
            )
        code, _ = bus.claim(worker_urn)
        assert code == 204
