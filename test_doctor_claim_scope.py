"""Doctor claim scope + no unattended Codex dispatch (2026-10-17).

The doctor claimed the normal worker job fix:singularity-shell-about-graphic via
/v1/jobs/next and failed it in 0s: process_triage_job is a triage brain, not an
executor. The bus claim endpoint cannot be kind-filtered per agent, so the
doctor restricts itself to triage:* / doctor:* (minus its own doctor:codex-fix:*
dispatches) and releases anything else straight back to the queue with
worker_error 'released_by_host' — a skip signal, never a failure.

All bus traffic is mocked at urllib.request.urlopen; nothing touches a socket.
"""

import importlib
import io
import json

import pytest

BUS = "http://192.0.2.67:5005"


class Reply(io.BytesIO):
    status = 200

    def __init__(self, payload=b"{}", status=200):
        super().__init__(payload)
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


class Bus:
    """Scripted bus: /v1/jobs/next returns `next_job`; everything else 200."""

    def __init__(self, next_job):
        self.next_job = next_job
        self.calls = []

    def __call__(self, request, timeout=0):
        url = request.full_url
        body = json.loads(request.data) if request.data else None
        self.calls.append(
            {"method": request.get_method(), "url": url, "body": body}
        )
        if url.endswith("/v1/jobs/next") or "/v1/jobs/next?" in url:
            if self.next_job is None:
                return Reply(b"", status=204)
            return Reply(json.dumps(self.next_job).encode())
        return Reply(b'{"ok":true}')

    @property
    def patches(self):
        return [c for c in self.calls if c["method"] == "PATCH"]


@pytest.fixture
def doctor(monkeypatch):
    monkeypatch.setenv("HIVE_URL", BUS)
    monkeypatch.delenv("HIVE_BUS_ADMIN_TOKEN", raising=False)
    from hive.workers import doctor as _doctor

    d = importlib.reload(_doctor)
    d._urn = "urn:agent:doctor:pythia:test-session"
    return d


def _install(doctor, monkeypatch, next_job):
    bus = Bus(next_job)
    monkeypatch.setattr(doctor.urllib.request, "urlopen", bus)
    return bus


# ─────────────────────────── claim scope ───────────────────────────
@pytest.mark.parametrize(
    "kind",
    [
        "triage:zeroclaw",
        "triage:fix:singularity-shell-about-graphic",
        "doctor:self-check",
        "doctor:dlq-sweep",
    ],
)
def test_doctor_claims_only_doctor_and_triage_kinds(doctor, monkeypatch, kind):
    job = {"id": "job-1", "kind": kind, "status": "queued"}
    bus = _install(doctor, monkeypatch, job)
    claimed = doctor.claim_next_job()
    assert claimed is not None and claimed["id"] == "job-1"
    assert bus.patches == []  # nothing released


def test_doctor_never_claims_its_own_fix_dispatch(doctor):
    assert not doctor.is_doctor_claimable("doctor:codex-fix:1729000000")


# ───────────────────── bus-side matcher scope (source fix) ─────────────────────
# The doctor used to be offered normal worker jobs (fix:singularity-shell-...,
# eligible_kinds=["zeroclaw"]) and had to claim-then-release them. The scope now
# lives in the bus matcher itself (hive/bus/domain/eligibility.py).
from hive.bus.domain.eligibility import agent_kind_aliases, job_agent_eligible  # noqa: E402


def _agent(kind, runtime=None, host="pythia"):
    return {
        "urn": f"urn:agent:{kind}:{host}:s",
        "host": host,
        "kind": kind,
        "runtime": runtime or kind,
        "capabilities": [],
        "eligible_aliases": agent_kind_aliases(kind, runtime or kind),
        "cost_tier": "B",
    }


def _job(kind, eligible_kinds=None):
    return {
        "id": "job-1",
        "kind": kind,
        "eligible_kinds": eligible_kinds,
        "required_capabilities": [],
        "eligible_hosts": None,
        "max_cost_tier": "B",
    }


def test_doctor_ineligible_for_zeroclaw_fix_job():
    ok, reason = job_agent_eligible(
        _job("fix:singularity-shell-about-graphic", ["zeroclaw"]), _agent("doctor")
    )
    assert ok is False
    assert "out of scope" in reason


def test_doctor_ineligible_for_wildcard_worker_job():
    ok, reason = job_agent_eligible(_job("build:native", ["*"]), _agent("doctor"))
    assert ok is False
    assert "out of scope" in reason


@pytest.mark.parametrize("kind", ["triage:fix:x", "doctor:sweep", "triage:zeroclaw"])
def test_doctor_eligible_for_triage_and_doctor_kinds(kind):
    ok, reason = job_agent_eligible(_job(kind), _agent("doctor"))
    assert ok is True, reason


def test_doctor_ineligible_for_its_own_codex_fix_dispatch():
    ok, reason = job_agent_eligible(_job("doctor:codex-fix:1"), _agent("doctor"))
    assert ok is False
    assert "codex-fix" in reason


def test_doctor_eligible_when_job_lists_doctor_in_eligible_kinds():
    ok, reason = job_agent_eligible(_job("feat:whatever", ["doctor"]), _agent("doctor"))
    assert ok is True, reason


def test_zeroclaw_worker_behaviour_is_unchanged():
    # still eligible for its own executor job
    assert job_agent_eligible(_job("fix:x", ["zeroclaw"]), _agent("zeroclaw"))[0] is True
    # still NOT eligible for triage work unless the job advertises triage
    ok, _ = job_agent_eligible(_job("triage:fix:x", ["triage"]), _agent("zeroclaw"))
    assert ok is False
    ok, _ = job_agent_eligible(_job("triage:fix:x", ["triage", "zeroclaw"]), _agent("zeroclaw"))
    assert ok is True


@pytest.mark.parametrize(
    "kind",
    [
        "fix:singularity-shell-about-graphic",
        "zeroclaw",
        "codex",
        "opencode",
        "build:native",
    ],
)
def test_doctor_releases_normal_worker_jobs(doctor, monkeypatch, kind):
    job = {"id": "abc12345-wrong", "kind": kind, "status": "claimed"}
    bus = _install(doctor, monkeypatch, job)
    assert doctor.claim_next_job() is None

    patches = bus.patches
    assert len(patches) == 1, patches
    assert patches[0]["url"].endswith("/v1/jobs/abc12345-wrong")
    body = patches[0]["body"]
    assert body["status"] == "queued"                 # back to the queue
    assert body.get("status") != "failed"             # NOT failed
    assert body["claimed_by"] == doctor._urn
    assert body["result"]["worker_error"] == "released_by_host"
    assert "status" not in body["result"]


def test_release_is_immediate_and_lease_never_taken(doctor, monkeypatch):
    """No patch to running/done/failed, no heartbeat side effects."""
    job = {"id": "wrong-1", "kind": "fix:x", "status": "claimed"}
    bus = _install(doctor, monkeypatch, job)
    doctor.claim_next_job()
    statuses = [p["body"].get("status") for p in bus.patches]
    assert statuses == ["queued"]


def test_empty_queue_returns_none(doctor, monkeypatch):
    bus = _install(doctor, monkeypatch, None)
    assert doctor.claim_next_job() is None
    assert bus.patches == []


def test_scope_prefixes_are_explicit(doctor):
    assert doctor.DOCTOR_CLAIM_KIND_PREFIXES == ("triage:", "doctor:")
    assert doctor.DOCTOR_CLAIM_KIND_EXCLUDE_PREFIXES == ("doctor:codex-fix:",)
    assert not doctor.is_doctor_claimable("")
    assert not doctor.is_doctor_claimable("triage")  # bare prefix is not a job


# ─────────────────────── dispatch never requires Codex ───────────────────────
def test_dispatch_codex_fix_never_requires_hard_codex(doctor, monkeypatch):
    bus = _install(doctor, monkeypatch, None)
    out = doctor.action_dispatch_codex_fix("fix the thing", "parent-1")
    assert "submitted" in out
    submits = [c for c in bus.calls if c["method"] == "POST" and c["url"].endswith("/v1/jobs")]
    assert len(submits) == 1
    body = submits[0]["body"]
    assert body["required_capabilities"] == ["hard:verified-git-delivery", "route:flash"]
    assert "hard:codex" not in body["required_capabilities"]
    # the fix runs on a zoder worker, never a codex-runtime worker
    assert body["eligible_kinds"] == ["zeroclaw"]


# ───────────────────────── alias-table contract ─────────────────────────
# Source-fix hardening for the live incident
# (fix:singularity-shell-about-graphic, eligible_kinds=['zeroclaw']).
# job_agent_eligible now scopes doctor agents via _doctor_scope_allowed --
# but the implementation still relies on RUNTIME_KIND_MAP never bridging
# the doctor kind to an executor runtime. These tests lock that contract.
import os  # noqa: E402

from hive.bus.domain.eligibility import (  # noqa: E402
    DOCTOR_KIND,
    DOCTOR_OPT_IN_EXCLUDE_ALIASES,
    RUNTIME_KIND_MAP,
    agent_is_doctor,
)


def test_alias_table_does_not_bridge_doctor_to_executor_kinds():
    # "doctor" must not appear in any executor's alias set.
    for runtime in (
        "zeroclaw", "codex", "codex-cli", "opencode", "opencode-cli",
        "claude", "claude-code", "claude-cli",
    ):
        aliases = RUNTIME_KIND_MAP.get(runtime, set())
        assert DOCTOR_KIND not in aliases, (
            f"runtime {runtime!r} aliases {sorted(aliases)!r} must not include "
            f"{DOCTOR_KIND!r} -- that would re-leak normal worker jobs to the doctor"
        )


def test_doctor_alias_set_does_not_contain_executor_kinds():
    # Doctor agents must not advertise any executor alias -- even if the
    # registration payload or runtime field were to drift, the alias set
    # alone must not match a normal worker job's eligible_kinds.
    aliases = agent_kind_aliases("doctor", "doctor")
    assert aliases == {"doctor"}
    assert not aliases.intersection({"zeroclaw", "codex", "opencode", "claude"})


def test_agent_is_doctor_matches_runtime_only():
    # An agent registered with kind="patch.notes" / runtime="doctor" is still a
    # doctor and must be scoped. An agent registered with kind="doctor" /
    # runtime="zoder" is also a doctor.
    assert agent_is_doctor({"kind": "patch.notes", "runtime": "doctor"}) is True
    assert agent_is_doctor({"kind": "doctor", "runtime": "zoder"}) is True
    assert agent_is_doctor({"kind": "zeroclaw", "runtime": "zeroclaw"}) is False


def test_doctor_scope_holds_for_non_scope_kind_with_zeroclaw_only():
    # Sanity: eligible_kinds=["zeroclaw"] is the exact live incident
    # (fix:singularity-shell-about-graphic) -- locked at the source.
    job = _job("fix:cover-bug", ["zeroclaw"])
    ok, reason = job_agent_eligible(job, _agent("doctor"))
    assert ok is False, reason
    assert "out of scope" in reason


@pytest.mark.parametrize(
    "eligible_kinds",
    [
        ["*"],
        ["zeroclaw"],
        ["codex"],
        ["opencode"],
        ["claude"],
    ],
)
def test_doctor_rejects_wildcard_and_executor_alias_opt_ins(eligible_kinds):
    # The reviewer flagged this specifically: a job whose only eligible_kinds
    # are '*' or executor aliases must NOT match a doctor agent -- the doctor
    # is a triage brain, not a wildcard worker. The only path for an explicit
    # opt-in is a literal 'doctor' entry (covered separately in
    # test_doctor_eligible_when_job_lists_doctor_in_eligible_kinds).
    job = _job("fix:cover-bug", eligible_kinds)
    ok, reason = job_agent_eligible(job, _agent("doctor"))
    assert ok is False, (eligible_kinds, reason)
    assert "out of scope" in reason


def test_doctor_opt_in_via_explicit_doctor_entry_still_works():
    # Sanity check on the boundary: when eligible_kinds lists 'doctor'
    # alongside '*' or an executor alias, the literal 'doctor' entry IS the
    # opt-in path described in the task ("whose eligible_kinds explicitly
    # lists 'doctor'"). The wildcard / executor entries do not subvert it.
    for eligible_kinds in (["*", "doctor"], ["zeroclaw", "doctor"]):
        ok, reason = job_agent_eligible(
            _job("feat:explicit-opt-in", eligible_kinds), _agent("doctor")
        )
        assert ok is True, (eligible_kinds, reason)


def test_doctor_opt_in_exclude_aliases_blocks_wildcard_and_executors():
    # The DOCTOR_OPT_IN_EXCLUDE_ALIASES set is the explicit mechanism that
    # closes the wildcard/executor-as-opt-in leak. It must contain '*' and
    # every executor alias. A future edit that drops one of these (e.g. to
    # support a new executor) must be paired with a deliberate decision to
    # keep the doctor out of that alias set -- this test forces the choice.
    assert "*" in DOCTOR_OPT_IN_EXCLUDE_ALIASES
    for alias in (
        "zeroclaw", "codex", "codex-cli", "opencode", "opencode-cli",
        "claude", "claude-code", "claude-cli",
    ):
        assert alias in DOCTOR_OPT_IN_EXCLUDE_ALIASES, alias


def test_runtime_kind_map_invariant_fires_when_executor_aliases_doctor():
    # The module-level invariant must fire if RUNTIME_KIND_MAP is edited so
    # an executor runtime aliases 'doctor'. A violating source edit cannot
    # even load -- the bus cannot come up with a bridged alias table.
    import importlib.util
    import sys

    src_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "hive", "bus", "domain", "eligibility.py",
    )
    with open(src_path) as fh:
        original = fh.read()
    mutated = original.replace(
        '"zeroclaw": {"zeroclaw"},',
        '"zeroclaw": {"zeroclaw", "doctor"},',
        1,
    )
    assert mutated != original, "invariant test failed to inject a violating source"
    mod_name = "eligibility_invariant_probe"
    spec = importlib.util.spec_from_loader(mod_name, loader=None)
    module = importlib.util.module_from_spec(spec)
    try:
        exec(compile(mutated, src_path, "exec"), module.__dict__)
    except RuntimeError as exc:
        assert "RUNTIME_KIND_MAP invariant violated" in str(exc)
        assert "zeroclaw" in str(exc)
    else:
        pytest.fail(
            "module-level RUNTIME_KIND_MAP invariant did NOT fire when "
            "executor runtime 'zeroclaw' aliased 'doctor'"
        )
    finally:
        sys.modules.pop(mod_name, None)
