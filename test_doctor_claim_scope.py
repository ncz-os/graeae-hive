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
