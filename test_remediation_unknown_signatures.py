"""The unknown-signature policy wired into the doctor (2026-10-27).

`remediation.py` / `remediation_manifest.py` added the policy, but
`doctor.py` had no import and no call site: the verify-delivery /
one-retry-elsewhere / park / gap-promotion / coverage-report behaviour was
dead code. These tests pin the WIRING, not just the policy:

* a failure the classifier does not know retries ONCE on another eligible
  host, then parks as needs-review with the signature recorded;
* a known failure goes through its existing handler, never the policy;
* the never-act classes (host_declines_kind / released_by_host /
  protection_violation) are never retried;
* the signature table and the retry counter live in the durable STATE,
  so a doctor restart does not reset them;
* the periodic coverage report is written from the STATE table.

Every test that reaches `process_triage_job` asserts the wiring marker
(`auto_detected` + `unknown_signature` tag, no LLM call): if the call site
is removed from doctor.py these fail, while the unit tests of the
`remediation` module alone would still pass.
"""

import json
import time

from unittest.mock import Mock

from hive.workers import doctor, remediation
from hive.workers.doctor_state import DurableStateStore, MemoryBackend

# A failure shape no manifest row claims: the unknown policy owns it.
UNKNOWN_REASON = "fuzz: widget exploded at /mnt/argonas/x 2026-10-27T12:00:00Z on host-studio.example.com:5005"
UNKNOWN_SIG = remediation.failure_signature(UNKNOWN_REASON)

# The three never-act classes.
NEVER_ACT_REASONS = (
    "host_declines_kind: worker refused this kind on hydra",
    "released_by_host: studio released the job on its own",
    "protection_violation: guard blocked the write",
)


def _fresh_state() -> DurableStateStore:
    """A clean in-process store for tests that must not depend on the
    autouse fixture's store (which is also what doctor.STATE is)."""
    return DurableStateStore(MemoryBackend(), refresh_secs=0.0)


def _triage_job(jid: str = "job-unknown-1") -> dict:
    return {"id": jid, "kind": "fix:hive-x", "description": "2 fails of base_kind=fix:hive-x", "max_retries": 3, "retry_count": 0}


def _failed_job(reason: str, jid: str = "failed-1") -> list[dict]:
    return [{"id": jid, "kind": "fix:hive-x", "failure_reason": reason, "result": {"failure_reason": reason}}]


def _wire(monkeypatch, failed: list[dict], release: Mock, llm: list):
    """Point process_triage_job's inputs at a fixed failure cluster and a
    counting LLM stub. If the unknown-signature call site exists in
    doctor.py the LLM is never reached for an unknown signature; if the
    wiring is removed, the LLM IS called (and the tests below fail)."""
    monkeypatch.setattr(doctor, "fetch_failed_jobs_for_kind", lambda *a, **k: failed)
    monkeypatch.setattr(doctor, "fetch_agent_registry_summary", lambda: {})
    monkeypatch.setattr(doctor, "_doctor_v2_decision", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(doctor, "apply_release", release)
    monkeypatch.setattr(
        doctor,
        "broadcast_doctor_message",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        doctor,
        "invoke_doctor_agent",
        lambda *a, **k: (llm.append(1), "should not be called"),
    )


def test_unknown_signature_retries_once_then_parks_with_signature(monkeypatch):
    """Unknown failure -> one release (retry elsewhere) -> park as
    needs-review with the signature recorded in the durable table."""
    release = Mock(return_value=("released job-unknown-1", True))
    llm: list = []
    _wire(monkeypatch, _failed_job(UNKNOWN_REASON), release, llm)

    # First pass: the retry.
    result = doctor.process_triage_job(_triage_job())
    assert llm == [], "an unknown signature must not burn an LLM call"
    assert result["auto_detected"] is True
    assert result["released_to_queue"] is True
    assert result["action"]["unknown_signature"] == UNKNOWN_SIG
    assert result["action"]["unknown_signature_retry"] is True
    assert result["action"]["action_type"] == "release_to_queue"
    release.assert_called_once()
    # The signature table row is durable STATE, not a module dict.
    row = remediation.get_signature_row(doctor.STATE, UNKNOWN_SIG)
    assert row and row["last_action"] == "retried"

    # Second pass: the one-retry budget is spent, so the job parks as
    # needs-review and the signature stays recorded.
    release.reset_mock(return_value=True, side_effect=True)
    result = doctor.process_triage_job(_triage_job())
    assert result["released_to_queue"] is False
    assert result["needs_review"] is True
    assert result["result"]["needs_operator"] is True
    assert result["result"]["unknown_signature"] == UNKNOWN_SIG
    assert result["result"]["raw_reason"] == UNKNOWN_REASON
    row = remediation.get_signature_row(doctor.STATE, UNKNOWN_SIG)
    assert row["last_action"] == "needs-review"
    release.assert_not_called()


def test_known_failure_is_unchanged_and_not_touched_by_the_policy(monkeypatch):
    """A failure whose signature a manifest row claims goes through its
    existing handler; the unknown policy must not record it."""
    # token_mismatch is claimed by a row in remediation_manifest, so its
    # normalised signature is a known shape.
    known_reason = "no token for host studio-192-168-207-10-5005"
    sig = remediation.failure_signature(known_reason)
    assert remediation.handler_for_signature(sig) is not None

    # The known path: _match_known_signature answers (escalate: no LAN token
    # configured), so the LLM and the unknown policy are both bypassed.
    monkeypatch.setattr(doctor, "fetch_failed_jobs_for_kind", lambda *a, **k: _failed_job(known_reason))
    monkeypatch.setattr(doctor, "fetch_agent_registry_summary", lambda: {})
    monkeypatch.setattr(doctor, "_doctor_v2_decision", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(doctor, "_match_known_signature", lambda *a, **k: {
        "action_type": "escalate",
        "confidence": 1.0,
        "reason": "token-mismatch without a configured LAN token",
    })
    llm: list = []
    monkeypatch.setattr(doctor, "invoke_doctor_agent", lambda *a, **k: (llm.append(1), "nope"))
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)

    result = doctor.process_triage_job(_triage_job("job-known-1"))
    assert llm == []
    assert result["action"]["action_type"] == "escalate"
    # The unknown policy never saw this signature: no table row, no retry
    # counter.
    assert remediation.get_signature_row(doctor.STATE, sig) is None
    assert doctor.STATE.timestamps(remediation.KEY_UNKNOWN_RETRY + ":" + sig + ":job-known-1", ttl=remediation.SIGNATURE_TTL_SEC) == []


def test_unknown_policy_helper_skips_known_signatures():
    """_unknown_signature_decision returns None for any signature a manifest
    row claims, including every never-act class."""
    assert doctor._unknown_signature_decision(_triage_job(), "") is None
    assert doctor._unknown_signature_decision(_triage_job(), "   ") is None
    for reason in NEVER_ACT_REASONS:
        assert doctor._unknown_signature_decision(_triage_job(), reason) is None, reason


def test_never_act_classes_are_never_retried(monkeypatch):
    """host_declines_kind / released_by_host / protection_violation stay
    report-only: the wired helper never requeues them and never writes a
    retry counter for them. The raw policy module alone is not the guard
    (it cannot see the manifest) — the WIRING is, by returning before the
    policy is ever consulted for a claimed signature."""
    for reason in NEVER_ACT_REASONS:
        monkeypatch.setattr(doctor, "STATE", _fresh_state())
        job = _triage_job("job-never-act")
        sig = remediation.failure_signature(reason)
        # Claimed by a manifest row, so the wired helper stays out of the
        # way entirely — no table row, no retry counter, no release.
        assert remediation.handler_for_signature(sig) is not None, reason
        assert doctor._unknown_signature_decision(job, reason) is None
        assert remediation.get_signature_row(doctor.STATE, sig) is None
        counter_key = remediation.KEY_UNKNOWN_RETRY + ":" + sig + ":job-never-act"
        assert doctor.STATE.timestamps(counter_key, ttl=remediation.SIGNATURE_TTL_SEC) == []


def test_signature_table_and_retry_counter_are_durable_state():
    """The policy's counters live in STATE, never a module dict: a fresh
    store with the same contents keeps refusing the second retry."""
    state = _fresh_state()
    job = _triage_job("job-durable-1")

    first = remediation.unknown_policy(state, job, UNKNOWN_REASON, commits_reachable_from_origin=False)
    assert first["action"] == "retried"

    # Simulate a restart: a brand-new store object seeded from the same
    # durable backend must hold the same table row and the same retry count.
    replay = _fresh_state()
    for key in state.keys(""):
        record = state.value(key)
        if record is not None:
            replay.set_value(key, record, ttl=remediation.SIGNATURE_TTL_SEC)
        else:
            stamps = state.timestamps(key, ttl=remediation.SIGNATURE_TTL_SEC)
            if stamps:
                for stamp in stamps:
                    replay.touch(key, ttl=remediation.SIGNATURE_TTL_SEC, at=stamp)

    row = remediation.get_signature_row(replay, UNKNOWN_SIG)
    assert row is not None and row["count"] >= 1
    key = remediation.KEY_UNKNOWN_RETRY + ":" + UNKNOWN_SIG + ":job-durable-1"
    assert len(replay.timestamps(key, ttl=remediation.SIGNATURE_TTL_SEC)) >= 1

    second = remediation.unknown_policy(replay, job, UNKNOWN_REASON, commits_reachable_from_origin=False)
    assert second["action"] == "needs-review", "a restarted doctor must not spend a second retry"
    assert second["result"]["unknown_signature"] == UNKNOWN_SIG


def test_unknown_signature_delivery_verified_marks_delivered(monkeypatch):
    """Branch (a): when the caller proves the commits are on origin, the
    policy completes the job with a delivered result and never requeues."""
    monkeypatch.setattr(doctor, "STATE", _fresh_state())
    state = doctor.STATE
    decision = remediation.unknown_policy(state, _triage_job(), UNKNOWN_REASON, commits_reachable_from_origin=True)
    assert decision["action"] == "delivered"
    assert decision["needs_review"] is False
    assert decision["result"]["unknown_signature"] == UNKNOWN_SIG
    row = remediation.get_signature_row(state, UNKNOWN_SIG)
    assert row["last_action"] == "delivered"


def test_coverage_report_written_from_state_table(tmp_path):
    """The periodic coverage report reads the durable signature table and
    writes coverage-<UTC date>.json, marking each signature handled /
    unknown / promoted."""
    state = _fresh_state()
    now = time.time()
    remediation.record_failure(state, UNKNOWN_SIG, "job-1", UNKNOWN_REASON, "retried", at=now - 10)
    remediation.record_failure(state, UNKNOWN_SIG, "job-2", UNKNOWN_REASON, "needs-review", at=now - 5)
    known_sig = remediation.failure_signature("no token for host studio")
    remediation.record_failure(state, known_sig, "job-3", "no token for host studio", "retried", at=now - 1)

    posts: list = []
    payload = remediation.coverage_report(
        state,
        now=now,
        coverage_dir=tmp_path,
        post_message=lambda topic, body: posts.append((topic, body)),
    )

    assert posts and posts[0][0] == "doctor.coverage"
    date = payload["date"]
    path = tmp_path / f"coverage-{date}.json"
    assert path.exists()
    on_disk = json.loads(path.read_text())
    assert on_disk["total_signatures"] == 2
    by_sig = {e["signature"]: e for e in on_disk["top"]}
    assert by_sig[UNKNOWN_SIG]["status"] == "unknown"
    assert by_sig[UNKNOWN_SIG]["count"] == 2
    assert by_sig[known_sig]["status"] == "handled"
    assert payload["counts"]["unknown"] == 1 and payload["counts"]["handled"] == 1

    # The due/interval bookkeeping is durable STATE too. `coverage_due`
    # compares against the store's clock (the live one here), so the mark is
    # written with the real now.
    assert remediation.coverage_due(state) is True
    remediation.mark_coverage_done(state)
    assert remediation.coverage_due(state) is False
    assert remediation.coverage_due(state, now=time.time() + remediation.COVERAGE_INTERVAL_SEC) is True


def test_gap_promotion_requires_an_unhandled_signature(monkeypatch):
    """Classifier-gap promotion fires for a repeated unhandled signature and
    never for one a manifest row claims."""
    state = _fresh_state()
    now = 1761590000.0
    for i in range(remediation.UNKNOWN_PROMOTE_COUNT):
        remediation.record_failure(state, UNKNOWN_SIG, f"job-{i}", UNKNOWN_REASON, "needs-review", at=now + i)

    messages: list = []
    jobs: list = []
    result = remediation.check_gap_promotion(
        state,
        UNKNOWN_SIG,
        now=now + 60,
        post_message=lambda topic, body: messages.append((topic, body)),
        open_job=lambda **kw: jobs.append(kw) or "gap-job-1",
    )
    assert result["promoted"] is True
    assert messages and messages[0][0] == "doctor.classifier_gap"
    assert jobs and jobs[0]["status"] == "needs-review"
    assert jobs[0]["kind"].startswith("classifier-gap:")

    # A known signature is never promoted, no matter how often it recurs.
    known_sig = remediation.failure_signature("no token for host studio")
    remediation.record_failure(state, known_sig, "job-k", "no token for host studio", "retried", at=now)
    assert remediation.check_gap_promotion(state, known_sig, now=now + 1) == {
        "promoted": False,
        "reason": "signature already has a handler row",
    }


def test_process_triage_job_routes_unknown_signature_without_llm(monkeypatch):
    """The wiring marker at the process_triage_job level: an unknown failure
    is answered by the policy (auto_detected + signature tag, zero LLM
    calls). Remove the call site from doctor.py and this fails."""
    llm: list = []
    release = Mock(return_value=("released job-unknown-1", True))
    monkeypatch.setattr(doctor, "STATE", _fresh_state())
    _wire(monkeypatch, _failed_job(UNKNOWN_REASON), release, llm)

    result = doctor.process_triage_job(_triage_job())
    assert llm == []
    assert result["exit_code"] == 0
    assert result["auto_detected"] is True
    assert result["action"]["signature"] == UNKNOWN_SIG
    assert "unknown_signature" in result["stdout"]
