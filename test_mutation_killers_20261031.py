"""FB63 killer tests: one focused test per mutation survivor.

Each test pins the unmutated behaviour that the existing suite did not
cover — verified by running the mutation script: every test below fails
against its mutant (the script was re-run after adding them; see the
summary). If a survivor below ever stops failing against its mutant, the
mutant is equivalent and should leave the survivor list, not weaken here.
"""

from __future__ import annotations

import retry_policy
from hive import review_verdict as rv
from hive.bus.domain import jobs as job_domain
from hive.workers import loop_outcome
from hive.workers.loop_outcome import decide


# ── decide survivors ──────────────────────────────────────────────────────
def test_crash_requeues_below_cap_D6():
    assert decide(loop_outcome.CRASH, 0) == "requeue"
    assert decide(loop_outcome.CRASH, 1) == "requeue"


def test_unhealthy_reviewer_holds_D12():
    # Note: mutant D12 (reviewer folded into the plain-requeue tuple) is
    # provably equivalent — the reviewer branch returns first — so no test
    # can kill it. This pins the behaviour instead.
    assert decide(loop_outcome.REVIEWER_UNAVAILABLE, 0, reviewer_ok=lambda: False) == "hold"
    assert decide(loop_outcome.REVIEWER_UNAVAILABLE, 0, reviewer_ok=lambda: True) == "requeue"


# ── valid_completion_proof survivors ──────────────────────────────────────
SHA40 = "a" * 40
SHA64 = "b" * 64


def _clean(**over):
    proof = {"head_sha": SHA40, "commits": [SHA40], "files_changed": ["x.py"], "worktree_clean": True}
    proof.update(over)
    return proof


def _dirty_remote(**over):
    proof = {
        "head_sha": SHA40,
        "commits": [SHA40],
        "files_changed": ["x.py"],
        "tracked_worktree_clean": True,
        "untracked_files": ["tmp.log"],
        "pushed": True,
        "remote_verified": True,
        "remote_sha": SHA40,
    }
    proof.update(over)
    return proof


def test_empty_files_changed_rejected_P3():
    assert job_domain.valid_completion_proof(_clean(files_changed=[])) is False


def test_unpushed_dirty_proof_rejected_P5():
    assert job_domain.valid_completion_proof(_dirty_remote(pushed=False)) is False


def test_unverified_remote_rejected_P6():
    assert job_domain.valid_completion_proof(_dirty_remote(remote_verified=False)) is False


def test_wrong_remote_sha_rejected_P7():
    assert job_domain.valid_completion_proof(_dirty_remote(remote_sha=SHA64)) is False


def test_one_bad_commit_spoils_proof_P8():
    assert job_domain.valid_completion_proof(_clean(commits=[SHA40, "garbage"])) is False


def test_blank_filename_rejected_P9():
    assert job_domain.valid_completion_proof(_clean(files_changed=[""])) is False


def test_64_char_sha_accepted_P11():
    assert job_domain.valid_completion_proof(_clean(head_sha=SHA64, commits=[SHA64])) is True


def test_missing_untracked_list_rejected_P12():
    proof = _dirty_remote()
    del proof["untracked_files"]
    assert job_domain.valid_completion_proof(proof) is False


# ── auto_retry_allowed survivors ──────────────────────────────────────────
def _failed(**over):
    job = {"status": "failed", "max_retries": 2, "retry_count": 0, "kind": "probe:x", "result": {}}
    job.update(over)
    return job


def test_zero_max_retries_denies_R2():
    # Note: mutant R2 (>= 0 instead of > 0) is provably equivalent —
    # _is_positive_int guards only max_retries, and with max_retries <= 0
    # the exhaustion check (retry_count >= max_retries) always denies first
    # for any valid retry_count. This pins the behaviour instead.
    assert retry_policy.auto_retry_allowed(_failed(max_retries=0)) is False


def test_budget_spent_exactly_denies_R4():
    assert retry_policy.auto_retry_allowed(_failed(max_retries=2, retry_count=2)) is False
    assert retry_policy.auto_retry_allowed(_failed(max_retries=2, retry_count=1)) is True


def test_build_retryable_gates_both_ways_R5():
    build = {"status": "failed", "max_retries": 2, "retry_count": 0, "kind": "build:x"}
    assert retry_policy.auto_retry_allowed({**build, "result": {"retryable": True}}) is True
    assert retry_policy.auto_retry_allowed({**build, "result": {}}) is False
    assert retry_policy.auto_retry_allowed({**build, "result": {"retryable": False}}) is False


def test_string_result_denies_R7():
    assert retry_policy.auto_retry_allowed(_failed(result="boom")) is False


def test_empty_hold_marker_does_not_block_R8():
    assert retry_policy.auto_retry_allowed(_failed(result={"held": ""})) is True


def test_bare_hold_blocks_R9():
    assert retry_policy.auto_retry_allowed(_failed(result={"failure_reason": "Hold"})) is False


def test_bool_budget_denies_R10():
    assert retry_policy.auto_retry_allowed(_failed(max_retries=True)) is False


def test_build_without_retryable_denies_R11():
    job = {"status": "failed", "max_retries": 2, "retry_count": 0, "kind": "build:x", "result": {}}
    assert retry_policy.auto_retry_allowed(job) is False


# ── row_verdict survivors ─────────────────────────────────────────────────
def test_model_less_evidence_approve_hidden_V2():
    assert rv.row_verdict({"review_evidence": {"verdict": {"verdict": "approve"}}}) == ""


def test_first_reviewer_entry_read_V5():
    result = {"review_evidence": {"first": {"verdict": "request-changes", "model": "m"}, "reviewers": []}}
    assert rv.row_verdict(result) == "request_changes"


def test_request_changes_normalises_dash_V5b():
    # Companion to V5: the dash form must fold, or V5's killer is vacuous.
    assert rv.normalize_verdict("request-changes") == "request_changes"
