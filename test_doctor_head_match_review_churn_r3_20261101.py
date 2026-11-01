"""Head-match r3: the second Muse / DeepSeek review of the fail-closed line.

Base sonnet/opghm4b-20261101 (a15b464). Every item was checked against source
before it was fixed; each held item has a test that fails on a15b464.

MUSE
  1 HIGH   held   ``verdict_only_between`` returned True for ``tip == reviewed``
                  before asking git whether the commit exists: two identical
                  garbage strings were "verdict-only". The shortcut now needs a
                  full 40-hex sha the workspace holds.
  2 MED    held   ``merge_handoff`` evaluated ``_descent_proof`` twice (once in
                  ``_verdict_only_descendant_approval``, once for the
                  not-proven branch); the decision now rests on ONE proof.
DEEPSEEK
  3 HIGH   held   ``review_workspace_changes`` diffed EVERY local branch and
                  every ``origin/review/*`` ref: an unrelated local branch, or
                  another review job's verdict ref, poisoned the answer. Only
                  the job's own commits (``baseline..HEAD``), the author
                  branch and the job's OWN review ref are read now; a push onto
                  the author branch from any local ref is still caught.
  4 MED    partly held -- the guard is NOT skipped after a budget failure
                  (it always runs; pinned), but when it overrode the budget
                  reason, ``budget_exceeded`` was lost from the disposition.
                  Both reasons are now kept in ``result["failure_reasons"]``.
  5 MED    held   a self-reported ``files_changed`` refused a review git had
                  proven clean. With a readable workspace git is the answer in
                  both directions; the self report only adds doubt (to the
                  message) when git cannot answer.
"""

from __future__ import annotations

import pytest

import test_doctor_head_match_hardening_20261101 as hardening
from hive.workers import doctor
from hive.workers import doctor_head_match as dhm
from test_doctor_head_match_hardening_20261101 import (
    BRANCH,
    ENV,
    OWN_VERDICT,
    REVIEW_JOB_ID,
    Origin,
    _approved_row,
    _review_job,
    _violation,
    commit,
    git,
    write,
)


@pytest.fixture
def worker(monkeypatch):
    """The hardening suite's worker module (deployed sidecars stubbed), reused as-is."""
    return hardening.worker.__wrapped__(monkeypatch)


# ── 1: identical strings are only "the same head" when git holds the commit ──


def test_1_identical_garbage_is_never_verdict_only(tmp_path):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    for junk in ("garbage", "HEAD", "main", "z" * 40, "0" * 40, "f" * 40, "g" * 40):
        assert dhm.verdict_only_between(str(ws), junk, junk) is False, junk
    # A real commit, abbreviated, is not a full sha: no shortcut on a prefix.
    assert dhm.verdict_only_between(str(ws), origin.tip[:12], origin.tip[:12]) is False


def test_1_identical_valid_full_sha_is_verdict_only(tmp_path):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    assert dhm.verdict_only_between(str(ws), origin.tip, origin.tip) is True
    # ...but only in a workspace that holds it.
    other = tmp_path / "empty"
    git(tmp_path, "init", "-q", str(other))
    assert dhm.verdict_only_between(str(other), origin.tip, origin.tip) is False


# ── 2: merge_handoff decides on ONE descent proof ───────────────────────────


def _count_proofs(monkeypatch, answers):
    calls = []

    def proof(job, approved):
        calls.append(1)
        return answers[min(len(calls), len(answers)) - 1]

    monkeypatch.setattr(doctor, "_descent_proof", proof)
    return calls


def test_2_merge_handoff_uses_a_single_proof(monkeypatch):
    for answers, expected in (
        # The proof changed between the two calls: the FIRST answer is the decision.
        (["refused", "not_proven"], "stale_approval"),
        (["not_proven", "proven"], "approval_not_proven"),
        (["not_proven", "refused"], "approval_not_proven"),
        (["proven", "not_proven"], "ready_to_merge"),
    ):
        calls = _count_proofs(monkeypatch, answers)
        row = _approved_row("a" * 40, "b" * 40)
        assert doctor.merge_handoff(row) == expected, answers
        assert len(calls) == 1, (answers, len(calls))


# ── 3: unrelated refs in the review workspace never poison the answer ───────


def test_3_unrelated_refs_are_ignored(worker, tmp_path):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    # Unrelated: a local branch descending from the reviewed head with code...
    git(ws, "checkout", "-q", "-b", "scratch-unrelated")
    write(ws, "pkg/unrelated.py", "x = 1\n")
    commit(ws, "unrelated local work")
    # ...and ANOTHER review job's verdict ref on the same branch, fetched.
    git(ws, "checkout", "-q", "-B", "other-review", origin.tip)
    write(ws, "review-verdict-someone-else.json", "{}")
    commit(ws, "another review's verdict")
    git(ws, "push", "-q", "origin", f"HEAD:refs/heads/{worker.review_verdict_ref(BRANCH, 'someone-else')}")
    git(ws, "fetch", "-q", "origin")
    # The job's own commits: only its verdict.
    git(ws, "checkout", "-q", BRANCH)
    write(ws, OWN_VERDICT, "{}")
    commit(ws, "verdict")
    git(ws, "push", "-q", "origin", f"HEAD:{worker.review_verdict_ref(BRANCH, REVIEW_JOB_ID)}")
    git(ws, "fetch", "-q", "origin")

    changes = worker.review_workspace_changes(str(ws), origin.tip, BRANCH, ENV, job_id=REVIEW_JOB_ID)
    assert changes == [OWN_VERDICT]
    assert _violation(worker, _review_job(), {"files_changed": [OWN_VERDICT]}, ws, origin.tip) is None


def test_3_the_jobs_own_commits_and_refs_are_still_read(worker, tmp_path):
    origin = Origin(tmp_path)
    # Code on HEAD (the job's own commits) is overreach.
    ws = origin.review_workspace("ws-head")
    write(ws, "pkg/widget.py", "value = 'reviewer'\n")
    commit(ws, "reviewer code")
    reason = _violation(worker, _review_job(), {"files_changed": []}, ws, origin.tip)
    assert reason and "pkg/widget.py" in reason
    # Code pushed to the job's OWN review ref from a side branch is overreach too.
    ws = origin.review_workspace("ws-own-ref")
    git(ws, "checkout", "-q", "-b", "side")
    write(ws, "pkg/sneaky.py", "x = 1\n")
    commit(ws, "code on the review ref")
    git(ws, "push", "-q", "origin", f"HEAD:refs/heads/{worker.review_verdict_ref(BRANCH, REVIEW_JOB_ID)}")
    git(ws, "checkout", "-q", BRANCH)
    reason = _violation(worker, _review_job(), {"files_changed": []}, ws, origin.tip)
    assert reason and "pkg/sneaky.py" in reason
    # The author branch rewritten off the reviewed head is still reported.
    ws = origin.review_workspace("ws-rewrite")
    git(ws, "checkout", "-q", "-b", "rewrite", origin.base)
    write(ws, "pkg/other.py", "y = 2\n")
    commit(ws, "rewrite")
    git(ws, "push", "-q", "-f", "origin", f"rewrite:{BRANCH}")
    git(ws, "checkout", "-q", "--detach", origin.tip)
    reason = _violation(worker, _review_job(), {"files_changed": []}, ws, origin.tip)
    assert reason and "rewritten" in reason


# ── 4: the guard always runs, and both reasons survive ──────────────────────


def _overreaching_ws(origin):
    ws = origin.review_workspace("ws")
    write(ws, "pkg/widget.py", "value = 'reviewer'\n")
    commit(ws, "reviewer code")
    return ws


def test_4_guard_runs_after_a_budget_failure_and_keeps_both_reasons(worker, tmp_path):
    origin = Origin(tmp_path)
    ws = _overreaching_ws(origin)
    result = {"files_changed": [], "budget_exceeded": {"host": "h"}}
    status, reason = worker.apply_review_overreach_guard(
        _review_job(), result, "failed", "budget_exceeded", workdir=str(ws), baseline=origin.tip, env=ENV
    )
    # Not requeued as if nothing happened: the overreach is the disposition...
    assert (status, reason) == ("failed", "review_overreach")
    assert result["review_overreach"].startswith("review_overreach")
    # ...and the budget failure is not lost.
    assert result["failure_reasons"] == ["budget_exceeded", "review_overreach"]
    assert result["budget_exceeded"] == {"host": "h"}


def test_4_a_harder_failure_keeps_its_disposition_and_records_both(worker, tmp_path):
    origin = Origin(tmp_path)
    ws = _overreaching_ws(origin)
    for earlier in ("protection_violation", "ai_attribution_trailer"):
        result = {"files_changed": []}
        status, reason = worker.apply_review_overreach_guard(
            _review_job(), result, "failed", earlier, workdir=str(ws), baseline=origin.tip, env=ENV
        )
        assert (status, reason) == ("failed", earlier)
        assert result["failure_reasons"] == [earlier, "review_overreach"]
        assert result["review_overreach"].startswith("review_overreach")


# ── 5: git wins in both directions when it can answer ───────────────────────


def test_5_git_clean_outranks_a_dirty_self_report(worker, tmp_path):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    assert _violation(worker, _review_job(), {"files_changed": ["pkg/widget.py"]}, ws, origin.tip) is None


def test_5_git_dirty_outranks_a_clean_self_report(worker, tmp_path):
    origin = Origin(tmp_path)
    ws = _overreaching_ws(origin)
    reason = _violation(worker, _review_job(), {"files_changed": []}, ws, origin.tip)
    assert reason and "pkg/widget.py" in reason


def test_5_the_self_report_only_adds_doubt_when_git_cannot_answer(worker, tmp_path):
    origin = Origin(tmp_path)
    reason = _violation(worker, _review_job(), {"files_changed": ["pkg/widget.py"]}, None, origin.tip)
    assert reason and "not proven" in reason and "pkg/widget.py" in reason
    # And a clean self report never makes an unreadable workspace acceptable.
    reason = _violation(worker, _review_job(), {"files_changed": []}, None, origin.tip)
    assert reason and "not proven" in reason
