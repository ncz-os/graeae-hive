"""Head-match r4: the third Muse / DeepSeek review of the fail-closed line.

Base sonnet/opghm5b-20261101 (b7f5642). Every item was checked against source
before it was fixed; each held item has a test that fails on b7f5642, and an
item that did not hold as stated carries a pinning test.

MUSE
  1 HIGH   held   ``classify_descendant`` answered ``verdict_only`` for
                  ``reviewed == tip`` after only ``has_commit``: an identical
                  ref NAME (``HEAD``, the branch) or an abbreviation was an
                  approval. The identical case now goes through
                  ``verdict_only_between`` (full 40-hex AND held here).
  2 HIGH   held   ``_files_from_git(...) or non_artifact_files(pushed_files)``
                  let the self report replace git's proven-empty answer. git
                  ``[]`` is now the answer; only a git FAILURE (``None``) lets
                  the self report add doubt.
DEEPSEEK
  3 HIGH   not held as stated -- a missing ``refs/remotes/origin/<branch>``
                  did not fail every review (pinned: a clean review is still
                  accepted). The REVERSE held: the author ref was silently
                  skipped, so a push onto the author branch went unseen. It is
                  now resolved (tracking ref, explicit fetch, recorded head)
                  and, if none resolves, the review is ``review_unprovable``.
  4 MED    held   the doctor brief and the worker spelled the ref / artifact
                  name separately and disagreed (padded, slash-wrapped and
                  punctuation-only branches). Both now use ``review_queue``.
  5 MED    held   ``_review_own_artifact`` accepted the PARENT job's verdict
                  name on the author branch; only this job's names now.
"""

from __future__ import annotations

import pytest

import test_doctor_head_match_hardening_20261101 as hardening
from hive import review_queue
from hive.workers import doctor
from hive.workers import doctor_head_match as dhm
from test_doctor_head_match_hardening_20261101 import (
    BRANCH,
    ENV,
    OWN_VERDICT,
    REVIEW_JOB_ID,
    WORK_JOB_ID,
    Origin,
    _review_job,
    _violation,
    commit,
    fail_diffs,
    git,
    write,
)


@pytest.fixture
def worker(monkeypatch):
    """The hardening suite's worker module (deployed sidecars stubbed), reused as-is."""
    return hardening.worker.__wrapped__(monkeypatch)


# ── 1: an identical head is validated exactly like verdict_only_between ─────


def test_1_identical_names_that_are_not_full_shas_are_never_verdict_only(tmp_path):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    # Every one of these names a commit this workspace holds (has_commit is
    # True), but none is a full sha: an identical ref name is not a head.
    for name in ("HEAD", BRANCH, f"origin/{BRANCH}", origin.tip[:12], origin.tip[:7]):
        assert dhm.has_commit(str(ws), name), name
        result = dhm.classify_descendant(str(ws), name, name)
        assert result.verdict != dhm.VERDICT_ONLY, name
        assert not result.ok_for_tip, name
        assert result.verdict == dhm.NOT_DESCENDANT, name


def test_1_identical_garbage_through_the_classifier(tmp_path):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    for junk in ("garbage", "z" * 40, "0" * 40, "f" * 40, "g" * 40):
        result = dhm.classify_descendant(str(ws), junk, junk)
        assert not result.ok_for_tip and result.verdict != dhm.VERDICT_ONLY, junk


def test_1_identical_valid_full_sha_through_the_classifier(tmp_path):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    result = dhm.classify_descendant(str(ws), origin.tip, origin.tip)
    assert result.verdict == dhm.VERDICT_ONLY and result.ok_for_tip
    # Upper-case hex is still a full sha.
    assert dhm.classify_descendant(str(ws), origin.tip.upper(), origin.tip.upper()).ok_for_tip is (
        dhm.verdict_only_between(str(ws), origin.tip.upper(), origin.tip.upper())
    )
    # ...and never in a workspace that does not hold it.
    other = tmp_path / "empty"
    git(tmp_path, "init", "-q", str(other))
    assert not dhm.classify_descendant(str(other), origin.tip, origin.tip).ok_for_tip


# ── 2: git's empty answer is an answer; only a git failure admits the report ─


def _side(ws, origin, name, text, message):
    """A head that does NOT descend from the delivered tip, committing ``name``."""
    git(ws, "checkout", "-q", "-B", "side", origin.base)
    write(ws, name, text)
    return commit(ws, message)


def test_2_files_from_git_distinguishes_empty_from_failure(tmp_path, monkeypatch):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    git(ws, "commit", "-q", "--allow-empty", "-m", "empty")
    empty = git(ws, "rev-parse", "HEAD")
    assert dhm._files_from_git(str(ws), empty) == []
    write(ws, OWN_VERDICT, "{}")
    verdict = commit(ws, "verdict")
    assert dhm._files_from_git(str(ws), verdict) == []
    write(ws, "pkg/x.py", "x = 1\n")
    code = commit(ws, "code")
    assert dhm._files_from_git(str(ws), code) == ["pkg/x.py"]
    fail_diffs(monkeypatch)
    assert dhm._files_from_git(str(ws), code) is None


def test_2_git_proves_empty_and_the_self_report_cannot_replace_it(tmp_path):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    side = _side(ws, origin, OWN_VERDICT, "{}", "verdict only, off the tip")
    result = dhm.classify_descendant(str(ws), origin.tip, side, pushed_files=["pkg/widget.py"])
    # git proved the reviewed commit changed no code: not overreach, whatever
    # the child says -- and still never an approval of the tip.
    assert result.verdict == dhm.NOT_DESCENDANT
    assert result.files == []
    assert not result.ok_for_tip and not result.newer_work
    # The report is surfaced as doubt in the message, never as the evidence.
    assert "pkg/widget.py" in result.reason


def test_2_git_fails_and_the_self_report_adds_doubt(tmp_path, monkeypatch):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    side = _side(ws, origin, OWN_VERDICT, "{}", "verdict only, off the tip")
    fail_diffs(monkeypatch)
    result = dhm.classify_descendant(str(ws), origin.tip, side, pushed_files=["pkg/widget.py"])
    assert result.verdict == dhm.OVERREACH and result.files == ["pkg/widget.py"]
    assert not result.ok_for_tip
    assert "not proven" in result.reason
    # A git failure with a clean report is not an approval and says why.
    result = dhm.classify_descendant(str(ws), origin.tip, side, pushed_files=[])
    assert result.verdict == dhm.NOT_DESCENDANT and not result.ok_for_tip
    assert "not proven" in result.reason


def test_2_child_reports_code_and_git_agrees(tmp_path):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    side = _side(ws, origin, "pkg/schema.py", "x = 1\n", "reviewer code off the tip")
    for report in (["pkg/schema.py"], [], ["pkg/other.py"]):
        result = dhm.classify_descendant(str(ws), origin.tip, side, pushed_files=report)
        assert result.verdict == dhm.OVERREACH, report
        # git's evidence, not the report, names the files.
        assert result.files == ["pkg/schema.py"], report
        assert not result.ok_for_tip


# ── 3: the author ref is resolved; unresolvable is "unprovable" ─────────────


def _drop_tracking_ref(ws):
    git(ws, "update-ref", "-d", f"refs/remotes/origin/{BRANCH}")


def test_3_pin_a_missing_tracking_ref_does_not_fail_a_clean_review(worker, tmp_path):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    _drop_tracking_ref(ws)
    assert _violation(worker, _review_job(), {"files_changed": []}, ws, origin.tip) is None


def test_3_a_missing_tracking_ref_is_resolved_by_an_explicit_fetch(worker, tmp_path):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    # Code pushed onto the author branch from a side branch, then the
    # remote-tracking ref is gone (a detached / single-ref checkout).
    git(ws, "checkout", "-q", "-b", "scratch")
    write(ws, "migrations/0009.sql", "ALTER TABLE jobs ADD COLUMN y;\n")
    commit(ws, "reviewer schema fix")
    git(ws, "push", "-q", "origin", f"scratch:{BRANCH}")
    git(ws, "checkout", "-q", "--detach", origin.tip)
    _drop_tracking_ref(ws)
    reason = _violation(worker, _review_job(), {"files_changed": []}, ws, origin.tip)
    assert reason and reason.startswith("review_overreach") and "migrations/0009.sql" in reason


def test_3_the_recorded_delivered_head_is_the_last_fallback(worker, tmp_path):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    _drop_tracking_ref(ws)
    git(ws, "remote", "set-url", "origin", str(tmp_path / "no-such-remote.git"))
    # Neither the tracking ref nor a fetch resolves: the recorded head does.
    job = _review_job(head_sha=origin.tip)
    assert _violation(worker, job, {"files_changed": []}, ws, origin.tip) is None
    # A recorded head that is not a full sha the workspace holds is no fallback.
    for recorded in (None, origin.tip[:12], "f" * 40, "HEAD"):
        reason = _violation(worker, _review_job(head_sha=recorded), {"files_changed": []}, ws, origin.tip)
        assert reason and reason.startswith("review_unprovable"), (recorded, reason)


def test_3_unresolvable_author_ref_is_unprovable_not_approved_not_overreach(worker, tmp_path):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    _drop_tracking_ref(ws)
    git(ws, "remote", "set-url", "origin", str(tmp_path / "no-such-remote.git"))
    result = {"files_changed": ["pkg/widget.py"]}
    reason = _violation(worker, _review_job(), result, ws, origin.tip)
    assert reason and reason.startswith("review_unprovable")
    assert "author branch" in reason
    # Not overreach-with-files: no file is claimed as changed.
    assert "changed pkg/widget.py" not in reason
    status, failure = worker.apply_review_overreach_guard(
        _review_job(), result, "done", None, workdir=str(ws), baseline=origin.tip, env=ENV
    )
    assert (status, failure) == ("failed", "review_unprovable")
    assert result["failure_reasons"] == ["review_unprovable"]
    # The doctor still never counts this child's verdict as an approve.
    child = {"id": REVIEW_JOB_ID, "status": "failed", "result": {**result, "verdict": {"verdict": "approve"}}}
    outcome = doctor._review_child_outcome(child, head_sha=origin.tip)
    assert outcome and outcome["kind"] == "reviewer_failure"


# ── 4: one helper spells the ref and the artifact for doctor and worker ─────


BRANCHES = (
    BRANCH,
    "hv/a/b/c",
    "hv/fo.merge-pass.r3",
    "feature/x.y/z",
    "hv/",
    " hv/padded ",
    "/hv/wrapped/",
    "@@@",
)


@pytest.mark.parametrize("branch", BRANCHES)
def test_4_doctor_brief_and_worker_ref_agree(worker, monkeypatch, branch):
    captured = {}
    monkeypatch.setattr(doctor, "submit_job_spec", lambda spec: (captured.update(spec), (201, {"id": "x"}))[1])
    monkeypatch.setattr(doctor, "_review_child_refusal", lambda job: "")
    row = {
        "id": WORK_JOB_ID,
        "kind": "fix:fo-merge-pass",
        "status": "needs-review",
        "head_branch": branch,
        "result": {"head_sha": "e0bb983" + "0" * 33},
    }
    doctor._review_queue_submit(row, {"author": "qwen38"}, [{"id": "r", "model_id": "gpt-oss-20b"}], now=1.0)
    desc = captured["description"]
    spec_branch = captured["head_branch"]
    ref = worker.review_verdict_ref(spec_branch, REVIEW_JOB_ID)
    prefix = ref[: -len(REVIEW_JOB_ID)]
    assert f"ref {prefix}(" in desc, (prefix, desc)
    artifact = worker.review_verdict_paths(spec_branch, REVIEW_JOB_ID)[0]
    assert artifact in desc, (artifact, desc)
    assert artifact == review_queue.review_verdict_paths(spec_branch, REVIEW_JOB_ID)[0]
    assert ref == review_queue.review_verdict_ref(spec_branch, REVIEW_JOB_ID)
    # The name the doctor told the reviewer to write is one the worker accepts.
    assert worker._review_own_artifact(artifact, _review_job(head_branch=spec_branch))


# ── 5: only THIS review job's own artifact name ─────────────────────────────


def test_5_the_parent_jobs_verdict_name_is_rejected(worker, tmp_path):
    origin = Origin(tmp_path)
    job = _review_job()
    assert not worker._review_own_artifact(f"review-verdict-{WORK_JOB_ID}.json", job)
    assert not worker._review_own_artifact(f".hive/review/review-verdict-{WORK_JOB_ID}.json", job)
    assert worker._review_own_artifact(f"review-verdict-{REVIEW_JOB_ID}.json", job)
    assert worker._review_own_artifact(OWN_VERDICT, job)
    # A job without its own id never falls back to the parent's id.
    no_id = _review_job(id=None)
    assert not worker._review_own_artifact(f"review-verdict-{WORK_JOB_ID}.json", no_id)
    # End to end: the parent's name committed on the author branch is refused.
    ws = origin.review_workspace("ws")
    name = f"review-verdict-{WORK_JOB_ID}.json"
    write(ws, name, "{}")
    commit(ws, "verdict under the parent's name")
    reason = _violation(worker, job, {"files_changed": [name]}, ws, origin.tip)
    assert reason and reason.startswith("review_overreach") and name in reason
