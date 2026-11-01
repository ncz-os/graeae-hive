"""Head-match and the never-review-a-review guard, together (merge 2026-11-01).

``hv/gh-review-head-match-r2-20261031`` taught the review queue that an approve
signed at a VERDICT-ONLY descendant of the delivered head covers that head
(the reviewer only added its verdict artifact). ``deploy/integrate-20261017``
taught it that a review job is never itself reviewed (PYTHIA 2026-11-01: the
ten-deep review-of-review chain). Both live in the same per-row path
(``process_awaiting_review_row``) and the same submit path
(``_review_queue_submit``), so the merge must keep both:

(a) a WORK row whose reviewed head is a verdict-only ancestor-descendant pair
    (tip T, reviewed R = T + one verdict json) is approved on T -- judged by the
    real git classifier against real repos in tmp dirs, not a stub -- and the
    merge pass reads it as ready to merge;
(b) a REVIEW-kind row carrying the very same shape is refused as a review
    target: the descendant rule never judges it, nothing is written, no review
    child is submitted, and a sweep over both rows clears only the work row.

Each parent fails half of this: the head-match branch has no review-kind guard,
the integrate branch has no descendant rule.
"""

from __future__ import annotations

import json
import os
import subprocess

import pytest

from hive import review_queue
from hive.workers import doctor
from hive.workers import doctor_head_match as dhm

ENV = {
    **os.environ,
    "GIT_AUTHOR_NAME": "Jason Perlow",
    "GIT_AUTHOR_EMAIL": "jperlow@gmail.com",
    "GIT_COMMITTER_NAME": "Jason Perlow",
    "GIT_COMMITTER_EMAIL": "jperlow@gmail.com",
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_CONFIG_GLOBAL": os.devnull,
}

BRANCH = "hv/bl-ct-dialect"
VERDICT_FILES = ("REVIEW_VERDICT_hv-bl-ct-dialect.json", "review-verdict-review-1.json")
WORK_ID = "01a19270-work"
REVIEW_ROW_ID = "01a19300-review"


def git(repo, *args):
    out = subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True, env=ENV)
    return out.stdout.strip()


def write(repo, name, text):
    path = os.path.join(str(repo), name)
    os.makedirs(os.path.dirname(path) or str(repo), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


def commit(repo, message):
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", message)
    return git(repo, "rev-parse", "HEAD")


@pytest.fixture
def verdict_only_descendant(tmp_path):
    """The author tip T and the review child's workspace holding R = T + verdict json only."""
    author = tmp_path / "author"
    author.mkdir()
    git(author, "init", "-q", "-b", "main")
    write(author, "pkg/widget.py", "value = 1\n")
    commit(author, "base")
    git(author, "checkout", "-q", "-b", BRANCH)
    write(author, "pkg/widget.py", "value = 2\n")
    tip = commit(author, "author work")
    review = tmp_path / "review-1-workspace"
    git(tmp_path, "clone", "-q", "-b", BRANCH, str(author), str(review))
    for name in VERDICT_FILES:
        write(review, name, json.dumps({"verdict": "approve"}))
    reviewed = commit(review, "review verdict")
    return {"tip": tip, "reviewed": reviewed, "review_workspace": str(review), "root": tmp_path / "workspaces"}


@pytest.fixture
def bus(monkeypatch, verdict_only_descendant):
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_ADOPT_DRY_RUN", False)
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_VERDICT_DESCENDANT", True)
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_HEAD_MATCH_DRY_RUN", False)
    # Never guess workspaces under the operator's real root.
    os.makedirs(verdict_only_descendant["root"], exist_ok=True)
    monkeypatch.setattr(doctor, "_WORKSPACE_ROOT", str(verdict_only_descendant["root"]))
    messages = []
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda topic, body: messages.append((topic, body)))

    class Bus:
        def __init__(self):
            self.calls = []
            self.patches = []
            self.children = {}
            self.rows = {}

        def http(self, method, path, body=None, **kwargs):
            self.calls.append((method, path, body))
            if method == "GET" and "parent_job_id=" in path:
                parent = path.split("parent_job_id=", 1)[1].split("&", 1)[0]
                return 200, {"jobs": [c for c in self.children.values() if c.get("parent_job_id") == parent]}
            if method == "GET" and path.startswith("/v1/jobs/"):
                jid = path.rsplit("/", 1)[-1]
                return (200, self.rows[jid]) if jid in self.rows else (404, {})
            if method == "PATCH":
                self.patches.append((path, body))
                jid = path.rsplit("/", 1)[-1]
                self.rows.setdefault(jid, {}).update(body or {})
                return 200, {}
            if method == "POST":
                return 201, {"id": "should-not-be-submitted"}
            return 200, {}

        def posts(self):
            return [c for c in self.calls if c[0] == "POST"]

        def patched(self, jid):
            return [p for p in self.patches if p[0].rsplit("/", 1)[-1] == jid]

    fake = Bus()
    monkeypatch.setattr(doctor, "_http", fake.http)
    return fake


def _awaiting_row(jid, kind, tip, *, child, parent=None):
    row = {
        "id": jid,
        "kind": kind,
        "status": "needs-review",
        "repo": "graeae-hive",
        "head_branch": BRANCH,
        "base_branch": "main",
        "head_sha": tip,
        "claimed_model": "qwen38",
        "result": {
            "head_sha": tip,
            "commits": [tip],
            "head_branch": BRANCH,
            "review_state": {
                "state": "awaiting_review",
                "author": "qwen38",
                "author_family": "alibaba",
                "attempt": 1,
                "max_attempts": 3,
                "review_job_id": child,
                "tried": [],
            },
        },
    }
    if parent:
        row["parent_job_id"] = parent
    return row


def _finished_child(bus, child_id, parent_id, reviewed, workspace):
    entry = {"model": "cerberus-reviewer", "verdict": "approve"}
    bus.children[child_id] = {
        "id": child_id,
        "kind": "review:fix:bl-ct-dialect-20261101",
        "status": "done",
        "parent_job_id": parent_id,
        "workdir": workspace,
        "result": {
            "reviewers": [entry],
            "head_sha": reviewed,
            "review_evidence": {"reviewers": [entry]},
            "files_changed": list(VERDICT_FILES),
            "commits": [reviewed],
        },
    }


@pytest.fixture
def classified(monkeypatch):
    """The REAL classifier, observed: which rows did the descendant rule judge?"""
    seen = []
    real = doctor.classify_review_head

    def spy(job, tip, reviewed, **kwargs):
        seen.append(str(job.get("id")))
        return real(job, tip, reviewed, **kwargs)

    monkeypatch.setattr(doctor, "classify_review_head", spy)
    return seen


def test_a_work_row_reviewed_at_a_verdict_only_descendant_is_approved_on_its_head(
    bus, verdict_only_descendant, classified
):
    tip, reviewed = verdict_only_descendant["tip"], verdict_only_descendant["reviewed"]
    assert tip != reviewed
    # The real git answer the doctor will get: R descends from T by a verdict only.
    assert dhm.classify_descendant(verdict_only_descendant["review_workspace"], tip, reviewed).verdict == (
        dhm.VERDICT_ONLY
    )
    work = _awaiting_row(WORK_ID, "fix:bl-ct-dialect", tip, child="review-1")
    _finished_child(bus, "review-1", WORK_ID, reviewed, verdict_only_descendant["review_workspace"])
    # The guard lets a work row through: it is the review TARGET, not a review.
    assert review_queue.review_chain_refusal(work, lambda _jid: None) == ""

    outcome, state, _ = doctor.process_awaiting_review_row(work, now=2000.0)

    assert outcome == "approved_mergeable", (outcome, state)
    assert classified == [WORK_ID]
    assert state["approved"]["head_sha"] == tip
    assert state["approved"]["approved_sha"] == reviewed
    assert state["approved"]["basis"] == dhm.VERDICT_ONLY
    body = bus.rows[WORK_ID]
    assert body["status"] == "needs-review"
    assert body["result"]["review_state"]["state"] == "approved_mergeable"
    # The merge pass reads the written row as approved ON the delivered head.
    assert doctor.merge_handoff(body) == "ready_to_merge"
    # Approving is not a submit: no review child was created for anything.
    assert bus.posts() == []


def test_a_review_kind_row_with_the_same_shape_is_still_refused_as_a_review_target(
    bus, verdict_only_descendant, classified, caplog
):
    tip, reviewed = verdict_only_descendant["tip"], verdict_only_descendant["reviewed"]
    review_row = _awaiting_row(
        REVIEW_ROW_ID, "review:fix:bl-ct-dialect-20261101", tip, child="review-2", parent=WORK_ID
    )
    _finished_child(bus, "review-2", REVIEW_ROW_ID, reviewed, verdict_only_descendant["review_workspace"])

    assert review_queue.is_review_kind(review_row)
    assert review_queue.review_chain_refusal(review_row, lambda _jid: None) == review_queue.REFUSE_REVIEW_OF_REVIEW

    outcome, state, _ = doctor.process_awaiting_review_row(review_row, now=2000.0)

    # Refused BEFORE the descendant rule: a verdict-only approve does not turn a
    # review job into an approved review target.
    assert outcome == "skipped_review_job", (outcome, state)
    assert classified == []
    assert bus.patched(REVIEW_ROW_ID) == []
    # Neither submit path creates a review child under it.
    with pytest.raises(RuntimeError, match="refused review child"):
        doctor._review_queue_submit(review_row, review_row["result"]["review_state"], [], now=2000.0)
    assert doctor.ensure_review_followup(review_row) == "skipped_review_job"
    assert bus.posts() == []


def test_one_sweep_clears_the_work_row_and_leaves_the_review_row_alone(bus, verdict_only_descendant, classified):
    tip, reviewed = verdict_only_descendant["tip"], verdict_only_descendant["reviewed"]
    work = _awaiting_row(WORK_ID, "fix:bl-ct-dialect", tip, child="review-1")
    review_row = _awaiting_row(
        REVIEW_ROW_ID, "review:fix:bl-ct-dialect-20261101", tip, child="review-2", parent=WORK_ID
    )
    _finished_child(bus, "review-1", WORK_ID, reviewed, verdict_only_descendant["review_workspace"])
    _finished_child(bus, "review-2", REVIEW_ROW_ID, reviewed, verdict_only_descendant["review_workspace"])

    summary = doctor.run_review_queue_sweep(rows=[work, review_row], now=2000.0)

    assert summary["approved_mergeable"] == 1, summary
    assert classified == [WORK_ID]
    assert bus.rows[WORK_ID]["result"]["review_state"]["state"] == "approved_mergeable"
    assert bus.patched(REVIEW_ROW_ID) == []
    assert bus.posts() == []
