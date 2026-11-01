"""Head-match is an approval gate: every unprovable answer must fail CLOSED.

The live churn this cures (hv/fo-merge-pass-r3, 2026-11-01): work 01a195bf ->
review 01a197c2 (head e0bb983) -> 69de2e7f (head aa73bb5, the review's own
verdict commit) -> 40db61c9, every one submitted by the doctor. A review child
pushed its verdict commit onto the author's branch, the branch head advanced,
and the doctor reviewed "the new head" again, forever. Head-match makes a
verdict-only descendant of the reviewed head count as the same head -- which is
only safe if nothing it cannot PROVE is ever read as "verdict-only".

Two independent reviews (Muse, DeepSeek) of the head-match line, each item
checked against source here. Items that held carry a test that fails on the
pre-fix tree (0fb8074); items that did not hold carry a pinning test.

MUSE
  M1 held     worker guard read result.files_changed only (an unreadable git
              state left it [] = "clean"; a push from another local ref never
              showed up): the changed files are now derived from git.
  M2 held     any REVIEW_VERDICT_*/review-verdict-* name, in any directory,
              was allowed: only this job's own names, in the review dirs.
  M3 held     ``_child_workspace`` joined an unchecked job id onto the root.
  M4 held     basename fallback + ``lstrip('./')`` (a character set).
  M5 held     ``changed_files`` returned [] on a git failure and the
              classifier read that as "verdict only".
DEEPSEEK
  D1 not held as stated (nothing after the guard assigns failure_reason);
              the REVERSE held: the guard replaced an earlier, harder reason
              (protection_violation, attribution). Both are pinned.
  D2 held     the doctor brief carried literal ``<job id>`` placeholders and
              the worker never appended the real ref; the generic delivery
              policy told a review job to push its checked-out (author) branch.
              The ref itself never collides with the author branch (pinned).
  D3 held     no workspace => the recorded basis was trusted.
  D4 not held ``review_queue.review_chain_refusal`` / ``REFUSE_REVIEW_OF_REVIEW``
              exist on this branch (pinned).
FOUND WIRING THE E2E
  a FAILED review child that still carried a structured ``verdict`` was
  parsed by the doctor as an approve: a refused (overreach) review is now
  never counted.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import uuid
from pathlib import Path

import pytest

from hive import review_queue
from hive.workers import doctor
from hive.workers import doctor_head_match as dhm

ROOT = Path(__file__).resolve().parent

ENV = {
    **os.environ,
    "GIT_AUTHOR_NAME": "Jason Perlow",
    "GIT_AUTHOR_EMAIL": "jperlow@gmail.com",
    "GIT_COMMITTER_NAME": "Jason Perlow",
    "GIT_COMMITTER_EMAIL": "jperlow@gmail.com",
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_CONFIG_GLOBAL": os.devnull,
}

BRANCH = "hv/fo-merge-pass-r3"
SLUG = "hv-fo-merge-pass-r3"
OWN_VERDICT = f"REVIEW_VERDICT_{SLUG}.json"


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


class Origin:
    """A bare ARGONAS stand-in holding the author branch at its delivered tip."""

    def __init__(self, root: Path):
        self.root = root
        self.bare = root / "origin.git"
        subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(self.bare)], check=True, env=ENV)
        author = root / "author"
        git(root, "clone", "-q", str(self.bare), str(author))
        git(author, "checkout", "-q", "-b", "main")
        write(author, "pkg/widget.py", "value = 1\n")
        self.base = commit(author, "base")
        git(author, "push", "-q", "origin", "main")
        git(author, "checkout", "-q", "-b", BRANCH)
        write(author, "pkg/widget.py", "value = 2\n")
        self.tip = commit(author, "author work")
        git(author, "push", "-q", "-u", "origin", BRANCH)
        self.author = author

    def review_workspace(self, name: str) -> Path:
        """A review job's workspace: the worker clones and checks out the author branch."""
        path = self.root / name
        git(self.root, "clone", "-q", "-b", BRANCH, str(self.bare), str(path))
        return path


def fail_diffs(monkeypatch):
    """Make every ``git diff`` the classifier asks fail; every other question still answers."""
    real = dhm._git

    def flaky(repo, *args):
        if args and args[0] == "diff":
            return 128, ""
        return real(repo, *args)

    monkeypatch.setattr(dhm, "_git", flaky)


# ── M4: the verdict artifact is a NAME in a REVIEW DIRECTORY, nothing else ──


def test_m4_a_code_adjacent_verdict_name_is_not_a_verdict_artifact():
    # Allowed: repo root and .hive/review/, either spelling, with or without "./".
    for path in (
        "REVIEW_VERDICT_x.json",
        "review-verdict-x.json",
        ".hive/review/REVIEW_VERDICT_x.json",
        ".hive/review/review-verdict-x.json",
        "./REVIEW_VERDICT_x.json",
        "./.hive/review/review-verdict-x.json",
    ):
        assert dhm.is_verdict_artifact(path), path
    # A verdict-shaped name anywhere else hides a change: never an artifact.
    for path in (
        "src/foo/review-verdict-x.json",
        "tests/REVIEW_VERDICT_x.json",
        "nested/review-verdict-x.json",
        ".github/REVIEW_VERDICT_x.json",
        "hive/review/REVIEW_VERDICT_x.json",
        ".hive/review/sub/REVIEW_VERDICT_x.json",
        # ``*`` must not cross a directory boundary.
        "REVIEW_VERDICT_dir/src/code.json",
        ".hive/review/REVIEW_VERDICT_a/b.json",
    ):
        assert not dhm.is_verdict_artifact(path), path


def test_m4_only_a_literal_leading_dot_slash_is_stripped():
    # ``lstrip('./')`` strips a character SET: ".github/x" became "github/x".
    assert dhm.normalize_path(".github/x") == ".github/x"
    assert dhm.normalize_path("./.github/x") == ".github/x"
    assert dhm.normalize_path("./.hive/review/REVIEW_VERDICT_x.json") == ".hive/review/REVIEW_VERDICT_x.json"
    assert dhm.normalize_path("..hidden") == "..hidden"


def test_m4_a_review_that_commits_a_nested_verdict_name_is_not_verdict_only(tmp_path):
    origin = Origin(tmp_path)
    review = origin.review_workspace("review")
    write(review, "src/foo/review-verdict-x.json", '{"hidden": "change"}')
    reviewed = commit(review, "a code-adjacent json")
    result = dhm.classify_descendant(str(review), origin.tip, reviewed)
    assert result.verdict != dhm.VERDICT_ONLY and not result.ok_for_tip
    assert dhm.verdict_only_between(str(review), origin.tip, reviewed) is False


# ── M5: a git failure is "not proven", never "no files changed" ─────────────


def test_m5_changed_files_reports_a_git_failure_explicitly(tmp_path, monkeypatch):
    origin = Origin(tmp_path)
    review = origin.review_workspace("review")
    # An empty successful diff is a real, empty answer.
    assert dhm.changed_files(str(review), origin.tip, origin.tip) == []
    fail_diffs(monkeypatch)
    assert dhm.changed_files(str(review), origin.base, origin.tip) is None


def test_m5_classifier_does_not_read_a_failed_diff_as_verdict_only(tmp_path, monkeypatch):
    origin = Origin(tmp_path)
    review = origin.review_workspace("review")
    write(review, "pkg/widget.py", "value = 'reviewer edit'\n")
    reviewed = commit(review, "code on top of the tip")
    fail_diffs(monkeypatch)
    result = dhm.classify_descendant(str(review), origin.tip, reviewed)
    assert result.verdict != dhm.VERDICT_ONLY
    assert not result.ok_for_tip
    # Unproven content is never fast-forwarded either.
    assert not result.newer_work
    assert "not proven" in result.reason


def test_m5_files_from_git_and_verdict_only_between_fail_closed(tmp_path, monkeypatch):
    origin = Origin(tmp_path)
    review = origin.review_workspace("review")
    write(review, OWN_VERDICT, "{}")
    reviewed = commit(review, "verdict")
    assert dhm.verdict_only_between(str(review), origin.tip, reviewed) is True
    fail_diffs(monkeypatch)
    assert dhm._files_from_git(str(review), reviewed) is None
    assert dhm.verdict_only_between(str(review), origin.tip, reviewed) is False
    # The overreach branch: a non-descendant whose diff cannot be read is not counted.
    git(review, "checkout", "-q", "-B", "side", origin.base)
    write(review, "pkg/side.py", "x = 1\n")
    side = commit(review, "side")
    assert dhm.classify_descendant(str(review), origin.tip, side).ok_for_tip is False


# ── M3: the job-id fallback never leaves the workspace root ─────────────────


def test_m3_child_workspace_job_id_fallback_cannot_escape_the_root(tmp_path, monkeypatch):
    root = tmp_path / "workspaces"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setattr(doctor, "_WORKSPACE_ROOT", str(root))
    assert doctor._child_workspace({"id": "../outside"}) is None
    assert doctor._child_workspace({"id": str(outside)}) is None
    # A uuid-shaped id that is a SYMLINK out of the root is not a workspace either.
    linked = str(uuid.uuid4())
    os.symlink(str(outside), str(root / linked))
    assert doctor._child_workspace({"id": linked}) is None
    # A non-uuid id is not a worker workspace name.
    (root / "review-1").mkdir()
    assert doctor._child_workspace({"id": "review-1"}) is None
    # The real layout still resolves.
    good = str(uuid.uuid4())
    (root / good).mkdir()
    assert doctor._child_workspace({"id": good}) == os.path.realpath(str(root / good))


# ── D3: no workspace => not proven ──────────────────────────────────────────


def _approved_row(tip, reviewed, row_id="01a195bf-0000-7000-8000-000000000001"):
    return {
        "id": row_id,
        "kind": "fix:fo-merge-pass",
        "status": "needs-review",
        "head_branch": BRANCH,
        "head_sha": tip,
        "result": {
            "head_sha": tip,
            "review_state": {
                "state": "approved_mergeable",
                "author": "qwen38",
                "author_family": "alibaba",
                "approved": {
                    "reviewers": ["cerberus-reviewer"],
                    "head_sha": tip,
                    "approved_sha": reviewed,
                    "basis": dhm.VERDICT_ONLY,
                },
            },
        },
    }


def test_d3_merge_handoff_with_no_workspace_is_not_proven(tmp_path, monkeypatch):
    monkeypatch.setattr(doctor, "_WORKSPACE_ROOT", str(tmp_path / "empty"))
    monkeypatch.setattr(doctor, "_head_match_child", lambda job: None)
    row = _approved_row("a" * 40, "b" * 40)
    assert doctor._verdict_only_descendant_approval(row, row["result"]["review_state"]["approved"]) is False
    assert doctor.merge_handoff(row) == "approval_not_proven"


def test_d3_merge_handoff_with_a_workspace_holding_the_descent_is_ready(tmp_path, monkeypatch):
    origin = Origin(tmp_path)
    root = tmp_path / "workspaces"
    root.mkdir()
    row = _approved_row(origin.tip, "0" * 40)
    review = origin.review_workspace(f"workspaces/{row['id']}")
    write(review, OWN_VERDICT, "{}")
    reviewed = commit(review, "verdict")
    row["result"]["review_state"]["approved"]["approved_sha"] = reviewed
    monkeypatch.setattr(doctor, "_WORKSPACE_ROOT", str(root))
    monkeypatch.setattr(doctor, "_head_match_child", lambda job: None)
    assert doctor.merge_handoff(row) == "ready_to_merge"


# ── the worker: M1, M2, D1, D2 ──────────────────────────────────────────────


@pytest.fixture
def worker(monkeypatch):
    """The tracked worker module with its deployed sidecars stubbed."""
    import importlib.util
    import sys
    import types

    budget = types.ModuleType("hive_budget")
    budget.FAILURE_REASON = "budget_exceeded"
    provenance = types.ModuleType("provenance")
    provenance.STATIC_AGENT_MODELS = {}
    provenance.KNOWN_COST_TIERS = set()
    monkeypatch.setitem(sys.modules, "hive_budget", budget)
    monkeypatch.setitem(sys.modules, "provenance", provenance)
    for name in ("codex_delivery", "llm_roles", "push_policy", "review_evidence"):
        module = types.ModuleType("hive." + name)
        if name == "codex_delivery":
            module.fleet_git_env = lambda *a, **kw: {}
        if name == "llm_roles":
            module.is_role_id = lambda value: False
            module.family = lambda value: None
            module.backing_model = lambda value: None
        if name == "push_policy":
            module.argonas_url_for_secret = lambda *a, **kw: None
            module.argonas_url_for_slug = lambda slug: slug
        monkeypatch.setitem(sys.modules, "hive." + name, module)
    spec = importlib.util.spec_from_file_location("head_match_hardening_worker", ROOT / "deploy" / "hive_worker.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


REVIEW_JOB_ID = "01a197c2-0000-7000-8000-000000000002"
WORK_JOB_ID = "01a195bf-0000-7000-8000-000000000001"


def _review_job(**over):
    job = {
        "id": REVIEW_JOB_ID,
        "kind": "review:fix:fo-merge-pass-20261101",
        "head_branch": BRANCH,
        "parent_job_id": WORK_JOB_ID,
    }
    job.update(over)
    return job


def _violation(worker, job, result, ws, baseline):
    return worker.review_result_violation(job, result, workdir=str(ws) if ws else None, baseline=baseline, env=ENV)


def test_m1_a_clean_read_only_review_proven_by_git_is_accepted(worker, tmp_path):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    assert _violation(worker, _review_job(), {"files_changed": []}, ws, origin.tip) is None
    # Its own verdict, published to its own review ref, is still read-only.
    write(ws, OWN_VERDICT, "{}")
    commit(ws, "verdict")
    git(ws, "push", "-q", "origin", f"HEAD:{worker.review_verdict_ref(BRANCH, REVIEW_JOB_ID)}")
    assert _violation(worker, _review_job(), {"files_changed": [OWN_VERDICT]}, ws, origin.tip) is None


def test_m1_code_pushed_to_the_author_branch_is_caught_even_when_the_report_is_clean(worker, tmp_path):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    # The reviewer edits on a side branch, pushes it ONTO the author branch and
    # returns to a clean checkout: HEAD == baseline, the self report is empty.
    git(ws, "checkout", "-q", "-b", "scratch")
    write(ws, "migrations/0007.sql", "ALTER TABLE jobs ADD COLUMN x;\n")
    commit(ws, "reviewer schema fix")
    git(ws, "push", "-q", "origin", f"scratch:{BRANCH}")
    git(ws, "checkout", "-q", BRANCH)
    assert git(ws, "rev-parse", "HEAD") == origin.tip
    reason = _violation(worker, _review_job(), {"files_changed": []}, ws, origin.tip)
    assert reason and reason.startswith("review_overreach")
    assert "migrations/0007.sql" in reason


def test_m1_an_unprovable_workspace_is_refused_never_trusted(worker, tmp_path):
    origin = Origin(tmp_path)
    # No workspace at all: the self report alone is never proof of cleanliness.
    reason = _violation(worker, _review_job(), {"files_changed": []}, None, origin.tip)
    assert reason and reason.startswith("review_overreach") and "not proven" in reason
    # A workspace whose baseline it does not hold cannot answer either.
    ws = origin.review_workspace("ws")
    reason = _violation(worker, _review_job(), {"files_changed": []}, ws, "f" * 40)
    assert reason and "not proven" in reason
    # The self report adds doubt only where git cannot answer (r3, DeepSeek
    # item 5: a git-clean workspace is accepted whatever the report says --
    # see test_doctor_head_match_review_churn_r3_20261101.py).
    reason = _violation(worker, _review_job(), {"files_changed": ["pkg/widget.py"]}, None, origin.tip)
    assert reason and "not proven" in reason and "pkg/widget.py" in reason


def test_m2_only_this_jobs_own_verdict_names_are_allowed(worker, tmp_path):
    origin = Origin(tmp_path)
    for name, allowed in (
        (OWN_VERDICT, True),
        (f"review-verdict-{REVIEW_JOB_ID}.json", True),
        (f".hive/review/review-verdict-{REVIEW_JOB_ID}.json", True),
        # The reviewed (parent) job's id: the spelling the old brief produced.
        (f"review-verdict-{WORK_JOB_ID}.json", True),
        ("review-verdict-someone-else.json", False),
        ("REVIEW_VERDICT_hv-other-branch.json", False),
        (f"src/foo/review-verdict-{REVIEW_JOB_ID}.json", False),
        (f"tests/{OWN_VERDICT}", False),
    ):
        ws = origin.review_workspace(f"ws-{abs(hash(name))}")
        write(ws, name, "{}")
        commit(ws, "verdict?")
        reason = _violation(worker, _review_job(), {"files_changed": [name]}, ws, origin.tip)
        assert (reason is None) is allowed, (name, reason)


def test_d1_the_review_guard_never_replaces_a_harder_failure(worker, tmp_path):
    origin = Origin(tmp_path)
    ws = origin.review_workspace("ws")
    write(ws, "pkg/widget.py", "value = 'reviewer'\n")
    commit(ws, "reviewer code")
    job = _review_job()
    for earlier in ("protection_violation", "ai_attribution_trailer", "delivery_integrity_check_failed"):
        result = {"files_changed": []}
        status, reason = worker.apply_review_overreach_guard(
            job, result, "failed", earlier, workdir=str(ws), baseline=origin.tip, env=ENV
        )
        assert (status, reason) == ("failed", earlier)
        # Combined, not lost: the overreach is still recorded on the row.
        assert result["review_overreach"].startswith("review_overreach")
    result = {"files_changed": []}
    status, reason = worker.apply_review_overreach_guard(
        job, result, "done", None, workdir=str(ws), baseline=origin.tip, env=ENV
    )
    assert (status, reason) == ("failed", "review_overreach")
    assert result["error"].startswith("review_overreach")
    # A non-review job is untouched.
    result = {}
    assert worker.apply_review_overreach_guard(
        {"id": WORK_JOB_ID, "kind": "fix:x"}, result, "done", None, workdir=str(ws), baseline=origin.tip, env=ENV
    ) == ("done", None)
    assert result == {}


def _function(source: str, name: str) -> ast.FunctionDef:
    tree = ast.parse(source)
    return next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name)


def test_d1_nothing_after_the_guard_reassigns_the_disposition():
    """Pins the ordering: the guard is the LAST writer of status/failure_reason."""
    fn = _function((ROOT / "deploy" / "hive_worker.py").read_text(encoding="utf-8"), "_run_job_inner")
    calls = [
        n.lineno
        for n in ast.walk(fn)
        if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "apply_review_overreach_guard"
    ]
    assert len(calls) == 1
    later = []
    for node in ast.walk(fn):
        if isinstance(node, (ast.Assign, ast.AugAssign, ast.AnnAssign)) and node.lineno > calls[0]:
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                for name in ast.walk(target):
                    if isinstance(name, ast.Name) and name.id in ("status", "failure_reason"):
                        later.append(node.lineno)
    assert later == [], later


def test_d2_the_doctor_brief_has_no_placeholder(monkeypatch):
    captured = {}
    monkeypatch.setattr(doctor, "submit_job_spec", lambda spec: (captured.update(spec), (201, {"id": "x"}))[1])
    monkeypatch.setattr(doctor, "_review_child_refusal", lambda job: "")
    row = {
        "id": WORK_JOB_ID,
        "kind": "fix:fo-merge-pass",
        "status": "needs-review",
        "head_branch": BRANCH,
        "result": {"head_sha": "e0bb983" + "0" * 33},
    }
    doctor._review_queue_submit(row, {"author": "qwen38"}, [{"id": "r", "model_id": "gpt-oss-20b"}], now=1.0)
    desc = captured["description"]
    assert "<" not in desc and ">" not in desc, desc
    assert OWN_VERDICT in desc
    assert "never the author branch" in desc


def test_d2_the_worker_brief_names_the_real_ref_for_a_review_job(worker):
    job = _review_job(head_sha="e0bb983")
    brief = worker.job_delivery_brief(job)
    ref = worker.review_verdict_ref(BRANCH, REVIEW_JOB_ID)
    assert ref in brief and f"review-verdict-{REVIEW_JOB_ID}.json" in brief
    assert "<" not in brief
    # The generic "push the checked-out branch" policy is exactly what put the
    # verdict commit on the author's branch: a review job never gets it.
    assert "Push that checked-out branch" not in brief
    work = worker.job_delivery_brief({"id": WORK_JOB_ID, "kind": "fix:x", "head_branch": BRANCH})
    assert "Push that checked-out branch" in work
    # The run path uses it.
    fn = _function((ROOT / "deploy" / "hive_worker.py").read_text(encoding="utf-8"), "_run_job_inner")
    assert any(isinstance(n, ast.Call) and getattr(n.func, "id", "") == "job_delivery_brief" for n in ast.walk(fn))


def test_d2_pin_the_review_ref_never_collides_with_the_author_branch(worker):
    """Not held: ``review/<branch>-<job>`` is a different ref and never a parent/child of the branch."""
    for branch in (BRANCH, "main", "review/x", "hv/a"):
        ref = worker.review_verdict_ref(branch, REVIEW_JOB_ID)
        assert ref != branch
        assert not branch.startswith(ref + "/") and not ref.startswith(branch + "/")


def test_d4_pin_review_chain_refusal_exists_on_this_branch():
    """Not held: both names come from the integrate merge and import cleanly."""
    from hive.review_queue import REFUSE_REVIEW_OF_REVIEW, review_chain_refusal

    assert REFUSE_REVIEW_OF_REVIEW == "review_of_review"
    assert review_chain_refusal({"id": "r", "kind": "review:x"}, lambda _jid: None) == REFUSE_REVIEW_OF_REVIEW
    assert review_queue.review_chain_refusal is review_chain_refusal


# ── E2E: the live loop ──────────────────────────────────────────────────────


@pytest.fixture
def bus(monkeypatch):
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_ADOPT_DRY_RUN", False)
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_VERDICT_DESCENDANT", True)
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_HEAD_MATCH_DRY_RUN", False)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda topic, body: None)
    monkeypatch.setattr(doctor, "_review_child_refusal", lambda job: "")

    class Bus:
        def __init__(self):
            self.calls = []
            self.children = {}
            self.rows = {}

        def http(self, method, path, body=None, **kwargs):
            self.calls.append((method, path, body))
            if method == "GET" and "parent_job_id=" in path:
                parent = path.split("parent_job_id=", 1)[1].split("&", 1)[0]
                return 200, {"jobs": [c for c in self.children.values() if c.get("parent_job_id") == parent]}
            if method == "PATCH":
                jid = path.rsplit("/", 1)[-1]
                self.rows.setdefault(jid, {}).update(json.loads(json.dumps(body or {})))
                return 200, {}
            if method == "POST":
                return 201, {"id": str(uuid.uuid4())}
            return 200, {}

        def posts(self):
            return [c for c in self.calls if c[0] == "POST"]

    fake = Bus()
    monkeypatch.setattr(doctor, "_http", fake.http)
    monkeypatch.setattr(doctor, "submit_job_spec", lambda spec: fake.http("POST", "/v1/jobs", spec))
    return fake


def _work_row(tip, child_id):
    return {
        "id": WORK_JOB_ID,
        "kind": "fix:fo-merge-pass",
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
                "review_job_id": child_id,
                "tried": [],
            },
        },
    }


def _finish_review_child(worker, bus, origin, ws, child_id, reviewed):
    """Run the WORKER's guard on the child exactly as run_job does, then post its row."""
    entry = {"model": "cerberus-reviewer", "verdict": "approve"}
    result = {
        "reviewers": [entry],
        "head_sha": reviewed,
        # A structured verdict rides along even on a refused child: the doctor
        # must never parse a refused review's verdict as an approve.
        "verdict": {"reviewers": [entry], "head_sha": reviewed},
        "review_evidence": {"reviewers": [entry]},
        "files_changed": [],
        "commits": [reviewed],
    }
    job = _review_job(id=child_id)
    status, reason = worker.apply_review_overreach_guard(
        job, result, "done", None, workdir=str(ws), baseline=origin.tip, env=ENV
    )
    if reason:
        result["failure_reason"] = reason
    bus.children[child_id] = {
        "id": child_id,
        "kind": job["kind"],
        "status": status,
        "parent_job_id": WORK_JOB_ID,
        "workdir": str(ws),
        "result": result,
    }
    return status, reason


def test_e2e_a_verdict_only_push_is_the_same_head_and_is_never_reviewed_again(worker, bus, tmp_path, monkeypatch):
    origin = Origin(tmp_path)
    root = tmp_path / "workspaces"
    root.mkdir()
    monkeypatch.setattr(doctor, "_WORKSPACE_ROOT", str(root))
    child_id = str(uuid.uuid4())
    ws = origin.review_workspace(f"workspaces/{child_id}")
    # The live shape: the review child commits its verdict and pushes it onto
    # the AUTHOR branch (e0bb983 -> aa73bb5), advancing the branch head.
    write(ws, OWN_VERDICT, json.dumps({"verdict": "approve"}))
    reviewed = commit(ws, "review verdict")
    git(ws, "push", "-q", "origin", f"HEAD:{BRANCH}")
    assert git(origin.bare, "rev-parse", f"refs/heads/{BRANCH}") == reviewed != origin.tip

    status, reason = _finish_review_child(worker, bus, origin, ws, child_id, reviewed)
    assert (status, reason) == ("done", None)

    row = _work_row(origin.tip, child_id)
    first = doctor.run_review_queue_sweep(rows=[row], now=2000.0)
    assert first["approved_mergeable"] == 1, first
    written = bus.rows[WORK_JOB_ID]
    state = written["result"]["review_state"]
    assert state["state"] == "approved_mergeable"
    assert state["approved"]["head_sha"] == origin.tip and state["approved"]["approved_sha"] == reviewed
    assert doctor.merge_handoff({**row, **written}) == "ready_to_merge"

    # The next sweeps read the row as the bus now holds it: same effective head,
    # NO new review job -- the loop that ran 01a197c2 -> 69de2e7f -> 40db61c9.
    for now in (3000.0, 10_000.0, 1_000_000.0):
        again = {**row, **json.loads(json.dumps(written))}
        doctor.run_review_queue_sweep(rows=[again], now=now)
    assert bus.posts() == []


def test_e2e_a_review_that_pushes_code_is_still_overreach(worker, bus, tmp_path, monkeypatch):
    origin = Origin(tmp_path)
    root = tmp_path / "workspaces"
    root.mkdir()
    monkeypatch.setattr(doctor, "_WORKSPACE_ROOT", str(root))
    child_id = str(uuid.uuid4())
    ws = origin.review_workspace(f"workspaces/{child_id}")
    write(ws, OWN_VERDICT, json.dumps({"verdict": "approve"}))
    write(ws, "migrations/0007.sql", "ALTER TABLE jobs ADD COLUMN x;\n")
    reviewed = commit(ws, "verdict + schema fix")
    git(ws, "push", "-q", "origin", f"HEAD:{BRANCH}")

    status, reason = _finish_review_child(worker, bus, origin, ws, child_id, reviewed)
    assert (status, reason) == ("failed", "review_overreach")

    row = _work_row(origin.tip, child_id)
    doctor.run_review_queue_sweep(rows=[row], now=2000.0)
    state = bus.rows[WORK_JOB_ID]["result"]["review_state"]
    assert state["last_failure"] == "review_overreach"
    assert not state.get("approved") and not state.get("approved_mergeable")
    assert state["state"] == "awaiting_review"


def test_e2e_a_refused_review_carrying_a_verdict_is_never_parsed_as_an_approve(bus, tmp_path, monkeypatch):
    """Found while wiring the E2E: a FAILED child with a ``verdict`` was parsed as an approve.

    The worker refuses an overreaching review (``failed`` / ``review_overreach``)
    but its result still carries the reviewer's structured verdict; the doctor
    read any non-``done`` child that had a ``verdict`` as a verdict.
    """
    origin = Origin(tmp_path)
    root = tmp_path / "workspaces"
    root.mkdir()
    monkeypatch.setattr(doctor, "_WORKSPACE_ROOT", str(root))
    child_id = str(uuid.uuid4())
    entry = {"model": "cerberus-reviewer", "verdict": "approve"}
    bus.children[child_id] = {
        "id": child_id,
        "kind": "review:fix:fo-merge-pass-20261101",
        "status": "failed",
        "parent_job_id": WORK_JOB_ID,
        "result": {
            "failure_reason": "review_overreach",
            "review_overreach": "review_overreach: the review job changed migrations/0007.sql ...",
            "verdict": {"reviewers": [entry], "head_sha": origin.tip},
            "reviewers": [entry],
            "head_sha": origin.tip,
        },
    }
    doctor.run_review_queue_sweep(rows=[_work_row(origin.tip, child_id)], now=2000.0)
    state = bus.rows[WORK_JOB_ID]["result"]["review_state"]
    assert state["last_failure"] == "review_overreach"
    assert not state.get("approved") and state["state"] == "awaiting_review"
