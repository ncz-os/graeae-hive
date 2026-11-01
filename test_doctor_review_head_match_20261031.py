"""A review approval must close the row it reviewed when the reviewer only ADDED its verdict.

Bug (production, 2026-10-31): four needs-review rows -- 01a19270
(``hv/bl-ct-dialect``), 1007c484 (``hv/bl-ct-system``), 01a1922a
(``hv/zf-failure-contract-r3``), 01a18fe6 (``hv/zc-master-prompt``) -- each had
a ``review:*`` child with a structured approve from ``cerberus-reviewer``, and
each parent stayed needs-review with ``doctor_reason`` "approved, but the review
job advanced the branch: reviewed head X". The doctor's exact-head rule measured
the wrong thing: the review job committed its own verdict artifact
(``REVIEW_VERDICT_<branch>.json`` / ``review-verdict-<job>.json``) onto the
AUTHOR's branch, so the head it approved is the author's tip plus one
verdict-only commit.

The four real shapes, as git-repo fixtures built here (tmp dirs, no network):

(a) verdict-only descendant -- dialect ``f0d41af`` = ``db4d910`` + one json
    file; failure-contract-r3 ``7705818`` = ``0c21d57`` + one json file. The
    approve at the descendant DOES cover the tip: record
    ``approved at R (verdict-only descendant of T)`` and clear the row to
    ``review_state.state = 'approved_mergeable'`` (status stays needs-review).
(b) review overreach -- bl-ct-system ``a94be15`` = ``3fa294b`` + verdict + a
    schema fix + a new test. The approval does not cover the author's tip:
    ``review_overreach``, never counted for T.
(c) code commits after the reviewed head that never reached the head branch --
    zc-master-prompt: reviewed ``d097ee0`` is a descendant of branch tip
    ``52510d6`` and not on the head branch. Classified as "approval covers newer
    work not on the head branch": rescued by a pure fast-forward push ONLY when
    the review child's workspace holds it, else reported with the exact sha and
    a one-line action.
(d) everything else stays head-exact: a different, non-descendant head is
    still not an approval.

Pinned here:

1. ``hive/workers/doctor_head_match.py`` -- the git-backed classifier (real
   git repos in tmp dirs) and the ancestor/verdict-only/overreach helpers;
2. the doctor's call site: ``process_awaiting_review_row`` records
   ``approved_mergeable`` with the approved sha and does not retry; the
   overreach and stray-tip cases are recorded, not cleared;
3. the review brief says where the verdict artifact goes and the worker refuses
   a review result whose commits touch anything else on the author branch.
"""

from __future__ import annotations

import json
import os
import subprocess

import pytest

from hive.workers import doctor, doctor_head_match as dhm

ENV = {
    **os.environ,
    "GIT_AUTHOR_NAME": "Jason Perlow",
    "GIT_AUTHOR_EMAIL": "jperlow@gmail.com",
    "GIT_COMMITTER_NAME": "Jason Perlow",
    "GIT_COMMITTER_EMAIL": "jperlow@gmail.com",
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_CONFIG_GLOBAL": os.devnull,
}

VERDICT_FILES = ("REVIEW_VERDICT_hv-bl-ct-dialect.json", "review-verdict-01a19270.json")


def git(repo, *args):
    out = subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True, env=ENV)
    return out.stdout.strip()


def write(repo, name, text):
    path = os.path.join(str(repo), name)
    os.makedirs(os.path.dirname(path) or str(repo), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)
    return path


def commit(repo, message):
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", message)
    return git(repo, "rev-parse", "HEAD")


def make_repo(path, *, base_files=("pkg/widget.py",)):
    os.makedirs(str(path), exist_ok=True)
    git(path, "init", "-q", "-b", "main")
    for name in base_files:
        write(path, name, "value = 1\n")
    return commit(path, "base")


class Shapes:
    """Clones of the four real shapes, each on its own branch, off one origin."""

    def __init__(self, root):
        self.root = root
        self.origin = root / "origin.git"
        subprocess.run(["git", "init", "-q", "--bare", str(self.origin)], check=True, env=ENV)
        seed = root / "seed"
        base = make_repo(seed)
        git(seed, "remote", "add", "origin", str(self.origin))
        git(seed, "push", "-q", "origin", "main")
        self.base = base
        self.seed = seed

    def branch_clone(self, branch):
        """A clone holding ``branch`` at the author tip and a worktree to commit in."""
        author = self.root / f"{branch}-author"
        git(self.root, "clone", "-q", str(self.origin), str(author))
        git(author, "checkout", "-q", "-b", branch)
        write(author, "pkg/widget.py", "value = 2\n")
        tip = commit(author, "author work")
        git(author, "push", "-q", "-u", "origin", branch)
        self.base_commit = tip
        return author, tip

    def review_workspace(self, author_dir, name):
        clone = self.root / f"{name}-review"
        git(self.root, "clone", "-q", str(author_dir), str(clone))
        return clone


def _commit_verdict(repo, files=VERDICT_FILES):
    for name in files:
        write(repo, name, json.dumps({"verdict": "approve"}))
    return commit(repo, "review verdict")


# ── (1) the brief and the worker guard ──────────────────────────────────────


@pytest.fixture
def worker(monkeypatch):
    """The tracked worker module with its deployed sidecars stubbed (see test_worker_review_queue)."""
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
    root = __import__("pathlib").Path(__file__).resolve().parent
    spec = importlib.util.spec_from_file_location("review_head_match_worker", root / "deploy" / "hive_worker.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _review_spec(**over):
    spec = {
        "kind": "review:fix-thing-20261031",
        "head_branch": "hv/bl-ct-dialect",
        "parent_job_id": "01a19270-aaaa",
        "repo": "graeae-hive",
    }
    spec.update(over)
    return spec


def test_review_brief_sends_the_verdict_to_its_own_ref(worker):
    brief = worker.review_brief_text(
        head_branch="hv/bl-ct-dialect", job_id="review-job-01a19270", head_sha="db4d910"
    )
    assert "review/hv/bl-ct-dialect-review-job-01a19270" in brief
    assert "REVIEW_VERDICT_hv/bl-ct-dialect.json" in brief or "review-verdict-review-job-01a19270.json" in brief
    # It must forbid a commit on the author's branch.
    assert "hv/bl-ct-dialect" in brief
    assert "never" in brief.lower() and "author branch" in brief.lower()


def test_worker_review_target_ref_and_guard(worker):
    assert worker.review_verdict_ref("hv/bl-ct-dialect", "01a19270-x") == "review/hv/bl-ct-dialect-01a19270-x"
    assert dhm.is_verdict_artifact("REVIEW_VERDICT_x.json")
    assert dhm.is_verdict_artifact("nested/review-verdict-x.json")
    assert not dhm.is_verdict_artifact("pkg/widget.py")
    # A read-only review result (no commits changed any file) is accepted.
    ok = worker.review_result_violation(_review_spec(), {"files_changed": [], "commits": []})
    assert ok is None
    # The review child's own result showing code changes on the author branch is refused.
    bad = worker.review_result_violation(
        _review_spec(),
        {"files_changed": ["pkg/widget.py", "tests/test_widget.py"], "commits": ["a94be15"]},
    )
    assert bad and "review_overreach" in bad
    # A verdict artifact recorded with the sibling ref is fine; same-branch code is not.
    mixed = worker.review_result_violation(
        _review_spec(), {"files_changed": ["REVIEW_VERDICT_hv-bl-ct-dialect.json", "migrations/0007.sql"]}
    )
    assert mixed and "review_overreach" in mixed


def test_doctor_submit_brief_names_the_verdict_ref(monkeypatch):
    captured = {}

    def fake_submit(spec):
        captured.update(spec)
        return 201, {"id": "review-1"}

    monkeypatch.setattr(doctor, "submit_job_spec", fake_submit)
    row = {
        "id": "01a19270-aaaa",
        "kind": "fix:bl-ct-dialect",
        "status": "needs-review",
        "repo": "graeae-hive",
        "head_branch": "hv/bl-ct-dialect",
        "result": {"head_sha": "d" * 40},
    }
    reviewers = [{"id": "cerberus-gpt-oss", "model_id": "gpt-oss-20b"}]
    review_id, _spec = doctor._review_queue_submit(row, {"author": "qwen38"}, reviewers, now=1000.0)
    assert review_id == "review-1"
    desc = captured["description"]
    # The artifact ref is keyed by the reviewer, so the brief can name it before
    # the child id exists; the worker's own helper spells the same ref.
    assert "SEPARATE ref review/" in desc and "never the author branch" in desc
    assert "REVIEW_VERDICT_hv-bl-ct-dialect.json" in desc
    assert "verdict artifact" in desc.lower()
    # One spelling on both lines: the slug helper the worker shares.
    assert doctor._review_ref_slug("hv/bl-ct-dialect") == "hv-bl-ct-dialect"


# ── (2) verdict-only descendant clears the row ──────────────────────────────


def test_verdict_only_descendant_is_an_approval_of_the_tip(tmp_path):
    shapes = Shapes(tmp_path)
    author, tip = shapes.branch_clone("hv/bl-ct-dialect")
    review = shapes.review_workspace(author, "dialect")
    verdict_head = _commit_verdict(review)
    assert dhm.ancestor(review, tip, verdict_head)
    result = dhm.classify_descendant(review, tip, verdict_head)
    assert result.verdict == dhm.VERDICT_ONLY
    assert result.ok_for_tip
    assert result.files == sorted(VERDICT_FILES)
    assert result.reason.startswith(f"approved at {verdict_head[:7]}")
    assert "verdict-only descendant" in result.reason


def test_nested_verdict_artifact_names_are_ignored_too(tmp_path):
    shapes = Shapes(tmp_path)
    author, tip = shapes.branch_clone("hv/zf-failure-contract-r3")
    review = shapes.review_workspace(author, "r3")
    write(review, ".hive/review/REVIEW_VERDICT_hv-zf-failure-contract-r3.json", "{}")
    verdict_head = commit(review, "verdict")
    result = dhm.classify_descendant(review, tip, verdict_head)
    assert result.verdict == dhm.VERDICT_ONLY and result.ok_for_tip


# ── (3) code changes: overreach vs newer work not on the head branch ────────


def test_review_overreach_is_never_counted_for_the_tip(tmp_path):
    shapes = Shapes(tmp_path)
    author, tip = shapes.branch_clone("hv/bl-ct-system")
    review = shapes.review_workspace(author, "system")
    # Production shape (b): a94be15 = 3fa294b + verdict + schema fix + test. The
    # reviewer committed onto the author's branch (a descendant of the tip) but
    # changed CODE: the approve covers the reviewer's edits, not the tip.
    write(review, VERDICT_FILES[0], "{}")
    write(review, "migrations/0007_schema.sql", "ALTER TABLE jobs ADD COLUMN x;\n")
    write(review, "tests/test_schema_x.py", "def test_x():\n    assert True\n")
    overreach_head = commit(review, "verdict + schema fix + test")
    # Left behind the tip (the production row's symptom: "differs", not "advanced").
    git(author, "reset", "-q", "--hard", tip)
    side = dhm.classify_descendant(review, tip, overreach_head)
    assert side.verdict == dhm.CODE_DESCENDANT  # descendant, but code: never counted
    assert not side.ok_for_tip

    # And the shape the row actually records: a head that does NOT descend
    # from the tip, carrying code plus the verdict.
    # (its parent is the row's BASE, not the delivered tip: the reviewer never
    # saw the author's last commit).
    git(review, "checkout", "-q", "-B", "side", shapes.base)
    write(review, VERDICT_FILES[0], "{}")
    write(review, "migrations/0007_schema.sql", "ALTER TABLE jobs ADD COLUMN x;\n")
    write(review, "tests/test_schema_x.py", "def test_x():\n    assert True\n")
    orphan = commit(review, "verdict + fix + test off the base")
    assert not dhm.is_ancestor_or_same(review, tip, orphan)
    result = dhm.classify_descendant(review, tip, orphan)
    assert result.verdict == dhm.OVERREACH
    assert not result.ok_for_tip
    assert "review_overreach" in result.reason
    assert set(result.files) >= {"migrations/0007_schema.sql", "tests/test_schema_x.py"}


def test_newer_work_not_on_the_head_branch_is_rescued_by_pure_ff(tmp_path):
    shapes = Shapes(tmp_path)
    author, tip = shapes.branch_clone("hv/zc-master-prompt")
    review = shapes.review_workspace(author, "zc")
    write(review, VERDICT_FILES[1], "{}")
    write(review, "pkg/widget.py", "value = 3\n")
    reviewed = commit(review, "review-and-fix")
    result = dhm.classify_descendant(review, tip, reviewed)
    assert result.verdict == dhm.CODE_DESCENDANT
    assert not result.ok_for_tip
    assert reviewed[:7] in result.reason and "not on the head branch" in result.reason
    # The review child's workspace holds it: a pure fast-forward is possible.
    assert dhm.can_fast_forward(review, tip, reviewed) is True
    pushed = dhm.rescue_ff(review, tip, reviewed, branch="hv/zc-master-prompt", dry_run=True)
    assert pushed.get("action") == "would_push" and pushed["sha"] == reviewed
    # Never a merge commit, never a force: the branch tip stays an ancestor.
    assert git(review, "rev-parse", "HEAD") == reviewed


def test_rescue_refuses_when_the_reviewed_head_was_rewritten(tmp_path):
    shapes = Shapes(tmp_path)
    author, tip = shapes.branch_clone("hv/zc-master-prompt")
    review = shapes.review_workspace(author, "zc")
    write(review, "pkg/widget.py", "value = 9\n")
    git(review, "commit", "-q", "--amend", "-a", "-m", "rewritten")
    rewritten = git(review, "rev-parse", "HEAD")
    assert not dhm.is_ancestor_or_same(review, tip, rewritten)
    assert dhm.can_fast_forward(review, tip, rewritten) is False
    refused = dhm.rescue_ff(review, tip, rewritten, branch="hv/zc-master-prompt", dry_run=True)
    assert refused["action"] == "refused"


# ── (5) everything else stays head-exact ────────────────────────────────────


def test_different_non_descendant_head_is_still_not_an_approval(tmp_path):
    shapes = Shapes(tmp_path)
    author, tip = shapes.branch_clone("hv/bl-ct-dialect")
    # A commit whose only change is the verdict artifact on a branch that is not
    # the delivered tip is NEITHER verdict-only (it does not descend from T) nor
    # an approval: head-exact.
    clean = tmp_path / "clean"
    git(tmp_path, "clone", "-q", str(shapes.origin), str(clean))
    git(clean, "checkout", "-q", "-b", "clean", "origin/main")
    write(clean, VERDICT_FILES[0], "{}")
    verdict_only = commit(clean, "verdict on a non-head branch")
    result = dhm.classify_descendant(clean, tip, verdict_only)
    assert result.verdict == dhm.NOT_DESCENDANT and not result.ok_for_tip
    # A different head that is NOT related to tip at all, and an unknown sha.
    git(author, "reset", "-q", "--hard", tip)
    write(author, "pkg/other.py", "x = 1\n")
    side = commit(author, "side work off the tip")
    assert dhm.classify_descendant(author, tip, side).verdict == dhm.CODE_DESCENDANT
    assert dhm.classify_descendant(clean, tip, "0" * 40).verdict == dhm.NOT_DESCENDANT
    assert dhm.is_ancestor_or_same(clean, tip, tip) is True


# ── the doctor call site ────────────────────────────────────────────────────


@pytest.fixture
def bus(monkeypatch):
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_ADOPT_DRY_RUN", False)
    messages = []
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda topic, body: messages.append((topic, body)))

    class Bus:
        def __init__(self):
            self.calls = []
            self.patches = []
            self.children = {}
            self.rows = {}
            self.messages = messages

        def http(self, method, path, body=None, **kwargs):
            self.calls.append((method, path, body))
            if method == "GET" and "parent_job_id=" in path:
                parent = path.split("parent_job_id=", 1)[1].split("&", 1)[0]
                return 200, {"jobs": [c for c in self.children.values() if c.get("parent_job_id") == parent]}
            if method == "PATCH":
                self.patches.append((path, body))
                jid = path.rsplit("/", 1)[-1]
                self.rows.setdefault(jid, {}).update(body or {})
                return 200, {}
            return 200, {}

        def finish(self, child_id, result, status="done"):
            self.children[child_id].update(status=status, result=result)

    fake = Bus()
    monkeypatch.setattr(doctor, "_http", fake.http)
    return fake


def _reviewing_row(*, head="db4d910" + "0" * 33, child="review-1", **over):
    row = {
        "id": "01a19270-aaaa",
        "kind": "fix:bl-ct-dialect",
        "status": "needs-review",
        "repo": "graeae-hive",
        "head_branch": "hv/bl-ct-dialect",
        "base_branch": "main",
        "head_sha": head,
        "claimed_model": "qwen38",
        "result": {
            "head_sha": head,
            "commits": [head],
            "head_branch": "hv/bl-ct-dialect",
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
    row["result"].update(over.pop("result", {}))
    row.update(over)
    return row


def _approve(head, *, reviewers=(("cerberus-reviewer", "openai"),), files=None, commits=None):
    entry = {"model": reviewers[0][0], "verdict": "approve"}
    return {
        "reviewers": [entry],
        "head_sha": head,
        "review_evidence": {"reviewers": [entry]},
        "files_changed": list(files or []),
        "commits": list(commits or []),
    }


class Classifier:
    """A stand-in for the git-backed classifier (the doctor calls it by dependency)."""

    def __init__(self, by_head):
        self.by_head = by_head
        self.seen = []

    def __call__(self, job, tip, reviewed, **kwargs):
        self.seen.append((tip, reviewed))
        return self.by_head[reviewed]


def test_doctor_records_approved_mergeable_for_a_verdict_only_descendant(bus, monkeypatch):
    tip, reviewed = "a" * 40, "b" * 40
    classifier = Classifier(
        {reviewed: dhm.HeadMatch(dhm.VERDICT_ONLY, reviewed, tip, files=list(VERDICT_FILES), reason=f"approved at {reviewed} (verdict-only descendant of {tip})")}
    )
    monkeypatch.setattr(doctor, "classify_review_head", classifier)
    row = _reviewing_row(head=tip)
    bus.children["review-1"] = {"id": "review-1", "kind": "review:fix", "status": "done", "parent_job_id": row["id"]}
    bus.finish("review-1", _approve(reviewed, files=list(VERDICT_FILES), commits=[reviewed]))

    outcome, state, _ = doctor.process_awaiting_review_row(row, now=2000.0)

    assert outcome == "approved_mergeable", (outcome, state)
    assert state["state"] == "approved_mergeable"
    assert state["approved"]["head_sha"] == tip
    assert state["approved"]["approved_sha"] == reviewed
    assert state["approved"]["basis"] == dhm.VERDICT_ONLY
    # Recorded on the row, still needs-review: only the merge pass may merge.
    body = bus.rows[row["id"]]
    assert body["status"] == "needs-review"
    assert body["result"]["review_state"]["state"] == "approved_mergeable"
    assert body["result"]["review_state"]["approved"]["approved_sha"] == reviewed


def test_doctor_verdict_descendant_switch_off_keeps_head_exact(bus, monkeypatch):
    tip, reviewed = "a" * 40, "b" * 40
    classifier = Classifier({})
    monkeypatch.setattr(doctor, "classify_review_head", classifier)
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_VERDICT_DESCENDANT", False)
    row = _reviewing_row(head=tip)
    bus.children["review-1"] = {"id": "review-1", "kind": "review:fix", "status": "done", "parent_job_id": row["id"]}
    bus.finish("review-1", _approve(reviewed, files=list(VERDICT_FILES), commits=[reviewed]))
    outcome, _state, _ = doctor.process_awaiting_review_row(row, now=2000.0)
    assert outcome == "review_failed"
    assert classifier.seen == []


def test_doctor_does_not_count_an_overreach_approval(bus, monkeypatch):
    tip, reviewed = "a" * 40, "c" * 40
    classifier = Classifier(
        {reviewed: dhm.HeadMatch(dhm.OVERREACH, reviewed, tip, files=["migrations/0007.sql"], reason="review_overreach: ...")}
    )
    monkeypatch.setattr(doctor, "classify_review_head", classifier)
    row = _reviewing_row(head=tip)
    bus.children["review-1"] = {"id": "review-1", "kind": "review:fix", "status": "done", "parent_job_id": row["id"]}
    bus.finish("review-1", _approve(reviewed, files=["migrations/0007.sql", "tests/test_x.py"], commits=[reviewed]))

    outcome, state, _ = doctor.process_awaiting_review_row(row, now=2000.0)

    assert outcome == "review_overreach"
    assert state["state"] == "awaiting_review"
    assert state["review_job_id"] is None
    assert state["last_failure"] == "review_overreach"
    assert state["review_head_match"]["verdict"] == dhm.OVERREACH
    assert "approved" not in state
    body = bus.rows[row["id"]]
    assert body["status"] == "needs-review"
    # The state must not be _any_ approving state; the record may still
    # mention the approve it refused to count (assert on state, not substrings).
    state = body["result"]["review_state"]
    assert state["state"] != "approved_mergeable"
    assert state["state"] != "approved"
    assert state["approved_mergeable"] is None
    assert state["review_head_match"]["verdict"] == "overreach"


def test_doctor_reports_newer_work_not_on_the_head_branch_with_the_sha(bus, monkeypatch):
    tip, reviewed = "a" * 40, "d" * 40
    result = dhm.HeadMatch(
        dhm.CODE_DESCENDANT,
        reviewed,
        tip,
        files=["pkg/widget.py"],
        reason=f"approval covers newer work not on the head branch: reviewed {reviewed[:12]} is a descendant of {tip[:12]}",
    )
    classifier = Classifier({reviewed: result})
    monkeypatch.setattr(doctor, "classify_review_head", classifier)
    monkeypatch.setattr(doctor, "rescue_reviewed_head", lambda *a, **kw: {"action": "unavailable", "sha": reviewed})
    row = _reviewing_row(head=tip)
    bus.children["review-1"] = {"id": "review-1", "kind": "review:fix", "status": "done", "parent_job_id": row["id"]}
    bus.finish("review-1", _approve(reviewed, commits=[reviewed]))

    outcome, state, _ = doctor.process_awaiting_review_row(row, now=2000.0)

    assert outcome == "approval_not_on_head"
    assert state["state"] == "awaiting_review"
    assert state["review_head_match"]["verdict"] == dhm.CODE_DESCENDANT
    assert state["review_head_match"]["action"]
    action = state["review_head_match"]["action"]
    assert reviewed[:12] in action
    assert "\n" not in action
    assert state["last_failure"] == dhm.CODE_DESCENDANT
    assert state["approved_mergeable"] is None
    assert bus.rows[row["id"]]["status"] == "needs-review"


def test_doctor_escalates_mergeable_when_the_rescue_fast_forwards(bus, monkeypatch):
    tip, reviewed = "a" * 40, "d" * 40
    result = dhm.HeadMatch(dhm.CODE_DESCENDANT, reviewed, tip, files=["pkg/widget.py"], reason="newer work")
    monkeypatch.setattr(doctor, "classify_review_head", Classifier({reviewed: result}))
    monkeypatch.setattr(
        doctor, "rescue_reviewed_head", lambda *a, **kw: {"action": "pushed", "sha": reviewed, "old": tip}
    )
    row = _reviewing_row(head=tip)
    bus.children["review-1"] = {"id": "review-1", "kind": "review:fix", "status": "done", "parent_job_id": row["id"]}
    bus.finish("review-1", _approve(reviewed, commits=[reviewed]))

    outcome, state, _ = doctor.process_awaiting_review_row(row, now=2000.0)

    assert outcome == "awaiting_review_after_rescue"
    assert state["state"] == "awaiting_review"
    assert state["review_job_id"] is None
    # The row is re-reviewed at the NEW tip; nothing is cleared.
    assert "approved" not in state
    assert state["review_head_match"]["rescued_sha"] == reviewed


def test_doctor_sweep_is_idempotent_and_capped(bus, monkeypatch):
    tip, reviewed = "a" * 40, "b" * 40
    classifier = Classifier({reviewed: dhm.HeadMatch(dhm.VERDICT_ONLY, reviewed, tip, files=list(VERDICT_FILES), reason="ok")})
    monkeypatch.setattr(doctor, "classify_review_head", classifier)
    rows = []
    for index in range(3):
        row = _reviewing_row(head=tip, child=f"review-{index}")
        row["id"] = f"parent-{index}"
        bus.children[f"review-{index}"] = {
            "id": f"review-{index}",
            "kind": "review:fix",
            "status": "done",
            "parent_job_id": row["id"],
        }
        bus.finish(f"review-{index}", _approve(reviewed, commits=[reviewed]))
        rows.append(row)
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_HEAD_MATCH_MAX", 2)
    summary = doctor.run_review_queue_sweep(rows=rows, now=2000.0)
    assert summary["approved_mergeable"] == 2
    assert summary["head_match_capped"] == 1
    # A second pass over the SAME rows is a no-op: the two rows are mergeable.
    second = doctor.run_review_queue_sweep(rows=[rows[0], rows[1]], now=2100.0)
    assert second.get("approved_mergeable", 0) == 0


def test_doctor_dry_run_reports_the_sha_and_writes_nothing(bus, monkeypatch):
    tip, reviewed = "a" * 40, "b" * 40
    classifier = Classifier({reviewed: dhm.HeadMatch(dhm.VERDICT_ONLY, reviewed, tip, files=list(VERDICT_FILES), reason="ok")})
    monkeypatch.setattr(doctor, "classify_review_head", classifier)
    monkeypatch.setattr(doctor, "HIVE_DOCTOR_HEAD_MATCH_DRY_RUN", True)
    row = _reviewing_row(head=tip)
    bus.children["review-1"] = {"id": "review-1", "kind": "review:fix", "status": "done", "parent_job_id": row["id"]}
    bus.finish("review-1", _approve(reviewed, commits=[reviewed]))
    outcome, state, _ = doctor.process_awaiting_review_row(row, now=2000.0)
    assert outcome == "head_match_previewed"
    assert state["review_head_match"]["approved_sha"] == reviewed
    assert bus.patches == []
    assert bus.rows == {}


def test_switch_default_is_on_and_accepts_falsy_spellings(monkeypatch):
    for value, expected in ((None, True), ("1", True), ("0", False), ("false", False), ("no", False), ("off", False)):
        if value is None:
            monkeypatch.delenv("HIVE_DOCTOR_VERDICT_DESCENDANT", raising=False)
        else:
            monkeypatch.setenv("HIVE_DOCTOR_VERDICT_DESCENDANT", value)
        assert dhm.env_switch("HIVE_DOCTOR_VERDICT_DESCENDANT", default=True) is expected
