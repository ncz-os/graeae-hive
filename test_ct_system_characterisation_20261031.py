"""Characterisation review: the lineage / content-identity system (2026-10-31).

Read-only review job ``01a1926d-d41d-7c9a-af07-40262f4eb964``: branch
``hv/bl-ct-system-20261031`` at HEAD ``db4d910`` was reviewed against base
``deploy/integrate-20261017``. This suite is the deliverable that PINS the
current behaviour of the two parts of the system this branch carries:

  1. ``scripts/lineage_diff.py``  -- the content-identity classifier behind
     ``docs/LINEAGE_RECONCILIATION.md``. Classification is by BLOB ID read
     from each tip's tree, so the row set is the UNION of both tips: a path
     identical on both sides is ``same`` (never visible in a plain tip-vs-tip
     diff), a rename with unchanged content is one ``only-A`` row plus one
     ``only-B`` row sharing a blob, and a deletion is ``only-<the other
     side>``.
  2. The ``completion`` gate in ``/opt/graeae/hive_worker.py`` -- the
     ``verify-delivery`` contract this job itself is subject to: it is a pure
     function of Git state, never of model prose.

Scope notes (deliberate, per the job's read-only restriction):

* NOTHING in this file writes to this repository: the classifier is exercised
  against a throwaway fixture repo built in a pytest tmp dir, and the
  ``completion_evidence`` / ``completion_ready`` probes run ``git`` against
  that fixture with a bounded env. The fixture is the only repo in sight for
  those calls.
* No network: the fixture has no remote, so ``remote_verified`` stays False
  and ``completion_ready`` stays False -- which is exactly the contract
  being pinned (no pushed remote ref, no ready delivery).
* The classifier's documented guarantees (one row per union path, blob-id
  identity, A-perspective stats, read-only execution) are pinned here so a
  future edit that changes row semantics, drops the union, or flips the
  stats direction is caught by the suite. The fixture in
  ``test_lineage_diff.py`` covers the same classifier; this file pins it from
  the review side (independent construction) and additionally pins the
  header, the read-only execution, and the completion gate.
"""

from __future__ import annotations

import importlib.util
import json
import os
import pathlib
import subprocess
import sys

import pytest

REPO = pathlib.Path(__file__).resolve().parent
SCRIPT = REPO / "scripts" / "lineage_diff.py"

SPEC = importlib.util.spec_from_file_location(
    "lineage_diff_ct", SCRIPT
)
lineage_diff_ct = importlib.util.module_from_spec(SPEC)
sys.modules["lineage_diff_ct"] = lineage_diff_ct  # dataclass introspection
SPEC.loader.exec_module(lineage_diff_ct)

HEAD_SHA = "db4d9106829488b034628c87f411d98b7998e13d"
JOB_ID = "01a1926d-d41d-7c9a-af07-40262f4eb964"


def _git(repo, *args, check=True):
    """Read-only git against the fixture; a host-exported GIT_DIR must not bleed in."""
    env = dict(os.environ)
    env.pop("GIT_DIR", None)
    env.pop("GIT_WORK_TREE", None)
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        env=env,
    )
    if check and proc.returncode != 0:
        raise AssertionError(f"git {args} failed: {proc.stderr}")
    return proc.stdout


@pytest.fixture
def fixture_repo(tmp_path):
    """Three commits, one rename, one deletion, one shared-identity file.

    root: keep.txt (same blob on both tips), edit.py, gone.txt, old_name.txt
    X:     edit.py edited, old_name.txt renamed to new_name.txt (content kept)
    Y:     edit.py edited differently, gone.txt deleted
    """
    repo = tmp_path / "ct-repo"
    repo.mkdir()
    _git(repo, "init", "--quiet")
    _git(repo, "config", "user.name", "CT Fixture")
    _git(repo, "config", "user.email", "ct-fixture@example.invalid")
    _git(repo, "config", "commit.gpgsign", "false")

    def commit(msg, files=None, rm=None):
        for name, content in (files or {}).items():
            (repo / name).write_text(content)
        if rm is not None:
            _git(repo, "rm", "--quiet", rm)
        _git(repo, "add", "--all")
        _git(repo, "commit", "--quiet", "--no-gpg-sign", "-m", msg)

    commit(
        "root",
        files={
            "keep.txt": "kept on both tips\n",
            "edit.py": "a = 1\nb = 2\n",
            "gone.txt": "deleted on Y\n",
            "old_name.txt": "identity survives the rename\n",
        },
    )
    # The root commit lands on the init default branch (master/main); rename it
    # to root so neither side of the diff ever carries an untracked branch tip.
    _git(repo, "branch", "--move", _git(repo, "symbolic-ref", "--short", "HEAD").strip(), "root")
    _git(repo, "checkout", "--quiet", "-b", "X")
    commit("X: edit + rename", files={"edit.py": "a = 10\nb = 20\n"})
    _git(repo, "mv", "old_name.txt", "new_name.txt")
    _git(repo, "add", "--all")
    _git(repo, "commit", "--quiet", "--no-gpg-sign", "-m", "X: rename")
    x = _git(repo, "rev-parse", "HEAD").strip()

    root = _git(repo, "rev-parse", "X~2").strip()
    _git(repo, "checkout", "--quiet", "-b", "Y", root)
    commit(
        "Y: edit + delete",
        files={"edit.py": "a = 1\nb = 2\nc = 3\n"},
        rm="gone.txt",
    )
    y = _git(repo, "rev-parse", "HEAD").strip()
    return {"repo": repo, "root": root, "x": x, "y": y}


def _blob(repo, ref: str, path: str) -> str | None:
    out = _git(repo, "ls-tree", "-r", ref, "--", path)
    return out.split()[2] if out.strip() else None


# ---------------------------------------------------------------------------
# 1. The classifier: union rows, blob-id identity, A-perspective stats.
# ---------------------------------------------------------------------------


def test_union_rows_classified_by_blob_identity(fixture_repo):
    repo = fixture_repo["repo"]
    rows = lineage_diff_ct.lineage_diff(str(repo), fixture_repo["x"], fixture_repo["y"])
    got = {r.path: r for r in rows}

    # One row per path in the UNION of the two tips -- no more, no fewer.
    assert set(got) == {"keep.txt", "edit.py", "gone.txt", "old_name.txt", "new_name.txt"}
    # keep.txt is identical on both tips: only visible because the row set is
    # the union, never in a plain tip-vs-tip diff.
    assert got["keep.txt"].cls == "same"
    assert got["keep.txt"].ins == 0 and got["keep.txt"].dels == 0
    # Same path, different content on both tips.
    assert got["edit.py"].cls == "differs"
    # Deletion on Y -> only-X (X is ref A); the rename keeps the blob under
    # its old name on X only.
    assert got["gone.txt"].cls == "only-A"
    # Rename with unchanged content: new name only-X, old name only-Y, ONE blob.
    # X is the ref that renamed; Y branched at the root, so it still holds the
    # old name. The identity survives as one blob under two names.
    assert got["old_name.txt"].cls == "only-B"
    assert got["new_name.txt"].cls == "only-A"
    assert (
        _blob(repo, fixture_repo["x"], "new_name.txt")
        == _blob(repo, fixture_repo["root"], "old_name.txt")
        is not None
    )
    # Content-identity rename: with --no-renames the classifier sees a delete
    # on the old name and an add on the new name -- the stat volume is (0,1)
    # and (1,0) respectively, and the IDENTITY is the shared blob id pinned
    # above, not a zero stat.
    assert (got["old_name.txt"].ins, got["old_name.txt"].dels) == (0, 1)
    assert (got["new_name.txt"].ins, got["new_name.txt"].dels) == (1, 0)


def test_stats_are_from_ref_a_perspective(fixture_repo):
    """`git diff B A`: A=X turns edit.py's 2 lines into 2 different ones
    (2 ins, 2 dels) plus one added line (c is B's, not A's -- wait: A adds a
    third line relative to B's 3? No: B has 3 lines, A has 2, all different
    where they overlap): 2 ins, 3 dels. gone.txt is only-A -> (1, 0)."""
    repo = fixture_repo["repo"]
    got = {r.path: r for r in lineage_diff_ct.lineage_diff(
        str(repo), fixture_repo["x"], fixture_repo["y"]
    )}
    assert (got["edit.py"].ins, got["edit.py"].dels) == (2, 3)
    assert (got["gone.txt"].ins, got["gone.txt"].dels) == (1, 0)
    # Flipping the arguments flips the direction: B=Y has 3 lines vs A's 2.
    flipped = {r.path: r for r in lineage_diff_ct.lineage_diff(
        str(repo), fixture_repo["y"], fixture_repo["x"]
    )}
    assert (flipped["edit.py"].ins, flipped["edit.py"].dels) == (3, 2)
    assert (flipped["gone.txt"].ins, flipped["gone.txt"].dels) == (0, 1)


def test_classifier_is_read_only(fixture_repo):
    """Tips, object set and index are untouched by a classification run."""
    repo = str(fixture_repo["repo"])
    objects_before = set(_git(repo, "rev-list", "--objects", "--all").split())
    status_before = _git(repo, "status", "--porcelain")
    tips_before = (_git(repo, "rev-parse", "X").strip(), _git(repo, "rev-parse", "Y").strip())

    lineage_diff_ct.lineage_diff(repo, "X", "Y")
    lineage_diff_ct._header(repo, "X", "Y")

    assert _git(repo, "status", "--porcelain") == status_before == ""
    assert _git(repo, "rev-list", "--objects", "--all").split()
    assert set(_git(repo, "rev-list", "--objects", "--all").split()) == objects_before
    assert (_git(repo, "rev-parse", "X").strip(), _git(repo, "rev-parse", "Y").strip()) == tips_before


def test_header_names_both_tips_and_the_divergence(fixture_repo):
    header = lineage_diff_ct._header(
        str(fixture_repo["repo"]), "X", "Y"
    )
    assert fixture_repo["x"] in header and fixture_repo["y"] in header
    assert "merge-base " in header
    # X carries 2 commits since the root (edit, rename); Y carries 1 (edit+delete).
    assert "A +2" in header and "B +1" in header


def test_unrelated_histories_degrade_to_no_common_base(fixture_repo, tmp_path):
    """Two repos with no shared object database: rev-parse resolves each ref
    inside its own repo, the classifier works per-repo (the cross-check is
    per-side), and the shared merge-base degrades to the documented
    "no common merge-base" header line instead of crashing."""
    a_repo, b_repo = fixture_repo["repo"], tmp_path / "ct-repo-unrelated"
    b_repo.mkdir()
    _git(b_repo, "init", "--quiet")
    _git(b_repo, "config", "user.name", "CT Fixture")
    _git(b_repo, "config", "user.email", "ct-fixture@example.invalid")
    (b_repo / "only.txt").write_text("unrelated\n")
    _git(b_repo, "add", "--all")
    _git(b_repo, "commit", "--quiet", "--no-gpg-sign", "-m", "unrelated root")
    other_sha = _git(b_repo, "rev-parse", "HEAD").strip()

    # The header probe is the only cross-repo call: it must report the
    # missing merge-base, not raise.
    header = lineage_diff_ct._header(
        str(a_repo), other_sha, fixture_repo["y"]
    )
    assert "no common merge-base" in header
    # Per-repo classification still returns the full union of each side.
    got_a = {
        r.path: r for r in lineage_diff_ct.lineage_diff(
            str(a_repo), fixture_repo["x"], fixture_repo["y"]
        )
    }
    for r in got_a.values():
        assert r.cls in ("same", "differs", "only-A", "only-B")
    got_b = {
        r.path: r for r in lineage_diff_ct.lineage_diff(
            str(b_repo), other_sha, other_sha
        )
    }
    assert got_b == {"only.txt": got_b["only.txt"]}
    assert got_b["only.txt"].cls == "same"


def test_cli_json_contract(fixture_repo):
    """The operator surface: exit 0, CLASS table, and the same rows as JSON."""
    repo = str(fixture_repo["repo"])
    argv = [fixture_repo["x"], fixture_repo["y"], "--repo", repo]
    table = subprocess.run(
        [sys.executable, str(SCRIPT), *argv],
        capture_output=True,
        text=True,
    )
    assert table.returncode == 0, table.stderr
    assert "CLASS" in table.stdout and "same=" in table.stdout

    as_json = subprocess.run(
        [sys.executable, str(SCRIPT), *argv, "--json"],
        capture_output=True,
        text=True,
    )
    assert as_json.returncode == 0, as_json.stderr
    payload = json.loads(as_json.stdout)
    classes = {r["path"]: r["cls"] for r in payload["rows"]}
    assert classes["keep.txt"] == "same"
    assert classes["edit.py"] == "differs"
    assert classes["old_name.txt"] == "only-B"
    assert classes["new_name.txt"] == "only-A"
    # Header and rows agree between the two surfaces for the same tips.
    assert fixture_repo["x"] in payload["header"]


def test_unknown_ref_is_a_bounded_error(fixture_repo):
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "no-such-ref-ct",
            fixture_repo["y"],
            "--repo",
            str(fixture_repo["repo"]),
        ],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 2
    assert "error" in proc.stderr.lower()


# ---------------------------------------------------------------------------
# 2. The completion gate (hive_worker): pure Git evidence, never prose.
# ---------------------------------------------------------------------------

# The deployed worker lives outside this repo (fleet path /opt/graeae); import
# it by file so the review pins the worker that is actually running the gate.
_WORKER_PATH = pathlib.Path("/opt/graeae/hive_worker.py")


@pytest.fixture
def hive_worker(tmp_path):
    assert _WORKER_PATH.is_file(), f"deployed worker missing: {_WORKER_PATH}"
    # The worker imports its sibling fleet modules (hive_budget, provenance,
    # the hive package) by bare name; they live next to the deployed script.
    # The DEPLOYED `hive` package is the one that ships with the deployed
    # worker -- the bus-side `hive/` tree of this repo lacks
    # hive/codex_delivery.py and would shadow it if the repo won sys.path.
    # A scratch sys.path with only /opt/graeae (prepended, not appended)
    # keeps both the real hive package and this repo's conftest out of the
    # import while the module loads.
    saved_path = sys.path[:]
    saved_modules = {
        name: sys.modules.pop(name)
        for name in list(sys.modules)
        if name == "hive" or name.startswith("hive.")
        or name in ("hive_budget", "provenance")
    }
    sys.path.insert(0, str(_WORKER_PATH.parent))
    try:
        spec = importlib.util.spec_from_file_location("hive_worker_ct", _WORKER_PATH)
        mod = importlib.util.module_from_spec(spec)
        sys.modules["hive_worker_ct"] = mod
        spec.loader.exec_module(mod)
    finally:
        sys.path[:] = saved_path
        for name in list(sys.modules):
            if name == "hive" or name.startswith("hive.") or name in (
                "hive_budget",
                "provenance",
            ):
                del sys.modules[name]
        sys.modules.update(saved_modules)
    return mod


def _completion_env():
    env = dict(os.environ)
    env.pop("GIT_DIR", None)
    env.pop("GIT_WORK_TREE", None)
    return env


def test_completion_gate_is_pure_git_state(fixture_repo, hive_worker):
    repo = str(fixture_repo["repo"])
    # HEAD must be at X for the probe (the branch this fixture models as the
    # job's task branch); the checkout is a read of refs, no object is written.
    _git(repo, "checkout", "--quiet", "X")
    env = _completion_env()
    # X vs the root have a clean worktree and no remote: everything
    # Git-derived, and the gate must report not-ready for the ABSENT remote,
    # not for any text.
    ev = hive_worker.completion_evidence(repo, fixture_repo["root"], env)
    assert ev["head_sha"] == fixture_repo["x"]
    assert len(ev["commits"]) == 2  # edit commit + rename commit
    assert ev["files_changed"]
    assert ev["tracked_worktree_clean"] is True
    assert ev["worktree_clean"] is True
    assert ev["remote_verified"] is False
    assert hive_worker.completion_ready(ev) is False

    # Flipping the worktree dirty is a Git fact the gate must see. `*.txt` is
    # not git-ignored on this host; the probe itself must not depend on the
    # host's ignore rules, so the dirty marker is a tracked file being edited.
    tracked = pathlib.Path(repo) / "edit.py"
    original = tracked.read_text()
    try:
        tracked.write_text(original + "# dirty marker\n")
        ev_dirty = hive_worker.completion_evidence(repo, fixture_repo["root"], env)
        assert ev_dirty["tracked_worktree_clean"] is False
        assert ev_dirty["worktree_clean"] is False
        assert hive_worker.completion_ready(ev_dirty) is False
    finally:
        tracked.write_text(original)
    # A pure UNTRACKED file still must not make the tracked tree dirty -- the
    # two cleanliness flags are distinct facts (the gate requires the tracked
    # one, and reports untracked files separately).
    untracked = pathlib.Path(repo) / "dirty.txt"
    untracked.write_text("scratch\n")
    try:
        ev_untracked = hive_worker.completion_evidence(repo, fixture_repo["root"], env)
        assert ev_untracked["tracked_worktree_clean"] is True
        assert ev_untracked["worktree_clean"] is False
        assert ev_untracked["untracked_files"]
        assert hive_worker.completion_ready(ev_untracked) is False
    finally:
        untracked.unlink()


def test_completion_ready_requires_a_verified_remote(hive_worker):
    """The ready predicate: commits AND changed files AND clean tracked tree
    AND remote_verified AND remote sha == head sha. The fixture can satisfy
    the first three and the remote check still fails, so readiness stays
    False without a push -- exactly the contract this job is gated on."""
    ev = {
        "commits": ["abc123"],
        "files_changed": ["scripts/lineage_diff.py"],
        "tracked_worktree_clean": True,
        "worktree_clean": True,
        "remote_verified": False,
        "remote_sha": None,
        "head_sha": "abc123",
    }
    assert hive_worker.completion_ready(ev) is False
    ev.update(remote_verified=True, remote_sha="abc123")
    assert hive_worker.completion_ready(ev) is True
    # A remote that does not match the local head is not verified.
    ev.update(remote_sha="deadbeef")
    assert hive_worker.completion_ready(ev) is False


def test_verify_delivery_reports_bounded_evidence(fixture_repo, hive_worker, capsys):
    """`--verify-delivery` prints one bounded JSON line and exits 1 for an
    unpushed fixture: the report names the required steps, never Git stderr
    or remote URLs."""
    _git(fixture_repo["repo"], "checkout", "--quiet", "X")
    rc = hive_worker.verify_delivery(
        str(fixture_repo["repo"]), fixture_repo["root"]
    )
    assert rc == 1
    out = capsys.readouterr().out
    report = json.loads(out.strip().splitlines()[-1])
    assert report["delivery_ready"] is False
    assert report["commit_count"] == 2  # X: edit commit + rename commit
    assert report["changed_file_count"] >= 2  # edit.py, old_name.txt, gone.txt
    assert report["worktree_clean"] is True
    assert report["remote_verified"] is False
    for key in (
        "delivery_ready",
        "commit_count",
        "changed_file_count",
        "worktree_clean",
        "remote_verified",
        "required",
    ):
        assert key in report
    assert report["files_changed"]  # X changed edit.py, renamed old_name.txt
