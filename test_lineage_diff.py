"""scripts/lineage_diff.py must classify every row of a tiny fixture repo.

The fixture is a throwaway git repository built in the caller's tmp dir:
several commits on two branches sharing a root, covering all four classes --

  same     a file present on BOTH tips with identical content (same blob id)
           -- including a file neither tip ever changed after the root
  differs  a file present on both tips with different content
  only-A   a file present only on branch A (including a path a rename
           moved away from on A)
  only-B   a file present only on branch B (including a path DELETED on A)

plus the edge cases the classifier exists for:

  * a rename with UNCHANGED content: the old name is only-B, the new name is
    only-A, and the test proves the two names carry the SAME blob -- content
    identity, not path identity, is what a convergence proposal decides on;
  * a file identical on both tips appears in the table as `same` even though
    a plain `git diff` between the tips never names it -- the row set is the
    UNION of the two tips' trees, which is the whole point of the script;
  * a file deleted on one side is `only-<the other side>`;
  * `differs` rows carry the stat line counts from `git diff --stat`.

The script must not modify either branch: the test asserts both branch tips
before and after the call, and that no object beyond the fixture commits
appears (the script's git invocations are all read-only).
"""

from __future__ import annotations

import importlib.util
import os
import pathlib
import subprocess
import sys

import pytest

REPO = pathlib.Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("lineage_diff", REPO / "scripts" / "lineage_diff.py")
lineage_diff = importlib.util.module_from_spec(SPEC)
sys.modules["lineage_diff"] = lineage_diff  # dataclass introspection needs the module registered
SPEC.loader.exec_module(lineage_diff)


def _git(repo, *args, check=True):
    """Run a git command; returns stdout. Raises on nonzero exit.

    GIT_DIR/GIT_WORK_TREE are cleared so a host that runs git with its own
    exported git dir cannot bleed into the fixture (a fixture must be the
    only repo in sight, or the diff under test is a diff of the wrong thing).
    """
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


def _blob(repo, ref: str, path: str) -> str | None:
    """The blob id a ref's tree has for `path`, or None if the path is absent."""
    out = _git(repo, "ls-tree", "-r", ref, "--", path)
    if not out.strip():
        return None
    return out.split()[2]


def _commit(repo, message: str, files: dict[str, str] | None = None, rm=None):
    """Commit: write `files` (name -> content) and/or `git rm` `rm`."""
    for name, content in (files or {}).items():
        path = repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    if rm is not None:
        _git(repo, "rm", "--quiet", rm)
    _git(repo, "add", "--all")
    _git(repo, "commit", "--quiet", "--no-gpg-sign", "-m", message)


@pytest.fixture
def fixture_repo(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "--quiet")
    _git(repo, "config", "user.name", "Fixture Author")
    _git(repo, "config", "user.email", "fixture@example.invalid")
    _git(repo, "config", "commit.gpgsign", "false")

    # root: five files. `shared.py` is never touched again -> `same` via the
    # union (absent from any tip-vs-tip diff). A and B both edit `both.py`
    # differently -> `differs`. A renames `moved.txt` -> `moved_new.txt`
    # (content kept); B deletes `moved.txt` and adds `moved_b.txt` with the
    # SAME content -> the rename/identity edge case.
    _commit(
        repo,
        "root",
        files={
            "shared.py": "shared root\n",
            "both.py": "both root\n",
            "moved.txt": "moved root\n",
            "stale.py": "stale root\n",
        },
    )
    root_sha = _git(repo, "rev-parse", "HEAD").strip()

    # branch A: shared edited (to 'A edit'), both edited (A version),
    # a_only added, stale deleted, moved.txt renamed to moved_new (content kept).
    _git(repo, "checkout", "--quiet", "-b", "A")
    _commit(
        repo,
        "A: first pass",
        files={"shared.py": "A edit\n", "both.py": "both A v1\nboth A v2\n"},
    )
    _commit(repo, "A: add a_only", files={"a_only.py": "A only file\n"})
    _commit(repo, "A: drop stale", rm="stale.py")
    _git(repo, "mv", "moved.txt", "moved_new.txt")
    _git(repo, "add", "--all")
    _git(repo, "commit", "--quiet", "--no-gpg-sign", "-m", "A: rename moved")
    a_sha = _git(repo, "rev-parse", "HEAD").strip()

    # branch B: from root again. shared edited (to 'A edit' -- byte-identical
    # to A's, so `same`), both edited (B version -- `differs`), b_only added,
    # moved.txt DELETED, and moved_b.txt ADDED with the same content A's
    # rename carried.
    _git(repo, "checkout", "--quiet", "-b", "B", root_sha)
    _commit(
        repo,
        "B: the whole pass",
        files={
            "shared.py": "A edit\n",
            "both.py": "both B v1\nboth B v2\nboth B v3\n",
            "b_only.py": "B only file\n",
            "moved_b.txt": "moved root\n",
        },
    )
    _git(repo, "rm", "--quiet", "moved.txt")
    _git(repo, "commit", "--quiet", "--no-gpg-sign", "-m", "B: swap moved")
    b_sha = _git(repo, "rev-parse", "HEAD").strip()
    return {
        "repo": repo,
        "root": root_sha,
        "a": a_sha,
        "b": b_sha,
    }


def test_every_classification_on_the_fixture(fixture_repo):
    repo = str(fixture_repo["repo"])
    a, b = fixture_repo["a"], fixture_repo["b"]

    # Read-only proof: both tips, and the object set, are unchanged after.
    objects_before = set(_git(repo, "rev-list", "--objects", "--all").split())

    rows = lineage_diff.lineage_diff(repo, a, b)
    got = {r.path: r for r in rows}

    objects_after = set(_git(repo, "rev-list", "--objects", "--all").split())
    assert objects_before == objects_after, "lineage_diff created or removed objects"
    assert _git(repo, "rev-parse", "A").strip() == a, "branch A was modified"
    assert _git(repo, "rev-parse", "B").strip() == b, "branch B was modified"
    # No working-tree change either (the script must not touch the index).
    assert _git(repo, "status", "--porcelain") == ""

    # The row set is the UNION of the two tips' trees -- one row per path,
    # no duplicates. `shared.py` (identical on both tips, never in any tip-vs-
    # tip diff) MUST be present as `same`; that is the bug the old
    # diff-only implementation had.
    assert len(rows) == len({r.path for r in rows})
    expected_paths = {
        "a_only.py",  # only-A
        "b_only.py",  # only-B
        "both.py",  # differs
        "moved_new.txt",  # only-A (the rename destination on A)
        "moved_b.txt",  # only-B (B's replacement for the deleted moved.txt)
        "shared.py",  # same -- identical on both tips
        "stale.py",  # only-B (deleted on A)
    }
    assert set(got) == expected_paths, f"rows: {[(r.cls, r.path) for r in rows]}"

    # --- the classes -----------------------------------------------------
    assert got["shared.py"].cls == "same"  # identical content on both tips
    assert got["both.py"].cls == "differs"  # same path, different content
    assert got["a_only.py"].cls == "only-A"  # present only on A
    assert got["b_only.py"].cls == "only-B"  # present only on B
    assert got["stale.py"].cls == "only-B"  # deleted on A == only-B

    # --- the rename / identity edge --------------------------------------
    # A renamed moved.txt -> moved_new.txt (content kept); B deleted moved.txt
    # and added moved_b.txt with the SAME content. So the root blob survives
    # under two names, one per tip, and the third name is gone from both:
    assert got["moved_new.txt"].cls == "only-A"
    assert got["moved_b.txt"].cls == "only-B"
    assert _blob(repo, a, "moved_new.txt") == _blob(repo, b, "moved_b.txt")
    assert _blob(repo, a, "moved_new.txt") == _blob(repo, fixture_repo["root"], "moved.txt")
    assert _blob(repo, a, "moved_new.txt") is not None
    # moved.txt is absent from BOTH tips in this fixture: A renamed it away
    # and B deleted it, so it contributes no row -- the union is over the TIPS,
    # not the history between them.
    assert "moved.txt" not in got

    # --- the table carries stat line counts ------------------------------
    # both.py: A has 2 lines, B has 3. Stats are from A's perspective
    # (ins = lines A adds relative to B, dels = lines A removes): 2 ins, 3 dels.
    assert got["both.py"].ins == 2 and got["both.py"].dels == 3
    # Content-identity rows: moved_new.txt is only-A, so from A's perspective
    # A adds it relative to B -> (1, 0). moved_b.txt is only-B -> (0, 1).
    assert (got["moved_new.txt"].ins, got["moved_new.txt"].dels) == (1, 0)
    assert (got["moved_b.txt"].ins, got["moved_b.txt"].dels) == (0, 1)
    # `same` rows carry no change at all -- nothing differs between the tips.
    assert (got["shared.py"].ins, got["shared.py"].dels) == (0, 0)
    # only-A/only-B rows carry their own line count (from A's perspective):
    # b_only.py is only-B -> A removes it relative to B -> (0, 1).
    assert got["b_only.py"].ins == 0

    # `differs` rows never have zero on both counts (content really moved).
    for r in rows:
        if r.cls == "differs":
            assert r.ins + r.dels > 0


def test_header_reports_both_tips_and_the_merge_base(fixture_repo):
    repo = str(fixture_repo["repo"])
    header = lineage_diff._header(repo, fixture_repo["a"], fixture_repo["b"])
    assert fixture_repo["a"] in header
    assert fixture_repo["b"] in header
    assert "merge-base " in header
    # A and B each carry commits after the root.
    assert "A +" in header
    assert "B +" in header


def test_main_table_and_json_exit_zero(fixture_repo):
    repo = str(fixture_repo["repo"])
    argv = [fixture_repo["a"], fixture_repo["b"], "--repo", repo]
    proc = subprocess.run(
        [sys.executable, str(REPO / "scripts" / "lineage_diff.py"), *argv],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert "CLASS" in proc.stdout
    assert "differs=" in proc.stdout
    assert "same=" in proc.stdout

    proc = subprocess.run(
        [sys.executable, str(REPO / "scripts" / "lineage_diff.py"), *argv, "--json"],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    import json

    payload = json.loads(proc.stdout)
    classes = {r["path"]: r["cls"] for r in payload["rows"]}
    # The JSON path must agree with the in-process call: same rows, same classes.
    assert classes["shared.py"] == "same"
    assert classes["both.py"] == "differs"
    assert classes["a_only.py"] == "only-A"
    assert classes["b_only.py"] == "only-B"
    assert classes["moved_new.txt"] == "only-A"
    assert classes["moved_b.txt"] == "only-B"


def test_missing_ref_is_a_clean_error(fixture_repo):
    repo = str(fixture_repo["repo"])
    proc = subprocess.run(
        [
            sys.executable,
            str(REPO / "scripts" / "lineage_diff.py"),
            "no-such-ref",
            fixture_repo["b"],
            "--repo",
            repo,
        ],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 2
    assert "error" in proc.stderr.lower()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
