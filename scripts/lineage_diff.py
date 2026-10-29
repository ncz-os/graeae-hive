#!/usr/bin/env python3
"""Per-file lineage diff between two git refs.

Prints one row per path present in EITHER tip, classified:

  same     present on both sides with identical content (same blob id)
  differs  present on both sides with different content
  only-A   present only on ref A
  only-B   present only on ref B

Classification is by BLOB ID read from each tip's tree: every path in the
union of the two trees gets exactly one row, including files that are
identical on both sides -- which a plain ``git diff`` between the tips never
shows at all, and which a convergence proposal must still name. A rename with
unchanged content therefore appears as an ``only-A`` old name plus an
``only-B`` new name sharing one blob; the content identity is visible in the
output, and that is what a convergence decision needs.

Method (all git plumbing; neither ref, the index, nor the working tree is
touched, and no object is written):

  1. ``git ls-tree -r <ref>`` enumerates the FULL tree of each tip
     (name + blob id). The union of the two trees is the row set -- nothing
     that exists on either side can be missed, whatever the history between
     the refs did.
  2. ``git diff-tree -r <A> <B>`` (raw, no renames) supplies the object ids
     for the cross-check: it must name exactly the paths whose blob ids
     differ, with the same ids. It is the only ``diff``-family call, used as
     a consistency check, not as the source of rows.
  3. ``git diff --stat <A> <B>`` supplies the line-level volume so `differs`
     rows are a real table, not just classes.
  4. ``git merge-base`` + ``git rev-list --count`` supply the commit counts
     each side carried since the common ancestor, printed in the header.

Read-only: the only git invocations are ``rev-parse``, ``ls-tree``,
``diff-tree``, ``diff`` and ``rev-list``.

Usage:
    scripts/lineage_diff.py A [B]
    A, B default to the two live deploy lineages of this repo.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass

DEFAULT_A = "deploy/integrate-20261017"
DEFAULT_B = "deploy/hive-canonical-20261029"

# git diff-tree --raw line: `:100644 100644 <old> <new> <S>\t<path>`
_RAW_LINE = re.compile(
    r"^:(?P<old_mode>\d{6}) (?P<new_mode>\d{6}) "
    r"(?P<old_sha>[0-9a-f]{7,40}) (?P<new_sha>[0-9a-f]{7,40}) "
    r"(?P<status>[MADRCUT])\t(?P<path>.+)$"
)
# git ls-tree -r --long line: `<mode> blob <sha> <size>\t<path>` where the size
# column is SPACE-padded (only the final field separator is a real tab), e.g.
# `100644 blob <40hex>      12\ta_only.py`. The path is quoted with C-style
# escaping if it holds special characters; none do in this repo or fixtures.
_LS_LONG = re.compile(r"^(?P<mode>\d{6}) blob (?P<sha>[0-9a-f]{7,40})\s+\d+\t(?P<path>.+)$")
# git diff --stat per-file line: ` <path> | <n> [+-bar]`
_STAT_LINE = re.compile(r"^ (?P<path>\S.*?)\s*\|\s*(?P<changes>\d+)(?:\s+(?P<bar>[+\-]+))?\s*$")
_ZERO = "0" * 40


def _git(repo: str, *args: str, env: dict[str, str] | None = None) -> str:
    """Run a read-only git command; fail loudly with the command attached.

    ``GIT_DIR``/``GIT_WORK_TREE`` are always cleared: a caller that runs with
    its own git dir exported (e.g. a fixture built under a scratch GIT_DIR)
    must not bleed it into the read-only plumbing here.
    """
    clean_env = dict(os.environ)
    clean_env.pop("GIT_DIR", None)
    clean_env.pop("GIT_WORK_TREE", None)
    if env:
        clean_env.update(env)
    proc = subprocess.run(
        ["git", "-C", repo, *args],
        capture_output=True,
        text=True,
        timeout=120,
        env=clean_env,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed ({proc.returncode}): {proc.stderr.strip()[:400]}")
    return proc.stdout


@dataclass
class Row:
    path: str
    cls: str  # same | differs | only-A | only-B
    ins: int = 0
    dels: int = 0


def _parse_stat(text: str) -> dict[str, tuple[int, int]]:
    """Map path -> (insertions, deletions) from `git diff --stat` output.

    Every per-file line starts with a single space: ``<path> | <n> <bar>``.
    The bar is proportional, not exact; the change total IS exact. A pure
    rename (unchanged content) prints as ``src => dst | 0``: that line is
    consumed and NOT mapped, so no row is polluted by a stat line that names
    two paths.
    """
    out: dict[str, tuple[int, int]] = {}
    for line in text.splitlines():
        if not line.startswith(" "):
            continue
        m = _STAT_LINE.match(line)
        if not m:
            continue
        if " => " in line:  # rename line: `src => dst | 0`
            continue
        path = m.group("path")
        changes = int(m.group("changes"))
        bar = m.group("bar") or ""
        plus, minus = bar.count("+"), bar.count("-")
        if changes == 0 or plus + minus == 0:
            out[path] = (0, 0)
            continue
        ratio = plus / (plus + minus)
        out[path] = (round(changes * ratio), changes - round(changes * ratio))
    return out


def _tip_blobs(repo: str, sha: str) -> dict[str, str]:
    """path -> blob id for every path in the tip's tree (full, recursive)."""
    out: dict[str, str] = {}
    for line in _git(repo, "ls-tree", "-r", "--long", sha).splitlines():
        m = _LS_LONG.match(line)
        if not m:  # pragma: no cover - git never emits anything else
            continue
        out[m.group("path")] = m.group("sha")
    return out


def lineage_diff(repo: str, ref_a: str, ref_b: str) -> list[Row]:
    """One Row per path in either tip, classified by content identity.

    The row set is the UNION of the two tips' trees. A path identical on both
    sides is `same`; git diff between the two tips alone cannot produce those
    rows (there is no line for a path whose blob id is equal on both ends),
    which is exactly what the union is for.
    """
    a_sha = _git(repo, "rev-parse", ref_a).strip()
    b_sha = _git(repo, "rev-parse", ref_b).strip()

    tree_a = _tip_blobs(repo, a_sha)
    tree_b = _tip_blobs(repo, b_sha)

    # `diff-tree -r A B` (two SHAs = commit-vs-commit, never the worktree)
    # names exactly the paths whose blob ids differ. It cross-checks the
    # ls-tree union: any path it names must exist in the union with matching
    # ids, so a partial tree read (pruned clone, partial output) cannot
    # silently misclassify a row.
    cross: dict[str, tuple[str, str]] = {}
    for line in _git(repo, "diff-tree", "-r", "--no-renames", a_sha, b_sha).splitlines():
        m = _RAW_LINE.match(line)
        if not m:
            continue
        cross[m.group("path")] = (m.group("old_sha"), m.group("new_sha"))
    for path, (old, new) in cross.items():
        if path in tree_a and old != _ZERO and tree_a[path] != old:
            raise RuntimeError(f"diff-tree and ls-tree disagree on {path!r} for ref A")
        if path in tree_b and new != _ZERO and tree_b[path] != new:
            raise RuntimeError(f"diff-tree and ls-tree disagree on {path!r} for ref B")

    # Line-level volume, computed B-then-A so that a row's `ins`/`dels` are
    # from A's perspective: `ins` = lines A adds relative to B, `dels` = lines
    # A removes relative to B. `git diff <sha> <sha>` (two SHAs, no `..`) is
    # commit-vs-commit; the `..` form is index-vs-commit and would silently
    # diff the live working tree.
    stat_map = _parse_stat(_git(repo, "diff", "--stat", "--no-renames", b_sha, a_sha))

    rows: list[Row] = []
    for path in sorted(set(tree_a) | set(tree_b)):
        a_blob = tree_a.get(path)
        b_blob = tree_b.get(path)
        if a_blob is not None and b_blob is not None:
            cls = "same" if a_blob == b_blob else "differs"
        elif a_blob is not None:
            cls = "only-A"
        else:
            cls = "only-B"
        ins, dels = stat_map.get(path, (0, 0))
        rows.append(Row(path=path, cls=cls, ins=ins, dels=dels))
    return rows


def _header(repo: str, ref_a: str, ref_b: str) -> str:
    a = _git(repo, "rev-parse", ref_a).strip()
    b = _git(repo, "rev-parse", ref_b).strip()
    try:
        base = _git(repo, "merge-base", a, b).strip()
        n_a = int(_git(repo, "rev-list", "--count", f"{base}..{a}").strip())
        n_b = int(_git(repo, "rev-list", "--count", f"{base}..{b}").strip())
        base_line = f"merge-base {base[:12]} (A +{n_a}, B +{n_b} commits since)"
    except RuntimeError:  # unrelated histories
        base_line = "no common merge-base (unrelated histories)"
    return f"A={a}  B={b}  {base_line}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("a", nargs="?", default=DEFAULT_A, help=f"ref A (default {DEFAULT_A})")
    parser.add_argument("b", nargs="?", default=DEFAULT_B, help=f"ref B (default {DEFAULT_B})")
    parser.add_argument("--repo", default=os.getcwd(), help="git repo to read (default: current dir)")
    parser.add_argument("--json", action="store_true", help="emit JSON instead of the table")
    args = parser.parse_args(argv)

    try:
        header = _header(args.repo, args.a, args.b)
        rows = lineage_diff(args.repo, args.a, args.b)
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.json:
        import json

        print(json.dumps({"header": header, "rows": [vars(r) for r in rows]}, indent=1))
        return 0

    print(header)
    counts = {c: 0 for c in ("same", "differs", "only-A", "only-B")}
    for r in rows:
        counts[r.cls] += 1
    print(
        f"total {len(rows)} paths: same={counts['same']} differs={counts['differs']} "
        f"only-A={counts['only-A']} only-B={counts['only-B']}"
    )
    print(f"{'CLASS':<8} {'+':>5} {'-':>5}  {'PATH'}")
    for r in rows:
        print(f"{r.cls:<8} {r.ins:>5} {r.dels:>5}  {r.path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
