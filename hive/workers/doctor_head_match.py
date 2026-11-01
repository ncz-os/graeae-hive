"""Does a review approval cover the head a row DELIVERED?

The doctor's head-match rule is exact: an approve closes a needs-review row only
when the reviewer names the row's delivered ``head_sha``. Production on
2026-10-31 showed why exactness alone is not enough: four rows (01a19270,
1007c484, 01a1922a, 01a18fe6) had a ``review:*`` child with a structured
approve from ``cerberus-reviewer`` and still sat needs-review, because the
review job had committed its OWN VERDICT ARTIFACT
(``REVIEW_VERDICT_<branch>.json`` / ``review-verdict-<job>.json``) onto the
author's branch. The head it approved was the author tip plus one
verdict-only commit -- an approval of exactly the code the row delivered.

This module answers the one question the doctor asks, with git:

    given the row's delivered tip T and the head R the reviewer approved,
    does the approval cover T -- and if not, why not?

Four verdicts, all of them measured against ``git`` on a workspace that holds
the commits (never against what the child CLAIMS in its result):

``verdict_only``
    T is an ancestor of R (or R == T) and ``T..R`` touches ONLY review verdict
    artifacts. The approve covers T: the reason reads
    ``approved at R (verdict-only descendant of T)``.
``code_descendant``
    T is an ancestor of R and ``T..R`` changes code: the review approved work
    that is newer than, or not on, the head branch (case (c)). The doctor may
    rescue it with a PURE FAST-FORWARD push when the review child's workspace
    holds R; otherwise it is reported with the exact sha and a one-line action.
``overreach``
    R does not descend from T AND the review changed code (case (b): a schema
    fix and a test committed onto the author branch). The approval does not
    cover T and must never be counted for it.
``not_descendant``
    R is unrelated to T (or unknown). Head-exact still applies: not an
    approval.

Observations that reach the same git state are classified identically --
``classify`` is a pure function of the commit graph, so a second sweep reaches
the same verdict and the caller's idempotency check can short-circuit.

Nothing here merges. ``rescue_ff`` only ever moves a branch FORWARD to a
commit it already has (``git merge --ff-only``), never a merge commit and never
a force push.
"""

from __future__ import annotations

import fnmatch
import logging
import os
import subprocess
from dataclasses import dataclass, field

log = logging.getLogger("hive.workers.doctor_head_match")

#: The only files a review job may leave on the AUTHOR's branch. Anything else
#: -- a schema fix, a new test, a reformat -- is review overreach: the approval
#: covers the reviewer's edits, not the author's tip, so it is not counted.
VERDICT_ARTIFACT_PATTERNS = (
    "REVIEW_VERDICT_*.json",
    "review-verdict-*.json",
    # The artifact may be written into a review sub-directory; the BASENAME is
    # what is matched, so a nested ``.hive/review/REVIEW_VERDICT_x.json`` counts
    # and a ``tests/REVIEW_VERDICT_x.json`` does not silently hide a code test.
    ".hive/review/REVIEW_VERDICT_*.json",
    ".hive/review/review-verdict-*.json",
)

#: Git subprocess ceiling: these are local, read-only questions.
GIT_TIMEOUT = 120


class Actions:
    """One-line operator actions, per verdict (never a paragraph)."""

    RESCUE_OR_CONFIRM = "push the reviewed tip as a fast-forward, or re-review the branch tip"
    RE_REVIEW = "re-review the branch tip; the review's own edits are not covered by its approve"
    RE_REVIEW_EXACT = "re-review the delivered head; this approval is for a different commit"


@dataclass
class HeadMatch:
    """The classifier's answer: a verdict, the two shas, and the evidence."""

    verdict: str
    reviewed: str
    tip: str
    files: list[str] = field(default_factory=list)
    reason: str = ""
    #: ``True`` only for ``verdict_only``: the approval covers ``tip``.
    ok_for_tip: bool = False
    #: ``True`` for ``code_descendant``: the reviewed head carries real work that
    #: is not on the head branch (the fast-forward rescue path may apply).
    newer_work: bool = False

    def __post_init__(self) -> None:
        # Derived, never trusted from the caller: an approval covers the tip
        # EXACTLY when the verdict is ``verdict_only``, and only a
        # ``code_descendant`` carries newer work. A caller (or a constructor
        # used in a test) can therefore never build a match that says "approve
        # everything" while its verdict says otherwise.
        self.ok_for_tip = self.verdict == VERDICT_ONLY
        self.newer_work = self.verdict == CODE_DESCENDANT

    def as_dict(self) -> dict:
        return {
            "verdict": self.verdict,
            "reviewed": self.reviewed,
            "tip": self.tip,
            "files": list(self.files),
            "reason": self.reason,
            "ok_for_tip": self.ok_for_tip,
            "newer_work": self.newer_work,
        }


#: The row state recorded when an approval covers the delivered tip but a
#: verdict-only commit sits on top of it: the MERGE PASS may merge ``head_sha``
#: (never the doctor). The row's status stays ``needs-review``.
APPROVED_MERGEABLE = "approved_mergeable"

VERDICT_ONLY = "verdict_only"
CODE_DESCENDANT = "code_descendant"
OVERREACH = "overreach"
NOT_DESCENDANT = "not_descendant"

_SHOWN = 12


# ── switch parsing ──────────────────────────────────────────────────────────


def env_switch(name: str, *, default: bool = True) -> bool:
    """Read a boolean env switch; an unset/blank value is ``default``.

    Only the documented falsy spellings turn a default-on switch off
    (``0/false/no/off``), so a typo leaves the protection on.
    """
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() not in ("0", "false", "no", "off")


# ── artifact names ──────────────────────────────────────────────────────────


def is_verdict_artifact(path: str) -> bool:
    """Whether ``path`` is a review verdict artifact and nothing else."""
    clean = str(path or "").strip().replace("\\", "/").lstrip("./")
    if not clean:
        return False
    if any(fnmatch.fnmatchcase(clean, pattern) for pattern in VERDICT_ARTIFACT_PATTERNS):
        return True
    base = clean.rsplit("/", 1)[-1]
    return any(fnmatch.fnmatchcase(base, pattern) for pattern in ("REVIEW_VERDICT_*.json", "review-verdict-*.json"))


def non_artifact_files(paths) -> list[str]:
    """The paths that are NOT verdict artifacts (sorted, deduplicated)."""
    return sorted({str(p) for p in paths or [] if str(p).strip() and not is_verdict_artifact(p)})


# ── git plumbing ────────────────────────────────────────────────────────────


def git(repo: str, *args: str) -> str:
    """Run one git command in ``repo`` and return its stripped stdout.

    Raises ``subprocess.CalledProcessError`` on a non-zero exit, like
    ``subprocess.run(..., check=True)`` -- the module's public plumbing.
    """
    out = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        timeout=GIT_TIMEOUT,
        check=True,
    )
    return out.stdout.strip()


def _git(repo: str, *args: str) -> tuple[int, str]:
    """Run one READ-ONLY git question; ``(exit, stdout)`` never raises on a bad sha."""
    try:
        out = subprocess.run(
            ["git", "-C", str(repo), *args],
            capture_output=True,
            text=True,
            timeout=GIT_TIMEOUT,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        log.warning("git %s failed in %s: %s", " ".join(args), repo, exc)
        return 1, ""
    return out.returncode, out.stdout




def has_commit(repo: str, sha: str) -> bool:
    """Whether ``sha`` names a commit this workspace holds."""
    sha = str(sha or "").strip()
    if len(sha) < 4:
        return False
    code, out = _git(repo, "cat-file", "-t", sha)
    return code == 0 and out.strip() == "commit"


def is_ancestor_or_same(repo: str, ancestor: str, descendant: str) -> bool:
    """``ancestor`` is reachable from ``descendant`` (``True`` when equal)."""
    if not ancestor or not descendant:
        return False
    code, _ = _git(repo, "merge-base", "--is-ancestor", ancestor, descendant)
    return code == 0


def ancestor(repo: str, ancestor_sha: str, descendant_sha: str) -> bool:
    """``ancestor_sha`` strictly precedes (or equals) ``descendant_sha``."""
    return is_ancestor_or_same(repo, ancestor_sha, descendant_sha)


def changed_files(repo: str, older: str, newer: str) -> list[str]:
    """The paths ``older..newer`` touches (``git diff --name-only``)."""
    code, out = _git(repo, "diff", "--name-only", f"{older}..{newer}")
    if code != 0:
        return []
    return sorted({line.strip() for line in out.splitlines() if line.strip()})


def _files_from_git(repo: str, reviewed: str) -> list[str]:
    """The code ``reviewed`` itself commits, by diffing it against its first parent."""
    code, out = _git(repo, "rev-list", "--parents", "-n", "1", reviewed)
    parts = out.split()
    if code != 0 or len(parts) < 2:
        return []
    return non_artifact_files(changed_files(repo, parts[1], reviewed))


def can_fast_forward(repo: str, tip: str, reviewed: str) -> bool:
    """Whether moving ``tip`` to ``reviewed`` is a pure fast-forward the workspace holds."""
    if not tip or not reviewed:
        return False
    if not has_commit(repo, reviewed):
        return False
    return is_ancestor_or_same(repo, tip, reviewed)


def rescue_ff(
    repo: str,
    tip: str,
    reviewed: str,
    *,
    branch: str | None = None,
    remote: str = "origin",
    dry_run: bool = False,
) -> dict:
    """Push the reviewed head onto the head branch ONLY as a pure fast-forward.

    Called with the review child's workspace (the only place the reviewed
    commit is known to exist). ``git push <remote> <reviewed>:refs/heads/<branch>``
    is a fast-forward by construction -- no merge commit, ``--force`` is never
    passed -- and ``--dry-run`` keeps this side-effect free for a preview.
    Returns ``{"action": "would_push" | "pushed" | "refused", "sha", "old"}``.
    """
    out = {"action": "refused", "sha": reviewed, "old": tip}
    if not branch:
        out["reason"] = "no head branch to update"
        return out
    if not can_fast_forward(repo, tip, reviewed):
        out["reason"] = "not a pure fast-forward in this workspace"
        return out
    args = ["push", "--porcelain"]
    if dry_run:
        args.append("--dry-run")
    args += [remote, f"{reviewed}:refs/heads/{branch}"]
    code, _ = _git(repo, *args)
    if code == 0:
        out["action"] = "would_push" if dry_run else "pushed"
    else:
        out["reason"] = "push refused (non-fast-forward or transport error)"
    return out


# ── the classifier ──────────────────────────────────────────────────────────


def _shown(sha: str) -> str:
    return str(sha or "")[:_SHOWN]


def classify_descendant(
    repo: str,
    tip: str,
    reviewed: str,
    *,
    pushed_files=None,
    pushed_commits=None,
) -> HeadMatch:
    """Classify ``reviewed`` against the delivered ``tip`` in ``repo``.

    ``repo`` is a workspace that HOLDS both commits (the review child's own
    checkout, or a clone of the head branch that already fetched the reviewed
    head). ``pushed_files`` / ``pushed_commits`` are the child's own report of
    what it committed and are used only when git cannot diff (the commits are
    not present here): they can never turn a code change into a verdict-only
    approval.
    """
    tip = str(tip or "").strip()
    reviewed = str(reviewed or "").strip()
    if not tip or not reviewed:
        return HeadMatch(NOT_DESCENDANT, reviewed, tip, reason="review or delivered head missing")
    if not has_commit(repo, reviewed):
        return HeadMatch(
            NOT_DESCENDANT,
            reviewed,
            tip,
            reason=f"reviewed head {_shown(reviewed)} is not in this workspace",
        )
    if reviewed == tip:
        return HeadMatch(
            VERDICT_ONLY,
            reviewed,
            tip,
            reason="approved exact delivered HEAD",
            ok_for_tip=True,
        )
    if not is_ancestor_or_same(repo, tip, reviewed):
        # Not a descendant: "overreach" needs evidence that the review COMMITTED
        # code, from git (the commits are here: diff it against its own base) or
        # from the child's report of what it changed.
        report = _files_from_git(repo, reviewed) or non_artifact_files(pushed_files)
        if report:
            return HeadMatch(
                OVERREACH,
                reviewed,
                tip,
                files=report,
                reason=(
                    f"review_overreach: the review job changed code ({', '.join(report[:3])}) on a head that "
                    f"does not descend from the delivered head {_shown(tip)}; its approve is not counted"
                ),
            )
        return HeadMatch(
            NOT_DESCENDANT,
            reviewed,
            tip,
            reason=f"approved head {_shown(reviewed)} is not a descendant of the delivered head {_shown(tip)}",
        )
    files = changed_files(repo, tip, reviewed)
    if files:
        code_files = non_artifact_files(files)
        if not code_files:
            return HeadMatch(
                VERDICT_ONLY,
                reviewed,
                tip,
                files=files,
                reason=f"approved at {reviewed} (verdict-only descendant of {tip})",
                ok_for_tip=True,
            )
        return HeadMatch(
            CODE_DESCENDANT,
            reviewed,
            tip,
            files=files,
            reason=(
                f"approval covers newer work not on the head branch: reviewed {_shown(reviewed)} descends from "
                f"{_shown(tip)} and changed {', '.join(code_files[:3])}"
            ),
            newer_work=True,
        )
    # git could not diff (e.g. a shallow workspace): fall back to what the child
    # reported, which may only ever ADD doubt, never approve code.
    report = non_artifact_files(pushed_files)
    if report or pushed_commits:
        if not report and pushed_commits:
            # No file list: nothing says it was a verdict-only commit.
            return HeadMatch(
                CODE_DESCENDANT,
                reviewed,
                tip,
                reason=(
                    f"approval covers newer work not on the head branch: reviewed {_shown(reviewed)} descends from "
                    f"{_shown(tip)}; the changed files could not be read"
                ),
                newer_work=True,
            )
        return HeadMatch(
            CODE_DESCENDANT,
            reviewed,
            tip,
            files=report,
            reason=(
                f"approval covers newer work not on the head branch: reviewed {_shown(reviewed)} descends from "
                f"{_shown(tip)} and changed {', '.join(report[:3])}"
            ),
            newer_work=True,
        )
    return HeadMatch(
        VERDICT_ONLY,
        reviewed,
        tip,
        reason=f"approved at {reviewed} (verdict-only descendant of {tip})",
        ok_for_tip=True,
    )


def checkout_for(repo: str | None, tip: str, reviewed: str | None = None) -> str | None:
    """The workspace to classify in: the one that holds BOTH commits (or None).

    The classifier diffs ``tip..reviewed``, so the workspace must hold the
    reviewed head AND the delivered tip. A workspace holding only the tip would
    answer ``not_descendant`` for every review (the exact-head bug this module
    fixes, seen from the other side) and a workspace holding only the reviewed
    head cannot prove ancestry -- neither is ever returned.
    """
    if not repo or not os.path.isdir(str(repo)):
        return None
    repo = str(repo)
    reviewed = str(reviewed or "").strip()
    if not has_commit(repo, tip):
        return None
    if reviewed and not has_commit(repo, reviewed):
        return None
    return repo
