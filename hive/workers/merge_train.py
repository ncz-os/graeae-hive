#!/usr/bin/env python3
"""Merge-train engine — ARGONAS-only, with the public-forge path behind an opt-in.

This is the executor the `merge_train.decide` SOP (§8.3 of
`docs/sop/hive-adapters-and-review-escalation.md`) hands work to. It measures
branches and reports them; the SOP decides what may be written.

Three defects it exists to close, all of them behaviour the previous
`~/merge-train/apply.py` had:

1. **It pushed `main` to GitLab on every fast-forward**, then opened `[DOCTOR]`
   merge requests through the GitLab API. Automation writing to a public forge is
   what the operator's 2026-10-17 rule forbids. Here the only remote written is
   the canonical ARGONAS bare repo. A GitLab or GitHub write is refused unless
   `MERGE_TRAIN_ALLOW_PUBLIC_FORGE=1`, and even then it is **refused**, not
   performed: the outward action belongs to the `outward-send` SOP, which is
   where operator approval, the secret gate and the attribution scan live. What
   this module emits instead is a `route_outward` request.
2. **It ran every 5 minutes for 17-21 minutes a run.** `MIN_INTERVAL_SECS` is a
   day, and `due_for_run()` is the only thing that answers whether a cycle should
   happen at all.
3. **It reported a bare `run failed`.** `os.environ["GLTOK"]` was read at import
   time, so one missing token aborted the whole cycle, and one repository's
   exception ended the run. Here every repository is measured in its own
   `try`, and the reasons are carried in `RepoOutcome.failures` — which is the
   field the SOP's decision records.

Env:
  MERGE_TRAIN_GIT_ROOT        the canonical bare repo root (default: /mnt/argonas_git)
  MERGE_TRAIN_ALLOW_PUBLIC_FORGE  "1" to emit outward requests instead of holding them.
                              OFF unless set, and it never authorises a push here.
  MERGE_TRAIN_MIN_INTERVAL    seconds between cycles (default: 86400, one day)
  MERGE_TRAIN_PATTERN         regex selecting the branches to consider
  MERGE_TRAIN_EXCLUDE         comma list of repo basenames to skip
  MERGE_TRAIN_REMOTE          the one remote this module may write (default: argonas)
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import tempfile
import time
from contextlib import suppress
from dataclasses import dataclass, field

log = logging.getLogger("merge_train")

GIT_ROOT = os.environ.get("MERGE_TRAIN_GIT_ROOT", "/mnt/argonas_git")
ALLOW_PUBLIC_FORGE = os.environ.get("MERGE_TRAIN_ALLOW_PUBLIC_FORGE", "0") == "1"
MIN_INTERVAL_SECS = float(os.environ.get("MERGE_TRAIN_MIN_INTERVAL", "86400"))
CANONICAL_REMOTE = os.environ.get("MERGE_TRAIN_REMOTE", "argonas")
BRANCH_PATTERN = re.compile(
    os.environ.get("MERGE_TRAIN_PATTERN", r"hive/|resubmit|codex|audit|retry|job-|wip"),
    re.IGNORECASE,
)
EXCLUDE = frozenset(name.strip() for name in os.environ.get("MERGE_TRAIN_EXCLUDE", "").split(",") if name.strip())
#: Beyond this the branch is divergent no matter what the merge says. The old
#: numbers, kept so the classification is unchanged.
MAX_REMOVED_LINES = 500
MAX_TOTAL_LINES = 1000
MAX_CHERRY_LINES = 500
MAX_BULK_CANDIDATES = 500

#: Never a force-push, on any remote, for any reason. `--delete` is here for the
#: same reason: it destroys a branch, and so does a `+` refspec.
FORBIDDEN = ("--force", "-f", "--force-with-lease", "push --mirror", "--delete", "--prune")

#: Hosts and remote names that are never this fleet's own forge. `CANONICAL_REMOTE`
#: comes from `MERGE_TRAIN_REMOTE`, so a typo there would otherwise redefine
#: "canonical" and aim the one write at a public forge — and these are also the
#: short names a remote is usually given, so the check catches the misconfiguration
#: rather than only the misspelling.
PUBLIC_FORGE_HOSTS = frozenset(
    {
        "gitlab",
        "gitlab.com",
        "github",
        "github.com",
        "bitbucket",
        "bitbucket.org",
        "gitea",
        "codeberg",
        "forgejo",
        "gogs",
    }
)


def _sh(args: list[str], cwd: str | None = None, timeout: float = 120.0) -> subprocess.CompletedProcess:
    return subprocess.run(args, cwd=cwd, capture_output=True, text=True, errors="replace", timeout=timeout)


def _git(*args: str, cwd: str | None = None) -> str:
    return _sh(["git", *args], cwd=cwd).stdout.strip()


# ── classification ───────────────────────────────────────────────────────────

BUCKET_DONE = "done"
BUCKET_FAST_FORWARD = "fast_forward"
BUCKET_CHERRY = "cherry_pick"
BUCKET_DIVERGENT = "divergent"
BUCKET_CONFLICT = "conflict"
BUCKET_NO_BASE = "no_base"

#: The wire names `merge_train.decide` parses (`TrainBucket`). Written out here
#: rather than imported, because this module must run on PYTHIA without the
#: zeroclaw crate; the integration test asserts the two lists agree.
BUCKETS = (
    BUCKET_DONE,
    BUCKET_FAST_FORWARD,
    BUCKET_CHERRY,
    BUCKET_DIVERGENT,
    BUCKET_CONFLICT,
    BUCKET_NO_BASE,
)


class MeasurementError(RuntimeError):
    """git could not answer, so no bucket is known for this branch.

    Distinct from "no merge base" and from "nothing to merge", both of which are
    buckets. Without it a failed `rev-list` reads as zero commits, which is DONE:
    a clean record for a measurement that never happened.
    """


#: The buckets that would ask for an outward write. `done` and `no_base` need
#: nothing outward: one has nothing to merge and the other has nothing to compute
#: against. Mirrors the decision's own rule, so the engine cannot ask for something
#: `merge_train.decide` would hold.
OUTWARD_CANDIDATE_BUCKETS = frozenset({BUCKET_FAST_FORWARD, BUCKET_CHERRY, BUCKET_DIVERGENT, BUCKET_CONFLICT})


@dataclass
class BranchStats:
    ahead: int = 0
    behind: int = 0
    added: int = 0
    removed: int = 0
    clobbered: int = 0
    #: Binary files in the diff. `--numstat` prints `-` for both columns of a
    #: binary file, which silently counted as zero lines; the count is kept so the
    #: classification is visible in the record, not only in its outcome.
    binary: int = 0

    def describe(self) -> str:
        return (
            f"ahead={self.ahead} behind={self.behind} added={self.added} "
            f"removed={self.removed} clobbered={self.clobbered} binary={self.binary}"
        )


def default_branch(repo: str) -> str:
    for candidate in ("main", "master"):
        if _sh(["git", "rev-parse", "--verify", "-q", candidate], cwd=repo).returncode == 0:
            return candidate
    return "main"


def _count(repo: str, spec: str) -> int:
    """`rev-list --count` for a spec, as an int — or a [`MeasurementError`].

    `_git` returns "" for a failed command as readily as for a zero count, so
    `int(_git(...) or 0)` reads a broken measurement as ZERO commits, which is
    DONE: a branch nobody looks at again. Every count checks the exit code.
    """
    result = _sh(["git", "rev-list", "--count", spec], cwd=repo)
    if result.returncode != 0:
        raise MeasurementError(f"rev-list --count {spec}: {result.stderr.strip()[:120] or 'failed'}")
    text = result.stdout.strip()
    if not text.isdigit():
        raise MeasurementError(f"rev-list --count {spec}: {text[:60]!r} is not a count")
    return int(text)


def _merge_base(repo: str, base: str, branch: str) -> str | None:
    """The merge base, or ``None`` when the two genuinely share no history.

    git exits 1 for "no merge base" and 128 for "fatal: bad object", and `_git`
    returns "" for both. Reporting the second as the first tells the operator a
    branch has no common ancestor when in fact the repository could not be read —
    two different jobs with different owners. The exit code is what separates them.
    """
    result = _sh(["git", "merge-base", base, branch], cwd=repo)
    if result.returncode == 1:
        return None
    if result.returncode != 0:
        raise MeasurementError(f"merge-base {base} {branch}: {result.stderr.strip()[:120] or 'failed'}")
    text = result.stdout.strip()
    return text or None


def _diff_names(repo: str, merge_base: str, other: str, extra: tuple[str, ...] = ()) -> set[str]:
    """A `git diff --name-only` result as a set of paths, or a [`MeasurementError`].

    Every one of these calls can fail, and `_git` returns "" for a failure exactly
    as it does for an empty diff. Two empty sets intersect to nothing, so a failed
    clobber check read as "nothing is clobbered" — one of the three conditions that
    make a branch DIVERGENT, and therefore one of the three that stand between it
    and a classification the SOP would otherwise have to reason about.
    """
    result = _sh(["git", "diff", *extra, "--name-only", merge_base, other], cwd=repo)
    if result.returncode != 0:
        raise MeasurementError(f"diff --name-only {merge_base} {other}: {result.stderr.strip()[:120] or 'failed'}")
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def _diff_lines(repo: str, merge_base: str, other: str) -> list[str]:
    """A `git diff --numstat` result as its lines, or a [`MeasurementError`]."""
    result = _sh(["git", "diff", "--numstat", merge_base, other], cwd=repo)
    if result.returncode != 0:
        raise MeasurementError(f"diff --numstat {merge_base} {other}: {result.stderr.strip()[:120] or 'failed'}")
    return [line for line in result.stdout.splitlines() if line.strip()]


def classify(repo: str, base: str, branch: str) -> tuple[str, BranchStats]:
    """The bucket for one branch, measured against `base`.

    Same thresholds as the old engine, in the same order, because the numbers are
    the operator's: a big divergence or any clobber is divergent whatever the
    merge says, then the merge itself, then behind/0 is a fast-forward, then a
    small conflict-free change behind is a cherry-pick candidate.

    Raises [`MeasurementError`] when git cannot answer: a measurement that failed
    and a measurement that says "nothing to merge" are different facts, and the
    caller has to be able to tell them apart.
    """
    stats = BranchStats()
    merge_base = _merge_base(repo, base, branch)
    if merge_base is None:
        return BUCKET_NO_BASE, stats
    stats.ahead = _count(repo, f"{base}..{branch}")
    if stats.ahead == 0:
        return BUCKET_DONE, stats
    stats.behind = _count(repo, f"{branch}..{base}")
    deleted = _diff_names(repo, merge_base, branch, extra=("--diff-filter=D",))
    touched_base = _diff_names(repo, merge_base, base)
    stats.clobbered = len(deleted & touched_base)
    numstat = _diff_lines(repo, merge_base, branch)
    for line in numstat:
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        if parts[0].isdigit():
            stats.added += int(parts[0])
        if parts[1].isdigit():
            stats.removed += int(parts[1])
        elif parts[0] == "-" or parts[1] == "-":
            # A binary file contributes ZERO to both thresholds, so a branch adding
            # a 4 GB blob was classified fast-forward and pushed with no human ever
            # seeing its size. Counted ABOVE the divergence ceiling, so a binary
            # change lands in `divergent` — a hold — whatever its actual size and
            # whatever the ceilings are later tuned to: the bucket is decided by one
            # counter being over its own limit, not by the sum of two ceilings
            # happening to exceed a third.
            stats.binary += 1
            stats.added = max(stats.added, MAX_TOTAL_LINES + 1)
    if stats.removed > MAX_REMOVED_LINES or stats.clobbered or (stats.added + stats.removed) > MAX_TOTAL_LINES:
        return BUCKET_DIVERGENT, stats
    merge_tree = _sh(["git", "merge-tree", "--write-tree", base, branch], cwd=repo)
    if merge_tree.returncode == 1 or "<<<<<<<" in merge_tree.stdout:
        return BUCKET_CONFLICT, stats
    if merge_tree.returncode != 0:
        # 1 is "these conflict". 128 is "fatal: bad object" — the merge was never
        # computed, and treating that as a clean merge made `behind == 0` reachable
        # from a repository nobody could read, which is an unattended push.
        raise MeasurementError(
            f"merge-tree --write-tree {base} {branch}: {merge_tree.stderr.strip()[:120] or 'failed'}"
        )
    if stats.behind == 0:
        return BUCKET_FAST_FORWARD, stats
    if stats.added + stats.removed < MAX_CHERRY_LINES:
        return BUCKET_CHERRY, stats
    return BUCKET_DIVERGENT, stats


def candidate_branches(repo: str, base: str) -> list[str]:
    """The branches worth looking at.

    Raises [`MeasurementError`] if the listing failed. An unreadable repository
    produced no branch names, which read as "no branch matched the pattern" — a
    clean statement about a repository nothing was able to look at.
    """
    result = _sh(["git", "for-each-ref", "--format=%(refname:short)", "refs/heads/"], cwd=repo)
    if result.returncode != 0:
        raise MeasurementError(f"for-each-ref refs/heads: {result.stderr.strip()[:120] or 'failed'}")
    listing = result.stdout
    return [branch for branch in listing.split() if branch != base and BRANCH_PATTERN.search(branch)]


def target_remote() -> str:
    """The remote this engine writes to, as configured.

    `MERGE_TRAIN_REMOTE` defaults to ARGONAS. It is RECORDED on every report rather
    than assumed, because `route_outward` has to know where a write would land to
    decide whether the write is outward at all — and a report that always said
    "argonas" made that question unanswerable.
    """
    return CANONICAL_REMOTE


def _bare(name: str) -> str:
    """A repository name without its `.git` suffix, lowercased."""
    lowered = name.strip().lower()
    return lowered[:-4] if lowered.endswith(".git") else lowered


def discover_repos(git_root: str = GIT_ROOT, exclude: frozenset[str] = EXCLUDE) -> list[str]:
    """The canonical bare repos this train is allowed to look at."""
    try:
        names = sorted(name for name in os.listdir(git_root) if name.endswith(".git"))
    except OSError as error:
        log.warning("cannot read git root %s (%s)", git_root, error)
        return []
    # Compare BARE names. A bare repo on disk is `openclaw.git`; an operator writing
    # `MERGE_TRAIN_EXCLUDE=openclaw` means `openclaw`. Accepting both spellings
    # removes the case where an exclusion silently does nothing — which is how a
    # repository nobody meant to touch stays in the train.
    excluded = {_bare(name) for name in exclude}
    return [os.path.join(git_root, name) for name in names if _bare(name) not in excluded]


# ── the decision the SOP reads ───────────────────────────────────────────────


@dataclass
class BranchReport:
    """One branch, in the shape `merge_train.decide` reads.

    Field names match the SOP's input contract (`record{...}`) exactly, so the
    payload is the report and not a translation of it.
    """

    repo: str
    branch: str
    bucket: str
    target: str
    base: str = ""
    sha: str = ""
    opens_mr: bool = False
    ahead: int = 0
    behind: int = 0
    clobbered: int = 0
    failures: list[str] = field(default_factory=list)

    def to_record(self) -> dict:
        # `base` is deliberately NOT in the record: `merge_train.decide` does not
        # read it, and a payload field the decision ignores is a field that can
        # drift without anything noticing. It travels on the report for the
        # executor's own use.
        return {
            "repo": self.repo,
            "branch": self.branch,
            "bucket": self.bucket,
            "target": self.target,
            "sha": self.sha,
            "opens_mr": self.opens_mr,
            "ahead": self.ahead,
            "behind": self.behind,
            "clobbered": self.clobbered,
            "failures": list(self.failures),
        }


@dataclass
class RepoOutcome:
    """One repository's cycle: every branch, plus why anything failed."""

    repo: str
    #: The path this outcome was measured from, so the writer reaches the SAME
    #: repository the classifier read. `repo` is only a basename, and two bare
    #: repos under one root can share one.
    path: str = ""
    reports: list[BranchReport] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return bool(self.failures)

    def to_records(self) -> list[dict]:
        """One `record` payload per measured branch, in measurement order.

        This is the whole set. `merge_train.decide` is called once per record, so a
        repository with four candidates is decided four times — and a caller that
        only had `to_record()` would decide the first branch and silently drop the
        other three.
        """
        return [report.to_record() for report in self.reports]

    def to_record(self) -> dict:
        """The `record` payload for the FIRST measured branch, failures and all.

        A shortcut for the common one-candidate repository, and the payload for a
        repository that failed: `failures` is what the SOP escalates on, and an empty
        record would lose the reason — which is the `run failed` this replaces.
        Use [`RepoOutcome.to_records`] to decide every branch.
        """
        if not self.reports:
            return {
                "repo": self.repo,
                "branch": "",
                "bucket": BUCKET_NO_BASE,
                "target": CANONICAL_REMOTE,
                "failures": list(self.failures) or ["no branch matched the pattern"],
            }
        # The record carries the FIRST branch's measurements; the SOP decides per
        # branch, so a repository with several candidates is run once per branch.
        record = self.reports[0].to_record()
        # The REPOSITORY's own failures travel with it. They are why the whole
        # repository is skipped, and a record carrying only the first branch's
        # failures would let the SOP decide a branch the run never looked at — which
        # is the `run failed` this replaces, arriving through a structured payload.
        if self.failures:
            merged = list(record.get("failures", []))
            merged.extend(f for f in self.failures if f not in merged)
            record["failures"] = merged
        return record

    def summary(self) -> str:
        if self.failed:
            return f"{self.repo}: FAILED ({'; '.join(self.failures)})"
        counts: dict[str, int] = {}
        for report in self.reports:
            counts[report.bucket] = counts.get(report.bucket, 0) + 1
        parts = ", ".join(f"{bucket}={count}" for bucket, count in sorted(counts.items()))
        return f"{self.repo}: {parts or 'no branches'}"


# ── the only write ───────────────────────────────────────────────────────────


def fast_forward(repo: str, base: str, sha: str, dry_run: bool = True, remote: str = CANONICAL_REMOTE) -> str:
    """Advance `base` to `sha` on the canonical remote, and nothing else.

    Refuses anything that is not a fast-forward, refuses a force-push, refuses any
    remote other than the canonical one, and refuses an empty sha. The refusal is
    the return value rather than an exception so a caller cannot skip it by
    forgetting a try.
    """
    if not sha or not sha.strip():
        return "REFUSED: fast-forward with no sha"
    if remote != CANONICAL_REMOTE:
        return (
            f"REFUSED: this engine writes {CANONICAL_REMOTE} only, not {remote!r}; "
            f"an outward push is an outward_send SOP action"
        )
    if _remote_host(remote).lower() in PUBLIC_FORGE_HOSTS:
        # `CANONICAL_REMOTE` is read from `MERGE_TRAIN_REMOTE`, so an operator's
        # typo redefines "canonical" — and `MERGE_TRAIN_REMOTE=gitlab` would
        # otherwise satisfy the check above and aim the only write at a public
        # forge. The name itself is refused, so the mistake cannot be laundered
        # through the one value that is supposed to authorise the write.
        return (
            f"REFUSED: {remote!r} names a public forge, not this fleet's canonical remote; "
            f"an outward push is an outward_send SOP action"
        )
    if _sh(["git", "rev-parse", "--verify", "-q", f"{sha}^{{commit}}"], cwd=repo).returncode != 0:
        return f"REFUSED: {sha[:12]} is not a commit in {os.path.basename(repo)}"
    if _sh(["git", "merge-base", "--is-ancestor", base, sha], cwd=repo).returncode != 0:
        return (
            f"REFUSED: {base} is not an ancestor of {sha[:12]}, so this is not a fast-forward; "
            f"a divergent or conflicting branch is a human's job"
        )
    # The remote's URL as well as its name: an innocent alias that happens to point
    # at a forge is the same mistake as a forge-named alias, and a name check alone
    # would let it through. A remote git cannot resolve is left to the push, which
    # fails without writing anything.
    url = _sh(["git", "remote", "get-url", remote], cwd=repo).stdout.strip()
    if url and _remote_host(url).lower() in PUBLIC_FORGE_HOSTS:
        return (
            f"REFUSED: {remote!r} resolves to {url}, a public forge, not this fleet's canonical "
            f"remote; an outward push is an outward_send SOP action"
        )
    if dry_run:
        return f"DRY-RUN: would advance {base} to {sha[:12]}"
    args = ["git", "push", remote, f"{sha}:refs/heads/{base}"]
    # The choke point runs HERE, on the only write, rather than being a helper
    # nobody calls: a guard that is not in front of the push is documentation.
    try:
        assert_no_forced_or_public_writes(args)
    except ValueError as error:
        return f"REFUSED: {error}"
    result = _sh(args, cwd=repo)
    if result.returncode != 0:
        return f"FAILED: push to {remote} rc={result.returncode} {result.stderr.strip()[:160]}"
    return f"advanced {base} to {sha[:12]} on {remote}"


def outward_request(report: BranchReport, kind: str, target: str) -> dict:
    """The request the `outward_send` SOP adjudicates. Nothing here sends it.

    Built as a plain dict with the gate results the SOP reads. Every gate is left
    at its **default** — no secret-gate record, no attribution scan, no approval —
    because a request that carried a clean gate result would be a request the
    decision reads as cleared. `outward_send`'s default for an absent gate is the
    unadjudicated finding, which routes to the operator: exactly right for a push
    nobody has looked at.
    """
    return {
        "kind": kind,
        "target": target,
        "text": (
            f"[DOCTOR] merge-train asks to {kind} for {report.repo}:{report.branch} "
            f"({report.bucket}, {report.ahead} ahead / {report.behind} behind)"
        ),
        "plain_text": True,
        # No gate result is asserted here. See the docstring.
    }


def _remote_host(target: str) -> str:
    """The HOST an outward request names, from either remote spelling.

    A git remote is written two ways and this reads both: `scheme://host/path`
    (`ssh://git@gitlab.com:22/group/repo.git`) and the scp form
    (`git@gitlab.com:group/repo.git`). Splitting on `://` alone returned the WHOLE
    scp string's first path segment — `git@gitlab.com:group` — so the field an
    operator reads to decide whether to approve named a user and a repository
    directory rather than a host.
    """
    text = (target or "").strip()
    if "://" in text:
        text = text.split("://", 1)[1]
        host = text.split("/", 1)[0]
    else:
        # scp form: `user@host:path` (or a bare `host:path`), so the host is what
        # precedes the FIRST colon — a port in brackets is part of it, not the split.
        authority = text.split("/", 1)[0]
        host = authority.rsplit(":", 1)[0] if ":" in authority else authority
    if "@" in host:
        host = host.rsplit("@", 1)[1]
    # A port is not part of the host: `ssh://git@gitlab.com:22/group/repo.git` names
    # the host `gitlab.com`. Bracketed IPv6 literals keep their brackets.
    if host.startswith("["):
        host = host[: host.index("]") + 1] if "]" in host else host
    elif ":" in host:
        host = host.split(":", 1)[0]
    return host or (target or "").strip()


def route_outward(report: BranchReport) -> dict | None:
    """What the SOP is asked to send outward, or ``None`` when there is nothing to ask.

    `None` when the opt-in is off (the default), when the target is the canonical
    ARGONAS remote — which is not outward and needs no request — or when the branch
    is not a bucket that would want a write at all.

    The opt-in does not authorise a push; this function cannot push. It only decides
    whether the engine *asks*, which is what puts the action in front of an operator
    instead of in front of a forge.
    """
    if not ALLOW_PUBLIC_FORGE:
        return None
    if report.target == CANONICAL_REMOTE and not report.opens_mr:
        # Naming ARGONAS in an `outward_send` request would ask an operator to
        # approve writing to the fleet's own canonical remote, which is the one write
        # this engine does not need approval for. A MERGE REQUEST on ARGONAS is a
        # different action from pushing to it, and the decision routes that outward
        # whatever the target — so it is the one case where ARGONAS is asked for.
        return None
    if report.bucket not in OUTWARD_CANDIDATE_BUCKETS:
        return None
    target = report.target
    host = _remote_host(target)
    kind = "pr_open" if report.opens_mr else "push_public_forge"
    return outward_request(report, kind, host)


# ── cadence ──────────────────────────────────────────────────────────────────


class Cadence:
    """The daily floor, with a push-event exemption.

    ``last_run`` is supplied rather than read from a clock so a test can place it,
    and so a restart cannot lose it: the caller persists it.
    """

    def __init__(self, last_run: float | None = None, interval: float = MIN_INTERVAL_SECS, now: float | None = None):
        self.last_run = last_run
        self.interval = float(interval)
        self._now = now

    def _clock(self) -> float:
        return time.time() if self._now is None else self._now

    def due(self, on_push_event: bool = False) -> bool:
        if on_push_event:
            return True
        if self.last_run is None:
            return True
        return (self._clock() - self.last_run) >= self.interval

    def seconds_until_due(self) -> float:
        if self.last_run is None:
            return 0.0
        return max(0.0, self.interval - (self._clock() - self.last_run))


def state_path(env: dict | None = None) -> str:
    """Where the cadence record lives.

    A default, because "no record" and "never run" are the same answer to
    `due_for_run`: with nothing configured and nothing written, the floor never
    engages and the timer runs at whatever interval the caller chose. The default
    is under the user's state directory rather than inside the git root, because the
    git root is the operator's canonical data and this is the engine's own
    bookkeeping. The path is logged at startup so an operator can see where the
    cadence lives instead of wondering why it does not.

    Read from the SUPPLIED environment only. A caller that passes a scoped env — a
    test, or anything else that means "pretend this is the host" — must not get the
    real user's state directory, so `os.environ` is never consulted here and an env
    with no state directory yields "" rather than a path into the caller's home.
    Nowhere to record is fail-closed and says so; a path the caller did not choose is
    not.
    """
    env = dict(os.environ if env is None else env)
    configured = (env.get("MERGE_TRAIN_STATE_PATH") or "").strip()
    if configured:
        return configured
    if env.get("MERGE_TRAIN_NO_DEFAULT_STATE"):
        return ""
    xdg_state = (env.get("XDG_STATE_HOME") or "").strip()
    if xdg_state:
        # XDG_STATE_HOME is ALREADY the state directory, so it is not extended with
        # `.local/state`; only the HOME fallback is.
        return os.path.join(xdg_state, "merge-train", "state.json")
    home = (env.get("HOME") or "").strip()
    if not home:
        return ""
    return os.path.join(home, ".local", "state", "merge-train", "state.json")


def record_cycle_end(at: float | None = None, env: dict | None = None) -> bool:
    """Record that a cycle finished, so the cadence floor can see it.

    Without this the record file is never written, `due_for_run` reads "never run"
    forever, and the daily floor never engages — the 5-minute timer defect with a
    one-day number in front of it. Written atomically for the same reason the
    doctor's state file is: a truncated write would read as a parse error and be
    treated as never-run.

    Recorded even when the cycle had failures: the cycle RAN, and what it failed at
    is in the outcome records. NOT recording a failed cycle would mean a broken
    repository re-runs every five minutes, which is the old behaviour exactly.
    """
    env = dict(os.environ if env is None else env)
    path = state_path(env)
    if not path:
        log.warning("no cadence record path and the default is disabled; the floor stays at never-run")
        return False
    stamp = time.time() if at is None else float(at)
    blob: dict = {}
    try:
        with open(path, encoding="utf-8") as handle:
            existing = json.load(handle)
        if isinstance(existing, dict):
            blob = existing
    except (OSError, ValueError):
        # A missing or unreadable record is overwritten rather than merged into: the
        # one field this file exists for is `last_run`.
        pass
    blob["last_run"] = stamp
    directory = os.path.dirname(path) or "."
    try:
        os.makedirs(directory, exist_ok=True)
        fd, temp_path = tempfile.mkstemp(dir=directory, prefix=".merge-train-state-")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(blob, handle)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, path)
            return True
        except OSError:
            with suppress(OSError):
                os.unlink(temp_path)
            raise
    except OSError as error:
        log.warning("merge-train state %s not writable (%s); the floor stays at never-run", path, error)
        return False


def due_for_run(env: dict | None = None, now: float | None = None) -> tuple[bool, float]:
    """``(due, seconds_until_due)`` from the environment's own record.

    The record is a single JSON file (`MERGE_TRAIN_STATE_PATH`), because the
    merge-train runs as a user timer with no bus of its own. An absent or
    unreadable record means "never run", not "ran just now" — reading a missing
    record as a fresh run is how a 5-minute timer ran 300 times a day.
    """
    env = dict(os.environ if env is None else env)
    interval = float(env.get("MERGE_TRAIN_MIN_INTERVAL", MIN_INTERVAL_SECS))
    path = state_path(env)
    last_run: float | None = None
    if path:
        try:
            with open(path, encoding="utf-8") as handle:
                blob = json.load(handle)
            if isinstance(blob, dict) and isinstance(blob.get("last_run"), (int, float)):
                last_run = float(blob["last_run"])
        except (OSError, ValueError) as error:
            log.warning("merge-train state %s unreadable (%s); treating as never-run", path, error)
    cadence = Cadence(last_run, interval=interval, now=now)
    return cadence.due(), cadence.seconds_until_due()


# ── the cycle ────────────────────────────────────────────────────────────────


def measure_repo(repo: str, pattern_checked: bool = True) -> RepoOutcome:
    """Every candidate branch in one repository, each measured in its own try.

    A git failure on one branch is recorded on that branch and on the repository;
    it does not end the cycle. That is the difference between "a bare `run
    failed`" and a record an operator can act on.
    """
    name = os.path.basename(repo)
    outcome = RepoOutcome(repo=name, path=repo)
    if not os.path.isdir(repo):
        outcome.failures.append(f"{name}: not a directory")
        return outcome
    try:
        base = default_branch(repo)
    except Exception as error:  # noqa: BLE001 - reported, never fatal
        outcome.failures.append(f"{name}: cannot resolve the default branch ({error})")
        return outcome
    try:
        branches = candidate_branches(repo, base)
    except Exception as error:  # noqa: BLE001
        outcome.failures.append(f"{name}: cannot list branches ({error})")
        return outcome
    if not branches:
        outcome.failures.append(f"{name}: no branch matched {BRANCH_PATTERN.pattern!r}")
        return outcome
    for branch in branches[:MAX_BULK_CANDIDATES]:
        failures: list[str] = []
        measured = True
        try:
            bucket, stats = classify(repo, base, branch)
            sha = _git("rev-parse", "--verify", "-q", branch, cwd=repo)
        except subprocess.TimeoutExpired:
            bucket, stats, sha = BUCKET_NO_BASE, BranchStats(), ""
            measured = False
            failures.append(f"{name}/{branch}: git timed out")
        except Exception as error:  # noqa: BLE001 - one bad branch is not a bad cycle
            # The bucket is a PLACEHOLDER for a branch nothing could measure, and
            # `failures` is the real claim: the SOP escalates on a non-empty
            # `failures`, and `fast_forward_candidates` refuses a repository that has
            # any. `no_base` is the placeholder because it is the one bucket that
            # escalates, so an unmeasurable branch can never be written to.
            bucket, stats, sha = BUCKET_NO_BASE, BranchStats(), ""
            measured = False
            failures.append(f"{name}/{branch}: {type(error).__name__}: {error}")
        # Only a branch that MEASURED and still resolved to no sha gets this reason:
        # the except paths above already recorded why, and a second entry for the
        # same failure doubled every failure count the operator reads.
        if measured and sha == "":
            failures.append(f"{name}/{branch}: no resolvable sha")
        outcome.reports.append(
            BranchReport(
                repo=name,
                branch=branch,
                bucket=bucket,
                # The remote this repo's branch would be written to, as configured.
                # Hardcoding the canonical name made every outward request name
                # ARGONAS — the one write that needs no approval.
                target=target_remote(),
                base=base,
                sha=sha,
                ahead=stats.ahead,
                behind=stats.behind,
                clobbered=stats.clobbered,
                failures=failures,
            )
        )
    outcome.failures.extend(f for report in outcome.reports for f in report.failures)
    return outcome


def run_cycle(
    git_root: str = GIT_ROOT,
    dry_run: bool = True,
    repos: list[str] | None = None,
    outward_reports: list[dict] | None = None,
) -> list[RepoOutcome]:
    """One cycle over every repository, each in its own try.

    Returns the per-repository outcomes so the caller can hand each `record` to
    `merge_train.decide`. With `dry_run` (the default) nothing is written at all;
    `outward_reports` collects the requests the opt-in would produce, so a caller
    can see what the SOP would be asked to send.
    """
    outcomes: list[RepoOutcome] = []
    targets = repos if repos is not None else discover_repos(git_root)
    for repo in targets:
        try:
            outcome = measure_repo(repo)
        except Exception as error:  # noqa: BLE001 - the whole point of this cycle
            name = os.path.basename(repo)
            outcome = RepoOutcome(repo=name, path=repo, failures=[f"{name}: {type(error).__name__}: {error}"])
            log.warning("merge-train %s failed: %s", name, error)
        outcomes.append(outcome)
        if outward_reports is not None and not outcome.failed:
            # A repository whose measurement failed enqueues nothing: the reasons
            # are already in its record and its outcome is an escalation, and a
            # request built from a measurement that did not happen is a request for
            # an action nobody can describe.
            for report in outcome.reports:
                # Only a bucket that would actually WANT a write asks for one. A
                # `done` branch or an orphan needs nothing outward, and emitting a
                # request for it would put a phantom action in front of the
                # operator's approval queue — the decision refuses these too.
                if report.bucket not in OUTWARD_CANDIDATE_BUCKETS:
                    continue
                request = route_outward(report)
                if request is not None:
                    outward_reports.append(request)
    return outcomes


def fast_forward_candidates(outcomes: list[RepoOutcome], dry_run: bool = True) -> list[str]:
    """Fast-forward every clean fast-forward bucket, ARGONAS only.

    A bucket is fast-forwardable only when the repository recorded NO failure this
    cycle and the branch is in the fast-forward bucket — the same pairing the
    decision enforces, so the executor cannot be talked into writing from a
    repository whose measurement failed.

    Each outcome carries the path it was measured from. Taking one shared path as
    an argument instead is a guess: the outcome's `repo` is a basename, so the
    argument version wrote the same branch into whichever repository the caller
    happened to name — and with none named, `--apply` reported success having
    written nothing at all.
    """
    results: list[str] = []
    for outcome in outcomes:
        if outcome.failed:
            results.append(f"{outcome.repo}: SKIPPED, {len(outcome.failures)} recorded failure(s)")
            continue
        if not outcome.path:
            results.append(f"{outcome.repo}: SKIPPED, no recorded repository path to write")
            continue
        for report in outcome.reports:
            if report.bucket != BUCKET_FAST_FORWARD:
                continue
            # The base the classification used, recorded when the branch was
            # measured — not re-resolved here, which would be a second git call per
            # branch and could name a different branch than the one classified.
            base = report.base or default_branch(outcome.path)
            results.append(
                f"{report.repo}/{report.branch}: {fast_forward(outcome.path, base, report.sha, dry_run=dry_run)}"
            )
    return results


def assert_no_forced_or_public_writes(args: list[str]) -> None:
    """Refuse a git invocation that force-pushes or writes outside ARGONAS.

    A single choke point in front of every write, so "we only push ARGONAS and
    never force" is one assertion rather than a habit spread over call sites.
    """
    lowered = [arg.strip().lower() for arg in args]
    # Matched against BOTH each argument and the whole command: an entry like
    # "push --mirror" spans two tokens, so a per-argument scan silently misses it
    # and `--mirror` sails through — a mirror push overwrites every ref on the
    # remote, which is worse than any force-push.
    joined = " ".join(lowered)
    for forbidden in FORBIDDEN:
        if any(forbidden in arg for arg in lowered) or forbidden in joined:
            raise ValueError(f"merge-train refuses {args!r}: {forbidden} is never allowed")
    # A `+` refspec is a force-push with no flag to grep for. `git push argonas
    # +main:refs/heads/main` deletes main's history in one token, so the refspecs
    # are checked for the marker directly.
    for arg in args:
        stripped = arg.strip()
        if stripped.startswith("+") or "+refs/" in stripped:
            raise ValueError(f"merge-train refuses {args!r}: a '+' refspec is a force-push and is never allowed")
    if "push" in lowered and CANONICAL_REMOTE not in lowered:
        raise ValueError(
            f"merge-train refuses {args!r}: a push must name {CANONICAL_REMOTE!r}, "
            f"the only remote this engine may write"
        )
    # A remote NAME is not a remote: `MERGE_TRAIN_REMOTE=gitlab` would satisfy the
    # check above and make every push in this module aim at a public forge. The
    # argument AFTER `push` is the one that names it, so that is the one checked —
    # a refspec is not a remote and must not be read as a host.
    push_at = lowered.index("push") if "push" in lowered else -1
    if push_at >= 0 and push_at + 1 < len(args):
        named = args[push_at + 1]
        host = _remote_host(named).lower()
        if host in PUBLIC_FORGE_HOSTS:
            raise ValueError(
                f"merge-train refuses {args!r}: {named!r} names a public forge, not this fleet's canonical remote"
            )


def main(argv: list[str] | None = None) -> int:
    import argparse
    import json

    parser = argparse.ArgumentParser(description="merge-train: measure, and fast-forward ARGONAS only")
    parser.add_argument("--apply", action="store_true", help="perform the ARGONAS fast-forwards")
    parser.add_argument("--git-root", default=GIT_ROOT)
    parser.add_argument("--json", action="store_true", help="emit one JSON payload per repository")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [merge-train] %(message)s")
    log.info("cadence record: %s", state_path() or "(none: the floor stays at never-run)")
    due, wait = due_for_run()
    if not due:
        log.info("cadence: next run in %.0fs", wait)
        return 0
    log.info("cycle: dry_run=%s public_forge=%s", not args.apply, ALLOW_PUBLIC_FORGE)
    outward: list[dict] = []
    outcomes = run_cycle(git_root=args.git_root, dry_run=not args.apply, outward_reports=outward)
    for outcome in outcomes:
        if args.json:
            # One line per MEASURED BRANCH: `merge_train.decide` is called once per
            # record, so one line per repository would drop every candidate but the
            # first — silently, because the output still looked well-formed.
            records = outcome.to_records()
            if not records:
                # A repository that produced NO branch record still has to appear:
                # its failures are the whole reason it is in the output, and an empty
                # list would mean the record the SOP escalates on never exists.
                records = [outcome.to_record()]
            for record in records:
                print(json.dumps(record, sort_keys=True))
        else:
            print(" ", outcome.summary())
    for request in outward:
        # Printed, never sent: the outward_send SOP is what sends it, after an
        # operator approves it.
        log.info("OUTWARD REQUEST (not sent): %s", json.dumps(request, sort_keys=True))
    if args.apply:
        for line in fast_forward_candidates(outcomes, dry_run=False):
            log.info("ff %s", line)
    # The cycle RAN, whatever it found. Recording that is what makes the floor mean
    # anything on the next invocation.
    if record_cycle_end():
        log.info("cycle end recorded")
    return 1 if any(outcome.failed for outcome in outcomes) else 0


if __name__ == "__main__":
    raise SystemExit(main())
