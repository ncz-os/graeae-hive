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

import logging
import os
import re
import subprocess
import time
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


@dataclass
class BranchStats:
    ahead: int = 0
    behind: int = 0
    added: int = 0
    removed: int = 0
    clobbered: int = 0

    def describe(self) -> str:
        return (
            f"ahead={self.ahead} behind={self.behind} added={self.added} "
            f"removed={self.removed} clobbered={self.clobbered}"
        )


def default_branch(repo: str) -> str:
    for candidate in ("main", "master"):
        if _sh(["git", "rev-parse", "--verify", "-q", candidate], cwd=repo).returncode == 0:
            return candidate
    return "main"


def classify(repo: str, base: str, branch: str) -> tuple[str, BranchStats]:
    """The bucket for one branch, measured against `base`.

    Same thresholds as the old engine, in the same order, because the numbers are
    the operator's: a big divergence or any clobber is divergent whatever the
    merge says, then the merge itself, then behind/0 is a fast-forward, then a
    small conflict-free change behind is a cherry-pick candidate.
    """
    stats = BranchStats()
    merge_base = _git("merge-base", base, branch, cwd=repo)
    if not merge_base:
        return BUCKET_NO_BASE, stats
    try:
        stats.ahead = int(_git("rev-list", "--count", f"{base}..{branch}", cwd=repo) or 0)
    except ValueError:
        return BUCKET_NO_BASE, stats
    if stats.ahead == 0:
        return BUCKET_DONE, stats
    stats.behind = int(_git("rev-list", "--count", f"{branch}..{base}", cwd=repo) or 0)
    deleted = set(
        filter(None, _git("diff", "--name-only", "--diff-filter=D", merge_base, branch, cwd=repo).splitlines())
    )
    touched_base = set(filter(None, _git("diff", "--name-only", merge_base, base, cwd=repo).splitlines()))
    stats.clobbered = len(deleted & touched_base)
    numstat = _git("diff", "--numstat", merge_base, branch, cwd=repo).splitlines()
    for line in numstat:
        parts = line.split("\t")
        if len(parts) >= 2:
            if parts[0].isdigit():
                stats.added += int(parts[0])
            if parts[1].isdigit():
                stats.removed += int(parts[1])
    if stats.removed > MAX_REMOVED_LINES or stats.clobbered or (stats.added + stats.removed) > MAX_TOTAL_LINES:
        return BUCKET_DIVERGENT, stats
    merge_tree = _sh(["git", "merge-tree", "--write-tree", base, branch], cwd=repo)
    if merge_tree.returncode == 1 or "<<<<<<<" in merge_tree.stdout:
        return BUCKET_CONFLICT, stats
    if stats.behind == 0:
        return BUCKET_FAST_FORWARD, stats
    if stats.added + stats.removed < MAX_CHERRY_LINES:
        return BUCKET_CHERRY, stats
    return BUCKET_DIVERGENT, stats


def candidate_branches(repo: str, base: str) -> list[str]:
    listing = _git("for-each-ref", "--format=%(refname:short)", "refs/heads/", cwd=repo)
    return [branch for branch in listing.split() if branch != base and BRANCH_PATTERN.search(branch)]


def discover_repos(git_root: str = GIT_ROOT, exclude: frozenset[str] = EXCLUDE) -> list[str]:
    """The canonical bare repos this train is allowed to look at."""
    try:
        names = sorted(name for name in os.listdir(git_root) if name.endswith(".git"))
    except OSError as error:
        log.warning("cannot read git root %s (%s)", git_root, error)
        return []
    return [os.path.join(git_root, name) for name in names if name not in exclude]


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
    sha: str = ""
    opens_mr: bool = False
    ahead: int = 0
    behind: int = 0
    clobbered: int = 0
    failures: list[str] = field(default_factory=list)

    def to_record(self) -> dict:
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
    reports: list[BranchReport] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return bool(self.failures)

    def to_record(self) -> dict:
        """The `record` payload for one repository, failures and all.

        A repository that failed still produces a record: `failures` is what the
        SOP escalates on, and an empty record would lose the reason — which is the
        `run failed` this replaces.
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
        return self.reports[0].to_record()

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
    if _sh(["git", "rev-parse", "--verify", "-q", f"{sha}^{{commit}}"], cwd=repo).returncode != 0:
        return f"REFUSED: {sha[:12]} is not a commit in {os.path.basename(repo)}"
    if _sh(["git", "merge-base", "--is-ancestor", base, sha], cwd=repo).returncode != 0:
        return (
            f"REFUSED: {base} is not an ancestor of {sha[:12]}, so this is not a fast-forward; "
            f"a divergent or conflicting branch is a human's job"
        )
    if dry_run:
        return f"DRY-RUN: would advance {base} to {sha[:12]}"
    result = _sh(["git", "push", remote, f"{sha}:refs/heads/{base}"], cwd=repo)
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


def route_outward(report: BranchReport) -> dict | None:
    """What the SOP is asked to send outward, or `None` when the path is off.

    OFF by default. The opt-in does not authorise a push — this function cannot
    push at all — it only decides whether the engine *asks*. Asking is what puts
    the action in front of an operator instead of in front of a forge.
    """
    if not ALLOW_PUBLIC_FORGE:
        return None
    target = report.target
    host = target.split("://")[-1].split("/")[0] or target
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


def due_for_run(env: dict | None = None, now: float | None = None) -> tuple[bool, float]:
    """``(due, seconds_until_due)`` from the environment's own record.

    The record is a single JSON file (`MERGE_TRAIN_STATE_PATH`), because the
    merge-train runs as a user timer with no bus of its own. An absent or
    unreadable record means "never run", not "ran just now" — reading a missing
    record as a fresh run is how a 5-minute timer ran 300 times a day.
    """
    env = dict(os.environ if env is None else env)
    interval = float(env.get("MERGE_TRAIN_MIN_INTERVAL", MIN_INTERVAL_SECS))
    path = (env.get("MERGE_TRAIN_STATE_PATH") or "").strip()
    last_run: float | None = None
    if path:
        try:
            with open(path, encoding="utf-8") as handle:
                import json

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
    outcome = RepoOutcome(repo=name)
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
        try:
            bucket, stats = classify(repo, base, branch)
            sha = _git("rev-parse", "--verify", "-q", branch, cwd=repo)
        except subprocess.TimeoutExpired:
            bucket, stats, sha = BUCKET_NO_BASE, BranchStats(), ""
            failures.append(f"{name}/{branch}: git timed out")
        except Exception as error:  # noqa: BLE001 - one bad branch is not a bad cycle
            bucket, stats, sha = BUCKET_NO_BASE, BranchStats(), ""
            failures.append(f"{name}/{branch}: {type(error).__name__}: {error}")
        if sha == "":
            failures.append(f"{name}/{branch}: no resolvable sha")
        outcome.reports.append(
            BranchReport(
                repo=name,
                branch=branch,
                bucket=bucket,
                target=CANONICAL_REMOTE,
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
            outcome = RepoOutcome(repo=name, failures=[f"{name}: {type(error).__name__}: {error}"])
            log.warning("merge-train %s failed: %s", name, error)
        outcomes.append(outcome)
        if outward_reports is not None:
            for report in outcome.reports:
                request = route_outward(report)
                if request is not None:
                    outward_reports.append(request)
    return outcomes


def fast_forward_candidates(
    outcomes: list[RepoOutcome], repo_path: str | None = None, dry_run: bool = True
) -> list[str]:
    """Fast-forward every clean fast-forward bucket, ARGONAS only.

    A bucket is fast-forwardable only when the repository recorded NO failure this
    cycle and the branch is in the fast-forward bucket — the same pairing the
    decision enforces, so the executor cannot be talked into writing from a
    repository whose measurement failed.
    """
    results: list[str] = []
    for outcome in outcomes:
        if outcome.failed:
            results.append(f"{outcome.repo}: SKIPPED, {len(outcome.failures)} recorded failure(s)")
            continue
        for report in outcome.reports:
            if report.bucket != BUCKET_FAST_FORWARD:
                continue
            if repo_path is None:
                results.append(f"{report.repo}/{report.branch}: {report.sha[:12]} (not applied)")
                continue
            results.append(
                f"{report.repo}/{report.branch}: "
                f"{fast_forward(repo_path, default_branch(repo_path), report.sha, dry_run=dry_run)}"
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
            raise ValueError(
                f"merge-train refuses {args!r}: a '+' refspec is a force-push and is never allowed"
            )
    if "push" in lowered and CANONICAL_REMOTE not in lowered:
        raise ValueError(
            f"merge-train refuses {args!r}: a push must name {CANONICAL_REMOTE!r}, "
            f"the only remote this engine may write"
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
    due, wait = due_for_run()
    if not due:
        log.info("cadence: next run in %.0fs", wait)
        return 0
    log.info("cycle: dry_run=%s public_forge=%s", not args.apply, ALLOW_PUBLIC_FORGE)
    outward: list[dict] = []
    outcomes = run_cycle(git_root=args.git_root, dry_run=not args.apply, outward_reports=outward)
    for outcome in outcomes:
        if args.json:
            print(json.dumps(outcome.to_record(), sort_keys=True))
        else:
            print(" ", outcome.summary())
    for request in outward:
        # Printed, never sent: the outward_send SOP is what sends it, after an
        # operator approves it.
        log.info("OUTWARD REQUEST (not sent): %s", json.dumps(request, sort_keys=True))
    if args.apply:
        for line in fast_forward_candidates(outcomes, dry_run=False):
            log.info("ff %s", line)
    return 1 if any(outcome.failed for outcome in outcomes) else 0


if __name__ == "__main__":
    raise SystemExit(main())
