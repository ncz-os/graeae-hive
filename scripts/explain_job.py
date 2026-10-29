#!/usr/bin/env python3
"""Explain one Hive job: why it failed, what the doctor would do, and the facts.

``GET /v1/jobs/<id>`` answers *what* the row says; it does not answer *why*. This
script assembles the pieces an operator actually reads when a job comes back
failed:

* the row (status, kind, attempts, who claimed it),
* the loop outcome class and the doctor's decision for it
  (``hive.workers.loop_outcome.classify`` + ``decide``),
* the ``projects/zoder-loop-events`` timeline from MNEMOS (phase, time, error),
* the reviewer record (``result.reviewer`` and its evidence),
* the workspace / branch / commit facts the worker proved with git,
* the failure text itself.

Read-only: it never PATCHes, requeues or writes anything. Configuration is
env-only (``HIVE_URL``, ``MNEMOS_URL``, ``MNEMOS_TOKEN``) — no address is
compiled into this file and no token is ever printed.

Fully offline: ``--row file.json`` and ``--events file.json`` replace both
network reads, so the whole tool is testable without a bus or MNEMOS.

    scripts/explain_job.py 01a17c00-... --row row.json --events events.json
    HIVE_URL=http://<bus-host>:5005 scripts/explain_job.py 01a17c00-...
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# `hive` is a package at the repo root; this script lives one level down.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hive.workers import loop_outcome  # noqa: E402

# What each doctor decision means, in the words of loop_outcome.decide().
ACTION_WORDS = {
    "requeue": "resubmit it (the existing resubmit path)",
    "hold": "do nothing this pass — the cause is outside the job",
    "needs_review": "stop automation; a human or the review lane looks once",
    "skip": "not the auto-requeue path — approved work waits for a merge",
    "default": "unknown outcome: today's behaviour (no opinion)",
}

_TIME_KEYS = ("time", "ts", "timestamp", "at", "created_at", "created", "when")
_ERROR_KEYS = ("error", "error_text", "message", "msg", "reason", "detail", "text", "failure")


def _short(value, limit: int = 200) -> str:
    """One readable line for a value of unknown shape."""
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        try:
            text = json.dumps(value, sort_keys=True)
        except (TypeError, ValueError):
            text = str(value)
    else:
        text = str(value)
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _first(payload: dict, keys) -> str:
    for key in keys:
        if payload.get(key):
            return _short(payload[key])
    return ""


def _seq(payload: dict) -> int:
    """Sort key tolerant of the str/int seq MNEMOS has been seen to store."""
    try:
        return int(payload.get("_seq") or 0)
    except (TypeError, ValueError):
        return 0


def _as_result(row: dict) -> dict:
    """``result`` as a dict whatever the row stored (object or JSON string)."""
    res = row.get("result")
    if isinstance(res, str):
        try:
            res = json.loads(res)
        except ValueError:
            return {"raw": res}
    return res if isinstance(res, dict) else {}


# ── inputs ──────────────────────────────────────────────────────────────────


def load_row(path: str | Path, job_id: str) -> dict:
    """The job row from a file: a single object, ``{"jobs": [...]}`` or a list.

    Raises ``ValueError`` with a message an operator can act on — never a bare
    JSONDecodeError, and never a silent success on the wrong file.
    """
    try:
        raw = json.loads(Path(path).read_text())
    except OSError as exc:
        raise ValueError(f"cannot read {path}: {exc}") from exc
    except ValueError as exc:
        raise ValueError(f"{path} is not valid JSON: {exc}") from exc
    if isinstance(raw, dict) and isinstance(raw.get("jobs"), list):
        candidates = raw["jobs"]
    elif isinstance(raw, dict):
        candidates = [raw]
    elif isinstance(raw, list):
        candidates = raw
    else:
        raise ValueError(f"{path} must be a job object, a {{'jobs': [...]}} body, or a list of jobs")
    for row in candidates:
        if not isinstance(row, dict):
            raise ValueError(f"{path}: every entry must be a job object, got {type(row).__name__}")
        if str(row.get("id") or "") == job_id:
            return row
    have = [str(r.get("id")) for r in candidates if isinstance(r, dict)]
    raise ValueError(f"{path} has no job {job_id}" + (f" (it has: {', '.join(have[:5])})" if have else ""))


def fetch_row(job_id: str, timeout: float = 10.0) -> dict:
    """``GET $HIVE_URL/v1/jobs/<id>``. Env-only configuration."""
    base = os.environ.get("HIVE_URL", "").strip().rstrip("/")
    if not base:
        raise ValueError("HIVE_URL is not set — export it, or pass --row FILE to work offline")
    url = base + "/v1/jobs/" + urllib.parse.quote(job_id, safe="")
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - operator-configured URL
            body = json.load(resp)
    except urllib.error.HTTPError as exc:
        raise ValueError(f"the bus answered {exc.code} for job {job_id}") from exc
    except urllib.error.URLError as exc:
        raise ValueError(f"cannot reach the bus: {exc.reason}") from exc
    except ValueError as exc:
        raise ValueError(f"the bus did not answer with JSON: {exc}") from exc
    if not isinstance(body, dict) or "id" not in body:
        raise ValueError(f"unexpected answer for job {job_id}: expected a job object")
    return body


def normalize_events(raw, job_id: str) -> list[dict] | None:
    """Loop events from a file body; None means "unavailable", [] means "none".

    Accepts a bare list of event payloads, ``{"events": [...]}``, or the raw
    MNEMOS search body (``memories``/``results`` rows with a JSON ``content``
    and ``metadata``). Same filtering and ordering as
    ``loop_outcome.fetch_loop_events``: rows of another job are dropped, a
    garbled row is skipped rather than sinking the trail, and the list is
    ordered by seq — which sorts mixed str/int seqs without raising.
    """
    if raw is None:
        return None
    rows = raw
    if isinstance(raw, dict):
        rows = raw.get("events") or raw.get("memories") or raw.get("results") or []
    if not isinstance(rows, list):
        return None
    events: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        if meta.get("job_id") and str(meta["job_id"]) != str(job_id):
            continue
        if "content" in row:
            payload = row.get("content")
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload or "{}")
                except ValueError:
                    continue  # one garbled event must not discard the trail
            if not isinstance(payload, dict):
                continue
            payload.setdefault("phase", meta.get("phase"))
            payload["_seq"] = meta.get("seq", 0)
            events.append(payload)
        else:
            payload = dict(row)
            payload.setdefault("_seq", payload.get("seq", meta.get("seq", 0)))
            events.append(payload)
    return sorted(events, key=_seq)


def load_events(path: str | Path, job_id: str) -> list[dict] | None:
    try:
        raw = json.loads(Path(path).read_text())
    except OSError as exc:
        raise ValueError(f"cannot read {path}: {exc}") from exc
    except ValueError as exc:
        raise ValueError(f"{path} is not valid JSON: {exc}") from exc
    events = normalize_events(raw, job_id)
    if events is None:
        raise ValueError(f"{path} must be an event list, an {{'events': [...]}} body, or an MNEMOS search body")
    return events


def get_events(job_id: str, path: str | None) -> list[dict] | None:
    """Events from --events, else MNEMOS when configured, else None."""
    if path:
        return load_events(path, job_id)
    if os.environ.get("MNEMOS_URL") and os.environ.get("MNEMOS_TOKEN"):
        return loop_outcome.fetch_loop_events(job_id)
    return None


# ── sections ────────────────────────────────────────────────────────────────


def section_job(row: dict) -> list[str]:
    retry = row.get("retry_count")
    try:
        retry_n = max(0, int(retry or 0))
    except (TypeError, ValueError):
        retry_n = 0
    max_retries = row.get("max_retries")
    attempts = retry_n + 1  # the retry counter counts REPLACEMENTS of an attempt
    lines = [
        f"  status:          {_short(row.get('status')) or '(missing)'}",
        f"  kind:            {_short(row.get('kind')) or '(missing)'}",
        f"  attempts:        {attempts} (retry_count={_short(retry) or 0}, max_retries={_short(max_retries) or 'n/a'})",
        f"  submitter:       {_short(row.get('submitter_urn'))}",
        f"  claimed_by:      {_short(row.get('claimed_by'))}",
        f"  project/repo:    {_short(row.get('project'))} {_short(row.get('repo'))}".rstrip(),
        (
            f"  branches:        head={_short(row.get('head_branch'))} "
            f"base={_short(row.get('base_branch'))} created_from={_short(row.get('branch_created_from'))}"
        ),
        f"  model/provider:  {_short(row.get('claimed_model'))} {_short(row.get('claimed_provider'))}".rstrip(),
        f"  timing:          started={_short(row.get('started_at'))} ended={_short(row.get('ended_at'))}",
    ]
    for key in ("last_release_reason", "last_decline_reason"):
        if row.get(key):
            lines.append(f"  {key}: {_short(row[key])}")
    return [f"job {_short(row.get('id')) or '(missing id)'}", *lines]


def section_loop(row: dict, events: list[dict] | None, requeues: int, reviewer_ok: bool) -> list[str]:
    cls = loop_outcome.classify(row, events_fetcher=lambda _jid: events)
    action = loop_outcome.decide(cls, max(0, requeues), reviewer_ok=lambda: reviewer_ok)
    res = _as_result(row)
    teardown = ((res.get("loop_result") or {}) if isinstance(res.get("loop_result"), dict) else {}).get("teardown")
    teardown = teardown if isinstance(teardown, dict) else {}
    lines = [
        f"  class:           {cls}",
        f"  doctor:          {action} — {ACTION_WORDS.get(action, action)}",
        f"  requeues spent:  {max(0, requeues)} of {loop_outcome.MAX_AUTO_REQUEUES}",
        f"  teardown reason: {_short(teardown.get('reason')) or '(none)'}",
    ]
    verdicts = (res.get("loop_result") or {}).get("verdicts") if isinstance(res.get("loop_result"), dict) else None
    if verdicts:
        lines.append(f"  loop verdicts:   {_short(verdicts)}")
    if str(row.get("status") or "") != "failed":
        lines.append(f"  note:            status is '{_short(row.get('status'))}' — the doctor only sweeps failed rows")
    return ["loop outcome", *lines]


def section_timeline(events: list[dict] | None) -> list[str]:
    if events is None:
        return [
            "timeline",
            "  (loop events unavailable: MNEMOS_URL/MNEMOS_TOKEN are not set, or --events was not given)",
        ]
    if not events:
        return ["timeline", "  (no loop events recorded for this job)"]
    lines = [f"timeline ({len(events)} events)"]
    for i, ev in enumerate(events, 1):
        phase = _short(ev.get("phase")) or "(no phase)"
        when = _first(ev, _TIME_KEYS) or "-"
        err = _first(ev, _ERROR_KEYS)
        extra = ""
        if not err and ev.get("reason") and ev.get("phase") != "teardown":
            err = _short(ev.get("reason"))
        elif ev.get("reason") and not err:
            err = f"reason={_short(ev.get('reason'))}"
        if err:
            extra = f"  {err}"
        lines.append(f"  {i:>3}. {phase:<18} t={when}{extra}")
    return lines


def section_reviewer(res: dict) -> list[str]:
    lines = ["reviewer"]
    record = res.get("reviewer")
    if not isinstance(record, dict):
        record = {}
    models = res.get("models") if isinstance(res.get("models"), dict) else {}
    evidence = res.get("review_evidence") if isinstance(res.get("review_evidence"), dict) else {}
    if not record and not evidence.get("reviewers") and not models.get("reviewers"):
        lines.append("  (no reviewer record on this row)")
        return lines
    lines.append(f"  model:           {_short(record.get('model')) or '(none)'}")
    lines.append(f"  verdict:         {_short(record.get('verdict')) or '(none)'}")
    lines.append(f"  source:          {_short(record.get('source')) or '(none)'}")
    lines.append(f"  author model:    {_short(models.get('author'))}")
    lines.append(f"  reviewer models: {_short(models.get('reviewers'))}")
    ev_reviewers = evidence.get("reviewers") if isinstance(evidence.get("reviewers"), list) else []
    if ev_reviewers:
        lines.append(
            f"  evidence:        {_short([r.get('model') or r.get('role') for r in ev_reviewers if isinstance(r, dict)])}"
        )
    briefs = res.get("hive_reviews") if isinstance(res.get("hive_reviews"), list) else []
    if briefs:
        texts = [
            f"[{_short(b.get('stage') or 'brief')}] {_short(b.get('review_text'), 120)}"
            for b in briefs
            if isinstance(b, dict)
        ]
        lines.append(f"  brief reviews:   {len(briefs)}: {'; '.join(texts)}")
    findings = res.get("reviewer_findings")
    if findings is not None:
        count = len(findings) if isinstance(findings, list) else 1
        lines.append(f"  findings:        {count}")
    if evidence.get("warnings"):
        lines.append(f"  warnings:        {_short(evidence['warnings'], 240)}")
    return lines


def section_workspace(row: dict, res: dict) -> list[str]:
    lines = ["workspace / branch / commit"]
    ws = res.get("workspace_preserved")
    lines.append(f"  workspace:       {_short(ws) or '(not preserved)'}")
    branch = _first(res, ("branch",))
    if not branch and isinstance(res.get("codex_delivery"), dict):
        branch = _short(res["codex_delivery"].get("branch"))
    lines.append(f"  branch:          {_short(branch) or '(none reported)'}")
    head = _first(res, ("head_sha",))
    verified = res.get("remote_verified")
    lines.append(
        f"  head:            {_short(head) or '(none)'}"
        + (f"  remote_verified={'yes' if verified else 'no'}" if verified is not None else "")
    )
    lines.append(f"  base:            {_first(res, ('base_sha',)) or '(none)'}")
    commits = res.get("commits") if isinstance(res.get("commits"), list) else []
    lines.append(f"  commits:         {len(commits)}" + (f"  {_short(commits, 120)}" if commits else ""))
    files = res.get("files_changed") if isinstance(res.get("files_changed"), list) else None
    if files is not None:
        lines.append(f"  files changed:   {len(files)}")
    if "worktree_clean" in res:
        lines.append(f"  worktree clean:  {'yes' if res.get('worktree_clean') else 'no'}")
    remote_sha = res.get("remote_sha")
    if remote_sha:
        lines.append(f"  remote sha:      {_short(remote_sha)}  pushed={'yes' if res.get('pushed') else 'no'}")
    if res.get("verification_error"):
        lines.append(f"  verify error:    {_short(res.get('verification_error'), 240)}")
    if isinstance(res.get("codex_delivery"), dict):
        delivery = res["codex_delivery"]
        commit = delivery.get("commit") if isinstance(delivery.get("commit"), dict) else {}
        lines.append(
            f"  codex delivery:  branch={_short(delivery.get('branch'))} commit={_short(commit.get('commit'))}"
        )
    return lines


def section_failure(res: dict) -> list[str]:
    lines = ["failure"]
    reason = _first(res, ("failure_reason", "failure_reason_error"))
    error = _first(res, ("error", "raw"))
    if not reason and not error and "exit_code" not in res:
        lines.append("  (nothing recorded on this row)")
        return lines
    lines.append(f"  failure_reason:  {reason or '(none)'}")
    lines.append(f"  error:           {error or '(none)'}")
    if "exit_code" in res:
        lines.append(f"  exit_code:       {_short(res.get('exit_code'))}")
    if res.get("tail"):
        lines.append(f"  tail:            {_short(res.get('tail'), 240)}")
    if res.get("budget_exceeded"):
        lines.append(f"  budget_exceeded: {_short(res.get('budget_exceeded'), 200)}")
    return lines


def explain(
    row: dict,
    events: list[dict] | None,
    *,
    requeues: int = 0,
    reviewer_healthy: bool = True,
) -> str:
    """The whole report. Pure: no network, no writes, whatever the row holds."""
    res = _as_result(row)
    sections = [
        section_job(row),
        section_loop(row, events, requeues, reviewer_healthy),
        section_timeline(events),
        section_reviewer(res),
        section_workspace(row, res),
        section_failure(res),
    ]
    return "\n".join("\n".join(section) for section in sections)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("job_id", help="job id, as returned by POST /v1/jobs")
    parser.add_argument("--row", help="JSON file with the job row instead of GET $HIVE_URL/v1/jobs/<id>")
    parser.add_argument("--events", help="JSON file with the loop events instead of MNEMOS")
    parser.add_argument(
        "--requeues",
        type=int,
        default=0,
        help="automatic requeues already spent on this job (default 0)",
    )
    parser.add_argument(
        "--reviewer-down",
        action="store_true",
        help="treat the production reviewer as unhealthy (turns reviewer_unavailable into 'hold')",
    )
    args = parser.parse_args(argv)
    try:
        row = load_row(args.row, args.job_id) if args.row else fetch_row(args.job_id)
        events = get_events(args.job_id, args.events)
    except ValueError as exc:
        print(f"explain_job: {exc}", file=sys.stderr)
        return 2
    print(explain(row, events, requeues=max(0, args.requeues), reviewer_healthy=not args.reviewer_down))
    return 0


if __name__ == "__main__":
    sys.exit(main())
