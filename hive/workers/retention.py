"""Terminal-job retention sweep (pure core) for the hive doctor.

Operator rule (2026-10-27): the queue must NEVER hold a backlog of dead jobs —
"we should never have a queue of 20+ dead jobs". A one-off manual purge is not
enough; the doctor (which runs on PYTHIA as a quadlet with bus access) sweeps
terminal rows periodically so the backlog stays bounded and observable.

Design: a PURE decision function (``plan_retention_sweep``) that takes already-
fetched job rows and returns a purge plan (ordered id list + per-id reason),
plus thin helpers (snapshot writer, status-age extraction). The periodic caller
lives in ``hive.workers.doctor``: it fetches rows through the bus HTTP API,
re-reads each candidate's status immediately before deleting (never purge a job
another code path may still be writing), then issues ``POST /v1/jobs/purge``
with an EXPLICIT id list (never an unfiltered purge).

Predicates (exact):

* DEAD statuses: ``failed`` / ``dead-letter`` / ``cancelled``.
  - HARD INVARIANT (the operator's rule): after every sweep,
    ``count(status in {failed, dead-letter, cancelled}) <= RETENTION_MAX_DEAD``
    (default 10). If the count is above the cap — regardless of TTL — the
    OLDEST dead rows are purged first until the count is exactly at the cap.
    Never let the invariant be violated by a busy period.
  - TTL: dead rows older than ``RETENTION_DEAD_TTL_SEC`` (default 3600 s)
    (age = now - COALESCE(ended_at, last_update_at)) are purged.
* TRIAGE work orders: ``triage:*`` jobs in ``done`` / ``cancelled`` /
  ``failed`` / ``dead-letter`` older than ``RETENTION_TRIAGE_TTL_SEC``
  (default 600 s) are purged — after the doctor has processed them; a triage
  row is a work order, not an archive. A triage row in a dead status is
  counted by BOTH the dead invariant and the triage TTL (first pick wins).
* Non-triage ``done`` jobs are never touched (proof-of-work archive).

PROTECTED set (never purged — explicit):
* Active statuses: ``queued`` / ``offered`` / ``claimed`` / ``running`` /
  ``needs-review``.
* ``done`` jobs that are NOT ``triage:*``.
* ``failed_completion`` (worker-side retryable — not part of the dead set) and
  any other status outside the dead/triage sets.

Audit: BEFORE any deletion, the doctor appends a JSONL snapshot of every row it
is about to remove (id, kind, status, claimed_by, ended_at, result summary) to
``~/.hive/retention/<utc>.jsonl`` (rotating by UTC hour) so the audit trail
survives the row deletion. A snapshot write failure aborts the sweep — no
purge without a snapshot.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

#: Terminal "dead" statuses counted by the invariant and aged by the dead TTL.
DEAD_STATUSES: tuple[str, ...] = ("failed", "dead-letter", "cancelled")

#: Terminal statuses purgeable for ``triage:*`` work-order rows.
TRIAGE_PURGE_STATUSES: tuple[str, ...] = ("done", "cancelled", "failed", "dead-letter")

#: Statuses that are NEVER purged — in-flight work (the protected active set).
PROTECTED_ACTIVE_STATUSES: frozenset[str] = frozenset(
    ("queued", "offered", "claimed", "running", "needs-review")
)

#: Kind prefix that marks a work-order (triage) row.
TRIAGE_KIND_PREFIX = "triage:"


def is_triage_job(kind: str | None) -> bool:
    """True when the job kind is a triage work order (``triage:*``)."""
    return bool(kind) and str(kind).startswith(TRIAGE_KIND_PREFIX)


def _age_at(job: dict[str, Any], now: float) -> float | None:
    """Age of a row at ``now``: seconds since ``ended_at`` (fallback
    ``last_update_at``). Returns None when no usable timestamp exists — such a
    row can only leave the queue under the invariant cap, never under TTL."""
    ts = job.get("ended_at")
    if not ts:
        ts = job.get("last_update_at")
    if not isinstance(ts, (int, float)) or isinstance(ts, bool) or ts <= 0:
        return None
    return now - float(ts)


def age_of(job: dict[str, Any]) -> float | None:
    """Public age helper (wall-clock now)."""
    return _age_at(job, time.time())


def _result_summary(job: dict[str, Any], limit: int = 300) -> str:
    """Compact summary of a job's result payload for the retention snapshot.
    Tolerates str/dict/None and JSON-encoded strings."""
    result = job.get("result")
    if result is None:
        return ""
    if isinstance(result, str):
        text = result
    else:
        try:
            text = json.dumps(result, separators=(",", ":"))
        except (TypeError, ValueError):
            text = str(result)
    text = text.strip()
    return text[:limit] if len(text) > limit else text


def _snapshot_entry(job: dict[str, Any], reason: str, now: float) -> dict[str, Any]:
    return {
        "id": job.get("id"),
        "kind": job.get("kind"),
        "status": job.get("status"),
        "claimed_by": job.get("claimed_by"),
        "ended_at": job.get("ended_at"),
        "last_update_at": job.get("last_update_at"),
        "age_sec": _age_at(job, now),
        "result_summary": _result_summary(job),
        "reason": reason,
    }


def _is_protected(job: dict[str, Any]) -> bool:
    """Explicit protected set: active statuses, non-triage done rows, and any
    status outside the dead/triage purge sets (e.g. failed_completion)."""
    status = job.get("status")
    if status in PROTECTED_ACTIVE_STATUSES:
        return True
    if status == "done" and not is_triage_job(job.get("kind")):
        return True
    if status not in DEAD_STATUSES and status not in TRIAGE_PURGE_STATUSES:
        return True
    return False


def plan_retention_sweep(
    jobs: list[dict[str, Any]],
    *,
    dead_ttl: float = 3600.0,
    triage_ttl: float = 600.0,
    max_dead: int = 10,
    now: float | None = None,
) -> dict[str, Any]:
    """Pure sweep planner. Given job rows (as returned by the bus listing),
    return the purge plan:

    {
      "purge_ids": [id, ...]    # explicit list for /v1/jobs/purge, oldest first
      "reasons":   {id: "ttl-dead" | "ttl-triage" | "cap-dead"}
      "entries":   [snapshot row, ...]   # one per purge id, same order
    }

    Rules (first pick wins the reason label; the dead set is processed first
    so the invariant is never violated by triage picks):

    1. CAP (hard invariant, operator rule 2026-10-27): if the dead set
       (``failed``/``dead-letter``/``cancelled``) is above ``max_dead``, the
       OLDEST dead rows are purged (``cap-dead``) until the dead count is
       exactly at the cap — regardless of TTL, so a busy period can never
       leave the queue with 20+ dead rows.
    2. DEAD TTL: dead survivors (the newest ``max_dead``) older than
       ``dead_ttl`` are purged (``ttl-dead``). A dead row younger than the
       TTL survives unless the cap forces it out in rule 1.
    3. TRIAGE TTL: ``triage:*`` rows in ``done``/``cancelled``/``failed``/
       ``dead-letter`` older than ``triage_ttl`` are purged (``ttl-triage``).
       A triage row in a dead status is picked by rule 1 or 2 first; the
       triage pass only adds rows those rules did not pick.

    Protected rows (active statuses, non-triage done, failed_completion, any
    other status) are never included. The sweep is idempotent: a second pass
    over the survivors of a full sweep is a no-op.
    """
    if now is None:
        now = time.time()

    dead_rows: list[dict[str, Any]] = []  # full current dead set, for the invariant
    triage_rows: list[dict[str, Any]] = []  # full current triage-terminal set

    for job in jobs:
        if not isinstance(job, dict) or not job.get("id"):
            continue
        if _is_protected(job):
            continue
        status = job.get("status")
        if status in DEAD_STATUSES:
            dead_rows.append(job)
        if status in TRIAGE_PURGE_STATUSES and is_triage_job(job.get("kind")):
            # A terminal triage:* row in a dead status (cancelled/failed/
            # dead-letter) belongs to BOTH sets: the dead invariant counts it,
            # and the triage TTL may also apply. `done` triage rows are
            # triage-only (never counted by the dead invariant).
            triage_rows.append(job)

    # Oldest (LARGEST age) first; rows without a usable timestamp are the
    # riskiest — sort them to the FRONT (-inf is smaller than any -age).
    def _sort_key(job: dict[str, Any]) -> tuple:
        a = _age_at(job, now)
        return (-(a) if a is not None else float("-inf"), str(job.get("id") or ""))

    dead_rows = sorted(dead_rows, key=_sort_key)
    triage_rows = sorted(triage_rows, key=_sort_key)
    max_dead = max(0, int(max_dead))

    reasons: dict[str, str] = {}
    entries: list[dict[str, Any]] = []

    def _pick(job: dict[str, Any], reason: str) -> None:
        if job["id"] in reasons:
            return
        reasons[job["id"]] = reason
        entries.append(_snapshot_entry(job, reason, now))

    # 1) CAP — the hard invariant. The queue must never hold more than
    #    ``max_dead`` dead rows, so when the dead set is over the cap the
    #    oldest (dead_count - max_dead) dead rows are purged, TTL or not.
    #    After this pass the dead survivors are exactly the newest
    #    max_dead rows — the invariant holds and the pass is a no-op the
    #    second time (idempotent).
    if len(dead_rows) > max_dead:
        for job in dead_rows[: len(dead_rows) - max_dead]:
            _pick(job, "cap-dead")

    # 2) DEAD TTL — among the surviving (newest max_dead) dead rows, purge
    #    those past the dead TTL. Younger survivors stay.
    for job in dead_rows:
        if job["id"] in reasons:
            continue
        a = _age_at(job, now)
        if a is not None and a > dead_ttl:
            _pick(job, "ttl-dead")

    # 3) TRIAGE TTL — triage work orders past their (shorter) TTL, oldest
    #    first. Not subject to the dead cap; a row already picked by rule 1
    #    or 2 is a no-op here.
    for job in triage_rows:
        a = _age_at(job, now)
        if a is not None and a > triage_ttl:
            _pick(job, "ttl-triage")

    return {"purge_ids": list(reasons.keys()), "reasons": reasons, "entries": entries}


def write_retention_snapshot(
    directory: str | Path,
    entries: list[dict[str, Any]],
    *,
    ts: float | None = None,
) -> Path | None:
    """Append a JSONL snapshot of the rows about to be purged. One file per
    UTC hour (``<utc>.jsonl``, e.g. ``2026-10-27T03.jsonl``) — a rotating local
    audit file that survives the row deletion. Returns the file path, or None
    when there is nothing to record. Raises on I/O errors so a failed audit
    write fails the sweep (no purge without a snapshot)."""
    if not entries:
        return None
    ts = ts if ts is not None else time.time()
    stamp = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H")
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{stamp}.jsonl"
    header = {"ts": ts, "event": "retention-sweep-snapshot", "count": len(entries)}
    lines = [json.dumps(header, separators=(",", ":"))]
    lines.extend(json.dumps(e, separators=(",", ":")) for e in entries)
    with open(path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return path


def default_snapshot_dir() -> Path:
    """``~/.hive/retention`` — the rotating local audit directory
    (override with ``HIVE_RETENTION_DIR`` for tests/alternate deployments)."""
    return Path(os.environ.get("HIVE_RETENTION_DIR", str(Path.home() / ".hive" / "retention")))


def parse_env_settings() -> dict[str, float]:
    """Read the retention knobs from the environment (env-only, no baked-in
    secrets). Defaults: dead TTL 3600 s, triage TTL 600 s, cap 10."""

    def _f(name: str, default: float) -> float:
        raw = os.environ.get(name)
        if raw is None:
            return default
        try:
            v = float(raw)
            return v if v > 0 else default
        except ValueError:
            return default

    def _i(name: str, default: int) -> int:
        raw = os.environ.get(name)
        if raw is None:
            return default
        try:
            v = int(raw)
            return v if v >= 0 else default
        except ValueError:
            return default

    return {
        "dead_ttl": _f("RETENTION_DEAD_TTL_SEC", 3600.0),
        "triage_ttl": _f("RETENTION_TRIAGE_TTL_SEC", 600.0),
        "max_dead": _i("RETENTION_MAX_DEAD", 10),
    }
