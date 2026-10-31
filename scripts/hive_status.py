#!/usr/bin/env python3
"""One-screen Hive operator status. Read-only.

Queue counts by status and kind, agents by kind and status, every
running/claimed job (host, age, head branch), every failed/needs-review
row with its loop-outcome class and the doctor's planned action, plus
flags: rows older than a threshold, agents offline but still claiming.

Configuration is env-only: HIVE_URL, and HIVE_BUS_ADMIN_TOKEN when a
route needs it. No token value is ever printed.

    HIVE_URL=http://<bus-host>:5005 scripts/hive_status.py
    scripts/hive_status.py --rows tests/fixtures/hive_status.sample.json
    scripts/hive_status.py --json --rows rows.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hive import review_queue  # noqa: E402
from hive.workers import loop_outcome  # noqa: E402

ACTIVE_JOB_STATUSES = ("claimed", "running")
ATTENTION_STATUSES = ("failed", "needs-review")


def _redacted_env() -> dict:
    """Config names only, never values (tokens must not leak into output)."""
    return {
        "HIVE_URL": "set" if os.environ.get("HIVE_URL") else "unset",
        "HIVE_BUS_ADMIN_TOKEN": "set" if os.environ.get("HIVE_BUS_ADMIN_TOKEN") else "unset",
    }


def _http_get(base: str, path: str, timeout: float = 10.0) -> dict | None:
    url = base.rstrip("/") + path
    headers = {"accept": "application/json"}
    token = os.environ.get("HIVE_BUS_ADMIN_TOKEN", "")
    if token:
        headers["authorization"] = "Bearer " + token
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        try:
            return {"_http_error": exc.code}
        except Exception:
            return {"_http_error": exc.code}
    except Exception as exc:  # noqa: BLE001 - read-only tool reports, never raises
        return {"_transport_error": str(exc)}


def fetch_snapshot(base: str) -> dict:
    jobs: list = []
    agents: list = []
    for status in ("queued", "claimed", "running", "failed", "needs-review", "done", "cancelled"):
        data = _http_get(base, "/v1/jobs?status=" + status + "&limit=500")
        if isinstance(data, dict) and isinstance(data.get("jobs"), list):
            jobs.extend(data["jobs"])
    data = _http_get(base, "/v1/agents?include_offline=true&limit=500")
    if isinstance(data, dict) and isinstance(data.get("agents"), list):
        agents = data["agents"]
    else:
        data = _http_get(base, "/v1/agents?limit=500")
        if isinstance(data, dict) and isinstance(data.get("agents"), list):
            agents = data["agents"]
    return {"jobs": jobs, "agents": agents}


def _age_str(ts: float | None, now: float) -> str:
    if not ts:
        return "-"
    try:
        delta = max(0, int(now - float(ts)))
    except (TypeError, ValueError):
        return "-"
    if delta < 3600:
        return f"{delta // 60}m"
    if delta < 86400:
        return f"{delta // 3600}h"
    return f"{delta // 86400}d"


def _job_ts(job: dict) -> float | None:
    for key in ("updated_at", "started_at", "ended_at"):
        try:
            if job.get(key):
                return float(job[key])
        except (TypeError, ValueError):
            continue
    return None


def build_status(rows: dict, *, older_than_hours: float = 24.0, now: float | None = None) -> dict:
    now = time.time() if now is None else float(now)
    jobs = rows.get("jobs", []) or []
    agents = rows.get("agents", []) or []
    # (count plainly; avoid clever one-liners that hide missing keys)
    by_status = {}
    for job in jobs:
        by_status[str(job.get("status") or "?")] = by_status.get(str(job.get("status") or "?"), 0) + 1
    by_kind = {}
    for job in jobs:
        by_kind[str(job.get("kind") or "?")] = by_kind.get(str(job.get("kind") or "?"), 0) + 1
    agents_by_kind: dict[str, int] = {}
    agents_by_status: dict[str, int] = {}
    for agent in agents:
        agents_by_kind[str(agent.get("kind") or "?")] = agents_by_kind.get(str(agent.get("kind") or "?"), 0) + 1
        agents_by_status[str(agent.get("status") or "?")] = agents_by_status.get(str(agent.get("status") or "?"), 0) + 1
    host_by_urn = {str(a.get("urn")): str(a.get("host") or "-") for a in agents if a.get("urn")}
    active = []
    for job in jobs:
        if job.get("status") in ACTIVE_JOB_STATUSES:
            urn = str(job.get("claimed_by") or "-")
            active.append(
                {
                    "id": str(job.get("id") or ""),
                    "kind": str(job.get("kind") or ""),
                    "status": str(job.get("status") or ""),
                    "host": host_by_urn.get(urn, "-"),
                    "claimed_by": urn,
                    "age": _age_str(_job_ts(job), now),
                    "head_branch": str(job.get("head_branch") or "-"),
                }
            )
    attention = []
    for job in jobs:
        if job.get("status") in ATTENTION_STATUSES:
            cls = loop_outcome.classify(job, events_fetcher=lambda _jid: None)
            local_only = cls == loop_outcome.KILLED_BY_SIGNAL and loop_outcome.has_local_only_commits(job)
            action = loop_outcome.decide(cls, 0, reviewer_ok=lambda: True, local_commits_only=local_only)
            row = {
                "id": str(job.get("id") or ""),
                "kind": str(job.get("kind") or ""),
                "status": str(job.get("status") or ""),
                "class": cls,
                "doctor_action": action,
                "age": _age_str(_job_ts(job), now),
                "head_branch": str(job.get("head_branch") or "-"),
            }
            # The review queue IS the triage queue: an awaiting_review row is an
            # ordinary needs-review row that also says why it is waiting.
            state = review_queue.review_state(job)
            if state is not None:
                row.update(
                    {
                        "review_state": review_queue.AWAITING_REVIEW,
                        "family": state.get("author_family"),
                        "review_reason": review_queue.awaiting_review_reason(job),
                        "next_attempt_at": state.get("next_attempt_at"),
                        "review_attempt": int(state.get("attempt") or 0),
                    }
                )
            attention.append(row)
    cutoff = now - float(older_than_hours) * 3600
    stale = [
        {"id": str(j.get("id") or ""), "status": str(j.get("status") or ""), "age": _age_str(_job_ts(j), now)}
        for j in jobs
        if (_job_ts(j) or now) < cutoff and j.get("status") in (*ACTIVE_JOB_STATUSES, *ATTENTION_STATUSES)
    ]
    claiming_urns = {
        str(j.get("claimed_by")) for j in jobs if j.get("status") in ACTIVE_JOB_STATUSES and j.get("claimed_by")
    }
    offline_claiming = [
        {"urn": str(a.get("urn")), "kind": str(a.get("kind") or ""), "host": str(a.get("host") or "-")}
        for a in agents
        if str(a.get("status") or "") == "offline" and str(a.get("urn") or "") in claiming_urns
    ]
    return {
        "queue": {"by_status": by_status, "by_kind": by_kind, "total": len(jobs)},
        "agents": {"by_kind": agents_by_kind, "by_status": agents_by_status, "total": len(agents)},
        "active": sorted(active, key=lambda r: r["id"]),
        "attention": sorted(attention, key=lambda r: r["id"]),
        "flags": {"stale": sorted(stale, key=lambda r: r["id"]), "offline_claiming": offline_claiming},
    }


def render_text(snapshot: dict) -> str:
    lines = ["queue:"]
    for status, count in sorted(snapshot["queue"]["by_status"].items()):
        lines.append(f"  {status}: {count}")
    lines.append("kinds:")
    for kind, count in sorted(snapshot["queue"]["by_kind"].items()):
        lines.append(f"  {kind}: {count}")
    lines.append("agents:")
    for kind, count in sorted(snapshot["agents"]["by_kind"].items()):
        lines.append(f"  {kind}: {count}")
    lines.append("agent statuses:")
    for status, count in sorted(snapshot["agents"]["by_status"].items()):
        lines.append(f"  {status}: {count}")
    lines.append("active (claimed/running):")
    if not snapshot["active"]:
        lines.append("  (none)")
    for row in snapshot["active"]:
        lines.append(
            f"  {row['id'][:12]} {row['status']} {row['kind']} host={row['host']} "
            f"age={row['age']} branch={row['head_branch']}"
        )
    lines.append("attention (failed/needs-review):")
    if not snapshot["attention"]:
        lines.append("  (none)")
    for row in snapshot["attention"]:
        line = (
            f"  {row['id'][:12]} {row['status']} {row['kind']} class={row['class']} "
            f"doctor={row['doctor_action']} age={row['age']}"
        )
        if row.get("review_state"):
            line += f" review={row['review_state']} reason={row['review_reason']}"
        lines.append(line)
    lines.append("flags:")
    if not snapshot["flags"]["stale"] and not snapshot["flags"]["offline_claiming"]:
        lines.append("  (none)")
    for row in snapshot["flags"]["stale"]:
        lines.append(f"  stale: {row['id'][:12]} {row['status']} age={row['age']}")
    for agent in snapshot["flags"]["offline_claiming"]:
        lines.append(f"  offline-claiming: {agent['kind']}@{agent['host']}")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="One-screen Hive operator status (read-only).")
    parser.add_argument("--json", action="store_true", help="Machine-readable output.")
    parser.add_argument("--rows", help="Offline mode: JSON file with {jobs, agents} (for tests).")
    parser.add_argument("--older-than-hours", type=float, default=24.0)
    args = parser.parse_args(argv)
    if args.rows:
        try:
            rows = json.loads(Path(args.rows).read_text())
        except Exception as exc:  # noqa: BLE001
            print(f"cannot read --rows file: {exc}", file=sys.stderr)
            return 2
    else:
        base = os.environ.get("HIVE_URL", "").strip()
        if not base:
            print("HIVE_URL is not set (env-only configuration).", file=sys.stderr)
            return 2
        rows = fetch_snapshot(base)
    _ = _redacted_env()  # computed for completeness; values never printed
    snapshot = build_status(rows, older_than_hours=args.older_than_hours)
    if args.json:
        print(json.dumps(snapshot, indent=2, sort_keys=True))
    else:
        print(render_text(snapshot), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
