#!/usr/bin/env python3
"""Replay failed loop rows through ``loop_outcome``: class and sweep action, offline.

``hive/workers/loop_outcome.py`` decides, for a failed zoder-loop row, *why* it
failed and what the orphan-failure sweep would do about it. Getting that decision
wrong is expensive in both directions — a wrong ``requeue`` burns a retry, a wrong
``skip`` strands finished work — so it should be inspectable without a live bus.

Input is a JSON file: the body of ``GET /v1/jobs?status=failed`` (``{"jobs": [...]}``),
a bare list of rows, or one row object. Output is one line per row:

    <job id>  <class>  <action>

Strictly read-only: no network, no writes, no bus. MNEMOS loop events are treated as
UNAVAILABLE (the fetcher returns None), so a row is classified from its own result
only — which is also the worst case a live sweep can meet when MNEMOS is down.

    scripts/replay_loop_outcomes.py rows.json
    scripts/replay_loop_outcomes.py rows.json --requeues 1 --reviewer-down
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# `hive` is a package at the repo root; this script lives one level down.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hive.workers import loop_outcome  # noqa: E402


def _no_events(job_id: str) -> None:  # noqa: ARG001 - signature matches the fetcher
    """The offline fetcher: no loop events are consulted, by construction."""
    return None


def load_rows(path: str | Path) -> list[dict]:
    """Rows from ``{"jobs": [...]}``, a bare list, or a single row object.

    Raises ``ValueError`` with a message an operator can act on — never a bare
    JSONDecodeError, and never a silent empty list for a file it could not read.
    """
    try:
        raw = json.loads(Path(path).read_text())
    except OSError as exc:
        raise ValueError(f"cannot read {path}: {exc}") from exc
    except ValueError as exc:
        raise ValueError(f"{path} is not valid JSON: {exc}") from exc
    if isinstance(raw, dict):
        rows = raw.get("jobs", [raw] if "id" in raw else None)
    elif isinstance(raw, list):
        rows = raw
    else:
        rows = None
    if not isinstance(rows, list):
        raise ValueError(f"{path} must be a job list, a {{'jobs': [...]}} body, or one job object")
    out = []
    for row in rows:
        if isinstance(row, dict):
            out.append(row)
        else:
            raise ValueError(f"{path}: every row must be an object, got {type(row).__name__}")
    return out


def replay(rows: list[dict], *, requeues_so_far: int = 0, reviewer_healthy: bool = True) -> list[dict]:
    """``[{id, class, action}]`` for each row. Pure: no network, no writes."""
    entries = []
    for row in rows:
        cls = loop_outcome.classify(row, events_fetcher=_no_events)
        action = loop_outcome.decide(
            cls,
            requeues_so_far,
            reviewer_ok=lambda: reviewer_healthy,  # noqa: B023 - evaluated once per row
        )
        entries.append({"id": str(row.get("id") or ""), "class": cls, "action": action})
    return entries


def format_report(entries: list[dict]) -> str:
    """One line per row: id, class, action. Ids are printed in full for grep-ability."""
    return "\n".join(f"{e['id']}  {e['class']}  {e['action']}" for e in entries)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("rows", help="JSON file of failed job rows")
    parser.add_argument(
        "--requeues",
        type=int,
        default=0,
        help="automatic requeues already spent on these rows (default 0)",
    )
    parser.add_argument(
        "--reviewer-down",
        action="store_true",
        help="treat the production reviewer as unhealthy (turns reviewer_unavailable into 'hold')",
    )
    args = parser.parse_args(argv)
    try:
        rows = load_rows(args.rows)
    except ValueError as exc:
        print(f"replay_loop_outcomes: {exc}", file=sys.stderr)
        return 2
    entries = replay(rows, requeues_so_far=max(0, args.requeues), reviewer_healthy=not args.reviewer_down)
    print(format_report(entries))
    return 0


if __name__ == "__main__":
    sys.exit(main())
