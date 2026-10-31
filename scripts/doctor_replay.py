#!/usr/bin/env python3
"""Replay recorded job rows through the doctor v1 and v2 decision paths.

`DOCTOR_V2_MODE=shadow` runs the v2 rule engine log-only beside the
established doctor. This harness replays that side-by-side offline: each case
in the sequence file (a job row plus its loop events) goes through

* v1 — the established sweep verdict: ``loop_outcome.classify`` +
  ``loop_outcome.decide`` (failed loop rows), or ``parked`` for rows that are
  already ``needs-review`` (today's report-only behaviour takes no automatic
  action there);
* v2 — ``doctor_v2.collect_evidence`` + ``decide_rules`` against the shipped
  ``docs/doctor_v2_rules.toml`` (``llm`` when no rule fires: the residual path).

It prints a table of agreements and disagreements with the responsible v2
rule and exits non-zero on any disagreement the allowlist file does not pin
(including stale allowlist entries, so the pin file cannot rot silently).

Strictly read-only: no network, no bus, no STATE writes, no doctor behaviour
change. The only I/O is reading the three input files and printing the table.

    scripts/doctor_replay.py
    scripts/doctor_replay.py --cases my_cases.json --format json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# `hive` is a package at the repo root; this script lives one level down.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hive.workers import doctor_v2, loop_outcome  # noqa: E402

DEFAULT_RULES = Path(__file__).resolve().parents[1] / "docs" / "doctor_v2_rules.toml"
DEFAULT_CASES = Path(__file__).resolve().parent / "doctor_replay_cases.json"
DEFAULT_ALLOWLIST = Path(__file__).resolve().parent / "doctor_replay_allowlist.json"

#: v1 sweep verdicts that mean the same as a v2 action. Grounded in the live
#: code: triage treats ``v2 escalate`` as ``needs_review``
#: (``doctor.py:process_triage_job``), the sweep skips ``hold``/``skip`` rows
#: without acting, and ``requeue`` is the sweep's release back to the queue.
AGREE_MAP = {
    "requeue": {"release_to_queue"},
    "needs_review": {"escalate", "needs_review"},
    "skip": {"no_action"},
    "parked": {"no_action", "needs_review", "escalate"},
    # `hold` (wait: the cause is outside the job) and `default` (unknown
    # outcome keeps today's behaviour) have no v2 equivalent by construction.
}


def agree(v1_action: str, v2_action: str) -> bool:
    """True when the two paths reach the same operational outcome."""
    return v2_action in AGREE_MAP.get(v1_action, set())


def replay_case(case: dict, rules: list[dict]) -> dict:
    """One row through both paths. Pure: no network, no writes."""
    job = case.get("job", {})
    events = case.get("events") or []
    if str(job.get("status") or "") != "failed":
        # The sweep only acts on failed loop rows; anything else (e.g. a
        # stuck needs-review row) is report-only under v1, never requeued.
        v1_class, v1_action = loop_outcome.UNKNOWN, "parked"
    else:
        v1_class = loop_outcome.classify(job, events_fetcher=lambda _jid: events)
        local_only = v1_class == loop_outcome.KILLED_BY_SIGNAL and loop_outcome.has_local_only_commits(job)
        v1_action = loop_outcome.decide(
            v1_class,
            int(case.get("requeues_so_far") or 0),
            reviewer_ok=lambda: bool(case.get("reviewer_healthy", True)),
            local_commits_only=local_only,
        )
    evidence = doctor_v2.collect_evidence(job)
    decision = doctor_v2.decide_rules(evidence, rules)
    if decision is None:
        v2_action, v2_rule = "llm", "-"
    else:
        v2_action, v2_rule = decision.action, decision.rule_id
    return {
        "id": str(case.get("id") or job.get("id") or ""),
        "v1_class": v1_class,
        "v1_action": v1_action,
        "v2_rule": v2_rule,
        "v2_action": v2_action,
        "agree": agree(v1_action, v2_action),
    }


def replay(cases: list[dict], rules: list[dict]) -> list[dict]:
    """Every case through both paths, in file order."""
    return [replay_case(case, rules) for case in cases]


def check_allowlist(results: list[dict], allowlist: list[dict]) -> tuple[list[dict], list[dict]]:
    """(unexpected disagreements, stale allowlist entries).

    A disagreement is expected only when the pin file names its case id and
    the exact v1/v2 actions and rule. A pin that matches no disagreement is
    stale and fails the run, so fixed divergences cannot hide behind old pins.
    """
    pins = {(p.get("id"), p.get("v1_action"), p.get("v2_action"), p.get("v2_rule")) for p in allowlist}
    used: set[tuple] = set()
    unexpected: list[dict] = []
    for row in results:
        if row["agree"]:
            continue
        key = (row["id"], row["v1_action"], row["v2_action"], row["v2_rule"])
        if key in pins:
            used.add(key)
        else:
            unexpected.append(row)
    stale = [
        p for p in allowlist if (p.get("id"), p.get("v1_action"), p.get("v2_action"), p.get("v2_rule")) not in used
    ]
    return unexpected, stale


def format_table(results: list[dict], allowlisted_ids: set[str]) -> str:
    """The human-readable agreement table, one line per case."""
    widths = {
        "id": max([len("case"), *[len(r["id"]) for r in results]]),
        "v1_class": max([len("v1-class"), *[len(r["v1_class"]) for r in results]]),
        "v1_action": max([len("v1-action"), *[len(r["v1_action"]) for r in results]]),
        "v2_rule": max([len("v2-rule"), *[len(r["v2_rule"]) for r in results]]),
        "v2_action": max([len("v2-action"), *[len(r["v2_action"]) for r in results]]),
    }
    header = (
        f"{'case':<{widths['id']}}  {'v1-class':<{widths['v1_class']}}  "
        f"{'v1-action':<{widths['v1_action']}}  {'v2-rule':<{widths['v2_rule']}}  "
        f"{'v2-action':<{widths['v2_action']}}  verdict"
    )
    lines = [header]
    for row in results:
        if row["agree"]:
            verdict = "AGREE"
        elif row["id"] in allowlisted_ids:
            verdict = "DISAGREE (allowlisted)"
        else:
            verdict = "DISAGREE (UNEXPECTED)"
        lines.append(
            f"{row['id']:<{widths['id']}}  {row['v1_class']:<{widths['v1_class']}}  "
            f"{row['v1_action']:<{widths['v1_action']}}  {row['v2_rule']:<{widths['v2_rule']}}  "
            f"{row['v2_action']:<{widths['v2_action']}}  {verdict}"
        )
    return "\n".join(lines) + "\n"


def load_json(path: Path, what: str):
    try:
        return json.loads(path.read_text())
    except OSError as exc:
        raise ValueError(f"cannot read {what} file {path}: {exc}") from exc
    except ValueError as exc:
        raise ValueError(f"{what} file {path} is not valid JSON: {exc}") from exc


def main(argv: list[str] | None = None) -> int:
    """Run the replay. Returns the process exit code (0 = clean)."""
    parser = argparse.ArgumentParser(description="Replay job rows through doctor v1 and v2.")
    parser.add_argument("--cases", default=str(DEFAULT_CASES))
    parser.add_argument("--allowlist", default=str(DEFAULT_ALLOWLIST))
    parser.add_argument("--rules", default=str(DEFAULT_RULES))
    parser.add_argument("--format", choices=("table", "json"), default="table")
    args = parser.parse_args(argv)
    try:
        raw_cases = load_json(Path(args.cases), "cases")
        allowlist = load_json(Path(args.allowlist), "allowlist")
        rules_doc = doctor_v2.load_rules(args.rules)
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"doctor_replay: {exc}", file=sys.stderr)
        return 2
    if isinstance(raw_cases, dict):
        cases = raw_cases.get("cases", [])
    elif isinstance(raw_cases, list):
        cases = raw_cases
    else:
        print("doctor_replay: cases file must hold a list or a {'cases': [...]} body", file=sys.stderr)
        return 2
    if not isinstance(cases, list) or not all(isinstance(c, dict) for c in cases):
        print("doctor_replay: every case must be an object", file=sys.stderr)
        return 2
    results = replay(cases, rules_doc)
    unexpected, stale = check_allowlist(results, allowlist if isinstance(allowlist, list) else [])
    allowlisted_ids = {r["id"] for r in results if not r["agree"]} - {r["id"] for r in unexpected}
    if args.format == "json":
        print(json.dumps({"results": results, "unexpected": unexpected, "stale_allowlist": stale}, indent=2))
    else:
        sys.stdout.write(format_table(results, allowlisted_ids))
    if unexpected or stale:
        for row in unexpected:
            print(
                f"doctor_replay: UNEXPECTED disagreement {row['id']}: "
                f"v1 {row['v1_class']}->{row['v1_action']}, v2 {row['v2_rule']}->{row['v2_action']}",
                file=sys.stderr,
            )
        for pin in stale:
            print(f"doctor_replay: stale allowlist entry for {pin.get('id')}: no such disagreement", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
