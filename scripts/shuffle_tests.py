#!/usr/bin/env python3
"""Hunt for tests whose result depends on what ran before them.

The repo's top-level ``test_*.py`` files are run in N random orders. Each file
gets its own pytest subprocess, so one file can only influence the next through
what it left behind in the working tree (a temp DB, a generated file, a socket, a
state file) -- which is exactly the leak this hunt looks for. Not a pytest
plugin: a hung file is killed by a per-file timeout, the way ``timeout`` does it.

    scripts/shuffle_tests.py --seed 1                 # 3 orders (default)
    scripts/shuffle_tests.py --seed 2 --orders 5
    scripts/shuffle_tests.py --seed 3 --files test_a.py test_b.py   # one order

The seed is printed with every run, every file's pass/fail/timeout is recorded
per order, and the files whose result differs between orders are printed at the
end with their position in each order and the file that ran immediately before
them (the first half of a minimal pair).

Run it with the interpreter that has the suite's dependencies (``sys.executable``
is what each file is run with); otherwise every file fails the same way and the
hunt can only report environment noise.

Files that collect no tests (pytest exit 5) are recorded as EMPTY, not failed:
``test_queue_logic.py`` and ``test_scheduler.py`` are plain-script checks run
with ``python3 file.py`` by design.

Exit codes: 0 when the run completes (differences are findings, not errors),
1 on a usage or environment problem, 2 when a file could not be started at all.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ORDERS = 3
DEFAULT_TIMEOUT = 300.0

PASS = "pass"
FAIL = "fail"
TIMEOUT = "timeout"
EMPTY = "empty"  # pytest exit 5: the file collects no tests under this config


def top_level_tests(root: Path = ROOT) -> list[str]:
    """Tracked top-level ``test_*.py`` files, sorted. ``tests/`` is not included."""
    out = subprocess.check_output(
        ["git", "-C", str(root), "ls-files", "test_*.py"], text=True
    )
    return sorted(p for p in out.split() if "/" not in p)


def make_orders(files: list[str], seed: int, orders: int) -> list[list[str]]:
    """``orders`` independent shuffles of ``files``, deterministic for ``seed``."""
    rng = random.Random(seed)
    return [rng.sample(files, len(files)) for _ in range(orders)]


def pytest_cmd(file: str, python: str | None = None) -> list[str]:
    """One file, one process. ``-p no:cacheprovider`` so runs cannot lean on a cache."""
    return [python or sys.executable, "-m", "pytest", "-p", "no:cacheprovider", file]


def run_cmd(cmd: list[str], timeout: float, cwd: Path | None = None) -> tuple[str, str]:
    """Run ``cmd``; return ``(status, output)`` with status pass/fail/timeout/empty.

    Exit 5 is pytest's "no tests collected": some top-level ``test_*.py`` files
    are plain-script checks (run with ``python3 file.py``) by design, so they
    are EMPTY, not failures. The child gets its own session so a timeout kills
    the whole process group: a pytest that spawned workers must not survive its
    own timeout and keep the next file from starting.
    """
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    try:
        out, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            proc.kill()
        out, _ = proc.communicate()
        return TIMEOUT, (out or "") + f"\n[timeout after {timeout:g}s: {' '.join(cmd)}]"
    if proc.returncode == 5:
        return EMPTY, out or ""
    status = PASS if proc.returncode == 0 else FAIL
    return status, out or ""


def run_order(
    files: list[str],
    runner,
    on_result=None,
) -> dict[str, str]:
    """Run ``files`` in sequence through ``runner(file) -> status``.

    ``runner`` returns ``(status, output)`` or just ``status``; the order of
    ``files`` is preserved, because position is what the hunt is about.
    """
    results: dict[str, str] = {}
    for index, file in enumerate(files, start=1):
        raw = runner(file)
        status, output = raw if isinstance(raw, tuple) else (raw, "")
        results[file] = status
        if on_result is not None:
            on_result(index, len(files), file, status, output)
    return results


def summarize(
    files: list[str], per_order: list[dict[str, str]]
) -> dict[str, list[str]]:
    """``file -> [status in each order]`` for every file, in the discovery order."""
    return {f: [order.get(f, "missing") for order in per_order] for f in files}


def differing(statuses: dict[str, list[str]]) -> dict[str, list[str]]:
    """The files whose status is not the same in every order."""
    return {f: s for f, s in statuses.items() if len(set(s)) > 1}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seed", type=int, default=0, help="shuffle seed (printed)")
    parser.add_argument("--orders", type=int, default=DEFAULT_ORDERS, help="how many random orders (default 3)")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="per-file timeout in seconds (default 300)")
    parser.add_argument("--root", type=Path, default=ROOT, help="repo root to run in")
    parser.add_argument("--files", nargs="+", help="run only these files (for minimising a pair)")
    parser.add_argument("--json", type=Path, help="also write the results as JSON")
    parser.add_argument("--verbose", action="store_true", help="print every file, not just the ones that misbehave")
    args = parser.parse_args(argv)

    root = args.root.resolve()
    files = args.files or top_level_tests(root)
    if not files:
        print("no test files found", file=sys.stderr)
        return 1

    orders = make_orders(files, args.seed, args.orders)
    print(
        f"seed={args.seed} orders={args.orders} files={len(files)} "
        f"timeout={args.timeout:g}s python={sys.executable} root={root}",
        flush=True,
    )

    per_order: list[dict[str, str]] = []
    for order_index, order in enumerate(orders, start=1):
        started = time.monotonic()

        def on_result(index, total, file, status, output, _i=order_index, _t=started):
            if status in (PASS, EMPTY) and not args.verbose:
                return
            tail = "\n".join(output.strip().splitlines()[-8:])
            print(f"  [{_i}] {index}/{total} {file}: {status.upper()}", flush=True)
            if tail and status != PASS:
                print("    " + tail.replace("\n", "\n    "), flush=True)

        results = run_order(order, lambda f: run_cmd(pytest_cmd(f), args.timeout, root), on_result)
        per_order.append(results)
        bad = {f: s for f, s in results.items() if s not in (PASS, EMPTY)}
        print(
            f"order {order_index}/{args.orders}: {len(results)} files, "
            f"{len(results) - len(bad)} ok, {len(bad)} not ok, "
            f"{time.monotonic() - started:.0f}s",
            flush=True,
        )

    statuses = summarize(files, per_order)
    print("\nfiles whose result differs between orders:")
    diff = differing(statuses)
    for file, seen in diff.items():
        where = ", ".join(
            f"order {i + 1}={s} (pos {order.index(file) + 1 if file in order else '?'})"
            for i, (s, order) in enumerate(zip(seen, orders, strict=True))
        )
        print(f"  DIFF {file}: {where}")
        for seen_order, order in zip(seen, orders, strict=True):
            if file in order:
                pos = order.index(file)
                before = order[pos - 1] if pos else "(first)"
                print(f"    order after {before} -> {seen_order}")
    if not diff:
        print("  (none: every file had the same result in every order)")

    if args.json:
        args.json.write_text(
            json.dumps(
                {
                    "seed": args.seed,
                    "orders": args.orders,
                    "timeout": args.timeout,
                    "results": per_order,
                    "differing": diff,
                },
                indent=1,
                sort_keys=True,
            )
            + "\n"
        )
        print(f"wrote {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
