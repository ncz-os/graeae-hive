#!/usr/bin/env python3
"""Audit non-test Python for hardcoded fleet addresses (the fleet LAN range).

Addresses must come from the environment or the fleet inventory, not source:
a baked-in IP goes stale silently (ULTRA .60, MEDUSA .64 and PROTEUS .25 in this
tree are already wrong). The baseline file records the occurrences that exist
today; ``--check`` fails when a file gains one, so the count can only go down.

    scripts/audit_hardcoded_addresses.py            # list occurrences
    scripts/audit_hardcoded_addresses.py --check    # exit 1 if any file exceeds baseline
    scripts/audit_hardcoded_addresses.py --write-baseline
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip())
BASELINE = ROOT / "scripts" / "hardcoded_addresses_baseline.json"
PATTERN = re.compile(r"192\.168\.207\.\d+")


def scan() -> dict[str, int]:
    files = subprocess.check_output(["git", "-C", str(ROOT), "ls-files", "*.py"], text=True).split()
    counts: dict[str, int] = {}
    for rel in files:
        name = Path(rel).name
        if name.startswith("test_") or "/tests/" in "/" + rel or rel == "scripts/audit_hardcoded_addresses.py":
            continue
        try:
            n = len(PATTERN.findall((ROOT / rel).read_text(errors="ignore")))
        except OSError:
            continue
        if n:
            counts[rel] = n
    return counts


def main(argv: list[str]) -> int:
    counts = scan()
    if "--write-baseline" in argv:
        BASELINE.write_text(json.dumps(counts, indent=1, sort_keys=True) + "\n")
        print(f"baseline written: {sum(counts.values())} occurrences in {len(counts)} files")
        return 0
    if "--check" in argv:
        base = json.loads(BASELINE.read_text()) if BASELINE.exists() else {}
        grew = {f: (base.get(f, 0), n) for f, n in counts.items() if n > base.get(f, 0)}
        for f, (was, now) in sorted(grew.items()):
            print(f"FAIL {f}: {was} -> {now} hardcoded fleet addresses; read the address from the environment")
        shrunk = sum(base.values()) - sum(counts.values())
        if shrunk > 0:
            print(f"note: {shrunk} fewer than baseline; refresh with --write-baseline")
        return 1 if grew else 0
    for f, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"{n:4d} {f}")
    print(f"total {sum(counts.values())} in {len(counts)} files")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
