"""CHANGELOG Sources stay complete: every Merge freebuff/* and Merge
sonnet/* subject merged today on the integrate or worker line appears
under Sources. No commit hashes in the text.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

CHANGELOG = Path(__file__).parent / "CHANGELOG.md"
DAY = "2026-10-29"
LINES = ("origin/deploy/integrate-20261017", "origin/deploy/hive-canonical-20261029")


def today_merges() -> list[str]:
    subjects = []
    for line in LINES:
        out = subprocess.run(
            ["git", "log", "--merges", "--first-parent", "--format=%ci %s", line],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent,
            check=True,
        ).stdout
        for row in out.splitlines():
            if row.startswith(DAY) and re.search(r"Merge (freebuff|sonnet)/", row):
                subjects.append(row.split(" ", 3)[3])
    return sorted(set(subjects))


def test_every_today_merge_appears_under_sources():
    text = CHANGELOG.read_text()
    sources = text.split("## Sources", 1)[1]
    missing = [s for s in today_merges() if s not in sources]
    assert not missing, "merges missing from Sources: " + ", ".join(missing)


def test_no_commit_hashes_in_text():
    text = CHANGELOG.read_text()
    assert not re.search(r"\b[0-9a-f]{40}\b", text), "full commit hash in changelog text"
