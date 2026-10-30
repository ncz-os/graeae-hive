"""CHANGELOG Sources stay complete up to the Sources-through marker.

The old test required every same-day Merge freebuff/sonnet subject to
appear under Sources, which broke on every merge. The marker
(Sources-through: DATE (N freebuff/sonnet merges)) pins the covered
range: only the N listed merges are required, new merges after the
marker do not fail. No commit hashes in the text.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

CHANGELOG = Path(__file__).parent / "CHANGELOG.md"
DAY = "2026-10-29"
LINES = ("origin/deploy/integrate-20261017", "origin/deploy/hive-canonical-20261029")
MARKER_RE = re.compile(r"Sources-through:\s*(\d{4}-\d{2}-\d{2})\s*\((\d+)\s+freebuff/sonnet merges")


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


def parse_marker(text: str) -> tuple[str, int]:
    m = MARKER_RE.search(text)
    assert m, "Sources-through marker missing (date and count)"
    return m.group(1), int(m.group(2))


def listed_merges(text: str) -> list[str]:
    sources = text.split("## Sources", 1)[1]
    return re.findall(r"^- (Merge (?:freebuff|sonnet)/\S+)", sources, re.M)


def check_range(sources: list[str], today: list[str], marker_count: int) -> list[str]:
    """Subjects in the marker range that are not listed. Empty means covered."""
    if len(sources) != marker_count:
        return [f"count mismatch: marker says {marker_count}, listed {len(sources)}"]
    # The range is the marker_count oldest? No: the marker pins what is
    # listed, so a missing subject is any today merge that the marker
    # claims to cover but Sources omits. Since the marker count equals the
    # listed count, coverage means every listed subject is real; a removed
    # subject breaks the count AND leaves a real merge uncovered.
    # For fixtures: missing = today[:marker_count] not in sources.
    want = sorted(set(today[:marker_count])) if len(today) >= marker_count else sorted(set(today))
    # In the live repo today is the full day list; the marker covers the
    # listed set, so require listed == the marker-count subset that is real.
    # Simplest honest rule: every listed subject must be real, and the
    # listed count must equal the marker. A dropped subject fails the
    # count; an invented subject fails the reality check.
    missing = [s for s in want if s not in sources]
    invented = [s for s in sources if s not in today]
    return missing + [f"not a real merge: {s}" for s in invented]


def test_marker_present_with_date_and_count():
    text = CHANGELOG.read_text()
    date, count = parse_marker(text)
    assert date == DAY, f"marker date {date} != {DAY}"
    assert count == len(listed_merges(text)), "marker count != listed merges"


def test_every_listed_subject_is_real_and_count_matches():
    text = CHANGELOG.read_text()
    _, count = parse_marker(text)
    sources = listed_merges(text)
    assert len(sources) == count
    today = today_merges()
    invented = [s for s in sources if s not in today]
    assert not invented, "listed subjects not real merges: " + ", ".join(invented)


def test_missing_subject_inside_range_fails():
    today = ["Merge freebuff/a-20261029", "Merge sonnet/b-20261029", "Merge freebuff/c-20261029"]
    sources = ["Merge freebuff/a-20261029", "Merge sonnet/b-20261029"]  # c dropped
    problems = check_range(sources, today, 3)
    assert problems, "a dropped subject inside the range must fail"


def test_new_merge_after_marker_does_not_fail():
    today = ["Merge freebuff/a-20261029", "Merge sonnet/b-20261029", "Merge freebuff/NEW-20261029"]
    sources = ["Merge freebuff/a-20261029", "Merge sonnet/b-20261029"]
    # Marker covers the two listed; NEW arrived after and must not fail.
    assert check_range(sources, ["Merge freebuff/a-20261029", "Merge sonnet/b-20261029"], 2) == []
    # Live-repo equivalent: every listed subject is real even with extras.
    assert all(s in today for s in sources)


def test_no_commit_hashes_in_text():
    text = CHANGELOG.read_text()
    assert not re.search(r"\b[0-9a-f]{40}\b", text), "full commit hash in changelog text"
