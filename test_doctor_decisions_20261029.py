"""FB24: the doctor decision table (docs/DOCTOR_DECISIONS.md) matches decide().

Parses the markdown table (every column) and calls decide() for every
reviewer-state / requeue-count / local-only cell; the verdict must equal
the documented cell. Also checks the cap column against MAX_AUTO_REQUEUES
and that every outcome class decide() knows is in the table.
"""

from __future__ import annotations

import inspect
import re
from pathlib import Path

import hive.workers.doctor as doctor
import hive.workers.loop_outcome as lo

DOC = Path(__file__).parent / "docs" / "DOCTOR_DECISIONS.md"

EXPECTED_COLS = [
    "outcome class",
    "requeues 0-1, reviewer up",
    "requeues 0-1, reviewer down",
    "requeues 2+",
    "local-only commits",
    "requeue cap",
    "cooldown",
    "operator sees",
]


def parse_table(text: str) -> tuple[list[str], list[list[str]]]:
    rows = [
        [c.strip() for c in line.strip().strip("|").split("|")]
        for line in text.splitlines()
        if line.startswith("| ")
    ]
    header, body = rows[0], [r for r in rows[1:] if not set("".join(r)) <= {"-", " "}]
    assert header == EXPECTED_COLS, f"table columns changed: {header}"
    assert all(len(r) == len(EXPECTED_COLS) for r in body), "ragged table row"
    return header, body


def test_table_matches_decide():
    header, body = parse_table(DOC.read_text())
    assert body, "decision table is empty"

    for row in body:
        cls, up, down, two, local, cap = row[0], row[1], row[2], row[3], row[4], row[5]

        def check(cls=cls, got=None, want=None, ctx=""):
            assert got == want, f"{cls} [{ctx}]: decide()={got!r}, doc says {want!r}"

        check(ctx="reviewer up", got=lo.decide(cls, 0, lambda: True), want=up)
        check(ctx="reviewer down", got=lo.decide(cls, 0, lambda: False), want=down)
        check(ctx="requeues 2+", got=lo.decide(cls, 2, lambda: True), want=two)
        if local != "--":
            check(
                ctx="local-only",
                got=lo.decide(cls, 0, lambda: True, local_commits_only=True),
                want=local,
            )
        if cap == "2":
            assert lo.MAX_AUTO_REQUEUES == 2
            check(ctx="cap", got=lo.decide(cls, 0, lambda: True), want="requeue")
            check(ctx="cap", got=lo.decide(cls, 2, lambda: True), want="needs_review")
        elif cap == "0":
            check(ctx="cap 0", got=lo.decide(cls, 0, lambda: True), want="needs_review")
        elif "0 when local-only" in cap:
            check(
                ctx="local cap",
                got=lo.decide(cls, 0, lambda: True, local_commits_only=True),
                want="needs_review",
            )


def test_every_known_class_is_documented():
    _, body = parse_table(DOC.read_text())
    documented = {r[0] for r in body}
    # Outcome classes are module-level UPPER string constants (a convention
    # this module follows 11/11: every decide() branch compares one).
    # A new class defined any other way (Enum, lowercase, another module)
    # will not be discovered here -- keep the convention, not the test.
    # (REVIEWER_UNAVAILABLE, AUTHOR_FAILED, ...); numeric UPPER
    # constants (MAX_AUTO_REQUEUES, *_TTL_SEC) are not classes.
    classes = {
        v
        for k, v in vars(lo).items()
        if k.isupper()
        and isinstance(v, str)
        and v == v.lower()
        and k[0].isalpha()  # skip _LOCAL_ONLY_STATUS (a commit flag, not a class)
        and not k.endswith("_REASON")  # skip EMPTY_DIFF_REASON (a human message)
    }
    sig_params = set(inspect.signature(lo.decide).parameters)
    assert "cls" in sig_params, "decide() signature changed; test needs updating"
    missing = {c for c in classes if c not in documented}
    assert not missing, f"undocumented outcome classes: {sorted(missing)}"
    assert documented <= classes | {"unknown"}, (
        f"table lists classes decide() does not know: {sorted(documented - classes - {'unknown'})}"
    )


def test_reviewer_health_only_matters_for_reviewer_unavailable():
    # decide() reads reviewer_ok() only in the REVIEWER_UNAVAILABLE branch;
    # pin that so a future branch that consults it breaks loudly.
    _, body = parse_table(DOC.read_text())
    for row in body:
        cls = row[0]
        if cls == "reviewer_unavailable":
            assert lo.decide(cls, 0, lambda: True) == "requeue"
            assert lo.decide(cls, 0, lambda: False) == "hold"
        else:
            for n in (0, 1):
                assert lo.decide(cls, n, lambda: True) == lo.decide(cls, n, lambda: False), cls


def test_killed_by_signal_local_only_never_requeues():
    for n in (0, 1, 2):
        assert lo.decide("killed_by_signal", n, lambda: True, local_commits_only=True) == "needs_review"


def test_cooldown_column_constants():
    # The cooldown column is documentary, but the numbers it cites are pinned
    # here so a constant change breaks the test instead of silently aging the doc.
    assert doctor.ORPHAN_RESUBMIT_TTL_SEC == 600
    assert doctor.LOOP_PARK_TTL_SEC == 30 * 24 * 3600
    assert doctor.LOOP_REQUEUE_TTL_SEC == 7 * 24 * 3600
    text = DOC.read_text()
    assert "600" in text and "30d" in text


def test_verdict_cells_are_known_verdicts():
    _, body = parse_table(DOC.read_text())
    allowed = {"requeue", "hold", "needs_review", "skip", "default", "--"}
    for row in body:
        for cell in row[1:5]:
            assert cell in allowed, f"{row[0]}: unknown verdict {cell!r}"
    assert re.search(r"MAX_AUTO_REQUEUES\s*=\s*2", (Path(__file__).parent / "hive" / "workers" / "loop_outcome.py").read_text())
