"""Registry contract test (2026-10-27).

docs/DETERMINISTIC-PATHS.md is the contract: every row that names a test must
name a test that EXISTS in the file set. This test parses the markdown table
and fails if a row references a missing test, so the registry cannot drift
from the suite.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent
REGISTRY = REPO_ROOT / "docs" / "DETERMINISTIC-PATHS.md"

# The file set a registry row may reference: every test file in the repo root
# plus the package test tree.
TEST_FILE_GLOBS = (
    "test_*.py",
    "tests/**/*.py",
)


def _test_functions() -> set[str]:
    """Every `def test_*` name defined in the file set."""
    names: set[str] = set()
    for pattern in TEST_FILE_GLOBS:
        for path in sorted(REPO_ROOT.glob(pattern)):
            if not path.is_file():
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            for match in re.finditer(r"^def (test_\w+)\(", text, re.MULTILINE):
                names.add(match.group(1))
    return names


def _registry_rows() -> list[tuple[str, str, str]]:
    """(reason, test_name, owner) for every table row that carries a test.

    Only the first table — the rows this branch owns — carries a test column;
    the second table (other in-flight jobs) has an Owner column instead and is
    skipped.
    """
    text = REGISTRY.read_text(encoding="utf-8")
    rows: list[tuple[str, str, str]] = []
    in_first_table = False
    for line in text.splitlines():
        if line.startswith("## "):
            in_first_table = False
            if line.startswith("## Rows this branch owns"):
                in_first_table = True
            continue
        if not in_first_table or not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) < 5:
            continue
        # Skip the header and the separator row.
        if cells[0] == "Reason (or prefix)" or set(cells[0]) <= {"-", ":", " "}:
            continue
        reason = cells[0].strip("`").strip()
        test_cell = cells[3]
        test_match = re.search(r"`(test_\w+)`", test_cell)
        if test_match:
            rows.append((reason, test_match.group(1), cells[4]))
    return rows


def test_registry_exists_and_has_rows():
    assert REGISTRY.is_file(), "docs/DETERMINISTIC-PATHS.md is missing — the contract file"
    rows = _registry_rows()
    assert rows, "the registry table has no rows"


def test_registry_row_names_an_existing_test():
    """A row that names a test which does not exist fails the suite."""
    existing = _test_functions()
    missing = []
    for reason, test_name, _owner in _registry_rows():
        if test_name not in existing:
            missing.append(f"{reason!r} -> {test_name}")
    assert not missing, "registry rows naming tests that do not exist: " + "; ".join(missing)


def test_every_reason_in_the_audit_has_a_row():
    """The audit's failure strings must each appear as a row (this branch's
    table or the other-jobs table) or be covered by a prefix row. The two
    known worker-side bugs are marked needs_operator with their owner."""
    text = REGISTRY.read_text(encoding="utf-8")
    audit_reasons = (
        # this branch
        "no_workspace_for_repo:",
        "no_workspace_for_kind:",
        "host_declines_kind",
        "released_by_host",
        "invalid_head_branch",
        "attribution",
        "dlq_protected",
        # other in-flight jobs
        "integrity",
        "dirty",
        "no-commits",
        "not_task_branch",
        "SIGHUP",
        "push_failed",
        "model_mismatch",
        "review-not-approved",
        "loop_unresolved",
        "codex_delivery_failed",
        "protection_violation",
        "no_repo_mapping",
        "malformed_job_id",
        # known worker-side bugs
        "unhandled_exception",
    )
    missing = [r for r in audit_reasons if r not in text]
    assert not missing, f"audit reasons missing from the registry: {missing}"
    # The two worker-side bugs must be marked needs_operator with an owner.
    assert "needs_operator" in text, "the worker-side bugs must be marked needs_operator"


def test_other_jobs_table_rows_carry_no_test():
    """Rows owned by other in-flight jobs must not claim a test in this file
    set: they are listed so the registry is complete, but the test lives in
    the owning branch."""
    text = REGISTRY.read_text(encoding="utf-8")
    in_other = False
    for line in text.splitlines():
        if line.startswith("## "):
            in_other = line.startswith("## Rows owned by other in-flight jobs")
            continue
        if in_other and line.startswith("|") and "`test_" in line:
            cells = [c.strip() for c in line.strip("|").split("|")]
            if len(cells) >= 3 and cells[0] != "Reason (or prefix)" and set(cells[0]) > {"-", ":", " "}:
                pytest.fail(f"other-jobs row claims a test: {cells[0]!r}")
