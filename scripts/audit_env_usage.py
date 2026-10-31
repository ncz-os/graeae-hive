"""FB46: audit which environment variables hive/ and deploy/ actually read.

Extends FB30's doc audit in the opposite direction: instead of asking
"which env names in the docs does the code read", this asks, for every
literal ``os.environ.get("NAME" ...)`` / ``os.getenv("NAME" ...)`` /
``os.environ["NAME"]`` read in ``hive/`` and ``deploy/``, whether
``docs/environment-requirements.json`` declares it — and, going the other
way, whether every declared variable is read anywhere in those two trees.

    python3 scripts/audit_env_usage.py [--root DIR]

Exit codes:

    0  both directions clean: every read variable is declared, every
       declared variable is read.
    1  at least one finding (an undocumented read or an unused declaration).

Importable: ``audit(root)`` returns a sorted list of finding strings, one
per problem, e.g. ``"undocumented: FOO_BAR read at deploy/hive_worker.py:42"``
or ``"unused: BAR_BAZ declared in docs/environment-requirements.json but read
nowhere in hive/ or deploy/"``.

Deliberate design decisions (the tests pin these):

* **Literal reads only.** The three syntactic forms above are matched with
  regular expressions over each ``.py`` file. Reads whose name comes from a
  variable (``os.environ.get(name)``) are not visible to a static scan and
  are out of scope by design: in this tree those helper names are few and
  documented inline where they resolve (``hive/workers/doctor.py``
  ``_env_seconds``), and a dynamic-resolution scan would be a parser, not
  an audit.

* **Fleet-inventory indirection is allowed.** The code does not read
  ``HIVE_FLEET_INVENTORY`` inline at the site; it assigns the name to a
  constant (``hive/core/fleet_inventory.py``: ``INVENTORY_ENV =
  "HIVE_FLEET_INVENTORY"``) and reads through it. An UPPER_SNAKE string
  literal that is *assigned* in the scanned trees counts as a read of that
  name — it is the same information, one hop later, and it keeps the
  audit honest about which variable the code actually resolves.

* **The docstring of ``hive/workers/merge_train.py`` quotes
  ``os.environ["GLTOK"]``** as a description of a removed defect. The
  audit is literal and does not parse docstrings: GLTOK is therefore an
  expected false positive, reported like any other undocumented read. The
  JSON fix keeps GLTOK out of the requirements file; the finding remains
  on the audit's report and is the one accepted exception (see
  ``test_real_repo_docstring_gltoq_is_a_known_false_positive`` in
  ``test_audit_env_usage_20261031.py``).

* **``hive/`` is scanned recursively, ``deploy/`` non-recursively**
  (``deploy/hive_worker.py`` plus the two ``.env.example`` files carry no
  Python reads; the systemd units and arm-orchestrator notes are not
  Python). Both trees are optional: a tree that is absent contributes no
  reads (a fixture may have only one of them).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

#: The three literal-read forms. ``os.environ.get("NAME"``,
#: ``os.getenv("NAME")`` and ``os.environ["NAME"]`` — with or without a
#: default argument, single or double quoted.
READ_RE = re.compile(
    r"os\.environ\.get\(\s*['\"]([A-Z][A-Z0-9_]+)['\"]"
    r"|os\.getenv\(\s*['\"]([A-Z][A-Z0-9_]+)['\"]"
    r"|os\.environ\[\s*['\"]([A-Z][A-Z0-9_]+)['\"]"
)

#: An UPPER_SNAKE literal assigned to a name: ``FOO_ENV = "FOO_BAR"``,
#: ``X = ("FOO_BAR",)`` etc. This is the fleet-inventory indirection: the
#: name is declared once as a string constant and read through it.
ASSIGN_RE = re.compile(
    r"^\s*(?:[A-Za-z_][A-Za-z0-9_]*\s*=\s*(?:\(?\s*)?|return\s+(?:\(?\s*)?)"
    r"['\"]([A-Z][A-Z0-9_]+)['\"]"
)


def scan_reads(py_file: Path) -> dict[str, list[tuple[str, int]]]:
    """Map env name -> list of (relative-ish path, line number) literal reads.

    A name reached through an assigned string constant in the same file is
    recorded at the constant's line (that is where the name is introduced).
    """
    reads: dict[str, list[tuple[str, int]]] = {}
    text = py_file.read_text(encoding="utf-8", errors="replace")
    for lineno, line in enumerate(text.splitlines(), 1):
        for m in READ_RE.finditer(line):
            name = next(g for g in m.groups() if g)
            reads.setdefault(name, []).append((str(py_file), lineno))
        for m in ASSIGN_RE.finditer(line):
            name = m.group(1)
            reads.setdefault(name, []).append((str(py_file), lineno))
    return reads


def collect_reads(root: Path) -> dict[str, list[tuple[str, int]]]:
    """All literal env reads in ``hive/`` (recursive) and ``deploy/`` (top level)."""
    reads: dict[str, list[tuple[str, int]]] = {}
    trees: list[tuple[Path, bool]] = [(root / "hive", True), (root / "deploy", False)]
    for tree, recursive in trees:
        if not tree.is_dir():
            continue
        files = sorted(tree.rglob("*.py")) if recursive else sorted(tree.glob("*.py"))
        for py in files:
            try:
                file_reads = scan_reads(py)
            except OSError:
                continue
            for name, sites in file_reads.items():
                reads.setdefault(name, []).extend(sites)
    return reads


def load_declared(root: Path) -> set[str]:
    """Variable names declared in docs/environment-requirements.json."""
    data = json.loads((root / "docs" / "environment-requirements.json").read_text())
    return {v["name"] for v in data["variables"]}


def _rel(root: Path, site: str) -> str:
    try:
        return str(Path(site).relative_to(root))
    except ValueError:
        return site


def audit(root: Path) -> list[str]:
    """Both directions. Returns a sorted list of finding strings."""
    root = root.resolve()
    reads = collect_reads(root)
    declared = load_declared(root)
    findings: list[str] = []
    for name in sorted(reads):
        if name in declared:
            continue
        sites = sorted(reads[name], key=lambda s: (_rel(root, s[0]), s[1]))
        first = sites[0]
        findings.append(f"undocumented: {name} read at {_rel(root, first[0])}:{first[1]}")
    for name in sorted(declared):
        if name not in reads:
            findings.append(
                f"unused: {name} declared in docs/environment-requirements.json but read nowhere in hive/ or deploy/"
            )
    return findings


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Audit env var reads in hive/ and deploy/ against docs/environment-requirements.json, both directions."
    )
    ap.add_argument("--root", default=".", help="repo root to audit (default: current directory)")
    args = ap.parse_args(argv)
    findings = audit(Path(args.root))
    if findings:
        print(f"{len(findings)} env-usage finding(s):")
        for f in findings:
            print(f"  - {f}")
        return 1
    print("env usage OK: every read variable is declared, every declared variable is read")
    return 0


if __name__ == "__main__":
    sys.exit(main())
