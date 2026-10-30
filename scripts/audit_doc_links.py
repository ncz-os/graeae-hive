"""FB30: audit relative links, path mentions, and env names in Hive docs.

Checks every docs/*.md and README.md:
  * markdown links [text](target): relative targets must resolve (against the
    doc's own directory); http(s)/mailto links and pure #anchors are skipped;
    a file#anchor target only requires the file.
  * bare path mentions: `scripts/...`, `docs/...`, `hive/...` tokens must
    exist under the repo root.
  * env names: backticked UPPER_SNAKE tokens (with an underscore) that code
    actually reads via os.environ/os.getenv must be declared in
    docs/environment-requirements.json, unless allowlisted as external
    (scripts/audit_doc_links.allowlist). Other backticked constants are code,
    not environment, and are ignored.

Exit 0 when clean, 1 with a finding list otherwise. Importable: audit(root)
returns a list of finding strings.

Run: python3 scripts/audit_doc_links.py [--root DIR]
Test: python3 -m pytest test_audit_doc_links_20261029.py
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

LINK_RE = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
PATH_RE = re.compile(r"`((?:scripts|docs|hive)/[\w\-./*]+)`?")
ENV_RE = re.compile(r"`([A-Z][A-Z0-9_]*_[A-Z0-9_]*)`")
SCHEMES = ("http://", "https://", "mailto:")
CODE_ENV_RE = re.compile(
    r"os\.environ(?:\.get)?\(\s*['\"]([A-Z][A-Z0-9_]+)['\"]"
    r"|os\.environ\[\s*['\"]([A-Z][A-Z0-9_]+)['\"]"
    r"|os\.getenv\(\s*['\"]([A-Z][A-Z0-9_]+)['\"]"
)


def load_declared_env(root: Path) -> set[str]:
    data = json.loads((root / "docs" / "environment-requirements.json").read_text())
    return {v["name"] for v in data["variables"]}


def load_allowlist(root: Path) -> set[str]:
    p = root / "scripts" / "audit_doc_links.allowlist"
    if not p.exists():
        return set()
    return {line.strip() for line in p.read_text().splitlines() if line.strip() and not line.startswith("#")}


def check_link(doc: Path, root: Path, target: str) -> str | None:
    if target.startswith(SCHEMES) or target.startswith("#"):
        return None
    path = target.split("#")[0].split("?")[0].strip()
    if not path:
        return None
    resolved = (doc.parent / path).resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError:
        return f"{doc.name}: link escapes repo: {target}"
    if not resolved.exists():
        return f"{doc.name}: broken link: {target}"
    return None


def collect_code_env(root: Path) -> set[str]:
    """Env names actually read in code (os.environ.get/[key], os.getenv).

    A backticked UPPER_SNAKE token in docs is only an env candidate when code
    reads it; otherwise it is a code constant (VALID_JOB_STATUSES, ...) and
    must not be required in the requirements file.
    """
    names: set[str] = set()
    for py in list((root / "hive").rglob("*.py")) + list((root / "scripts").glob("*.py")) + list(root.glob("*.py")):
        try:
            text = py.read_text()
        except OSError:
            continue
        for m in CODE_ENV_RE.finditer(text):
            names.add(next(g for g in m.groups() if g))
    return names


def check_path_mention(doc: Path, root: Path, mention: str) -> str | None:
    clean = mention.rstrip(".,;:)")
    if "*" in clean:
        hits = sorted(str(x.relative_to(root)) for x in root.glob(clean))
        if not hits:
            return f"{doc.name}: glob matches nothing: {clean}"
        return None
    resolved = (root / clean).resolve()
    if not str(resolved).startswith(str(root.resolve())):
        return f"{doc.name}: path mention escapes repo: {mention}"
    if not resolved.exists():
        return f"{doc.name}: missing path: {clean}"
    return None


def audit(root: Path) -> list[str]:
    root = root.resolve()
    findings: list[str] = []
    declared = load_declared_env(root)
    allowed = load_allowlist(root)
    code_env = collect_code_env(root)
    docs = sorted((root / "docs").glob("*.md"))
    if (root / "README.md").exists():
        docs.append(root / "README.md")
    for doc in docs:
        text = doc.read_text()
        for target in LINK_RE.findall(text):
            problem = check_link(doc, root, target)
            if problem:
                findings.append(problem)
        for mention in PATH_RE.findall(text):
            if mention in allowed:
                continue  # cross-tree or planned path, justified in the allowlist
            problem = check_path_mention(doc, root, mention)
            if problem:
                findings.append(problem)
        for name in set(ENV_RE.findall(text)):
            if name not in code_env:
                continue  # a code constant, not an env var
            if name not in declared and name not in allowed:
                findings.append(f"{doc.name}: undeclared env var: {name}")
    return sorted(set(findings))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="repo root to audit")
    args = ap.parse_args(argv)
    findings = audit(Path(args.root))
    if findings:
        print(f"{len(findings)} doc-link finding(s):")
        for f in findings:
            print(f"  - {f}")
        return 1
    print("doc links OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
