"""FB30: audit script tests with temp fixtures.

Builds a miniature repo tree under tmp_path and asserts scripts/audit_doc_links
finds exactly the planted problems and passes a clean tree.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent


def _load_audit():
    spec = importlib.util.spec_from_file_location("audit_doc_links", ROOT / "scripts" / "audit_doc_links.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


audit_mod = _load_audit()


def make_tree(tmp_path: Path, *, doc_extra: str = "", allow: str = "") -> Path:
    (tmp_path / "docs").mkdir()
    (tmp_path / "scripts").mkdir()
    (tmp_path / "hive").mkdir()
    (tmp_path / "docs" / "environment-requirements.json").write_text(
        json.dumps({"variables": [{"name": "FIXTURE_TOKEN", "kind": "secret", "required": False, "meaning": "x"}]})
    )
    (tmp_path / "scripts" / "real_script.py").write_text("# real\n")
    (tmp_path / "hive" / "mod.py").write_text('import os\nX = os.environ.get("FIXTURE_TOKEN")\nY = os.environ.get("MISSING_TOKEN", "dflt")\n')
    (tmp_path / "docs" / "a.md").write_text(
        "# A\n\n"
        "[real](a.md)\n\n"
        "[gone](../nope.md)\n\n"
        "`scripts/real_script.py` and `scripts/gone.py`.\n\n"
        "`FIXTURE_TOKEN`, `MISSING_TOKEN`, `NOT_A_REAL_THING`.\n\n"
        + doc_extra
    )
    if allow:
        (tmp_path / "scripts" / "audit_doc_links.allowlist").write_text(allow)
    return tmp_path


def test_finds_broken_link_missing_path_and_undeclared_env(tmp_path):
    root = make_tree(tmp_path)
    findings = audit_mod.audit(root)
    assert any("nope.md" in f and "broken link" in f for f in findings), findings
    assert any("scripts/gone.py" in f and "missing path" in f for f in findings), findings
    assert any("MISSING_TOKEN" in f and "undeclared env" in f for f in findings), findings
    # Declared vars and code constants are not findings.
    assert not any("FIXTURE_TOKEN" in f for f in findings), findings
    assert not any("NOT_A_REAL_THING" in f for f in findings), findings


def test_clean_tree_passes(tmp_path):
    root = make_tree(tmp_path)
    (root / "docs" / "a.md").write_text("# A\n\n[real](a.md)\n\n`scripts/real_script.py`.\n\n`FIXTURE_TOKEN`.\n")
    assert audit_mod.audit(root) == []


def test_allowlist_covers_external_names(tmp_path):
    root = make_tree(tmp_path, allow="# other system's var\nMISSING_TOKEN\n")
    findings = audit_mod.audit(root)
    assert not any("MISSING_TOKEN" in f for f in findings), findings
    assert any("gone" in f for f in findings), findings  # other findings remain


def test_glob_mentions_checked_against_matches(tmp_path):
    root = make_tree(tmp_path, doc_extra="`docs/*.md` and `docs/*.nothing`.\n")
    findings = audit_mod.audit(root)
    assert not any("docs/*.md" in f for f in findings), findings
    assert any("docs/*.nothing" in f and "glob matches nothing" in f for f in findings), findings
