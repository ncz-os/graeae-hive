"""FB46: audit_env_usage tests with temp fixtures.

Builds miniature repo trees under tmp_path and asserts scripts/audit_env_usage
finds exactly the planted problems in both directions, and passes a clean
tree. Also checks the literal-read forms, the string-constant indirection,
and the known false positive in hive/workers/merge_train.py's docstring.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SCRIPT = ROOT / "scripts" / "audit_env_usage.py"


def _load_audit():
    spec = importlib.util.spec_from_file_location("audit_env_usage", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


audit_mod = _load_audit()


def make_tree(
    tmp_path: Path,
    *,
    declared: list[str] = ("DECLARED_ONE",),
    hive_src: str | None = None,
    deploy_src: str | None = None,
) -> Path:
    if hive_src is None:
        hive_src = f'import os\nA = os.environ.get("{declared[0]}")\n'
    if deploy_src is None:
        deploy_src = f'import os\nB = os.environ.get("{declared[0]}")\n'
    (tmp_path / "docs").mkdir()
    (tmp_path / "hive").mkdir()
    (tmp_path / "deploy").mkdir()
    (tmp_path / "docs" / "environment-requirements.json").write_text(
        json.dumps({"variables": [{"name": n, "kind": "text", "required": False, "meaning": "x"} for n in declared]})
    )
    (tmp_path / "hive" / "mod.py").write_text(hive_src)
    if deploy_src is not None:
        (tmp_path / "deploy" / "worker.py").write_text(deploy_src)
    return tmp_path


def test_clean_tree_passes(tmp_path):
    root = make_tree(tmp_path)
    assert audit_mod.audit(root) == []


def test_undocumented_read_is_flagged(tmp_path):
    root = make_tree(
        tmp_path,
        hive_src='import os\nA = os.environ.get("DECLARED_ONE")\nB = os.environ.get("SECRET_MISSING", "")\n',
    )
    findings = audit_mod.audit(root)
    assert any(f.startswith("undocumented: SECRET_MISSING") for f in findings), findings
    assert not any("DECLARED_ONE" in f for f in findings), findings
    # The finding carries the site (path:line) so a reader can check it.
    assert any("hive/mod.py:" in f for f in findings if f.startswith("undocumented: SECRET_MISSING")), findings


def test_unused_declaration_is_flagged(tmp_path):
    root = make_tree(tmp_path, declared=("READ_NAME", "NEVER_READ"))
    findings = audit_mod.audit(root)
    assert any(f.startswith("unused: NEVER_READ") for f in findings), findings
    assert not any("READ_NAME" in f for f in findings), findings


def test_both_directions_report_independently(tmp_path):
    root = make_tree(
        tmp_path,
        declared=("ALPHA", "GHOST"),
        hive_src='import os\nA = os.environ.get("ALPHA")\nB = os.environ.get("BETTER", "0")\n',
        deploy_src=None,
    )
    findings = audit_mod.audit(root)
    assert any(f.startswith("undocumented: BETTER") for f in findings), findings
    assert any(f.startswith("unused: GHOST") for f in findings), findings


def test_all_three_literal_forms_are_found(tmp_path):
    root = make_tree(
        tmp_path,
        hive_src=(
            "import os\n"
            "A = os.environ.get('FORM_GET', 'd')\n"
            "B = os.getenv('FORM_GETENV')\n"
            "C = os.environ['FORM_SUBSCRIPT']\n"
        ),
        deploy_src=None,
    )
    findings = audit_mod.audit(root)
    for name in ("FORM_GET", "FORM_GETENV", "FORM_SUBSCRIPT"):
        assert any(f.startswith(f"undocumented: {name}") for f in findings), findings


def test_string_constant_indirection_counts_as_a_read(tmp_path):
    # The fleet-inventory shape: the name is assigned to a constant and
    # read through it; the literal on the assignment line counts as a read
    # of that name, so a declaration for it is not flagged unused.
    root = make_tree(
        tmp_path,
        declared=("DECLARED_ONE", "INDIRECT_NAME"),
        hive_src=(
            "import os\n"
            'INVENTORY_ENV = "INDIRECT_NAME"\n'
            "def path(env=None):\n"
            "    source = os.environ if env is None else env\n"
            "    return source.get(INVENTORY_ENV)\n"
        ),
    )
    findings = audit_mod.audit(root)
    assert not any("INDIRECT_NAME" in f for f in findings), findings
    # And the indirection must actually be what satisfies the declaration:
    # drop the assignment and the name becomes a genuine unused finding.
    (root / "hive" / "mod.py").write_text('import os\nA = os.environ.get("DECLARED_ONE")\n')
    findings = audit_mod.audit(root)
    assert any(f.startswith("unused: INDIRECT_NAME") for f in findings), findings


def test_reads_outside_hive_and_deploy_are_invisible(tmp_path):
    # A read in scripts/ or at the repo root is not in scope: it is neither
    # a finding nor a satisfaction of a declaration.
    root = make_tree(tmp_path, deploy_src=None, declared=("DECLARED_ONE", "OUT_OF_TREE"))
    (root / "scripts").mkdir()
    (root / "scripts" / "tool.py").write_text('import os\nZ = os.environ.get("OUT_OF_TREE")\n')
    (root / "top.py").write_text('import os\nZ = os.environ.get("TOP_LEVEL_ONLY")\n')
    findings = audit_mod.audit(root)
    assert not any("TOP_LEVEL_ONLY" in f for f in findings), findings
    assert any(f.startswith("unused: OUT_OF_TREE") for f in findings), findings


def test_absent_tree_is_not_an_error(tmp_path):
    root = make_tree(tmp_path)
    for p in sorted((root / "deploy").glob("*.py")):
        p.unlink()
    (root / "deploy").rmdir()
    findings = audit_mod.audit(root)
    assert findings == [], findings


def test_subprocess_exit_codes(tmp_path):
    clean = make_tree(tmp_path)
    proc = subprocess.run([sys.executable, str(SCRIPT), "--root", str(clean)], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr

    (tmp_path / "dirty").mkdir()
    dirty = make_tree(
        tmp_path / "dirty", hive_src='import os\nA = os.environ.get("DECLARED_ONE")\nB = os.environ["MISREAD"]\n'
    )
    proc = subprocess.run([sys.executable, str(SCRIPT), "--root", str(dirty)], capture_output=True, text=True)
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "undocumented: MISREAD" in proc.stdout


def test_real_repo_docstring_gltoq_is_a_known_false_positive():
    """The audit is literal: the merge_train docstring quotes os.environ["GLTOK"]
    (a description of a removed defect) and the audit cannot tell that apart
    from a read. Pin the behaviour so a future change to either the docstring
    or the scanner is a deliberate decision, not a silent drift."""
    root = ROOT
    findings = audit_mod.audit(root)
    gltoq = [f for f in findings if f.startswith("undocumented: GLTOK")]
    assert len(gltoq) == 1, findings
    assert "hive/workers/merge_train.py" in gltoq[0], gltoq
    # And it must NOT leak into the requirements file: the docstring quote is
    # not a variable the tree reads.
    declared = audit_mod.load_declared(root)
    assert "GLTOK" not in declared


def test_real_repo_report_is_stable():
    """The real-repo audit returns a deterministic, sorted list of findings.

    Run before the JSON fix this lists every undocumented read plus the
    unused declarations; run after it, the only finding is the GLTOK
    docstring false positive. The test asserts shape only, so it holds in
    both states and catches nondeterminism.
    """
    findings = audit_mod.audit(ROOT)
    assert findings == sorted(set(findings))
    for f in findings:
        assert f.startswith("undocumented: ") or f.startswith("unused: "), f
