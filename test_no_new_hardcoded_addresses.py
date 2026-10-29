"""New hardcoded fleet addresses are refused (operator 2026-10-29)."""

import subprocess
import sys
from pathlib import Path


def test_no_file_gains_a_hardcoded_fleet_address():
    script = Path(__file__).parent / "scripts" / "audit_hardcoded_addresses.py"
    run = subprocess.run([sys.executable, str(script), "--check"], capture_output=True, text=True)
    assert run.returncode == 0, run.stdout
