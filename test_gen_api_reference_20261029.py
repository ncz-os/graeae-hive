"""Tests for scripts/gen_api_reference.py (no network, no secrets)."""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from scripts.gen_api_reference import generate, main  # noqa: E402


def test_generation_is_deterministic():
    assert generate() == generate()


def test_every_route_in_app_appears_in_doc():
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import agent_bus

    spec = agent_bus.app.openapi()
    text = generate()
    missing = []
    for path, methods in spec["paths"].items():
        for method in methods:
            if method.lower() == "head":
                continue
            if f"`{method.upper()} {path}`" not in text:
                missing.append(f"{method.upper()} {path}")
    assert not missing, f"routes missing from doc: {missing}"


def test_check_fires_on_drift(tmp_path):
    out = tmp_path / "API_REFERENCE.md"
    assert main(["--out", str(out)]) == 0
    assert main(["--check", "--out", str(out)]) == 0
    out.write_text(out.read_text() + "\n<!-- drift -->\n")
    assert main(["--check", "--out", str(out)]) == 1


def test_no_addresses_or_tokens_in_doc():
    text = generate()
    assert not re.search(r"192\.168\.\d+\.\d+", text)
    assert not re.search(r"10\.\d+\.\d+\.\d+", text)
    assert "<bus-host>" in text  # placeholder host used in examples
    assert "VALID_AGENT_STATUSES" in text
    assert "VALID_JOB_STATUSES" in text
