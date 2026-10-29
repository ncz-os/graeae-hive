"""Tests for scripts/render_control_env.py (no network, TEST-NET fixtures)."""

import json
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
RENDER = HERE / "render_control_env.py"

# Assembled at runtime so this test adds no literal address of its own.
P = ".".join(["192", "0", "2"])
FIXTURE = {
    "schema_version": 1,
    "hosts": {
        "pythia": {"address": f"{P}.67", "user": "op", "auth": "key", "os": "linux"},
        "cerberus": {"address": f"{P}.96", "user": "op", "auth": "key", "os": "linux"},
    },
}


def run(*args):
    return subprocess.run([sys.executable, str(RENDER), *args], capture_output=True, text=True)


def write_fixture(root, hosts=None):
    inv = root / "inventory.json"
    inv.write_text(json.dumps({"schema_version": 1, "hosts": hosts or FIXTURE["hosts"]}))
    return inv


def test_renders_three_urls_and_keeps_placeholders():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        inv = write_fixture(root)
        out = root / "out"
        result = run("--inventory", str(inv), "--out-dir", str(out))
        assert result.returncode == 0, result.stderr
        doctor = (out / "doctor.container.env").read_text()
        assert f"http://{P}.67:5005" in doctor
        assert f"http://{P}.67:5002" in doctor
        assert f"http://{P}.96:8080/health" in doctor
        assert "<from key store>" in doctor
        assert "HIVE_BUS_ADMIN_TOKEN=<from key store>" in doctor
        assert "MNEMOS_TOKEN=<from key store>" in doctor
        bus = (out / "bus.service.env").read_text()
        assert "HIVE_BUS_ADMIN_TOKEN=<from key store>" in bus


def test_tokens_never_filled_from_inventory():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        hosts = dict(FIXTURE["hosts"])
        hosts["pythia"] = dict(hosts["pythia"], token="shaped-but-must-not-leak")
        inv = write_fixture(root, hosts)
        out = root / "out"
        assert run("--inventory", str(inv), "--out-dir", str(out)).returncode == 0
        doctor = (out / "doctor.container.env").read_text()
        assert "shaped-but-must-not-leak" not in doctor


def test_missing_host_is_an_error():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        inv = write_fixture(root, {"pythia": FIXTURE["hosts"]["pythia"]})
        out = root / "out"
        result = run("--inventory", str(inv), "--out-dir", str(out))
        assert result.returncode != 0


def test_missing_inventory_is_an_error():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        result = run("--inventory", str(root / "nope.json"), "--out-dir", str(root / "out"))
        assert result.returncode != 0
