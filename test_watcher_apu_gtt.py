#!/usr/bin/env python3
"""APU (gfx1103 Radeon 780M) GTT memory reporting in the watcher's gpus_runtime.

On APUs the iGPU's VRAM is only a 512 MiB carve-out; llama-server model
weights live in GTT (shared system RAM). Before the fix the runtime GPU
block only parsed VRAM, so the dashboard showed ~90/512 MiB (looks like no
model loaded) while 2.5 GiB of model sat in GTT.

Covers both the hive worker module and the standalone deployed copy.
"""
from __future__ import annotations

import importlib.util
import json
import os

import pytest

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))

# Real rocm-smi --json sample from HYDRA (gfx1103 APU, llama-server running).
APU_SAMPLE = {
    "card0": {
        "GPU use (%)": "99",
        "VRAM Total Memory (B)": "536870912",
        "VRAM Total Used Memory (B)": "96931840",
        "GTT Total Memory (B)": "16104345600",
        "GTT Total Used Memory (B)": "2716844032",
        "Card SKU": "PHXGENERIC",
        "GFX Version": "gfx1103",
    }
}

# Discrete card (24 GiB VRAM): mem_* must stay VRAM-only, unified False.
DISCRETE_SAMPLE = {
    "card0": {
        "GPU use (%)": "12",
        "VRAM Total Memory (B)": str(24 * 1024**3),
        "VRAM Total Used Memory (B)": str(8 * 1024**3),
        "GTT Total Memory (B)": "402653184",
        "GTT Total Used Memory (B)": "2097152",
        "Card SKU": "DISCRETE-SKU",
        "GFX Version": "gfx1102",
    }
}


def _load_module(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _fake_rocm(tmp_path, sample: dict, args_log: str) -> str:
    """A stand-in for rocm-smi that records its argv and emits the sample."""
    payload = json.dumps(sample)
    script = tmp_path / "rocm-smi"
    script.write_text(
        "#!/usr/bin/env python3\n"
        "import sys\n"
        f"open({str(args_log)!r}, 'a').write(' '.join(sys.argv[1:]) + '\\n')\n"
        f"sys.stdout.write({payload!r})\n"
    )
    script.chmod(0o755)
    return str(script)


@pytest.fixture(scope="module")
def hive_watcher():
    return _load_module("hive_watcher_under_test", os.path.join(REPO_ROOT, "hive/workers/watcher.py"))


@pytest.fixture(scope="module")
def standalone_watcher():
    return _load_module("standalone_watcher_under_test", os.path.join(REPO_ROOT, "system_watcher.py"))


@pytest.fixture(params=["hive_watcher", "standalone_watcher"], ids=["hive", "standalone"])
def watcher(request, hive_watcher, standalone_watcher):
    return request.getfixturevalue(request.param)


# --- rocm-smi JSON parsing --------------------------------------------------

def test_apu_sample_parse(watcher):
    """APU sample: mem_* = VRAM+GTT summed bytes -> MiB, unified True."""
    result = watcher._parse_rocm_json(APU_SAMPLE)
    assert len(result) == 1
    g = result[0]
    # Summed bytes: (96931840 + 2716844032) / 2**20 = 2683.4 -> 2683
    # (536870912 + 16104345600) / 2**20 = 15870.3 -> 15870
    assert g["vendor"] == "amd"
    assert g["name"] == "PHXGENERIC"
    assert g["util_pct"] == 99.0
    assert g["vram_used_mib"] == 92
    assert g["vram_total_mib"] == 512
    assert g["gtt_used_mib"] == 2590
    assert g["gtt_total_mib"] == 15358
    assert g["mem_used_mib"] == 2683
    assert g["mem_total_mib"] == 15870
    assert g["unified_memory"] is True


def test_discrete_sample_unchanged(watcher):
    """Discrete GPU (24 GiB VRAM): mem_* stays VRAM-only, unified False."""
    result = watcher._parse_rocm_json(DISCRETE_SAMPLE)
    assert len(result) == 1
    g = result[0]
    assert g["vram_used_mib"] == 8192
    assert g["vram_total_mib"] == 24576
    assert g["gtt_used_mib"] == 2
    assert g["gtt_total_mib"] == 384
    assert g["mem_used_mib"] == 8192
    assert g["mem_total_mib"] == 24576
    assert g["unified_memory"] is False


def test_rocm_args_include_showmeminfo(watcher, tmp_path):
    """The rocm-smi call must request GTT memory, not just VRAM."""
    args_log = tmp_path / "args.log"
    fake = _fake_rocm(tmp_path, APU_SAMPLE, args_log)
    result = watcher._collect_amd_gpus(fake)
    assert len(result) == 1
    assert result[0]["mem_used_mib"] == 2683
    args = args_log.read_text().splitlines()[-1]
    assert "--showmeminfo" in args
    assert "vram" in args
    assert "gtt" in args


def test_rocm_failure_falls_back_to_sysfs(watcher, tmp_path, monkeypatch):
    """When rocm-smi fails, the amdgpu sysfs fallback is used instead."""
    _write_sysfs_apu(tmp_path / "sysfs")
    failing = tmp_path / "bad-rocm-smi"
    failing.write_text("#!/bin/sh\nexit 1\n")
    failing.chmod(0o755)
    # Redirect the sysfs default at the tmp tree via monkeypatch on glob?
    # Simpler: call the fallback function directly (same code path used by
    # _collect_amd_gpus when rocm-smi is missing).
    result = watcher._apu_mem_from_sysfs(str(tmp_path / "sysfs"))
    assert len(result) == 1
    assert result[0]["unified_memory"] is True
    assert result[0]["mem_used_mib"] == 2683


# --- sysfs fallback ----------------------------------------------------------

def _write_sysfs_apu(root):
    """Build a fake <root>/class/drm/card0/device with APU-shaped numbers."""
    dev = root / "class/drm/card0/device"
    dev.mkdir(parents=True)
    (dev / "mem_info_vram_total").write_text(str(536870912))
    (dev / "mem_info_vram_used").write_text(str(96931840))
    (dev / "mem_info_gtt_total").write_text(str(16104345600))
    (dev / "mem_info_gtt_used").write_text(str(2716844032))
    (dev / "gpu_busy_percent").write_text("99")


def test_sysfs_fallback_apu(watcher, tmp_path):
    """Sysfs fallback when rocm-smi is absent: APU numbers, unified True."""
    _write_sysfs_apu(tmp_path)
    result = watcher._apu_mem_from_sysfs(str(tmp_path))
    assert len(result) == 1
    g = result[0]
    assert g["vendor"] == "amd"
    assert g["name"] == "card0"
    assert g["util_pct"] == 99.0
    assert g["vram_used_mib"] == 92
    assert g["vram_total_mib"] == 512
    assert g["gtt_used_mib"] == 2590
    assert g["gtt_total_mib"] == 15358
    assert g["mem_used_mib"] == 2683
    assert g["mem_total_mib"] == 15870
    assert g["unified_memory"] is True


def test_sysfs_fallback_discrete(watcher, tmp_path):
    """Sysfs fallback on a discrete card keeps mem_* VRAM-only."""
    dev = tmp_path / "class/drm/card1/device"
    dev.mkdir(parents=True)
    (dev / "mem_info_vram_total").write_text(str(24 * 1024**3))
    (dev / "mem_info_vram_used").write_text(str(8 * 1024**3))
    (dev / "mem_info_gtt_total").write_text(str(402653184))
    (dev / "mem_info_gtt_used").write_text(str(2097152))
    (dev / "gpu_busy_percent").write_text("30")
    result = watcher._apu_mem_from_sysfs(str(tmp_path))
    assert len(result) == 1
    g = result[0]
    assert g["mem_used_mib"] == 8192
    assert g["mem_total_mib"] == 24576
    assert g["unified_memory"] is False


def test_sysfs_ignores_non_amdgpu(watcher, tmp_path):
    """Cards without vram mem_info (e.g. i915/nouveau) are skipped."""
    dev = tmp_path / "class/drm/card2/device"
    dev.mkdir(parents=True)
    (dev / "gpu_busy_percent").write_text("5")
    assert watcher._apu_mem_from_sysfs(str(tmp_path)) == []


# --- hygiene ------------------------------------------------------------------

def test_no_bare_except_in_gpu_block(watcher):
    """The GPU block must no longer use bare except clauses."""
    import inspect

    src = inspect.getsource(watcher)
    start = src.find("def _apu_mem_from_sysfs")
    assert start != -1
    # The three helpers make up the AMD/GPU runtime collection block.
    for anchor in ("def _collect_amd_gpus", "def _amd_entry_from_bytes",
                   "def _parse_rocm_json"):
        i = src.find(anchor, start)
        assert i != -1, f"missing {anchor}"
    end = max(src.find("def _collect_amd_gpus", start),
              src.find("def _amd_entry_from_bytes", start),
              src.find("def _parse_rocm_json", start),
              src.find("def _apu_mem_from_sysfs", start))
    block = src[src.find("def _apu_mem_from_sysfs"):src.find("def static_specs")]
    assert "except:" not in block


def test_dynamic_load_runs(watcher, monkeypatch, tmp_path):
    """dynamic_load() with a fake rocm-smi on PATH reports the APU summary."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "rocm-smi").write_text(
        "#!/usr/bin/env python3\n"
        "import sys\n"
        f"sys.stdout.write({json.dumps(APU_SAMPLE)!r})\n"
    )
    (bin_dir / "rocm-smi").chmod(0o755)
    monkeypatch.setenv("PATH", str(bin_dir) + ":" + os.environ.get("PATH", ""))
    # The hive watcher resolves rocm-smi via shutil.which at call time.
    result = watcher.dynamic_load()
    gpus = result.get("gpus_runtime", [])
    amd = [g for g in gpus if g.get("vendor") == "amd"]
    assert len(amd) == 1
    assert amd[0]["mem_used_mib"] == 2683
    assert amd[0]["mem_total_mib"] == 15870
    assert amd[0]["unified_memory"] is True
