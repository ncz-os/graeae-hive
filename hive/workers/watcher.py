#!/usr/bin/env python3
"""
GRAEAE Hive Mind — fleet system watcher daemon.

Registers the host as kind=system with static hardware specs + dynamic load.
Heartbeats current cpu/ram/load/disk metrics. Optionally claims jobs that
require physical resources (build, compile, render) when load permits.

Prevents ARGOS-style oversubscription (CLAUDE.md gotcha: load=92 incident
2026-05-20 from multi-job spawning). Dispatcher filters by required_resources
AND current load before assigning to a host.

Env:
  HIVE_URL                  http://192.0.2.67:5005
  AGENT_HOST                default hostname -s
  LOAD_THRESHOLD            max 1-min load_avg before refusing new jobs (default 0.75 × cpu_count)
  HEARTBEAT_INTERVAL        seconds (default 15)
  CLAIM_JOBS                if "1", actively claim eligible jobs and dispatch to local executor (default "0" = passive monitor only)
"""
from __future__ import annotations

import contextlib
import json
import os
import platform
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

HIVE_URL = os.environ.get("HIVE_URL", "http://192.0.2.67:5005")
# C1: bus transport auth bearer token (env-sourced, never baked in).
HIVE_BUS_TOKEN = os.environ.get("HIVE_BUS_TOKEN", "")
AGENT_HOST = os.environ.get("AGENT_HOST", socket.gethostname().split(".")[0])
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "15"))
CLAIM_JOBS = os.environ.get("CLAIM_JOBS", "0") == "1"

_urn: str = ""
_STATIC_SPECS: dict = {}  # captured at register; reused by the heartbeat target calc
_running = True


def _signal(signum, frame):
    global _running
    print(f"[sysw] signal {signum} — shutting down", flush=True)
    _running = False


signal.signal(signal.SIGTERM, _signal)
signal.signal(signal.SIGINT, _signal)


def _http(method: str, path: str, body: dict | None = None) -> tuple[int, dict | None]:
    url = f"{HIVE_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    headers = {"content-type": "application/json"}
    if HIVE_BUS_TOKEN:
        headers["authorization"] = f"Bearer {HIVE_BUS_TOKEN}"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, None
    except Exception as e:
        print(f"[sysw] http error {method} {path}: {e}", flush=True)
        return 0, None


_IS_MACOS = platform.system() == "Darwin"

# APUs (Ryzen gfx1103/780M etc.) expose only a small VRAM carve-out; the rest
# of the iGPU's memory lives in GTT (shared system RAM). Threshold: any
# AMD card whose VRAM total is under 2 GiB is treated as an APU.
_APU_VRAM_THRESHOLD_BYTES = 2 * 1024 * 1024 * 1024


def _apu_mem_from_sysfs(sysfs_root: str = "/sys") -> list[dict]:
    """Fallback GPU runtime metrics read straight from amdgpu sysfs.

    Used when rocm-smi is missing or fails. Only reports cards that expose
    the mem_info_vram_* files (amdgpu driver). sysfs_root may be overridden
    for testing (expects <root>/class/drm/card*/device).
    """
    import glob as _glob

    out: list[dict] = []
    for dev in sorted(_glob.glob(sysfs_root + "/class/drm/card*/device")):
        pfx = dev + "/"
        try:
            with open(pfx + "mem_info_vram_total") as f:
                vram_total = int(f.read().strip() or 0)
            with open(pfx + "mem_info_vram_used") as f:
                vram_used = int(f.read().strip() or 0)
        except (OSError, ValueError):
            continue  # not an amdgpu card (no vram info)
        gtt_total = gtt_used = 0
        with contextlib.suppress(OSError, ValueError):
            with open(pfx + "mem_info_gtt_total") as f:
                gtt_total = int(f.read().strip() or 0)
        with contextlib.suppress(OSError, ValueError):
            with open(pfx + "mem_info_gtt_used") as f:
                gtt_used = int(f.read().strip() or 0)
        util = None
        with contextlib.suppress(OSError, ValueError):
            with open(pfx + "gpu_busy_percent") as f:
                util = float(f.read().strip() or 0)
        entry = _amd_entry_from_bytes(
            name=os.path.basename(dev.rsplit("/", 1)[0]),
            util=util,
            vram_used=vram_used, vram_total=vram_total,
            gtt_used=gtt_used, gtt_total=gtt_total,
            temp_c=None, power_w=None,
        )
        out.append(entry)
    return out


def _amd_entry_from_bytes(*, name: str, util, vram_used: int, vram_total: int,
                          gtt_used: int, gtt_total: int, temp_c, power_w) -> dict:
    """Build one gpus_runtime entry from raw byte counters.

    APUs (VRAM total < 2 GiB) report VRAM+GTT as the memory figures and
    unified_memory=True; discrete cards keep mem_* as VRAM-only.
    """
    unified = bool(vram_total and vram_total < _APU_VRAM_THRESHOLD_BYTES)
    mem_used = vram_used + gtt_used if unified else vram_used
    mem_total = vram_total + gtt_total if unified else vram_total
    return {
        "vendor": "amd", "name": name, "util_pct": util,
        "mem_used_mib": int(mem_used / (1024 * 1024)),
        "mem_total_mib": int(mem_total / (1024 * 1024)),
        "vram_used_mib": int(vram_used / (1024 * 1024)),
        "vram_total_mib": int(vram_total / (1024 * 1024)),
        "gtt_used_mib": int(gtt_used / (1024 * 1024)),
        "gtt_total_mib": int(gtt_total / (1024 * 1024)),
        "unified_memory": unified,
        "temp_c": temp_c, "power_w": power_w,
    }


def _parse_rocm_json(data: dict) -> list[dict]:
    """Parse a rocm-smi --json payload (see --showmeminfo vram gtt)."""
    gpus: list[dict] = []
    for k, v in data.items():
        if not isinstance(v, dict):
            continue
        name = v.get("Card SKU") or v.get("Card series") or v.get("Card model") or k
        try:
            util = float((v.get("GPU use (%)") or "0").replace("%", ""))
        except (TypeError, ValueError):
            util = None
        vram_used = vram_total = gtt_used = gtt_total = 0
        try:
            vram_used = int(float(v.get("VRAM Total Used Memory (B)") or 0))
        except (TypeError, ValueError):
            pass
        try:
            vram_total = int(float(v.get("VRAM Total Memory (B)") or 0))
        except (TypeError, ValueError):
            pass
        try:
            gtt_used = int(float(v.get("GTT Total Used Memory (B)") or 0))
        except (TypeError, ValueError):
            pass
        try:
            gtt_total = int(float(v.get("GTT Total Memory (B)") or 0))
        except (TypeError, ValueError):
            pass
        temp = None
        for tk in v:
            if "temperature" in tk.lower() and "edge" in tk.lower():
                try:
                    temp = float(v[tk])
                    break
                except (TypeError, ValueError):
                    pass
        try:
            power = float((v.get("Average Graphics Package Power (W)") or "").replace("W", "").strip() or 0)
        except (TypeError, ValueError):
            power = None
        gpus.append(_amd_entry_from_bytes(
            name=name, util=util,
            vram_used=vram_used, vram_total=vram_total,
            gtt_used=gtt_used, gtt_total=gtt_total,
            temp_c=temp, power_w=power,
        ))
    return gpus


def _collect_amd_gpus(runtime_bin: str | None) -> list[dict]:
    """Runtime AMD GPU metrics via rocm-smi, with an amdgpu-sysfs fallback.

    On APUs (gfx1103 e.g. Radeon 780M: 512 MiB VRAM carve-out, model weights
    live in GTT), mem_* combines VRAM+GTT and unified_memory is True so the
    dashboard bars reflect what's actually loaded.
    """
    if runtime_bin:
        try:
            r = subprocess.run(
                [runtime_bin, "--showproductname", "--showuse", "--showmemuse",
                 "--showmeminfo", "vram", "gtt",
                 "--showtemp", "--showpower", "--json"],
                capture_output=True, text=True, timeout=5,
            )
            if r.stdout and r.stdout.strip():
                data = json.loads(r.stdout)
                if isinstance(data, dict):
                    gpus = _parse_rocm_json(data)
                    if gpus:
                        return gpus
        except Exception:
            pass
    # rocm-smi missing or produced nothing — try amdgpu sysfs directly.
    return _apu_mem_from_sysfs()


def _amd_runtime_bin() -> str | None:
    """Locate rocm-smi (PATH or /opt/rocm*/bin). Standalone-safe helper."""
    p = shutil.which("rocm-smi")
    if p:
        return p
    import glob as _glob

    for d in sorted(_glob.glob("/opt/rocm-*/bin")) + ["/opt/rocm/bin"]:
        cand = os.path.join(d, "rocm-smi")
        if os.path.exists(cand):
            return cand
    return None


def _rocm_bin_dirs() -> list[str]:
    """ROCm installs outside the systemd PATH (/opt/rocm-<ver>/bin). Glob them."""
    dirs = ["/opt/rocm/bin"]
    try:
        import glob as _glob

        dirs += sorted(_glob.glob("/opt/rocm-*/bin"))
    except Exception:
        pass
    return dirs


def _find_bin(name: str, extra_dirs: list[str] | None = None) -> str | None:
    """Locate a tool on PATH or in the given extra dirs (e.g. ROCm's /opt/rocm*/bin)."""
    p = shutil.which(name)
    if p:
        return p
    for d in extra_dirs or []:
        cand = os.path.join(d, name)
        if os.path.exists(cand):
            return cand
    return None


def static_specs() -> dict:
    """One-shot specs gathered at startup."""
    out = {
        "hostname": AGENT_HOST,
        "fqdn": socket.getfqdn(),
        "os": platform.system(),
        "kernel": platform.release(),
        "arch": platform.machine(),
        "python": sys.version.split()[0],
    }
    # CPU
    out["cpu_count"] = os.cpu_count()
    if _IS_MACOS:
        try:
            r = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"],
                               capture_output=True, text=True, timeout=5)
            out["cpu_model"] = r.stdout.strip()
            r2 = subprocess.run(["sysctl", "-n", "hw.logicalcpu"],
                                capture_output=True, text=True, timeout=5)
            out["cpu_threads"] = int(r2.stdout.strip())
        except Exception:
            pass
    else:
        try:
            with open("/proc/cpuinfo") as f:
                content = f.read()
            m = re.search(r"model name\s*:\s*(.+)", content)
            if m:
                out["cpu_model"] = m.group(1).strip()
            out["cpu_threads"] = content.count("processor\t:")
        except Exception:
            pass
    # RAM
    if _IS_MACOS:
        try:
            r = subprocess.run(["sysctl", "-n", "hw.memsize"],
                               capture_output=True, text=True, timeout=5)
            ram_bytes = int(r.stdout.strip())
            out["ram_gb"] = round(ram_bytes / 1024**3, 1)
            out["ram_kb"] = ram_bytes // 1024
        except Exception:
            pass
    else:
        try:
            with open("/proc/meminfo") as f:
                mi = f.read()
            m = re.search(r"MemTotal:\s+(\d+)", mi)
            if m:
                out["ram_kb"] = int(m.group(1))
                out["ram_gb"] = round(int(m.group(1)) / 1024 / 1024, 1)
        except Exception:
            pass
    # Disk
    try:
        for mnt in ("/srv", "/", "/var"):
            if os.path.exists(mnt):
                s = shutil.disk_usage(mnt)
                key = f"disk_{mnt.replace('/', '_') or '_root'}_total_gb"
                out[key] = round(s.total / 1024**3, 1)
    except Exception:
        pass
    # GPU detection
    gpus = []
    # Apple Silicon (macOS)
    if _IS_MACOS:
        try:
            r = subprocess.run(["system_profiler", "SPDisplaysDataType", "-json"],
                               capture_output=True, text=True, timeout=10)
            data = json.loads(r.stdout)
            for item in data.get("SPDisplaysDataType", []):
                gpus.append({
                    "vendor": "apple",
                    "name": item.get("sppci_model", "Apple Silicon GPU"),
                    "vram_mib": None,
                    "info": item,
                })
        except Exception:
            pass
    # NVIDIA
    _nvsmi = _find_bin("nvidia-smi", ["/usr/bin", "/usr/local/bin", "/usr/sbin"])
    if _nvsmi:
        try:
            r = subprocess.run([_nvsmi, "--query-gpu=name,memory.total,driver_version",
                                "--format=csv,noheader,nounits"], capture_output=True, text=True, timeout=5)
            for line in r.stdout.strip().splitlines():
                parts = [p.strip() for p in line.split(",")]
                if len(parts) < 2:
                    continue
                name = parts[0]
                # a probe/unavailable GPU reports [N/A]/N/A — keep the row but
                # don't fail the loop (int("[N/A]") must not abort the others).
                try:
                    vram = int(parts[1])
                except (TypeError, ValueError):
                    vram = None
                if name and name not in ("[N/A]", "N/A"):
                    gpus.append({"vendor": "nvidia", "name": name, "vram_mib": vram,
                                 "driver": parts[2] if len(parts) > 2 else ""})
        except Exception:
            pass
    # AMD/ROCm — the binary is usually at /opt/rocm*/bin, NOT on the systemd PATH
    _rocsmi = _find_bin("rocm-smi", _rocm_bin_dirs())
    if _rocsmi:
        try:
            r = subprocess.run([_rocsmi, "--showproductname", "--json"], capture_output=True, text=True, timeout=5)
            data = json.loads(r.stdout) if r.stdout.strip() else {}
            for k, v in data.items():
                if isinstance(v, dict) and ("Card SKU" in v or "GPU ID" in v):
                    gpus.append({"vendor": "amd", "card_id": k, "info": v})
        except Exception:
            pass
    if not gpus:
        # try lspci (Linux only)
        try:
            r = subprocess.run(["lspci"], capture_output=True, text=True, timeout=5)
            for line in r.stdout.splitlines():
                if re.search(r"VGA|3D|Display", line):
                    gpus.append({"vendor": "unknown", "lspci": line.strip()})
        except Exception:
            pass
    out["gpus"] = gpus
    # Container runtimes
    out["has_docker"] = bool(shutil.which("docker"))
    out["has_podman"] = bool(shutil.which("podman"))
    out["has_buildx"] = (bool(shutil.which("docker")) and
                         subprocess.run(["docker", "buildx", "version"], capture_output=True).returncode == 0
                         ) if shutil.which("docker") else False
    # CI runners
    out["has_gitlab_runner"] = bool(shutil.which("gitlab-runner"))
    # XDNA NPU (HYDRA Ryzen 8700G / cixmini Cix Sky1)
    try:
        r = subprocess.run(["lspci"], capture_output=True, text=True, timeout=5)
        if re.search(r"IPU|NPU|Neural|XDNA|AI Engine", r.stdout, re.I):
            out["has_npu"] = True
    except Exception:
        pass
    return out


def dynamic_load() -> dict:
    """Snapshot of current load."""
    out = {"ts": time.time()}
    with contextlib.suppress(Exception):
        out["load_1min"], out["load_5min"], out["load_15min"] = os.getloadavg()
    if _IS_MACOS:
        try:
            r = subprocess.run(["vm_stat"], capture_output=True, text=True, timeout=5)
            stats: dict[str, int] = {}
            for line in r.stdout.splitlines():
                m = re.match(r"Pages\s+(\w[\w\s]+):\s+(\d+)", line)
                if m:
                    stats[m.group(1).strip()] = int(m.group(2))
            page_size = 4096
            try:
                r2 = subprocess.run(["sysctl", "-n", "hw.pagesize"],
                                    capture_output=True, text=True, timeout=5)
                page_size = int(r2.stdout.strip())
            except Exception:
                pass
            total_pages = sum(stats.values()) or 1
            free_pages = stats.get("free", 0) + stats.get("speculative", 0)
            wired = stats.get("wired down", 0)
            active = stats.get("active", 0)
            used_pages = wired + active
            out["ram_used_pct"] = round(100 * used_pages / max(total_pages, 1), 1)
            out["ram_free_gb"] = round(free_pages * page_size / 1024**3, 2)
        except Exception:
            pass
    else:
        try:
            with open("/proc/meminfo") as f:
                mi = f.read()
            for key in ("MemTotal", "MemFree", "MemAvailable", "Buffers", "Cached"):
                m = re.search(rf"{key}:\s+(\d+)", mi)
                if m:
                    out[key.lower() + "_kb"] = int(m.group(1))
            if "memtotal_kb" in out and "memavailable_kb" in out:
                used = out["memtotal_kb"] - out["memavailable_kb"]
                out["ram_used_pct"] = round(100 * used / out["memtotal_kb"], 1)
        except Exception:
            pass
    try:
        # Legacy single-volume fields (back-compat for older dashboards)
        s = shutil.disk_usage("/srv" if os.path.exists("/srv") else "/")
        out["srv_used_pct"] = round(100 * (s.total - s.free) / s.total, 1)
        out["srv_free_gb"] = round(s.free / 1024**3, 1)
    except Exception:
        pass
    # Multi-volume disk snapshot — enumerate all real filesystems (skip
    # pseudo/loop/tmpfs mounts). Each entry: {mount, total_gb, used_pct, free_gb, fstype}.
    try:
        volumes = []
        seen = set()
        def _add_vol(mnt):
            try:
                if mnt in seen or not os.path.isdir(mnt):
                    return
                s = shutil.disk_usage(mnt)
                if s.total < 1024**3:   # skip <1GB (probably pseudo)
                    return
                seen.add(mnt)
                volumes.append({
                    "mount": mnt,
                    "total_gb": round(s.total / 1024**3, 1),
                    "free_gb": round(s.free / 1024**3, 1),
                    "used_pct": round(100 * (s.total - s.free) / s.total, 1),
                })
            except Exception:
                pass
        if _IS_MACOS:
            # macOS: parse `df -k`
            try:
                r = subprocess.run(["df", "-k"], capture_output=True, text=True, timeout=5)
                for line in r.stdout.splitlines()[1:]:
                    parts = line.split()
                    if len(parts) < 6:
                        continue
                    fs = parts[0]; mnt = parts[-1]
                    if fs.startswith("/dev/") and not mnt.startswith(("/System", "/private/var/vm")):
                        _add_vol(mnt)
            except Exception:
                pass
        else:
            # Linux: parse /proc/mounts
            try:
                with open("/proc/mounts") as f:
                    for line in f:
                        parts = line.split()
                        if len(parts) < 3:
                            continue
                        dev, mnt, fstype = parts[0], parts[1], parts[2]
                        if fstype in {"tmpfs","devtmpfs","proc","sysfs","cgroup","cgroup2",
                                      "devpts","mqueue","pstore","bpf","tracefs","debugfs",
                                      "configfs","fusectl","rpc_pipefs","squashfs","autofs",
                                      "binfmt_misc","hugetlbfs","fuse.gvfsd-fuse","fuse.portal",
                                      "overlay","nsfs"}:
                            continue
                        # Skip nfs auto-mount points that aren't actually mounted
                        if "x-systemd.automount" in line and fstype == "autofs":
                            continue
                        if not dev.startswith(("/dev/", "192.168.", "10.", "/")):
                            continue
                        _add_vol(mnt)
            except Exception:
                pass
        # Always include / explicitly
        _add_vol("/")
        # Dedupe by (total_gb, free_gb) — bind-mounts and overlay views share
        # the same underlying device size. Keep the shortest mount path as
        # the canonical one (usually the actual mount, not a bind).
        if volumes:
            by_sig: dict = {}
            for v in volumes:
                sig = (v.get("total_gb"), v.get("free_gb"))
                if sig not in by_sig or len(v.get("mount","")) < len(by_sig[sig].get("mount","")):
                    by_sig[sig] = v
            volumes = sorted(by_sig.values(), key=lambda x: x.get("mount") or "")
        out["volumes"] = volumes
    except Exception:
        pass
    # CPU utilization snapshot (instantaneous; system_watcher polls every ~15s)
    try:
        if _IS_MACOS:
            # parse `iostat -c2 1` second sample → user+sys
            r = subprocess.run(["iostat", "-c", "2", "1"], capture_output=True, text=True, timeout=8)
            lines = [l for l in r.stdout.splitlines() if l.strip()]
            if len(lines) >= 2:
                parts = lines[-1].split()
                # us sy id format
                if len(parts) >= 5:
                    with contextlib.suppress(Exception):
                        out["cpu_used_pct"] = float(parts[-5]) + float(parts[-4])
        else:
            # /proc/stat — single-sample using `top -bn2 -d 0.5`-style isn't portable;
            # use two reads of /proc/stat 200ms apart
            def _read_cpu():
                with open("/proc/stat") as f:
                    line = f.readline()
                vals = [int(x) for x in line.split()[1:]]
                idle = vals[3] + vals[4]   # idle + iowait
                total = sum(vals)
                return idle, total
            i1, t1 = _read_cpu()
            time.sleep(0.25)
            i2, t2 = _read_cpu()
            dt = t2 - t1
            di = i2 - i1
            if dt > 0:
                out["cpu_used_pct"] = round(100.0 * (1 - di/dt), 1)
    except Exception:
        pass
    # uptime
    if _IS_MACOS:
        try:
            r = subprocess.run(["sysctl", "-n", "kern.boottime"], capture_output=True, text=True, timeout=5)
            m = re.search(r"sec\s*=\s*(\d+)", r.stdout)
            if m:
                out["uptime_sec"] = time.time() - int(m.group(1))
        except Exception:
            pass
    else:
        try:
            with open("/proc/uptime") as f:
                out["uptime_sec"] = float(f.read().split()[0])
        except Exception:
            pass
    # Runtime GPU metrics (added 2026-05-26 for dashboard GPU status panel).
    # nvidia-smi → name, util%, mem MiB, temp C; same shape for rocm-smi (AMD).
    # Use _find_bin (PATH *and* /opt/rocm*/bin, /usr/sbin) — the live metrics were
    # blank on hosts whose tools are off-PATH (ACHILLES' rocm-smi), so the GPU
    # chart showed nothing even though the static specs listed the GPU.
    try:
        gpus_rt = []
        _nvsmi_rt = _find_bin("nvidia-smi", ["/usr/bin", "/usr/local/bin", "/usr/sbin"])
        if _nvsmi_rt:
            try:
                r = subprocess.run(
                    [_nvsmi_rt, "--query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw",
                     "--format=csv,noheader,nounits"],
                    capture_output=True, text=True, timeout=5,
                )
                for line in r.stdout.strip().splitlines():
                    parts = [p.strip() for p in line.split(",")]
                    if len(parts) >= 5:
                        with contextlib.suppress(Exception):
                            gpus_rt.append({
                                "vendor": "nvidia",
                                "name": parts[0],
                                "util_pct": float(parts[1]) if parts[1] else None,
                                "mem_used_mib": int(parts[2]) if parts[2] else None,
                                "mem_total_mib": int(parts[3]) if parts[3] else None,
                                "temp_c": float(parts[4]) if parts[4] else None,
                                "power_w": float(parts[5]) if len(parts) > 5 and parts[5] not in ("", "[Not Supported]") else None,
                            })
            except Exception:
                pass
        # rocm-smi via the robust bin probe (PATH + ROCm dirs); _collect_amd_gpus
        # falls back to amdgpu sysfs when it is absent.
        gpus_rt.extend(_collect_amd_gpus(_find_bin("rocm-smi", _rocm_bin_dirs())))
        out["gpus_runtime"] = gpus_rt
    except Exception:
        pass
    return out


def capabilities_from_specs(specs: dict) -> list[str]:
    caps = ["system", "host"]
    if specs.get("has_docker"):     caps.append("docker")
    if specs.get("has_podman"):     caps.append("podman")
    if specs.get("has_buildx"):     caps.extend(["build", "buildx", "multi-arch-build"])
    if specs.get("has_gitlab_runner"): caps.append("ci-runner")
    if specs.get("has_npu"):        caps.extend(["npu", "xdna"])
    for g in specs.get("gpus", []) or []:
        v = g.get("vendor", "")
        if v == "nvidia": caps.extend(["gpu", "nvidia-gpu", "cuda"])
        elif v == "amd":  caps.extend(["gpu", "amd-gpu", "rocm", "vulkan-compute"])
        elif v == "intel": caps.extend(["gpu", "intel-igpu"])
        elif v == "apple": caps.extend(["gpu", "apple-gpu", "metal", "mlx"])
    if specs.get("ram_gb", 0) >= 32:  caps.append("ram-32g+")
    if specs.get("ram_gb", 0) >= 64:  caps.append("ram-64g+")
    if specs.get("ram_gb", 0) >= 128: caps.append("ram-128g+")
    if specs.get("cpu_count", 0) >= 16: caps.append("cpu-16t+")
    if specs.get("cpu_count", 0) >= 32: caps.append("cpu-32t+")
    if specs.get("os") == "Darwin":  caps.append("macos")
    if specs.get("arch") == "arm64" or specs.get("arch") == "aarch64": caps.append("arm64")
    return caps


def _target_workers(specs: dict, load: dict) -> int:
    """Capacity-derived zeroclaw fan-out for this host, recomputed each heartbeat.

    Uses the shared policy (hive.workers.fanout) when importable, else an inline
    equivalent so a standalone watcher copy still reports a sane target.
    """
    cpu = int(specs.get("cpu_count") or os.cpu_count() or 1)
    ram_gb = float(specs.get("ram_gb") or 0)
    load1 = float(load.get("load_1min") or 0.0)
    load_per_cpu = load1 / max(1, cpu)
    gpus = len(specs.get("gpus") or load.get("gpus") or []) if isinstance(specs.get("gpus") or load.get("gpus"), list) else 0
    try:
        from hive.workers.fanout import desired_worker_count

        return desired_worker_count(cpu_count=cpu, mem_gb=ram_gb, load_per_cpu=load_per_cpu, gpus=gpus)
    except Exception:  # standalone copy — same policy, inline
        by_cpu, by_ram = max(1, cpu // 2), max(1, int(ram_gb // 2))
        base = min(by_cpu, by_ram) + gpus
        if load_per_cpu >= 0.9:
            base -= 2
        elif load_per_cpu >= 0.7:
            base -= 1
        return max(1, min(base, 16))


def register():
    global _urn
    specs = static_specs()
    _STATIC_SPECS.clear()
    _STATIC_SPECS.update(specs)
    caps = capabilities_from_specs(specs)
    load = dynamic_load()
    body = {
        "runtime": "system",
        "kind": "system",
        "host": AGENT_HOST,
        "pid": os.getpid(),
        "model": "n/a",
        "provider": "local",
        "autonomy_level": "autonomous" if CLAIM_JOBS else "interactive",
        "capabilities": caps,
        "version": platform.platform(),
        "metadata": {
            "specs": specs,
            "load": load,
            "daemon": "system_watcher.py",
            "target_workers": _target_workers(specs, load),
        },
    }
    # system is a NEW runtime — RUNTIME_KIND_MAP may not include it; service falls back to allowing kind==runtime
    code, resp = _http("POST", "/v1/agents/register", body)
    if code == 200 and resp:
        _urn = resp["urn"]
        print(f"[sysw] registered urn={_urn} caps={caps}", flush=True)
    else:
        print(f"[sysw] register failed code={code} resp={resp}", flush=True)
        sys.exit(1)


def heartbeat():
    load = dynamic_load()
    # Push fresh load + a capacity-derived worker target into agent metadata so
    # /v1/hosts always has current data and the host reconcile can size its
    # zeroclaw fan-out dynamically (not a fixed count).
    _http("POST", "/v1/agents/heartbeat", {
        "urn": _urn,
        "metadata": {"load": load, "target_workers": _target_workers(_STATIC_SPECS, load)},
    })
    # NOTE: previous code also POSTed to /v1/messages as topic=system.load,
    # but that flooded the bus (11 hosts × 4 msgs/min = 2640/hr of pure
    # telemetry). The load is already in agent metadata via heartbeat above
    # and /v1/hosts surfaces it; SSE listeners can subscribe to agent.metadata
    # events instead. Broadcast removed 2026-05-26.


def main():
    print(f"[sysw] starting on {AGENT_HOST} HIVE_URL={HIVE_URL} CLAIM_JOBS={CLAIM_JOBS}", flush=True)
    register()
    while _running:
        heartbeat()
        time.sleep(HEARTBEAT_INTERVAL)
    print("[sysw] clean shutdown", flush=True)


if __name__ == "__main__":
    main()
