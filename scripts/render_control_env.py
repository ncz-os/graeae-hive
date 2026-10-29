#!/usr/bin/env python3
"""Render control-host env files from a fleet inventory.

Takes a fleet inventory JSON (the format of hive/core/fleet_inventory.py:
schema_version + hosts map with address/user/auth/os) and renders the two
committed examples (deploy/doctor.container.env.example,
deploy/bus.service.env.example) with real NON-SECRET values filled in from
the inventory:

  HIVE_URL                 http://<pythia address>:5005
  MNEMOS_URL               http://<pythia address>:5002
  DOCTOR_REVIEWER_HEALTH_URL  http://<cerberus address>:8080/health

Every token stays a placeholder (``<from key store>``): this script refuses
to write any secret value. Usage:

    scripts/render_control_env.py --inventory <path> --out-dir <dir>

Writes ``<out-dir>/doctor.container.env`` and ``<out-dir>/bus.service.env``.
Never writes to /etc. Stdlib only.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
DOCTOR_EXAMPLE = HERE / "deploy" / "doctor.container.env.example"
BUS_EXAMPLE = HERE / "deploy" / "bus.service.env.example"

PLACEHOLDER = "<from key store>"

SECRET_NAMES = ("TOKEN", "KEY", "SECRET", "PASSWORD", "PASSWD", "CREDENTIAL")


def is_secret_name(name: str) -> bool:
    upper = name.upper()
    return any(tok in upper for tok in SECRET_NAMES)


def load_hosts(inventory_path: Path) -> dict[str, dict]:
    data = json.loads(inventory_path.read_text(encoding="utf-8"))
    hosts = data.get("hosts")
    if not isinstance(hosts, dict) or not hosts:
        raise SystemExit(f"inventory {inventory_path} has no 'hosts' object")
    out: dict[str, dict] = {}
    for name, raw in hosts.items():
        key = str(name).strip().lower()
        if not key or not isinstance(raw, dict):
            continue
        address = str(raw.get("address") or raw.get("ip") or "").strip()
        if not address:
            continue
        out[key] = {"address": address}
    if not out:
        raise SystemExit(f"inventory {inventory_path} declares no usable host")
    return out


def required_host(hosts: dict[str, dict], name: str) -> str:
    try:
        return hosts[name]["address"]
    except KeyError:
        raise SystemExit(f"inventory has no usable host {name!r}") from None


def render_doctor(hosts: dict[str, dict]) -> str:
    template = DOCTOR_EXAMPLE.read_text(encoding="utf-8")
    pythia = required_host(hosts, "pythia")
    cerberus = required_host(hosts, "cerberus")
    values = {
        "HIVE_URL": f"http://{pythia}:5005",
        "MNEMOS_URL": f"http://{pythia}:5002",
        "DOCTOR_REVIEWER_HEALTH_URL": f"http://{cerberus}:8080/health",
    }
    lines: list[str] = []
    for line in template.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            lines.append(line)
            continue
        name, _, _ = line.partition("=")
        name = name.strip()
        if not name or is_secret_name(name):
            if name in values:
                raise SystemExit(f"refusing to fill secret-shaped {name}")
            lines.append(line)
            continue
        if name in values:
            lines.append(f"{name}={values[name]}")
        else:
            lines.append(line)
    rendered = "\n".join(lines) + "\n"
    if PLACEHOLDER not in rendered:
        raise SystemExit("rendered doctor env lost its token placeholders")
    return rendered


def render_bus() -> str:
    text = BUS_EXAMPLE.read_text(encoding="utf-8")
    if PLACEHOLDER not in text:
        raise SystemExit("bus example lost its token placeholder")
    return text if text.endswith("\n") else text + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Render control-host env files from inventory.")
    ap.add_argument("--inventory", type=Path, required=True, help="fleet inventory JSON path")
    ap.add_argument("--out-dir", type=Path, required=True, help="output directory (created)")
    args = ap.parse_args(argv)

    if not DOCTOR_EXAMPLE.is_file() or not BUS_EXAMPLE.is_file():
        print("missing committed example files", file=sys.stderr)
        return 2
    hosts = load_hosts(args.inventory)
    doctor = render_doctor(hosts)
    bus = render_bus()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "doctor.container.env").write_text(doctor, encoding="utf-8")
    (args.out_dir / "bus.service.env").write_text(bus, encoding="utf-8")
    print(f"wrote {args.out_dir / 'doctor.container.env'} and {args.out_dir / 'bus.service.env'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
