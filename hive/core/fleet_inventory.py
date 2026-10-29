"""Where the fleet is — host addresses come from operator config, never source.

A baked-in address goes stale silently: ULTRA (.60), MEDUSA (.64) and PROTEUS
(.25) in this tree were already wrong, and every clone of the repo carried the
same wrong map. The operator rule is that an address comes from the environment
or from a fleet inventory file, so this module is the one reader of that file.

The file is JSON, its path is ``HIVE_FLEET_INVENTORY``, and there is deliberately
no default path (a default path could itself name an address):

    {
      "schema_version": 1,
      "hosts": {
        "cerberus": {"address": "192.0.2.96", "user": "jasonperlow",
                     "auth": "key", "os": "linux"},
        "cixmini":  {"address": "192.0.2.99", "user": "mini",
                     "auth": "password", "password_env": "CIXMINI_SSH_PASSWORD"}
      }
    }

``docs/fleet-inventory.example.json`` ships the shape with TEST-NET addresses.
Both ``address`` and the older ``ip`` key are accepted, because the doctor's
legacy tables used ``ip`` and an operator may copy one across.

Everything here is fail-soft by design: a missing, unreadable, malformed or
partial inventory yields an empty (or smaller) result plus one log line per
distinct problem, never an exception. A caller that needs an address gets ``""``
and refuses the action it wanted to take — the doctor must not crash-loop because
the NAS is unmounted, and it must not guess an address either.

Stdlib only, so a container that runs the doctor module standalone can import it.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

#: Environment variable holding the inventory file's path. No default: see above.
INVENTORY_ENV = "HIVE_FLEET_INVENTORY"

#: Recognised ``schema_version`` values. Informational only — a future version is
#: still read, because refusing to read it would take the fleet offline.
SCHEMA_VERSION = 1

DEFAULT_AUTH = "key"
DEFAULT_OS = "linux"

log = logging.getLogger("hive.fleet_inventory")

#: Keys whose line has already been emitted. One line per problem per process:
#: a doctor in a crash loop must not turn a missing file into a log flood.
_warned: set[str] = set()


def warn_once(key: str, message: str, *, level: int = logging.WARNING) -> None:
    """Emit ``message`` once per process for ``key``.

    Shared with callers outside this module (the doctor warns this way when an
    optional base URL is unset), so the "one log line" rule is implemented once.
    """
    if key in _warned:
        return
    _warned.add(key)
    log.log(level, message)


def reset_warnings() -> None:
    """Forget which lines were emitted. Tests, and an operator config reload."""
    _warned.clear()


def inventory_path(env: dict | None = None) -> str:
    """The configured inventory path, or ``""`` when nothing is configured."""
    source = os.environ if env is None else env
    return str(source.get(INVENTORY_ENV) or "").strip()


def _normalize(name: str, raw: object) -> dict | None:
    """One host entry, or None when it names no address.

    ``address`` (preferred) or ``ip``. User/auth/os default rather than being
    required, so an inventory written for one consumer still works for another;
    an entry with no address is dropped, because every caller wants the address.
    """
    if not isinstance(raw, dict):
        return None
    address = str(raw.get("address") or raw.get("ip") or "").strip()
    if not address:
        return None
    entry = dict(raw)
    entry["host"] = name
    entry["address"] = address
    entry["user"] = str(raw.get("user") or "").strip()
    entry["auth"] = str(raw.get("auth") or DEFAULT_AUTH).strip().lower() or DEFAULT_AUTH
    entry["os"] = str(raw.get("os") or DEFAULT_OS).strip().lower() or DEFAULT_OS
    return entry


def load_inventory(path: str | None = None, env: dict | None = None) -> dict[str, dict]:
    """``host name (lowercase) -> entry``; empty when unset, unreadable or bad.

    Never raises. A partial inventory keeps the entries it *can* use and reports
    the ones it cannot, so one typo does not disable every other host.
    """
    resolved = (path or inventory_path(env) or "").strip()
    if not resolved:
        warn_once(
            "inventory.unset",
            f"{INVENTORY_ENV} is not set: no fleet addresses are known, so every "
            "feature that needs a host address (restarts, token fix, host-scoped "
            "dispatch) stays disabled",
        )
        return {}
    try:
        raw = json.loads(Path(resolved).expanduser().read_text())
    except OSError as exc:
        warn_once(
            f"inventory.read:{resolved}",
            f"fleet inventory {resolved} could not be read ({exc}); no fleet "
            "addresses are known and host-addressed actions stay disabled",
        )
        return {}
    except ValueError as exc:
        warn_once(
            f"inventory.json:{resolved}",
            f"fleet inventory {resolved} is not valid JSON ({exc}); no fleet "
            "addresses are known and host-addressed actions stay disabled",
        )
        return {}

    hosts = raw.get("hosts") if isinstance(raw, dict) else None
    if not isinstance(hosts, dict):
        # Accept a bare `{name: {address: ...}}` map, but say so: a file with the
        # wrong shape must not look like a valid one that happens to be empty.
        if isinstance(raw, dict) and raw and all(isinstance(v, dict) for v in raw.values()):
            warn_once(
                f"inventory.shape:{resolved}",
                f"fleet inventory {resolved} has no 'hosts' object; reading the top-level object as the host map",
            )
            hosts = raw
        else:
            warn_once(
                f"inventory.shape:{resolved}",
                f"fleet inventory {resolved} does not contain a 'hosts' object; "
                "no fleet addresses are known and host-addressed actions stay disabled",
            )
            return {}

    out: dict[str, dict] = {}
    for name, value in hosts.items():
        key = str(name).strip().lower()
        if not key:
            continue
        entry = _normalize(key, value)
        if entry is None:
            warn_once(
                f"inventory.entry:{resolved}:{key}",
                f"fleet inventory {resolved}: host {key!r} names no address; ignored",
            )
            continue
        out[key] = entry
    if not out:
        warn_once(
            f"inventory.empty:{resolved}",
            f"fleet inventory {resolved} declares no usable host; host-addressed actions stay disabled",
        )
    return out


def hosts(inventory: dict | None = None, env: dict | None = None) -> dict[str, dict]:
    """Every known host. ``inventory`` short-circuits the file read (tests)."""
    return dict(load_inventory(env=env) if inventory is None else inventory)


def host(name: str | None, inventory: dict | None = None, env: dict | None = None) -> dict | None:
    """One host's entry, or None. Names are matched case-insensitively."""
    key = str(name or "").strip().lower()
    if not key:
        return None
    return hosts(inventory, env).get(key)


def address(name: str | None, inventory: dict | None = None, env: dict | None = None) -> str:
    """One host's address, or ``""`` — never a guess, never the hostname."""
    entry = host(name, inventory, env) or {}
    return str(entry.get("address") or "")


def names(inventory: dict | None = None, env: dict | None = None) -> list[str]:
    """Known host names, sorted — for a prompt or an error message."""
    return sorted(hosts(inventory, env))
