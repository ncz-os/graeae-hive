"""The fleet inventory: addresses come from config, and a bad file never raises.

Covers the four cases the brief names — missing, malformed, partial, and each
replaced call site — plus the "one log line" rule that keeps a crash-looping
doctor from turning a missing file into a log flood.
"""

from __future__ import annotations

import json
import logging

import pytest

from hive.core import fleet_inventory

GOOD = {
    "schema_version": 1,
    "hosts": {
        "cerberus": {"address": "192.0.2.96", "user": "jasonperlow", "auth": "key", "os": "linux"},
        "cixmini": {
            "address": "192.0.2.99",
            "user": "mini",
            "auth": "password",
            "password_env": "CIXMINI_SSH_PASSWORD",
        },
    },
}


@pytest.fixture(autouse=True)
def _fresh_warnings():
    """Warnings are once-per-process; give each test its own process-worth."""
    fleet_inventory.reset_warnings()
    yield
    fleet_inventory.reset_warnings()


def _write(tmp_path, name: str, payload) -> str:
    path = tmp_path / name
    path.write_text(payload if isinstance(payload, str) else json.dumps(payload))
    return str(path)


# ── missing ───────────────────────────────────────────────────────────────────


def test_missing_file_yields_no_hosts_and_does_not_raise(tmp_path):
    inventory = fleet_inventory.load_inventory(path=str(tmp_path / "nope.json"))
    assert inventory == {}
    assert fleet_inventory.address("cerberus", inventory=inventory) == ""
    assert fleet_inventory.names(inventory=inventory) == []


def test_unset_path_yields_no_hosts():
    assert fleet_inventory.load_inventory(env={}) == {}
    assert fleet_inventory.load_inventory(env={fleet_inventory.INVENTORY_ENV: "  "}) == {}
    assert fleet_inventory.inventory_path(env={}) == ""


def test_the_configured_path_is_read_from_the_environment(tmp_path):
    path = _write(tmp_path, "inv.json", GOOD)
    assert fleet_inventory.inventory_path(env={fleet_inventory.INVENTORY_ENV: path}) == path
    assert fleet_inventory.address("cerberus", env={fleet_inventory.INVENTORY_ENV: path}) == "192.0.2.96"


# ── malformed ─────────────────────────────────────────────────────────────────


def test_malformed_json_yields_no_hosts(tmp_path):
    assert fleet_inventory.load_inventory(path=_write(tmp_path, "bad.json", "{not json")) == {}


def test_wrong_shape_yields_no_hosts(tmp_path):
    assert fleet_inventory.load_inventory(path=_write(tmp_path, "shape.json", {"hosts": ["cerberus"]})) == {}
    assert fleet_inventory.load_inventory(path=_write(tmp_path, "none.json", [])) == {}
    assert fleet_inventory.load_inventory(path=_write(tmp_path, "str.json", '"cerberus"')) == {}


# ── partial ───────────────────────────────────────────────────────────────────


def test_partial_inventory_keeps_the_usable_hosts(tmp_path):
    path = _write(
        tmp_path,
        "partial.json",
        {
            "hosts": {
                "cerberus": {"address": "192.0.2.96"},
                "noaddress": {"user": "someone"},
                "notadict": "192.0.2.1",
                "": {"address": "192.0.2.2"},
            }
        },
    )
    inventory = fleet_inventory.load_inventory(path=path)
    assert set(inventory) == {"cerberus"}, "one unusable entry must not drop the rest"
    entry = inventory["cerberus"]
    assert entry["address"] == "192.0.2.96"
    assert entry["user"] == ""
    assert entry["auth"] == fleet_inventory.DEFAULT_AUTH
    assert entry["os"] == fleet_inventory.DEFAULT_OS


def test_legacy_ip_key_is_accepted(tmp_path):
    path = _write(tmp_path, "ip.json", {"hosts": {"ultra": {"ip": "192.0.2.60", "os": "macos"}}})
    inventory = fleet_inventory.load_inventory(path=path)
    assert inventory["ultra"]["address"] == "192.0.2.60"
    assert inventory["ultra"]["os"] == "macos"


def test_a_flat_map_is_read_and_reported(tmp_path):
    path = _write(tmp_path, "flat.json", {"cerberus": {"address": "192.0.2.96"}})
    inventory = fleet_inventory.load_inventory(path=path)
    assert inventory["cerberus"]["address"] == "192.0.2.96"


def test_names_are_lowercased_and_lookup_is_case_insensitive(tmp_path):
    path = _write(tmp_path, "case.json", {"hosts": {"CERBERUS": {"address": "192.0.2.96"}}})
    inventory = fleet_inventory.load_inventory(path=path)
    assert list(inventory) == ["cerberus"]
    assert fleet_inventory.address("CERBERUS", inventory=inventory) == "192.0.2.96"
    assert fleet_inventory.host("  cerberus ", inventory=inventory)["host"] == "cerberus"
    assert fleet_inventory.host("", inventory=inventory) is None


def test_password_env_survives_normalisation(tmp_path):
    path = _write(tmp_path, "pw.json", GOOD)
    entry = fleet_inventory.load_inventory(path=path)["cixmini"]
    assert entry["auth"] == "password"
    assert entry["password_env"] == "CIXMINI_SSH_PASSWORD"


# ── one log line ──────────────────────────────────────────────────────────────


def test_warn_once_emits_once_per_key(caplog):
    with caplog.at_level(logging.INFO, logger="hive.fleet_inventory"):
        fleet_inventory.warn_once("k", "first")
        fleet_inventory.warn_once("k", "second")
        fleet_inventory.warn_once("other", "third")
    assert [r.message for r in caplog.records] == ["first", "third"]


def test_a_missing_file_is_reported_once_even_when_read_repeatedly(tmp_path, caplog):
    missing = str(tmp_path / "nope.json")
    with caplog.at_level(logging.INFO, logger="hive.fleet_inventory"):
        for _ in range(3):
            assert fleet_inventory.load_inventory(path=missing) == {}
    assert len(caplog.records) == 1


# ── each replaced call site ───────────────────────────────────────────────────


def _inventory_for(monkeypatch, tmp_path, payload) -> str:
    path = _write(tmp_path, "inv.json", payload)
    monkeypatch.setenv(fleet_inventory.INVENTORY_ENV, path)
    return path


def test_doctor_fleet_table_is_the_inventory(monkeypatch, tmp_path):
    _inventory_for(monkeypatch, tmp_path, {"hosts": {"newbox": {"address": "192.0.2.5", "user": "u"}}})
    from hive.workers import doctor

    hosts = doctor.all_fleet_hosts()
    assert hosts["newbox"] == {"ip": "192.0.2.5", "os": "linux", "auth": "key", "user": "u"}


def test_doctor_fleet_table_is_empty_without_an_inventory(monkeypatch, tmp_path):
    monkeypatch.delenv(fleet_inventory.INVENTORY_ENV, raising=False)
    from hive.workers import doctor

    assert doctor.all_fleet_hosts() == {}


def test_restart_allowlist_is_built_from_the_inventory(monkeypatch, tmp_path):
    _inventory_for(monkeypatch, tmp_path, {"hosts": {"newbox": {"address": "192.0.2.5", "user": "u", "auth": "key"}}})
    from hive.workers import doctor_gates

    targets = doctor_gates._build_restart_targets()
    assert targets["newbox"] == {"address": "192.0.2.5", "user": "u", "auth": "key"}
    # cixmini stays static because its address and user are their own variables.
    assert "cixmini" in targets
    assert "address" not in targets["cixmini"]

    monkeypatch.setattr(doctor_gates, "RESTART_TARGETS", targets)
    resolved, reason = doctor_gates.resolve_restart_target("newbox", env={})
    assert reason == ""
    assert resolved == {"host": "newbox", "address": "192.0.2.5", "user": "u", "auth": "key"}


def test_a_host_missing_from_the_inventory_is_refused_by_name(monkeypatch, tmp_path):
    _inventory_for(monkeypatch, tmp_path, {"hosts": {}})
    from hive.workers import doctor_gates

    monkeypatch.setattr(doctor_gates, "RESTART_TARGETS", doctor_gates._build_restart_targets())
    target, reason = doctor_gates.resolve_restart_target("cerberus", env={})
    assert target is None
    assert "cerberus" in reason and "not a restart target" in reason


def test_argonas_clone_url_needs_a_configured_host(monkeypatch):
    from hive import registry

    monkeypatch.setattr(registry, "ARGONAS_SSH_HOST", "")
    assert registry.argonas_ssh_url("graeae-hive") == ""
    assert registry.discover_argonas(host=None) == set()

    monkeypatch.setattr(registry, "ARGONAS_SSH_HOST", "192.0.2.101")
    assert registry.argonas_ssh_url("graeae-hive") == "ssh://jasonperlow@192.0.2.101/mnt/datapool/git/graeae-hive.git"
