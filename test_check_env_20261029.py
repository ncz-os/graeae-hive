"""G1: the environment readiness gate for the F1 env-only change.

F1 (operator rule 2026-10-29) moved every bus URL, MNEMOS URL, doctor model
endpoint and fleet address out of the source and into the environment. The
failure mode that creates is a doctor that starts happily with nothing set and
is then quietly blind: one log line, the feature off, the doctor still polling.

``scripts/check_env.py`` is the gate that makes that loud. These tests cover
the failure modes it has to catch, and the two rules it must never break:

  * a secret's value is never printed, and
  * the exit code is 0 only when every required variable passed.

Also covered: the machine-readable requirements file and the markdown it
mirrors must not drift apart, because the checker trusts the JSON and the
operator reads the markdown.
"""

from __future__ import annotations

import importlib.util
import io
import json
import pathlib
import re
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

ROOT = pathlib.Path(__file__).parent
REQUIREMENTS = ROOT / "docs" / "environment-requirements.json"
ENVIRONMENT_DOC = ROOT / "docs" / "ENVIRONMENT.md"


def _load_checker():
    """Load scripts/check_env.py without installing anything.

    The module is registered in ``sys.modules`` before it is executed: on
    Python 3.14 ``dataclasses`` looks the defining module up by name while the
    class body is being built, and fails if it is not there.
    """
    spec = importlib.util.spec_from_file_location("check_env", ROOT / "scripts" / "check_env.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


check_env = _load_checker()

#: A complete, valid environment. Tests copy it and break exactly one thing.
GOOD_ENV = {
    "HIVE_URL": "http://bus.example.invalid:5005",
    "HIVE_BUS_TOKEN": "worker-token-value",
    "HIVE_BUS_ADMIN_TOKEN": "admin-token-value",
    "HIVE_FLEET_INVENTORY": "",  # filled in per-test with a real file
    "ARGONAS_SSH_HOST": "argonas.example.invalid",
    "ARGONAS_SSH_PASS": "ssh-password-value",
    "DOCTOR_OPENWEIGHT_BASE_URL": "http://openweight.example.invalid:8080/v1",
    "DOCTOR_TRIAGE_BASE_URL": "http://triage.example.invalid:8080/v1",
    "DOCTOR_REVIEW_BASE_URL": "http://review.example.invalid:8080/v1",
    "DOCTOR_FIX_BASE_URL": "http://fix.example.invalid:8006/v1",
    "DOCTOR_FALLBACK_BASE_URLS": "http://a.example.invalid:8080/v1,http://b.example.invalid:8080/v1",
    "DOCTOR_FALLBACK_MODELS": "gemma4-e2b,gemma4-31b",
    "DOCTOR_OPENWEIGHT_MODEL": "gpt-oss-20b",
    "DOCTOR_OPENWEIGHT_API_KEY": "openweight-api-key-value",
    "DOCTOR_REVIEWER_HEALTH_URL": "http://review.example.invalid:8080/health",
    "HIVE_TYDEUS_BASE_URL": "http://strict.example.invalid:8006/v1",
    "MNEMOS_URL": "http://mnemos.example.invalid:5002",
    "MNEMOS_TOKEN": "mnemos-token-value",
    "HIVE_BUS_URL": "http://bus.example.invalid:5005",
    "CIXMINI_IP": "cixmini.example.invalid",
    "DOCTOR_CIXMINI_SUDO_USER": "mini",
    "CIXMINI_SSH_PASSWORD": "cixmini-password-value",
}


@pytest.fixture
def inventory(tmp_path):
    """A valid fleet inventory file."""
    path = tmp_path / "fleet-inventory.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "hosts": {
                    "alpha": {"address": "192.0.2.10", "user": "u", "auth": "key", "os": "linux"},
                },
            }
        )
    )
    return path


@pytest.fixture
def env(inventory):
    """GOOD_ENV with the inventory pointed at a real file."""
    out = dict(GOOD_ENV)
    out["HIVE_FLEET_INVENTORY"] = str(inventory)
    return out


def _report(env_overrides=None, base=None):
    """Run the checker over a copy of the good environment."""
    source = dict(base if base is not None else GOOD_ENV)
    source.update(env_overrides or {})
    return check_env.check_environment(source)


def _by_name(report, name):
    for result in report.results:
        if result.name == name:
            return result
    raise AssertionError(f"no result for {name}")


def _write_env(directory, source):
    """Write an environment dict to a KEY=VALUE file and return its path."""
    path = pathlib.Path(directory) / "doctor.env"
    path.write_text("".join(f"{k}={v}\n" for k, v in source.items()))
    return str(path)


# ── the requirements file ───────────────────────────────────────────────────


def test_requirements_file_exists_and_parses():
    specs = check_env.load_requirements(REQUIREMENTS)
    assert specs, "docs/environment-requirements.json must list the variables"
    assert len(specs) >= 15


def test_every_requirement_uses_a_known_kind():
    for spec in check_env.load_requirements(REQUIREMENTS):
        assert spec["kind"] in check_env.KNOWN_KINDS, f"{spec.get('name')} has kind {spec.get('kind')!r}"


def test_every_requirement_has_a_name_and_a_meaning():
    for spec in check_env.load_requirements(REQUIREMENTS):
        assert spec.get("name"), "every requirement needs a name"
        assert spec.get("meaning"), f"{spec['name']} needs a meaning"


def test_missing_requirements_file_yields_no_specs(tmp_path):
    assert check_env.load_requirements(tmp_path / "nope.json") == []


def test_malformed_requirements_file_yields_no_specs(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("{not json")
    assert check_env.load_requirements(path) == []


def test_requirements_file_has_no_real_addresses():
    """The example values must be placeholders: no live host, no real token."""
    text = REQUIREMENTS.read_text()
    assert "192.168." not in text, "a private fleet address leaked into the requirements file"
    ip_literal = re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b")
    for line in text.splitlines():
        if '"example"' not in line:
            continue
        value = line.split(":", 1)[1].strip().rstrip(",").strip('"')
        if not value:
            continue
        assert not ip_literal.search(value), f"example {value!r} contains an IP address"
        # A placeholder is angle-bracketed, a URL, a path, or a model id (which
        # has no dot in it). A bare dotted value is a hostname, and would be real.
        is_placeholder = "<" in value or "://" in value or "/" in value or "." not in value
        assert is_placeholder, f"example {value!r} is not a placeholder"


# ── the doc and the JSON must agree ─────────────────────────────────────────


def _documented_variables():
    """Every variable named in a markdown table row of ENVIRONMENT.md.

    A variable name is an upper-case environment-variable name. The filter
    matters: the same page has tables whose first column is a number (the exit
    codes) and those are not variables.
    """
    names = set()
    for line in ENVIRONMENT_DOC.read_text().splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) < 2:
            continue
        first = cells[0]
        if not first.startswith("`"):
            continue
        name = first.strip("`")
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", name):
            continue
        names.add(name)
    return names


def test_every_requirement_is_documented():
    documented = _documented_variables()
    for spec in check_env.load_requirements(REQUIREMENTS):
        assert spec["name"] in documented, f"{spec['name']} is in the JSON but not in docs/ENVIRONMENT.md"


def test_every_documented_variable_is_a_requirement():
    documented = _documented_variables()
    known = {spec["name"] for spec in check_env.load_requirements(REQUIREMENTS)}
    assert documented - known == set(), f"documented but not checked: {sorted(documented - known)}"


# ── the happy path ──────────────────────────────────────────────────────────


def test_a_complete_environment_is_ready(env):
    report = _report(base=env)
    assert report.ready, [r.name for r in report.failures]
    assert report.failures == []


def test_every_variable_is_checked(env):
    report = _report(base=env)
    assert len(report.results) == len(check_env.load_requirements(REQUIREMENTS))


def test_required_and_optional_are_distinguished(env):
    report = _report(base=env)
    required = {r.name for r in report.results if r.required}
    optional = {r.name for r in report.results if not r.required}
    assert "HIVE_URL" in required
    assert "HIVE_BUS_TOKEN" in optional


def test_an_optional_variable_failing_does_not_gate(env):
    report = _report({"HIVE_BUS_TOKEN": ""}, base=env)
    assert report.ready
    assert not _by_name(report, "HIVE_BUS_TOKEN").ok
    assert _by_name(report, "HIVE_BUS_TOKEN") in report.optional_failures


# ── missing and empty ───────────────────────────────────────────────────────


def test_a_missing_required_variable_fails(env):
    env.pop("HIVE_URL")
    report = _report(base=env)
    assert not report.ready
    assert _by_name(report, "HIVE_URL").status == "FAIL"


def test_an_empty_required_variable_fails(env):
    report = _report({"MNEMOS_URL": ""}, base=env)
    assert not report.ready
    assert _by_name(report, "MNEMOS_URL").status == "FAIL"


def test_a_whitespace_only_required_variable_fails(env):
    report = _report({"DOCTOR_FIX_BASE_URL": "   "}, base=env)
    assert not report.ready
    assert _by_name(report, "DOCTOR_FIX_BASE_URL").status == "FAIL"


def test_the_inventory_variable_is_required(env):
    env.pop("HIVE_FLEET_INVENTORY")
    report = _report(base=env)
    assert not report.ready
    assert _by_name(report, "HIVE_FLEET_INVENTORY").status == "FAIL"


# ── URL well-formedness ─────────────────────────────────────────────────────


def test_a_url_that_does_not_parse_fails(env):
    report = _report({"HIVE_URL": "http://[::1"}, base=env)
    assert not report.ready
    assert "parse" in _by_name(report, "HIVE_URL").detail


def test_a_url_with_a_non_http_scheme_fails(env):
    report = _report({"HIVE_URL": "ftp://bus.example.invalid:5005"}, base=env)
    assert not report.ready
    assert "scheme" in _by_name(report, "HIVE_URL").detail


def test_a_url_with_no_host_fails(env):
    report = _report({"HIVE_URL": "http://"}, base=env)
    assert not report.ready


def test_a_url_with_embedded_credentials_fails(env):
    report = _report({"HIVE_URL": "http://user:hunter2@bus.example.invalid:5005"}, base=env)
    assert not report.ready
    assert "credentials" in _by_name(report, "HIVE_URL").detail


def test_a_url_with_embedded_credentials_never_prints_the_password(env):
    report = _report({"HIVE_URL": "http://user:hunter2@bus.example.invalid:5005"}, base=env)
    line = check_env.format_result(_by_name(report, "HIVE_URL"))
    assert "hunter2" not in line
    assert "***@" in line


def test_a_url_list_with_one_bad_entry_fails(env):
    report = _report({"DOCTOR_FALLBACK_BASE_URLS": "http://ok.example.invalid:8080/v1,not-a-url"}, base=env)
    result = _by_name(report, "DOCTOR_FALLBACK_BASE_URLS")
    assert not result.ok
    assert "entry" in result.detail


def test_a_url_list_with_an_empty_entry_fails(env):
    report = _report(
        {"DOCTOR_FALLBACK_BASE_URLS": "http://ok.example.invalid:8080/v1,,http://b.example.invalid"}, base=env
    )
    assert not _by_name(report, "DOCTOR_FALLBACK_BASE_URLS").ok


def test_a_good_url_list_passes(env):
    report = _report(base=env)
    assert _by_name(report, "DOCTOR_FALLBACK_BASE_URLS").ok


def test_a_text_list_with_an_empty_entry_fails(env):
    report = _report({"DOCTOR_FALLBACK_MODELS": "gemma4-e2b,,gemma4-31b"}, base=env)
    assert not _by_name(report, "DOCTOR_FALLBACK_MODELS").ok


def test_a_malformed_url_list_is_reported_even_though_the_variable_is_optional(env):
    """An optional variable that is set but broken is still worth reporting."""
    report = _report({"DOCTOR_FALLBACK_BASE_URLS": "nonsense"}, base=env)
    assert report.ready, "an optional variable must not gate the deploy"
    assert not _by_name(report, "DOCTOR_FALLBACK_BASE_URLS").ok
    assert _by_name(report, "DOCTOR_FALLBACK_BASE_URLS") in report.optional_failures


# ── the inventory ───────────────────────────────────────────────────────────


def test_a_missing_inventory_file_fails(env, tmp_path):
    report = _report({"HIVE_FLEET_INVENTORY": str(tmp_path / "absent.json")}, base=env)
    assert not report.ready
    assert "does not exist" in _by_name(report, "HIVE_FLEET_INVENTORY").detail


def test_a_malformed_inventory_file_fails(env, tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("{not json")
    report = _report({"HIVE_FLEET_INVENTORY": str(path)}, base=env)
    assert not report.ready
    assert "not valid JSON" in _by_name(report, "HIVE_FLEET_INVENTORY").detail


def test_an_inventory_with_no_hosts_fails(env, tmp_path):
    path = tmp_path / "empty.json"
    path.write_text(json.dumps({"schema_version": 1, "hosts": {}}))
    report = _report({"HIVE_FLEET_INVENTORY": str(path)}, base=env)
    assert not report.ready
    assert "no hosts" in _by_name(report, "HIVE_FLEET_INVENTORY").detail


def test_an_inventory_whose_hosts_have_no_address_fails(env, tmp_path):
    path = tmp_path / "noaddr.json"
    path.write_text(json.dumps({"schema_version": 1, "hosts": {"alpha": {"user": "u"}}}))
    report = _report({"HIVE_FLEET_INVENTORY": str(path)}, base=env)
    assert not report.ready
    assert "no host with an address" in _by_name(report, "HIVE_FLEET_INVENTORY").detail


def test_an_inventory_that_is_a_directory_fails(env, tmp_path):
    report = _report({"HIVE_FLEET_INVENTORY": str(tmp_path)}, base=env)
    assert not report.ready


def test_an_inventory_with_the_legacy_ip_key_passes(env, tmp_path):
    path = tmp_path / "legacy.json"
    path.write_text(json.dumps({"schema_version": 1, "hosts": {"alpha": {"ip": "192.0.2.10"}}}))
    report = _report({"HIVE_FLEET_INVENTORY": str(path)}, base=env)
    assert _by_name(report, "HIVE_FLEET_INVENTORY").ok


# ── secrets are never printed ───────────────────────────────────────────────


def test_a_secret_value_is_never_printed(env):
    report = _report(base=env)
    line = check_env.format_result(_by_name(report, "HIVE_BUS_ADMIN_TOKEN"))
    assert "admin-token-value" not in line
    assert "set" in line


def test_the_whole_report_contains_no_secret_value(env):
    report = _report(base=env)
    buf = io.StringIO()
    check_env.format_report(report, buf)
    text = buf.getvalue()
    for secret in (
        "admin-token-value",
        "worker-token-value",
        "mnemos-token-value",
        "openweight-api-key-value",
        "ssh-password-value",
        "cixmini-password-value",
    ):
        assert secret not in text


def test_redaction_is_by_name_not_by_kind():
    """A variable nobody marked secret is still redacted if its name says so."""
    assert check_env.is_secret_name("DOCTOR_OPENWEIGHT_API_KEY")
    assert check_env.is_secret_name("some_token")
    assert check_env.is_secret_name("PASSWORD")
    assert check_env.is_secret_name("ARGONAS_SSH_PASS")
    assert not check_env.is_secret_name("HIVE_URL")
    assert not check_env.is_secret_name("DOCTOR_OPENWEIGHT_MODEL")


def test_a_secret_that_fails_does_not_print_its_value(env):
    report = _report({"HIVE_BUS_ADMIN_TOKEN": "   "}, base=env)
    line = check_env.format_result(_by_name(report, "HIVE_BUS_ADMIN_TOKEN"))
    assert "FAIL" in line
    assert "admin-token-value" not in line


def test_the_json_report_hides_secret_values(env):
    report = _report(base=env)
    payload = check_env.report_to_json(report)
    text = json.dumps(payload)
    assert "admin-token-value" not in text
    entry = next(v for v in payload["variables"] if v["name"] == "HIVE_BUS_ADMIN_TOKEN")
    assert entry["value"] is None


# ── the probe ───────────────────────────────────────────────────────────────


class _Handler(BaseHTTPRequestHandler):
    status = 200
    body = b'{"ok": true}'

    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
        self.send_response(self.status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(self.body)))
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, *args):
        pass


def _serve(handler_cls):
    server = HTTPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{server.server_port}"


def test_the_probe_reports_the_status_only(env):
    server, base = _serve(_Handler)
    try:
        result = check_env.probe_bus({**env, "HIVE_URL": base})
        assert result.ok
        assert "HTTP 200" in result.detail
        assert "/v1/agents/stats" in result.detail
    finally:
        server.shutdown()


def test_the_probe_never_prints_the_body(env):
    server, base = _serve(_Handler)
    try:
        result = check_env.probe_bus({**env, "HIVE_URL": base})
        line = check_env.format_result(result)
        assert "true" not in line
        assert "HTTP 200" in line
    finally:
        server.shutdown()


def test_the_probe_fails_when_the_bus_does_not_answer(env):
    result = check_env.probe_bus({**env, "HIVE_URL": "http://127.0.0.1:1"}, timeout=1.0)
    assert not result.ok
    assert "failed" in result.detail


def test_the_probe_without_a_bus_url_fails(env):
    result = check_env.probe_bus({**env, "HIVE_URL": ""})
    assert not result.ok


def test_the_probe_with_a_credentialed_url_is_refused(env):
    result = check_env.probe_bus({**env, "HIVE_URL": "http://u:p@bus.example.invalid:5005"})
    assert not result.ok
    assert "cannot probe" in result.detail


# ── the exit code ───────────────────────────────────────────────────────────


def test_exit_zero_when_every_required_variable_passes(env, tmp_path, capsys):
    code = check_env.main(["--env-file", _write_env(tmp_path, env)])
    assert code == check_env.EXIT_OK
    assert "READY" in capsys.readouterr().out


def test_exit_one_when_a_required_variable_is_missing(tmp_path):
    path = tmp_path / "doctor.env"
    path.write_text("HIVE_URL=http://bus.example.invalid:5005\n")
    assert check_env.main(["--env-file", str(path)]) == check_env.EXIT_ENV


def test_exit_one_when_the_requirements_file_cannot_be_read(tmp_path):
    assert check_env.main(["--requirements", str(tmp_path / "nope.json")]) == check_env.EXIT_ENV


def test_exit_one_when_a_required_variable_is_malformed(tmp_path):
    path = tmp_path / "doctor.env"
    path.write_text("HIVE_URL=ftp://bus.example.invalid:5005\n")
    assert check_env.main(["--env-file", str(path)]) == check_env.EXIT_ENV


def test_exit_two_when_the_probe_fails(env, tmp_path, capsys):
    path = tmp_path / "doctor.env"
    broken = dict(env)
    broken["HIVE_URL"] = "http://127.0.0.1:1"
    path.write_text("".join(f"{k}={v}\n" for k, v in broken.items()))
    code = check_env.main(["--env-file", str(path), "--probe", "--probe-timeout", "1"])
    assert code == check_env.EXIT_PROBE
    assert "PROBE FAILED" in capsys.readouterr().out


def test_exit_zero_with_a_probe_that_answers(env, tmp_path, capsys):
    server, base = _serve(_Handler)
    try:
        path = tmp_path / "doctor.env"
        good = dict(env)
        good["HIVE_URL"] = base
        path.write_text("".join(f"{k}={v}\n" for k, v in good.items()))
        code = check_env.main(["--env-file", str(path), "--probe", "--probe-timeout", "2"])
        assert code == check_env.EXIT_OK
        assert "HTTP 200" in capsys.readouterr().out
    finally:
        server.shutdown()


def test_the_json_output_is_valid_json(env, capsys):
    check_env.main(["--env-file", _write_env(pathlib.Path("/tmp"), env), "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert payload["ready"] is True
    assert len(payload["variables"]) == len(check_env.load_requirements(REQUIREMENTS))


# ── env files ───────────────────────────────────────────────────────────────


def test_env_file_parsing_handles_comments_and_quotes(tmp_path):
    path = tmp_path / "x.env"
    path.write_text('# a comment\n\nexport A="one"\nB=two\nC=\nnotakeyvalue\n')
    parsed = check_env.load_env_file(path)
    assert parsed == {"A": "one", "B": "two", "C": ""}
