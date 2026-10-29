#!/usr/bin/env python3
"""Is this environment ready to deploy the doctor (and the rest of the fleet)?

F1 (operator rule 2026-10-29) made the bus URL, the MNEMOS URL, the doctor
model endpoints and the fleet addresses environment-only. That is the right
shape — an address belongs to the environment, not to the source — but it moves
the failure mode: a container started without the variables now runs *quietly
and wrongly* instead of loudly and wrongly. The doctor logs one line, disables
the feature it needed, and keeps polling. Nothing is broken; the doctor is just
blind, and you find out when a job silently never gets triaged.

This script is the pre-deploy gate that makes the failure loud instead.

    python scripts/check_env.py                 # check every required variable
    python scripts/check_env.py --probe         # ... and ask the bus once
    python scripts/check_env.py --env-file /etc/graeae/doctor.env
    python scripts/check_env.py --json          # machine-readable

What it checks, per variable, from ``docs/environment-requirements.json`` (the
machine-readable twin of ``docs/ENVIRONMENT.md``):

  url        parses, scheme is http/https, a host is present, no credentials
             embedded in the URL
  url_list   every comma-separated entry satisfies the url rules
  text       set and non-empty
  text_list  every comma-separated entry is non-empty
  secret     set and non-empty; the value is NEVER printed
  inventory  the file exists, is readable, is valid JSON and declares at least
             one usable host

Secret values are never printed. The rule is by NAME, not by kind: any
variable whose name contains token, key, secret, password, passwd or credential
is reported as ``set (len N)`` and nothing else, so a token that is accidentally
typed into a non-secret variable is still not echoed. A URL that turns out to
carry credentials is reported with the userinfo replaced by ``***@`` rather
than as the whole URL.

Exit codes:

    0  ready: every required variable passed (and the probe answered, if asked)
    1  not ready: at least one required variable is missing or malformed
    2  the variables are fine but the probe did not answer

The probe is deliberately bounded: one GET, a short timeout, and the response
body is read up to a cap and discarded. Only the status is reported.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import pathlib
import re
import sys
from dataclasses import dataclass, field
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

ROOT = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_REQUIREMENTS = ROOT / "docs" / "environment-requirements.json"

EXIT_OK = 0
EXIT_ENV = 1
EXIT_PROBE = 2

#: A name that matches this is never echoed. By NAME and not by kind, so a
#: token that lands in a variable someone forgot to mark secret still is not
#: printed. ``ARGONAS_SSH_PASS`` is covered by the ``_pass`` suffix rule.
SECRET_NAME_RE = re.compile(r"token|key|secret|password|passwd|credential", re.IGNORECASE)
SECRET_NAME_SUFFIXES = ("_PASS", "-PASS", ".PASS")

#: Kinds the checker knows how to validate. An unknown kind is an error in the
#: requirements file, not a silently skipped check.
KNOWN_KINDS = {"url", "url_list", "text", "text_list", "secret", "inventory"}

#: How much of the probe's response body is read before it is thrown away.
PROBE_READ_CAP = 4096


def is_secret_name(name: str) -> bool:
    """True when a variable's value must never be printed."""
    upper = str(name or "")
    if SECRET_NAME_RE.search(upper):
        return True
    return upper.endswith(SECRET_NAME_SUFFIXES)


def redact_userinfo(raw: str) -> str:
    """A URL with any embedded credentials replaced by ``***@``.

    Used only for reporting. A URL with a password in it is exactly the thing
    that must not reach a log line or a terminal scrollback.
    """
    try:
        parts = urlsplit(raw)
    except ValueError:
        return "<unparsable url>"
    host = parts.hostname or ""
    try:
        port = parts.port
    except ValueError:
        port = None
    netloc = f"{host}:{port}" if port else host
    if parts.username is not None or parts.password is not None:
        netloc = f"***@{netloc}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, ""))


@dataclass
class Result:
    """The outcome of checking one variable."""

    name: str
    kind: str
    ok: bool
    detail: str = ""
    required: bool = False
    secret: bool = False
    shown: str = ""

    @property
    def status(self) -> str:
        return "ok" if self.ok else "FAIL"


@dataclass
class Report:
    """Everything the checker found."""

    results: list[Result] = field(default_factory=list)
    probe: Result | None = None

    @property
    def failures(self) -> list[Result]:
        return [r for r in self.results if not r.ok and r.required]

    @property
    def optional_failures(self) -> list[Result]:
        return [r for r in self.results if not r.ok and not r.required]

    @property
    def ready(self) -> bool:
        return not self.failures


# --------------------------------------------------------------------------- #
# per-kind validators
# --------------------------------------------------------------------------- #


def check_url(raw: str) -> tuple[bool, str]:
    """A URL that parses, is http(s), names a host and carries no credentials."""
    value = (raw or "").strip()
    if not value:
        return False, "is not set"
    try:
        parts = urlsplit(value)
    except ValueError as exc:
        return False, f"does not parse as a URL ({exc})"
    if parts.scheme not in ("http", "https"):
        return False, f"scheme is {parts.scheme or 'missing'!r}, expected http or https"
    if not parts.hostname:
        return False, "names no host"
    if parts.username is not None or parts.password is not None:
        return False, f"embeds credentials ({redact_userinfo(value)})"
    return True, ""


def check_url_list(raw: str) -> tuple[bool, str]:
    """Every comma-separated entry is a valid URL."""
    value = (raw or "").strip()
    if not value:
        return False, "is not set"
    parts = [p.strip() for p in value.split(",")]
    if not any(parts):
        return False, "is not set"
    for part in parts:
        if not part:
            return False, "has an empty entry"
        ok, detail = check_url(part)
        if not ok:
            return False, f"entry {redact_userinfo(part)} {detail}"
    return True, ""


def check_text(raw: str) -> tuple[bool, str]:
    """Set and non-empty."""
    value = (raw or "").strip()
    if not value:
        return False, "is not set"
    return True, ""


def check_text_list(raw: str) -> tuple[bool, str]:
    """Set, and every comma-separated entry is non-empty."""
    value = (raw or "").strip()
    if not value:
        return False, "is not set"
    parts = [p.strip() for p in value.split(",")]
    if not any(parts):
        return False, "is not set"
    if any(not p for p in parts):
        return False, "has an empty entry"
    return True, ""


def check_secret(raw: str) -> tuple[bool, str]:
    """Set and non-empty. The value never appears in the result."""
    value = raw or ""
    if not value.strip():
        return False, "is not set"
    return True, ""


def check_inventory(raw: str) -> tuple[bool, str]:
    """The inventory file is readable, valid JSON and declares a usable host."""
    value = (raw or "").strip()
    if not value:
        return False, "is not set"
    path = pathlib.Path(value).expanduser()
    if not path.exists():
        return False, "inventory file does not exist"
    if not path.is_file():
        return False, "inventory path is not a regular file"
    try:
        text = path.read_text()
    except OSError as exc:
        return False, f"inventory file could not be read ({exc.strerror or exc})"
    try:
        parsed = json.loads(text)
    except ValueError as exc:
        return False, f"inventory file is not valid JSON ({exc})"
    if not isinstance(parsed, dict):
        return False, "inventory file is not a JSON object"
    hosts = parsed.get("hosts")
    if not isinstance(hosts, dict):
        # The loader also accepts a bare {name: {...}} map; check it the same way
        # so the checker and the loader never disagree about what is usable.
        hosts = parsed if parsed and all(isinstance(v, dict) for v in parsed.values()) else None
    if not isinstance(hosts, dict) or not hosts:
        return False, "inventory file declares no hosts"
    usable = 0
    for entry in hosts.values():
        if not isinstance(entry, dict):
            continue
        address = entry.get("address") or entry.get("ip")
        if str(address or "").strip():
            usable += 1
    if not usable:
        return False, "inventory file declares no host with an address"
    return True, ""


VALIDATORS = {
    "url": check_url,
    "url_list": check_url_list,
    "text": check_text,
    "text_list": check_text_list,
    "secret": check_secret,
    "inventory": check_inventory,
}


# --------------------------------------------------------------------------- #
# requirements
# --------------------------------------------------------------------------- #


def load_requirements(path: str | pathlib.Path | None = None) -> list[dict]:
    """The variable list, or ``[]`` when the file is missing or malformed."""
    target = pathlib.Path(path) if path else DEFAULT_REQUIREMENTS
    try:
        raw = json.loads(target.read_text())
    except (OSError, ValueError):
        return []
    if not isinstance(raw, dict):
        return []
    variables = raw.get("variables")
    return [v for v in variables if isinstance(v, dict)] if isinstance(variables, list) else []


def required_for(spec: dict, profile: str) -> bool:
    """Whether this variable gates the given profile."""
    if not spec.get("required"):
        return False
    profiles = spec.get("profiles")
    if not profiles:
        return True
    if profile == "all":
        return True
    return profile in profiles


# --------------------------------------------------------------------------- #
# checking
# --------------------------------------------------------------------------- #


def check_variable(spec: dict, env: dict, profile: str = "doctor") -> Result:
    """Check one variable against ``env``."""
    name = str(spec.get("name") or "")
    kind = str(spec.get("kind") or "")
    secret = is_secret_name(name)
    required = required_for(spec, profile)
    raw = env.get(name, "")
    if kind not in VALIDATORS:
        return Result(name=name, kind=kind, ok=False, detail=f"unknown kind {kind!r}", required=required, secret=secret)
    ok, detail = VALIDATORS[kind](raw if isinstance(raw, str) else str(raw))
    shown = ""
    if ok and not secret and kind in ("url", "url_list") and raw:
        shown = redact_userinfo(raw if isinstance(raw, str) else str(raw))
    return Result(name=name, kind=kind, ok=ok, detail=detail, required=required, secret=secret, shown=shown)


def check_environment(
    env: dict | None = None,
    requirements: list[dict] | None = None,
    profile: str = "doctor",
) -> Report:
    """Check every variable in the requirements list against ``env``."""
    source = os.environ if env is None else env
    specs = load_requirements() if requirements is None else requirements
    report = Report(results=[check_variable(spec, source, profile) for spec in specs])
    return report


def probe_bus(env: dict | None = None, timeout: float = 5.0) -> Result:
    """One bounded GET against the bus. Status only; the body is discarded.

    The endpoint is ``HIVE_URL/v1/agents/stats``: a cheap read that proves the
    bus is up and answering on the configured address. It is a reachability
    check, not a health check — a 401 still proves the bus answered.
    """
    source = os.environ if env is None else env
    base = str(source.get("HIVE_URL") or "").strip().rstrip("/")
    if not base:
        return Result(name="HIVE_URL", kind="url", ok=False, detail="is not set, cannot probe")
    ok, detail = check_url(base)
    if not ok:
        return Result(name="HIVE_URL", kind="url", ok=False, detail=f"cannot probe: HIVE_URL {detail}")
    url = base + "/v1/agents/stats"
    request = Request(url, method="GET", headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:  # noqa: S310 - operator-configured URL
            status = getattr(response, "status", None) or response.getcode()
            response.read(PROBE_READ_CAP)
    except HTTPError as exc:
        return Result(name="HIVE_URL", kind="url", ok=False, detail=f"probe {url} answered HTTP {exc.code}")
    except (URLError, OSError) as exc:
        reason = getattr(exc, "reason", exc)
        return Result(name="HIVE_URL", kind="url", ok=False, detail=f"probe {url} failed ({reason})")
    return Result(name="HIVE_URL", kind="url", ok=True, detail=f"probe {url} answered HTTP {status}")


# --------------------------------------------------------------------------- #
# environment files
# --------------------------------------------------------------------------- #


def load_env_file(path: str | pathlib.Path) -> dict:
    """Read a KEY=VALUE file into a dict. Blank lines and #comments ignored."""
    out: dict[str, str] = {}
    for raw_line in pathlib.Path(path).read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if key:
            out[key] = value
    return out


# --------------------------------------------------------------------------- #
# reporting
# --------------------------------------------------------------------------- #


def format_result(result: Result) -> str:
    """One line per variable. Never contains a secret value."""
    if result.ok:
        if result.secret:
            return f"{result.name}: ok (set)"
        if result.shown:
            return f"{result.name}: ok ({result.shown})"
        if result.detail:
            return f"{result.name}: ok - {result.detail}"
        return f"{result.name}: ok"
    suffix = f" - {result.detail}" if result.detail else ""
    return f"{result.name}: FAIL{suffix}"


def format_report(report: Report, stream=None) -> None:
    """The human-readable report.

    ``stream`` defaults to the *current* ``sys.stdout``, looked up at call time
    rather than bound at import time: a caller that redirects stdout after the
    module was imported (a test harness, a pager) still gets the report.
    """
    out = stream if stream is not None else sys.stdout
    for result in report.results:
        out.write(format_result(result) + "\n")
    if report.probe is not None:
        out.write(format_result(report.probe) + "\n")
    failures = report.failures
    optional = report.optional_failures
    out.write("\n")
    if failures:
        out.write(f"NOT READY: {len(failures)} required variable(s) failed.\n")
    else:
        out.write("READY: every required variable passed.\n")
    if optional:
        out.write(f"Note: {len(optional)} optional variable(s) failed (not a gate).\n")
    if report.probe is not None and not report.probe.ok:
        out.write("PROBE FAILED: the variables are set but the bus did not answer.\n")


def report_to_json(report: Report) -> dict:
    """The machine-readable report."""
    return {
        "ready": report.ready,
        "variables": [
            {
                "name": r.name,
                "kind": r.kind,
                "ok": r.ok,
                "required": r.required,
                "secret": r.secret,
                "detail": r.detail,
                "value": None if r.secret else (r.shown or None),
            }
            for r in report.results
        ],
        "probe": None if report.probe is None else {"ok": report.probe.ok, "detail": report.probe.detail},
    }


# --------------------------------------------------------------------------- #
# entry point
# --------------------------------------------------------------------------- #


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="check_env.py",
        description="Check that the environment is ready for the doctor (and the rest of the fleet).",
    )
    parser.add_argument(
        "--requirements",
        default=str(DEFAULT_REQUIREMENTS),
        help="path to the machine-readable requirements file (default: docs/environment-requirements.json)",
    )
    parser.add_argument(
        "--profile",
        default="doctor",
        help="deployment profile whose required variables gate the exit code (default: doctor)",
    )
    parser.add_argument(
        "--env-file", default=None, help="read KEY=VALUE pairs from this file instead of the process environment"
    )
    parser.add_argument("--probe", action="store_true", help="also do one bounded GET against HIVE_URL/v1/agents/stats")
    parser.add_argument("--probe-timeout", type=float, default=5.0, help="probe timeout in seconds (default: 5)")
    parser.add_argument("--json", action="store_true", help="emit the report as JSON")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the checks. Returns the exit code."""
    args = build_parser().parse_args(argv)

    # The inventory loader logs a warning per problem; the checker reports
    # problems itself, so the loader's warnings would be noise here.
    logging.getLogger("hive.fleet_inventory").setLevel(logging.ERROR)

    specs = load_requirements(args.requirements)
    if not specs:
        if args.json:
            print(json.dumps({"ready": False, "error": f"no requirements at {args.requirements}"}))
        else:
            print(f"NOT READY: no requirements could be read from {args.requirements}")
        return EXIT_ENV

    env = load_env_file(args.env_file) if args.env_file else dict(os.environ)

    report = check_environment(env, specs, args.profile)
    if args.probe:
        report.probe = probe_bus(env, timeout=args.probe_timeout)

    if args.json:
        print(json.dumps(report_to_json(report), indent=2, sort_keys=True))
    else:
        format_report(report)

    if not report.ready:
        return EXIT_ENV
    if report.probe is not None and not report.probe.ok:
        return EXIT_PROBE
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
