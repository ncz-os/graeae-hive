#!/usr/bin/env python3
"""
GRAEAE Hive Mind -- thin claim/execute worker.

Replaces the legacy zc-gateway/zc-worker@ (zeroclaw-stack container) +
zc_oneshot.py/zc_native_build.py build-harness path. Per GRAEAE architecture
consult 2026-09-14 (7-muse consensus, winner MiniMax-M3 0.95): the hive's
job-claim path should be a THIN wrapper that shells out to `zoder`/`codex`
and lets THEM own model selection via their own per-host config -- not a
second independent routing layer (KNEMON, or an in-process build harness).

This script claims exactly ONE kind of job (--kind zeroclaw|codex), resolves
the job's repo to a fresh per-job clone (never a shared mutable workspace --
that was zc_oneshot's bug, fixed here the same way zc_native_build.py fixed
it), runs the driving tool inside it, and reports the result. Normal zeroclaw
jobs use the host's ~/.zoder/config.toml escalation ladder. Strict rescue
jobs pin an exact local/EIH route allowlist; codex jobs always get
`codex exec -m $CODEX_MODEL` -- a single fixed rung set per host via env,
NOT negotiated per job.

Env:
  HIVE_URL            required: bus base URL. No default; unset refuses to poll
                      (see docs/ENVIRONMENT.md)
  AGENT_HOST          default hostname -s
  HIVE_WORKER_SLOT    stable per-host worker slot (default 1)
  WORKER_KIND         "zeroclaw" or "codex" (required)
  POLL_INTERVAL       seconds between /v1/jobs/next polls (default 5)
  HEARTBEAT_INTERVAL  seconds (default 20)
  JOB_TIMEOUT         seconds, hard kill of the driving subprocess (default 7200)
  WORKSPACE_ROOT       default ~/hive-workspaces
  KEEP_WORKSPACE      if "1", don't rm -rf the per-job clone after (debug)
  CODEX_MODEL         default gpt-6-luna (see note below)
  CODEX_SANDBOX       codex sandbox mode, default workspace-write.
                        danger-full-access is REFUSED unless
                        HIVE_ALLOW_UNSANDBOXED_CODEX=1 is set on the host.
  HIVE_ALLOW_UNSANDBOXED_CODEX  opt-in that permits the unsandboxed mode
  HIVE_GIT_AUTHOR_NAME   repo-local identity, default Jason Perlow
  HIVE_GIT_AUTHOR_EMAIL  repo-local identity, default jperlow@gmail.com

CODEX_MODEL note (2026-09-15): the escalation ladder's documented default
rung is gpt-5.3-codex-spark, but this account's live codex model catalog
(queried via chatgpt-api.com/backend-api/codex/models earlier this session)
does NOT serve a spark variant -- confirmed on codex-cli 0.144.1 AND after
upgrading to 0.153.3, same 400 both times: "The 'gpt-5.3-codex-spark' model
is not supported when using Codex with a ChatGPT account." Every codex-kind
hive job was failing on this, fleet-wide, regardless of host or codex
version. gpt-5.6-luna is the cheapest VALIDATED model actually served to
this account (~/.claude/rules/00-AGENT-DIRECTIVES.md escalation ladder) --
used as the default here until spark (or an equivalent low-cost rung)
actually appears in the live catalog.

2026-10-26: the gpt-5.6 family is deprecated; the default is now gpt-6-luna,
the cheapest gpt-6 rung. A codex job whose preferred_models names a different
gpt-* model fails with codex_model_mismatch instead of silently running on
this host's model (see codex_model_mismatch).

Provenance + escalation policy (2026-10-16): the worker now records the model
that ACTUALLY ran, never a guess. AGENT_MODELS maps a zoder agent alias to the
exact (model, provider, cost_tier) that alias is configured to dispatch to;
escalation_model reports what a ladder rung will really launch; and the
completion result carries ``models: {author, reviewers}`` parsed from the
zoder loop's JSON stdout. The policy itself follows
~/.claude/rules/orchestration-routing-20261014.md and the FleetOps source of
truth, gitops/fleet/zoder-config/config.toml: MiniMax-M3.1-Flash (then funded
DeepSeek) before MiniMax-M3; the TYDEUS/CERBERUS locals (coder/qwen38,
reviewer/gemma4-31b) for suitable fixes and cross-family reviews; EIH
nvidia/nemotron-3-ultra-550b-a55b health-checked as a backup reviewer only;
never Claude/Anthropic-backed, never a paid frontier reseller, never the
retired :4100 gateway.
"""

from __future__ import annotations

import calendar
import contextlib
import fcntl
import glob
import hashlib
import json
import os
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Sequence
from pathlib import Path

# The zoder engine's spend cap (2026-10-25). The exact-string matcher lives in
# its own stdlib-only module because the doctor classifies the same text and
# must not import this file (it runs where hive_worker may not be deployed,
# and the runtime import failed silently -- losing the fallback classification).
import hive_budget

# Runtime model provenance resolution (2026-10-17). The worker used to keep a
# hardcoded AGENT_MODELS table that labeled coder=nemotron35 / reviewer=qwen38;
# after the fleet rollout the bus recorded a claimed_model the fleet never
# dispatched. ``provenance`` owns the live-config resolution:
#   [agents.X].model_provider -> [providers.models.<provider>].model / uri
# with the static table as a fallback, a process-level cache, and a
# provenance_source of 'live_config' / 'static_fallback'. ``hive_worker`` is
# a thin wrapper that delegates to it.
import provenance

# Worker-owned delivery for the codex driver (fault 2026-10-17): codex's
# workspace-write sandbox keeps .git read-only by design, so the WORKER
# commits, reviews and pushes the driver's edits from outside the sandbox.
# ``fleet_git_env`` is the one ARGONAS credential helper shared by the clone,
# the zeroclaw push and the codex push.
from hive import codex_delivery, llm_roles, push_policy, review_evidence, review_record
from hive.core import fleet_inventory
from hive.codex_delivery import fleet_git_env


def _find_driver(name: str) -> str:
    """Resolve a driver binary's absolute path at import time. A bare PATH
    lookup is not enough: systemd services and non-interactive SSH sessions
    don't source the interactive shell's PATH, so `zoder`/`codex` installed
    under ~/.local/bin (the common fleet install location) silently
    FileNotFoundError otherwise -- caught deploying to PROTEUS, 2026-09-14,
    where `which zoder` in a plain ssh command found nothing despite the
    binary being present and working interactively."""
    found = shutil.which(name)
    if found:
        return found
    for candidate in (
        os.path.expanduser(f"~/.local/bin/{name}"),
        f"/usr/local/bin/{name}",
        f"/opt/homebrew/bin/{name}",
        os.path.expanduser(f"~/.npm-global/bin/{name}"),
    ):
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return name  # let it fail loudly with FileNotFoundError at exec time


def _driver_supports_verified_delivery(driver: str) -> bool:
    """Require the CLI contract used by the delivery/completion gate.

    A binary that can start but lacks these flags would either fail every
    queued job or bypass the check, so it must not register as a verified
    delivery worker during a rolling fleet update.
    """
    if WORKER_KIND == "zeroclaw":
        command = [driver, "loop", "--help"]
        required = ("--require-approval", "--approve", "--check")
    else:
        command = [driver, "exec", "--help"]
        required = ("--sandbox", "--cd")
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    output = result.stdout or ""
    if result.returncode != 0 or not all(flag in output for flag in required):
        return False
    if WORKER_KIND == "zeroclaw":
        return True
    # The sandbox must also be able to reach the network: every job delivers
    # by pushing to its origin. The worker passes
    # `-c sandbox_workspace_write.network_access=true`, so what the driver
    # must support is the `-c/--config` override. Config KEYS never appear in
    # `codex exec --help` (codex-cli 0.163), so probing for the key string
    # refused every codex worker fleet-wide (2026-10-17 crash loop).
    return "--config" in output


def _coder_base_url_ok(url: str) -> bool:
    """Whether the manifest's coder base is acceptable.

    The base address is configuration: the check this replaces compared against a
    literal fleet address, which meant every clone of the repo carried one and a
    re-addressed host failed the gate for being correct. ``HIVE_TYDEUS_BASE_URL``
    pins the exact base when the operator sets it; unset, a well-formed http(s)
    base is accepted and the weaker check is reported once.
    """
    base = str(url or "").strip()
    if not base.startswith(("http://", "https://")):
        return False
    expected = os.environ.get("HIVE_TYDEUS_BASE_URL", "").strip().rstrip("/")
    if not expected:
        fleet_inventory.warn_once(
            "worker.coder_base",
            "HIVE_TYDEUS_BASE_URL is not set: the strict-route coder base is checked "
            "for shape only, not identity (set it to pin the route)",
        )
        return True
    return base.rstrip("/") == expected


def load_strict_routes():
    """Consume the pinned FleetOps validator; never guess an unready role."""

    def require(cond, msg="strict role readiness unavailable"):
        if not cond:
            raise ValueError(msg)

    try:
        manifest = Path(
            os.environ.get("HIVE_STRICT_ROLE_CONFIG", "~/.zoder/role-readiness.json")
        ).expanduser()
        vpath = Path(
            os.environ.get("HIVE_STRICT_ROLE_VALIDATOR", "~/.zoder/role_readiness.py")
        ).expanduser()
        require(vpath.is_file(), "validator missing")
        with vpath.open("rb") as stream:
            raw = stream.read(65537)
        require(len(raw) <= 65536, "validator size")
        sha = os.environ.get("HIVE_STRICT_ROLE_VALIDATOR_SHA256", "")
        require(
            re.fullmatch(r"[0-9a-f]{64}", sha)
            and hashlib.sha256(raw).hexdigest() == sha,
            "sha mismatch",
        )
        proc = subprocess.run(
            [sys.executable, "-", "--manifest", str(manifest), "--json"],
            input=raw,
            capture_output=True,
            timeout=10,
            check=False,
        )
        require(proc.returncode == 0, "nonzero exit")
        out = proc.stdout
        require(len(out) <= 16384, "stdout too large")
        data = json.loads(out)
        require(isinstance(data, dict), "bad json")
        require(
            set(data.keys())
            == {"schema_version", "coder", "reviewer", "secondary", "eih_reviewers"},
            "keys mismatch",
        )
        require(
            type(data["schema_version"]) is int and data["schema_version"] == 1,
            "schema version",
        )
        coder = data["coder"]
        require(isinstance(coder, dict), "coder dict")
        require(coder.get("provider_id") == "tydeus-coder", "coder provider")
        require(coder.get("model_id") == "qwen38", "coder model")
        # WHICH base the coder is served on is operator config, never source
        # (operator rule 2026-10-29): with HIVE_TYDEUS_BASE_URL set the exact base
        # is pinned, and unset checks only that a real http(s) base was declared.
        require(_coder_base_url_ok(coder.get("base_url", "")), "coder base")
        reviewer = data["reviewer"]
        require(isinstance(reviewer, dict), "reviewer dict")
        require(reviewer.get("provider_id") == "cerberus-reviewer", "reviewer provider")
        model = reviewer.get("model_id", "")
        require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", model), "reviewer model")
        secondary = data["secondary"]
        eih = data["eih_reviewers"]
        require(isinstance(eih, list) and 1 <= len(eih) <= 16, "eih count")
        seen = set()
        for r in eih:
            require(isinstance(r, dict), "eih dict")
            require(r.get("provider_id") == "nvidia-eih", "eih provider")
            require(
                r.get("base_url") == "https://integrate.api.nvidia.com/v1", "eih base"
            )
            m = r.get("model_id", "")
            require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", m), "eih model")
            key = (r["provider_id"], m)
            require(key not in seen, "eih dup")
            seen.add(key)
        require(secondary in eih, "secondary not in pool")
        author = "qwen38"
        rev_model = model
        author_pair = f"tydeus-coder={author}"
        gemma_pair = f"cerberus-reviewer={rev_model}"
        eih_pairs = [f"nvidia-eih={r['model_id']}" for r in eih]
        allowed = ",".join([author_pair, gemma_pair] + eih_pairs)
        rev_allowed = ",".join([gemma_pair] + eih_pairs)
        rev_pool = ",".join(eih_pairs)
        return {
            "author": author,
            "reviewer": rev_model,
            "allowed_routes": allowed,
            "reviewer_allowed_routes": rev_allowed,
            "reviewer_pool_routes": rev_pool,
        }
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError):
        raise ValueError("strict role readiness unavailable") from None


def _driver_supports_strict_routes(driver: str) -> bool:
    try:
        result = subprocess.run(
            [driver, "loop", "--help"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    out = result.stdout or ""
    flags = set(re.findall(r"--[a-z][a-z0-9-]*", out))
    return result.returncode == 0 and all(
        f in flags
        for f in [
            "--allowed-routes",
            "--reviewer-allowed-routes",
            "--reviewer-pool-routes",
        ]
    )


# ---------------------------------------------------------------------------
# Model provenance + escalation policy.
#
# The runtime resolution (tomllib -> live config -> static fallback, with a
# process-level cache and a provenance_source of 'live_config' /
# 'static_fallback') lives in ``provenance.py`` so the worker stays a thin
# caller. ``AGENT_MODELS`` here is the STATIC FALLBACK table -- the exact
# route each zoder agent alias dispatches to per the FleetOps source of truth
# (gitops/fleet/zoder-config/config.toml), used when the host's live
# ``~/.zoder/config.toml`` cannot be read or does not define the agent:
#   [agents.coder]        -> custom.coder        -> qwen38
#   [agents.reviewer]     -> custom.reviewer     -> gemma4-31b
#   [agents.minimax]      -> custom.minimax      -> api.minimax.io MiniMax-M3
#   [agents.minimax_flash]-> custom.minimax_flash-> api.minimax.io MiniMax-M3.1-Flash-Preview
# The 2026-10-17 rollout moved coder from nemotron35 to qwen38 and reviewer
# from qwen38 to gemma4-31b; the older labels were the bug.
# When the live config is readable, ``provenance.agents()`` overlays it on
# this table, so a host whose provider points somewhere else reports that
# host's real model rather than this fallback.
AGENT_MODELS: dict[str, dict] = {
    k: dict(v) for k, v in provenance.STATIC_AGENT_MODELS.items()
}

# Families this fleet may NEVER author or review with, regardless of a host
# override or a stale env file: the routing rule forbids Anthropic-backed
# models and paid frontier resellers, and the :4100 gateway is retired.
_FORBIDDEN_MODEL_RE = re.compile(
    r"(^|[/_.-])(claude|anthropic|opus|sonnet|haiku)([/_.-]|$)|"
    r"(^|[/_.-])(azure|aws|gcp|oci)/",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Live provenance resolution (2026-10-17) -- see ``provenance.py``.
#
# Resolution rules (now owned by ``provenance.ProvenanceResolver``):
#   [agents.X].model_provider  (e.g. 'custom.coder')
#     -> [providers.models.custom.coder].model  (and .uri for the tier)
# The provider label recorded is the provider key itself. Cost tier comes
# from KNOWN_COST_TIERS when the model is already classified, else 'A' for a
# LAN/loopback uri and 'B' for anything remote. Cached per process; falls
# back to AGENT_MODELS with provenance_source='static_fallback' when the
# config is unreadable. ``hive_worker`` is a thin caller.
# ---------------------------------------------------------------------------

# Re-export the resolution primitives for tests / external callers that
# already reference them on this module.
KNOWN_COST_TIERS = provenance.KNOWN_COST_TIERS
ZODER_CONFIG_PATH = os.environ.get(
    "HIVE_ZODER_CONFIG", os.path.expanduser("~/.zoder/config.toml")
)
try:  # Python 3.11+
    import tomllib as tomllib  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fleet runs 3.11+
    tomllib = None  # type: ignore


def _cost_tier_for(model: str, uri: str | None) -> str:
    """'A' for LAN/loopback endpoints, 'B' for remote; known models win."""
    return provenance._cost_tier_for(model, uri)


def _provider_table(config: dict, provider: str) -> dict | None:
    """Look up a `providers.models.<provider>` entry ('custom.coder')."""
    return provenance._provider_table(config, provider)


def _resolve_from_config(config: dict) -> dict[str, dict]:
    """Agent alias -> routed record, per this host's live config.toml."""
    return provenance.resolve_agents_from_config(config)


def agent_models() -> tuple[dict[str, dict], str]:
    """(alias -> record, provenance_source) resolved per process.

    provenance_source is 'live_config' when the host's ~/.zoder/config.toml
    was read, else 'static_fallback' when it was missing/unreadable and the
    corrected static table is in use. Reads the live config lazily once per
    process and caches the result.

    Honours ``ZODER_CONFIG_PATH`` (this module's override slot, monkeypatched
    by tests) when it differs from the env / home default.
    """
    default_path = os.environ.get(
        "HIVE_ZODER_CONFIG", os.path.expanduser("~/.zoder/config.toml")
    )
    override_path = ZODER_CONFIG_PATH if ZODER_CONFIG_PATH != default_path else None
    resolver = provenance.default_resolver()
    if override_path is not None and resolver._config_path != override_path:
        # Tests (and ops scripts) want a different config; build a one-off
        # resolver that reads from that path. The static fallback stays the
        # same; only the live overlay changes.
        resolver = provenance.ProvenanceResolver(config_path=override_path)
        return resolver.resolve()
    table = resolver.models()
    return table, resolver.source()


def reset_agent_models_cache() -> None:
    """Drop the cached routing table (tests / after a config redeploy)."""
    provenance.reset_default_resolver()
    resolver = provenance.default_resolver()
    resolver.reset()


def forbidden_model(reference: str) -> bool:
    """True when a model/alias names a family the routing policy forbids."""
    ref = (reference or "").strip()
    if not ref:
        return False
    if ":4100" in ref:
        return True
    return bool(_FORBIDDEN_MODEL_RE.search(ref))


def agent_model(agent: str) -> dict | None:
    """The routed model record for a zoder agent alias, or None if unknown.

    Never invent a model: an alias absent from AGENT_MODELS returns None and
    the caller reports no claimed model rather than a guessed one.
    """
    if not agent or forbidden_model(agent):
        return None
    entry = agent_models()[0].get(agent)
    return dict(entry) if entry else None


def provenance_source() -> str:
    """'live_config' or 'static_fallback' for the current routing table."""
    return agent_models()[1]


def escalation_model(agent: str) -> dict | None:
    """What the ladder rung `agent` actually launches.

    The `flash` rung is deliberately absent: it balances MiniMax-M3.1-Flash
    against the funded DeepSeek account per job, so the exact model is not
    known until the job picks a side. Report it honestly as unknown.
    """
    if agent == "deepseek":
        return None
    return agent_model(agent)


def provenance_patch(agent: str) -> dict:
    """Bus PATCH fields (claimed_*) for the model this attempt launched.

    Only fields with a known value are returned: an unknown route records
    nothing rather than a placeholder claim.
    """
    record = launch_record(agent)
    if not record:
        return {}
    return {
        "claimed_model": record["model"],
        "claimed_provider": record["provider"],
        "claimed_cost_tier": record["cost_tier"],
        "provenance_source": provenance_source(),
    }


def _bare_model(model: str) -> str:
    """Drop a `provider:` / `provider=` / `host:` prefix from a model string."""
    text = (model or "").strip()
    m = re.search(r"(?:^|[:\s])([A-Za-z0-9][A-Za-z0-9._/-]*)$", text)
    return m.group(1) if m else text


# A loop JSON object is printed as a single line by `zoder --json loop`. Bound
# what we are willing to consider so a pathological run cannot make parsing
# quadratic or hold megabytes of output.
_MAX_JSON_LINE = 4 * 1024 * 1024
_MODEL_FIELDS = ("model", "model_id", "modelId", "agent_model")
_REVIEWER_CONTAINERS = ("reviewers", "reviewer_results", "review_results", "reviews")


def _first_model(payload: dict) -> str | None:
    """First non-empty model string in a record, preferring exact fields."""
    for key in _MODEL_FIELDS:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _canonical_model(reference: str | None) -> str | None:
    """Normalize a model reference: an alias becomes its routed model, any
    other string is the exact model the loop reported. Forbidden families and
    empty values are dropped -- provenance never records a forbidden model."""
    if not reference:
        return None
    if forbidden_model(reference):
        return None
    record = agent_model(reference)
    if record:
        return record["model"]
    bare = _bare_model(reference)
    return bare or None


def parse_loop_reviewers(stdout: str) -> list[str]:
    """Reviewer entries the loop's own JSON named, canonicalized in order.

    This is the loop's CLAIM about its review phase, kept separate from
    :func:`parse_loop_models` (which also derives an author for the
    completion record) because the ledger -- not this list -- is the ground
    truth for which reviewer model actually ran.
    """
    if not stdout:
        return []
    record = None
    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith("{") or len(line) > _MAX_JSON_LINE:
            continue
        try:
            candidate = json.loads(line)
        except (ValueError, RecursionError):
            continue
        if isinstance(candidate, dict) and any(
            key in candidate for key in _REVIEWER_CONTAINERS
        ):
            record = candidate
    if record is None:
        return []
    reviewer_entries = None
    for key in _REVIEWER_CONTAINERS:
        if key in record:
            reviewer_entries = record.get(key)
            break
    if isinstance(reviewer_entries, (dict, str)):
        reviewer_entries = [reviewer_entries]
    if not isinstance(reviewer_entries, list):
        return []
    out: list[str] = []
    for entry in reviewer_entries:
        model = None
        if isinstance(entry, str):
            model = entry
        elif isinstance(entry, dict):
            model = _first_model(entry)
            if model is None:
                for nested in entry.values():
                    if isinstance(nested, dict) and _first_model(nested):
                        model = _first_model(nested)
                        break
        model = _canonical_model(model)
        if model and model not in out:
            out.append(model)
    return out


def parse_loop_models(stdout: str) -> dict | None:
    """Extract ``{author, reviewers}`` from `zoder --json loop` stdout.

    The loop's machine-readable record is the last single-line JSON object on
    stdout (log lines may precede it). Absent a parseable record -- ``author``
    and a reviewer list cannot be established -- return None so the caller
    reports no models rather than an invented attribution.

    Reviewer entries may be strings, objects with a ``model`` field, or
    ``{round: {"model": ...}}`` nesting; every reported model is de-duplicated
    while preserving order (the order the loop actually used them).
    """
    if not stdout:
        return None
    record = None
    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith("{") or len(line) > _MAX_JSON_LINE:
            continue
        try:
            candidate = json.loads(line)
        except (ValueError, RecursionError):
            continue
        if isinstance(candidate, dict) and (
            _first_model(candidate) or any(k in candidate for k in _REVIEWER_CONTAINERS)
        ):
            record = candidate  # keep the LAST record: the final verdict wins
    if record is None:
        return None
    author = _canonical_model(_first_model(record))
    reviewers: list[str] = []
    for key in _REVIEWER_CONTAINERS:
        entries = record.get(key)
        if isinstance(entries, (dict, str)):
            entries = [entries]
        if not isinstance(entries, list):
            continue
        for entry in entries:
            model = None
            if isinstance(entry, str):
                model = entry
            elif isinstance(entry, dict):
                model = _first_model(entry)
                if model is None:
                    for nested in entry.values():
                        if isinstance(nested, dict) and _first_model(nested):
                            model = _first_model(nested)
                            break
            model = _canonical_model(model)
            if model and model != author and model not in reviewers:
                reviewers.append(model)
    if not author and not reviewers:
        return None
    return {"author": author, "reviewers": reviewers}


HIVE_ALLOW_UNSANDBOXED_CODEX_ENV = "HIVE_ALLOW_UNSANDBOXED_CODEX"
UNSANDBOXED_CODEX_MODE = "danger-full-access"
DEFAULT_CODEX_SANDBOX = "workspace-write"

HIVE_URL = os.environ.get("HIVE_URL", "")
# No baked-in bus address (operator rule 2026-10-29). Unset is refused where the
# worker would otherwise spin on relative URLs; one log line, no crash loop.
if not HIVE_URL:
    fleet_inventory.warn_once(
        "worker.HIVE_URL",
        "HIVE_URL is not set: this worker cannot poll the bus and will refuse to run "
        "(set HIVE_URL in its unit environment)",
    )
AGENT_HOST = os.environ.get("AGENT_HOST", socket.gethostname().split(".")[0])
WORKER_KIND = os.environ.get("WORKER_KIND", "").strip().lower()
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "5"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "20"))
JOB_TIMEOUT = int(os.environ.get("JOB_TIMEOUT", "7200"))
WORKSPACE_ROOT = os.path.expanduser(
    os.environ.get("WORKSPACE_ROOT", "~/hive-workspaces")
)
KEEP_WORKSPACE = os.environ.get("KEEP_WORKSPACE", "0") == "1"
CODEX_MODEL = os.environ.get("CODEX_MODEL", "gpt-6-luna")
CODEX_SANDBOX = os.environ.get("CODEX_SANDBOX", DEFAULT_CODEX_SANDBOX)
AUTO_RESTART = os.environ.get("HIVE_WORKER_AUTO_RESTART", "0") == "1"
SOURCE_MTIME_NS = os.stat(__file__).st_mtime_ns
# Optional per-host override for zeroclaw jobs. Unset by default -- bare
# `zoder loop` (no --agent) is meant to defer entirely to that host's own
# ~/.zoder/config.toml escalation ladder, per this worker's original design.
# In practice, several hosts' zoder installs have real bugs/config drift in
# their AUTOMATIC (no-pin) routing path -- confirmed 2026-09-15 on both
# TYDEUS and CERBERUS, where bare invocation kept resolving to a stale/
# removed model (a provider-identity bug fixed in zoder itself, plus a
# separate automatic-routing-ignores-primary_model issue not yet fixed
# upstream) while the SAME model, invoked explicitly via --agent, dispatched
# correctly. Setting ZODER_AGENT sidesteps the broken automatic path
# entirely by pinning a known-good agent alias, the same way CODEX_MODEL
# already sidesteps codex's model-catalog drift above.
# HIVE_* overrides are set by host GitOps. EnvironmentFile values are applied
# after systemd Environment= lines, so a stale /etc/hive-worker-env ZODER_AGENT
# would otherwise override the host route drop-in.
_raw_zoder_agent = os.environ.get("HIVE_ZODER_AGENT") or os.environ.get(
    "ZODER_AGENT", ""
)
# Policy guard: a stale env file or host drop-in that pins a forbidden family
# (Claude/Anthropic, a paid reseller alias, the retired :4100 route) is ignored
# -- the worker falls back to the policy ladder rather than launching it.
if forbidden_model(_raw_zoder_agent):
    print(
        f"[hive-worker:{WORKER_KIND}] ignoring forbidden author route {_raw_zoder_agent!r}",
        flush=True,
    )
    _raw_zoder_agent = ""
ZODER_AGENT = _raw_zoder_agent
# `zoder loop`'s adversarial-review phase picks its own reviewer model
# independently of --agent -- confirmed 2026-09-16 on ARGOS: --agent minimax
# fixed authoring, but the review phase still auto-routed to "MiniMax-M2.7",
# a real but locally-unconfigured catalog entry (sambanova-hosted, no
# provider for it in this host's config.toml) pulled from zoder's global
# model_corpus.json, which is unfiltered by what a given host can actually
# reach. `--reviewer <MODEL>` pins it the same way ZODER_AGENT pins the
# author.
_raw_zoder_reviewer = os.environ.get("HIVE_ZODER_REVIEWER") or os.environ.get(
    "ZODER_REVIEWER", ""
)
if forbidden_model(_raw_zoder_reviewer):
    print(
        f"[hive-worker:{WORKER_KIND}] ignoring forbidden reviewer route {_raw_zoder_reviewer!r}",
        flush=True,
    )
    _raw_zoder_reviewer = ""
# Default follows the canonical host route (gitops/fleet/zoder-config/
# hive-worker-route.conf, deployed as 20-host-route.conf): qwen38 on TYDEUS is
# the cross-family local reviewer for the TYDEUS qwen38 author. CERBERUS
# gemma4-31b and EIH Nemotron remain available as explicit overrides.
ZODER_REVIEWER = _raw_zoder_reviewer or "qwen38"

# Reviewer quiet windows (2026-10-03, retargeted 2026-10-16): the window
# switch trades the local reviewer model for the EIH backup reviewer during the
# RiskyEats publish bursts. The original incident was gemma4-31b on CERBERUS:
# 4 slots shared with the RiskyEats narrator, and every zeroclaw slot reviewing
# on it during the publish aggregate bursts starved the narrator ("all healthy
# endpoints failed" rose from ~30/day to 387). The canonical reviewer is now
# qwen38 on TYDEUS (:8006), which shares a host with the TYDEUS generator, so
# the same headroom argument applies. Inside these local-time windows the
# review goes to HIVE_ZODER_REVIEWER_ALT (EIH Nemotron) instead -- that model
# is the policy's BACKUP reviewer and is otherwise health-checked, not used as
# the standing default.
#
# The windows open ONE HOUR BEFORE each RiskyEats publish (07:30 and 18:00
# local) so a loop that started earlier -- and is therefore still holding a
# gemma slot when the publish fires -- has already been routed to the EIH
# reviewer. Evaluated when the job is claimed, so the lead has to cover loops
# claimed before the burst. HIVE_ZODER_REVIEWER_QUIET_WINDOWS still overrides.
ZODER_REVIEWER_ALT = os.environ.get(
    "HIVE_ZODER_REVIEWER_ALT", "nvidia/nemotron-3-ultra-550b-a55b"
)
REVIEWER_QUIET_WINDOWS = os.environ.get(
    "HIVE_ZODER_REVIEWER_QUIET_WINDOWS", "06:30-09:00,17:00-19:30"
)
REVIEWER_QUIET_TZ = os.environ.get("HIVE_ZODER_REVIEWER_QUIET_TZ", "America/New_York")

# Commit-message trailers that attribute authorship to a model/tool. Only the
# configured git identity may author a hive commit, so any such trailer means
# the delivery is not attributable and must not be pushed.
_TRAILER_START_RE = re.compile(r"^\s*-{2,}\s*$")
# Author/committer headers as emitted by `git log --format=raw` (%an <%ae>).
_COMMIT_HEADER_RE = re.compile(
    r"^(?:Author|Committer):\s*(?P<name>[^<]*?)\s*<(?P<email>[^>]*)>", re.MULTILINE
)
# SINGLE SOURCE OF TRUTH for what counts as an AI-attribution line.
#
# This pattern is the ONLY definition of the offending shape. It is compiled
# here for the gate (commit_attribution_violations / _is_ai_attribution) and
# the identical string literal is embedded into the generated strip scripts
# (the filter-branch --msg-filter and the commit-msg hook), so the gate and
# the stripper cannot drift apart.
#
# Every alternative is anchored to a TRAILER shape -- a leading key, a colon,
# and a value -- or to one of the two exact Claude Code badge line forms.
# A substring search over "generated by" / "assisted by" / "ai attribution"
# anywhere in a line silently deleted legitimate body prose ("units
# generated by gen.py from models.toml", "regenerated with ruff", a commit
# that describes this very AI-attribution feature), which is exactly what an
# attribution gate must never do.
_AI_ATTRIBUTION_TRAILER_PATTERN = (
    r"^\s*(?:co[-\s]?authored-by|authored-by|generated[-\s]by|generated[-\s]with|"
    r"assisted[-\s]by|written[-\s]with|ai[-\s]attribution)\s*:"
)
# Exact badge forms emitted by Claude Code, optionally robot-emoji prefixed,
# optionally carrying the markdown link target, plus the bare session URL
# line some versions append on its own.
_AI_ATTRIBUTION_BADGE_PATTERN = (
    r"^\s*(?:\U0001F916\s*)?Generated with \[Claude Code\]"
    r"(?:\s*\([^)]*\))?\s*$"
    r"|^\s*https://claude\.ai/code/session[/_]\S*\s*$"
)
_AI_ATTRIBUTION_LINE_RE = re.compile(
    _AI_ATTRIBUTION_TRAILER_PATTERN + "|" + _AI_ATTRIBUTION_BADGE_PATTERN,
    re.IGNORECASE,
)
# The exact source text embedded into generated strip scripts. Kept next to
# the compiled pattern above so a change to one is impossible to make without
# changing the other: _strip_script_source() interpolates THIS string.
_AI_ATTRIBUTION_LINE_SOURCE = (
    _AI_ATTRIBUTION_TRAILER_PATTERN + "|" + _AI_ATTRIBUTION_BADGE_PATTERN
)


# Attribution by destination (operator rule): singularityos-lab repos REQUIRE
# `Assisted-by: <tool>:<model>` and `AI-Scope: <sentence>` trailers (their CI
# check-ai-disclosure.cjs fails without them). In those repos, and only there,
# these two trailers are kept; Co-Authored-By and Claude links are still
# stripped everywhere. The decision travels in the job env so the gate, the
# filter-branch fallback and the commit-msg hook agree.
_DISCLOSURE_TRAILER_SOURCE = r"^\s*(?:assisted[-\s]by|ai[-\s]scope)\s*:"
_DISCLOSURE_TRAILER_RE = re.compile(_DISCLOSURE_TRAILER_SOURCE, re.IGNORECASE)
DISCLOSURE_ENV = "HIVE_KEEP_AI_DISCLOSURE"
_DISCLOSURE_ORGS = ("singularityos-lab",)
_DISCLOSURE_CHECK_FILE = os.path.join(".github", "scripts", "check-ai-disclosure.cjs")


def repo_requires_ai_disclosure(workdir: str, env: dict | None = None) -> bool:
    """True for repos whose policy REQUIRES Assisted-by/AI-Scope trailers:
    any remote URL under singularityos-lab, or the disclosure CI check file."""
    if os.path.isfile(os.path.join(workdir, _DISCLOSURE_CHECK_FILE)):
        return True
    try:
        proc = subprocess.run(
            ["git", "-C", workdir, "remote", "-v"],
            env=env,
            timeout=30,
            capture_output=True,
            text=True,
            check=False,
        )
        out = getattr(proc, "stdout", "") or ""
    except Exception:  # noqa: BLE001 - detection must never fail a job
        return False
    return isinstance(out, str) and any(org in out for org in _DISCLOSURE_ORGS)


def _keep_disclosure(env: dict | None) -> bool:
    return bool(env) and env.get(DISCLOSURE_ENV) == "1"


def _is_ai_attribution(line: str, keep_disclosure: bool = False) -> bool:
    """True when `line` is an attribution TRAILER or an exact badge line.

    Shared by the gate and the strippers -- see
    _AI_ATTRIBUTION_LINE_SOURCE for why the match is anchored rather than a
    free substring search. With `keep_disclosure`, the required
    Assisted-by/AI-Scope disclosure trailers are not attribution.
    """
    if keep_disclosure and _DISCLOSURE_TRAILER_RE.match(line):
        return False
    return _AI_ATTRIBUTION_LINE_RE.match(line) is not None


def commit_attribution_violations(
    workdir: str, env: dict, revisions: list[str] | str, name: str = "", email: str = ""
) -> list[dict]:
    """Commits on `revisions` carrying an AI/Co-Authored-By trailer or an author
    that is not the configured git identity. Returns one bounded record per
    offending commit (empty list = clean)."""
    revs = [revisions] if isinstance(revisions, str) else list(revisions or [])
    if not revs:
        return []
    try:
        raw = subprocess.check_output(
            # %an <%ae> precedes the message so an author that is not the
            # configured identity is caught even when the body is clean.
            # --no-walk: a list of job commit SHAs must name exactly those
            # commits. Without it git log walks every ancestor, so trailers
            # already on the base branch failed every delivery. Ranges
            # (base..HEAD) still expand normally.
            [
                "git",
                "-C",
                workdir,
                "log",
                "--no-merges",
                "--no-walk=unsorted",
                "--format=%H%x00%ae%x00%B%x1e",
                "--end-of-options",
                *revs,
            ],
            env=env,
            stderr=subprocess.DEVNULL,
        )
    except (subprocess.SubprocessError, OSError) as exc:
        raise RuntimeError(
            f"attribution git check failed: {type(exc).__name__}: {exc}"
        ) from exc
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")
    try:
        name = (
            name
            or subprocess.check_output(
                ["git", "-C", workdir, "config", "--get", "user.name"],
                env=env,
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
        )
        email = (
            email
            or subprocess.check_output(
                ["git", "-C", workdir, "config", "--get", "user.email"],
                env=env,
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
        )
    except (subprocess.SubprocessError, OSError) as exc:
        raise RuntimeError(
            f"attribution identity check failed: {type(exc).__name__}: {exc}"
        ) from exc
    violations: list[dict] = []
    # %x1e (record separator) ends each record: git terminates a format with a
    # newline, so splitting on an empty line between two NULs would depend on
    # that newline's exact position.
    for chunk in raw.split("\x1e"):
        chunk = chunk.strip("\x00\n")
        if not chunk:
            continue
        sha, _, rest = chunk.partition("\x00")
        raw_author, _, body = rest.partition("\x00")
        author_ok = True
        if email and raw_author.strip() and raw_author.strip() != email:
            author_ok = False
        trailer_present = False
        in_trailers = False
        for line in body.splitlines():
            if _COMMIT_HEADER_RE.match(line):  # body-level Author: forgery
                author_ok = author_ok and (
                    not email
                    or _COMMIT_HEADER_RE.match(line).group("email").strip() == email
                )
                continue
            if _TRAILER_START_RE.match(line):
                in_trailers = True
                continue
            if _is_ai_attribution(line, _keep_disclosure(env)):
                trailer_present = True
                in_trailers = True
                continue
            if in_trailers and line.strip() and not re.match(r"^[\w-]+:\s", line):
                in_trailers = False
        if trailer_present or not author_ok:
            violations.append(
                {
                    "commit": sha[:12],
                    "trailer": trailer_present,
                    "author_mismatch": not author_ok,
                }
            )
    return violations


_PATCH_TRAILER_LINE_RE = re.compile(r"^\s*co-authored-by:", re.IGNORECASE)


def _clean_message(message: str, keep_disclosure: bool = False) -> str:
    """Commit message with AI-attribution / Co-Authored-By lines removed."""
    kept = [
        line
        for line in message.splitlines()
        if not (
            _PATCH_TRAILER_LINE_RE.match(line)
            or _is_ai_attribution(line, keep_disclosure)
        )
    ]
    return "\n".join(kept).strip()


# Hook + filter-branch --msg-filter script lives NEXT TO the worker module
# in a fixed, worker-owned location, NEVER inside the workdir: the workdir
# path is allowed to contain shell metacharacters (an orchestrator can name
# a workspace after anything; tests deliberately exercise paths with a
# single-quote and space -- tempfile.TemporaryDirectory(prefix="hive
# workspace '")). Routing --msg-filter through `sh -c "python3 <path>"` is
# only safe when `<path>` is a single, sanitised shell token, so the
# canonical strip script is written under a name we control absolutely
# (no embedded spaces, no quote characters, no globs). The commit-msg
# hook does not share the path: it embeds the strip logic directly so
# its own argv processing stays self-contained.
_FILTER_BRANCH_SQUELCH = "1"


# Worker-installed strip script sidecar: lives next to hive_worker.py and
# is referenced by absolute path. The directory is fixed (we control it),
# and the file is sanitised on write: a missing or unsafe path causes the
# fallback to refuse rather than pass a tainted string to `sh -c`.
_STRIP_SCRIPT_NAME = "hive_strip_msg.py"
# The interpreter filter-branch runs the strip script with. Resolved once
# at import time: `sh -c` inherits whatever PATH git's child shell picks
# up, and we want the same Python we are currently running so module
# imports (if any) and behaviour are identical. The path is then
# validated against the same shell-metacharacter set so even a symlinked
# or relocated interpreter cannot smuggle shell syntax into the cmd.
_PYTHON_BIN = sys.executable
# Shell metacharacters that would let a workdir contaminate the shell
# command we hand to git filter-branch. The canonical path is owned by us
# and MUST be free of these; we validate at write time AND at call time.
_SHELL_META_CHARS = frozenset(" \t\n\"'`$\\;|&<>*?()[]{}#~!=")


def _worker_dir() -> Path:
    """Directory containing hive_worker.py -- the strip script lives here
    so its absolute path is reproducible, fixed, and never user-controlled.

    Validated at module-load time: a worker dir that contains shell
    metacharacters would let the rest of the worker hand git filter-branch
    a poisoned `sh -c` command. Such an installation is unsafe by
    construction (the same problem would afflict EVERY shell-tool
    invocation in the worker, not just the strip script), so fail
    loudly here rather than silently degrading later.
    """
    d = Path(os.path.abspath(__file__)).parent
    _assert_safe_path(d)
    return d


def _strip_script_path() -> Path:
    """Absolute path of the worker-owned filter-branch --msg-filter script.

    Next to hive_worker.py when the worker user can write there (root-run
    hosts); otherwise under WORKSPACE_ROOT/.hive-bin. The deploy installs
    /opt/graeae as root while non-root workers run as their own user, and a
    PermissionError there silently disabled the filter-branch fallback
    (2026-10-17, typhon-worker). The script is rewritten on every job's hook
    install, so its content is always the worker's own.
    """
    worker_dir = _worker_dir()
    if os.access(worker_dir, os.W_OK):
        return worker_dir / _STRIP_SCRIPT_NAME
    fallback = Path(os.path.abspath(WORKSPACE_ROOT)) / ".hive-bin"
    _assert_safe_path(fallback)
    return fallback / _STRIP_SCRIPT_NAME


def _assert_safe_path(path: Path, *, allow_parent_traversal: bool = False) -> None:
    """Reject a path component that would let `sh -c` misparse a subcommand.

    The filter-branch fallback invokes this script via a single shell token;
    any of the metacharacters in `_SHELL_META_CHARS` would change the parsing
    shape. The hook install also enforces this so the commit-msg hook never
    receives a path that contains a newline or other script-tricks.
    """
    text = str(path)
    for ch in _SHELL_META_CHARS:
        if ch in text:
            raise ValueError(
                f"path {text!r} contains shell metacharacter {ch!r}; refusing"
            )
    if not allow_parent_traversal and ".." in Path(text).parts:
        raise ValueError(f"path {text!r} contains '..' component; refusing")


_HOOK_MARKER = "HIVE_ATTRIBUTION_HOOK_V1"
_PREV_HOOK_SUFFIX = "commit-msg.hive-prev"
# Standard hook names a repository may already rely on. We install ONLY
# commit-msg, into the directory git will actually resolve, so every other
# hook keeps running untouched (see install_attribution_hook).
_STANDARD_HOOK_NAMES = (
    "pre-commit",
    "prepare-commit-msg",
    "post-commit",
    "pre-push",
    "commit-msg",
)


def _effective_hooks_dir(workdir: str, env: dict) -> Path | None:
    """The hooks directory git will actually resolve for `workdir`.

    Honours a pre-existing ``core.hooksPath`` (local, then inherited) and
    falls back to the git dir's own ``hooks/``. Returns None when git cannot
    answer, so the caller can skip the install rather than guess.
    """
    try:
        out = subprocess.run(
            ["git", "-C", workdir, "rev-parse", "--git-path", "hooks"],
            env=env,
            timeout=30,
            capture_output=True,
            text=True,
            check=False,
        )
    except (subprocess.SubprocessError, OSError):
        return None
    # A test harness (or a patched shim) may hand back no result object at
    # all; treat that exactly like git being unable to answer.
    if out is None or out.returncode != 0 or not (out.stdout or "").strip():
        return None
    candidate = Path(out.stdout.strip())
    if not candidate.is_absolute():
        # rev-parse returns a path relative to the CWD it was given; the
        # caller passed -C workdir, but never assume -- anchor to workdir.
        candidate = Path(workdir) / candidate
    return candidate


def _strip_script_source() -> str:
    """Python source for a stdin->stdout trailer-strip filter.

    Used both by the worker-owned filter-branch --msg-filter script and
    the per-workdir commit-msg hook (the hook reads the message file as
    argv[1] then forwards it through this same logic, fully embedded so
    the hook never imports from the worker module). Mirrors
    commit_attribution_violations so a commit the gate would flag is
    caught before it is written.
    """
    return f"""
import re, sys

# Anchored to trailer shape / exact badge lines -- NOT a substring search.
# Interpolated from the worker's own single shared pattern so the hook and
# the gate can never disagree about what is stripped.
_LINE = re.compile(
    {_AI_ATTRIBUTION_LINE_SOURCE!r},
    re.IGNORECASE,
)
# Required disclosure trailers (Assisted-by / AI-Scope) are kept in repos
# whose policy demands them; the worker sets {DISCLOSURE_ENV}=1 for those.
_KEEP = re.compile({_DISCLOSURE_TRAILER_SOURCE!r}, re.IGNORECASE)
import os as _os
_KEEP_ON = _os.environ.get({DISCLOSURE_ENV!r}) == "1"


def _drop(line):
    if _KEEP_ON and _KEEP.match(line):
        return False
    return _LINE.match(line) is not None
""".lstrip()


def _write_executable(path: Path, body: str) -> None:
    """Write `body` to `path` and make it executable.

    Used for hooks. The path lives inside the orchestrator-controlled
    workdir, so it is deliberately NOT run through _assert_safe_path: git
    invokes a hook by exec'ing the resolved path directly, never through a
    shell, so a space or an apostrophe in the directory name is harmless
    (and refusing it would disable attribution stripping entirely in a
    legitimately-named workspace).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | 0o111)  # ensure executable


def _write_worker_strip_script(path: Path) -> None:
    """Write the filter-branch --msg-filter script at a worker-controlled
    absolute path. Called once per worker process; subsequent invocations
    reuse the existing file. The path is validated against shell-meta so
    a malicious caller cannot slip an injection past the sanitiser.
    """
    body = (
        "#!/usr/bin/env python3\n"
        + _strip_script_source()
        + (
            "_src = sys.stdin.read()\n"
            "_kept = []\n"
            "for _line in _src.splitlines(keepends=True):\n"
            "    if _drop(_line):\n"
            "        continue\n"
            "    _kept.append(_line)\n"
            'sys.stdout.write("".join(_kept))\n'
        )
    )
    # The strip script IS handed to `sh -c` (via --msg-filter), so its path
    # must be a single clean shell token.
    _assert_safe_path(path)
    _write_executable(path, body)


def _write_commit_msg_hook(
    path: Path, prev_hook: Path | None = None, keep_disclosure: bool = False
) -> None:
    """commit-msg hook: rewrite the message file (argv[1]) in place when
    it contains a disallowed trailer line. Always exits 0 -- a commit we
    cannot clean is still a commit the gate will catch and fail the
    delivery on.

    Defence in depth on argv handling:
      * Require sys.argv >= 2 (git always passes one arg, the message
        file path, but a hostile or buggy invocation cannot force us
        into "sys.argv[1] missing" behaviour).
      * Refuse any path that is not a plain regular file -- a symlink
        could point at a file the workdir or its owner controls; a
        directory / FIFO / device file is never what commit-msg is
        meant to see. This check uses ``stat.S_ISREG`` (no follow) so a
        redirected symlink is caught.
      * Open with ``errors="replace"`` so an oddly-encoded message
        never raises UnicodeDecodeError; every read/write uses explicit
        OSError handling so a missing or unreadable file exits 0
        quietly. An attacker cannot use the hook to scribble on the
        workdir -- we only ever touch the file git gave us.

    NOTE: commit-msg always invokes the hook with CWD set to the workdir
    root and argv[1] being the path to ``.git/COMMIT_EDITMSG`` (or
    ``MERGE_MSG``/``SQUASH_MSG`` etc.). We treat the relative path as
    workdir-local via ``os.path.abspath``; relying on this is safe
    because git controls the invocation environment.

    CHAINING: after cleaning, the hook exec's the repository's previous
    commit-msg hook (moved aside to ``commit-msg.hive-prev`` at install
    time) if one existed and is still executable. Overwriting the repo's
    own commit-msg hook without running it would silently disable whatever
    policy that hook enforced.
    """
    prev_literal = repr(str(prev_hook)) if prev_hook is not None else "None"
    body = (
        "#!/usr/bin/env python3\n"
        f"# {_HOOK_MARKER}\n"
        + _strip_script_source()
        # Baked in at install: git may run this hook from an agent process
        # that does not inherit the worker's job environment.
        + ("_KEEP_ON = True\n" if keep_disclosure else "")
        + (
            "import os, stat, sys\n"
            f"_PREV = {prev_literal}\n"
            "def _chain():\n"
            "    # Run the repository's original commit-msg hook, if any.\n"
            "    try:\n"
            "        if _PREV is not None and os.path.isfile(_PREV) \\\n"
            "                and os.access(_PREV, os.X_OK):\n"
            "            os.execv(_PREV, [_PREV] + sys.argv[1:])\n"
            "    except OSError:\n"
            "        pass\n"
            '_path = sys.argv[1] if len(sys.argv) >= 2 else ""\n'
            "if not _path:\n"
            "    _chain()\n"
            "    sys.exit(0)\n"
            "# Resolve against CWD (which git sets to the workdir root);\n"
            "# git may give us either a relative or absolute path here.\n"
            "_resolved = os.path.abspath(_path)\n"
            "try:\n"
            "    _st = os.lstat(_resolved)\n"
            "except OSError:\n"
            "    _chain()\n"
            "    sys.exit(0)\n"
            "# Refuse anything but a plain regular file: a symlink the\n"
            "# workdir controls could redirect us to attacker-chosen\n"
            "# bytes; a directory / FIFO / device is never what\n"
            "# commit-msg is meant to read.\n"
            "if not stat.S_ISREG(_st.st_mode):\n"
            "    _chain()\n"
            "    sys.exit(0)\n"
            "try:\n"
            '    with open(_resolved, "r", encoding="utf-8", errors="replace") as _f:\n'
            "        _body = _f.read()\n"
            "except OSError:\n"
            "    _chain()\n"
            "    sys.exit(0)\n"
            "_kept = []\n"
            "for _line in _body.splitlines(keepends=True):\n"
            "    if _drop(_line):\n"
            "        continue\n"
            "    _kept.append(_line)\n"
            '_cleaned = "".join(_kept)\n'
            "if _cleaned != _body:\n"
            "    try:\n"
            '        with open(_resolved, "w", encoding="utf-8") as _f:\n'
            "            _f.write(_cleaned)\n"
            "    except OSError:\n"
            "        pass\n"
            "_chain()\n"
            "sys.exit(0)\n"
        )
    )
    _write_executable(path, body)


def install_attribution_hook(workdir: str, env: dict) -> Path:
    """Install a per-job commit-msg hook so offending trailers are never
    written into a hive commit, plus the worker-owned filter-branch
    --msg-filter script that the FALLBACK rewrites unpushed commits with.

    The hook is installed by writing ONLY ``commit-msg`` into the hooks
    directory git will actually resolve (``core.hooksPath`` when already
    set, otherwise ``<gitdir>/hooks``). We deliberately do NOT redirect
    ``core.hooksPath`` at a private directory: that would disable every
    hook the repository already had (pre-commit formatters, pre-push
    policy, an existing commit-msg check). Any pre-existing
    ``commit-msg`` is preserved by moving it aside to
    ``commit-msg.hive-prev`` and chaining to it from our hook, which
    exec's it after cleaning the message.

    The strip script for filter-branch lives next to ``hive_worker.py`` --
    a path the orchestrator cannot influence -- and is wired up by
    absolute path so git-filter-branch's ``sh -c`` receives a single,
    sanitised token.

    The install never raises: a hook or strip-script install that fails is
    logged but does not block the job (the gate still catches a surviving
    trailer). Returns the effective hooks directory.
    """
    strip_script = _strip_script_path()
    hooks_dir = _effective_hooks_dir(workdir, env) or (Path(workdir) / ".git" / "hooks")
    commit_msg = hooks_dir / "commit-msg"
    try:
        prev_hook: Path | None = None
        existing_prev = hooks_dir / _PREV_HOOK_SUFFIX
        if commit_msg.is_file() and os.access(commit_msg, os.X_OK):
            try:
                current = commit_msg.read_text(encoding="utf-8", errors="replace")
            except OSError:
                current = ""
            if _HOOK_MARKER not in current:
                # A real pre-existing hook: preserve it for chaining.
                # Re-installing over our own hook must NOT clobber the
                # hook we moved aside on the first install.
                _write_executable(existing_prev, current)
                prev_hook = existing_prev
            elif existing_prev.is_file():
                prev_hook = existing_prev
        elif existing_prev.is_file():
            # Our hook was removed but the preserved original remains:
            # keep chaining to it.
            prev_hook = existing_prev
        keep = repo_requires_ai_disclosure(workdir, env)
        if keep:
            env[DISCLOSURE_ENV] = "1"
        _write_commit_msg_hook(commit_msg, prev_hook, keep_disclosure=keep)
        _write_worker_strip_script(strip_script)
    except (subprocess.SubprocessError, OSError, ValueError) as exc:
        print(
            f"[hive-worker:{WORKER_KIND}] hook install at {hooks_dir} failed: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )
    return hooks_dir


def _hooks_path() -> str:
    """Absolute path of the worker-owned strip script (filter-branch)."""
    return str(_strip_script_path())


def _rev_on_remote(workdir: str, sha: str, env: dict) -> bool:
    """True when `sha` is reachable from any refs/remotes/* in `workdir`."""
    try:
        out = subprocess.run(
            [
                "git",
                "-C",
                workdir,
                "for-each-ref",
                "--count=1",
                "--contains",
                sha,
                "refs/remotes",
            ],
            env=env,
            timeout=30,
            capture_output=True,
            text=True,
            check=False,
        )
    except (subprocess.SubprocessError, OSError):
        return False
    return out.returncode == 0 and bool(out.stdout.strip())


def strip_ai_attribution(
    workdir: str,
    env: dict,
    revisions: list[str] | str,
    baseline: str = "",
    name: str = "",
    email: str = "",
) -> dict:
    """Fallback cleanup: rewrite unpushed commits to drop AI/Co-Authored-By
    trailers when the commit-msg hook did not exist for that worktree.

    PRIMARY prevention: a per-job commit-msg hook installed by
    ``install_attribution_hook`` already strips these trailers before any
    commit object is created, so a hive commit with a trailer is the
    abnormal case. THIS function is the FALLBACK that scrubs a pre-existing
    chain: it runs a single ``git filter-branch -f --msg-filter <worker
    strip script> <range>`` over the unpushed history, leaving a delivered
    history strictly untouched -- if any commit in the supplied revisions is
    reachable from a remote, the whole rewrite is skipped and the offending
    SHAs are reported as ``unfixable``. The configure-git's-identity check
    still fires, and ``enforce_attribution`` is still the final gate.

    ``baseline`` is the SHA the job started on (parent of the unpushed set).
    When omitted, ``strip_ai_attribution`` rewrites only the explicit
    ``revisions``; passing ``baseline`` lets one filter-branch call cover the
    whole chain via the ``<baseline>..HEAD`` rev-list form, which is how
    ``enforce_attribution`` invokes it.
    """
    revs = (
        [revisions]
        if isinstance(revisions, str)
        else [r for r in (revisions or []) if r]
    )
    report: dict = {"stripped": [], "failed": [], "remaining": [], "unfixable": []}
    if not revs:
        return report
    try:
        if not _current_branch(workdir, env):
            report["error"] = "not_on_a_branch"
            return report
        # Only commits that actually carry an attribution trailer matter. A
        # range can legitimately include clean commits that another job
        # already pushed to the same branch (concurrent jobs, 2026-10-17:
        # job 01a14813 was failed for two clean, already-pushed commits).
        offenders = {
            v["commit"]
            for v in commit_attribution_violations(workdir, env, revs)
            if v.get("trailer")
        }
        if not offenders:
            return report
        # If any OFFENDING commit is already reachable from a remote, the
        # chain is unsafe to rewrite -- the rewrite would silently re-sign
        # commits the remote already has. Mark them and bail.
        unfixable: list[str] = []
        for sha in revs:
            if sha[:12] in offenders and _rev_on_remote(workdir, sha, env):
                unfixable.append(sha[:12])
        if unfixable:
            report["unfixable"] = unfixable
            return report
        # Build the filter-branch rev-list. ``baseline..HEAD`` covers the
        # entire unpushed chain in one shot when the caller hands us a
        # baseline. When the caller does NOT (defence-in-depth for any
        # future direct caller, no test path exercises this), derive the
        # boundary TOPOLOGICALLY -- never by %ct, which is misleading in
        # a DAG (a chronologically older commit can sit on an unrelated
        # branch while the chain's actual root is younger). We walk the
        # chains in $revs with `git rev-list --parents`, pick the parents
        # that are NOT in $revs as the boundary SHAs, and ask git for
        # their merge-base so filter-branch's single rev expression
        # `<boundary>..HEAD` covers exactly the rev set regardless of
        # branching or rebasing.
        if not baseline:
            try:
                rev_list_parents = subprocess.check_output(
                    ["git", "-C", workdir, "rev-list", "--parents"] + revs,
                    env=env,
                    text=True,
                    stderr=subprocess.DEVNULL,
                )
            except (subprocess.SubprocessError, OSError) as exc:
                report["error"] = (f"baseline_revlist_failed: {type(exc).__name__}")[
                    :200
                ]
                report["failed"] = [r[:12] for r in revs]
                return report
            rev_set = set(revs)
            boundary_parents: list[str] = []
            for line in rev_list_parents.splitlines():
                parts = line.split()
                if len(parts) < 2:
                    continue
                # First entry is the SHA itself, then its parents.
                for parent in parts[1:]:
                    if parent not in rev_set:
                        boundary_parents.append(parent)
                        break
            if not boundary_parents:
                report["error"] = "no_topological_boundary"
                report["failed"] = [r[:12] for r in revs]
                return report
            try:
                if len(boundary_parents) == 1:
                    baseline = boundary_parents[0]
                else:
                    # Multiple roots in $revs -- the LCA across all their
                    # parents is the single SHA filter-branch needs to
                    # cover the whole rev set without rewriting any
                    # pre-rev ancestor.
                    baseline = subprocess.check_output(
                        [
                            "git",
                            "-C",
                            workdir,
                            "merge-base",
                            "--octopus",
                            *boundary_parents,
                        ],
                        env=env,
                        text=True,
                        stderr=subprocess.DEVNULL,
                    ).strip()
                    if not baseline:
                        raise subprocess.SubprocessError("empty merge-base")
            except (subprocess.SubprocessError, OSError) as exc:
                report["error"] = (f"merge_base_failed: {type(exc).__name__}")[:200]
                report["failed"] = [r[:12] for r in revs]
                return report
        range_expr = f"{baseline}..HEAD"
        filter_env = os.environ.copy()
        filter_env["FILTER_BRANCH_SQUELCH_WARNING"] = _FILTER_BRANCH_SQUELCH
        for var, val in env.items():
            filter_env.setdefault(var, val)
        # Make sure the strip script exists before we hand its path to
        # `sh -c`. The worker owns the file -- sitting next to
        # hive_worker.py -- so the absolute path contains no
        # orchestrator-controlled characters.
        strip_script = _hooks_path()
        if not os.path.isfile(strip_script):
            install_attribution_hook(workdir, env)
            strip_script = _hooks_path()
        # Defence-in-depth revalidation: even though _hooks_path() returns
        # a string we composed from Path operands, a corrupted worker dir
        # could surface a tampered path. Treating the path as a single
        # shell token we trust only after we have re-checked it here.
        _assert_safe_path(Path(strip_script))
        # The msg-filter is invoked by `sh -c <cmd>`. A safe path becomes
        # a single shell token; an unsafe one would corrupt the command.
        # We invoke the python interpreter by absolute PATH so the cmd
        # does not depend on whatever PATH git's child shell inherits.
        msg_filter_cmd = f"{_PYTHON_BIN} {strip_script}"
        cmd = [
            "git",
            "-C",
            workdir,
            "filter-branch",
            "-f",
            "--msg-filter",
            msg_filter_cmd,
            range_expr,
        ]
        try:
            proc = subprocess.run(
                cmd,
                env=filter_env,
                timeout=300,
                capture_output=True,
                text=True,
                check=False,
            )
        except (subprocess.SubprocessError, OSError) as exc:
            report["error"] = f"filter_branch: {type(exc).__name__}: {exc}"[:200]
            report["failed"] = [r[:12] for r in revs]
            return report
        if proc.returncode != 0:
            report["error"] = f"filter_branch_failed: {proc.stderr.strip()[:200]}"
            report["failed"] = [r[:12] for r in revs]
            return report
        # The chain was rewritten, so the pre-rewrite SHAs no longer name
        # commits. Re-discover the unpushed set and report each surviving
        # tail as ``stripped`` (count of unpushed commits); ``remaining`` is
        # populated only when something the gate cares about still lingers
        # (a surviving trailer or a foreign author).
        unpushed = subprocess.check_output(
            ["git", "-C", workdir, "rev-list", "HEAD", "--not", "--remotes"],
            env=env,
            text=True,
            stderr=subprocess.DEVNULL,
        ).split()
        report["stripped"] = [r[:12] for r in revs]
        if unpushed:
            remaining = commit_attribution_violations(
                workdir, env, unpushed, name, email
            )
            if remaining:
                report["remaining"] = remaining
                # A trailer that survives filter-branch is also a `failed`
                # the way the gate sees it -- it is still in the history
                # that would be pushed.
                report["failed"] = [r["commit"] for r in remaining]
        return report
    except (subprocess.SubprocessError, OSError) as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"[:200]
        return report


def _quiet_windows(spec: str) -> list[tuple[int, int]]:
    out = []
    for part in spec.split(","):
        try:
            a, b = part.strip().split("-")
            ah, am = (int(x) for x in a.split(":"))
            bh, bm = (int(x) for x in b.split(":"))
        except ValueError:
            continue
        # Reject out-of-range times: a typo like 99:00-100:00 would otherwise
        # create a window that never matches and silently disable the switch.
        if not (0 <= ah < 24 and 0 <= bh <= 24 and 0 <= am < 60 and 0 <= bm < 60):
            continue
        if bh == 24 and bm != 0:  # only 24:00 is a valid end-of-day boundary
            continue
        # Overnight windows are not supported: a reversed or empty interval would
        # never match, so drop it instead of silently disabling the switch.
        if bh * 60 + bm <= ah * 60 + am:
            continue
        out.append((ah * 60 + am, bh * 60 + bm))
    return out


#: Model-name prefixes of the OpenAI family. Codex-authored work must not be
#: reviewed by the same family (2026-10-26: gpt-oss-20b became the fleet
#: reviewer, and the codex worker reused it for its own delivery review).
_OPENAI_FAMILY_PREFIXES = ("gpt-", "gpt_", "o1", "o3", "o4", "codex", "openai/")


def is_openai_family(model: str) -> bool:
    """True when `model` is, or may be, an OpenAI-family model.

    Role ids (``cerberus-reviewer``) take their family from the fleet role map;
    a role id whose family the map does not give is treated as OpenAI, so an
    unknown role can never review Codex-authored work. Literal model names are
    matched by prefix (gpt-oss included).
    """
    name = (model or "").strip()
    if not name:
        return False
    if llm_roles.is_role_id(name):
        fam = llm_roles.family(name)
        return fam is None or fam == "openai"
    return name.lower().startswith(_OPENAI_FAMILY_PREFIXES)


def codex_review_model(now_local=None) -> str:
    """Reviewer for a Codex-authored delivery: never the author's own family.

    Uses the normal reviewer unless it is OpenAI-family, in which case
    HIVE_CODEX_REVIEWER (default: the cross-family alternate reviewer) takes
    over. An OpenAI-family override is refused too, so a misconfiguration
    cannot reintroduce same-family review.
    """
    chosen = review_model(now_local)
    if not is_openai_family(chosen) and not is_mimo_model(chosen):
        return chosen
    override = os.environ.get("HIVE_CODEX_REVIEWER", "").strip()
    if override and not is_openai_family(override) and not is_mimo_model(override):
        return override
    if (
        ZODER_REVIEWER_ALT
        and not is_openai_family(ZODER_REVIEWER_ALT)
        and not is_mimo_model(ZODER_REVIEWER_ALT)
    ):
        return ZODER_REVIEWER_ALT
    return "nvidia/nemotron-3-ultra-550b-a55b"


def review_model(now_local=None) -> str:
    """Reviewer for this run: the pinned one, or the alternate inside a quiet
    window. Returns "" when no reviewer is pinned (zoder decides)."""
    if not ZODER_REVIEWER:
        return ""
    if is_mimo_model(ZODER_REVIEWER):
        # MiMo has no reviewer placement (operator 2026-10-26); a stale pin
        # falls back to the alternate reviewer, never to MiMo.
        return "" if is_mimo_model(ZODER_REVIEWER_ALT) else ZODER_REVIEWER_ALT
    if now_local is None:
        try:
            from datetime import datetime
            from zoneinfo import ZoneInfo

            now_local = datetime.now(ZoneInfo(REVIEWER_QUIET_TZ))
        except Exception:  # noqa: BLE001 -- no tzdata: keep the pinned reviewer
            return ZODER_REVIEWER
    minute = now_local.hour * 60 + now_local.minute
    for start, end in _quiet_windows(REVIEWER_QUIET_WINDOWS):
        if start <= minute < end and ZODER_REVIEWER_ALT and not is_mimo_model(ZODER_REVIEWER_ALT):
            return ZODER_REVIEWER_ALT
    return ZODER_REVIEWER


# ---------------------------------------------------------------------------
# Provider cooldown (incident 2026-10-17): the MiniMax Token Plan answered
# HTTP 429 "Token Plan usage limit reached ... (2056)" for every request, but
# the ladder kept sending the flash/minimax rungs there, so jobs burned their
# whole retry budget on zero-diff attempts (01a14892-d979, -d8c0,
# 01a1483e-912c, 01a14838-36ae all ended on author_model MiniMax-M3). A worker
# that observes such a limit writes a host-local marker under its state dir and
# the ladder skips the MiniMax rungs until it lapses. Detection, the marker
# format and the substitution live here (not in provenance.py): kept local to
# this feature so the concurrent failure-reporting change stays mergeable.
COOLDOWN_SECS = 60 * 60.0
MINIMAX_COOLDOWN_PROVIDER = "minimax"
# Matched against zoder's JSON author_error / reviewer error / raw stderr.
_RATE_LIMIT_RE = re.compile(
    r"\b429\b|usage limit|rate[-_ ]?limit|quota|too many requests",
    re.IGNORECASE,
)
_MINIMAX_MODEL_RE = re.compile(r"^minimax", re.IGNORECASE)
_LIMIT_FIELDS = (
    "author_error",
    "reviewer_error",
    "error",
    "stderr",
    "message",
    "detail",
)


def _epoch(now=None) -> float:
    """Accept a UTC epoch, a ``struct_time`` (what deepseek_peak takes), or None."""
    if now is None:
        return time.time()
    if isinstance(now, (int, float)):
        return float(now)
    return float(calendar.timegm(time.struct_time(now)))


def worker_state_dir() -> str:
    """Where host-local worker state lives (``$HIVE_WORKER_STATE_DIR`` or XDG)."""
    env = os.environ.get("HIVE_WORKER_STATE_DIR", "").strip()
    if env:
        return os.path.expanduser(env)
    base = os.environ.get("XDG_STATE_HOME", "").strip() or os.path.expanduser(
        "~/.local/state"
    )
    return os.path.join(os.path.expanduser(base), "hive-worker")


def cooldown_path(
    provider: str = MINIMAX_COOLDOWN_PROVIDER, state_dir: str | None = None
) -> str:
    root = state_dir if state_dir is not None else worker_state_dir()
    return os.path.join(root, "cooldowns", f"{provider.lower()}.json")


def read_cooldown(
    provider: str = MINIMAX_COOLDOWN_PROVIDER,
    *,
    now=None,
    state_dir: str | None = None,
) -> dict | None:
    """The LIVE cooldown marker for ``provider``, else None.

    Expired or malformed markers are inert and removed best-effort: a corrupt
    state file must never block a rung (fail open on a busy host).
    """
    path = cooldown_path(provider, state_dir)
    marker = _load_cooldown(path)
    if marker is None:
        return None
    if _epoch(now) >= marker["expires_at"]:
        # Re-check under the lock: a writer may have replaced the expired
        # marker since we read it, and that fresh marker must survive.
        current = None
        with contextlib.suppress(OSError), _cooldown_lock(path):
            current = _load_cooldown(path)
            if current is not None and _epoch(now) >= current["expires_at"]:
                os.remove(path)
                current = None
        return current
    return marker


def _load_cooldown(path: str) -> dict | None:
    """The marker at ``path`` (live or expired), or None if absent/corrupt."""
    try:
        with open(path, encoding="utf-8") as fh:
            marker = json.load(fh)
        marker["expires_at"] = float(marker["expires_at"])
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return None
    return marker


@contextlib.contextmanager
def _cooldown_lock(path: str):
    """Exclusive flock serializing one marker's read-modify-write.

    Every zeroclaw slot on a host is its own process, so two slots hitting a
    429 together could both read the old marker and the shorter expiry could
    land last. flock is per open file description, so it also serializes
    threads that open the lock file separately.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(f"{path}.lock", "a", encoding="utf-8") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def write_cooldown(
    model: str,
    *,
    reason: str = "",
    provider: str = MINIMAX_COOLDOWN_PROVIDER,
    ttl_secs: float = COOLDOWN_SECS,
    now=None,
    state_dir: str | None = None,
) -> dict:
    """Write (or extend) the marker atomically and return it.

    A still-live marker is only ever extended, never shortened: a second 429
    from another slot on the same host must not cut the hold-off short.
    """
    path = cooldown_path(provider, state_dir)
    now_epoch = _epoch(now)
    marker = {
        "provider": provider,
        "model": model or "",
        "created_at": now_epoch,
        "expires_at": now_epoch + ttl_secs,
        "reason": (reason or "")[:500],
    }
    with _cooldown_lock(path):
        # A marker written moments ago (from another slot's 429) still counts:
        # the attempt failed now, so the hold-off is measured from its own
        # expiry. Read and replace under one lock so no writer can shorten it.
        current = _load_cooldown(path)
        if current is not None and now_epoch < current["expires_at"]:
            marker["created_at"] = min(
                float(current.get("created_at", now_epoch)), now_epoch
            )
            marker["expires_at"] = max(current["expires_at"], marker["expires_at"])
            marker["model"] = current.get("model") or marker["model"]
        fd, tmp = tempfile.mkstemp(
            dir=os.path.dirname(path), prefix=".cooldown-", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(marker, fh)
            os.replace(tmp, path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise
    return marker


def rate_limited(text: str | None) -> bool:
    return bool(text) and bool(_RATE_LIMIT_RE.search(text))


def loop_limit_models(stdout: str | None) -> list[str]:
    """MiniMax models whose zoder JSON record shows a rate-limit/quota error.

    Reads only the loop's last JSON verdict (the same record
    ``parse_loop_models`` uses) and looks for a rate-limit line in its error
    fields; an ``author_model`` / reviewer ``model`` starting with ``MiniMax``
    names the model to hold off. Empty when the run was not MiniMax-walled.
    """
    if not stdout:
        return []
    record = None
    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith("{") or len(line) > _MAX_JSON_LINE:
            continue
        try:
            candidate = json.loads(line)
        except (ValueError, RecursionError):
            continue
        if isinstance(candidate, dict) and _first_model(candidate):
            record = candidate  # keep the LAST record: the final verdict wins
    if record is None:
        return []
    models = []
    author = _first_model(record)
    if author and _MINIMAX_MODEL_RE.match(author) and _record_limited(record):
        models.append(author)
    for key in _REVIEWER_CONTAINERS:
        entries = record.get(key)
        if isinstance(entries, (dict, str)):
            entries = [entries]
        if not isinstance(entries, list):
            continue
        for entry in entries:
            model = _first_model(entry) if isinstance(entry, dict) else entry
            if (
                isinstance(model, str)
                and _MINIMAX_MODEL_RE.match(model.strip())
                and _record_limited(entry if isinstance(entry, dict) else {})
            ):
                models.append(model.strip())
    return list(dict.fromkeys(models))


def _record_limited(record: dict) -> bool:
    """The record (or one reviewer entry) carries a rate-limit/quota message."""
    return any(
        rate_limited(record[field])
        for field in _LIMIT_FIELDS
        if isinstance(record.get(field), str)
    )


def detect_minimax_limit(
    stdout: str | None,
    launched_agent: str | None = None,
    raw_output: str | None = None,
) -> str | None:
    """Model to cool down when this run hit a MiniMax rate-limit/quota, else None.

    Two signals: the loop JSON names a MiniMax model with a rate-limit error;
    or a MiniMax route was launched and the output carries a rate-limit line.
    """
    models = loop_limit_models(stdout)
    if models:
        return models[0]
    # Any MiniMax route: the minimax/minimax_flash aliases and explicit pins
    # such as minimax.M3 or MiniMax-M3 all reach the walled Token Plan.
    if (
        launched_agent
        and _MINIMAX_PIN_RE.match(launched_agent)
        and rate_limited(raw_output if raw_output is not None else stdout)
    ):
        return "MiniMax"
    return None


def cooldown_active(now=None) -> dict | None:
    """The live MiniMax cooldown marker, if any (see escalation_agent)."""
    return read_cooldown(MINIMAX_COOLDOWN_PROVIDER, now=now)


# Escalation ladder (operator 2026-10-03, reordered 2026-10-16 per
# ~/.claude/rules/orchestration-routing-20261014.md): a retried job moves up a
# rung instead of re-running the same model. Before this, every slot was pinned
# to `coder`, so a job the local coder could not do failed on every attempt with
# "loop ended unresolved" and zero edits. Rung = the job's retry_count.
#   coder   -> local TYDEUS coder (qwen38) / the host's HIVE_ZODER_AGENT
#   flash   -> MiniMax-M3.1-Flash, balanced with the funded DeepSeek account
#              off-peak (DeepSeek before MiniMax-M3, never mid-peak)
#   minimax -> MiniMax-M3
# The local rung stays first: a suitable fix is cheapest on TYDEUS. Everything
# after it is the policy's cloud order -- Flash/DeepSeek ahead of M3, never
# Claude/Anthropic, no paid frontier reseller, no retired :4100 route.
# A job may start on a given rung with the capability `route:<alias>`.
ZODER_LADDER = [
    rung.strip()
    for rung in os.environ.get("HIVE_ZODER_LADDER", "coder,flash,minimax").split(",")
    if rung.strip()
]


def deepseek_peak(now_utc=None) -> bool:
    """DeepSeek bills 2x 01:00-04:00 and 06:00-10:00 UTC, Mon-Fri (UTC weekday)."""
    now_utc = now_utc or time.gmtime()
    if now_utc.tm_wday >= 5:
        return False
    return 1 <= now_utc.tm_hour < 4 or 6 <= now_utc.tm_hour < 10


# The ladder rungs whose MiniMax side the engine dispatches itself (the flash
# rung's is the separate minimax_flash alias), plus any explicit `route:` pin
# that names MiniMax at all (`route:minimax`, `route:minimax_flash`, or a model
# string such as `route:minimax.M3`).
_MINIMAX_PIN_RE = re.compile(r"^minimax", re.IGNORECASE)
MINIMAX_RUNGS = frozenset({"flash", "minimax", "minimax_flash"})
SUBSTITUTE_REMOTE = "deepseek"
SUBSTITUTE_LOCAL = "coder"


def minimax_substitute(now_utc=None) -> str:
    """Rung to use while MiniMax is cooled down.

    DeepSeek off-peak (the funded account is 1x there); during DeepSeek's
    2x peak window the Xiaomi MiMo flash alternate when this host has it
    (see mimo_alternate_available), else the local coder, so a cooled job
    never spends peak DeepSeek money on a rung that is not the top of the
    ladder.
    """
    if not deepseek_peak(now_utc):
        return SUBSTITUTE_REMOTE
    return MIMO_AGENT if mimo_alternate_available() else SUBSTITUTE_LOCAL


# ---------------------------------------------------------------------------
# Xiaomi MiMo flash alternate (operator 2026-10-26: "Go with your
# recommendations"). mimo-v2.6-flash is a cheap-tier ALTERNATE, ranked BELOW
# DeepSeek flash off-peak and MiniMax-M3.1-Flash (it matched deepseek-flash on
# review and coding in the 2026-10-26 benchmark but was 3-4x slower on large
# prompts). It is used only when neither preferred cheap route is usable:
#   * MiniMax is cooled down (quota-walled) AND DeepSeek is in its peak window
#     -> minimax_substitute() returns mimo_flash instead of the local coder;
#   * the flash rung would go to MiniMax-M3.1-Flash during DeepSeek's peak
#     while the MiniMax 5-hour window is below MINIMAX_LOW_PCT -> mimo_flash.
# Nothing else changes: the ladder order, the flash balance, the off-peak
# DeepSeek substitution and every reviewer are untouched. mimo-v2.6-pro has no
# automatic placement. The rung exists only on a host whose live zoder config
# carries the mimo_flash agent with a key (FleetOps deploy.py installs the key
# on STUDIO, ACHILLES, CERBERUS and MINOS only); everywhere else the old
# behaviour holds. MiMo is never an author route for strict rescue jobs and
# never a reviewer (see review_model / codex_review_model).
MIMO_AGENT = "mimo_flash"
MIMO_MODEL = "mimo-v2.6-flash"
_MIMO_RE = re.compile(r"(^|[/._-])mimo([._-]|$)", re.IGNORECASE)
MINIMAX_LOW_PCT = float(os.environ.get("HIVE_MINIMAX_LOW_PCT", "25"))
MINIMAX_QUOTA_URL = "https://api.minimax.io/v1/coding_plan/remains"
MINIMAX_QUOTA_TTL = 600.0
MINIMAX_QUOTA_ERROR_TTL = 120.0
_mimo_cache: dict = {}


def is_mimo_model(name: str) -> bool:
    """True for a Xiaomi MiMo model id or agent alias (mimo_flash, mimo-v2.6-pro)."""
    return bool(_MIMO_RE.search((name or "").strip()))


def _live_zoder_config() -> dict | None:
    path = ZODER_CONFIG_PATH
    try:
        mtime = os.stat(path).st_mtime
    except OSError:
        return None
    cached = _mimo_cache.get("config")
    if cached and cached[0] == (path, mtime):
        return cached[1]
    if tomllib is None:
        return None
    try:
        with open(path, "rb") as fh:
            config = tomllib.load(fh)
    except (OSError, ValueError):
        config = None
    _mimo_cache["config"] = ((path, mtime), config)
    return config


def mimo_alternate_available() -> bool:
    """The host's live zoder config routes agent mimo_flash to mimo-v2.6-flash
    with a non-empty key. False on any read or shape problem (fail closed: the
    pre-MiMo behaviour)."""
    config = _live_zoder_config()
    if not isinstance(config, dict):
        return False
    agent = (config.get("agents") or {}).get(MIMO_AGENT) or {}
    ref = str(agent.get("model_provider") or "")
    if agent.get("enabled") is False or not ref.startswith("custom."):
        return False
    profile = (
        ((config.get("providers") or {}).get("models") or {}).get("custom") or {}
    ).get(ref.split(".", 1)[1]) or {}
    key = str(profile.get("api_key") or "").strip()
    return profile.get("model") == MIMO_MODEL and bool(key) and not key.startswith("__")


def _minimax_quota_key() -> str:
    config = _live_zoder_config() or {}
    custom = ((config.get("providers") or {}).get("models") or {}).get("custom") or {}
    for alias in ("minimax_flash", "minimax"):
        key = str((custom.get(alias) or {}).get("api_key") or "").strip()
        if key and not key.startswith("__"):
            return key
    return ""


def _fetch_minimax_interval_pct() -> float | None:
    """MiniMax Token Plan 5-hour window remaining percent, or None (unknown)."""
    key = _minimax_quota_key()
    if not key:
        return None
    req = urllib.request.Request(
        MINIMAX_QUOTA_URL, headers={"authorization": "Bearer " + key}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
    except (urllib.error.URLError, OSError, ValueError):
        return None
    if not isinstance(body, dict) or (body.get("base_resp") or {}).get("status_code") != 0:
        return None
    rows = body.get("model_remains") or []
    if not rows or not isinstance(rows[0], dict):
        return None
    pct = rows[0].get("current_interval_remaining_percent")
    return float(pct) if isinstance(pct, (int, float)) else None


def minimax_window_low(now=None) -> bool:
    """True when the MiniMax 5-hour window is below MINIMAX_LOW_PCT.

    Cached per process for MINIMAX_QUOTA_TTL (MINIMAX_QUOTA_ERROR_TTL after a
    failed read). Unknown counts as NOT low, so a quota endpoint outage keeps
    the pre-MiMo behaviour.
    """
    at = _epoch(now)
    cached = _mimo_cache.get("quota")
    if cached and at - cached[0] < (MINIMAX_QUOTA_TTL if cached[1] is not None else MINIMAX_QUOTA_ERROR_TTL):
        pct = cached[1]
    else:
        pct = _fetch_minimax_interval_pct()
        _mimo_cache["quota"] = (at, pct)
    return pct is not None and pct < MINIMAX_LOW_PCT


def _iso_utc(epoch: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch))


# preferred_models -> starting rung (the bus returns the submitter's ranked
# list on every claim). Only these exact names route; anything else -- Claude,
# a paid reseller, an EIH-hosted closed model, a typo -- is ignored, never
# guessed at. A provider prefix is accepted only for a verified fleet provider
# serving that same rung, so `nvidia/...` (EIH) or `siliconflow/...` never
# maps to the DeepSeek rung.
_PREFERRED_MODEL_RUNGS = {
    "coder": "coder",
    "qwen38": "coder",
    "flash": "flash",
    "deepseek": "deepseek",
    "deepseek-flash": "deepseek",
    "minimax_flash": "minimax_flash",
    "minimax": "minimax",
    "minimax-m3": "minimax",
}
_PREFERRED_FLASH_RE = re.compile(r"^minimax-m3\.1-flash(-preview)?$")
# Fleet zoder provider ids (config.fleet.toml / config.toml) -> the only rung
# that provider may serve. deepseek-direct is api.deepseek.com; the MiniMax ids
# are api.minimax.io. A prefix must name one of these AND agree with the model.
_DIRECT_PROVIDER_RUNGS = {
    "tydeus-coder": "coder",
    "custom.coder": "coder",
    "deepseek-direct": "deepseek",
    "custom.deepseek": "deepseek",
    "minimax-m3": "minimax",
    "custom.minimax": "minimax",
    "minimax-flash": "minimax_flash",
    "custom.minimax_flash": "minimax_flash",
}


def preferred_model_rung(job: dict) -> str:
    """The ladder rung named by the job's first routable preferred model, or ""."""
    models = job.get("preferred_models") or []
    if isinstance(models, str):
        models = [models]
    if not isinstance(models, (list, tuple)):
        return ""
    for raw in models:
        if not isinstance(raw, str) or not raw.strip() or forbidden_model(raw):
            continue
        name = raw.strip().lower()
        prefix = None
        if "/" in name:
            prefix, name = name.split("/", 1)
            if prefix not in _DIRECT_PROVIDER_RUNGS:
                continue
        rung = _PREFERRED_MODEL_RUNGS.get(name)
        if rung is None and _PREFERRED_FLASH_RE.match(name):
            rung = "minimax_flash"
        if rung and prefix is not None and _DIRECT_PROVIDER_RUNGS[prefix] != rung:
            continue
        if rung:
            return rung
    return ""


def escalation_agent(job: dict, now_utc=None) -> str:
    """Return the zoder engine agent for this attempt of `job`, or "" to defer
    to the host's own routing (no ladder, no host pin).

    Contract: `route:<alias>` names the STARTING rung; without a pin, the
    first routable entry of the job's `preferred_models` does (see
    preferred_model_rung). Each retry climbs one
    rung from there and the top rung repeats once reached. An alias that is
    not on the ladder becomes a rung of its own ahead of the paid rungs (the
    local coder is skipped, since an explicit route outranks it)."""
    caps = job.get("required_capabilities") or []
    pinned = [
        c.split(":", 1)[1]
        for c in caps
        if isinstance(c, str) and c.startswith("route:")
    ]
    ladder = list(ZODER_LADDER)
    start = 0
    # A route pin wins; otherwise the submitter's first routable preferred
    # model picks the starting rung the same way.
    first = pinned[0] if pinned and pinned[0] else preferred_model_rung(job)
    if first:
        if first in ladder:
            start = ladder.index(first)
        else:
            # An explicit route outranks the local coder, so a retry climbs to
            # the next paid rung rather than dropping back to qwen38.
            ladder = [first] + [r for r in ladder if r != "coder"]
    if not ladder:
        return ZODER_AGENT
    try:
        retries = max(0, int(job.get("retry_count") or 0))
    except (TypeError, ValueError):
        retries = 0
    rung = ladder[min(start + retries, len(ladder) - 1)]
    cooled = cooldown_active(now_utc)
    pinned_minimax = bool(pinned) and bool(_MINIMAX_PIN_RE.match(pinned[0]))
    if cooled and (rung in MINIMAX_RUNGS or pinned_minimax):
        # The MiniMax Token Plan is quota-walled (see cooldown_active): sending
        # the attempt there buys a zero-diff failure and burns a retry.
        substitute = minimax_substitute(now_utc)
        print(
            f"[hive-worker:{WORKER_KIND}] job {job.get('id', '')}: rung {rung} "
            f"held off (MiniMax cooldown until {_iso_utc(cooled['expires_at'])}, "
            f"model={cooled.get('model') or 'unknown'}) -> {substitute}"
            + (" [explicit route pin skipped]" if pinned_minimax else ""),
            flush=True,
        )
        rung = substitute
    if rung == "coder" and ZODER_AGENT:
        return ZODER_AGENT
    if rung == "flash":
        digest = hashlib.sha256(str(job.get("id", "")).encode()).digest()
        if digest[0] % 2 == 0 and not deepseek_peak(now_utc):
            return "deepseek"
        # MiniMax side. During DeepSeek's peak window with the MiniMax 5-hour
        # window low, the MiMo flash alternate takes it (when this host has it).
        if (
            deepseek_peak(now_utc)
            and mimo_alternate_available()
            and minimax_window_low(now_utc)
        ):
            print(
                f"[hive-worker:{WORKER_KIND}] job {job.get('id', '')}: flash rung "
                f"-> {MIMO_AGENT} (DeepSeek peak, MiniMax 5h window below "
                f"{MINIMAX_LOW_PCT:g}%)",
                flush=True,
            )
            return MIMO_AGENT
        return "minimax_flash"
    return rung


# Per-process attempt fallback, used only when the bus GET cannot supply the
# authoritative retry_count (see job_with_attempt). Keyed by job id.
_local_attempts: dict[str, int] = {}


def job_with_attempt(job: dict) -> dict:
    """Return `job` carrying the retry counter for THIS attempt.

    `POST /v1/jobs/next` builds its claim payload field-by-field and never
    includes `retry_count`, so every attempt looked like attempt 0 and a
    retried job never climbed ZODER_LADDER (observed 2026-10-03: a requeued
    job ran its second attempt on the `flash` rung with `retry_count=0`
    instead of climbing to `minimax`/MiniMax-M3). The value IS stored on the
    job row and is incremented by the bus while making the job claimable
    again, so re-read the row (GET /v1/jobs/{id}) and merge it in. This fixes
    the worker without changing bus behaviour.

    If the bus cannot be read, fall back to an attempt counter this process
    observes across claims of the same job, so a retry still climbs even
    during a bus blip. A claim that already carries `retry_count` is trusted
    as-is (cheap, no extra request).
    """
    if job.get("retry_count") is not None:
        return job
    job_id = job.get("id")
    if not job_id:
        return job
    code, fresh = _http("GET", f"/v1/jobs/{job_id}", timeout=10)
    if code == 200 and isinstance(fresh, dict) and fresh.get("retry_count") is not None:
        try:
            attempt = max(0, int(fresh["retry_count"]))
        except (TypeError, ValueError):
            attempt = _local_attempts.get(job_id, -1) + 1
        _local_attempts[job_id] = attempt
        merged = dict(job)
        merged["retry_count"] = attempt
        if fresh.get("max_retries") is not None:
            merged["max_retries"] = fresh["max_retries"]
        return merged
    attempt = _local_attempts.get(job_id, -1) + 1
    _local_attempts[job_id] = attempt
    return {**job, "retry_count": attempt}


# ARGONAS canonical bare repos, as ssh://jasonperlow@<ARGONAS>/<git root>.
# Extend as needed -- a job whose `repo` field isn't a URL and isn't in this
# map gets DECLINED, never guessed at.
#
# 2026-10-25 (job 01a17539-d8ca): every entry is an ARGONAS ssh URL *as
# jasonperlow*. Two entries used to be public GitLab https URLs ("zoder",
# "artistpack") and the rest used root@; the zoder job's branch was pushed to
# public GitLab because the worker took REPO_MAP verbatim. A forge URL here is
# a legal FETCH name for a job to *name*, but never a push target, so the map
# now only ever carries the private clone.
_FLEET_SLUGS = (
    "riskyeats",
    "zeroclaw",
    "mnemos",
    "cix-installer",
    "meta-cix",
    "ic-engine",
    "florida-licenses",
    "graeae-hive",
    "zoder",
    "artistpack",
    "ncz-screensavers",
    "wayshade",
    "singularity-plugins",
    "amd64-kernel-t2",
    "fleet-ops",
)
REPO_MAP = {slug: push_policy.argonas_url_for_slug(slug) for slug in _FLEET_SLUGS}
# GitLab's ncz-os/graeae-hive-mind is this same codebase; ARGONAS keeps it as
# graeae-hive.git. Without this alias a job naming the GitLab repo was
# declined with no_repo_mapping.
REPO_MAP["graeae-hive-mind"] = REPO_MAP["graeae-hive"]

_urn: str = ""
_running = True
_current_job_id: str | None = None
# The exact model/agent THIS worker launched for the job it is currently
# running. Set from the command that was actually built (never inferred later),
# so a claim renewal carries the same provenance as the first PATCH.
_current_provenance: dict = {}
# result['models'] determined from the loop's own JSON output, once the run ends.
_current_run_models: dict | None = None
# The zoder ledger window [start, end] of the run in flight; result['models']
# is attributed from the provider calls that landed inside it.
_current_run_window: tuple[float, float] | None = None
_claim_renewal_blocked_id: str | None = None

# ARGONAS login preflight (2026-10-26). Every job clones from ARGONAS, so a
# worker that cannot log in there must not claim jobs: before this, a broken
# transport failed every claimed job with clone_transport_error and burned
# its retries fleet-wide. Checked before each claim, cached for
# ARGONAS_CHECK_TTL seconds when it passes and retried after
# ARGONAS_RECHECK_SECS when it fails; the state rides on every heartbeat.
ARGONAS_CHECK_TTL = float(os.environ.get("HIVE_ARGONAS_CHECK_TTL", "600"))
ARGONAS_RECHECK_SECS = float(os.environ.get("HIVE_ARGONAS_RECHECK_SECS", "60"))
_argonas_auth: dict = {"ok": None, "checked_at": 0.0, "detail": ""}


def argonas_auth_ok(now: float | None = None) -> bool:
    """True when this worker can log in to ARGONAS; rechecks on a schedule."""
    now = time.time() if now is None else now
    age = now - _argonas_auth["checked_at"]
    ok = _argonas_auth["ok"]
    if ok is not None and age < (ARGONAS_CHECK_TTL if ok else ARGONAS_RECHECK_SECS):
        return bool(ok)
    ok, detail = codex_delivery.argonas_login_check()
    if ok != _argonas_auth["ok"]:
        print(
            f"[hive-worker:{WORKER_KIND}] argonas_auth={'ok' if ok else 'fail'}"
            + (f" ({detail})" if detail else "")
            + ("" if ok else "; not claiming jobs until it passes"),
            flush=True,
        )
    _argonas_auth.update(ok=ok, checked_at=now, detail=detail)
    return ok


#: Operator drain switch (2026-10-26). While this file exists the worker
#: finishes its current job and claims nothing new, so a host can be emptied
#: for a config deploy without stopping units or killing in-flight work.
#: Remove the file to resume. Reported on every heartbeat as ``draining``.
DRAIN_FILE = os.path.expanduser(
    os.environ.get("HIVE_WORKER_DRAIN_FILE", "~/.config/hive-worker/drain")
)


def draining() -> bool:
    return os.path.exists(DRAIN_FILE)


#: Build-host busy gate (2026-10-28). A host that is running an image or system
#: build must not take new Hive jobs: they compete for the CPU, RAM and IO the
#: build needs, and the build's cleanup rules assume nothing else is running.
#: Between jobs only -- a job already running is never interrupted. Two signals:
#:   * a marker file in ``BUSY_DIR`` (a build script ``touch``es one; a pid on
#:     its first line makes it stale when that process is gone, otherwise it
#:     expires after ``BUSY_MARKER_TTL`` seconds);
#:   * a running build orchestrator named in ``HIVE_WORKER_BUSY_PROCS``.
BUSY_DIR = os.path.expanduser(
    os.environ.get("HIVE_WORKER_BUSY_DIR", "~/.config/hive-worker/busy.d")
)
BUSY_MARKER_TTL = float(os.environ.get("HIVE_WORKER_BUSY_TTL", str(12 * 3600)))
BUSY_PROCS = tuple(
    name.strip()
    for name in os.environ.get(
        "HIVE_WORKER_BUSY_PROCS",
        "build-image.sh,bitbake,mkosi,mksquashfs,mmdebstrap,debootstrap",
    ).split(",")
    if name.strip()
)


#: Pipeline participation (2026-10-28): a host that is a stage of a pipeline
#: (CI job on a GitLab runner / Buildkite agent, the RiskyEats pipeline, the apt
#: publish) takes no new jobs either. Two signals besides the busy markers:
#:   * a named pipeline process, ``HIVE_WORKER_PIPELINE_PROCS``;
#:   * any process owned by a CI user, ``HIVE_WORKER_PIPELINE_USERS`` (the
#:     runner daemon itself is ignored; idle hosts own no such processes).
PIPELINE_PROCS = tuple(
    name.strip()
    for name in os.environ.get(
        "HIVE_WORKER_PIPELINE_PROCS",
        "ncz-reprepro-publish.sh,reprepro,riskyeats.pipeline,pipeline.py",
    ).split(",")
    if name.strip()
)
PIPELINE_USERS = tuple(
    name.strip()
    for name in os.environ.get(
        "HIVE_WORKER_PIPELINE_USERS", "gitlab-runner,buildkite-agent"
    ).split(",")
    if name.strip()
)
_PIPELINE_DAEMONS = ("gitlab-runner", "gitlab-ci-multi-runner", "buildkite-agent")


def _pipeline_uids() -> dict[int, str]:
    out: dict[int, str] = {}
    for name in PIPELINE_USERS:
        try:
            import pwd

            out[pwd.getpwnam(name).pw_uid] = name
        except (ImportError, KeyError):
            continue
    return out


def _busy_marker_pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def build_busy(now: float | None = None) -> str:
    """Why this host is busy with a build, or "" when it may claim work."""
    now = time.time() if now is None else now
    try:
        names = sorted(os.listdir(BUSY_DIR))
    except OSError:
        names = []
    for name in names:
        path = os.path.join(BUSY_DIR, name)
        try:
            age = now - os.path.getmtime(path)
            with open(path, encoding="utf-8", errors="replace") as fh:
                first = fh.readline().strip()
        except OSError:
            continue
        if first.isdigit():
            if _busy_marker_pid_alive(int(first)):
                return f"marker {name} (pid {first})"
        elif age < BUSY_MARKER_TTL:
            return f"marker {name}"
    # CI users are dedicated service accounts (gitlab-runner, buildkite-agent):
    # on an idle host they own no processes besides the daemon, so any other
    # process of theirs is a pipeline job. Do not list a human's login here.
    ci_uids = _pipeline_uids()
    if not (BUSY_PROCS or PIPELINE_PROCS or ci_uids):
        return ""
    try:
        pids = [p for p in os.listdir("/proc") if p.isdigit()]
    except OSError:
        return ""
    for pid in pids:
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as fh:
                argv = [a.decode("utf-8", "replace") for a in fh.read().split(b"\0")]
        except OSError:
            continue
        # argv[0], then for interpreter-run scripts argv[1] (``bash build-image.sh``)
        # and ``python -m module`` (argv[2])
        bases = [os.path.basename(a) for a in argv[:3] if a]
        for base in bases:
            if base in BUSY_PROCS:
                return f"process {base} (pid {pid})"
            if base in PIPELINE_PROCS:
                return f"pipeline {base} (pid {pid})"
        if ci_uids:
            try:
                uid = os.stat(f"/proc/{pid}").st_uid
            except OSError:
                continue
            # the runner/agent daemon is always up; only its job processes count
            if uid in ci_uids and not (bases[:1] and bases[0] in _PIPELINE_DAEMONS and argv[1:2] and argv[1] in ("run", "start")):
                return f"pipeline CI job as {ci_uids[uid]} ({bases[0] if bases else '?'}, pid {pid})"
    return ""


def argonas_auth_state() -> str:
    ok = _argonas_auth["ok"]
    return "unknown" if ok is None else ("ok" if ok else "fail")


def _cgroup_has_other_processes() -> bool:
    """Keep a shared engine child alive even after this worker becomes idle."""
    try:
        with open("/proc/self/cgroup", encoding="ascii") as handle:
            group = next(
                line.split(":", 2)[2].strip()
                for line in handle
                if line.startswith("0::")
            )
        root = os.path.join("/sys/fs/cgroup", group.lstrip("/"))
        if not os.path.isdir(root):
            return True
        seen = False

        def _walk_error(error: OSError) -> None:
            raise error

        for directory, _, filenames in os.walk(root, onerror=_walk_error):
            if "cgroup.procs" not in filenames:
                continue
            seen = True
            with open(
                os.path.join(directory, "cgroup.procs"), encoding="ascii"
            ) as handle:
                if any(int(pid) != os.getpid() for pid in handle.read().split()):
                    return True
        return not seen
    except (OSError, StopIteration, ValueError):
        # Unknown cgroup layout: defer the restart rather than risk its children.
        return True


def restart_after_source_update() -> None:
    """Exit only between claims; systemd then loads the installed revision."""
    if not AUTO_RESTART:
        return
    try:
        changed = os.stat(__file__).st_mtime_ns != SOURCE_MTIME_NS
    except OSError:
        return
    if changed and not _cgroup_has_other_processes():
        print(
            f"[hive-worker:{WORKER_KIND}] source updated; restarting while idle",
            flush=True,
        )
        raise SystemExit(75)


def _signal(signum, frame):
    global _running
    print(f"[hive-worker:{WORKER_KIND}] signal {signum} -- shutting down", flush=True)
    _running = False


signal.signal(signal.SIGTERM, _signal)
signal.signal(signal.SIGINT, _signal)


def _http(
    method: str, path: str, body: dict | None = None, timeout: float = 15
) -> tuple[int, dict | None]:
    url = f"{HIVE_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method, headers={"content-type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, None
    except Exception as e:
        print(
            f"[hive-worker:{WORKER_KIND}] http error {method} {path}: {e}", flush=True
        )
        return 0, None


def registered_models() -> dict:
    """The models this worker is configured to launch, for /v1/agents/register.

    A zeroclaw worker used to register the literal string 'zoder', which says
    nothing about what it runs. It now registers the configured coder (author)
    and reviewer aliases' real models -- the ones `zoder agents` lists for
    this host -- so the bus's agent roster names the fleet's actual models.
    Unknown routes are omitted (never a placeholder); codex keeps CODEX_MODEL.
    """
    if WORKER_KIND != "zeroclaw":
        return {"model": CODEX_MODEL, "models": [CODEX_MODEL]}
    primary = ""
    if ZODER_AGENT and not forbidden_model(ZODER_AGENT):
        primary = launch_model(ZODER_AGENT) or ""
    reviewer_alias = ZODER_REVIEWER or ZODER_REVIEWER_ALT
    reviewer = launch_model(reviewer_alias) or ""
    if not primary:
        # No author pin: this host's first ladder rung is what it launches, so
        # register that model rather than nothing (and never 'zoder').
        first_rung = ZODER_LADDER[0] if ZODER_LADDER else ""
        if first_rung:
            primary = launch_model(first_rung) or ""
    models: list[str] = []
    for model in (primary, reviewer):
        if model and model not in models:
            models.append(model)
    return {"model": primary or None, "models": models}


def register() -> None:
    global _urn
    driver = _find_driver("zoder" if WORKER_KIND == "zeroclaw" else "codex")
    if not os.path.isfile(driver) or not os.access(driver, os.X_OK):
        print(
            f"[hive-worker:{WORKER_KIND}] driver unavailable; refusing registration",
            flush=True,
        )
        raise SystemExit(2)
    if not _driver_supports_verified_delivery(driver):
        print(
            f"[hive-worker:{WORKER_KIND}] driver lacks verified-delivery CLI contract; refusing registration",
            flush=True,
        )
        raise SystemExit(2)
    caps = [
        "python",
        "git",
        "bash",
        "linux-compile",
        WORKER_KIND,
        "verified-git-delivery",
    ]
    if WORKER_KIND == "zeroclaw" and _driver_supports_strict_routes(driver):
        try:
            load_strict_routes()
            caps.append("strict-rescue-routes")
        except ValueError:
            pass
    launches = registered_models()
    body = {
        "runtime": WORKER_KIND,
        "kind": WORKER_KIND,
        "host": AGENT_HOST,
        "instance": f"hive-worker-{WORKER_KIND}-{os.environ.get('HIVE_WORKER_SLOT', '1')}",
        "pid": os.getpid(),
        "model": launches["model"],
        "models": launches["models"],
        "provider": "local",
        "autonomy_level": "autonomous",
        "capabilities": caps,
        "version": "hive_worker.py/2",
        # The stable slot lets the bus replace stale registrations on restart;
        # job UUIDs still isolate workspaces.
        "metadata": {
            "driver": WORKER_KIND,
            "workspace_root": WORKSPACE_ROOT,
            "slot": os.environ.get("HIVE_WORKER_SLOT", "1"),
            "authored_by": ZODER_AGENT if WORKER_KIND == "zeroclaw" else "codex",
            "reviewed_by": ZODER_REVIEWER if WORKER_KIND == "zeroclaw" else "",
        },
    }
    code, resp = _http("POST", "/v1/agents/register", body)
    if code == 200 and resp:
        _urn = resp["urn"]
        print(
            f"[hive-worker:{WORKER_KIND}] registered urn={_urn} "
            f"models={launches['models']}",
            flush=True,
        )
    else:
        print(
            f"[hive-worker:{WORKER_KIND}] register failed code={code} resp={resp}",
            flush=True,
        )
        sys.exit(1)


def heartbeat_loop() -> None:
    global _claim_renewal_blocked_id
    while _running:
        time.sleep(HEARTBEAT_INTERVAL)
        if not _urn:
            continue
        _hb_code, _ = _http(
            "POST",
            "/v1/agents/heartbeat",
            {
                "urn": _urn,
                "metadata": {
                    "current_job": _current_job_id,
                    "argonas_auth": argonas_auth_state(),
                    "draining": draining(),
                    "build_busy": build_busy(),
                },
            },
            timeout=10,
        )
        if _hb_code == 404:
            # The bus no longer knows this URN (reaped after a long stall, or a
            # bus restore): without re-registering, the worker 404s here and on
            # /v1/jobs/next forever (observed live 2026-10-16: hydra worker
            # urn:...:70ed36aa hammered 404s for hours). Re-register and carry on.
            print(
                f"[hive-worker:{WORKER_KIND}] heartbeat 404 (agent not registered), re-registering",
                flush=True,
            )
            try:
                register()
            except SystemExit:
                return
            continue
        job_id = _current_job_id
        if not job_id or job_id == _claim_renewal_blocked_id:
            continue
        # An agent heartbeat does not refresh the job's 30-minute lease.
        # A same-status PATCH does, without replacing an existing result.
        code, _ = _http(
            "PATCH",
            f"/v1/jobs/{job_id}",
            {
                "status": "claimed",
                "claimed_by": _urn,
            },
            timeout=15,
        )
        if code in (403, 409):
            # The claim was reassigned or the job became terminal/cancelled.
            # Never try to claim it again through this heartbeat.
            _claim_renewal_blocked_id = job_id
            print(
                f"[hive-worker:{WORKER_KIND}] WARNING: claim renewal rejected "
                f"for {job_id}: code={code} (provenance="
                f"{_claim_provenance(job_id).get('claimed_model')})",
                flush=True,
            )
        elif code != 200:
            print(
                f"[hive-worker:{WORKER_KIND}] WARNING: claim renewal failed "
                f"for {job_id}: code={code}",
                flush=True,
            )


# Jobs are submitted only by registered orchestrator agents (bus-enforced,
# require_orchestrator_submitter), so this isn't an adversarial-multi-tenant
# boundary -- but a job's `repo` field still shouldn't be able to point git
# (which inherits this host's SSH agent / credential helpers) at an arbitrary
# host. Only resolve against the known-repo map; never trust a bare URL from
# the job (codex review finding, 2026-09-14).
# URL forms are accepted ONLY when they normalize to a name already in
# REPO_MAP; the clone then uses the REPO_MAP URL, never the job's string. That
# keeps the 2026-09-14 guarantee (no arbitrary hosts) while letting jobs that
# name the repo by its GitLab or ARGONAS URL resolve (4 jobs sat queued as
# no_repo_mapping on 2026-10-03 for exactly this).
_REPO_URL_RES = (
    re.compile(
        r"^https://gitlab\.com/(?:ncz-os|argonautsystems)/([A-Za-z0-9._-]+?)(?:\.git)?/?$"
    ),
    re.compile(
        r"^git@gitlab\.com:(?:ncz-os|argonautsystems)/([A-Za-z0-9._-]+?)(?:\.git)?$"
    ),
    re.compile(
        r"^(?:ssh://)?(?:root|jasonperlow)@192\.168\.207\.101[:/]+mnt/datapool/git/([A-Za-z0-9._-]+?)\.git/?$"
    ),
)


_REGISTRY_TTL_SECONDS = 300.0
_registry_cache: dict[str, tuple[float, str | None]] = {}


def _registry_clone_block(slug: str) -> dict | None:
    """The registry's ``clone`` block for ``slug`` (cached), or None."""
    now = time.time()
    cached = _registry_cache.get(slug)
    if cached and now - cached[0] < _REGISTRY_TTL_SECONDS:
        return cached[1]
    clone: dict | None = None
    try:
        code, body = _http("GET", f"/v1/repos/{slug}", timeout=10)
        if code == 200 and isinstance(body, dict):
            raw = body.get("clone")
            if isinstance(raw, dict):
                clone = raw
    except Exception:
        clone = None
    _registry_cache[slug] = (now, clone)
    return clone


def _registry_repo_url(slug: str) -> str | None:
    """Resolve a repo slug to its PRIVATE ARGONAS clone URL, or None.

    The bus serves the authoritative, auto-updating catalog at /v1/repos (built
    from the gitlab PAT + github + ARGONAS + buildkite + R2). REPO_MAP is only a
    bootstrap/offline fallback; without this, every repo not hand-added to
    REPO_MAP is declined as no_repo_mapping (the exact gap 2026-10-16). The URL
    still comes from the *trusted catalog*, never the job string, so the
    2026-09-14 "no arbitrary host" guarantee holds. Cached with a short TTL.

    ONLY ``clone.ssh`` on the ARGONAS host is accepted (2026-10-25, job
    01a17539-d8ca). The previous fallback chain -- ``ssh``, then ``nfs``, then
    the first value of ``clone.remotes`` -- chose a *forge* URL for a repo
    that also lists ARGONAS, and the zoder job's branch was pushed to public
    GitLab. ``clone.remotes``/``clone.forges`` are never consulted now: a
    repo with no ARGONAS ``clone.ssh`` resolves to None, which the caller
    turns into ``failure_reason=no_private_remote`` (fail closed). A
    ``root@`` ARGONAS URL is rewritten to jasonperlow.
    """
    if not slug:
        return None
    return push_policy.resolve_private_clone_url(_registry_clone_block(slug))


def resolve_repo_url(job: dict) -> str | None:
    """The job's ARGONAS (private) clone URL, or None if it has none.

    Whatever the job names -- a bare slug, a GitLab URL, an ARGONAS URL --
    the returned URL is always the registry/REPO_MAP ARGONAS clone as
    jasonperlow. A forge URL is only ever a *name* here, never a target.
    """
    repo = (job.get("repo") or "").strip()
    if not repo:
        return None
    if repo in REPO_MAP:
        return REPO_MAP[repo]
    for rx in _REPO_URL_RES:
        m = rx.match(repo)
        if m:
            slug = m.group(1)
            return REPO_MAP.get(slug) or _registry_repo_url(slug)
    if any(t in repo for t in ("://", "@", "/")):
        # A URL or qualified path that did NOT match a known forge/ARGONAS
        # pattern: refuse. Without this the registry fallback would query the bus
        # with arbitrary URL-shaped slugs (observed live 2026-10-16: GET
        # /v1/repos/https%3A//evil.example/x.git). Only bare slugs are looked up.
        return None
    return _registry_repo_url(repo)


def repo_slug(job: dict) -> str | None:
    """The registry slug a job's ``repo`` field names, or None."""
    repo = (job.get("repo") or "").strip()
    if not repo:
        return None
    if repo in REPO_MAP:
        return "graeae-hive" if repo == "graeae-hive-mind" else repo
    for rx in _REPO_URL_RES:
        m = rx.match(repo)
        if m:
            return m.group(1)
    if any(t in repo for t in ("://", "@", "/")):
        return None
    return repo


def private_remote_failure(job: dict, repo_url: str | None) -> str | None:
    """Why this job has no legal push target, or None when it has one.

    Fleet policy (2026-10-25): the ONLY push target is the registry's ARGONAS
    clone. A repo with no ARGONAS clone fails closed with
    ``failure_reason=no_private_remote`` instead of pushing to a forge.
    """
    if not repo_url:
        return (
            f"no_private_remote: repo {job.get('repo')!r} has no ARGONAS clone "
            f"(ssh://{push_policy.ARGONAS_USER}@{push_policy.ARGONAS_HOST}"
            f"{push_policy.ARGONAS_GIT_ROOT}<repo>.git); automation pushes only "
            "to ARGONAS, never to a public forge"
        )
    reason = push_policy.push_refusal_reason(repo_url)
    if reason:
        return f"no_private_remote: resolved remote is not a legal ARGONAS push target ({reason})"
    return None


# Bus job ids are UUIDv7 (see agent_bus.py uuidv7()). Enforced here because
# job_id is used as a path component below -- an unvalidated id could
# traverse out of WORKSPACE_ROOT via "../" or an absolute path (codex review
# finding, 2026-09-14).
_JOB_ID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


def _redact_tree(value):
    """redact_secrets applied to every string in a JSON-shaped value."""
    if isinstance(value, str):
        return redact_secrets(value)
    if isinstance(value, dict):
        return {key: _redact_tree(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_redact_tree(item) for item in value]
    return value


# F2 idempotency audit (2026-10-29): ONE terminal patch per (job id, status) per
# process. The bus's auto-retry branch fires on `req.status == "failed"`, not only
# on a transition, so a second `failed` PATCH increments retry_count and requeues a
# job the first patch already failed. run_job's always-terminal except path does
# exactly that after _run_job_inner has already failed the row. Renewals
# (claimed/running) are deliberately NOT deduped: repeating them is the point.
_TERMINAL_PATCH_STATUSES = frozenset({"done", "failed", "cancelled", "needs-review"})
_terminal_patched: set[tuple[str, str]] = set()
_TERMINAL_PATCH_MEMORY = 5000  # long-lived worker: bound the set, oldest first


def _terminal_patch_already_sent(job_id: str, status: str) -> bool:
    """True when this process already sent this exact terminal patch."""
    if status not in _TERMINAL_PATCH_STATUSES or not job_id:
        return False
    return (job_id, status) in _terminal_patched


def _remember_terminal_patch(job_id: str, status: str) -> None:
    if status not in _TERMINAL_PATCH_STATUSES or not job_id:
        return
    if len(_terminal_patched) >= _TERMINAL_PATCH_MEMORY:
        # A set has no order; dropping half is enough to keep a multi-day worker
        # from growing without bound, and a dropped entry can only cost one
        # redundant terminal patch (the bus still refuses a second terminal move
        # on a finished row).
        for _ in range(_TERMINAL_PATCH_MEMORY // 2):
            _terminal_patched.pop()
    _terminal_patched.add((job_id, status))


def patch_job(job_id: str, status: str, result: dict) -> bool:
    # PATCH /v1/jobs/{id} requires claimed_by to match the job's current
    # claimant (agent_bus.py: "job update requires claimed_by to match the
    # current claimant") -- omitting it 403s every single call (found live
    # on PROTEUS, 2026-09-14: every job ended up stranded in 'claimed'
    # because this worker never sent it).
    # Every free-text field (tail, finalizer/attribution stderr, exception
    # text) is redacted here, once, so no path can post a credential.
    body = {"status": status, "result": _redact_tree(result), "claimed_by": _urn}
    if _terminal_patch_already_sent(job_id, status):
        print(
            f"[hive-worker:{WORKER_KIND}] {job_id}: terminal patch {status} already sent "
            "by this process — skipping the duplicate (it would spend a retry)",
            flush=True,
        )
        return True
    if status in ("claimed", "running", "done", "needs-review", "failed"):
        # Provenance-in-progress: the bus serializes claimed_model/provider/
        # runtime/cost_tier, so every renewal keeps reporting the real route
        # instead of the registration-time placeholder.
        body.update(_claim_provenance(job_id))
    code, _ = _http(
        "PATCH",
        f"/v1/jobs/{job_id}",
        body,
        timeout=15,
    )
    if code != 200:
        print(
            f"[hive-worker:{WORKER_KIND}] WARNING: patch({job_id}, {status}) got code={code} -- "
            f"job may be stranded in claimed state",
            flush=True,
        )
    if code == 200:
        _remember_terminal_patch(job_id, status)
    return code == 200


def _claim_provenance(job_id: str) -> dict:
    """claimed_* fields for a PATCH about `job_id`: this run's launched route
    when it is the current job, else nothing (never another job's route).

    ``claimed_provenance_source`` ('live_config' / 'static_fallback') is
    forwarded alongside the four ``claimed_*`` fields so every PATCH about
    the running job -- initial claim, renewal, terminal status -- carries
    the source alongside the route. A host whose live zoder config resolved
    coder to qwen38 is a different operational story from one that fell
    back to the corrected static table, and the bus / dashboard need the
    distinction to surface either correctly.
    """
    if job_id != _current_job_id or not _current_provenance:
        return {}
    # Never fall back to 'zoder': a route whose model this host cannot resolve
    # reports no model rather than the registration-time placeholder.
    model = _current_provenance.get("model")
    provider = _current_provenance.get("provider")
    tier = _current_provenance.get("cost_tier")
    source = _current_provenance.get("provenance_source")
    observed = _current_provenance.get("observed_model")
    if observed and observed != model:
        # The ledger of this run proves the launched alias was not the model
        # that served the calls (a fallback rung answered instead): claim the
        # model that really ran, so the bus never reports the ladder's intent.
        record = agent_model(observed) or {}
        model = observed
        provider = record.get("provider") or provider
        tier = record.get("cost_tier") or tier
        source = "ledger"
    return {
        "claimed_model": model,
        "claimed_provider": (provider if model else None),
        "claimed_runtime": _current_provenance.get("runtime"),
        "claimed_cost_tier": (tier if model else None),
        "claimed_provenance_source": source,
    }


def _worker_slot() -> str:
    """This worker's slot id (the one the register body already advertises)."""
    return os.environ.get("HIVE_WORKER_SLOT", "1").strip() or "1"


def record_observed_models(models: dict | None) -> None:
    """Store the ledger's author for this job on the running provenance.

    Called as soon as the run's ledger window is readable, so the claim
    PATCH (first one and every renewal) names the model that actually served
    the calls instead of the model the launched alias is configured to use.
    A ledger that agrees with the launched route changes nothing.
    """
    if not isinstance(models, dict):
        return
    author = models.get("author")
    if isinstance(author, str) and author:
        _current_provenance["observed_model"] = author


def release_job(job_id: str, reason: str) -> None:
    # NOT /v1/jobs/{id}/release -- that endpoint is a completely different
    # feature (zc-gate's human-gated "Release & Push" PR workflow, expects a
    # preview bundle already on the job). The real way to decline a claimed
    # job back to the queue is a PATCH to status=queued (same claimed_by
    # requirement as patch_job above), matching the worker_error/note shape
    # observed from the bus's own existing workers (thrash-guard dead-letters
    # a job after 3 such declines).
    patch_job(job_id, "queued", {"worker_error": reason, "via": "hive_worker"})


def release_without_retry(job_id: str, result: dict, reason: str) -> bool:
    """Hand a claimed job back to the queue for a host that still has budget.

    The bus increments `retry_count` on every `claimed -> queued` release (its
    backoff escalates per release) and exposes no field that lets a worker set
    the counter back: JobUpdate carries no retry_count/retry_backoff_until, and
    the only route that zeroes it is the admin requeue, which would also erase
    this job's real attempt history. So the worker does the two things it owns:

      * releases the claim now, in ONE `claimed -> queued` transition, so the
        job is claimable by another host (this is the "do not consume the retry
        on that host" outcome -- the attempt is not retried HERE); and
      * records the attempt counter and the cap's reset time on the result, so
        the next claimant (and the doctor) can see that this attempt was a
        host-budget artefact rather than job work that failed.

    This function is the ONLY transition this disposition makes: the caller
    must not also `patch_job` the terminal status, or the job would be moved
    twice (and a later `failed` would burn the retry this path exists to
    preserve). Returns True when the bus accepted the release.

    A release is counted by the bus as a decline; `budget_exceeded` is not in
    DECLINE_REASON_PREFIXES, so it never counts toward dead-lettering.
    """
    released = dict(result)
    released["worker_error"] = reason
    released["via"] = "hive_worker"
    return patch_job(job_id, "queued", released)


def completion_evidence(workdir: str, baseline: str, env: dict) -> dict:
    """Inspect Git, never infer delivered work from model prose or exit status."""

    def git(*args):
        return subprocess.check_output(
            ["git", "-C", workdir, *args],
            env=env,
            text=True,
            stderr=subprocess.PIPE,
            timeout=60,
        ).strip()

    evidence = {
        "base_sha": baseline,
        "commits": [],
        "files_changed": [],
        "worktree_clean": False,
        "remote_verified": False,
        # Carried for the failure-reason builder (read-only probe of the
        # push transport error); popped out of the bus result by the caller.
        "workdir": workdir,
    }
    try:
        head = git("rev-parse", "HEAD")
        evidence["head_sha"] = head
        entries = _porcelain_entries(workdir, env)
        evidence["untracked_files"] = [path for xy, path in entries if xy == "??"]
        evidence["tracked_worktree_clean"] = all(xy == "??" for xy, _ in entries)
        evidence["worktree_clean"] = not entries
        # Divergent/replaced histories cannot be presented as this job's work.
        git("merge-base", "--is-ancestor", baseline, head)
        evidence["commits"] = git("rev-list", f"{baseline}..{head}").splitlines()
        evidence["files_changed"] = git(
            "diff", "--name-only", baseline, head
        ).splitlines()
        upstream = git("rev-parse", "--symbolic-full-name", "@{upstream}")
        branch = git("symbolic-ref", "--short", "HEAD")
        evidence["branch"] = branch
        remote = git("config", f"branch.{branch}.remote")
        merge = git("config", f"branch.{branch}.merge")
        if remote == "." or not upstream.startswith("refs/remotes/"):
            raise ValueError("upstream must be a remote branch")
        actual = git("ls-remote", "--exit-code", remote, merge).split()
        evidence["remote_ref"] = merge
        evidence["remote_sha"] = actual[0] if actual else None
        evidence["remote_verified"] = evidence["remote_sha"] == head
        evidence["pushed"] = evidence["remote_verified"]
    except (subprocess.SubprocessError, OSError, ValueError) as exc:
        # Bounded, redacted, actionable: the command, its status and the LAST
        # stderr lines -- never a bare exception class name, which told an
        # operator nothing about why the push was rejected.
        stderr = (getattr(exc, "stderr", "") or "") + (getattr(exc, "output", "") or "")
        if not isinstance(stderr, str):
            stderr = ""
        detail = _stderr_tail(stderr.splitlines())
        if isinstance(exc, subprocess.CalledProcessError):
            cmd = exc.cmd if isinstance(exc.cmd, list) else str(exc.cmd).split()
            what = f"`{' '.join(cmd[3:])}` exited {exc.returncode}"
        else:
            what = f"{type(exc).__name__}: {exc}"
        evidence["verification_error"] = _cap_text(
            f"{what}: {detail}" if detail else what
        )
    return evidence


def completion_ready(evidence: dict) -> bool:
    return bool(
        evidence.get("commits")
        and evidence.get("files_changed")
        and evidence.get("tracked_worktree_clean") is True
        and evidence.get("remote_verified") is True
        and evidence.get("remote_sha") == evidence.get("head_sha")
    )


# ---------------------------------------------------------------------------
# Delivery finalizer (2026-10-04). On 2026-10-03 a zoder loop edited the right
# files on the task branch (122 changed lines, a new module and tests) but
# never ran `git commit`/`git push`, so every iteration's --check gate saw
# check_passed=false and the job was marked failed; the work survived only
# because it was recovered by hand. Before judging the loop outcome we now
# rescue substantive uncommitted work: stage the EXPLICIT changed paths only
# (never `git add -A`), commit, and push the task branch. Build junk and
# credential-looking files are never committed; a finalizer failure is
# recorded but never masks the loop's original outcome.
_MAX_RESCUE_FILE_BYTES = 1_000_000
# A repo with no .gitignore for target/, node_modules/ or a venv can present
# thousands of "changed" paths. Never sweep more than this many surviving paths
# (overridable via HIVE_FINALIZER_MAX_FILES), and never even parse a porcelain
# listing bigger than _MAX_PORCELAIN_ENTRIES entries.
_MAX_RESCUE_FILES = 200
_MAX_PORCELAIN_ENTRIES = 5000
_IGNORED_PATH_SUFFIXES = (".pyc", ".log")
_SECRET_PATH_RES = (
    re.compile(r"(^|/)\.env($|\.)", re.IGNORECASE),
    re.compile(r"\.(key|pem)$", re.IGNORECASE),
    re.compile(r"(^|/)id_[^/]*$"),
)
_SECRET_CONTENT_RE = re.compile(r"(?i)(api[_-]?key|secret|token)\s*[=:]\s*\S{16,}")


def _finalizer_ignored(path: str) -> bool:
    if path.endswith(_IGNORED_PATH_SUFFIXES):
        return True
    if "__pycache__" in path.split("/"):
        return True
    return os.path.basename(path).startswith(".opencode")


# Untracked scratch the finalizer must never rescue (2026-10-26). A stopped job's
# finalizer pushed a commit holding only .tmp/tmp.*/{run/wayland-0.lock,socket,
# socket-wait,schemas/*}; other jobs left build/tmp/.tmp*/ledger.jsonl,
# build/tmp/*.lock, short/.tmp*/ledger.jsonl and zoder switch-transfer json under
# build/tmp. These rules apply to UNTRACKED paths only: a tracked file that changed
# is the job's work wherever it lives. The repo's own .gitignore already hides
# ignored paths from `git status`, and the finalizer never uses `git add -f`/`-A`.
_SCRATCH_DIR_NAMES = frozenset({"tmp", ".tmp"})
_SCRATCH_DIR_PREFIXES = (".tmp",)
_SCRATCH_BASENAME_RES = (
    re.compile(r"\.lock$"),
    # socket, socket-wait, socket_1, socket.0 -- not socket.c or sockets.md
    re.compile(r"^socket(?:$|[-_][^.]*$|\.\d+$)"),
    re.compile(r"\.sock$"),
)
# Lockfiles that ARE deliverables when a job adds them. Best effort: any other
# untracked *.lock is skipped as scratch, recorded in finalizer.skipped, and left
# in the preserved workspace (operator rule 2026-10-26: *.lock is scratch).
_REAL_LOCKFILES = frozenset(
    {
        "Cargo.lock",
        "poetry.lock",
        "uv.lock",
        "Pipfile.lock",
        "yarn.lock",
        "flake.lock",
        "Gemfile.lock",
        "composer.lock",
        "bun.lock",
        "pdm.lock",
        "mix.lock",
        "pubspec.lock",
        "deno.lock",
        "pixi.lock",
    }
)
# rye / pip-tools style: requirements.lock, requirements-dev.lock, ...
_REAL_LOCKFILE_RE = re.compile(r"^requirements[-_.\w]*\.lock$")


def _finalizer_scratch(path: str) -> bool:
    """Whether a path NEW to the repo is scratch (tmp dirs, locks, sockets)."""
    parts = path.split("/")
    for part in parts[:-1]:
        if part in _SCRATCH_DIR_NAMES or part.startswith(_SCRATCH_DIR_PREFIXES):
            return True
    base = parts[-1]
    if base in _REAL_LOCKFILES or _REAL_LOCKFILE_RE.match(base):
        return False
    return any(rx.search(base) for rx in _SCRATCH_BASENAME_RES)


# A loop that exits on one of these signals (directly, or as 128+N after its own
# handler ran) was STOPPED by an operator or the host, not failed on its merits.
_STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT, signal.SIGKILL, signal.SIGHUP, signal.SIGQUIT)


def stopped_by_signal(exit_code: int | None, timed_out: bool = False) -> str | None:
    """Name of the stop signal that ended the loop, or None.

    A worker timeout kills with SIGKILL but reports exit_code -1 and timed_out; it
    is the job running out of time, not a stop, so it is never reported here."""
    if timed_out or not isinstance(exit_code, int) or isinstance(exit_code, bool):
        return None
    for sig in _STOP_SIGNALS:
        if exit_code in (-int(sig), 128 + int(sig)):
            return sig.name
    return None


def _finalizer_secret_path(path: str) -> bool:
    return any(rx.search(path) for rx in _SECRET_PATH_RES)


def _finalizer_secret_content(full: str) -> bool:
    try:
        if os.path.getsize(full) > _MAX_RESCUE_FILE_BYTES:
            return False
        with open(full, "r", encoding="utf-8", errors="ignore") as fh:
            return bool(_SECRET_CONTENT_RE.search(fh.read(_MAX_RESCUE_FILE_BYTES)))
    except OSError:
        return False


class _TooManyFilesError(Exception):
    """`git status --porcelain` yielded more entries than we are willing to
    parse. The caller reports `too_many_files` instead of sweeping a huge repo.
    """

    def __init__(self, count: int) -> None:
        super().__init__(f"porcelain entries exceed the bound at {count}")
        self.count = count


def _finalizer_max_files(env: dict) -> int:
    """Number of surviving paths the finalizer may commit, from
    HIVE_FINALIZER_MAX_FILES when it holds a non-negative integer."""
    raw = env.get("HIVE_FINALIZER_MAX_FILES")
    if raw is None or raw == "":
        return _MAX_RESCUE_FILES
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return _MAX_RESCUE_FILES
    return value if value >= 0 else _MAX_RESCUE_FILES


def _porcelain_changed_paths(
    workdir: str, env: dict, max_entries: int | None = None
) -> list[str]:
    return [path for _xy, path in _porcelain_entries(workdir, env, max_entries)]


def _porcelain_entries(
    workdir: str, env: dict, max_entries: int | None = None
) -> list[tuple[str, str]]:
    """(XY status, path) per entry of `git status --porcelain -z` (NUL-delimited, so paths
    with spaces/quotes need no unquoting). Rename/copy entries carry the new
    path first and the original path in the following NUL token.

    When max_entries is set, stop and raise _TooManyFilesError as soon as more
    entries than that are seen, so a runaway worktree is never fully parsed.
    """
    raw = subprocess.check_output(
        ["git", "-C", workdir, "status", "--porcelain", "-z", "--untracked-files=all"],
        env=env,
        stderr=subprocess.DEVNULL,
    )
    if isinstance(raw, str):  # tolerate a text-mode/mocked check_output
        raw = raw.encode("utf-8", "surrogateescape")
    paths: list[tuple[str, str]] = []
    tokens = raw.split(b"\x00")
    i = 0
    entries = 0
    while i < len(tokens):
        entry = tokens[i]
        i += 1
        if not entry:
            continue
        entries += 1
        if max_entries is not None and entries > max_entries:
            raise _TooManyFilesError(entries)
        text = entry.decode("utf-8", "surrogateescape")
        path = text[3:] if len(text) > 3 else ""
        if text[:1] in ("R", "C"):
            i += 1  # the original path follows in its own NUL token
        if path:
            paths.append((text[:2], path))
    return paths


def _current_branch(workdir: str, env: dict) -> str | None:
    try:
        out = subprocess.check_output(
            ["git", "-C", workdir, "symbolic-ref", "--short", "HEAD"],
            env=env,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        return out or None
    except (subprocess.SubprocessError, OSError):
        return None


def _default_branch(workdir: str, env: dict) -> str | None:
    try:
        out = subprocess.check_output(
            [
                "git",
                "-C",
                workdir,
                "symbolic-ref",
                "--short",
                "refs/remotes/origin/HEAD",
            ],
            env=env,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (subprocess.SubprocessError, OSError):
        return None
    if not out:
        return None
    return out.split("/", 1)[1] if out.startswith("origin/") else out


def _on_task_branch(workdir: str, env: dict) -> tuple[bool, str | None]:
    branch = _current_branch(workdir, env)
    if not branch:
        return False, None
    default = _default_branch(workdir, env)
    if default is None:
        # Without a resolvable origin/HEAD, never treat a conventional default
        # branch as a task branch.
        return branch not in ("main", "master"), branch
    return branch != default, branch


def finalize_uncommitted_work(
    workdir: str,
    job: dict,
    env: dict,
    cancelled: bool = False,
    baseline: str | None = None,
    stopped: str | None = None,
) -> dict:
    """Commit and push substantive uncommitted work before the delivery gate.

    ``stopped`` names the signal that stopped the loop (see stopped_by_signal).
    A stopped job's real changes are committed LOCALLY in the preserved
    workspace and never pushed; the job result carries the record. Untracked
    scratch is never committed, and nothing at all is committed when only
    scratch/ignorable paths changed. The commit subject is neutral: the job id
    stays in the job result, never in a commit on a shared branch.

    Returns a bounded record for the job result. Never raises: a finalizer
    failure must not mask the loop's original outcome.
    """
    report: dict = {
        "status": "nothing_to_commit",
        "committed": False,
        "pushed": False,
        "skipped": [],
    }
    report["job_id"] = str(job.get("id") or "") or None
    if stopped:
        # Recorded on every path, including the ones that commit nothing.
        report["stopped_by"] = stopped
    if cancelled:
        report["status"] = "cancelled"
        return report
    try:
        if baseline:
            report["commits"] = _commits_on_range(workdir, baseline, env)
        on_branch, branch = _on_task_branch(workdir, env)
        if not on_branch or not branch:
            report["status"] = "not_task_branch"
            report["branch"] = branch
            return report
        report["branch"] = branch
        codex_delivery.exclude_build_output(workdir, env)
        try:
            changed_entries = _porcelain_entries(workdir, env, _MAX_PORCELAIN_ENTRIES)
        except _TooManyFilesError as exc:
            # Too many entries to trust the listing: never stage or push.
            report["status"] = "too_many_files"
            report["count"] = exc.count
            report["paths"] = []
            return report
        to_commit: list[str] = []
        for xy, path in changed_entries:
            if _finalizer_ignored(path):
                report["skipped"].append({"path": path, "reason": "ignorable"})
                continue
            # New to the repo = untracked ("??") or added to the index but not
            # in HEAD ("A?"): a loop that ran `git add` on scratch does not make
            # it work. Files already in HEAD, including renames/copies of them
            # ("R?"/"C?"), are always the job's work.
            if (xy == "??" or xy[:1] == "A") and _finalizer_scratch(path):
                report["skipped"].append({"path": path, "reason": "scratch"})
                continue
            if _finalizer_secret_path(path):
                report["skipped"].append({"path": path, "reason": "secret_path"})
                continue
            full = os.path.join(workdir, path)
            try:
                too_large = os.path.isfile(full) and (
                    os.path.getsize(full) > _MAX_RESCUE_FILE_BYTES
                )
            except OSError:
                too_large = False
            if too_large:
                report["skipped"].append({"path": path, "reason": "too_large"})
                continue
            if os.path.isfile(full) and _finalizer_secret_content(full):
                report["skipped"].append({"path": path, "reason": "secret_content"})
                continue
            to_commit.append(path)
        if not to_commit:
            report["status"] = (
                "skipped_only" if report["skipped"] else "nothing_to_commit"
            )
            return report
        max_files = _finalizer_max_files(env)
        if len(to_commit) > max_files:
            # A repo without a .gitignore can present thousands of paths; commit
            # and push NOTHING rather than sweep junk into the task branch.
            report["status"] = "too_many_files"
            report["count"] = len(to_commit)
            report["paths"] = to_commit[:20]
            return report
        subprocess.check_output(
            ["git", "-C", workdir, "add", "--", *to_commit],
            env=env,
            stderr=subprocess.STDOUT,
        )
        # Neutral subject (2026-10-26): "WIP rescue: <brief> (hive job <id>)"
        # put job ids and brief text on shared branches. The id is recorded in
        # the job result (report["job_id"]) instead.
        message = "WIP: preserve uncommitted work from an unfinished run"
        # --only commits exactly the filtered paths: a path the loop had already
        # staged (a secret, an oversized blob) stays in the index uncommitted
        # instead of riding along on a whole-index commit.
        subprocess.check_output(
            ["git", "-C", workdir, "commit", "-m", message, "--only", "--", *to_commit],
            env=env,
            stderr=subprocess.STDOUT,
        )
        report["committed"] = True
        report["commit"] = subprocess.check_output(
            ["git", "-C", workdir, "rev-parse", "HEAD"],
            env=env,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        report["commits"] = list(
            dict.fromkeys((report.get("commits") or []) + [report["commit"]])
        )
        try:
            committed_paths = subprocess.check_output(
                ["git", "-C", workdir, "diff", "--name-only", "HEAD~1", "HEAD"],
                env=env,
                text=True,
                stderr=subprocess.DEVNULL,
            ).splitlines()
        except (subprocess.SubprocessError, OSError):
            committed_paths = []
        unexpected = sorted({p for p in committed_paths if p} - set(to_commit))
        if unexpected:
            # Something outside the filtered set reached the commit: keep the
            # commit local, record it, and never push.
            report["status"] = "finalizer_error"
            report["finalizer_error"] = "committed_unexpected_paths"
            report["unexpected_paths"] = unexpected[:20]
            report["error"] = "committed_unexpected_paths: " + ", ".join(
                unexpected[:20]
            )
            return report
        if stopped:
            # A stopped job never pushes (2026-10-26): its finalizer once pushed
            # scratch to the shared head branch while another attempt was racing
            # for it. The commit stays in the preserved workspace; the operator
            # (or the workspace-reuse rescue) decides what happens to it.
            report["status"] = "committed_local_stopped"
            report["pushed"] = False
            return report
        # HARD GUARD, immediately before the push (2026-10-25): refuse unless
        # `git push origin` here reaches the ARGONAS clone as jasonperlow --
        # checked on the configured url AND on what git would actually dial.
        # The commit stays local; the caller fails the job.
        refusal = guard_workspace_push(workdir, env, "origin")
        if refusal:
            report["status"] = "push_refused"
            report["pushed"] = False
            report["error"] = _cap_text(refusal)
            return report
        try:
            subprocess.check_output(
                ["git", "-C", workdir, "push", "--set-upstream", "origin", branch],
                env=env,
                stderr=subprocess.STDOUT,
            )
            report["pushed"] = True
            report["status"] = "committed_pushed"
        except (subprocess.CalledProcessError, OSError) as exc:
            # Keep git's own lines: "Permission denied (publickey)" is the
            # reason an operator needs, and the exit code alone never said it.
            # `output` carries the merged stderr under stderr=STDOUT.
            output = getattr(exc, "output", "") or getattr(exc, "stderr", "") or ""
            if not isinstance(output, str):
                output = str(output)
            detail = _stderr_tail(output.splitlines())
            report["status"] = "push_failed"
            report["returncode"] = getattr(exc, "returncode", None)
            report["error"] = _cap_text(
                f"push_failed: {detail}" if detail else "push_failed"
            )
        return report
    except Exception as exc:  # noqa: BLE001 -- must never mask the loop outcome
        report["status"] = "finalizer_error"
        report["error"] = f"{type(exc).__name__}: {exc}"[:300]
        return report


def enforce_attribution(workdir: str, baseline: str, env: dict) -> dict:
    """Strip AI/Co-Authored-By trailers from this job's commits, then verify.

    Runs after the finalizer (so rescued commits are covered too) and before
    the evidence is gathered, so the delivery gate -- and the push the gate
    verifies -- never includes an invented attribution. Returns a bounded
    report; `status == "violation"` means a trailer survived the rewrite and
    the caller must fail the delivery.
    """
    name = env.get("HIVE_GIT_AUTHOR_NAME") or "Jason Perlow"
    email = env.get("HIVE_GIT_AUTHOR_EMAIL") or "jperlow@gmail.com"
    report: dict = {
        "status": "clean",
        "configured_identity": f"{name} <{email}>",
        "stripped": [],
        "violations": [],
    }
    try:
        commits = _commits_on_range(workdir, baseline, env)
    except (subprocess.SubprocessError, OSError, RuntimeError) as exc:
        report["status"] = "check_failed"
        report["error"] = str(exc)[:500]
        return report
    if not commits:
        report["status"] = "nothing_delivered"
        return report
    # Only run filter-branch when there is something to scrub. The
    # commit-msg hook in PRIMARY-prevention already drops most trailers at
    # commit time, and a chain the gate considers clean is left alone (so
    # test fixtures that pre-set commit_attribution_violations to return
    # empty do not trip on the subprocess call the strip would otherwise
    # need).
    try:
        pending = commit_attribution_violations(workdir, env, commits, name, email)
    except (subprocess.SubprocessError, OSError, RuntimeError) as exc:
        report["status"] = "check_failed"
        report["error"] = str(exc)[:500]
        report["checked_commits"] = commits
        return report
    if pending:
        try:
            rewrite = strip_ai_attribution(workdir, env, commits, baseline, name, email)
        except (subprocess.SubprocessError, OSError, RuntimeError) as exc:
            report["status"] = "check_failed"
            report["error"] = str(exc)[:500]
            report["checked_commits"] = commits
            return report
        report["stripped"] = report.get("stripped", []) + rewrite.get("stripped", [])
        for key in ("failed", "unfixable", "error"):
            if rewrite.get(key):
                report[key] = rewrite[key]
        if push_has_attribution(rewrite):
            report["status"] = "violation"
            report["reason"] = (
                "ai_attribution_trailer: could not strip "
                + ", ".join(
                    (rewrite.get("failed") or []) + (rewrite.get("unfixable") or [])
                )[:10]
                + "; only the configured git identity may author a hive commit"
            )
            return report
        try:
            commits = _commits_on_range(workdir, baseline, env)
        except (subprocess.SubprocessError, OSError, RuntimeError) as exc:
            report["status"] = "check_failed"
            report["error"] = str(exc)[:500]
            return report
    # Rewriting a parent re-hashes every descendant, so SHAs captured before
    # the first rewrite no longer name a commit: re-discover the range once
    # and re-check exactly that.
    try:
        remaining = commit_attribution_violations(workdir, env, commits, name, email)
    except (subprocess.SubprocessError, OSError, RuntimeError) as exc:
        report["status"] = "check_failed"
        report["error"] = str(exc)[:500]
        report["checked_commits"] = commits
        return report
    report["checked_commits"] = commits
    report["violations"] = remaining
    if remaining:
        report["status"] = "violation"
        report["reason"] = (
            "ai_attribution_trailer: "
            + ", ".join(v["commit"] for v in remaining[:10])
            + " still attributes authorship to a model; only the configured git "
            "identity may author a hive commit"
        )
    elif report.get("stripped"):
        report["status"] = "stripped"
        report["reason"] = "stripped AI-attribution trailer(s) from " + ", ".join(
            report["stripped"][:10]
        )
    else:
        report["status"] = "clean"
    return report


def enforce_attribution_for_shas(workdir: str, commits: list[str], env: dict) -> dict:
    """Check the exact commit SHAs recorded by the delivery finalizer."""
    name = env.get("HIVE_GIT_AUTHOR_NAME") or "Jason Perlow"
    email = env.get("HIVE_GIT_AUTHOR_EMAIL") or "jperlow@gmail.com"
    report = {
        "status": "clean",
        "configured_identity": f"{name} <{email}>",
        "stripped": [],
        "violations": [],
    }
    if not commits:
        report["status"] = "nothing_delivered"
        report["checked_commits"] = []
        return report
    try:
        violations = commit_attribution_violations(workdir, env, commits, name, email)
    except (subprocess.SubprocessError, OSError, RuntimeError) as exc:
        return {
            **report,
            "status": "check_failed",
            "checked_commits": commits,
            "error": str(exc)[:500],
        }
    report["checked_commits"] = list(commits)
    report["violations"] = violations
    if violations:
        report["status"] = "violation"
        report["reason"] = "ai_attribution_trailer: " + ", ".join(
            v["commit"] for v in violations[:10]
        )
    return report


def delivery_integrity_errors(
    attribution: dict,
    finalizer: dict,
    evidence: dict,
    codex_delivery: dict | None = None,
) -> list[str]:
    """Cross-check independently recorded delivery facts before success."""
    errors: list[str] = []
    commits = evidence.get("commits") or []
    recorded = finalizer.get("commits") or []
    push = (codex_delivery or {}).get("push") or {}
    attribution_status = attribution.get("status")
    if attribution_status == "check_failed":
        errors.append(
            "attribution check failed: "
            + str(attribution.get("error") or "unknown git error")
        )
    if attribution_status == "nothing_delivered" and (
        commits or recorded or (codex_delivery or {}).get("head")
    ):
        errors.append(
            f"integrity contradiction: attribution=nothing_delivered; evidence commits={commits!r}, finalizer commits={recorded!r}"
        )
    if (
        finalizer.get("pushed") is False
        and push.get("status") == "pushed"
        and evidence.get("remote_verified") is True
    ):
        errors.append(
            "integrity contradiction: finalizer.pushed=false; codex push.status=pushed; remote_verified=true"
        )
    if commits and not recorded and not (codex_delivery or {}).get("head"):
        errors.append(
            f"integrity contradiction: evidence commits={commits!r}; finalizer recorded no commit SHAs"
        )
    branch = evidence.get("branch")
    remote_ref = evidence.get("remote_ref")
    # A job that delivered NOTHING has no delivered branch: `branch` is just the
    # branch its workspace was checked out on, and for a read-only review that is
    # the default branch. Requiring evidence of an actual delivery keeps the
    # "landed on the default branch" guard for commits and pushes without
    # failing every read-only job (2026-10-27: three review: children died with
    # delivery_integrity_check_failed while the same check passed for jobs that
    # genuinely committed).
    delivered_something = bool(
        commits
        or recorded
        or (codex_delivery or {}).get("head")
        or push.get("status") == "pushed"
        or evidence.get("remote_verified") is True
    )
    if delivered_something and branch and remote_ref and remote_ref.endswith("/" + branch):
        default = (
            _default_branch(evidence.get("workdir", ""), os.environ)
            if evidence.get("workdir")
            else None
        )
        if branch == default or (default is None and branch in ("main", "master")):
            errors.append(
                f"delivery landed on default branch {branch!r} ({remote_ref})"
            )
    shas = recorded or ((codex_delivery or {}).get("head"),)
    for sha in shas:
        if not sha:
            continue
        try:
            subject = subprocess.check_output(
                [
                    "git",
                    "-C",
                    evidence.get("workdir", ""),
                    "show",
                    "-s",
                    "--format=%s",
                    "--end-of-options",
                    sha,
                ],
                env=os.environ,
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
        except (subprocess.SubprocessError, OSError) as exc:
            errors.append(
                f"subject verification failed for {sha}: {type(exc).__name__}: {exc}"
            )
            continue
        kind = str(finalizer.get("kind") or "").strip().lower()
        prefix = kind + ":" if kind else ""
        if len(subject) > 120 or (prefix and subject.lower().startswith(prefix)):
            errors.append(f"invalid commit subject for {sha}: {subject[:140]!r}")
    return errors


def push_has_attribution(report: dict) -> bool:
    """True when a trailer could not be removed from a commit BEFORE push.

    `failed`/`unfixable` both mean the trailer is still in the history that
    would be pushed -- the delivery must fail rather than ship it.
    """
    return bool(report.get("failed") or report.get("unfixable"))


def _commits_on_range(workdir: str, baseline: str, env: dict) -> list[str]:
    """Commits this job added on top of `baseline` (oldest first)."""
    try:
        out = subprocess.check_output(
            [
                "git",
                "-C",
                workdir,
                "rev-list",
                "--no-merges",
                "--reverse",
                f"{baseline}..HEAD",
            ],
            env=env,
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except (subprocess.SubprocessError, OSError) as exc:
        raise RuntimeError(
            f"commit range check failed: {type(exc).__name__}: {exc}"
        ) from exc
    return [line.strip() for line in out.splitlines() if line.strip()]


def launched_reviewer_route(codex_delivery_result: dict | None = None) -> str | None:
    """The reviewer route this worker launched the review with (2026-10-26).

    The fallback of the one reviewer record when no structured verdict exists:
    the Codex delivery's ``review.reviewer``, else the route recorded at launch
    (the role id, e.g. cerberus-reviewer, then the model backing it)."""
    if codex_delivery_result is not None:
        return (codex_delivery_result.get("review") or {}).get("reviewer") or None
    return _current_provenance.get("reviewer") or _current_provenance.get("reviewer_model") or None


def zeroclaw_review_gate(
    status: str,
    failure_reason: str | None,
    result: dict,
    *,
    has_commits: bool,
    run_models: dict | None,
    codex: bool = False,
) -> tuple[str, str | None]:
    """Fail closed for a zoder-loop delivery (operator 2026-10-26).

    A ``done`` job that committed work needs a named reviewer model on record,
    and none of the approving reviewers may share the author's family; otherwise
    it is ``needs-review`` with the reason in ``result["error"]``. A job that
    changed nothing needs no reviewer, and the Codex kind has its own gate in
    the delivery step.
    """
    if status != "done" or codex or not has_commits:
        return status, failure_reason
    problem = review_evidence.zeroclaw_problem(
        result.get("review_evidence"), run_models if isinstance(run_models, dict) else None
    )
    if not problem:
        return status, failure_reason
    if isinstance(result.get("review_evidence"), dict):
        result["review_evidence"].setdefault("warnings", []).append(problem)
    if os.environ.get("HIVE_ZEROCLAW_REVIEW_GATE", "enforce").strip().lower() == "warn":
        # Escape hatch (a systemd drop-in and a restart): record the warning on
        # the row but leave the disposition alone, if the gate ever misfires.
        result.setdefault("review_gate_warning", problem[:500])
        return status, failure_reason
    result["error"] = problem[:3000]
    return "needs-review", "review_unverified"


def resolve_delivery_status(
    exit_code: int, evidence: dict, finalizer: dict, cancelled: bool = False
) -> tuple[str, str | None]:
    """Map the loop outcome + post-finalizer evidence to a terminal status.

    The loop's outcome is never masked: a rescued branch upgrades an
    unresolved loop to `needs-review` only when the delivery is actually
    verified (commits, clean worktree, remote SHA match).
    """
    ready = completion_ready(evidence)
    if exit_code == 0:
        if ready:
            return "done", None
        return "needs-review", "completion_evidence_missing"
    if not cancelled and ready:
        return "needs-review", "loop_unresolved_work_rescued"
    return "failed", "loop_unresolved"


# ---------------------------------------------------------------------------
# Failure reasons (2026-10-17).
#
# Every `failed` row used to carry error='': job 01a148a8-c815 on ULTRA logged
# `failed exit=0 elapsed=2806.9s` with no reason, and an earlier codex attempt
# of the same job that could not push ("Permission denied (publickey)" to
# ARGONAS) also left nothing behind. Operators cannot tell a reviewer that
# never approved from a rejected push, a dead endpoint or an attribution
# violation without opening the host's logs -- which they may not have. Every
# failed path now records WHY, derived from evidence the worker already holds,
# and passes it through secret redaction before it reaches the bus.
FAILURE_ERROR_MAX = 500
FAILURE_TAIL_LINES = 5

_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)\b([A-Za-z0-9_]*(?:password|passwd|secret|token|api[_-]?key|"
    r"private[_-]?key|credential)[A-Za-z0-9_]*)(\s*[=:]\s*)(\"[^\"]*\"|'[^']*'|\S+)"
)
# Bare credential shapes that need no `KEY=value` context.
_SECRET_SHAPES = (
    # scheme://user:password@host -- SSH remotes embed the fleet password here.
    re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://[^\s/:@]+:)[^\s@/]+@"),
    # Well-known token prefixes.
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{16,}"),
    re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{16,}"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    # PEM private-key material (whole block, including the base64 body).
    re.compile(
        r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----",
        re.DOTALL,
    ),
    re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----"),
)


def redact_secrets(text: str) -> str:
    """Scrub credential material before failure evidence reaches the bus.

    Failure text is raw stderr from git/ssh/the driver, so it can carry the
    fleet SSH password (GIT_SSH_COMMAND/sshpass), an https remote with userinfo,
    or a provider token echoed by a tool. Redaction is deliberately blunt: a
    false positive only costs readability, a leak is permanent in the bus row.
    """
    if not text:
        return ""
    clean = _SECRET_ASSIGNMENT_RE.sub(r"\1\2[REDACTED]", text)
    clean = _SECRET_SHAPES[0].sub(r"\1[REDACTED]@", clean)
    for rx in _SECRET_SHAPES[1:]:
        clean = rx.sub("[REDACTED]", clean)
    return clean


_LOOP_RESULT_PREFIX = "ZODER_LOOP_RESULT "
_LOOP_RESULT_MAX = 16384


def parse_zoder_loop_result(out: str | None) -> dict | None:
    """The LAST ``ZODER_LOOP_RESULT {json}`` line of a zoder loop's output.

    The loop prints one parseable result line so the bus row can carry the real
    teardown reason and per-iteration verdicts; the 4000-char tail is dominated
    by the echoed brief. None when absent or malformed (never raises).
    """
    found = None
    for line in (out or "").splitlines():
        line = line.strip()
        if not line.startswith(_LOOP_RESULT_PREFIX) or len(line) > _LOOP_RESULT_MAX:
            continue
        try:
            candidate = json.loads(line[len(_LOOP_RESULT_PREFIX):])
        except (ValueError, RecursionError):
            continue
        if isinstance(candidate, dict) and isinstance(candidate.get("teardown"), dict):
            found = candidate
    return found


def _cap_text(text: str, limit: int = FAILURE_ERROR_MAX) -> str:
    """Redact, flatten to one bus-row-safe line, then bound the length."""
    flat = " | ".join(
        line.strip() for line in redact_secrets(text or "").splitlines() if line.strip()
    )
    if len(flat) <= limit:
        return flat
    return flat[: limit - 3].rstrip() + "..."


def _stderr_tail(lines: Sequence[str], count: int = FAILURE_TAIL_LINES) -> str:
    """The last `count` non-empty stderr lines, joined for a single error row."""
    kept = [line.strip() for line in lines if line and line.strip()]
    return " | ".join(kept[-count:])


def parse_loop_record(stdout: str) -> dict | None:
    """The LAST single-line JSON object on the loop's stdout (its verdict).

    `zoder loop --json` prints one summary object; log lines may precede it and
    a later, more specific record wins -- the same rule parse_loop_models uses.
    Only loop-shaped objects qualify, so an unrelated JSON log line is never
    mistaken for the verdict.
    """
    if not stdout:
        return None
    record = None
    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith("{") or len(line) > _MAX_JSON_LINE:
            continue
        try:
            candidate = json.loads(line)
        except (ValueError, RecursionError):
            continue
        if isinstance(candidate, dict) and any(
            key in candidate
            for key in (
                "final_verdict",
                "resolved",
                "log",
                "iterations",
                "author_error",
            )
        ):
            record = candidate
    return record


def _loop_iterations(record: dict) -> list:
    iterations = record.get("log") or record.get("iterations") or []
    return iterations if isinstance(iterations, list) else []


def _unresolved_findings(record: dict) -> list[dict]:
    """Blocking findings from the loop's LAST iteration ([] if none reported)."""
    iterations = _loop_iterations(record)
    if not iterations or not isinstance(iterations[-1], dict):
        return []
    last = iterations[-1]
    findings = last.get("findings") or last.get("unresolved_findings") or []
    return (
        [f for f in findings if isinstance(f, dict)]
        if isinstance(findings, list)
        else []
    )


def loop_failure_reason(record: dict | None) -> str:
    """Why the zoder loop counts as failed, from its own JSON -- or "".

    Priority follows the operator's diagnosis order: the final reviewer
    verdict, then an author turn that never produced a result, then an author
    that produced nothing, then the blocking findings left unresolved.
    """
    if not isinstance(record, dict):
        return ""
    parts: list[str] = []
    verdict = str(record.get("final_verdict") or "").strip()
    if verdict:
        parts.append(f"final_verdict={verdict}")
    iterations = _loop_iterations(record)
    last = iterations[-1] if iterations and isinstance(iterations[-1], dict) else {}
    author_error = str(
        record.get("author_error") or last.get("author_error") or ""
    ).strip()
    if author_error:
        parts.append(f"author_error={author_error}")
    elif (
        last
        and str(last.get("substance") or "").lower() == "empty"
        and not last.get("diff_lines")
    ):
        parts.append("author produced no edits")
    findings = _unresolved_findings(record)
    if findings:
        blocking = [
            f
            for f in findings
            if str(f.get("severity", "")).lower() in ("critical", "high")
        ]
        parts.append(f"{len(blocking) or len(findings)} unresolved review finding(s)")
        titles = [
            f"[{f.get('severity', '?')}] {f.get('title') or f.get('body') or 'finding'}"
            for f in (blocking or findings)[:3]
        ]
        parts.append("; ".join(str(t)[:120] for t in titles))
    if record.get("resolved") is False and not parts:
        parts.append("loop resolved=false")
    return ", ".join(parts)


def _git_output(args: list[str], cwd: str | None, env: dict) -> tuple[int, list[str]]:
    """Run one git command; return (rc, non-empty output lines)."""
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return 1, [f"{type(exc).__name__}: {exc}"]
    combined = (proc.stdout or "") + (proc.stderr or "")
    return proc.returncode, [line for line in combined.splitlines() if line.strip()]


def _remote_of(workdir: str, env: dict) -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", workdir, "remote", "get-url", "origin"],
            env=env,
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=30,
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def _push_stderr(workdir: str, branch: str, env: dict) -> list[str]:
    """git's own lines from a read-only `ls-remote` probe of the branch.

    Read-only: it repairs nothing, it only reproduces the transport error the
    delivery was rejected with (e.g. `Permission denied (publickey)`) so the
    bus row names the actual cause instead of `remote_verified=False`. The
    probe runs with GIT_TERMINAL_PROMPT=0 so it can never hang on a password
    prompt -- the worker is not attached to a terminal, and hanging here would
    hold the job open forever.
    """
    probe_env = dict(env)
    probe_env["GIT_TERMINAL_PROMPT"] = "0"
    remote = _remote_of(workdir, env) or "origin"
    _, lines = _git_output(
        ["-C", workdir, "ls-remote", "--heads", remote, branch], workdir, probe_env
    )
    return lines


def delivery_failure_reason(
    exit_code: int,
    evidence: dict,
    finalizer: dict,
    attribution: dict,
    env: dict | None = None,
) -> str:
    """Why the delivery verifier rejected this run, from its own evidence."""
    env = env if env is not None else os.environ.copy()
    workdir = evidence.get("workdir") or ""
    head = evidence.get("head_sha") or ""
    commits = evidence.get("commits") or []
    parts: list[str] = []
    if evidence:
        if not head:
            parts.append("git: repository has no HEAD")
        if not commits:
            parts.append(
                f"no commits on top of base {str(evidence.get('base_sha') or '?')[:12]}"
            )
    finalizer_error = str(finalizer.get("error") or "").strip()
    status = str(finalizer.get("status") or "")
    if status == "push_failed" and finalizer_error:
        # The finalizer already holds git's push stderr; quoting it is the
        # whole point (the codex `Permission denied (publickey)` attempt).
        parts.append(f"push rejected: {finalizer_error}")
    elif status and status != "nothing_to_commit":
        parts.append(
            f"finalizer={status}" + (f": {finalizer_error}" if finalizer_error else "")
        )
    if workdir and evidence.get("tracked_worktree_clean") is False:
        parts.append("worktree still has uncommitted tracked changes")
    if commits and evidence.get("remote_verified") is not True:
        if evidence.get("verification_error"):
            parts.append(f"remote check failed: {evidence['verification_error']}")
        elif workdir:
            branch = str(evidence.get("branch") or "").strip()
            remote = str(evidence.get("remote_ref") or branch or "HEAD").split("/")[-1]
            # A successful `--heads` listing puts ref<TAB>sha on stdout; only
            # stderr-shaped lines are the failure story.
            lines = _push_stderr(workdir, remote, env)
            tail = _stderr_tail([line for line in lines if "\t" not in line])
            if tail:
                parts.append(f"push rejected: {tail}")
            else:
                parts.append("push rejected: remote branch not found or not updated")
    if attribution.get("status") in ("violation", "check_failed"):
        parts.append(
            f"attribution: {attribution.get('reason') or 'ai_attribution_trailer'}"
        )
    elif attribution.get("status") in ("failed", "unfixable") or push_has_attribution(
        attribution
    ):
        stuck = (attribution.get("failed") or attribution.get("unfixable") or [])[:3]
        where = f" from {', '.join(str(c) for c in stuck)}" if stuck else ""
        parts.append(
            f"attribution: AI-attribution trailer could not be removed{where} before push"
        )
    if exit_code < 0:
        # Loop and reviewer exits are always 0/1; a signal IS the cause, so it
        # leads before the delivery evidence it interrupted.
        parts.insert(0, exit_failure_reason(exit_code, ""))
    elif exit_code == 0:
        parts.insert(
            0, "loop exited 0 but the run counts as a failure: delivery not verified"
        )
    return ", ".join(p for p in parts if p)


def exit_failure_reason(exit_code: int, tail: str, timed_out: bool = False) -> str:
    """The child's exit code (or signal) plus the last stderr lines we hold."""
    if timed_out:
        return (
            "TIMEOUT: killed after the job timeout; last output: "
            f"{_stderr_tail((tail or '').splitlines())}"
        )
    if exit_code < 0:
        try:
            name = signal.Signals(-exit_code).name
        except ValueError:
            name = "unknown"
        reason = f"killed by signal {name} ({exit_code})"
    else:
        reason = f"exit code {exit_code}"
    lines = _stderr_tail((tail or "").splitlines())
    if lines:
        return f"{reason}: {lines}"
    # The caller merges the loop's stdout into this stream; if nothing was
    # captured, say so rather than leaving the row empty.
    return f"{reason} (no output captured)"


# ---------------------------------------------------------------------------
# Engine spend cap (2026-10-25): "Budget exceeded".
#
# With no [cost] block in a fleet zoder config the engine applies its own
# default daily cap, and once the day's spend passes it EVERY provider call
# fails immediately (~36 ms) with:
#
#   Budget exceeded: $10.0039 of $10.00 Day limit. Cannot make further API
#   calls until the budget resets.
#
# That is a fact about the host's spend budget, not about the job's work.
# Left unclassified it surfaced only through the symptoms -- zero commits, an
# empty author turn (tool_calls 0, substance Empty), verdict
# reviewer_unavailable, a "cancel not confirmed" exit -- so job 01a17539-d92f
# burned a retry on a host that could not make a single paid call. The worker
# now names it as its own failure reason, records the parsed spend, and hands
# the attempt back WITHOUT spending a retry.
#
# The matcher itself lives in hive_budget (stdlib only) so the doctor can
# classify the same text without importing this module -- see that file.
BUDGET_EXCEEDED_FAILURE = hive_budget.FAILURE_REASON


def budget_exceeded_from(output: str | None) -> dict | None:
    """The engine's spend-cap failure in ``output``, else None.

    ONE matcher for every caller (stdout, stderr, the buffered tail, the
    loop's JSON verdict); it lives in hive_budget so the doctor shares it
    rather than keeping a second copy that could drift.
    """
    return hive_budget.from_text(output)


def _budget_message_line(output: str) -> str:
    """The engine's own sentence, re-joined when the output wrapped it."""
    return hive_budget.message_line(output)


def budget_failure_fields(
    output: str | None, host: str | None = None, *, now=None
) -> dict | None:
    """Bus-result fields for a spend-cap failure, or None when not one.

    The shape is what the doctor's job-row rule reads (failure_reason=
    budget_exceeded plus the parsed numbers and the host that hit the cap), so
    triage no longer has to infer the cap from reviewer_unavailable + an empty
    author turn.
    """
    detected = budget_exceeded_from(output)
    if detected is None:
        return None
    reset_at = budget_reset_at(detected.get("period"), now=now)
    where = host or AGENT_HOST
    return {
        "failure_reason": BUDGET_EXCEEDED_FAILURE,
        "budget_exceeded": {
            "host": where,
            "spent_usd": detected.get("spent_usd"),
            "limit_usd": detected.get("limit_usd"),
            "period": detected.get("period"),
            "reset_at": reset_at,
            "resets_at_iso": hive_budget.iso_utc(reset_at),
            "message": detected["message"],
        },
        "error": _cap_text(f"budget_exceeded: zoder {hive_budget.summary(detected)} on {where}"),
    }


def budget_reset_at(period: str | None, *, now=None) -> float | None:
    """UTC epoch the engine's spend window rolls over, or None if unknown."""
    return hive_budget.reset_at(period, now=_epoch(now) if now is not None else None)


def build_failure_error(
    status: str,
    exit_code: int,
    tail: str,
    loop_record: dict | None,
    evidence: dict,
    finalizer: dict,
    attribution: dict,
    env: dict | None = None,
    timed_out: bool = False,
) -> str:
    """Non-empty, redacted, bounded WHY for any failed disposition."""
    if status != "failed":
        return ""
    reasons = [
        reason
        for reason in (
            loop_failure_reason(loop_record),
            delivery_failure_reason(exit_code, evidence, finalizer, attribution, env),
            str(attribution.get("reason") or "")
            if attribution.get("status") == "violation"
            else "",
        )
        if reason
    ]
    # An exit 0 says nothing by itself: every exit-0 row must state WHY the
    # run counts as a failure.
    if exit_code == 0 and reasons and "exit 0" not in reasons[0].lower():
        reasons[0] = f"failed with exit 0: {reasons[0]}"
    # A non-zero exit is part of the story even when a cause was already found:
    # the delivery says WHAT was rejected, the exit says HOW the run died.
    if exit_code != 0:
        tail_reason = exit_failure_reason(exit_code, tail, timed_out=timed_out)
        picked = "; ".join([*reasons, tail_reason]) if reasons else tail_reason
    elif reasons:
        picked = " | ".join(reasons)
    else:
        picked = (
            "failed with exit 0: the driver reported failure without a reason "
            "(inspect the host log)"
        )
    # Redact and cap, but keep the mandatory non-empty text: a truncation here
    # must never hand the bus an empty error again.
    return _cap_text(picked) or "failed without a recorded reason (inspect host logs)"


def verify_delivery(workdir: str, baseline: str) -> int:
    """Return bounded, actionable evidence to zoder's check/fix loop."""
    evidence = completion_evidence(workdir, baseline, os.environ.copy())
    ready = completion_ready(evidence)
    # Lists and paths can grow without bound; neither Git stderr nor remote URLs
    # belong in model-visible output (they can contain credentials).
    report = {
        "delivery_ready": ready,
        "commit_count": len(evidence["commits"]),
        "changed_file_count": len(evidence["files_changed"]),
        "files_changed": [p[:240] for p in evidence["files_changed"][:20]],
        "worktree_clean": evidence["worktree_clean"],
        "tracked_worktree_clean": evidence.get("tracked_worktree_clean", False),
        "untracked_files": evidence.get("untracked_files", [])[:20],
        "remote_verified": evidence["remote_verified"],
        "verification_error": _cap_text(evidence.get("verification_error") or "")
        or None,
        "required": (
            "Delivery verified."
            if ready
            else "Checkout a dedicated branch; commit legitimate task changes with "
            "the configured Git identity; push with upstream tracking; leave tracked "
            "files clean and remote HEAD matching local HEAD. Respect explicit job "
            "restrictions; report blockers instead of inventing edits."
        ),
    }
    print(json.dumps(report, ensure_ascii=True))
    return 0 if ready else 1


def configure_git_identity(workdir: str, env: dict) -> None:
    """Supply the authorized fleet identity even on hosts without global config."""
    for key, value in (
        ("user.name", env.get("HIVE_GIT_AUTHOR_NAME") or "Jason Perlow"),
        ("user.email", env.get("HIVE_GIT_AUTHOR_EMAIL") or "jperlow@gmail.com"),
    ):
        subprocess.run(
            ["git", "-C", workdir, "config", "--local", key, value],
            check=True,
            timeout=30,
            env=env,
            capture_output=True,
        )


def launch_model(agent: str) -> str | None:
    """The model this host's zoder launches for ``agent`` -- or None.

    The registration-time placeholder ('zoder') is never returned: the bus
    records the actual model (e.g. ``deepseek-flash`` for ``--agent deepseek``,
    ``gemma4-31b`` for ``--reviewer gemma4-31b``). Aliases whose side is
    picked per job (`flash`) and models the routing policy forbids stay
    unknown, exactly as :func:`escalation_model` reports them.
    """
    if not agent:
        return None
    record = escalation_model(agent)
    if record:
        return record["model"]
    # `escalation_model` refuses the `deepseek`/`flash` rungs because their
    # side is picked per job -- but when the job DID launch `deepseek`, the
    # model its provider is configured to serve is what ran and must be
    # claimed; the generic resolver answers for it and for local aliases
    # (e.g. `coder`) that are not in the static table.
    model = provenance.primary_model(agent)
    if model and not forbidden_model(model):
        return model
    bare = _bare_model(agent)
    if not bare or forbidden_model(bare):
        return None
    # A bare name is only accepted as a model when this host's zoder actually
    # knows it -- never as a passthrough for an arbitrary string. A model
    # already serving a configured route (`qwen38` is `coder`'s model) is
    # known even though it is not itself an [agents.X] alias: an operator may
    # pin `--agent`/`--reviewer` to the model name.
    known = provenance.default_resolver().models()
    served = {str(record.get("model")) for record in known.values()}
    return bare if bare in served or provenance.agent_configured(bare) else None


def launch_record(agent: str) -> dict | None:
    """(model, provider, cost_tier) for a launched alias, or None if unknown."""
    model = launch_model(agent)
    if not model:
        return None
    record = agent_model(agent) or {}
    provider = record.get("provider")
    if not provider:
        settings = provenance.agent_settings(agent) or {}
        configured = settings.get("model_provider")
        provider = configured if isinstance(configured, str) else "zoder"
    tier = record.get("cost_tier") or _cost_tier_for(model, (record.get("uri") or None))
    return {"model": model, "provider": provider, "cost_tier": tier}


def _set_provenance(author: str, reviewer: str = "") -> None:
    """Record the author agent/model and reviewer this attempt launches.

    Only exact, known routes are recorded: an alias this host's zoder does
    not know leaves the model unknown (and any earlier launch record for the
    previous job is cleared by run_job), so the claim never carries a
    placeholder like the registration-time 'zoder'.
    """
    author_record = launch_record(author) if author else None
    reviewer_record = launch_record(reviewer) if reviewer else None
    # A role id (cerberus-reviewer) names a job, not a model: record the role
    # and the model the fleet role map says backs it.
    reviewer_role = reviewer if reviewer and llm_roles.is_role_id(reviewer) else None
    reviewer_model = reviewer_record["model"] if reviewer_record else None
    if reviewer_role:
        reviewer_model = llm_roles.backing_model(reviewer_role) or reviewer_model
    _current_provenance.update(
        {
            "agent": author,
            "reviewer": reviewer,
            "reviewer_role": reviewer_role,
            "reviewer_model": reviewer_model,
            "model": author_record["model"] if author_record else None,
            "provider": author_record["provider"] if author_record else None,
            "cost_tier": author_record["cost_tier"] if author_record else None,
            "runtime": f"zoder:{author}" if author else "zoder",
            "provenance_source": provenance_source(),
        }
    )


# ---------------------------------------------------------------------------
# Ground-truth model attribution (2026-10-17).
#
# The completion result used to trust the loop's own JSON for what ran:
# `author`/`reviewers` there name the models the loop THOUGHT it used, and a
# loop that reports nothing left `result.models.author=None` while the
# reviewer list named the EIH backup even though `hive_reviews` shows the
# local gemma4-31b did the work. The ledger (~/.zoder/ledger.jsonl, one row
# per provider call: ts_utc / provider / model) is the ground truth. Each
# job records the ledger window [run start, run end] and attributes the
# reviewer phase to every model that served a review call in it; the author
# is the first model the loop recorded, or the first model in the window.
# ---------------------------------------------------------------------------
_ZODER_LEDGER_ENV = "HIVE_ZODER_LEDGER"
_LEDGER_MAX_BYTES = 64 * 1024 * 1024
# Paths that appear in the ledger's `tags.path`/`tags.cwd` for review calls.
_REVIEW_TAG_RE = re.compile(r"review|adversar", re.IGNORECASE)
# Loop records carry errors under these keys; a reviewer row that failed
# never reviewed the code.
_ERROR_KEYS = ("error", "reviewer_error", "author_error", "message", "detail")
_LEDGER_TS_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?"
)


def zoder_ledger_path() -> str:
    """The host's zoder usage ledger (overridable for tests)."""
    configured = os.environ.get(_ZODER_LEDGER_ENV)
    if configured:
        return os.path.expanduser(configured)
    return os.path.expanduser("~/.zoder/ledger.jsonl")


def ledger_stamp(value: str | float | None = None) -> float:
    """Epoch seconds for a ledger row timestamp, or 0 when unparseable.

    Accepts the ledger's RFC3339 ``ts_utc`` (nanosecond precision) as well as
    a bare epoch number. ``calendar.timegm`` is used so the parse is
    independent of the host's TZ database.
    """
    text = str(value)
    m = _LEDGER_TS_RE.match(text)
    if m:
        year, month, day, hour, minute, second, frac = m.groups()
        try:
            value = calendar.timegm(
                (int(year), int(month), int(day), int(hour), int(minute), int(second))
            )
        except (ValueError, OverflowError):
            return 0.0
        return value + (float(f"0.{frac}") if frac else 0.0)
    try:
        value = float(text)
    except (TypeError, ValueError):
        return 0.0
    return value


def _window(row: dict, since: float, until: float) -> bool:
    """True when a ledger row's timestamp is inside [since, until]."""
    stamp = ledger_stamp(row.get("ts_utc"))
    return bool(stamp) and since <= stamp <= until


def ledger_calls(
    start: float,
    end: float,
    path: str | None = None,
    max_bytes: int = _LEDGER_MAX_BYTES,
) -> list[dict]:
    """Ledger rows whose ts_utc lies in [start, end], in file order.

    Only the tail of the file is read (the run's rows were appended last)
    and only bounded, well-formed rows are returned -- the ledger is shared
    by every zoder process on the host, so a concurrent writer may leave a
    partial line. Never raises: an unreadable ledger yields no rows.
    """
    target = path or zoder_ledger_path()
    try:
        size = os.path.getsize(target)
        with open(target, "rb") as fh:
            if size > max_bytes:
                fh.seek(size - max_bytes)
                fh.readline()  # drop the partial line the seek landed in
            data = fh.read()
    except OSError:
        return []
    rows = []
    for raw in data.decode("utf-8", "replace").splitlines():
        raw = raw.strip()
        if not raw.startswith("{"):
            continue
        try:
            row = json.loads(raw)
        except (ValueError, RecursionError):
            continue
        if isinstance(row, dict) and _window(row, start, end):
            rows.append(row)
    return rows


def _ledger_model(row: dict) -> str | None:
    """The model a genuine provider call row names, or None."""
    if not isinstance(row, dict):
        return None
    provider = str(row.get("provider") or "")
    model = str(row.get("model") or "").strip()
    if not model or provider.startswith("__zoder_"):
        return None
    if forbidden_model(model):
        return None
    return model


def _ledger_tags(row: dict) -> dict:
    tags = row.get("tags") if isinstance(row, dict) else None
    return tags if isinstance(tags, dict) else {}


def _ledger_zero(value: object) -> bool:
    """True only for an explicit numeric zero (a missing count is unknown)."""
    return (
        isinstance(value, (int, float)) and not isinstance(value, bool) and value == 0
    )


def _ledger_failed(row: dict) -> bool:
    """True when a row records a call that served nothing.

    Besides rows carrying an error key, zoder writes a failed dispatch (an
    armed billable reservation that never got a final entry: a 404, a refused
    route, a probe the provider rejected) as ``tokens_in == 0``,
    ``tokens_out == 0``, ``cost_unknown: true`` and empty ``tags``, under the
    model it tried. Such a row is never an author or a reviewer (incident
    2026-10-26: 77 such rows from a nested ``zoder health --probe`` credited
    a qwen38 job to the first probed model).
    """
    for key in _ERROR_KEYS:
        value = row.get(key)
        if value:
            return True
    return (
        _ledger_zero(row.get("tokens_in"))
        and _ledger_zero(row.get("tokens_out"))
        and bool(row.get("cost_unknown"))
        and not _ledger_tags(row)
    )


# `zoder loop` (the only thing the zeroclaw worker launches) tags every call
# it makes with caller == "zoder" and task "loop" (authoring) or "review".
# Untagged rows come from nested CLI runs by the agent, probes or reconciled
# reservations; rows tagged with another task come from other subcommands.
_LOOP_CALLER = "zoder"
_LOOP_TASKS = ("loop", "review")


def _ledger_loop_row(row: dict) -> bool:
    """True when ``zoder loop`` itself wrote this row (verified on CERBERUS)."""
    tags = _ledger_tags(row)
    return tags.get("caller") == _LOOP_CALLER and tags.get("task") in _LOOP_TASKS


def _ledger_phase(row: dict) -> str:
    """'review' for a review call, else 'author' for a genuine model call.

    ``zoder loop`` tags its calls with ``tags.task``: 'review' for the
    adversarial review phase and 'loop' for the authoring turns -- the live
    rows carry that tag and nothing else (no ``phase``/``path``), so it is
    the one signal the reviewer models are actually known by on a fleet host.
    """
    tags = row.get("tags") if isinstance(row.get("tags"), dict) else {}
    for key in ("phase", "kind", "role", "task", "purpose"):
        value = tags.get(key)
        if isinstance(value, str) and _REVIEW_TAG_RE.search(value):
            return "review"
    path = tags.get("path") or tags.get("cwd") or ""
    if isinstance(path, str) and _REVIEW_TAG_RE.search(path):
        return "review"
    return "author"


def _ledger_stamp(row: dict) -> float:
    """Epoch seconds for a row, using the ROOT ``ts_utc`` field only."""
    if not isinstance(row, dict):
        return 0.0
    return ledger_stamp(row.get("ts_utc"))


def _row_slot(row: dict) -> str:
    """The worker slot a row was written by, when the engine records one."""
    if not isinstance(row, dict):
        return ""
    for container in (row.get("tags"), row):
        if not isinstance(container, dict):
            continue
        for key in ("slot", "worker_slot", "hive_slot"):
            value = container.get(key)
            if isinstance(value, (str, int)) and str(value).strip():
                return str(value).strip()
    return ""


def attribute_models(
    rows: Sequence[dict],
    author_record: dict | None = None,
    reviewer_record: dict | None = None,
    slot: str | None = None,
) -> dict | None:
    """``{author, reviewers, source}`` from the ledger window of one run.

    ``author_record`` / ``reviewer_record`` are the loop's own JSON records
    (rows as :func:`parse_loop_models` sees them); the ledger fills the gaps
    the loop left and overrules what it got wrong:

    * the author is the loop's named author (``author_record["model"]``)
      when a call of it is in the window, else the model the worker launched
      (``author_record["launched"]``) when it served a call, else the model
      that served the EARLIEST call in the window. The ledger is
      append-only but NOT chronological -- each provider call is appended by
      whichever worker served it -- so file order says nothing about which
      call came first.
    * failed calls (:func:`_ledger_failed`) never count, and when the window
      holds rows ``zoder loop`` tagged as its own (:func:`_ledger_loop_row`)
      only those count: nested ``zoder health --probe`` / CLI runs by the
      agent write untagged rows into the same window.
    * the reviewers are exactly the models that served a review call in the
      window. A model the loop merely *named* as a reviewer but that never
      served a call (the EIH backup neighbour the config prefers, while
      ``hive_reviews`` shows the local gemma4-31b) is NOT added: the bus
      records what ran, not what was configured.
    * ``slot``, when known, drops rows another hive slot on the same host
      wrote into the overlapping window. Rows without a slot tag are kept --
      the engine does not always record one.

    Returns None when the window shows no model at all.
    """
    slot = (slot or "").strip()
    timed: list[tuple[float, int, dict]] = []
    for index, row in enumerate(rows):
        if slot and _row_slot(row) and _row_slot(row) != slot:
            continue
        timed.append((_ledger_stamp(row), index, row))
    # A window holding rows the loop tagged as its own counts only those:
    # anything else in it was written by a nested CLI run, a probe or another
    # subcommand on the same host. A ledger with no loop-tagged row at all
    # (an engine that does not tag) keeps every row.
    if any(_ledger_loop_row(row) for _stamp, _index, row in timed):
        timed = [item for item in timed if _ledger_loop_row(item[2])]
    # Stable on (timestamp, file position): rows with an equal or unparseable
    # stamp keep their relative file order instead of reaching the previous
    # job's calls.
    timed.sort(key=lambda item: (item[0] or float("inf"), item[1]))
    author_calls: list[str] = []
    reviewers: list[str] = []
    for _stamp, _index, row in timed:
        model = _ledger_model(row)
        if not model or _ledger_failed(row):
            continue
        if _ledger_phase(row) == "review":
            if model not in reviewers:
                reviewers.append(model)
        elif model not in author_calls:
            author_calls.append(model)
    named_author = (author_record or {}).get("model")
    launched = (author_record or {}).get("launched")
    earliest = author_calls[0] if author_calls else None
    if named_author and (not author_calls or named_author in author_calls):
        # The loop named its author and the ledger agrees (or shows no
        # successful call to contradict it).
        author = named_author
    elif launched and launched in author_calls:
        # The loop named nobody, or a model that served no call: the model
        # the worker itself launched wins whenever the ledger saw it serve.
        author = launched
    else:
        # Only when the launched model served nothing: the one that actually
        # served first.
        author = earliest
    reviewers = [m for m in reviewers if m != author]
    # No ledger reviewer at all: the loop's named reviewers are the only
    # evidence left, so keep them rather than reporting an empty review.
    if not reviewers:
        reviewers = [
            m for m in (reviewer_record or {}).get("reviewers", []) if m and m != author
        ]
    if not author and not reviewers:
        return None
    return {"author": author, "reviewers": reviewers, "source": "ledger"}


# ---------------------------------------------------------------------------
# Workspace containment (incident 2026-10-17, HYDRA, job
# fix:p3-npu-d-depends-r2).
#
# The worker ALWAYS creates WORKSPACE_ROOT/<job_id> and nothing else, but a
# codex job launched with `codex exec --sandbox danger-full-access` could find
# and edit/switch branches in ANY directory the user owns -- that job reached
# into another session's live image-build clone
# ~/Projects/p3-amd64-20261017/cix-installer. Two independent layers close
# that: (1) the sandbox itself, (2) a before/after snapshot of the git state
# of ~/Projects repos for EVERY driver (codex AND zoder), which fails the job
# rather than touching someone else's tree.
# ---------------------------------------------------------------------------

# Opt-in that allows an operator to run the codex driver unsandboxed. Absent
# it, danger-full-access is refused regardless of what CODEX_SANDBOX says.
# Extra writable roots handed to codex: only TMPDIR, and only when it already
# lives INSIDE this job's workspace. Never a path outside WORKSPACE_ROOT.
PROTECTED_PROJECTS_GLOBS = ("*", "*/*")
PROTECTED_REPO_SCAN_LIMIT = 200


def assert_workdir_in_workspace(workdir: str) -> str:
    """Refuse any git operation against a directory outside WORKSPACE_ROOT.

    Defence in depth against (2): a job's repo must never resolve to a
    pre-existing local checkout somewhere else on the host. Returns the
    resolved workdir so callers can use the anchored path.
    """
    root = os.path.realpath(WORKSPACE_ROOT)
    resolved = os.path.realpath(workdir)
    if resolved != root and not resolved.startswith(root + os.sep):
        raise ValueError(
            f"workdir {resolved!r} is outside WORKSPACE_ROOT {root!r}; refusing"
        )
    return resolved


# ---------------------------------------------------------------------------
# ARGONAS-only push target (incident 2026-10-25, job 01a17539-d8ca).
#
# TYDEUS pushed job 01a17539-d8ca's branch to PUBLIC GitLab: the workspace
# origin was https://gitlab.com/ncz-os/zoder.git (REPO_MAP's zoder entry, and
# the registry helper's clone.remotes fallback). Fleet policy is that
# automation pushes ONLY to the registry's ARGONAS clone, as jasonperlow.
#
# Three choke points, all in this section:
#   * _force_workspace_origin -- after the clone, before the driver runs:
#     origin points at the ARGONAS URL, pushurl too, and any remote whose
#     url/pushurl is a public forge is removed from the workspace config.
#   * guard_push_target -- immediately before EVERY `git push`, re-reads the
#     resolved push url and refuses anything that is not ARGONAS-as-jasonperlow.
#   * _git_remote_mutation_refusal -- the same check for a `git remote add` /
#     `set-url` the finalizer might perform.
# ---------------------------------------------------------------------------


def _git_config(workdir: str, env: dict, *args: str) -> str:
    try:
        proc = subprocess.run(
            ["git", "-C", workdir, "config", *args],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    # A stubbed subprocess.run may return None; a config read must never raise.
    return (getattr(proc, "stdout", "") or "").strip()


def workspace_remote_config(workdir: str, env: dict) -> str:
    """``remote.<name>.<key> <value>`` lines for every configured remote."""
    return _git_config(workdir, env, "--get-regexp", r"^remote\..*")


# Test-only escape hatch for offline push tests, which redirect the ARGONAS
# ssh url to a local bare repo with url.<base>.insteadOf. It relaxes ONLY the
# rewrite check below; the configured url must still be an ARGONAS clone and
# `_force_workspace_origin` still refuses to leave a non-ARGONAS origin in
# place. Production never sets it (it is a module attribute, not an env var).
ALLOW_LOCAL_TRANSPORT_REWRITE = False


def guard_workspace_push(workdir: str, env: dict, remote: str = "origin") -> str | None:
    """None when `git push <remote>` would reach the ARGONAS clone, else why not.

    Gates on BOTH the configured push url and the url git actually dials:
    ``url.<base>.insteadOf`` (or a ``--push`` override) can redirect a remote
    to a different host after configuration, and the incident this guards
    against was precisely a push that landed somewhere other than ARGONAS.
    """
    configured = configured_push_url(workdir, env, remote)
    refusal = guard_push_target(configured, where=f"{remote} (configured)")
    if refusal:
        return refusal
    if ALLOW_LOCAL_TRANSPORT_REWRITE:
        # Tests only. Set by a test module attribute, never from the
        # environment: production has no way to switch this on, and the
        # CONFIGURED url above must still be an ARGONAS clone.
        return None
    effective = resolved_push_url(workdir, env, remote)
    # git rewrote the url (url.<base>.insteadOf, or a --push override).
    # A rewrite may point at the same ARGONAS repo (spelling only), but never
    # at a different host or path: an insteadOf rule is otherwise a one-line
    # way to send the push somewhere ARGONAS never sees.
    if (
        effective
        and effective != configured
        and not push_policy.is_url_equivalent(effective, configured)
    ):
        return (
            f"push_refused: {remote} is configured as "
            f"{push_policy.redact_remote_url(configured)!r} but git would dial "
            f"{push_policy.redact_remote_url(effective)!r} (rewritten by "
            "config). Automation pushes to ARGONAS only."
        )
    return None


def guard_push_target(push_url: str | None, *, where: str = "push") -> str | None:
    """None when ``push_url`` is a legal ARGONAS target, else the refusal text."""
    reason = push_policy.push_refusal_reason(push_url)
    if not reason:
        return None
    # The url is quoted verbatim in the refusal, and that text reaches the job
    # row: a forge remote may carry a token in its userinfo, so it is redacted
    # here rather than logged.
    return (
        f"push_refused: {where} target "
        f"{push_policy.redact_remote_url(push_url)!r} is not the ARGONAS clone "
        f"(ssh://{push_policy.ARGONAS_USER}@{push_policy.ARGONAS_HOST}"
        f"{push_policy.ARGONAS_GIT_ROOT}...): {reason}. Automation pushes to "
        "ARGONAS only; the orchestrator pushes to forges after review."
    )


def configured_push_url(workdir: str, env: dict, remote: str = "origin") -> str:
    """The url configured for pushes, BEFORE any url.<base>.insteadOf rewrite.

    RACE SCOPE, stated plainly: this reads ``.git/config`` with no lock, so a
    writer that mutates the file between this read and the ``git push`` that
    follows it is not observed. That window is accepted rather than locked,
    because the only thing inside it is the driver, and the driver runs
    BEFORE this guard -- the sequence is clone -> _force_workspace_origin ->
    driver -> guard -> push, so a driver edit is already in the file when it
    is read. Nothing else runs concurrently in a job workspace: the worker
    owns it, the reaper is post-driver, and jobs get one workspace each. A
    lock would add a failure mode (stuck lock, stale pid) to defend against a
    writer that by construction cannot exist. What protects us if that ever
    stops being true is not this read but the post-rewrite re-check in
    _force_workspace_origin and the url `git` itself resolves at push time,
    which is re-read by resolved_push_url and gated separately.

    ``pushurl`` wins over ``url`` (that is how git decides), and within a key
    the LAST value in the file wins: ``git config --get-all`` lists values in
    read order, so the final line is the one in effect. Returning the first
    would report a shadowed value -- a stale forge URL could then look like
    "not configured" while git still dials it.
    """
    for key in (f"remote.{remote}.pushurl", f"remote.{remote}.url"):
        values = [
            value.strip()
            for value in _git_config(workdir, env, "--get-all", key).splitlines()
            if value.strip()
        ]
        if values:
            return values[-1]
    return ""


def resolved_push_url(workdir: str, env: dict, remote: str = "origin") -> str:
    """The url `git push <remote>` would use (pushurl wins over url).

    NOTE: git applies ``url.<base>.insteadOf`` here, so this can differ from
    what is configured. ``configured_push_url`` is the value to gate on; this
    one is what git will actually dial.
    """
    try:
        proc = subprocess.run(
            ["git", "-C", workdir, "remote", "get-url", "--push", remote],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return (getattr(proc, "stdout", "") or "").strip()


def git_remote_mutation_refusal(url: str | None) -> str | None:
    """Refuse a `git remote add`/`set-url` that points at a public forge.

    Only forge/web URLs are refused here: the workspace's own origin is
    deliberately set to an ssh URL on the ARGONAS host, and any other
    ARGONAS-host remote is judged again at push time by guard_push_target.
    """
    if push_policy.is_https_remote(url) or push_policy.is_public_forge_url(url):
        return (
            f"remote_refused: {push_policy.redact_remote_url(url)!r} is a "
            "public forge; automation may only configure the ARGONAS clone as "
            "a remote"
        )
    return None


def _force_workspace_origin(workdir: str, repo_url: str, env: dict) -> str | None:
    """Point the workspace's origin at ``repo_url`` and strip forge remotes.

    Returns None on success, or an error string (the caller fails the job with
    failure_reason=no_private_remote). Never pushes, never fetches.
    """
    canonical = push_policy.canonical_argonas_url(repo_url)
    if not canonical:
        return (
            f"no_private_remote: clone url "
            f"{push_policy.redact_remote_url(repo_url)!r} is not an ARGONAS "
            f"remote; refusing to leave it as the workspace origin"
        )
    try:
        has_origin = subprocess.run(
            ["git", "-C", workdir, "remote", "get-url", "origin"],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        has_origin = None
    added = False
    try:
        if getattr(has_origin, "returncode", 1) != 0:
            subprocess.run(
                ["git", "-C", workdir, "remote", "add", "origin", canonical],
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
                check=True,
            )
            # `remote add` already sets remote.origin.url, but not the push
            # url; only the push url needs the follow-up below.
            added = True
        # Both url and pushurl: the pushurl wins for `git push origin`, so a
        # stale forge pushurl left by an earlier job must be overwritten too.
        # When `remote add` just ran there is nothing to overwrite, so the
        # extra `set-url` is skipped rather than issued twice for one value.
        if not added:
            subprocess.run(
                ["git", "-C", workdir, "remote", "set-url", "origin", canonical],
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
                check=True,
            )
        subprocess.run(
            [
                "git",
                "-C",
                workdir,
                "remote",
                "set-url",
                "--push",
                "origin",
                canonical,
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return (
            f"no_private_remote: could not set the workspace origin to ARGONAS: {exc}"
        )
    # STEP 2 -- STRIP every non-private remote from the workspace config.
    # This is the "strip forge remotes" half of the contract: after this loop
    # the workspace has exactly one remote, `origin`, and it is the ARGONAS
    # clone. The scan is deliberately broader than a forge-host match (see
    # push_policy.non_private_remotes): a personal fork over ssh, a root@
    # ARGONAS url, a bind-mounted local path or a token-bearing https url is
    # just as much a way to push past ARGONAS, so it goes too. `remote
    # remove` drops every url/pushurl/fetch key for the name in one go.
    #
    # Names are read back AFTER the rewrite above, so the scan sees the
    # origin just set, and keep="origin" guarantees the good one survives.
    for name in push_policy.non_private_remotes(
        workspace_remote_config(workdir, env), keep="origin"
    ):
        subprocess.run(
            ["git", "-C", workdir, "remote", "remove", name],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        print(
            f"[hive-worker:{WORKER_KIND}] removed non-ARGONAS remote {name!r} "
            f"from the job workspace",
            flush=True,
        )
    # STEP 3 -- verify what git will actually push to, not what we asked it to
    # set. A remote that survived the loop, or an url.<base>.insteadOf
    # rewrite, shows up here and fails the job instead of reaching a forge.
    actual = resolved_push_url(workdir, env)
    reason = push_policy.push_refusal_reason(actual)
    if reason:
        return (
            f"no_private_remote: workspace origin resolved to "
            f"{push_policy.redact_remote_url(actual)!r}, which is not the "
            f"ARGONAS clone: {reason}"
        )
    return None


# ---------------------------------------------------------------------------
# Job-workspace process reaper (incident 2026-10-21, job 01a1601b).
#
# The driver's process group is killed on timeout (os.killpg of the child we
# spawned), but an agent is free to spawn DETACHED background processes that
# call setsid()/start their own session -- those leave our process group and
# survive the killpg. Job 01a1601b's agent started 368 `yes` processes whose
# cwd was the job workspace; they were still running 2 hours after the job's
# 7200 s timeout expired. Every such process is killed here AFTER the driver
# exits, matched ONLY by ownership + working directory -- never by
# pattern-matching command lines (a command line is attacker-controlled and
# would both miss disguised leftovers and risk killing an unrelated process).
#
# Safety rules:
#   * only processes whose real /proc/<pid>/cwd resolves INSIDE this job's
#     workspace directory,
#   * only processes owned by the worker's own uid,
#   * never the worker itself and never any ancestor of the worker,
#   * SIGTERM -> grace period -> SIGKILL, always by exact PID.
# ---------------------------------------------------------------------------

REAP_TERM_GRACE_SEC = float(os.environ.get("REAP_TERM_GRACE_SEC", "5.0"))
REAP_POLL_SEC = float(os.environ.get("REAP_POLL_SEC", "0.2"))


def _proc_stat_fields(pid: int) -> list[str]:
    """The whitespace-separated fields of /proc/<pid>/stat AFTER the comm.

    comm may itself contain spaces and parentheses, so the fields are taken
    from AFTER the final ')': field 0 is the process state, field 1 its PPID.
    Returns [] when the process is gone or /proc is unreadable.
    """
    try:
        with open(f"/proc/{pid}/stat", "rb") as fh:
            stat_line = fh.read().decode("utf-8", "replace")
    except OSError:
        return []
    close = stat_line.rfind(")")
    if close < 0:
        return []
    return stat_line[close + 1 :].split()


def _ancestors_of(pid: int) -> set[int]:
    """pids on this process's own ancestor chain (pid itself excluded).

    Built by walking /proc/<pid>/stat's PPID field up to pid 1. If /proc is
    unreadable (a container without procfs) we degrade to "unknown" and return
    an empty set -- the caller then relies on the uid+cwd filter, which can
    only ever select a process sitting inside the job workspace.
    """
    seen: set[int] = set()
    current = pid
    while current and current > 1:
        fields = _proc_stat_fields(current)
        if len(fields) < 2:
            break
        try:
            ppid = int(fields[1])
        except ValueError:
            break
        seen.add(ppid)
        current = ppid
    return seen


# Shared per-user services that may legitimately have their cwd inside a job
# workspace and must never be reaped with it (2026-10-21): zoder cold-starts
# `zeroclaw daemon --ephemeral` from whatever directory the first zoder of the
# session runs in, so the daemon's cwd is often the workspace of the job that
# happened to start it, while every other zoder on the host talks to it over
# ~/.zoder/data/daemon.sock. Reaping it at the end of that job kills the agent
# of every other job still running. Identified by /proc/<pid>/exe (the binary
# actually executing), never by the command line.
REAP_SHARED_DAEMON_BINARIES = tuple(
    path.strip()
    for path in os.environ.get(
        "REAP_SHARED_DAEMON_BINARIES", "~/.local/bin/zeroclaw"
    ).split(",")
    if path.strip()
)


def _shared_daemon_paths() -> frozenset[str]:
    """Resolved paths of the shared daemon binaries on this host."""
    paths = set()
    for path in REAP_SHARED_DAEMON_BINARIES:
        try:
            paths.add(os.path.realpath(os.path.expanduser(path)))
        except OSError:
            continue
    return frozenset(paths)


def _is_shared_daemon(pid: int) -> bool:
    """True when ``pid`` executes the installed shared daemon binary itself.

    Matched on the resolved binary path, not the basename, so a copy or
    renamed executable that a job drops into its workspace is not exempt.
    """
    try:
        exe = os.readlink(f"/proc/{pid}/exe")
    except OSError:
        return False
    # The kernel appends " (deleted)" when the binary was replaced on disk
    # (e.g. a reinstall) while the daemon kept running.
    exe = exe.removesuffix(" (deleted)")
    return exe in _shared_daemon_paths()


def _pid_in_workspace(pid: int, root: str, uid: int) -> bool:
    """True when ``pid`` is owned by ``uid``, its realpath'd cwd is ``root``
    or below it, and it is not a shared daemon (see _is_shared_daemon)."""
    proc_dir = f"/proc/{pid}"
    try:
        if os.stat(proc_dir).st_uid != uid:
            return False
        cwd = os.readlink(f"{proc_dir}/cwd")
    except (OSError, ValueError):
        return False
    try:
        resolved = os.path.realpath(cwd)
    except OSError:
        return False
    if resolved != root and not resolved.startswith(root + os.sep):
        return False
    return not _is_shared_daemon(pid)


def _workspace_processes(workspace: str, uid: int, exclude: set[int]) -> list[int]:
    """pids owned by ``uid`` whose cwd is inside ``workspace`` (not excluded).

    The workspace is realpath'd here and the cwd of every candidate is also
    realpath'd before the containment test, so a symlinked or `..`-escaping
    cwd can never be mistaken for "inside" and, conversely, a process that
    merely *mentions* the path without actually being there is not selected.
    """
    try:
        root = os.path.realpath(workspace)
    except OSError:
        return []
    if not root or not os.path.isdir(root):
        return []
    pids: list[int] = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        if pid == os.getpid() or pid in exclude:
            continue
        if _pid_in_workspace(pid, root, uid):
            pids.append(pid)
    return pids


def _proc_comm(pid: int) -> str:
    """Best-effort comm for a pid, for logging only ('' if unavailable)."""
    try:
        with open(f"/proc/{pid}/comm", "rb") as fh:
            return fh.read().decode("utf-8", "replace").strip()
    except OSError:
        return ""


def _pid_alive(pid: int) -> bool:
    """True while ``pid`` is a process that can still run or hold resources.

    A ZOMBIE (state ``Z``) is NOT alive for our purposes: it has already been
    killed and is only waiting for its parent to wait() it, it consumes no
    CPU and no memory, and a further SIGKILL to it is a no-op. Counting a
    zombie as alive would make the SIGTERM->SIGKILL escalation loop believe a
    reaped process ignored SIGTERM and would report a false leak. /proc/<pid>
    keeps existing for a zombie, so the state must be read explicitly.
    """
    fields = _proc_stat_fields(pid)
    if fields:
        return fields[0] != "Z"
    return os.path.isdir(f"/proc/{pid}")


def _proc_start_time(pid: int) -> str:
    """Kernel start time of ``pid`` (/proc/<pid>/stat field 22), '' if gone.

    Paired with the pid it identifies one process for its whole life: a pid
    that is freed and reused gets a different start time, so re-checking it
    before every signal keeps the reaper from hitting an unrelated process
    that inherited a leftover's pid during the grace period.
    """
    fields = _proc_stat_fields(pid)
    # fields[0] is stat field 3 (state), so field 22 (starttime) is index 19.
    return fields[19] if len(fields) > 19 else ""


REAP_MAX_SCANS = int(os.environ.get("REAP_MAX_SCANS", "5"))


def _pin_target(pid: int, root: str, uid: int) -> tuple[int | None, str] | None:
    """Pin ``pid`` and confirm the pinned process is still a workspace leftover.

    Returns (pidfd or None, start_time), or None when the pid is gone or now
    names a process that does not qualify. With a pidfd every later signal
    goes to exactly this process, so a pid freed and reused at any point
    after the check can never be hit. Without pidfd support (old kernel or
    Python) the recorded start time is re-checked before each os.kill.
    """
    start = _proc_start_time(pid)
    if not start:
        return None
    fd = None
    if hasattr(os, "pidfd_open"):
        try:
            fd = os.pidfd_open(pid)
        except ProcessLookupError:
            return None
        except OSError:
            fd = None
    # Re-verify AFTER pinning: a pid reused between the scan and the pin
    # either changes the start time or fails the workspace/uid check.
    if _proc_start_time(pid) != start or not _pid_in_workspace(pid, root, uid):
        if fd is not None:
            os.close(fd)
        return None
    return fd, start


def _signal_target(pid: int, fd: int | None, start: str, sig: int) -> bool:
    """Send ``sig`` to the pinned process; False if it is gone or not ours."""
    try:
        if fd is not None:
            signal.pidfd_send_signal(fd, sig)
        else:
            if _proc_start_time(pid) != start:
                return False
            os.kill(pid, sig)
        return True
    except ProcessLookupError:
        return False


def _target_alive(pid: int, start: str) -> bool:
    """The pinned process is still running (not exited, not a zombie)."""
    return _proc_start_time(pid) == start and _pid_alive(pid)


def reap_workspace_processes(workspace: str) -> int:
    """Terminate leftover processes rooted in ``workspace``; return the count.

    Runs after a job ends (any status) so detached background processes the
    driver spawned cannot outlive the job. Selection is uid + realpath'd cwd
    inside the workspace (never the command line), excluding the worker, its
    ancestors and shared daemons. Each target is pinned with a pidfd and
    re-verified before it is signalled (SIGTERM, grace, SIGKILL). The
    workspace is re-scanned up to REAP_MAX_SCANS times so a leftover that
    forked during cleanup is reaped too. The count is the number of pinned
    processes that are gone afterwards; it is reported on the job result as
    ``reaped_processes``.
    """
    if not workspace:
        return 0
    try:
        root = os.path.realpath(workspace)
    except OSError:
        return 0
    uid = os.getuid()
    excluded = _ancestors_of(os.getpid()) | {os.getpid(), 1}
    pinned: dict[int, tuple[int | None, str]] = {}
    signalled: set[int] = set()
    try:
        for _ in range(max(1, REAP_MAX_SCANS)):
            batch: dict[int, tuple[int | None, str]] = {}
            for pid in _workspace_processes(workspace, uid, excluded):
                if pid in pinned:
                    # Already handled (TERM, grace, KILL) in an earlier pass;
                    # a re-scan only looks for leftovers forked since then.
                    continue
                target = _pin_target(pid, root, uid)
                if target is not None:
                    pinned[pid] = target
                    batch[pid] = target
            if not batch:
                break
            for pid, (fd, start) in batch.items():
                comm = _proc_comm(pid)
                try:
                    sent = _signal_target(pid, fd, start, signal.SIGTERM)
                except PermissionError:
                    sent = False
                    print(
                        f"[hive-worker:{WORKER_KIND}] reap: no permission to SIGTERM "
                        f"pid={pid} comm={comm!r}",
                        flush=True,
                    )
                if sent:
                    signalled.add(pid)
                    print(
                        f"[hive-worker:{WORKER_KIND}] reap: SIGTERM pid={pid} "
                        f"comm={comm!r} workspace={workspace}",
                        flush=True,
                    )
            deadline = time.time() + REAP_TERM_GRACE_SEC
            surviving = dict(batch)
            while surviving and time.time() < deadline:
                surviving = {
                    pid: t for pid, t in surviving.items() if _target_alive(pid, t[1])
                }
                if surviving:
                    time.sleep(REAP_POLL_SEC)
            for pid, (fd, start) in sorted(surviving.items()):
                comm = _proc_comm(pid)
                try:
                    sent = _signal_target(pid, fd, start, signal.SIGKILL)
                except PermissionError:
                    sent = False
                    print(
                        f"[hive-worker:{WORKER_KIND}] reap: no permission to SIGKILL "
                        f"pid={pid} comm={comm!r}",
                        flush=True,
                    )
                if sent:
                    signalled.add(pid)
                    print(
                        f"[hive-worker:{WORKER_KIND}] reap: SIGKILL pid={pid} "
                        f"comm={comm!r} (ignored SIGTERM)",
                        flush=True,
                    )
        # A SIGKILLed process needs a moment to read as gone (or as a zombie
        # until its parent waits), so settle briefly instead of sampling once.
        settle_until = time.time() + REAP_POLL_SEC * 5
        while time.time() < settle_until:
            if not any(_target_alive(pid, t[1]) for pid, t in pinned.items()):
                break
            time.sleep(REAP_POLL_SEC)
        # Only what this call signalled and is gone now: a target that exited
        # on its own before any signal was not reaped by us.
        return sum(1 for pid in signalled if not _target_alive(pid, pinned[pid][1]))
    finally:
        for fd, _start in pinned.values():
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass


def resolve_codex_sandbox(value: str | None = None) -> str:
    """Return the sandbox mode the codex driver may run with.

    CODEX_SANDBOX is still honoured as an override, but
    danger-full-access is refused unless the operator opted in explicitly on
    the host -- that mode is what let a job edit another session's clone.
    """
    sandbox = (value if value is not None else CODEX_SANDBOX).strip()
    if not sandbox:
        return DEFAULT_CODEX_SANDBOX
    if sandbox == UNSANDBOXED_CODEX_MODE:
        if os.environ.get(HIVE_ALLOW_UNSANDBOXED_CODEX_ENV) != "1":
            raise RuntimeError(
                f"CODEX_SANDBOX={UNSANDBOXED_CODEX_MODE!r} refused: an unsandboxed "
                "codex run can edit any directory the user owns (incident "
                "2026-10-17). Set "
                f"{HIVE_ALLOW_UNSANDBOXED_CODEX_ENV}=1 on the host to allow it."
            )
        print(
            f"[hive-worker:{WORKER_KIND}] WARNING: {HIVE_ALLOW_UNSANDBOXED_CODEX_ENV}=1 "
            f"-- running codex UNSANDBOXED ({UNSANDBOXED_CODEX_MODE}); it may modify "
            "any path the user owns",
            flush=True,
        )
    return sandbox


def codex_model_mismatch(job: dict) -> str:
    """The Codex model the job asked for when this host runs a different one, else "".

    preferred_models only steers zoder rungs; a Codex worker always runs
    CODEX_MODEL. A job that names a gpt-* model must therefore either match
    the host or fail loudly, never run on a cheaper model unnoticed.
    """
    models = job.get("preferred_models") or []
    if isinstance(models, str):
        models = [models]
    if not isinstance(models, (list, tuple)):
        return ""
    for raw in models:
        if not isinstance(raw, str):
            continue
        name = raw.strip().lower()
        if name.startswith("openai/"):
            name = name.split("/", 1)[1]
        if name.startswith("gpt-"):
            return "" if name == CODEX_MODEL.strip().lower() else name
    return ""


# Appended (never prepended: the worker takes the commit subject from the brief's
# first line) to every Codex prompt. Inside the workspace-write sandbox ~/.zoder
# and the git metadata are read-only by design, so an agent that tries to commit,
# push or run `zoder review` itself only fails (job 01a178c2-44ac, 2026-10-26);
# the worker commits, reviews and pushes after the agent exits.
CODEX_WORKER_NOTE = (
    "\n\nWorker note: leave your edits uncommitted in the workspace. Do not run "
    "git commit, git push or zoder review: the sandbox cannot write them, and the "
    "worker commits, reviews and pushes after you exit."
)


def codex_exec_args(workdir: str, sandbox: str | None = None) -> list[str]:
    """Argv for `codex exec`, sandboxed to this job's workspace by default.

    workspace-write + network_access keeps `git fetch`/`git push` working
    (jobs deliver by pushing), while the sandbox root and --cd pin the agent
    inside WORKSPACE_ROOT/<job_id>.
    """
    return [
        "exec",
        "--sandbox",
        resolve_codex_sandbox(sandbox),
        "-c",
        'approval_policy="never"',
        "-c",
        "sandbox_workspace_write.network_access=true",
        "--cd",
        workdir,
        "--skip-git-repo-check",
        "-m",
        CODEX_MODEL,
    ]


def _protected_repos(projects_root: str) -> list[str]:
    """Git repos at depth 1 and 2 under ~/Projects, bounded and cheap."""
    root = os.path.expanduser(projects_root)
    found: list[str] = []
    for pattern in PROTECTED_PROJECTS_GLOBS:
        for path in sorted(glob.glob(os.path.join(root, pattern))):
            if not os.path.isdir(path):
                continue
            if os.path.isdir(os.path.join(path, ".git")):
                found.append(path)
            if len(found) > PROTECTED_REPO_SCAN_LIMIT:
                print(
                    f"[hive-worker:{WORKER_KIND}] protection: more than "
                    f"{PROTECTED_REPO_SCAN_LIMIT} repos under {root}; skipping the "
                    "snapshot rather than scanning unbounded",
                    flush=True,
                )
                return []
    return found


def _repo_head_and_branch(path: str, env: dict) -> tuple[str, str] | None:
    def run(args: list[str]) -> str | None:
        try:
            out = subprocess.run(
                ["git", "-C", path, *args],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=15,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        return (out.stdout or "").strip() if out is not None else None

    head = run(["rev-parse", "HEAD"])
    if not head:
        return None
    branch = run(["symbolic-ref", "--short", "HEAD"]) or "DETACHED"
    return head, branch


def snapshot_protected_repos(
    env: dict | None = None, projects_root: str = "~/Projects"
) -> dict[str, tuple[str, str]]:
    """HEAD sha + branch for every ~/Projects repo (depth 2, bounded)."""
    env = env if env is not None else os.environ.copy()
    snapshot: dict[str, tuple[str, str]] = {}
    for path in _protected_repos(projects_root):
        state = _repo_head_and_branch(path, env)
        if state:
            snapshot[path] = state
    return snapshot


def protection_enforced() -> bool:
    """Whether a moved ~/Projects checkout fails the job (vs. a warning).

    HIVE_PROTECT_ENFORCE=1 / 0 decides explicitly. Unset, only the codex
    driver enforces: it is the one whose agent had full filesystem reach
    (2026-10-17 escape), while other sessions' builds on the same host move
    their own checkouts legitimately during long zoder runs.
    """
    value = os.environ.get("HIVE_PROTECT_ENFORCE", "").strip()
    if value in ("1", "0"):
        return value == "1"
    return WORKER_KIND == "codex"


def compare_protected_repos(
    before: dict[str, tuple[str, str]],
    env: dict | None = None,
    projects_root: str = "~/Projects",
) -> list[dict]:
    """Repos whose HEAD/branch moved during the run (never auto-reverted)."""
    after = snapshot_protected_repos(env=env, projects_root=projects_root)
    violations = []
    for path, (head, branch) in sorted(after.items()):
        previous = before.get(path)
        if previous is None:
            continue
        if previous != (head, branch):
            violations.append(
                {"path": path, "before": previous, "after": (head, branch)}
            )
    return violations


def report_provider_cooldown(
    job_id: str, model: str, status: str, result: dict, *, now=None
) -> dict:
    """Write the cooldown marker and return the result fields that explain it.

    The attempt is reported with its real status and counts against
    retry_count: the bus increments retry_count on every claimed->queued
    release too, so there is no way to hand it back for free. What the
    cooldown buys is that the retry runs on a non-MiniMax rung. The error
    string says so, and keeps the attempt's own failure reason after it.
    """
    marker = write_cooldown(model, reason=f"job {job_id}: provider rate-limit", now=now)
    until = _iso_utc(marker["expires_at"])
    error = (
        f"provider_cooldown: MiniMax provider rate-limited this attempt "
        f"(model={model}); MiniMax rungs are held off for ~"
        f"{int(COOLDOWN_SECS // 60)} min (until {until}). This attempt counts "
        f"against retry_count; retries run on the DeepSeek substitute "
        f"off-peak and on the local coder during DeepSeek's peak window."
    )
    prior = str(result.get("error") or "").strip()
    if prior:
        error = _cap_text(f"{error} | {prior}")
    fields = {
        "via": "hive_worker",
        "error": error,
        "provider_cooldown": {**marker, "expires_at_iso": until},
        "failure_reason": "provider_rate_limited",
        "prior_status": status,
    }
    return fields


def run_job(job: dict) -> None:
    """Guarantees a terminal disposition (patch or release) for every claimed
    job, even on an exception type we didn't anticipate (codex review
    finding, 2026-09-14: PermissionError/OSError etc previously escaped this
    function entirely, killing the daemon with the job left claimed
    forever)."""
    global _current_job_id, _claim_renewal_blocked_id
    global _current_provenance, _current_run_models, _current_run_window
    job_id = job.get("id", "")
    _claim_renewal_blocked_id = None
    _current_job_id = job_id
    _current_provenance = {}
    _current_run_models = None
    _current_run_window = None
    try:
        _run_job_inner(job)
    except Exception as e:
        print(
            f"[hive-worker:{WORKER_KIND}] job {job_id}: unhandled exception: {e!r}",
            flush=True,
        )
        if job_id:
            patch_job(
                job_id,
                "failed",
                {
                    "via": "hive_worker",
                    "failure_reason": "unhandled_exception",
                    "error": _cap_text(f"unhandled: {e!r}"),
                },
            )
    finally:
        _current_job_id = None
        _current_provenance = {}
        _current_run_models = None
        _current_run_window = None


def _rescue_workspace(job_id: str, workdir: str, repo_url: str) -> dict | None:
    """Move an existing job checkout aside without losing anything.

    Returns a description of what was preserved, or None if the move itself
    failed (the caller then keeps the old fail-and-preserve behaviour).
    Unpushed commits on the checked-out branch are also pushed to
    rescue/hive-<job_id>-<ts> on the job's REPO_MAP URL (never the checkout's
    own `origin`, which an earlier job could have rewritten), best effort, so a
    re-imaged host (HYDRA, 2026-10-03) cannot take them with it. Uncommitted
    edits stay in the moved directory; nothing is staged, stashed or deleted.
    """
    ts = time.strftime("%Y%m%dT%H%M%S")
    rescue_root = os.path.join(WORKSPACE_ROOT, ".rescued")
    dest = os.path.join(rescue_root, f"{job_id}-{ts}")
    info: dict = {"moved_to": dest}
    try:
        unpushed = subprocess.run(
            ["git", "-C", workdir, "log", "--oneline", "HEAD", "--not", "--remotes"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if unpushed.returncode == 0 and unpushed.stdout.strip():
            branch = f"rescue/hive-{job_id}-{ts}"
            # HARD GUARD (2026-10-25): the rescue branch is just as much a push
            # as a delivery, so it gets the same ARGONAS-only check. Call it
            # once and reuse the reason: a second call would double-count any
            # logging/state the guard grows, and could disagree if it is ever
            # made stateful.
            refusal_reason = guard_push_target(repo_url, where="rescue")
            if refusal_reason:
                pushed = None
                info["push_refused"] = refusal_reason
            else:
                pushed = subprocess.run(
                    [
                        "git",
                        "-C",
                        workdir,
                        "push",
                        "--quiet",
                        repo_url,
                        f"HEAD:refs/heads/{branch}",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=300,
                )
            info["unpushed_commits"] = len(unpushed.stdout.strip().splitlines())
            info["rescue_branch"] = (
                branch if pushed is not None and pushed.returncode == 0 else None
            )
        dirty = subprocess.run(
            ["git", "-C", workdir, "status", "--porcelain"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        info["uncommitted_entries"] = (
            len(dirty.stdout.splitlines()) if dirty.returncode == 0 else None
        )
    except (OSError, subprocess.SubprocessError) as exc:
        info["inspect_error"] = str(exc)[:200]
    try:
        os.makedirs(rescue_root, exist_ok=True)
        os.rename(workdir, dest)
    except OSError as exc:
        print(
            f"[hive-worker:{WORKER_KIND}] job {job_id}: rescue move failed: {exc}",
            flush=True,
        )
        return None
    print(
        f"[hive-worker:{WORKER_KIND}] job {job_id}: rescued prior workspace {info}",
        flush=True,
    )
    return info


def _probe_remote_branch(repo_url: str, branch: str, *, env: dict | None = None) -> str:
    """Three-state probe of `<branch>` on `<repo_url>` via `git ls-remote`.

    Returns one of:
      "present"         -- the ref `refs/heads/<branch>` exists on origin.
      "absent"          -- exit 0 with no matching line in stdout.
                           This is the ONLY state that the worker treats as
                           "branch is genuinely missing on origin".
      "transport_error" -- exit code !=0, or the subprocess could not
                           start, or the call timed out. Per `git
                           ls-remote`'s contract, a non-zero exit always
                           indicates a transport/auth/host-key failure
                           (auth failure → exit 2, known_hosts mismatch →
                           exit 128, network unreachable → exit 128, etc);
                           conflating it with "absent" hides the real
                           reason and turns a retryable transport hiccup
                           into a permanent-looking branch_not_found
                           terminal failure (operator 2026-10-21,
                           deepseek-flash review follow-up). The worker
                           MUST surface a transport error as clone_failed
                           (not branch_not_found) so the bus row keeps a
                           retryable signal.

    Used by the 2-branch clone decision (operator 2026-10-21, hive-bus
    incident 01a15453-* / 01a15bd0-a126 / 01a1550a-2fd5): briefs say
    "clone base X, create NEW branch Y", but the worker has to know
    whether Y already exists on origin (in which case the old `--branch Y`
    clone is the right move) or whether to fall back to `--branch X` +
    local `git checkout -B Y`.

    `git ls-remote --heads origin refs/heads/<branch>` is the safest
    probe: its exit status is 0 even when the ref is absent (it just
    prints nothing), so we parse stdout and treat any non-empty,
    ref-matching line as existence."""

    probe_env = os.environ.copy() if env is None else dict(env)
    # Reuse the fleet git transport (GIT_SSH_COMMAND from fleet_git_env) so
    # the probe and the real clone authenticate the same way; a "branch
    # missing on origin" verdict never comes back false-positive due to
    # a per-call key mismatch. See the transport comment in
    # _run_job_inner above.
    git_ssh_cmd = probe_env.get("GIT_SSH_COMMAND")
    if not git_ssh_cmd:
        git_ssh_cmd = os.environ.get("GIT_SSH_COMMAND")
    if git_ssh_cmd:
        probe_env["GIT_SSH_COMMAND"] = git_ssh_cmd
    try:
        proc = subprocess.run(
            ["git", "ls-remote", "--heads", repo_url, f"refs/heads/{branch}"],
            capture_output=True,
            text=True,
            env=probe_env,
            timeout=60,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return "transport_error"
    if proc.returncode != 0:
        # exit != 0 means ssh failed before reaching the remote, or the
        # remote rejected the connection (auth, host key, dns, timeout).
        # Falling back to "treat as missing" would silently downgrade a
        # short-lived transport hiccup into a permanent branch_not_found
        # the bus can't retry. Return the dedicated state so the caller
        # can map it to clone_failed with the real error in `error`.
        return "transport_error"
    if not proc.stdout:
        return "absent"
    return (
        "present"
        if any(
            line.rstrip().endswith(f"refs/heads/{branch}")
            for line in proc.stdout.splitlines()
        )
        else "absent"
    )


# Backwards-compatible alias: existing callers / older test snapshots
# expected a bool. The probe is intentionally wider now (transport errors
# are no longer collapsed into False), so this shim is for any external
# caller that wants the old "true iff the ref definitely exists" semantic
# without caring about transport. New code should call
# `_probe_remote_branch` directly so it can distinguish absent from
# transport_error; only the head/base branch decision needs that
# distinction.
def _remote_branch_exists(
    repo_url: str, branch: str, *, env: dict | None = None
) -> bool:
    return _probe_remote_branch(repo_url, branch, env=env) == "present"


def _capture_probe_stderr(repo_url: str, branch: str, *, timeout: int = 30) -> str:
    """Re-run the same ls-remote probe once more and return its stderr.

    Used by the 2-branch clone decision to surface the real transport
    error (ssh auth, known_hosts mismatch, dns, network unreachable) on
    the bus row instead of an opaque `transport_error: see worker log`.
    Deliberately a duplicate of `_probe_remote_branch`: a successful
    transport failure is itself rare and this only runs once per job
    when `_probe_remote_branch` returns `transport_error`."""

    try:
        proc = subprocess.run(
            ["git", "ls-remote", "--heads", repo_url, f"refs/heads/{branch}"],
            capture_output=True,
            text=True,
            env=os.environ.copy(),
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return f"{type(exc).__name__}: {exc}"
    err = (proc.stderr or "").strip()
    if err:
        return _stderr_tail(err.splitlines())
    return proc.stdout.strip() or f"git ls-remote exit {proc.returncode}"


def _run_job_inner(job: dict) -> None:
    global _current_run_window
    job_id = job["id"]
    kind = job.get("kind", "")
    description = job.get("description", "")
    head_branch = job.get("head_branch")
    base_branch = job.get("base_branch")
    # The route this attempt actually launched ("" = host default / unknown).
    agent = ""
    # The alias the zoder command really carries (--agent / -m / strict route);
    # initialised unconditionally so the claim path can never see it unbound.
    launched_agent = ""

    if not _JOB_ID_RE.match(job_id):
        print(
            f"[hive-worker:{WORKER_KIND}] job id {job_id!r} doesn't look like a UUID, refusing",
            flush=True,
        )
        release_job(job_id, "malformed_job_id")
        return

    if WORKER_KIND == "zeroclaw":
        socket_path = os.path.expanduser("~/.zoder/data/daemon.sock")
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
                probe.settimeout(2.0)
                probe.connect(socket_path)
        except FileNotFoundError:
            # An ABSENT socket is a cold daemon, not a dead one: zoder
            # cold-starts `zeroclaw daemon --ephemeral` on first use (see
            # REAP_SHARED_DAEMON_BINARIES), so an idle host has no socket until
            # the first job runs. Failing here made every idle host release
            # every job it claimed as daemon_lost (2026-10-28: hydra, minos,
            # tydeus), and the queue churned with nothing running.
            print(
                f"[hive-worker:{WORKER_KIND}] {socket_path} absent, daemon is cold; "
                "zoder starts it on first use",
                flush=True,
            )
        except OSError as exc:
            # A socket FILE that refuses or times out is a daemon that died or
            # hung and left its socket behind: that is the dead engine this
            # preflight exists to catch before a wasted agent loop.
            patch_job(job_id, "failed", {
                "via": "hive_worker",
                "failure_reason": "daemon_lost",
                "error": _cap_text(f"daemon.sock preflight failed: {type(exc).__name__}: {exc}"),
                "review_iterations": 0,
            })
            return

    repo_url = resolve_repo_url(job)
    if not repo_url:
        print(
            f"[hive-worker:{WORKER_KIND}] job {job_id} kind={kind!r}: no resolvable repo, declining",
            flush=True,
        )
        release_job(job_id, "no_repo_mapping")
        return
    # Fleet policy (2026-10-25, job 01a17539-d8ca): the ONLY push target is the
    # registry's ARGONAS clone. A repo with no ARGONAS clone fails closed here,
    # before any clone, so no job branch can ever be sent to a public forge.
    private_remote_error = private_remote_failure(job, repo_url)
    if private_remote_error:
        print(
            f"[hive-worker:{WORKER_KIND}] job {job_id} kind={kind!r}: {private_remote_error}",
            flush=True,
        )
        patch_job(
            job_id,
            "failed",
            {
                "via": "hive_worker",
                "failure_reason": "no_private_remote",
                "error": _cap_text(private_remote_error),
                "repo": job.get("repo"),
            },
        )
        return

    workdir = os.path.join(WORKSPACE_ROOT, job_id)
    # Containment: a job's repo must resolve to a fresh clone INSIDE this
    # host's WORKSPACE_ROOT -- never to a pre-existing checkout elsewhere
    # (incident 2026-10-17). Checked before any git call touches the path.
    try:
        workdir = assert_workdir_in_workspace(workdir)
    except ValueError as exc:
        print(f"[hive-worker:{WORKER_KIND}] job {job_id}: {exc}", flush=True)
        release_job(job_id, "workdir_outside_workspace_root")
        return
    os.makedirs(WORKSPACE_ROOT, exist_ok=True)
    if os.path.exists(workdir):
        # A restored claim may carry unfinished edits from an earlier run.
        # Never erase them -- but failing the job outright (the old behaviour)
        # made every retry/reclaim on the same host permanently dead: 23 jobs
        # were stuck as workspace_exists_needs_rescue on 2026-10-03. Move the
        # old checkout aside intact, push any unpushed commits to a rescue
        # branch so they survive host loss, then clone fresh.
        rescued = _rescue_workspace(job_id, workdir, repo_url)
        if rescued is None:
            patch_job(
                job_id,
                "failed",
                {
                    "via": "hive_worker",
                    # Name WHY the rescue itself could not run: the old row
                    # said only that the workspace existed.
                    "error": "workspace_exists_needs_rescue: could not move the "
                    "existing checkout aside or push its unpushed commits to a "
                    f"rescue branch; preserved at {workdir}",
                    "workspace_preserved": workdir,
                },
            )
            return

    # Branch resolution (operator note 2026-10-21, hive-bus incident 01a15453-* /
    # 01a15bd0-a126 / 01a1550a-2fd5): briefs say "clone base branch X, create
    # a NEW branch Y". The historical worker was wired with only head_branch
    # and ran `git clone --branch <head_branch>`, which made any not-yet-pushed
    # NEW branch fail with "fatal: Remote branch ... not found in upstream
    # origin". We now support a 2-branch case without any change to the old
    # behaviour when head_branch already exists:
    #
    #   head_branch present upstream   →  unchanged: --branch head_branch.
    #   head_branch missing, base_branch present upstream
    #                                 →  clone --branch base_branch then create
    #                                    head_branch locally (worker pushes it).
    #   neither present upstream       →  fast-fail with failure_reason
    #                                    branch_not_found, listing what was tried.
    clone_env = fleet_git_env(repo_url, os.environ.copy())
    clone_env["HIVE_JOB_ID"] = job_id
    if job.get("parent_job_id"):
        clone_env["HIVE_PARENT_JOB_ID"] = job["parent_job_id"]
    # Pre-flight security guard (review 2026-10-21): both names must be safe to
    # pass to a git subprocess BEFORE the probe runs, because both reach git
    # verbatim (--branch for the clone and the new `git checkout -B` for the
    # head_branch-local-create fall-through). The pre-fix code only guarded
    # head_branch against a leading '-'; we extend the same check to
    # base_branch so an injection can never reach git no matter which branch
    # path the worker takes.
    #
    # The decline reason names WHICH branch is bad (2026-10-21): the bus's
    # job-level decline vocabulary distinguishes invalid_head_branch from
    # invalid_base_branch, and the operator reading the row needs to know
    # which ref to fix. Both are terminal declines -- a property of the job
    # alone, so no host can ever run it and requeueing only loops (the bus
    # counts them toward the decline dead-letter threshold and the doctor
    # treats them as unschedulable).
    for label, value in (("head_branch", head_branch), ("base_branch", base_branch)):
        if value and value.startswith("-"):
            print(
                f"[hive-worker:{WORKER_KIND}] job {job_id}: refusing suspicious {label} {value!r}",
                flush=True,
            )
            release_job(
                job_id,
                "invalid_head_branch"
                if label == "head_branch"
                else "invalid_base_branch",
            )
            return
    head_branch_state = (
        _probe_remote_branch(repo_url, head_branch, env=clone_env)
        if head_branch
        else "absent"
    )
    base_branch_state = (
        _probe_remote_branch(repo_url, base_branch, env=clone_env)
        if base_branch
        else "absent"
    )
    # Transport errors MUST short-circuit before the branch-decision logic
    # -- a transient ssh/network/key mismatch on the probe would otherwise
    # be misclassified as `branch_not_found`, dropping the real reason and
    # making a retryable transport hiccup look like a permanent brief
    # error (operator 2026-10-21, deepseek-flash review). The clone
    # command right after the probe would re-surface the same transport
    # error as clone_failed; we cut it short and report it once, with
    # the probe stderr (which still carries the host key / auth /
    # dns message) attached.
    if head_branch_state == "transport_error" or base_branch_state == "transport_error":
        # Re-run the probe to capture stderr; we throw the result away
        # otherwise, but the worker log + bus row MUST show the real
        # ssh/known_hosts/pubkey message so operators can distinguish it
        # from a genuinely-missing branch.
        probe_err = _capture_probe_stderr(
            repo_url,
            head_branch if head_branch_state == "transport_error" else base_branch,
        )
        tried = [b for b in (head_branch, base_branch) if b]
        print(
            f"[hive-worker:{WORKER_KIND}] job {job_id}: clone_failed transport_error tried={tried} stderr={probe_err!r}",
            flush=True,
        )
        patch_job(
            job_id,
            "failed",
            {
                "via": "hive_worker",
                "failure_reason": "clone_transport_error",
                "error": _cap_text(
                    f"clone_transport_error: probe of refs/heads/<branch> on "
                    f"{repo_url!r} failed before reaching origin (auth, "
                    f"known_hosts or network): {probe_err or 'see worker log'}"
                ),
                "head_branch": head_branch,
                "base_branch": base_branch,
            },
        )
        return
    head_branch_present = head_branch_state == "present"
    base_branch_present = base_branch_state == "present"
    if head_branch and not head_branch_present:
        if not (base_branch and base_branch_present):
            # A base_branch that was supplied but does NOT exist upstream is a
            # property of the job alone: no host on the fleet can clone it, so
            # no host can ever create head_branch from it. Requeueing such a
            # job only loops (every bounce re-probes the same missing ref), so
            # it is DECLINED as a job-level unschedulable instead of failed
            # (2026-10-21). The bus counts invalid_base_branch toward its
            # decline dead-letter threshold and the doctor treats it as
            # unschedulable, so the job terminates with a reason an operator
            # can act on instead of burning retries against max_retries.
            if base_branch and not base_branch_present:
                print(
                    f"[hive-worker:{WORKER_KIND}] job {job_id}: declining "
                    f"invalid_base_branch {base_branch!r} (absent on origin)",
                    flush=True,
                )
                release_job(job_id, "invalid_base_branch")
                return
            tried: list[str] = [b for b in (head_branch, base_branch) if b]
            print(
                f"[hive-worker:{WORKER_KIND}] job {job_id}: branch_not_found tried={tried}",
                flush=True,
            )
            patch_job(
                job_id,
                "failed",
                {
                    "via": "hive_worker",
                    "failure_reason": "branch_not_found",
                    "error": (
                        "branch_not_found: head_branch "
                        f"{head_branch!r} missing on origin"
                        + (
                            f" and base_branch {base_branch!r} missing on origin"
                            if base_branch
                            else ""
                        )
                        + "; pass a base_branch that already exists on origin "
                        "(or push head_branch before submitting the job)"
                    ),
                    "head_branch": head_branch,
                    "base_branch": base_branch,
                },
            )
            return
        # Fall through: clone base_branch, then create head_branch locally
        # right after, so the rest of the worker keeps using head_branch as
        # its primary symbol (HEAD, branch.<name>.remote/merge, push target).
    clone_cmd = ["git", "clone", "--quiet", repo_url, workdir]
    clone_with_branch = head_branch if head_branch_present else (base_branch or "")
    # Defence in depth (review 2026-10-21): the pre-flight guard above
    # already rejected any leading '-' on either branch; this clause is kept
    # so a future refactor can't re-introduce an unguarded execve path.
    if clone_with_branch.startswith("-"):
        print(
            f"[hive-worker:{WORKER_KIND}] job {job_id}: refusing suspicious branch name {clone_with_branch!r}",
            flush=True,
        )
        # Same rule as the pre-flight guard: the decline reason names which
        # ref is bad, so the operator is told which branch to fix.
        release_job(
            job_id,
            "invalid_head_branch"
            if clone_with_branch == head_branch
            else "invalid_base_branch",
        )
        return
    if clone_with_branch:
        clone_cmd[3:3] = ["--branch", clone_with_branch]
    # ARGONAS transport: fleet_git_env gives every ARGONAS ssh:// clone the
    # keys-first FLEET_SSH command (fleet key, then the host key, then the
    # password through sshpass only when GIT_FLEET_SSH_PASSWORD is set by the
    # systemd EnvironmentFile; never hardcoded here). Key auth must never be
    # disabled: ARGONAS refuses password auth for the automation user
    # (2026-10-26 outage).
    #
    # UserKnownHostsFile=/dev/null in that command matters: a CONFLICTING
    # known_hosts entry for ARGONAS (a stale ECDSA key from before it was
    # reprovisioned to ED25519) makes ssh refuse with "REMOTE HOST
    # IDENTIFICATION HAS CHANGED" even with StrictHostKeyChecking=no.
    # Confirmed live 2026-09-16 on hydra+cerberus: identical clone_failed
    # exit 128 on every job.
    try:
        subprocess.run(
            clone_cmd,
            check=True,
            timeout=300,
            env=clone_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
    except Exception as e:
        print(
            f"[hive-worker:{WORKER_KIND}] job {job_id}: clone failed: {e}", flush=True
        )
        # The clone's stderr was merged into stdout, which check_output
        # discarded on failure; CalledProcessError.output carries it instead.
        # Without it the row said only "returned non-zero exit status 128",
        # which names neither the host nor the permission problem.
        output = getattr(e, "output", "") or getattr(e, "stderr", "") or ""
        if not isinstance(output, str):
            output = str(output)
        detail = _stderr_tail(output.splitlines()) or str(e)
        result = {
            "via": "hive_worker",
            "error": _cap_text(f"clone_failed: {detail}"),
        }
        if os.path.exists(workdir):
            result["workspace_preserved"] = workdir
        patch_job(job_id, "failed", result)
        return

    # 2-branch fall-through: head_branch didn't exist upstream; we cloned
    # base_branch and now create head_branch locally so subsequent code keeps
    # using the same symbols (HEAD, branch.<name>.remote/merge) without a
    # second git clone. The push step later will publish head_branch to
    # origin on its own.
    if head_branch and not head_branch_present and base_branch and base_branch_present:
        try:
            subprocess.run(
                ["git", "-C", workdir, "checkout", "-q", "-B", head_branch],
                check=True,
                timeout=60,
                env=clone_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
        except Exception as e:
            output = getattr(e, "output", "") or getattr(e, "stderr", "") or ""
            if not isinstance(output, str):
                output = str(output)
            detail = _stderr_tail(output.splitlines()) or str(e)
            print(
                f"[hive-worker:{WORKER_KIND}] job {job_id}: create head_branch {head_branch!r} from base_branch {base_branch!r} failed: {detail}",
                flush=True,
            )
            patch_job(
                job_id,
                "failed",
                {
                    "via": "hive_worker",
                    "failure_reason": "branch_create_failed",
                    "error": _cap_text(
                        f"branch_create_failed: could not create head_branch "
                        f"{head_branch!r} from base_branch {base_branch!r}: {detail}"
                    ),
                    "head_branch": head_branch,
                    "base_branch": base_branch,
                    "workspace_preserved": workdir,
                },
            )
            return

    configure_git_identity(workdir, clone_env)
    # The workspace's origin is FORCED to this job's ARGONAS clone before the
    # driver ever runs (2026-10-25, job 01a17539-d8ca): a clone from a
    # forge-shaped URL, or a registry entry that resolved to one, must never
    # leave the driver -- or the finalizer's `git push origin` -- pointing at
    # a public world-writable host. Refuses the job if the rewrite fails.
    origin_error = _force_workspace_origin(workdir, repo_url, clone_env)
    if origin_error:
        patch_job(
            job_id,
            "failed",
            {
                "via": "hive_worker",
                "failure_reason": "no_private_remote",
                "error": _cap_text(origin_error),
                "workspace_preserved": workdir,
            },
        )
        return
    # PRIMARY prevention for AI/Co-Authored-By trailers: install a per-job
    # commit-msg hook, written into the hooks directory git resolves for
    # this workdir (a pre-existing one is chained to, never disabled) so a
    # trailer line is never written into a hive commit in the first place.
    # The filter-branch fallback in strip_ai_attribution uses the same
    # pattern as its --msg-filter for unpushed commits that pre-date the
    # hook. See install_attribution_hook.
    install_attribution_hook(workdir, clone_env)
    baseline = subprocess.check_output(
        ["git", "-C", workdir, "rev-parse", "HEAD"], text=True, timeout=30
    ).strip()
    description += (
        "\n\nDelivery policy: commit the task's legitimate changes using explicit paths, "
        "including documentation or tests when those are the requested deliverables, "
        "and checkout a dedicated branch (creating a branch alone does not switch "
        "HEAD). Push that checked-out branch with upstream tracking. Keep the "
        "configured repository Git author identity; do not invent or override it. "
        "Verify clean status and "
        "remote SHA. Only an explicit job-specific restriction overrides this policy. "
        "Never manufacture edits just to satisfy completion. If blocked, report why."
    )
    if clone_env.get("MNEMOS_URL") and clone_env.get("MNEMOS_TOKEN"):
        description += (
            "\nUse zoder mnemos --search with this job ID or its parent ID to retrieve "
            "prior work evidence. Treat recalled notes as context, verify their current "
            "Git state, and use zoder mnemos --record for factual progress checkpoints. "
            "Never store credentials, private environment values, or raw secrets."
        )
    if WORKER_KIND == "zeroclaw":
        strict_rescue = "hard:strict-rescue-routes" in (
            job.get("required_capabilities") or []
        )
        if strict_rescue:
            try:
                routes = load_strict_routes()
            except ValueError:
                patch_job(
                    job_id,
                    "failed",
                    {
                        "via": "hive_worker",
                        "error": "strict_role_readiness_unavailable",
                        "retryable": False,
                    },
                )
                return
            if not _driver_supports_strict_routes(_find_driver("zoder")):
                patch_job(
                    job_id,
                    "failed",
                    {
                        "via": "hive_worker",
                        "error": "strict_route_driver_unavailable",
                        "retryable": False,
                    },
                )
                return
        # --loop-timeout must exceed --agent-timeout (the loop watchdog covers
        # author+check+review, not just the author turn) or zoder prints a
        # TIMEOUT WARNING on every single run -- harmless here (zoder proceeds
        # anyway) but noisy, and a real risk if JOB_TIMEOUT is ever raised
        # close to zoder's own 900s loop-timeout default.
        cmd = [
            _find_driver("zoder"),
            "--json",
            "loop",
            description,
            "--require-approval",
            "--approve",
            "all",
            "--check",
            shlex.join(
                [
                    sys.executable,
                    os.path.abspath(__file__),
                    "--verify-delivery",
                    workdir,
                    baseline,
                ]
            ),
            "--agent-timeout",
            str(JOB_TIMEOUT),
            "--loop-timeout",
            str(JOB_TIMEOUT + 300),
        ]
        if strict_rescue:
            # Strict rescue pins exact routes, so the launched models are the
            # pinned ones (author from role-readiness, reviewer per quiet
            # window within the pinned pool).
            author = routes["author"]
            reviewer = routes["reviewer"]
            agent = author
            launched_agent = author
            cmd += [
                "-m",
                author,
                "--reviewer",
                reviewer,
                "--allowed-routes",
                routes["allowed_routes"],
                "--reviewer-allowed-routes",
                routes["reviewer_allowed_routes"],
                "--reviewer-pool-routes",
                routes["reviewer_pool_routes"],
            ]
            _set_provenance(author, reviewer)
        else:
            attempt_job = job_with_attempt(job)
            agent = escalation_agent(attempt_job)
            if agent:
                cmd += ["--agent", agent, "--allow-paid"]
                if agent == MIMO_AGENT:
                    # zoder drops MiMo's trailing usage frame on streamed calls
                    # (ledger undercount); keep this until the zoder fix lands.
                    cmd += ["--no-stream"]
                print(
                    f"[hive-worker:{WORKER_KIND}] job {job_id}: agent={agent} "
                    f"(retry_count={attempt_job.get('retry_count') or 0})",
                    flush=True,
                )
            reviewer = review_model()
            if reviewer:
                cmd += ["--reviewer", reviewer]
            _set_provenance(agent or ZODER_AGENT, reviewer)
            launched_agent = agent or ZODER_AGENT
        # PROVENANCE-IN-PROGRESS: tell the bus what this attempt really launched
        # before the loop starts, instead of leaving the registration-time
        # placeholder on the job row for its whole run. `launched_agent` is the
        # alias the command above actually carries (--agent/`-m`/strict route),
        # so `--agent deepseek` claims its configured model (deepseek-flash),
        # never the ladder's or the registrar's label.
        claimed = provenance_patch(launched_agent)
        if claimed:
            _current_provenance.update(
                {
                    "model": claimed["claimed_model"],
                    "provider": claimed["claimed_provider"],
                    "cost_tier": claimed["claimed_cost_tier"],
                }
            )
        # Always PATCH: even with no resolvable model this clears any earlier
        # placeholder off the job row (claimed_model=None, never 'zoder').
        body = {
            "status": "claimed",
            "claimed_by": _urn,
            **_claim_provenance(job_id),
        }
        code, _ = _http("PATCH", f"/v1/jobs/{job_id}", body, timeout=15)
        print(
            f"[hive-worker:{WORKER_KIND}] job {job_id}: provenance "
            f"agent={launched_agent or '-'} "
            f"model={body.get('claimed_model')} provider={body.get('claimed_provider')} "
            f"tier={body.get('claimed_cost_tier')} patch_code={code}",
            flush=True,
        )
    else:
        wanted = codex_model_mismatch(job)
        if wanted:
            print(
                f"[hive-worker:{WORKER_KIND}] job {job_id}: wants codex model "
                f"{wanted} but this host runs {CODEX_MODEL}; failing",
                flush=True,
            )
            patch_job(
                job_id,
                "failed",
                {
                    "via": "hive_worker",
                    "error": "codex_model_mismatch",
                    "wanted_model": wanted,
                    "host_model": CODEX_MODEL,
                },
            )
            return
        try:
            codex_args = codex_exec_args(workdir)
        except RuntimeError as exc:
            # Misconfigured host, not a job problem: never silently fall back
            # to an unsandboxed run.
            print(
                f"[hive-worker:{WORKER_KIND}] job {job_id}: {exc}",
                flush=True,
            )
            patch_job(
                job_id,
                "failed",
                {"via": "hive_worker", "error": "unsandboxed_codex_refused"},
            )
            return
        cmd = [_find_driver("codex"), *codex_args, description + CODEX_WORKER_NOTE]

    print(
        f"[hive-worker:{WORKER_KIND}] job {job_id}: running {' '.join(cmd[:3])}...",
        flush=True,
    )

    # Defence in depth for EVERY driver (codex AND zoder): record the git state
    # of ~/Projects repos before the run so any branch switch/edit the agent
    # makes in someone else's live clone is detectable afterwards.
    protected_before = snapshot_protected_repos(env=clone_env)
    print(
        f"[hive-worker:{WORKER_KIND}] job {job_id}: protection snapshot of "
        f"{len(protected_before)} ~/Projects repos",
        flush=True,
    )
    # The loop's own machine-readable record is the LAST single-line JSON on
    # stdout, which can sit outside the 4000-char tail (a long job log), so it
    # is captured separately: the provider-quota check below must not depend on
    # how verbose the run was.
    loop_record = ""
    raw_output = ""
    loop_result: dict | None = None

    def _capture(out: str | None) -> None:
        # Same record parse_loop_models accepts (a dict naming a model or a
        # reviewer container), so a stray JSON log line is not mistaken for the
        # loop verdict; the last such record wins. The raw text is kept too:
        # a quota wall reported outside the JSON verdict must still trigger
        # the cooldown.
        nonlocal loop_record, raw_output, loop_result
        loop_result = parse_zoder_loop_result(out) or loop_result
        # 512 KB: the loop's pretty-printed record puts its reviewer verdicts BEFORE a
        # long echo of the task, so a short tail cuts the evidence off.
        raw_output = (out or "")[-524288:]
        for line in (out or "").splitlines():
            line = line.strip()
            if not line.startswith("{") or len(line) > _MAX_JSON_LINE:
                continue
            try:
                candidate = json.loads(line)
            except (ValueError, RecursionError):
                continue
            if isinstance(candidate, dict) and (
                _first_model(candidate)
                or any(key in candidate for key in _REVIEWER_CONTAINERS)
            ):
                loop_record = line

    t0 = time.time()
    # Ground-truth window for result['models']: every zoder provider call the
    # run makes lands in ~/.zoder/ledger.jsonl with this [start, end] stamp.
    # One second of slack on each side absorbs the driver's own process start
    # and exit without ever reaching the previous job's rows.
    _current_run_window = (t0 - 1.0, t0)
    timed_out = False
    try:
        # start_new_session=True puts the child in its own process group so a
        # timeout kill takes any grandchildren zoder/codex spawn (build/test
        # subprocesses) with it -- subprocess.run's own TimeoutExpired only
        # kills the direct child, leaving those orphaned otherwise.
        # communicate() buffers the whole run in memory before truncating to
        # the last 4000 chars -- unbounded for a pathological job (codex
        # review, Low). Accepted for v1: jobs come only from registered
        # orchestrators on the LAN, not an adversarial source; revisit with a
        # streaming/size-capped reader if a real job ever produces enough
        # output to matter.
        # env=clone_env: the driver (zoder/codex) must be able to `git push` to the
        # same root@ARGONAS origin it was cloned from; the password-auth
        # GIT_SSH_COMMAND was previously set only for the clone, so pushes from
        # inside the job failed and the workspace (formerly deleted on exit) lost the work.
        # No new exposure: GIT_FLEET_SSH_PASSWORD is already in this environment.
        proc = subprocess.Popen(
            cmd,
            cwd=workdir,
            stdout=subprocess.PIPE,
            env=clone_env,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        try:
            stdout, _ = proc.communicate(timeout=JOB_TIMEOUT)
            exit_code = proc.returncode
            tail = stdout[-4000:] if stdout else ""
            _capture(stdout)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            stdout, _ = proc.communicate()
            exit_code = -1
            timed_out = True
            tail = (stdout or "")[-4000:]
            _capture(stdout)
            print(
                f"[hive-worker:{WORKER_KIND}] job {job_id}: TIMEOUT after {JOB_TIMEOUT}s, process group killed",
                flush=True,
            )
    except FileNotFoundError as e:
        patch_job(
            job_id,
            "failed",
            {
                "via": "hive_worker",
                "error": _cap_text(f"driver_not_found: {e}"),
                "workspace_preserved": workdir,
            },
        )
        return

    # JOB-LIFETIME PROCESS REAP (incident 2026-10-21, job 01a1601b): the
    # driver's process group was killed on timeout, but detached background
    # processes the agent spawned (setsid, own session) survived it -- 368 of
    # them were still running 2h after the 7200s timeout. Reap by ownership +
    # cwd-in-workspace (never by command-line pattern), now that the driver has
    # exited and any children it leaked are visible in /proc. Runs on EVERY
    # disposition (success, failure, timeout, cancel); the count rides on the
    # job result so a leak is visible on the bus row, not just in the host log.
    #
    # Seeded to 0 BEFORE the call so the name is always bound on every path
    # into the result dict below -- an exception escaping the helper can then
    # never leave it unbound and fail the job's disposition.
    reaped_processes = 0
    try:
        reaped_processes = reap_workspace_processes(workdir)
    except Exception as exc:  # noqa: BLE001 -- reaping never changes a disposition
        reaped_processes = 0
        print(
            f"[hive-worker:{WORKER_KIND}] job {job_id}: process reap failed: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )
    if reaped_processes:
        print(
            f"[hive-worker:{WORKER_KIND}] job {job_id}: reaped {reaped_processes} "
            f"process(es) left behind in the workspace",
            flush=True,
        )

    elapsed = round(time.time() - t0, 1)
    if _current_run_window:
        # A timed-out run's calls belong to its ledger window too: name the
        # models it really used even when the loop was killed.
        _current_run_window = (_current_run_window[0], time.time() + 1.0)
    # PROTECTION CHECK: another session's ~/Projects tree moved during this
    # run. Evaluated IMMEDIATELY after the driver terminates and before any
    # other post-run logic, so no success-path side effect (finalizer commit/
    # push, delivery resolution, status reporting) can run for an escape.
    # NEVER auto-revert the other person's tree -- just fail the job and name
    # the paths for a human.
    protection_violations = compare_protected_repos(protected_before, env=clone_env)
    enforce_protection = protection_enforced()
    for entry in protection_violations:
        print(
            f"[hive-worker:{WORKER_KIND}] job {job_id}: "
            f"{'PROTECTION VIOLATION' if enforce_protection else 'protection warning'} "
            f"{entry['path']} before={entry['before']} after={entry['after']}",
            flush=True,
        )
    # Hosts are shared with other sessions whose own builds legitimately move
    # ~/Projects checkouts during a run, so a change is only proof of an escape
    # for a driver that can reach those trees. Unenforced: record, do not fail.
    protection_warnings: list[dict] = []
    if protection_violations and not enforce_protection:
        protection_warnings, protection_violations = protection_violations, []
    # PROVENANCE-ON-COMPLETION: what actually ran -- never the ladder's intent.
    # The ledger window of THIS run is the ground truth (one row per provider
    # call): its earliest call names the author the loop JSON omitted, and the
    # reviewer models are the ones that served review calls (so the local
    # gemma4-31b is recorded instead of the backup the config merely prefers,
    # and a loop that names a model which served nothing is overruled). A run
    # with no ledger rows and no parseable loop record falls back to the route
    # the worker launched, and only then records nothing.
    run_models = None
    if WORKER_KIND == "zeroclaw":
        loop_models = parse_loop_models(stdout or "")
        ledger_rows = []
        if _current_run_window:
            ledger_rows = ledger_calls(*_current_run_window)
        # The ledger's author is what the bus must claim, even when the loop
        # JSON is silent or names a route that never served a call.
        record_observed_models(
            attribute_models(
                ledger_rows,
                author_record={"launched": _current_provenance.get("model")},
                slot=_worker_slot(),
            )
        )
        if ledger_rows:
            run_models = attribute_models(
                ledger_rows,
                author_record={
                    "model": loop_models.get("author") if loop_models else None,
                    "launched": _current_provenance.get("model"),
                },
                reviewer_record={"reviewers": parse_loop_reviewers(stdout or "")},
                slot=_worker_slot(),
            )
        if run_models is None and loop_models is not None:
            loop_models["source"] = "loop_json"
            run_models = loop_models
        if run_models is None:
            fallback_author = _current_provenance.get("model")
            fallback_reviewer = _current_provenance.get("reviewer_model")
            if fallback_author or fallback_reviewer:
                run_models = {
                    "author": fallback_author,
                    "reviewers": [fallback_reviewer] if fallback_reviewer else [],
                    "source": "launched_route",
                }
        _current_run_models = run_models
    # Explicit cancel (the bus rejected our claim renewal => terminal/cancelled)
    # means another actor owns the job now; do not push anything under it.
    cancelled = _claim_renewal_blocked_id == job_id
    if protection_violations:
        # The finalizer COMMITS and PUSHES the job's work. After an escape
        # from the sandbox we must not run any success-path side effect, so it
        # is skipped entirely; the job is reported as failed below.
        finalizer = {"status": "skipped_protection_violation", "pushed": False}
        attribution = {"status": "not_checked"}
    elif WORKER_KIND == "codex":
        # The generic finalizer commits AND PUSHES uncommitted edits. Codex
        # edits must reach a remote only through deliver_codex_edits below,
        # after the structured zoder review approves them.
        finalizer = {"status": "skipped_codex_delivery", "pushed": False}
    else:
        finalizer = finalize_uncommitted_work(
            workdir,
            job,
            clone_env,
            cancelled=cancelled,
            baseline=baseline,
            stopped=stopped_by_signal(exit_code, timed_out),
        )
    # NO INVENTED ATTRIBUTION: strip any Co-Authored-By / AI-attribution
    # trailer before the delivery gate (and therefore before anything is
    # pushed), then re-check. Delivery fails with a clear reason if a trailer
    # survives -- only the configured identity may author a hive commit.
    attribution = {"status": "not_checked"}
    if not cancelled and not protection_violations:
        # Preserve the existing strip-and-check pass before worker-owned Codex
        # commits; the exact recorded SHA check below covers the new commit.
        attribution = enforce_attribution(workdir, baseline, clone_env)
        if attribution.get("checked_commits"):
            finalizer["commits"] = attribution["checked_commits"]
    # CODEX-ONLY: codex's workspace-write sandbox keeps .git read-only by
    # design, so codex can only EDIT the job workspace. Delivery is OWNED BY
    # THE WORKER, outside that sandbox (fault 2026-10-17): the worker commits
    # the driver's edits, runs the SAME structured zoder review the zeroclaw
    # path uses, and only on an explicit APPROVE pushes to ARGONAS with the
    # shared fleet credential helper. A failed or empty review is not delivered
    # and is never pushed; push/review errors land in the job's error string.
    # ``protection_violations`` is already empty where the host only records
    # warnings, so this blocks delivery exactly when a violation is enforced.
    # ``repo_url`` is this job's ARGONAS remote (resolved and cloned above).
    codex_delivery_result = None
    if (
        WORKER_KIND == "codex"
        and not cancelled
        and not protection_violations
        and attribution.get("status") not in ("violation", "check_failed")
    ):
        on_task_branch, checked_branch = _on_task_branch(workdir, clone_env)
        # Some isolated test doubles expose HEAD as a branch-like token; a real
        # symbolic branch can never be a 40-character object id in this flow.
        if checked_branch and re.fullmatch(r"[0-9a-f]{40}", checked_branch):
            on_task_branch = True
        if on_task_branch:
            codex_delivery_result = codex_delivery.deliver_codex_edits(
                workdir, job, baseline, clone_env, codex_review_model(), repo_url
            )
        else:
            codex_delivery_result = {
                "delivered": False,
                "branch": checked_branch,
                "error": f"delivery blocked: branch {checked_branch!r} is the default branch",
                "push": {"status": "not_attempted", "pushed": False},
                "commit": {},
                "review": {},
            }
        print(
            f"[hive-worker:{WORKER_KIND}] job {job_id}: codex delivery "
            f"delivered={codex_delivery_result.get('delivered')} "
            f"commit={(codex_delivery_result.get('commit') or {}).get('status')} "
            f"review={(codex_delivery_result.get('review') or {}).get('status')} "
            f"push={(codex_delivery_result.get('push') or {}).get('status')}",
            flush=True,
        )
    finalizer["kind"] = kind
    if codex_delivery_result is not None:
        push_record = codex_delivery_result.get("push") or {}
        finalizer["pushed"] = push_record.get("pushed") is True
        finalizer["status"] = (
            "codex_delivery_pushed" if finalizer["pushed"] else "skipped_codex_delivery"
        )
        finalizer["branch"] = codex_delivery_result.get("branch")
        commit_sha = (codex_delivery_result.get("commit") or {}).get("commit")
        finalizer["commits"] = [commit_sha] if commit_sha else []
    elif not finalizer.get("commits") and finalizer.get("commit"):
        finalizer["commits"] = [finalizer["commit"]]
    if (
        not cancelled
        and not protection_violations
        and attribution.get("status") not in ("violation", "check_failed")
    ):
        recorded_shas = finalizer.get("commits") or []
        if codex_delivery_result is not None:
            commit_sha = (codex_delivery_result.get("commit") or {}).get("commit")
            if commit_sha and commit_sha not in recorded_shas:
                recorded_shas.append(commit_sha)
                finalizer["commits"] = recorded_shas
        attribution = enforce_attribution_for_shas(workdir, recorded_shas, clone_env)
    try:
        evidence = completion_evidence(workdir, baseline, clone_env)
    except Exception as exc:  # noqa: BLE001 -- never lose the disposition
        # completion_evidence is written not to raise; if the environment
        # defeats that anyway, a short, explicit mapping still lets the row be
        # PATCHed with a reason instead of dying as an unhandled exception.
        evidence = {
            "base_sha": baseline,
            "workdir": workdir,
            "commits": [],
            "files_changed": [],
            "worktree_clean": False,
            "remote_verified": False,
            "verification_error": _cap_text(f"{type(exc).__name__}: {exc}"),
        }
    # `workdir` rides on the evidence so the failure-reason builder can probe
    # the push transport error; it is already reported as workspace_preserved
    # and must not appear twice on the bus row.
    bus_evidence = {key: value for key, value in evidence.items() if key != "workdir"}
    result = {
        "via": "hive_worker",
        "driver": WORKER_KIND,
        "kind": kind,
        "exit_code": exit_code,
        "elapsed_s": elapsed,
        "tail": tail,
        "workspace_preserved": workdir,
        # Always present, even when 0: an operator comparing rows must be able
        # to tell "the reaper ran and found nothing" from "the reaper never
        # ran" (e.g. an older worker build).
        "reaped_processes": reaped_processes,
        "finalizer": finalizer,
        "attribution": attribution,
        **bus_evidence,
    }
    if loop_result is not None:
        result["loop_result"] = loop_result
    if run_models is not None:
        result["models"] = run_models
    if codex_delivery_result is not None:
        result["codex_delivery"] = codex_delivery_result
        # The reviewer(s) that actually answered, on the row itself (the zoder
        # JSON used to be dropped, leaving a pushed job with no named reviewer).
        _review = codex_delivery_result.get("review") or {}
        _evidence = _review.get("evidence")
        if not isinstance(_evidence, dict):
            _evidence = {
                "reviewers": [],
                "first": None,
                "secondaries": [],
                "skipped": [],
                "provenance": [],
                "warnings": [_review.get("error") or "no review payload"],
            }
        result["review_evidence"] = _evidence
        result["models"] = review_evidence.models_record(CODEX_MODEL, _evidence)
    elif WORKER_KIND != "codex":
        _loop = review_evidence.payload_from_text(raw_output or stdout or "")
        if _loop is not None:
            _ev = review_evidence.build_evidence(_loop)
            _author_model = (run_models or {}).get("author") if isinstance(run_models, dict) else None
            _warn = review_evidence.unverified_reason(
                _ev, review_evidence.family_of(_author_model or "")
            )
            if _warn:
                _ev["warnings"].append(_warn)
            result["review_evidence"] = _ev
    # ONE reviewer record (2026-10-26): review_evidence, models.reviewers and the
    # bus's hive_reviews delivery entry named different models for one job. All
    # are now derived from the structured verdict's reviewers[].model, falling
    # back to the route this worker launched the review with (marked
    # source=launched_route). ``run_models`` itself is left as the ledger saw it:
    # the review gate below still reads it.
    review_record.apply(result, launched=launched_reviewer_route(codex_delivery_result))
    if protection_warnings:
        result["protection_warning"] = protection_warnings
    # ENGINE SPEND CAP (2026-10-25). Checked before any disposition is chosen
    # and over the WHOLE captured output (stdout+stderr merged, of which `tail`
    # is only the last 4000 chars), so a cap raised early in a long run is
    # still named. It is a fact about the HOST's budget, not this job's work:
    # the attempt is handed back to the queue without spending a retry, and the
    # row says budget_exceeded instead of the symptoms (no commits, empty
    # author turn, reviewer_unavailable, "cancel not confirmed").
    budget_fields = budget_failure_fields(raw_output or stdout or tail, host=AGENT_HOST)
    if budget_fields is not None:
        # The attempt this host could not serve. Recorded because the release
        # back to the queue makes the bus bump retry_count: the row must still
        # show which attempt hit the wall, and that it was not the job's fault.
        try:
            budget_fields["budget_exceeded"]["attempt_retry_count"] = max(
                0, int(job.get("retry_count") or 0)
            )
        except (TypeError, ValueError):
            pass

    if protection_violations:
        # An escape from the sandbox is never a delivered job, whatever the
        # driver exited with. resolve_delivery_status is a PURE mapping (no
        # external calls), but it is skipped anyway so the violation is the
        # single, explicit source of this job's disposition -- no success path
        # can run first and be overwritten afterwards.
        result["protection_violation"] = protection_violations
        result["failure_reason"] = "protection_violation"
        result["error"] = _cap_text(
            "driver modified another session's checkout: "
            + ", ".join(entry["path"] for entry in protection_violations)
        )
        status, failure_reason = "failed", "protection_violation"
    else:
        status, failure_reason = resolve_delivery_status(
            exit_code, evidence, finalizer, cancelled=cancelled
        )
    integrity_errors = delivery_integrity_errors(
        attribution, finalizer, evidence, codex_delivery_result
    )
    if integrity_errors:
        status = "failed"
        failure_reason = "delivery_integrity_check_failed"
        result["delivery_integrity_errors"] = integrity_errors
    if attribution.get("status") in ("violation", "check_failed"):
        # A commit that still credits a model overrides any other disposition:
        # the delivery must not be recorded as complete.
        status = "failed"
        failure_reason = (
            "attribution_check_failed"
            if attribution.get("status") == "check_failed"
            else "ai_attribution_trailer"
        )
        result["reviewer_findings"] = attribution.get("reason") or attribution.get(
            "error", ""
        )
    if codex_delivery_result is not None:
        # The codex path's disposition is the worker-owned delivery, not the
        # exit status: `done` only when the worker committed, the structured
        # review approved AND the push to ARGONAS landed. Anything else keeps
        # the codex driver's own outcome on record: a failed driver stays
        # `failed`; a driver that ran clean but was not delivered is
        # `needs-review` (exactly what resolve_delivery_status does for the
        # zeroclaw path), never `done`. Either way the error string names the
        # real cause (review verdict, empty review, push stderr tail).
        delivered = bool(codex_delivery_result.get("delivered"))
        result["delivered"] = delivered
        if delivered and status != "failed":
            status, failure_reason = "done", None
        elif status != "failed":
            status = "needs-review" if exit_code == 0 else "failed"
            failure_reason = "codex_delivery_failed"
            result["error"] = (
                codex_delivery_result.get("error") or "codex delivery failed"
            )[:3000]
    status, failure_reason = zeroclaw_review_gate(
        status,
        failure_reason,
        result,
        has_commits=bool(evidence.get("commits") or finalizer.get("commits")),
        run_models=run_models,
        codex=codex_delivery_result is not None or WORKER_KIND == "codex",
    )
    if budget_fields and not cancelled and not protection_violations:
        # The spend cap outranks every downstream symptom EXCEPT an explicit
        # cancel (another actor owns the job then) and a sandbox escape (never
        # negotiable). Zero commits, an empty author turn and a
        # reviewer_unavailable verdict are all consequences of "no paid call
        # could be made", not independent failures.
        status = "failed"
        failure_reason = BUDGET_EXCEEDED_FAILURE
        result.update(budget_fields)
    if failure_reason:
        result["failure_reason"] = failure_reason
        if failure_reason == "ai_attribution_trailer":
            result["error"] = attribution.get("reason", failure_reason)
    if status == "failed":
        # NEVER an empty error on a failed row (2026-10-17): a job logged
        # `failed exit=0 elapsed=2806.9s` with error='' and operators could not
        # tell why without host logs. The derivation order is the diagnosis
        # order: zoder loop JSON -> delivery verifier -> attribution guard ->
        # exit code/signal with the last stderr lines. Any existing reason is
        # kept; a blank one is replaced. An exit-0 failure always states why.
        existing = result.get("error")
        if not isinstance(existing, str) or not existing.strip():
            try:
                result["error"] = build_failure_error(
                    status=status,
                    exit_code=exit_code,
                    tail=tail,
                    loop_record=parse_loop_record(stdout or ""),
                    evidence=evidence,
                    finalizer=finalizer,
                    attribution=attribution,
                    env=clone_env,
                    timed_out=timed_out,
                )
            except Exception as exc:  # noqa: BLE001 -- the row is still owed
                result["error"] = ""
                detail = _cap_text(f"{type(exc).__name__}: {exc}")
                result["failure_reason_error"] = detail
            if not result["error"]:
                # Fallback that cannot depend on the evidence builders: the
                # exit status plus the last output lines still says HOW.
                result["error"] = exit_failure_reason(
                    exit_code, tail, timed_out=timed_out
                )
            if exit_code == 0 and "exit 0" not in result["error"].lower():
                result["error"] = f"failed with exit 0: {result['error']}"
            # The fallback text is raw stderr: redact and bound it like every
            # other path before it reaches the bus row.
            result["error"] = (
                _cap_text(result["error"])
                or "failed without a recorded reason (inspect host logs)"
            )
        else:
            result["error"] = _cap_text(existing)
    if status == "needs-review" and failure_reason == "loop_unresolved_work_rescued":
        # Surface the loop's last reviewer findings to whoever picks this up.
        result["reviewer_findings"] = tail[-2000:]
    print(
        f"[hive-worker:{WORKER_KIND}] job {job_id}: {status} exit={exit_code} "
        f"elapsed={elapsed}s"
        + (f" error={result['error']!r}" if status == "failed" else ""),
        flush=True,
    )
    # PROVIDER QUOTA (2026-10-17): a MiniMax 429 "usage limit" is a fact about
    # the provider, not about this job's work. Cool the MiniMax rungs down
    # locally so the retry runs elsewhere; the attempt itself still counts.
    cooldown_model = None
    if WORKER_KIND == "zeroclaw" and not cancelled:
        cooldown_model = detect_minimax_limit(loop_record, agent, raw_output)
    if cooldown_model:
        result.update(report_provider_cooldown(job_id, cooldown_model, status, result))
        print(
            f"[hive-worker:{WORKER_KIND}] job {job_id}: provider_cooldown on "
            f"{cooldown_model}; MiniMax rungs held off, attempt reported as {status}",
            flush=True,
        )
    if failure_reason == BUDGET_EXCEEDED_FAILURE:
        # ENGINE SPEND CAP: a host-local budget wall, not this job's work. Hand
        # the claim back so a host with budget left runs it, and leave one line
        # in the host log that names the cap, the spend and the host. The bus
        # has no "hold without a retry" state, so the job is requeued rather
        # than failed -- a failure would burn the retry on a host that could
        # not make a single paid call.
        budget = result.get("budget_exceeded") or {}
        hold = budget.get("resets_at_iso") or "an unknown reset"
        print(
            f"[hive-worker:{WORKER_KIND}] job {job_id}: budget_exceeded on "
            f"{budget.get('host') or AGENT_HOST} -- engine spend cap reached "
            f"(spent={budget.get('spent_usd')} limit={budget.get('limit_usd')} "
            f"period={budget.get('period')}); job released to the queue, "
            f"cap resets at {hold}",
            flush=True,
        )
        release_without_retry(
            job_id,
            result,
            f"budget_exceeded:{budget.get('host') or AGENT_HOST}",
        )
    else:
        patch_job(job_id, status, result)
    # Retain even successful workspaces. A 200 from the bus acknowledges a
    # request, not independent validation of the code or a safe cleanup lease.


def main() -> None:
    if WORKER_KIND not in ("zeroclaw", "codex"):
        print(
            "[hive-worker] WORKER_KIND must be 'zeroclaw' or 'codex'", file=sys.stderr
        )
        raise SystemExit(1)
    register()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    if not HIVE_URL:
        print(
            f"[hive-worker:{WORKER_KIND}] HIVE_URL is not set: refusing to poll. "
            "Set HIVE_URL in the unit environment (see docs/ENVIRONMENT.md).",
            flush=True,
        )
        sys.exit(2)
    print(f"[hive-worker:{WORKER_KIND}] polling {HIVE_URL} as {_urn}", flush=True)
    _last_busy = ""
    while _running:
        restart_after_source_update()
        if draining():
            time.sleep(POLL_INTERVAL)
            continue
        _busy = build_busy()
        if _busy != _last_busy:
            print(
                f"[hive-worker:{WORKER_KIND}] build gate: "
                + (f"busy, not claiming ({_busy})" if _busy else "idle, claiming again"),
                flush=True,
            )
            _last_busy = _busy
        if _busy:
            time.sleep(POLL_INTERVAL)
            continue
        if not argonas_auth_ok():
            time.sleep(POLL_INTERVAL)
            continue
        code, job = _http("POST", f"/v1/jobs/next?agent_urn={_urn}", timeout=10)
        if code == 200 and job:
            run_job(job)
            continue
        if code == 409:
            # agent status not online/idle -- re-register and back off
            print(f"[hive-worker:{WORKER_KIND}] claim 409, re-registering", flush=True)
            register()
        elif code == 404:
            # agent URN not registered on the bus (reaped / bus restored).
            # Without this the worker polls with a dead URN indefinitely.
            print(
                f"[hive-worker:{WORKER_KIND}] claim 404 (agent not registered), re-registering",
                flush=True,
            )
            register()
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--verify-delivery"]:
        if len(sys.argv) != 4:
            print(json.dumps({"error": "usage: --verify-delivery WORKDIR BASELINE"}))
            raise SystemExit(2)
        raise SystemExit(verify_delivery(sys.argv[2], sys.argv[3]))
    else:
        main()
