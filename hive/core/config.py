"""Typed hive settings.

One place for process configuration, so runtime code imports ``get_settings()``
instead of calling ``os.getenv`` directly. Replaces the ~64 scattered
``os.environ`` reads (agent_bus 16, zeroclaw_doctor 38, mcp_bridge 8, …).

Pattern adapted from MNEMOS (``mnemos/core/config.py``) — see
``docs/analysis-mnemos-reuse-20261015.md``. Kept dependency-light: plain
dataclass + ``from_env`` (no pydantic-settings), because the hive already
depends on pydantic and this needs nothing more.

Defaults are IDENTICAL to the inline ``os.environ.get(...)`` defaults that
preceded this module, so importing it is behavior-preserving. The frozen
dataclass also makes settings immutable within a process.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_csv(name: str) -> frozenset[str]:
    raw = os.environ.get(name, "")
    return frozenset(part.strip() for part in raw.split(",") if part.strip())


@dataclass(frozen=True)
class Settings:
    # ── bus ────────────────────────────────────────────────────────────────
    hive_url: str
    port: int
    # Optional bearer tokens — the fleet is local-LAN only and does not require
    # auth (operator 2026-10-15); these exist for callers that still send one.
    bus_token: str
    bus_admin_token: str
    host_denylist: frozenset[str]
    provider_denylist: frozenset[str]

    # ── persistence ────────────────────────────────────────────────────────
    backend: str  # sqlite | oracle | db2
    agent_bus_db: str
    oracle_dsn: str
    db2_dsn: str
    sqlite_busy_timeout_ms: int

    # ── jobs / scheduling ──────────────────────────────────────────────────
    claim_lease_seconds: int
    claim_scan_window: int
    claim_hard_max_age: int
    retain_done_jobs_days: int
    retain_test_jobs_days: int

    # ── mnemos / mcp bridge ────────────────────────────────────────────────
    mnemos_url: str
    mnemos_token: str
    mcp_bridge_token: str
    mcp_bridge_admin_token: str

    # ── repo registry ──────────────────────────────────────────────────────
    repo_catalog: str
    gitlab_pat: str
    github_pat: str
    buildkite_api_token: str
    r2_account_id: str
    cloudflare_r2_api_token: str
    argonas_ssh_pass: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            hive_url=_env("HIVE_URL", "http://127.0.0.1:5005").rstrip("/"),
            port=_env_int("PORT", 5005),
            bus_token=_env("HIVE_BUS_TOKEN", ""),
            bus_admin_token=_env("HIVE_BUS_ADMIN_TOKEN", ""),
            host_denylist=_env_csv("HIVE_HOST_DENYLIST"),
            provider_denylist=_env_csv("HIVE_PROVIDER_DENYLIST"),
            backend=_env("HIVE_BACKEND", "sqlite").lower(),
            agent_bus_db=_env("AGENT_BUS_DB", "/srv/agent-bus/agents.db"),
            oracle_dsn=_env("ORACLE_DSN", ""),
            db2_dsn=_env("DB2_DSN", "") or _env("HIVE_DB2_DSN", ""),
            sqlite_busy_timeout_ms=_env_int("AGENT_BUS_SQLITE_BUSY_TIMEOUT_MS", 5000),
            claim_lease_seconds=_env_int("CLAIM_LEASE_SECONDS", 1800),
            claim_scan_window=_env_int("CLAIM_SCAN_WINDOW", 10000),
            claim_hard_max_age=_env_int("HIVE_CLAIM_HARD_MAX_AGE", 14400),
            retain_done_jobs_days=_env_int("RETAIN_DONE_JOBS_DAYS", 30),
            retain_test_jobs_days=_env_int("RETAIN_TEST_JOBS_DAYS", 3),
            mnemos_url=_env("MNEMOS_URL", "http://192.0.2.67:5002").rstrip("/"),
            mnemos_token=_env("MNEMOS_TOKEN", ""),
            mcp_bridge_token=_env("MCP_BRIDGE_TOKEN", ""),
            mcp_bridge_admin_token=_env("MCP_BRIDGE_ADMIN_TOKEN", ""),
            repo_catalog=_env("HIVE_REPO_CATALOG", ""),
            gitlab_pat=_env("GITLAB_PAT", ""),
            github_pat=_env("GITHUB_PAT", ""),
            buildkite_api_token=_env("BUILDKITE_API_TOKEN", ""),
            r2_account_id=_env("R2_ACCOUNT_ID", ""),
            cloudflare_r2_api_token=_env("CLOUDFLARE_R2_API_TOKEN", ""),
            argonas_ssh_pass=_env("ARGONAS_SSH_PASS", ""),
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process settings, read once and cached.

    Tests that mutate the environment should call ``get_settings.cache_clear()``.
    """
    return Settings.from_env()
