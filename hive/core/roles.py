"""Runtime role sets — single source of truth.

Previously duplicated: ``agent_bus.py`` defined ORCHESTRATOR_RUNTIMES /
WORKER_ONLY_RUNTIMES and ``mcp_bridge.py`` hardcoded its own orchestrator set,
which had drifted (missing ``codex-orchestrator``) — so the bridge sent the
worker token where the bus required the admin tier. Extract both here so the
bus and the bridge cannot disagree.
"""

from __future__ import annotations

# Runtimes that may SUBMIT jobs (orchestrators) and register with the admin tier.
ORCHESTRATOR_RUNTIMES: frozenset[str] = frozenset(
    {
        "claude-code",
        "claude-cli",
        "claude",
        "codex-orchestrator",
        "opencode",  # agentic CLI — submits its own orchestration jobs AND claims
        "opencode-cli",
        "human",
        "mnemos",
        "doctor",  # PYTHIA triage authority — submits codex sub-jobs + auto-triage
        "dream-walker",  # zeropi Dream Walker — dispatches hourly analysis jobs
        "unknown",
    }
)

# Runtimes that may ONLY claim/execute jobs (workers), never submit.
# NOTE: opencode/opencode-cli are deliberately NOT here — opencode is a valid
# orchestrator (operator 2026-10-16): it both submits and claims.
WORKER_ONLY_RUNTIMES: frozenset[str] = frozenset(
    {
        "codex",
        "codex-cli",
        "hermes",
        "zeroclaw",
        "openclaw",
        "ic-engine",
        "system",  # fleet hosts (system-watcher daemons)
    }
)
