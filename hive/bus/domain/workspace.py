"""Bus-side workspace resolution (moved verbatim from agent_bus.py, Phase 1c).

GRAEAE permanent logjam-fix (consensus 1.0, 2026-06-04): bus-side first-class
workspace resolution mirroring the worker KIND_WORKSPACE_MAP (prefix -> workspace
name).
"""

from __future__ import annotations

import re

WORKSPACE_CATALOG_PREFIXES = {
    "argonaut:": "florida-licenses",
    "fix:codex-pro-oauth-verify": "mnemos",
    "feat:knemon": "mnemos",
    "fix:knemon": "mnemos",
    "feat:oracle-backend": "mnemos",
    "fix:sync-provider-models": "mnemos",
    "fix:knemon-cost": "mnemos",
    "mnemos:": "mnemos",
    "riskybiz:": "florida-licenses",
    "riskyeats:": "riskyeats",
    "investorclaw:": "ic-engine",
    "ic-engine:": "ic-engine",
    "ncz-os-zeroclaw:": "zeroclaw",
    "ncz-os-openclaw:": "openclaw",
    "ncz-os-": "ncz-installer",
    "cixmini-os:": "cix-installer",
    "fleet-infra:": "fleet-ops",
}
WORKSPACE_NAMES = set(WORKSPACE_CATALOG_PREFIXES.values())
ADMISSION_NONCOMMIT_PREFIXES = (
    "review",
    "architecture",
    "triage",
    "docs:",
    "investigation",
    "track:",
    "ops:",
    "diag:",
    "ping:",
    "hive-stats",
    "dream-walker",
    "research",
    "analysis",
    "design",
)


def resolve_workspace_shadow(kind, description, target_workspace, project):
    """SHADOW admission: can the workspace be resolved at SUBMIT? Returns (ws_or_None, source).
    source in {explicit, project, prefix-fallback, repo-hint, noncommit, UNRESOLVABLE}."""
    k = kind or ""
    if target_workspace:
        return (target_workspace, "explicit")
    if project and project in WORKSPACE_NAMES:
        return (project, "project")
    for prefix in sorted(WORKSPACE_CATALOG_PREFIXES, key=len, reverse=True):
        if k.startswith(prefix):
            return (WORKSPACE_CATALOG_PREFIXES[prefix], "prefix-fallback")
    if re.search(r"\brepo:\s*([A-Za-z0-9_./:\-]+)", description or "", re.IGNORECASE):
        return (None, "repo-hint")
    if any(k.startswith(pp) for pp in ADMISSION_NONCOMMIT_PREFIXES):
        return (None, "noncommit")
    return (None, "UNRESOLVABLE")
