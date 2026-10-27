"""Conservative infrastructure classification; preserve protection/routing holds."""

from __future__ import annotations

import json

from retry_policy import _is_blocked_result


def engine_unavailable(metadata) -> bool:
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (ValueError, TypeError):
            return True
    if not isinstance(metadata, dict):
        return False
    return metadata.get("engine_ready") is False or metadata.get("engine_status") in {
        "down",
        "unreachable",
        "stopped",
        "unhealthy",
    }


def infrastructure_failure(result) -> str | None:
    if not isinstance(result, dict) or _is_blocked_result(result) or result.get("retryable") is False:
        return None
    evidence = "\n".join(
        str(result.get(k) or "")
        for k in (
            "failure_reason",
            "worker_error",
            "error",
            "stderr",
            "tail",
            "exception_type",
        )
    ).lower()
    if any(
        marker in evidence
        for marker in (
            "protection_violation",
            "host_declines_kind",
            "released_by_host",
            "no_workspace_for_kind",
            "no_workspace_for_repo",
            "no_repo_mapping",
        )
    ):
        return None
    if (
        "not cancelled" in evidence
        or "cancel unacknowledged" in evidence
        or "session-cancel unacknowledged" in evidence
    ):
        return "engine_cancel_unacknowledged"
    if (
        "daemon stopped by pid" in evidence
        or "socket is gone" in evidence
        or (
            "daemon.sock" in evidence
            and any(
                word in evidence for word in ("unreachable", "connection refused", "no such file", "socket is gone")
            )
        )
    ):
        return "engine_unavailable"
    if "provider idle timeout" in evidence or "provider_idle_timeout" in evidence:
        return "provider_idle_timeout"
    return None
