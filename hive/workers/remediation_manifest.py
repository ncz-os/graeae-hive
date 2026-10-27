"""Single source of truth for the doctor's deterministic failure handlers.

Every failure class the classifier knows has a row here: the signature
prefix, the handler that resolves it, and the test that pins it. The
doctor's ``KNOWN_FAILURE_SIGNATURES`` regex table and the in-code branches
in ``doctor.py`` implement these rows; this manifest is the contract.

``docs/DETERMINISTIC-PATHS.md`` is GENERATED from this module (see the
manifest test in ``test_remediation_unknown_signatures.py``); it must not
be hand-maintained prose, which is how it drifted from the code before.

The in-flight branches ``fix/doctor-failure-matrix-20261027`` and
``fix/doctor-decline-paths-20261027`` add their rows to this same table;
the unknown-signature fallback in ``hive.workers.remediation`` is what
covers whatever no row here claims.
"""
from __future__ import annotations

# signature prefix -> {"handler": name of the code path that owns it,
#                      "test": name of the test that pins that handler}
#
# Prefixes are matched against ``remediation.failure_signature(reason)``
# (lowercase, volatile parts stripped) with ``str.startswith``.
SIGNATURE_HANDLERS: dict[str, dict[str, str]] = {
    # ── worker-side delivery failures (hive_worker.py) ────────────────
    "finalizer=push_failed": {
        "handler": "worker.push_policy.finalizer",
        "test": "test_push_policy_finalizer",
    },
    "exit code": {
        "handler": "worker.exit_failure_reason",
        "test": "test_exit_failure_reason",
    },
    "signal": {
        "handler": "worker.exit_failure_reason",
        "test": "test_exit_failure_reason",
    },
    "timed out": {
        "handler": "worker.timeout_finalizer",
        "test": "test_timeout_finalizer",
    },
    # ── doctor-side deterministic branches (hive/workers/doctor.py) ───
    "token_mismatch": {
        "handler": "doctor._match_known_signature",
        "test": "test_known_signature_fix_dispatched_is_announced_and_released",
    },
    "no token for host": {
        "handler": "doctor._match_known_signature",
        "test": "test_known_signature_fix_dispatched_is_announced_and_released",
    },
    "transient ws/gateway error": {
        "handler": "doctor._match_known_signature",
        "test": "test_transient_gateway_error_releases",
    },
    "unschedulable": {
        "handler": "doctor._is_unschedulable",
        "test": "test_unschedulable_escalates_without_llm",
    },
    "budget_exceeded": {
        "handler": "worker.release_without_retry",
        "test": "test_budget_exceeded_releases_to_queue",
    },
    "delivery_integrity_check_failed": {
        "handler": "worker.delivery_integrity_errors",
        "test": "test_delivery_integrity_failure",
    },
    "ai_attribution_trailer": {
        "handler": "worker.attribution_guard",
        "test": "test_attribution_trailer_fails_closed",
    },
    "codex_delivery_failed": {
        "handler": "worker.codex_delivery",
        "test": "test_codex_delivery_failed_never_done",
    },
    "protection_violation": {
        "handler": "worker.protection_guard",
        "test": "test_protection_violation_always_fails",
    },
    "retry-budget-exhausted": {
        "handler": "doctor.retry_budget_exhausted",
        "test": "test_failed_job_with_max_retries_zero_is_not_requeued",
    },
    # ── dead-letter remediation (hive/workers/doctor.py) ──────────────
    "transient-infra-failure": {
        "handler": "doctor.classify_dead_letter",
        "test": "test_dlq_transient_failure_with_no_budget_becomes_needs_review",
    },
    "unschedulable:": {
        "handler": "doctor.classify_dead_letter",
        "test": "test_dlq_protected_by_kind_prefix_and_project",
    },
}


def handler_for_signature(signature: str) -> dict[str, str] | None:
    """The manifest row owning ``signature``, or None when no row claims it.

    Longest prefix wins, so a specific row is not shadowed by a broader one
    with the same stem. An empty signature claims nothing: there is no
    evidence to match, and the unknown policy owns the empty case.
    """
    sig = (signature or "").strip()
    if not sig:
        return None
    best: tuple[int, dict[str, str]] | None = None
    for prefix, row in SIGNATURE_HANDLERS.items():
        if sig.startswith(prefix) and (best is None or len(prefix) > best[0]):
            best = (len(prefix), row)
    return best[1] if best else None


def signature_is_handled(signature: str) -> bool:
    """Whether the classifier has a deterministic row for this signature."""
    return handler_for_signature(signature) is not None
