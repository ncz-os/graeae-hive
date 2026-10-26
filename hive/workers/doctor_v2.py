"""Deterministic doctor v2 triage.

This module deliberately has no bus or worker imports.  The reconciler owns
state changes; this module produces a bounded decision from a snapshot of
evidence.  The existing doctor remains the executor and keeps its approval
gates, work-contract guard, backoff, and capability filtering.
"""
from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 compatibility
    tomllib = None

_VOLATILE = (
    (re.compile(r"\b\d{4}-\d\d-\d\d[T ][^\s]+\b"), "<time>"),
    (re.compile(r"(?:/[^\s:'\"]+)+"), "<path>"),
    (re.compile(r"\b[0-9a-f]{8,}\b", re.I), "<hex>"),
    (re.compile(r"\b\d+(?:\.\d+)?\b"), "<n>"),
)
_EVIDENCE_KEYS = ("failure_reason", "error", "tail", "stderr", "stdout", "stack", "note")


def _result(job: dict[str, Any]) -> dict[str, Any]:
    value = job.get("result")
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {"raw": value}
        except (TypeError, ValueError):
            return {"raw": value}
    return {}


def _line(value: Any) -> str:
    return str(value or "").strip().splitlines()[0] if str(value or "").strip() else ""


def _error_class(text: str) -> str:
    lowered = text.lower()
    if re.search(r"(?:429|502|503|504|rate.?limit|connect(?:ion)? timeout|dns)", lowered):
        return "provider_transport"
    if "oom" in lowered or "enospc" in lowered or "disk full" in lowered:
        return "host_fault"
    if "session-cancel" in lowered or "daemon.sock is unreachable" in lowered:
        return "zoder_infra"
    if "unhandled" in lowered or "traceback" in lowered or "exception" in lowered:
        return "exception"
    return "generic"


def normalize(text: str) -> str:
    result = text or ""
    for pattern, replacement in _VOLATILE:
        result = pattern.sub(replacement, result)
    return re.sub(r"\s+", " ", result).strip()


@dataclass(frozen=True)
class Evidence:
    job: dict[str, Any]
    result: dict[str, Any]
    failure_reason: str
    error: str
    evidence_lines: tuple[str, ...]
    retry_count: int
    host: str
    lineage: str
    prior_actions: tuple[dict[str, Any], ...] = ()
    loop_record: dict[str, Any] = field(default_factory=dict)
    head_exists: bool | None = None

    @property
    def evidence_text(self) -> str:
        return "\n".join(self.evidence_lines)

    @property
    def signature(self) -> str:
        first = self.evidence_lines[0] if self.evidence_lines else ""
        basis = "|".join((self.failure_reason, _error_class(self.error), normalize(first)))
        return hashlib.sha256(basis.encode()).hexdigest()


def collect_evidence(job: dict[str, Any], *, prior_actions: Iterable[dict[str, Any]] = (),
                     loop_record: dict[str, Any] | None = None,
                     head_exists: bool | None = None, tail_limit: int = 16_384) -> Evidence:
    """Build the bounded evidence snapshot consumed by rules and the LLM."""
    result = _result(job)
    failure = str(result.get("failure_reason") or job.get("failure_reason") or "").strip()
    error = str(result.get("error") or job.get("error") or "").strip()
    lines: list[str] = []
    for key in _EVIDENCE_KEYS:
        value = result.get(key) or job.get(key)
        if value:
            text = str(value)[-tail_limit:]
            lines.extend(line.strip() for line in text.splitlines() if line.strip())
    # The first matching line is stable, and avoids making a volatile tail
    # position part of the signature.
    if not lines and error:
        lines.append(error)
    base_kind = str(job.get("base_kind") or job.get("kind") or "")
    head = str(job.get("head_branch") or "")
    lineage = f"{base_kind}|{head}"
    return Evidence(job, result, failure, error, tuple(lines), int(job.get("retry_count") or result.get("retry_count") or 0),
                    str(job.get("host") or job.get("claimed_host") or ""), lineage,
                    tuple(prior_actions), loop_record or {}, head_exists)


@dataclass(frozen=True)
class Decision:
    action: str
    rule_id: str
    reason: str
    signature: str
    max_retries: int = 0
    retry_scope: str = "same_host"
    evidence_quote: str = ""
    model: str | None = None
    cost_estimate: float | None = None
    latency_ms: float | None = None


def load_rules(path: str | Path) -> list[dict[str, Any]]:
    """Load ordered TOML rules; TOML preserves file order for array tables."""
    if tomllib is None:
        raise RuntimeError("Python tomllib is required to load doctor rules")
    with open(path, "rb") as stream:
        data = tomllib.load(stream)
    return list(data.get("rule", []))


def _matches(rule: dict[str, Any], evidence: Evidence) -> bool:
    failure_re = rule.get("failure_reason_regex")
    evidence_re = rule.get("evidence_regex")
    checks = []
    if failure_re:
        checks.append(bool(re.search(str(failure_re), evidence.failure_reason, re.I)))
    if evidence_re:
        checks.append(bool(re.search(str(evidence_re), evidence.evidence_text, re.I)))
    if not checks:
        return False
    return all(checks) if rule.get("match_all") else any(checks)


def _same_signature_count(evidence: Evidence) -> int:
    return sum(1 for action in evidence.prior_actions if action.get("signature") == evidence.signature)


def decide_rules(evidence: Evidence, rules: list[dict[str, Any]]) -> Decision | None:
    """First-match deterministic decision, with the global two-retry guard."""
    for rule in rules:
        if not _matches(rule, evidence):
            continue
        retries = int(rule.get("max_retries", 0))
        if retries and _same_signature_count(evidence) >= 2:
            return Decision("escalate", rule["id"], "same signature reached global retry cap", evidence.signature,
                            retries, str(rule.get("retry_scope", "same_host")), evidence_lines(evidence))
        quote = evidence_lines(evidence)
        reason = str(rule.get("note", "")).replace("{signature}", evidence.signature)
        # R9 is deliberately terminal for the host, not a normal retry.
        if rule.get("id") == "R9":
            reason += f" host={evidence.host}; health note required; one escalation per host/day"
        return Decision(str(rule["action"]), str(rule["id"]), reason, evidence.signature, retries,
                        str(rule.get("retry_scope", "same_host")), quote)
    return None


def evidence_lines(evidence: Evidence) -> str:
    return evidence.evidence_lines[0] if evidence.evidence_lines else ""


def validate_llm_decision(raw: dict[str, Any], evidence: Evidence, *, rule_cap: int = 2,
                          used_retries: int = 0) -> Decision:
    """Validate residual JSON, especially the verbatim evidence quote."""
    allowed = {"release_to_queue", "escalate", "needs_review", "no_action"}
    action = raw.get("action") or raw.get("action_type")
    quote = str(raw.get("evidence_quote") or raw.get("quote") or "")
    if action not in allowed or not quote or quote not in evidence.evidence_text:
        return Decision("escalate", "llm", "invalid residual decision or evidence quote", evidence.signature,
                        0, "same_host", quote, str(raw.get("model") or "llm"))
    requested = int(raw.get("max_retries", 0) or 0)
    permitted = max(0, min(requested, rule_cap, 2 - used_retries))
    return Decision(str(action), "llm", str(raw.get("reason") or ""), evidence.signature, permitted,
                    str(raw.get("retry_scope") or "same_host"), quote, str(raw.get("model") or "llm"))


def bs_audit_signal(job: dict[str, Any]) -> str:
    """Small, dependency-free equivalent of the live bs_audit rule input."""
    result = _result(job)
    kind = str(job.get("kind") or "")
    response = str(result.get("summary") or result.get("full_response") or result.get("text") or result.get("output") or "")
    mandatory = kind in {"riskyeats", "code"} or kind.startswith(("riskyeats:", "feat:", "fix:", "argonaut:", "riskybiz:"))
    if mandatory and not result.get("commits"):
        if len(response.strip()) < 40:
            return "BS:empty+nocommit"
        if any(x in response.lower() for x in ("i don't have the ability", "as an ai", "no code change required")):
            return "BS:confab+nocommit"
        return "BS:nocommit-mandatory"
    return "ok-committed" if mandatory else "ok-noncommit"


def shadow_compare(old: dict[str, Any], new: Decision | None) -> dict[str, Any]:
    old_action = old.get("action") or old.get("action_type") or "unknown"
    new_action = new.action if new else "llm"
    return {"old_action": old_action, "new_action": new_action, "agree": old_action == new_action}


def shadow_report(records: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Summarize persisted side-by-side decisions for a report command."""
    report = {"total": 0, "agreement": 0, "disagreement": 0}
    for record in records:
        report["total"] += 1
        if record.get("agree"):
            report["agreement"] += 1
        else:
            report["disagreement"] += 1
    return report


def residual_decision(evidence: Evidence, call: Callable[[str], dict[str, Any]], *, rule_cap: int = 2) -> Decision:
    started = time.monotonic()
    payload = {
        "failure_reason": evidence.failure_reason, "error": evidence.error,
        "evidence": list(evidence.evidence_lines), "retry_count": evidence.retry_count,
        "host": evidence.host, "lineage": evidence.lineage,
        "required": "Return JSON with action, reason, evidence_quote, max_retries, retry_scope, model.",
    }
    try:
        decision = validate_llm_decision(call(json.dumps(payload)), evidence, rule_cap=rule_cap,
                                         used_retries=_same_signature_count(evidence))
    except Exception as exc:  # residual failures are safe escalation
        decision = Decision("escalate", "llm", f"residual model failure: {exc}", evidence.signature)
    return Decision(decision.action, decision.rule_id, decision.reason, decision.signature, decision.max_retries,
                    decision.retry_scope, decision.evidence_quote, decision.model, decision.cost_estimate,
                    (time.monotonic() - started) * 1000)


def sweep_needs_review(jobs: Iterable[dict[str, Any]], *, now: float | None = None, older_than_hours: float = 24) -> list[dict[str, Any]]:
    """Report-only classification of old needs-review jobs; never mutates jobs."""
    cutoff = (now or time.time()) - older_than_hours * 3600
    rows = list(jobs)
    out = []
    for job in rows:
        if job.get("status") != "needs-review" or float(job.get("updated_at") or job.get("ended_at") or 0) > cutoff:
            continue
        kind = job.get("base_kind") or job.get("kind")
        later_done = any(other is not job and (other.get("base_kind") or other.get("kind")) == kind and other.get("status") == "done" for other in rows)
        reason = "superseded by later completed job" if later_done else ("stale" if job.get("head_branch") else "actionable")
        out.append({"job_id": job.get("id"), "classification": "superseded" if later_done else reason,
                    "orchestrator_note": f"report-only needs-review sweep: {reason}; evidence={job.get('failure_reason') or 'none'}"})
    return out
