"""The doctor's deterministic triage table (operator rule 2026-10-31).

The doctor is a DETERMINISTIC STATE MACHINE first. A model is called only for
an AMBIGUOUS classification and a short reason. Everything a rule can decide is
decided here, by ROW SHAPE, before any model request is built:

* the row carries no result and no reason -> ``empty_result`` (escalate);
* the row carries a zoder loop teardown -> the loop-outcome table
  (``loop_outcome._TEARDOWN_TO_CLASS``) decides, namespaced ``loop:<class>``;
* the row carries a clone/transport failure -> ``retryable_transport``;
* the never-act classes (declines, protection violations) are claimed by their
  own callers, so this table returns ``None`` for them and a second,
  conflicting decision is impossible.

What is left is a MODEL-CLASSIFIABLE RESIDUE: evidence the tables do not know.
Even there the model's freedom is closed -- it may pick one of ``CLASSES`` and
give a short reason, and ``parse_constrained`` rejects anything else so the call
moves to the next chain rung.

This module is stdlib only and imports nothing from the doctor, so it can be
used by ``doctor.py``, ``doctor_models.py`` and the weekend status script without
an import cycle.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any

from hive.workers import loop_outcome

#: The closed set of actions a doctor decision may name. A model may only
#: choose one of these; a reply naming anything else is rejected.
CLASSES = ("no_action", "release_to_queue", "escalate")

#: A model reason is a SHORT reason, not an essay.
MAX_REASON_CHARS = 400

#: Confidence is a 0..1 number; anything outside is clamped.
MIN_CONFIDENCE = 0.0
MAX_CONFIDENCE = 1.0

#: Restoration alert (operator 2026-10-31): a dedicated light model is restored
#: when the existing capacity is demonstrably not enough.
ALERT_CALLS_PER_HOUR = 30
ALERT_SLOT_TAKEN_PER_HOUR = 3
ALERT_TOPIC = "doctor.light_model_needed"
WINDOW_SECS = 3600.0

#: Where the per-call ledger lives when the deploy does not say otherwise.
LEDGER_ENV = "DOCTOR_MODEL_CALL_LEDGER"
LEDGER_FILENAME = "model-calls.jsonl"

#: Never-act classes: claimed by the decline / protection callers, never here.
#: ``no_workspace_for_repo`` is a workspace-decline shape (the decline caller
#: owns it); the BARE ``no_repo_mapping`` is NOT here -- it is
#: deployment-dependent and the doctor decides it after reading the catalog.
NEVER_ACT_MARKERS = (
    "host_declines_kind",
    "released_by_host",
    "protection_violation",
    "no_workspace_for_repo",
)

#: Deterministic release shapes: a transport-side failure that is not the job's
#: work.
RETRYABLE_RE = (
    "clone_failed",
    "session-cancel",
    "daemon.sock is unreachable",
    "connection reset",
    "connection refused",
    "could not resolve host",
    "could not read from remote",
    "timed out",
    "timeout",
    "temporary failure",
    "503",
    "502",
    "504",
)

#: Deterministic no-action shapes: nothing for the doctor to do.
NO_ACTION_RE = (
    "reboot",  # the host was rebooted: the job did not fail at its work
    "already merged",
    "superseded",
    "duplicate",
)

#: Loop-outcome classes whose failure is outside the job's own work.
RETRYABLE_LOOP_CLASSES = frozenset(
    {
        loop_outcome.REVIEWER_UNAVAILABLE,
        loop_outcome.AUTHOR_FAILED,
        loop_outcome.TIMEOUT,
        loop_outcome.CRASH,
        loop_outcome.KILLED_BY_SIGNAL,
        loop_outcome.CLONE_FAILED,
    }
)

MUSE_STANDARD_MODEL = "muse-spark-1.3"


@dataclass(frozen=True)
class RowClass:
    """A deterministic classification of one failed row."""

    rule_id: str
    action: str
    reason: str
    ambiguous: bool = False


def _result(row: dict[str, Any]) -> dict[str, Any]:
    """The row's result as a dict, whatever the bus stored."""
    value = row.get("result")
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {"raw": value}
        return parsed if isinstance(parsed, dict) else {"raw": value}
    return {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _evidence_text(row: dict[str, Any], result: dict[str, Any]) -> str:
    parts = [_text(row.get("failure_reason")), _text(result.get("failure_reason"))]
    for key in ("worker_error", "error", "tail", "stderr"):
        parts.append(_text(result.get(key))[:4096])
        parts.append(_text(row.get(key))[:4096])
    return "\n".join(part for part in parts if part)


def _teardown_reason(result: dict[str, Any]) -> str:
    loop_result = result.get("loop_result")
    if not isinstance(loop_result, dict):
        return ""
    teardown = loop_result.get("teardown")
    if isinstance(teardown, dict):
        return _text(teardown.get("reason") or teardown.get("error"))
    return _text(teardown)


def classify_row(row: dict[str, Any]) -> RowClass | None:
    """The table decision for ``row``, or ``None`` when it is never-act.

    ``None`` is not "ambiguous": it means another caller owns this row (see
    ``NEVER_ACT_MARKERS``). The residue -- evidence no rule names -- is what
    comes back as ``ambiguous`` and may be sent to a model.
    """
    if not isinstance(row, dict):
        return None
    result = _result(row)
    evidence = _evidence_text(row, result)

    if any(marker in evidence for marker in NEVER_ACT_MARKERS):
        return None

    teardown = _teardown_reason(result)
    if teardown:
        cls = loop_outcome._TEARDOWN_TO_CLASS.get(teardown)  # noqa: SLF001 -- ONE table
        if cls is None:
            # A teardown reason nobody has seen is evidence, not a shape.
            return RowClass("loop:unknown", "escalate", f"loop teardown {teardown!r} has no class")
        action = "release_to_queue" if cls in RETRYABLE_LOOP_CLASSES else "escalate"
        return RowClass(f"loop:{cls}", action, f"zoder loop tore down: {cls}")

    if not result and not evidence:
        return RowClass("empty_result", "escalate", "row carries no result and no failure reason")

    lowered = evidence.lower()
    if any(marker in lowered for marker in NO_ACTION_RE):
        return RowClass("residue:no_action", "no_action", "the row names a settled outcome")
    if any(marker in lowered for marker in RETRYABLE_RE):
        return RowClass("retryable_transport", "release_to_queue", "transient transport failure")

    # Evidence no table names. The DEFAULT is still deterministic: the row goes
    # to a human, and that is the classification (``ambiguous`` marks it as the
    # candidate for one constrained confirmation, not as an unanswered row).
    return RowClass(
        "residue:unknown",
        "escalate",
        "evidence does not match a deterministic rule; escalated",
        ambiguous=True,
    )


def as_action(cls: RowClass | None) -> dict[str, Any]:
    """The legacy action dict for a table decision (``None`` -> own caller)."""
    if cls is None:
        return {"action_type": "escalate", "confidence": 1.0, "reason": "never-act class; own caller decides"}
    return {
        "action_type": cls.action,
        "confidence": 1.0,
        "reason": cls.reason,
        "rule_id": cls.rule_id,
        "needs_review": cls.action == "escalate",
    }


# ── constrained model output ──────────────────────────────────────────────


def decision_schema() -> dict[str, Any]:
    """The JSON schema a doctor model reply must satisfy.

    Enum, no additional properties and a bounded reason: a small or overloaded
    model cannot answer outside the classes, and the grammar a llama-server
    compiles from this schema pins the shape of every token it emits.
    """
    return {
        "type": "object",
        "properties": {
            "action_type": {"type": "string", "enum": list(CLASSES)},
            "confidence": {"type": "number", "minimum": MIN_CONFIDENCE, "maximum": MAX_CONFIDENCE},
            "reason": {"type": "string", "maxLength": MAX_REASON_CHARS},
        },
        "required": ["action_type"],
        "additionalProperties": False,
    }


def structured_request_fields() -> dict[str, Any]:
    """The request fields that constrain a reply to :func:`decision_schema`.

    Three spellings of one grammar, because the rungs differ: ``response_format``
    is what every OpenAI-compatible endpoint honours, ``json_schema`` is what
    llama-server reads, and ``grammar`` is llama-server's (and vLLM's) GBNF
    carrier -- the object form is built from the same schema, so the schema
    cannot drift from the grammar. A server that ignores an unknown field
    ignores it; a server that honours any of them cannot answer outside the
    enum."""
    schema = decision_schema()
    return {
        "response_format": {"type": "json_object"},
        "json_schema": schema,
        "grammar": {"type": "json_schema", "json_schema": schema},
    }


def parse_constrained(text: str) -> tuple[bool, dict[str, Any]]:
    """``(ok, obj)`` for a model reply; ``obj`` is ``{"error": ...}`` when bad.

    Mirrors the four rules the schema states: it must be JSON, it must name an
    action in ``CLASSES``, confidence is clamped into 0..1 and the reason is
    truncated to ``MAX_REASON_CHARS``. Everything else is DROPPED, so a reply
    cannot smuggle a retry scope or an evidence quote the rules never
    validated.
    """
    body = _text(text)
    if not body:
        return False, {"error": "empty"}
    try:
        raw = json.loads(body)
    except (TypeError, ValueError):
        return False, {"error": "not_json"}
    if not isinstance(raw, dict):
        return False, {"error": "not_object"}
    action = _text(raw.get("action_type"))
    if action not in CLASSES:
        return False, {"error": f"action_type {action!r} is not one of {list(CLASSES)}"}
    try:
        confidence = float(raw.get("confidence", 1.0))
    except (TypeError, ValueError):
        return False, {"error": "confidence is not a number"}
    return True, {
        "action_type": action,
        "confidence": max(MIN_CONFIDENCE, min(MAX_CONFIDENCE, confidence)),
        "reason": _text(raw.get("reason"))[:MAX_REASON_CHARS],
    }


# ── the call ledger and the restoration alert ─────────────────────────────


def ledger_path(env: dict[str, str] | None = None) -> str:
    """The JSONL ledger path: the env var, else the doctor's state directory."""
    source = os.environ if env is None else env
    named = str(source.get(LEDGER_ENV) or "").strip()
    if named:
        return os.path.expanduser(named)
    root = str(source.get("XDG_STATE_HOME") or "").strip() or os.path.join(
        os.path.expanduser(str(source.get("HOME") or "~")), ".local", "state"
    )
    return os.path.join(os.path.expanduser(root), "hive-doctor", LEDGER_FILENAME)


def model_call_counter(
    entries: list[dict[str, Any]],
    *,
    now: float,
    slot_entries: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """The last-hour counter the status line and the alert read.

    ``slot_entries`` carries slot-taken events restored from the ledger's
    sidecar (they survive a restart); they count toward the SLOT threshold
    only, never toward the call count -- a slot-taken event is a record ABOUT
    a call, not a second call.
    """
    cutoff = float(now) - WINDOW_SECS
    live = [
        entry
        for entry in entries
        if isinstance(entry, dict)
        # A malformed line is not a call; skip it rather than lose the window.
        and isinstance(entry.get("at"), int | float)
        and float(entry["at"]) > cutoff
    ]
    calls = len(live)
    restored = [
        entry
        for entry in (slot_entries or [])
        if isinstance(entry, dict) and isinstance(entry.get("at"), int | float) and float(entry["at"]) > cutoff
    ]
    seen = {float(entry["at"]) for entry in live if entry.get("slot_taken")}
    taken = sum(1 for entry in live if entry.get("slot_taken"))
    taken += sum(1 for entry in restored if float(entry["at"]) not in seen)
    alert_reason = None
    if calls > ALERT_CALLS_PER_HOUR:
        alert_reason = "calls_per_hour"
    elif taken > ALERT_SLOT_TAKEN_PER_HOUR:
        alert_reason = "free_slot_taken"
    return {
        "calls_last_hour": calls,
        "slot_taken_last_hour": taken,
        "alert": alert_reason is not None,
        "alert_reason": alert_reason,
    }


def format_counter_line(counter: dict[str, Any]) -> str:
    """One status line: ``doctors: model_calls=N/1h slot_taken=M/1h alert=...``."""
    line = (
        f"doctors: model_calls={int(counter.get('calls_last_hour') or 0)}/1h "
        f"slot_taken={int(counter.get('slot_taken_last_hour') or 0)}/1h"
    )
    if counter.get("alert"):
        line += f" {ALERT_TOPIC} reason={counter.get('alert_reason')}"
    return line


LEDGER_FIELDS = ("at", "rung", "phase", "latency_ms", "outcome", "skipped_because")


class CallLedger:
    """Every doctor model call, one JSONL line per rung, plus the alert.

    A line carries exactly ``at, rung, phase, latency_ms, outcome,
    skipped_because`` -- never a prompt, never a key. The alert is
    emitted ONCE per crossing: ``_alerted_reason`` remembers which hour's alert
    fired, so call 31 and call 32 of the same hour do not both broadcast, and a
    quiet hour re-arms it.
    """

    def __init__(self, path: str | None = None, *, clock=None, max_lines: int = 5000):
        self.path = path if path is not None else ledger_path()
        self.clock = clock or time.time
        self.max_lines = max(1, int(max_lines))
        self._lock = threading.Lock()
        self._entries: list[dict[str, Any]] | None = None
        self._alerted_reason: str | None = None
        # ``slot_taken`` is not one of the six line fields, but losing it on a
        # restart would hide exactly the sustained pattern the alert exists to
        # catch, so the timestamps live in a sidecar beside the ledger.
        self._slot_path = f"{self.path}.slot-taken"

    # -- storage ---------------------------------------------------------
    def _load(self) -> list[dict[str, Any]]:
        if self._entries is not None:
            return self._entries
        entries: list[dict[str, Any]] = []
        try:
            with open(self.path, encoding="utf-8") as fh:
                for line in fh:
                    try:
                        entry = json.loads(line)
                    except ValueError:
                        continue
                    if isinstance(entry, dict):
                        entries.append(entry)
        except OSError:
            entries = []
        self._entries = entries[-self.max_lines :]
        return self._entries

    def _append_slot_taken(self, now: float) -> None:
        try:
            directory = os.path.dirname(self._slot_path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            with open(self._slot_path, "a", encoding="utf-8") as fh:
                fh.write(f"{now}\n")
        except OSError:
            pass

    def _slot_taken_entries(self, now: float) -> list[dict[str, Any]]:
        """The sidecar's timestamps inside the window, as counter entries."""
        try:
            with open(self._slot_path, encoding="utf-8") as fh:
                stamps = [float(line) for line in fh if line.strip()]
        except (OSError, ValueError):
            return []
        return [{"at": at, "outcome": "slot_taken", "slot_taken": True} for at in stamps if now - at < WINDOW_SECS]

    def _append(self, entry: dict[str, Any]) -> None:
        try:
            directory = os.path.dirname(self.path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, sort_keys=True) + "\n")
        except OSError:
            # A ledger that cannot be written must never stop a triage call.
            pass

    # -- the one public call ---------------------------------------------
    def record(
        self,
        *,
        rung: str,
        phase: str,
        latency_ms: float,
        outcome: str,
        skipped_because: str | None = None,
        slot_taken: bool = False,
        at: float | None = None,
    ) -> dict[str, Any]:
        """Record one call and return ``{alert, alert_reason, counters...}``."""
        now = self.clock() if at is None else float(at)
        entry = {
            "at": float(now),
            "rung": str(rung or ""),
            "phase": str(phase or ""),
            "latency_ms": round(float(latency_ms or 0.0), 1),
            "outcome": str(outcome or ""),
            "skipped_because": (str(skipped_because)[:200] if skipped_because else None),
            "slot_taken": bool(slot_taken),
        }
        entry = {k: entry[k] for k in (*LEDGER_FIELDS, "slot_taken")}
        # ``slot_taken`` is an INPUT to the counter, not a persisted field:
        # the line on disk carries exactly the six enumerated keys.
        persisted = {k: entry[k] for k in LEDGER_FIELDS}
        with self._lock:
            entries = self._load()
            entries.append({**persisted, **({"slot_taken": True} if slot_taken else {})})
            if slot_taken:
                self._append_slot_taken(now)
            if len(entries) > self.max_lines:
                del entries[: len(entries) - self.max_lines]
            self._append(persisted)
            counter = model_call_counter(entries, now=now)
            reason = counter.get("alert_reason")
            alert = bool(counter.get("alert")) and reason != self._alerted_reason
            if reason:
                self._alerted_reason = reason
            else:
                # A quiet hour re-arms the alert.
                self._alerted_reason = None
        return {**counter, "alert": alert}

    def counter(self, now: float | None = None) -> dict[str, Any]:
        now = self.clock() if now is None else float(now)
        # The sidecar carries the slot-taken history across a restart. Merge it
        # with the in-memory flags, one event per ``at``, so an event is never
        # counted twice within this process nor lost across a restart.
        return model_call_counter(self._load(), now=now, slot_entries=self._slot_taken_entries(now))


# ── the default chain order ───────────────────────────────────────────────


def default_chain(inventory: dict | None = None, *, env: dict | None = None, repo: str | None = None):
    """The operator's order: gpt-oss-20b on CERBERUS, qwen38, M3, DeepSeek, Muse.

    Delegates to ``doctor_models.default_rungs`` so there is ONE definition of
    the order. A local rung whose host the inventory does not know keeps an
    empty ``base_url`` and is skipped by the chain (the address is never
    guessed). No light-model rung is restored on HYDRA/ACHILLES: existing
    capacity is enough while doctor usage is rare (operator 2026-10-31).
    """
    from hive.workers import doctor_models  # local: ONE definition, no cycle

    return doctor_models.default_rungs(inventory, env=env, repo=repo)


__all__ = [
    "ALERT_CALLS_PER_HOUR",
    "ALERT_SLOT_TAKEN_PER_HOUR",
    "ALERT_TOPIC",
    "CLASSES",
    "CallLedger",
    "LEDGER_FIELDS",
    "MAX_REASON_CHARS",
    "MUSE_STANDARD_MODEL",
    "RowClass",
    "as_action",
    "classify_row",
    "decision_schema",
    "default_chain",
    "format_counter_line",
    "ledger_path",
    "model_call_counter",
    "parse_constrained",
    "structured_request_fields",
]
