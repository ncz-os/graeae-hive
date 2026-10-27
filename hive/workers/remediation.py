"""Bounded, self-describing path for failures the classifier has never seen.

The audit (DETERMINISM-GAPS-20261027) showed every KNOWN failure class has a
deterministic handler, but the NEXT unknown reason has none: the doctor would
park it in needs-review and hope a human notices, or let an LLM invent a
decision. This module closes that gap with code, no model:

* ``failure_signature(reason)`` — the normaliser. Strips the volatile parts of
  a raw failure reason (ids, numbers, paths, hosts, payloads, timestamps) and
  keeps the stable stems, so two jobs with the same failure shape produce the
  same signature and the same job run twice with different ids does too.
* ``SignatureTable`` — the durable signature table in the doctor's STATE:
  per-signature count / first_seen / last_seen / sample job ids / last action /
  handlers seen, 30-day TTL, bounded (max 500 signatures, evict oldest).
* ``unknown_policy(...)`` — what to do when the classifier has no row:
  a. commits reachable from origin -> treat as delivered (done, with notes);
  b. else, fewer than UNKNOWN_RETRY_MAX retries for this signature+job ->
     retry once on a different host, tagged ``unknown_signature``;
  c. else -> needs-review with ``needs_operator: true`` and the evidence.
* ``check_gap_promotion(...)`` — when an unhandled signature reaches
  UNKNOWN_PROMOTE_COUNT occurrences within UNKNOWN_PROMOTE_WINDOW_SEC, post one
  ``doctor.classifier_gap`` message and open ONE deduped
  ``classifier-gap:<signature>`` needs-review job asking for a handler + test.
* ``coverage_report(...)`` — the continuous version of the manual audit: every
  COVERAGE_INTERVAL_SEC (and on a ``doctor.coverage.request`` message) write
  ``~/.hive/doctor/coverage-<UTC date>.json`` with the top signatures marked
  handled/unknown/promoted and post ``doctor.coverage``.

The handler table itself lives in ``remediation_manifest.py`` (the single
source of truth); the in-flight ``fix/doctor-failure-matrix-20261027`` and
``fix/doctor-decline-paths-20261027`` branches add their rows there, so this
module only classifies what no row claims.
"""
from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from hive.workers.remediation_manifest import SIGNATURE_HANDLERS, handler_for_signature

# ── Tunables (env-overridable) ──────────────────────────────────────────────

#: Per signature+job, how many times the unknown policy may cheaply retry on a
#: different host before the job is parked for a human.
UNKNOWN_RETRY_MAX = int(os.environ.get("UNKNOWN_RETRY_MAX", "1"))

#: How many occurrences of an UNHANDLED signature, within the window, turn it
#: into a named, counted, actionable gap (one message + one needs-review job).
UNKNOWN_PROMOTE_COUNT = int(os.environ.get("UNKNOWN_PROMOTE_COUNT", "3"))
UNKNOWN_PROMOTE_WINDOW_SEC = float(os.environ.get("UNKNOWN_PROMOTE_WINDOW_SEC", str(7 * 86400)))

#: How often the coverage report is (re)written and posted.
COVERAGE_INTERVAL_SEC = float(os.environ.get("COVERAGE_INTERVAL_SEC", str(24 * 3600)))

#: Coverage file location; ``<UTC date>`` is the UTC date the report covers.
COVERAGE_DIR = Path(os.environ.get("DOCTOR_COVERAGE_DIR", str(Path.home() / ".hive" / "doctor")))
COVERAGE_TOP_N = 20
COVERAGE_POST_TOP_N = 10

#: Signature table bounds.
SIGNATURE_TTL_SEC = float(os.environ.get("SIGNATURE_TTL_SEC", str(30 * 86400)))  # 30 days
SIGNATURE_MAX_ENTRIES = int(os.environ.get("SIGNATURE_MAX_ENTRIES", "500"))
SIGNATURE_SAMPLE_JOBS_MAX = 5
SIGNATURE_REASONS_MAX = 5

#: State key prefixes (named once so the doctor's TTL table and the keys this
#: module writes cannot drift apart).
KEY_SIGNATURE = "sig"
KEY_UNKNOWN_RETRY = "unknown_retry"
KEY_COVERAGE = "coverage"

#: Reason text kept per signature, capped so one loud failure cannot bloat the
#: durable table.
_REASON_CAP = 200


# ── 1. Signature normaliser ─────────────────────────────────────────────────

# Volatile patterns, applied most-specific-first so a 40-char SHA is not left
# as two hex halves and a timestamp is not left as a date plus a time. Each
# volatile span is replaced by a UNIQUE control-char token; the tokens become
# stable labels (<uuid>, <id>, <ts>, <payload>, <path>, <host>) only after the
# generic decimal-number rule has run, so nothing inside them is re-stripped.
_UUID_RE = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.IGNORECASE)
_HEX_ID_RE = re.compile(r"\b[0-9a-f]{7,}\b", re.IGNORECASE)
_TIMESTAMP_RE = re.compile(
    r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:[.,]\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?"
    r"|\b\d{2}/\d{2}/\d{4} \d{2}:\d{2}(?::\d{2})?"
    r"|\b\d{4}/\d{2}/\d{2} \d{2}:\d{2}(?::\d{2})?"
    r"|\b\d{2}:\d{2}(?::\d{2})\s?(?:am|pm)\b"
    r"|\b(?:mon|tues|wednes|thurs|fri|satur|sun)[a-z]* \d{1,2}, \d{4}\b",
    re.IGNORECASE,
)
_QUOTED_RE = re.compile(r"""(["'])(.*?)\1""")
_ABS_PATH_RE = re.compile(r"(?:/[A-Za-z0-9_.-]+){2,}")
_HOSTNAME_RE = re.compile(r"\b[A-Za-z][A-Za-z0-9-]*(?:\.[A-Za-z0-9-]+)+(?::\d+)?\b")
_UNIT_NUM_RE = re.compile(r"\b\d+(?:[.,]\d+)?\s?(?:ms|s|m|h|d)\b")
_DECIMAL_RE = re.compile(r"\b\d+(?:[.,]\d+)?\b")

# Stable stems the normaliser must PRESERVE even when they contain digits or
# look like numbers ("finalizer=...", "exit code", "timed out"): they are what
# a classifier row keys on, so stripping them would make the signature
# useless. The regex alternation tries patterns in this order at each position,
# so a specific stem wins over a later generic overlap.
_STABLE_STEMS = (
    r"finalizer=[^\s,;:]+",
    r"exit code",
    r"\bsignal\b",
    r"permission denied",
    r"no such file",
    r"timed out",
    r"\bnot found\b",
    r"\bpushed\b",
    r"\bdirty\b",
)
_STEM_RE = re.compile("|".join(f"({p})" for p in _STABLE_STEMS), re.IGNORECASE)

# Unique per-token placeholder: \x02 + tag + \x03. A real reason can never
# contain these control characters, and each tag is a distinct letter, so two
# different volatile spans never collide on the same placeholder.
_PUNCT_RE = re.compile(r"[^a-z0-9=<>\x00-\x1f]+")


def failure_signature(reason: str) -> str:
    """Normalise a raw failure reason into a stable, self-describing signature.

    Lowercases and strips the volatile parts — hex ids >= 7 chars, UUIDs,
    decimal numbers, absolute paths, hostnames, quoted payloads, timestamps —
    collapses whitespace and normalises punctuation, while PRESERVING the
    stable stems a classifier row keys on: ``finalizer=...``, ``exit code``,
    ``signal``, ``permission denied``, ``no such file``, ``timed out``,
    ``not found``, ``pushed``, ``dirty``.

    The stems are protected with placeholders for the whole stripping pass
    (they contain digits — ``finalizer=push_failed`` and ``exit code 128``),
    so the generic decimal and hex rules cannot eat them. Two different jobs
    with the same failure shape produce the same signature; the same job run
    twice with different ids does too. The result is short enough to use as a
    durable-state key and a job-kind fragment.
    """
    text = (reason or "").lower()
    if not text.strip():
        return "empty_reason"

    # 1. Protect the stable stems behind NUL-delimited placeholders.
    stems: list[str] = []

    def _protect(m: re.Match) -> str:
        stem = m.group(0)
        if stem not in stems:
            stems.append(stem)
        # Control-char placeholder: \x00 + chr(1 + idx) + \x00. No digits,
        # so the decimal rule cannot split it.
        return f"\x00{chr(1 + stems.index(stem))}\x00"

    masked = _STEM_RE.sub(_protect, text)

    # 2. Volatile spans -> unique tokens, most specific first. UUIDs and hex
    #    ids MUST run before the decimal rule: a 40-char SHA contains digits,
    #    so a decimal pass first would split it into <n> fragments. Each token
    #    is \x02 + tag + \x03; the token itself has no digits, so the decimal
    #    rule cannot re-match inside it.
    masked = _UUID_RE.sub("\x02\x01\x02", masked)
    masked = _HEX_ID_RE.sub("\x02\x02\x02", masked)
    masked = _TIMESTAMP_RE.sub("\x02\x03\x02", masked)
    masked = _QUOTED_RE.sub(lambda m: "\x02\x04\x02", masked)
    masked = _ABS_PATH_RE.sub("\x02\x05\x02", masked)
    masked = _HOSTNAME_RE.sub("\x02\x06\x02", masked)
    # Unit-suffixed numbers (600s, 42.5s, 10m, 1h2m) become one <n> so the
    # unit letter is not left behind. Runs before the generic decimal rule.
    masked = _UNIT_NUM_RE.sub("<n>", masked)
    # Decimal numbers last: everything that could be mistaken for a number
    # (hex ids, timestamps, ports in hostnames) is already tokenised.
    masked = _DECIMAL_RE.sub("<n>", masked)

    # 3. Whitespace and punctuation: collapse to single spaces, keep only
    #    [a-z0-9=<>] plus the control tokens (NUL, \x02, \x03).
    masked = re.sub(r"\s+", " ", masked).strip()
    masked = _PUNCT_RE.sub(" ", masked)
    masked = re.sub(r"\s+", " ", masked).strip()

    # 4. Restore the protected stems (already lowercase) verbatim.
    for idx, stem in enumerate(stems):
        masked = masked.replace(f"\x00{chr(1 + idx)}\x00", stem)

    # 5. Tokens -> stable labels.
    masked = masked.replace("\x02\x01\x02", "<uuid>")
    masked = masked.replace("\x02\x02\x02", "<id>")
    masked = masked.replace("\x02\x03\x02", "<ts>")
    masked = masked.replace("\x02\x04\x02", "<payload>")
    masked = masked.replace("\x02\x05\x02", "<path>")
    masked = masked.replace("\x02\x06\x02", "<host>")
    return re.sub(r"\s+", " ", masked).strip()


def signature_job_kind(signature: str) -> str:
    """The kind for a classifier-gap work item, bounded to the kind field."""
    safe = re.sub(r"[^a-z0-9-]+", "-", signature).strip("-") or "unknown"
    return f"classifier-gap:{safe[:64]}"


# ── 2. Durable signature table ──────────────────────────────────────────────


def _signature_key(signature: str) -> str:
    return f"{KEY_SIGNATURE}:{signature}"


def _evict_oldest_signatures(state, max_entries: int) -> None:
    """Bound the table: keep at most ``max_entries`` signatures, evict oldest.

    "Oldest" is the signature whose newest observation is furthest in the past
    (its last_seen, else first_seen). The eviction itself is durable: the
    evicted keys are tombstoned, so a later load cannot resurrect them.
    """
    keys = state.keys(KEY_SIGNATURE + ":")
    if len(keys) <= max_entries:
        return

    def _recency(key: str) -> float:
        record = state.value(key)
        if not isinstance(record, dict):
            return 0.0
        newest = record.get("last_seen") or record.get("first_seen") or 0.0
        return float(newest or 0.0)

    ordered = sorted(keys, key=_recency, reverse=True)  # newest first
    for key in ordered[max_entries:]:
        state.forget(key)


def record_failure(state, signature: str, job_id: str, reason: str, action: str, at: float | None = None) -> dict:
    """Record one observation of ``signature`` and return the updated row.

    The row is per signature: {count, first_seen, last_seen, sample_job_ids
    (max 5), sample_reasons (max 5), last_action, handlers_seen}. ``action`` is
    the decision taken this time (delivered / retried / needs-review /
    promoted); the first action is the handler the row saw, and a later
    different action is appended so the table shows the full path. The row is
    persisted before the return, and the table is evicted to its bound.
    """
    now = at if at is not None else time.time()
    key = _signature_key(signature)
    row = state.value(key)
    if not isinstance(row, dict):
        row = {"count": 0, "first_seen": now, "last_seen": now, "sample_job_ids": [], "sample_reasons": [], "last_action": "", "handlers_seen": []}
    count = int(row.get("count") or 0) + 1
    first_seen = row.get("first_seen") or now
    last_seen = now
    sample_ids = list(row.get("sample_job_ids") or [])
    sample_reasons = list(row.get("sample_reasons") or [])
    if job_id and job_id not in sample_ids:
        sample_ids.append(job_id)
        sample_ids = sample_ids[-SIGNATURE_SAMPLE_JOBS_MAX:]
    if reason and reason not in sample_reasons:
        sample_reasons.append(reason[:_REASON_CAP])
        sample_reasons = sample_reasons[-SIGNATURE_REASONS_MAX:]
    handlers_seen = list(row.get("handlers_seen") or [])
    if action and action not in handlers_seen:
        handlers_seen.append(action)
        handlers_seen = handlers_seen[-8:]
    updated = {
        "count": count,
        "first_seen": first_seen,
        "last_seen": last_seen,
        "sample_job_ids": sample_ids,
        "sample_reasons": sample_reasons,
        "last_action": action,
        "handlers_seen": handlers_seen,
    }
    state.set_value(key, updated, ttl=SIGNATURE_TTL_SEC)
    _evict_oldest_signatures(state, SIGNATURE_MAX_ENTRIES)
    return updated


def get_signature_row(state, signature: str) -> dict | None:
    """The table row for ``signature``, or None when never observed."""
    row = state.value(_signature_key(signature))
    return row if isinstance(row, dict) else None


def count_within_window(row: dict | None, window_sec: float, now: float) -> int:
    """How many occurrences of the row fall inside ``window_sec`` of ``now``.

    The row carries first/last seen but not a full histogram, so the honest
    bounds are: below the window it is the whole count, and above it the count
    can be no more than itself. A row seen once within the window counts at
    least 1. This is a conservative bound, not a precise histogram, and it is
    conservative in the SAFE direction (it over-counts rather than
    under-counts, so promotion can fire at the threshold rather than later).
    """
    if not row:
        return 0
    count = int(row.get("count") or 0)
    last_seen = float(row.get("last_seen") or 0.0)
    if count <= 0 or last_seen <= 0:
        return 0
    if now - last_seen > window_sec:
        # Last seen is outside the window; nothing recent is provably inside it.
        return 0
    return count


def all_signature_rows(state) -> dict[str, dict]:
    """Every live signature row, keyed by signature."""
    rows: dict[str, dict] = {}
    for key in state.keys(KEY_SIGNATURE + ":"):
        signature = key[len(KEY_SIGNATURE + ":"):]
        row = state.value(key)
        if isinstance(row, dict):
            rows[signature] = row
    return rows


# ── 3. Unknown policy (code, no model) ──────────────────────────────────────


def unknown_policy(
    state,
    job: dict,
    reason: str,
    *,
    commits_reachable_from_origin: bool | None = None,
    at: float | None = None,
) -> dict:
    """The deterministic decision for a signature with no handler row.

    In order:
      a. the row's commits are reachable from origin -> delivered: complete the
         job with ``dirty_worktree``/``unknown_signature`` notes (work that is
         on origin is not thrown away);
      b. else, this signature+job has had fewer than UNKNOWN_RETRY_MAX
         retries -> retry once on a different host, tagged
         ``unknown_signature``, with a durable attempt counter;
      c. else -> needs-review with ``needs_operator: True``, the signature,
         the raw reason and the sample job id in the result.

    ``commits_reachable_from_origin`` is the caller's check (git-level); when
    None the caller does not know, and branch (a) is skipped — the doctor does
    not guess delivery. The decision is recorded in the signature table.
    """
    job_id = str(job.get("id") or "")
    base_kind = str(job.get("kind") or "")
    signature = failure_signature(reason)
    now = at if at is not None else time.time()

    if commits_reachable_from_origin:
        row = record_failure(state, signature, job_id, reason, "delivered", at=now)
        return {
            "action": "delivered",
            "needs_review": False,
            "signature": signature,
            "reason": reason,
            "job_id": job_id,
            "result": {
                "status_note": "dirty_worktree/unknown_signature: commits are on origin; treated as delivered",
                "dirty_worktree": True,
                "unknown_signature": signature,
                "base_kind": base_kind,
                "count": row.get("count"),
            },
        }

    retry_key = f"{KEY_UNKNOWN_RETRY}:{signature}:{job_id}"
    attempts = state.timestamps(retry_key, ttl=SIGNATURE_TTL_SEC)
    if len(attempts) < UNKNOWN_RETRY_MAX:
        state.touch(retry_key, ttl=SIGNATURE_TTL_SEC, at=now)
        # Re-read AFTER touch so the returned count reflects the increment.
        attempts = state.timestamps(retry_key, ttl=SIGNATURE_TTL_SEC)
        row = record_failure(state, signature, job_id, reason, "retried", at=now)
        return {
            "action": "retried",
            "needs_review": False,
            "signature": signature,
            "reason": reason,
            "job_id": job_id,
            "result": {
                "status_note": f"unknown_signature {signature}: retry {len(attempts)}/{UNKNOWN_RETRY_MAX} on a different host",
                "unknown_signature": signature,
                "unknown_signature_retry": True,
                "base_kind": base_kind,
                "attempts_used": len(attempts),
            },
        }

    row = record_failure(state, signature, job_id, reason, "needs-review", at=now)
    return {
        "action": "needs-review",
        "needs_review": True,
        "needs_operator": True,
        "signature": signature,
        "reason": reason,
        "job_id": job_id,
        "result": {
            "status_note": f"unknown_signature {signature}: retry budget exhausted; operator review required",
            "needs_operator": True,
            "unknown_signature": signature,
            "raw_reason": reason,
            "sample_job_id": job_id,
            "base_kind": base_kind,
            "count": row.get("count"),
        },
    }


# ── 4. Gap promotion ────────────────────────────────────────────────────────


def _gap_job_key(signature: str) -> str:
    return f"{KEY_SIGNATURE}:gap:{signature}"


def check_gap_promotion(
    state,
    signature: str,
    *,
    now: float | None = None,
    post_message=None,
    open_job=None,
) -> dict:
    """Promote an UNHANDLED signature to a named, counted, actionable gap.

    Fires only when the signature has no handler row AND its count within
    UNKNOWN_PROMOTE_WINDOW_SEC is >= UNKNOWN_PROMOTE_COUNT. When it fires:
      a. post ONE ``doctor.classifier_gap`` message with the signature, count,
         first/last seen, up to 3 sample job ids and the raw reasons;
      b. open ONE ``needs-review`` job of kind ``classifier-gap:<signature>``,
         durably deduped (at most one per signature per 7 d), whose
         description carries the samples and asks for a handler + test.

    It NEVER fires for a signature that has a handler row. ``post_message`` and
    ``open_job`` are the bus hooks (injected so tests stay off the live bus).
    Returns {"promoted": bool, ...}.
    """
    if signature_is_handled(signature):
        return {"promoted": False, "reason": "signature already has a handler row"}

    now = now if now is not None else time.time()
    row = get_signature_row(state, signature)
    count = count_within_window(row, UNKNOWN_PROMOTE_WINDOW_SEC, now)
    if count < UNKNOWN_PROMOTE_COUNT:
        return {"promoted": False, "reason": f"count {count} below threshold {UNKNOWN_PROMOTE_COUNT}"}

    gap_key = _gap_job_key(signature)
    if state.seen(gap_key, ttl=UNKNOWN_PROMOTE_WINDOW_SEC):
        return {"promoted": False, "reason": "gap job already open for this signature in the window"}

    row = row or {}
    samples = list(row.get("sample_job_ids") or [])[:3]
    reasons = list(row.get("sample_reasons") or [])
    message_payload = {
        "signature": signature,
        "count": count,
        "first_seen": row.get("first_seen"),
        "last_seen": row.get("last_seen"),
        "sample_job_ids": samples,
        "raw_reasons": reasons,
    }
    if post_message is not None:
        post_message("doctor.classifier_gap", message_payload)

    kind = signature_job_kind(signature)
    description = (
        f"classifier gap: signature {signature!r} has no handler row and has occurred "
        f"{count}x in the last {int(UNKNOWN_PROMOTE_WINDOW_SEC / 3600)}h. "
        f"Open a handler + test in remediation_manifest.py for this shape.\n"
        f"sample job ids: {', '.join(samples) or '(none)'}\n"
        f"raw reasons:\n" + "\n".join(f"  - {r}" for r in reasons)
    )
    job_ref = None
    if open_job is not None:
        job_ref = open_job(kind=kind, status="needs-review", description=description)

    # Dedup is recorded AFTER the message + job: if either hook raises, the
    # signature is not marked promoted and the next cycle retries the gap.
    state.mark(gap_key, ttl=UNKNOWN_PROMOTE_WINDOW_SEC, at=now)
    record_failure(state, signature, samples[0] if samples else "", "", "promoted", at=now)
    return {"promoted": True, "kind": kind, "count": count, "job": job_ref, "message": message_payload}


def signature_is_handled(signature: str) -> bool:
    """Whether the classifier has a deterministic row for this signature."""
    return handler_for_signature(signature) is not None


# ── 5. Coverage report ──────────────────────────────────────────────────────


def coverage_report(
    state,
    *,
    now: float | None = None,
    coverage_dir: Path | None = None,
    post_message=None,
) -> dict:
    """Write the coverage file and post the summary.

    Every COVERAGE_INTERVAL_SEC (and on a ``doctor.coverage.request``) the
    doctor calls this. It writes ``<coverage_dir>/coverage-<UTC date>.json``
    with the top COVERAGE_TOP_N signatures by count, each marked
    ``handled`` / ``unknown`` / ``promoted``, and posts ``doctor.coverage``
    with the counts and the top COVERAGE_POST_TOP_N.
    """
    now = now if now is not None else time.time()
    utc_date = datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y-%m-%d")
    rows = all_signature_rows(state)

    def _status(signature: str) -> str:
        if signature_is_handled(signature):
            return "handled"
        row = rows.get(signature) or {}
        if row.get("last_action") == "promoted" or row.get("handlers_seen") and "promoted" in row.get("handlers_seen", []):
            return "promoted"
        return "unknown"

    entries = []
    for signature, row in rows.items():
        entries.append(
            {
                "signature": signature,
                "count": int(row.get("count") or 0),
                "first_seen": row.get("first_seen"),
                "last_seen": row.get("last_seen"),
                "status": _status(signature),
            }
        )
    entries.sort(key=lambda e: (-e["count"], e["signature"]))
    top = entries[:COVERAGE_TOP_N]

    payload = {
        "date": utc_date,
        "generated_at": now,
        "total_signatures": len(rows),
        "counts": {
            "handled": sum(1 for e in top if e["status"] == "handled"),
            "unknown": sum(1 for e in top if e["status"] == "unknown"),
            "promoted": sum(1 for e in top if e["status"] == "promoted"),
        },
        "top": top,
    }

    directory = Path(coverage_dir) if coverage_dir is not None else COVERAGE_DIR
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"coverage-{utc_date}.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")

    if post_message is not None:
        post_message("doctor.coverage", {**payload, "top": top[:COVERAGE_POST_TOP_N]})
    payload["path"] = str(path)
    return payload


def coverage_due(state, now: float | None = None) -> bool:
    """Whether the periodic coverage report is due (COVERAGE_INTERVAL_SEC)."""
    now = now if now is not None else time.time()
    last = state.last_mark(f"{KEY_COVERAGE}:last")
    if last is None:
        return True
    return (now - last) >= COVERAGE_INTERVAL_SEC


def mark_coverage_done(state, now: float | None = None) -> None:
    """Record that the coverage report ran, so the interval is measured from it."""
    state.mark(f"{KEY_COVERAGE}:last", ttl=COVERAGE_INTERVAL_SEC, at=now)


def handle_coverage_request(state) -> dict:
    """The on-demand path: a ``doctor.coverage.request`` message."""
    return coverage_report(state)
