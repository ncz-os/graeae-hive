"""Verdict-completion recovery for stuck codex_delivery_failed rows (2026-10-27).

The codex delivery path can hold a needs-review row with
``failure_reason=codex_delivery_failed`` (e.g. "produced no workspace edits")
even though the review itself succeeded and its structured verdict JSON is
already sitting in the row's ``result.tail``. The doctor's needs-review pass
completes exactly that row: it extracts the LAST verdict JSON object from the
tail and PATCHes the row to ``done``.

This module covers the four guarantees the pass owes:

* the extraction tolerates leading prose and a trailing "N tokens used" line
  and picks the LAST object with a ``verdict`` key (not the last ``{``);
* a verdict row produces EXACTLY ONE ``done`` PATCH carrying the exact result
  fields and the row's own ``claimed_by``;
* a row without a parseable verdict gets NO action (no PATCH, no purge, no
  retry) — it is left for the failure-matrix retry handler;
* a row that already has ``result.verdict`` is skipped (idempotent);
* a successful completion is done exactly once, enforced by a durable
  success marker in the doctor's STATE store, written only AFTER the bus
  accepted the PATCH (so a restart never re-PATCHes a completed row);
* a FAILED PATCH leaves only a short backoff marker, never the 24h
  completion window, so the next sweep retries it — in the same process
  once the backoff lapses, and in a fresh process reading the same durable
  state.

No live bus: every HTTP call is monkeypatched and recorded.
"""

from __future__ import annotations

import json
import time

from hive.workers import doctor


HEAD40 = "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678"
HEAD64 = "ab" * 32
CLAIMANT = "urn:agent:zoder:cerberus:worker-1"

VERDICT_OBJ = {
    "verdict": "pass",
    "confidence": 0.92,
    "model": "qwen38",
    "head": HEAD40,
    "findings": [],
}

# Leading prose (including a decoy object WITHOUT a verdict key), the
# pretty-printed verdict JSON, then the driver's trailing tokens-used line.
TAIL = (
    "[2026-10-27T18:00:00] driver starting review:graeae-hive-x\n"
    'log noise with a decoy object {"note": "not a verdict"} before the json\n'
    "Review complete. Structured result follows:\n"
    + json.dumps(VERDICT_OBJ, indent=2)
    + "\n12345 tokens used\n"
)


def _job(jid="job-v1", status="needs-review", claimed_by=CLAIMANT, result=None):
    return {
        "id": jid,
        "kind": "review:graeae-hive-x",
        "status": status,
        "claimed_by": claimed_by,
        "repo": "graeae-hive",
        "result": result
        if result is not None
        else {
            "failure_reason": "codex_delivery_failed",
            "error": "delivery blocked: codex produced no workspace edits to commit",
            "tail": TAIL,
        },
    }


def _sweep(monkeypatch, jobs):
    """Run the completion pass with all bus traffic captured and no noise."""
    patches, messages = [], []

    def _http(method, path, body=None, timeout=15.0):
        patches.append((method, path, body))
        return 200, {}

    monkeypatch.setattr(doctor, "_http", _http)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda topic, payload: messages.append((topic, payload)))
    return doctor.complete_stuck_verdict_jobs(jobs), patches, messages


# ── extraction ──────────────────────────────────────────────────────────────


def test_tail_with_leading_prose_and_trailing_tokens_used(monkeypatch):
    got = doctor.extract_verdict_from_tail(TAIL)
    assert got == VERDICT_OBJ


def test_last_verdict_object_wins():
    tail = 'lead {"verdict": "first"}\n{"verdict": "second", "head": "x"}\n7 tokens used'
    assert doctor.extract_verdict_from_tail(tail) == {"verdict": "second", "head": "x"}


def test_pretty_printed_json_with_nested_objects():
    obj = {"verdict": "pass", "findings": [{"severity": "low", "note": "nit"}]}
    tail = "prose before\n" + json.dumps(obj, indent=2) + "\n9 tokens used"
    assert doctor.extract_verdict_from_tail(tail) == obj


def test_no_verdict_key_anywhere_is_none():
    assert doctor.extract_verdict_from_tail('{"note": "no verdict key"}\n3 tokens used') is None


def test_malformed_json_is_none_never_raises():
    assert doctor.extract_verdict_from_tail("{not json at all") is None
    assert doctor.extract_verdict_from_tail("just prose, no json") is None
    assert doctor.extract_verdict_from_tail("") is None
    assert doctor.extract_verdict_from_tail(None) is None


def test_plain_json_stream_is_parsed_whole():
    assert doctor.extract_verdict_from_tail(json.dumps(VERDICT_OBJ)) == VERDICT_OBJ


# ── the done PATCH: exact fields and claimed_by ────────────────────────────


def test_verdict_row_produces_exactly_one_done_patch(monkeypatch):
    summary, patches, messages = _sweep(monkeypatch, [_job()])
    assert summary == {"scanned": 1, "completed": 1, "skipped": 0, "no_action": 0}
    assert len(patches) == 1
    method, path, body = patches[0]
    assert method == "PATCH"
    assert path == "/v1/jobs/job-v1"
    assert body["status"] == "done"
    assert body["claimed_by"] == CLAIMANT  # copied from the row
    result = body["result"]
    finished_at = result["finished_at"]
    assert isinstance(finished_at, float) and finished_at > 0
    expected = {
        "summary": {
            "model": "qwen38",
            "verdict": "pass",
            "confidence": 0.92,
            "head": HEAD40,
        },
        "deliverable": VERDICT_OBJ,
        "head_sha": HEAD40,
        "commits": [HEAD40],
        "files_changed": ["(none: read-only review)"],
        "worktree_clean": True,
        "verdict": VERDICT_OBJ,
        "verdict_json": json.dumps(VERDICT_OBJ, sort_keys=True),
        "delivery_note": doctor.build_verdict_completion_result(_job(), VERDICT_OBJ, 1.0)[
            "delivery_note"
        ],
        "finished_at": finished_at,
    }
    assert result == expected


def test_unclaimed_row_patch_carries_no_claimed_by(monkeypatch):
    summary, patches, _ = _sweep(monkeypatch, [_job(claimed_by=None)])
    assert summary["completed"] == 1
    method, path, body = patches[0]
    assert "claimed_by" not in body
    assert body["status"] == "done"


def test_head_only_when_40_or_64_hex(monkeypatch):
    tail64 = json.dumps({**VERDICT_OBJ, "head": HEAD64}) + "\n2 tokens used"
    job64 = _job(jid="job-v2", result={"failure_reason": "codex_delivery_failed", "tail": tail64})
    summary, patches, _ = _sweep(monkeypatch, [job64])
    assert summary["completed"] == 1
    assert patches[0][2]["result"]["head_sha"] == HEAD64
    assert patches[0][2]["result"]["commits"] == [HEAD64]

    tail_bad = json.dumps({**VERDICT_OBJ, "head": "not-a-sha"})
    job_bad = _job(jid="job-v3", result={"failure_reason": "codex_delivery_failed", "tail": tail_bad})
    summary, patches, _ = _sweep(monkeypatch, [job_bad])
    assert summary["completed"] == 1
    result = patches[0][2]["result"]
    assert "head_sha" not in result
    assert "commits" not in result
    # summary still carries the raw head value
    assert result["summary"]["head"] == "not-a-sha"


def test_one_verdict_completed_message_per_completed_row(monkeypatch):
    summary, _, messages = _sweep(monkeypatch, [_job(jid="a"), _job(jid="b")])
    assert summary["completed"] == 2
    assert [m[0] for m in messages] == ["doctor.verdict_completed", "doctor.verdict_completed"]
    assert [m[1]["job_id"] for m in messages] == ["a", "b"]
    assert all(m[1]["host"] == doctor.AGENT_HOST for m in messages)


# ── no action cases ─────────────────────────────────────────────────────────


def test_no_parseable_verdict_gets_no_action(monkeypatch):
    job = _job(jid="job-nv", result={"failure_reason": "codex_delivery_failed",
                                     "error": "delivery blocked: codex produced no workspace edits to commit",
                                     "tail": "driver log: review ran, no structured output\n5 tokens used"})
    summary, patches, messages = _sweep(monkeypatch, [job])
    assert summary == {"scanned": 1, "completed": 0, "skipped": 0, "no_action": 1}
    assert patches == []
    assert messages == []
    # no durable attempt recorded either: the row stays live for other handlers
    assert not doctor.STATE.seen(
        doctor.KEY_VERDICT_COMPLETION + ":job-nv", ttl=doctor.VERDICT_COMPLETION_TTL
    )


def test_existing_verdict_gets_no_action(monkeypatch):
    job = _job(
        jid="job-ev",
        result={
            "failure_reason": "codex_delivery_failed",
            "tail": TAIL,
            "verdict": {"verdict": "pass", "confidence": 0.9, "findings": []},
            "deliverable": "already completed earlier",
        },
    )
    summary, patches, messages = _sweep(monkeypatch, [job])
    assert summary == {"scanned": 1, "completed": 0, "skipped": 1, "no_action": 0}
    assert patches == []
    assert messages == []


def test_malformed_json_in_tail_gets_no_action(monkeypatch):
    job = _job(jid="job-mj", result={"failure_reason": "codex_delivery_failed",
                                     "tail": "{broken json {{{\n1 tokens used"})
    summary, patches, messages = _sweep(monkeypatch, [job])
    assert summary["no_action"] == 1
    assert patches == []
    assert messages == []


def test_other_rows_are_untouched(monkeypatch):
    jobs = [
        _job(jid="ok", result={"failure_reason": "completion_evidence_missing", "tail": TAIL}),
        _job(jid="ok2", status="failed"),
        _job(jid="ok3", result={"failure_reason": "budget_exceeded"}),
    ]
    summary, patches, messages = _sweep(monkeypatch, jobs)
    assert summary["completed"] == 0
    assert patches == []
    assert messages == []


# ── completion dedupe (durable success marker, written after the 200) ──────


def test_at_most_one_attempt_per_row_per_process(monkeypatch):
    jobs = [_job(jid="job-once")]
    summary, patches, _ = _sweep(monkeypatch, jobs)
    assert summary == {"scanned": 1, "completed": 1, "skipped": 0, "no_action": 0}
    assert len(patches) == 1
    # The durable SUCCESS marker is in STATE after the bus accepted the PATCH.
    assert doctor.STATE.seen(doctor.KEY_VERDICT_COMPLETION + ":job-once", ttl=doctor.VERDICT_COMPLETION_TTL)

    # A second pass in the SAME process: marker seen -> zero further work.
    patches.clear()
    summary2, _, messages2 = _sweep(monkeypatch, jobs)
    assert summary2 == {"scanned": 1, "completed": 0, "skipped": 1, "no_action": 0}
    assert patches == []
    assert messages2 == []


def test_failed_patch_gets_short_backoff_not_full_ttl(monkeypatch):
    """A failed PATCH must NOT burn the 24h completion window.

    The durable completion marker is written only after a successful PATCH;
    a failed one leaves only the short retry backoff, so the marker is not
    ``seen`` under ``VERDICT_COMPLETION_TTL`` even in this same process.
    """
    calls = {"n": 0}

    def _http(method, path, body=None, timeout=15.0):
        calls["n"] += 1
        return 503, {"error": "bus down"}

    monkeypatch.setattr(doctor, "_http", _http)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    job = _job(jid="job-fail")
    summary = doctor.complete_stuck_verdict_jobs([job])
    assert summary["no_action"] == 1
    assert calls["n"] == 1
    success_key = doctor.KEY_VERDICT_COMPLETION + ":job-fail"
    backoff_key = doctor.KEY_VERDICT_COMPLETION_RETRY + ":job-fail"
    # Not completed: no full-TTL success marker, so the row is still live for
    # the next sweep (this process, or a fresh one after a restart).
    assert not doctor.STATE.seen(success_key, ttl=doctor.VERDICT_COMPLETION_TTL)
    # ...but a short backoff marker WAS recorded so we do not hammer a down bus
    # on every pass.
    assert doctor.STATE.seen(backoff_key, ttl=doctor.VERDICT_COMPLETION_RETRY_TTL)

    # Within the backoff the row is still skipped (no extra bus traffic):
    summary2 = doctor.complete_stuck_verdict_jobs([job])
    assert summary2["skipped"] == 1
    assert calls["n"] == 1  # unchanged: the second pass made no new call

    # Once the backoff has lapsed (simulated by advancing the store's clock),
    # the same process retries the row.
    store = doctor.STATE
    real_time = store._time
    store._time = lambda: real_time() + doctor.VERDICT_COMPLETION_RETRY_TTL + 1
    try:
        calls["n"] = 0
        summary3 = doctor.complete_stuck_verdict_jobs([job])
        assert summary3["no_action"] == 1
        assert calls["n"] == 1  # retried after the backoff lapsed
    finally:
        store._time = real_time


def test_failed_patch_is_retried_by_a_fresh_process_from_the_durable_store(
    monkeypatch, tmp_path
):
    """A failed PATCH must not block recovery across a restart for the TTL.

    Simulates a process restart by swapping in a brand-new in-memory store on
    top of the SAME durable state the failed pass wrote. The row must still be
    eligible for the next sweep (no live 24h completion marker), and a fresh
    success completes it exactly once.
    """
    state_path = tmp_path / "durable-state.json"
    from hive.workers.doctor_state import DurableStateStore, FileBackend, MemoryBackend

    # Process 1: file-backed durable store.
    monkeypatch.setattr(
        doctor, "STATE", DurableStateStore(FileBackend(str(state_path)), refresh_secs=0.0)
    )
    calls = {"n": 0}

    def _http(method, path, body=None, timeout=15.0):
        calls["n"] += 1
        return 500, {"error": "internal server error"}

    monkeypatch.setattr(doctor, "_http", _http)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    job = _job(jid="job-r")
    summary1 = doctor.complete_stuck_verdict_jobs([job])
    assert summary1["no_action"] == 1
    assert calls["n"] == 1

    # A failed pass must leave only a SHORT backoff record in the durable store,
    # never a live completion marker (which would block the next process for the
    # whole TTL).
    assert state_path.exists()
    durable_ns = doctor.STATE.namespace
    success_key = doctor.KEY_VERDICT_COMPLETION + ":job-r"
    backoff_key = doctor.KEY_VERDICT_COMPLETION_RETRY + ":job-r"
    with open(state_path) as fh:
        blob = json.load(fh)
    durable_records = blob.get(durable_ns, {})
    # The backoff record IS durable; the success record is NOT.
    assert backoff_key in durable_records
    assert success_key not in durable_records
    backoff_record = durable_records[backoff_key]
    newest = max(
        float(v)
        for v in [backoff_record.get("at"), *backoff_record.get("timestamps", [])]
        if v is not None
    )
    assert (time.time() - newest) <= doctor.VERDICT_COMPLETION_RETRY_TTL
    assert not doctor.STATE.seen(success_key, ttl=doctor.VERDICT_COMPLETION_TTL)

    # A row whose ONLY durable record is a backoff must NOT be treated as
    # completed by the next process: a fresh file-backed store sees no success
    # marker in the durable file.
    fresh_probe = DurableStateStore(FileBackend(str(state_path)), refresh_secs=0.0)
    assert not fresh_probe.seen(success_key, ttl=doctor.VERDICT_COMPLETION_TTL)

    # Process 2: fresh store reading the SAME durable file, clock advanced past
    # the backoff so the retry is due.
    fresh = DurableStateStore(FileBackend(str(state_path)), refresh_secs=0.0)
    real_time = fresh._time
    fresh._time = lambda: real_time() + doctor.VERDICT_COMPLETION_RETRY_TTL + 5
    monkeypatch.setattr(doctor, "STATE", fresh)

    def _http_ok(method, path, body=None, timeout=15.0):
        calls["n"] += 1
        return 200, {}

    monkeypatch.setattr(doctor, "_http", _http_ok)
    messages: list = []
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda t, p: messages.append((t, p)))
    summary2 = doctor.complete_stuck_verdict_jobs([job])
    assert summary2["completed"] == 1
    assert calls["n"] == 2  # exactly one retry after the failure
    assert [m[0] for m in messages] == ["doctor.verdict_completed"]
    # The success now left the full-TTL marker in the durable store.
    assert doctor.STATE.seen(success_key, ttl=doctor.VERDICT_COMPLETION_TTL)
    with open(state_path) as fh:
        blob2 = json.load(fh)
    record2 = blob2.get(durable_ns, {}).get(success_key, {})
    newest2 = max(
        float(v)
        for v in [record2.get("at"), *record2.get("timestamps", [])]
        if v is not None
    )
    assert (time.time() - newest2) <= doctor.VERDICT_COMPLETION_TTL


def test_successful_completion_is_done_exactly_once_across_restarts(
    monkeypatch, tmp_path
):
    """A successful completion must PATCH exactly once, including after a
    restart: the durable success marker (written only after the 200) blocks
    every later attempt in this and any fresh process."""
    state_path = tmp_path / "durable-state.json"
    from hive.workers.doctor_state import DurableStateStore, FileBackend, MemoryBackend

    monkeypatch.setattr(
        doctor, "STATE", DurableStateStore(FileBackend(str(state_path)), refresh_secs=0.0)
    )
    calls = {"n": 0}

    def _http_ok(method, path, body=None, timeout=15.0):
        calls["n"] += 1
        return 200, {}

    monkeypatch.setattr(doctor, "_http", _http_ok)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    job = _job(jid="job-once-r")
    summary1 = doctor.complete_stuck_verdict_jobs([job])
    assert summary1 == {"scanned": 1, "completed": 1, "skipped": 0, "no_action": 0}
    assert calls["n"] == 1

    # Same process, second pass: skipped, no second PATCH.
    summary2 = doctor.complete_stuck_verdict_jobs([job])
    assert summary2 == {"scanned": 1, "completed": 0, "skipped": 1, "no_action": 0}
    assert calls["n"] == 1

    # Fresh process on the same durable state: still skipped, still no PATCH.
    fresh = DurableStateStore(FileBackend(str(state_path)), refresh_secs=0.0)
    monkeypatch.setattr(doctor, "STATE", fresh)
    summary3 = doctor.complete_stuck_verdict_jobs([job])
    assert summary3 == {"scanned": 1, "completed": 0, "skipped": 1, "no_action": 0}
    assert calls["n"] == 1  # exactly one PATCH total, across both processes


def test_dry_run_completes_without_any_bus_traffic(monkeypatch):
    monkeypatch.setattr(doctor, "VERDICT_COMPLETION_DRY_RUN", True)
    summary, patches, messages = _sweep(monkeypatch, [_job(jid="job-dry")])
    assert summary["completed"] == 1
    assert patches == []  # no PATCH in dry-run
    assert messages == []  # no broadcast either
    # marker still recorded: dry-run is a decision, matching DLQ semantics
    assert doctor.STATE.seen(doctor.KEY_VERDICT_COMPLETION + ":job-dry", ttl=doctor.VERDICT_COMPLETION_TTL)


# ── candidate eligibility ───────────────────────────────────────────────────


def test_error_mentioning_no_workspace_edits_is_a_candidate():
    job = _job(result={"error": "delivery blocked: codex produced no workspace edits to commit",
                       "tail": TAIL})
    assert doctor._is_verdict_completion_candidate(job) is True
    assert doctor._row_result_dict(job).get("failure_reason") is None


def test_row_result_tolerates_json_string_column():
    job = _job(result=json.dumps({"failure_reason": "codex_delivery_failed", "tail": TAIL}))
    assert doctor._is_verdict_completion_candidate(job) is True
