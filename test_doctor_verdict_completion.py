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
* at most one attempt per row per process, enforced by a durable marker in
  the doctor's STATE store.

No live bus: every HTTP call is monkeypatched and recorded.
"""

from __future__ import annotations

import json

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


# ── one attempt per row per process (durable marker) ───────────────────────


def test_at_most_one_attempt_per_row_per_process(monkeypatch):
    jobs = [_job(jid="job-once")]
    summary, patches, _ = _sweep(monkeypatch, jobs)
    assert summary == {"scanned": 1, "completed": 1, "skipped": 0, "no_action": 0}
    assert len(patches) == 1
    # The durable marker landed in STATE before the PATCH.
    assert doctor.STATE.seen(doctor.KEY_VERDICT_COMPLETION + ":job-once", ttl=doctor.VERDICT_COMPLETION_TTL)

    # A second pass in the SAME process: marker seen -> zero further work.
    patches.clear()
    summary2, _, messages2 = _sweep(monkeypatch, jobs)
    assert summary2 == {"scanned": 1, "completed": 0, "skipped": 1, "no_action": 0}
    assert patches == []
    assert messages2 == []


def test_marker_blocks_second_attempt_even_if_first_patch_failed(monkeypatch):
    """A failed PATCH still counts as the one attempt for this process.

    The marker is recorded BEFORE the bus call, so a crash or 4xx between
    marker and PATCH cannot produce a second PATCH in the same process; the
    next process gets its own single attempt.
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
    assert doctor.STATE.seen(doctor.KEY_VERDICT_COMPLETION + ":job-fail", ttl=doctor.VERDICT_COMPLETION_TTL)

    calls["n"] = 0
    summary2 = doctor.complete_stuck_verdict_jobs([job])
    assert summary2["skipped"] == 1
    assert calls["n"] == 0


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
