"""H1: ``scripts/explain_job.py`` — the diagnosis, entirely offline.

Covered, per the brief: one case per ``loop_outcome`` class with the doctor's
decision it maps to, and the missing/garbled data the tool must survive (a row
with no ``result``, a JSON-string ``result``, a garbled event, an event that
belongs to another job). Plus the two hard rules: with ``--row``/``--events``
the tool never reaches the network, and no token ever reaches stdout/stderr.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
import urllib.request

import pytest

from hive.workers import loop_outcome

ROOT = pathlib.Path(__file__).parent
ROW = ROOT / "tests" / "fixtures" / "explain_job_row.sample.json"
EVENTS = ROOT / "tests" / "fixtures" / "explain_job_events.sample.json"
JOB_ID = "01a17c10-loop-timeout"


def _load_explain():
    """Load scripts/explain_job.py without installing anything."""
    spec = importlib.util.spec_from_file_location("explain_job", ROOT / "scripts" / "explain_job.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mod = _load_explain()


def _row(**overrides) -> dict:
    row = json.loads(ROW.read_text())
    row.update(overrides)
    return row


def _failed_with(reason: str | None) -> dict:
    """A failed row whose loop_result carries ``reason`` as teardown reason."""
    row = _row(result={"loop_result": {"teardown": {"reason": reason}} if reason else {}})
    return row


# ── one case per outcome class ──────────────────────────────────────────────


@pytest.mark.parametrize(
    ("reason", "expected_action"),
    [
        ("approved", "skip"),
        ("reviewer_unavailable", "requeue"),
        ("author_failed", "requeue"),
        ("timeout", "requeue"),
        ("crash", "requeue"),
        ("exhausted", "needs_review"),
        ("nothing_to_commit", "needs_review"),
    ],
)
def test_every_teardown_reason_prints_its_class_and_action(reason, expected_action):
    out = mod.explain(_failed_with(reason), None)
    expected_class = loop_outcome._TEARDOWN_TO_CLASS[reason]
    assert f"class:           {expected_class}" in out
    assert f"doctor:          {expected_action} —" in out


def test_unknown_when_there_is_nothing_to_go_on():
    out = mod.explain(_failed_with(None), None)
    assert "class:           unknown" in out
    assert "doctor:          default —" in out


def test_class_comes_from_the_events_when_the_row_says_nothing():
    events = [{"phase": "loop_start"}, {"phase": "teardown", "reason": "reviewer_unavailable"}]
    out = mod.explain(_failed_with(None), events)
    assert f"class:           {loop_outcome.REVIEWER_UNAVAILABLE}" in out
    assert "doctor:          requeue —" in out


def test_a_trail_without_teardown_is_a_crash():
    out = mod.explain(_failed_with(None), [{"phase": "loop_start"}, {"phase": "author_start"}])
    assert f"class:           {loop_outcome.CRASH}" in out


def test_requeue_cap_and_reviewer_health_are_visible():
    row = _failed_with("reviewer_unavailable")
    capped = mod.explain(row, None, requeues=loop_outcome.MAX_AUTO_REQUEUES)
    assert "doctor:          needs_review —" in capped
    assert f"requeues spent:  {loop_outcome.MAX_AUTO_REQUEUES} of {loop_outcome.MAX_AUTO_REQUEUES}" in capped
    down = mod.explain(row, None, requeues=0, reviewer_healthy=False)
    assert "doctor:          hold —" in down


def test_a_row_that_is_not_failed_says_so():
    out = mod.explain(_row(status="done"), None)
    assert "the doctor only sweeps failed rows" in out


# ── missing and garbled data ────────────────────────────────────────────────


def test_a_row_without_result_still_explains_itself():
    row = _row()
    row.pop("result")
    out = mod.explain(row, None)
    assert "status:          failed" in out
    assert "(nothing recorded on this row)" in out
    assert "workspace:       (not preserved)" in out


def test_a_json_string_result_is_read_not_crashed_on():
    row = _row(result='{"loop_result": {"teardown": {"reason": "timeout"}}}')
    out = mod.explain(row, None)
    assert f"class:           {loop_outcome.TIMEOUT}" in out


def test_a_garbled_event_and_a_foreign_job_event_are_dropped_not_fatal():
    events = mod.load_events(EVENTS, JOB_ID)
    # 7 rows in the file: a garbled content, a non-dict content and another
    # job's teardown must all be gone, and the rest ordered by seq.
    assert [e.get("phase") for e in events] == ["loop_start", "author_start", "review", "teardown"]
    out = mod.explain(_row(), events)
    assert "timeline (4 events)" in out
    assert "reviewer_unavailable: no route answered" in out


def test_events_that_are_not_a_list_are_a_message_not_a_traceback(tmp_path, capsys):
    path = tmp_path / "events.json"
    path.write_text(json.dumps({"events": 5}))
    with pytest.raises(ValueError, match="must be an event list"):
        mod.load_events(path, JOB_ID)
    path.write_text("{not json")
    with pytest.raises(ValueError, match="not valid JSON"):
        mod.load_events(path, JOB_ID)
    with pytest.raises(ValueError, match="cannot read"):
        mod.load_events(tmp_path / "missing.json", JOB_ID)


def test_a_row_file_without_this_job_is_refused(tmp_path):
    path = tmp_path / "row.json"
    path.write_text(json.dumps({"jobs": [{"id": "another-job"}]}))
    with pytest.raises(ValueError, match="no job " + JOB_ID):
        mod.load_row(path, JOB_ID)
    path.write_text(json.dumps([1, 2]))
    with pytest.raises(ValueError, match="every entry must be a job object"):
        mod.load_row(path, JOB_ID)


def test_every_outcome_class_survives_a_row_with_no_events_at_all():
    """All eight classes, straight through the printed report."""
    seen = set()
    for reason in (None, *loop_outcome._TEARDOWN_TO_CLASS):
        out = mod.explain(_failed_with(reason), None)
        line = next(row_line for row_line in out.splitlines() if row_line.startswith("  class:"))
        seen.add(line.split()[1])
    assert seen == set(loop_outcome._TEARDOWN_TO_CLASS.values()) | {loop_outcome.UNKNOWN}


# ── the hard rules ──────────────────────────────────────────────────────────


def test_offline_run_never_touches_the_network(monkeypatch, tmp_path, capsys):
    """--row/--events replace the bus and MNEMOS. Proven, not promised."""

    def _forbidden(*_a, **_k):  # pragma: no cover - must never run
        raise AssertionError("explain_job reached the network")

    monkeypatch.setattr(urllib.request, "urlopen", _forbidden)
    monkeypatch.setenv("HIVE_URL", "http://bus.example.invalid:5005")
    monkeypatch.setenv("MNEMOS_URL", "http://mnemos.example.invalid:5002")
    monkeypatch.setenv("MNEMOS_TOKEN", "would-be-used")
    assert mod.main([JOB_ID, "--row", str(ROW), "--events", str(EVENTS)]) == 0
    out = capsys.readouterr().out
    assert f"job {JOB_ID}" in out
    assert "loop outcome" in out


def test_no_token_reaches_the_report(monkeypatch, capsys):
    monkeypatch.setenv("MNEMOS_TOKEN", "sekrit-token-value")
    monkeypatch.setenv("HIVE_TOKEN", "sekrit-token-value")
    assert mod.main([JOB_ID, "--row", str(ROW), "--events", str(EVENTS)]) == 0
    captured = capsys.readouterr()
    assert "sekrit" not in captured.out
    assert "sekrit" not in captured.err


def test_no_configured_bus_is_a_message_not_a_traceback(monkeypatch, capsys):
    monkeypatch.delenv("HIVE_URL", raising=False)
    assert mod.main([JOB_ID]) == 2
    assert "HIVE_URL is not set" in capsys.readouterr().err


def test_all_six_sections_are_printed(capsys):
    assert mod.main([JOB_ID, "--row", str(ROW), "--events", str(EVENTS)]) == 0
    out = capsys.readouterr().out
    for header in (
        f"job {JOB_ID}",
        "loop outcome",
        "timeline (4 events)",
        "reviewer",
        "workspace / branch / commit",
        "failure",
    ):
        assert header in out, header


# ── the facts the operator came for ─────────────────────────────────────────


def test_attempts_and_workspace_facts_come_from_the_row(capsys):
    assert mod.main([JOB_ID, "--row", str(ROW), "--events", str(EVENTS)]) == 0
    out = capsys.readouterr().out
    assert "attempts:        2 (retry_count=1, max_retries=3)" in out
    assert "/var/lib/graeae-hive/workspaces/01a17c10-loop-timeout" in out
    assert "branch:          freebuff/h1-20261029" in out
    assert "remote_verified=no" in out
    assert "failure_reason:  " in out or "failure_reason:" in out


def test_the_reviewer_record_is_printed(capsys):
    assert mod.main([JOB_ID, "--row", str(ROW), "--events", str(EVENTS)]) == 0
    out = capsys.readouterr().out
    assert "model:           nemotron-3-ultra" in out
    assert "verdict:         request_changes" in out
    assert "source:          zoder_verdict" in out
    assert "brief reviews:   1:" in out


def test_an_events_file_supplies_the_timeline_without_mnemos(monkeypatch, capsys):
    monkeypatch.delenv("MNEMOS_URL", raising=False)
    monkeypatch.delenv("MNEMOS_TOKEN", raising=False)
    assert mod.main([JOB_ID, "--row", str(ROW), "--events", str(EVENTS)]) == 0
    assert "t=2026-10-29T17:00:00Z" in capsys.readouterr().out


def test_no_events_available_says_why(monkeypatch, capsys):
    monkeypatch.delenv("MNEMOS_URL", raising=False)
    monkeypatch.delenv("MNEMOS_TOKEN", raising=False)
    assert mod.main([JOB_ID, "--row", str(ROW)]) == 0
    assert "loop events unavailable" in capsys.readouterr().out
