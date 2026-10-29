"""F5: the offline replay harness, and the loop_outcome edges it must survive.

Covered here, per the brief: ``loop_result`` with missing fields, events with
out-of-order ``_seq``, MNEMOS answering with a list vs a dict, and a job whose
``result`` is a JSON string. Plus the harness itself: it must never touch the
network or write anything.
"""

from __future__ import annotations

import importlib.util
import io
import json
import pathlib
import urllib.request

import pytest

from hive.workers import loop_outcome

ROOT = pathlib.Path(__file__).parent
FIXTURE = ROOT / "tests" / "fixtures" / "loop_failed_rows.sample.json"


def _load_replay():
    """Load scripts/replay_loop_outcomes.py without installing anything."""
    spec = importlib.util.spec_from_file_location("replay_loop_outcomes", ROOT / "scripts" / "replay_loop_outcomes.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


replay_mod = _load_replay()


# ── the harness ──────────────────────────────────────────────────────────────


def test_replay_classifies_every_sample_row():
    entries = replay_mod.replay(replay_mod.load_rows(FIXTURE))
    got = {(e["id"], e["class"], e["action"]) for e in entries}
    assert got == {
        ("01a17c00-approved", loop_outcome.APPROVED_UNMERGED, "skip"),
        ("01a17c01-timeout-jsonstring", loop_outcome.TIMEOUT, "requeue"),
        ("01a17c02-no-result", loop_outcome.UNKNOWN, "default"),
        ("01a17c03-partial-teardown", loop_outcome.UNKNOWN, "default"),
        ("01a17c04-reviewer-text", loop_outcome.REVIEWER_UNAVAILABLE, "requeue"),
        ("01a17c05-nothing-to-commit", loop_outcome.NOTHING_TO_COMMIT, "needs_review"),
    }


def test_replay_never_touches_the_network(monkeypatch):
    """The harness's whole point: MNEMOS is not consulted, and nothing is written."""

    def _forbidden(*_a, **_k):  # pragma: no cover - must never run
        raise AssertionError("the replay harness reached the network")

    monkeypatch.setattr(urllib.request, "urlopen", _forbidden)
    monkeypatch.setenv("MNEMOS_URL", "http://127.0.0.1:9")
    monkeypatch.setenv("MNEMOS_TOKEN", "would-be-used")
    entries = replay_mod.replay(replay_mod.load_rows(FIXTURE))
    assert len(entries) == 6


def test_reviewer_down_turns_a_requeue_into_a_hold():
    rows = [{"id": "r", "result": {"error": "reviewer_unavailable: down"}}]
    assert replay_mod.replay(rows, reviewer_healthy=False)[0]["action"] == "hold"
    assert replay_mod.replay(rows, reviewer_healthy=True)[0]["action"] == "requeue"


def test_requeue_cap_is_honoured():
    rows = [{"id": "t", "result": {"error": "timeout"}}]
    assert replay_mod.replay(rows, requeues_so_far=0)[0]["action"] == "requeue"
    assert replay_mod.replay(rows, requeues_so_far=loop_outcome.MAX_AUTO_REQUEUES)[0]["action"] == "needs_review"


def test_report_is_one_line_per_row():
    report = replay_mod.format_report(replay_mod.replay(replay_mod.load_rows(FIXTURE)))
    lines = report.splitlines()
    assert len(lines) == 6
    assert all(len(line.split("  ")) == 3 for line in lines)


def test_a_single_row_object_is_accepted(tmp_path):
    path = tmp_path / "one.json"
    path.write_text(json.dumps({"id": "solo", "result": {"error": "timeout"}}))
    assert replay_mod.load_rows(path)[0]["id"] == "solo"


def test_bad_input_is_a_message_not_a_traceback(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("{not json")
    with pytest.raises(ValueError, match="not valid JSON"):
        replay_mod.load_rows(path)
    with pytest.raises(ValueError, match="cannot read"):
        replay_mod.load_rows(tmp_path / "missing.json")
    with pytest.raises(ValueError, match="every row must be an object"):
        path.write_text(json.dumps([1, 2]))
        replay_mod.load_rows(path)


def test_main_exits_nonzero_with_a_usable_message(tmp_path, capsys):
    path = tmp_path / "bad.json"
    path.write_text('{"nope": 1}')
    assert replay_mod.main([str(path)]) == 2
    assert "must be a job list" in capsys.readouterr().err


# ── loop_outcome edges ───────────────────────────────────────────────────────


def test_loop_result_missing_fields_is_unknown_not_an_exception():
    assert loop_outcome.classify({"id": "a", "result": {"loop_result": {}}}, events_fetcher=lambda _i: None) == (
        loop_outcome.UNKNOWN
    )
    assert loop_outcome.classify({"id": "b", "result": {"loop_result": None}}, events_fetcher=lambda _i: None) == (
        loop_outcome.UNKNOWN
    )
    assert (
        loop_outcome.classify(
            {"id": "c", "result": {"loop_result": {"teardown": "not-a-dict"}}}, events_fetcher=lambda _i: None
        )
        == loop_outcome.UNKNOWN
    )


def test_a_json_string_result_is_decoded():
    job = {"id": "j", "result": '{"loop_result": {"teardown": {"reason": "crash"}}}'}
    assert loop_outcome.classify(job, events_fetcher=lambda _i: None) == loop_outcome.CRASH


def test_an_invalid_json_string_result_is_unknown():
    job = {"id": "j2", "result": "{not json"}
    assert loop_outcome.classify(job, events_fetcher=lambda _i: None) == loop_outcome.UNKNOWN


def _fake_mnemos(monkeypatch, body):
    class _Resp:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

        def read(self):
            return json.dumps(body).encode()

    monkeypatch.setenv("MNEMOS_URL", "http://mnemos.invalid")
    monkeypatch.setenv("MNEMOS_TOKEN", "t")
    monkeypatch.setattr(urllib.request, "urlopen", lambda *_a, **_k: _Resp())


def _memories(*seqs):
    return [
        {
            "metadata": {"job_id": "j", "seq": seq, "phase": "teardown"},
            "content": json.dumps({"reason": "timeout", "phase": "teardown"}),
        }
        for seq in seqs
    ]


def test_out_of_order_seq_is_sorted(monkeypatch):
    _fake_mnemos(monkeypatch, {"memories": _memories(3, 1, 2)})
    events = loop_outcome.fetch_loop_events("j")
    assert [e["_seq"] for e in events] == [1, 2, 3]


def test_mnemos_answering_with_a_list_works(monkeypatch):
    _fake_mnemos(monkeypatch, _memories(2, 1))
    events = loop_outcome.fetch_loop_events("j")
    assert [e["_seq"] for e in events] == [1, 2]


def test_a_trail_without_teardown_is_a_crash(monkeypatch):
    _fake_mnemos(
        monkeypatch,
        {"memories": [{"metadata": {"job_id": "j", "seq": 1, "phase": "author"}, "content": json.dumps({"x": 1})}]},
    )
    job = {"id": "j", "result": {}}
    assert loop_outcome.classify(job) == loop_outcome.CRASH


def test_events_for_another_job_are_ignored(monkeypatch):
    body = {
        "memories": [
            {"metadata": {"job_id": "other", "seq": 1, "phase": "teardown"}, "content": json.dumps({"reason": "crash"})}
        ]
    }
    _fake_mnemos(monkeypatch, body)
    assert loop_outcome.fetch_loop_events("j") == []


def test_mnemos_unavailable_returns_none(monkeypatch):
    monkeypatch.delenv("MNEMOS_TOKEN", raising=False)
    assert loop_outcome.fetch_loop_events("j") is None


def test_mnemos_transport_failure_returns_none(monkeypatch):
    def _boom(*_a, **_k):
        raise OSError("connection refused")

    monkeypatch.setenv("MNEMOS_URL", "http://mnemos.invalid")
    monkeypatch.setenv("MNEMOS_TOKEN", "t")
    monkeypatch.setattr(urllib.request, "urlopen", _boom)
    assert loop_outcome.fetch_loop_events("j") is None


def test_a_string_seq_does_not_raise(monkeypatch):
    """Mixed/mistyped ``seq`` must sort or be ignored, never take the sweep down."""
    body = {
        "memories": [
            {"metadata": {"job_id": "j", "seq": "3", "phase": "author"}, "content": json.dumps({"x": 1})},
            {"metadata": {"job_id": "j", "seq": 1, "phase": "teardown"}, "content": json.dumps({"reason": "crash"})},
        ]
    }
    _fake_mnemos(monkeypatch, body)
    events = loop_outcome.fetch_loop_events("j")
    assert events is not None, "a mistyped seq must not make the whole trail unavailable"
    assert loop_outcome.classify({"id": "j", "result": {}}) == loop_outcome.CRASH


def test_an_event_with_unparseable_content_is_skipped(monkeypatch):
    body = {"memories": [{"metadata": {"job_id": "j", "seq": 1, "phase": "teardown"}, "content": "{not json"}]}
    _fake_mnemos(monkeypatch, body)
    assert loop_outcome.fetch_loop_events("j") == []


def test_io_is_only_read(monkeypatch):
    """Nothing in this module opens a file for writing."""
    original = io.open

    def _guard(*args, **kwargs):
        mode = kwargs.get("mode", args[1] if len(args) > 1 else "r")
        if set(mode) & {"w", "a", "+", "x"}:
            raise AssertionError(f"loop_outcome wrote to {args[0]}")
        return original(*args, **kwargs)

    monkeypatch.setattr(io, "open", _guard)
    loop_outcome.classify({"id": "z", "result": {}}, events_fetcher=lambda _i: None)
