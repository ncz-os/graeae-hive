"""The doctor is a deterministic state machine FIRST (operator rule 2026-10-31).

A model is called only for an AMBIGUOUS classification and a short reason. Every
place a model used to be dialled with a prompt the row shape already answers is
now a table lookup in ``hive/workers/doctor_rules.py``:

1. classification by ROW SHAPE -- a missing ``result`` is not a model question,
   a zoder loop teardown reason is not a model question, and the residue a model
   is allowed to judge is a closed enum (``no_action`` / ``release_to_queue`` /
   ``escalate``). No evidence quote, no retry scope, no free-form action;
2. CONSTRAINED output -- the request carries a JSON schema (``response_format``
   for the OpenAI-compatible rungs, ``json_schema`` for llama-server's grammar),
   and a reply that does not parse into the enum is REJECTED so the call moves
   to the next rung. The legacy ``DOCTOR_FALLBACK_*`` rungs inherit the same
   constraint;
3. the default chain order, slot aware, with no restored light model;
4. one JSONL ledger line per doctor model call (rung, phase, latency, outcome,
   skipped-because) and the restoration alert: more than 30 model calls in an
   hour, or the doctor's free slot found taken more than 3 times in an hour,
   emits ``doctor.light_model_needed`` ONCE.

The row fixtures the golden tests pin are the SHAPES of
``~/Projects/loopprog-20261029/triage-evidence/*.json`` (no such file exists on
every host, so the fixtures below are the four row shapes plus the two residue
rows verbatim). For every one of them the deterministic table must return a
decision -- and the model must never be dialled.
"""

from __future__ import annotations

import io
import json
import logging
import re
from pathlib import Path

import pytest

from hive.workers import doctor_rules

# ── the real row shapes ────────────────────────────────────────────────────


def _row(reason=None, *, result=..., kind="fix:thing", **extra):
    row = {
        "id": "job-0001",
        "kind": kind,
        "status": "failed",
        "head_branch": "fix/thing",
        "retry_count": 0,
    }
    if result != ...:
        row["result"] = result
    if reason is not None:
        row["failure_reason"] = reason
    row.update(extra)
    return row


#: name -> (row, expected deterministic action, why)
ROW_SHAPES = {
    "empty_result": (_row(result={}, failure_reason=""), "escalate"),
    "loop_teardown_author_failed": (_row(result={"loop_result": {"teardown": {"reason": "author_failed"}}}), "release_to_queue"),
    "loop_teardown_reviewer_unavailable": (
        _row(result={"loop_result": {"teardown": {"reason": "reviewer_unavailable"}}}),
        "release_to_queue",
    ),
    "loop_teardown_exhausted": (_row(result={"loop_result": {"teardown": {"reason": "exhausted"}}}), "escalate"),
    "loop_teardown_nothing_to_commit": (
        _row(result={"loop_result": {"teardown": {"reason": "nothing_to_commit"}}}),
        "escalate",
    ),
    "clone_failed": (_row(result={"failure_reason": "clone_failed: could not reach remote"}), "release_to_queue"),
    "no_result_no_reason": (_row(), "escalate"),
    "no_action_residue": (_row(result={"failure_reason": "the host was rebooted"}), "no_action"),
    "evidence_residue": (_row(result={"failure_reason": "transient 503 from the provider"}), "release_to_queue"),
    "unknown_residue": (_row(result={"failure_reason": "something nobody has ever seen"}), "escalate"),
}


# ── 1. classification by row shape ────────────────────────────────────────


@pytest.mark.parametrize("name", sorted(ROW_SHAPES))
def test_classified_rows_are_decided_by_table_and_never_reach_a_model(name, monkeypatch):
    """Every shape in the table is decided by the table.

    The ONE shape the table cannot name (``unknown_residue``) is the single
    case a model may be consulted -- and even then the answer is constrained
    and a model that will not answer escalates. So a model is only allowed
    when the row is explicitly marked ambiguous."""
    from hive.workers import doctor

    row, expected = ROW_SHAPES[name]
    ambiguous = doctor_rules.classify_row(row).ambiguous

    def no_model(*_a, **_k):
        raise AssertionError(f"row {name!r} reached the model: it is decided by its shape")

    monkeypatch.setattr(doctor, "invoke_doctor_agent", no_model)
    monkeypatch.setattr(doctor, "_http", lambda *a, **k: (200, {}))
    if ambiguous:
        # The constrained residue call: prose does not parse, so the row
        # escalates without any free-form diagnosis.
        monkeypatch.setattr(doctor, "invoke_doctor_agent", lambda *a, **k: (True, "I would release it"))
    action = doctor._classify_triage_state(row, base_kind="fix")["action_type"]
    assert action == expected


def test_every_classified_row_carries_a_deterministic_rule_id():
    """Every shape gets a rule id and a class -- including the residue, which
    the table answers with its own deterministic default (escalate)."""
    for name, (row, _expected) in ROW_SHAPES.items():
        cls = doctor_rules.classify_row(row)
        assert cls is not None, f"{name}: a row shape the table does not know"
        assert cls.rule_id, f"{name}: no rule id"
        assert cls.action in doctor_rules.CLASSES
        # Only the residue may be model-confirmed; everything else never is.
        assert cls.ambiguous is (name == "unknown_residue"), name


def test_a_missing_result_dict_is_classified_not_guessed():
    """An empty ``result`` is information, not a question for a model."""
    cls = doctor_rules.classify_row(_row(result={}, failure_reason=""))
    assert cls.rule_id == "empty_result"
    assert cls.action == "escalate"


def test_loop_teardown_classes_use_the_loop_outcome_table():
    """One table for loop teardown reasons -- loop_outcome._TEARDOWN_TO_CLASS."""
    from hive.workers import loop_outcome

    for reason in loop_outcome._TEARDOWN_TO_CLASS:
        row = _row(result={"loop_result": {"teardown": {"reason": reason}}})
        cls = doctor_rules.classify_row(row)
        assert cls is not None, f"teardown reason {reason!r} is not classified"
        assert cls.rule_id == f"loop:{loop_outcome._TEARDOWN_TO_CLASS[reason]}"


def test_never_act_classes_are_not_classified_by_this_table():
    """Declines / protection violations have their own callers; the table
    must not claim them and start a second, conflicting decision."""
    for reason in ("no_workspace_for_repo:x", "host_declines_kind", "protection_violation"):
        assert doctor_rules.classify_row(_row(result={"worker_error": reason})) is None
    # And the bare marker, as the decline caller stores it.
    for marker in doctor_rules.NEVER_ACT_MARKERS:
        assert doctor_rules.classify_row(_row(failure_reason=marker)) is None


# ── 2. constrained output ─────────────────────────────────────────────────


def test_the_schema_enumerates_the_classes():
    schema = doctor_rules.decision_schema()
    assert schema["additionalProperties"] is False
    assert set(schema["properties"]["action_type"]["enum"]) == set(doctor_rules.CLASSES)
    assert schema["required"] == ["action_type"]
    assert schema["properties"]["reason"]["maxLength"] <= doctor_rules.MAX_REASON_CHARS


def _reply(text):
    return json.dumps({"choices": [{"message": {"content": text}}]}).encode()


class FakeServer:
    """A recording OpenAI-compatible endpoint."""

    def __init__(self, bodies):
        self.bodies = list(bodies)
        self.requests = []

    def __call__(self, request, timeout=0):
        self.requests.append(
            {"url": request.full_url, "body": json.loads(request.data.decode())}
        )
        body = self.bodies.pop(0)
        resp = io.BytesIO(_reply(body))
        resp.__enter__ = lambda: resp  # noqa: SLF001 -- context manager shape
        resp.__exit__ = lambda *_a: None
        return resp


def test_a_model_call_carries_the_grammar_and_the_json_schema():
    server = FakeServer(['{"action_type": "no_action", "reason": "host rebooted"}'])
    from hive.workers import doctor

    ok, text = doctor._invoke_openweight(
        "p", "alias", base_url="http://h/v1", model="m", urlopen=server, structured=True
    )
    assert ok, text
    body = server.requests[0]["body"]
    assert body["response_format"] == {"type": "json_object"}
    # llama-server's grammar: the schema travels too, so a small model cannot
    # answer outside the enum.
    assert body["json_schema"]["properties"]["action_type"]["enum"] == list(doctor_rules.CLASSES)
    assert body["grammar"] == {"type": "json_schema", "json_schema": body["json_schema"]}


def test_a_reply_outside_the_enum_is_rejected():
    ok, parsed = doctor_rules.parse_constrained(
        '{"action_type": "restart_everything", "reason": "trust me"}'
    )
    assert ok is False
    assert "action_type" in parsed["error"]


def test_a_reply_that_is_not_json_is_rejected():
    ok, parsed = doctor_rules.parse_constrained("I think you should release it")
    assert (ok, parsed) == (False, {"error": "not_json"})


def test_confidence_and_reason_are_bounded():
    ok, parsed = doctor_rules.parse_constrained(
        json.dumps(
            {
                "action_type": "no_action",
                "confidence": 7,
                "reason": "x" * 1000,
                "evidence_quote": "quoted",
                "extra": "dropped",
            }
        )
    )
    assert ok
    assert parsed["confidence"] == 1.0
    assert len(parsed["reason"]) <= doctor_rules.MAX_REASON_CHARS
    assert "extra" not in parsed
    assert "evidence_quote" not in parsed


def test_a_rejected_reply_moves_the_call_to_the_next_rung(monkeypatch):
    """When the first rung's reply does not parse, the next rung is tried."""
    from hive.workers import doctor

    monkeypatch.setattr(doctor, "_chain", lambda: _fake_chain([("r1", "prose"), ("r2", '{"action_type":"no_action","reason":"ok"}')]))
    ok, text = doctor.invoke_doctor_agent("p", phase="triage")
    assert ok
    action = doctor.parse_doctor_response(text)
    assert action["action_type"] == "no_action"


class _StubChain:
    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = 0

    def call(self, phase, **_kw):
        from hive.workers import doctor_models

        self.calls += 1
        rung, text = self.replies.pop(0)
        attempts = [rung]
        if text.startswith("{"):
            return doctor_models.CallOutcome(ok=True, text=text, rung=rung, attempts=attempts)
        return doctor_models.CallOutcome(ok=False, text="unusable reply", rung=rung, attempts=attempts)


def _fake_chain(replies):
    return _StubChain(replies)


def test_the_legacy_failover_rungs_inherit_the_constraint(monkeypatch):
    from hive.workers import doctor

    seen = []

    def fake(prompt, alias, *, base_url=None, model=None, urlopen=None, structured=False):
        seen.append({"base": base_url, "structured": structured})
        if len(seen) == 1:
            return False, f"model {model} HTTP 503: b'busy'"
        return True, '{"action_type": "no_action", "reason": "r"}'

    monkeypatch.setattr(doctor, "chain_from_env", lambda: None)
    monkeypatch.setattr(doctor, "_invoke_openweight", fake)
    monkeypatch.setitem(doctor._PHASE_BASE_URLS, "triage", "http://a/v1")
    monkeypatch.setattr(doctor, "DOCTOR_FALLBACK_BASE_URLS", ["http://b/v1"])
    monkeypatch.setattr(doctor, "DOCTOR_FALLBACK_MODELS", ["m2"])
    ok, _text = doctor.invoke_doctor_agent("p", phase="triage")
    assert ok
    assert [s["structured"] for s in seen] == [True, True]


# ── 3. the chain order and the light-model rule ───────────────────────────


def test_default_chain_order_is_gpt_oss_qwen_m3_deepseek_muse():
    rungs = doctor_rules.default_chain()
    assert [r.model for r in rungs] == [
        "gpt-oss-20b",
        "qwen38",
        "MiniMax-M3",
        "deepseek-flash",
        doctor_rules.MUSE_STANDARD_MODEL,
    ]
    assert all(r.phases == ["triage", "review", "fix"] for r in rungs)


def test_no_light_model_rung_is_restored_anywhere():
    """Operator 2026-10-31: no dedicated light model comes back on HYDRA or
    ACHILLES -- existing capacity covers the rare use."""
    for rung in doctor_rules.default_chain():
        assert rung.host not in {"hydra", "achilles"}


# ── 4. the ledger and the restoration alert ───────────────────────────────


def test_each_call_writes_one_jsonl_line_per_rung(tmp_path):
    path = tmp_path / "calls.jsonl"
    ledger = doctor_rules.CallLedger(str(path), clock=lambda: 1000.0)
    ledger.record(rung="cerberus-gpt-oss", phase="triage", latency_ms=12, outcome="ok")
    ledger.record(rung="tydeus-qwen38", phase="triage", latency_ms=7, outcome="busy", skipped_because="busy 8/8")
    lines = [json.loads(line) for line in path.read_text().splitlines()]
    assert len(lines) == 2
    assert set(lines[0]) == {"at", "rung", "phase", "latency_ms", "outcome", "skipped_because"}
    assert lines[1]["skipped_because"] == "busy 8/8"


def test_the_ledger_line_never_carries_a_prompt_or_a_key(tmp_path):
    path = tmp_path / "calls.jsonl"
    ledger = doctor_rules.CallLedger(str(path), clock=lambda: 1000.0)
    ledger.record(rung="minimax-m3", phase="triage", latency_ms=1, outcome="quota", skipped_because="quota")
    line = path.read_text().strip()
    assert "api" not in line.lower().replace("minimax-m3", "")
    assert "Bearer" not in line


def test_more_than_thirty_calls_in_an_hour_raises_the_alert_once(tmp_path):
    path = tmp_path / "calls.jsonl"
    ledger = doctor_rules.CallLedger(str(path), clock=lambda: 2000.0)
    for _ in range(doctor_rules.ALERT_CALLS_PER_HOUR):
        assert ledger.record(rung="c", phase="triage", latency_ms=1, outcome="ok")["alert"] is False
    summary = ledger.record(rung="c", phase="triage", latency_ms=1, outcome="ok")
    assert summary["alert"] is True
    assert summary["alert_reason"] == "calls_per_hour"
    assert summary["calls_last_hour"] == doctor_rules.ALERT_CALLS_PER_HOUR + 1
    # ONCE: the 32nd call in the same hour does not repeat the alert.
    assert ledger.record(rung="c", phase="triage", latency_ms=1, outcome="ok")["alert"] is False


def test_an_hour_without_calls_is_not_an_alert(tmp_path):
    now = [0.0]
    ledger = doctor_rules.CallLedger(str(tmp_path / "l.jsonl"), clock=lambda: now[0])
    for _ in range(10):
        now[0] += 1000
        assert ledger.record(rung="c", phase="triage", latency_ms=1, outcome="ok")["alert"] is False


def test_a_slot_found_taken_more_than_three_times_in_an_hour_alerts_once(tmp_path):
    ledger = doctor_rules.CallLedger(str(tmp_path / "l.jsonl"), clock=lambda: 5000.0)
    for _ in range(doctor_rules.ALERT_SLOT_TAKEN_PER_HOUR):
        assert ledger.record(rung="c", phase="triage", latency_ms=1, outcome="ok", slot_taken=True)["alert"] is False
    summary = ledger.record(rung="c", phase="triage", latency_ms=1, outcome="ok", slot_taken=True)
    assert summary["alert"] is True
    assert summary["alert_reason"] == "free_slot_taken"
    assert summary["slot_taken_last_hour"] == doctor_rules.ALERT_SLOT_TAKEN_PER_HOUR + 1
    assert ledger.record(rung="c", phase="triage", latency_ms=1, outcome="ok", slot_taken=True)["alert"] is False


def test_the_counter_is_exposed_and_formatted_for_the_status_line():
    counter = doctor_rules.model_call_counter([{"at": 10.0, "outcome": "ok"}], now=20.0)
    assert counter["calls_last_hour"] == 1
    assert counter["alert"] is False
    line = doctor_rules.format_counter_line(counter)
    assert line.startswith("doctors:")


def test_process_triage_job_records_the_counter_on_its_result(monkeypatch):
    from hive.workers import doctor

    monkeypatch.setattr(doctor, "_classify_triage_state", lambda job: {"action_type": "escalate", "reason": "boom"})
    monkeypatch.setattr(doctor, "_note_doctor_model_call", _noop)
    out = doctor.process_triage_job(_row(result={}, failure_reason=""))
    counter = out["doctor_model_calls"]
    # The result carries the same counter the status line renders.
    assert counter["calls_last_hour"] == doctor.doctor_model_call_counter()["calls_last_hour"]
    assert "slot_taken_last_hour" in counter
    assert "alert" in counter


def _noop(*_a, **_k):
    return None


# ── the doctor status carries the counter ─────────────────────────────────


def test_doctor_agent_status_dict_carries_the_counter(monkeypatch):
    from hive.workers import doctor

    status = doctor.doctor_agent_status()
    assert "model_calls" in status
    assert "calls_last_hour" in status["model_calls"]


# ── the weekend triage line renders the counter ───────────────────────────


def test_weekend_triage_renders_the_doctor_call_counter():
    from scripts import hive_status

    rows = {
        "jobs": [],
        "agents": [],
        "doctor": {"model_calls": {"calls_last_hour": 4, "slot_taken_last_hour": 1, "alert": False}},
    }
    snapshot = hive_status.build_status(rows, now=1000.0)
    assert snapshot["doctor"]["model_calls"]["calls_last_hour"] == 4
    text = hive_status.render_text(snapshot)
    assert "doctors:" in text


def test_weekend_triage_renders_an_alert_visibly():
    from scripts import hive_status

    rows = {"jobs": [], "agents": [], "doctor": {"model_calls": {"calls_last_hour": 31, "alert": True,
                                                                "alert_reason": "calls_per_hour"}}}
    text = hive_status.render_text(hive_status.build_status(rows, now=1000.0))
    assert "doctor.light_model_needed" in text


# ── structure: the module is stdlib-only and complete ─────────────────────


def test_module_is_stdlib_only_and_defines_what_it_uses():
    source = Path("hive/workers/doctor_rules.py").read_text()
    imported = set(re.findall(r"^(?:from|import)\s+(\S+)", source, re.M))
    for banned in ("requests", "httpx", "pydantic", "jsonschema"):
        assert banned not in imported
    assert "from hiring" not in source
    for name in ("classify_row", "decision_schema", "parse_constrained", "CallLedger", "default_chain"):
        assert hasattr(doctor_rules, name)


def test_the_classified_result_shape_matches_the_legacy_action():
    cls = doctor_rules.classify_row(_row(result={"failure_reason": "the host was rebooted"}))
    action = doctor_rules.as_action(cls)
    assert action["action_type"] in doctor_rules.CLASSES
    assert action["rule_id"] == "residue:no_action"
    assert logging.getLogger("doctor.rules") is not None


def test_the_residue_prompt_asks_only_for_the_enum(monkeypatch):
    from hive.workers import doctor

    prompt = doctor.build_residue_prompt(
        _row(result={"failure_reason": "the host was rebooted"}),
        doctor_rules.classify_row(_row(result={"failure_reason": "the host was rebooted"})),
    )
    assert "decision classes" in prompt
    for action in doctor_rules.CLASSES:
        assert action in prompt
    assert "diagnostic hosts" not in prompt.lower()
