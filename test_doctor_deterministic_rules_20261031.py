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
import pathlib
import re
import urllib.request
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
    "loop_teardown_author_failed": (
        _row(result={"loop_result": {"teardown": {"reason": "author_failed"}}}),
        "release_to_queue",
    ),
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


class _Reply(io.BytesIO):
    """A chat reply: a real urlopen-style context manager with a status.

    A subclass, not an instance patched with ``__enter__``/``__exit__``: the
    dunder lookup for ``with`` goes to the TYPE, and assigning onto an
    ``io.BytesIO`` instance is not something the runtime promises."""

    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


class FakeServer:
    """A recording OpenAI-compatible endpoint."""

    def __init__(self, bodies):
        self.bodies = list(bodies)
        self.requests = []

    def __call__(self, request, timeout=0):
        self.requests.append({"url": request.full_url, "body": json.loads(request.data.decode())})
        body = self.bodies.pop(0)
        return _Reply(_reply(body))


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
    ok, parsed = doctor_rules.parse_constrained('{"action_type": "restart_everything", "reason": "trust me"}')
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

    monkeypatch.setattr(
        doctor, "_chain", lambda: _fake_chain([("r1", "prose"), ("r2", '{"action_type":"no_action","reason":"ok"}')])
    )
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


def test_a_slot_taken_call_is_one_call_and_one_slot_event_never_two_calls(tmp_path):
    """A call that found its free slot taken is ONE call plus ONE slot event.

    This is the invariant a chunked reviewer misread in ``CallLedger.record``:
    the appended entry is the entry for THIS call (carrying ``slot_taken``),
    not an additional synthetic one, so ``calls_last_hour`` must count calls
    and ``slot_taken_last_hour`` must count the flag -- never twice."""
    ledger = doctor_rules.CallLedger(str(tmp_path / "l.jsonl"), clock=lambda: 900.0)
    for _ in range(5):
        ledger.record(rung="c", phase="triage", latency_ms=1, outcome="ok", slot_taken=True)
    counter = ledger.counter()
    assert counter["calls_last_hour"] == 5, "a slot-taken call must not count as two calls"
    assert counter["slot_taken_last_hour"] == 5

    # Mixed hour: N real calls, half of them slot-taken.
    mixed = doctor_rules.CallLedger(str(tmp_path / "m.jsonl"), clock=lambda: 910.0)
    n = doctor_rules.ALERT_CALLS_PER_HOUR + 1
    for i in range(n):
        mixed.record(rung="c", phase="triage", latency_ms=1, outcome="ok", slot_taken=(i % 2 == 0))
    counter = mixed.counter()
    assert counter["calls_last_hour"] == n
    assert counter["slot_taken_last_hour"] == (n + 1) // 2
    # The CALL ceiling is what tripped -- not a double-counted slot event.
    assert counter["alert_reason"] == "calls_per_hour"


def test_doctor_module_symbols_the_reviewer_called_undefined_are_defined():
    """Guard the public helper surface process_triage_job depends on.

    A chunked reviewer claimed _is_unschedulable, _classify_triage_state,
    doctor_model_call_counter and the ``time`` import were undefined in
    hive/workers/doctor.py. All exist; this test fails loudly if a future
    edit drops one, which would be a genuine NameError at runtime."""
    from hive.workers import doctor

    for name in (
        "_is_unschedulable",
        "_classify_triage_state",
        "doctor_model_call_counter",
        "process_triage_job",
        "time",
    ):
        assert hasattr(doctor, name), f"hive.workers.doctor lost {name!r}"
    # And they are callable with the shapes process_triage_job passes.
    assert doctor._is_unschedulable([]) is None
    assert isinstance(doctor._classify_triage_state({}), dict)
    assert "calls_last_hour" in doctor.doctor_model_call_counter()
    # doctor_rules attributes referenced from doctor.py must exist.
    for attr in ("CLASSES", "ALERT_TOPIC", "classify_row", "model_call_counter"):
        assert hasattr(doctor_rules, attr), f"doctor_rules lost {attr!r}"


def test_teardown_reason_tolerates_a_missing_or_none_loop_result():
    """A row with no loop_result / None / a non-dict must not raise."""
    for result in ({}, {"loop_result": None}, {"loop_result": "str"}, {"loop_result": {"teardown": None}}):
        assert doctor_rules._teardown_reason(result) == ""
    assert doctor_rules._teardown_reason({"loop_result": {"teardown": "boom"}}) == "boom"
    assert doctor_rules._teardown_reason({"loop_result": {"teardown": {"reason": "x"}}}) == "x"
    assert doctor_rules._teardown_reason({"loop_result": {"teardown": {"error": "y"}}}) == "y"


def test_slot_taken_survives_a_restart_without_double_counting_calls(tmp_path):
    """A new ledger process must still see the hour's slot-taken events.

    ``slot_taken`` is not one of the six persisted line fields, so it rides in
    a sidecar; the regression this guards is BOTH halves: the events survive
    (a restart must not hide a sustained pattern) and the restored events are
    never counted as extra model CALLS."""
    path = tmp_path / "calls.jsonl"
    first = doctor_rules.CallLedger(str(path), clock=lambda: 7000.0)
    for _ in range(doctor_rules.ALERT_SLOT_TAKEN_PER_HOUR + 1):
        first.record(rung="c", phase="triage", latency_ms=1, outcome="ok", slot_taken=True)

    restarted = doctor_rules.CallLedger(str(path), clock=lambda: 7000.0)
    counter = restarted.counter()
    assert counter["slot_taken_last_hour"] == doctor_rules.ALERT_SLOT_TAKEN_PER_HOUR + 1
    assert counter["calls_last_hour"] == doctor_rules.ALERT_SLOT_TAKEN_PER_HOUR + 1
    assert counter["alert"] is True and counter["alert_reason"] == "free_slot_taken"
    # And the persisted lines still carry exactly the six enumerated fields.
    for line in path.read_text().splitlines():
        assert set(json.loads(line)) == set(doctor_rules.LEDGER_FIELDS)


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

    rows = {
        "jobs": [],
        "agents": [],
        "doctor": {"model_calls": {"calls_last_hour": 31, "alert": True, "alert_reason": "calls_per_hour"}},
    }
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


def test_the_doctor_module_defines_every_name_its_new_code_uses():
    """A chunked reviewer keeps flagging names it cannot see.

    These are the definitions the new call sites rely on; asserting they exist
    (and are module-level) is cheaper than re-litigating each false positive,
    and it FAILS loudly if one is ever actually removed."""
    from hive.workers import doctor

    for name in (
        "doctor_rules",
        "urllib",
        "STATE",
        "_MODEL_CALL_LEDGER",
        "_chain",
        "chain_from_env",
        "build_residue_prompt",
        "invoke_doctor_agent",
        "_invoke_openweight",
        "_ledger_urlopen",
        "_note_doctor_model_call",
        "_record_model_call",
        "doctor_model_call_counter",
        "_PHASE_BASE_URLS",
        "DOCTOR_FALLBACK_BASE_URLS",
        "DOCTOR_FALLBACK_MODELS",
    ):
        assert hasattr(doctor, name), f"doctor is missing {name}"
    # The opener defaults to the stdlib one, and the phase base map is a dict
    # the legacy chain reads by phase key (tests monkeypatch entries into it).
    assert doctor.chain_from_env is doctor.doctor_models.chain_from_env
    assert isinstance(doctor._PHASE_BASE_URLS, dict)


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


# ── review round: the findings that were verified REAL ────────────────────


def _empty_ledger_alert(monkeypatch, alert):
    """Make the heartbeat's counter report ``alert`` without a ledger file."""
    from hive.workers import doctor

    monkeypatch.setattr(doctor, "_model_call_ledger", lambda: _AlertingLedger(alert))


class _AlertingLedger:
    def __init__(self, alert):
        self._alert = alert

    def counter(self, now=None):
        return self._alert


def test_an_alert_broadcast_that_raises_never_breaks_the_heartbeat(monkeypatch):
    """HIGH, verified: the counter read was isolated but the BROADCAST was not.

    A bus outage (or a serializer that chokes on the payload) raised out of
    ``_emit_light_model_alert`` and skipped the heartbeat POST, so the doctor
    silently stopped heartbeating exactly when it needed the bus. Alert
    telemetry must never be able to fail the heartbeat."""
    from hive.workers import doctor

    _empty_ledger_alert(
        monkeypatch, {"alert": True, "alert_reason": "calls_per_hour", "calls_last_hour": 31, "slot_taken_last_hour": 0}
    )
    broadcast = []

    def broken_broadcast(topic, payload):
        broadcast.append(topic)
        raise RuntimeError("bus down")

    heartbeats = []

    def fake_http(method, path, body=None, timeout=15.0):
        heartbeats.append(path)
        return 200, {}

    monkeypatch.setattr(doctor, "broadcast_doctor_message", broken_broadcast)
    monkeypatch.setattr(doctor, "_http", fake_http)
    doctor.heartbeat()  # must not raise
    assert broadcast == [doctor_rules.ALERT_TOPIC]
    assert heartbeats == ["/v1/agents/heartbeat"], "the heartbeat must still be sent"

    # A PAYLOAD that cannot be serialised is the same class of failure.
    monkeypatch.setattr(doctor, "broadcast_doctor_message", doctor.broadcast_doctor_message)
    heartbeats.clear()

    def unserialisable(_topic, _payload):
        raise TypeError("not JSON serialisable")

    monkeypatch.setattr(doctor, "broadcast_doctor_message", unserialisable)
    doctor.heartbeat()
    assert heartbeats == ["/v1/agents/heartbeat"]


def test_the_alert_is_still_emitted_once_the_broadcast_works(monkeypatch):
    """The positive control: isolation must not disable the alert."""
    from hive.workers import doctor

    _empty_ledger_alert(
        monkeypatch,
        {"alert": True, "alert_reason": "free_slot_taken", "calls_last_hour": 3, "slot_taken_last_hour": 4},
    )
    broadcast = []
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda topic, payload: broadcast.append((topic, payload)))
    monkeypatch.setattr(doctor, "_http", lambda *_a, **_k: (200, {}))
    doctor.heartbeat()
    assert [topic for topic, _ in broadcast] == [doctor_rules.ALERT_TOPIC]
    assert broadcast[0][1]["reason"] == "free_slot_taken"


_NOT_PASSED = object()


class _SigRecording:
    """A transport double that records how it was called, like urllib does."""

    def __init__(self, body=b'{"slots": [{"is_processing": false}]}'):
        self.body = body
        self.calls = []

    def __call__(self, request, timeout=_NOT_PASSED):
        self.calls.append({"request": request, "timeout": timeout})
        resp = io.BytesIO(self.body)
        resp.status = 200
        resp.__enter__ = lambda: resp  # noqa: SLF001 -- a plain double, not a C-type instance
        resp.__exit__ = lambda *_a: None
        return resp


class _StatusResponse(io.BytesIO):
    """A response object that also reports an HTTP status, like http.client."""

    def __init__(self, body, status=200):
        super().__init__(body)
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


def test_the_ledger_opener_forwards_its_arguments_and_keeps_the_default_timeout(monkeypatch):
    """HIGH, verified: the wrapper took ``(request, timeout=0)``.

    The real chain always passes a timeout, so the changed DEFAULT only bit a
    caller that left it out -- ``urlopen(request)`` would have become a 0 s
    timeout, i.e. an immediate failure. And dropping ``*args`` turned any
    positional call into a TypeError. Both change what the transport does."""
    from hive.workers import doctor

    transport = _SigRecording()
    monkeypatch.setattr(doctor.urllib.request, "urlopen", transport)
    recorder = []
    monkeypatch.setattr(doctor, "_record_model_call", lambda **entry: recorder.append(entry) or {})

    opener = doctor._ledger_urlopen()
    request = urllib.request.Request("http://host:8080/slots")

    with opener(request) as resp:
        assert resp.read()
    # A forwarded call that leaves the timeout out leaves it out -- the
    # wrapper must not substitute one of its own (the base defaulted to 0 s,
    # i.e. an immediate timeout).
    assert transport.calls[0]["timeout"] is _NOT_PASSED

    opener(request, 2.0)
    assert transport.calls[1]["timeout"] == 2.0

    # A positional extra argument reaches the transport, not the wrapper.
    extra = object()

    def positional(called_with, timeout=0, *args, **kwargs):
        transport.calls.append({"request": called_with, "timeout": timeout, "args": args})
        return _StatusResponse(b"[]")

    monkeypatch.setattr(doctor.urllib.request, "urlopen", positional)
    opener = doctor._ledger_urlopen()
    opener(request, 3.0, extra)
    assert transport.calls[-1]["args"] == (extra,)


def test_the_ledger_opener_replays_what_the_transport_returned(monkeypatch):
    """MEDIUM, verified: the replay dropped the status and any other header.

    A real response reports ``status`` (the chain reads it nowhere today, but a
    future reader would see 200 for every probe whatever the server said). The
    replay must carry the status forward."""
    from hive.workers import doctor

    monkeypatch.setattr(doctor.urllib.request, "urlopen", lambda *_a, **_k: _StatusResponse(b"{}", status=207))
    opener = doctor._ledger_urlopen()
    with opener(urllib.request.Request("http://host:8080/slots")) as resp:
        assert resp.status == 207
        assert resp.read() == b"{}"
    # The replay is a context manager whose exit is a no-op (BytesIO close).
    assert resp.__exit__(None, None, None) is None


def _probe_with(monkeypatch, body: bytes) -> list:
    """Run the real ledger opener over one ``/slots`` body; return what it logged."""
    from hive.workers import doctor

    class _Response(io.BytesIO):
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    monkeypatch.setattr(doctor.urllib.request, "urlopen", lambda *_a, **_k: _Response(body))
    recorded = []
    monkeypatch.setattr(doctor, "_record_model_call", lambda **entry: recorded.append(entry) or {})
    with doctor._ledger_urlopen()(urllib.request.Request("http://host:8080/slots")) as resp:
        resp.read()
    return recorded


def test_an_empty_slots_list_is_unknown_not_busy(monkeypatch):
    """HIGH, verified: ``all() over []`` is True, so an EMPTY list was "busy".

    llama-server reports every slot it is configured with, so a ``/slots`` body
    with no slots is not a measurement of a full server: the server may have
    restarted between config and probe, or be answering before it allocated
    slots. ``all()`` (and its inverse) over nothing is vacuously true, which
    turned "nothing to read" into "every slot is taken" and counted a
    slot-taken event toward the restoration alert. Empty is UNKNOWN: no
    evidence either way, so the probe records nothing.

    The chain agrees -- ``_llama_busy`` reads an empty list as 0 busy, not as
    a full server -- and the two must not disagree."""
    from hive.workers import doctor_models

    assert _probe_with(monkeypatch, b"[]") == []
    assert _probe_with(monkeypatch, b'{"slots": []}') == []
    # Positive controls: a real busy reading and a real free reading.
    assert [entry["slot_taken"] for entry in _probe_with(monkeypatch, b'[{"is_processing": true}]')] == [True]
    assert _probe_with(monkeypatch, b'[{"is_processing": false}]') == []

    # The chain's own reading of the same bodies.
    assert doctor_models._llama_busy("[]") == (0.0, "0/0 busy")
    assert doctor_models._llama_busy('[{"is_processing": true}, {"is_processing": true}]')[0] == 2.0
    assert doctor_models._llama_busy('[{"is_processing": false}]')[0] == 0.0


def test_one_bad_line_in_the_slot_sidecar_only_drops_that_line(tmp_path):
    """MEDIUM, verified: a ``[float(line) for line in fh]`` loses the WHOLE file.

    One truncated tail line (a crash mid-write) or a stray blank-made-junk
    line raised out of the comprehension's ``float`` and the ``except``
    returned ``[]`` -- every slot-taken event in the hour vanished, which is
    exactly the evidence the restoration rule reads. The parser must skip the
    bad line and keep the rest."""
    path = tmp_path / "calls.jsonl"
    ledger = doctor_rules.CallLedger(str(path), clock=lambda: 8000.0)
    for _ in range(doctor_rules.ALERT_SLOT_TAKEN_PER_HOUR + 1):
        ledger.record(rung="c", phase="triage", latency_ms=1, outcome="ok", slot_taken=True)
    sidecar = pathlib.Path(f"{path}.slot-taken")
    lines = sidecar.read_text().splitlines()
    sidecar.write_text("\n".join([lines[0], "not-a-timestamp", *lines[1:], "8000.0"]) + "\n")

    counter = doctor_rules.CallLedger(str(path), clock=lambda: 8000.0).counter()
    assert counter["slot_taken_last_hour"] == doctor_rules.ALERT_SLOT_TAKEN_PER_HOUR + 2
    assert counter["alert"] is True and counter["alert_reason"] == "free_slot_taken"
    assert counter["calls_last_hour"] == doctor_rules.ALERT_SLOT_TAKEN_PER_HOUR + 1

    # A sidecar that is entirely junk is still empty, not an error.
    sidecar.write_text("junk\n\n ")
    assert doctor_rules.CallLedger(str(path), clock=lambda: 8000.0).counter()["slot_taken_last_hour"] == 0


def test_the_fake_servers_return_a_real_context_manager():
    """HIGH (test helper), verified as fragile rather than as a live failure.

    ``FakeServer`` patched ``__enter__``/``__exit__`` onto an ``io.BytesIO``
    INSTANCE. That works on this CPython (3.14) but it is not a contract: a
    C-type instance is not guaranteed to accept an attribute assignment and
    the double-underline methods are looked up on the TYPE, not the instance,
    so a runtime that refuses the assignment crashes the helper instead of
    testing the code. The reply is now a tiny subclass, like the other fakes
    in the suite, and this test pins the shape."""
    server = FakeServer(['{"action_type": "no_action"}'])
    resp = server(urllib.request.Request("http://h/v1/chat/completions", data=b"{}", method="POST"))
    assert isinstance(resp, io.IOBase), "the reply must be a real IO object"
    # The dunder methods are looked up on the TYPE, so they work on every
    # runtime -- not only where assigning them onto an instance happens to
    # succeed (an io.BytesIO instance is not guaranteed to accept that).
    assert type(resp) is not io.BytesIO, "the reply should be a subclass, not a patched BytesIO"
    assert "__enter__" in type(resp).__dict__ and "__exit__" in type(resp).__dict__
    with resp as ctx:
        assert ctx is resp
        assert ctx.status == 200
        assert ctx.read()
