"""Doctor LLM failover + no-unattended-Codex (2026-10-17).

HYDRA (.78) and ACHILLES (.11) are BUILD hosts and their orchestrator unloads
the reasoning model while compiling, so a phase base is routinely down. The
doctor must walk its LOCAL fallback chain (HYDRA -> ACHILLES -> CERBERUS
gemma4-31b), only on transport errors / HTTP 5xx / empty completions, and never
fall back to a remote or paid provider.

Codex never runs unattended (operator 2026-10-10): a codex/gpt alias in
DOCTOR_<PHASE>_MODEL is a config error that logs once and uses the open-weight
chain — the local gateway is never called.

All network is mocked at urllib.request.urlopen; nothing here touches a socket.
"""

import importlib
import io
import json
import urllib.error

import pytest

HYDRA = "http://192.0.2.78:8080/v1"
ACHILLES = "http://192.0.2.11:8080/v1"
CERBERUS = "http://192.0.2.96:8080/v1"


class Reply(io.BytesIO):
    """Minimal urlopen() response context manager."""

    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self, *args):
        return super().read(*args)


def _chat_reply(text):
    body = {"choices": [{"message": {"content": text}}]}
    return Reply(json.dumps(body).encode())


class Recorder:
    """urlopen replacement that answers per-URL and records every call."""

    def __init__(self, handlers):
        # handlers: {url_prefix: "reply text" | int status | Exception}
        self.handlers = handlers
        self.calls = []

    def __call__(self, request, timeout=0):
        url = request.full_url
        base = url.split("/chat/completions")[0]
        self.calls.append({"url": url, "base": base, "body": request.data})
        outcome = self.handlers.get(base, ConnectionRefusedError("no handler"))
        if isinstance(outcome, Exception):
            raise outcome
        if isinstance(outcome, int):
            raise urllib.error.HTTPError(url, outcome, "err", {}, io.BytesIO(b"boom"))
        return _chat_reply(outcome)

    @property
    def bases(self):
        return [c["base"] for c in self.calls]


@pytest.fixture
def doctor(monkeypatch):
    """Reload the doctor with a deterministic local-only configuration."""
    monkeypatch.setenv("DOCTOR_TRIAGE_MODEL", "gemma4-e2b")
    monkeypatch.setenv("DOCTOR_REVIEW_MODEL", "gemma4-e2b")
    monkeypatch.setenv("DOCTOR_TRIAGE_BASE_URL", HYDRA)
    monkeypatch.setenv("DOCTOR_REVIEW_BASE_URL", ACHILLES)
    monkeypatch.delenv("DOCTOR_FALLBACK_BASE_URLS", raising=False)
    monkeypatch.delenv("DOCTOR_FALLBACK_MODELS", raising=False)
    monkeypatch.delenv("DOCTOR_OPENWEIGHT_API_KEY", raising=False)
    from hive.workers import doctor as _doctor

    return importlib.reload(_doctor)


def _install(doctor, monkeypatch, handlers):
    rec = Recorder(handlers)
    monkeypatch.setattr(doctor.urllib.request, "urlopen", rec)
    # Never hit the local gateway or read API keys off disk in a unit test.
    monkeypatch.setattr(doctor, "_invoke_gateway", _forbidden_gateway)
    monkeypatch.setattr(doctor, "_openweight_api_key", lambda: "")
    return rec


class GatewayCalled(AssertionError):
    pass


def _forbidden_gateway(prompt):  # pragma: no cover - must never run
    raise GatewayCalled("the unattended Codex/gateway path was invoked")


# ─────────────────────────── failover ───────────────────────────
def test_default_chain_is_local_hydra_achilles_cerberus(doctor):
    assert doctor.DOCTOR_FALLBACK_BASE_URLS == [HYDRA, ACHILLES, CERBERUS]
    assert doctor.DOCTOR_FALLBACK_MODELS == ["gemma4-e2b", "gemma4-e2b", "gemma4-31b"]


def test_phase_base_answers_without_any_failover(doctor, monkeypatch):
    rec = _install(doctor, monkeypatch, {HYDRA: '{"action_type":"no_action"}'})
    ok, text = doctor.invoke_doctor_agent("diagnose", phase="triage")
    assert ok and text.strip() == '{"action_type":"no_action"}'
    assert not text.startswith("[doctor failover:")
    assert rec.bases == [HYDRA]


def test_phase_base_down_fails_over_to_achilles(doctor, monkeypatch):
    rec = _install(
        doctor,
        monkeypatch,
        {ACHILLES: '{"action_type":"no_action"}'},
    )
    ok, text = doctor.invoke_doctor_agent("diagnose", phase="triage")
    assert ok
    assert rec.bases == [HYDRA, ACHILLES]
    assert f"[doctor failover: {ACHILLES}" in text
    assert '{"action_type":"no_action"}' in text


def test_both_rocm_hosts_down_lands_on_cerberus_with_its_own_model(doctor, monkeypatch):
    rec = _install(
        doctor,
        monkeypatch,
        {
            HYDRA: ConnectionRefusedError("unloaded for compile"),
            ACHILLES: urllib.error.URLError("timed out"),
            CERBERUS: '{"action_type":"release_to_queue"}',
        },
    )
    ok, text = doctor.invoke_doctor_agent("diagnose", phase="triage")
    assert ok
    assert rec.bases == [HYDRA, ACHILLES, CERBERUS]
    # each rung carries its own model id: only CERBERUS serves gemma4-31b
    models = [json.loads(c["body"])["model"] for c in rec.calls]
    assert models == ["gemma4-e2b", "gemma4-e2b", "gemma4-31b"]
    assert "gemma4-31b" in text


@pytest.mark.parametrize("status", [500, 502, 503, 504])
def test_http_5xx_triggers_failover(doctor, monkeypatch, status):
    rec = _install(
        doctor,
        monkeypatch,
        {HYDRA: status, ACHILLES: '{"action_type":"no_action"}'},
    )
    ok, text = doctor.invoke_doctor_agent("diagnose", phase="triage")
    assert ok
    assert rec.bases == [HYDRA, ACHILLES]


def test_empty_completion_triggers_failover(doctor, monkeypatch):
    rec = _install(
        doctor,
        monkeypatch,
        {HYDRA: "", ACHILLES: '{"action_type":"no_action"}'},
    )
    ok, _ = doctor.invoke_doctor_agent("diagnose", phase="triage")
    assert ok
    assert rec.bases == [HYDRA, ACHILLES]


def test_http_4xx_does_not_fail_over(doctor, monkeypatch):
    """A bad model id / bad request fails identically everywhere — surface it."""
    rec = _install(
        doctor,
        monkeypatch,
        {HYDRA: 400, ACHILLES: '{"action_type":"no_action"}'},
    )
    ok, text = doctor.invoke_doctor_agent("diagnose", phase="triage")
    assert not ok
    assert rec.bases == [HYDRA]
    assert "HTTP 400" in text


def test_every_endpoint_down_reports_failure_without_remote_fallback(doctor, monkeypatch):
    rec = _install(doctor, monkeypatch, {})  # all default to ConnectionRefused
    ok, text = doctor.invoke_doctor_agent("diagnose", phase="triage")
    assert not ok
    assert rec.bases == [HYDRA, ACHILLES, CERBERUS]
    assert "failed on every local endpoint" in text


def test_phase_base_is_not_retried_as_a_fallback_rung(doctor, monkeypatch):
    """review runs on ACHILLES, which is also rung 2 — it must be skipped."""
    rec = _install(
        doctor,
        monkeypatch,
        {ACHILLES: '{"action_type":"no_action"}'},
    )
    ok, _ = doctor.invoke_doctor_agent("verify", phase="review")
    assert ok
    assert rec.bases == [ACHILLES]


def test_failover_chain_never_leaves_the_lan(doctor, monkeypatch):
    rec = _install(doctor, monkeypatch, {})
    doctor.invoke_doctor_agent("diagnose", phase="triage")
    chain = [(HYDRA, "gemma4-e2b")] + doctor._fallback_rungs(HYDRA)
    for base, _model in chain:
        assert base.startswith(("http://192.168.", "http://127.0.0.1", "http://localhost")), base
    assert all(c["base"].startswith("http://192.168.") for c in rec.calls)


# ─────────────────────── unattended Codex is gone ───────────────────────
@pytest.mark.parametrize("alias", ["codex", "gateway", "gpt", "gpt-5.6-luna"])
def test_codex_alias_is_a_config_error_not_a_gateway_call(doctor, monkeypatch, alias):
    monkeypatch.setitem(doctor._PHASE_MODELS, "triage", alias)
    rec = _install(doctor, monkeypatch, {HYDRA: '{"action_type":"no_action"}'})
    ok, text = doctor.invoke_doctor_agent("diagnose", phase="triage")
    # no GatewayCalled exception => the gateway was never called
    assert ok
    assert rec.bases == [HYDRA]
    assert '{"action_type":"no_action"}' in text


def test_codex_alias_config_error_logs_once(doctor, monkeypatch, caplog):
    monkeypatch.setitem(doctor._PHASE_MODELS, "triage", "codex")
    _install(doctor, monkeypatch, {HYDRA: '{"action_type":"no_action"}'})
    monkeypatch.setattr(doctor, "_codex_alias_warned", False)
    with caplog.at_level("ERROR"):
        doctor.invoke_doctor_agent("one", phase="triage")
        doctor.invoke_doctor_agent("two", phase="triage")
    hits = [r for r in caplog.records if "CONFIG ERROR" in r.getMessage()]
    assert len(hits) == 1, [r.getMessage() for r in caplog.records]


def test_codex_alias_still_uses_the_failover_chain(doctor, monkeypatch):
    monkeypatch.setitem(doctor._PHASE_MODELS, "triage", "codex")
    rec = _install(
        doctor,
        monkeypatch,
        {HYDRA: ConnectionRefusedError("down"), ACHILLES: '{"action_type":"no_action"}'},
    )
    ok, _ = doctor.invoke_doctor_agent("diagnose", phase="triage")
    assert ok
    assert rec.bases == [HYDRA, ACHILLES]
