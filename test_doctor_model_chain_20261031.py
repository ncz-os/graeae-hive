"""Slot-aware model chain for the doctor (operator 2026-10-31).

The doctor's main model is gpt-oss (CERBERUS gpt-oss-20b, llama-server :8080,
8 slots) and it can be overloaded, so it gets backups M3, DeepSeek, Muse -- and
qwen -- and it must be SLOT AWARE: a busy endpoint is skipped, not retried
until the call fails.

Everything the chain decides is pinned here against FAKE HTTP servers
(``/slots`` on a llama-server, ``/metrics`` on vLLM) and a FAKE CLOCK:

1. the chain config -- a TOML file named by ``DOCTOR_MODEL_CHAIN`` with the
   default rung order, and the legacy env chain as the ONE-RUNG default so an
   unset ``DOCTOR_MODEL_CHAIN`` reproduces today's behaviour byte for byte;
2. slot awareness -- all-slots-busy falls through, the reserve is respected,
   a quota-exhausted rung is skipped, the DeepSeek peak window is honoured,
   and the doctor's own in-flight calls are counted;
3. selection -- first healthy, non-busy rung for the phase wins; a review never
   picks the author's family; one structured log line per decision; and a
   failed reply (503, empty, prose-only, truncated) falls through to the next
   rung inside the same call;
4. backoff -- a busy rung cools down with exponential backoff, and a cached
   probe result keeps the next decision off the socket.

No socket is opened: the probes take an injected ``urlopen`` and the chain an
injected clock. No key value is ever read: a rung names the ENV VAR that holds
its key.
"""

from __future__ import annotations

import calendar
import importlib
import inspect
import io
import json
import os

import pytest

from hive.review_queue import family_of

# ── fakes ──────────────────────────────────────────────────────────────────


class Resp(io.BytesIO):
    """Minimal urlopen() response context manager."""

    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


def _rooted(table: dict, server_root: str):
    """Look a probe table up by either the server root or the OpenAI base.

    Probes address the server root (``<base minus /v1>/slots``); the test
    writes the base it knows (``http://host:port/v1``). A base entry wins."""
    if server_root + "/v1" in table:
        return table[server_root + "/v1"]
    return table.get(server_root)


class FakeCluster:
    """A fake HTTP world: llama ``/slots``, vLLM ``/metrics`` and chat replies.

    ``slots[base]`` is the number of BUSY slots reported at that server root,
    ``metrics[base]`` the ``(running, waiting)`` pair, ``chat[base]`` the chat
    reply (a text, an int HTTP status or an Exception). Every request is
    recorded so a test can assert what was (not) contacted.
    """

    def __init__(self):
        # The fake serves at the OpenAI base (``<base>/chat/completions``) and
        # at the server root (``<base without /v1>/slots``); both keys are the
        # base the test knows.
        self.slots: dict[str, int] = {}
        self.slot_counts: dict[str, int] = {}  # if set: [{is_processing}...] length
        self.metrics: dict[str, tuple[int, int]] = {}
        self.chat: dict[str, object] = {}
        self.calls: list[str] = []
        self.probe_calls = 0

    def __call__(self, request, timeout=0):
        url = request.full_url if hasattr(request, "full_url") else str(request)
        self.calls.append(url)
        root = url.split("/chat/completions")[0] if "/chat/completions" in url else ""
        if root:
            outcome = self.chat.get(root, ConnectionRefusedError("no chat handler"))
            if isinstance(outcome, Exception):
                raise outcome
            if isinstance(outcome, int):
                import urllib.error

                raise urllib.error.HTTPError(url, outcome, "err", {}, io.BytesIO(b"denied"))
            body = {"choices": [{"message": {"content": outcome}}]}
            return Resp(json.dumps(body).encode())
        self.probe_calls += 1
        server_root = url.rsplit("/", 1)[0]
        if url.endswith("/slots"):
            busy = _rooted(self.slots, server_root)
            if busy is None:
                busy = max(busy or 0, 0) if server_root in self.slots else 0
            total = _rooted(self.slot_counts, server_root) or 8
            slots = [{"is_processing": i < busy} for i in range(total)]
            return Resp(json.dumps({"slots": slots}).encode())
        if url.endswith("/metrics"):
            running, waiting = _rooted(self.metrics, server_root) or (0, 0)
            text = (
                "# HELP vllm:num_requests_running\n"
                f'vllm:num_requests_running{{model_name="qwen38"}} {running}\n'
                f'vllm:num_requests_waiting{{model_name="qwen38"}} {waiting}\n'
            )
            return Resp(text.encode())
        return Resp(b"{}")

    @property
    def chat_calls(self) -> list[str]:
        return [u for u in self.calls if "/v1/chat" in u]

    def chats_to(self, base: str) -> int:
        root = base[:-3] if base.endswith("/v1") else base
        return sum(1 for u in self.chat_calls if u.startswith(root))


class Clock:
    def __init__(self, now: float = 1_700_000_000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _rung(**overrides) -> dict:
    base = {
        "id": "r1",
        "model": "gpt-oss-20b",
        "base_url": "http://cerberus.invalid:8080/v1",
        "family": "openai",
        "kind": "llama",
        "cost_class": "A",
        "phases": ["triage", "review", "fix"],
        "max_concurrent": 8,
        "quota_guard": None,
        "api_key_env": None,
    }
    base.update(overrides)
    return base


CERBERUS = "http://cerberus.invalid:8080/v1"
TYDEUS = "http://tydeus.invalid:8006/v1"
M3 = "http://minimax.invalid/v1"
DEEPSEEK = "http://deepseek.invalid/v1"
MUSE = "http://muse.invalid/v1"


def _utc(year: int, month: int, day: int, hour: int) -> float:
    """An epoch second for a UTC wall clock (fake-clock tests)."""
    return float(calendar.timegm((year, month, day, hour, 0, 0, 0, 0, 0)))


def _default_chain() -> list[dict]:
    from hive.workers import doctor_models

    return [
        doctor_models.ChainRung.from_dict(_rung(id="cerberus-gpt-oss", model="gpt-oss-20b", base_url=CERBERUS)),
        doctor_models.ChainRung.from_dict(
            _rung(id="tydeus-qwen38", model="qwen38", base_url=TYDEUS, family="alibaba", kind="vllm", max_concurrent=4)
        ),
        doctor_models.ChainRung.from_dict(
            _rung(
                id="minimax-m3", model="MiniMax-M3", base_url=M3, family="minimax", kind="openai", quota_guard="minimax"
            )
        ),
        doctor_models.ChainRung.from_dict(
            _rung(
                id="deepseek-flash",
                model="deepseek-flash",
                base_url=DEEPSEEK,
                family="deepseek",
                kind="openai",
                quota_guard="deepseek_peak",
            )
        ),
        doctor_models.ChainRung.from_dict(
            _rung(id="muse-spark", model="muse-spark-1.3", base_url=MUSE, family="meta", kind="openai")
        ),
    ]


@pytest.fixture
def fake(monkeypatch):
    world = FakeCluster()
    monkeypatch.setattr("urllib.request.urlopen", world)
    return world


@pytest.fixture
def chain(fake, monkeypatch):
    """A Chain over the DEFAULT rung order with a probe urlopen and a fake clock."""
    from hive.workers import doctor_models

    clock = Clock()
    monkeypatch.setenv("MUSE_BASE_URL", MUSE)
    chain = doctor_models.ModelChain(
        rungs=_default_chain(),
        urlopen=fake,
        clock=clock,
        probe_cache_secs=5.0,
        reserve=2,
    )
    chain.clock = clock  # type: ignore[attr-defined]
    return chain


# ── 1. chain config ────────────────────────────────────────────────────────


def test_default_chain_order_is_the_operator_order():
    """gpt-oss-20b, qwen38, M3, DeepSeek, Muse -- in that order."""
    from hive.workers import doctor_models

    rungs = doctor_models.default_rungs()
    assert [r.id for r in rungs] == [
        "cerberus-gpt-oss",
        "tydeus-qwen38",
        "minimax-m3",
        "deepseek-flash",
        "muse-spark",
    ]
    assert [r.model for r in rungs] == ["gpt-oss-20b", "qwen38", "MiniMax-M3", "deepseek-flash", "muse-spark-1.3"]
    by_id = {r.id: r for r in rungs}
    assert by_id["cerberus-gpt-oss"].kind == "llama"
    assert by_id["cerberus-gpt-oss"].max_concurrent == 8
    # A local llama-server is unauthenticated: it names NO key env. Pinning this
    # matters -- a key env here would send the MiniMax key to CERBERUS.
    assert by_id["cerberus-gpt-oss"].api_key_env is None
    assert by_id["tydeus-qwen38"].api_key_env is None
    assert by_id["tydeus-qwen38"].kind == "vllm"
    assert by_id["minimax-m3"].api_key_env == "MINIMAX_API_KEY"
    assert by_id["deepseek-flash"].api_key_env == "DEEPSEEK_API_KEY"
    assert by_id["muse-spark"].api_key_env == "MUSE_API_KEY"
    # The family column agrees with the ONE family table (operator 2026-10-31).
    for rung in rungs:
        assert rung.family == family_of(rung.model), rung.id


def test_default_chain_uses_the_fleet_inventory_for_local_hosts():
    """No address in source: local rungs resolve their host through the inventory."""
    from hive.workers import doctor_models

    inventory = {
        "cerberus": {"address": "192.0.2.96", "user": "u", "auth": "key", "os": "linux"},
        "tydeus": {"address": "192.0.2.73", "user": "u", "auth": "key", "os": "linux"},
    }
    rungs = {r.id: r for r in doctor_models.default_rungs(inventory=inventory)}
    assert rungs["cerberus-gpt-oss"].base_url == "http://192.0.2.96:8080/v1"
    assert rungs["tydeus-qwen38"].base_url == "http://192.0.2.73:8006/v1"
    assert rungs["tydeus-qwen38"].server_root == "http://192.0.2.73:8006"
    # With no inventory the local rung has no address and is skipped, never guessed.
    empty = {r.id: r for r in doctor_models.default_rungs(inventory={})}
    assert empty["cerberus-gpt-oss"].base_url == ""


def test_chain_file_names_the_rungs_and_api_keys_by_env_name(tmp_path):
    from hive.workers import doctor_models

    path = tmp_path / "chain.toml"
    path.write_text(
        """
reserve = 1
[[rungs]]
id = "a"
model = "gpt-oss-20b"
base_url = "http://a.invalid:8080/v1"
kind = "llama"
phases = ["triage"]
max_concurrent = 8

[[rungs]]
id = "b"
model = "MiniMax-M3"
base_url = "http://b.invalid/v1"
kind = "openai"
api_key_env = "MINIMAX_API_KEY"
quota_guard = "minimax"
"""
    )
    cfg = doctor_models.load_chain_file(str(path))
    assert cfg.reserve == 1
    assert [r.id for r in cfg.rungs] == ["a", "b"]
    assert cfg.rungs[1].api_key_env == "MINIMAX_API_KEY"
    # The family column is filled from the table when the file omits it.
    assert cfg.rungs[0].family == "openai"
    assert cfg.rungs[1].family == "minimax"


def test_chain_file_refuses_an_inline_key_value(tmp_path):
    """A rung names the ENV VAR; a literal key in the file is a config error."""
    from hive.workers import doctor_models

    path = tmp_path / "chain.toml"
    path.write_text(
        """
[[rungs]]
id = "a"
model = "MiniMax-M3"
base_url = "http://a.invalid/v1"
api_key = "sk-should-never-be-here"
"""
    )
    with pytest.raises(ValueError) as excinfo:
        doctor_models.load_chain_file(str(path))
    assert "sk-should-never-be-here" not in str(excinfo.value)


def test_unset_chain_env_reproduces_todays_single_rung_chain(fake, monkeypatch):
    """DOCTOR_MODEL_CHAIN unset -> the legacy env chain, as ONE rung per phase."""
    from hive.workers import doctor, doctor_models

    monkeypatch.delenv("DOCTOR_MODEL_CHAIN", raising=False)
    monkeypatch.setenv("DOCTOR_TRIAGE_MODEL", "gemma4-e2b")
    monkeypatch.setenv("DOCTOR_OPENWEIGHT_MODEL", "gpt-oss-20b")
    monkeypatch.setenv("DOCTOR_OPENWEIGHT_BASE_URL", "http://hydra.invalid:8080/v1")
    monkeypatch.setenv("DOCTOR_TRIAGE_BASE_URL", "http://hydra.invalid:8080/v1")
    monkeypatch.setenv("DOCTOR_FALLBACK_BASE_URLS", "")
    fake.chat["http://hydra.invalid:8080/v1"] = "hello"
    clock = Clock()

    chain = doctor_models.chain_from_env(urlopen=fake, clock=clock)
    assert chain is None, "an unset DOCTOR_MODEL_CHAIN keeps the legacy chain, untouched"

    # And the legacy chain is what the doctor actually calls: one rung, the
    # phase model on the phase base, no probe traffic at all.
    monkeypatch.setattr(doctor.urllib.request, "urlopen", fake)
    monkeypatch.setattr(doctor, "_openweight_api_key", lambda: "")
    monkeypatch.setattr(doctor, "_phase_model", lambda phase: "gemma4-e2b")
    monkeypatch.setitem(doctor._PHASE_BASE_URLS, "triage", "http://hydra.invalid:8080/v1")
    ok, text = doctor.invoke_doctor_agent("p", phase="triage")
    assert ok and text == "hello"
    assert fake.chat_calls == ["http://hydra.invalid:8080/v1/chat/completions"]
    assert fake.probe_calls == 0


def test_legacy_chain_bytes_are_unchanged_by_the_new_module(fake, monkeypatch):
    """The same two-rung legacy chain, with and without the chain module: equal."""
    from hive.workers import doctor, doctor_models

    monkeypatch.delenv("DOCTOR_MODEL_CHAIN", raising=False)
    monkeypatch.setenv("DOCTOR_FALLBACK_BASE_URLS", "")
    monkeypatch.setattr(doctor.urllib.request, "urlopen", fake)
    monkeypatch.setattr(doctor, "_openweight_api_key", lambda: "")
    monkeypatch.setattr(doctor, "_phase_model", lambda phase: "gpt-oss-20b")
    monkeypatch.setitem(doctor._PHASE_BASE_URLS, "triage", "http://cerberus.invalid:8080/v1")
    fake.chat = {CERBERUS: "answer"}

    first = doctor.invoke_doctor_agent("p", phase="triage")
    consulted: list[int] = []
    monkeypatch.setattr(doctor_models, "chain_from_env", lambda *a, **k: consulted.append(1) or None)
    second = doctor.invoke_doctor_agent("p", phase="triage")
    assert first == second == (True, "answer")
    # The one call site asked -- and, with no chain file, ran the legacy path.
    assert consulted, "invoke_doctor_agent no longer consults the chain"


# ── 2. slot awareness ──────────────────────────────────────────────────────


def test_all_slots_busy_falls_through_to_the_next_rung(chain, fake):
    fake.slots = {CERBERUS: 8, TYDEUS: 0}
    fake.chat[CERBERUS] = "cerberus should not answer"
    fake.chat[TYDEUS] = "qwen answers"
    decision = chain.choose("triage")
    assert decision is not None and decision.rung.id == "tydeus-qwen38"
    assert fake.chats_to(CERBERUS) == 0
    assert "busy" in " ".join(decision.reasons_skipped).lower()


def test_reserve_is_respected(chain, fake):
    # 6 busy of 8 with the default reserve of 2 is "full": 6 >= 8 - 2.
    fake.slots = {CERBERUS: 6, TYDEUS: 0}
    fake.chat[TYDEUS] = "qwen"
    assert chain.choose("triage").rung.id == "tydeus-qwen38"
    # 5 busy of 8 is still one slot of headroom: CERBERUS wins. (A fresh chain:
    # the probe cache is what "a few seconds" means, not a live reading.)
    chain.invalidate()
    fake.slots = {CERBERUS: 5, TYDEUS: 0}
    assert chain.choose("triage").rung.id == "cerberus-gpt-oss"
    # A reserve of 4 needs four free slots.
    chain.reserve = 4
    chain.invalidate()
    fake.slots = {CERBERUS: 5}
    assert chain.choose("triage").rung.id == "tydeus-qwen38"


def test_vllm_metrics_decide_the_saturation(chain, fake):
    fake.slots = {CERBERUS: 0}
    fake.chat[CERBERUS] = "cerberus"
    # qwen38's max_concurrent is 4: 3 running + 1 waiting is full.
    fake.metrics = {TYDEUS: (1, 3)}  # running + waiting = 4 == cap
    decision = chain.choose("fix")
    assert decision.rung.id == "cerberus-gpt-oss"
    fake.slots = {CERBERUS: 8}
    chain.invalidate()
    decision = chain.choose("fix")
    assert decision.rung.id == "minimax-m3", decision


def test_probe_results_are_cached_for_a_few_seconds(chain, fake):
    fake.slots = {CERBERUS: 0}
    chain.choose("triage")
    probe_calls = fake.probe_calls
    chain.choose("triage")
    chain.choose("triage")
    assert fake.probe_calls == probe_calls, "a second decision must reuse the cached probe"
    chain.clock.advance(6.0)
    chain.choose("triage")
    assert fake.probe_calls > probe_calls


def test_in_flight_calls_are_counted_against_the_rung(chain, fake):
    fake.slots = {CERBERUS: 4, TYDEUS: 0}
    assert chain.choose("triage").rung.id == "cerberus-gpt-oss"
    with chain.lease(chain.rung("cerberus-gpt-oss")):
        # 4 probe-busy + 2 in flight of our own >= 8 - 2.
        with chain.lease(chain.rung("cerberus-gpt-oss")):
            decision = chain.choose("triage")
            assert decision.rung.id == "tydeus-qwen38"
            assert "in flight" in " ".join(decision.reasons_skipped).lower()
    assert chain.in_flight("cerberus-gpt-oss") == 0


def test_quota_exhausted_rung_is_skipped(chain, fake):
    fake.metrics = {TYDEUS: (4, 0)}
    fake.slots = {CERBERUS: 8}
    chain.mark_quota_exhausted("minimax-m3", "usage limit reached (2056)", retry_after=3600)
    decision = chain.choose("triage")
    assert decision.rung.id == "deepseek-flash", decision
    assert "usage limit reached (2056)" in " ".join(decision.reasons_skipped) or any(
        "quota" in r for r in decision.reasons_skipped
    )
    chain.clock.advance(3601)
    chain.invalidate()
    assert chain.choose("triage").rung.id == "minimax-m3"


def test_minimax_quota_exit_3_marks_m3(chain):
    chain.mark_quota_exhausted("minimax-m3", "minimax-quota exited 3", retry_after=600)
    assert chain.is_quota_blocked("minimax-m3")
    chain.clock.advance(601)
    assert not chain.is_quota_blocked("minimax-m3")


def test_deepseek_peak_window(chain, fake):
    """Peak (Mon-Fri 01:00-04:00 / 06:00-10:00 UTC) doubles the price."""
    from hive.workers import doctor_models

    # 2023-11-14 02:00Z is a Tuesday inside the first peak window.
    peak = doctor_models.deepseek_peak(_utc(2023, 11, 14, 2))
    assert peak
    # 05:00Z is between the two windows.
    assert not doctor_models.deepseek_peak(_utc(2023, 11, 14, 5))
    # 07:00Z is inside the second one.
    assert doctor_models.deepseek_peak(_utc(2023, 11, 14, 7))
    # A Saturday is never peak.
    assert not doctor_models.deepseek_peak(_utc(2023, 11, 18, 2))

    fake.slots = {CERBERUS: 8}
    fake.metrics = {TYDEUS: (4, 0)}
    chain.mark_quota_exhausted("minimax-m3", "usage limit reached (2056)", retry_after=3600)
    # Bulk work in peak: DeepSeek is skipped, Muse answers.
    chain.clock.now = _utc(2023, 11, 14, 2)
    decision = chain.choose("triage")
    assert decision.rung.id == "muse-spark", decision
    assert any("peak" in r for r in decision.reasons_skipped)
    # Operator-blocking work may spend the peak rate.
    chain.invalidate()
    decision = chain.choose("triage", operator_blocking=True)
    assert decision.rung.id == "deepseek-flash", decision


# ── 3. selection ───────────────────────────────────────────────────────────


def test_phase_membership(chain, fake):
    fake.slots = {CERBERUS: 0}
    fake.slots[TYDEUS + "/v1"] = 0
    fake.metrics = {TYDEUS: (0, 0)}
    chain.rung("minimax-m3").phases = ["review"]
    chain.invalidate()
    assert chain.choose("fix").rung.id == "cerberus-gpt-oss"
    chain.rung("cerberus-gpt-oss").phases = []
    fake.metrics = {TYDEUS: (4, 0)}
    chain.invalidate()
    assert chain.choose("fix").rung.id == "deepseek-flash"
    chain.rung("minimax-m3").phases = ["review"]
    for rung in chain.rungs:
        rung.phases = ["triage"] if rung.id != "tydeus-qwen38" else ["fix"]
    chain.clock.now = _utc(2023, 11, 18, 0)  # Saturday: no DeepSeek peak
    chain.invalidate()
    assert chain.choose("fix") is None


def test_review_never_picks_the_authors_family(chain, fake):
    fake.slots = {CERBERUS: 0}
    fake.chat[CERBERUS] = "should not review a gpt-oss author"
    fake.slots[CERBERUS + "/v1"] = 0
    decision = chain.choose("review", author_model="gpt-oss-20b")
    assert decision.rung.id != "cerberus-gpt-oss"
    assert decision.rung.family != "openai"
    assert any("author family" in r for r in decision.reasons_skipped)
    # A tydeus author keeps qwen out of the pick too.
    chain.rung("cerberus-gpt-oss").phases = ["triage"]
    chain.invalidate()
    decision = chain.choose("review", author_model="qwen38")
    assert decision.rung.family not in {"openai", "alibaba"}


def test_review_with_no_cross_family_rung_returns_none(fake, monkeypatch):
    from hive.workers import doctor_models

    monkeypatch.setenv("MUSE_BASE_URL", MUSE)
    only = [doctor_models.ChainRung.from_dict(_rung(id="cerberus-gpt-oss"))]
    chain = doctor_models.ModelChain(rungs=only, urlopen=fake, clock=Clock())
    fake.slots = {CERBERUS: 0}
    assert chain.choose("review", author_model="gpt-oss-20b") is None
    # Triage has no author pairing rule: the same rung is fine.
    assert chain.choose("triage").rung.id == "cerberus-gpt-oss"


def test_every_decision_logs_one_structured_line(chain, fake, caplog):
    fake.slots = {CERBERUS: 8, TYDEUS: 0}
    with caplog.at_level("INFO", logger="doctor.models"):
        decision = chain.choose("triage")
    lines = [r.getMessage() for r in caplog.records if "doctor.chain" in r.getMessage()]
    assert len(lines) == 1, lines
    line = json.loads(lines[0].split("doctor.chain ", 1)[1])
    assert line["phase"] == "triage"
    assert line["rung"] == decision.rung.id
    assert line["model"] == "qwen38"
    assert any(s.startswith("cerberus-gpt-oss") for s in line["skipped"])
    assert decision.rung.id == "tydeus-qwen38"


@pytest.mark.parametrize(
    "reply,structured",
    [
        (503, True),
        ("", True),
        ("   \n ", True),
        ("I think the job should be released.", True),
        ('{"action_type": "release_to_queue", "reason": "trunc', True),
        (None, False),
    ],
)
def test_a_failed_reply_falls_through_within_the_same_call(fake, chain, reply, structured):
    fake.slots = {CERBERUS: 0}
    fake.slots[TYDEUS + "/v1"] = 0
    fake.metrics = {TYDEUS: (0, 0)}
    if reply is None:
        # Prose is a fine answer for a prose phase: the FIRST rung answers and
        # no other rung is touched.
        fake.chat[CERBERUS] = "plain prose is fine here"
        fake.chat[TYDEUS] = '{"action_type": "no_action", "reason": "ok"}'
        outcome = chain.call("triage", system="S", prompt="P", structured=structured)
        assert outcome.ok and outcome.rung == "cerberus-gpt-oss"
        assert outcome.text == "plain prose is fine here"
        assert fake.chats_to(TYDEUS) == 0
        return
    fake.chat[CERBERUS] = reply
    fake.chat[TYDEUS] = '{"action_type": "no_action", "reason": "ok"}'
    outcome = chain.call("triage", system="S", prompt="P", structured=structured)
    assert outcome.ok, outcome.text
    assert outcome.text.strip()
    assert outcome.rung == "tydeus-qwen38"
    assert "cerberus-gpt-oss" in outcome.attempts
    assert fake.chats_to(CERBERUS) == 1
    assert fake.chats_to(TYDEUS) == 1


def test_a_fenced_action_reply_is_usable(fake, chain):
    from hive.workers import doctor_models

    assert doctor_models._acceptable('```json\n{"action_type": "release_to_queue"}\n```', True)
    assert not doctor_models._acceptable("I think the job should be released.", True)
    assert not doctor_models._acceptable('{"action_type": "release_to_queue", "reason": "trunc', True)
    assert not doctor_models._acceptable("   \n ", True)


def test_a_prose_reply_succeeds_for_a_prose_phase(fake, chain):
    fake.slots = {CERBERUS: 0}
    fake.chat[CERBERUS] = "plain prose is fine here"
    outcome = chain.call("triage", system="S", prompt="P", structured=False)
    assert outcome.ok and outcome.rung == "cerberus-gpt-oss" and outcome.text == "plain prose is fine here"


def test_attempts_are_bounded(fake, chain):
    fake.slots = {CERBERUS: 0, TYDEUS: 0}
    fake.metrics = {TYDEUS: (0, 0)}
    for base in (CERBERUS, TYDEUS, M3, DEEPSEEK):
        fake.chat[base] = 503
    os.environ.pop("MUSE_BASE_URL", None)
    outcome = chain.call("triage", system="S", prompt="P", structured=True, max_attempts=2)
    assert not outcome.ok
    assert len(fake.chat_calls) == 2, fake.chat_calls


# ── 4. backoff / cool-down ─────────────────────────────────────────────────


def test_a_429_cools_the_rung_down_with_exponential_backoff(fake, chain):
    fake.slots = {CERBERUS: 0}
    fake.metrics = {TYDEUS: (0, 0)}
    fake.chat[CERBERUS] = 429
    fake.chat[TYDEUS] = "qwen"
    outcome = chain.call("triage", system="S", prompt="P", structured=False)
    assert outcome.ok and outcome.rung == "tydeus-qwen38"
    assert chain.is_cooling("cerberus-gpt-oss")
    first = chain.cooldown_remaining("cerberus-gpt-oss")
    assert first > 0
    # The next call does not even dial the cooled rung.
    before = fake.chats_to(CERBERUS)
    chain.call("triage", system="S", prompt="P", structured=False)
    assert fake.chats_to(CERBERUS) == before
    # A second failure doubles the cool-down.
    chain.clock.advance(first + 1)
    chain.invalidate()
    fake.chat[CERBERUS] = 503
    chain.call("triage", system="S", prompt="P", structured=False)
    assert chain.cooldown_remaining("cerberus-gpt-oss") > first


def test_a_transport_error_is_busy_and_cools_down(fake, chain):
    fake.slots = {CERBERUS: 0}
    fake.metrics = {TYDEUS: (0, 0)}
    fake.chat[CERBERUS] = ConnectionResetError("reset")
    fake.chat[TYDEUS] = "qwen"
    outcome = chain.call("triage", system="S", prompt="P", structured=False)
    assert outcome.ok and outcome.rung == "tydeus-qwen38"
    assert chain.is_cooling("cerberus-gpt-oss")


def test_a_4xx_other_than_429_is_not_retried(fake, chain):
    fake.slots = {CERBERUS: 0}
    fake.metrics = {TYDEUS: (0, 0)}
    fake.chat[CERBERUS] = 400
    fake.chat[TYDEUS] = "qwen"
    chain.call("triage", system="S", prompt="P", structured=False)
    assert fake.chats_to(TYDEUS) == 0
    assert chain.is_cooling("cerberus-gpt-oss")


# ── keys ──────────────────────────────────────────────────────────────────


def test_a_key_is_read_by_name_and_never_from_the_rung(monkeypatch):
    from hive.workers import doctor_models

    monkeypatch.setenv("MINIMAX_API_KEY", "sk-test-value")
    rung = doctor_models.ChainRung.from_dict(_rung(id="m", model="MiniMax-M3", api_key_env="MINIMAX_API_KEY"))
    assert rung.api_key() == "sk-test-value"
    assert "sk-test-value" not in repr(rung)
    monkeypatch.delenv("MINIMAX_API_KEY")
    assert rung.api_key() == ""


def test_contributor_tier_only_for_operator_approved_repos():
    from hive.workers import doctor_models

    for repo in doctor_models.CONTRIBUTOR_TIER_REPOS:
        assert doctor_models.muse_model_for_repo(repo) == doctor_models.MUSE_CONTRIBUTOR_MODEL
    for repo in ("some-other-repo", "", None, "graeae-hive-fork"):
        assert doctor_models.muse_model_for_repo(repo) == doctor_models.MUSE_STANDARD_MODEL


def test_doctor_module_imports_the_chain_and_keeps_its_legacy_path():
    doctor = importlib.import_module("hive.workers.doctor")
    assert doctor.doctor_models.ModelChain
    # One call site: the legacy chain builder is still what invoke_doctor_agent
    # falls back to when DOCTOR_MODEL_CHAIN is unset.
    import inspect

    source = inspect.getsource(doctor.invoke_doctor_agent)
    assert "chain_from_env" in source


# ── module completeness (a chunked reviewer must see call + definition) ────


def test_helpers_a_chunked_reader_calls_are_defined_in_this_module():
    """`_acceptable` and `_attempt` are module-level, next to their caller.

    A truncated diff view once reported both as missing (or as never making an
    HTTP call). They are not: this test resolves each name from the module and
    exercises the real call path end to end."""
    from hive.workers import doctor_models

    assert callable(doctor_models._acceptable)
    assert inspect.signature(doctor_models._acceptable).parameters.keys() == {"text", "structured"}
    assert callable(doctor_models.ModelChain._attempt)
    assert callable(doctor_models.ModelChain.call)
    # _attempt really performs the call and classifies every outcome.
    assert {"ok", "busy", "quota", "config"} <= set(doctor_models._ATTEMPT_KINDS)


def test_attempt_performs_the_http_call_and_classifies_each_outcome(fake, monkeypatch):
    from hive.workers import doctor_models

    rung = doctor_models.ChainRung.from_dict(_rung(id="c", model="gpt-oss-20b", base_url=CERBERUS))
    chain = doctor_models.ModelChain(rungs=[rung], urlopen=fake, clock=Clock())

    fake.chat[CERBERUS] = "answered"
    assert chain._attempt(rung, system="S", prompt="P", timeout=5) == (True, "answered", "ok")
    assert fake.chat_calls == [CERBERUS + "/chat/completions"]

    fake.chat[CERBERUS] = 503
    assert chain._attempt(rung, system="S", prompt="P", timeout=5)[2] == "busy"
    fake.chat[CERBERUS] = 400
    assert chain._attempt(rung, system="S", prompt="P", timeout=5)[2] == "config"

    # A timeout is a transport failure, not a hang: it returns "busy".
    def boom(request, timeout=0):
        raise TimeoutError("timed out")

    monkeypatch.setattr(chain, "urlopen", boom)
    ok, text, kind = chain._attempt(rung, system="S", prompt="P", timeout=5)
    assert (ok, kind) == (False, "busy") and "TimeoutError" in text
