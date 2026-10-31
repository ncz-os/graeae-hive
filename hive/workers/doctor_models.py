"""Slot-aware model chain for the doctor (operator 2026-10-31).

The doctor's main model is gpt-oss (CERBERUS ``gpt-oss-20b``, llama-server
:8080, 8 slots) and it can be overloaded. The operator's rule: give it backups
(MiniMax-M3, DeepSeek, Muse -- and qwen) and make the choice SLOT AWARE. A busy
endpoint is *skipped*, not dialled until the call fails.

One rung of the chain is::

    {id, model, base_url, api_key_env, family, kind, cost_class,
     phases[], max_concurrent, quota_guard}

where

* ``api_key_env`` is the NAME of an env var, never a value. A rung never
  carries a key, and this module never logs or returns one;
* ``family`` is the ONE family table (``hive/core/model-families.json`` through
  ``hive.review_queue.family_of``), so a review rung can never be the author's
  family;
* ``kind`` is ``llama`` (probe ``GET /slots``), ``vllm`` (probe ``GET
  /metrics``) or ``openai`` (hosted: guarded by its quota, not a slot probe);
* ``quota_guard`` is ``minimax`` (a Token Plan 429 ``usage limit reached
  (2056)`` or a ``minimax-quota`` exit 3 blocks the rung) or ``deepseek_peak``
  (bulk work avoids the 2x peak window).

The chain is read from the TOML/JSON file named by ``DOCTOR_MODEL_CHAIN``.
When that variable is unset ``chain_from_env`` returns ``None`` and the doctor
keeps its legacy single-base-per-phase chain: nothing changes for a deploy
that has not opted in.

Everything that talks to the network takes an injected ``urlopen`` and the
chain takes an injected ``clock``, so the tests drive fake servers and a fake
clock and no socket is opened. Pure stdlib.
"""

from __future__ import annotations

import json
import logging
import os
import time
import tomllib
import urllib.error
import urllib.request
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field

from hive.core import fleet_inventory
from hive.review_queue import family_of

log = logging.getLogger("doctor.models")
#: The single-structured-line logger for a chain decision.
decision_log = logging.getLogger("doctor.models.decision")

CHAIN_ENV = "DOCTOR_MODEL_CHAIN"
KINDS = ("llama", "vllm", "openai")
PHASES = ("triage", "review", "fix")

#: Operator-approved repos for the Meta contributor tier (``muse-spark-1.3``
#: vs ``muse-spark-1.3-contributor``). Any other repo gets the standard tier.
CONTRIBUTOR_TIER_REPOS = ("graeae-hive", "fleet-ops", "tokenomics", "ncz-screensavers", "zoder")
MUSE_STANDARD_MODEL = "muse-spark-1.3"
MUSE_CONTRIBUTOR_MODEL = "muse-spark-1.3-contributor"
#: The env var holding the Meta Muse API base (there is no in-source default:
#: an address in source goes stale without anybody noticing).
MUSE_BASE_URL_ENV = "MUSE_BASE_URL"
MUSE_API_KEY_ENV = "MUSE_API_KEY"

#: DeepSeek bills 2x 01:00-04:00 and 06:00-10:00 UTC, Mon-Fri (UTC weekday).
DEEPSEEK_PEAK_HOURS_UTC = ((1, 4), (6, 10))

#: Advertised TLS endpoints. An address is not a fleet address: these are
#: provider API bases that cannot move to the inventory.
MINIMAX_BASE_URL = "https://api.minimax.io/v1"
DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"

#: Probe cache: "a few seconds" -- one decision burst reuses one probe.
PROBE_CACHE_SECS = 5.0
PROBE_TIMEOUT = 2.0
#: Reserve in slots on a llama-server: never take the last two.
DEFAULT_RESERVE = 2

#: First cool-down after a busy rung; each further failure doubles it.
BASE_COOLDOWN_SECS = 30.0
MAX_COOLDOWN_SECS = 900.0
#: How many rungs one call may try before giving up.
DEFAULT_MAX_ATTEMPTS = 4


# ── the peak window ────────────────────────────────────────────────────────


def deepseek_peak(now: float) -> bool:
    """True inside DeepSeek's 2x peak window (Mon-Fri, 01-04 / 06-10 UTC)."""
    dt = time.gmtime(float(now))
    if dt.tm_wday >= 5:  # Sat/Sun
        return False
    return any(start <= dt.tm_hour < end for start, end in DEEPSEEK_PEAK_HOURS_UTC)


def muse_model_for_repo(repo: str | None) -> str:
    """Muse tier for ``repo``: contributor only on an operator-approved repo."""
    name = str(repo or "").strip()
    if name and name in CONTRIBUTOR_TIER_REPOS:
        return MUSE_CONTRIBUTOR_MODEL
    return MUSE_STANDARD_MODEL


# ── a rung ─────────────────────────────────────────────────────────────────


@dataclass
class ChainRung:
    """One rung of the doctor's model chain. Never holds a key value."""

    id: str
    model: str
    base_url: str
    api_key_env: str | None = None
    family: str = ""
    kind: str = "llama"
    cost_class: str = "A"
    phases: list[str] = field(default_factory=lambda: list(PHASES))
    max_concurrent: int = 1
    quota_guard: str | None = None
    #: Local rungs resolve ``base_url`` from the fleet inventory at load time.
    host: str | None = None
    port: int | None = None

    @classmethod
    def from_dict(cls, raw: dict) -> ChainRung:
        """Build a rung from a config mapping; refuse a literal key value."""
        if not isinstance(raw, dict):
            raise ValueError("rung must be a table/object")
        if "api_key" in raw or "key" in raw or "api_key_value" in raw:
            # Never echo the value: the error names the field, and the value is
            # not part of the message.
            raise ValueError(
                f"rung {raw.get('id')!r}: carries a literal api key; name the env var in api_key_env instead"
            )
        rung_id = str(raw.get("id") or "").strip()
        model = str(raw.get("model") or "").strip()
        if not rung_id or not model:
            raise ValueError("rung needs an id and a model")
        kind = str(raw.get("kind") or "llama").strip().lower()
        if kind not in KINDS:
            raise ValueError(f"rung {rung_id}: kind {kind!r} is not one of {KINDS}")
        phases = [str(p).strip().lower() for p in (raw.get("phases") or list(PHASES)) if str(p).strip()]
        for phase in phases:
            if phase not in PHASES:
                raise ValueError(f"rung {rung_id}: unknown phase {phase!r}")
        family = str(raw.get("family") or "").strip().lower() or family_of(model)
        return cls(
            id=rung_id,
            model=model,
            base_url=str(raw.get("base_url") or "").strip().rstrip("/"),
            api_key_env=(str(raw["api_key_env"]).strip() if raw.get("api_key_env") else None),
            family=family,
            kind=kind,
            cost_class=str(raw.get("cost_class") or "A").strip().upper(),
            phases=phases,
            max_concurrent=max(1, int(raw.get("max_concurrent") or 1)),
            quota_guard=(str(raw["quota_guard"]).strip() or None) if raw.get("quota_guard") else None,
            host=(str(raw["host"]).strip().lower() or None) if raw.get("host") else None,
            port=(int(raw["port"]) if raw.get("port") is not None else None),
        )

    def api_key(self, env: dict | None = None) -> str:
        """The key VALUE, read from the env var this rung NAMES (never printed)."""
        name = self.api_key_env
        if not name:
            return ""
        source = os.environ if env is None else env
        return str(source.get(name) or "").strip()

    @property
    def server_root(self) -> str:
        """``scheme://host:port`` for the slot probe (never the ``/v1`` prefix)."""
        base = self.base_url
        if not base:
            return ""
        root = base[:-3] if base.endswith("/v1") else base
        return root.rstrip("/")

    def __repr__(self) -> str:  # never a key value: the rung holds none
        return f"ChainRung(id={self.id!r}, model={self.model!r}, base_url={self.base_url!r}, kind={self.kind!r})"


# ── the chain file ─────────────────────────────────────────────────────────


@dataclass
class ChainConfig:
    reserve: int = DEFAULT_RESERVE
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    rungs: list[ChainRung] = field(default_factory=list)


def _read_chain_document(path: str) -> dict:
    """A TOML or JSON chain document (by suffix, TOML by default)."""
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    data = json.loads(text) if str(path).lower().endswith(".json") else tomllib.loads(text)
    if not isinstance(data, dict):
        raise ValueError(f"{path}: the chain file must be a table/object")
    return data


def load_chain_file(path: str) -> ChainConfig:
    """Parse a chain file into a :class:`ChainConfig`."""
    data = _read_chain_document(path)
    rungs = []
    for raw in data.get("rungs") or []:
        rungs.append(ChainRung.from_dict(raw))
    if not rungs:
        raise ValueError(f"{path}: the chain file declares no rungs")
    return ChainConfig(
        reserve=max(0, int(data.get("reserve", DEFAULT_RESERVE))),
        max_attempts=max(1, int(data.get("max_attempts", DEFAULT_MAX_ATTEMPTS))),
        rungs=rungs,
    )


# ── the default chain ──────────────────────────────────────────────────────


def _local_base(host: str, port: int, inventory: dict | None = None) -> str:
    """``http://<inventory address>:<port>/v1`` or ``""`` when the host is unknown."""
    address = fleet_inventory.address(host, inventory=inventory)
    if not address:
        return ""
    if ":" in address and not address.startswith("["):
        address = f"[{address}]"  # IPv6 literal
    return f"http://{address}:{port}/v1"


def default_rungs(
    inventory: dict | None = None,
    *,
    env: dict | None = None,
    repo: str | None = None,
) -> list[ChainRung]:
    """The operator's default order, with local addresses from the inventory.

    gpt-oss-20b on CERBERUS, qwen38 on TYDEUS (vLLM :8006), MiniMax-M3,
    deepseek-flash, muse-spark. A local rung whose host the inventory does not
    know has an empty ``base_url`` and is skipped by the chain (never guessed).
    """
    source = os.environ if env is None else env
    muse_base = str(source.get(MUSE_BASE_URL_ENV) or "").strip().rstrip("/")
    raw = [
        {
            "id": "cerberus-gpt-oss",
            "model": "gpt-oss-20b",
            "host": "cerberus",
            "port": 8080,
            "base_url": _local_base("cerberus", 8080, inventory),
            "kind": "llama",
            "cost_class": "A",
            "max_concurrent": 8,
            "phases": ["triage", "review", "fix"],
        },
        {
            "id": "tydeus-qwen38",
            "model": "qwen38",
            "host": "tydeus",
            "port": 8006,
            "base_url": _local_base("tydeus", 8006, inventory),
            "kind": "vllm",
            "cost_class": "A",
            "max_concurrent": 4,
            "phases": ["triage", "review", "fix"],
        },
        {
            "id": "minimax-m3",
            "model": "MiniMax-M3",
            "base_url": MINIMAX_BASE_URL,
            "kind": "openai",
            "api_key_env": "MINIMAX_API_KEY",
            "cost_class": "B",
            "max_concurrent": 2,
            "quota_guard": "minimax",
            "phases": ["triage", "review", "fix"],
        },
        {
            "id": "deepseek-flash",
            "model": "deepseek-flash",
            "base_url": DEEPSEEK_BASE_URL,
            "kind": "openai",
            "api_key_env": "DEEPSEEK_API_KEY",
            "cost_class": "B",
            "max_concurrent": 2,
            "quota_guard": "deepseek_peak",
            "phases": ["triage", "review", "fix"],
        },
        {
            "id": "muse-spark",
            "model": muse_model_for_repo(repo),
            "base_url": muse_base,
            "kind": "openai",
            "api_key_env": MUSE_API_KEY_ENV,
            "cost_class": "B",
            "max_concurrent": 2,
            "phases": ["triage", "review", "fix"],
        },
    ]
    rungs = [ChainRung.from_dict(entry) for entry in raw]
    for rung in rungs:
        rung.family = rung.family or family_of(rung.model)
    return rungs


# ── the probe ──────────────────────────────────────────────────────────────


class _Probe:
    """Per-rung capacity cache: ``{rung id: (expires_at, busy, detail)}``."""

    def __init__(self, urlopen=None, clock=None, cache_secs: float = PROBE_CACHE_SECS):
        self.urlopen = urlopen or urllib.request.urlopen
        self.clock = clock or time.time
        self.cache_secs = cache_secs
        self.cache: dict[str, tuple[float, float | None, str]] = {}

    def invalidate(self, rung_id: str | None = None) -> None:
        if rung_id is None:
            self.cache.clear()
        else:
            self.cache.pop(rung_id, None)

    def __call__(self, rung: ChainRung) -> tuple[float | None, str]:
        """``(busy, detail)``; busy ``None`` means "could not be read"."""
        if rung.kind not in ("llama", "vllm"):
            return 0.0, "no slot probe for kind openai"
        now = self.clock()
        hit = self.cache.get(rung.id)
        if hit and hit[0] > now:
            return hit[1], hit[2]
        busy, detail = self._fetch(rung)
        self.cache[rung.id] = (now + self.cache_secs, busy, detail)
        return busy, detail

    def _fetch(self, rung: ChainRung) -> tuple[float | None, str]:
        root = rung.server_root
        if not root:
            return None, "no address"
        try:
            if rung.kind == "vllm":
                return _vllm_busy(self._read(root + "/metrics"))
            return _llama_busy(self._read(root + "/slots"))
        except Exception as exc:  # noqa: BLE001 -- any probe failure is "unreadable"
            return None, f"probe failed: {type(exc).__name__}"

    def _read(self, url: str) -> str:
        with self.urlopen(url, timeout=PROBE_TIMEOUT) as resp:  # noqa: S310
            body = resp.read(1 << 20)
        return body.decode("utf-8", "replace") if isinstance(body, bytes) else str(body)


def _llama_busy(text: str) -> tuple[float | None, str]:
    """Busy slots from an llama-server ``/slots`` body."""
    data = json.loads(text)
    slots = data.get("slots") if isinstance(data, dict) else data
    if not isinstance(slots, list):
        return None, "unparsable /slots"
    busy = sum(1 for slot in slots if isinstance(slot, dict) and slot.get("is_processing"))
    return float(busy), f"{busy}/{len(slots)} busy"


def _metric_value(line: str) -> float | None:
    try:
        return float(line.rsplit(None, 1)[-1])
    except (IndexError, ValueError):
        return None


def _vllm_busy(text: str) -> tuple[float | None, str]:
    """Running + waiting from vLLM's Prometheus text."""
    running = waiting = None
    for line in text.splitlines():
        if line.startswith("vllm:num_requests_running"):
            running = _metric_value(line)
        elif line.startswith("vllm:num_requests_waiting"):
            waiting = _metric_value(line)
    if running is None:
        return None, "unparsable /metrics"
    return running + (waiting or 0), f"running={running:g} waiting={waiting or 0:g}"


# ── the decision ───────────────────────────────────────────────────────────


@dataclass
class Decision:
    """The rung the chain picked, plus why the rest were skipped."""

    rung: ChainRung
    phase: str
    reasons_skipped: list[str] = field(default_factory=list)


@dataclass
class CallOutcome:
    ok: bool
    text: str
    rung: str | None = None
    attempts: list[str] = field(default_factory=list)


class ModelChain:
    """First healthy, non-busy rung for the phase wins (slot aware).

    Per rung the chain tracks: its cool-down after a busy reply (429/503/a
    timeout), its quota block (MiniMax Token Plan), and how many calls the
    doctor itself has in flight (a lease the caller holds around one call).
    """

    def __init__(
        self,
        rungs: list[ChainRung],
        *,
        urlopen=None,
        clock=None,
        probe_cache_secs: float = PROBE_CACHE_SECS,
        reserve: int = DEFAULT_RESERVE,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        structured_reply: bool = True,
    ):
        self.rungs = list(rungs)
        self.reserve = max(0, int(reserve))
        self.max_attempts = max(1, int(max_attempts))
        self.structured_reply = structured_reply
        self.clock = clock or time.time
        self.urlopen = urlopen or urllib.request.urlopen
        self.probe = _Probe(self.urlopen, self.clock, probe_cache_secs)
        self._probe_memo: dict[str, tuple[float, float | None, str]] = {}
        self._in_flight: Counter = Counter()
        self._cooldown_until: dict[str, float] = {}
        self._cooldown_failures: Counter = Counter()
        self._quota_until: dict[str, float] = {}
        self._quota_reason: dict[str, str] = {}

    def _probe_rung(self, rung: ChainRung) -> tuple[float | None, str]:
        """``probe`` with this chain's own cache (invalidate() clears it too)."""
        now = self.clock()
        hit = self._probe_memo.get(rung.id)
        if hit and hit[0] > now:
            return hit[1], hit[2]
        busy, detail = self.probe(rung)
        self._probe_memo[rung.id] = (now + self.probe.cache_secs, busy, detail)
        return busy, detail

    def invalidate(self, rung_id: str | None = None) -> None:
        """Drop cached probe results (this chain's memo and the shared probe)."""
        if rung_id is None:
            self._probe_memo.clear()
        else:
            self._probe_memo.pop(rung_id, None)
        self.probe.invalidate(rung_id)

    # -- lookup ---------------------------------------------------------
    def rung(self, rung_id: str) -> ChainRung:
        for rung in self.rungs:
            if rung.id == rung_id:
                return rung
        raise KeyError(rung_id)

    def in_flight(self, rung_id: str) -> int:
        return int(self._in_flight.get(rung_id, 0))

    @contextmanager
    def lease(self, rung: ChainRung):
        """Count one call of the doctor's own against ``rung``."""
        self._in_flight[rung.id] += 1
        try:
            yield rung
        finally:
            self._in_flight[rung.id] = max(0, self._in_flight[rung.id] - 1)

    # -- busy / quota / cooldown ----------------------------------------
    def is_cooling(self, rung_id: str) -> bool:
        return self._cooldown_until.get(rung_id, 0.0) > self.clock()

    def cooldown_remaining(self, rung_id: str) -> float:
        return max(0.0, self._cooldown_until.get(rung_id, 0.0) - self.clock())

    def is_quota_blocked(self, rung_id: str) -> bool:
        return self._quota_until.get(rung_id, 0.0) > self.clock()

    def mark_busy(self, rung_id: str, reason: str) -> float:
        """Cool a busy rung down; each further failure doubles the window."""
        failures = self._cooldown_failures[rung_id] + 1
        self._cooldown_failures[rung_id] = failures
        delay = min(BASE_COOLDOWN_SECS * (2 ** (failures - 1)), MAX_COOLDOWN_SECS)
        self._cooldown_until[rung_id] = self.clock() + delay
        log.info("doctor.chain.busy rung=%s reason=%s cooldown=%ss", rung_id, reason[:120], int(delay))
        return delay

    def mark_quota_exhausted(self, rung_id: str, reason: str, retry_after: float = 3600.0) -> None:
        """Block a rung until its quota window lapses (M3 Token Plan)."""
        self._quota_until[rung_id] = self.clock() + max(1.0, float(retry_after))
        self._quota_reason[rung_id] = str(reason or "quota exhausted")[:200]
        self.invalidate(rung_id)
        log.warning("doctor.chain.quota rung=%s reason=%s", rung_id, self._quota_reason[rung_id])

    # -- selection -------------------------------------------------------
    def _skip_reason(self, rung: ChainRung, phase: str, author_family: str | None) -> str | None:
        """Why ``rung`` cannot serve ``phase`` right now; None means it can."""
        if not rung.base_url:
            return "no address configured"
        if phase not in rung.phases:
            return f"not a {phase} rung"
        if author_family and rung.family == author_family:
            return f"author family {author_family}"
        if self.is_quota_blocked(rung.id):
            return f"quota: {self._quota_reason.get(rung.id, 'exhausted')}"
        if self.is_cooling(rung.id):
            return f"cooling down {int(self.cooldown_remaining(rung.id))}s"
        return None

    def capacity_skip(self, rung: ChainRung, *, operator_blocking: bool = False) -> str | None:
        """The slot/quota/price reason ``rung`` is busy now; None means it is free."""
        if rung.quota_guard == "deepseek_peak" and not operator_blocking and deepseek_peak(self.clock()):
            return "deepseek peak window (2x price); bulk work waits"
        # The probe cache keys on the rung, and a test (or an operator) may keep
        # several chains over the same fake/real fleet in one process, so a
        # cache entry is only reused by the chain that measured it.
        busy, detail = self._probe_rung(rung)
        if busy is None:
            # An unreadable probe is not proof of capacity: fail closed.
            return f"capacity unreadable ({detail})"
        if rung.kind == "llama":
            limit = rung.max_concurrent
            if busy + self.in_flight(rung.id) >= max(1, limit - self.reserve):
                return f"busy {detail} + {self.in_flight(rung.id)} in flight, reserve {self.reserve}"
        else:
            if busy + self.in_flight(rung.id) >= rung.max_concurrent:
                return f"saturated {detail} + {self.in_flight(rung.id)} in flight (cap {rung.max_concurrent})"
        return None

    def choose(
        self,
        phase: str,
        *,
        author_model: str | None = None,
        operator_blocking: bool = False,
        prefer_unread: bool = False,
    ) -> Decision | None:
        """The first healthy, non-busy rung for ``phase``; None if none is free.

        ``author_model`` is the job's author: a REVIEW rung of the author's
        family is never chosen (one family table, ``review_queue.family_of``).
        ``prefer_unread`` re-probes rungs whose cached reading was "busy" and
        take them first (a busy site comes back first; a probe cache entry is
        only "a few seconds" old, so an exact answer that arrived by transport
        error is retried before a colder rung).
        """
        author_family = None
        if phase == "review" and str(author_model or "").strip():
            author_family = family_of(str(author_model).strip())
        skipped: list[str] = []
        for rung in self.rungs:
            if prefer_unread and rung.id in self._probe_memo:
                self.invalidate(rung.id)
            reason = self._skip_reason(rung, phase, author_family)
            if reason is None:
                reason = self.capacity_skip(rung, operator_blocking=operator_blocking)
            if reason:
                skipped.append(f"{rung.id}: {reason}")
                continue
            decision = Decision(rung=rung, phase=phase, reasons_skipped=skipped)
            self._log_decision(decision)
            return decision
        decision_log.info(
            "doctor.chain %s",
            json.dumps(
                {"phase": phase, "rung": None, "model": None, "author_family": author_family, "skipped": skipped},
                sort_keys=True,
            ),
        )
        return None

    def _log_decision(self, decision: Decision) -> None:
        decision_log.info(
            "doctor.chain %s",
            json.dumps(
                {
                    "phase": decision.phase,
                    "rung": decision.rung.id,
                    "model": decision.rung.model,
                    "family": decision.rung.family,
                    "skipped": decision.reasons_skipped,
                },
                sort_keys=True,
            ),
        )

    # -- the call --------------------------------------------------------
    def call(
        self,
        phase: str,
        *,
        system: str,
        prompt: str,
        structured: bool | None = None,
        author_model: str | None = None,
        operator_blocking: bool = False,
        max_attempts: int | None = None,
        timeout: float = 120.0,
    ) -> CallOutcome:
        """Try rungs for ``phase`` until one answers; bounded attempts.

        A busy reply (429/503/a transport error) cools the rung down and moves
        on. A 4xx that is not 429 is a config error that would fail everywhere:
        it cools the rung and STOPS. A reply that is empty, prose-only for a
        structured phase or truncated is a failure of that rung and the next
        rung is tried inside the same call.
        """
        tries = max(1, int(self.max_attempts if max_attempts is None else max_attempts))
        structured = self.structured_reply if structured is None else structured
        attempts: list[str] = []
        rung_unread = False
        for _ in range(tries):
            # `prefer_unread` on a repeat is NOT part of the first pass: the
            # decision is logged exactly once per attempt.
            decision = self.choose(
                phase,
                author_model=author_model,
                operator_blocking=operator_blocking,
                prefer_unread=rung_unread,
            )
            if decision is None:
                break
            rung = decision.rung
            attempts.append(rung.id)
            rung_unread = False
            with self.lease(rung):
                ok, text, kind = self._attempt(rung, system=system, prompt=prompt, timeout=timeout)
            if ok and _acceptable(text, structured):
                return CallOutcome(ok=True, text=text.strip(), rung=rung.id, attempts=attempts)
            if ok:
                # A reply that arrived but is unusable: this rung failed.
                self.mark_busy(rung.id, "unusable reply (empty/prose/truncated)")
                continue
            if kind == "busy":
                self.mark_busy(rung.id, text)
                self.invalidate(rung.id)
                rung_unread = True
                continue
            if kind == "quota":
                self.mark_quota_exhausted(rung.id, text)
                continue
            if kind == "config":
                self.mark_busy(rung.id, text)
                break
            break
        return CallOutcome(ok=False, text="; ".join(attempts) or "no rung available", attempts=attempts)

    def _attempt(self, rung: ChainRung, *, system: str, prompt: str, timeout: float) -> tuple[bool, str, str]:
        """One HTTP call. Returns ``(ok, text, kind)`` where kind is
        ``ok|busy|quota|config|transport``."""
        body = {
            "model": rung.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.0,
            "stream": False,
        }
        url = f"{rung.base_url}/chat/completions"
        headers = {"content-type": "application/json"}
        key = rung.api_key()
        if key:
            headers["authorization"] = f"Bearer {key}"
        request = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST", headers=headers)
        log.info("doctor.chain.call rung=%s model=%s", rung.id, rung.model)
        try:
            with self.urlopen(request, timeout=timeout) as resp:
                payload = json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return False, f"HTTP {exc.code}", _http_kind(rung, exc.code)
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            return False, f"transport {type(exc).__name__}", "busy"
        except ValueError:
            return False, "unparsable body", "busy"
        try:
            text = (payload.get("choices") or [{}])[0].get("message", {}).get("content") or ""
        except (AttributeError, IndexError, TypeError):
            return False, "unparsable body", "busy"
        return True, str(text), "ok"


def _http_kind(rung: ChainRung, status: int) -> str:
    """Classify an HTTP status for the chain."""
    if status == 429:
        return "quota" if rung.quota_guard else "busy"
    if status in (408, 500, 502, 503, 504) or status >= 500:
        return "busy"
    return "config"


def _acceptable(text: str, structured: bool) -> bool:
    """Is this reply usable for the phase?

    Empty or whitespace is never usable. For a structured phase the reply must
    carry a JSON object -- prose and a truncated object are failures of the
    rung (operator 2026-10-31)."""
    body = str(text or "").strip()
    if not body:
        return False
    if not structured:
        return True
    from hive.workers.doctor import parse_doctor_response  # local: avoid an import cycle

    obj = parse_doctor_response(body)
    if not obj:
        return False
    # doctor.parse_doctor_response falls back to the largest braced span, so a
    # TRUNCATED '{"action_type": "release_to_queue", "reason": "trunc' parses
    # as a bare object. A structured action needs its action_type.
    return bool(str(obj.get("action_type") or "").strip())


# ── the env entry point ────────────────────────────────────────────────────


def chain_from_env(
    *,
    urlopen=None,
    clock=None,
    inventory: dict | None = None,
    env: dict | None = None,
    repo: str | None = None,
) -> ModelChain | None:
    """A :class:`ModelChain` when ``DOCTOR_MODEL_CHAIN`` is set, else ``None``.

    ``None`` is the contract with the doctor's call site: the legacy
    single-base-per-phase chain is kept, byte for byte, for every deploy that
    has not opted in.
    """
    source = os.environ if env is None else env
    path = str(source.get(CHAIN_ENV) or "").strip()
    if not path:
        return None
    cfg = load_chain_file(path)
    return ModelChain(
        cfg.rungs,
        urlopen=urlopen,
        clock=clock,
        reserve=cfg.reserve,
        max_attempts=cfg.max_attempts,
    )


__all__ = [
    "CHAIN_ENV",
    "CONTRIBUTOR_TIER_REPOS",
    "MUSE_CONTRIBUTOR_MODEL",
    "MUSE_STANDARD_MODEL",
    "CallOutcome",
    "ChainConfig",
    "ChainRung",
    "Decision",
    "ModelChain",
    "chain_from_env",
    "deepseek_peak",
    "default_rungs",
    "load_chain_file",
    "muse_model_for_repo",
]
