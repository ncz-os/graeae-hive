#!/usr/bin/env python3
"""Durable safety state for the Hive doctor (zeroclaw SOP phase 3, 2026-10-22).

The doctor keeps four counters that are **safety limits**, and it keeps all four
in module-level dicts:

| State | Today | Limit it enforces |
|---|---|---|
| `_seen_job_attempts` | dict of job id -> list of timestamps | loop detection (escalate at 3 cycles) |
| `_recent_auto_triage` | dict of base_kind -> timestamp | the per-kind re-triage cooldown |
| `_dlq_seen` | dict of job id -> timestamp | do not re-decide a dead-letter job every cycle |
| `_ssh_actions_log` / `_host_last_action` | deque + dict | SSH rate limit and per-host cooldown |

A restart wipes all of them. That is not a performance problem: it resets every
safety limit the doctor has. After a pod restart the SSH rate limiter believes it
has never restarted anything, the loop detector believes it has never seen the job,
and the DLQ sweep re-decides every dead-letter job it has already decided — which
is how a broken worker gets another cycle.

This module moves them behind a store whose source of truth is **outside the
process**, with an in-memory cache in front so the 30-second loop does not pay an
HTTP round trip per read:

    store = state_store_from_env()          # or DurableStateStore(backend)
    history = store.touch_timestamps("loop:job-1", window=1800.0)
    if len(history) >= 3:
        ...                                   # escalate

## Backends, and what happens when they are not configured

`DOCTOR_STATE_BACKEND` picks one:

- `auto` (default) — `bus` when a bus URL is reachable-looking (one is configured
  and `DOCTOR_STATE_BACKEND` was not forced), else `file`, else `memory`.
- `bus` — records travel as `doctor.state` messages on the bus the doctor already
  talks to. No bus change, no new route, no new table.
- `file` — one JSON file, replaced atomically. Durable across a restart and needs
  no network.
- `memory` — today's behaviour, with one difference: the store logs, once, that
  the safety limits will reset on restart. Silently pretending otherwise is the
  defect this module exists to remove.

Every backend failure is non-fatal: the cache keeps serving, so a bus outage
degrades the doctor's durability, never its safety. It never makes a limit *looser*
than the cache says, and the cache is only ever populated from a real read or a
real write.

Env:
  DOCTOR_STATE_BACKEND   auto | bus | file | memory (default: auto)
  DOCTOR_STATE_PATH      file backend path (default: none -> memory)
  DOCTOR_STATE_NAMESPACE bus topic prefix (default: doctor.state)
  DOCTOR_STATE_REFRESH   cache refresh interval, seconds (default: 30)
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
from contextlib import suppress

log = logging.getLogger("doctor.state")

DEFAULT_NAMESPACE = "doctor.state"
DEFAULT_REFRESH_SECS = 30.0

# Every key the doctor reads, with its default TTL. Listing them here is what
# makes "which safety limits are durable" a question with an answer rather than a
# grep, and the expiry is the limit's own window: an entry older than its window
# is no longer evidence of anything.
COUNTER_TTLS = {
    "loop": 1800.0,  # LOOP_WINDOW_SEC
    "cooldown": 3600.0,  # AUTO_TRIAGE_COOLDOWN
    "dlq": 21600.0,  # DLQ_SEEN_TTL
    "ssh": 3600.0,  # MAX_SSH_PER_HOST window
    "host_cooldown": 1800.0,  # MIN_HOST_COOLDOWN
    "token_fix": 3600.0,  # TOKEN_FIX_WINDOW_SEC
    "failures": 86400.0,  # a repository's recorded cycle failures
}


def _now() -> float:
    return time.time()


class StateBackend:
    """The durable side of the store. Three implementations, one contract."""

    name = "abstract"

    def load(self, namespace: str) -> dict:
        """Every live record as ``{key: value}``. Unreachable means ``{}``."""
        raise NotImplementedError

    def save(self, namespace: str, records: dict) -> bool:
        """Replace the namespace's records. True when it is believed durable."""
        raise NotImplementedError


class MemoryBackend(StateBackend):
    """Today's behaviour, named so the log line is honest about it."""

    name = "memory"

    def __init__(self):
        self._records: dict[str, dict] = {}

    def load(self, namespace: str) -> dict:
        return {key: dict(value) for key, value in self._records.items()}

    def save(self, namespace: str, records: dict) -> bool:
        self._records = {key: dict(value) for key, value in records.items()}
        return True


class FileBackend(StateBackend):
    """One JSON file, replaced atomically, so a crash cannot truncate it."""

    name = "file"

    def __init__(self, path: str):
        self.path = os.path.expanduser(path)

    def load(self, namespace: str) -> dict:
        try:
            with open(self.path, encoding="utf-8") as handle:
                blob = json.load(handle)
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as error:
            log.warning("state file %s unreadable (%s); treating as empty", self.path, error)
            return {}
        if not isinstance(blob, dict):
            log.warning("state file %s is not an object; treating as empty", self.path)
            return {}
        records = blob.get(namespace)
        if not isinstance(records, dict):
            return {}
        return {str(key): dict(value) for key, value in records.items() if isinstance(value, dict)}

    # The namespace is the CONTAINER, not a key prefix: the store's keys are bare
    # (`loop:<job id>`), and this backend persists every one of them under the
    # namespace. An earlier version filtered on a `"<namespace>/"` prefix the store
    # never wrote, so every load came back empty and a restart silently reset every
    # safety limit — which is the exact defect this module exists to remove, with
    # the failure on the read side instead of the write side.

    def save(self, namespace: str, records: dict) -> bool:
        blob: dict = {}
        try:
            with open(self.path, encoding="utf-8") as handle:
                existing = json.load(handle)
            if isinstance(existing, dict):
                blob = existing
        except FileNotFoundError:
            pass
        except (OSError, ValueError):
            # A corrupt file must not stop the doctor writing a good one; the
            # namespace is replaced wholesale either way.
            log.warning("state file %s unreadable before save; rewriting", self.path)
        blob[namespace] = records
        directory = os.path.dirname(self.path) or "."
        # Write to a sibling temp file and `os.replace` it in, so a crash mid-write
        # cannot leave a truncated state file where the real one was. A partial
        # file would read as "no records", i.e. every safety limit reset.
        try:
            os.makedirs(directory, exist_ok=True)
            fd, temp_path = tempfile.mkstemp(dir=directory, prefix=".doctor-state-")
        except OSError as error:
            log.warning("state file %s not writable (%s); keeping in memory", self.path, error)
            return False
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(blob, handle)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, self.path)
            return True
        except OSError as error:
            log.warning("state file %s not writable (%s); keeping in memory", self.path, error)
            with suppress(OSError):
                os.unlink(temp_path)
            return False


class BusBackend(StateBackend):
    """Records travel as ``<namespace>`` messages on the bus.

    The doctor already publishes to ``/v1/messages`` and already polls
    ``GET /v1/messages``, so this needs no new route and no migration. The newest
    record per key wins, which makes the message log an append-only journal the
    store replays.

    ``http`` is a callable ``(method, path, body) -> (status, response)`` so this
    module does not import the doctor (and so a test can pass a stub).
    """

    name = "bus"

    def __init__(self, http, topic: str = DEFAULT_NAMESPACE, actor_urn: str = "", limit: int = 500):
        self._http = http
        self.topic = topic
        self.actor_urn = actor_urn
        self.limit = int(limit)

    def load(self, namespace: str) -> dict:
        status, response = self._http("GET", f"/v1/messages?topic={namespace}&limit={self.limit}")
        if status != 200 or not isinstance(response, dict):
            log.warning("bus state read failed (status=%s); serving from cache", status)
            return {}
        messages = response.get("messages") or []
        newest: dict[str, tuple[float, dict]] = {}
        for message in messages:
            payload = message.get("payload")
            if not isinstance(payload, dict):
                continue
            key = payload.get("key")
            value = payload.get("value")
            if not isinstance(key, str) or not isinstance(value, dict):
                continue
            ts = float(message.get("ts") or 0.0)
            previous = newest.get(key)
            if previous is None or ts >= previous[0]:
                newest[key] = (ts, value)
        return {key: value for key, (_ts, value) in newest.items()}

    def save(self, namespace: str, records: dict) -> bool:
        written = 0
        for key, value in records.items():
            status, _response = self._http(
                "POST",
                "/v1/messages",
                {"from_urn": self.actor_urn, "topic": namespace, "payload": {"key": key, "value": value}},
            )
            if status in (200, 201, 202, 204):
                written += 1
        return written == len(records)


class DurableStateStore:
    """The doctor's safety state, with a cache in front of a durable backend.

    ``cache`` is never *looser* than what was written: a read that fails returns
    the cache, and a write always updates the cache first and the backend second.
    So a backend outage costs durability, not correctness — the limit the process
    is enforcing right now is the limit it keeps enforcing.
    """

    def __init__(
        self,
        backend: StateBackend,
        namespace: str = DEFAULT_NAMESPACE,
        refresh_secs: float = DEFAULT_REFRESH_SECS,
        time_fn=_now,
    ):
        self.backend = backend
        self.namespace = namespace
        self.refresh_secs = float(refresh_secs)
        self._time = time_fn
        self._lock = threading.RLock()
        self._cache: dict[str, dict] = {}
        self._loaded_at: float | None = None
        self._warned_memory = False
        # Whether the last write is believed to have reached the backend. The
        # cache always holds the newer value, so an in-process limit is never
        # looser than what it should be; what a failed write costs is DURABILITY
        # across a restart, and a caller whose limit is a safety boundary (the SSH
        # rate limiter) can fail closed on this rather than assume the write landed.
        self.persist_ok = True

    # ── the store surface the doctor uses ──────────────────────────────

    def timestamps(self, key: str, ttl: float) -> list[float]:
        """Every recorded timestamp for ``key`` still inside ``ttl``, oldest first."""
        now = self._time()
        record = self._get(key, ttl=ttl)
        stamps = [float(stamp) for stamp in record.get("timestamps", []) if isinstance(stamp, (int, float))]
        return sorted(stamp for stamp in stamps if now - stamp <= ttl)

    def touch(self, key: str, ttl: float, at: float | None = None) -> list[float]:
        """Record "now" for ``key`` and return the windowed list including it."""
        # Load BEFORE writing. A store that has not read yet holds an empty cache,
        # and saving that cache would erase everything the previous process left
        # behind — which is worse than the in-memory state this module replaced,
        # because it looks durable while silently discarding the history on the
        # first write after a restart.
        self._ensure_loaded()
        with self._lock:
            now = self._time() if at is None else float(at)
            record = self._record_locked(key, ttl=ttl)
            stamps = sorted(
                float(stamp)
                for stamp in record.get("timestamps", [])
                if isinstance(stamp, (int, float)) and now - float(stamp) <= ttl
            )
            stamps.append(now)
            # Sorted AFTER the append, not before: `at` is caller-supplied and may
            # be in the past, and the contract on the return value is oldest-first.
            stamps.sort()
            self._write_locked(key, {"timestamps": stamps}, ttl=ttl)
            return stamps

    def mark(self, key: str, ttl: float, at: float | None = None) -> float:
        """Record a single timestamp for ``key`` (the cooldown / seen-set shape)."""
        now = self._time() if at is None else float(at)
        self._ensure_loaded()
        with self._lock:
            self._write_locked(key, {"at": now}, ttl=ttl)
        return now

    def last_mark(self, key: str) -> float | None:
        record = self._get(key, ttl=None)
        value = record.get("at")
        return float(value) if isinstance(value, (int, float)) else None

    def seen(self, key: str, ttl: float) -> bool:
        """Whether ``key`` was marked inside ``ttl``.

        A window of zero admits **nothing**, including the mark made at that very
        instant: `<=` would admit it (`0 <= 0`), which turns "this approval has
        already expired" into "this approval is valid". Non-positive windows read
        as expired rather than as unlimited.
        """
        if ttl <= 0:
            return False
        last = self.last_mark(key)
        if last is None:
            return False
        return (self._time() - last) <= ttl

    def value(self, key: str, default=None):
        record = self._get(key, ttl=None)
        return record.get("value", default)

    def set_value(self, key: str, value, ttl: float) -> None:
        """Store one value under a key, stamped so it can expire like any other."""
        self._ensure_loaded()
        with self._lock:
            self._write_locked(key, {"value": value, "at": self._time()}, ttl=ttl)

    def keys(self, prefix: str = "") -> list[str]:
        self._ensure_loaded()
        with self._lock:
            return sorted(key for key in self._cache if key.startswith(prefix))

    def forget(self, key: str) -> None:
        self._ensure_loaded()
        with self._lock:
            self._cache.pop(key, None)
            self._persist_locked()

    def refresh(self) -> None:
        """Drop the cache so the next read comes from the backend."""
        with self._lock:
            self._loaded_at = None
            self._cache = {}

    # ── internals ──────────────────────────────────────────────────────

    def _ensure_loaded(self) -> None:
        now = self._time()
        with self._lock:
            if self._loaded_at is not None and (now - self._loaded_at) < self.refresh_secs:
                return
            try:
                loaded = self.backend.load(self.namespace)
            except Exception as error:  # noqa: BLE001 - a backend must never stop the doctor
                log.warning("state load failed (%s); serving from cache", error)
                loaded = {}
            # A load that came back empty while the cache holds records is a
            # backend outage, not a wipe: keep what we know. `refresh()` is how
            # an operator asks for a genuine reload.
            if loaded:
                self._cache = loaded
            elif self._loaded_at is None:
                if self._cache:
                    log.warning("state load returned nothing; keeping %d cached records", len(self._cache))
                self._cache = {}
            self._loaded_at = now
            if self.backend.name == "memory" and not self._warned_memory:
                self._warned_memory = True
                log.warning(
                    "doctor safety state is IN MEMORY: cooldowns, loop history, DLQ-seen and the "
                    "SSH rate counters reset on every restart. Set DOCTOR_STATE_PATH (file) or "
                    "DOCTOR_STATE_BACKEND=bus to make them durable."
                )

    def _get(self, key: str, ttl: float | None) -> dict:
        self._ensure_loaded()
        with self._lock:
            record = self._cache.get(key) or {}
        if ttl is None:
            return record
        return record if self._is_live(record, ttl) else {}

    def _record_locked(self, key: str, ttl: float) -> dict:
        record = self._cache.get(key) or {}
        return record if self._is_live(record, ttl) else {}

    def _is_live(self, record: dict, ttl: float) -> bool:
        """Whether a record still carries evidence, judged on its newest stamp.

        A record with no timestamp at all is NOT live: it cannot be expired, so it
        is also not evidence of anything. `set_value` therefore stamps `at` too,
        so every record this store writes has a lifetime.
        """
        stamps = [float(s) for s in record.get("timestamps", []) if isinstance(s, (int, float))]
        at = record.get("at")
        newest = max([*stamps, float(at)] if isinstance(at, (int, float)) else stamps, default=None)
        if newest is None:
            return False
        return (self._time() - newest) <= ttl

    def _write_locked(self, key: str, record: dict, ttl: float) -> None:
        self._cache[key] = record
        self._cache = {k: v for k, v in self._cache.items() if self._is_live(v, self._ttl_for(k))}
        self._persist_locked()

    def _ttl_for(self, key: str) -> float:
        """The longest window this key could be under, so one namespace's
        entries do not expire on another's TTL."""
        prefix = key.split(":", 1)[0]
        return COUNTER_TTLS.get(prefix, max(COUNTER_TTLS.values()))

    def _persist_locked(self) -> None:
        try:
            self.persist_ok = bool(self.backend.save(self.namespace, self._cache))
            if not self.persist_ok:
                log.warning("state save was not accepted; the counters are in memory only")
        except Exception as error:  # noqa: BLE001 - see _ensure_loaded
            self.persist_ok = False
            log.warning("state save failed (%s); the in-process limits still hold", error)


def state_store_from_env(env: dict | None = None, time_fn=_now) -> DurableStateStore:
    """Build the store the environment asks for.

    ``auto`` never silently picks the memory backend when a durable option is
    configured: a path or a bus URL wins, and only a host with neither gets
    memory (with the warning the store logs on first use).
    """
    env = dict(os.environ if env is None else env)
    backend_name = (env.get("DOCTOR_STATE_BACKEND") or "auto").strip().lower()
    namespace = (env.get("DOCTOR_STATE_NAMESPACE") or DEFAULT_NAMESPACE).strip() or DEFAULT_NAMESPACE
    refresh = float(env.get("DOCTOR_STATE_REFRESH") or DEFAULT_REFRESH_SECS)
    path = (env.get("DOCTOR_STATE_PATH") or "").strip()
    bus_url = (env.get("HIVE_URL") or "").strip()

    if backend_name == "auto":
        if path:
            backend_name = "file"
        elif bus_url:
            backend_name = "bus"
        else:
            backend_name = "memory"

    if backend_name == "file":
        if not path:
            log.warning("DOCTOR_STATE_BACKEND=file but DOCTOR_STATE_PATH is unset; using memory")
            backend = MemoryBackend()
        else:
            backend = FileBackend(path)
    elif backend_name == "bus":
        if not bus_url:
            log.warning("DOCTOR_STATE_BACKEND=bus but HIVE_URL is unset; using memory")
            backend = MemoryBackend()
        else:
            backend = BusBackend(_env_http(bus_url, env), topic=namespace)
    elif backend_name == "memory":
        backend = MemoryBackend()
    else:
        raise ValueError(f"unknown DOCTOR_STATE_BACKEND {backend_name!r}")

    return DurableStateStore(backend, namespace=namespace, refresh_secs=refresh, time_fn=time_fn)


def _env_http(bus_url: str, env: dict):
    """A minimal `http(method, path, body)` bound to the bus URL.

    Deliberately its own urllib call rather than the doctor's `_http`: this
    module must be importable without the doctor's registration state, and the
    doctor's helper attaches a mutation actor URN the store does not have until
    the doctor has registered.
    """
    import urllib.error
    import urllib.request

    token = env.get("HIVE_BUS_TOKEN") or env.get("HIVE_BUS_ADMIN_TOKEN") or ""

    def http(method: str, path: str, body: dict | None = None, timeout: float = 10.0):
        data = json.dumps(body).encode() if body is not None else None
        headers = {"content-type": "application/json"}
        if token:
            headers["authorization"] = f"Bearer {token}"
        request = urllib.request.Request(f"{bus_url.rstrip('/')}{path}", data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read()
                return response.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as error:
            try:
                return error.code, json.loads(error.read())
            except Exception:  # noqa: BLE001 - a non-JSON error body is still a status
                return error.code, None
        except Exception as error:  # noqa: BLE001 - the store degrades, it does not stop
            log.warning("state http %s %s: %s", method, path, error)
            return 0, None

    return http
