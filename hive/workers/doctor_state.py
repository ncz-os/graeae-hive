#!/usr/bin/env python3
"""Doctor safety state: one local file in production, with guarded replacement.

Reads distinguish a complete empty state from unavailable/corrupt state. Writes
compare the file with the bytes last read under a process lock, then fsync and
atomically replace it. A stale writer refuses; its actual pending mutations are
retained for the next successful read. Counters may remain conservatively cached
on outage; approvals require fresh, healthy state.

The environment factory supports auto/file only, defaulting to
$XDG_STATE_HOME/hive-doctor/state.json or $HOME/.local/state/hive-doctor/state.json.
DOCTOR_STATE_PATH overrides that path. Existing bus deployments require an
operator migration; this code never silently migrates or resets their history.
MemoryBackend and BusBackend are retained for offline tests/replay compatibility.
"""

from __future__ import annotations

import fcntl
import json
import logging
import math
import os
import tempfile
import threading
import time
from collections import Counter
from contextlib import suppress

log = logging.getLogger("doctor.state")

DEFAULT_NAMESPACE = "doctor.state"
DEFAULT_REFRESH_SECS = 30.0

#: The window a key with no entry in [`COUNTER_TTLS`] is pruned at. A day, chosen
#: because it is long enough that a limit keeps its meaning across a weekend and
#: short enough that an unlisted key cannot grow the cache without bound. The
#: alternative — infinity for an unrecognised prefix — makes a typo'd key prefix a
#: silent memory leak.
DEFAULT_TTL_SECS = 86_400.0

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
        self._read_bytes: bytes | None = None
        self._read_ok = False

    def _read(self) -> bytes | None:
        try:
            with open(self.path, "rb") as handle:
                return handle.read()
        except FileNotFoundError:
            return None

    def load(self, namespace: str) -> dict:
        self._read_ok = False
        raw = self._read()
        blob = json.loads(raw) if raw is not None else {}
        if not isinstance(blob, dict):
            raise ValueError("state file is not an object")
        records = blob.get(namespace, {})
        if not isinstance(records, dict) or any(not isinstance(v, dict) for v in records.values()):
            raise ValueError("invalid state namespace")
        for record in records.values():
            stamps = record.get("timestamps", [])
            if not isinstance(stamps, list):
                raise ValueError("invalid safety history")
            values = [*stamps, *([record["at"]] if "at" in record else [])]
            if not values or any(type(v) not in (int, float) or not math.isfinite(v) for v in values):
                raise ValueError("invalid safety timestamp")
        self._read_bytes, self._read_ok = raw, True
        return records

    # The namespace is the CONTAINER, not a key prefix: the store's keys are bare
    # (`loop:<job id>`), and this backend persists every one of them under the
    # namespace. An earlier version filtered on a `"<namespace>/"` prefix the store
    # never wrote, so every load came back empty and a restart silently reset every
    # safety limit — which is the exact defect this module exists to remove, with
    # the failure on the read side instead of the write side.

    def save(self, namespace: str, records: dict) -> bool:
        if not self._read_ok:
            return False
        directory = os.path.dirname(self.path) or "."
        os.makedirs(directory, exist_ok=True)
        # Serialize comparison + replacement across cooperating processes. A stale
        # writer refuses instead of overwriting records it has never read.
        with open(self.path + ".lock", "a", encoding="utf-8") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if self._read() != self._read_bytes:
                self._read_ok = False
                return False
            return self._replace(namespace, records)

    def _replace(self, namespace: str, records: dict) -> bool:
        blob = json.loads(self._read_bytes) if self._read_bytes is not None else {}
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
            self._read_bytes = json.dumps(blob).encode()
            dir_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
            return True
        except OSError as error:
            log.warning("state file %s not writable (%s); keeping in memory", self.path, error)
            with suppress(OSError):
                os.unlink(temp_path)
            return False

    def consume_grant(self, namespace: str, key: str, digest: str, now_fn, max_age: float) -> bool:
        """Validate and spend one grant under the same cross-process file lock."""
        directory = os.path.dirname(self.path) or "."
        os.makedirs(directory, exist_ok=True)
        with open(self.path + ".lock", "a", encoding="utf-8") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            records = self.load(namespace)  # invalid/unreadable state refuses
            # Read the clock only now, with the lock held: a wait for another
            # writer must not let a grant be spent after it has expired.
            now = now_fn()
            record = records.get(key, {})
            grant = record.get("value") if not record.get("tombstone") else None
            if not _valid_unspent_grant(grant, digest, now, max_age):
                return False
            updated = dict(records)
            updated[key] = {**record, "value": {**grant, "used": True}, "at": now}
            return self._replace(namespace, updated)


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
        #: Set by `load` when the route returned as many messages as were asked
        #: for, i.e. the journal is longer than one read. A bounded read of an
        #: unbounded journal silently drops the OLDEST records — which are the ones
        #: a safety limit is made of — so the store treats a truncated load as no
        #: load at all rather than as a smaller state.
        self.truncated = False
        #: What this backend has already published for each key, so a write posts
        #: the records that CHANGED rather than the whole cache. Re-posting every
        #: record on every write grows the journal by the size of the state per
        #: write — quadratically over a run — until every read is truncated and the
        #: store can never load again. Seeded by `load`, updated by `save`.
        self._published: dict[str, dict] = {}

    def load(self, namespace: str) -> dict:
        self.truncated = False
        # ONE more than the window is asked for, so "the journal is longer than one
        # read" is DECIDABLE: a journal of exactly `limit` messages is complete, and
        # asking for `limit` and treating a full page as truncation called that
        # complete journal truncated — refusing to load from the exact moment it
        # became safe to.
        window = self.limit + 1
        status, response = self._http("GET", f"/v1/messages?topic={namespace}&limit={window}")
        if status != 200 or not isinstance(response, dict):
            raise OSError(f"bus state read failed (status={status})")
        messages = response.get("messages")
        if not isinstance(messages, list):
            raise ValueError("bus state response has no messages list")
        if len(messages) >= window:
            self.truncated = True
            log.warning(
                "bus state read hit the %d-message limit for %s; the journal is longer than one "
                "read, so this load is INCOMPLETE and the cache is kept",
                self.limit,
                namespace,
            )
            return {}
        newest: dict[str, tuple[float, dict]] = {}
        for message in messages:
            payload = message.get("payload")
            if not isinstance(payload, dict):
                continue
            key = payload.get("key")
            value = payload.get("value")
            if not isinstance(key, str) or not isinstance(value, dict):
                continue
            try:
                ts = float(message.get("ts") or 0.0)
            except (TypeError, ValueError):
                # One message with an ISO timestamp must not discard the whole
                # read: every OTHER record in the journal is still evidence, and
                # dropping it all is the same class of defect as the truncation
                # above. Treated as the oldest possible entry, which loses only
                # this message's claim to be the newest.
                log.warning("bus state message for %s has a non-numeric ts; treating it as oldest", key)
                ts = 0.0
            previous = newest.get(key)
            if previous is None or ts >= previous[0]:
                newest[key] = (ts, value)
        # This load was complete, so what it returned is what is already published.
        self._published = {key: value for key, (_ts, value) in newest.items()}
        return {key: value for key, (_ts, value) in newest.items()}

    def save(self, namespace: str, records: dict) -> bool:
        """Post the records that changed, and only those.

        Incremental because the journal is append-only and unbounded: posting the
        whole cache on every write would make the read limit reachable after a few
        dozen writes, and a truncated read is treated as no read at all.
        """
        written = 0
        pending = 0
        for key, value in records.items():
            if self._published.get(key) == value:
                continue
            pending += 1
            status, _response = self._http(
                "POST",
                "/v1/messages",
                {"from_urn": self.actor_urn, "topic": namespace, "payload": {"key": key, "value": value}},
            )
            if status in (200, 201, 202, 204):
                written += 1
                # Only a POST the route accepted is recorded as published, so a
                # failed one is retried by the next write rather than forgotten.
                self._published[key] = value
        return written == pending


def _valid_unspent_grant(grant, digest: str, now: float, max_age: float) -> bool:
    return (
        isinstance(grant, dict)
        and grant.get("digest") == digest
        and isinstance(grant.get("approver"), str)
        and grant["approver"].startswith("operator:")
        and grant.get("used") is False
        and type(grant.get("granted_at")) in (int, float)
        and type(grant.get("expires_at")) in (int, float)
        and math.isfinite(grant["granted_at"])
        and math.isfinite(grant["expires_at"])
        and max_age > 0
        and grant["granted_at"] <= now < min(grant["expires_at"], grant["granted_at"] + max_age)
    )


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
        #: False until a load has actually SUCCEEDED. A cache that has never been
        #: loaded holds only what this process wrote, so persisting it over a
        #: durable backend that was merely unreachable would delete every record the
        #: previous process left — the load failed, so nobody knows what those
        #: records were, and overwriting them is the one thing that must not happen.
        self._loaded_ok = False
        #: The records this process wrote while the backend was refusing them, by
        #: key. A write during an outage reaches the cache and nothing else:
        #: `_persist_locked` refuses it because nothing has ever loaded, so the
        #: first load that SUCCEEDS is the moment the cache is replaced by the
        #: backend's state — and that replacement is where a mark made during the
        #: outage is forgotten, resetting a safety limit with nothing restarted.
        #: Merged back over the loaded state while it is still true, and dropped
        #: only once a save has been ACCEPTED, because a load is not what makes a
        #: record durable and the next load would otherwise drop the merge again.
        self._written_since_load: dict[str, dict] = {}

    # ── the store surface the doctor uses ──────────────────────────────

    @property
    def loaded_ok(self) -> bool:
        """Whether the last read was complete and successful."""
        self._ensure_loaded()
        return self._loaded_ok

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
            retention = max(self._ttl_for(key), ttl)
            record = self._record_locked(key, ttl=retention)
            stamps = sorted(
                float(stamp)
                for stamp in record.get("timestamps", [])
                if isinstance(stamp, (int, float)) and now - float(stamp) <= retention
            )
            stamps.append(now)
            # Sorted AFTER the append, not before: `at` is caller-supplied and may
            # be in the past, and the contract on the return value is oldest-first.
            stamps.sort()
            self._write_locked(key, {"timestamps": stamps}, ttl=ttl)
            return [stamp for stamp in stamps if now - stamp <= ttl]

    def mark(self, key: str, ttl: float, at: float | None = None) -> float:
        """Record a single timestamp for ``key`` (the cooldown / seen-set shape)."""
        now = self._time() if at is None else float(at)
        self._ensure_loaded()
        with self._lock:
            self._write_locked(key, {"at": now}, ttl=ttl)
        return now

    def last_mark(self, key: str) -> float | None:
        """When this key was last recorded.

        The newest of `at` and every entry in `timestamps`, not `at` alone. `mark`
        writes `at` and `touch` writes `timestamps`, so a `seen()` on a key some
        caller reached with `touch` read `None` and reported the limit as absent —
        which is the same shape as "never happened".
        """
        record = self._get(key, ttl=None)
        return _newest_of(record)

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

    def consume_grant(self, digest: str, max_age: float) -> bool:
        """Spend a scoped grant; file authority is reread while its lock is held."""
        key = f"approval:{digest}"
        if isinstance(self.backend, FileBackend):
            with self._lock:
                try:
                    accepted = self.backend.consume_grant(self.namespace, key, digest, self._time, max_age)
                except (OSError, ValueError, json.JSONDecodeError):
                    self._loaded_ok = False
                    self.persist_ok = False
                    return False
                self._loaded_at = None
                self.persist_ok = accepted
                return accepted
        self.reload()
        now = self._time()
        with self._lock:
            grant = self._cache.get(key, {}).get("value")
            if not self._loaded_ok or not self.persist_ok or not _valid_unspent_grant(grant, digest, now, max_age):
                return False
            self._write_locked(key, {"value": {**grant, "used": True}, "at": now}, ttl=self._ttl_for(key))
            return self.persist_ok

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
        """Retract a key, durably.

        Written as a TOMBSTONE rather than as an omission, because the bus backend
        is an append-only journal: dropping the key from the cache leaves the
        record already in the journal, and the next replay resurrects the approval
        somebody just revoked. The tombstone is itself a record, so a later load
        sees the retraction as the newest thing said about the key.

        The tombstone just written is never pruned by its own call: a retraction is
        held for as long as the record it retracts, not for as long as the key's
        prefix window, so pruning it here is what would let a replay resurrect the
        approval.
        """
        self._ensure_loaded()
        with self._lock:
            self._cache[key] = {"tombstone": True, "at": self._time()}
            self._written_since_load[key] = self._cache[key]
            self._cache = {
                k: v
                for k, v in self._cache.items()
                if (k == key and v.get("tombstone")) or self._is_live(v, _prune_ttl_of(k, 0.0))
            }
            self._persist_locked()

    def clear(self) -> None:
        """Retract every key in ONE write.

        The same effect as forgetting each key in turn, and one round-trip instead
        of one per key: on a file backend the whole namespace is replaced once, and
        on the bus the retractions are a single `save`. Used by
        `doctor.reset_safety_state`, which is a human action on a store that can
        hold hundreds of keys.
        """
        self._ensure_loaded()
        with self._lock:
            if not self._cache:
                return
            now = self._time()
            for key in list(self._cache):
                self._cache[key] = {"tombstone": True, "at": now}
                self._written_since_load[key] = self._cache[key]
            self._persist_locked()

    def reload(self) -> None:
        """Read fresh authority without discarding this process's pending writes."""
        with self._lock:
            self._loaded_at = None
        self._ensure_loaded()

    def refresh(self) -> None:
        """Drop the cache so the next read comes from the backend."""
        with self._lock:
            self._loaded_at = None
            self._cache = {}
            # The unpersisted records go WITH the cache. This is the operator's
            # "believe the backend from scratch", so merging them back over the
            # loaded state would put back precisely what it was asked to drop —
            # and, unlike an unattended reload, nobody is left to be confused by it.
            self._written_since_load = {}

    # ── internals ──────────────────────────────────────────────────────

    def _ensure_loaded(self) -> None:
        now = self._time()
        with self._lock:
            if self._loaded_at is not None and (now - self._loaded_at) < self.refresh_secs:
                return
            try:
                loaded = self.backend.load(self.namespace)
                self._loaded_ok = True
            except Exception as error:  # noqa: BLE001 - a backend must never stop the doctor
                log.warning("state load failed (%s); serving from cache", error)
                loaded = {}
                self._loaded_ok = False
            if getattr(self.backend, "truncated", False):
                # A bounded read of a longer journal would drop the oldest records,
                # and the oldest records are the ones a window is made of. Treat it
                # as no answer rather than as a smaller state: the cache is kept and
                # the operator is told, because the alternative is a safety limit
                # resetting itself with nothing restarted.
                loaded = {}
                # A truncated read is a FAILED read. It must not leave `_loaded_ok`
                # set, because that flag is what lets `_persist_locked` write: a
                # cache built from a partial journal would overwrite the journal
                # with the partial view and destroy the records the read dropped.
                # Nor may it stamp `_loaded_at`, or the next refresh window would
                # skip the retry and the store would serve the partial cache for
                # ever, believing it had loaded.
                self._loaded_ok = False
            if self._loaded_ok:
                if not loaded:
                    # Missing counters must never loosen a local safety limit.
                    # Grants are different: an empty authoritative read revokes
                    # them. Only pending writes may be merged back below.
                    loaded = {k: v for k, v in self._cache.items() if not k.startswith("approval:")}
                self._cache = self._merged_over(loaded)
            if not self._loaded_ok:
                # Leave `_loaded_at` unset so the NEXT read retries the backend. A
                # failed load marked as fresh would be skipped for the whole refresh
                # window, and the cache it serves is not known to be complete.
                self.persist_ok = False
                return
            # NOT cleared here. A load does not make these records durable — only an
            # accepted save does — and `refresh_secs` reloads long before the next
            # write arrives, so clearing on the load would drop the merged records
            # at the very next refresh and leave the limit reset anyway.
            self._loaded_at = now
            if self.backend.name == "memory" and not self._warned_memory:
                self._warned_memory = True
                log.warning(
                    "doctor safety state is IN MEMORY: cooldowns, loop history, DLQ-seen and the "
                    "SSH rate counters reset on every restart. Use the file runtime backend "
                    "with DOCTOR_STATE_PATH to make them durable."
                )

    def _merged_over(self, loaded: dict) -> dict:
        """``loaded`` with this process's refused writes merged back over it.

        Replacing the cache with what loaded is how a retraction another process
        made gets applied here, so the replacement is not the whole answer: the
        marks the refused writes are made of exist only in the cache, and dropping
        them resets a limit the process is still enforcing. Newest ``at`` wins per
        key, so another process's more recent record still takes the key back, and
        a tombstone merges like any other record — a retraction still retracts.
        """
        if not self._written_since_load:
            return loaded
        merged = dict(loaded)
        for key, record in self._written_since_load.items():
            other = merged.get(key) or {}
            if "timestamps" in record and "timestamps" in other and not other.get("tombstone"):
                # Both writers extended the same history. Preserve the shared
                # prefix once and every distinct reservation from either writer.
                stamps = Counter(record["timestamps"]) | Counter(other["timestamps"])
                merged[key] = {"timestamps": sorted(stamps.elements()), "tombstone": False}
                continue
            theirs = _newest_of(merged.get(key) or {})
            mine = _newest_of(record)
            if theirs is None or (mine is not None and mine >= theirs):
                merged[key] = record
        return merged

    def _get(self, key: str, ttl: float | None) -> dict:
        self._ensure_loaded()
        with self._lock:
            record = self._cache.get(key) or {}
        if record.get("tombstone"):
            return {}
        if ttl is None:
            return record
        return record if self._is_live(record, ttl) else {}

    def _record_locked(self, key: str, ttl: float) -> dict:
        record = self._cache.get(key) or {}
        if record.get("tombstone"):
            return {}
        return record if self._is_live(record, ttl) else {}

    def _is_live(self, record: dict, ttl: float) -> bool:
        """Whether a record still carries evidence, judged on its newest stamp.

        A record with no timestamp at all is NOT live: it cannot be expired, so it
        is also not evidence of anything. `set_value` therefore stamps `at` too,
        so every record this store writes has a lifetime.

        A tombstone carries `at` and nothing else, so the stamp below already sees
        it; it is named here so the retraction is not mistaken for missing data.
        """
        newest = _newest_of(record)
        if newest is None:
            return False
        return (self._time() - newest) <= ttl

    def _write_locked(self, key: str, record: dict, ttl: float) -> None:
        # A real write supersedes a retraction: the key is being used again.
        self._cache[key] = {**record, "tombstone": False}
        self._written_since_load[key] = self._cache[key]
        # Prune with the LONGER of this write's window and the key's canonical one.
        # Pruning with `ttl` alone means a caller passing a short window silently
        # truncates the history a caller with the canonical window depends on — the
        # second loop detector to touch `loop:<job>` with a shorter window would
        # erase the first one's evidence. Pruning never shortens.
        prune_ttl = max(self._ttl_for(key), ttl)
        self._cache = {k: v for k, v in self._cache.items() if self._is_live(v, _prune_ttl_of(k, prune_ttl))}
        self._persist_locked()

    def _ttl_for(self, key: str) -> float:
        """The longest window this key could be under, so one namespace's
        entries do not expire on another's TTL."""
        prefix = key.split(":", 1)[0]
        if prefix in COUNTER_TTLS:
            return COUNTER_TTLS[prefix]
        # An unconfigured prefix is pruned at the LONGEST window any configured
        # prefix uses, so an unknown key is never the shortest-lived record in the
        # cache. `max()` over an empty table raises and this runs inside the write
        # path, so the `default` keeps an emptied table degrading to the named
        # default — and never to infinity.
        return max(COUNTER_TTLS.values(), default=DEFAULT_TTL_SECS)

    def _persist_locked(self) -> None:
        self._written_since_load = {
            key: record for key, record in self._written_since_load.items() if key in self._cache
        }
        if not self._loaded_ok and self.backend.name != "memory":
            # Nothing has ever been read back, so this cache is not known to be the
            # whole state. Writing it would replace a durable backend's contents with
            # one process's partial view. The counters stay in memory — which keeps
            # them CORRECT for this process — and the operator is told, because the
            # remedy is the backend's.
            log.warning(
                "state has never loaded from %s; NOT writing, so the durable records survive. "
                "The in-process limits hold; the durability does not until the backend answers.",
                self.backend.name,
            )
            self.persist_ok = False
        else:
            try:
                self.persist_ok = bool(self.backend.save(self.namespace, self._cache))
                if not self.persist_ok:
                    log.warning("state save was not accepted; the counters are in memory only")
            except Exception as error:  # noqa: BLE001 - see _ensure_loaded
                self.persist_ok = False
                log.warning("state save failed (%s); the in-process limits still hold", error)
        # Pending entries are actual local mutations, not a snapshot of reads.
        # Copying the whole cache here can resurrect an unrelated revoked grant.
        if self.persist_ok:
            self._written_since_load.clear()
        else:
            self._loaded_at = None


def _newest_of(record: dict) -> float | None:
    """The newest stamp a record carries, or None if it carries none.

    `at` and `timestamps` are the two shapes the write paths use — `mark` and
    `set_value` write `at`, `touch` writes `timestamps`, `forget` writes a
    tombstone's `at` — so "which of these two records is newer" is the newest of
    both. None means the record is not evidence of anything, which is the same
    thing `_is_live` says about one it will not honour.
    """
    stamps = [float(s) for s in record.get("timestamps", []) if isinstance(s, (int, float))]
    at = record.get("at")
    if isinstance(at, (int, float)):
        stamps.append(float(at))
    return max(stamps) if stamps else None


def _prune_ttl_of(key: str, floor: float) -> float:
    """The window to prune ``key`` at, never below ``floor``."""
    return max(floor, COUNTER_TTLS.get(key.split(":", 1)[0], DEFAULT_TTL_SECS))


def state_store_from_env(env: dict | None = None, time_fn=_now) -> DurableStateStore:
    """One runtime backend: a local durable file, opened lazily on first use.

    The message journal is bounded and has no snapshot/compaction contract.
    MemoryBackend and BusBackend remain available to offline replay/tests, but
    neither is a runtime safety store. Explicit old settings fail at startup.
    """
    env = dict(os.environ if env is None else env)
    backend_name = (env.get("DOCTOR_STATE_BACKEND") or "auto").strip().lower()
    if backend_name not in {"auto", "file"}:
        raise ValueError("doctor runtime requires DOCTOR_STATE_BACKEND=file (or auto)")
    path = (env.get("DOCTOR_STATE_PATH") or "").strip()
    if not path:
        root = env.get("XDG_STATE_HOME")
        if not root:
            home = env.get("HOME")
            if not home:
                raise ValueError("set DOCTOR_STATE_PATH or HOME for durable doctor state")
            root = os.path.join(home, ".local", "state")
        path = os.path.join(root, "hive-doctor", "state.json")
    namespace = (env.get("DOCTOR_STATE_NAMESPACE") or DEFAULT_NAMESPACE).strip() or DEFAULT_NAMESPACE
    refresh = float(env.get("DOCTOR_STATE_REFRESH") or DEFAULT_REFRESH_SECS)
    return DurableStateStore(FileBackend(path), namespace=namespace, refresh_secs=refresh, time_fn=time_fn)
