# Time and unit handling

Base: `origin/deploy/hive-canonical-20261029` (integrated line). Audited with
`grep -rn "time\.time()\|datetime\.now\|utcnow\|astimezone\|fromtimestamp\|strftime" hive/`
plus `deploy/hive_worker.py`. Pinned by `test_time_units_20261031.py`.

**The rule the codebase already follows: every timestamp that is stored, sent or
compared is an epoch number of SECONDS in UTC.** No naive `datetime` is stored
anywhere, there is no `datetime.utcnow()` (deprecated) call, and milliseconds
appear only in values whose name ends in `_ms`. The table below is that
contract, field by field, followed by the two places where the audit found it
broken.

## Stored fields

| Field | Unit | Timezone | Where written | Where read |
| --- | --- | --- | --- | --- |
| `jobs.claimed_at` | epoch s (REAL) | UTC | `hive/bus/routers/jobs.py` claim / `next` (`now` at the claim), never touched by a later PATCH | `select_orphan_claim_candidates`, `reaper.select_orphan_claims` (hard cap), `doctor_v2` evidence |
| `jobs.claim_lease_expires_at` | epoch s | UTC | claim + `PATCH` renewal: `now + CLAIM_LEASE_SECONDS` (`jobs.py` 1129/1171/1552/2073) | `select_orphan_claim_candidates` (`<= now`), `reaper.select_orphan_claims` |
| `jobs.last_update_at` | epoch s | UTC | every accepted `PATCH` | reaper: `COALESCE(last_update_at, claimed_at)` staleness |
| `jobs.created_at` / `ended_at` | epoch s | UTC | submit / terminal PATCH | listings, retention, `doctor._row_touched_at` |
| `agents.last_heartbeat` | epoch s | UTC | heartbeat route | `select_stale_agent_urns` (90 s), `select_dead_agent_urns` (300 s), `purge_offline_agents` (6 h) |
| `events.ts` | epoch s | UTC | `hive/bus/deps.py:emit_event` | dashboards, audits |
| `capacity_state.limited_until` | epoch s | UTC | `hive/bus/capacity.py` on a learned rate/usage window; persisted by `domain/capacity_ledger.flush_capacity_ledger` | `CapacityLedger.headroom` (`until > now` denies) |
| `capacity_state.window_start` | epoch s, **always 0.0** | — | `flush_capacity_ledger` writes the literal `0.0` | `load_capacity_ledger` unpacks it as `_window` and discards it — the column is dead, see Findings |
| `capacity_state.updated_at` | epoch s | UTC | `flush_capacity_ledger` | nothing reads it |
| doctor `STATE` record `at` / `timestamps[]` | epoch s | UTC | `DurableStateStore.mark/touch` (`time_fn`, default `time.time`) | `seen`, `last_mark`, `timestamps` — every safety limit in `COUNTER_TTLS` |
| `result.review_state.next_attempt_at` | epoch s | UTC | `review_queue` on a failed attempt | `review_queue` backoff (`next attempt at …Z` rendered with `time.gmtime`) |
| `jobs.result.*_at` (review evidence, verdict completion) | epoch s | UTC | review/verdict writers | `doctor`, `review_queue` |

## Durations (seconds, all `float`)

| Constant | Value | Meaning |
| --- | --- | --- |
| `CLAIM_LEASE_SECONDS` (`bus/domain/jobs.py`) | 1800, env `CLAIM_LEASE_SECONDS` | lease granted at claim, renewed per PATCH |
| `CLAIM_STALE_AFTER` (`bus/app.py`) | 1800 | no progress ⇒ orphan |
| `CLAIM_STALE_AFTER_BY_KIND` | orchestration/investigation/benchmark 3600, migration 7200 | longer staleness per kind |
| `CLAIM_HARD_MAX_AGE` / `..._BY_KIND` | 14400; migration 21600 | un-refreshable cap measured from `claimed_at` |
| `HEARTBEAT_STALE_AFTER` | 90 | online → stale |
| `HEARTBEAT_OFFLINE_AFTER` | 300 | stale → offline; `HIVE_AGENT_STALE_SECONDS` may only shorten it |
| `AGENT_PURGE_AFTER` | 21600 | offline agent row hard-deleted |
| `MIN_HOST_COOLDOWN` (`doctor.py`) | 1800 | no second SSH action on a host |
| `AUTO_TRIAGE_COOLDOWN` | 3600 | per-kind re-triage |
| `DOCTOR_NEEDS_REVIEW_ESCALATE_SECONDS` | 1800 | idle before a stuck needs-review row is escalated |
| `MINIMAX_QUOTA_TTL` / `MINIMAX_QUOTA_ERROR_TTL` (`deploy/hive_worker.py`) | 600 / 120 | cache of the 5-hour quota read; a failed read caches for 120 s |
| `DATETIME`/date bucketing | — | `retention.py:264` `%Y-%m-%dT%H` and `remediation.py:509` `%Y-%m-%d` both `fromtimestamp(ts, tz=UTC)`; `eih_probe` filename `%Y-%m-%d` from `now.astimezone(UTC)` |
| `deepseek_spend_alert` | aware `datetime` | `utcnow()` = `datetime.now(dt.UTC)`; naive ISO input is interpreted as UTC (`parse_ts`); every render `astimezone(dt.UTC)` |

## Milliseconds (`_ms`, never stored as a timestamp)

| Value | Where | Clock |
| --- | --- | --- |
| `first_token_ms`, `headers_ms`, `latency_ms` | `hive/tools/eih_probe.py` | `time.monotonic()` deltas × 1000 |
| `latency_ms` on the job record | `hive/bus/routers/jobs.py:1758` | `int(duration * 1000)` from a monotonic duration |
| sweep duration | `hive/workers/doctor_v2.py:263` | monotonic × 1000, log only |
| `uuidv7()` high 48 bits | `hive/bus/deps.py:101` | `int(time.time() * 1000)` — by design, the UUID layout is milliseconds |
| DB busy timeout | `hive/persistence/sqlite.py:567` | `busy_timeout_ms / 1000` → sqlite wants seconds |

## Boundaries

Every window above is half-open at the "expired" end; the tests assert both
sides (`test_time_units_20261031.py`):

| Window | Inside | At the boundary | Just past |
| --- | --- | --- | --- |
| claim lease | `lease > now`: live | `lease == now`: **reaped** (`<=`) | reaped |
| hard cap | `now - claimed_at < 4 h` | `== 4 h`: **reaped** (`>=`), even with fresh PATCHes | reaped |
| kind staleness | `now - progress < ttl` | `== ttl`: **reaped** | reaped |
| heartbeat stale | `now - hb <= 90`: online | `== 90`: online (`<` is strict) | 90.001: stale |
| heartbeat offline | `< 300` | `== 300`: not offline yet | 300.001: offline |
| agent purge | `hb < cutoff`: kept | `== cutoff`: kept (strict `<`) | deleted |
| host cooldown | `since < 1800`: skipped | `== 1800`: allowed | allowed |
| triage / `STATE.seen` | `now - at <= ttl`: active | `== ttl`: **still active** (inclusive) | inactive; a `ttl` of 0 admits nothing |
| needs-review escalation | `now - touched <= 1800`: quiet | `== 1800`: quiet (`<=`) | escalated, once per (classification, evidence, blocker) |
| `limited_until` | `until > now`: denied | `== now`: **admitted** | admitted |
| MiniMax quota cache | `at - cached < 600` | `== 600`: refetch (strict `<`) | refetch |
| DeepSeek peak hours | `1 <= h < 4`, `6 <= h < 10`, Mon–Fri UTC | `04:00`/`10:00` are off-peak | off-peak |

## Findings

1. **`hive/workers/doctor_v2.py::sweep_needs_review` — falsy zero.**
   `cutoff = (now or time.time()) - older_than_hours * 3600`. `now=0.0` is a
   legitimate instant (epoch 0, and the obvious start value in a fake clock),
   but `or` replaces it with the wall clock, so a caller that asks "what is old
   relative to epoch 0" silently gets "old relative to right now". Every other
   time helper in `hive/` uses `time.time() if now is None else now` (see
   `escalate_stuck_needs_review`, `host_in_cooldown`, `CapacityLedger.headroom`).
   Pinned by `test_sweep_needs_review_honours_epoch_zero` (`xfail(strict=True)`).
   Not fixed here because a "needs review at time 0" sweep is untested product
   behaviour, not a test-only path.

2. **`deploy/hive_worker.py::escalation_agent` — epoch passed to a struct_time
   API.** `_epoch(now)` returns a float, and `escalation_agent(job, now_utc=…)`
   passes that float to `deepseek_peak()`, which immediately reads
   `now_utc.tm_wday` (it takes `time.gmtime()`). Result:
   `AttributeError: 'float' object has no attribute 'tm_wday'` for every
   non-`None` `now_utc`. The production callers pass nothing (so the default
   `time.gmtime()` is used and the bug is latent), and any replay or fake-clock
   caller dies. `hive/bus/capacity.py::time_multiplier` implements the same
   pricing rule correctly from an epoch, and
   `test_bus_time_multiplier_agrees_with_the_worker_rule_in_utc` pins that the
   two rules agree hour by hour — two implementations of one rule is the real
   risk here. Pinned by `test_escalation_agent_accepts_an_epoch`
   (`xfail(strict=True)`).

3. **`capacity_state.window_start` is written as a constant `0.0` and never
   read** (`domain/capacity_ledger.py` writes `window_start=0.0`,
   `load_capacity_ledger` ignores it as `_window`). Not a unit mismatch — a
   dead column. Left alone: dropping it needs a migration.

## Not defects (checked, correct)

- No naive/aware mix: the only `datetime` values are constructed with `tz=UTC`,
  converted with `astimezone`, or `fromtimestamp(..., tz=UTC)`.
- No `datetime.utcnow()`; the one `utcnow()` is a local helper returning an
  aware UTC value.
- No epoch-millisecond value stored in a seconds field: the ms uses are the
  five listed above, all named `_ms`, all derived from `monotonic()` or the
  UUID layout.
- Local timezone never leaks: `time.gmtime` / `time.strftime` with `gmtime` are
  used for every rendered UTC timestamp (`review_queue`, `doctor` file names),
  and the tests build their instants with `calendar.timegm`, so they pass in
  any `TZ`.
