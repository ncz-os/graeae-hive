# PLAN — FB43: time and unit audit with fake-clock tests

Base: `origin/deploy/integrate-20261017` tip `7b4f29e` (the integrated line; the
`hive-canonical` base named for the sibling FB42 is where this work lands too).
Branch: `freebuff/fb43-20261030`.

## Audit

Grep every clock read and every stored timestamp: `time.time()`,
`datetime.now`, `utcfromtimestamp`, `utcnow`, `astimezone`, `fromtimestamp`,
`strftime`, `*_ms`. Record field, unit, timezone, writer and reader in
`docs/TIME_HANDLING.md`, plus the duration constants and the milliseconds-shaped
values (which must never be mistaken for timestamps).

## Tests (`test_time_units_20261031.py`)

Fake clock where the code accepts one; both sides of every boundary.

| Area | How frozen | Boundaries |
| --- | --- | --- |
| claim lease | real bus, assert the written units and `lease - claimed_at == CLAIM_LEASE_SECONDS` | epoch-seconds (not ms), lease exactly `+constant` |
| orphan reaper | pure `select_orphan_claims` with explicit `now` | `lease <= now`, `now - claimed_at >= hard cap`, `now - progress >= stale_after`, kind overrides, NULL claim |
| agent reaper | `reap_agent_heartbeats(db, now_ts)` on a temp sqlite bus | 90 s stale, 300 s offline, 6 h purge, each `±1 ms` |
| doctor cooldowns | `DurableStateStore(MemoryBackend(), time_fn=Clock())` | host cooldown 1800 s, triage 3600 s (inclusive), zero window |
| needs-review escalation | injected store clock + stubbed broadcast | escalate only past 1800 s, once per state hash |
| provider windows | worker module loaded as FB42 does | DeepSeek peak hours/weekend in UTC, `time_multiplier` agreement, MiniMax 600 s / 120 s cache TTL, `limited_until` |

## Findings (pinned as `xfail(strict=True)`, not fixed)

1. `doctor_v2.sweep_needs_review`: `(now or time.time())` swallows `now=0.0`.
2. `deploy/hive_worker.escalation_agent`: epoch float reaches `deepseek_peak`,
   which wants a `struct_time` → `AttributeError` for any explicit `now_utc`.
3. (note, not fixed) `capacity_state.window_start` is written as `0.0` and never
   read — a dead column, needs a migration to drop.

## Risks

- The bus tests each get their own temp sqlite dir, so no cross-test leakage
  (FB42's lesson).
- Loading `deploy/hive_worker.py` stubs its absent sidecars; the module is
  exec'd, never installed.
- `xfail(strict=True)` means a fix flips these to failures, not silent passes.

## Verify

`pytest test_time_units_20261031.py`, then the full suite once for interference,
plus `ruff check` / `ruff format --check` on the two new files.
