# PLAN — FB57: stats endpoints correctness (graeae-hive)

Base: `origin/deploy/integrate-20261017` tip `0f3a5a7`. Branch `freebuff/fb57-20261030`.

## What each endpoint computes (verified against this base)

- `GET /v1/stats/costs` (`stats.py` + `sqlite.cost_stats_rows:1895`): buckets
  over jobs with `ended_at >= cutoff` by day/provider/model/runtime/tier/
  claimed_by/kind; provider bucket is a CASE on
  `JSON_VALUE(result,'$.agent_alias')` with claimed_provider fallback;
  everything else COALESCE-to-'unknown'; sums COALESCE nulls to 0, cost
  ROUND 4, avg duration ROUND 2. Router totals = sums over buckets (cost
  re-rounded to 4).
- `GET /v1/stats/workers` (`sqlite.worker_stats_rows:1820`): reads the
  `worker_kind_stats` rollup (written on terminal PATCH via
  `upsert_worker_kind_stats`; cost via `estimate_cost(provider, model, in,
  out)` on the EFFECTIVE gateway values, NOT the row's estimated_cost_usd;
  urn key = `stable_worker_id` session-stripped). success_pct formula,
  avg over success+fail, system-urn exclusion, ORDER BY success DESC.
- `GET /v1/stats/cache` (`sqlite.cache_stats:1861`): COUNT/SUM over
  `hive_cache`, LIKE-count of `cache_hit` job results, top entries by
  hits (cache_key truncated to 16 chars).

## Tests (`test_stats_correctness_20261031.py`, in-process harness, temp DB)

1. **costs**: insert jobs directly (fixed ended_at incl. exact-cutoff boundary,
   pre-cutoff, NULL ended_at; null cost; zero tokens; mixed
   models/providers/runtimes/tiers/claimants; agent_alias variants incl. the
   four codex aliases, deepseek-direct, minimax, unknown fallback). Independent
   Python recomputation per group_by (CASE replicated, UTC DATE for day),
   buckets compared order-insensitively + router totals. Binary-clean decimals
   only (no .xxxx5) so Python/SQLite rounding agrees.
2. **workers**: real lifecycle via HTTP (submit→claim→PATCH done with tokens +
   fabricated valid proof / failed with retryable False / submitter-cancelled)
   on two workers; recompute counts/token sums from the JOBS table
   (deterministic inputs) + cost via `estimate_cost` on the same effective
   inputs; assert success_pct formula, ordering, kind filter, top_n, and the
   system-urn exclusion (one directly-upserted system row). last_run/avg_dur
   asserted as sane ranges (they use wall-clock at PATCH time).
3. **cache**: `cache_store` + `cache_record_hit` + job rows with/without
   cache_hit results; recompute totals/entries independently; plus empty-DB
   zeros and top_n truncation.

## Risks

- `estimate_cost` pricing-table dependence: expected cost computed by calling
  the same function on the same inputs — validates the plumbing (effective
  gateway values, summation), not the price table itself. Stated in the test.
- Duration/last_run nondeterminism: range assertions only.
- Full suite ~150 s; file first, then full suite once.
