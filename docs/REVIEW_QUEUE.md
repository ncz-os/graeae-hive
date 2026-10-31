# Review Queue and Cross-Family Reviewer Roster

Operator rule 2026-10-31:

> always have an alternate review protocol or a review queue; the roster is
> every local and provider model; always pair reviewer and author from
> different model families.

The queue exists because of one night's failure mode: all 8 slots of the
CERBERUS `gpt-oss-20b` endpoint were busy, about 20 loops ended
`reviewer_unavailable` and were recorded as **failed** even though the work
had been authored, committed and pushed. The queue turns that into a state:
the row waits for a reviewer instead of dying.

Code: `hive/workers/reviewer_roster.py` (roster + pairing),
`hive/workers/review_queue.py` (queue states, retry, load probe),
`hive/workers/doctor.py` (needs-review sweep and dashboard counts).
Data: `hive/workers/model_families.json`, `hive/workers/reviewer_roster.json`.

## 1. Model family table

`hive/workers/model_families.json` is the **one source of truth**. The roster
never re-states a family by hand: every route's `family` must equal
`family_of(route.model)` (a unit test enforces it).

| pattern | family |
| --- | --- |
| `qwen38`, `Qwen*` | alibaba |
| `gemma*` | google |
| `gpt-oss*`, `gpt-6*`, `codex*` | openai |
| `nemotron*` | nvidia |
| `MiniMax-M3`, `MiniMax-M3.1*`, `minimax*` | minimax |
| `deepseek*` | deepseek |
| `muse-spark*` | meta |
| `mimo*` | xiaomi |
| `stepfun*` | stepfun |
| `claude*` | anthropic |
| anything else | `unknown:<model-id>` |

An unknown model is **its own family**. It never matches anything, including
itself: a reviewer whose family is unknown, and an author whose family is
unknown, are both refused (`family_of` / `families_pair`).

## 2. Reviewer roster

`hive/workers/reviewer_roster.json`, one row per reviewer-capable route:

| id | provider | model | family | location | conc. | cost | notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `tydeus-qwen38` | tydeus | `qwen38` | alibaba | local `192.0.2.73:8006` | 8 | A | |
| `cerberus-gemma4-31b` | cerberus | `gemma4-31b` | google | local `192.0.2.96:8080` | 4 | A | standing local reviewer |
| `cerberus-gpt-oss-20b-reviewer` | cerberus | `gpt-oss-20b` | openai | local `192.0.2.96:8080` | 8 | A | served as `cerberus-reviewer` |
| `cerberus-gpt-oss-20b-narrator` | cerberus | `gpt-oss-20b` | openai | local `192.0.2.96:8080` | 8 | A | served as `cerberus-narrator`, same endpoint |
| `provider-minimax-m3` | minimax | `MiniMax-M3` | minimax | provider | 2 | B | |
| `provider-minimax-m3-1-flash` | minimax | `MiniMax-M3.1-Flash-Preview` | minimax | provider | 2 | B | |
| `provider-deepseek-flash` | deepseek | `deepseek-flash` | deepseek | provider | 2 | B | |
| `provider-muse-spark-1-3` | meta | `muse-spark-1.3` | meta | provider | 2 | B | contributor tier: approved repos only |
| `provider-stepfun-3` | stepfun | `stepfun-3` | stepfun | provider | 2 | B | contributor tier: approved repos only |
| `eih-nemotron` | nvidia-eih | `nvidia/nemotron-3-ultra-550b-a55b` | nvidia | eih | 2 | A | **slow** (measured 39 s for a one-word reply): last for bulk work |
| `provider-mimo-v2-6-flash` | xiaomi | `mimo-v2.6-flash` | xiaomi | provider | 2 | B | **never reviews** |
| `provider-mimo-v2-6-pro` | xiaomi | `mimo-v2.6-pro` | xiaomi | provider | 2 | B | **never reviews** |

Flags:

* `never_reviewer` — MiMo is listed for completeness but is never picked.
* `approved_repo` — StepFun and Meta contributor tiers review only the
  operator-approved repos: `graeae-hive`, `fleet-ops`, `tokenomics`,
  `ncz-screensavers`, `zoder`. A row without a recognised repo is refused.
* `slow` — lowest priority for bulk (the queue sorts `slow` last).

The CERBERUS `reviewer` and `narrator` route ids front **one** 8-slot
llama-server. `route_capacity()` therefore shares one budget across routes with
the same `base_url` (the lowest concurrency among them wins), so the queue can
never send more work to an endpoint than the endpoint has slots.

## 3. Pairing rules (`pick_reviewers`)

`pick_reviewers(author_model, roster, health, load, *, author_family,
co_author_models, repo, bulk, include_expensive)` returns an **ordered** list of
reviewers, or an **empty list**.

1. Cross-family only: the reviewer's family must differ from the author's
   family and from every known co-author family. Same-family pairs are never
   returned.
2. Unhealthy routes (`health[route_id]` in `down`/`unhealthy`/`unavailable`/
   `failed`) and saturated routes (`load >= route_capacity`) are skipped.
3. Flags: `never_reviewer` skipped; `approved_repo` only for approved repos;
   cost class C only with `include_expensive`.
4. No cross-family reviewer available -> **empty list** -> the row is PARKED.
   A job is never reviewed by its own family, and never self-reviewed.
5. Deterministic tie-break: `slow` last (bulk), then cost class A -> B -> C,
   then location local -> provider -> eih, then `priority_rank`, then route id.

## 4. Queue states

The queue entry lives in the row's own `result.awaiting_review`, so the row
keeps the `needs-review` status every existing guard protects.

| state | meaning |
| --- | --- |
| `queued` | waiting for the next cross-family reviewer |
| `submitted` | a review job id is recorded; awaiting a verdict |
| `parked` | no cross-family reviewer exists (needs a human) |
| `exhausted` | retries exhausted (needs a human) |
| `approved` | a structured JSON verdict approved the head |

Transitions: `queued -> submitted` on a successful submit; `submitted` /
`queued -> queued` (with backoff) on failure; `-> parked` when the pairing is
empty; `-> exhausted` after `max_attempts`; `submitted -> approved` only from a
structured verdict.

**A review only counts as an approval when it is a structured JSON verdict whose
`reviewers[].model` names a cross-family model** and whose head matches the
delivered head. Prose-only, empty, truncated, 503 and non-JSON reviews are
never approvals: they are recorded as a failed attempt and retried.

## 5. Load awareness

`LoadProbe` reads the local endpoint's own counters, cheaply, cached for 30 s
(strictly: a cache entry is reused while `now - cached_at <= 30` and re-read
afterwards, so a slot freed at the 30 s boundary is visible on the next read):

* llama-server: `GET <base>/slots`, counting slots with `is_processing: true`;
* vLLM: `GET <base>/metrics`, `vllm:num_requests_running`.

A probe failure (connection refused, non-JSON, HTML error page) yields **None**
(*unknown*), never `0`: an unknown load must not be mistaken for an idle
endpoint.

`RouteInflight` caps concurrent review calls per endpoint. A submitted review
keeps its slot (`hold`) until the review job is terminal
(`release_hold`/`reset`), so a second sweep cannot oversubscribe the endpoint.

## 6. Configuration knobs

| knob | default | meaning |
| --- | --- | --- |
| `LOAD_CACHE_SECONDS` | `30` | load probe cache lifetime |
| `LOAD_TIMEOUT_SECONDS` | `2` | per-request probe timeout |
| `DEFAULT_BASE_SECONDS` | `30` | first retry delay |
| `DEFAULT_MAX_SECONDS` | `900` | backoff ceiling |
| `DEFAULT_JITTER` | `0.2` | +/- jitter on the delay |
| `DEFAULT_MAX_ATTEMPTS` | `6` | attempts before `exhausted` |

The roster JSON is data, not code: adding a provider route is a new row plus
a family rule (or an `unknown` family is derived automatically).

## 7. Doctor integration

* `doctor.needs_review_queue_counts(rows)` adds `queued`, `submitted`,
  `parked`, `exhausted` and `unreviewed` counts for the dashboard.
* `doctor.review_followup_blocker(row)` names the queue when a row is awaiting
  review, so the sweep never calls a queued row stale and never escalates it as
  stuck; `escalate_stuck_needs_review` skips queued rows.

## 8. Rollback

No deployment is part of this change; the worker keeps its existing
`reviewer_unavailable` behaviour until the queue consumer is wired in. To roll
back:

1. Revert the commit(s) on `hv/gh-review-queue-20261031`.
2. The queue only ever writes a `result` field; rows already carrying
   `awaiting_review` degrade to plain `needs-review` rows (the blocker text
   disappears when the module is absent), so no data migration is needed.
3. Delete `docs/REVIEW_QUEUE.md` and the two JSON data files if the code goes
   away; nothing else reads them.
