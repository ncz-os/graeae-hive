# The review queue

*Operator rule 2026-10-31: always have an alternate review protocol or a review
queue, and always pair reviewer and author from different model families.*

## Why this exists

A loop whose reviewer endpoint is busy used to end `reviewer_unavailable` and
**fail**. On the night of 2026-10-30 all 8 slots of the CERBERUS
`gpt-oss-20b` endpoint were busy and about 20 loops failed with work that was
already committed and pushed. The reviewer being unavailable says nothing about
the author's work, so it must never fail the job.

## There is no separate queue

The review queue **is** the triage queue (operator, 2026-10-31: *"queue should
be part of triage queue"*). There is no separate queue, table or status
namespace. The queue is:

> the Hive job rows in `needs-review` (and the `failed` / `dead-letter` rows the
> doctor's needs-review sweep already reads) that carry a
> `result.review_state` block.

Everything that shows needs-review rows — the weekend triage check
(`scripts/hive_status.py`), `GET /v1/jobs`, `GET /v1/jobs/summary`, the
dashboard and the doctor's logs — shows these rows too. No new listing exists.

### The `awaiting_review` row state

When a loop finishes authoring and the reviewer cannot answer (busy, timeout,
prose-only or empty output, truncation, a 503), the worker pushes the branch and
leaves the row `needs-review` with:

```json
{
  "review_state": {
    "state": "awaiting_review",
    "author": "qwen38",
    "author_family": "alibaba",
    "tried": [{"model_id": "gpt-oss-20b", "family": "openai",
               "failure": "review_busy", "at": 1793422499.5}],
    "attempt": 1,
    "max_attempts": 3,
    "next_attempt_at": 1793423399.5,
    "last_failure": "reviewer_unavailable",
    "updated_at": 1793422499.5
  }
}
```

`suggested_action` in the triage view, and `review_queue_triage_row()` in
`hive/workers/doctor.py`, translate that block into one line an operator can act
on (`awaiting cross-family review: ...`). The doctor never classifies these rows
stale while attempts remain.

## 1. The model family table

`hive/core/model-families.json` is the **one** source of truth. The family of
every roster model is answered here and nowhere else; a unit test asserts every
roster model has exactly one family.

| Pattern | Family |
| --- | --- |
| `qwen38`, `Qwen*` | alibaba |
| `gemma*` | google |
| `gpt-oss*`, `gpt-6*`, `codex*` | openai |
| `nemotron*` | nvidia |
| `MiniMax-M3`, `M3.1` | minimax |
| `deepseek*` | deepseek |
| `muse-spark*` | meta |
| `mimo*` | xiaomi (never a reviewer) |
| `stepfun*` | stepfun |
| `claude*` | anthropic |
| anything else | its own family (`model:<id>`) — matches nothing |

The **longest** matching pattern wins, so a specific id is never shadowed by a
broader pattern. An unknown model is its own family and therefore never shares a
family with anything: it fails closed rather than guessing.

## 2. The reviewer roster

`hive/core/reviewer-roster.json` lists every reviewer-capable route: provider,
model id, family, location (`local` / `provider` / `eih`), concurrency limit,
cost class and flags. It was written from the real zoder route config and
`deploy/hive_worker.py`, not invented.

| Route | Model | Family | Location | Concurrency | Notes |
| --- | --- | --- | --- | --- | --- |
| `tydeus-qwen38` | `qwen38` | alibaba | local | 4 | TYDEUS `192.0.2.73:8006`, vLLM |
| `cerberus-gemma` | `gemma4-31b` | google | local | 2 | CERBERUS `192.0.2.96:8080` |
| `cerberus-gpt-oss` | `gpt-oss-20b` | openai | local | 8 | CERBERUS; serves roles `cerberus-reviewer` **and** `cerberus-narrator` |
| `minimax-m3` | `MiniMax-M3` | minimax | provider | 2 | |
| `minimax-m3-1-flash` | `MiniMax-M3.1-Flash-Preview` | minimax | provider | 2 | |
| `deepseek-flash` | `deepseek-flash` | deepseek | provider | 2 | |
| `muse-spark-1.3` | `muse-spark-1.3` | meta | provider | 2 | contributor tier |
| `stepfun-3.5` | `stepfun-3.5` | stepfun | provider | 2 | contributor tier |
| `eih-nemotron` | `nvidia/nemotron-3-ultra-550b-a55b` | nvidia | eih | 1 | **slow** (39 s for a one-word reply); bulk priority lowest |

Rules encoded in the flags:

* **MiMo is never a reviewer.** It is not in the roster at all, and
  `_reviewable()` rejects any `mimo*` route as a second line of defence.
* **StepFun and Meta are contributor tiers**, allowed only on operator-approved
  repos: `graeae-hive`, `fleet-ops`, `tokenomics`, `ncz-screensavers`, `zoder`.
  On any other repo they are not picked.
* **EIH Nemotron is allowed but slow**, so it sorts last for bulk work.

## 3. Pairing

`hive.review_queue.pick_reviewers(author_model, roster, health, load)`:

1. drops every route whose family equals the author's family (and every known
   co-author's family);
2. drops unhealthy routes (`health(route) -> bool`);
3. drops saturated routes (`load(route) >= concurrency`);
4. applies the contributor-tier repo rule;
5. sorts with a deterministic tie-break — cost class, then family load, then
   route id — so a repeated call returns the same order.

**An empty list is a verdict.** With no cross-family reviewer available the row
is **parked** (`parked_reason: no_cross_family_reviewer`): never self-reviewed,
never failed, never sent to a same-family reviewer.

The worker exposes the same pick as `review_queue_roster()` /
`review_queue_reviewers(...)` so both sides share one implementation. A review
is only an **approval** when it is a structured JSON verdict naming
`reviewers[].model` **and** the head sha it reviewed. Prose-only, empty,
truncated, 503 or non-JSON output is never an approval — it is a retryable
failure kind (`review_busy`, `review_timeout`, `review_prose_only`,
`review_empty`, `review_truncated`, `review_http_503`).

## 4. The sweep

The doctor's **existing** needs-review sweep (`report_needs_review_sweep` →
`run_review_queue_sweep`) consumes awaiting rows on the same pass:

| Row state | Action |
| --- | --- |
| awaiting, budget left, no reviewer available | **parked** — `no_cross_family_reviewer` |
| awaiting, budget left, cross-family pick exists | submit ONE read-only review job naming the pick |
| child returned a structured **approve** for the delivered head | record `approved`, hand to the merge path |
| child returned **request-changes** | record it, back to the author path |
| child failed / prose-only | `attempt + 1`, schedule with exponential backoff |
| attempts exhausted | stay `needs-review` with `attempts_exhausted_owner_wait` |

The row always stays `needs-review`. Outcomes are counted per row, so one bad
row cannot stop the sweep.

## 5. Load awareness

`route_load_fn()` reads llama-server `/slots` (count of `is_processing` slots)
for local llama-server routes and vLLM `/metrics` for vLLM routes — by the
route's declared `server` kind, so each probe is one request. Reads are cached
for **30 s** (cheap). Provider routes have no slot endpoint and return 0; their
concurrency cap is enforced at dispatch. The sweep never sends more than a
route's configured concurrency to that route.

## 6. Visibility

* `scripts/hive_status.py` — the weekend triage listing shows `review=<state>`
  and `reason=<...>` on the attention row.
* `GET /v1/jobs?review_state=awaiting_review` — a filter over the same
  `needs-review` rows. An unknown value is a **400**, never "everything".
* `GET /v1/jobs/summary` — `awaiting_review: {n, oldest_age_sec}`, counted as a
  slice of `needs_review`.
* Doctor logs/broadcasts: `doctor.review_approved`, `doctor.review_request_changes`,
  `doctor.review_retry`, `doctor.review_queue_exhausted`, `doctor.sweep_error`.

## Config knobs

| Knob | Where | Meaning |
| --- | --- | --- |
| `AWAITING_REVIEW` | `hive/review_queue.py` | the one state value (`awaiting_review`) |
| `MAX_REVIEW_ATTEMPTS` | `hive/review_queue.py` | attempts before owner-wait (3) |
| `REVIEW_BACKOFF_SEC` | `hive/review_queue.py` | first backoff step (900 s), doubling |
| `MAX_BACKOFF_SEC` | `hive/review_queue.py` | backoff ceiling (7200 s) |
| `LOAD_CACHE_SEC` | `hive/review_queue.py` | load read cache (30 s) |
| `APPROVED_REPOS` | `hive/review_queue.py` | contributor-tier repos |
| `REVIEW_QUEUE_ROUTE_HEALTH` | `hive/workers/doctor.py` | per-route health overrides (unlisted = healthy) |
| `REVIEW_QUEUE_ROUTE_LOAD` | `hive/workers/doctor.py` | override for the load reader |
| `HIVE_ZODER_REVIEWER_ALT` | `deploy/hive_worker.py` | quiet-window backup reviewer (EIH Nemotron) |
| `HIVE_ZODER_REVIEWER_QUIET_WINDOWS` | `deploy/hive_worker.py` | publish-burst windows routed to the backup |
| `reviewer-roster.json` | `hive/core/` | concurrency, cost class, flags per route |

## Rollback

Nothing here is deployed by this change; deployment to PYTHIA needs the
operator. If the behaviour must be reverted:

1. **Worker side** — remove the review-queue section from
   `deploy/hive_worker.py` and the `review_queue` import. Rows then fall back to
   the old `resolve_delivery_status` disposition (`reviewer_unavailable`
   fails the job again). No data migration is needed: `result.review_state` is
   an additive key the old code ignores.
2. **Doctor side** — remove the review-queue section from
   `hive/workers/doctor.py`; the sweep stops consuming awaiting rows and they
   simply stay `needs-review` (visible, not failed). Remove the `review_state`
   filter and `awaiting_review` summary key from
   `hive/bus/routers/jobs.py` if the API surface must shrink too.
3. **Data files** — `hive/core/model-families.json` and
   `hive/core/reviewer-roster.json` can stay: they are pure data and harmless
   without the code.
4. **Rows already awaiting** — patch `result.review_state` off the affected
   rows (or set them to `done`/`failed`) to return them to ordinary needs-review
   handling.

## Tests

* `test_review_queue_20261031.py` — family table, roster, pairing matrix, the
  sweep table, verdict parsing, load probe.
* `test_triage_awaiting_review_20261031.py` — triage listing, `?review_state=`
  filter, summary counts.
* `test_worker_review_queue_20261031.py` — the worker seams: cross-family pick,
  failure classification, disposition, resume delay.
