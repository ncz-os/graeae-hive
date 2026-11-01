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
on (`awaiting cross-family review: ...`). While attempts remain (or a review is
in flight) the report classifies the row `awaiting_review`, never `stale`, and it
is never escalated. Once the attempts are spent and nothing is in flight the row
is `stale`, its suggested action names the owner, and
`escalate_stuck_needs_review` sends ONE `doctor.needs_review_stuck` for it.

## 1. The model family table

`hive/core/model-families.json` is the **one** source of truth. The family of
every roster model is answered here and nowhere else; a unit test asserts every
roster model has exactly one family.

| Pattern | Family |
| --- | --- |
| `qwen38`, `Qwen*` | alibaba |
| `gemma*` | google |
| `gpt-oss*`, `gpt-6*`, `codex*`, `*-codex*` | openai |
| `nemotron*` | nvidia |
| `minimax*` (M3, M3.1, M3.1-Flash-Preview, M4, ...) | minimax |
| `deepseek*` | deepseek |
| `muse-spark*` | meta |
| `mimo*` | xiaomi (never a reviewer) |
| `stepfun*` | stepfun |
| `claude*` | anthropic |
| anything else | its own family (`model:<id>`) — matches nothing |

Names match **case-insensitively** (`minimax-m3.1-flash-preview` and
`MiniMax-M3.1-Flash-Preview` are one model). The **longest** matching pattern
wins, so a specific id is never shadowed by a broader pattern. A roster route id
or fleet role id (`tydeus-qwen38`, `cerberus-reviewer`, `cerberus-narrator`)
resolves to its route's model through `reviewer-roster.json` first; only then
are the deployed llm-roles map and `provenance` aliases (`minimax_flash`) asked,
so a known name never depends on which optional modules are importable. An
unknown model is its own family and therefore never shares a family with
anything: it fails closed rather than guessing.

## 2. The reviewer roster

`hive/core/reviewer-roster.json` lists every reviewer-capable route: provider,
model id, family, location (`local` / `provider` / `eih`), concurrency limit,
cost class and flags. It was written from the real zoder route config and
`deploy/hive_worker.py`, not invented.

| Route | Model | Family | Location | Concurrency | Notes |
| --- | --- | --- | --- | --- | --- |
| `tydeus-qwen38` | `qwen38` | alibaba | local | 4 | host `tydeus`, port 8006, vLLM |
| `cerberus-gemma` | `gemma4-31b` | google | local | 2 | host `cerberus`, port 8080, llama-server |
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
* **EIH Nemotron is allowed but slow** (`priority: -10`), so it sorts last for
  bulk work — load reorders routes only within a priority tier, never above it.
* **Local routes carry `host` + `port`, never an address.** The address comes
  from the operator's fleet inventory (`HIVE_FLEET_INVENTORY`, see
  `hive/core/fleet_inventory.py`); with no inventory entry the route's load is
  unknown and it is not picked.

### The Codex route (operator, 2026-10-31)

Codex is a reviewer route (`codex-worker`, family **openai**, same family as
`gpt-oss*`) served **only** by the confined local Hive Codex worker:

| Field | Value |
| --- | --- |
| kind | `codex` (`workspace-write`, limited to the job workspace) |
| service | CERBERUS `hive-worker-codex.service` |
| location | `local` — **never** EIH, never Azure/AWS/GCP/OCI, never the retired `:4100` gateway |
| token | PYTHIA holds the master OAuth token; nothing in Hive logs in, refreshes or reads it |
| concurrency | 1 |
| ladder | `gpt-6-luna` → `gpt-6.1-terra` → `gpt-6-sol` |
| authorization model | `gpt-6-astra` — only on an explicit operator authorization recorded in the job, never auto-selected, never pinned |

**Gating.** Every automatic Codex dispatch — including any started from a cron
or a hook — goes through `review_queue.codex_gate()`; there is no path that
picks the route around it. The gate refuses, in order, when:

1. `HIVE_REVIEW_CODEX_ENABLED` is `0`/`false`/`no`/`off` (`codex_disabled`).
   Unset means on **only** while the worker is online;
2. the codex worker kind is not online (`codex_worker_offline`) — an offline
   worker would otherwise strand the review;
3. a live cooldown marker exists at
   `<state_dir>/cooldowns/codex.json` (`codex_cooldown`); the worker writes one
   when Codex reports a usage limit, it expires after `CODEX_COOLDOWN_SECS`
   (6 h) and an expired or corrupt marker is inert;
4. the plan usage share cannot be read (`codex_usage_unknown`) — a host with no
   usage probe fails **closed**;
5. the remaining share is under 30 % (`codex_quota_low`, `min_remaining_pct`).

**Priority.** Codex sorts **last** for bulk review (`priority: -5`, below local,
DeepSeek and MiniMax M3) and **first** for the hard lane
(`hard_lane_priority: 20`). The lane is `hard` when the delivered change touches
security, auth/authn/authz/oauth, redaction, budget or architecture paths (see
`_HARD_PATH_RES`); everything else is `bulk`. The hard lane still never
self-reviews: an openai author or co-author is dropped before the order
matters, so Codex-authored code always gets a **non-openai** review.

**The review job.** A Codex review is a normal Hive job of kind `codex`
(`eligible_kinds: ["codex"]`, `max_concurrency: 1`) with the same structured
JSON verdict contract as every other reviewer (`approve`/`request_changes` and
`reason`, `reviewers[]` naming the model e.g. `gpt-6-luna`, and the reviewed
head sha). Prose-only or empty output is never an approval.

## 3. Pairing

`hive.review_queue.pick_reviewers(author_model, roster, health, load)`:

1. returns `[]` (park) when there is no author at all — no family to exclude
   means no cross-family guarantee;
2. drops every route whose family equals the author's family (and every known
   co-author's family);
3. drops unhealthy routes (`health(route) -> bool`);
4. drops saturated routes (`load(route) >= concurrency`);
5. applies the contributor-tier repo rule;
6. sorts with a deterministic tie-break, so a repeated call returns the same
   order. Every key component below is compared ASCENDING (lowest first), and
   priority values are negated in the key, so a HIGHER `priority` value is
   picked EARLIER. The key depends on the row's LANE (see §2, "The Codex
   route"):
   * `bulk` — roster `flags.priority` descending, then `local` < `provider` <
     `eih`, then route id. Codex carries `priority: -5`, the lowest of any
     non-slow route, so for bulk review it sorts behind every local, DeepSeek
     and MiniMax M3 reviewer.
   * `hard` — `hard_lane_priority` descending, then `flags.priority`
     descending, then route id. Codex carries `hard_lane_priority: 20`, the
     highest in the roster, so it leads the authoring review of hard-lane
     (security/auth/redaction/budget/architecture) work.
   The lane never relaxes pairing: family exclusion happens before the order
   matters, so a hard-lane Codex pick still cannot review an openai author.

**An empty list is a verdict.** With no cross-family reviewer available the row
is **parked** (`parked_reason: no_cross_family_reviewer`): never self-reviewed,
never failed, never sent to a same-family reviewer.

The worker exposes the same pick as `review_queue_roster()` /
`review_queue_reviewers(...)` so both sides share one implementation. A review
is only an **approval** when it is a structured JSON verdict naming
`reviewers[].model` **and** the head sha it reviewed, and that head is the
source row's delivered head (never the head the child itself claims). Only
CROSS-family entries count: a reviewer in the author's (or a co-author's)
family is ignored, and a verdict with no cross-family entry is
`review_same_family`. A head that differs is `review_head_mismatch`; a verdict
or row naming no head is `review_head_missing`. Any cross-family
request-changes outweighs an approve. Prose-only, empty, truncated, 503 or
non-JSON output is never an approval — it is a retryable failure kind
(`review_busy`, `review_timeout`, `review_prose_only`, `review_empty`,
`review_truncated`, `review_http_503`).

## 4. The sweep

The doctor's **existing** needs-review sweep (`report_needs_review_sweep` →
`run_review_queue_sweep`) consumes awaiting rows on the same pass:

| Row state | Action |
| --- | --- |
| awaiting, budget left, no reviewer available | **parked** — `no_cross_family_reviewer` |
| awaiting, budget left, cross-family pick exists | submit ONE read-only review job naming the pick |
| awaiting, cross-family routes exist but all at their cap | wait unchanged (`capacity`): no attempt spent, no park |
| child returned a structured **approve** for the delivered head | record `approved`, hand to the merge path |
| child returned **request-changes** | record it, back to the author path |
| child failed / prose-only / same-family / wrong head | clear the dead child, schedule with exponential backoff; the next submit is `attempt + 1` |
| attempts exhausted | stay `needs-review` with `attempts_exhausted_owner_wait`, recorded ONCE (later sweeps neither PATCH nor broadcast) |

The row always stays `needs-review`. Outcomes are counted per row, so one bad
row cannot stop the sweep.

## 5. Load awareness

`route_load_fn()` reads llama-server `http://<address>:<port>/slots` (count of
`is_processing` slots) for local llama-server routes and vLLM
`http://<address>:<port>/metrics` for vLLM routes — both at the server ROOT,
not under `/v1` — by the route's declared `server` kind, so each probe is one
request. `<address>` is the fleet inventory's entry for the route's `host`.
Reads are cached for **30 s** (cheap). Provider routes have no slot endpoint and
return 0.

The sweep never sends more than a route's configured concurrency to that route:
across one pass it adds the reviews already in flight on that route plus the
ones this pass submitted to the probed load. A queued review is not yet visible
to the probe; a running one may be counted twice, which errs on the side of the
cap.

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
| `HIVE_REVIEW_CODEX_ENABLED` | `hive/review_queue.py` | Codex route switch (unset = on while the worker is online; `0`/`false`/`no`/`off` = off) |
| `CODEX_REVIEW_USAGE` | `hive/workers/doctor.py` | Codex plan-usage probe; unset fails the gate closed |
| `CODEX_MIN_REMAINING_PCT` | `hive/review_queue.py` | Codex quota guard: skip under 30 % remaining |
| `CODEX_COOLDOWN_SECS` | `hive/review_queue.py` | Codex usage-limit cooldown marker lifetime (6 h) |
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

## The queue reviews the rows the older worker line left (2026-10-31)

Production showed the gap the operator named: every doctor sweep logged
`review_queue scanned=0 submitted=0` while thirteen needs-review rows sat
unapproved. The queue above only scanned rows that already carried
`result.review_state`, and only the NEW worker code writes that block — workers
still run the older `deploy/hive-canonical` line. The needs-review sweep was
report-only, so nothing ever reviewed those rows.

`submit_adopted_reviews` (`hive/workers/doctor.py`) closes it each sweep:

1. **Adopt.** A `needs-review` row with a pushed `head_branch` whose `result`
   carries no structured approve for the CURRENT head becomes an
   `awaiting_review` row. That covers `reviewer_unavailable`, request-changes at
   an OLDER head, a review that did not approve, no reviewer at all, and
   prose-only/empty evidence. `review_state` is built FROM the row — author and
   family from `claimed_model`/`result`, `tried[]` from the reviewers and rounds
   the result recorded, `attempt` 0 — with `adopted_from`/`adopted_head`
   recording the provenance. **The row's status is not changed**: it stays
   `needs-review`, because the queue IS triage.
2. **Review.** Up to `HIVE_DOCTOR_REVIEW_SUBMIT_MAX` (default 3) review jobs per
   sweep go through the existing roster pairing: a cross-family reviewer from
   `hive/core/reviewer-roster.json`, never the author's family, skipping routes
   at their concurrency cap or failing the health check. Retry backoff and
   `max_attempts` come from the existing `review_state`.
3. **Record.** Only a structured verdict counts (see
   `interpret_review_outcome`). Prose, empty, truncated and 503 output is never
   an approval, and an approve counts only for the exact head sha it reviewed.
   The structured verdict, with `reviewers[].model` named, is recorded on the
   row (`review_state.approved`) for the operator-merge pass.
4. **Never merge or cancel.** The doctor records the verdict; it does not merge,
   cancel or change status.

The adoption phase and the review phase are ONE pass over one listing, so
`HIVE_DOCTOR_REVIEW_SUBMIT_MAX` is a cap on the SWEEP, not on each phase: the
review phase's budget is the cap minus what adoption already submitted, and it
never drops below zero. A dry run previews up to the same cap (and no further)
through that one shared floor, so the log lists exactly the reviews a real
sweep would submit.

### A partial block is repaired, never skipped

A row can carry a `result.review_state` block that the sweep cannot advance:
one missing `author`, `attempt` or `max_attempts`, or carrying the wrong shape
(`attempt: "zero"`, `tried: "qwen"`) — a partial write, a hand edit, or a bus
dialect that stringified the JSON. Such a block is NOT "the queue already owns
this row": `usable_review_state` refuses it and the adoption pass REPAIRS it
from the row (review 2026-10-31, high). The repair keeps whatever the block
still holds that is trustworthy — the author it named, the attempts spent, a
pending `next_attempt_at`, a `parked_reason` and a live `review_job_id` — so a
repair neither re-forges a known author nor lets a row spend its retry budget
twice, and records the defect in `repaired_from` / `adopted_from`. A block with
a live review is still never a second review, and a COMPLETE block is never
re-adopted.

### No duplicate review jobs

A row is never submitted twice. Before any route picking,
`process_awaiting_review_row` reads the row's live review: `review_job_id` from
`review_state`, and for an adopted row the earlier worker's own
`result.auto_review_job_id` or any non-terminal `review:` child. A live child
means the row is `awaited`, not submitted. `adopt_legacy_review_row` also
refuses to adopt a row with a review child still running.

### Lane B is never the doctor's

Rows in project `riskyeats`, on the `test/` `refactor/` `fix/` `docs/` `perf/`
`ops/` `feat/` branch families, or whose job id is in a lane-B owner-wait chain
(`01a18ab8-0a4f…`, `01a18a8d-9ffa…`) are not adopted, not submitted for and not
touched: their verdict is the owner's (`lane_b_row`).

### Configuration

| Variable | Default | Meaning |
|---|---|---|
| `HIVE_DOCTOR_ADOPT_LEGACY_REVIEWS` | `1` (ON) | Adopt and review legacy needs-review rows |
| `HIVE_DOCTOR_REVIEW_SUBMIT_MAX` | `3` | Review jobs one sweep may submit (never below 0) |
| `HIVE_DOCTOR_ADOPT_DRY_RUN` | `0` | Log what it would submit; submit and write nothing |

Every one of these is parsed defensively at import (`doctor._env_int`): an unset
or unparsable value uses the default with a logged warning instead of taking the
doctor down at import, and a value below the floor is clamped (a negative cap
means "submit nothing", exit 0, never a crash loop). `HIVE_DOCTOR_ADOPT_LEGACY_REVIEWS`
and `HIVE_DOCTOR_ADOPT_DRY_RUN` are read at import from the process env only.

## Tests

* `test_review_queue_20261031.py` — family table, roster, pairing matrix, the
  sweep table, verdict parsing, load probe.
* `test_triage_awaiting_review_20261031.py` — triage listing, `?review_state=`
  filter, summary counts.
* `test_worker_review_queue_20261031.py` — the worker seams: cross-family pick,
  failure classification, disposition, resume delay.
* `test_review_codex_20261031.py` — the Codex route: roster row and family
  entry, the ladder and the authorization-only model, an openai author never
  gets Codex (either direction, `gpt-oss` included), the gate's five refusals
  with a fake usage source and cooldown marker, the concurrency cap, bulk-vs-
  hard-lane ordering, the kind `codex` job spec, no-token-access, and the
  structured verdict contract (prose-only is never an approval).
* `test_doctor_adopt_legacy_reviews_20261031.py` — adoption of each legacy
  shape, no adoption while a review is in flight or an approve covers the head,
  cross-family pairing (an alibaba author never gets a qwen reviewer), the
  per-sweep cap, lane-B exclusion, fake-clock backoff, and idempotency across
  sweeps (including a row already holding `review_job_id`).
* `test_doctor_review_queue_hardening_20261031.py` — the two findings of the
  structured review: a partial or malformed `review_state` block is repaired
  (adopted, not skipped) and every import-time env int is parsed defensively.
* `test_review_queue_fixes_20261031.py` — the defects found in review: name
  resolution (case, aliases, role ids, newer MiniMax), parking with no author,
  cross-family and head-bound approvals, retry/exhaust progression, per-route
  caps, EIH ordering, inventory-resolved root probe URLs, JSON-text results,
  report classification and owner escalation.

## Approvals on descendant heads (`hive/workers/doctor_head_match.py`)

Production 2026-10-31: four rows stayed `needs-review` with doctor reason
`approved, but the review job advanced the branch` — review children commit
their verdict file onto the author's branch, so the head they approved is never
the row's tip. The exact-head rule now runs through `classify_descendant`:

* **verdict-only descendant** — `R` descends from tip `T` and `T..R` touches
  only `REVIEW_VERDICT_*.json` / `review-verdict-*.json`. The approve counts:
  recorded as *approved at R (verdict-only descendant of T)*, blocker cleared
  to `result.review_state.state = approved_mergeable` with `approved_sha` so the
  merge pass can act. The doctor never merges.
* **review_overreach** — the review's own commits touched code on the author
  branch (schema fix + test). The approval is **not** counted for `T`, and the
  row records `review_overreach` with a re-review action.
* **newer work** — `R` descends from `T` with code changes: classified
  *approval covers newer work not on the head branch*; the doctor fast-forwards
  `R` onto the head branch only from the review child's workspace (pure FF,
  existing rescue path), otherwise reports the exact sha plus one-line action.
* **everything else stays head-exact** — a non-descendant head is still not an
  approval.

Review jobs publish their verdict to `review/<branch>-<job>`, never the author's
head branch; the worker/doctor refuse a review result whose commits touch files
outside the verdict artifact. The rule is idempotent, per-sweep capped, honours
dry-run, and is switched by `HIVE_DOCTOR_VERDICT_DESCENDANT` (default on).
Tests: `test_doctor_review_head_match_20261031.py`.

### Where the verdict artifact goes, and what it is called

The ref and the artifact name are keyed on the review **job id**, and the brief
says exactly what the guard enforces:

```
review/<head-branch>-<job-id>          # e.g. review/hv/bl-ct-dialect-01a19270-child
REVIEW_VERDICT_<head-branch-slug>.json # e.g. REVIEW_VERDICT_hv-bl-ct-dialect.json
review-verdict-<job-id>.json           # e.g. review-verdict-01a19270-child.json
```

The reviewer's model id is reported in the brief but never appears in a ref or
a file name: `review_result_violation` allows only the job-keyed names, so a
brief naming the reviewer sent the review to publish where its own result was
then refused as overreach.

### Clearing the row is not enough: the merge pass must agree

A verdict-only descendant approve is recorded with `approved.head_sha` set to
the **delivered tip** `T` and `approved.approved_sha` to the commit the reviewer
signed `R`. Those two shas are different *by construction* (that is what a
descendant is), so `merge_handoff` must not treat their difference as staleness:
it exempts `basis == "verdict_only"` (re-proving ancestry and the artifact-only
diff against git whenever a workspace still holds both commits). A row whose
tip has moved since the approve is still `stale_approval`, and a row carrying
`state = approved_mergeable` is read even though `review_state()` only admits
the in-flight `awaiting_review` shape. Without both, the doctor cleared the
blocker and the merge pass refused to act — the row was stuck in a different
place.

`_head_branch_workspace` resolves a row's clone under `HIVE_WORKSPACE_ROOT`
only: no `~/hive-workspaces` fallback, and a job id that escapes the root (or
is absolute outside it) names no workspace. Guessing a path from an id must
never let a sweep grade a checkout the row does not own.
