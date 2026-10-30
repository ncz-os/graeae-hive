# Offload queue

`scripts/offload_queue.py` plans and feeds a single-seat, hour-metered
offload agent. It is a planner and a submit helper. It does not run the
agent, run the acceptance gates, merge, or restart anything. Placeholders
only in this document: no addresses, no tokens, no URNs.

## The model: a consumable queue per lane

A lane is one seat: one agent session at a time, each session bound to one
model.

- **Capacity.** Hours, metered. A session is billed at least
  `sessionMinimumHours` (default 1) times its model's multiplier, then in
  whole session-hours. A daily pool of hours EXPIRES at `dailyResetAt`.
  A separate wallet does not expire.
- **Wallet policy.** `walletPolicy: reserve` is the only policy. The planner
  never plans against the wallet and never suggests spending it. A config
  with any other value is refused at load time.
- **Consumable queue.** Jobs of the configured kind (`kind`, default
  `freebuff`) wait on the bus. The seat claims the next job. Queued work is
  the demand; the pool is the supply that expires.
- **Acceptance gate.** Every job carries its own gate in the brief (below).
  Capacity is only ever spent on work whose result can be verified.
- **Switch cost.** Changing model means a new session, so a new minimum.
  The plan groups queued jobs into one block per model and orders the blocks
  so the seat never returns to a model it has left.

## Daily cadence

1. **Morning: fill.** Run `plan`. Submit the real tasks; if the verdict is
   `UNDER-USED`, pull evergreen items as many as the plan says
   (`evergreen pull N`). Run `release` so the polling agent claims the
   active block's model first.
2. **During the day: verify and merge.** The orchestrator applies the
   declared gates to what the seat delivers, then reviews and merges. `status`
   shows stuck claims and how stale the balance snapshot is. Re-run `plan`
   after each block; the pool and the time left change.
3. **Evening: top up the overnight block.** Before the reset, run `plan`
   again. Whatever the verdict says will expire unused is the budget for one
   last block that finishes before `dailyResetAt` minus the margin.

## The rule: spend capacity only on real, verifiable work

- The planner says `no useful work queued` rather than invent work. An
  empty queue and an empty evergreen backlog is a valid answer; unused pool
  hours expire, and that is cheaper than unreviewable output.
- Only the classes the seat is FOR are submitted (below). The planner
  refuses the others.
- A task with no `check:` line, no `branch:` line, or (for the gated
  classes) no `gates:` line is refused. Without a gate the result cannot be
  verified, so the hours would be wasted.
- Retries are off by default (`maxRetries: 0`): a failed run is reviewed,
  not silently re-spent.

## Privacy rule: only operator-approved repos

A hosted agent may use submitted data for training. Anything in a brief, and
the repository it points at, leaves the building. Therefore:

- `allowedRepos` lists the repositories the operator has approved for
  offload. **The default is empty, and an empty list refuses every submit**,
  with the reason `repo not approved for offload`.
- Matching is exact, per repo name. There are no wildcards.
- `evergreen pull` goes through the same check and never pulls an item for a
  repo that is not in the list.
- Do not put secrets, customer data or unreleased security material in a
  brief. The `security` class is refused for the same reason.

## Work classes

Accepted ("iteration-friendly"): `test-write`, `test-harden`, `flake-hunt`,
`lint-type-cleanup`, `dead-code`, `mechanical-migration`,
`characterization`, `soak-loop`, `doc-from-code`.

Refused with the reason `class not suitable for the offload seat`: `design`,
`security`, `hardware`, `live-infra`, `irreversible`. An unknown or missing
class is refused too.

Default `minutesPerTask` (override per class in the config; a job's own
`estimate-minutes` wins): `test-write` 45, `test-harden` 40, `flake-hunt` 60,
`lint-type-cleanup` 30, `dead-code` 30, `mechanical-migration` 60,
`characterization` 45, `soak-loop` 90, `doc-from-code` 30, `default` 30.

## The brief: structured gate lines

Each of these is a line of its own in the brief:

```
check: <exact command that must pass>
branch: <branch name to push>
gates: no-assertion-weakening, red-on-base, no-new-skips, suite-before-after
base: <optional base branch>
```

- `check:` and `branch:` are required for every class. The branch name must
  be a valid git ref name. Two different values for the same line are an
  error, so the verifier never has to guess.
- `gates:` is required for `test-write`, `test-harden`, `lint-type-cleanup`,
  `dead-code` and `mechanical-migration`, and every name must be one of
  `no-assertion-weakening`, `red-on-base`, `no-new-skips`,
  `suite-before-after`. The planner does NOT run the gates. It checks they
  are declared and copies them to the job.
- The brief must be under 48 KiB.

The job description starts with a parseable header (`offload-task: v1`,
`title`, `class`, `model`, `multiplier`, `estimate-minutes`, `ab_test`,
`gates`, `check`, `branch`), then a `---` line, then the brief. The verifier
reads the gates from that header. `head_branch` carries the branch,
`base_branch` the base (task field, `base:` line, or `baseBranch` in the
config; the bus refuses a new `head_branch` whose base is not on origin).

## Model policy

The approved band is `minMultiplier` (default 2) to `maxMultiplier`
(default 4). A requested model outside it is refused with the reason
`model outside the approved multiplier band (2x floor, 4x cap)` (the numbers
follow the config). The model must be in the models snapshot, available, and
have a multiplier.

- **No model given:** the available model at `defaultMultiplier` (default 3)
  is used; the lowest model id wins so the choice is stable. If there is
  none, the task is refused.
- **1x (A/B test):** allowed only when the config sets `allowAbTest: true`
  AND the task carries `ab_test: true`. Either alone is refused.
- **Above the cap:** needs the model id in `approvedAboveCap`, which is
  operator-managed. Approval does not make an unavailable model usable.
- **Hold rule:** a session starts with a hold of the multiplier in hours.
  The plan refuses (`CANNOT-START`) a session whose hold exceeds the
  remaining pool, with the reason `start-of-session hold exceeds the remaining
  pool`. It likewise refuses when `sessionMinimumHours` times the multiplier
  exceeds the pool, or when the first task cannot plausibly finish before
  `dailyResetAt` minus `resetMarginMinutes`.

## Configuration

`OFFLOAD_CONFIG` names a JSON file. Every key is optional.

| key | default | meaning |
| --- | --- | --- |
| `kind` | `freebuff` | the job kind; the only product-specific word, and it is a value |
| `where` | empty | free-text label for the host class the seat runs on; `orchestration-only` turns on the build-heavy warning |
| `allowedRepos` | `[]` | approved repos; empty refuses every submit |
| `minMultiplier` / `maxMultiplier` / `defaultMultiplier` | 2 / 4 / 3 | the approved band and the default tier |
| `allowAbTest` | `false` | permits 1x jobs that carry `ab_test: true` |
| `approvedAboveCap` | `[]` | model ids allowed above the cap |
| `walletPolicy` | `reserve` | the only accepted value |
| `minutesPerTask` | table above | minutes by class |
| `sessionMinimumHours` | 1 | minimum billed hours per session |
| `resetMarginMinutes` | 30 | stop planning this long before the reset |
| `staleSnapshotMinutes` | 15 | balance snapshot age that warns |
| `leaseSeconds` | 1800 | claim lease used when a row carries none |
| `priorityBands` | top 90, step 10, floor 10, deferred 0 | band for block rank r is `max(floor, top - r*step)` |
| `evergreenFile` | none | the standing backlog |
| `branchPrefix` | `offload/` | prefix for derived evergreen branch names |
| `baseBranch` | `main` | base for new head branches; `null` to send none |
| `maxRetries` | 0 | bus retry budget for submitted jobs |
| `balancedSlackHours` | `sessionMinimumHours * minMultiplier` | unused pool below this is BALANCED |

Unknown config keys are listed in the plan as warnings, so a typo is loud.

## Environment

| name | use |
| --- | --- |
| `HIVE_URL` | bus base URL, as for `scripts/hive_status.py`; never compiled in |
| `HIVE_ACTOR_URN` | submitter identity for `submit` (never printed; a dry run shows a placeholder) |
| `HIVE_BUS_ADMIN_TOKEN` | optional bearer token; never printed |
| `OFFLOAD_CONFIG` | the config file |
| `OFFLOAD_BALANCE_FILE` | the balance snapshot |
| `OFFLOAD_MODELS_FILE` | the models snapshot |

The command-line flags `--config`, `--balance`, `--models` override the files;
`--jobs FILE` reads the queue from a file instead of the bus (offline);
`--now` fixes the clock.

Balance snapshot: `{"balance": {"dailyRemainingHours", "walletHours",
"spendableHours", "dailyResetAt", "updatedAt"}, "sessionModel",
"sessionMultiplier", "sessionStartedAt", "state"}`; nulls are allowed and are
reported under `cannot compute`. The planner uses `dailyRemainingHours` only;
`walletHours` and `spendableHours` are displayed or ignored, never planned.
A session counts as open when `sessionModel` and `sessionStartedAt` are set
and `state` is not one of idle, closed, ended, stopped, none, inactive,
expired. An open session's model is ordered first and does not pay a new
minimum.

Models snapshot: `{"models": [{"id", "displayName", "available",
"costMultiplier"}]}`.

## Commands

```
offload_queue.py submit --task task.json [--dry-run] [--json]
offload_queue.py plan [--json]
offload_queue.py release [--dry-run] [--job-id ID ...]
offload_queue.py status [--json]
offload_queue.py evergreen list
offload_queue.py evergreen pull N [--dry-run]
```

Exit codes: 0 done, 2 configuration or bus error, 3 a task was refused,
4 release could not apply the bands.

### submit

One task, one job. The task file is JSON: `title`, `repo`, `class`, `brief`,
and optionally `model`, `depends_on`, `ab_test`, `estimateMinutes`,
`base_branch`. Unknown keys are refused (a mistyped `depends_on` must not be
dropped silently). Checks, in this order: repo approved, class, brief size,
`check:`, `branch:`, `gates:`, model, `depends_on` ids exist on the bus. The
job gets `eligible_kinds [kind]`, `preferred_models [model]`, a priority from
the plan band of the block the task joins, and an idempotency key derived from
repo, class, title, branch, check and brief: submitting the same task twice
returns the existing job (`existing job <id>`). The bus coalesces only while
the first job is active.

### plan

Reads the queue, balance, models and config and prints, for the current
window: blocks (model, multiplier, tasks, work minutes, session hours, cost
hours, band), model switches naive versus planned, the total against
`dailyRemainingHours`, the projected unused pool at `dailyResetAt`, deferred
tasks, and one verdict:

| verdict | meaning |
| --- | --- |
| `UNDER-USED` | pool would expire unused; the plan says how many evergreen items fit |
| `BALANCED` | the work uses the pool; what is left is below one session minimum |
| `OVER` | some tasks do not fit; they are listed and deferred |
| `CANNOT-START` | not even the first session can start (hold, minimum or time) |
| `UNKNOWN` | the pool is null or from before the reset; the plan says what it cannot compute |

Cost of a block is `max(sessionMinimumHours, ceil(minutes / 60)) * multiplier`
(an open session's block skips the minimum). Tasks are filled in claim order
until the affordable whole session-hours or the time before the reset are
used. With `where` marked `orchestration-only`, the plan warns when it holds
`soak-loop`, `flake-hunt` or `mechanical-migration` tasks.

### release

Computes the plan and assigns the band of each block to the queued jobs of the
configured kind: the active block (first planned block) gets the top band,
later blocks lower ones, deferred tasks the `deferred` band. Jobs of other kinds
are never touched. `--dry-run` prints without writing. Each PATCH is read back.

**Known gap.** The bus update API (`PATCH /v1/jobs/{id}`, `JobUpdate`) has no
`priority` field today, so a bus that follows this document's version accepts
the call and leaves the priority unchanged. `release` detects that, reports
`not-applied` and exits 4 instead of claiming success. Until the bus accepts
priority on queued rows, bands are effective only for jobs at submit time
(`submit` already applies them). Adding an optional `priority` to `JobUpdate`
for `queued`/`offered` rows is the missing bus change.

### status

Queue depth by state for the kind, planned hours (from the current plan) versus
estimated spent hours (done jobs since the window start, one session per
model; an estimate, not billing), the balance snapshot age with a warning past
`staleSnapshotMinutes`, jobs claimed or running beyond their lease, and the
evergreen backlog depth.

### evergreen

A backlog of standing low-risk tasks, JSON (a list or `{"items": [...]}`) or
plain text (items separated by a `---` line, `key: value` lines, the `brief:`
value continues until the next key). Each item has `title`, `repo`, `brief`,
`check`, `estimateMinutes`, `class`; it may also carry `branch`, `gates`,
`model`. A missing branch is derived from `branchPrefix`, the title and the
date. A gated class needs `gates`; an item without it is skipped, not invented.
`pull N` submits N new jobs through the same validation as `submit`, in file
order; items already queued are reported and not counted; items for repos not
in `allowedRepos` are never submitted.
