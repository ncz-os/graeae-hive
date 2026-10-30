# Changelog — 2026-10-29

User-facing features and fixes merged today on the integrate line
(`deploy/integrate-20261017`) and the worker line
(`deploy/hive-canonical-20261029`). One line each, no hashes; branch
names under Sources. Operator-visible changes are called out inline.

New environment variables are listed in `docs/ENVIRONMENT.md`: the
worker and doctor address settings, `HIVE_AGENT_STALE_SECONDS`, and
the control-host variables.

## Bus
- Agent deregistration endpoint with an optional stale-agent reaper
  rule (`HIVE_AGENT_STALE_SECONDS`).
- The `freebuff` agent kind registers, heartbeats and may submit like
  the other agentic CLI kinds.
- Loop-outcome inputs are stored on the job (`loop_result` teardown)
  so the doctor can act on why a loop failed.

## Doctor
- New deterministic outcome classes (signal-killed, clone-failed and
  the loop-teardown set) with per-class counters.
- The orphan-failure sweep keys its cooldown on the set of failed rows
  instead of the base kind, so a new failure is acted on at once.
- An empty diff is parked once for review instead of requeued as a
  reviewer outage.
- The needs-review sweep reports merged, superseded and parked rows
  with evidence and a suggested action (report only).

## Worker
- The deploy script accepts `HIVE_DEPLOY_*` path overrides and ships a
  hermetic test proving kinds map to units, installs are idempotent,
  dirty checkouts fail loudly and dry runs write nothing.
- Worker test suites run on macOS (follow-up fixes the reported
  collection-crash cause).

## Tools
- `explain_job`: one command printing status, loop class, timeline,
  reviewer record, workspace facts and failure text, fully offline via
  `--row`/`--events`.
- `submit_job`: a submit helper encoding the operating rules (no host
  pins without reason, builders-only image builds, no codex without an
  explicit allow, content-derived idempotency key).
- `hive_status`: one-screen operator status (counts, active jobs,
  attention rows with planned doctor action, stale/offline flags).
- `gen_api_reference`: generated `docs/API_REFERENCE.md` from
  `app.openapi()` with a `--check` drift test.
- End-to-end doctor test driving the real bus app in-process across
  every failure shape.
- Job lifecycle document with a transition table checked both ways
  against the real state machine plus live endpoint spot checks.
- Reviewer failover file and dual-reviewer design note (docs).

## Docs
- Product documentation written from the code (G6).
- Control-host environment pack and deploy doc; IMAGE build runbook
  for the builder hosts; fleet inventory source of truth.
- Needs-review policy design (doc only).
- This changelog.

## Tests
- Remaining macOS/environment failures made hermetic or precisely
  skipped.
- Idempotency audit fixes (one codex-fix dispatch per parent, one
  terminal PATCH per job and worker, stubbed sidecars) with
  regression tests that failed before and pass after.
- Retry-budget tests updated to the current resubmit-as-new-job
  contract.

## Sources

Sources-through: 2026-10-29 (20 freebuff/sonnet merges listed below)
- Merge freebuff/fb13-20261029
- Merge freebuff/fb12-20261029
- Merge freebuff/fb11-20261029
- Merge freebuff/fb8-20261029
- Merge sonnet/h8-20261029
- Merge freebuff/fb5-20261029
- Merge freebuff/g6-20261029
- Merge freebuff/fb6-20261029
- Merge sonnet/h7-20261029
- Merge sonnet/h2-20261029
- Merge freebuff/h6-20261029
- Merge freebuff/h1-20261029
- Merge freebuff/g1-20261029
- Merge remote-tracking branch 'origin/freebuff/f1-20261029' into merged-run
- Merge feat/doctor-loop-outcomes-20261029
- Merge sonnet/nr1-20261029
- Merge sonnet/h5-20261029 (worker line)
- Merge freebuff/g7-20261029 (worker line)
- Merge freebuff/g4-20261029 (worker line)
- Merge freebuff/g2-20261029
- Merge freebuff/fb16-20261029
- Merge freebuff/fb18-20261029
