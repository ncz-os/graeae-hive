# Freebuff as a Hive worker (prompt-inbox profile)

This directory holds the settings that let a Freebuff session take jobs from the
Hive bus and report back, using only the prompt inbox and the Hive MCP tools.
There is no bus or worker code in it: the bus is used as it is today.

Files:

- `inbox.hive.example.json`: the inbox settings (copy it, fill the placeholders).
- `job-template.md`: the shape of a job description that suits an unattended Freebuff session.
- `../test_freebuff_hive_profile_20261029.py`: checks the example file, then runs the profile against the real bus
  in-process through the real `hive_job_next` / `hive_job_update` MCP handlers.

## What is verified and what is not

The inbox rules this profile relies on (message `id` / `from` / `text` checks, `allowFrom`, the model guard,
dedupe by id, `{{placeholders}}`, the JSONPath subset) follow `docs/prompt-inbox.md` of the Freebuff inbox patch.

The keys `valueMaps`, `sourceMeta`, `noWork` and the `map.text` template are **not** in the revision of that
document this profile was written against. They are this profile's own contract with the inbox patch: the
patch has to implement them as written here, or the example needs adjusting. The test simulates them.

The real inbox build was not run. Everything on the bus side was.

## Prerequisites

1. A Freebuff build that has the prompt inbox, with the keys above.
2. A `freebuff` agent registered on the bus (`POST /v1/agents/register`, kind `freebuff`, runtime `freebuff`).
   Its URN has the form `urn:agent:<kind>:<host>:<session>`. Keep it online (heartbeat); the bus only hands
   jobs to an `online` or `idle` agent.
3. A Hive MCP server entry named `hive` in the Freebuff `mcp.json`, pointing at the bridge in `hive/mcp/bridge.py`.
   The bridge takes its address and credentials from its environment, not from this directory.
4. These environment variables where Freebuff runs (no value is stored in the example):
   - `HIVE_AGENT_URN`: the registered URN of this Freebuff agent.
   - `HIVE_ORCHESTRATOR_URN_1`, `HIVE_ORCHESTRATOR_URN_2`: the submitters whose jobs may run. Drop the second
     entry if you only have one.
5. In the copied settings, replace `<allowed-freebuff-model-key>` with the model key(s) you allow.

## How a job moves

| Inbox event | Sent to the bus | Job status |
| --- | --- | --- |
| source poll (`hive_job_next`) | claims the job for `agent_urn` | `claimed` |
| `accepted` | `status: claimed` | `claimed` |
| `started` | `status: running` | `running` |
| `done`, outcome `completed` | `status: done` | `done` |
| `done`, outcome `error` or `interrupted` | `status: failed` | `failed` (see limits: default retries) |
| `done`, outcome `session_ended` | `status: queued` | `queued` (released, backoff of about 10 s) |
| `not_accepted` | `status: queued` | `queued` (released, reason in the result) |

The mapping is done with `valueMaps`, so the sink reports Freebuff's words and the bus gets its own.

The poll `args` carry `agent_urn`; an empty queue returns `{"_status": 204}`, which `noWork` treats as nothing to do.
The message `text` is the job description plus `repo` and `head_branch`; `model` is the first of
`preferred_models`, and the inbox model guard (`allowedModels`, `maxCostMultiplier`) still has the last word.

## Safety properties

- `allowFrom` is required for an `mcp` source and lists the orchestrator URNs and nothing else; a job from anybody
  else is refused (`source_not_allowed`) and not run (see limits for what happens to it).
- The source polls only when the TUI is idle with nothing queued and no prompt open, so a job is taken one at a
  time. `claimsOnPoll` makes an item that cannot wait refused instead of held.
- Nothing answers a prompt for the session: the inbox never does. The profile gives Freebuff work, not permissions.
- The inbox rate limit (5 messages a minute, shared with files) applies.
- `autoResume` is off, so a restarted session does not replay a job on its own.
- The `done` sink sets `includeText: false`: the session transcript is never copied into the job result.
  Only outcome, attempt, model and completion time are reported.
- Model and cost guards (`allowedModels`, `maxCostMultiplier`) apply to every message.
- The file holds placeholders only: no address, no URN, no token.

## Submitting a job for Freebuff

- One task per job; a message is at most 64 KiB, and a long brief belongs in the repo, not in the job.
- Set `eligible_kinds` to `["freebuff"]`. The default of `scripts/submit_job.py` is `zeroclaw`.
- Use `depends_on` to chain steps; do not put two steps in one job.
- Put the model you want in `preferred_models`; it must pass the inbox model guard.
- The submitter must be an orchestrator that is in `allowFrom`. The `from` of a message is the submitter URN and the
  inbox takes at most 64 characters, so `kind` + `host` of the submitter must total 16 characters or fewer.
- Use `max_retries: 0` with a `zero_retry_reason` for work where a second run costs more than a failed one
  (see limits). Without it the bus retries a failed job twice.
- Pick a `docs:*` or other non-commit kind for work that produces no commits (see limits).

A task file for `scripts/submit_job.py` (a dry run by default; `--send` needs `HIVE_URL` and `HIVE_ACTOR_URN`):

```json
{
  "kind": "docs:freebuff-profile",
  "description": "Fix the broken links in docs/ and report the list of files changed. See job-template.md for the rules.",
  "eligible_kinds": ["freebuff"],
  "repo": "<org>/<repo>",
  "head_branch": "docs/fix-links",
  "base_branch": "main",
  "max_retries": 0,
  "zero_retry_reason": "one attempt only: a retry costs a session hour"
}
```

```
python3 scripts/submit_job.py task.json          # prints the payload
python3 scripts/submit_job.py task.json --send   # posts it
```

Write the description with `job-template.md`. Only give Freebuff work that is fine to send to a hosted service:
the hosted service may use the data for training.

## Reading the result

`GET /v1/jobs/<job-id>` returns `status`, `claimed_by` and `result`. For a Freebuff job `result` holds `outcome`,
`attempt`, `model`, `completedAt` and, after a release, `last_released_by`. `GET /v1/jobs?status=failed` lists the
failures; `include_result` is on by default. The transcript is not in it (see `includeText`).

## Known limits (found by running the profile on the real bus)

1. **A failure is retried by default.** With the default `max_retries` of 2 a `failed` update puts the job back to
   `queued` with a backoff; it ends `failed` only with `max_retries: 0` (which needs `zero_retry_reason`).
2. **Code kinds need commit evidence.** For a kind that is expected to commit, a `done` without `head_sha`,
   `commits`, `files_changed` and a clean worktree gets HTTP 409 `completion_evidence_missing` and the job is parked
   as `needs-review`. The profile's `done` sink reports no evidence, so use a non-commit kind such as `docs:*`, or
   add the evidence to the `result` map (this needs a field the inbox can fill).
3. **A second `done` replaces the first.** A repeat of `done` is accepted, but it re-runs the gate with the new
   result. If the agent reported evidence itself and the inbox then repeats `done` without it, a finished
   commit-gated job goes to `needs-review`. Report evidence once, from the last write.
4. **An auto-review job is claimable by the author.** The `needs-review` parking submits a review job without
   `eligible_kinds`; the same inbox can claim it and review its own work.
5. **No retry from the inbox.** After `queued`, a `queued` -> `running` update is refused with 409
   (`invalid status transition 'queued' -> 'running'`). Only a claim leaves `queued`.
6. **A released job is a duplicate for the same inbox.** The inbox dedupes by id forever, so a job released by
   `session_ended` or `not_accepted` and claimed again by the same inbox is `duplicate` and sits `claimed` until the
   bus orphan sweep (1800 s). Use a fresh session or inbox directory to rerun it.
7. **A job from a submitter that is not allowed bounces.** It is claimed, rejected (`source_not_allowed`) and released
   again, and release does not exclude the agent, so it comes back every backoff period. Remove or cancel such a job.
8. **A long submitter URN is never delivered.** See the 16-character rule above; the job is released with
   `from_invalid`.
9. **A late, different outcome on a terminal job is refused** with 409 (`invalid status transition`).
