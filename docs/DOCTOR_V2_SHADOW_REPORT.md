# Doctor v2 shadow replay report (FB39)

Date: 2026-10-31. Harness: `scripts/doctor_replay.py`, cases
`scripts/doctor_replay_cases.json`, pins
`scripts/doctor_replay_allowlist.json`. No doctor code was changed; rerun
with `scripts/doctor_replay.py` (exit 0 = every disagreement is pinned and no
pin is stale).

Method: 21 synthetic cases cover every `loop_outcome` class
(`reviewer_unavailable`, `author_failed`, `timeout`, `crash`,
`exhausted_request_changes`, `nothing_to_commit`, `approved_unmerged`,
`killed_by_signal`, `clone_failed`, `empty_diff`, `unknown`) plus review-stuck,
143/kill, empty-diff and `loop_unresolved` shapes. Each case runs the v1 sweep
verdict (`loop_outcome.classify` + `decide`, offline) and the v2 path
(`doctor_v2.collect_evidence` + `decide_rules` against the shipped
`docs/doctor_v2_rules.toml`). Agreement means the same operational outcome
(`requeue` = `release_to_queue`; `needs_review` = `escalate`/`needs_review`;
`skip`/`parked` = `no_action`, or a human-owning action).

Result: 9 agreements, 12 pinned disagreements. The pattern is systematic, not
noise: v2 escalates almost every loop failure v1 would requeue.

## Findings (all pinned in the allowlist; operator decisions needed)

- **F1 — v2 evidence ignores `loop_result.teardown`.** Real loop rows carry
  only `loop_result` (see `tests/fixtures/loop_failed_rows.sample.json`):
  no top-level `failure_reason`, often no `error`. `collect_evidence` reads
  neither, so these rows present empty evidence (R5) or only the bs_audit
  signal (R10). Reviewer outages, author failures, timeouts, crashes,
  143/kills and transient 503s all escalate under v2 where v1 requeues.
  In enforce mode, automatic retries become human escalations. Either teach
  v2 `loop_result.teardown` or bless the escalation policy explicitly.
- **F2 — no reviewer-outage rule.** Even with `failure_reason` populated, no
  rule matches a reviewer outage, so v2 defers to the residual LLM while v1
  requeues. Decide: add a rule or bless the LLM fallback.
- **F3 — clone policy divergence.** V1 retries clone failures as transient;
  v2 R1 escalates immediately as a bad brief. Pick one policy.
- **F4 — approved-unmerged pages a human.** V1 skips (the merge, not the
  doctor, owns the wait); v2 R10 escalates. Confirm a merge wait should
  notify someone.
- **F5 — unknown rows escalate.** V1 keeps today's behaviour; v2 R10
  escalates. Confirm unknown shapes should page someone.
- **F6 — R7 owns `loop_unresolved` with failing tests.** V1 keeps today's
  behaviour; v2 escalates with test names. Confirm the routing.
- **F7 — v2 has no wait state.** V1 holds when the reviewer is down (retry
  later); v2 R5-escalates on empty evidence. Decide whether v2 needs a hold
  equivalent.

## Agreements worth keeping

- Budget-spent reviewer outages, exhausted request_changes, nothing-to-commit,
  local-only kills, empty diffs and stuck needs-review rows converge
  (human-owning on both sides).
- R8 (host OOM) and R2 (populated 502) agree with v1 requeues — the specific
  rules work when the evidence they match on is present, which is exactly
  what F1 is about.
