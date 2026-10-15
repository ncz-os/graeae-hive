# Refactor: graeae-hive-mind → harness-agnostic

**Status:** Phase A scaffold (2026-10-15)
**Author:** opencode (per CLAUDE.md "opencode is the fleet's backup harness to zoder")
**Reviewer (target):** zoder
**Decision record:** `mem_1792097699627_c0b529`

---

## Goal

Make graeae-hive-mind flexible for use with **any** agent harness, with these four as first-class priorities (in order):

1. **zoder** — long-range coding tool, fleet's primary author + reviewer
2. **opencode** — fleet's backup harness, runs on every zoder host
3. **codex** — secondary/escalation only, **NEVER auto-run** (operator 2026-10-10)
4. **claude** — opencode's MCP-hosted LLM + Claude Code desktop

---

## Why now

The bus currently hardcodes assumptions:

- `worker.py` routes `kind="code"` → `zc_native_build` (hardcoded)
- `agent_bus.py` has two transport-auth patterns (`require_mutation_actor` from 14ca985 + `_load_token_set` from 89f6181) — two sources of truth
- Auto-commit daemon (`/opt/hive-auto-commit.sh`) is hive-specific
- 89f6181 cherry-pick verdict (per DeepSeek 2026-10-15): blind cherry-pick would erase 14ca985 audit fixes. Re-author as delta.

A harness-agnostic core means: any harness can register, claim, execute, report. The bus does not know or care about the implementing engine.

---

## Target architecture

```
┌────────────────────────────────────────────────────────────────┐
│ agent_bus.py (FastAPI orchestrator — thin)                      │
│   /v1/agents  /v1/jobs  /v1/events                              │
│   claim/submit work on capability match, not kind               │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│ hive/harness/         (adapter registry, pluggable)             │
│   ├── __init__.py                                                │
│   ├── base.py         HarnessAdapter Protocol                   │
│   ├── registry.py     load adapter by harness_kind              │
│   ├── zoder.py        priority=0 — ZoderAdapter (zeroclaw)       │
│   ├── opencode.py     priority=1 — OpenCodeAdapter (MCP+zcn)     │
│   ├── codex.py        priority=2 — CodexAdapter (CLI, gated)    │
│   ├── claude.py       priority=3 — ClaudeAdapter (OAuth, desktp) │
│   └── generic.py      fallback for any harness                 │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│ hive/auth/           (per-harness auth, pluggable)              │
│   bearer    default, replaces require_mutation_actor              │
│   oauth     claude (Claude Pro / OAuth subscription)             │
│   api_key   codex, zoder (metered providers)                     │
│   none      local zoder (no remote call)                         │
└────────────────────────────────────────────────────────────────┘
```

---

## Sequence (5 phases)

### Phase A — Interface (non-breaking)

> **Scope of this branch:** only A1–A3 land here. **A4 and A5 are PENDING**
> — the `agents` table is *not* yet migrated with `harness_kind` /
> `harness_version`, and `/v1/agents` does not yet filter on them. Nothing in
> this branch wires the adapter registry into live dispatch, so the scaffold
> is inert until Phase B/D. Do not read the ✓ on A2/A3 as "harness support is
> live".

- **A1:** `wip/hive-eff/2026-10-15` branch (this branch) ✓
- **A2:** `hive/harness/__init__.py` + `base.py` + `registry.py` ✓
- **A3:** `docs/refactor-proposal-harness-agnostic-20261015.md` ✓ (this file)
- **A4:** Schema migration: add `harness_kind` + `harness_version` to `agents` table ⧗ PENDING
  - `ALTER TABLE agents ADD COLUMN harness_kind TEXT DEFAULT 'opencode'`
  - `ALTER TABLE agents ADD COLUMN harness_version TEXT DEFAULT 'unknown'`
  - backfill existing `kind='zeroclaw'` rows → `harness_kind='opencode'`
- **A5:** `harness_kind` filter on `/v1/agents` and dashboard ⧗ PENDING

### Phase B — Adapters (zoder + opencode first; codex + claude gated)

- **B1:** `hive/harness/zoder.py` — ZoderAdapter (zeroclaw engine, see CLAUDE.md fleet zoder config 2026-10-01)
- **B2:** `hive/harness/opencode.py` — OpenCodeAdapter (MCP + zc_native_build, fleet-config paths)
- **B3:** `hive/harness/codex.py` — CodexAdapter (CLI, gated by `HARNESS_CODEX_AUTORUN=0`)
- **B4:** `hive/harness/claude.py` — ClaudeAdapter (OAuth, Claude Pro subscription)
- **B5:** `hive/harness/generic.py` — GenericAdapter (any harness that speaks the protocol)

### Phase C — Auth plugins

- **C1:** `hive/auth/bearer.py` — replaces `require_mutation_actor` from 14ca985
- **C2:** `hive/auth/oauth.py` — Claude Pro / subscription
- **C3:** `hive/auth/api_key.py` — Codex / metered providers
- **C4:** `hive/auth/none.py` — local zoder (no remote auth)
- **C5:** `HIVE_HARNESS_AUTH_<KIND>` env config + auto-detection from `Authorization` header

### Phase D — Worker dispatch

- **D1:** Replace hardcoded `if kind == "code": zc_native_build` in `worker.py` with adapter dispatch
- **D2:** Job spec gains `required_harness_capabilities: list[str]` + `harness_priority: list[str]` (default `[zoder, opencode, codex, claude]`)
- **D3:** Claim eligibility filters by capability match + priority ranking
- **D4:** Auto-retry endpoint (item 1) re-authored per DeepSeek strategy (NOT blind cherry-pick of 89f6181)

### Phase E — Tests + observability

- **E1:** Per-adapter unit tests (mock transport)
- **E2:** Harness-agnostic claim/execute e2e test
- **E3:** Dashboard filter by `harness_kind` + success-rate metric
- **E4:** Adversarial review checklist in CI
- **E5:** Fix 41/58 test failures from `test_defect_fixes.py` run 2026-10-15 (alignment: `EnvironmentFile=/etc/graeae-hive/auto-restore.env`, etc.)

---

## Risks (must mitigate before merge)

| ID | Risk | Mitigation |
|----|------|------------|
| C1 | Auth split = privilege escalation class | zoder review **mandatory** before merge. Regression test for token revocation + session signing. |
| C2 | Drop `require_mutation_actor` duplicates both auth surfaces | All callers refactored in single PR. Snapshot tests of `Authorization` header handling. |
| B3 | Codex accidental auto-run violates operator policy | `HARNESS_CODEX_AUTORUN=0` default. Unit test asserts `codex` excluded from auto-route list when env unset. |
| D1 | Missed hardcoded kind routing case = silent regression | Grep for `kind ==` + `kind=` in worker.py. Snapshot test of dispatch table. |
| D4 | Auto-retry re-author touches 14ca985 reaper fixes | Re-run audit regression suite after W12. Same regression test list as 14ca985 closes. |
| A4 | Schema migration on live DB | Migration script idempotent (`ADD COLUMN IF NOT EXISTS`). Run on PYTHIA with backup. |

---

## Process per CLAUDE.md

- MNEMOS decision record: `mem_1792097699627_c0b529` ✓
- Versioned doc on wip branch: this file ✓
- zoder review per phase before merge (no auto-fire codex per CLAUDE.md 2026-10-10)
- One worktree per phase (this is phase A's worktree)
- Push wip to argonas after each phase

---

## Operator directive: Spark remote-agent removal (2026-10-15)

**This section exists because commit `3c0474a`'s message did not name the
Spark removal** — the change rode inside a "dead-code cleanup + ruff lint pass"
commit and both the pre-merge and post-merge review flagged it as looking
accidental. It was not accidental. It was operator-directed on 2026-10-15.

Directive: the **remote Spark agent takeover path is removed from the hive**.
The GB10 Spark is not integrated/tested with the fleet, and the OAuth-capped
"reserve the queue for Spark" behaviour was starving real work. Removed surface
(textual — verified with `git log -S`, so any re-introduction shows in blame):

- `SPARK_*` module constants + `note_spark_seen*` take-over bookkeeping
- `_job_queued_age` queue-age probe used only by the Spark reservation
- `QUARANTINED_HOSTS` admission gate
- admission `proof` fields `remote_verified` / `remote_sha`
- dashboard Spark panel
- `dashboard.html` `MODEL_NAMES` map (Spark provider display names)

### Consequence for `valid_completion_proof` (agent_bus.py)

`valid_completion_proof()` no longer checks `remote_verified` / `remote_sha`.
Local Git evidence (`head_sha` + `commits` + `files_changed` + `worktree_clean`)
is now the **sole** completion gate. This is correct under the harness-agnostic
direction: completion is proven at the *bus* (local git state), not by a
remote-agent attestation that only existed to serve the Spark path. The
function still **fails closed** — see its docstring; the removal widened
*which* proofs are accepted, it did not weaken the reject path.

---

## Cross-references

- CLAUDE.md — fleet zoder config 2026-10-01 (coder=qwen38 TYDEUS :8006, reviewer=gemma4-31b CERBERUS :8080, backup=nemotron-3-ultra-550b EIH)
- AGENTS.md — three-remote git pattern (argonas + github + gitlab)
- mem_1792097373904_3787b3 — graeae-hive session state 2026-10-15 (the audit work this refactor builds on)
- DeepSeek consult 2026-10-15: 89f6181 cherry-pick verdict (re-author as delta)
- Auto-commit daemon installed 2026-10-15: `/opt/hive-auto-commit.sh` (push HEAD:master, no `2>/dev/null` on staging, richer template)

---

## Open questions for review

1. Should `harness_priority` be a per-job field, or fleet-wide config? (Fleet-wide = simpler; per-job = flexible but more knobs)
2. Should the CodexAdapter refuse to register when `HARNESS_CODEX_AUTORUN=0`, or accept registration but mark `enabled=False`? (Refuse = cleaner; mark = operators can debug)
3. Where does `agent_bus.py`'s split (Phase D) land? Single PR, or one PR per router file?
4. Does the auth plugin need to support per-endpoint granularity (admin vs worker tier)? Currently `require_mutation_actor` is per-endpoint; new plugin is per-harness.