# Decision: hive communication planes — MCP vs A2A vs internal REST

**Status:** Accepted (2026-10-15)
**Owner:** opencode (operator-directed)
**Decision record:** `mem_1792100370499_37349d` (session), this doc
**Supersedes:** none

---

## Question

Should the hive adopt a current agent-interop standard — A2A (Agent2Agent,
Linux Foundation) — as the *basis* of hive communication?

## Context

The fleet already runs three overlapping communication surfaces:

1. **Custom REST bus** — `agent_bus.py` (`/v1/agents/register`, `/v1/agents/heartbeat`,
   `/v1/jobs/next`, `PATCH /v1/jobs/{id}`, `/v1/events` SSE). Fleet-internal.
2. **MCP bridge** — `mcp_bridge.py` (SSE `:5006`), 15 tools spanning **hive +
   MNEMOS + GRAEAE**:
   - hive coordination: `hive_job_create`, `hive_job_next` (claim), `hive_job_update`,
     `hive_agent_register`, `hive_agent_list`, `hive_message_publish`, `hive_message_list`
   - MNEMOS: `mnemos_memory_search` / `_create` / `_get`
   - GRAEAE: `graeae_consult` / `_muses` / `_modes` / `_get`
   - auth scopes: `bridge:use` (client), `bridge:admin` (register-class ops)
3. **MNEMOS** — has its own MCP interface *and* is re-exposed via the bridge.

Key observation: **MCP already exposes the hive's coordination verbs** (`hive_job_next`
= claim, `hive_job_update` = complete/fail). MCP is therefore not only the tool
plane — it is the live external coordination plane today.

## Decision

**Do not make A2A the basis. Keep MCP as the primary universal plane; add A2A
only as an additive external *delegation facade* over the same job core.**

Three planes, one core:

| Plane | Who | Protocol | Status |
|---|---|---|---|
| Internal | fleet workers ↔ bus | custom REST (bus core) | live — keep |
| Universal external | MCP-capable agents/humans | **MCP** (tools + coordination) | live — keep, dedup |
| Delegation facade | foreign A2A agents | **A2A** (AgentCard + JSON-RPC Tasks) | **deferred → Phase F** |

Both MCP and A2A front the **same hive job core** (register/next/update). No
third bespoke coordination protocol.

## Rationale

- **MCP covers coordination mechanically today.** A2A is not required for the
  fleet to function. Adding it now is a standards/ledger win, not a capability win.
- **A2A fills what MCP does *not* model:** Agent Cards (identity+capability+auth
  discovery), a formal **Task** object with lifecycle
  (submitted/working/input-required/completed/failed/canceled), task resume/stream
  (`message/stream`, `tasks/resubscribe`), and ingress for **non-MCP** frameworks
  (ADK/LangGraph/BeeAI/CrewAI). That is the real reason to add it — external interop.
- **The hive core is a policy engine A2A does not model:** global dedup
  (`dedup_hash`), claim leases + reaper, MiniMax/OAuth cost caps, capability match,
  host denylist, dependency resolution, completion-proof gate. These stay in the core.
- **`hive_job_next` as an MCP tool is a mild shoehorn** — MCP is stateless
  request/response; a job claim is a *leased* mutation. It works, but A2A's Task
  model is the more honest long-term face for delegation.
- **Complementary, not competing:** A2A explicitly positions itself as complementary
  to MCP (agent↔agent vs agent↔tools). "Both" is the endorsed posture.

## Mapping (A2A façade ← hive)

| Hive | A2A | MCP (today) |
|---|---|---|
| `AgentRegister` (kind/caps/cost_tier/auth) | **AgentCard** `/.well-known/agent-card.json` | `hive_agent_register` |
| capabilities | card `skills` | tool list |
| Job | **Task** | object returned by `hive_job_next` |
| job status queued/claimed/running/done/failed/cancelled | **TaskState** | (opaque) |
| `/v1/events` SSE | `message/stream` / `tasks/resubscribe` | SSE transport |
| heartbeat / job update | push notification | `hive_job_update` |
| `/v1/jobs/next` | `message/send` + `tasks/get` | `hive_job_next` |

Gap: A2A has no *global queue/scheduler* noun → **the bus exposes ONE AgentCard and
fans tasks to workers behind it**; workers are not individual A2A servers.

## Not doing

- Not replacing the internal REST core with A2A (loses fleet semantics + latency).
- Not replacing MCP (it is live, in MNEMOS, and already covers coordination).
- Not force-converging the `github/gitlab` security branch (separate, needs human review).

## Action items

1. **A2A façade = Phase F** — implement only when a non-MCP A2A agent must join.
   Version-pin `a2a-sdk`; do a spec-level pass (not README-level) first.
2. **MCP dedup (do now, cheap):** the fleet has duplicated MCP surfaces —
   `mnemos_memory_*` via the hive bridge *and* MNEMOS's own MCP endpoint. Pick one
   gateway (the bridge) or document the split so clients don't drift.
3. **Auth consolidation (do now):** two auth models coexist (`bridge:use`/`bridge:admin`
   scopes vs bus bearer/admin tiers) — same "two sources of truth" the refactor flagged.
   Define the boundary explicitly.
4. **Optional:** adopt a CloudEvents envelope for `/v1/events` (cheap interop win).

## Refs

- `docs/refactor-proposal-harness-agnostic-20261015.md` (harness adapter plane)
- A2A: <https://github.com/a2aproject/A2A> — Linux Foundation, Apache-2.0;
  JSON-RPC 2.0 over HTTP(S), Agent Cards, SSE, push notifications
- MCP: hive `mcp_bridge.py` (`:5006`, 15 tools); MNEMOS MCP interface
