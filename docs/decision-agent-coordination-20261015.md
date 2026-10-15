# Agent awareness, communication & coordination

**Status:** implemented (adapter + worker loop); bus primitives pre-existed.
**Owner:** opencode · 2026-10-15

## What already existed (the bus)

| Need | Primitive |
|---|---|
| **Awareness** | `GET /v1/agents` (roster, filters), `GET /v1/hosts` (load/specs), `GET /v1/agents/stats` |
| **Communication** | `POST /v1/messages` (direct `to_urn` or broadcast `to_urn=null`), `GET /v1/messages` (poll; `to_urn` includes broadcasts) |
| **Coordination** | claim leases (`claimed_by` = who is doing what, no double-claim), `GET /v1/events` SSE, `GET /v1/jobs` (who holds what) |

The primitives were there; **workers did not use them.**

## What was added (harness/worker)

`hive/harness/generic.py` — worker-side bus protocol gains:
- `roster()` — list peers (awareness)
- `publish_message(from_urn, topic, payload, to_urn=None)` — send (broadcast = `to_urn=None`)
- `list_messages(to_urn, topic, since)` — poll

`hive/workers/run.py` — the loop now:
- `announce(topic, payload)` on startup (`worker.online`) and on demand
- `poll_messages()` every iteration; delivers each new message to an optional
  `on_message` handler; tracks a `since` cursor; **never raises** (a poll failure
  must not stop work)

## Coordination model

1. **No double-work** is already guaranteed by the claim lease, not by messaging —
   messaging is for *intent*, not mutual exclusion.
2. **Awareness** = `roster()` (who else is online, their capabilities/load).
3. **Communication** = topics (`worker.online`, `job.intent:*`, `budget.warn`, …);
   direct or broadcast.
4. **Coordination** = messages + the existing claim/lease + the capacity ledger:
   a worker announces intent, peers observe via `poll_messages`, and the bus's
   lease still arbitrates the actual claim.

## Not (yet) built — candidates if needed
- Topic conventions doc + a typed message schema.
- Advisory soft-locks ("claiming kind X on host Y") as messages.
- A `GET /v1/roster` summary grouped by role/capability (today: client-side over `/v1/agents`).
- Auto-intent messages from the claim path (`job.intent.claimed` on every claim).

## Refs
- `hive/harness/generic.py`, `hive/workers/run.py`
- `docs/decision-capacity-ledger-20261015.md` (capacity = a coordination input)
