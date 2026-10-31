# PLAN — FB60: event stream endpoints (graeae-hive)

Base: `origin/deploy/integrate-20261017` tip `18f65cc`. Branch `freebuff/fb60-20261030`.

## Mechanics (verified against this base, `hive/bus/routers/events.py`)

- `GET /v1/events` (SSE): 503 when `len(EVENT_QUEUE) >= MAX_EVENT_SUBSCRIBERS`
  (100). Catch-up phase (only when `since_id` given): one `tail_events`
  (`id > since_id ORDER BY id ASC LIMIT 1000`) yielding `{id, data}` frames.
  Live phase: `request.is_disconnected()` → break; `queue.get()` with 15 s
  timeout → `{data}` frame (NO id — live resume via last-event-id is NOT
  supported); TimeoutError → `{event: ping}` when due; `finally` pops the sub.
- Fan-out drops on full queues: `deps.emit_event` uses
  `suppress(QueueFull)` + `put_nowait` — a slow consumer loses events, never
  blocks the publisher (found in code; pinned by test).
- `GET /v1/events/log`: `tail_events(since_id, clamp_limit(limit))`,
  `last_id` = last row id or the passed since_id.

## Tests (`test_event_streams_20261031.py`, in-process ASGITransport, temp DB)

Seed events via `deps.emit_event` on a repo tx (real `id` autoincrement).
httpx `client.stream` + frame parser (split `\n\n`, id:/data:/event: lines):

1. **ordering**: 5 seeded rows, `since_id=0` → first 5 frames have ids 1..5 in order, then cancel.
2. **reconnect**: `since_id=3` → first frame id 4; `/log` `last_id` semantics; document that LIVE frames carry no id (resume covers DB catch-up only).
3. **slow consumer**: fill a subscriber queue to maxsize(1000), emit → no block/exception, still 1000 (drop, not backpressure).
4. **disconnect cleanup**: open stream, read catch-up, aclose → poll until sub gone from EVENT_QUEUE (timeout 5 s); assert `asyncio.all_tasks()` count returns to baseline (no leaked tasks) — same assertion covers "no leaked tasks".
5. **subscriber cap**: 100 dummy queues → 503; cleanup restores 200.
6. **log endpoint**: since_id filter, limit clamp (0/negative→default, huge→max 1000), empty → `last_id == since_id`.

## Risks

- Infinite live loop: every stream read is wrapped in `asyncio.wait_for`;
  cancel via exiting the `client.stream` context (aclose).
- `is_disconnected`/aclose propagation under ASGITransport is assumed; if the
  sub does not pop on aclose, fall back to asserting pop-on-generator-close
  via `arequest`... first try the direct approach and read the failure.
- EVENT_QUEUE is global: tests use unique sub flows and a final
  `EVENT_QUEUE.clear()` in cleanup with no live streams attached.
- Full suite ~150 s; file first, then full suite once.
