# PLAN — FB44: error shape and pagination contract

Base: `origin/deploy/integrate-20261017` tip `7b4f29e`. Branch
`freebuff/fb44-20261030`.

## Surface

The app publishes 42 routes. `GET /` is HTML, `GET /v1/events` is an SSE stream
and `POST /mcp` is JSON-RPC, so those three cannot be probed with a refused
JSON request; each is either given its own shape test (`/health`, `/mcp`) or
declared exempt with the reason.

The contract every other route must keep:

- a route's refusal is `{"detail": "<sentence>"}`;
- the framework's own query/body validation is `{"detail": [{loc,msg,type,…}]}`;
- no body carries a traceback, a filesystem path, a SQL fragment or a driver
  message; `500` is not used for a client mistake.

## Files

| file | what it does |
| --- | --- |
| `test_error_shapes_20261030.py` | walks `app.openapi()`, builds a refused request per route from its own path template, and asserts the envelope + the leak needles on every 4xx. Plus one test per documented code (400/401/403/404/409/422) and the `/health` 503 document shape. |
| `test_pagination_20261030.py` | seeds 3 000 jobs, messages and events (3x the 1 000 max) directly through the repository, then checks defaults (absent/0/negative), the clamp at the max, `total` vs page size, prefix stability of `limit=N` inside `limit=M`, newest-first listing vs oldest-high-priority-first dispatch, the `since_id` cursor, non-integer limits, and that the generated reference documents every paged route with its real default. |
| `scripts/gen_api_reference.py` | a static "Pagination and errors" block appended to the generated document, so a regeneration keeps the section. Regenerated `docs/API_REFERENCE.md`. |

## Findings (pinned as `xfail(strict=True)`, not fixed)

1. `POST /mcp` answers `{"error": …}` for both 400 and 404 instead of the
   `{"detail": …}` every other route uses. Changing it is client-visible for
   MCP callers, so it needs a decision, not a drive-by edit.
2. `POST /mcp` wraps every handler in `except Exception: return {"error":
   str(e)}, 400`, so an internal fault is reported as a client mistake and its
   raw message (paths included) reaches the caller. Demonstrated by making a
   handler raise `RuntimeError("… /srv/agent-bus/bus.db is locked")`.

## Risks

- The schema walk's probes must be refusals, not 500s; a route whose probe
  faults would fail `test_every_error_body_uses_the_standard_detail_envelope`
  (which also asserts no 5xx).
- Seeding inserts only the columns the listing reads; the app's own insert path
  is unchanged.
- `docs/API_REFERENCE.md` is generated — edited via the generator, never by hand.
