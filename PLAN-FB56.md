# PLAN — FB56: authorization matrix (graeae-hive)

Base: `origin/deploy/integrate-20261017` tip `f50f9d7`. Branch `freebuff/fb56-20261030`.

## Survey (verified against this base)

Declared dependency vs effective gate:

- `require_mutation_actor` (X-Hive-Actor = registered ACTIVE agent + shared
  secret if set; `deps.py`): the 5 mutation routes that declare it behave
  exactly that — BUT the three `/v1/admin/*` routes + both purge routes imply
  admin-only while `is_admin_tier` (the bearer-token admin gate, used only by
  deregister) is never consulted. Any active worker can requeue/reclassify/
  purge. → FB56-4.
- `POST /v1/agents/{urn}/plan-reset` (`agents.py:486`): zero auth — anyone
  zeroes anyone's MTD spend, defeating the subscription throttle. → FB56-1.
- `POST /v1/jobs/{id}/auto-retry` (`jobs.py:1896`): zero auth — anyone can
  consume a job's retry budget / bounce failed→queued. → FB56-2.
- `POST /v1/jobs` (submit): caller anonymous; authorization rides entirely on
  body `submitter_urn` being a registered orchestrator-runtime agent
  (`require_orchestrator_submitter`) — no proof of possession, so anyone can
  submit AS any orchestrator. → FB56-3 (design observation, needs operator call).
- `POST /v1/jobs/{id}/release`: no caller check (uses `active_orchestrator_urn`
  as submitter internally). Anonymous trigger of release-job creation. → FB56-5.
- Claimant-gated PATCH, self-or-admin deregister, from_urn-gated message
  publish, registered+active dequeue/claim all match their declarations.

## Files

- `docs/AUTH_MATRIX.md` (new): per-route table (method, path, declared
  dependency, effective gate, who may call, test name) + findings FB56-1..5.
  Machine-readable section delimited by
  `<!-- FB56-MATRIX-START -->` / `<!-- FB56-MATRIX-END -->` markers
  (`METHOD | PATH | GATE` rows) that the test parses for completeness in both
  directions (no undocumented route, no stale entry).
- `test_auth_matrix_20261031.py` (new, repo root): parses the matrix from the
  doc AND the route list from `app.openapi()`, asserts the two sets are equal,
  then asserts each cell live via ASGITransport in-process harness
  (anonymous 200s, 401-no-actor, 403-wrong-principal, 409-unregistered,
  worker-as-orchestrator 403, any-worker-admin-op 200). Reuses the
  `test_api_contract_20261029` harness pattern (temp DB, `_deps._HIVE_REPO=None`).
- No `pyproject.toml` change: stdlib + httpx + pytest only.

## Risks

- 42 routes × live calls in one class: share one temp-DB harness (setUpClass),
  unique ids per test; register orchestrator + worker once.
- `plan-reset`/`auto-retry` anonymous-200 assertions pin CURRENT (vulnerable)
  behaviour deliberately — they are the regression tripwire for the future fix,
  marked with the finding id in the test name/docstring.
- Full suite ~150 s; run file first, then full suite once.
