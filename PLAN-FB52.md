# PLAN FB52: doctor chaos tests

Target: `hive/workers/doctor.py` (4987 lines). Base `deploy/integrate-20261017`, branch `freebuff/fb52-20261030`.

## Approach
- All bus I/O goes through `_http(method, path, body, timeout)`. Chaos tests monkeypatch `doctor._http` with a scripted fake bus (sequence of responses or exceptions per route), plus `doctor.STATE` swapped for an in-memory/tmp store, `DRY_RUN` off, `_urn` set, `time.sleep` no-op.
- Exercise the decision paths that act on rows: `sweep_orphan_failures`, `action_resubmit_jobs`, `patch_job`, `fetch_failed_jobs_for_kind`.

## Tests (new file `tests/test_doctor_chaos.py`)
1. 5xx burst on `/v1/jobs?status=failed` -> sweep returns error dict, no requeue/submit/patch, no exception.
2. Timeout (`_http` returns (0, None)) on list and on requeue -> no crash, base_kind not marked seen (retry next pass).
3. Partial / non-dict JSON (`resp` is a list or str, `jobs` is None) -> no crash.
4. Status race: row listed as failed, then requeue/PATCH returns 409 -> one action, not double; no duplicate POST replacement.
5. Double-act guard: same row appearing twice in listing -> only one requeue target.
6. Reviewer route unavailable -> loop_outcome decision path does not crash; row not requeued blindly.
7. One log line per skipped action (budget-exhausted held row logs warning).
8. Budget-spent replacement chain: no second replacement minted.

## Risks
- Module import side effects (env vars, state store path). Use monkeypatch and tmp_path.
- Test depends on `loop_outcome` / `broadcast_doctor_message` network: stub them.
- If a real crash or double action is found: write minimal reproducer, report as finding; do not change production code unless trivially safe.
- `hypothesis` not required; no new dependency.
