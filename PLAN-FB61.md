# PLAN-FB61 — Doctor v2 rule unit tests

## Brief
`hive/workers/doctor_v2.py`: one focused unit test per rule (positive,
negative, boundary), plus a test that every rule has a documented id and a
docstring. Coverage before/after.

## Recon
- Rules live in `docs/doctor_v2_rules.toml` (R1..R10), loaded via `load_rules`;
  first-match in `decide_rules`, retry guard via `DOCTOR_MAX_SAME_SIGNATURE_RESUBMITS` (default 2).
- `match_all` only on R6/R7; all others are any-match (one regex suffices).
- `collect_evidence` auto-appends a `bs_audit:` line for non-ok audits — this
  HIDES R5 (`evidence_regex ^$` needs truly empty evidence_text) and can
  trigger R10. Test helper must use non-mandatory kinds for R5.
- Existing `tests/test_doctor_v2_rules.py` (10 tests, green): R1-R4/R6-R10
  positives, retry-guard, env cap, R9 terminal, LLM quote, normalize, bs_audit+R10,
  sweep, shadow_report. Gaps: R5 (no test at all), negatives per rule,
  per-rule max_retries boundaries, first-match ordering, rule-doc completeness,
  `validate_llm_decision` branches, `residual_decision`, `shadow_compare`,
  `sweep_needs_review` branches, `_error_class` classes.
- No pytest-cov/coverage pkg; Python 3.14 → use `sys.monitoring` line
  coverage of `hive/workers/doctor_v2.py` before (existing file) vs after
  (existing + new). Report as line coverage with the branch caveat.

## Files
- New: `test_doctor_v2_rule_units_20261031.py` (repo root, beside the other
  `test_*_20261030.py` files — no new deps, stdlib + pytest only).
- No production code changes expected. If a rule behaves nonsensically,
  pin as xfail(strict) + finding, do not touch rules.

## Tests (~40)
1. `test_every_rule_has_id_note_and_valid_shape` — ids unique/ordered R1..R10,
   every rule has non-empty id/action/note, action in the allowed set,
   retry_scope valid, max_retries >= 0; every public function used has a docstring.
2. Per rule R1..R10: positive / negative / boundary:
   - R1: reason match; evidence-only match; case-insensitive; unrelated → not R1.
   - R2: 429/503/dns/connect-timeout; "timed out" alone does NOT match;
     max_retries min(3, cap 2) = 2; ordering beats R6 on overlap.
   - R3: all three reasons escalate with 0; text-only does not trigger.
   - R4: release same_host max 1; 1 prior still releases, 2 priors escalate.
   - R5: truly-empty evidence (kind ops, no keys) escalates; one char → skip R5.
   - R6: needs BOTH halves (each half alone skips); other_host, max 1.
   - R7: needs BOTH halves; failing-tests text without loop_unresolved skips;
     loop_unresolved + green output matches NO rule (None).
   - R8: each host-fault phrase; other_host, max 2.
   - R9: reason-only and evidence-only each match (any-match); host-terminal
     reason text; quote verbatim; never retried.
   - R10: auto bs_audit line triggers; ok-committed job skips R10.
3. Guard/env: invalid env → default 2; retry_cap=0 forces escalate on release
   rules; different-signature priors don't count.
4. `validate_llm_decision`: bad action, missing quote, non-substring quote,
   clamp 99 → cap, release-with-zero-permitted → escalate, used_retries consume.
5. `residual_decision`: raising model → escalate with latency; ok model passthrough.
6. `bs_audit_signal` branches, `_error_class` classes, `shadow_compare`,
   `sweep_needs_review` (recent excluded, stale/actionable/superseded,
   report-only), `default_evidence_summary` variants.

## Risks
- R5 positive is fragile if `collect_evidence` gains new auto-lines; the test
  asserts the empty-text precondition explicitly so a future change fails loudly.
- `sys.monitoring` line coverage only — state the branch-coverage caveat.
