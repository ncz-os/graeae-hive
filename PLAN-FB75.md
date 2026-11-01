# PLAN-FB75 — graeae-hive characterisation tests

Base: `deploy/integrate-20261017` @ `6ed42bb`. Branch `freebuff/fb75-20261030`.
Method (brief): three largest modules with the fewest test references; skip
`hive/workers/*` and `hive/doctor*.py` (three doctor branches are editing them).
Tests only — no production file is touched.

## Choosing the targets

Measured on this base, counting production modules >=200 lines and the number of
test files (root `test_*.py` plus `tests/`) that reference each module by name:

| module | lines | referencing test files |
|---|---|---|
| `deploy/hive_worker.py` | 7203 | 12 |
| `hive/persistence/sqlite.py` | 2425 | 53 |
| `hive/bus/routers/jobs.py` | 2144 | 93 |
| `hive/review_queue.py` | 1399 | 9 |
| `hive/tools/eih_probe.py` | 921 | **1** |
| `system_watcher.py` | 708 | **1** |
| `hive/tools/deepseek_spend_alert.py` | 689 | **1** |

The three largest modules that only ONE test file references are therefore
`eih_probe`, `system_watcher` and `deepseek_spend_alert`. The larger modules were
excluded by the brief's own rule (doctor/workers) or because they are already
covered by dozens of test files; `hive/review_queue.py` has 9 referencing files.

## Files

- `test_characterisation_fb75_20261031.py` — one new file, ~45 tests, three
  sections. Nothing else is created or edited.

## What is pinned (all values taken from probes against this base)

### `hive/tools/eih_probe.py`

- `is_banned_model`: DeepSeek/minimax/OpenAI/Anthropic/Google/xAI families are
  banned, case- and whitespace-insensitively; Nemotron and `z-ai/*` are not.
- `resolve_models`: the four permitted defaults when unset or blank, comma
  splitting with trimming, and `ConfigError` for a banned id.
- `prompt_class_for`: `medium` exactly on UTC hours 0/6/12/18, `tiny` otherwise,
  including the conversion of a non-UTC `now`.
- `build_medium_prompt`: byte-identical across calls, `MEDIUM_TASK` header,
  `# block 0000` markers, `target_tokens * 4` character floor.
- `prompt_for`: `medium` -> the expanded prompt, anything else -> the tiny prompt.
- `key_from_file` / `key_from_zoder_config` / `load_api_key`: quote stripping,
  comment and non-key lines ignored, `NVIDIA_API_KEY` accepted, env precedence,
  `EIH_KEY_FILE` override, and the `ConfigError` when nothing is found.
- `redact` / `error_excerpt`: explicit secret, `nvapi-` and `Bearer` patterns,
  whitespace collapse, and the 300-character cap ending in `…`.
- `classify`: the full outcome vocabulary — timeout, transport at no status,
  `404_unprovisioned` only when the body says "function not found", the four
  retryable statuses, `budget_exhausted` only for an empty body with
  `finish_reason == "length"`, and `http_<status>` for other errors.
- `should_retry`: exactly the retryable statuses plus timeout/transport.
- `make_row`: the row is exactly `ROW_FIELDS` in order, numbers default to null,
  `measured_at` is second-precision `...Z`, and a secret can never appear.
- `in_publish_window` (21:55 inclusive, 22:30 exclusive) and `load_gate`
  (strictly greater than the CPU count; unknown inputs never gate).
- `RowWriter`: filename by UTC date, 0600 file appended, `False` plus one log
  line at the size cap, and `prune` deleting only `eih-probe-*.jsonl` older than
  the retention window while ignoring junk names.

### `hive/tools/deepseek_spend_alert.py`

- `Config` defaults and normalisation (currency upper-cased, bus URL trimmed),
  `state_path` beside the snapshots, and the `ConfigError` paths for a
  non-numeric or non-positive ceiling and for non-numeric/non-positive
  thresholds; thresholds are sorted and de-duplicated.
- `parse_ts` (Z, naive-as-UTC, explicit offset), `fmt_ts`, `month_key`,
  `month_start`.
- `read_snapshots`: missing file -> `[]`; malformed JSON, missing fields,
  unparsable numbers and other currencies are skipped; timestamps normalised.
- `append_snapshot`: writes only `SNAPSHOT_FIELDS`, sorted, one line.
- `topups_between`: decreases are never negative top-ups, the month-start figure
  anchors the first step, out-of-window snapshots are ignored, result rounded.
- `month_start_total`: baseline when it names the month, else the first snapshot
  inside it, else the newest before it, else `ConfigError`.
- `compute_spend`: the arithmetic (`start + top-ups - current`), the clamp to
  zero, the percentage, and `ConfigError` when no snapshot prices it.
- `format_alert_message`: the exact operator-approved sentence.
- `thresholds_due`: reached-and-unsent thresholds, suppression within the month,
  and a stale month re-opening every threshold.
- `load_state` / `save_state`: round trip, corruption -> `{}`.

### `system_watcher.py`

- `_amd_entry_from_bytes`: APU (VRAM < 2 GiB and non-zero) folds GTT into
  `mem_*` and sets `unified_memory`; a discrete card keeps VRAM-only figures; a
  zero-VRAM card is NOT unified.
- `_parse_rocm_json`: name precedence, `%` and `W` suffixes, missing numeric
  fields defaulting to 0, non-dict keys skipped, temperature key matching.
- `_apu_mem_from_sysfs`: reads a fake sysfs tree and skips a card without the
  vram files.
- `capabilities_from_specs`: base tags, each tool/vendor tag, the RAM and CPU
  thresholds, Darwin and arm64.
- `_target_workers`: integer, bounded to at least 1, and never higher under a
  saturating load than at idle for the same hardware.
- `_find_bin`: PATH hit, extra-directory hit, and `None` when absent.

## Suspected defects — pinned as `xfail(strict=True)` and listed in the summary

1. **`eih_probe.classify` reports `ok` for a broken stream.** A transport failure
   during the SSE body (status already 200) falls through to the success branch,
   so a truncated probe with no content is recorded as `ok`. Only `timeout` is
   special-cased ahead of the status checks.
2. **`eih_probe.resolve_models` accepts an id that is not `vendor/name` shaped**
   even though its docstring promises `ConfigError` for exactly that case; only
   the banned-family patterns and emptiness are validated.
3. **`system_watcher.capabilities_from_specs` emits duplicate tags** — two AMD
   GPUs append `gpu`/`amd-gpu`/`rocm`/`vulkan-compute` twice.
4. **`system_watcher._find_bin` returns a non-executable file** from its extra
   directories (plain `os.path.exists`, unlike `shutil.which`), so a stray 0644
   file would be handed to `subprocess.run`.

## Verification

- `python -m pytest -q test_characterisation_fb75_20261031.py` and the full
  `pytest -q` suite, in one batched command, plus
  `ruff check test_characterisation_fb75_20261031.py`.
- `zoder --json review --scope branch --base origin/deploy/integrate-20261017
  -m nvidia/nemotron-3-ultra-550b-a55b`, then push and `git ls-remote`.

## Risks

- `_target_workers` prefers `hive.workers.fanout`; the assertions are therefore
  comparative (idle vs saturated), not exact counts, so a policy change does not
  break the characterisation for the wrong reason.
- The sysfs and rocm tests use temporary trees and literals only; nothing reads
  the real `/sys`, and no test makes a network call.
