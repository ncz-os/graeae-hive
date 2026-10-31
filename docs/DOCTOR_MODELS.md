# Doctor model chain (slot aware)

Operator rule 2026-10-31: the doctor's main model is gpt-oss (CERBERUS
`gpt-oss-20b`, llama-server `:8080`, 8 slots) but it can be overloaded — give it
backups (MiniMax-M3, DeepSeek, Muse, and qwen) and make the choice **slot
aware**: a busy endpoint is *skipped*, not dialled until the call fails.

Everything lives in `hive/workers/doctor_models.py`; `hive/workers/doctor.py`
has one call site in `invoke_doctor_agent`.

## Opting in

Set `DOCTOR_MODEL_CHAIN` to a TOML (or JSON) file:

```toml
reserve = 2          # llama-server slots the doctor never takes (default 2)
max_attempts = 4     # rungs one call may try (default 4)

[[rungs]]
id = "cerberus-gpt-oss"
model = "gpt-oss-20b"
base_url = "http://192.0.2.101:8080/v1"   # from the fleet inventory
kind = "llama"                                 # llama | vllm | openai
family = "openai"
cost_class = "A"
phases = ["triage", "review", "fix"]
max_concurrent = 8                             # the llama-server's slot count
# host/port (instead of a base_url) resolve the address from the fleet inventory
```

When `DOCTOR_MODEL_CHAIN` is **unset** `chain_from_env` returns `None` and the
legacy chain runs unchanged: one base per phase (`DOCTOR_<PHASE>_MODEL`,
`DOCTOR_<PHASE>_BASE_URL`, `DOCTOR_OPENWEIGHT_*`) plus the
`DOCTOR_FALLBACK_BASE_URLS` / `DOCTOR_FALLBACK_MODELS` rungs. There is no extra
probe traffic on that path.

## The default order

`default_rungs()` builds the operator's order (used when a deploy wants the
defaults without hand-writing the file):

1. `cerberus-gpt-oss` — `gpt-oss-20b`, CERBERUS llama-server `:8080`, 8 slots
2. `tydeus-qwen38` — `qwen38`, TYDEUS vLLM `:8006`
3. `minimax-m3` — `MiniMax-M3`, `https://api.minimax.io/v1`, quota guard `minimax`
4. `deepseek-flash` — `deepseek-flash`, `https://api.deepseek.com/v1`, guard `deepseek_peak`
5. `muse-spark` — Meta Muse, `muse-spark-1.3` (standard tier) or
   `muse-spark-1.3-contributor` on an operator-approved repo: graeae-hive,
   fleet-ops, tokenomics, ncz-screensavers, zoder

A local rung whose host the fleet inventory does not know gets an empty
`base_url` and is skipped — the address is never guessed.

## Slot awareness

| kind | probe | busy when |
| --- | --- | --- |
| `llama` | `GET <root>/slots` | `is_processing` count + doctor's own in-flight calls `>= slots - reserve` |
| `vllm` | `GET <root>/metrics` | `vllm:num_requests_running` + `num_requests_waiting` + in-flight `>= max_concurrent` |
| `openai` | none | quota guard / cool-down only |

`<root>` is the base URL with a trailing `/v1` removed. Probe results are
cached for 5 seconds per chain. A probe that cannot be read is **not** proof of
capacity: the rung is skipped (fail closed).

An **empty** `slots` list is UNKNOWN, not busy: llama-server reports every slot
it is configured with, so a body with no slots measures nothing (a server
between a restart and its allocation, say), and `all()` over an empty list is
vacuously true. `_llama_busy` reads it as `0/0 busy` (free) and the doctor's
slot-taken observer records no event for it — only a non-empty list whose
slots are all `is_processing` is the "free slot found taken" evidence the
restoration alert counts.

Hosted quota rules:

* `quota_guard = "minimax"` — a Token Plan 429 `usage limit reached (2056)` (or
  a `minimax-quota` run exiting 3, reported via `mark_quota_exhausted`) blocks
  M3 for the quota window.
* `quota_guard = "deepseek_peak"` — DeepSeek doubles its price 01:00–04:00 and
  06:00–10:00 UTC, Mon–Fri. Bulk work skips the rung in those windows;
  operator-blocking work (`operator_blocking=True`) may spend the peak rate.

The doctor's own in-flight calls per rung are counted by `ModelChain.lease`;
a rung at its cap is skipped and the next rung is considered.

A 429 / 503 / timeout / transport error is "busy": the rung cools down for 30s,
doubling on each further failure (capped at 15 minutes). Other 4xx statuses are
a config error: the rung cools down and the call stops (it would fail
everywhere). A reply that is empty, prose-only for a structured phase, or
truncated is a failure of that rung and the next rung is tried in the same
call. "Structured" means the reply parses to an object with a non-empty
`action_type` (`doctor.parse_doctor_response` alone accepts a bare truncated
object, so the chain checks for the field).

A busy rung's cached "busy" reading is re-probed on the next attempt, so a site
that answers (with a transport error, say) is retried before a colder rung.

## Selection

The first healthy, non-busy rung that lists the phase wins. On a REVIEW the
chain never picks a rung of the **author's** family (`review_queue.family_of`,
the same table `reviewer-roster.json` uses). Every decision logs one line on
`doctor.models`:

```
doctor.chain {"phase": "review", "rung": "deepseek-flash", "model": "deepseek-flash",
              "family": "deepseek", "skipped": ["cerberus-gpt-oss: busy 7/8 busy ..."]}
```

## Env and secrets the deploy must provide

Names only — no values ever appear in the repo or in the logs. A rung carries
the **name** of its key variable (`api_key_env`); a rung with a literal key is
rejected at load time.

| Name | Purpose |
| --- | --- |
| `DOCTOR_MODEL_CHAIN` | path to the TOML/JSON chain file (unset = legacy chain) |
| `MINIMAX_API_KEY` | MiniMax-M3 API key |
| `DEEPSEEK_API_KEY` | DeepSeek API key |
| `MUSE_BASE_URL` | Meta Muse API base URL |
| `MUSE_API_KEY` | Meta Muse API key |

The existing `DOCTOR_<PHASE>_MODEL`, `DOCTOR_<PHASE>_BASE_URL`,
`DOCTOR_OPENWEIGHT_*`, `DOCTOR_FALLBACK_BASE_URLS`, `DOCTOR_FALLBACK_MODELS` and
`DOCTOR_OPENWEIGHT_API_KEY` keep their meaning for the legacy (unset) path.
