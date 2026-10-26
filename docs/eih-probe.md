# EIH standing probe — hourly latency/failure sample

**Status:** Proposed (2026-10-26) · **Owner:** orchestrator jasonperlow-84 ·
**Tool:** `hive/tools/eih_probe.py` · **Units:** `deploy/systemd/eih-probe.{service,timer}`

## Why

The fleet's belief that "after hours is better" on EIH (the NVIDIA inference
hub, `https://integrate.api.nvidia.com/v1`) rests on **one spot check**. The
usage ledgers hold no latency and no first-token time at all, so nobody can
say what an hour actually costs or how often it fails.

History mining (2026-10-26) found the failures are not one thing:

| Failure | What it looks like from the client |
|---|---|
| gateway outage | `503` |
| our own burst | `429` |
| reasoning model burning the budget before answering | `finish_reason=length`, empty visible answer |
| slow reply | client timeout |
| one anycast address unreachable from some hosts | transport error / connect timeout |
| model listed but not provisioned | `404` `Function not found` |

This probe measures all six, per model, per hour.

## What it measures

Permitted open-weight models only (fixed list in the file):

- `nvidia/nemotron-3-ultra-550b-a55b`
- `nvidia/nemotron-3-super-120b-a12b`
- `nvidia/nemotron-3.5-lightning-30b-a3b`
- `z-ai/glm-5.3`

`EIH_PROBE_MODELS` overrides the list, but an id matching a banned family
(`deepseek*`, `minimax*`, `openai/gpt*`, `anthropic/*`, `google/gemini*`,
`x-ai/grok*`) is rejected with a configuration error — EIH carries Nemotron and
other open-weight models; DeepSeek is direct-only.

Two prompt classes:

- **tiny** — every run. Fixed arithmetic prompt with a checked answer,
  `max_tokens` 512, streamed.
- **medium** — only when the UTC hour is divisible by 6. A ~8k-token
  review-style prompt generated deterministically from a fixed seed text,
  `max_tokens` 1024, streamed.

A reasoning model that spends the budget without emitting a visible answer
(`finish_reason=length`, empty content) is recorded as `budget_exhausted`, not
as a failure: the probe worked, the model answered with its thinking.

## How it is polite

- **Strictly sequential.** One request in flight, ever. One request per model
  per run, plus at most one retry (after 2 s) for `429`, `503`, `504`, `529`,
  a timeout, or a transport error. The retry is its own row with `attempt=2`.
- **Timeouts.** 60 s to first token, 300 s total, per request.
- **Host gate.** If the 1-minute load average exceeds the CPU count, no
  request is made: one row per model with outcome `skipped_load`. Between
  21:55 and 22:30 UTC (the publish window) nothing runs either: outcome
  `skipped_window`. The host load average is on every row.

## Output

Append-only JSONL, one object per line, under `$EIH_PROBE_DIR` (default
`~/Projects/eih-probe/`), file `eih-probe-YYYY-MM-DD.jsonl` by **UTC** date.
Directory mode `0700`, files mode `0600`. Files older than 30 days are deleted
on each run; a file is capped at 5 MB, after which that day stops accepting
rows and the cap is logged once. Never written under `/tmp`.

Fields (exactly these, in this order; missing numbers are `null`, never `0`):

```
measured_at  host  resolved_ip  resolved_ip_source  provider_id  model_id
prompt_class  probe_tool  attempt  latency_ms  first_token_ms  headers_ms
http_status  outcome  finish_reason  tokens_in  tokens_out  tokens_reasoning
host_load1  error_excerpt  tool_version
```

`resolved_ip` is the address actually connected to, read from the socket peer
(`sock.getpeername()`); if that is impossible it is the first `getaddrinfo`
answer and `resolved_ip_source` says `getaddrinfo` instead of `socket_peer`.

`outcome` is one of `ok`, `budget_exhausted`, `429`, `503`, `504`, `529`,
`404_unprovisioned`, `timeout`, `transport`, `http_<code>`, `skipped_load`,
`skipped_window`. An HTTP 404 whose body contains `Function not found` is
`404_unprovisioned` (the model is listed but not provisioned).

`error_excerpt` is the redacted, whitespace-collapsed, 300-character tail of
the failure: the API key, anything matching `nvapi-[A-Za-z0-9_-]+`, and any
`Bearer` token are replaced with `[REDACTED]` before truncation.

Exit code is `0` even when every probe fails — failures are data. Non-zero
means a configuration error (no key, a banned model id).

## The key

The tool reads the EIH key the way zoder is configured on a fleet host:

1. `EIH_API_KEY` in the environment;
2. the key file — `EIH_KEY_FILE` if set, otherwise `~/.zoder/secrets.env`
   (which `~/.zoder/shell-env.sh` sources, `KEY=value` lines) or
   `~/.zoder/nvapi.key`;
3. the `api_key` of the EIH block (`custom.eih_reviewer`, `custom.eih_glm`,
   `nvidia-eih`) in `~/.zoder/config.toml` / `config.fleet.toml`.

The key is never logged, never printed, and never written to a row.

## Running it

```sh
# what a run would do (no request, no row)
/usr/bin/python3 -m hive.tools.eih_probe --dry-run

# one real cycle
/usr/bin/python3 -m hive.tools.eih_probe
```

The timer is installed on a host by copying the pair and enabling the timer —
not part of this change:

```sh
mkdir -p ~/.config/systemd/user
cp deploy/systemd/eih-probe.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now eih-probe.timer
```

`Persistent=false` is deliberate: a missed hourly sample is worthless once the
host is back, and a catch-up burst would measure the wrong hour.

## Reading it

First token, not total latency, is what a review job feels:

```sh
python3 - <<'PY'
import json, glob
for line in open(sorted(glob.glob("/home/jasonperlow/Projects/eih-probe/eih-probe-*.jsonl"))[-1]):
    row = json.loads(line)
    print(row["measured_at"], row["model_id"], row["outcome"], row["first_token_ms"], row["latency_ms"])
PY
```

## Testing

`test_eih_probe.py` drives the tool through an injected fake transport — no
network, no real provider call — and covers every outcome class, the retry
row, both skip gates (asserting zero requests), the one-in-flight invariant,
banned model ids, key redaction when a fake server echoes the key back, the
exact row field set, `medium` only at `hour % 6 == 0`, rotation/size cap, and
file modes.
