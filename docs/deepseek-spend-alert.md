# DeepSeek month-to-date spend alert

The fleet's DeepSeek key is metered prepaid with an operator ceiling of **$100
per calendar month**. The only record of real money is the DeepSeek account
balance endpoint. This tool snapshots that balance hourly, computes
month-to-date spend, and posts an alert when spend crosses **70% ($70)** and
again at **90% ($90)**.

**Alert only — it never blocks or throttles API calls.** Hitting a threshold
sends a message to the Hive bus topic `fleet.alert`; nothing else changes. The
operator decides what to do.

- Tool: `hive/tools/deepseek_spend_alert.py` (stdlib only, no dependencies)
- Units: `deploy/systemd/deepseek-spend-alert.{service,timer}`
- Tests: `test_deepseek_spend_alert.py`

## How spend is computed

```
month-to-date spend = balance at the start of the calendar month (UTC)
                    + topped-up money added since then
                    - current total balance
```

- **Month start** is the balance at the first moment of the UTC month. In
  priority order the tool uses: the configured baseline *if it names that
  month*, else the first snapshot inside the month, else the newest snapshot
  from before the month (last month's closing balance).
- **Top-ups** are increases in `topped_up_balance` between consecutive
  snapshots. A decrease is never a negative top-up.
- **Grants are not spend offsets.** Burning `granted_balance` counts as spend;
  only real topped-up money offsets.
- October 2026 is a partial month, so its start balance is the configured
  baseline: `2026-10-25T23:30:00Z`, total `$252.11`, topped up `$240.71`.

## Environment variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `DEEPSEEK_API_KEY` | *(required)* | Bearer key, read at run time only. Never logged or written. |
| `DEEPSEEK_SNAPSHOT_PATH` | `~/.hive/deepseek-balance.jsonl` | Snapshot file (one JSON object per line). |
| `DEEPSEEK_MONTHLY_CEILING_USD` | `100` | Monthly ceiling used for the percentage. |
| `DEEPSEEK_ALERT_THRESHOLDS` | `70,90` | Comma-separated percentage thresholds. |
| `DEEPSEEK_BASELINE_TS` | `2026-10-25T23:30:00Z` | Month-start anchor timestamp. |
| `DEEPSEEK_BASELINE_TOTAL` | `252.11` | Balance at that anchor. |
| `DEEPSEEK_BASELINE_TOPPED_UP` | `240.71` | Topped-up balance at that anchor. |
| `DEEPSEEK_BASELINE_MONTH` | `2026-10` | The only month the baseline applies to. |
| `DEEPSEEK_CURRENCY` | `USD` | Currency filter for snapshots. |
| `HIVE_BUS_URL` | `http://192.0.2.67:5005` | Hive bus base URL. |
| `HIVE_SHARED_SECRET` | *(unset)* | Optional bus shared secret. |
| `HIVE_HOST_NAME` | short hostname | Host segment of the agent URN. |

Threshold bookkeeping lives in `deepseek-spend-alert-state.json` next to the
snapshot file, so each threshold fires **at most once per calendar month**.

## Install steps (for the orchestrator — not performed by the change)

1. Install the secret, mode 600, owned by the service user:

   ```sh
   sudo install -d -m 700 /etc/graeae-hive
   sudo install -m 600 -o jasonperlow /dev/null /etc/graeae-hive/deepseek-spend-alert.env
   # then add: DEEPSEEK_API_KEY=...
   sudo chmod 600 /etc/graeae-hive/deepseek-spend-alert.env
   ```

2. Install the units:

   ```sh
   sudo install -m 644 deploy/systemd/deepseek-spend-alert.service \
                        deploy/systemd/deepseek-spend-alert.timer /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now deepseek-spend-alert.timer
   ```

3. Sanity check once, without touching the bus:

   ```sh
   sudo systemctl start deepseek-spend-alert.service
   journalctl -u deepseek-spend-alert.service -n 20 --no-pager
   ```

   Or by hand from the repo root:
   `python3 -m hive.tools.deepseek_spend_alert --no-bus`

## Behaviour on failure

| Situation | Result |
| --- | --- |
| Balance fetch fails | Exit `1`, **no snapshot written**, no alert; next hour retries. |
| Bus unreachable | Exit `1`, snapshot still written, threshold **not** marked; next hour retries. |
| `DEEPSEEK_API_KEY` missing | Exit `2` with a clear config error; no request is made. |
| `--no-bus` | Snapshot + message, never contacts the bus, marks nothing, exits `0`. |
| `--dry-run` | Prints the message only; writes and sends nothing. |

## Bus agent identity

The tool registers its own agent URN each run using bus `kind` and `runtime` of
`human` — it is a read-only reporter acting for the operator, not a coding
worker, so the worker kinds (`claude`, `codex`, `opencode`, `zeroclaw`) would
misrepresent it in the agent list. The bus mints the URN and the tool uses what
it returns, then heartbeats once before posting.

## Dashboard

The bus dashboard (`static/dashboard.js`, `dashboard.html`) has **no existing
place for fleet alerts**, so per the task no dashboard line was added and no new
bus endpoints were introduced. Alerts are visible on the bus under the
`fleet.alert` topic.
