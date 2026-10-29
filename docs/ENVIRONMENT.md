# Environment variables operators must set

Operator rule (2026-10-29): **an address comes from the environment or a fleet
inventory, never from source.** A baked-in address goes stale without anyone
noticing (ULTRA, MEDUSA and PROTEUS in this tree were all wrong) and every clone
of the repo carries it.

Nothing in this list has a baked-in default any more. Where a variable is unset,
the feature it configures is **disabled with one log line** and the code takes its
existing "unavailable" path; it does not crash and it does not guess.

The doctor is the service most affected: deploy it **with these variables set**,
or the model calls and bus features it needs are off.

## The bus

| Variable | Meaning | Example |
|---|---|---|
| `HIVE_URL` | Base URL of the graeae-hive bus. Required by the doctor, `deploy/hive_worker.py`, the system watcher, `hive-auto-restore.py` and `bs_audit.py`. | `http://<bus-host>:5005` |
| `HIVE_BUS_TOKEN` | Optional worker-tier bearer token. | — |
| `HIVE_BUS_ADMIN_TOKEN` | Optional admin-tier bearer token; the doctor needs it to register as an orchestrator and to call `/v1/admin/*`. | — |

Unset `HIVE_URL`: the doctor warns once and its bus calls return "no answer"; a
worker refuses to poll and exits `2`; the watcher skips bus calls; `bs_audit.py`
refuses to run.

## Fleet addresses

| Variable | Meaning | Example |
|---|---|---|
| `HIVE_FLEET_INVENTORY` | Path to the fleet inventory JSON. **No default path**, because a default path would itself carry an address. Shape: [`docs/fleet-inventory.example.json`](fleet-inventory.example.json) (TEST-NET addresses only — copy it outside the repo and fill in the real ones). | `/etc/graeae/fleet-inventory.json` |
| `ARGONAS_SSH_HOST` | ARGONAS hostname or address, used to build `ssh://` clone URLs and to run ARGONAS discovery. | `<argonas-host>` |
| `ARGONAS_SSH_PASS` | Password for the ARGONAS discovery user (`root`). Discovery only. | — |

The inventory carries `address`, `user`, `auth` (`key`\|`password`), `os`
(`linux`\|`macos`) and, for a password host, `password_env`. It is read by:

- `hive/core/fleet_inventory.py` — the one loader (missing, malformed and partial
  files all degrade: no addresses is not a crash).
- `hive/workers/doctor.py` — the full fleet table used by the token-fix path.
- `hive/workers/doctor_gates.py` — the restart allowlist. `cixmini` is the one
  static entry, because its address and sudo user are their own variables
  (`CIXMINI_IP`, `DOCTOR_CIXMINI_SUDO_USER`, `CIXMINI_SSH_PASSWORD`).

Unset `HIVE_FLEET_INVENTORY`: no host is known, so a restart is refused **by
name** and token-mismatch auto-fix falls through to the LLM instead of SSHing to a
guess. Editing the inventory takes effect on the next doctor start.

## Doctor model endpoints (all required)

Every phase base is env-only. There is no CERBERUS/TYDEUS default: which host
serves which model is exactly the sort of address that goes stale in source.

| Variable | Meaning | Example |
|---|---|---|
| `DOCTOR_OPENWEIGHT_BASE_URL` | OpenAI-compatible base for the triage/review phases and any unknown phase. | `http://<cerberus-host>:8080/v1` |
| `DOCTOR_TRIAGE_BASE_URL` | Triage phase base. Defaults to `DOCTOR_OPENWEIGHT_BASE_URL`. | `http://<hydra-host>:8080/v1` |
| `DOCTOR_REVIEW_BASE_URL` | Review phase base. Defaults to `DOCTOR_OPENWEIGHT_BASE_URL`. | `http://<cerberus-host>:8080/v1` |
| `DOCTOR_FIX_BASE_URL` | Fix phase base (where `qwen38` is served). | `http://<tydeus-host>:8006/v1` |
| `DOCTOR_FALLBACK_BASE_URLS` | Comma list of local llama-server bases tried after the phase base, in order. **No default rungs.** | `http://<hydra-host>:8080/v1,http://<cerberus-host>:8080/v1` |
| `DOCTOR_FALLBACK_MODELS` | Parallel list of model ids, one per fallback base. Keeps its non-addressdefault (`gemma4-e2b,gemma4-e2b,gemma4-31b`); it does nothing with no bases. | `gemma4-e2b,gemma4-31b` |
| `DOCTOR_OPENWEIGHT_MODEL` | Model id for the open-weight path. | `gpt-oss-20b` |
| `DOCTOR_OPENWEIGHT_API_KEY` | Key for the open-weight path, when the server wants one. | — |
| `DOCTOR_REVIEWER_HEALTH_URL` | Optional bounded probe used by the loop sweep before an automatic requeue. Unset means "assume healthy"; the requeue cap still applies. | `http://<cerberus-host>:8080/health` |

Unset `DOCTOR_OPENWEIGHT_BASE_URL` (and no phase override): `invoke_doctor_agent`
returns `disabled: no base URL configured` and logs one line; it never POSTs to a
relative URL. Unset `DOCTOR_FIX_BASE_URL`: the fix phase is disabled the same way.
Unset `DOCTOR_FALLBACK_BASE_URLS`: the phase base is the whole chain, so there is
no failover rung.

## Strict-route worker

| Variable | Meaning | Example |
|---|---|---|
| `HIVE_TYDEUS_BASE_URL` | Pins the exact base the strict-route coder must be served from. Unset: the manifest's coder `base_url` is checked for shape only, and the weaker check is logged once. | `http://<tydeus-host>:8006/v1` |

## MNEMOS

| Variable | Meaning | Example |
|---|---|---|
| `MNEMOS_URL` | MNEMOS base URL (MCP bridge, `Settings.mnemos_url`, loop-event fetches). | `http://<mnemos-host>:5002` |
| `MNEMOS_TOKEN` | Bearer token. No default; `mnemos.*` bridge tools refuse without it. | — |

## Alerts

| Variable | Meaning | Example |
|---|---|---|
| `HIVE_BUS_URL` | Bus base URL used by `hive/tools/deepseek_spend_alert.py`. | `http://<bus-host>:5005` |

Unset with the bus enabled: the tool raises a configuration error before running
(it does not post anywhere); `--no-bus` runs it offline.
