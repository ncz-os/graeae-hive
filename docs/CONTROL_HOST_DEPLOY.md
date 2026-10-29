# Control host deploy (doctor + bus)

The doctor runs in a container from a systemd quadlet unit and the bus runs
as a systemd service (`graeae-hive.service`, UNVERIFIED unit name from the
brief — no such unit file exists in this repo). The integrate branch makes
their addresses environment-only (Source: `docs/ENVIRONMENT.md`,
`docs/environment-requirements.json`, `scripts/check_env.py`). The doctor
container currently has NONE of `HIVE_URL`, `HIVE_BUS_ADMIN_TOKEN`,
`HIVE_FLEET_INVENTORY`, `MNEMOS_URL`, `MNEMOS_TOKEN` set (per the brief).
This doc supplies them without committing any secret or real address.

## Files

- `deploy/doctor.container.env.example` — placeholders only (Source: the
  file itself). Copy outside the repo and fill in.
- `deploy/bus.service.env.example` — admin-token placeholder only. The bus
  serves `HIVE_URL`; it does not consume it.
- `scripts/render_control_env.py` — fills the three address-derived values
  from an inventory JSON (`HIVE_URL` → `http://<pythia>:5005`,
  `MNEMOS_URL` → `http://<pythia>:5002`,
  `DOCTOR_REVIEWER_HEALTH_URL` → `http://<cerberus>:8080/health`) and leaves
  every token as `<from key store>` (Source: the script; tested by
  `scripts/test_render_control_env.py`, 4 passed).
- No committed file carries a real hostname, address, or token (placeholders
  use `<...>`).

## Ordered steps

1. Render outside the repo (UNVERIFIED host paths from the brief; the render
   itself is verified by tests):
   `scripts/render_control_env.py --inventory <inventory.json> --out-dir
   <secrets-dir>` → `<secrets-dir>/doctor.container.env`,
   `<secrets-dir>/bus.service.env`.
2. Put real tokens into the rendered files by hand from the operator key
   store (replace the two `<from key store>` placeholders). Never print
   them; never commit them (Source: lane rules; `check_env.py` never prints
   secret values — Source: `scripts/check_env.py` docstring).
3. Install the inventory JSON at the path the doctor will read
   (example: `/etc/graeae/fleet-inventory.json`) and bind-mount it into the
   doctor container read-only (UNVERIFIED mount syntax — no quadlet file in
   this repo to cite).
4. Wire the quadlet: non-secrets (`HIVE_URL`, `HIVE_FLEET_INVENTORY`,
   `MNEMOS_URL`, model bases) as quadlet `Environment=` lines;
   secrets (`HIVE_BUS_ADMIN_TOKEN`, `MNEMOS_TOKEN`) ONLY via
   `EnvironmentFile=<secrets-dir>/doctor.container.env` with mode `0600`
   (UNVERIFIED unit syntax — no quadlet file in this repo; the secret/non-
   secret split follows `docs/ENVIRONMENT.md` and
   `environment-requirements.json` kinds `secret` vs `url`/`inventory`).
5. Verify BEFORE restarting anything: run `scripts/check_env.py` with the
   same environment the container will see
   (`scripts/check_env.py --env-file <secrets-dir>/doctor.container.env`;
   `--probe` for one bounded bus GET) (Source: `scripts/check_env.py`
   docstring: exit 0 ready, 1 not ready, 2 probe failed). Fix every failure
   before proceeding.
6. Restart the doctor container (UNVERIFIED command — host procedure, no
   restart from a repo commit per lane rules).
7. Restart the bus with its env file
   (`EnvironmentFile=<secrets-dir>/bus.service.env`, mode `0600`)
   (UNVERIFIED unit syntax). Announce a bus restart to other sessions first
   (per FB1 runbook 04; UNVERIFIED in this repo).
8. Re-run `check_env.py --probe` and confirm the doctor registers and polls
   (Source: `check_env.py` probe semantics).

## Rollback

- Restore the previous env files and restart the same two units; no repo
  change is involved so nothing needs reverting in git (UNVERIFIED
  procedure).
- The doctor degrades safely when a variable is unset (one log line, feature
  disabled — Source: `docs/ENVIRONMENT.md` header), so a bad value can be
  neutralized by unsetting it and restarting.

## What NOT to do

- Do NOT put tokens in quadlet `Environment=` lines, unit files, or the repo
  (Source: lane rules; secret kinds in `environment-requirements.json`).
- Do NOT commit rendered files with real addresses or tokens (placeholders
  only in `deploy/*.example`).
- Do NOT restart services from a repo commit or over SSH automation; host
  procedure only, and I (the merger) do the deploys (per the brief).
