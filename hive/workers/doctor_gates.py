#!/usr/bin/env python3
"""The rails the doctor SOP's decisions need on the executor side (phase 3).

Three defects this module closes, each of them a behaviour change rather than a
refactor. All three are gated so that deploying this file alone changes nothing
until an operator sets the switch.

## 1. Destructive actions were never gated

`cancel_jobs` hard-deleted up to 20 jobs **plus every child job** through
`POST /v1/jobs/purge`, at whatever confidence the model claimed, and a DLQ
`remove` did the same for a single job. `restart_service` killed a systemd unit on
a fleet host. None of the three waited for anyone.

`ApprovalGate.require()` is now on all three paths. An approval is **scoped to a
digest of the action** — the kind plus the exact ids, or the host and service —
so approving one purge never approves the next. The standing auto-approve that
removes the wait entirely is `DOCTOR_AUTO_APPROVE_DESTRUCTIVE`, **off unless set**.

## 2. Two purge endpoints with two payload keys, and one wrong success report

| Intent | Route | id key | Response | Count read from |
|---|---|---|---|---|
| `cancel_failed` | `POST /v1/jobs/purge` | `ids` (+ `status: failed`) | `{deleted, matched_ids}` | `deleted` |
| `dlq_remove` | `POST /v1/admin/jobs/purge` | `job_ids` | `{ok, not_found, skipped_not_dead_letter}` | `ok` |

The DLQ path reported `removed <id> via admin purge` for **any** 2xx — including
the case where the bus returned `ok: []` and the job was in
`skipped_not_dead_letter`, i.e. nothing was deleted at all. That is a doctor
telling the operator it removed a job it did not remove. `purge_request` /
`purge_result` now own the whole contract in one table, and `purge_result` says
what actually happened.

## 3. `cixmini` was offered as a restart target and could never be reached

`DOCTOR_SYSTEM` lists `cixmini` among the legal `target_host` values and
`action_restart_service` has a whole branch for it (sshpass, `mini@ip`,
`CIXMINI_SSH_PASSWORD`) — but the function resolves the address from `FLEET_HOSTS`
*before* that branch, and `cixmini` is not in `FLEET_HOSTS`. So the branch was
dead code and every cixmini restart answered `unknown host 'cixmini'`.

`resolve_restart_target()` is now the single source: one table of restart targets,
blocklist winning over allowlist, and a target that is listed but unconfigured
refused **by name** ("no recorded address, set CIXMINI_IP") instead of pretending
it is an unknown host. cixmini is now consistently in or out.

Env:
  DOCTOR_GATE_DESTRUCTIVE          "0" to turn the rail OFF (default: ON). The
                                   opt-out exists only for the transition window,
                                   before the SOP can issue approvals; with the
                                   gate ON and no approvals arriving, a
                                   destructive action is REFUSED, which is the
                                   intended fail-closed behaviour.
  DOCTOR_AUTO_APPROVE_DESTRUCTIVE  "1" for a standing auto-approve (default: off)
  DOCTOR_APPROVAL_TTL              seconds an approval stays valid (default: 900)
  CIXMINI_IP                      the address for the cixmini restart target
  DOCTOR_CIXMINI_SUDO_USER         the ssh/sudo user on cixmini (default: mini)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os

log = logging.getLogger("doctor.gates")

# Env read once per process, like the doctor's other configuration. The tests
# monkeypatch the module attributes.
GATE_DESTRUCTIVE = os.environ.get("DOCTOR_GATE_DESTRUCTIVE", "1") == "1"
AUTO_APPROVE_DESTRUCTIVE = os.environ.get("DOCTOR_AUTO_APPROVE_DESTRUCTIVE", "0") == "1"
APPROVAL_TTL_SECS = float(os.environ.get("DOCTOR_APPROVAL_TTL", "900"))
APPROVAL_PREFIX = "approval"

# The actions that wait for an operator. Everything else in the playbook is
# bounded and reversible (a release, a requeue) or is a dispatch.
DESTRUCTIVE_ACTIONS = frozenset({"cancel_jobs", "dlq_remove", "restart_service"})


def action_digest(kind: str, **parts) -> str:
    """A stable digest of one action's exact target.

    Stable because the parts are sorted by name and JSON-encoded with sorted
    keys, so the same intent digests the same on every host and every restart —
    an approval recorded before a restart must still match after it.
    """
    payload = json.dumps({"kind": kind, "parts": parts}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()[:32]


class ApprovalGate:
    """Whether a destructive action may proceed, and who said yes.

    Approvals live in the durable store, so an approval does not evaporate when
    the process restarts — the same property the store exists for, applied to the
    one record where losing it would let a purge through unasked.
    """

    def __init__(
        self, store, gate_enabled: bool | None = None, auto_approve: bool | None = None, ttl: float | None = None
    ):
        self.store = store
        self.gate_enabled = GATE_DESTRUCTIVE if gate_enabled is None else bool(gate_enabled)
        self.auto_approve = AUTO_APPROVE_DESTRUCTIVE if auto_approve is None else bool(auto_approve)
        self.ttl = APPROVAL_TTL_SECS if ttl is None else float(ttl)

    def record_approval(self, kind: str, digest: str, by: str = "operator") -> None:
        """Record an approval for exactly this action. Idempotent."""
        self.store.mark(f"{APPROVAL_PREFIX}:{digest}", ttl=self.ttl)
        log.info("approval recorded kind=%s digest=%s by=%s", kind, digest[:12], by)

    def revoke(self, digest: str) -> None:
        self.store.forget(f"{APPROVAL_PREFIX}:{digest}")

    def approved(self, kind: str, digest: str) -> bool:
        """Whether an APPROVAL for this exact action exists.

        This answers only that question. It deliberately does NOT consider whether
        the gate is switched on: a disabled gate means "no approval is needed", not
        "no approval was given", and folding the two into one boolean makes a
        caller read `approved() == False` as a refusal when the rail is simply off.
        `require()` owns the switch; this owns the record.
        """
        del kind  # the digest is the identity of the action; the kind is not needed
        return self.store.seen(f"{APPROVAL_PREFIX}:{digest}", ttl=self.ttl)

    def require(self, kind: str, **parts) -> str | None:
        """``None`` when the action may proceed; the refusal reason when it may not.

        The order of the three escape hatches matters and is the whole contract:

        1. a non-destructive action is never gated;
        2. `DOCTOR_GATE_DESTRUCTIVE=0` switches the rail **off** — the action is
           allowed, and that is the only thing the switch does;
        3. a standing auto-approve allows it and says so in the log;
        4. otherwise an approval recorded for this exact action is required.

        The refusal names the digest an approval must be recorded against —
        otherwise "it asked me to approve something" has nothing to approve.
        """
        if kind not in DESTRUCTIVE_ACTIONS:
            return None
        if not self.gate_enabled:
            return None
        digest = action_digest(kind, **parts)
        if self.auto_approve:
            log.warning("%s proceeding on the STANDING auto-approve (%s)", kind, digest)
            return None
        if self.approved(kind, digest):
            return None
        return (
            f"{kind} needs an operator approval; none is recorded for digest {digest} "
            f"(parts={parts}). Record it, or set DOCTOR_AUTO_APPROVE_DESTRUCTIVE=1 for a "
            f"standing auto-approve."
        )


# ── the one purge contract ──────────────────────────────────────────────────

#: One row per intent: where it goes, which key carries the ids, what the route
#: refuses to touch, and where the truth about what happened comes back. Two
#: intents, two routes, two payload keys, two response shapes — written down once
#: instead of remembered twice.
PURGE_INTENTS = {
    "cancel_failed": {
        "path": "/v1/jobs/purge",
        "id_key": "ids",
        "body": {"status": "failed"},
        "removed_key": "deleted",
        "refuses": (
            "the route AND-combines every filter and excludes active statuses unless "
            "include_active is set, so `status: failed` plus explicit ids is the narrowest "
            "call it accepts"
        ),
    },
    "dlq_remove": {
        "path": "/v1/admin/jobs/purge",
        "id_key": "job_ids",
        "body": {},
        "removed_key": "ok",
        "refuses": "the route only deletes jobs currently in dead-letter; anything else is skipped",
    },
}


def _as_id_list(value) -> list[str]:
    """Ids from a response field, or an empty list.

    Never iterates anything that is not a list: a route that answered with the
    string `"ok"` instead of a list would otherwise yield `["o", "k"]` and report
    two deleted jobs that do not exist.
    """
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, (str, int))]


def purge_request(intent: str, job_ids: list[str], reason: str) -> tuple[str, dict]:
    """The (path, body) for one purge intent.

    Raises ``KeyError`` for an unknown intent rather than defaulting: an intent
    nobody named has no known-safe endpoint, and guessing one is how the original
    `ids` / `job_ids` mismatch happened.
    """
    spec = PURGE_INTENTS[intent]
    body = dict(spec["body"])
    body[spec["id_key"]] = list(job_ids)
    body["reason"] = reason
    return spec["path"], body


def purge_result(intent: str, response, requested: list[str]) -> dict:
    """What the route actually did, per its own response shape.

    ``removed`` counts only ids the route names as deleted. A job the route
    *skipped* — a non-dead-letter job on the DLQ route, a concurrent state change
    on the bulk route — is reported in ``skipped`` rather than counted, which is
    the whole point: the doctor's summary must not claim a removal that did not
    happen.

    What is exact and what is an attribution: **`removed` is always exact** — it is
    the route's own count, or the length of the route's own id list. `skipped` is
    exact when the route names the ids it deleted, and is otherwise an
    ATTRIBUTION: a route that returns a count but no id list cannot say which ids
    it kept, so the shortfall is reported against the tail of the requested list
    and the count is what the operator should believe. Reporting no skips at all
    would hide a partial purge; reporting the count as zero would invent one.
    """
    spec = PURGE_INTENTS[intent]
    removed_key = spec["removed_key"]
    if not isinstance(response, dict):
        return {"removed": 0, "skipped": list(requested), "partial": bool(requested), "attributed": True}
    if intent == "cancel_failed":
        removed = response.get(removed_key)
        removed_ids = _as_id_list(response.get("matched_ids"))
        if not isinstance(removed, int):
            removed = len(removed_ids)
        removed = max(0, removed)
        if removed_ids:
            # The route named the ids it deleted: the ones it did not name are the
            # ones it kept.
            skipped = [job_id for job_id in requested if job_id not in removed_ids]
        else:
            # No id list, only a count. The count is what the route CONFIRMED, so it
            # is the number of removals; the shortfall against what was asked for
            # is the partial. Deriving `skipped` from an empty id list would report
            # a successful purge as having deleted nothing at all, which is the
            # false-negative direction of the bug this function exists to close.
            shortfall = max(0, len(requested) - removed)
            skipped = requested[len(requested) - shortfall :] if shortfall else []
            return {
                "removed": removed,
                "skipped": skipped,
                "partial": bool(skipped),
                # The route confirmed a COUNT and named no ids, so which ids it
                # kept is not knowable from the response. `skipped` is therefore
                # attributed, not observed, and this flag is how a caller knows
                # not to print it as if it were observed.
                "attributed": True,
            }
        return {"removed": removed, "skipped": skipped, "partial": bool(skipped), "attributed": False}
    removed = response.get(removed_key)
    # `ok` is a list today. If a future bus returned a bare count instead, that
    # count is still what it CONFIRMED, so it is honoured rather than read as
    # "nothing was deleted" — the same rule the bulk route already gets.
    if isinstance(removed, int):
        removed_count, removed_ids = max(0, removed), []
    else:
        removed_ids = _as_id_list(removed)
        removed_count = len(removed_ids)
    skipped = _as_id_list(response.get("skipped_not_dead_letter"))
    not_found = _as_id_list(response.get("not_found"))
    if removed_ids:
        untouched = [job_id for job_id in requested if job_id not in removed_ids]
    else:
        # No id list: a count is all the route confirmed, so the shortfall is what
        # it kept. Deriving `untouched` from an empty id list while ALSO honouring
        # the count would report "1 removed" and "1 not removed" for one request.
        shortfall = max(0, len(requested) - removed_count)
        untouched = requested[len(requested) - shortfall :] if shortfall else []
    return {
        "removed": removed_count,
        "skipped": skipped,
        "not_found": not_found,
        "untouched": untouched,
        "partial": bool(untouched),
        "attributed": not removed_ids and bool(requested),
    }


# ── one restart allowlist ────────────────────────────────────────────────────

#: The hosts a restart may target. `FLEET_HOSTS` from the doctor, plus cixmini —
#: which `DOCTOR_SYSTEM` already offered and the executor could not reach.
#: `address` comes from the environment so a host is only listed once somebody
#: has recorded where it is; an unconfigured entry is refused BY NAME.
RESTART_TARGETS: dict[str, dict] = {
    "cerberus": {"address": "192.0.2.96", "user": "jasonperlow", "auth": "key"},
    "medusa": {"address": "192.0.2.64", "user": "jasonperlow", "auth": "key"},
    "proteus": {"address": "192.0.2.25", "user": "jasonperlow", "auth": "key"},
    "hydra": {"address": "192.0.2.78", "user": "jasonperlow", "auth": "key"},
    "bigpi": {"address": "192.0.2.65", "user": "jasonperlow", "auth": "key"},
    "clawpi": {"address": "192.0.2.54", "user": "jasonperlow", "auth": "key"},
    "zeropi": {"address": "192.0.2.56", "user": "jasonperlow", "auth": "key"},
    "typhon": {"address": "192.0.2.61", "user": "jasonperlow", "auth": "key"},
    # DOCTOR_SYSTEM has offered this since 2026-05-26. It is here NOW, and it is
    # reachable now: address and sudo user come from the environment, and until
    # both are set the restart is refused with the variable to set rather than
    # reported as an unknown host.
    "cixmini": {
        "address_env": "CIXMINI_IP",
        # Read at resolve time, not import time, so a test can set it and so a
        # later config reload does not need a restart to take effect.
        "user_env": "DOCTOR_CIXMINI_SUDO_USER",
        "user": "mini",
        "auth": "password",
        "password_env": "CIXMINI_SSH_PASSWORD",
    },
}

#: Hosts no restart may ever target, whatever the allowlist says (the doctor's
#: `RESTART_BLOCKED_HOSTS`: production and the doctor host itself).
RESTART_BLOCKED_HOSTS = frozenset({"argos", "pythia"})


def resolve_restart_target(host: str | None, env: dict | None = None) -> tuple[dict | None, str]:
    """``(target, reason)`` — the connection spec, or why there isn't one.

    The blocklist is checked first, then the allowlist, then the entry's own
    configuration. That order matters: a blocked host is refused as blocked even
    if it is allowlisted, because a stale allowlist entry must not re-open ARGOS.
    """
    env = dict(os.environ if env is None else env)
    name = (host or "").strip().lower()
    if not name:
        return None, "restart_service needs a target_host"
    if name in RESTART_BLOCKED_HOSTS:
        return None, f"{name} is in the restart blocklist (production or the doctor host) — REFUSED"
    entry = RESTART_TARGETS.get(name)
    if entry is None:
        return None, (
            f"{name!r} is not a restart target; DOCTOR_SYSTEM must not offer it. "
            f"Known targets: {', '.join(sorted(RESTART_TARGETS))}"
        )
    address_env = entry.get("address_env")
    address = env.get(address_env, "") if address_env else entry.get("address", "")
    if not (address or "").strip():
        return None, (
            f"{name} is a restart target but has no recorded address; set "
            f"{address_env or 'the entry address'} before a restart can target it"
        )
    if entry.get("auth") == "password" and not (env.get(entry.get("password_env", ""), "") or ""):
        return None, (f"restart_service refused: {name} needs {entry.get('password_env')} and it is not set")
    # The env overrides the entry's default, and the entry's default is what makes
    # the host usable without configuration. Reading only the env would turn
    # cixmini's recorded `mini` user into "unrecorded" on every host that has not
    # set the override.
    user = (env.get(entry["user_env"], "") if entry.get("user_env") else "") or entry.get("user", "")
    if not (user or "").strip():
        return None, (
            f"{name} is a restart target but no user is recorded for it; set "
            f"{entry.get('user_env') or 'the entry user'}"
        )
    return {
        "host": name,
        "address": address.strip(),
        "user": user.strip(),
        "auth": entry.get("auth", "key"),
    }, ""


def restart_target_names() -> list[str]:
    """The hosts a restart may actually be aimed at, for `DOCTOR_SYSTEM`.

    Derived from the same table the executor reads, so the prompt cannot offer a
    host the executor would refuse.
    """
    return sorted(RESTART_TARGETS)
