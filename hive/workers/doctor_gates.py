#!/usr/bin/env python3
"""Executor safety rails: exact approvals, purge contracts, restart targets.

Every purge, restart and delegated code-fix task requires a scoped approval.
Legacy gate_enabled/auto_approve constructor options are accepted for caller
compatibility but cannot authorize work. Approvals are refreshed from healthy
state before use; configuration flags and model output are never grants.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os

# Fleet addresses come from operator config, never source (HIVE_FLEET_INVENTORY).
# Stdlib-only module: the loader it uses is stdlib-only too.
from hive.core import fleet_inventory

log = logging.getLogger("doctor.gates")

# Env read once per process, like the doctor's other configuration. The tests
# monkeypatch the module attributes.
GATE_DESTRUCTIVE = os.environ.get("DOCTOR_GATE_DESTRUCTIVE", "1") == "1"
AUTO_APPROVE_DESTRUCTIVE = os.environ.get("DOCTOR_AUTO_APPROVE_DESTRUCTIVE", "0") == "1"
APPROVAL_TTL_SECS = float(os.environ.get("DOCTOR_APPROVAL_TTL", "900"))
APPROVAL_PREFIX = "approval"

# Privileged work includes delegated tasks, which can themselves restart/purge.
DESTRUCTIVE_ACTIONS = frozenset({"cancel_jobs", "dlq_remove", "restart_service", "dispatch_codex_fix"})


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
        configured_ttl = APPROVAL_TTL_SECS if ttl is None else float(ttl)
        self.ttl = min(configured_ttl, 900.0) if math.isfinite(configured_ttl) else 0.0

    def record_approval(self, kind: str, digest: str, by: str = "operator:local") -> None:
        """Operator CLI issues a scoped, expiring, single-use grant."""
        if (
            self.ttl <= 0
            or kind not in DESTRUCTIVE_ACTIONS
            or not by.startswith("operator:")
            or len(by) <= len("operator:")
        ):
            raise ValueError("grant requires a known action and operator approver identity")
        now = self.store._time()
        grant = {"digest": digest, "approver": by, "granted_at": now, "expires_at": now + self.ttl, "used": False}
        self.store.set_value(f"{APPROVAL_PREFIX}:{digest}", grant, ttl=self.ttl)
        if not self.store.loaded_ok or not self.store.persist_ok:
            raise OSError("grant was not persisted")
        log.info("approval recorded kind=%s digest=%s by=%s", kind, digest[:12], by)

    def revoke(self, digest: str) -> None:
        self.store.forget(f"{APPROVAL_PREFIX}:{digest}")

    def approved(self, kind: str, digest: str) -> bool:
        """Inspection only; the executor must call `require` to consume."""
        del kind
        self.store.reload()
        grant = self.store.value(f"{APPROVAL_PREFIX}:{digest}")
        return (
            self.store.loaded_ok
            and self.store.persist_ok
            and isinstance(grant, dict)
            and grant.get("digest") == digest
            and grant.get("used") is False
            and isinstance(grant.get("approver"), str)
            and grant["approver"].startswith("operator:")
            and type(grant.get("granted_at")) in (int, float)
            and type(grant.get("expires_at")) in (int, float)
            and math.isfinite(grant["granted_at"])
            and math.isfinite(grant["expires_at"])
            and self.ttl > 0
            and self.store._time() < min(grant["expires_at"], grant["granted_at"] + self.ttl)
        )

    def require(self, kind: str, **parts) -> str | None:
        """Return a refusal unless this exact privileged action is approved."""
        if kind not in DESTRUCTIVE_ACTIONS:
            return None
        digest = action_digest(kind, **parts)
        if self.store.consume_grant(digest, self.ttl):
            return None
        return f"{kind} needs a scoped operator grant for digest {digest}"


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
            # The route NAMED the ids it deleted, and that list is the exact truth.
            # It is preferred over the count beside it, which can disagree: a route
            # reporting five deletions while naming two would otherwise print
            # "5 removed, 3 skipped" for a request of five — or "5 removed" for a
            # request of two.
            removed = len(removed_ids)
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

def _build_restart_targets() -> dict[str, dict]:
    """The hosts a restart may target: the inventory, plus cixmini.

    Every entry that only names an address, user and auth comes from the fleet
    inventory (``HIVE_FLEET_INVENTORY``) — the table this function replaces held
    stale addresses for three hosts and put a real address in every clone of the
    repo. The inventory's ``password_env`` is carried across so a password-auth
    host still refuses before any SSH when its password is unset.

    cixmini stays a static ENTRY with no address: its address and sudo user come
    from CIXMINI_IP / DOCTOR_CIXMINI_SUDO_USER, which is why it can be listed (and
    offered by DOCTOR_SYSTEM) while remaining refused by name until configured.
    """
    targets: dict[str, dict] = {
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
        }
    }
    for name, entry in fleet_inventory.load_inventory().items():
        if name in targets:
            continue
        target = {"address": entry["address"], "user": entry["user"], "auth": entry["auth"]}
        if entry.get("password_env"):
            target["password_env"] = entry["password_env"]
        targets[name] = target
    return targets


#: The hosts a restart may target. Built once at import: an operator who edits the
#: inventory restarts the doctor to pick it up, which is the same rule as every
#: other env read in this worker.
RESTART_TARGETS: dict[str, dict] = _build_restart_targets()

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
    if entry.get("auth") == "password" and entry.get("password_env") and not env.get(entry["password_env"], ""):
        return None, f"restart_service refused: {name} needs {entry['password_env']} and it is not set"
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
