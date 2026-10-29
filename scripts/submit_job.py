#!/usr/bin/env python3
"""Build a Hive job from a task file, with the operator's rules encoded.

A submitted job is expensive to get wrong: a stray ``eligible_hosts`` pin
strands it, ``codex`` is out of quota, a named paid model spends money the
operator did not choose, and a re-run that mints a second id creates duplicate
work. This script turns a task file into a payload that already obeys the rules,
and refuses the ones that cannot be satisfied:

* no ``eligible_hosts`` unless ``--pin-reason`` says why the pin is needed;
* a build-required job (``build:`` kind or a ``hard:build*`` capability, i.e.
  one that runs a real build) may only run on builders — proteus, hydra,
  achilles, argos — resolved as NAMES through the fleet inventory
  (``HIVE_FLEET_INVENTORY``), never literal addresses, and never minos;
* ``preferred_models`` is omitted unless the task file names one, and a paid
  model (cost tier B/C) gets a warning without ``--reason``;
* ``eligible_kinds`` defaults to zeroclaw; ``codex`` is refused unless
  ``--allow-codex``;
* ``idempotency_key`` is derived from the task file content, so a re-run
  coalesces instead of creating a second job;
* the standard operating-rules block is prepended to the description.

Dry run is the default and prints the payload. ``--send`` POSTs it and needs
``HIVE_URL`` and ``HIVE_ACTOR_URN``. Tests run offline; no address is
compiled into this file.

    scripts/submit_job.py task.json
    HIVE_URL=... HIVE_ACTOR_URN=... scripts/submit_job.py task.json --send
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

# `hive` is a package at the repo root; this script lives one level down.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pydantic import ValidationError  # noqa: E402

from hive.bus.domain.eligibility import host_pin_warnings  # noqa: E402
from hive.bus.schemas import JobCreate  # noqa: E402
from hive.core.fleet_inventory import INVENTORY_ENV  # noqa: E402
from hive.core.fleet_inventory import hosts as inventory_hosts
from hive.core.models import LLM_RATES, PROVIDER_COST_TIER, cost_tier_for  # noqa: E402
from hive.scheduler import job_requires_build  # noqa: E402

#: Operator rule 2026-10-29: the hosts a real build may run on. Resolved
#: against the inventory as names; ``ultra`` is a conventional build host for
#: role hints but is not on this list, and ``minos`` is never one.
BUILDER_HOSTS = ("proteus", "hydra", "achilles", "argos")

#: Refused outright for a build-required job, whatever the inventory says.
NEVER_BUILDER = ("minos",)

DEFAULT_KINDS = ["zeroclaw"]

RULES_BLOCK = """OPERATING RULES (operator, 2026-10-29) — read before you start:
1. Work only inside the job workspace; change nothing outside it.
2. Push to ARGONAS only. No GitHub, no GitLab, no PRs, no SSH to fleet hosts,
   no restarts or deploys, no edits under /opt /srv /etc, no secrets.
3. Run `ruff` only on the files you changed; never `git stash` or `--no-verify`.
4. Finish with the tests pasted and a structured non-author review
   (`zoder --json review --scope branch --base <base>`), JSON verdict attached.
5. Do not merge. Claude verifies and merges."""

#: Every field JobCreate accepts, minus the one this script fills in at send
#: time. A task file key outside this set is a typo, not a new feature.
_TASK_KEYS = frozenset(JobCreate.model_fields) - {"submitter_urn"}


def load_task(path: str | Path) -> tuple[dict, str]:
    """The task file as ``(dict, canonical text)``.

    Canonical text is key-sorted and compact, so re-running on a byte-identical
    (or merely reformatted) task file yields the same idempotency key.
    """
    try:
        raw = Path(path).read_text()
    except OSError as exc:
        raise ValueError(f"cannot read {path}: {exc}") from exc
    try:
        task = json.loads(raw)
    except ValueError as exc:
        raise ValueError(f"{path} is not valid JSON: {exc}") from exc
    if not isinstance(task, dict):
        raise ValueError(f"{path} must be a JSON object describing one task")
    canonical = json.dumps(task, sort_keys=True, separators=(",", ":"))
    return task, canonical


def idempotency_key(canonical: str) -> str:
    """Stable per task-file content: a re-run must not create a second job."""
    return "submit-job:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


def _providers_of(model: str) -> list[str]:
    """Providers known to serve ``model`` — from the name prefix, then rates."""
    name = str(model or "").strip().lower()
    if not name:
        return []
    out = []
    if "/" in name:
        out.append(name.split("/", 1)[0])
    out.extend(provider for (provider, key) in LLM_RATES if key == name)
    return out


def paid_model_tier(model: str) -> str:
    """Cost tier of a named model ('A' free, 'B'/'C' paid, '' unknown)."""
    tiers = {cost_tier_for(p) for p in _providers_of(model)}
    if not tiers:
        # No known provider: fall back to the name itself so a bare
        # provider-prefixed-free model is not cried down as paid.
        return PROVIDER_COST_TIER.get(str(model or "").strip().lower(), "")
    return sorted(tiers)[-1]


def resolve_builders(inventory: dict[str, dict]) -> list[str]:
    """Builder names that exist in the inventory, in BUILDER_HOSTS order."""
    known = {name.lower() for name in inventory}
    return [name for name in BUILDER_HOSTS if name in known]


def _check_unknown_keys(task: dict) -> None:
    # A leading underscore is a comment convention ("_comment"), not a field.
    unknown = sorted(key for key in task if not str(key).startswith("_") and key not in _TASK_KEYS)
    if unknown:
        raise ValueError(
            f"unknown task key(s): {', '.join(unknown)} — allowed keys are JobCreate fields: "
            f"{', '.join(sorted(_TASK_KEYS))}"
        )


def _check_eligible_kinds(payload: dict, *, allow_codex: bool) -> None:
    kinds = [str(k).strip().lower() for k in (payload.get("eligible_kinds") or [])]
    if not kinds:
        payload["eligible_kinds"] = list(DEFAULT_KINDS)
        return
    if "codex" in kinds and not allow_codex:
        raise ValueError(
            "eligible_kinds includes codex, but Codex is out of quota: rerun with --allow-codex "
            "to submit it anyway (eligible_kinds defaults to zeroclaw)"
        )


def _check_host_pin(payload: dict, *, pin_reason: str | None) -> None:
    pinned = payload.get("eligible_hosts")
    if pinned:
        if not (pin_reason or "").strip():
            raise ValueError(
                f"eligible_hosts={list(pinned)} needs --pin-reason: hosts are interchangeable for "
                "ordinary work, and a pin strands the job when the pinned host fails it"
            )
        payload["pin_reason"] = pin_reason.strip()
    elif (pin_reason or "").strip():
        raise ValueError("--pin-reason was given but the task file sets no eligible_hosts")


def _check_build_job(payload: dict, inventory: dict[str, dict], *, pin_reason: str | None) -> None:
    """A build-required job runs on builders only, by name, never minos."""
    if not job_requires_build(payload):
        return
    for name in payload.get("eligible_hosts") or []:
        if str(name).strip().lower() in NEVER_BUILDER:
            raise ValueError(f"{name} is never a builder: image builds run on {', '.join(BUILDER_HOSTS)}")
    builders = resolve_builders(inventory)
    if not builders:
        raise ValueError(
            f"this is a build-required job but none of {', '.join(BUILDER_HOSTS)} is in the fleet "
            f"inventory ({INVENTORY_ENV} not set or names none of them); refusing to guess a host"
        )
    if not payload.get("eligible_hosts"):
        payload["eligible_hosts"] = builders
        # The bus warns about an unexplained pin; this one is the operator's
        # rule, so say so unless the operator gave a better reason.
        payload.setdefault("pin_reason", (pin_reason or "").strip() or "build job: builders only")
        return
    pinned = [str(name).strip().lower() for name in payload["eligible_hosts"]]
    off_builder = [name for name in pinned if name not in BUILDER_HOSTS]
    if off_builder:
        raise ValueError(
            f"eligible_hosts={off_builder} is not a builder: a build-required job runs on "
            f"{', '.join(BUILDER_HOSTS)} only (resolved by name via {INVENTORY_ENV})"
        )
    missing = [name for name in pinned if name not in {n.lower() for n in inventory}]
    if missing:
        raise ValueError(
            f"eligible_hosts={missing} is not in the fleet inventory ({INVENTORY_ENV}); "
            "names are resolved there, addresses never come from source"
        )


def build_payload(
    task: dict,
    canonical: str,
    *,
    pin_reason: str | None = None,
    reason: str | None = None,
    allow_codex: bool = False,
    inventory: dict[str, dict] | None = None,
) -> tuple[dict, list[str]]:
    """``(payload, warnings)``. Raises ``ValueError`` for a rule it cannot honour."""
    _check_unknown_keys(task)
    if not str(task.get("kind") or "").strip():
        raise ValueError('the task file needs a "kind" (e.g. "build:ncz-os-image", "code-edit")')
    if not str(task.get("description") or "").strip():
        raise ValueError('the task file needs a "description": the work itself')

    payload = {key: task[key] for key in sorted(task) if key in _TASK_KEYS}
    payload["description"] = RULES_BLOCK + "\n\n" + str(task["description"]).strip()

    _check_eligible_kinds(payload, allow_codex=allow_codex)
    _check_host_pin(payload, pin_reason=pin_reason)
    _check_build_job(payload, inventory if inventory is not None else inventory_hosts(), pin_reason=pin_reason)

    warnings: list[str] = []
    preferred = payload.get("preferred_models")
    if preferred:
        paid = [m for m in preferred if paid_model_tier(str(m)) in ("B", "C")]
        if paid and not (reason or "").strip():
            warnings.append(
                f"preferred_models names paid model(s) {paid} with no --reason: the default is to "
                "omit preferred_models so the host ladder starts at qwen38 — pass --reason to keep them"
            )
        paid_tier = [f"{m} (tier {paid_model_tier(str(m))})" for m in paid]
        if paid and (reason or "").strip():
            warnings.append(f"paid model(s) kept with --reason: {'; '.join(paid_tier)} — reason: {reason.strip()}")

    payload.setdefault("idempotency_key", idempotency_key(canonical))
    payload.setdefault("submitter_urn", os.environ.get("HIVE_ACTOR_URN", "").strip())
    warnings.extend(host_pin_warnings(payload.get("kind"), payload.get("eligible_hosts"), payload.get("pin_reason")))
    try:
        JobCreate(**payload)  # the bus accepts exactly this, or the error is ours to show
    except ValidationError as exc:
        raise ValueError(f"the bus would reject this payload: {exc}") from exc
    return payload, warnings


def send(payload: dict, timeout: float = 15.0) -> dict:
    """``POST $HIVE_URL/v1/jobs``. Env-only configuration."""
    base = os.environ.get("HIVE_URL", "").strip().rstrip("/")
    if not base:
        raise ValueError("HIVE_URL is not set — export it, or drop --send for a dry run")
    if not str(payload.get("submitter_urn") or "").strip():
        raise ValueError("HIVE_ACTOR_URN is not set — it becomes the job's submitter_urn")
    body = json.dumps(payload).encode("utf-8")
    try:
        req = urllib.request.Request(
            base + "/v1/jobs",
            data=body,
            method="POST",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - operator-configured URL
            answer = json.load(resp)
    except urllib.error.HTTPError as exc:
        detail = ""
        with contextlib.suppress(Exception):  # the status alone must still be reported
            detail = ": " + exc.read().decode("utf-8", "replace")[:300]
        raise ValueError(f"the bus refused the job ({exc.code}){detail}") from exc
    except urllib.error.URLError as exc:
        raise ValueError(f"cannot reach the bus: {exc.reason}") from exc
    except ValueError as exc:
        raise ValueError(f"the bus did not answer with JSON: {exc}") from exc
    if not isinstance(answer, dict):
        raise ValueError(f"unexpected answer from the bus: {answer!r}")
    return answer


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("task_file", help="JSON task file: kind, description and optional JobCreate fields")
    parser.add_argument("--send", action="store_true", help="POST the job (default is a dry run that prints it)")
    parser.add_argument("--pin-reason", help="why eligible_hosts is pinned; required when the task file pins hosts")
    parser.add_argument("--reason", help="why a paid preferred_model is wanted (suppresses the paid-model warning)")
    parser.add_argument("--allow-codex", action="store_true", help="allow codex in eligible_kinds despite the quota")
    args = parser.parse_args(argv)

    try:
        task, canonical = load_task(args.task_file)
        payload, warnings = build_payload(
            task,
            canonical,
            pin_reason=args.pin_reason,
            reason=args.reason,
            allow_codex=args.allow_codex,
        )
    except ValueError as exc:
        print(f"submit_job: {exc}", file=sys.stderr)
        return 2

    for warning in warnings:
        print(f"warning: {warning}", file=sys.stderr)

    if not args.send:
        print(json.dumps(payload, indent=2, sort_keys=True))
        print(
            f"# dry run: {len(warnings)} warning(s), nothing sent. "
            "--send posts it (needs HIVE_URL and HIVE_ACTOR_URN).",
            file=sys.stderr,
        )
        return 0

    if not payload.get("submitter_urn"):
        print("submit_job: HIVE_ACTOR_URN is not set — it becomes the job's submitter_urn", file=sys.stderr)
        return 2
    try:
        answer = send(payload)
    except ValueError as exc:
        print(f"submit_job: {exc}", file=sys.stderr)
        return 2
    print(f"submitted job {answer.get('id') or answer}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
