"""Submit-time branch admission (defect 2026-10-25, nine dead jobs).

Nine jobs died because ``head_branch`` did not exist on the ARGONAS remote.
The worker discovers that only AFTER claiming: it probes ``refs/heads/<head>``
on origin, and when the ref is missing and there is no usable ``base_branch``
it fails the job with ``branch_not_found`` (hive_worker._run_job_inner). By
then a worker slot, a lease and a retry budget are gone — and when no worker at
all is eligible the job sits in ``queued`` forever, which is the other half of
"a job that cannot start".

The bus can cheaply answer the same question at SUBMIT, so it does:

  * ``head_branch`` present on the remote            -> accepted.
  * ``head_branch`` missing, ``base_branch`` present -> accepted, and the job
    records ``branch_created_from=<base>`` so the operator can see the branch
    will be created by the worker (which clones base and creates head, the
    behaviour hive_worker already implements).
  * neither present -> 422 naming the missing ref. The job is never accepted
    in a state it cannot start from.

Rules that keep this from becoming an outage or a hole:

  1. FAIL OPEN on transport trouble. ``git ls-remote`` exiting non-zero means
     ssh/auth/dns failed before the remote answered — that is not evidence the
     branch is missing (the worker makes the same distinction and returns
     "transport_error"). An unreachable remote must not reject every job.
     A transport error is NEVER cached: the next submit re-probes, so a fleet
     that recovers from a network blip does not keep failing open for a minute.
  2. Only PROBE what we can resolve. A job with no ``repo``, a repo that is not
     in the catalog, or an ambiguous catalog alias (``hive`` is an alias of
     ``graeae-hive-mind``, NOT of ``graeae-hive``) has no trustworthy remote,
     so the submit path accepts it and says so in ``warnings[]`` rather than
     guessing a URL — a wrong remote would reject valid branches.
  3. NEVER hand an untrusted string to git. Every argv element is either a
     validated branch name or a URL that passed :func:`valid_remote_url`, and
     the call runs with ``shell=False`` and a scratch config so a hostile
     catalog entry (or a hostile ``GIT_*`` env) cannot turn ``git ls-remote``
     into code execution. ``ext::``/``fd::`` transports — the known git
     remote-execution gadgets — are refused outright.

The probe result is cached per (url, ref) for a short window: a fleet of
orchestrators submitting the same branch must not turn submit into a serial
connection storm against ARGONAS.
"""

from __future__ import annotations

import os
import re
import subprocess
import time
from typing import Any
from urllib.parse import urlparse

#: Branch names we are willing to hand to git. This is git's own rule set
#: (refs cannot contain space, ``~ ^ : ? * [ \``, a control char, ``..``,
#: ``@{``, a leading/trailing ``/`` or ``.``, a trailing ``.lock``, or start
#: with ``-``) expressed as an allowlist. Anything git would accept *and* the
#: allowlist accepts is safe to pass as its own argv element; anything outside
#: the allowlist is refused rather than risked. Unicode branch names are the
#: one deliberate loss: a submitter who really needs one gets a clear 422
#: naming the ref instead of a silent mis-probe.
_BRANCH_ALLOWED = re.compile(r"^[A-Za-z0-9._/@+()\-]+$")


def _env_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
    """A bounded float from the environment, falling back on ANY bad value.

    Read at import time, so a typo in the unit file must not crash the bus on
    startup — a bad value degrades to the documented default.
    """
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    if value != value:  # NaN
        return default
    return max(minimum, min(maximum, value))


#: Timeout for a single probe. Submit is interactive; a slow NAS must not stall
#: the bus beyond a few seconds.
PROBE_TIMEOUT = _env_float("HIVE_BRANCH_PROBE_TIMEOUT", 15.0, minimum=1.0, maximum=120.0)

#: Cache window (seconds) for SUCCESSFUL probe results. 0 disables caching
#: (tests). Transport errors are never cached, whatever this says.
PROBE_CACHE_TTL = _env_float("HIVE_BRANCH_PROBE_TTL", 60.0, minimum=0.0, maximum=3600.0)

_PROBE_CACHE: dict[tuple[str, str], tuple[float, str]] = {}

#: URL schemes a catalog remote may use. No ``ext::``/``fd::``/``file://`` — the
#: first two let a remote string execute a local command, and a job submit is
#: not an excuse to read the bus host's disk.
_ALLOWED_SCHEMES = ("ssh", "https", "http", "git")


def valid_branch_name(name: str) -> bool:
    """Whether ``name`` is a git branch name that is safe to pass to git."""
    n = (name or "").strip()
    if not n or n != name or len(n) > 255:
        return False
    if not _BRANCH_ALLOWED.match(n):
        return False
    if n.startswith(("/", ".", "-")) or n.endswith(("/", ".")):
        return False
    if ".." in n or "@{" in n or "//" in n:
        return False
    if n.endswith(".lock"):
        return False
    return all(part and not part.endswith(".lock") for part in n.split("/"))


# Bare absolute paths are the ARGONAS NFS mount spelling for the fleet's bare
# repos (``/mnt/datapool/git/...``). This is a DELIBERATE local-filesystem
# exception — the bus host mounts the fleet's git server as NFS and the
# catalog entries use the mount path, not a hostname. The threat model: a
# catalog entry is operator-managed data, so the acceptance surface is the
# catalog's, and the guardrail is the prefix allowlist below (never arbitrary
# paths). ``file://`` URLs remain rejected: those are submitter-shaped input,
# and a submit is not an excuse to read the bus host's disk.
_NFS_BARE_ROOTS: tuple[str, ...] = ("/mnt/datapool/git/",)


def valid_remote_url(url: str) -> bool:
    """Whether ``url`` may be handed to ``git ls-remote`` as a remote.

    Accepts ``ssh://``, ``git@host:path`` scp-style, ``https?://`` and
    ``git://``; rejects git's helper transports (``ext::``, ``fd::``,
    ``file://``) and anything with shell metacharacters or whitespace.

    Bare absolute paths are accepted ONLY under ``_NFS_BARE_ROOTS`` (the
    ARGONAS NFS mount of the fleet's bare repos) — never arbitrary local
    paths, which is what the ``file://`` rejection is protecting against.
    """
    u = (url or "").strip()
    if not u or u != url or len(u) > 2048:
        return False
    if any(ch.isspace() or ch in ";&|$`<>\\\"'" for ch in u):
        return False
    if u.startswith("-"):
        return False
    if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*::", u):  # ext::sh -c ..., fd:://
        return False
    if re.match(r"^[A-Za-z0-9._-]+@[A-Za-z0-9._-]+:", u):  # scp-style
        return True
    if "://" in u:
        return urlparse(u).scheme.lower() in _ALLOWED_SCHEMES
    # A bare absolute path to a local bare repo (ARGONAS NFS mount spelling).
    return u.startswith(_NFS_BARE_ROOTS)


def clear_probe_cache() -> None:
    """Drop cached probe results (tests, and an operator after a force-push)."""
    _PROBE_CACHE.clear()


def _cache_get(url: str, ref: str) -> str | None:
    if PROBE_CACHE_TTL <= 0:
        return None
    hit = _PROBE_CACHE.get((url, ref))
    if not hit:
        return None
    ts, state = hit
    if time.time() - ts > PROBE_CACHE_TTL:
        _PROBE_CACHE.pop((url, ref), None)
        return None
    return state


def _cache_put(url: str, ref: str, state: str) -> None:
    # Only a DEFINITE answer is worth remembering. ``transport_error`` is a
    # statement about the network right now, not about the branch, so caching
    # it would keep failing open after the remote came back.
    if PROBE_CACHE_TTL > 0 and state in ("present", "absent"):
        _PROBE_CACHE[(url, ref)] = (time.time(), state)


def _probe_env() -> dict:
    """A git environment with the remote-execution knobs turned off.

    ``GIT_TERMINAL_PROMPT=0`` stops an auth prompt from hanging a submit, and
    ``GIT_ALLOW_PROTOCOL``/``GIT_PROTOCOL_FROM_USER=0`` keep a hostile catalog
    URL from reaching a helper transport.
    """
    env = dict(os.environ)
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GIT_PROTOCOL_FROM_USER"] = "0"
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    env.setdefault("GIT_SSH_COMMAND", "ssh -o BatchMode=yes -o StrictHostKeyChecking=no")
    return env


def probe_remote_branch(repo_url: str, branch: str) -> str:
    """Three-state existence probe for ``refs/heads/<branch>`` on ``repo_url``.

    Returns ``"present"``, ``"absent"`` or ``"transport_error"`` — the same
    vocabulary ``hive_worker._probe_remote_branch`` uses, for the same reason:
    a non-zero exit means git never got an answer, which must not be read as
    "the branch is missing".

    Every argument is validated before it reaches git, and git is invoked with
    an argv list and ``shell=False``, so no submitter-controlled string can be
    interpreted by a shell.
    """
    if not valid_remote_url(repo_url) or not valid_branch_name(branch):
        # NOT "absent": an input we refuse to probe says nothing about whether
        # the ref exists, and reporting it as absent would surface a misleading
        # "create the branch" 422 for what is really an unusable remote/ref.
        # This mirrors git never having answered.
        return "transport_error"
    ref = f"refs/heads/{branch}"
    cached = _cache_get(repo_url, ref)
    if cached is not None:
        return cached
    try:
        proc = subprocess.run(
            ["git", "ls-remote", "--heads", repo_url, ref],
            capture_output=True,
            text=True,
            env=_probe_env(),
            timeout=PROBE_TIMEOUT,
            check=False,
            shell=False,
        )
    except (OSError, subprocess.SubprocessError):
        return "transport_error"
    if proc.returncode != 0:
        return "transport_error"
    state = "present" if any(line.rstrip().endswith(ref) for line in (proc.stdout or "").splitlines()) else "absent"
    _cache_put(repo_url, ref, state)
    return state


def resolve_remote_url(repo: str | None) -> str | None:
    """The clone URL for a job's ``repo``, or None when we cannot be sure.

    Unambiguous repo ids only (exact id, or an alias that maps to EXACTLY one
    catalog entry). This is intentionally *not* the fuzzy ranker: ``hive``
    resolves fuzzily to ``graeae-hive-mind`` while the ARGONAS repo this task
    targets is ``graeae-hive`` — probing the wrong remote would reject valid
    branches, which is worse than not probing at all.

    The returned URL is additionally checked against :func:`valid_remote_url`
    so a compromised/odd catalog entry can never reach git.
    """
    from hive import repos as repo_catalog

    raw = (repo or "").strip()
    if not raw:
        return None
    try:
        catalog = repo_catalog.load_catalog()
    except Exception:  # noqa: BLE001 - no catalog: cannot resolve, do not guess
        return None
    entry = None
    if raw in catalog:
        entry = catalog[raw]
    else:
        lowered = raw.lower()
        matches = [
            candidate
            for candidate in catalog.values()
            if lowered
            in {
                candidate["id"].lower(),
                (candidate.get("display_name") or "").lower(),
                *[a.lower() for a in (candidate.get("aliases") or [])],
            }
        ]
        if len(matches) == 1:
            entry = matches[0]
    if not entry:
        return None
    for key in ("clone_ssh", "clone_nfs", "argonas_bare"):
        url = entry.get(key)
        if isinstance(url, str) and valid_remote_url(url):
            return url
    return None


def branch_admission(
    *,
    repo: str | None,
    head_branch: str | None,
    base_branch: str | None,
    prober=probe_remote_branch,
    resolver=resolve_remote_url,
) -> dict[str, Any]:
    """Decide, at submit, whether this job's branches can start.

    Returns a dict with:

      ``ok``       -- False when the job MUST be rejected (422).
      ``reason``   -- machine-readable rejection code (``missing_ref`` /
                      ``invalid_ref``).
      ``detail``   -- operator-facing sentence naming the missing ref.
      ``state``    -- ``no_head_branch`` (nothing to check out or create),
                      ``head_present``, ``head_creatable_from_base``,
                      ``unprobed``, ``unreachable``, ``invalid_ref`` or
                      ``missing_ref``.
      ``url``      -- the remote probed (None when unprobed).
      ``branch_created_from`` -- base branch the worker will create head from.
      ``warnings`` -- non-fatal notes (unprobed repo, unreachable remote).
    """
    out: dict[str, Any] = {
        "ok": True,
        "reason": None,
        "detail": None,
        "state": "unprobed",
        "url": None,
        "branch_created_from": None,
        "warnings": [],
        # Always present so callers cannot hit a KeyError on an early return;
        # None means "not determined on this path".
        "head_state": None,
        "base_state": None,
    }
    head = (head_branch or "").strip() or None
    base = (base_branch or "").strip() or None
    for label, value in (("head_branch", head), ("base_branch", base)):
        if value is not None and not valid_branch_name(value):
            out.update(
                ok=False,
                reason="invalid_ref",
                state="invalid_ref",
                detail=(
                    f"{label} {value!r} is not a valid git branch name; a job whose "
                    "branch cannot be checked out can never start"
                ),
            )
            return out
    if head is None:
        # No head_branch at all: the worker clones the remote's default branch.
        # This is NOT "the head ref is present" -- a distinct state keeps the
        # submit response honest about what was actually checked.
        out["state"] = "no_head_branch"
        return out

    url = resolver(repo) if repo else None
    if not url:
        out["state"] = "unprobed"
        out["warnings"].append(
            f"branch_not_verified: no remote could be resolved for repo={repo!r}; "
            f"head_branch={head!r} was not checked against origin"
        )
        return out
    out["url"] = url

    head_state = prober(url, head)
    base_state = prober(url, base) if base else "absent"
    if head_state == "transport_error" or base_state == "transport_error":
        out["state"] = "unreachable"
        ref = head if head_state == "transport_error" else base
        out["warnings"].append(
            f"branch_not_verified: could not reach {url} to check {ref!r} (transport error); "
            "the job was accepted unchecked"
        )
        return out
    out["head_state"] = head_state
    out["base_state"] = base_state if base else None
    if head_state == "present":
        out["state"] = "head_present"
        return out
    # head_branch is missing on origin: the only way to start is to create it
    # from a base branch that DOES exist there.
    if base and base_state == "present":
        out["state"] = "head_creatable_from_base"
        out["branch_created_from"] = base
        return out
    missing = [f"head_branch={head!r}"]
    if base and base_state != "present":
        missing.append(f"base_branch={base!r}")
    out.update(
        ok=False,
        reason="missing_ref",
        state="missing_ref",
        detail=(
            f"ref not found on remote: {', '.join(missing)}. Create the branch (or push it) "
            "before submitting, or pass a base_branch that exists on origin so the "
            "worker can create head_branch from it — a job whose branch cannot be "
            "checked out can never start"
        ),
    )
    return out
