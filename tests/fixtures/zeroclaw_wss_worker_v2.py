#!/usr/bin/env python3
"""ZeroClaw WSS Worker v2 — workspace-aware + commit-verifying.

POLICY (user 2026-05-26): WSS via gateway open-weight models is DEFAULT.
Codex is a scarce resource reserved for:
  - kind=codex          (explicit)
  - kind=review:*       (adversarial review)
  - kind=doctor:codex-fix:* (doctor escalations)

Both paths run inside a real per-kind git workspace (auto-cloned on first
use), and any reported commit SHA is verified via `git cat-file -e`.
"""
import json
import logging
import os
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path

import fcntl

sys.path.insert(0, str(Path(__file__).parent))
import wss_driver  # noqa: E402

HIVE_URL = os.environ.get("HIVE_URL", "http://192.0.2.67:5005")
# C1: bus transport auth — every request carries this bearer token (the bus now
# rejects unauthenticated /v1/* + /mcp). Sourced from env, never baked in.
HIVE_BUS_TOKEN = os.environ.get("HIVE_BUS_TOKEN", "")
# MNEMOS — the commit-DAG store. The worker records every meaningful completed
# job + its commits here so OTHER agents can cross-reference prior work and
# link future work (GRAEAE-blessed arch 2026-06-01). On the Spark this is the
# tunnelled fleet MNEMOS at localhost:5012; on fleet hosts it's PYTHIA:5002.
MNEMOS_URL = os.environ.get("MNEMOS_URL", "http://192.0.2.67:5002")
MNEMOS_TOKEN = os.environ.get("MNEMOS_TOKEN", "")
AGENT_HOST = os.environ.get("AGENT_HOST") or socket.gethostname().split(".")[0]
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "30"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "15"))
GATEWAY_HOST = os.environ.get("GATEWAY_HOST", "127.0.0.1")
GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", "42617"))
JOB_TIMEOUT = float(os.environ.get("JOB_TIMEOUT", "900"))
INSTANCE = os.environ.get("ZEROCLAW_INSTANCE_ID") or os.environ.get("INSTANCE", "1")
# Gateway agent loop cap. Default 10 is too low — real repo jobs (read+grep+
# edit+test+commit) routinely exceed it and die "max tool iterations". 40 lets
# them finish; the JOB_TIMEOUT wall still bounds runaway loops.
MAX_TOOL_ITERS = int(os.environ.get("MAX_TOOL_ITERS", "40"))

# ── NATIVE build executor (operator 2026-06-16) ──────────────────────────────
# CODE/build kinds run on the isolated, jailed, fs-diff-verified native executor
# (zc_native_build) instead of the WSS gateway loop. WSS could not reliably land
# commits and was silently no-opping code/security jobs via its workspace-less
# "NON-CODE" preamble; the native executor clones per-job, runs a sandboxed agent
# pinned to a throwaway export dir, verifies real filesystem changes, and the
# HARNESS (never the agent) does all git commit+push to a fresh zcbuild/<jid>
# branch. WSS is now reserved for NON-CODE (analysis/answer/review) kinds.
NATIVE_BUILD_ALIAS = os.environ.get("ZC_BUILD_ALIAS", "hive_ngc_1")
NATIVE_JOB_TIMEOUT = float(os.environ.get("NATIVE_JOB_TIMEOUT", "1800"))
LEASE_RENEW_INTERVAL = max(5.0, float(os.environ.get("HIVE_LEASE_RENEW_INTERVAL", "60")))
COMPLETION_OUTBOX = Path(os.environ.get(
    "HIVE_COMPLETION_OUTBOX",
    os.path.expanduser(f"~/.zeroclaw/hive_completion_outbox.{INSTANCE}.json"),
))
# Bound how much durable recovery work can monopolize one idle poll.  Entries
# remain on disk for subsequent passes, and operators can tune the generous
# default for their outage/recovery profile.
COMPLETION_OUTBOX_BATCH_SIZE = max(
    1, int(os.environ.get("HIVE_COMPLETION_OUTBOX_BATCH_SIZE", "10"))
)
_COMPLETION_OUTBOX_CURSOR = 0
# Explicit path to the native executor module; falls back to well-known
# locations. Lets ops swap the executor without editing the worker.
NATIVE_EXECUTOR_PATH = os.environ.get("ZC_BUILD_ONESHOT_PATH", "").strip()

# Non-commit kinds: analysis/answer/review work that produces TEXT, not a repo
# change. When such a kind has no workspace mapping, run it workspace-less
# (dispatch to the gateway, accept the text answer as done) instead of
# fail-looping on no_workspace_for_kind. Anything NOT in this set that lacks a
# mapping is assumed to need a repo -> cancelled terminal (needs a mapping),
# never silently "done" (which would fake-complete a code job).
NONCOMMIT_KIND_PREFIXES = (
    "review", "architecture", "triage", "docs:", "investigation",
    "track:", "ops:", "diag:", "ping:", "hive-stats", "dream-walker",
    "research", "analysis", "design",
)

# Provider-kind aliases that are served by this worker and must be removed
# together when a workspace-less job is handed back to an orchestrator.
_ZEROCLAW_PROVIDER_ALIASES = frozenset({"zoder"})

WORKSPACE_ROOT = Path(os.path.expanduser("~/codex-workspace")) / f"inst{INSTANCE}"  # per-instance isolation
WORKSPACE_ROOT.mkdir(parents=True, exist_ok=True)

# Codex = ADVERSARIAL/REVIEW stage only — NOT the implementer (operator
# 2026-06-02). Implementation/coding runs on the open-weight WSS chains
# (deepseek/groq/together/xai/gemini); codex is reached only for the
# review/verify step. These prefixes are the ONLY kinds that may resolve to a
# codex-led registry: explicit `codex`, adversarial `review:`, and the doctor's
# `doctor:codex-fix` verify escalation. Coding-ish kinds (fix:/refactor:/perf:/
# security:/audit:) are DELIBERATELY excluded here so they implement on
# open-weight and only their review phase touches codex. `codex` itself stays
# OUT of TIER_CHAINS / MODEL_REGISTRIES general chains (see TIER_CHAINS notes).
CODEX_REVIEW_KINDS_PREFIXES = ("codex", "review:", "doctor:codex-fix")
# Back-compat alias (older name); same value — codex is review-only either way.
CODEX_KINDS_PREFIXES = CODEX_REVIEW_KINDS_PREFIXES

# kind-prefix → (subdir under ~/codex-workspace, git_url-or-None)
KIND_WORKSPACE_MAP = {
    "argonaut:dbpr-site-scaffold-single-multivertical": ("florida-licenses", None),
    "argonaut:dbpr-platform-bootstrap-with-workspace": ("florida-licenses", None),
    "argonaut:dbpr-platform-bootstrap": ("florida-licenses", None),
    "argonaut:dbpr-discovery-sweep-35-categories": ("florida-licenses", None),
    "argonaut:":        ("florida-licenses", None),
    "fix:codex-pro-oauth-verify": ("mnemos",         "https://gitlab.com/mnemos-os/mnemos.git"),
    # knemon (router), oracle-backend, provider-model sync all live in the mnemos
    # codebase — route feat:/fix: variants there so they run+commit instead of
    # being cancelled for no workspace (operator 2026-06-01: high-value backlog).
    "feat:knemon":      ("mnemos",         "https://gitlab.com/mnemos-os/mnemos.git"),
    "fix:knemon":       ("mnemos",         "https://gitlab.com/mnemos-os/mnemos.git"),
    "feat:oracle-backend": ("mnemos",      "https://gitlab.com/mnemos-os/mnemos.git"),
    "fix:sync-provider-models": ("mnemos", "https://gitlab.com/mnemos-os/mnemos.git"),
    "fix:knemon-cost":  ("mnemos",         "https://gitlab.com/mnemos-os/mnemos.git"),
    "mnemos:":          ("mnemos",         "https://gitlab.com/mnemos-os/mnemos.git"),
    "riskybiz:p1-sunbiz-entity-resolver": ("florida-licenses", None),
    "riskybiz:":        ("florida-licenses", None),
    "riskyeats:":       ("riskyeats",      "https://gitlab.com/perlowja/riskyeats.git"),
    "investorclaw:":    ("ic-engine",      "https://gitlab.com/argonautsystems/ic-engine.git"),
    "ic-engine:":       ("ic-engine",      "https://gitlab.com/argonautsystems/ic-engine.git"),
    "ncz-os-zeroclaw:": ("zeroclaw",       "https://gitlab.com/nclawzero/zeroclaw.git"),
    "ncz-os-openclaw:": ("openclaw",       "https://gitlab.com/nclawzero/openclaw.git"),
    "ncz-os-":          ("ncz-installer",  "https://gitlab.com/nclawzero/ncz-installer.git"),
    "cixmini-os:":      ("cix-installer",  "https://gitlab.com/nclawzero/cix-installer.git"),
    "fleet-infra:":     ("fleet-ops",      None),
}

# Hosts with limited compute — only accept light kinds.
NARROW_HOSTS = {"cixmini", "bigpi", "clawpi", "zeropi"}
NARROW_ALLOWLIST = ("cixmini-os:", "ncz-os-", "fleet-infra:")
_HOST_LOWER = (AGENT_HOST or "").lower()

# Provider fallback chains by job class + cost. On a retryable failure
# (timeout / throttle / rate-limit / quota / provider-error), the worker
# dispatches the job to the NEXT LLM in the chain. Spans xai, deepseek,
# together, groq, siliconflow, nvidia (NGC). (gemini omitted — no
# hive_gemini agent alias exists yet; add one to include it.)
TIER_CHAINS = {
    "A": ["hive_deepseek_pro_1", "hive_xai_1", "hive_together_1",
          "hive_siliconflow_1", "hive_groq_1", "hive_nvidia_1"],
    "B": ["hive_deepseek_1", "hive_groq_1", "hive_siliconflow_1",
          "hive_together_1", "hive_xai_1", "hive_nvidia_1"],
    "C": ["hive_groq_1", "hive_deepseek_1", "hive_siliconflow_1",
          "hive_together_1", "hive_nvidia_1"],
}

# ── COST-SAVINGS ARCHITECTURE (operator 2026-06-01) ──────────────────────────
# The doctor classifies each job by task-type/complexity into a cost-tiered
# model REGISTRY; the worker dispatches to the cheapest-adequate model first and
# escalates on failure. Each alias' actual model is configured gateway-side
# (model_provider). Cheap models (deepseek-flash/nova/groq) carry high-volume
# simple work; PRO (codex gpt-5.3-codex OAuth $0, gpt-5-pro, deepseek-pro)
# handles complex code; NVIDIA (NGC + GB10 local) runs on the Spark.
# Registries are cheapest-first. Until open-weight provider keys are revalidated
# fleet-side they fall back to codex (hive_groq_1 -> openai.codex); the
# classification is correct now and real savings activate as cheap providers
# and the Spark come online.
# NOTE 2026-06-01: open-weight provider keys are BROKEN fleet-side (deepseek/
# groq/nvidia/etc fall back to gpt-4o, rate-limited). Until they are re-keyed,
# every registry LEADS with codex (hive_groq_1 -> openai.codex, gpt-5.3-codex
# OAuth $0) so jobs run on the one working high-quality model instead of
# falling to gpt-4o. The cheap aliases stay as later escalation slots and will
# activate (real cost savings) once their keys are validated.
# 2026-06-01: open-weight provider keys are BROKEN fleet-side. Escalating into
# those aliases makes the gateway fall back to an EMBEDDING model (bge-small) or
# return empty -> code jobs fail. Until the cheap providers are re-keyed, every
# registry uses ONLY the two WORKING models: hive_groq_1 -> openai.codex
# (gpt-5.3-codex OAuth $0) and hive_deepseek_2 -> openai.gpt5_2_pro (gpt-5-pro).
# The cost-savings classification framework stays; the model list collapses to
# what actually serves. Re-expand the cheap tiers once their keys validate.
_WORKING = ["hive_groq_1", "hive_deepseek_2"]
MODEL_REGISTRIES = {
    # WEAK + NON-LOCAL MODELS REMOVED (operator 2026-06-01). Removed: groq 8B/17B/32B/
    # 70B-llama (weak + rate-limited), siliconflow (invalid key), and NVIDIA/NGC
    # (Spark-ONLY per directive; the GB10 Spark is not yet integrated/tested with the
    # current mnemos+hive fixes). codex stays OUT of the general chain (scarce $100 OAuth
    # pool; reached only via kind=codex/review/doctor). Capable, validated, allowed coder
    # for non-Spark workers = deepseek-direct ONLY: v4-pro for code, v4-flash for triage.
    # Multiple aliases per tier spread load across the deepseek-direct account.
    "cheapo": ["hive_deepseek_3", "hive_deepseek_4", "hive_deepseek_pro_2"],
    "medium": ["hive_deepseek_pro_2", "hive_deepseek_pro_3", "hive_deepseek_pro_4"],
    "pro":    ["hive_deepseek_pro_2", "hive_deepseek_pro_3", "hive_deepseek_pro_4", "hive_deepseek_pro_5"],
    "nvidia": ["hive_deepseek_pro_2"],
}
# Task-type -> registry (the doctor's classification dictionary). Longest-prefix
# match wins. Complex code/reasoning -> pro; standard code/ops -> medium;
# simple/high-volume/triage -> cheapo; data/nvidia-class -> nvidia (Spark).
TASK_REGISTRY_MAP = {
    "review": "pro", "architecture": "pro", "design": "pro",
    "feat:": "pro", "mnemos:": "pro", "riskyeats:": "pro", "doctor:": "pro",
    "ncz-os-zeroclaw:": "pro",
    "fix:": "medium", "fleet-infra:": "medium", "ncz-os": "medium",
    "cixmini-os:": "medium",
    "argonaut:": "pro", "riskybiz:": "cheapo", "triage": "cheapo",
    "diag:": "cheapo", "ping:": "cheapo", "research": "cheapo",
    "track:": "cheapo", "ops:": "cheapo", "hive-stats": "cheapo",
}


# KEY HEALTH MONITOR (operator 2026-06-01): aliases whose provider HARD-fails
# (bad key/endpoint/connection — not a transient throttle) get a cooldown so the
# escalation chain skips them instead of wasting a dispatch on a known-dead
# model. alias -> unix-ts the cooldown expires. Throttles do NOT cooldown (they
# are expected + transient, e.g. codex's hourly OAuth action limit).
_ALIAS_COOLDOWN: dict[str, float] = {}
ALIAS_COOLDOWN_SEC = float(os.environ.get("ALIAS_COOLDOWN_SEC", "600"))


def _alias_cooled(alias: str) -> bool:
    exp = _ALIAS_COOLDOWN.get(alias)
    return bool(exp and time.time() < exp)


# --- WORKAROUND for zeroclaw #7066 (gateway WS frame reports the FIRST configured
# provider, not the actual routed model). Resolve the alias we dispatched to its
# REAL model_provider + model from config so usage_ledger/dashboard attribute
# cost to the true provider, not openai. Remove once a build with #7066 ships.
_ALIAS_REAL_CACHE = {}
def _alias_real_provider(alias):
    """Resolve a hive alias -> (provider_family, model) from the daemon's REAL
    config so completion PATCHes attribute cost to the provider that actually
    ran the job (deepseek/groq/together/xai/gemini/siliconflow/bedrock), not the
    worker's registration default. Reads the central config-dir the daemon loads
    first, then the legacy ~/.zeroclaw path. Returns the provider FAMILY (the
    part before the '.' in model_provider, e.g. "deepseek" from "deepseek.v4_pro")
    which is what KNEMON cost-stats group + price on."""
    if not _ALIAS_REAL_CACHE:
        cfg = None
        for _p in ("/etc/nclawzero/zeroclaw/config.toml",
                   os.path.expanduser("~/.zeroclaw/config.toml")):
            try:
                cfg = open(_p).read(); break
            except Exception:
                continue
        try:
            prov_model = {}
            if cfg:
                for m in re.finditer(r"\[providers\.models\.([a-z0-9_.]+)\]\n((?:(?!\[).*\n)*)", cfg):
                    mm = re.search(r"^model = \"([^\"]+)\"", m.group(2), re.M)
                    prov_model[m.group(1)] = mm.group(1) if mm else m.group(1)
                for m in re.finditer(r"\[agents\.([a-z0-9_]+)\]\n((?:(?!\[).*\n)*)", cfg):
                    mp = re.search(r"model_provider = \"([^\"]+)\"", m.group(2))
                    if mp:
                        prof = mp.group(1)                 # e.g. "deepseek.v4_pro"
                        family = prof.split(".", 1)[0]     # cost/display key: "deepseek"
                        _ALIAS_REAL_CACHE[m.group(1)] = (family, prov_model.get(prof, prof))
            if not _ALIAS_REAL_CACHE:
                _ALIAS_REAL_CACHE["__err__"] = (None, None)
        except Exception:
            _ALIAS_REAL_CACHE["__err__"] = (None, None)
    return _ALIAS_REAL_CACHE.get(alias, (None, None))


def classify_registry(kind: str) -> str:
    """Doctor classification: job kind -> cost-tiered model registry name."""
    best = None
    for prefix, reg in TASK_REGISTRY_MAP.items():
        if (kind or "").startswith(prefix) and (best is None or len(prefix) > len(best[0])):
            best = (prefix, reg)
    return best[1] if best else "medium"

# Per-host provider lock. When HIVE_PROVIDER_LOCK is set (e.g. "nvidia" on the
# NVIDIA Spark, which must use NGC keys only), every tier chain is restricted
# to that provider's aliases — no fallback to other LLMs. Empty (default) keeps
# the full cross-provider fallback above.
_PROVIDER_LOCK = os.environ.get("HIVE_PROVIDER_LOCK", "").strip().lower()
if _PROVIDER_LOCK:
    TIER_CHAINS = {
        _t: ([a for a in _chain if _PROVIDER_LOCK in a] or [f"hive_{_PROVIDER_LOCK}_1"])
        for _t, _chain in TIER_CHAINS.items()
    }

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [zc-wss-worker@%(process)d] %(message)s")
log = logging.getLogger("wss-worker")


def http(method, path, body=None, timeout=20):
    url = HIVE_URL + path
    data = json.dumps(body).encode() if body is not None else None
    headers = {"content-type": "application/json"}
    if HIVE_BUS_TOKEN:
        headers["authorization"] = f"Bearer {HIVE_BUS_TOKEN}"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            txt = r.read()
            return r.status, (json.loads(txt) if txt else {})
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read())
        except Exception:
            body = {"error": str(e)}
        return e.code, body
    except Exception as e:
        return 0, {"error": str(e)}


def register():
    body = {
        "host": AGENT_HOST, "kind": "zeroclaw", "runtime": "zeroclaw",
        "autonomy_level": "interactive", "auth_method": "api",
        # '*' = claim ANY job regardless of required_capabilities / workspace
        # capability (operator architecture 2026-06-01: all executors eligible
        # for all jobs; job-type -> KNEMON picks the model). The named caps stay
        # for display/scoring; '*' is what bypasses the claim-time cap gate.
        "capabilities": ["*", "code-edit","build","test","debug","refactor",
                         "python","bash","docker","linux",
                         "wss-driven","workspace-aware","open-weights-first"],
        "model": "wss-via-gateway+codex-fallback",
        "provider": "openai",
    }
    code, resp = http("POST", "/v1/agents/register", body, timeout=15)
    if code in (200, 201):
        urn = resp.get("urn")
        log.info("registered urn=%s host=%s narrow=%s",
                 urn, AGENT_HOST, _HOST_LOWER in NARROW_HOSTS)
        return urn
    log.error("register failed code=%s resp=%s", code, resp)
    return None


def heartbeat(urn):
    code, _ = http("POST", "/v1/agents/heartbeat", {"urn": urn}, timeout=10)
    return code in (200, 201, 204)


def claim_next(urn):
    code, resp = http("POST", f"/v1/jobs/next?agent_urn={urn}", timeout=15)
    if code in (200, 201) and isinstance(resp, dict) and resp.get("id"):
        return resp
    return None


def patch_job(jid, urn, status, result=None):
    if status != "running":
        _stop_lease_renewal(jid)
    body = {"status": status, "claimed_by": urn}
    if result is not None:
        body["result"] = result
    code, resp = http("PATCH", f"/v1/jobs/{jid}", body, timeout=15)
    if code not in (200, 201, 204):
        log.warning("patch %s -> %s: %s", jid, code, str(resp)[:200])
    return code in (200, 201, 204)


_LEASE_RENEWALS = {}
_LEASE_RENEWALS_LOCK = threading.Lock()


def _start_lease_renewal(jid, urn):
    stop = threading.Event()

    def _renew():
        while not stop.wait(LEASE_RENEW_INTERVAL):
            if not patch_job(jid, urn, "running"):
                log.warning("lease renewal failed for %s; will retry", jid)

    thread = threading.Thread(target=_renew, name=f"lease-{jid[:12]}", daemon=True)
    with _LEASE_RENEWALS_LOCK:
        prior = _LEASE_RENEWALS.pop(jid, None)
        _LEASE_RENEWALS[jid] = (stop, thread)
    if prior:
        prior[0].set()
    thread.start()


def _stop_lease_renewal(jid):
    with _LEASE_RENEWALS_LOCK:
        renewal = _LEASE_RENEWALS.pop(jid, None)
    if renewal:
        renewal[0].set()


def _read_completion_outbox_unlocked():
    try:
        data = json.loads(COMPLETION_OUTBOX.read_text())
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        # Genuinely no outbox yet -- an empty dict is the correct answer.
        return {}
    except Exception as exc:
        # Anything else (corrupt JSON, permissions, partial write) must NOT
        # degrade to {}. _update_completion_outbox does read -> update -> write,
        # so returning {} here makes the following write overwrite the file with
        # only the new entry, destroying every pending completion. Fail loudly
        # and let the caller abort with the file intact.
        log.error("completion outbox read failed, refusing to overwrite: %s", exc)
        raise


def _write_completion_outbox_unlocked(entries):
    try:
        COMPLETION_OUTBOX.parent.mkdir(parents=True, exist_ok=True)
        tmp = COMPLETION_OUTBOX.with_name(
            f"{COMPLETION_OUTBOX.name}.tmp.{os.getpid()}.{threading.get_ident()}"
        )
        tmp.write_text(json.dumps(entries, separators=(",", ":")))
        tmp.replace(COMPLETION_OUTBOX)
        return True
    except Exception as exc:
        log.critical("completion outbox write failed: %s", exc)
        return False


@contextmanager
def _completion_outbox_lock():
    COMPLETION_OUTBOX.parent.mkdir(parents=True, exist_ok=True)
    lock_path = COMPLETION_OUTBOX.with_name(COMPLETION_OUTBOX.name + ".lock")
    with lock_path.open("a+") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _read_completion_outbox():
    with _completion_outbox_lock():
        return _read_completion_outbox_unlocked()


def _write_completion_outbox(entries):
    with _completion_outbox_lock():
        return _write_completion_outbox_unlocked(entries)


def _update_completion_outbox(update):
    """Run one interprocess-safe read/modify/write transaction."""
    with _completion_outbox_lock():
        entries = _read_completion_outbox_unlocked()
        update(entries)
        return _write_completion_outbox_unlocked(entries)


def _completion_is_acknowledged(jid, urn, status, claimed_retry_count=None):
    code, current = http("GET", f"/v1/jobs/{jid}", timeout=15)
    if code != 200 or not isinstance(current, dict):
        return False
    if current.get("status") == status and current.get("claimed_by") == urn:
        return True
    return (
        status == "failed"
        and claimed_retry_count is not None
        and current.get("status") == "queued"
        and current.get("claimed_by") is None
        and current.get("retry_count") == claimed_retry_count + 1
    )


def persist_terminal_job(jid, urn, status, result, *, claimed_retry_count=None, save_on_failure=True):
    """Persist an authoritative terminal state or durably retain it for retry."""
    _stop_lease_renewal(jid)
    for delay in (0, 1, 2, 4):
        if delay:
            time.sleep(delay)
        if patch_job(jid, urn, status, result) or _completion_is_acknowledged(
                jid, urn, status, claimed_retry_count):
            _update_completion_outbox(lambda entries: entries.pop(jid, None))
            return True
    if save_on_failure:
        entry = {"jid": jid, "urn": urn, "status": status, "result": result,
                 "claimed_retry_count": claimed_retry_count,
                 "saved_at": time.time()}
        _update_completion_outbox(lambda entries: entries.__setitem__(jid, entry))
    return False


def flush_completion_outbox():
    global _COMPLETION_OUTBOX_CURSOR
    entries = [
        entry for entry in _read_completion_outbox().values()
        if isinstance(entry, dict)
    ]
    entry_count = len(entries)
    if entry_count > COMPLETION_OUTBOX_BATCH_SIZE:
        start = _COMPLETION_OUTBOX_CURSOR % entry_count
        entries = (entries[start:] + entries[:start])[:COMPLETION_OUTBOX_BATCH_SIZE]
        _COMPLETION_OUTBOX_CURSOR = (start + COMPLETION_OUTBOX_BATCH_SIZE) % entry_count
    for entry in entries:
        persist_terminal_job(entry.get("jid"), entry.get("urn"), entry.get("status"),
                             entry.get("result"),
                             claimed_retry_count=entry.get("claimed_retry_count"),
                             save_on_failure=False)


def _mnemos_http(method, path, body=None, timeout=15):
    if not MNEMOS_TOKEN:
        return 0, {"error": "MNEMOS_TOKEN is not configured"}
    url = MNEMOS_URL + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"content-type": "application/json",
                 "authorization": f"Bearer {MNEMOS_TOKEN}"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            txt = r.read()
            return r.status, (json.loads(txt) if txt else {})
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, {"error": str(e)}
    except Exception as e:
        return 0, {"error": str(e)}


def _record_job_to_mnemos(job, result, status):
    """Land a referenceable commit-DAG node in MNEMOS so other agents see this
    work (prior) and new jobs can link their lineage (future). Per GRAEAE arch
    2026-06-01: memory node ALWAYS (authoritative), KG triples BEST-EFFORT
    (the /v1/kg/triples pool is intermittently unavailable — never block on it).
    Records only meaningful outcomes: a real commit, or a 'done' deliverable.
    Junk cancels are skipped to keep the graph clean."""
    try:
        commits = result.get("commits") or []
        if status != "done" and not commits:
            return
        jid = job.get("id", "")
        kind = job.get("kind", "") or ""
        parent = job.get("parent_job_id") or ""
        files = (result.get("files_changed") or [])[:25]
        ws = result.get("workspace")
        repo = Path(ws).name if ws else (result.get("repo") or "no-repo")
        alias = (result.get("alias_tried") or [result.get("gateway_model", "")])[-1]
        summary = (result.get("full_response") or result.get("stdout_preview")
                   or result.get("error") or "")[:700]
        content = (
            f"HIVE JOB {jid} [{kind}] -> {status} on {AGENT_HOST}\n"
            f"repo={repo} alias={alias} commits={commits or 'none'}\n"
            f"files={files}\n"
            + (f"derives_from job {parent}\n" if parent else "")
            + f"\n{summary}"
        )
        meta = {
            "type": "project",
            "tags": ["hive-job", "commit-dag", (kind.split(':', 1)[0] or "job"), repo],
            "job_id": jid, "parent_job_id": parent, "kind": kind,
            "status": status, "repo": repo, "host": AGENT_HOST,
            "commits": commits, "files_changed": files,
        }
        code, resp = _mnemos_http("POST", "/v1/memories",
                                  {"content": content, "metadata": meta}, timeout=12)
        if code in (200, 201):
            log.info("  recorded commit-DAG node %s -> mnemos %s",
                     jid[:12], resp.get("id", ""))
        else:
            log.warning("  mnemos memory write %s: %s", code, str(resp)[:120])
        # DAG edges — best-effort. Skip silently on pool-down / any error.
        # Native executor commits are dicts ({sha,subject}); WSS commits are bare
        # SHA strings. Normalize to a SHA so triples are clean either way.
        commit_shas = [c.get("sha") if isinstance(c, dict) else c for c in commits]
        commit_shas = [c for c in commit_shas if c]
        triples = [(f"job:{jid}", "has_kind", kind)]
        for c in commit_shas:
            triples += [(f"job:{jid}", "produced", f"commit:{c}"),
                        (f"commit:{c}", "in_repo", repo)]
            for f in files:
                triples.append((f"commit:{c}", "touches", f))
        if parent:
            triples.append((f"job:{jid}", "derives_from", f"job:{parent}"))
        for s, p, o in triples:
            _mnemos_http("POST", "/v1/kg/triples",
                         {"subject": s, "predicate": p, "object": o}, timeout=6)
    except Exception as e:
        log.warning("  mnemos record skipped: %s", e)


def pick_agent_alias(tier, kind):
    chain = TIER_CHAINS.get((tier or "C").upper(), TIER_CHAINS["C"])
    return chain[0]


def _git(workspace, *args, timeout=30):
    try:
        return subprocess.run(["git", "-C", str(workspace), *args],
                              capture_output=True, text=True, timeout=timeout)
    except Exception:
        class _R: returncode = 1; stdout = ""; stderr = "git-exec-failed"
        return _R()


def _verify_commit(workspace, sha):
    if not (isinstance(sha, str) and re.fullmatch(r"[a-f0-9]{7,40}", sha)):
        return False
    r = _git(workspace, "cat-file", "-e", f"{sha}^{{commit}}", timeout=5)
    return r.returncode == 0


def _sha_on_remote(workspace, remote, sha):
    """Strict: confirm `sha` is the OBJECT ID (col 1) of some ref on `remote`.

    This is the anti-FAKE-completion check — a local `git push` returning 0 is
    NOT proof the commit landed; only the remote ref listing is. Matching col-1
    avoids false positives where the SHA appears inside a ref name. Defensive:
    any failure -> False.
    """
    if not (isinstance(sha, str) and re.fullmatch(r"[a-f0-9]{40}", sha)):
        return False
    lr = _git(workspace, "ls-remote", remote, timeout=60)
    if lr.returncode != 0:
        return False
    for line in (lr.stdout or "").splitlines():
        parts = line.split()
        if parts and parts[0] == sha:
            return True
    return False


COMMIT_MANDATORY_PREFIXES = ("feat:", "fix:", "argonaut:", "riskyeats:", "riskybiz:")


def _ensure_origin(path, subdir):
    """Set origin from ~/.zeroclaw/workspace_remotes.json {subdir:url} (untracked,
    token-bearing) so worker commits can push. No token in code (directive 14)."""
    try:
        import json as _json
        _remotes = _json.load(open(os.path.expanduser("~/.zeroclaw/workspace_remotes.json")))
    except Exception:
        return
    _url = _remotes.get(subdir)
    if not _url:
        return
    _cur = _git(path, "remote", "get-url", "origin", timeout=5)
    if _cur.returncode != 0:
        _git(path, "remote", "add", "origin", _url, timeout=5)
    elif _cur.stdout.strip() != _url:
        _git(path, "remote", "set-url", "origin", _url, timeout=5)


def _clone_url(subdir, git_url):
    """Resolve an auth'd clone URL from ~/.zeroclaw/workspace_remotes.json (token-bearing,
    synced to every worker via deploy.sh) so ANY host can clone private repos — not just
    hosts with a git credential helper. Falls back to the tokenless map URL."""
    try:
        import json as _json
        _r = _json.load(open(os.path.expanduser("~/.zeroclaw/workspace_remotes.json")))
        return _r.get(subdir) or git_url
    except Exception:
        return git_url


def _ensure_workspace(subdir, git_url, kind):
    path = WORKSPACE_ROOT / subdir
    git_dir = path / ".git"
    if git_dir.is_dir():
        _git(path, "fetch", "--quiet", "--all", timeout=60)
        _ensure_origin(path, subdir)
        return path, None
    if git_url is None:
        path.mkdir(parents=True, exist_ok=True)
        r = subprocess.run(
            ["git", "init", "--quiet", str(path)],
            capture_output=True, text=True, timeout=30,
        )
        if r.returncode != 0:
            return None, f"git_init_failed:{r.stderr[:200]}"
        _git(path, "config", "user.name",  "Jason Perlow", timeout=5)
        _git(path, "config", "user.email", "jperlow@gmail.com", timeout=5)
        _ensure_origin(path, subdir)
        log.info("created local git workspace: kind=%s path=%s", kind, path)
        return path, None
    log.info("first-use clone: kind=%s url=%s → %s", kind, git_url, path)
    r = subprocess.run(
        ["git", "clone", "--quiet", "--depth", "50", _clone_url(subdir, git_url), str(path)],
        capture_output=True, text=True, timeout=180,
    )
    if r.returncode != 0:
        return None, f"clone_failed:{r.stderr[:200]}"
    _git(path, "config", "user.name",  "Jason Perlow", timeout=5)
    _git(path, "config", "user.email", "jperlow@gmail.com", timeout=5)
    return path, None


def _resolve_workspace(kind, description):
    if _HOST_LOWER in NARROW_HOSTS and not any(
        kind.startswith(p) for p in NARROW_ALLOWLIST
    ):
        return None, f"host_declines_kind:{_HOST_LOWER}"

    if kind.startswith("doctor:codex-fix"):
        m = re.search(r"\brepo:\s*([a-z0-9_\-]+)", (description or ""), re.I)
        if not m:
            return None, "doctor_fix_missing_repo_hint"
        repo = m.group(1).lower()
        for prefix, (subdir, git_url) in KIND_WORKSPACE_MAP.items():
            if subdir.lower() == repo:
                return _ensure_workspace(subdir, git_url, kind)
        return None, f"no_workspace_for_repo:{repo}"

    best = None
    for prefix, val in KIND_WORKSPACE_MAP.items():
        if kind.startswith(prefix) and (best is None or len(prefix) > len(best[0])):
            best = (prefix, val)
    if best is None:
        # Non-commit kind with no mapping -> run workspace-less (text answer).
        # Sentinel werr "noncommit_workspaceless" tells process_job to dispatch
        # with workspace=None rather than treat it as a hard failure.
        if any(kind.startswith(p) for p in NONCOMMIT_KIND_PREFIXES):
            return None, "noncommit_workspaceless"
        # B (2026-06-02): zeroclaw is the MAIN worker — handle generic repo-dev
        # jobs by cloning the repo named in a `repo:` hint, then running the
        # OPEN-WEIGHT WSS chain on it (NO codex, NO claude). Only fall through to
        # the orchestrator release when there is genuinely no repo to work in.
        m = re.search(r"\brepo:\s*([A-Za-z0-9_./:\-]+)", (description or ""), re.I)
        if m:
            ref = m.group(1).strip().rstrip("/")
            if ref.startswith("http") or ref.startswith("git@"):
                sub = re.sub(r"[^A-Za-z0-9_-]", "-", ref.split("/")[-1].replace(".git", ""))[:40]
                return _ensure_workspace(sub, ref, kind)
            for _p, (_sub, _url) in KIND_WORKSPACE_MAP.items():
                if _sub.lower() == ref.lower() and _url:
                    return _ensure_workspace(_sub, _url, kind)
            sub = re.sub(r"[^A-Za-z0-9_-]", "-", ref)[:40]
            return _ensure_workspace(sub, "https://gitlab.com/argonautsystems/" + ref + ".git", kind)
        return None, "no_workspace_for_kind"
    prefix, (subdir, git_url) = best
    return _ensure_workspace(subdir, git_url, kind)


def _augment_task_for_wss(task, workspace, kind):
    """Prepend a workspace+host preamble so the agent knows where to work."""
    fleet_ips = (
        "PYTHIA=192.0.2.67 TYPHON=192.0.2.61 "
        "CERBERUS=192.0.2.96 ARGONAS=192.0.2.101"
    )
    if workspace is None:
        # Workspace-less (non-commit) job: produce a written answer, no repo.
        return (
            f"[WORKER CONTEXT]\n"
            f"You are operating on host {AGENT_HOST} as user jasonperlow.\n"
            f"This is a NON-CODE job (kind: {kind}) — produce a written "
            f"answer/analysis/review. There is no git repository to modify.\n"
            f"Fleet hosts available via passwordless SSH: {fleet_ips}\n"
            f"[END WORKER CONTEXT]\n\n[TASK]\n{task}\n[END TASK]"
        )
    preamble = (
        f"[WORKER CONTEXT]\n"
        f"You are operating on host {AGENT_HOST} as user jasonperlow.\n"
        f"Working git repository: {workspace}\n"
        f"  (it is a clone of the canonical remote; use `cd {workspace}` "
        f"before any git operation)\n"
        f"Job kind: {kind}\n"
        f"Fleet hosts available via passwordless SSH: {fleet_ips}\n"
        f"If you need resources that don't exist here (Oracle, GPU, etc.), "
        f"`ssh jasonperlow@<host>` to a capable host and run remotely.\n"
        f"\n[EXECUTION MANDATE — READ CAREFULLY]\n"
        f"This is a CODE job, not a question. You MUST actually USE your shell "
        f"and file-editing tools to make the change in {workspace} — do NOT "
        f"merely describe, plan, or explain what should be done. Concretely:\n"
        f"  1. Inspect the repo (read/grep the relevant files).\n"
        f"  2. Make the real edits with your edit tools.\n"
        f"  3. `git add` the changes and `git commit` them as "
        f"`Jason Perlow <jperlow@gmail.com>` (never @nvidia.com).\n"
        f"  4. End your reply with the resulting commit SHA.\n"
        f"A reply that only contains analysis/plan/description with NO commit is "
        f"a FAILED job. If the task genuinely needs no code change, say so "
        f"explicitly and why. Otherwise: edit, commit, report the SHA.\n"
        f"[END EXECUTION MANDATE]\n"
        f"[END WORKER CONTEXT]\n\n"
        f"[TASK]\n{task}\n[END TASK]"
    )
    return preamble


# SECURITY (review H4): the former `codex_cli_run` helper was DEAD CODE — defined
# but never called (CODE jobs now route to the native zc_native_build executor,
# non-code jobs to wss_run). It carried a latent
# `codex exec --dangerously-bypass-approvals-and-sandbox` invocation: a
# fully-unsandboxed, auto-approved shell agent one call site away from being live.
# Removed outright so the dangerous flag cannot be reintroduced by a future caller.
# If a sandboxed codex path is ever needed, add it via zc_native_build
# (level=supervised, sandbox_enabled=true), never with --dangerously-bypass.


def wss_run(task, kind, workspace, alias, job_id, timeout_sec):
    """DEFAULT path — open-weight via local zeroclaw gateway."""
    started = time.time()
    # Capture HEAD BEFORE the agent runs so post-run diff finds real commits.
    pre_head = (_git(workspace, "rev-parse", "HEAD", timeout=10).stdout.strip()
                if workspace is not None else "")
    augmented = _augment_task_for_wss(task, workspace, kind)
    try:
        raw_result = wss_driver.drive_agent_via_wss(
            host=GATEWAY_HOST, agent_alias=alias, task=augmented,
            job_id=job_id, timeout_sec=timeout_sec,
            max_tool_iterations=MAX_TOOL_ITERS,
        )
    except Exception as e:
        raw_result = {"exit_code": 1, "via": "wss_driver",
                      "error": f"driver_crash: {type(e).__name__}: {e}"}
    if workspace is None:
        # Non-commit job: no repo to harvest commits from. The text answer IS
        # the deliverable; mark no_code_output so the gate accepts a clean run.
        raw_result["commits"] = []
        raw_result["files_changed"] = []
        raw_result["workspace"] = None
        raw_result.setdefault("worker_error", "no_code_output")
        raw_result["duration_sec"] = round(time.time() - started, 2)
        return raw_result
    # Recompute commits authoritatively from the workspace (ignore regex harvest)
    post_head = _git(workspace, "rev-parse", "HEAD", timeout=10).stdout.strip()
    commits, files = [], []
    if post_head and post_head != pre_head:
        raw_log = _git(workspace, "log", "--format=%H",
                       f"{pre_head}..{post_head}", timeout=15).stdout
        for c in raw_log.strip().split("\n"):
            if c and _verify_commit(workspace, c):
                commits.append(c)
        if commits:
            fc = _git(workspace, "diff", "--name-only",
                      f"{pre_head}..{post_head}", timeout=15).stdout
            files = [f for f in fc.strip().split("\n") if f][:50]
    raw_result["commits"] = commits          # authoritative override
    raw_result["files_changed"] = files
    raw_result["workspace"] = str(workspace)
    raw_result["pre_head"] = pre_head[:12]
    raw_result["post_head"] = post_head[:12]
    raw_result["duration_sec"] = round(time.time()-started, 2)
    return raw_result


def route_chain(tier, kind, fallback):
    """Ask the hive KNEMON cost router for an ordered alias chain that fits
    the job budget. GRACEFUL: any error/timeout/empty -> return the local
    fallback chain so job execution NEVER breaks on a routing hiccup."""
    try:
        code, resp = http("POST", "/v1/knemon/route",
                          {"max_cost_tier": (tier or "C"), "kind": kind or ""},
                          timeout=4)
        if code in (200, 201) and isinstance(resp, dict):
            chain = resp.get("chain")
            if isinstance(chain, list):
                # keep only usable non-empty string aliases (defensive vs a
                # malformed router response); fall through to fallback if none.
                clean = [a.strip() for a in chain if isinstance(a, str) and a.strip()]
                if clean:
                    return clean
    except Exception as e:
        log.warning("  knemon route failed (%s) -> local fallback", e)
    return fallback


def is_oauth_alias(alias):
    return alias in {"hive_codex", "hive_gpt", "hive_gpt_mini", "hive_gpt_heavy"}


def report_oauth_success(alias):
    try:
        http("POST", "/v1/knemon/cap",
             {"success": True, "model": alias, "reporter": AGENT_HOST}, timeout=3)
    except Exception:
        pass


# ── NATIVE build path (CODE kinds) ───────────────────────────────────────────
_NATIVE_EXECUTOR_MOD = None  # None=untried, False=tried-and-missing, module=loaded


def _is_code_kind(kind):
    """A CODE kind produces a repo commit (build/feat/fix/mnemos/codex/doctor-fix
    /repo-prefixed). Explicitly NON-code text kinds (review/architecture/triage/
    docs/…) are NOT code and stay on the WSS gateway text path."""
    if not kind:
        return False
    return not any(kind.startswith(p) for p in NONCOMMIT_KIND_PREFIXES)


def _native_repo_for(kind, desc):
    """Resolve the workspace-remotes subdir (repo) for a CODE kind, or None.

    Order: ``build:<repo>`` suffix → longest-prefix worker KIND_WORKSPACE_MAP
    subdir → an explicit ``repo:<name>`` hint in the description. The returned
    name must exist in the native executor's workspace_remotes.json; if it does
    not, the executor fails LOUD with ``no_remote_for:<repo>`` (a visible failure,
    never a silent no-op)."""
    if kind.startswith("build:"):
        repo = kind[len("build:"):].split(":", 1)[0].strip()
        return repo or None
    best = None
    for prefix, (subdir, _url) in KIND_WORKSPACE_MAP.items():
        if kind.startswith(prefix) and (best is None or len(prefix) > len(best[0])):
            best = (prefix, subdir)
    if best:
        return best[1]
    m = re.search(r"\brepo:\s*([A-Za-z0-9_.-]+)", desc or "", re.I)
    if m:
        return m.group(1)
    return None


def _load_native_executor():
    """Import the native build executor module once (cached). Tries the explicit
    ZC_BUILD_ONESHOT_PATH first, then well-known deploy locations."""
    global _NATIVE_EXECUTOR_MOD
    if _NATIVE_EXECUTOR_MOD is not None:
        return _NATIVE_EXECUTOR_MOD or None
    import importlib.util
    candidates = [
        NATIVE_EXECUTOR_PATH,
        str(Path(__file__).parent / "zc_native_build.py"),
        os.path.expanduser("~/.zeroclaw/zc_native_build.py"),
        os.path.expanduser("~/fleet-ops/gitops/zeroclaw/zc_native_build.py"),
    ]
    for path in candidates:
        if path and os.path.isfile(path):
            try:
                spec = importlib.util.spec_from_file_location("zc_native_build", path)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                if hasattr(mod, "run"):
                    _NATIVE_EXECUTOR_MOD = mod
                    log.info("native executor loaded from %s", path)
                    return mod
            except Exception as e:
                log.warning("native executor load failed at %s: %s", path, e)
    _NATIVE_EXECUTOR_MOD = False  # tried, not found
    log.error("native executor NOT available (set ZC_BUILD_ONESHOT_PATH)")
    return None


def _native_status(result, kind):
    """Map the native executor's exit_code to a bus status.

    0 = verified commit (pushed, or --no-push committed) → done
    3 = cross-family review BLOCKED (work preserved, not pushed) → needs-review
    other (1/2) → failed"""
    ec = result.get("exit_code")
    if ec == 0:
        return "done"
    if ec == 3 or result.get("blocked"):
        return "needs-review"
    return "failed"


def _dispatch_native_build(urn, job, jid, kind, desc, repo, started):
    """Run a CODE job through the native executor and patch the bus result.

    The executor self-manages clone/jail/verify/commit/push, so this path does
    NOT use the WSS anti-fake push gate — the executor's verified push (to
    zcbuild/<jid>) IS the authoritative success signal."""
    mod = _load_native_executor()
    if mod is None:
        result = {"via": "zc_native_build", "exit_code": 2, "kind": kind,
                  "error": "native_executor_unavailable",
                  "commits": [], "files_changed": []}
        persisted = persist_terminal_job(
            jid, urn, "failed", result,
            claimed_retry_count=job.get("retry_count"),
        )
        log.error("→ native UNAVAILABLE %s kind=%s", jid[:12], kind[:50])
        if not persisted:
            log.critical("native failure pending authoritative bus persistence: %s", jid)
        return
    mnemos_refs = job.get("mnemos_refs") or job.get("mnemos_refs_json") or None
    try:
        try:
            result = mod.run(repo, kind, desc, NATIVE_BUILD_ALIAS, jid, None,
                             NATIVE_JOB_TIMEOUT, True, True, mnemos_refs=mnemos_refs)
        except TypeError:
            # Older executor without the mnemos_refs kwarg.
            result = mod.run(repo, kind, desc, NATIVE_BUILD_ALIAS, jid, None,
                             NATIVE_JOB_TIMEOUT, True, True)
    except Exception as e:
        log.exception("native build crashed")
        result = {"via": "zc_native_build", "exit_code": 2, "kind": kind,
                  "error": f"native_crash:{type(e).__name__}:{e}",
                  "commits": [], "files_changed": []}
    if not isinstance(result, dict):
        result = {"via": "zc_native_build", "exit_code": 2, "kind": kind,
                  "error": "native_non_dict_result", "commits": [], "files_changed": []}
    result.setdefault("kind", kind)
    result.setdefault("commits", [])
    result.setdefault("files_changed", [])
    result["duration_sec"] = round(time.time() - started, 2)
    status = _native_status(result, kind)
    persisted = persist_terminal_job(
        jid, urn, status, result,
        claimed_retry_count=job.get("retry_count"),
    )
    if persisted:
        _record_job_to_mnemos(job, result, status)
    log_fn = log.info if persisted else log.critical
    log_fn("→ native %s%s %s exit=%s pushed=%s commits=%d %s",
           status, "" if persisted else " (bus persistence pending)",
           jid[:12], result.get("exit_code"), result.get("pushed"),
           len(result.get("commits") or []),
           result.get("error") or result.get("pushed_branch") or "")


def process_job(urn, job):
    jid = job["id"]
    kind = job.get("kind", "") or ""
    desc = job.get("description", "") or ""
    tier = job.get("max_cost_tier", "C")
    # COST-SAVINGS: doctor classifies the job -> cost-tiered registry; dispatch
    # cheapest-adequate model first, escalate on failure (the chain rotation in
    # the loop below). Falls back to the legacy tier chain if classification is
    # somehow empty.
    registry = classify_registry(kind)
    local_chain = MODEL_REGISTRIES.get(registry) or TIER_CHAINS.get((tier or "C").upper(), TIER_CHAINS["C"])
    # KNEMON cost router (2026-06-02): hive returns a budget-fitted chain;
    # falls back to local_chain on any error/timeout so execution is robust.
    chain = route_chain(tier, kind, local_chain)
    log.info("claimed %s kind=%s tier=%s -> registry=%s chain0=%s", jid[:12], kind[:50], tier, registry, (chain[0] if chain else None))
    if not patch_job(jid, urn, "running"):
        log.error("refusing to execute %s because running state was not acknowledged", jid)
        return
    _start_lease_renewal(jid, urn)
    started = time.time()
    try:
        # ── execution-backend selection (operator 2026-06-16) ──
        # CODE/build kinds run on the NATIVE executor (isolated per-job clone,
        # jailed sandboxed agent, filesystem-diff verification, harness-only git
        # commit+push to zcbuild/<jid>). The WSS gateway path below is reserved
        # for NON-CODE (analysis/answer/review) kinds — it cannot reliably land
        # commits and was silently no-opping code/security jobs through its
        # workspace-less "NON-CODE" preamble. Narrow/low-compute hosts decline
        # heavy native builds (handled by _resolve_workspace's host gate below).
        _narrow_declines = (_HOST_LOWER in NARROW_HOSTS
                            and not any(kind.startswith(p) for p in NARROW_ALLOWLIST))
        if _is_code_kind(kind) and not _narrow_declines:
            native_repo = _native_repo_for(kind, desc)
            if native_repo:
                _dispatch_native_build(urn, job, jid, kind, desc, native_repo, started)
                return

        workspace, werr = _resolve_workspace(kind, desc)
        # Permanent preflight failures: do NOT fail-loop (the hive re-serves
        # failed jobs forever). Terminate them so the queue drains.
        #  - host_declines_kind: this host shouldn't run it -> release to queued
        #    so a capable host claims it.
        #  - no_workspace_for_kind / _for_repo / doctor_fix_missing_repo_hint:
        #    no host has a mapping -> cancelled (resubmit once a mapping exists).
        if werr and werr != "noncommit_workspaceless":
            if werr.startswith("host_declines_kind"):
                patch_job(jid, urn, "queued",
                          {"worker_error": werr, "note": "released_by_host"})
                log.info("→ released %s (%s)", jid[:12], werr)
                return
            # Option A (2026-06-02): these are ORCHESTRATOR (claude/codex) jobs.
            # A zeroclaw worker with no workspace handler must NOT cancel/destroy
            # them — release to queued + strip zeroclaw from eligible_kinds so a
            # Claude/Codex orchestrator claims them and we stop re-grabbing/churning.
            _cur = job.get("eligible_kinds") or []
            _new_elig = [k for k in _cur if k != "zeroclaw" and k not in _ZEROCLAW_PROVIDER_ALIASES] or ["claude", "codex"]
            _stop_lease_renewal(jid)
            code, resp = http(
                "PATCH",
                f"/v1/jobs/{jid}",
                {
                    "status": "queued",
                    "claimed_by": urn,
                    "eligible_kinds": _new_elig,
                    "result": {"worker_error": werr,
                               "note": "released_to_orchestrator:no_zeroclaw_handler"},
                },
                timeout=15,
            )
            if code not in (200, 201, 204):
                log.error("failed to release %s to orchestrator: %s %s", jid[:12], code, str(resp)[:200])
                return
            log.info("→ released %s to orchestrator %s (%s) — no zeroclaw handler", jid[:12], _new_elig, werr)
            return
        if werr == "noncommit_workspaceless":
            workspace = None  # run the chain workspace-less (text answer)
        if True:
            # ALL kinds (incl doctor:/codex/review:) run through the same WSS
            # gateway agent. Try the tier's provider chain in order: if a
            # provider times out / is throttled (rate-limit, quota, session
            # usage) / errors, fall through to the NEXT LLM in the chain.
            # Stop on success (commit or clean answer) or a non-retryable fail.
            result = None
            for idx, alias in enumerate(chain):
                # KHM: skip a cooled-down (known-dead) alias unless it is the
                # last option left (always keep a fallback to dispatch *something*).
                if _alias_cooled(alias) and idx < len(chain) - 1:
                    log.info("  skip alias=%s (cooldown) — KHM", alias)
                    continue
                log.info("  → WSS alias=%s (%d/%d) workspace=%s",
                         alias, idx + 1, len(chain), workspace)
                result = wss_run(desc, kind, workspace, alias, jid, JOB_TIMEOUT)
                # Detect throttle/failure from error+worker_error AND the agent's
                # full_response — codex reports its OAuth action rate-limit
                # ("Rate limit exceeded: too many actions") only in the response
                # body with exit_code=0, which previously LOOKED like success and
                # broke the chain instead of escalating to the next (working)
                # model. Escalate when throttled/hard-failed AND no commit landed.
                blob = (str(result.get("error") or "") + " "
                        + str(result.get("worker_error") or "") + " "
                        + str(result.get("full_response") or "")[:600]).lower()
                throttled = any(s in blob for s in (
                    "rate limit", "rate_limit", "too many actions", "429",
                    "throttle", "quota", "overloaded", "overload"))
                hard_fail = any(s in blob for s in (
                    "timeout", "unavailable", "all model_providers", "driver_crash",
                    "max tool iterations", "connection refused", "502", "503", "500"))
                # KHM: hard-fail (bad key/endpoint, NOT throttle) cools the alias
                # so future jobs skip it; success clears any cooldown.
                if hard_fail and not throttled and not result.get("commits"):
                    _ALIAS_COOLDOWN[alias] = time.time() + ALIAS_COOLDOWN_SEC
                    log.info("  KHM: cooling alias=%s for %ds (hard-fail)", alias, int(ALIAS_COOLDOWN_SEC))
                elif result.get("commits") or result.get("exit_code") == 0:
                    _ALIAS_COOLDOWN.pop(alias, None)
                oauth_success = (
                    is_oauth_alias(alias) and not throttled and not hard_fail
                    and (result.get("commits") or result.get("exit_code") == 0)
                )
                if oauth_success:
                    report_oauth_success(alias)
                escalate = (throttled or hard_fail) and not result.get("commits")
                if result.get("commits") or not escalate or idx == len(chain) - 1:
                    break
                log.info("  alias %s throttled/failed (%s) — escalating to next model",
                         alias, blob[:70])
            result["alias_tried"] = chain[:idx + 1]
    except Exception as e:
        log.exception("dispatch failed")
        result = {"exit_code": 1, "via": "worker",
                  "error": f"dispatch_crash: {type(e).__name__}: {e}",
                  "commits": [], "files_changed": []}
    result["duration_sec"] = round(time.time() - started, 2)
    result["kind"] = kind
    # #7066 workaround: stamp TRUE provider/model from the dispatched alias
    _tried = result.get("alias_tried") or []
    if _tried:
        _rp, _rm = _alias_real_provider(_tried[-1])
        if _rp:
            result["gateway_provider_reported"] = result.get("gateway_provider")
            result["gateway_model_reported"] = result.get("gateway_model")
            result["gateway_provider"] = _rp
            result["gateway_model"] = _rm
    # --- anti-fake-completion gate: verify real commit + push to remote ---
    commits = result.get("commits") or []
    workspace = result.get("workspace")
    has_remote = False
    pushed = False
    if workspace:
        rr = _git(Path(workspace), "remote", "get-url", "origin", timeout=10)
        has_remote = (rr.returncode == 0 and bool(rr.stdout.strip()))
    if commits and has_remote:
        # Push to a UNIQUE per-job branch, never the shared/protected default —
        # pushing HEAD to a shared branch fails non-fast-forward when the remote
        # moved (observed 2026-06-01 on feat/knemon-mvp). A fresh hive/<jobid>
        # branch is always conflict-free, so the commit is provably landed and
        # reviewable/mergeable later.
        branch = "hive/" + re.sub(r"[^0-9a-zA-Z_-]", "", jid)[:24]
        _git(Path(workspace), "branch", "-f", branch, "HEAD", timeout=10)
        pr = _git(Path(workspace), "push", "origin", f"{branch}:{branch}", timeout=180)
        # ANTI-FAKE: a 0 exit from `git push` is NOT proof the commit landed.
        # Resolve the FULL tip SHA and confirm it is actually present on origin
        # via ls-remote (strict col-1). NEVER trust the local push exit alone.
        tip = _git(Path(workspace), "rev-parse", "HEAD", timeout=10).stdout.strip()
        verified = bool(tip) and _sha_on_remote(Path(workspace), "origin", tip)
        pushed = verified
        result["pushed"] = pushed
        result["pushed_branch"] = branch if pushed else None
        if not pushed:
            _perr = (pr.stderr or "").strip()[:160]
            if pr.returncode == 0:
                result["worker_error"] = (
                    "push_unverified: local commits NOT confirmed on remote "
                    "(ls-remote missing tip SHA) — possible fake completion")
            else:
                result["worker_error"] = "push_failed:" + _perr
    requires_commit = has_remote and not kind.startswith("review:")  # review jobs emit a report, not a commit
    if requires_commit:
        # For a repo job the VERIFIED, PUSHED commit is the authoritative
        # success signal — honor it even when the agent returns non-zero
        # because it hit a soft cap (e.g. "max tool iterations").
        if commits and pushed:
            status = "done"
        elif result.get("exit_code") == 0 and result.get("worker_error") == "no_code_output":
            # Bug-2 fix (2026-06-02): a COMMIT-MANDATORY kind that produced no code
            # is a FAILURE, not a clean "nothing to do" — fail it so it retries/
            # escalates instead of minting a fake completion. Other repo kinds
            # (analysis / already-fixed) still legitimately accept no-commit.
            if any(kind.startswith(p) for p in COMMIT_MANDATORY_PREFIXES):
                status = "failed"
                result.setdefault("error", "no_code_output_on_commit_mandatory_kind")
            else:
                status = "done"
        else:
            status = "failed"
            if not commits:
                result.setdefault("error", "fake_prevented:exit0_but_zero_verified_commits")
            elif not pushed:
                result.setdefault("error", "commit_made_but_push_failed")
    elif result.get("exit_code") == 0 and result.get("worker_error") in (None, "", "no_code_output"):
        # Non-repo job (no remote to commit to): a clean agent run is success
        # even with no code output — analysis/answer/triage jobs legitimately
        # produce text, not commits. Real failures (no_workspace_for_kind,
        # driver_crash, push_failed, ...) still fall through to 'failed'.
        status = "done"
    else:
        status = "failed"
    persisted = persist_terminal_job(
        jid, urn, status, result,
        claimed_retry_count=job.get("retry_count"),
    )
    if persisted:
        _record_job_to_mnemos(job, result, status)  # commit-DAG node (best-effort)
    commits = result.get("commits") or []
    log_fn = log.info if persisted else log.critical
    log_fn("→ %s%s %s commits=%d dur=%.1fs %s",
           status, "" if persisted else " (bus persistence pending)",
           jid[:12], len(commits), result["duration_sec"],
           result.get("error") or result.get("worker_error") or "")


def main():
    if not MNEMOS_TOKEN:
        log.critical("MNEMOS_TOKEN must be configured; refusing to start with no provenance credential")
        raise SystemExit(2)
    log.info("starting wss-worker-v2 on %s, gateway=%s:%s, poll=%ss "
             "(open-weights default, codex scarce)",
             AGENT_HOST, GATEWAY_HOST, GATEWAY_PORT, POLL_INTERVAL)
    urn = register()
    if not urn:
        sys.exit(1)

    # Background heartbeat: keep the agent visibly ONLINE in the dashboard
    # even while the main loop is blocked for ~20s on a real agent job
    # (inline heartbeat used to lapse during work -> worker flickered offline).
    def _heartbeat_loop():
        while True:
            try:
                heartbeat(urn)
            except Exception:
                pass
            time.sleep(HEARTBEAT_INTERVAL)

    threading.Thread(target=_heartbeat_loop, daemon=True).start()

    while True:
        try:
            flush_completion_outbox()
            job = claim_next(urn)
        except Exception as e:
            log.warning("claim error: %s", e)
            time.sleep(POLL_INTERVAL)
            continue
        if not job:
            time.sleep(POLL_INTERVAL)
            continue
        try:
            process_job(urn, job)
        except Exception:
            log.exception("process_job error")


if __name__ == "__main__":
    main()
