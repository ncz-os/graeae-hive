"""The Freebuff prompt-inbox profile for the Hive bus (``freebuff-hive-profile/``).

Two halves:

* a structural check of ``inbox.hive.example.json`` (written here, no external
  schema library): required keys, no literal IPv4 address, no URN that is not an
  obvious placeholder, no token;
* the profile run against the REAL bus (in-process ASGI, temp sqlite) through the
  REAL MCP tool handler of ``hive/mcp/bridge.py``. A tiny simulator of the
  inbox's source and sink behaviour, written below, polls ``hive_job_next``,
  maps the result to a message with the example's ``map``, and reports the
  lifecycle through ``hive_job_update`` with the example's ``args`` and
  ``valueMaps``. Every assertion reads the job back from the bus.

What is simulated, and from where: the inbox rules (id/from/text validation,
``allowFrom``, model guard, dedupe by id, ``{{placeholders}}``, the JSONPath
subset) follow ``docs/prompt-inbox.md`` of the Freebuff inbox patch. The keys
``valueMaps``, ``sourceMeta``, ``noWork`` and the ``map.text`` template are this
profile's contract with that patch; they are not in the revision of that
document this was written against (see ``freebuff-hive-profile/README.md``).
The real inbox build was not run.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import tempfile
import time as real_time
import types
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus.deps import hive_repo  # noqa: E402
from hive.bus.routers import jobs as jobs_router  # noqa: E402

HERE = Path(__file__).resolve().parent
PROFILE = HERE / "freebuff-hive-profile"
EXAMPLE = PROFILE / "inbox.hive.example.json"
README = PROFILE / "README.md"

# Inbox message limits, from the inbox document.
ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
FROM_MAX = 64
TEXT_MAX = 64 * 1024

PLACEHOLDER_NAMES = {
    "id",
    "event",
    "from",
    "outcome",
    "startedAt",
    "completedAt",
    "reason",
    "json",
    "attempt",
    "model",
}
IPV4 = re.compile(r"(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?![\d.])")
ENV_PLACEHOLDER = re.compile(r"\$\{[A-Z][A-Z0-9_]*\}")
ANGLE_PLACEHOLDER = re.compile(r"<[a-z0-9][a-z0-9-]*>")
MUSTACHE = re.compile(r"\{\{\s*([^}]*?)\s*\}\}")


def _load_example() -> dict:
    return json.loads(EXAMPLE.read_text())


def _strings(node):
    """Every string in a JSON document, keys included."""
    if isinstance(node, str):
        yield node
    elif isinstance(node, dict):
        for key, value in node.items():
            yield key
            yield from _strings(value)
    elif isinstance(node, list):
        for item in node:
            yield from _strings(item)


# --------------------------------------------------------------------------
# structural check of the example (a JSON-schema-like walk, no library)
# --------------------------------------------------------------------------

TOP_KEYS = {
    "dir",
    "sources",
    "sinks",
    "allowModelOverride",
    "allowedModels",
    "maxCostMultiplier",
    "autoResume",
}
SOURCE_REQUIRED = {"type", "server", "tool", "args", "intervalSec", "claimsOnPoll", "map", "allowFrom"}
SOURCE_ALLOWED = SOURCE_REQUIRED | {"noWork", "sourceMeta", "itemsPath"}
SINK_REQUIRED = {"type", "server", "tool", "events", "args", "valueMaps"}
SINK_ALLOWED = SINK_REQUIRED | {"includeText"}
EVENTS = {"accepted", "started", "done", "not_accepted"}
OUTCOMES = {"completed", "interrupted", "error", "session_ended"}
HIVE_STATUSES = {"queued", "claimed", "running", "done", "failed"}


def structural_errors(doc: dict) -> list[str]:
    """Problems with ``doc``, as strings; empty means it passes."""
    errs: list[str] = []

    def need(cond: bool, msg: str) -> None:
        if not cond:
            errs.append(msg)

    unknown = set(doc) - TOP_KEYS
    need(not unknown, f"unknown top-level keys {sorted(unknown)} (the inbox refuses unknown keys)")
    need(isinstance(doc.get("dir"), str) and doc["dir"], "dir must be a non-empty string")
    need(isinstance(doc.get("allowModelOverride"), bool), "allowModelOverride must be a boolean")
    need(isinstance(doc.get("allowedModels"), list) and doc["allowedModels"], "allowedModels must be a non-empty list")
    need(
        isinstance(doc.get("maxCostMultiplier"), (int, float)) and 0 < doc["maxCostMultiplier"] <= 4,
        "maxCostMultiplier must be a number in (0, 4]",
    )
    need(doc.get("autoResume") is False, "autoResume must be false (a resume would race the bus release)")

    sources = doc.get("sources")
    sinks = doc.get("sinks")
    need(isinstance(sources, list) and len(sources) == 1, "exactly one source")
    need(isinstance(sinks, list) and sinks, "sinks must be a non-empty list")
    if not (isinstance(sources, list) and isinstance(sinks, list)):
        return errs

    for src in sources:
        need(src.get("type") == "mcp", "source type must be mcp")
        missing = SOURCE_REQUIRED - set(src)
        need(not missing, f"source misses keys {sorted(missing)}")
        extra = set(src) - SOURCE_ALLOWED
        need(not extra, f"source has unknown keys {sorted(extra)}")
        need(src.get("tool") == "hive_job_next", "source tool must be hive_job_next")
        need(set(src.get("args", {})) == {"agent_urn"}, "source args must be exactly agent_urn")
        need(src.get("claimsOnPoll") is True, "claimsOnPoll must be true: hive_job_next claims what it returns")
        need(src.get("intervalSec") == 30, "intervalSec must be 30")
        allow = src.get("allowFrom")
        need(
            isinstance(allow, list) and allow and all(ENV_PLACEHOLDER.fullmatch(a) for a in allow),
            "allowFrom must be a non-empty list of ${ENV} placeholders",
        )
        need(set(src.get("map", {})) >= {"id", "from", "text", "model"}, "map needs id, from, text, model")
        need(src.get("map", {}).get("id") == "$.id", "map.id must be $.id")
        need(src.get("map", {}).get("from") == "$.submitter_urn", "map.from must be $.submitter_urn")
        need(src.get("map", {}).get("model") == "$.preferred_models[0]", "map.model must be $.preferred_models[0]")
        template = (src.get("map", {}).get("text") or {}).get("template", "")
        for needle in ("{{$.description}}", "{{$.repo}}", "{{$.head_branch}}"):
            need(needle in template, f"text template misses {needle}")
        need(
            "push your branch to the remote named in the brief, then report" in template,
            "text template misses the push-and-report line",
        )
        no_work = src.get("noWork", {})
        need(no_work == {"path": "$._status", "equals": 204}, "noWork must match the bridge's {_status: 204} marker")

    seen_events: set[str] = set()
    for sink in sinks:
        if sink.get("type") == "file":
            need(set(sink) == {"type"}, "the file sink takes no other keys")
            continue
        need(sink.get("type") == "mcp", "sink type must be file or mcp")
        missing = SINK_REQUIRED - set(sink)
        need(not missing, f"sink misses keys {sorted(missing)}")
        extra = set(sink) - SINK_ALLOWED
        need(not extra, f"sink has unknown keys {sorted(extra)}")
        need(sink.get("tool") == "hive_job_update", "sink tool must be hive_job_update")
        events = set(sink.get("events", []))
        need(events and events <= EVENTS, f"sink events must be a subset of {sorted(EVENTS)}")
        seen_events |= events
        args = sink.get("args", {})
        need({"id", "status", "result", "claimed_by"} <= set(args), "sink args need id, status, result, claimed_by")
        need(args.get("id") == "{{id}}", "sink id must be {{id}}")
        need(ENV_PLACEHOLDER.fullmatch(args.get("claimed_by", "")) is not None, "claimed_by must be an ${ENV} name")
        need(isinstance(args.get("result"), dict), "sink result must be an object")
        status_map = sink.get("valueMaps", {}).get("status", {})
        need(status_map, "sink valueMaps.status is required")
        need(set(status_map.values()) <= HIVE_STATUSES, "valueMaps may only produce statuses the bus knows")
        if "done" in events:
            need(args["status"] == "{{outcome}}", "the done sink must map from {{outcome}}")
            need(set(status_map) == OUTCOMES, f"the done sink must map exactly {sorted(OUTCOMES)}")
            need(
                {"outcome", "attempt", "model", "completedAt"} <= set(args["result"]),
                "done result needs outcome, attempt, model, completedAt",
            )
            need("text" not in args["result"], "text must not be in the example result")
            need(sink.get("includeText") is False, "includeText must be false by default")
        else:
            need(args["status"] == "{{event}}", "non-done sinks must map from {{event}}")
            need(set(status_map) == events, "valueMaps must cover exactly the sink's events")
    need(seen_events == EVENTS, f"every event must reach a sink, missing {sorted(EVENTS - seen_events)}")

    for text in _strings(doc):
        for name in MUSTACHE.findall(text):
            if not name.startswith("$"):
                need(name in PLACEHOLDER_NAMES, f"unknown placeholder {{{{{name}}}}}")
    return errs


class ExampleFileTests(unittest.TestCase):
    def test_example_passes_the_structural_check(self):
        self.assertEqual(structural_errors(_load_example()), [])

    def test_the_check_has_teeth(self):
        # A copy with a safeguard removed must fail, or the check proves nothing.
        base = _load_example()
        broken = json.loads(json.dumps(base))
        del broken["sources"][0]["allowFrom"]
        self.assertTrue(structural_errors(broken))
        broken = json.loads(json.dumps(base))
        broken["sinks"][-1]["valueMaps"]["status"]["error"] = "released"
        self.assertTrue(structural_errors(broken))
        broken = json.loads(json.dumps(base))
        broken["sinks"][-1]["args"]["result"]["text"] = "{{text}}"
        self.assertTrue(structural_errors(broken))

    def test_no_literal_ipv4_address(self):
        for path in (EXAMPLE, README, PROFILE / "job-template.md"):
            hits = IPV4.findall(path.read_text())
            self.assertEqual(hits, [], f"{path.name} contains an address")

    def test_no_urn_other_than_placeholders(self):
        for text in _strings(_load_example()):
            for urn in re.findall(r"urn:[^\s\"']+", text):
                self.assertRegex(urn, r"^urn:agent:<[a-z-]+>:<[a-z-]+>:<[a-z-]+>$", f"real-looking URN: {urn}")
        # the allowFrom entries must not be literals at all
        for entry in _load_example()["sources"][0]["allowFrom"]:
            self.assertRegex(entry, r"^\$\{HIVE_[A-Z_0-9]+\}$")
        for path in (README, PROFILE / "job-template.md"):
            for urn in re.findall(r"urn:agent:\S+", path.read_text()):
                self.assertRegex(urn, r"^urn:agent:<[a-z-]+>:<[a-z-]+>:<[a-z-]+>`?[.,]?$", f"{path.name}: {urn}")

    def test_no_token_or_secret_shaped_value(self):
        doc = _load_example()
        for text in _strings(doc):
            stripped = ENV_PLACEHOLDER.sub("", MUSTACHE.sub("", ANGLE_PLACEHOLDER.sub("", text)))
            self.assertNotRegex(stripped, r"[A-Za-z0-9+/_=-]{32,}", f"token-shaped string: {text[:40]!r}")
            self.assertNotRegex(text, r"(?i)\bbearer\s+\S+")
            self.assertNotRegex(text, r"^(sk|ghp|glpat|xox[abp])[-_]")
        for key in _keys(doc):
            self.assertNotRegex(key, r"(?i)token|password|passwd|secret|credential|api_?key")

    def test_scan_catches_what_it_claims_to(self):
        self.assertTrue(IPV4.findall("host 203.0.113.9 here"))
        self.assertFalse(IPV4.findall("version 1.2.3"))
        self.assertRegex("a" * 40, r"[A-Za-z0-9+/_=-]{32,}")


def _keys(node):
    if isinstance(node, dict):
        for key, value in node.items():
            yield key
            yield from _keys(value)
    elif isinstance(node, list):
        for item in node:
            yield from _keys(item)


# --------------------------------------------------------------------------
# the inbox simulator (source, validation, dedupe, sinks)
# --------------------------------------------------------------------------


def jsonpath(node, path: str):
    """The inbox's JSONPath subset: ``$`` then ``.name`` / ``[n]`` steps. ``None`` when absent."""
    if not path.startswith("$"):
        raise ValueError(f"unsupported path {path!r}")
    for kind, step in re.findall(r"\.([A-Za-z0-9_-]+)|\[(\d+)\]", path[1:]):
        if kind:
            node = node.get(kind) if isinstance(node, dict) else None
        else:
            idx = int(step)
            node = node[idx] if isinstance(node, list) and idx < len(node) else None
        if node is None:
            return None
    return node


def expand_env(node, env: dict[str, str]):
    if isinstance(node, str):
        return ENV_PLACEHOLDER.sub(lambda m: env[m.group(0)[2:-1]], node)
    if isinstance(node, dict):
        return {k: expand_env(v, env) for k, v in node.items()}
    if isinstance(node, list):
        return [expand_env(v, env) for v in node]
    return node


def fill(node, ctx: dict[str, str]):
    """Substitute ``{{name}}`` in every string, one pass; an unknown name is an error."""
    if isinstance(node, str):

        def one(match):
            name = match.group(1)
            if name not in PLACEHOLDER_NAMES:
                raise ValueError(f"unknown placeholder {name}")
            return str(ctx.get(name, ""))

        return MUSTACHE.sub(one, node)
    if isinstance(node, dict):
        return {k: fill(v, ctx) for k, v in node.items()}
    if isinstance(node, list):
        return [fill(v, ctx) for v in node]
    return node


class InboxSim:
    """The inbox's MCP source and sinks, as far as the profile depends on them.

    One instance is one inbox directory: its handled-id set is its ``accepted/``
    + ``rejected/`` records, so a second instance is a fresh inbox.
    """

    def __init__(self, call, settings: dict, env: dict[str, str]):
        self.call = call  # async (tool, args) -> (is_error, body)
        self.settings = expand_env(settings, env)
        self.source = self.settings["sources"][0]
        self.handled: set[str] = set()
        self.attempts: dict[str, int] = {}
        self.sink_log: list[dict] = []  # every sink call: tool args as sent + outcome
        self.disposition: dict[str, str] = {}

    # ---- source ----
    def _no_work(self, body) -> bool:
        rule = self.source["noWork"]
        return jsonpath(body, rule["path"]) == rule["equals"]

    def _message(self, job: dict) -> dict:
        mp = self.source["map"]
        template = mp["text"]["template"]
        text = re.sub(
            r"\{\{(\$[^}]*)\}\}",
            lambda m: "" if jsonpath(job, m.group(1)) is None else str(jsonpath(job, m.group(1))),
            template,
        )
        return {
            "id": jsonpath(job, mp["id"]),
            "from": jsonpath(job, mp["from"]),
            "text": text,
            "model": jsonpath(job, mp["model"]),
        }

    def _refusal(self, msg: dict) -> str | None:
        """The first check a message fails, as ``<code>: <explanation>``."""
        if not isinstance(msg["id"], str) or not ID_RE.match(msg["id"]):
            return "id_invalid: id must be 1-128 characters of A-Za-z0-9._-"
        frm = msg["from"]
        if not isinstance(frm, str) or not 1 <= len(frm) <= FROM_MAX or re.search(r"[\x00-\x1f\x7f]", frm):
            return "from_invalid: from must be 1-64 characters, no control characters"
        if not msg["text"].strip() or len(msg["text"].encode()) > TEXT_MAX:
            return "text_invalid: text must be non-empty and at most 64 KiB"
        if frm not in self.source["allowFrom"]:
            return "source_not_allowed: from is not in allowFrom"
        if msg["model"]:
            if not self.settings.get("allowModelOverride"):
                return "model_override_disabled: the settings do not allow a choice"
            if msg["model"] not in self.settings.get("allowedModels", []):
                return "model_not_allowed: not in allowedModels"
        return None

    async def poll(self) -> dict | None:
        """One source poll. ``None`` when there was no work; else the message record."""
        is_error, body = await self.call(self.source["tool"], dict(self.source["args"]))
        assert not is_error, body
        if self._no_work(body):
            return None
        msg = self._message(body)
        job_id = msg["id"]
        if job_id in self.handled:
            # Dedupe by id: skipped, no turn, no sink event (inbox document, Idempotency).
            self.disposition[job_id] = "duplicate"
            return {**msg, "state": "duplicate"}
        self.handled.add(job_id)
        reason = self._refusal(msg)
        if reason:
            self.disposition[job_id] = "rejected"
            await self.emit("not_accepted", msg, reason=reason)
            return {**msg, "state": "rejected", "reason": reason}
        self.attempts[job_id] = self.attempts.get(job_id, 0) + 1
        self.disposition[job_id] = "accepted"
        await self.emit("accepted", msg)
        return {**msg, "state": "accepted"}

    async def start(self, msg: dict) -> list[dict]:
        return await self.emit("started", msg)

    async def finish(self, msg: dict, outcome: str) -> list[dict]:
        assert outcome in OUTCOMES, outcome
        return await self.emit("done", msg, outcome=outcome)

    def requeue_locally(self, msg: dict) -> dict:
        """The inbox's own retry after ``session_ended`` with nothing produced: same id, next attempt."""
        job_id = msg["id"]
        assert self.attempts[job_id] < 3, "the inbox gives up after 3 attempts"
        self.attempts[job_id] += 1
        return msg

    # ---- sinks ----
    async def emit(self, event: str, msg: dict, *, outcome: str = "", reason: str = "") -> list[dict]:
        ctx = {
            "id": msg["id"],
            "event": event,
            "from": msg["from"],
            "outcome": outcome or ("not_accepted" if event == "not_accepted" else ""),
            "completedAt": "2026-01-01T00:00:00.000Z" if event == "done" else "",
            "startedAt": "2026-01-01T00:00:00.000Z" if event == "started" else "",
            "reason": reason,
            "attempt": self.attempts.get(msg["id"], 1),
            "model": msg.get("model") or "",
        }
        sent = []
        for sink in self.settings["sinks"]:
            if sink["type"] != "mcp" or event not in sink["events"]:
                continue
            args = fill(sink["args"], ctx)
            for arg_name, table in sink["valueMaps"].items():
                args[arg_name] = table[args[arg_name]]
            if not sink.get("includeText"):
                assert "text" not in json.dumps(args), "text reached a sink with includeText off"
            is_error, body = await self.call(sink["tool"], dict(args))
            entry = {"event": event, "args": args, "is_error": is_error, "body": body}
            self.sink_log.append(entry)
            sent.append(entry)
        return sent


# --------------------------------------------------------------------------
# the real bus + the real MCP tool handler
# --------------------------------------------------------------------------


def _load_bridge():
    spec = importlib.util.spec_from_file_location("freebuff_profile_bridge", HERE / "hive" / "mcp" / "bridge.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Clock:
    """A shiftable ``time`` for the jobs router, so a release backoff can pass without sleeping."""

    def __init__(self):
        self.offset = 0.0

    def time(self) -> float:
        return real_time.time() + self.offset


class ProfileOnRealBusTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from hive.bus import deps

        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        env = mock.patch.dict(os.environ, {"AGENT_BUS_DB": os.path.join(self._td.name, "profile.db")})
        env.start()
        self.addCleanup(env.stop)
        repo = mock.patch.object(deps, "_HIVE_REPO", None)
        repo.start()
        self.addCleanup(repo.stop)
        await hive_repo().init()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t")
        self.addAsyncCleanup(self.client.aclose)

        self.clock = Clock()
        clock = mock.patch.object(jobs_router, "time", types.SimpleNamespace(time=self.clock.time))
        clock.start()
        self.addCleanup(clock.stop)

        # The REAL bridge module, with its HTTP client pointed at the in-process bus.
        self.bridge = _load_bridge()
        self.addAsyncCleanup(self.bridge.client.aclose)
        for name, value in (
            ("client", httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t")),
            ("HIVE_URL", "http://t"),
            ("HIVE_BUS_TOKEN", ""),
            ("HIVE_BUS_ADMIN_TOKEN", ""),
        ):
            patcher = mock.patch.object(self.bridge, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.addAsyncCleanup(self.bridge.client.aclose)

        self.orch = await self._register("opencode", "studio", ["code", "orchestrate"])
        self.outsider = await self._register("opencode", "other", ["code", "orchestrate"])
        self.longname = await self._register("opencode", "a-long-host-name", ["code", "orchestrate"])
        self.agent = await self._register("freebuff", "studio", ["code"], cost_tier="A", auth="free")
        await self.client.post("/v1/agents/heartbeat", json={"urn": self.agent})
        self.env = {
            "HIVE_AGENT_URN": self.agent,
            "HIVE_ORCHESTRATOR_URN_1": self.orch,
            "HIVE_ORCHESTRATOR_URN_2": "urn:agent:opencode:unused:00000000-0000-0000-0000-000000000000",
        }
        self.model = "model-from-the-picker"

    async def _register(self, kind, host, caps, cost_tier="C", auth="api"):
        r = await self.client.post(
            "/v1/agents/register",
            json={
                "kind": kind,
                "host": host,
                "runtime": kind,
                "status": "online",
                "capabilities": caps,
                "cost_tier": cost_tier,
                "auth_method": auth,
            },
        )
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()["urn"]

    # ---- MCP through the bridge's real typed handler ----
    async def mcp(self, tool: str, args: dict):
        from mcp.server.auth.middleware.auth_context import AuthenticatedUser
        from mcp.server.auth.provider import AccessToken
        from mcp.types import CallToolRequestParams
        from starlette.requests import Request

        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/messages/",
                "raw_path": b"/messages/",
                "query_string": b"",
                "headers": [],
                "scheme": "http",
                "server": ("test", 80),
            }
        )
        request.scope["user"] = AuthenticatedUser(
            AccessToken(token="placeholder", client_id="profile-test", scopes=["bridge:use"])
        )
        handler = self.bridge.server._request_handlers["tools/call"].handler
        result = await handler(
            types.SimpleNamespace(request=request), CallToolRequestParams(name=tool, arguments=dict(args))
        )
        return result.is_error, json.loads(result.content[0].text)

    def inbox(self, **overrides) -> InboxSim:
        settings = _load_example()
        settings["allowedModels"] = [self.model]
        settings.update(overrides)
        return InboxSim(self.mcp, settings, self.env)

    # ---- bus helpers ----
    async def submit(self, tag: str, *, submitter=None, kind="docs:freebuff-profile", **extra) -> str:
        body = {
            "kind": kind,
            "description": f"profile test {tag}: change one file",
            "submitter_urn": submitter or self.orch,
            "eligible_kinds": ["freebuff"],
            "max_retries": 0,
            "zero_retry_reason": "profile test: one attempt, a retry costs a session hour",
        }
        if kind.startswith("docs:"):
            body.update({"repo": "example-org/example-repo", "head_branch": f"profile/{tag}"})
        body.update(extra)
        r = await self.client.post("/v1/jobs", json=body)
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()["id"]

    async def job(self, job_id: str) -> dict:
        r = await self.client.get(f"/v1/jobs/{job_id}")
        self.assertEqual(r.status_code, 200, r.text)
        job = r.json()
        if isinstance(job.get("result"), str):
            job["result"] = json.loads(job["result"])
        return job

    # ------------------------------------------------------------------
    # the source
    # ------------------------------------------------------------------
    async def test_no_work_marker_is_recognised_and_claims_nothing(self):
        is_error, body = await self.mcp("hive_job_next", {"agent_urn": self.agent})
        self.assertFalse(is_error, body)
        self.assertEqual(body["_status"], 204)  # the exact marker the example's noWork matches
        self.assertIsNone(await self.inbox().poll())

    async def test_poll_claims_and_maps_a_job_to_a_message(self):
        job_id = await self.submit("map", preferred_models=[self.model, "second-choice"])
        box = self.inbox()
        msg = await box.poll()
        self.assertEqual(msg["state"], "accepted")
        self.assertEqual(msg["id"], job_id)
        self.assertEqual(msg["from"], self.orch)
        self.assertEqual(msg["model"], self.model)  # preferred_models[0]
        self.assertIn("profile test map: change one file", msg["text"])
        self.assertIn("repo: example-org/example-repo", msg["text"])
        self.assertIn("head_branch: profile/map", msg["text"])
        self.assertTrue(
            msg["text"].endswith("push your branch to the remote named in the brief, then report"), msg["text"]
        )
        # The real ids and URNs fit the inbox's own limits.
        self.assertRegex(msg["id"], ID_RE)
        self.assertLessEqual(len(msg["from"]), FROM_MAX)
        job = await self.job(job_id)
        self.assertEqual(job["claimed_by"], self.agent)
        self.assertEqual(job["status"], "claimed")

    async def test_no_model_without_preferred_models(self):
        await self.submit("nomodel")
        msg = await self.inbox().poll()
        self.assertIsNone(msg["model"])

    # ------------------------------------------------------------------
    # lifecycle on the bus
    # ------------------------------------------------------------------
    async def test_happy_path_queued_claimed_running_done(self):
        job_id = await self.submit("happy", preferred_models=[self.model])
        self.assertEqual((await self.job(job_id))["status"], "queued")
        box = self.inbox()
        msg = await box.poll()
        self.assertEqual((await self.job(job_id))["status"], "claimed")  # accepted -> claimed
        self.assertTrue(all(not s["is_error"] for s in box.sink_log), box.sink_log)
        await box.start(msg)
        self.assertEqual((await self.job(job_id))["status"], "running")
        sent = await box.finish(msg, "completed")
        self.assertEqual([s["args"]["status"] for s in sent], ["done"])
        self.assertFalse(sent[0]["is_error"], sent)
        job = await self.job(job_id)
        self.assertEqual(job["status"], "done")
        self.assertEqual(
            {k: job["result"][k] for k in ("outcome", "attempt", "model", "completedAt")},
            {"outcome": "completed", "attempt": "1", "model": self.model, "completedAt": "2026-01-01T00:00:00.000Z"},
        )
        self.assertNotIn("text", job["result"])  # includeText is off
        self.assertEqual(
            [s["args"]["status"] for s in box.sink_log], ["claimed", "running", "done"]
        )  # exactly the three lifecycle writes

    async def test_error_and_interrupted_end_failed(self):
        for outcome in ("error", "interrupted"):
            with self.subTest(outcome=outcome):
                job_id = await self.submit(f"fail-{outcome}")
                box = self.inbox()
                msg = await box.poll()
                await box.start(msg)
                sent = await box.finish(msg, outcome)
                self.assertFalse(sent[0]["is_error"], sent)
                job = await self.job(job_id)
                self.assertEqual(job["status"], "failed")
                self.assertEqual(job["result"]["outcome"], outcome)
                self.assertEqual(job["claimed_by"], self.agent)

    async def test_error_with_the_default_retry_budget_requeues_instead_of_failing(self):
        # FINDING: the bus auto-retries a `failed` PATCH while budget is left (max_retries defaults to 2),
        # so an inbox `error` only ends `failed` when the job was submitted with max_retries 0.
        job_id = await self.submit("retry", max_retries=2, zero_retry_reason=None)
        box = self.inbox()
        msg = await box.poll()
        await box.start(msg)
        await box.finish(msg, "error")
        job = await self.job(job_id)
        self.assertEqual(job["status"], "queued")
        self.assertEqual(job["retry_count"], 1)
        self.assertIsNone(job["claimed_by"])

    async def test_session_ended_releases_the_job_and_a_second_poll_can_claim_it(self):
        job_id = await self.submit("session")
        box = self.inbox()
        msg = await box.poll()
        await box.start(msg)
        sent = await box.finish(msg, "session_ended")
        self.assertFalse(sent[0]["is_error"], sent)  # claimed/running -> queued is allowed by PATCH
        job = await self.job(job_id)
        self.assertEqual(job["status"], "queued")
        self.assertIsNone(job["claimed_by"])
        self.assertEqual(job["result"]["outcome"], "session_ended")
        self.assertEqual(job["result"]["attempt"], "1")  # the attempt that was cut short is on the row
        self.assertEqual(job["result"]["last_released_by"], self.agent)

        # A release starts a backoff: an immediate poll finds nothing, a later one claims the same job.
        self.assertIsNone(await self.inbox().poll())
        self.clock.offset += 600
        again = await self.inbox().poll()  # a fresh inbox directory: nothing handled yet
        self.assertEqual((again["state"], again["id"]), ("accepted", job_id))
        job = await self.job(job_id)
        self.assertEqual((job["status"], job["claimed_by"]), ("claimed", self.agent))

    async def test_session_ended_then_inbox_local_retry_is_refused_by_the_bus(self):
        # FINDING: with session_ended -> queued the bus row is released, but the inbox (nothing produced)
        # requeues the message itself under the same id and then reports `started`. queued -> running
        # through hive_job_update is refused (PATCH has no queued -> running cell; only a claim leaves queued).
        job_id = await self.submit("localretry")
        box = self.inbox()
        msg = await box.poll()
        await box.start(msg)
        await box.finish(msg, "session_ended")
        box.requeue_locally(msg)
        sent = await box.start(msg)
        self.assertTrue(sent[0]["is_error"])
        self.assertEqual(sent[0]["body"]["status"], 409)
        self.assertIn("invalid status transition 'queued' -> 'running'", sent[0]["body"]["body"])
        self.assertEqual((await self.job(job_id))["status"], "queued")  # still releasable, not corrupted

    async def test_reclaim_of_a_released_job_by_the_same_inbox_is_a_duplicate(self):
        # FINDING: the inbox queues an id once, ever. A job the bus gives back and the same agent claims
        # again has the same id, so the inbox skips it and the claim sits until the orphan sweep.
        job_id = await self.submit("dup")
        box = self.inbox()
        msg = await box.poll()
        await box.finish(msg, "session_ended")
        self.clock.offset += 600
        again = await box.poll()
        self.assertEqual((again["state"], again["id"]), ("duplicate", job_id))
        job = await self.job(job_id)
        self.assertEqual((job["status"], job["claimed_by"]), ("claimed", self.agent))  # no event will follow

    async def test_not_accepted_is_claimed_then_released_with_the_reason(self):
        job_id = await self.submit("model", preferred_models=["a-model-not-in-allowedModels"])
        box = self.inbox()
        seen_claimed = []

        original = box.emit

        async def spy(event, msg, **kw):
            seen_claimed.append((event, (await self.job(job_id))["status"]))
            return await original(event, msg, **kw)

        box.emit = spy
        msg = await box.poll()
        self.assertEqual(msg["state"], "rejected")
        self.assertTrue(msg["reason"].startswith("model_not_allowed:"))
        self.assertEqual(seen_claimed, [("not_accepted", "claimed")])  # it was claimed first ...
        job = await self.job(job_id)
        self.assertEqual(job["status"], "queued")  # ... and released
        self.assertIsNone(job["claimed_by"])
        self.assertEqual(job["result"]["outcome"], "not_accepted")
        self.assertTrue(job["result"]["reason"].startswith("model_not_allowed:"))

    async def test_submitter_not_in_allowfrom_is_never_run_and_not_left_claimed(self):
        job_id = await self.submit("outsider", submitter=self.outsider)
        box = self.inbox()
        msg = await box.poll()
        self.assertEqual(msg["state"], "rejected")
        self.assertEqual(msg["from"], self.outsider)
        self.assertTrue(msg["reason"].startswith("source_not_allowed:"))
        self.assertEqual(box.disposition[job_id], "rejected")
        # no started / done events: the simulator never ran it
        self.assertEqual([s["event"] for s in box.sink_log], ["not_accepted"])
        job = await self.job(job_id)
        self.assertEqual(job["status"], "queued")  # not left claimed forever
        self.assertIsNone(job["claimed_by"])
        self.assertIsNone(job["claimed_at"])
        self.assertTrue(job["result"]["reason"].startswith("source_not_allowed:"))  # the documented reason
        self.assertEqual(job["result"]["last_released_by"], self.agent)
        r = await self.client.get("/v1/jobs", params={"status": "claimed"})
        self.assertEqual([j for j in r.json()["jobs"] if j["id"] == job_id], [])

    async def test_submitter_urn_longer_than_the_inbox_from_limit_is_refused_and_released(self):
        # FINDING: `from` is the submitter URN, `urn:agent:<kind>:<host>:<36-char uuid>`, and the inbox
        # takes at most 64 characters, so kind + host must total 16 or fewer. A longer URN can never be
        # delivered, even when it is listed in allowFrom.
        self.assertGreater(len(self.longname), FROM_MAX)
        self.assertLessEqual(len(self.outsider), FROM_MAX)
        self.env["HIVE_ORCHESTRATOR_URN_2"] = self.longname
        job_id = await self.submit("longurn", submitter=self.longname)
        box = self.inbox()
        msg = await box.poll()
        self.assertEqual(msg["state"], "rejected")
        self.assertTrue(msg["reason"].startswith("from_invalid:"), msg["reason"])
        job = await self.job(job_id)
        self.assertEqual((job["status"], job["claimed_by"]), ("queued", None))
        self.assertTrue(job["result"]["reason"].startswith("from_invalid:"))

    async def test_a_job_that_is_not_allowed_comes_back_after_the_backoff(self):
        # FINDING: release does not exclude this agent, so a not-allowed job bounces claim/release
        # (about every 10 s plus the poll interval) until the submitter or allowFrom is fixed.
        job_id = await self.submit("bounce", submitter=self.outsider)
        await self.inbox().poll()
        self.assertIsNone(await self.inbox().poll())  # inside the backoff
        self.clock.offset += 600
        again = await self.inbox().poll()
        self.assertEqual((again["id"], again["state"]), (job_id, "rejected"))

    # ------------------------------------------------------------------
    # idempotency and the completion gate
    # ------------------------------------------------------------------
    async def test_same_job_reported_twice_leaves_one_terminal_state(self):
        job_id = await self.submit("twice")
        box = self.inbox()
        msg = await box.poll()
        await box.start(msg)
        first = await box.finish(msg, "completed")
        second = await box.finish(msg, "completed")  # the sink is at-least-once
        self.assertFalse(first[0]["is_error"] or second[0]["is_error"], (first, second))
        job = await self.job(job_id)
        self.assertEqual(job["status"], "done")
        self.assertEqual(job["result"]["outcome"], "completed")
        # A different, late outcome for the same id cannot reopen or flip the terminal row.
        late = await box.finish(msg, "error")
        self.assertTrue(late[0]["is_error"])
        self.assertEqual(late[0]["body"]["status"], 409)
        self.assertIn("invalid status transition 'done' -> 'failed'", late[0]["body"]["body"])
        job = await self.job(job_id)
        self.assertEqual(job["status"], "done")
        self.assertEqual(job["result"]["outcome"], "completed")
        # One terminal row for the id, and the audit trail shows no second transition out of done.
        r = await self.client.get("/v1/jobs", params={"status": "done"})
        self.assertEqual([j["id"] for j in r.json()["jobs"]].count(job_id), 1)
        r = await self.client.get("/v1/jobs", params={"status": "failed"})
        self.assertEqual([j for j in r.json()["jobs"] if j["id"] == job_id], [])

    async def test_repeated_failed_report_stays_failed(self):
        job_id = await self.submit("failtwice")
        box = self.inbox()
        msg = await box.poll()
        await box.start(msg)
        await box.finish(msg, "error")
        again = await box.finish(msg, "error")
        self.assertFalse(again[0]["is_error"], again)
        self.assertEqual((await self.job(job_id))["status"], "failed")
        flipped = await box.finish(msg, "completed")
        self.assertTrue(flipped[0]["is_error"])
        self.assertEqual((await self.job(job_id))["status"], "failed")

    async def test_commit_gated_kind_does_not_reach_done_with_the_profile_result(self):
        # FINDING: a code kind (or any job with a repo outside the read-only families) needs commit
        # evidence for `done` (head_sha, commits, files_changed, clean worktree). The sink result carries
        # none, so the bus answers 409 completion_evidence_missing and parks the row as needs-review.
        job_id = await self.submit("gated", kind="feat:freebuff-profile")
        box = self.inbox()
        msg = await box.poll()
        await box.start(msg)
        sent = await box.finish(msg, "completed")
        self.assertTrue(sent[0]["is_error"])
        self.assertEqual(sent[0]["body"]["status"], 409)
        self.assertIn("completion_evidence_missing", sent[0]["body"]["body"])
        job = await self.job(job_id)
        self.assertEqual(job["status"], "needs-review")
        self.assertEqual(job["result"]["failure_reason"], "completion_evidence_missing")
        self.assertEqual(job["result"]["outcome"], "completed")  # the profile's fields are kept as evidence

    async def test_commit_gated_kind_reaches_done_when_the_result_carries_evidence(self):
        # The same job, with the evidence a worker would add, completes through the same sink call.
        job_id = await self.submit("gated-ok", kind="feat:freebuff-profile")
        box = self.inbox()
        msg = await box.poll()
        await box.start(msg)
        sha = "0123456789abcdef0123456789abcdef01234567"
        is_error, body = await self.mcp(
            "hive_job_update",
            {
                "id": job_id,
                "status": "done",
                "claimed_by": self.agent,
                "result": {
                    "outcome": "completed",
                    "head_sha": sha,
                    "commits": [sha],
                    "files_changed": ["README.md"],
                    "worktree_clean": True,
                },
            },
        )
        self.assertFalse(is_error, body)
        self.assertEqual((await self.job(job_id))["status"], "done")

    async def test_thin_done_after_evidenced_done_reopens_the_row(self):
        # FINDING: `done` -> `done` is an allowed PATCH, but it replaces the result and re-runs the
        # completion gate. If the agent reports evidence itself and the at-least-once sink then repeats
        # `done` with only the profile's fields, a finished commit-gated row is parked as needs-review
        # and the evidence is gone. Evidence must be reported once, by the last write.
        job_id = await self.submit("reopen", kind="feat:freebuff-profile")
        box = self.inbox()
        msg = await box.poll()
        await box.start(msg)
        sha = "0123456789abcdef0123456789abcdef01234567"
        is_error, body = await self.mcp(
            "hive_job_update",
            {
                "id": job_id,
                "status": "done",
                "claimed_by": self.agent,
                "result": {"commits": [sha], "head_sha": sha, "files_changed": ["README.md"], "worktree_clean": True},
            },
        )
        self.assertFalse(is_error, body)
        self.assertEqual((await self.job(job_id))["status"], "done")
        sent = await box.finish(msg, "completed")
        self.assertTrue(sent[0]["is_error"])
        job = await self.job(job_id)
        self.assertEqual(job["status"], "needs-review")
        self.assertNotIn("head_sha", job["result"])

    async def test_auto_review_job_of_a_parked_row_is_claimable_by_the_same_agent(self):
        # FINDING: parking a freebuff job as needs-review makes the bus submit a review job for it with no
        # eligible_kinds restriction, from the same submitter. The same inbox claims and accepts it, so the
        # author would review its own work.
        job_id = await self.submit("selfreview", kind="feat:freebuff-profile")
        box = self.inbox()
        msg = await box.poll()
        await box.start(msg)
        await box.finish(msg, "completed")
        review_id = (await self.job(job_id))["result"]["auto_review_job_id"]
        review = await self.job(review_id)
        self.assertTrue(review["kind"].startswith("review:"))
        self.assertIsNone(review["eligible_kinds"])
        again = await box.poll()
        self.assertEqual((again["id"], again["state"]), (review_id, "accepted"))
        self.assertEqual((await self.job(review_id))["claimed_by"], self.agent)


class ReadmeSubmitExampleTests(unittest.TestCase):
    def test_readme_task_example_builds_a_valid_freebuff_job(self):
        text = README.read_text()
        blocks = re.findall(r"```json\n(.*?)```", text, re.S)
        task = next(json.loads(b) for b in blocks if '"eligible_kinds"' in b)
        spec = importlib.util.spec_from_file_location("submit_job_for_profile", HERE / "scripts" / "submit_job.py")
        submit = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(submit)
        canonical = json.dumps(task, sort_keys=True, separators=(",", ":"))
        with mock.patch.dict(os.environ, {"HIVE_ACTOR_URN": "urn:agent:opencode:host:placeholder"}):
            payload, _ = submit.build_payload(task, canonical, inventory={})
        self.assertEqual(payload["eligible_kinds"], ["freebuff"])
        self.assertEqual(payload["max_retries"], 0)
        self.assertTrue(payload["zero_retry_reason"].strip())
        self.assertIn("head_branch", payload)
        self.assertIn("repo", payload)


if __name__ == "__main__":
    unittest.main()
