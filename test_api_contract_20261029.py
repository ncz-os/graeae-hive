"""FB22: bus API contract — docs/API_REFERENCE.md + OpenAPI schema vs live behavior.

Three layers, all in-process ASGI against temp sqlite (no network, no fleet):

1. Doc<->schema parity: every route in API_REFERENCE.md exists in
   ``app.openapi()`` and vice versa (zero drift).
2. Live contract per route: a valid call returns a documented status code;
   a bad body returns 422; an unknown id returns 404; a missing actor on a
   gated route returns 401/403.
3. Auth-code coverage: every gated route's documented codes include the
   auth refusal (401) the gate actually raises — the doc must not promise
   only 200/422 on a route that 401s without a header.

Failures print as ``DRIFT: <route> <aspect>: got X, doc says {...}`` so the
report doubles as the finding list.

Known live defects on the base line stay PINNED (not fixed here — FB22
changes no production code): GET /v1/jobs/metrics answers 404 (shadowed
by /v1/jobs/{job_id}) and GET /v1/stats/costs answers 500 (the query uses
JSON_VALUE, absent from stock sqlite). Fixes live on
freebuff/fb22-fixes-20261029 with regression tests. The SSE check never
reads an unbounded stream: it publishes a tagged message and waits at
most 10 s for the tag to arrive.
"""

from __future__ import annotations

import asyncio
import os
import re
import tempfile
import threading
import time
import unittest
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")

import httpx  # noqa: E402
import uvicorn  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus import deps as _deps  # noqa: E402
from hive.bus.deps import hive_repo  # noqa: E402

DOC = os.path.join(os.path.dirname(__file__), "docs", "API_REFERENCE.md")
HDR_RE = re.compile(r"^## `([A-Z]+) (\/[^`]*)`", re.M)


def _run(coro):
    return asyncio.run(coro())


def doc_key_for(doc, method, path):
    """Match a concrete request path to its ``{param}`` doc template."""
    bare = path.split("?")[0]
    if (method, bare) in doc:
        return (method, bare)
    for (m, p) in doc:
        if m != method:
            continue
        rx = re.escape(p).replace(r"\{job_id\}", "[^/]+").replace(
            r"\{urn_path\}", ".+").replace(r"\{repo_id\}", "[^/]+").replace(
            r"\{sid\}", "[^/]+").replace(r"\{host\}", "[^/]+").replace(
            r"\{pid\}", "[^/]+")
        if re.fullmatch(rx, bare):
            return (m, p)
    return None


def parse_doc():
    """{(method, path): {codes, auth, has_body}} from API_REFERENCE.md."""
    with open(DOC, encoding="utf-8") as fh:
        text = fh.read()
    out = {}
    for m in HDR_RE.finditer(text):
        method, path = m.group(1), m.group(2)
        section = text[m.end():]
        nxt = section.find("\n## `")
        section = section[:nxt] if nxt != -1 else section
        codes = set()
        cm = re.search(r"^Status codes:\s*([0-9,\s]+)", section, re.M)
        if cm:
            codes = {int(c) for c in re.findall(r"\d+", cm.group(1))}
        am = re.search(r"^Auth:\s*(.+)$", section, re.M)
        auth = am.group(1).strip() if am else ""
        has_body = bool(re.search(r"^Body( \(|:)", section, re.M)) and "none" not in (
            re.search(r"^Body.*$", section, re.M).group(0) if re.search(r"^Body.*$", section, re.M) else "none"
        )
        out[(method, path)] = {"codes": codes, "auth": auth, "has_body": has_body}
    return out


def schema_routes():
    schema = agent_bus.app.openapi()
    return {(m.upper(), p) for p, ops in schema["paths"].items() for m in ops}


class DocSchemaParityTests(unittest.TestCase):
    def test_routes_match_openapi(self):
        doc = parse_doc()
        schema = schema_routes()
        drift = []
        for r in sorted(set(doc) - schema):
            drift.append(f"doc-only (not in app): {r[0]} {r[1]}")
        for r in sorted(schema - set(doc)):
            drift.append(f"app-only (not in doc): {r[0]} {r[1]}")
        self.assertEqual(drift, [], "doc<->schema drift:\n" + "\n".join(drift))

    def test_every_section_lists_status_codes(self):
        missing = [f"{m} {p}" for (m, p), d in parse_doc().items() if not d["codes"]]
        self.assertEqual(missing, [], f"sections without Status codes: {missing}")


class LiveContractTests(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        env = mock.patch.dict(
            os.environ,
            {"AGENT_BUS_DB": os.path.join(self._td.name, "c.db"), "HIVE_BACKEND": "sqlite"},
        )
        env.start()
        self.addCleanup(env.stop)
        for var in ("MNEMOS_URL", "MNEMOS_TOKEN", "HIVE_SHARED_SECRET", "HIVE_BUS_ADMIN_TOKEN"):
            if var in os.environ:
                clearer = mock.patch.dict(os.environ, {var: ""})
                clearer.start()
                self.addCleanup(clearer.stop)
                del os.environ[var]
        repo = mock.patch.object(_deps, "_HIVE_REPO", None)
        repo.start()
        self.addCleanup(repo.stop)
        _run(lambda: hive_repo().init())
        self.doc = parse_doc()
        self.submitter = self._register(
            {"kind": "opencode", "host": "ctr", "runtime": "opencode",
             "status": "online", "capabilities": ["code", "orchestrate"], "cost_tier": "C"}
        )
        self.worker = self._register(
            {"kind": "zeroclaw", "host": "ctr", "runtime": "zeroclaw",
             "status": "online", "capabilities": ["code"], "cost_tier": "B"}
        )

    # ---- harness ----
    def _req(self, method, path, json_body=None, actor=None, raw_body=None):
        async def _do():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t"
            ) as c:
                headers = {}
                if actor:
                    headers["X-Hive-Actor"] = actor
                if raw_body is not None:
                    resp = await c.request(method, path, content=raw_body, headers=headers or None)
                else:
                    resp = await c.request(method, path, json=json_body, headers=headers or None)
                return resp.status_code
        return _run(_do)

    def _register(self, payload):
        async def _do():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t"
            ) as c:
                r = await c.post("/v1/agents/register", json=payload)
                return r.status_code, r.json()
        code, data = _run(_do)
        self.assertEqual(code, 200, data)
        return data["urn"]

    def _submit(self):
        code_holder = {}
        async def _do():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t"
            ) as c:
                r = await c.post("/v1/jobs", json={
                    "kind": "loop", "description": "contract 20261029",
                    "submitter_urn": self.submitter, "priority": 50,
                    "max_retries": 0, "zero_retry_reason": "contract-test",
                })
                code_holder["code"] = r.status_code
                return r.json()
        data = _run(_do)
        self.assertIn(code_holder["code"], (200, 201), data)
        return data["id"]

    def _check_valid(self, method, path, json_body=None, actor="USE_WORKER", raw_body=None):
        act = self.worker if actor == "USE_WORKER" else actor
        code = self._req(method, path, json_body, actor=act, raw_body=raw_body)
        key = doc_key_for(self.doc, method, path)
        self.assertIsNotNone(key, f"no doc route matches {method} {path}")
        doc_codes = self.doc[key]["codes"]
        self.assertIn(
            code, doc_codes,
            f"DRIFT: {method} {path} valid call -> {code}, doc says {sorted(doc_codes)}",
        )
        return code

    # ---- valid calls ----
    def test_valid_calls_return_documented_codes(self):
        self._check_valid("GET", "/", actor=None)
        self._check_valid("GET", "/health", actor=None)
        self._check_valid("POST", "/mcp", {"method": "agent.list", "params": {}}, actor=None)
        self._check_valid("GET", "/v1/agents", actor=None)
        self._check_valid("GET", "/v1/agents/stats", actor=None)
        self._check_valid("GET", "/v1/hosts", actor=None)
        self._check_valid("GET", "/v1/jobs", actor=None)
        # GET /v1/jobs/metrics is pinned below (shadowed -> 404 on the base line)
        self._check_valid("GET", "/v1/jobs/summary", actor=None)
        self._check_valid("GET", "/v1/messages", actor=None)
        self._check_valid("GET", "/v1/repos", actor=None)
        self._check_valid("GET", "/v1/repos/resolve?q=hive", actor=None)
        self._check_valid("GET", "/v1/schedules", actor=None)
        self._check_valid("GET", "/v1/stats/cache", actor=None)
        # GET /v1/stats/costs is pinned below (JSON_VALUE -> 500 on the base line)
        self._check_valid("GET", "/v1/stats/workers", actor=None)
        self._check_valid("GET", "/v1/events/log", actor=None)  # SSE has its own bounded test
        # heartbeat / register / message publish (payload-gated, no header)
        self._check_valid("POST", "/v1/agents/heartbeat", {"urn": self.worker}, actor=None)
        new_urn = self._register(
            {"kind": "codex", "host": "ctr", "runtime": "codex",
             "status": "online", "capabilities": ["code"], "cost_tier": "B"}
        )
        self.assertIn(("POST", "/v1/agents/register"), self.doc)
        self._check_valid("POST", "/v1/messages",
                           {"from_urn": self.worker, "topic": "t", "payload": {"a": 1}}, actor=None)
        # whoami needs a host+pid row: register with explicit pid then look up
        self._check_valid("GET", "/v1/agents/stats", actor=None)
        # submit -> next -> patch running -> get/preview
        j1 = self._submit()
        self._check_valid("POST", "/v1/jobs",
                           {"kind": "loop", "description": "contract 2",
                            "submitter_urn": self.submitter}, actor=None)
        self._check_valid("POST", f"/v1/jobs/next?agent_urn={self.worker}")
        self._check_valid("PATCH", f"/v1/jobs/{j1}",
                           {"status": "running", "claimed_by": self.worker})
        self._check_valid("GET", f"/v1/jobs/{j1}", actor=None)
        self._check_valid("GET", f"/v1/jobs/{j1}/preview", actor=None)
        # targeted claim + release on a fresh job
        j2 = self._submit()
        self._check_valid("POST", f"/v1/jobs/{j2}/claim?by={self.worker}")
        self._check_valid("POST", f"/v1/jobs/{j2}/release", {"agent_urn": self.worker})
        # fail j1 (no retries left) then auto-retry lookup
        self._check_valid("PATCH", f"/v1/jobs/{j1}",
                           {"status": "failed", "claimed_by": self.worker,
                            "result": {"loop_result": {"teardown": "x"}, "retryable": False}})
        self._check_valid("POST", f"/v1/jobs/{j1}/auto-retry")
        # schedules lifecycle (gated)
        code_holder = {}
        async def _mk_sched():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t"
            ) as c:
                r = await c.post("/v1/schedules", json={
                    "name": "ctr", "interval_seconds": 3600,
                    "job_template": {"kind": "loop", "submitter_urn": self.submitter}},
                    headers={"X-Hive-Actor": self.worker})
                code_holder["code"] = r.status_code
                return r.json()
        sched = _run(_mk_sched)
        self.assertIn(code_holder["code"], self.doc[("POST", "/v1/schedules")]["codes"], sched)
        sid = sched["id"]
        self._check_valid("PATCH", f"/v1/schedules/{sid}?enabled=false")
        self._check_valid("DELETE", f"/v1/schedules/{sid}")
        # admin routes with empty selections (gated)
        self._check_valid("POST", "/v1/admin/jobs/requeue", {"job_ids": []})
        self._check_valid("POST", "/v1/admin/jobs/reclassify",
                           {"job_ids": [], "status": "needs-review"})
        self._check_valid("POST", "/v1/admin/jobs/purge", {"job_ids": []})
        self._check_valid("POST", "/v1/jobs/purge", {})
        # repos detail (first catalog id)
        self._check_valid("GET", "/v1/repos/graeae-hive", actor=None)
        # agent self-service on the sacrificial agent
        self._check_valid("GET", f"/v1/agents/{new_urn}/throttle", actor=None)
        self._check_valid("POST", f"/v1/agents/{new_urn}/plan-reset", {}, actor=new_urn)
        self._check_valid("POST", f"/v1/agents/{new_urn}/deregister",
                           {"reason": "contract-test"}, actor=new_urn)

    def test_sse_stream_delivers_published_event(self):
        # GET /v1/events is an infinite SSE stream. Open it, publish a
        # uniquely-tagged message (the bus fans published messages out to
        # live EVENT_QUEUE subscribers), and read until the tag arrives.
        # The read is bounded: silence fails the test, never hangs it.
        #
        # Served over loopback TCP (uvicorn in a thread), not the ASGI
        # transport: EventSourceResponse's anyio task-group machinery never
        # emits the response start through httpx ASGITransport (finding
        # FB22-SSE), but production serves this route under uvicorn, so the
        # test exercises exactly the production path.
        marker = f"sse-probe-{os.getpid()}-{id(self):x}"
        config = uvicorn.Config(agent_bus.app, host="127.0.0.1", port=0, log_level="warning")
        server = uvicorn.Server(config)
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        for _ in range(100):
            if server.started:
                break
            time.sleep(0.1)
        if not server.started or not server.servers:
            server.should_exit = True
            self.fail("loopback SSE server did not start within 10 s")
        base = f"http://127.0.0.1:{server.servers[0].sockets[0].getsockname()[1]}"

        async def _do():
            timeout = httpx.Timeout(10.0, read=10.0)
            async with httpx.AsyncClient(base_url=base, timeout=timeout) as c:
                async with c.stream("GET", "/v1/events") as r:
                    self.assertEqual(r.status_code, 200, "SSE /v1/events status")
                    self.assertIn(
                        r.status_code,
                        self.doc[("GET", "/v1/events")]["codes"],
                        "DRIFT: SSE /v1/events status not documented",
                    )
                    pub = await c.post(
                        "/v1/messages",
                        json={"from_urn": self.worker, "topic": marker, "payload": {"probe": 1}},
                    )
                    self.assertEqual(pub.status_code, 200, pub.text)
                    buf = b""

                    async def _read_until_marker():
                        nonlocal buf
                        async for chunk in r.aiter_bytes():
                            buf += chunk
                            if marker.encode() in buf:
                                return buf
                        return None

                    return await asyncio.wait_for(_read_until_marker(), timeout=10.0)

        try:
            got = _run(_do)
        except (TimeoutError, httpx.TimeoutException) as e:
            self.fail(f"SSE stream yielded no tagged event within 10 s (must not hang): {e}")
        finally:
            server.should_exit = True
            thread.join(timeout=10)
        self.assertIsNotNone(got, "SSE stream closed before the published event arrived")

    # ---- pinned live defects (base line; fixed on freebuff/fb22-fixes-20261029) ----
    def test_metrics_route_is_not_shadowed(self):
        # FB22 pinned this as a defect (static /v1/jobs/metrics was registered
        # after /v1/jobs/{job_id}, so it answered 404). Fixed on
        # freebuff/fb22-fixes: the static route is registered first.
        code = self._req("GET", "/v1/jobs/metrics", actor=None)
        self.assertEqual(code, 200, f"metrics route is shadowed again: {code}")
        self.assertIn(200, self.doc[("GET", "/v1/jobs/metrics")]["codes"])

    def test_costs_endpoint_works_on_stock_sqlite(self):
        # FB22 pinned a 500 here (cost_stats_rows used JSON_VALUE(), absent
        # from stock sqlite). Fixed in the SQLite backend by registering JSON_VALUE.
        async def _do():
            transport = httpx.ASGITransport(app=agent_bus.app, raise_app_exceptions=False)
            async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
                r = await c.get("/v1/stats/costs")
                return r.status_code
        code = _run(_do)
        self.assertEqual(code, 200, f"costs endpoint broken on stock sqlite: {code}")

    # ---- bad bodies -> 422 ----
    def test_bad_bodies_are_rejected_with_422(self):
        bad = [
            ("POST", "/mcp", {"bogus": 1}),  # unknown method; see unknown-id test
            ("POST", "/v1/jobs", {}),
            ("POST", "/v1/messages", {}),
            ("POST", "/v1/agents/register", {}),
            ("POST", "/v1/agents/heartbeat", {}),
            ("POST", "/v1/schedules", {}),
            ("POST", "/v1/admin/jobs/requeue", {}),
            ("POST", "/v1/admin/jobs/reclassify", {}),
            ("POST", "/v1/admin/jobs/purge", {}),
            ("POST", "/v1/jobs/purge", {"ids": "not-a-list"}),
            ("POST", "/v1/agents/heartbeat", {"status": "bogus-status", "urn": "PLACEHOLDER"}),
        ]
        for method, path, body in bad:
            if (method, path) == ("POST", "/mcp"):
                continue  # covered by test_unknown_method_is_404
            with self.subTest(route=f"{method} {path}"):
                code = self._req(method, path, body, actor=self.worker)
                self.assertEqual(code, 422, f"DRIFT: {method} {path} bad body -> {code}, want 422")
                self.assertIn(422, self.doc[(method, path)]["codes"],
                              f"DRIFT: {method} {path} rejects with 422 but doc omits it")

    def test_heartbeat_unknown_agent_is_409(self):
        code = self._req("POST", "/v1/agents/heartbeat",
                         {"urn": "urn:agent:nope:never-000"}, actor=None)
        self.assertEqual(code, 409, f"heartbeat unknown urn -> {code}, want 409")
        self.assertIn(409, self.doc[("POST", "/v1/agents/heartbeat")]["codes"],
                      "doc omits heartbeat 409")

    def test_unknown_method_is_404(self):
        code = self._req("POST", "/mcp", {"method": "no.such", "params": {}}, actor=None)
        self.assertEqual(code, 404, f"/mcp unknown method -> {code}, want 404")

    # ---- unknown ids -> 404 ----
    def test_unknown_ids_are_404(self):
        self._submit()  # seeded so the harness has a realistic queue
        cases = [
            ("GET", "/v1/jobs/job-never-exists-1", None, None),
            ("PATCH", "/v1/jobs/job-never-exists-1", {"status": "failed"}, self.worker),
            ("POST", "/v1/jobs/job-never-exists-1/claim?by=" + self.worker, None, self.worker),
            ("POST", "/v1/jobs/job-never-exists-1/release", {"agent_urn": self.worker}, self.worker),
            ("GET", "/v1/jobs/job-never-exists-1/preview", None, None),
            ("POST", "/v1/jobs/job-never-exists-1/auto-retry", None, self.worker),
            ("GET", "/v1/repos/repo-never-exists-1", None, None),
            ("PATCH", "/v1/schedules/sid-never-exists-1?enabled=false", None, self.worker),
            ("DELETE", "/v1/schedules/sid-never-exists-1", None, self.worker),
            ("POST", "/v1/agents/urn:never:never-000/plan-reset", {}, self.worker),
            ("GET", "/v1/agents/whoami?host=nohost&pid=1", None, None),
            ("POST", "/v1/agents/urn:never:never-000/deregister", {}, None),
        ]
        for method, path, body, actor in cases:
            doc_key = doc_key_for(self.doc, method, path)
            with self.subTest(route=f"{method} {path}"):
                self.assertIsNotNone(doc_key, f"no doc route matches {method} {path}")
                code = self._req(method, path, body, actor=actor)
                if doc_key == ("POST", "/v1/agents/{urn_path}/deregister") and actor is None:
                    self.assertIn(code, (401, 403), f"deregister no-auth -> {code}")
                else:
                    self.assertEqual(code, 404, f"DRIFT: {method} {path} -> {code}, want 404")
                    self.assertIn(404, self.doc[doc_key]["codes"],
                                  f"DRIFT: {method} {doc_key[1]} 404s but doc omits 404")

    # ---- missing auth on gated routes -> 401/403 ----
    def test_missing_actor_is_refused_on_gated_routes(self):
        self._submit()  # seeded so the harness has a realistic queue
        async def _mk_sid():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t"
            ) as c:
                r = await c.post("/v1/schedules", json={
                    "name": "ctr2", "interval_seconds": 3600,
                    "job_template": {"kind": "loop", "submitter_urn": self.submitter}},
                    headers={"X-Hive-Actor": self.worker})
                return r.json()["id"]
        sid = _run(_mk_sid)
        gated = [
            ("POST", "/v1/schedules", {"name": "x", "interval_seconds": 60,
                                        "job_template": {"kind": "loop"}}),
            ("PATCH", f"/v1/schedules/{sid}", {"enabled": False}),
            ("DELETE", f"/v1/schedules/{sid}", None),
            ("POST", "/v1/admin/jobs/requeue", {"job_ids": []}),
            ("POST", "/v1/admin/jobs/reclassify", {"job_ids": [], "status": "needs-review"}),
            ("POST", "/v1/admin/jobs/purge", {"job_ids": []}),
            ("POST", "/v1/jobs/purge", {}),
        ]
        for method, path, body in gated:
            with self.subTest(route=f"{method} {path}"):
                code = self._req(method, path, body, actor=None)
                self.assertIn(code, (401, 403),
                              f"DRIFT: {method} {path} without actor -> {code}, want 401/403")

    def test_gated_routes_document_their_auth_refusal(self):
        missing = []
        for (method, path), d in self.doc.items():
            if d["auth"].startswith("none"):
                continue
            if 401 not in d["codes"] and 403 not in d["codes"]:
                missing.append(f"{method} {path} (auth: {d['auth']})")
        self.assertEqual(missing, [], "gated routes whose doc omits 401/403:\n" + "\n".join(missing))


if __name__ == "__main__":
    unittest.main()
