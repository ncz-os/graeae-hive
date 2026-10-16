"""Regression tests for production defects found in the subsystem review."""

import asyncio
import importlib.util
import io
import json
import os
import runpy
import tempfile
import time
import types
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest import mock

# Resolve every fixture/script path relative to THIS file, not the process CWD,
# so the suite passes from any working directory (e.g. `pytest tests/`).
HERE = Path(__file__).resolve().parent

os.environ["HIVE_BUS_TOKEN"] = "test-bus-token"
os.environ["HIVE_BUS_ADMIN_TOKEN"] = "test-admin-token"
os.environ["MCP_BRIDGE_TOKEN"] = "test-mcp-token"
os.environ["MCP_BRIDGE_ADMIN_TOKEN"] = "test-mcp-admin-token"
os.environ.setdefault("AGENT_BUS_DB", str(Path(tempfile.gettempdir()) / "hive-defect-tests.db"))

from starlette.requests import Request
from starlette.responses import Response

import agent_bus
from hive import scheduler as sched
from hive.bus.routers import admin as hive_admin_router
from hive.persistence.db2 import DB2_DDL, Db2HiveRepository, _adapt_sql
from hive.persistence.oracle import (
    _ORACLE_JOB_ADDITIONS,
    _ORACLE_REQUIRED_JOB_COLUMNS,
    OracleHiveRepository,
)
from hive.persistence.oracle import (
    _translate_sql as _translate_oracle_sql,
)
from hive.persistence.sqlite import SqliteHiveRepository


def load_script(name, filename):
    spec = importlib.util.spec_from_file_location(name, filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


auto_restore = load_script("hive_auto_restore", str(HERE / "hive-auto-restore.py"))
with mock.patch("pathlib.Path.mkdir"):
    worker = load_script("zeroclaw_worker_under_test", str(HERE / "tests" / "fixtures" / "zeroclaw_wss_worker_v2.py"))


class McpBridgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_mcp2_import_registers_typed_handlers(self):
        bridge = load_script("mcp_bridge_under_test", str(HERE / "mcp_bridge.py"))
        from mcp.types import PaginatedRequestParams

        self.assertEqual(bridge.MCP_VERSION.split(".", 1)[0], "2")
        handlers = getattr(bridge.server, "_request_handlers", {})
        self.assertTrue({"tools/list", "tools/call", "resources/list", "prompts/list"}.issubset(handlers))
        result = await handlers["tools/list"].handler(None, PaginatedRequestParams())
        self.assertIn("mnemos_memory_search", {tool.name for tool in result.tools})
        await bridge.client.aclose()

    async def test_all_mnemos_memory_tools_use_versioned_routes(self):
        bridge = load_script("mcp_bridge_routes_under_test", str(HERE / "mcp_bridge.py"))
        seen = []

        async def fake_mnemos(method, path, **kwargs):
            seen.append((method, path, kwargs))
            return {"ok": True}

        with mock.patch.object(bridge, "_mnemos", fake_mnemos):
            await bridge.handle_call_tool("mnemos_memory_search", {"query": "q"})
            await bridge.handle_call_tool("mnemos_memory_create", {"content": "c", "category": "test"})
            await bridge.handle_call_tool("mnemos_memory_get", {"id": "mem_1"})
        self.assertEqual(
            [path for _method, path, _kwargs in seen],
            [
                "/v1/memories/search",
                "/v1/memories",
                "/v1/memories/mem_1",
            ],
        )
        await bridge.client.aclose()

    async def test_orchestrator_registration_uses_admin_token(self):
        bridge = load_script("mcp_bridge_admin_auth_under_test", str(HERE / "mcp_bridge.py"))
        seen = []

        class Reply:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return {"urn": "urn:agent:human:test:1"}

        async def request(method, url, **kwargs):
            seen.append((method, url, kwargs))
            return Reply()

        with mock.patch.object(bridge.client, "request", request):
            for runtime in sorted(agent_bus.ORCHESTRATOR_RUNTIMES):
                await bridge._hive(
                    "POST",
                    "/v1/agents/register",
                    json={"kind": runtime, "runtime": runtime},
                    admin_authorized=True,
                )
        self.assertEqual(len(seen), len(agent_bus.ORCHESTRATOR_RUNTIMES))
        self.assertTrue(
            all(kwargs["headers"]["Authorization"] == "Bearer test-admin-token" for _method, _url, kwargs in seen)
        )
        await bridge.client.aclose()

    async def test_bridge_requires_inbound_bearer_and_has_admin_scope(self):
        bridge = load_script("mcp_bridge_inbound_auth_under_test", str(HERE / "mcp_bridge.py"))
        self.assertTrue(bridge.app.user_middleware)

        async def request(token=None):
            headers = [] if token is None else [(b"authorization", f"Bearer {token}".encode())]
            scope = {
                "type": "http",
                "method": "POST",
                "path": "/messages/",
                "raw_path": b"/messages/",
                "query_string": b"",
                "headers": headers,
                "scheme": "http",
                "server": ("test", 80),
                "client": ("test", 1),
            }
            sent = []

            async def receive():
                return {"type": "http.request", "body": b"", "more_body": False}

            async def send(message):
                sent.append(message)

            await bridge.app(scope, receive, send)
            return next(message["status"] for message in sent if message["type"] == "http.response.start")

        self.assertEqual(await request(), 401)
        self.assertEqual(await request("wrong"), 401)
        self.assertEqual(await request("test-mcp-token"), 400)
        await bridge.client.aclose()

    async def test_bridge_rejects_reused_downstream_credential(self):
        bridge = load_script("mcp_bridge_distinct_auth_under_test", str(HERE / "mcp_bridge.py"))
        bridge.MCP_BRIDGE_TOKEN = bridge.HIVE_BUS_TOKEN
        with self.assertRaisesRegex(RuntimeError, "must be distinct"):
            async with bridge.lifespan(bridge.app):
                pass
        await bridge.client.aclose()

    async def test_tool_failures_set_mcp_is_error(self):
        bridge = load_script("mcp_bridge_error_result_under_test", str(HERE / "mcp_bridge.py"))
        from mcp.server.auth.middleware.auth_context import AuthenticatedUser
        from mcp.server.auth.provider import AccessToken
        from mcp.types import CallToolRequestParams

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
            AccessToken(
                token="test-mcp-token",
                client_id="client",
                scopes=["bridge:use"],
            )
        )
        context = types.SimpleNamespace(request=request)
        handler = bridge.server._request_handlers["tools/call"].handler
        with mock.patch.object(bridge, "_mnemos", side_effect=OSError("down")):
            result = await handler(
                context,
                CallToolRequestParams(
                    name="mnemos_memory_search",
                    arguments={"query": "q"},
                ),
            )
        self.assertTrue(result.is_error)
        self.assertTrue(result.model_dump(by_alias=True)["isError"])
        await bridge.client.aclose()

    async def test_memory_tags_are_nested_in_metadata(self):
        bridge = load_script("mcp_bridge_tags_under_test", str(HERE / "mcp_bridge.py"))
        seen = {}

        async def fake_mnemos(_method, _path, **kwargs):
            seen.update(kwargs["json"])
            return {"ok": True}

        with mock.patch.object(bridge, "_mnemos", fake_mnemos):
            outcome = await bridge.handle_call_tool(
                "mnemos_memory_create",
                {"content": "c", "category": "test", "tags": ["release"]},
            )
        self.assertFalse(outcome.is_error)
        self.assertNotIn("tags", seen)
        self.assertEqual(seen["metadata"]["tags"], ["release"])
        await bridge.client.aclose()

    async def test_ordinary_bridge_scope_cannot_borrow_admin_bus_token(self):
        bridge = load_script("mcp_bridge_tool_auth_under_test", str(HERE / "mcp_bridge.py"))
        outcome = await bridge.handle_call_tool(
            "hive_agent_register",
            {"kind": "human", "host": "test"},
        )
        self.assertTrue(outcome.is_error)
        self.assertIn("administrator MCP bridge credential required", outcome.content[0].text)
        await bridge.client.aclose()

    async def test_worker_registration_still_uses_worker_token(self):
        bridge = load_script("mcp_bridge_worker_auth_under_test", str(HERE / "mcp_bridge.py"))
        seen = []

        class Reply:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return {"urn": "urn:agent:codex:test:1"}

        async def request(_method, _url, **kwargs):
            seen.append(kwargs)
            return Reply()

        with mock.patch.object(bridge.client, "request", request):
            for runtime in sorted(agent_bus.WORKER_ONLY_RUNTIMES):
                await bridge._hive(
                    "POST",
                    "/v1/agents/register",
                    json={"kind": runtime, "runtime": runtime},
                )
        self.assertEqual(len(seen), len(agent_bus.WORKER_ONLY_RUNTIMES))
        self.assertTrue(all(kwargs["headers"]["Authorization"] == "Bearer test-bus-token" for kwargs in seen))
        await bridge.client.aclose()


@unittest.skip(
    "Bus bearer-token auth (require_bus_auth / browser sessions) is not "
    "implemented on the argonas line — the fleet is local-LAN only and does not "
    "require auth (operator 2026-10-15). These tests target the forge lineage."
)
class AuthTests(unittest.IsolatedAsyncioTestCase):
    async def _request(self, path, token=None, method="GET", cookie=None, call_next=None):
        headers = []
        if token is not None:
            headers.append((b"authorization", f"Bearer {token}".encode()))
        if cookie is not None:
            headers.append((b"cookie", cookie.encode()))
        request = Request(
            {
                "type": "http",
                "method": method,
                "path": path,
                "raw_path": path.encode(),
                "query_string": b"",
                "headers": headers,
                "scheme": "http",
                "server": ("test", 80),
            }
        )

        if call_next is None:

            async def call_next(_request):
                return Response(status_code=204)

        return await agent_bus.require_bus_auth(request, call_next)

    async def test_v1_and_mcp_require_constant_bearer(self):
        self.assertEqual((await self._request("/v1/jobs")).status_code, 401)
        self.assertEqual((await self._request("/mcp", "wrong")).status_code, 401)
        self.assertEqual((await self._request("/v1/jobs", "test-bus-token")).status_code, 204)

    async def test_admin_uses_separate_token(self):
        self.assertEqual((await self._request("/v1/admin/jobs/requeue", "test-bus-token")).status_code, 401)
        self.assertEqual((await self._request("/v1/admin/jobs/requeue", "test-admin-token")).status_code, 204)
        self.assertEqual((await self._request("/health")).status_code, 204)

    async def test_all_operator_mutations_require_admin_role(self):
        paths = [
            ("POST", "/v1/agents/urn:agent:claude:host:id/plan-reset"),
            ("POST", "/v1/schedules"),
            ("PATCH", "/v1/schedules/sched-1"),
            ("DELETE", "/v1/schedules/sched-1"),
            ("POST", "/v1/knemon/bypass"),
            ("POST", "/v1/knemon/cap/clear"),
            ("POST", "/v1/knemon/cap/probe-ok"),
        ]
        for method, path in paths:
            with self.subTest(method=method, path=path):
                self.assertEqual((await self._request(path, "test-bus-token", method)).status_code, 401)
                self.assertEqual((await self._request(path, "test-admin-token", method)).status_code, 204)

    async def test_browser_session_cookie_authenticates_api_and_sse(self):
        async def issue(request):
            return await agent_bus.create_browser_session(request)

        response = await self._request(
            "/v1/auth/session",
            "test-admin-token",
            "POST",
            call_next=issue,
        )
        cookie = response.headers["set-cookie"].split(";", 1)[0]
        self.assertIn("HttpOnly", response.headers["set-cookie"])
        self.assertIn("SameSite=strict", response.headers["set-cookie"])
        self.assertEqual((await self._request("/v1/events", cookie=cookie)).status_code, 204)
        self.assertEqual((await self._request("/v1/schedules", method="POST", cookie=cookie)).status_code, 204)
        https_request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/v1/auth/session",
                "raw_path": b"/v1/auth/session",
                "query_string": b"",
                "headers": [(b"authorization", b"Bearer test-admin-token")],
                "scheme": "https",
                "server": ("test", 443),
            }
        )
        https_request.state.auth_role = "admin"
        https_response = await agent_bus.create_browser_session(https_request)
        self.assertIn("Secure", https_response.headers["set-cookie"])

    async def test_browser_session_cookie_cannot_renew_itself(self):
        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/v1/auth/session",
                "raw_path": b"/v1/auth/session",
                "query_string": b"",
                "headers": [],
                "scheme": "http",
                "server": ("test", 80),
            }
        )
        request._cookies = {
            "hive_session": agent_bus._session_cookie(
                "admin",
                int(time.time()) + 120,
            )
        }
        request.state.auth_role = "admin"
        with self.assertRaises(agent_bus.HTTPException) as raised:
            await agent_bus.create_browser_session(request)
        self.assertEqual(raised.exception.status_code, 401)

    async def test_startup_rejects_equal_worker_and_admin_tokens(self):
        old_worker, old_admin = agent_bus.HIVE_BUS_TOKEN, agent_bus.HIVE_BUS_ADMIN_TOKEN
        agent_bus.HIVE_BUS_TOKEN = agent_bus.HIVE_BUS_ADMIN_TOKEN = "same-secret"
        try:
            with self.assertRaisesRegex(RuntimeError, "must be distinct"):
                async with agent_bus.lifespan(agent_bus.app):
                    pass
        finally:
            agent_bus.HIVE_BUS_TOKEN, agent_bus.HIVE_BUS_ADMIN_TOKEN = old_worker, old_admin

    async def test_worker_role_cannot_register_synthetic_orchestrator(self):
        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/v1/agents/register",
                "raw_path": b"/v1/agents/register",
                "query_string": b"",
                "headers": [],
                "scheme": "http",
                "server": ("test", 80),
            }
        )
        request.state.auth_role = "worker"
        with self.assertRaises(agent_bus.HTTPException) as raised:
            await agent_bus.register(
                agent_bus.AgentRegister(host="attacker", runtime="human", kind="human"),
                request,
            )
        self.assertEqual(raised.exception.status_code, 403)


class PersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_job_list_result_projection_is_opt_in_without_breaking_default(self):
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "jobs.db"))
            await repo.init(agent_bus.SCHEMA)
            await repo.submit_job(
                id="job-result",
                submitter_urn="urn:submitter",
                kind="test",
                started_at=1.0,
            )
            async with repo.connection() as db:
                await db.execute(
                    "UPDATE jobs SET status='done', ended_at=?, result=? WHERE id=?",
                    (2.0, json.dumps({"payload": "large"}), "job-result"),
                )
                await db.commit()

            with mock.patch.object(agent_bus, "connect_db", repo.connection):
                historical = await agent_bus.list_jobs(status="done")
                compact = await agent_bus.list_jobs(status="done", include_result=False)

            self.assertEqual(historical["jobs"][0]["result"], {"payload": "large"})
            self.assertNotIn("result", compact["jobs"][0])

    async def test_claim_resolves_candidate_dependencies_in_one_query(self):
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "claims.db"))
            await repo.init(agent_bus.SCHEMA)
            now = time.time()
            async with repo.connection() as db:
                await db.execute(
                    "INSERT INTO agents (urn, kind, host, session_id, capabilities, started_at, "
                    "last_heartbeat, status, runtime, cost_tier) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "urn:agent:zeroclaw:host:session",
                        "zeroclaw",
                        "host",
                        "session",
                        "[]",
                        now,
                        now,
                        "online",
                        "zeroclaw",
                        "B",
                    ),
                )
                for job_id, status in (("dep-done", "done"), ("dep-pending", "queued")):
                    await db.execute(
                        "INSERT INTO jobs (id, submitter_urn, kind, priority, eligible_kinds, status, started_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (job_id, "urn:submitter", "dependency", 0, '["other"]', status, now),
                    )
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, priority, depends_on, status, started_at) "
                    "VALUES (?, ?, ?, ?, ?, 'queued', ?)",
                    ("blocked", "urn:submitter", "test", 10, '["dep-pending"]', now),
                )
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, priority, depends_on, status, started_at) "
                    "VALUES (?, ?, ?, ?, ?, 'queued', ?)",
                    ("ready", "urn:submitter", "test", 9, '["dep-done"]', now),
                )
                await db.commit()

            dependency_queries = []

            @asynccontextmanager
            async def traced_connection():
                async with repo.connection() as db:
                    original_execute = db.execute

                    def traced_execute(sql, parameters=None):
                        if "SELECT id FROM jobs WHERE id IN" in sql and "status='done'" in sql:
                            dependency_queries.append(sql)
                        return original_execute(sql, parameters) if parameters is not None else original_execute(sql)

                    db.execute = traced_execute
                    yield db

            with (
                mock.patch.object(agent_bus, "connect_db", traced_connection),
                mock.patch.object(agent_bus, "emit_event", mock.AsyncMock()),
            ):
                claimed = await agent_bus.dequeue_next_job("urn:agent:zeroclaw:host:session")

            self.assertEqual(claimed["id"], "ready")
            self.assertEqual(len(dependency_queries), 1)

    async def test_fresh_sqlite_has_dedup_column_and_active_unique_index(self):
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "fresh.db"))
            await repo.init(agent_bus.SCHEMA)
            async with repo.connection() as db:
                cols = await (await db.execute("PRAGMA table_info(jobs)")).fetchall()
                indexes = await (await db.execute("PRAGMA index_list(jobs)")).fetchall()
            self.assertIn("dedup_hash", {row[1] for row in cols})
            self.assertIn("jobs_active_dedup_uq", {row[1] for row in indexes})

    async def test_db2_lifespan_uses_repository_init_not_sqlite_schema(self):
        class FakeRepo:
            def __init__(self):
                self.initialized = object()
                self.closed = False

            async def init(self, schema=None):
                self.initialized = schema

            async def close(self):
                self.closed = True

        fake = FakeRepo()

        async def idle_reaper(_app):
            await asyncio.Event().wait()

        old_backend = agent_bus.HIVE_BACKEND
        agent_bus.HIVE_BACKEND = "db2"
        try:
            with (
                mock.patch.object(agent_bus, "hive_repo", return_value=fake),
                mock.patch.object(agent_bus, "reaper_task", idle_reaper),
            ):
                async with agent_bus.lifespan(agent_bus.app):
                    self.assertIsNone(fake.initialized)
            self.assertTrue(fake.closed)
        finally:
            agent_bus.HIVE_BACKEND = old_backend

    async def test_oracle_populated_core_schema_is_converged_and_verified(self):
        repo = OracleHiveRepository("oracle://user:pass@db/TEST")
        columns = {name: ("CLOB", None) for name in _ORACLE_REQUIRED_JOB_COLUMNS - set(_ORACLE_JOB_ADDITIONS)}
        columns["STATUS"] = ("VARCHAR2", 16)
        indexes = set()
        checks = [("CK_HIVE_JOBS_STATUS", "status IN ('queued','done')")]
        active = [("job-old", "same-hash"), ("job-new", "same-hash")]
        sql_seen = []

        dead_lettered: list = []

        async def fake_execute(_raw, sql, params=()):
            sql_seen.append((sql, params))
            upper = sql.upper()
            if "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH" in upper:
                return [(name, data_type, length) for name, (data_type, length) in columns.items()]
            if "SELECT INDEX_NAME FROM USER_INDEXES" in upper:
                return [(name,) for name in indexes]
            if "SELECT CONSTRAINT_NAME, SEARCH_CONDITION_VC" in upper:
                # A NOT NULL check must never be mistaken for the status enum.
                return checks + [("SYS_C_STATUS_NN", '"STATUS" IS NOT NULL')]
            if upper.startswith("ALTER TABLE HIVE_JOBS ADD ("):
                for name in _ORACLE_JOB_ADDITIONS:
                    if name in upper:
                        data_type = (
                            "NUMBER"
                            if name
                            in {
                                "RETRY_COUNT",
                                "MAX_RETRIES",
                                "RETRY_BACKOFF_UNTIL",
                                "LAST_UPDATE_AT",
                                "CLAIM_LEASE_EXPIRES_AT",
                                "DECLINE_COUNT",
                            }
                            else "CLOB"
                        )
                        length = None
                        if name in {"REQUIRED_AUTONOMY", "PROJECT", "DEDUP_HASH"}:
                            data_type = "VARCHAR2"
                            length = {"REQUIRED_AUTONOMY": 32, "PROJECT": 120, "DEDUP_HASH": 64}[name]
                        columns[name] = (data_type, length)
                        break
            elif "MODIFY (STATUS VARCHAR2(32))" in upper:
                columns["STATUS"] = ("VARCHAR2", 32)
            elif "DROP CONSTRAINT" in upper:
                checks.clear()
            elif "ADD CONSTRAINT CK_HIVE_JOBS_STATUS_RUNTIME" in upper:
                checks.append(("CK_HIVE_JOBS_STATUS_RUNTIME", sql))
            elif "SELECT ID, DEDUP_HASH FROM HIVE_JOBS" in upper:
                return active
            elif "DEDUP_HASH=NULL" in upper:
                # The migration now dead-letters the duplicate as well as
                # clearing its dedup_hash, so the id is the LAST bind, not the
                # first (:1 carries the migration result JSON).
                job_id = params[-1]
                active[:] = [row for row in active if row[0] != job_id]
                if "DEAD-LETTER" in upper:
                    dead_lettered.append(job_id)
            elif "CREATE INDEX IDX_HIVE_JOBS_CLAIM_LEASE" in upper:
                indexes.add("IDX_HIVE_JOBS_CLAIM_LEASE")
            elif "CREATE UNIQUE INDEX JOBS_ACTIVE_DEDUP_UQ" in upper:
                indexes.add("JOBS_ACTIVE_DEDUP_UQ")
            return []

        with mock.patch("hive.persistence.oracle._raw_execute", side_effect=fake_execute):
            await repo._apply_migrations(object())
            await repo._verify_schema(object())
        self.assertTrue(_ORACLE_JOB_ADDITIONS.keys() <= columns.keys())
        self.assertEqual(columns["STATUS"], ("VARCHAR2", 32))
        self.assertEqual(active, [("job-old", "same-hash")])
        # Nulling dedup_hash alone satisfied the unique index while leaving the
        # duplicate claimable, so a worker still ran it -- the double execution
        # the index exists to prevent. It must be dead-lettered too.
        self.assertEqual(dead_lettered, ["job-new"])
        self.assertIn("JOBS_ACTIVE_DEDUP_UQ", indexes)
        self.assertFalse(any("DROP CONSTRAINT SYS_C_STATUS_NN" in sql for sql, _ in sql_seen))

    async def test_oracle_repository_init_runs_migration_and_verification(self):
        repo = OracleHiveRepository("oracle://user:pass@db/TEST")
        raw = mock.AsyncMock()
        fake_driver = types.SimpleNamespace(connect_async=mock.AsyncMock(return_value=raw))
        with (
            mock.patch.dict("sys.modules", {"oracledb": fake_driver}),
            mock.patch.object(repo, "_apply_migrations", mock.AsyncMock()) as migrate,
            mock.patch.object(repo, "_verify_schema", mock.AsyncMock()) as verify,
        ):
            await repo.init()
        migrate.assert_awaited_once_with(raw)
        verify.assert_awaited_once_with(raw)
        raw.commit.assert_awaited_once()

    async def test_health_returns_503_when_database_probe_fails(self):
        @asynccontextmanager
        async def broken_connection():
            raise OSError("database unavailable")
            yield

        with mock.patch.object(agent_bus, "connect_db", broken_connection):
            response = await agent_bus.health()
        self.assertEqual(response.status_code, 503)
        self.assertEqual(json.loads(response.body)["status"], "unhealthy")

    async def test_health_returns_503_when_reaper_worker_has_stopped(self):
        class Cursor:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def fetchone(self):
                return (0,)

        class Db:
            def execute(self, _sql):
                return Cursor()

        @asynccontextmanager
        async def working_connection():
            yield Db()

        stopped = mock.Mock()
        stopped.done.return_value = True
        old = getattr(agent_bus.app.state, "reaper_task", None)
        agent_bus.app.state.reaper_task = stopped
        try:
            with mock.patch.object(agent_bus, "connect_db", working_connection):
                response = await agent_bus.health()
        finally:
            agent_bus.app.state.reaper_task = old
        self.assertEqual(response.status_code, 503)
        self.assertIn("reaper task is not running", json.loads(response.body)["detail"])

    async def test_db2_populated_schema_is_migrated_and_verified(self):
        class Result:
            def __init__(self, rows=None):
                self.rows = rows or []

            def __await__(self):
                async def done():
                    return self

                return done().__await__()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def fetchall(self):
                return list(self.rows)

        class FakeDb:
            def __init__(self):
                self.columns = {
                    "HIVE_AGENTS": {
                        "URN",
                        "KIND",
                        "HOST",
                        "SESSION_ID",
                        "PID",
                        "CAPABILITIES",
                        "VERSION",
                        "STARTED_AT",
                        "LAST_HEARTBEAT",
                        "STATUS",
                        "METADATA",
                    },
                    "HIVE_JOBS": {
                        "ID",
                        "SUBMITTER_URN",
                        "PARENT_JOB_ID",
                        "KIND",
                        "DESCRIPTION",
                        "PRIORITY",
                        "DEADLINE",
                        "REQUIRED_CAPABILITIES",
                        "ELIGIBLE_KINDS",
                        "STATUS",
                        "CLAIMED_BY",
                        "CLAIMED_AT",
                        "STARTED_AT",
                        "ENDED_AT",
                        "RESULT",
                    },
                }
                self.indexes = {"HIVE_JOBS": set()}
                # Simulate an installation whose old parallel tables already
                # recorded both local migration versions.
                self.versions = {1, 2}
                self.active_jobs = [("job-old", "same-hash"), ("job-new", "same-hash")]
                self.retired = []
                self.sql = []
                self.lengths = {"HIVE_AGENTS": {"STATUS": 32}, "HIVE_JOBS": {"STATUS": 16}}

            def execute(self, sql, params=None):
                self.sql.append((sql, tuple(params or ())))
                upper = sql.upper()
                if "FROM HIVE_SCHEMA_MIGRATIONS" in upper:
                    return Result([(v,) for v in sorted(self.versions)])
                if "SELECT ID, DEDUP_HASH FROM JOBS" in upper:
                    return Result(self.active_jobs)
                if "FROM SYSCAT.COLUMNS" in upper:
                    if "COLNAME, LENGTH" in upper:
                        return Result(list(self.lengths[params[0]].items()))
                    return Result([(v,) for v in sorted(self.columns[params[0]])])
                if "FROM SYSCAT.INDEXES" in upper:
                    return Result([(v,) for v in sorted(self.indexes.get(params[0], set()))])
                if "FROM SYSCAT.CHECKS" in upper:
                    return Result([("CK_OLD_STATUS", "CHECK (status IN ('online'))")])
                if upper.startswith("ALTER TABLE") and "ADD COLUMN" in upper:
                    table = upper.split()[2]
                    name = upper.split("ADD COLUMN", 1)[1].strip().split()[0]
                    self.columns[table].add(name)
                elif "ALTER COLUMN STATUS SET DATA TYPE VARCHAR(32)" in upper:
                    self.lengths["HIVE_JOBS"]["STATUS"] = 32
                elif upper.startswith("CREATE UNIQUE INDEX"):
                    self.indexes["HIVE_JOBS"].add("JOBS_ACTIVE_DEDUP_UQ")
                elif upper.startswith("UPDATE JOBS SET STATUS='DEAD-LETTER'"):
                    self.retired.append(params[-1])
                elif upper.startswith("INSERT INTO HIVE_SCHEMA_MIGRATIONS"):
                    self.versions.add(int(params[0]))
                return Result()

        db = FakeDb()
        repo = Db2HiveRepository("database=test")
        await repo._apply_migrations(db)
        await repo._verify_schema(db)
        self.assertTrue({1, 2}.issubset(db.versions))
        self.assertIn("DEDUP_HASH", db.columns["HIVE_JOBS"])
        self.assertIn("DECLINE_COUNT", db.columns["HIVE_JOBS"])
        self.assertIn("DEDUP_ACTIVE_HASH", db.columns["HIVE_JOBS"])
        self.assertIn("JOBS_ACTIVE_DEDUP_UQ", db.indexes["HIVE_JOBS"])
        self.assertEqual(db.retired, ["job-new"])
        self.assertTrue(any("DROP CHECK CK_OLD_STATUS" in sql for sql, _params in db.sql))
        self.assertEqual(db.lengths["HIVE_JOBS"]["STATUS"], 32)
        self.assertTrue(any("ALTER COLUMN status SET DATA TYPE VARCHAR(32)" in sql for sql, _ in db.sql))
        catalog_sql = [sql for sql, params in db.sql if params]
        self.assertTrue(all("?" in sql for sql in catalog_sql))

    async def test_fresh_db2_ddl_has_active_only_generated_key(self):
        self.assertIn("dedup_active_hash VARCHAR(64) GENERATED ALWAYS", DB2_DDL)
        self.assertIn("hive_schema_migrations", DB2_DDL)

    async def test_db2_adapter_targets_canonical_mnemos_hive_tables(self):
        cases = {
            "SELECT * FROM jobs WHERE id=?": "FROM hive_jobs",
            "INSERT INTO agents (urn) VALUES (?)": "INTO hive_agents",
            "UPDATE scheduled_jobs SET enabled=0": "UPDATE hive_scheduled_jobs",
            "DELETE FROM messages WHERE id=?": "FROM hive_messages",
            "CREATE INDEX idx_test ON worker_kind_stats(kind)": "ON hive_worker_kind_stats",
        }
        for source, expected in cases.items():
            with self.subTest(source=source):
                adapted, _ = _adapt_sql(source, ("x",) if "?" in source else ())
                self.assertIn(expected, adapted)
        pragma, params = _adapt_sql("PRAGMA table_info(jobs)")
        self.assertIn("syscat.columns", pragma)
        self.assertEqual(params, ("HIVE_JOBS",))
        cache_sql, _ = _adapt_sql(
            "INSERT INTO hive_cache (cache_key) VALUES (?) "
            "ON CONFLICT(cache_key) DO UPDATE SET result_json=excluded.result_json",
            ("k",),
        )
        self.assertIn("MERGE INTO hive_cache", cache_sql)

    async def test_admin_requeue_clears_stale_execution_and_result_fields(self):
        seen = []

        class Cursor:
            def __init__(self, row=None):
                self.row = row

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def fetchone(self):
                return self.row

        class Db:
            def execute(self, sql, params=None):
                seen.append((sql, tuple(params or ())))
                cursor = Cursor(("failed",) if sql.startswith("SELECT status") else None)
                if sql.startswith("SELECT status"):
                    return cursor

                async def executed():
                    return cursor

                return executed()

            async def commit(self):
                return None

        @asynccontextmanager
        async def connection():
            yield Db()

        with mock.patch.object(hive_admin_router, "connect_db", connection):
            result = await agent_bus.admin_requeue_jobs(
                agent_bus.RequeueRequest(job_ids=["job-1"]),
            )
        self.assertEqual(result["ok"], ["job-1"])
        update = next(sql for sql, _params in seen if sql.lstrip().startswith("UPDATE jobs"))
        for fragment in (
            "ended_at=NULL",
            "result=NULL",
            "claimed_runtime=NULL",
            "claimed_host_caps=NULL",
            "claim_lease_expires_at=NULL",
            "tokens_in=NULL",
            "tokens_out=NULL",
            "estimated_cost_usd=NULL",
            "result_mnemos_id=NULL",
        ):
            self.assertIn(fragment, update)
        oracle_sql, oracle_params = _translate_oracle_sql(update, seen[-1][1])
        db2_sql, db2_params = _adapt_sql(update, seen[-1][1])
        self.assertIn("UPDATE hive_jobs", oracle_sql)
        self.assertIn(":1", oracle_sql)
        self.assertEqual(oracle_params, seen[-1][1])
        self.assertIn("UPDATE hive_jobs", db2_sql)
        self.assertIn("?", db2_sql)
        self.assertEqual(db2_params, seen[-1][1])

    async def test_admin_requeue_resets_real_default_sqlite_row(self):
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "requeue.db"))
            await repo.init(agent_bus.SCHEMA)
            async with repo.connection() as db:
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, status, started_at, "
                    "claimed_by, ended_at, result, claimed_runtime, tokens_in, "
                    "estimated_cost_usd, result_mnemos_id, claim_lease_expires_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "job-real",
                        "urn:submitter",
                        "fix:test",
                        "failed",
                        1.0,
                        "urn:worker",
                        2.0,
                        "{}",
                        "zeroclaw",
                        10,
                        1.25,
                        "mem-1",
                        3.0,
                    ),
                )
                await db.commit()
            with mock.patch.object(hive_admin_router, "connect_db", repo.connection):
                result = await agent_bus.admin_requeue_jobs(
                    agent_bus.RequeueRequest(job_ids=["job-real"]),
                )
            self.assertEqual(result["ok"], ["job-real"])
            async with repo.connection() as db:
                async with db.execute(
                    "SELECT status, claimed_by, ended_at, result, claimed_runtime, "
                    "tokens_in, estimated_cost_usd, result_mnemos_id, claim_lease_expires_at "
                    "FROM jobs WHERE id=?",
                    ("job-real",),
                ) as cur:
                    row = await cur.fetchone()
            self.assertEqual(row, ("queued", None, None, None, None, None, None, None, None))


class SchedulerDequeueTests(unittest.IsolatedAsyncioTestCase):
    """End-to-end scheduler wiring tests against the agent_bus dequeue flow.

    These complement the pure-API checks in ``test_scheduler.py`` by
    exercising the full dequeue_next_job path (eligibility + role-aware
    candidate ordering + MiniMax pool gate + claim UPDATE) against a
    real SQLite repository.
    """

    async def _seed_worker_and_jobs(
        self,
        repo,
        *,
        urn: str,
        kind: str,
        host: str,
        capabilities: list[str],
        current_load: dict | None,
        jobs: list[tuple[str, str, int, str | None, str | None]],
    ):
        """Insert one worker + N jobs. jobs is a list of
        (job_id, kind, priority, required_caps_json, eligible_hosts_json).
        """
        now = time.time()
        async with repo.connection() as db:
            await db.execute(
                "INSERT INTO agents (urn, kind, host, session_id, capabilities, current_load, "
                "started_at, last_heartbeat, status, runtime, cost_tier, model, provider) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    urn,
                    kind,
                    host,
                    "session",
                    json.dumps(capabilities),
                    json.dumps(current_load) if current_load else None,
                    now,
                    now,
                    "online",
                    kind,
                    "B",
                    "default",
                    "default",
                ),
            )
            for jid, jkind, jprio, jcaps, jhosts in jobs:
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                    "required_capabilities, eligible_hosts, started_at) "
                    "VALUES (?, ?, ?, ?, 'queued', ?, ?, ?)",
                    (jid, "urn:submitter", jkind, jprio, jcaps, jhosts, now),
                )
            await db.commit()

    async def test_dequeue_ordinary_prefers_non_build_worker(self):
        """An idle non-build worker wins the claim for ordinary work over an
        idle build worker when both are eligible."""
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "role.db"))
            await repo.init(agent_bus.SCHEMA)
            jobs = [
                ("job-ord", "feat:edit", 5, None, None),
                ("job-other", "feat:docs", 4, None, None),
            ]
            await self._seed_worker_and_jobs(
                repo,
                urn="urn:agent:zeroclaw:typhon:s1",
                kind="zeroclaw",
                host="typhon",
                capabilities=[],
                current_load={"utilization": 0.0, "deficit": 0.0},
                jobs=jobs,
            )
            with (
                mock.patch.object(agent_bus, "connect_db", repo.connection),
                mock.patch.object(agent_bus, "emit_event", mock.AsyncMock()),
            ):
                claimed = await agent_bus.dequeue_next_job(
                    "urn:agent:zeroclaw:typhon:s1",
                )
            self.assertEqual(claimed["id"], "job-ord")

    async def test_dequeue_build_job_picked_by_build_worker(self):
        """A build: kind job with a hard:linux-compile capability is claimable
        by a worker that advertises the matching capability + role:build."""
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "build.db"))
            await repo.init(agent_bus.SCHEMA)
            await self._seed_worker_and_jobs(
                repo,
                urn="urn:agent:zeroclaw:hydra:s1",
                kind="zeroclaw",
                host="hydra",
                capabilities=["role:build", "linux-compile"],
                current_load={"utilization": 0.0, "deficit": 0.0},
                jobs=[
                    ("job-build", "build:compile", 5, json.dumps(["hard:linux-compile"]), None),
                ],
            )
            with (
                mock.patch.object(agent_bus, "connect_db", repo.connection),
                mock.patch.object(agent_bus, "emit_event", mock.AsyncMock()),
            ):
                claimed = await agent_bus.dequeue_next_job(
                    "urn:agent:zeroclaw:hydra:s1",
                )
            self.assertEqual(claimed["id"], "job-build")

    async def test_dequeue_minimax_global_cap_blocks_sixth_claim(self):
        """Six MiniMax-M3 route workers can not all claim; the 6th gets 204."""
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "minimax.db"))
            await repo.init(agent_bus.SCHEMA)
            now = time.time()
            # Five MiniMax-M3 lease holders (active, not expired).
            active_holders = [f"urn:agent:zeroclaw:w{i}:s1" for i in range(5)]
            async with repo.connection() as db:
                for i, holder in enumerate(active_holders):
                    await db.execute(
                        "INSERT INTO agents (urn, kind, host, session_id, capabilities, "
                        "started_at, last_heartbeat, status, runtime, cost_tier, model, provider) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            holder,
                            "zeroclaw",
                            f"w{i}",
                            "session",
                            "[]",
                            now,
                            now,
                            "online",
                            "zeroclaw",
                            "B",
                            "minimax-m3",
                            "minimax",
                        ),
                    )
                # Their active MiniMax jobs.
                for i, holder in enumerate(active_holders):
                    await db.execute(
                        "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                        "claimed_by, claimed_provider, claimed_model, claim_lease_expires_at, "
                        "started_at) "
                        "VALUES (?, 'urn:submitter', 'feat:edit', 5, 'running', "
                        "?, 'minimax', 'minimax-m3', ?, ?)",
                        (f"job-mx-{i}", holder, now + 3600, now),
                    )
                # The 6th worker + 6 MiniMax job.
                await db.execute(
                    "INSERT INTO agents (urn, kind, host, session_id, capabilities, "
                    "started_at, last_heartbeat, status, runtime, cost_tier, model, provider) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "urn:agent:zeroclaw:w6:s1",
                        "zeroclaw",
                        "w6",
                        "session",
                        "[]",
                        now,
                        now,
                        "online",
                        "zeroclaw",
                        "B",
                        "minimax-m3",
                        "minimax",
                    ),
                )
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                    "started_at) VALUES (?, 'urn:submitter', 'feat:edit', 5, 'queued', ?)",
                    ("job-mx-6", now),
                )
                await db.commit()

            from fastapi.responses import Response

            with (
                mock.patch.object(agent_bus, "connect_db", repo.connection),
                mock.patch.object(agent_bus, "emit_event", mock.AsyncMock()),
            ):
                response = await agent_bus.dequeue_next_job(
                    "urn:agent:zeroclaw:w6:s1",
                )
            # 6th MiniMax worker hits the global cap; we expect a 204 No
            # Content response carrying the minimax_pool_saturated detail
            # header (NOT silently no_jobs_available) so the worker falls
            # through to a non-MiniMax route chain.
            self.assertIsInstance(response, Response)
            self.assertEqual(response.status_code, 204)
            self.assertEqual(
                response.headers.get("X-Hive-Claim-Result"),
                "minimax_pool_saturated",
            )
            detail = response.headers.get("X-Hive-Claim-Detail", "").lower()
            self.assertIn("minimax", detail)
            self.assertIn("global cap", detail)

    async def test_dequeue_minimax_lease_holder_can_still_claim_non_minimax_job(self):
        """A worker holding an active MiniMax lease is still eligible for any
        non-MiniMax job — the lease is exclusive to the MiniMax model."""
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "minimax-mix.db"))
            await repo.init(agent_bus.SCHEMA)
            now = time.time()
            urn = "urn:agent:zeroclaw:typhon:s-mix"
            async with repo.connection() as db:
                # Worker with neutral model/provider — eligible for the
                # generic local job, ineligible for MiniMax.
                await db.execute(
                    "INSERT INTO agents (urn, kind, host, session_id, capabilities, "
                    "started_at, last_heartbeat, status, runtime, cost_tier, model, provider) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        urn,
                        "zeroclaw",
                        "typhon",
                        "session",
                        "[]",
                        now,
                        now,
                        "online",
                        "zeroclaw",
                        "B",
                        "grok-4.3",
                        "xai",
                    ),
                )
                # Existing active MiniMax lease held by this same worker
                # (would normally mean "you're on MiniMax" — but here the
                # worker's model/provider is NOT MiniMax so this is the
                # historical "lease still running but worker is now on a
                # different route" case; the lease must not prevent claiming a
                # non-MiniMax job).
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                    "claimed_by, claimed_provider, claimed_model, claim_lease_expires_at, "
                    "started_at) "
                    "VALUES (?, 'urn:submitter', 'feat:edit', 5, 'running', "
                    "?, 'minimax', 'minimax-m3', ?, ?)",
                    ("job-mx-history", urn, now + 3600, now),
                )
                # Generic queued job.
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                    "started_at) VALUES (?, 'urn:submitter', 'feat:edit', 5, 'queued', ?)",
                    ("job-local", now),
                )
                await db.commit()
            with (
                mock.patch.object(agent_bus, "connect_db", repo.connection),
                mock.patch.object(agent_bus, "emit_event", mock.AsyncMock()),
            ):
                claimed = await agent_bus.dequeue_next_job(urn)
            self.assertEqual(claimed["id"], "job-local")

    async def test_dequeue_role_penalty_deprioritises_build_job_for_general_worker(self):
        """When a build worker is online and idle AND a queued build: job
        exists, the general worker still picks it up if it polls first
        (role-aware scoring is a *preference*, not a hard block — see the
        task directive's "should prefer suitable build capacity"). The
        verification path is the scoring key: with only one candidate, the
        general worker gets it, but its role_penalty is high enough to
        LOSE to a build worker on the same candidate set."""
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "role-race.db"))
            await repo.init(agent_bus.SCHEMA)
            now = time.time()
            # Build worker (idle) and general worker (busy on a different
            # workload). Both have jobs queued. The build worker should claim
            # the build: job; the build-shaped paths it joins leave the
            # non-build jobs for the general worker.
            async with repo.connection() as db:
                # Build worker idle.
                await db.execute(
                    "INSERT INTO agents (urn, kind, host, session_id, capabilities, "
                    "current_load, started_at, last_heartbeat, status, runtime, "
                    "cost_tier, model, provider) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "urn:agent:zeroclaw:hydra:s-build",
                        "zeroclaw",
                        "hydra",
                        "session",
                        '["role:build", "linux-compile"]',
                        json.dumps({"utilization": 0.0, "deficit": 0.0}),
                        now,
                        now,
                        "online",
                        "zeroclaw",
                        "B",
                        "default",
                        "default",
                    ),
                )
                # General worker busy.
                await db.execute(
                    "INSERT INTO agents (urn, kind, host, session_id, capabilities, "
                    "current_load, started_at, last_heartbeat, status, runtime, "
                    "cost_tier, model, provider) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "urn:agent:zeroclaw:typhon:s-gen",
                        "zeroclaw",
                        "typhon",
                        "session",
                        "[]",
                        json.dumps({"utilization": 0.0, "deficit": 0.0}),
                        now,
                        now,
                        "online",
                        "zeroclaw",
                        "B",
                        "default",
                        "default",
                    ),
                )
                # Build job + ordinary job.
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                    "started_at) "
                    "VALUES (?, 'urn:submitter', 'build:compile', 5, 'queued', ?)",
                    ("job-build-only", now),
                )
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                    "started_at) "
                    "VALUES (?, 'urn:submitter', 'feat:edit', 5, 'queued', ?)",
                    ("job-ord-only", now),
                )
                await db.commit()
            # General worker polls first — it gets the non-build job because
            # role-aware scoring de-prioritises build: jobs for non-builders.
            with (
                mock.patch.object(agent_bus, "connect_db", repo.connection),
                mock.patch.object(agent_bus, "emit_event", mock.AsyncMock()),
            ):
                general_claim = await agent_bus.dequeue_next_job(
                    "urn:agent:zeroclaw:typhon:s-gen",
                )
            self.assertEqual(general_claim["id"], "job-ord-only")
            # Build job remains unclaimed (will be picked up by the build
            # worker when it polls).
            async with (
                repo.connection() as db,
                db.execute(
                    "SELECT status, claimed_by FROM jobs WHERE id=?",
                    ("job-build-only",),
                ) as cur,
            ):
                row = await cur.fetchone()
            self.assertEqual(row, ("queued", None))

    async def test_dequeue_starved_worker_wins_via_deficit(self):
        """A worker whose last_served_at is much older than the fresh worker's
        gets the higher-deficit (older idle-gap) boost — verified at the
        dequeue level via two equal-priority jobs with the starved worker
        winning the lexicographic tie-break that comes from the scoring key."""
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "starve.db"))
            await repo.init(agent_bus.SCHEMA)
            now = time.time()
            urn_starved = "urn:agent:zeroclaw:forge:s-starved"
            async with repo.connection() as db:
                # Starved worker: last_served far in the past, deficit high.
                await db.execute(
                    "INSERT INTO agents (urn, kind, host, session_id, capabilities, "
                    "current_load, started_at, last_heartbeat, status, runtime, cost_tier, model, provider) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        urn_starved,
                        "zeroclaw",
                        "forge",
                        "session",
                        "[]",
                        json.dumps(
                            {
                                "utilization": 0.0,
                                "last_served_at": now - 7200,  # 2h ago
                                "deficit": 12.0,
                            }
                        ),
                        now,
                        now,
                        "online",
                        "zeroclaw",
                        "B",
                        "default",
                        "default",
                    ),
                )
                # Two equal-priority jobs. The starved worker's pick is
                # lexicographic — same as a fresh worker — but the score
                # delta on the deficit field is positive, so this worker
                # beats any equally-eligible fresh worker on the same
                # candidate. We just verify it CAN claim one of them.
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                    "started_at) VALUES (?, 'urn:submitter', 'feat:edit', 5, 'queued', ?)",
                    ("job-aa", now),
                )
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                    "started_at) VALUES (?, 'urn:submitter', 'feat:edit', 5, 'queued', ?)",
                    ("job-bb", now),
                )
                await db.commit()
            with (
                mock.patch.object(agent_bus, "connect_db", repo.connection),
                mock.patch.object(agent_bus, "emit_event", mock.AsyncMock()),
            ):
                claimed = await agent_bus.dequeue_next_job(urn_starved)
            self.assertIn(claimed["id"], ("job-aa", "job-bb"))

    async def test_dequeue_local_eih_routes_share_pool_with_general_workers(self):
        """Local (local-llamacpp) and EIH (ngc-proxy) jobs are general-pool
        work — they do NOT consume a MiniMax-M3 lease and a general worker
        (grok-4.3 / xai) is free to claim them even when 5 MiniMax leases
        are active elsewhere in the fleet. Only MiniMax-route workers
        themselves are gated by the pool; the MiniMax cap is a route-class
        cap, not a worker cap.
        """
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "local-eih.db"))
            await repo.init(agent_bus.SCHEMA)
            now = time.time()
            urn = "urn:agent:zeroclaw:typhon:s-local"
            async with repo.connection() as db:
                # Five MiniMax-M3 lease holders, pool saturated.
                for i in range(5):
                    holder = f"urn:agent:zeroclaw:w{i}:s1"
                    await db.execute(
                        "INSERT INTO agents (urn, kind, host, session_id, capabilities, "
                        "started_at, last_heartbeat, status, runtime, cost_tier, model, provider) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            holder,
                            "zeroclaw",
                            f"w{i}",
                            "session",
                            "[]",
                            now,
                            now,
                            "online",
                            "zeroclaw",
                            "B",
                            "minimax-m3",
                            "minimax",
                        ),
                    )
                    await db.execute(
                        "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                        "claimed_by, claimed_provider, claimed_model, claim_lease_expires_at, "
                        "started_at) "
                        "VALUES (?, 'urn:submitter', 'feat:edit', 5, 'running', "
                        "?, 'minimax', 'minimax-m3', ?, ?)",
                        (f"job-mx-{i}", holder, now + 3600, now),
                    )
                # General (grok-4.3) worker — would be on a local/EIH route.
                await db.execute(
                    "INSERT INTO agents (urn, kind, host, session_id, capabilities, "
                    "started_at, last_heartbeat, status, runtime, cost_tier, model, provider) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        urn,
                        "zeroclaw",
                        "typhon",
                        "session",
                        "[]",
                        now,
                        now,
                        "online",
                        "zeroclaw",
                        "B",
                        "grok-4.3",
                        "xai",
                    ),
                )
                # Local job + EIH job queued (both general pool, not MiniMax).
                for jid in ("job-local-1", "job-eih-1"):
                    await db.execute(
                        "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                        "started_at) "
                        "VALUES (?, 'urn:submitter', 'feat:edit', 5, 'queued', ?)",
                        (jid, now),
                    )
                await db.commit()
            with (
                mock.patch.object(agent_bus, "connect_db", repo.connection),
                mock.patch.object(agent_bus, "emit_event", mock.AsyncMock()),
            ):
                claimed = await agent_bus.dequeue_next_job(urn)
            # Pool saturated (5/5 MiniMax leases) but a general worker on
            # the grok-4.3/xai route is unaffected — it claims the highest-
            # priority queued job from the local/EIH pool.
            self.assertIn(claimed["id"], ("job-local-1", "job-eih-1"))

    async def test_dequeue_minimax_pool_observability_event_is_emitted(self):
        """When the MiniMax pool denies a claim, ``minimax.pool.denied`` is
        emitted on the bus with the reason + pool counts so fleet operators
        can grep the events table (NOT stderr) for saturation signals."""
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "mx-deny.db"))
            await repo.init(agent_bus.SCHEMA)
            now = time.time()
            urn = "urn:agent:zeroclaw:typhon:s-deny"
            async with repo.connection() as db:
                # Five MiniMax-M3 lease holders (saturated pool).
                for i in range(5):
                    holder = f"urn:agent:zeroclaw:w{i}:s1"
                    await db.execute(
                        "INSERT INTO agents (urn, kind, host, session_id, capabilities, "
                        "started_at, last_heartbeat, status, runtime, cost_tier, model, provider) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            holder,
                            "zeroclaw",
                            f"w{i}",
                            "session",
                            "[]",
                            now,
                            now,
                            "online",
                            "zeroclaw",
                            "B",
                            "minimax-m3",
                            "minimax",
                        ),
                    )
                    await db.execute(
                        "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                        "claimed_by, claimed_provider, claimed_model, claim_lease_expires_at, "
                        "started_at) "
                        "VALUES (?, 'urn:submitter', 'feat:edit', 5, 'running', "
                        "?, 'minimax', 'minimax-m3', ?, ?)",
                        (f"job-mx-{i}", holder, now + 3600, now),
                    )
                # 6th MiniMax-M3 worker (will be denied).
                await db.execute(
                    "INSERT INTO agents (urn, kind, host, session_id, capabilities, "
                    "started_at, last_heartbeat, status, runtime, cost_tier, model, provider) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        urn,
                        "zeroclaw",
                        "typhon",
                        "session",
                        "[]",
                        now,
                        now,
                        "online",
                        "zeroclaw",
                        "B",
                        "minimax-m3",
                        "minimax",
                    ),
                )
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                    "started_at) "
                    "VALUES (?, 'urn:submitter', 'feat:edit', 5, 'queued', ?)",
                    ("job-mx-6", now),
                )
                await db.commit()
            # Capture emitted events.
            from fastapi.responses import Response

            captured_events = []

            async def fake_emit(db, kind, payload):
                captured_events.append((kind, payload))

            with (
                mock.patch.object(agent_bus, "connect_db", repo.connection),
                mock.patch.object(agent_bus, "emit_event", fake_emit),
            ):
                response = await agent_bus.dequeue_next_job(urn)
            self.assertIsInstance(response, Response)
            self.assertEqual(response.status_code, 204)
            denied = [e for e in captured_events if e[0] == "minimax.pool.denied"]
            self.assertEqual(
                len(denied),
                1,
                f"exactly one minimax.pool.denied event must be emitted; got {captured_events!r}",
            )
            _kind, payload = denied[0]
            self.assertEqual(payload["urn"], urn)
            self.assertEqual(payload["pool_active"], sched.MINIMAX_GLOBAL_MAX)
            self.assertEqual(payload["pool_cap"], sched.MINIMAX_GLOBAL_MAX)
            self.assertIn("global cap", payload["reason"])

    async def test_dequeue_atomic_two_pollers_only_one_wins(self):
        """Two workers race for the same single queued job; the dequeue flow
        uses a CAS UPDATE WHERE status IN ('queued','offered') so exactly
        one of the two polling workers claims the job. The other gets a
        204 (no_jobs_available) response — no double-claim, no partial
        commit. This is the atomic-fair-claim guarantee: the SQL UPDATE
        under BEGIN IMMEDIATE is the single arbiter."""
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "atomic.db"))
            await repo.init(agent_bus.SCHEMA)
            now = time.time()
            urn_a = "urn:agent:zeroclaw:typhon:s-a"
            urn_b = "urn:agent:zeroclaw:typhon:s-b"
            async with repo.connection() as db:
                for urn in (urn_a, urn_b):
                    await db.execute(
                        "INSERT INTO agents (urn, kind, host, session_id, capabilities, "
                        "current_load, started_at, last_heartbeat, status, runtime, "
                        "cost_tier, model, provider) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            urn,
                            "zeroclaw",
                            "typhon",
                            "session",
                            "[]",
                            json.dumps({"utilization": 0.0, "deficit": 0.0}),
                            now,
                            now,
                            "online",
                            "zeroclaw",
                            "B",
                            "default",
                            "default",
                        ),
                    )
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                    "started_at) VALUES (?, 'urn:submitter', 'feat:edit', 5, 'queued', ?)",
                    ("job-only-one", now),
                )
                await db.commit()
            from fastapi.responses import Response

            results: list = []
            with (
                mock.patch.object(agent_bus, "connect_db", repo.connection),
                mock.patch.object(agent_bus, "emit_event", mock.AsyncMock()),
            ):
                # Sequential polls — under BEGIN IMMEDIATE the second claim
                # observes status='claimed' and returns 204.
                a = await agent_bus.dequeue_next_job(urn_a)
                b = await agent_bus.dequeue_next_job(urn_b)
                results = [a, b]
            # Exactly one worker got the job; the other got a 204 Response.
            claimed = [r for r in results if not isinstance(r, Response)]
            no_claim = [r for r in results if isinstance(r, Response)]
            self.assertEqual(len(claimed), 1, "exactly one worker claims the job")
            self.assertEqual(claimed[0]["id"], "job-only-one")
            self.assertEqual(len(no_claim), 1, "the second poller sees 204 (no_jobs_available)")
            self.assertEqual(no_claim[0].status_code, 204)
            # And the claimed_by column in the DB is exactly one of the two urns.
            async with (
                repo.connection() as db,
                db.execute(
                    "SELECT claimed_by FROM jobs WHERE id=?",
                    ("job-only-one",),
                ) as cur,
            ):
                row = await cur.fetchone()
            self.assertIn(row[0], (urn_a, urn_b), f"claimed_by must be one of the two pollers; got {row[0]!r}")

    async def test_dequeue_minimax_offered_state_counts_against_global_cap(self):
        """Per the rollout directive (2026-10-01): "a hard global maximum
        of 5 concurrent OR ASSIGNED MiniMax sessions". An ``offered`` job
        has been routed to a worker (``claimed_by`` is set) but the worker
        hasn't yet actively claimed it — it's ASSIGNED, so it MUST count
        against the global pool cap. Five offered MiniMax jobs (none yet
        active) + a 6th polling MiniMax-M3 worker → the 6th is denied
        with ``X-Hive-Claim-Result: minimax_pool_saturated``."""
        from fastapi.responses import Response as _Response

        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "minimax-offered.db"))
            await repo.init(agent_bus.SCHEMA)
            now = time.time()
            active_holders = [f"urn:agent:zeroclaw:w{i}:s1" for i in range(5)]
            async with repo.connection() as db:
                for i, holder in enumerate(active_holders):
                    await db.execute(
                        "INSERT INTO agents (urn, kind, host, session_id, capabilities, "
                        "started_at, last_heartbeat, status, runtime, cost_tier, model, provider) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            holder,
                            "zeroclaw",
                            f"w{i}",
                            "session",
                            "[]",
                            now,
                            now,
                            "online",
                            "zeroclaw",
                            "B",
                            "minimax-m3",
                            "minimax",
                        ),
                    )
                # Five OFFERED (not claimed, not running) MiniMax jobs —
                # assigned to workers but not yet actively claimed. These
                # count as ASSIGNED sessions per the directive.
                for i, holder in enumerate(active_holders):
                    await db.execute(
                        "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                        "claimed_by, claimed_provider, claimed_model, claim_lease_expires_at, "
                        "started_at) "
                        "VALUES (?, 'urn:submitter', 'feat:edit', 5, 'offered', "
                        "?, 'minimax', 'minimax-m3', ?, ?)",
                        (f"job-mx-offered-{i}", holder, now + 3600, now),
                    )
                # The 6th worker + 1 queued MiniMax job.
                urn_6 = "urn:agent:zeroclaw:w6:s1"
                await db.execute(
                    "INSERT INTO agents (urn, kind, host, session_id, capabilities, "
                    "started_at, last_heartbeat, status, runtime, cost_tier, model, provider) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        urn_6,
                        "zeroclaw",
                        "w6",
                        "session",
                        "[]",
                        now,
                        now,
                        "online",
                        "zeroclaw",
                        "B",
                        "minimax-m3",
                        "minimax",
                    ),
                )
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                    "started_at) VALUES (?, 'urn:submitter', 'feat:edit', 5, 'queued', ?)",
                    ("job-mx-6-queued", now),
                )
                await db.commit()

            with (
                mock.patch.object(agent_bus, "connect_db", repo.connection),
                mock.patch.object(agent_bus, "emit_event", mock.AsyncMock()),
            ):
                response = await agent_bus.dequeue_next_job(urn_6)
            self.assertIsInstance(response, _Response)
            self.assertEqual(response.status_code, 204)
            self.assertEqual(
                response.headers.get("X-Hive-Claim-Result"),
                "minimax_pool_saturated",
                "5 offered MiniMax jobs must count as ASSIGNED and saturate the global pool",
            )
            detail = response.headers.get("X-Hive-Claim-Detail", "").lower()
            self.assertIn("global cap", detail, f"denial detail must reference the global cap; got {detail!r}")

    async def test_dequeue_minimax_offered_state_counts_against_per_worker_cap(self):
        """A worker that already holds an OFFERED (not yet actively
        claimed) MiniMax job must NOT be allowed to take another MiniMax
        job — the per-worker cap of 1 covers concurrent AND assigned
        leases. The denial reason is the per-worker cap, not the global
        cap (the global pool has plenty of headroom here)."""
        from fastapi.responses import Response as _Response

        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "minimax-offered-perw.db"))
            await repo.init(agent_bus.SCHEMA)
            now = time.time()
            urn = "urn:agent:zeroclaw:typhon:s-offered-perw"
            async with repo.connection() as db:
                # Worker holding one OFFERED MiniMax lease.
                await db.execute(
                    "INSERT INTO agents (urn, kind, host, session_id, capabilities, "
                    "started_at, last_heartbeat, status, runtime, cost_tier, model, provider) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        urn,
                        "zeroclaw",
                        "typhon",
                        "session",
                        "[]",
                        now,
                        now,
                        "online",
                        "zeroclaw",
                        "B",
                        "minimax-m3",
                        "minimax",
                    ),
                )
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                    "claimed_by, claimed_provider, claimed_model, claim_lease_expires_at, "
                    "started_at) "
                    "VALUES (?, 'urn:submitter', 'feat:edit', 5, 'offered', "
                    "?, 'minimax', 'minimax-m3', ?, ?)",
                    ("job-mx-offered-already", urn, now + 3600, now),
                )
                # A second queued MiniMax job the same worker tries to claim.
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                    "started_at) VALUES (?, 'urn:submitter', 'feat:edit', 5, 'queued', ?)",
                    ("job-mx-second-attempt", now),
                )
                await db.commit()
            with (
                mock.patch.object(agent_bus, "connect_db", repo.connection),
                mock.patch.object(agent_bus, "emit_event", mock.AsyncMock()),
            ):
                response = await agent_bus.dequeue_next_job(urn)
            self.assertIsInstance(response, _Response)
            self.assertEqual(response.status_code, 204)
            # Per-worker cap is the reason (not global cap — only 1
            # assignment exists fleet-wide).
            self.assertEqual(
                response.headers.get("X-Hive-Claim-Result"),
                "minimax_pool_saturated",
                "an offered MiniMax lease must count against the per-worker cap",
            )
            detail = response.headers.get("X-Hive-Claim-Detail", "").lower()
            self.assertIn("per-worker cap", detail, f"denial detail must reference the per-worker cap; got {detail!r}")

    async def test_dequeue_minimax_stale_offered_lease_does_not_count(self):
        """A MiniMax lease whose ``claim_lease_expires_at`` is in the past
        does NOT count against the pool — even if the job is still in
        ``offered`` status. The pool math filters by lease expiry so a
        crashed worker doesn't permanently reduce capacity. A new MiniMax
        worker can claim even when an expired offered lease exists."""
        from fastapi.responses import Response as _Response

        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "minimax-stale-offered.db"))
            await repo.init(agent_bus.SCHEMA)
            now = time.time()
            urn = "urn:agent:zeroclaw:typhon:s-stale-offered"
            async with repo.connection() as db:
                await db.execute(
                    "INSERT INTO agents (urn, kind, host, session_id, capabilities, "
                    "started_at, last_heartbeat, status, runtime, cost_tier, model, provider) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        urn,
                        "zeroclaw",
                        "typhon",
                        "session",
                        "[]",
                        now,
                        now,
                        "online",
                        "zeroclaw",
                        "B",
                        "minimax-m3",
                        "minimax",
                    ),
                )
                # Stale offered MiniMax job — lease expired 1 hour ago.
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                    "claimed_by, claimed_provider, claimed_model, claim_lease_expires_at, "
                    "started_at) "
                    "VALUES (?, 'urn:submitter', 'feat:edit', 5, 'offered', "
                    "?, 'minimax', 'minimax-m3', ?, ?)",
                    ("job-mx-stale-offered", "urn:stale:worker:s1", now - 3600, now),
                )
                # Fresh queued MiniMax job.
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, priority, status, "
                    "started_at) VALUES (?, 'urn:submitter', 'feat:edit', 5, 'queued', ?)",
                    ("job-mx-fresh", now),
                )
                await db.commit()
            with (
                mock.patch.object(agent_bus, "connect_db", repo.connection),
                mock.patch.object(agent_bus, "emit_event", mock.AsyncMock()),
            ):
                claimed = await agent_bus.dequeue_next_job(urn)
            # Stale offered lease does NOT count — worker claims the fresh
            # MiniMax job. The claim must succeed (return a dict, not a
            # 204 Response with minimax_pool_saturated).
            self.assertNotIsInstance(claimed, _Response)
            self.assertEqual(claimed["id"], "job-mx-fresh")


class ServiceTests(unittest.TestCase):
    def test_auto_restore_requests_result_free_server_sorted_projection(self):
        seen = {}
        server_order = [{"id": "new"}, {"id": "old"}]

        def fake_http(method, path, body=None, timeout=0):
            seen.update(method=method, path=path)
            return 200, {"jobs": server_order}

        with mock.patch.object(auto_restore, "_http", fake_http):
            self.assertEqual(auto_restore.fetch_failed_jobs(limit=25), server_order)
        self.assertIn("include_result=false", seen["path"])
        self.assertIn("limit=25", seen["path"])

    def test_auto_restore_calls_admin_requeue(self):
        seen = {}

        def fake_http(method, path, body=None, timeout=0):
            seen.update(method=method, path=path, body=body)
            return 200, {"ok": ["job-1"], "not_found": [], "skipped_done": []}

        with mock.patch.object(auto_restore, "_http", fake_http):
            self.assertEqual(auto_restore.restore_job("job-1"), (True, ""))
        self.assertEqual(
            seen,
            {
                "method": "POST",
                "path": "/v1/admin/jobs/requeue",
                "body": {"job_ids": ["job-1"], "reason": "auto-restore"},
            },
        )

    @unittest.skip(
        "auto-restore creds-gated fail-fast targets the forge's bus-auth design; "
        "the fleet is local-LAN only and does not require auth (operator 2026-10-15)."
    )
    def test_auto_restore_missing_credentials_is_fatal_before_poll(self):
        with (
            mock.patch.object(auto_restore, "HIVE_BUS_TOKEN", ""),
            mock.patch.object(auto_restore, "HIVE_BUS_ADMIN_TOKEN", ""),
            mock.patch.object(auto_restore, "fetch_failed_jobs") as fetch,
            mock.patch.object(auto_restore, "log"),
        ):
            self.assertEqual(auto_restore.main(), 2)
        fetch.assert_not_called()

    @unittest.skip(
        "auto-restore creds-gated fail-fast targets the forge's bus-auth design; "
        "the fleet is local-LAN only and does not require auth (operator 2026-10-15)."
    )
    def test_auto_restore_queue_read_failure_is_fatal(self):
        with (
            mock.patch.object(auto_restore, "HIVE_BUS_TOKEN", "worker"),
            mock.patch.object(auto_restore, "HIVE_BUS_ADMIN_TOKEN", "admin"),
            mock.patch.object(auto_restore, "load_state", return_value={}),
            mock.patch.object(auto_restore, "fetch_failed_jobs", side_effect=RuntimeError("HTTP 503")),
            mock.patch.object(auto_restore, "log"),
        ):
            self.assertEqual(auto_restore.main(), 1)

    def test_auto_restore_unit_loads_required_root_managed_environment(self):
        unit = (HERE / "hive-auto-restore.service").read_text()
        self.assertIn("EnvironmentFile=/etc/graeae-hive/auto-restore.env", unit)
        self.assertNotIn("EnvironmentFile=-", unit)
        self.assertIn("UMask=0077", unit)

    def test_oauth_cap_parse_failure_fails_closed(self):
        with tempfile.TemporaryDirectory() as td:
            cap = Path(td) / "cap.json"
            old = agent_bus.KNEMON_CAP_FILE
            agent_bus.KNEMON_CAP_FILE = str(cap)
            try:
                self.assertFalse(agent_bus.oauth_cap_state()["capped"])
                cap.write_text("not-json")
                state = agent_bus.oauth_cap_state()
                self.assertTrue(state["capped"])
                self.assertIn("storage_error", state)
            finally:
                agent_bus.KNEMON_CAP_FILE = old

    def test_oracle_migration_targets_physical_table(self):
        sql = (HERE / "migrations" / "2026-06-03-jobs-dedup.sql").read_text()
        oracle = sql.split("-- ── SQLite", 1)[0]
        self.assertIn("ALTER TABLE HIVE_JOBS", oracle)
        self.assertIn("ON HIVE_JOBS", oracle)

    def test_doctor_selects_admin_credential_for_registration(self):
        doctor = load_script("zeroclaw_doctor_auth_under_test", str(HERE / "zeroclaw_doctor.py"))
        seen = {}

        class Reply(io.BytesIO):
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        def urlopen(request, timeout=0):
            seen["authorization"] = request.get_header("Authorization")
            return Reply(b'{"urn":"urn:agent:doctor:test:1"}')

        with mock.patch.object(doctor.urllib.request, "urlopen", urlopen):
            code, _ = doctor._http(
                "POST",
                "/v1/agents/register",
                {"kind": "doctor", "runtime": "doctor"},
            )
        self.assertEqual(code, 200)
        self.assertEqual(seen["authorization"], "Bearer test-admin-token")

    def test_bundled_audit_script_sends_bus_bearer(self):
        seen = {}

        class Reply(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

        def urlopen(request, timeout=0):
            seen["authorization"] = request.get_header("Authorization")
            return Reply(b'{"jobs":[]}')

        with (
            mock.patch.dict(os.environ, {"HIVE_BUS_TOKEN": "audit-token"}),
            mock.patch("urllib.request.urlopen", urlopen),
            mock.patch("builtins.open", mock.mock_open()),
        ):
            runpy.run_path("bs_audit.py", run_name="bs_audit_under_test")
        self.assertEqual(seen["authorization"], "Bearer audit-token")

    def test_worker_authorized_valid_oauth_probe_closes_breaker(self):
        async def run():
            body = json.dumps(
                {
                    "success": True,
                    "model": "hive_codex",
                    "reporter": "worker-1",
                }
            ).encode()
            sent = False

            async def receive():
                nonlocal sent
                if sent:
                    return {"type": "http.disconnect"}
                sent = True
                return {"type": "http.request", "body": body, "more_body": False}

            request = Request(
                {
                    "type": "http",
                    "method": "POST",
                    "path": "/v1/knemon/cap",
                    "raw_path": b"/v1/knemon/cap",
                    "query_string": b"",
                    "headers": [],
                    "scheme": "http",
                    "server": ("test", 80),
                },
                receive,
            )
            request.state.auth_role = "worker"
            with (
                mock.patch.object(agent_bus, "clear_oauth_cap", return_value=None),
                mock.patch.object(agent_bus, "oauth_cap_state", return_value={"capped": False}),
            ):
                return await agent_bus.knemon_cap_set(request)

        result = asyncio.run(run())
        self.assertTrue(result["cleared"])


class WorkerTests(unittest.TestCase):
    def test_completion_outbox_flush_is_bounded(self):
        entries = {
            f"job-{index}": {
                "jid": f"job-{index}",
                "urn": "urn-1",
                "status": "done",
                "result": {},
            }
            for index in range(5)
        }
        flushed = []
        with (
            mock.patch.object(worker, "COMPLETION_OUTBOX_BATCH_SIZE", 2),
            mock.patch.object(worker, "_COMPLETION_OUTBOX_CURSOR", 0),
            mock.patch.object(worker, "_read_completion_outbox", return_value=entries),
            mock.patch.object(
                worker,
                "persist_terminal_job",
                side_effect=lambda jid, *_args, **_kwargs: flushed.append(jid),
            ),
        ):
            worker.flush_completion_outbox()
            worker.flush_completion_outbox()
        self.assertEqual(flushed, ["job-0", "job-1", "job-2", "job-3"])

    def test_failed_terminal_write_is_retained_and_later_flushed(self):
        with tempfile.TemporaryDirectory() as td:
            outbox = Path(td) / "outbox.json"
            with (
                mock.patch.object(worker, "COMPLETION_OUTBOX", outbox),
                mock.patch.object(worker, "patch_job", return_value=False),
                mock.patch.object(worker, "_completion_is_acknowledged", return_value=False),
                mock.patch.object(worker.time, "sleep", return_value=None),
            ):
                self.assertFalse(worker.persist_terminal_job("job-1", "urn-1", "done", {"ok": True}))
            self.assertIn("job-1", json.loads(outbox.read_text()))
            with (
                mock.patch.object(worker, "COMPLETION_OUTBOX", outbox),
                mock.patch.object(worker, "patch_job", return_value=True),
            ):
                worker.flush_completion_outbox()
            self.assertNotIn("job-1", json.loads(outbox.read_text()))

    def test_lease_thread_renews_until_stopped(self):
        calls = []
        with (
            mock.patch.object(worker, "LEASE_RENEW_INTERVAL", 0.01),
            mock.patch.object(worker, "patch_job", side_effect=lambda *args: calls.append(args) or True),
        ):
            worker._start_lease_renewal("job-lease", "urn-1")
            time.sleep(0.04)
            worker._stop_lease_renewal("job-lease")
        self.assertTrue(any(args[2] == "running" for args in calls))

    def test_no_workspace_release_is_one_atomic_valid_patch(self):
        calls = []

        def fake_http(method, path, body=None, timeout=0):
            calls.append((method, path, body))
            return 200, {"ok": True}

        job = {"id": "job-route", "kind": "unmapped-kind", "eligible_kinds": ["zeroclaw", "zoder"]}
        with (
            mock.patch.object(worker, "patch_job", return_value=True),
            mock.patch.object(worker, "_start_lease_renewal"),
            mock.patch.object(worker, "_stop_lease_renewal"),
            mock.patch.object(worker, "_is_code_kind", return_value=False),
            mock.patch.object(worker, "_resolve_workspace", return_value=(None, "no_workspace_for_kind")),
            mock.patch.object(worker, "route_chain", return_value=[]),
            mock.patch.object(worker, "http", side_effect=fake_http),
        ):
            worker.process_job("urn-worker", job)
        routing = [call for call in calls if call[1] == "/v1/jobs/job-route"]
        self.assertEqual(len(routing), 1)
        self.assertEqual(routing[0][2]["status"], "queued")
        self.assertEqual(routing[0][2]["eligible_kinds"], ["claude", "codex"])

    def test_completion_outbox_is_instance_scoped_and_locked(self):
        source = (HERE / "tests" / "fixtures" / "zeroclaw_wss_worker_v2.py").read_text()
        self.assertIn("hive_completion_outbox.{INSTANCE}.json", source)
        self.assertIn("fcntl.LOCK_EX", source)

    def test_failed_completion_acknowledges_exact_server_retry_transition(self):
        with mock.patch.object(
            worker,
            "http",
            return_value=(
                200,
                {
                    "status": "queued",
                    "claimed_by": None,
                    "retry_count": 4,
                },
            ),
        ):
            self.assertTrue(
                worker._completion_is_acknowledged(
                    "job-1",
                    "urn-1",
                    "failed",
                    claimed_retry_count=3,
                )
            )
            self.assertFalse(
                worker._completion_is_acknowledged(
                    "job-1",
                    "urn-1",
                    "failed",
                    claimed_retry_count=2,
                )
            )

    def test_worker_refuses_to_start_without_mnemos_token(self):
        with mock.patch.object(worker, "MNEMOS_TOKEN", ""):
            with self.assertRaises(SystemExit) as raised:
                worker.main()
        self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
