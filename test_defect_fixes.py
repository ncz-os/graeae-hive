"""Regression tests for production defects found in the subsystem review."""

import asyncio
import importlib.util
import json
import os
import tempfile
import time
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest import mock

os.environ["HIVE_BUS_TOKEN"] = "test-bus-token"
os.environ["HIVE_BUS_ADMIN_TOKEN"] = "test-admin-token"
os.environ.setdefault("AGENT_BUS_DB", str(Path(tempfile.gettempdir()) / "hive-defect-tests.db"))

import agent_bus
from hive.persistence.db2 import DB2_DDL, Db2HiveRepository
from hive.persistence.sqlite import SqliteHiveRepository
from starlette.requests import Request
from starlette.responses import Response


def load_script(name, filename):
    spec = importlib.util.spec_from_file_location(name, filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


auto_restore = load_script("hive_auto_restore", "hive-auto-restore.py")
with mock.patch("pathlib.Path.mkdir"):
    worker = load_script("zeroclaw_worker_under_test", "zeroclaw_wss_worker_v2.py")


class McpBridgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_mcp2_import_registers_typed_handlers(self):
        bridge = load_script("mcp_bridge_under_test", "mcp_bridge.py")
        from mcp.types import PaginatedRequestParams
        self.assertEqual(bridge.MCP_VERSION.split(".", 1)[0], "2")
        handlers = getattr(bridge.server, "_request_handlers", {})
        self.assertTrue({"tools/list", "tools/call", "resources/list", "prompts/list"}.issubset(handlers))
        result = await handlers["tools/list"].handler(None, PaginatedRequestParams())
        self.assertIn("mnemos_memory_search", {tool.name for tool in result.tools})
        await bridge.client.aclose()

    async def test_all_mnemos_memory_tools_use_versioned_routes(self):
        bridge = load_script("mcp_bridge_routes_under_test", "mcp_bridge.py")
        seen = []

        async def fake_mnemos(method, path, **kwargs):
            seen.append((method, path, kwargs))
            return {"ok": True}

        with mock.patch.object(bridge, "_mnemos", fake_mnemos):
            await bridge.handle_call_tool("mnemos_memory_search", {"query": "q"})
            await bridge.handle_call_tool("mnemos_memory_create", {"content": "c", "category": "test"})
            await bridge.handle_call_tool("mnemos_memory_get", {"id": "mem_1"})
        self.assertEqual([path for _method, path, _kwargs in seen], [
            "/v1/memories/search", "/v1/memories", "/v1/memories/mem_1",
        ])
        await bridge.client.aclose()


class AuthTests(unittest.IsolatedAsyncioTestCase):
    async def _request(self, path, token=None, method="GET", cookie=None, call_next=None):
        headers = []
        if token is not None:
            headers.append((b"authorization", f"Bearer {token}".encode()))
        if cookie is not None:
            headers.append((b"cookie", cookie.encode()))
        request = Request({"type": "http", "method": method, "path": path,
                           "raw_path": path.encode(), "query_string": b"",
                           "headers": headers, "scheme": "http", "server": ("test", 80)})

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
            "/v1/auth/session", "test-admin-token", "POST", call_next=issue,
        )
        cookie = response.headers["set-cookie"].split(";", 1)[0]
        self.assertIn("HttpOnly", response.headers["set-cookie"])
        self.assertIn("SameSite=strict", response.headers["set-cookie"])
        self.assertEqual((await self._request("/v1/events", cookie=cookie)).status_code, 204)
        self.assertEqual((await self._request("/v1/schedules", method="POST", cookie=cookie)).status_code, 204)
        https_request = Request({"type": "http", "method": "POST", "path": "/v1/auth/session",
                                 "raw_path": b"/v1/auth/session", "query_string": b"", "headers": [],
                                 "scheme": "https", "server": ("test", 443)})
        https_request.state.auth_role = "admin"
        https_response = await agent_bus.create_browser_session(https_request)
        self.assertIn("Secure", https_response.headers["set-cookie"])

    async def test_worker_role_cannot_register_synthetic_orchestrator(self):
        request = Request({"type": "http", "method": "POST", "path": "/v1/agents/register",
                           "raw_path": b"/v1/agents/register", "query_string": b"", "headers": [],
                           "scheme": "http", "server": ("test", 80)})
        request.state.auth_role = "worker"
        with self.assertRaises(agent_bus.HTTPException) as raised:
            await agent_bus.register(
                agent_bus.AgentRegister(host="attacker", runtime="human", kind="human"), request,
            )
        self.assertEqual(raised.exception.status_code, 403)


class PersistenceTests(unittest.IsolatedAsyncioTestCase):
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
            with mock.patch.object(agent_bus, "hive_repo", return_value=fake), \
                    mock.patch.object(agent_bus, "reaper_task", idle_reaper):
                async with agent_bus.lifespan(agent_bus.app):
                    self.assertIsNone(fake.initialized)
            self.assertTrue(fake.closed)
        finally:
            agent_bus.HIVE_BACKEND = old_backend

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
            async def __aenter__(self): return self
            async def __aexit__(self, *_args): return None
            async def fetchone(self): return (0,)

        class Db:
            def execute(self, _sql): return Cursor()

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
                async def done(): return self
                return done().__await__()

            async def __aenter__(self): return self
            async def __aexit__(self, *_args): return None
            async def fetchall(self): return list(self.rows)

        class FakeDb:
            def __init__(self):
                self.columns = {
                    "AGENTS": {"URN", "KIND", "HOST", "SESSION_ID", "PID", "CAPABILITIES",
                               "VERSION", "STARTED_AT", "LAST_HEARTBEAT", "STATUS", "METADATA"},
                    "JOBS": {"ID", "SUBMITTER_URN", "PARENT_JOB_ID", "KIND", "DESCRIPTION",
                             "PRIORITY", "DEADLINE", "REQUIRED_CAPABILITIES", "ELIGIBLE_KINDS",
                             "STATUS", "CLAIMED_BY", "CLAIMED_AT", "STARTED_AT", "ENDED_AT", "RESULT"},
                }
                self.indexes = {"JOBS": set()}
                self.versions = set()
                self.active_jobs = [("job-old", "same-hash"), ("job-new", "same-hash")]
                self.retired = []
                self.sql = []

            def execute(self, sql, params=None):
                self.sql.append((sql, tuple(params or ())))
                upper = sql.upper()
                if "FROM HIVE_SCHEMA_MIGRATIONS" in upper:
                    return Result([(v,) for v in sorted(self.versions)])
                if "SELECT ID, DEDUP_HASH FROM JOBS" in upper:
                    return Result(self.active_jobs)
                if "FROM SYSCAT.COLUMNS" in upper:
                    return Result([(v,) for v in sorted(self.columns[params[0]])])
                if "FROM SYSCAT.INDEXES" in upper:
                    return Result([(v,) for v in sorted(self.indexes.get(params[0], set()))])
                if upper.startswith("ALTER TABLE"):
                    table = upper.split()[2]
                    name = upper.split("ADD COLUMN", 1)[1].strip().split()[0]
                    self.columns[table].add(name)
                elif upper.startswith("CREATE UNIQUE INDEX"):
                    self.indexes["JOBS"].add("JOBS_ACTIVE_DEDUP_UQ")
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
        self.assertIn("DEDUP_HASH", db.columns["JOBS"])
        self.assertIn("DECLINE_COUNT", db.columns["JOBS"])
        self.assertIn("DEDUP_ACTIVE_HASH", db.columns["JOBS"])
        self.assertIn("JOBS_ACTIVE_DEDUP_UQ", db.indexes["JOBS"])
        self.assertEqual(db.retired, ["job-new"])
        catalog_sql = [sql for sql, params in db.sql if params]
        self.assertTrue(all("?" in sql for sql in catalog_sql))

    async def test_fresh_db2_ddl_has_active_only_generated_key(self):
        self.assertIn("dedup_active_hash VARCHAR(64) GENERATED ALWAYS", DB2_DDL)
        self.assertIn("hive_schema_migrations", DB2_DDL)


class ServiceTests(unittest.TestCase):
    def test_auto_restore_calls_admin_requeue(self):
        seen = {}

        def fake_http(method, path, body=None, timeout=0):
            seen.update(method=method, path=path, body=body)
            return 200, {"ok": ["job-1"], "not_found": [], "skipped_done": []}

        with mock.patch.object(auto_restore, "_http", fake_http):
            self.assertEqual(auto_restore.restore_job("job-1"), (True, ""))
        self.assertEqual(seen, {"method": "POST", "path": "/v1/admin/jobs/requeue",
                                "body": {"job_ids": ["job-1"], "reason": "auto-restore"}})

    def test_auto_restore_missing_credentials_is_fatal_before_poll(self):
        with mock.patch.object(auto_restore, "HIVE_BUS_TOKEN", ""), \
                mock.patch.object(auto_restore, "HIVE_BUS_ADMIN_TOKEN", ""), \
                mock.patch.object(auto_restore, "fetch_failed_jobs") as fetch, \
                mock.patch.object(auto_restore, "log"):
            self.assertEqual(auto_restore.main(), 2)
        fetch.assert_not_called()

    def test_auto_restore_unit_loads_required_root_managed_environment(self):
        unit = Path("hive-auto-restore.service").read_text()
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
        sql = Path("migrations/2026-06-03-jobs-dedup.sql").read_text()
        oracle = sql.split("-- ── SQLite", 1)[0]
        self.assertIn("ALTER TABLE HIVE_JOBS", oracle)
        self.assertIn("ON HIVE_JOBS", oracle)

    def test_doctor_selects_admin_credential_for_requeue(self):
        source = Path("zeroclaw_doctor.py").read_text()
        self.assertIn('path.startswith("/v1/admin/")', source)
        self.assertIn("HIVE_BUS_ADMIN_TOKEN", source)


class WorkerTests(unittest.TestCase):
    def test_failed_terminal_write_is_retained_and_later_flushed(self):
        with tempfile.TemporaryDirectory() as td:
            outbox = Path(td) / "outbox.json"
            with mock.patch.object(worker, "COMPLETION_OUTBOX", outbox), \
                    mock.patch.object(worker, "patch_job", return_value=False), \
                    mock.patch.object(worker, "_completion_is_acknowledged", return_value=False), \
                    mock.patch.object(worker.time, "sleep", return_value=None):
                self.assertFalse(worker.persist_terminal_job("job-1", "urn-1", "done", {"ok": True}))
            self.assertIn("job-1", json.loads(outbox.read_text()))
            with mock.patch.object(worker, "COMPLETION_OUTBOX", outbox), \
                    mock.patch.object(worker, "patch_job", return_value=True):
                worker.flush_completion_outbox()
            self.assertNotIn("job-1", json.loads(outbox.read_text()))

    def test_lease_thread_renews_until_stopped(self):
        calls = []
        with mock.patch.object(worker, "LEASE_RENEW_INTERVAL", 0.01), \
                mock.patch.object(worker, "patch_job", side_effect=lambda *args: calls.append(args) or True):
            worker._start_lease_renewal("job-lease", "urn-1")
            time.sleep(0.04)
            worker._stop_lease_renewal("job-lease")
        self.assertTrue(any(args[2] == "running" for args in calls))

    def test_no_workspace_release_is_one_atomic_valid_patch(self):
        calls = []

        def fake_http(method, path, body=None, timeout=0):
            calls.append((method, path, body))
            return 200, {"ok": True}

        job = {"id": "job-route", "kind": "unmapped-kind",
               "eligible_kinds": ["zeroclaw", "zoder"]}
        with mock.patch.object(worker, "patch_job", return_value=True), \
                mock.patch.object(worker, "_start_lease_renewal"), \
                mock.patch.object(worker, "_stop_lease_renewal"), \
                mock.patch.object(worker, "_is_code_kind", return_value=False), \
                mock.patch.object(worker, "_resolve_workspace", return_value=(None, "no_workspace_for_kind")), \
                mock.patch.object(worker, "route_chain", return_value=[]), \
                mock.patch.object(worker, "http", side_effect=fake_http):
            worker.process_job("urn-worker", job)
        routing = [call for call in calls if call[1] == "/v1/jobs/job-route"]
        self.assertEqual(len(routing), 1)
        self.assertEqual(routing[0][2]["status"], "queued")
        self.assertEqual(routing[0][2]["eligible_kinds"], ["claude", "codex"])

    def test_completion_outbox_is_instance_scoped_and_locked(self):
        source = Path("zeroclaw_wss_worker_v2.py").read_text()
        self.assertIn('hive_completion_outbox.{INSTANCE}.json', source)
        self.assertIn("fcntl.LOCK_EX", source)


if __name__ == "__main__":
    unittest.main()
