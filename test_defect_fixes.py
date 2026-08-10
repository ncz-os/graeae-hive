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


class AuthTests(unittest.IsolatedAsyncioTestCase):
    async def _request(self, path, token=None):
        headers = []
        if token is not None:
            headers.append((b"authorization", f"Bearer {token}".encode()))
        request = Request({"type": "http", "method": "GET", "path": path,
                           "raw_path": path.encode(), "query_string": b"",
                           "headers": headers, "scheme": "http", "server": ("test", 80)})

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


if __name__ == "__main__":
    unittest.main()
