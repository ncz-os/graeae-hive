"""FB22-fix: GET /v1/jobs/metrics must not be shadowed by /v1/jobs/{job_id}.

FastAPI matches routes in registration order. ``/v1/jobs/{job_id}`` is
registered before the static ``/v1/jobs/metrics`` route, so a metrics call
is captured as job_id="metrics" and answered 404. Fails on the base line
(404), passes once the metrics route is registered first (200).
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus import deps as _deps  # noqa: E402
from hive.bus.deps import hive_repo  # noqa: E402


def _run(coro):
    return asyncio.run(coro())


class MetricsRouteTests(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        env = mock.patch.dict(
            os.environ,
            {"AGENT_BUS_DB": os.path.join(self._td.name, "m.db"), "HIVE_BACKEND": "sqlite"},
        )
        env.start()
        self.addCleanup(env.stop)
        repo = mock.patch.object(_deps, "_HIVE_REPO", None)
        repo.start()
        self.addCleanup(repo.stop)
        _run(lambda: hive_repo().init())

    def test_metrics_route_is_reachable(self):
        async def _do():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t"
            ) as c:
                r = await c.get("/v1/jobs/metrics")
                return r.status_code, r.json()
        code, data = _run(_do)
        self.assertEqual(code, 200, f"metrics shadowed: {code} {data}")
        for key in ("pending", "running", "completed", "failed", "total"):
            self.assertIn(key, data)


if __name__ == "__main__":
    unittest.main()
