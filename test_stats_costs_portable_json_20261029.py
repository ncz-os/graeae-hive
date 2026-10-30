"""GET /v1/stats/costs must work on stock sqlite builds.

The provider-breakdown query uses the SQL/JSON function ``JSON_VALUE()``. Stock SQLite does not
provide it, so the route used to 500 there; the SQLite backend now registers a JSON_VALUE function
on every connection. The shared SQL must keep the standard spelling because the other backends
(Oracle in production) translate it; see test_json_value_dialect_20261030.py.
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


class StatsCostsTests(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        env = mock.patch.dict(
            os.environ,
            {"AGENT_BUS_DB": os.path.join(self._td.name, "s.db"), "HIVE_BACKEND": "sqlite"},
        )
        env.start()
        self.addCleanup(env.stop)
        repo = mock.patch.object(_deps, "_HIVE_REPO", None)
        repo.start()
        self.addCleanup(repo.stop)
        _run(lambda: hive_repo().init())

    def test_costs_stats_do_not_require_json_value(self):
        async def _do():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t"
            ) as c:
                r = await c.get("/v1/stats/costs")
                return r.status_code
        code = _run(_do)
        self.assertEqual(code, 200, f"costs stats broken: {code}")


if __name__ == "__main__":
    unittest.main()
