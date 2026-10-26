"""The bus's KNEMON cost router is gone (operator decision 2026-10-26).

Removed: hive/bus/routers/knemon.py, its /v1/knemon/* endpoints, the bus
KNEMON_PROVIDERS table + knemon_candidates, and the dashboard OAuth-cap banner.

Kept on purpose: the registry's "knemon" route data, which still generates the
[router.providers] section of each host's zoder.toml (zoder's own feature).
"""

from __future__ import annotations

import importlib
import shutil
import subprocess
import sys
import tomllib
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
REMOVED_PATHS = {
    "/v1/knemon/route",
    "/v1/knemon/bypass",
    "/v1/knemon/cap",
    "/v1/knemon/cap/clear",
    "/v1/knemon/cap/probe-ok",
    "/v1/knemon/providers",
    "/v1/knemon/spend",
}


class BusRouterRemoved(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import hive.bus.app as bus_app

        cls.app = bus_app.app

    def test_openapi_has_no_knemon_paths(self):
        paths = set(self.app.openapi()["paths"])
        self.assertTrue(paths, "OpenAPI listing is empty; the app did not build")
        self.assertFalse([p for p in paths if p.startswith("/v1/knemon")], sorted(paths))

    def test_no_registered_route_serves_a_removed_path(self):
        registered = {getattr(r, "path", "") for r in self.app.routes}
        self.assertFalse(registered & REMOVED_PATHS)
        self.assertFalse([p for p in registered if "knemon" in p])

    def test_app_startup_does_not_import_the_router_module(self):
        self.assertNotIn("hive.bus.routers.knemon", sys.modules)
        with self.assertRaises(ModuleNotFoundError):
            importlib.import_module("hive.bus.routers.knemon")

    def test_app_facade_no_longer_reexports_cap_helpers(self):
        import agent_bus

        for name in (
            "KNEMON_BYPASS_FILE",
            "KNEMON_CAP_FILE",
            "OAUTH_CAP_RESET_SEC",
            "clear_oauth_cap",
            "knemon_bypass_active",
            "oauth_cap_state",
        ):
            self.assertFalse(hasattr(agent_bus, name), name)

    def test_models_has_no_knemon_machinery(self):
        from hive.core import models

        self.assertFalse(hasattr(models, "KNEMON_PROVIDERS"))
        self.assertFalse(hasattr(models, "knemon_candidates"))

    def test_repository_has_no_spend_summary(self):
        from hive.persistence.base import HiveMindRepository

        self.assertFalse(hasattr(HiveMindRepository, "spend_summary"))


class DashboardBannerRemoved(unittest.TestCase):
    JS = (HERE / "static" / "dashboard.js").read_text(encoding="utf-8")
    HTML = (HERE / "dashboard.html").read_text(encoding="utf-8")

    def test_dashboard_no_longer_polls_knemon(self):
        self.assertNotIn("/v1/knemon", self.JS)
        self.assertNotIn("/v1/knemon", self.HTML)
        for sym in ("refreshCapBanner", "renderCapBanner", "_capState", "cap-banner"):
            self.assertNotIn(sym, self.JS, sym)
        self.assertNotIn('id="cap-banner"', self.HTML)

    def test_dashboard_js_parses(self):
        node = shutil.which("node")
        if not node:
            raise unittest.SkipTest("node not available")
        out = subprocess.run(
            [node, "--check", str(HERE / "static" / "dashboard.js")],
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(out.returncode, 0, out.stderr)


class ZoderRouterProvidersKept(unittest.TestCase):
    """zoder's KNEMON feature keeps its data: the generated [router.providers]."""

    def test_generated_zoder_toml_keeps_router_providers(self):
        from hive.core import model_registry as mr

        reg = mr.load_registry(mr.DEFAULT_REGISTRY)
        want = [
            m.knemon_alias
            for m in sorted(
                (m for m in reg.models if "knemon" in m.routes),
                key=lambda m: m.knemon_order or 0,
            )
        ]
        self.assertTrue(want)
        for host in reg.hosts:
            doc = tomllib.loads((mr.GENERATED_DIR / host / "zoder.toml").read_text())
            providers = doc["router"]["providers"]
            got = [providers[str(i)]["alias"] for i in range(len(providers))]
            self.assertEqual(got, want, host)


if __name__ == "__main__":
    unittest.main()
