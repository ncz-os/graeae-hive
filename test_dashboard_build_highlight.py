"""A host whose workers report build_busy is highlighted on the Worker Pool.

Contract (2026-10-28): the worker's build gate publishes ``build_busy`` in its
heartbeat metadata; the host card gets the ``building`` class and a BUILDING
badge in amber (never the red/green of a failure/healthy state), sorts first,
and says it is not claiming new work.
"""

import re
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
JS = (HERE / "static" / "dashboard.js").read_text()
CSS = (HERE / "static" / "dashboard.css").read_text()


class BuildHighlight(unittest.TestCase):
    def test_reads_build_busy_from_agent_metadata(self):
        self.assertIn("a.metadata.build_busy", JS)
        self.assertRegex(JS, r"typeof a\.metadata\.build_busy==='string'")

    def test_building_host_gets_class_badge_and_sorts_first(self):
        self.assertIn("' building'", JS)
        self.assertIn("wc-build-badge", JS)
        self.assertIn("BUILDING", JS)
        self.assertIn("(!!b.building-!!a.building)", JS)
        self.assertIn("not claiming", JS)

    def test_a_building_host_is_not_dimmed_as_idle(self):
        # building wins over the idle class even when no job is running
        self.assertRegex(JS, r"h\.building\?' building':\(h\.busy\?'':' idle'\)")

    def test_style_is_amber_not_red_or_green(self):
        rule = re.search(r"\.worker-card\.building\{([^}]*)\}", CSS)
        self.assertIsNotNone(rule)
        self.assertIn("#d29922", rule.group(1))
        self.assertNotRegex(rule.group(1), r"#(da3633|f85149|238636)")


if __name__ == "__main__":
    unittest.main()
