"""Tests for hive.core.models — the model/pricing registry + zoder catalog."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import hive.core.models as models


class CatalogTests(unittest.TestCase):
    def tearDown(self):
        models._zoder_catalog.cache_clear()

    def test_zoder_catalog_is_preferred(self):
        with tempfile.TemporaryDirectory() as td:
            cat = Path(td) / "pricing.json"
            cat.write_text(
                json.dumps({"models": {"minimax-m3": {"input_usd_per_mtok": 0.5, "output_usd_per_mtok": 1.5}}})
            )
            with mock.patch.object(models, "ZODER_PRICING_JSON", cat):
                models._zoder_catalog.cache_clear()
                # catalog (fresh) wins over the built-in 0.40/1.20
                self.assertEqual(models.rate_for("minimax", "minimax-m3"), (0.5, 1.5))

    def test_missing_catalog_falls_back_to_builtin(self):
        with tempfile.TemporaryDirectory() as td:
            missing = Path(td) / "nope.json"
            with mock.patch.object(models, "ZODER_PRICING_JSON", missing):
                models._zoder_catalog.cache_clear()
                # built-in table used when the catalog is absent
                self.assertEqual(models.rate_for("minimax", "minimax-m3"), (0.40, 1.20))

    def test_provider_wildcard_and_free_tier_fallback(self):
        models._zoder_catalog.cache_clear()
        with mock.patch.object(models, "ZODER_PRICING_JSON", Path("/nonexistent")):
            models._zoder_catalog.cache_clear()
            # unknown model on a known provider -> provider wildcard
            self.assertEqual(models.rate_for("groq", "some-new-model"), models.LLM_RATES[("groq", "*")])
            # tier-A provider -> free
            self.assertEqual(models.rate_for("local-llamacpp", "whatever"), (0.0, 0.0))


class CostTierTests(unittest.TestCase):
    def test_tiers(self):
        self.assertEqual(models.cost_tier_for("minimax"), "C")  # not in the map -> conservative C
        self.assertEqual(models.cost_tier_for("groq"), "B")
        self.assertEqual(models.cost_tier_for("local-llamacpp"), "A")


if __name__ == "__main__":
    unittest.main()
