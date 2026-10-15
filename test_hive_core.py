"""Tests for hive.core.config (typed settings)."""

import dataclasses
import os
import unittest
from unittest import mock

from hive.core.config import Settings, get_settings


class SettingsTests(unittest.TestCase):
    def test_defaults_match_legacy_inline_defaults(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            s = Settings.from_env()
        self.assertEqual(s.hive_url, "http://127.0.0.1:5005")
        self.assertEqual(s.backend, "sqlite")
        self.assertEqual(s.agent_bus_db, "/srv/agent-bus/agents.db")
        self.assertEqual(s.claim_lease_seconds, 1800)
        self.assertEqual(s.mnemos_url, "http://192.0.2.67:5002")

    def test_env_overrides_and_types(self):
        env = {
            "HIVE_URL": "http://bus.example:5005/",
            "HIVE_BACKEND": "Oracle",
            "CLAIM_LEASE_SECONDS": "60",
            "HIVE_HOST_DENYLIST": "a, b ,c",
            "HIVE_DB2_DSN": "database=test",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            s = Settings.from_env()
        self.assertEqual(s.hive_url, "http://bus.example:5005")  # trailing / stripped
        self.assertEqual(s.backend, "oracle")  # lowercased
        self.assertEqual(s.claim_lease_seconds, 60)
        self.assertEqual(s.host_denylist, frozenset({"a", "b", "c"}))
        self.assertEqual(s.db2_dsn, "database=test")

    def test_db2_dsn_prefers_db2_dsn_over_hive_db2_dsn(self):
        with mock.patch.dict(os.environ, {"DB2_DSN": "primary", "HIVE_DB2_DSN": "fallback"}, clear=True):
            self.assertEqual(Settings.from_env().db2_dsn, "primary")
        with mock.patch.dict(os.environ, {"HIVE_DB2_DSN": "fallback"}, clear=True):
            self.assertEqual(Settings.from_env().db2_dsn, "fallback")

    def test_bad_int_falls_back_to_default(self):
        with mock.patch.dict(os.environ, {"CLAIM_LEASE_SECONDS": "not-a-number"}, clear=True):
            self.assertEqual(Settings.from_env().claim_lease_seconds, 1800)

    def test_get_settings_is_cached_and_immutable(self):
        get_settings.cache_clear()
        a = get_settings()
        self.assertIs(a, get_settings())
        with self.assertRaises(dataclasses.FrozenInstanceError):
            a.hive_url = "x"  # frozen dataclass
        get_settings.cache_clear()


if __name__ == "__main__":
    unittest.main()
