"""Tests for the harness adapter registry (Phase B).

No live bus required: the transport is injected. Verifies priority order,
per-harness capability/auth contracts, the codex operator gate, and the
bus-protocol method→HTTP mapping.
"""

import os
import unittest
from unittest import mock

from hive.harness import (
    get_harness_adapter,
    is_harness_enabled,
    list_enabled_harnesses,
    list_harnesses,
)
from hive.harness.base import HarnessAdapter, HarnessRegistration

EXPECTED_ORDER = ["zoder", "opencode", "codex", "claude", "generic"]
EXPECTED_PRIORITY = {"zoder": 0, "opencode": 1, "codex": 2, "claude": 3, "generic": 99}


class Recorder:
    """Captures (method, url, body) and returns a canned response."""

    def __init__(self, responses=None):
        self.calls = []
        self.responses = responses or {}

    def __call__(self, method, url, token, body):
        self.calls.append((method, url, body))
        return self.responses.get(method, (200, {}))


class RegistryTests(unittest.TestCase):
    def test_list_harnesses_is_priority_ordered(self):
        self.assertEqual(list_harnesses(), EXPECTED_ORDER)

    def test_every_adapter_satisfies_protocol_and_declares_priority(self):
        for kind, priority in EXPECTED_PRIORITY.items():
            with self.subTest(kind=kind):
                adapter = get_harness_adapter(kind)
                self.assertIsInstance(adapter, HarnessAdapter)
                self.assertEqual(adapter.harness_kind, kind)
                self.assertEqual(adapter.capabilities().priority, priority)

    def test_codex_is_operator_gated(self):
        # Disabled by default...
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("HARNESS_CODEX_AUTORUN", None)
            self.assertFalse(is_harness_enabled("codex"))
            self.assertNotIn("codex", list_enabled_harnesses())
            self.assertFalse(get_harness_adapter("codex").is_enabled())
            # ...enabled only with the explicit env.
            os.environ["HARNESS_CODEX_AUTORUN"] = "1"
            self.assertTrue(is_harness_enabled("codex"))
            self.assertIn("codex", list_enabled_harnesses())
            self.assertTrue(get_harness_adapter("codex").is_enabled())
            os.environ.pop("HARNESS_CODEX_AUTORUN", None)

    def test_unknown_kind_raises_keyerror(self):
        with self.assertRaises(KeyError):
            get_harness_adapter("nonexistent-harness")

    def test_module_class_escape_hatch_loads(self):
        adapter = get_harness_adapter("hive.harness.generic:GenericAdapter")
        self.assertEqual(adapter.harness_kind, "generic")

    def test_claude_uses_oauth_and_subscription_tier(self):
        caps = get_harness_adapter("claude").capabilities()
        self.assertEqual(caps.auth_scheme, "oauth")
        self.assertEqual(caps.cost_tier, "subscription")


class BusProtocolTests(unittest.TestCase):
    def _adapter(self, kind="zoder", responses=None):
        adapter = get_harness_adapter(kind)
        rec = Recorder(responses)
        adapter._transport = rec
        adapter.bus_url = "http://bus.test"
        return adapter, rec

    def test_register_posts_and_returns_urn(self):
        adapter, rec = self._adapter(
            responses={"POST": (200, {"urn": "urn:agent:zoder:h:s"})}
        )
        urn = adapter.register(
            HarnessRegistration(harness_kind="zoder", harness_version="1", capabilities=["code"])
        )
        self.assertEqual(urn, "urn:agent:zoder:h:s")
        method, url, body = rec.calls[0]
        self.assertEqual(method, "POST")
        self.assertEqual(url, "http://bus.test/v1/agents/register")
        self.assertEqual(body["kind"], "zoder")
        self.assertIn("code", body["capabilities"])

    def test_register_failure_raises(self):
        adapter, _ = self._adapter(responses={"POST": (500, {"detail": "nope"})})
        with self.assertRaises(RuntimeError):
            adapter.register(HarnessRegistration(harness_kind="zoder", harness_version="1"))

    def test_claim_maps_204_to_none(self):
        adapter, rec = self._adapter(responses={"POST": (204, None)})
        self.assertIsNone(adapter.claim("urn:agent:zoder:h:s"))
        self.assertIn("?agent_urn=urn:agent:zoder:h:s", rec.calls[0][1])

    def test_complete_and_fail_patch_job(self):
        adapter, rec = self._adapter()
        adapter.complete("urn:x", "job-1", {"summary": "ok"})
        adapter.fail("urn:x", "job-2", "boom")
        self.assertEqual([c[0] for c in rec.calls], ["PATCH", "PATCH"])
        self.assertEqual(rec.calls[0][1], "http://bus.test/v1/jobs/job-1")
        self.assertEqual(rec.calls[0][2]["status"], "done")
        self.assertEqual(rec.calls[1][2]["status"], "failed")
        self.assertEqual(rec.calls[1][2]["result"], {"error": "boom"})

    def test_execute_without_executor_fails_loudly(self):
        adapter, _ = self._adapter()
        with self.assertRaises(NotImplementedError):
            adapter.execute("urn:x", {"id": "job"})

    def test_execute_delegates_to_injected_executor(self):
        adapter, _ = self._adapter()
        seen = {}

        def executor(urn, job):
            seen["urn"], seen["job"] = urn, job
            return {"summary": "ran"}

        adapter._executor = executor
        self.assertEqual(adapter.execute("urn:x", {"id": "job"}), {"summary": "ran"})
        self.assertEqual(seen, {"urn": "urn:x", "job": {"id": "job"}})


if __name__ == "__main__":
    unittest.main()
