"""Cross-harness / cross-model communication + coordination.

A zoder worker (MiniMax), a claude worker (Anthropic) and a codex worker (OpenAI)
must be able to announce to + poll each other over the SAME bus — the bus has no
harness/model notion, and the agent-to-agent methods live on the shared
GenericAdapter. This proves it end-to-end with one shared in-memory bus.
"""

import unittest
import urllib.parse

from hive.harness import get_harness_adapter


class FakeBus:
    """Minimal in-memory stand-in for the hive bus (messages + roster)."""

    def __init__(self):
        self.messages = []
        self.agents = []
        self._n = 0

    def transport(self, method, url, token, body):
        path = urllib.parse.urlparse(url).path
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        if method == "POST" and path == "/v1/messages":
            self._n += 1
            mid = f"msg-{self._n}"
            self.messages.append(
                {
                    "id": mid,
                    "from": body["from_urn"],
                    "to": body.get("to_urn"),
                    "topic": body["topic"],
                    "payload": body["payload"],
                    "ts": float(self._n),
                }
            )
            return 200, {"id": mid}
        if method == "GET" and path == "/v1/messages":
            to_urn = (qs.get("to_urn") or [None])[0]
            since = float((qs.get("since") or ["0"])[0])
            out = [m for m in self.messages if m["ts"] >= since and (m["to"] is None or m["to"] == to_urn)]
            return 200, {"count": len(out), "messages": out}
        if method == "GET" and path == "/v1/agents":
            return 200, {"count": len(self.agents), "agents": self.agents}
        return 200, {}


def _adapter(kind, bus):
    a = get_harness_adapter(kind)
    a._transport = bus.transport
    return a


class CrossHarnessTests(unittest.TestCase):
    def setUp(self):
        self.bus = FakeBus()
        self.zoder = _adapter("zoder", self.bus)  # MiniMax
        self.claude = _adapter("claude", self.bus)  # Anthropic
        self.codex = _adapter("codex", self.bus)  # OpenAI

    def test_broadcast_reaches_all_harnesses(self):
        self.zoder.publish_message(
            from_urn="urn:agent:zoder:hydra:s1", topic="budget.warn", payload={"pool": "minimax"}
        )
        # every other harness sees the broadcast
        self.assertIn("budget.warn", {m["topic"] for m in self.claude.list_messages(to_urn="urn:agent:claude:x:s")})
        self.assertIn("budget.warn", {m["topic"] for m in self.codex.list_messages(to_urn="urn:agent:codex:y:s")})

    def test_direct_message_is_targeted(self):
        self.claude.publish_message(
            from_urn="urn:agent:claude:pythia:s1",
            to_urn="urn:agent:zoder:hydra:s1",
            topic="job.intent:build",
            payload={"kind": "build:compile"},
        )
        zoder_inbox = self.zoder.list_messages(to_urn="urn:agent:zoder:hydra:s1")
        self.assertIn("job.intent:build", {m["topic"] for m in zoder_inbox})
        # a different worker does NOT see a direct message addressed elsewhere
        other = self.zoder.list_messages(to_urn="urn:agent:zoder:other:s9")
        self.assertNotIn("job.intent:build", {m["topic"] for m in other})

    def test_roster_is_harness_agnostic(self):
        self.bus.agents = [
            {"urn": "urn:agent:zoder:hydra:s1", "kind": "zoder", "model": "minimax-m3", "provider": "minimax"},
            {"urn": "urn:agent:claude:pythia:s1", "kind": "claude", "model": "claude-opus", "provider": "anthropic"},
            {"urn": "urn:agent:codex:minos:s1", "kind": "codex", "model": "gpt-5.5", "provider": "openai"},
        ]
        peers = self.zoder.roster()
        kinds = {p["kind"] for p in peers}
        self.assertEqual(kinds, {"zoder", "claude", "codex"})  # all harnesses visible


if __name__ == "__main__":
    unittest.main()
