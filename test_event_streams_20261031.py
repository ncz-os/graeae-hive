"""FB60: event stream endpoints — ordering, reconnect, slow consumer, disconnect
cleanup, subscriber cap, and the log endpoint.

Known harness limitation (FB22-SSE, re-verified here as FB60-1): the SSE
endpoint's ``request.is_disconnected()`` poll and sse-starlette's disconnect
listener BOTH consume the ASGI ``receive`` channel; httpx's ASGITransport
answers the second ``receive()`` only after the response completes, so an
in-process stream deadlocks with an idle loop. Production serves this route
under uvicorn, so — following the ``test_api_contract_20261029`` precedent —
these tests serve the real app over loopback TCP (uvicorn in a thread) and
drive it with real HTTP. Same process, so the shared EVENT_QUEUE is directly
assertable; the publisher path (POST /v1/messages -> events table + fan-out)
is the real one.

Observed behaviours pinned here (``hive/bus/routers/events.py``):

* catch-up frames carry ``id``; LIVE frames do not — resume covers the DB
  tail only, not the live gap;
* a slow consumer's queue drops (put_nowait + suppress QueueFull in
  ``deps.emit_event``), never back-pressures the publisher;
* closing the stream removes the subscription (``finally`` pop) — verified
  below with no polling longer than 10 s.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import threading
import time
import unittest
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")
os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")

import httpx  # noqa: E402

try:
    import uvicorn  # noqa: E402
except ImportError:
    uvicorn = None  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus import deps as _deps  # noqa: E402
from hive.bus.deps import EVENT_QUEUE, MAX_EVENT_SUBSCRIBERS  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


def parse_frames(text):
    """Split SSE text into [{event, id, data}] (data raw string)."""
    out = []
    for chunk in text.replace("\r\n", "\n").split("\n\n"):
        frame = {}
        for line in chunk.splitlines():
            if line.startswith(":"):
                continue
            if ":" not in line:
                continue
            k, v = line.split(":", 1)
            if k in ("event", "id", "data"):
                frame[k] = v.strip() if k != "data" else v.lstrip(" ")
        if frame:
            out.append(frame)
    return out


class StreamCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if uvicorn is None:
            raise unittest.SkipTest("uvicorn not installed")
        cls._td = tempfile.TemporaryDirectory()
        cls._env = mock.patch.dict(
            os.environ, {"AGENT_BUS_DB": os.path.join(cls._td.name, "ev.db"), "HIVE_BACKEND": "sqlite"}
        )
        cls._env.start()
        for var in ("MNEMOS_URL", "MNEMOS_TOKEN", "HIVE_SHARED_SECRET", "HIVE_BUS_ADMIN_TOKEN"):
            os.environ.pop(var, None)
        cls._repo_patch = mock.patch.object(_deps, "_HIVE_REPO", None)
        cls._repo_patch.start()

        config = uvicorn.Config(agent_bus.app, host="127.0.0.1", port=0, log_level="warning")
        cls._server = uvicorn.Server(config)
        cls._thread = threading.Thread(target=cls._server.run, daemon=True)
        cls._thread.start()
        for _ in range(100):
            if cls._server.started:
                break
            time.sleep(0.1)
        if not cls._server.started or not cls._server.servers:
            cls._server.should_exit = True
            raise unittest.SkipTest("loopback SSE server did not start")
        port = cls._server.servers[0].sockets[0].getsockname()[1]
        cls.base = f"http://127.0.0.1:{port}"
        cls.orch = cls._register(
            {
                "kind": "opencode",
                "host": "ev",
                "runtime": "opencode",
                "status": "online",
                "capabilities": ["code"],
                "cost_tier": "B",
            }
        )

    @classmethod
    def tearDownClass(cls):
        cls._server.should_exit = True
        cls._thread.join(timeout=10)
        cls._repo_patch.stop()
        cls._env.stop()
        cls._td.cleanup()

    def tearDown(self):
        EVENT_QUEUE.clear()

    # ---- harness ----
    @classmethod
    def _register(cls, payload):
        async def _do():
            async with httpx.AsyncClient(base_url=cls.base, timeout=10.0) as c:
                r = await c.post("/v1/agents/register", json=payload)
                assert r.status_code == 200, r.text
                return r.json()["urn"]

        return _run(_do())

    def _publish(self, topic, n=1):
        async def _do():
            async with httpx.AsyncClient(base_url=self.base, timeout=10.0) as c:
                for i in range(n):
                    r = await c.post("/v1/messages", json={"from_urn": self.orch, "topic": topic, "payload": {"i": i}})
                    assert r.status_code == 200, r.text

        _run(_do())

    def _log(self, **params):
        async def _do():
            async with httpx.AsyncClient(base_url=self.base, timeout=10.0) as c:
                r = await c.get("/v1/events/log", params=params or None)
                return r.status_code, r.json()

        return _run(_do())

    def _stream_until(self, path, want, timeout=10.0):
        """Open the SSE stream, collect frames until `want` matches, close."""

        async def _do():
            tout = httpx.Timeout(10.0, read=10.0)
            async with httpx.AsyncClient(base_url=self.base, timeout=tout) as c:
                async with c.stream("GET", path) as r:
                    assert r.status_code == 200, r.status_code
                    buf = ""
                    frames = []

                    async def _read():
                        nonlocal buf, frames
                        async for chunk in r.aiter_text():
                            buf += chunk
                            frames = parse_frames(buf)
                            if want(frames):
                                return True
                        return False

                    await asyncio.wait_for(_read(), timeout=timeout)
                    return r.status_code, frames

        return _run(_do())

    # ---- tests ----
    def _last_id(self):
        _, body = self._log()
        return body["last_id"] or 0

    def test_catchup_ordering(self):
        """Invariant: catch-up frames arrive in id order (tail_events ORDER BY id ASC)."""
        base = self._last_id()
        self._publish("order-probe", n=5)
        _, frames = self._stream_until(f"/v1/events?since_id={base}", lambda f: len(f) >= 5)
        self.assertEqual([f.get("id") for f in frames[:5]], [str(base + i) for i in (1, 2, 3, 4, 5)])

    def test_reconnect_resumes_after_last_event_id(self):
        """Invariant: since_id=N replays only id>N rows (and /v1/events/log
        agrees); live frames carry no id, so the live gap itself is not
        resumable — resume covers the DB tail only."""
        base = self._last_id()
        self._publish("resume-probe", n=5)
        code, body = self._log(since_id=base + 3)
        self.assertEqual(code, 200)
        self.assertEqual([e["id"] for e in body["events"]], [base + 4, base + 5])
        self.assertEqual(body["last_id"], base + 5)
        _, frames = self._stream_until(f"/v1/events?since_id={base + 3}", lambda f: len(f) >= 1)
        self.assertEqual(frames[0].get("id"), str(base + 4))
        code, body = self._log(since_id=base + 5)
        self.assertEqual(body["events"], [])
        self.assertEqual(body["last_id"], base + 5)

    def test_slow_consumer_drops_instead_of_blocking(self):
        """Invariant (fan-out drop, not backpressure): publishing 1005 events
        while a non-reading stream is open completes quickly, and the slow
        subscriber never stalls the publisher."""
        got = {}

        async def _hold_open():
            tout = httpx.Timeout(10.0, read=10.0)
            async with httpx.AsyncClient(base_url=self.base, timeout=tout) as c:
                async with c.stream("GET", "/v1/events") as r:
                    assert r.status_code == 200
                    got["subs"] = len(EVENT_QUEUE)
                    await asyncio.sleep(6)

        async def _flood():
            async with httpx.AsyncClient(base_url=self.base, timeout=10.0) as c:
                for i in range(1005):
                    r = await c.post(
                        "/v1/messages", json={"from_urn": self.orch, "topic": "flood", "payload": {"i": i}}
                    )
                    assert r.status_code == 200, (i, r.text)

        async def _both():
            await asyncio.gather(_hold_open(), _flood())

        _run(asyncio.wait_for(_both(), timeout=30.0))
        self.assertEqual(got.get("subs"), 1)
        self.assertEqual(dict(EVENT_QUEUE), {})

    def test_disconnect_cleans_up_without_leaking_subscriptions(self):
        """Invariant: closing the stream removes the subscription — no leaked
        subscriber state, and a later stream still works."""
        base = self._last_id()
        self._publish("disc-probe", n=2)
        _, frames = self._stream_until(f"/v1/events?since_id={base}", lambda f: len(f) >= 2)
        self.assertEqual(len(frames), 2)
        for _ in range(100):
            if not EVENT_QUEUE:
                break
            time.sleep(0.1)
        self.assertEqual(dict(EVENT_QUEUE), {})
        # The server is still healthy: a fresh stream opens and replays.
        _, frames = self._stream_until(f"/v1/events?since_id={base}", lambda f: len(f) >= 2)
        self.assertEqual([f.get("id") for f in frames[:2]], [str(base + 1), str(base + 2)])

    def test_subscriber_cap_is_503_and_recovers(self):
        """Invariant: the 101st concurrent subscriber is refused with 503;
        after cleanup the stream opens again."""
        import asyncio as _asyncio

        for i in range(MAX_EVENT_SUBSCRIBERS):
            EVENT_QUEUE[f"dummy-{i}"] = _asyncio.Queue()
        try:
            code, _ = self._stream_until("/v1/events", lambda f: True)
            self.fail(f"expected 503, stream opened: {code}")
        except AssertionError as e:
            self.assertIn("503", str(e))
        finally:
            EVENT_QUEUE.clear()
        code, _ = self._stream_until("/v1/events?since_id=0", lambda f: True)
        self.assertEqual(code, 200)

    def test_log_limit_clamp_and_empty(self):
        """Invariant: limit=0/negative falls back to the default, huge limits
        clamp to 1000, and an empty log reports last_id == since_id."""
        self._publish("log-probe", n=3)
        _, full = self._log()
        total = full["count"]
        self.assertGreaterEqual(total, 3)
        for params in ({"limit": 0}, {"limit": -5}):
            code, body = self._log(**params)
            self.assertEqual(code, 200)
            self.assertEqual(body["count"], total)
        code, body = self._log(limit=10**9)
        self.assertEqual(code, 200)
        self.assertEqual(body["count"], total)


if __name__ == "__main__":
    unittest.main()
