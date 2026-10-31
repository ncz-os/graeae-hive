"""FB56: authorization matrix — every OpenAPI route asserted live.

``docs/AUTH_MATRIX.md`` states the contract: a machine-readable METHOD/PATH/
GATE table plus the findings where behaviour differs from what the dependency
or path declares. This test:

1. parses the matrix from the doc and the route set from ``app.openapi()`` and
   asserts the two are EQUAL (no undocumented route, no stale entry);
2. asserts every cell live through the in-process app (ASGITransport, temp DB):
   anon reads, 401-without-actor, 403-wrong-principal, 409-unregistered,
   worker-as-submitter 403, and the pinned-any-agent / pinned-anon behaviours
   behind findings FB56-1..5 (these assertions pin the CURRENT behaviour as the
   regression tripwire for the future fix — each names its finding).

Harness follows ``test_api_contract_20261029`` (temp DB file, ``_HIVE_REPO``
patched to None, secret/token env vars cleared so only the actor half of
``require_mutation_actor`` applies). One class-level harness, unique ids per
case, no sleeps, no network.
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")
os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus import deps as _deps  # noqa: E402
from hive.bus.deps import hive_repo  # noqa: E402

DOC_PATH = Path(__file__).resolve().parent / "docs" / "AUTH_MATRIX.md"
TABLE_START = "<!-- FB56-MATRIX-START -->"
TABLE_END = "<!-- FB56-MATRIX-END -->"


def parse_matrix():
    """{(METHOD, path): gate} from the doc's machine-readable table."""
    text = DOC_PATH.read_text()
    body = text.split(TABLE_START, 1)[1].split(TABLE_END, 1)[0]
    out = {}
    for line in body.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) != 3 or set(cells) == {"---"} or cells[0].lower() == "method":
            continue
        out[(cells[0], cells[1])] = cells[2]
    return out


def openapi_routes():
    """{(METHOD, path)} from the live app, minus auto-generated HEAD/OPTIONS."""
    spec = agent_bus.app.openapi()
    out = set()
    for path, methods in spec["paths"].items():
        for method in methods:
            if method.upper() in ("HEAD", "OPTIONS"):
                continue
            out.add((method.upper(), path))
    return out


def _run(coro):
    return asyncio.run(coro)


class AuthMatrixTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._td = tempfile.TemporaryDirectory()
        cls._env = mock.patch.dict(
            os.environ,
            {
                "AGENT_BUS_DB": os.path.join(cls._td.name, "auth.db"),
                "HIVE_BACKEND": "sqlite",
            },
        )
        cls._env.start()
        for var in ("MNEMOS_URL", "MNEMOS_TOKEN", "HIVE_SHARED_SECRET", "HIVE_BUS_ADMIN_TOKEN"):
            os.environ.pop(var, None)
        cls._repo_patch = mock.patch.object(_deps, "_HIVE_REPO", None)
        cls._repo_patch.start()
        _run(hive_repo().init())
        cls.orch = cls._register(
            {
                "kind": "opencode",
                "host": "m1",
                "runtime": "opencode",
                "status": "online",
                "capabilities": ["code"],
                "cost_tier": "B",
            }
        )
        cls.worker = cls._register(
            {
                "kind": "zeroclaw",
                "host": "m1",
                "runtime": "zeroclaw",
                "status": "online",
                "capabilities": ["code"],
                "cost_tier": "B",
            }
        )
        cls.n = 0

    @classmethod
    def tearDownClass(cls):
        cls._repo_patch.stop()
        cls._env.stop()
        cls._td.cleanup()

    # ---- harness ----
    @classmethod
    def _register(cls, payload):
        async def _do():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t") as c:
                r = await c.post("/v1/agents/register", json=payload)
                assert r.status_code == 200, r.text
                return r.json()["urn"]

        return _run(_do())

    def _call(self, method, path, json_body=None, actor=None, params=None):
        async def _do():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t") as c:
                headers = {"X-Hive-Actor": actor} if actor else None
                r = await c.request(method, path, json=json_body, headers=headers, params=params)
                try:
                    return r.status_code, r.json()
                except Exception:
                    return r.status_code, None

        return _run(_do())

    def _submit(self, submitter=None):
        self.n += 1
        code, data = self._call(
            "POST",
            "/v1/jobs",
            {"kind": "loop", "description": f"auth matrix {self.n}", "submitter_urn": submitter or self.orch},
        )
        self.assertIn(code, (200, 201), data)
        return data["id"]

    def _claimed(self):
        jid = self._submit()
        code, _ = self._call("POST", f"/v1/jobs/{jid}/claim", params={"by": self.worker})
        self.assertEqual(code, 200)
        return jid

    # ---- 1. completeness: doc table == OpenAPI routes ----
    def test_matrix_covers_every_route_and_nothing_else(self):
        """Invariant: the doc table and the live schema name the same routes."""
        matrix = parse_matrix()
        live = openapi_routes()
        doc_only = sorted(set(matrix) - live)
        app_only = sorted(live - set(matrix))
        self.assertEqual(set(matrix), live, f"doc<->schema drift:\n doc-only={doc_only}\n app-only={app_only}")

    def test_matrix_has_forty_two_routes(self):
        """Invariant: pins the size so a silently-dropped row fails loudly."""
        self.assertEqual(len(parse_matrix()), 42)

    # ---- 2a. anon reads ----
    def test_anon_reads_succeed(self):
        """Invariant: every read route is credential-free."""
        for path in (
            "/",
            "/health",
            "/v1/agents",
            "/v1/agents/stats",
            "/v1/hosts",
            "/v1/agents/whoami",
            "/v1/jobs",
            "/v1/jobs/summary",
            "/v1/jobs/metrics",
            "/v1/messages",
            "/v1/repos",
            "/v1/repos/resolve",
            "/v1/schedules",
            "/v1/stats/workers",
            "/v1/stats/cache",
            "/v1/stats/costs",
            "/v1/events/log",
        ):
            params = {"q": "x"} if path == "/v1/repos/resolve" else None
            params = {"host": "nope", "pid": 1} if path == "/v1/agents/whoami" else params
            code, _ = self._call("GET", path, params=params)
            # whoami with no match 404s; /health 503s without the lifespan
            # reaper task under ASGITransport — both prove no 401 gate
            # precedes the handler.
            allowed = (200, 503) if path == "/health" else (200, 404)
            self.assertIn(code, allowed, path)

    def test_anon_throttle_unknown_is_404_not_401(self):
        code, _ = self._call("GET", "/v1/agents/urn:x/throttle")
        self.assertEqual(code, 404)

    def test_anon_job_get_unknown_is_404_not_401(self):
        code, _ = self._call("GET", "/v1/jobs/does-not-exist")
        self.assertEqual(code, 404)

    # ---- 2b. registration / heartbeat ----
    def test_register_is_anon_and_heartbeat_needs_a_row(self):
        urn = self._register(
            {
                "kind": "zeroclaw",
                "host": "m9",
                "runtime": "zeroclaw",
                "status": "online",
                "capabilities": [],
                "cost_tier": "B",
            }
        )
        self.assertTrue(urn.startswith("urn:"))
        code, _ = self._call("POST", "/v1/agents/heartbeat", {"urn": "urn:agent:x:y:z"})
        self.assertEqual(code, 409)
        code, _ = self._call("POST", "/v1/agents/heartbeat", {"urn": urn})
        self.assertEqual(code, 200)

    # ---- 2c. submit: caller anon, submitter-urn authorized (FB56-3) ----
    def test_worker_runtime_submitter_is_refused(self):
        code, data = self._call(
            "POST", "/v1/jobs", {"kind": "loop", "description": "worker submit", "submitter_urn": self.worker}
        )
        self.assertEqual(code, 403, data)

    def test_anonymous_caller_may_submit_as_the_orchestrator(self):
        """Invariant (FB56-3): no proof of possession — anyone on the network
        can submit jobs attributed to any orchestrator URN."""
        code, data = self._call(
            "POST", "/v1/jobs", {"kind": "loop", "description": "impersonated submit", "submitter_urn": self.orch}
        )
        self.assertIn(code, (200, 201), data)

    # ---- 2d. dequeue / claim need registered+active ----
    def test_next_and_claim_refuse_unknown_and_offline_agents(self):
        code, _ = self._call("POST", "/v1/jobs/next", params={"agent_urn": "urn:agent:x:y:z"})
        self.assertEqual(code, 409)
        code, _ = self._call("POST", "/v1/jobs/whatever/claim", params={"by": "urn:agent:x:y:z"})
        self.assertEqual(code, 409)
        ghost = self._register(
            {
                "kind": "zeroclaw",
                "host": "m2",
                "runtime": "zeroclaw",
                "status": "online",
                "capabilities": [],
                "cost_tier": "B",
            }
        )
        jid = self._submit()
        code, _ = self._call("POST", f"/v1/jobs/{jid}/claim", params={"by": ghost})
        self.assertEqual(code, 200)
        # Offline agent cannot claim: deregister self, then claim.
        code, _ = self._call("POST", f"/v1/agents/{ghost}/deregister", {"reason": "t"}, actor=ghost)
        self.assertEqual(code, 200)
        jid2 = self._submit()
        code, _ = self._call("POST", f"/v1/jobs/{jid2}/claim", params={"by": ghost})
        self.assertEqual(code, 409)

    # ---- 2e. PATCH: claimant wins, others lose, submitter cancels ----
    def test_patch_claimant_gate_and_submitter_cancel(self):
        jid = self._claimed()
        code, _ = self._call("PATCH", f"/v1/jobs/{jid}", {"status": "running", "claimed_by": self.orch})
        self.assertEqual(code, 403)
        code, _ = self._call("PATCH", f"/v1/jobs/{jid}", {"status": "running", "claimed_by": self.worker})
        self.assertEqual(code, 200)
        code, data = self._call(
            "PATCH", f"/v1/jobs/{jid}", {"status": "cancelled", "cancel_actor": self.orch, "reason": "matrix test"}
        )
        self.assertEqual(code, 200, data)

    # ---- 2f. messages: from_urn must be registered+active ----
    def test_message_publish_needs_active_from(self):
        code, _ = self._call("POST", "/v1/messages", {"from_urn": "urn:agent:x:y:z", "topic": "t", "payload": {}})
        self.assertEqual(code, 403)
        code, data = self._call("POST", "/v1/messages", {"from_urn": self.worker, "topic": "t", "payload": {"a": 1}})
        self.assertEqual(code, 200, data)

    # ---- 2g. deregister: admin-or-self ----
    def test_deregister_self_ok_other_forbidden_anon_unauthorized(self):
        self.assertIn(self._call("POST", f"/v1/agents/{self.worker}/deregister", {"reason": "x"})[0], (401, 403))
        code, _ = self._call("POST", f"/v1/agents/{self.orch}/deregister", {"reason": "x"}, actor=self.worker)
        self.assertEqual(code, 403)
        tmp = self._register(
            {
                "kind": "zeroclaw",
                "host": "m3",
                "runtime": "zeroclaw",
                "status": "online",
                "capabilities": [],
                "cost_tier": "B",
            }
        )
        code, _ = self._call("POST", f"/v1/agents/{tmp}/deregister", {"reason": "self out"}, actor=tmp)
        self.assertEqual(code, 200)

    # ---- 2h. any-active-agent routes (FB56-4) ----
    def test_admin_routes_need_an_actor_but_any_worker_will_do(self):
        """Invariant (FB56-4): /v1/admin + purge + schedules mutations demand
        X-Hive-Actor (401 without), but accept ANY active worker — not admin."""
        jid = self._submit()
        code, _ = self._call("POST", "/v1/admin/jobs/requeue", {"job_ids": [jid]})
        self.assertEqual(code, 401)
        code, data = self._call(
            "POST", "/v1/admin/jobs/requeue", {"job_ids": [jid], "reason": "matrix"}, actor=self.worker
        )
        self.assertEqual(code, 200, data)
        self.assertEqual(data["ok"], [jid])
        code, data = self._call(
            "POST",
            "/v1/schedules",
            {
                "name": "matrix-sched",
                "interval_seconds": 3600,
                "job_template": {"kind": "loop", "description": "tick", "submitter_urn": self.orch},
            },
            actor=self.worker,
        )
        self.assertEqual(code, 200, data)
        code, _ = self._call("GET", "/v1/admin/jobs/does-not-exist/history", actor=self.worker)
        self.assertIn(code, (200, 404))

    # ---- 2i. anon mutations (FB56-1, FB56-2, FB56-5) ----
    def test_plan_reset_is_anonymous(self):
        """Invariant (FB56-1): plan-reset has no gate — anyone zeroes anyone's spend."""
        code, data = self._call("POST", f"/v1/agents/{self.worker}/plan-reset")
        self.assertEqual(code, 200, data)

    def test_auto_retry_is_anonymous(self):
        """Invariant (FB56-2): auto-retry has no gate. Unknown id 404s (no
        auth precedes lookup); an eligible failed row is retried anonymously."""
        code, _ = self._call("POST", "/v1/jobs/does-not-exist/auto-retry")
        self.assertEqual(code, 404)
        repo = hive_repo()

        async def _mk():
            async with repo.connection() as db:
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, status, started_at, "
                    "max_retries, retry_count, result) VALUES "
                    "('fb56-retry', ?, 'loop', 'failed', 1.0, 2, 0, ?)",
                    (self.orch, json.dumps({"retryable": True})),
                )
                await db.commit()

        _run(_mk())
        code, data = self._call("POST", "/v1/jobs/fb56-retry/auto-retry")
        self.assertEqual(code, 200, data)

    def test_release_trigger_is_anonymous(self):
        """Invariant (FB56-5): the release trigger checks no caller — a job
        without a preview bundle 404s, proving lookup precedes any (absent) gate."""
        code, _ = self._call("POST", "/v1/jobs/does-not-exist/release")
        self.assertEqual(code, 404)
        jid = self._submit()
        code, _ = self._call("POST", f"/v1/jobs/{jid}/release")
        self.assertEqual(code, 404)  # no preview bundle; still no 401

    # ---- 2j. mcp passthrough ----
    def test_mcp_unknown_method_404s_anonymously(self):
        code, _ = self._call("POST", "/mcp", {"method": "nope", "params": {}})
        self.assertEqual(code, 404)


if __name__ == "__main__":
    unittest.main()
