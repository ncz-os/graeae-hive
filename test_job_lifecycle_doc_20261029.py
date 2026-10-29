"""The lifecycle doc stays honest: parsed table vs the real state machine.

(a) every status in the code's status set appears in the doc (transition
table, status list, or findings);
(b) every transition the doc lists is in the real
``STATUS_TRANSITIONS`` map, a sample is also exercised against the real
PATCH endpoint in-process, and sampled transitions NOT listed are
refused by both the map and the endpoint.
"""

from __future__ import annotations

import asyncio
import os
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus import deps as _deps  # noqa: E402
from hive.bus.deps import hive_repo  # noqa: E402
from hive.bus.domain.jobs import STATUS_TRANSITIONS  # noqa: E402

DOC = Path(__file__).parent / "docs" / "JOB_LIFECYCLE.md"


def _run(coro):
    return asyncio.run(coro)


def parse_table(text: str) -> list[tuple[str, str, str]]:
    lines = text.splitlines()
    start = next(i for i, line in enumerate(lines) if re.match(r"\|\s*from\s*\|\s*to\s*\|\s*via\s*\|", line))
    rows = []
    for line in lines[start + 2 :]:
        if not line.strip().startswith("|"):
            break
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) == 3 and set(cells) != {"----", "--", "---"} and not set(cells) <= {"--", "---", "----"}:
            rows.append((cells[0], cells[1], cells[2]))
    return rows


class LifecycleDocTests(unittest.TestCase):
    def test_every_status_appears_in_doc(self):
        text = DOC.read_text()
        rows = parse_table(text)
        in_table = {s for row in rows for s in row[:2]}
        for status in sorted(_deps.VALID_JOB_STATUSES):
            self.assertIn(status, text, f"status {status!r} appears nowhere in the doc")
            if status not in in_table:
                self.assertIn("Findings", text)
                self.assertIn(status, text.split("Findings", 1)[1])

    def test_every_listed_transition_is_in_the_map(self):
        rows = parse_table(self._doc_text())
        for src, dst, _via in rows:
            self.assertIn(
                dst, STATUS_TRANSITIONS.get(src, set()), f"doc lists {src!r} -> {dst!r} but the map refuses it"
            )

    def test_map_has_no_undocumented_patch_transition(self):
        documented = {(src, dst) for src, dst, _ in parse_table(self._doc_text())}
        for src, targets in STATUS_TRANSITIONS.items():
            for dst in targets:
                self.assertIn((src, dst), documented, f"map allows {src!r} -> {dst!r} but the doc omits it")

    def test_sampled_unlisted_transitions_are_refused(self):
        refused = [
            ("done", "queued"),
            ("done", "failed"),
            ("failed", "queued"),
            ("failed", "done"),
            ("cancelled", "done"),
            ("cancelled", "queued"),
            ("dead-letter", "cancelled"),
            ("needs-review", "queued"),
            ("queued", "needs-review"),
            ("running", "claimed"),
            ("offered", "needs-review"),
        ]
        documented = {(src, dst) for src, dst, _ in parse_table(self._doc_text())}
        for src, dst in refused:
            self.assertNotIn((src, dst), documented)
            self.assertNotIn(dst, STATUS_TRANSITIONS.get(src, set()), f"map allows {src!r} -> {dst!r}")

    @staticmethod
    def _doc_text() -> str:
        return DOC.read_text()


class LifecycleEndpointTests(unittest.TestCase):
    """Spot-check the table against the real PATCH endpoint in-process."""

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        env = mock.patch.dict(
            os.environ, {"AGENT_BUS_DB": str(Path(self._td.name) / "e2e.db"), "HIVE_BACKEND": "sqlite"}
        )
        env.start()
        self.addCleanup(env.stop)
        repo = mock.patch.object(_deps, "_HIVE_REPO", None)
        repo.start()
        self.addCleanup(repo.stop)
        _run(hive_repo().init())
        self.submitter = self._register(
            {
                "kind": "opencode",
                "host": "e2e",
                "runtime": "opencode",
                "status": "online",
                "capabilities": ["code", "orchestrate"],
                "cost_tier": "C",
                "auth_method": "api",
            }
        )
        self.worker = self._register(
            {
                "kind": "zeroclaw",
                "host": "e2e",
                "runtime": "zeroclaw",
                "status": "online",
                "capabilities": ["code"],
                "cost_tier": "B",
                "auth_method": "api",
            }
        )

    def _request(self, method, path, body=None, actor=None):
        async def _do():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t") as c:
                headers = {"X-Hive-Actor": actor} if actor and method in ("POST", "PATCH", "PUT", "DELETE") else None
                resp = await c.request(method, path, json=body, headers=headers)
                try:
                    return resp.status_code, resp.json()
                except Exception:
                    return resp.status_code, resp.text

        return _run(_do())

    def _register(self, payload):
        code, data = self._request("POST", "/v1/agents/register", payload)
        self.assertEqual(code, 200, data)
        return data["urn"]

    def _submit(self, kind="e2e:x", priority=200):
        code, data = self._request(
            "POST",
            "/v1/jobs",
            {
                "kind": kind,
                "description": "LIFECYCLE DOC SPOT CHECK",
                "submitter_urn": self.submitter,
                "priority": priority,
            },
            actor=self.worker,
        )
        self.assertIn(code, (200, 201), data)
        return data["id"]

    def _claim(self, jid):
        code, data = self._request("POST", f"/v1/jobs/next?agent_urn={self.worker}", None, actor=self.worker)
        self.assertEqual(code, 200, data)
        self.assertEqual(data["id"], jid)

    def test_endpoint_accepts_and_refuses_per_table(self):
        # queued -> claimed via PATCH is refused (claim bypass); via claim endpoint works.
        jid = self._submit()
        code, _ = self._request(
            "PATCH", f"/v1/jobs/{jid}", {"status": "claimed", "claimed_by": self.worker}, actor=self.worker
        )
        self.assertEqual(code, 409)
        self._claim(jid)
        # claimed -> failed is accepted and stays failed with retryable False.
        code, _ = self._request(
            "PATCH",
            f"/v1/jobs/{jid}",
            {"status": "failed", "claimed_by": self.worker, "result": {"retryable": False}},
            actor=self.worker,
        )
        self.assertEqual(code, 200)
        code, job = self._request("GET", f"/v1/jobs/{jid}")
        self.assertEqual(job["status"], "failed")
        # failed -> queued via PATCH is refused (admin requeue path owns it).
        code, _ = self._request(
            "PATCH", f"/v1/jobs/{jid}", {"status": "queued", "claimed_by": self.worker}, actor=self.worker
        )
        self.assertEqual(code, 409)
        # needs-review -> cancelled is accepted (row keeps its claimant via the
        # completion gate: kind fix:* requires commit proof, empty result parks
        # it as needs-review with the claimant attached).
        jid_nr = self._submit(kind="fix:doc-check")
        self._claim(jid_nr)
        code, _ = self._request(
            "PATCH",
            f"/v1/jobs/{jid_nr}",
            {"status": "done", "claimed_by": self.worker, "result": {}},
            actor=self.worker,
        )
        self.assertEqual(code, 409)  # completion gate: row parked, answer 409
        code, job_nr = self._request("GET", f"/v1/jobs/{jid_nr}")
        self.assertEqual(job_nr["status"], "needs-review")
        code, _ = self._request(
            "PATCH", f"/v1/jobs/{jid_nr}", {"status": "cancelled", "claimed_by": self.worker}, actor=self.worker
        )
        self.assertEqual(code, 200)
        # cancelled -> queued via PATCH is refused.
        code, _ = self._request("PATCH", f"/v1/jobs/{jid_nr}", {"status": "queued"}, actor=self.worker)
        self.assertEqual(code, 409)
        # done -> queued via PATCH is refused.
        jid2 = self._submit()
        self._claim(jid2)
        code, _ = self._request(
            "PATCH",
            f"/v1/jobs/{jid2}",
            {"status": "done", "claimed_by": self.worker, "result": {"note": "no commit needed"}},
            actor=self.worker,
        )
        self.assertEqual(code, 200)
        code, _ = self._request(
            "PATCH", f"/v1/jobs/{jid2}", {"status": "queued", "claimed_by": self.worker}, actor=self.worker
        )
        self.assertEqual(code, 409)
