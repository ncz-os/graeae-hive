"""The lifecycle doc stays honest: parsed table vs the real state machine.

(a) every status in the code's status set appears in the doc (transition
table, status list, or findings);
(b) every transition the doc lists is in the real
``STATUS_TRANSITIONS`` map, a sample is also exercised against the real
PATCH endpoint in-process, and sampled transitions NOT listed are
refused by both the map and the endpoint.
(c) the FULL matrix: every (from-status, to-status) pair of the valid
statuses is driven through the real PATCH endpoint with an authorised
actor, accepted exactly where the doc table lists the pair and refused
elsewhere. Genuine code-vs-doc disagreements stay pinned as
``xfail(strict=True)`` with a finding id; nothing else may diverge.
"""

from __future__ import annotations

import asyncio
import os
import re
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

import pytest

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


MATRIX_STATUSES = sorted(_deps.VALID_JOB_STATUSES)

#: The doc's PATCH table as a set of accepted (from, to) pairs.
DOC_ACCEPTED = {(src, dst) for src, dst, _ in parse_table(DOC.read_text())}

#: Commit evidence that satisfies valid_completion_proof for commit-requiring
#: kinds (fabricated SHAs: the gate checks shape, not history).
VALID_PROOF = {
    "head_sha": "a" * 40,
    "commits": ["a" * 40],
    "files_changed": ["matrix.py"],
    "worktree_clean": True,
}

#: Genuine code-vs-doc disagreements. A pair listed here must keep diverging
#: (pinned as xfail) until code and doc agree; a pair NOT listed here must
#: behave exactly as the doc table says.
MATRIX_DIVERGENCES: dict[tuple[str, str], str] = {
    ("offered", "queued"): (
        "FB35-FINDING-1: doc ACCEPTS, endpoint 409s unclaimed offered rows "
        "('cannot release a job with no current claimant'); the cell holds only "
        "for a claimant-bearing offered row, which no endpoint in this tree "
        "produces (nothing assigns offered with a claimant)"
    ),
}


def _matrix_marks(frm: str, to: str):
    reason = MATRIX_DIVERGENCES.get((frm, to))
    if reason is None:
        return ()
    return pytest.mark.xfail(strict=True, reason=reason)


MATRIX_PARAMS = [
    pytest.param(frm, to, marks=_matrix_marks(frm, to))
    for frm in MATRIX_STATUSES
    for to in MATRIX_STATUSES
]
MATRIX_IDS = [f"{frm}->{to}" for frm in MATRIX_STATUSES for to in MATRIX_STATUSES]


class MatrixHarness:
    """One fresh temp-sqlite bus per matrix pair, with real endpoints."""

    def __init__(self):
        self.worker = ""
        self.submitter = ""
        self.kinds: dict[str, str] = {}

    def request(self, method, path, body=None, actor=None):
        async def _do():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t"
            ) as c:
                headers = {"X-Hive-Actor": actor} if actor and method in ("POST", "PATCH", "PUT", "DELETE") else None
                resp = await c.request(method, path, json=body, headers=headers)
                try:
                    return resp.status_code, resp.json()
                except Exception:
                    return resp.status_code, resp.text

        return _run(_do())

    def register(self, payload):
        code, data = self.request("POST", "/v1/agents/register", payload)
        assert code == 200, data
        return data["urn"]

    def submit(self, frm: str, to: str, kind: str = "e2e:x") -> str:
        code, data = self.request(
            "POST",
            "/v1/jobs",
            {
                "kind": kind,
                "description": f"LIFECYCLE MATRIX {frm}->{to} {uuid.uuid4().hex}",
                "submitter_urn": self.submitter,
                "priority": 200,
            },
            actor=self.worker,
        )
        assert code in (200, 201), data
        jid = data["id"]
        self.kinds[jid] = kind
        return jid

    def claim(self, jid: str) -> None:
        code, data = self.request("POST", f"/v1/jobs/{jid}/claim?by={self.worker}", None, actor=self.worker)
        assert code == 200, data

    def direct(self, jid: str, status: str) -> None:
        async def _do():
            async with hive_repo().connection() as tx:
                await tx.execute("UPDATE jobs SET status=? WHERE id=?", (status, jid))
                await tx.commit()

        _run(_do())

    def status(self, jid: str) -> str:
        code, job = self.request("GET", f"/v1/jobs/{jid}")
        assert code == 200, job
        return job["status"]

    def stage(self, frm: str, to: str) -> str:
        """A fresh job sitting in ``frm``; raises loudly if the setup path broke."""
        if frm in ("dead-letter", "failed_completion"):
            # No endpoint stores these; stage the row directly (same technique
            # as test_lifecycle_findings_20261029) and PATCH through the
            # endpoint under test.
            jid = self.submit(frm, to)
            self.direct(jid, frm)
        elif frm == "offered":
            # Nothing in this tree assigns offered; offered rows arrive
            # unclaimed from outside the bus.
            jid = self.submit(frm, to)
            self.direct(jid, "offered")
        else:
            kind = "fix:matrix-nr" if frm == "needs-review" else "e2e:x"
            jid = self.submit(frm, to, kind=kind)
            if frm == "queued":
                pass
            elif frm == "claimed":
                self.claim(jid)
            elif frm == "running":
                self.claim(jid)
                code, data = self.request(
                    "PATCH", f"/v1/jobs/{jid}", {"status": "running", "claimed_by": self.worker}, actor=self.worker
                )
                assert code == 200, data
            elif frm == "needs-review":
                # Commit-requiring kind + empty proof parks the row (409 is
                # the gate's answer; the row is what matters).
                self.claim(jid)
                self.request("PATCH", f"/v1/jobs/{jid}", {"status": "done", "claimed_by": self.worker}, actor=self.worker)
            elif frm == "done":
                self.claim(jid)
                code, data = self.request(
                    "PATCH", f"/v1/jobs/{jid}", {"status": "done", "claimed_by": self.worker, "result": {}}, actor=self.worker
                )
                assert code == 200, data
            elif frm == "failed":
                self.claim(jid)
                code, data = self.request(
                    "PATCH",
                    f"/v1/jobs/{jid}",
                    {"status": "failed", "claimed_by": self.worker, "result": {"retryable": False}},
                    actor=self.worker,
                )
                assert code == 200, data
            elif frm == "cancelled":
                self.claim(jid)
                code, data = self.request(
                    "PATCH", f"/v1/jobs/{jid}", {"status": "cancelled", "claimed_by": self.worker}, actor=self.worker
                )
                assert code == 200, data
            else:  # pragma: no cover - MATRIX_STATUSES is the closed set
                raise AssertionError(f"no staging path for {frm!r}")
        got = self.status(jid)
        assert got == frm, f"staging broke: wanted from={frm!r}, row is {got!r}"
        return jid


@pytest.fixture
def matrix_harness(tmp_path, monkeypatch):
    monkeypatch.setitem(os.environ, "AGENT_BUS_DB", str(tmp_path / "matrix.db"))
    monkeypatch.setitem(os.environ, "HIVE_BACKEND", "sqlite")
    for var in ("MNEMOS_URL", "MNEMOS_TOKEN", "HIVE_SHARED_SECRET", "HIVE_BUS_ADMIN_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(_deps, "_HIVE_REPO", None)
    _run(hive_repo().init())
    harness = MatrixHarness()
    harness.submitter = harness.register(
        {"kind": "opencode", "host": "mx", "runtime": "opencode",
         "status": "online", "capabilities": ["code", "orchestrate"],
         "cost_tier": "C", "auth_method": "api"}
    )
    harness.worker = harness.register(
        {"kind": "zeroclaw", "host": "mx", "runtime": "zeroclaw",
         "status": "online", "capabilities": ["code"],
         "cost_tier": "B", "auth_method": "api"}
    )
    yield harness


def test_matrix_covers_every_valid_pair():
    assert len(MATRIX_STATUSES) == 10, MATRIX_STATUSES
    assert len(MATRIX_PARAMS) == 100, len(MATRIX_PARAMS)
    assert len(DOC_ACCEPTED) == 29, sorted(DOC_ACCEPTED)


@pytest.mark.parametrize("frm,to", MATRIX_PARAMS, ids=MATRIX_IDS)
def test_lifecycle_matrix_pair(matrix_harness, frm, to):
    """One cell of the FULL lifecycle matrix through the real PATCH endpoint."""
    harness = matrix_harness
    jid = harness.stage(frm, to)
    body: dict = {"status": to, "claimed_by": harness.worker}
    if to == "done" and harness.kinds[jid].startswith("fix:"):
        body["result"] = dict(VALID_PROOF)
    elif to == "done":
        body["result"] = {}
    elif to == "failed":
        body["result"] = {"retryable": False}
    code, _ = harness.request("PATCH", f"/v1/jobs/{jid}", body, actor=harness.worker)
    final = harness.status(jid)
    if (frm, to) in DOC_ACCEPTED:
        assert code in (200, 201) and final == to, (
            f"pair {frm!r} -> {to!r}: doc ACCEPTS but PATCH answered {code} "
            f"and the row is {final!r} (wanted {to!r})"
        )
    else:
        assert code not in (200, 201) and final == frm, (
            f"pair {frm!r} -> {to!r}: doc REFUSES but PATCH answered {code} "
            f"and the row is {final!r} (wanted unchanged {frm!r})"
        )
