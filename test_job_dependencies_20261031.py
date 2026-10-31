"""FB54: dependency graph semantics — the doc table executed against the real code.

``docs/JOB_DEPENDENCIES.md`` states the contract (sections 1–8) with
file:line citations. Its section-5 status table is normative and
machine-readable: ``DependsOnTableTests`` parses the rows between the
``FB54-STATUS-TABLE`` markers and executes each one against a temp
sqlite repository through the real paths (``claim_job``'s
in-transaction gate for blocked/claimable rows; the same
status-write-plus-``release_dependencies`` transaction the routers run
for dead rows; the claim-time purge drop for ``purged``).

The companion classes pin what the table cannot tabulate: submit-time
cycle prevention (``CycleTests``), progressive unblocking along chains
and diamonds (``ChainTests``), and the needs-review quarantine
(``NeedsReviewDepTests``) — including the open finding FB54-1 that no
watchdog exists for permanently unsatisfiable edges.

Repo style follows ``test_job_prefs_deps_20261017`` /
``test_dependency_release_20261027`` (direct ``SqliteHiveRepository``
on a temp file); the two submit-validation tests use the in-process app
(``httpx.ASGITransport``) like ``test_api_contract_20261029``.
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus import deps as _deps  # noqa: E402
from hive.bus.deps import release_dependencies  # noqa: E402
from hive.bus.schemas import JobCreate, JobUpdate  # noqa: E402
from hive.persistence import sqlite as sqlite_mod  # noqa: E402
from hive.persistence.sqlite import DependencyNotReady  # noqa: E402

DOC_PATH = Path(__file__).resolve().parent / "docs" / "JOB_DEPENDENCIES.md"

TABLE_START = "<!-- FB54-STATUS-TABLE-START -->"
TABLE_END = "<!-- FB54-STATUS-TABLE-END -->"


def parse_status_table():
    """Read the normative table from the doc; return [(dep_status, outcome, edge)]."""
    text = DOC_PATH.read_text()
    body = text.split(TABLE_START, 1)[1].split(TABLE_END, 1)[0]
    rows = []
    for line in body.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) != 3 or set(cells) == {"---"}:
            continue
        if cells[0].lower() == "dep status":
            continue
        status = cells[0].strip("`")
        rows.append((status, cells[1], cells[2]))
    return rows


def _run(coro):
    return asyncio.run(coro)


class _RepoCase(unittest.IsolatedAsyncioTestCase):
    async def _repo(self, name="dep-sem.db"):
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        repo = sqlite_mod.SqliteHiveRepository(str(Path(td.name) / name))
        await repo.init(agent_bus.SCHEMA)
        return repo

    async def _mk(self, repo, job_id, *, status="queued", deps=None, claimed_by=None):
        now = time.time()
        async with repo.connection() as db:
            await db.execute(
                "INSERT INTO jobs (id, submitter_urn, kind, status, started_at, "
                "depends_on, claimed_by) VALUES (?, 'urn:submitter', 'build', ?, ?, ?, ?)",
                (job_id, status, now, json.dumps(deps) if deps else None, claimed_by),
            )
            await db.commit()

    async def _agent(self, repo, urn="urn:agent:zeroclaw:host:depsem"):
        now = time.time()
        async with repo.connection() as db:
            await db.execute(
                "INSERT INTO agents (urn, kind, host, session_id, capabilities, started_at, "
                "last_heartbeat, status, runtime, cost_tier) "
                "VALUES (?, 'zeroclaw', 'host', 'session', '[]', ?, ?, 'online', 'zeroclaw', 'B')",
                (urn, now, now),
            )
            await db.commit()
        return urn

    async def _claim(self, repo, job_id, urn):
        """Attempt a real claim; return ('claimed', rc) or ('blocked', None)."""
        now = time.time()
        try:
            async with repo.connection() as tx:
                rc = await repo.claim_job(
                    tx,
                    job_id=job_id,
                    agent_urn=urn,
                    now=now,
                    lease_expires_at=now + 60,
                    runtime="zeroclaw",
                    model="qwen38",
                    provider="local",
                    cost_tier="B",
                )
                await tx.commit()
        except DependencyNotReady:
            return ("blocked", None)
        return ("claimed", rc)

    async def _edge(self, repo, job_id):
        async with repo.connection() as tx:
            async with tx.execute("SELECT depends_on FROM jobs WHERE id=?", (job_id,)) as cur:
                row = await cur.fetchone()
        raw = row[0] if row else None
        return json.loads(raw) if raw else None

    async def _kill(self, repo, dep_id, *, dead_status):
        """Mirror the router: status write + release in ONE transaction."""
        async with repo.connection() as tx:
            await tx.execute("UPDATE jobs SET status=? WHERE id=?", (dead_status, dep_id))
            released = await release_dependencies(
                tx,
                dep_id,
                dead_status,
                emit_event=_noop_emit,
            )
            await tx.commit()
        return released


async def _noop_emit(db, kind, payload, *, commit=True):
    return None


NON_DEAD = {"queued", "offered", "claimed", "running", "needs-review", "done"}
DEAD = {"failed", "failed_completion", "dead-letter", "cancelled"}


class DependsOnTableTests(_RepoCase):
    """Every row of the doc's status table, executed against the real code."""

    def test_doc_table_has_the_eleven_expected_rows(self):
        """Invariant: the table the tests below execute is complete — no silent row loss."""
        rows = parse_status_table()
        self.assertEqual(
            [s for s, _, _ in rows],
            [
                "queued",
                "offered",
                "claimed",
                "running",
                "needs-review",
                "done",
                "failed",
                "failed_completion",
                "dead-letter",
                "cancelled",
                "purged",
            ],
        )

    async def test_live_dep_statuses_block_or_satisfy_with_edge_kept(self):
        """Invariant: a non-dead dep never strips the edge; only `done` lets the claim through."""
        for dep_status in sorted(NON_DEAD):
            with self.subTest(dep_status=dep_status):
                repo = await self._repo(name=f"live-{dep_status}.db")
                urn = await self._agent(repo)
                await self._mk(
                    repo, "dep", status=dep_status, claimed_by=(urn if dep_status in ("offered", "claimed") else None)
                )
                await self._mk(repo, "dependent", deps=["dep"])
                outcome, _rc = await self._claim(repo, "dependent", urn)
                # _claim reports "claimed" for a successful claim, i.e. the doc's "claimable".
                expected = "claimed" if dep_status == "done" else "blocked"
                self.assertEqual(outcome, expected, f"dep {dep_status}")
                self.assertEqual(await self._edge(repo, "dependent"), ["dep"], f"dep {dep_status}: edge must be kept")

    async def test_dead_dep_statuses_release_and_unblock(self):
        """Invariant: entering a dead status strips the id (same-tx release) and the dependent claims."""
        for dep_status in sorted(DEAD):
            with self.subTest(dep_status=dep_status):
                repo = await self._repo(name=f"dead-{dep_status}.db")
                urn = await self._agent(repo)
                await self._mk(repo, "dep", status="claimed", claimed_by=urn)
                await self._mk(repo, "dependent", deps=["dep"])
                released = await self._kill(repo, "dep", dead_status=dep_status)
                self.assertEqual(len(released), 1)
                self.assertEqual(released[0]["dependent_id"], "dependent")
                self.assertEqual(released[0]["failed_dep_id"], "dep")
                self.assertEqual(released[0]["dep_status"], dep_status)
                self.assertEqual(released[0]["remaining"], [])
                self.assertIsNone(
                    await self._edge(repo, "dependent"), "emptied edge must become NULL (canonical no-deps)"
                )
                outcome, rc = await self._claim(repo, "dependent", urn)
                self.assertEqual(outcome, "claimed")
                self.assertEqual(rc, 1)

    async def test_purged_dep_releases_with_purged_status(self):
        """Invariant: a purged dep row releases its dependents with dep_status='purged'."""
        repo = await self._repo(name="purged.db")
        urn = await self._agent(repo)
        await self._mk(repo, "dep", status="failed")
        await self._mk(repo, "dependent", deps=["dep"])
        async with repo.connection() as tx:
            await tx.execute("DELETE FROM jobs WHERE id='dep'")
            released = await release_dependencies(tx, "dep", "purged", emit_event=_noop_emit)
            await tx.commit()
        self.assertEqual(len(released), 1)
        self.assertEqual(released[0]["dep_status"], "purged")
        self.assertIsNone(await self._edge(repo, "dependent"))
        outcome, rc = await self._claim(repo, "dependent", urn)
        self.assertEqual((outcome, rc), ("claimed", 1))

    async def test_preexisting_purge_is_dropped_at_claim_time(self):
        """Invariant: a purge that predates the release path cannot block forever (claim-time drop)."""
        repo = await self._repo(name="stale-purge.db")
        urn = await self._agent(repo)
        await self._mk(repo, "dep", status="failed")
        await self._mk(repo, "dependent", deps=["dep"])
        async with repo.connection() as db:
            await db.execute("DELETE FROM jobs WHERE id='dep'")
            await db.commit()
        # No release ran for this delete — the claim path must drop the dead id itself.
        outcome, rc = await self._claim(repo, "dependent", urn)
        self.assertEqual((outcome, rc), ("claimed", 1))
        self.assertIsNone(await self._edge(repo, "dependent"))


class CycleTests(_RepoCase):
    def test_submit_refuses_unknown_dependency_ids(self):
        """Invariant: an edge can only name a job that already exists — dangling edges are unsubmittable."""
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        with (
            mock.patch.dict(os.environ, {"AGENT_BUS_DB": os.path.join(td.name, "c.db"), "HIVE_BACKEND": "sqlite"}),
            mock.patch.object(_deps, "_HIVE_REPO", None),
        ):
            for var in ("MNEMOS_URL", "MNEMOS_TOKEN", "HIVE_SHARED_SECRET", "HIVE_BUS_ADMIN_TOKEN"):
                os.environ.pop(var, None)
            _run(_deps.hive_repo().init())

            async def _do():
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t"
                ) as c:
                    r = await c.post(
                        "/v1/agents/register",
                        json={
                            "kind": "opencode",
                            "host": "ctr",
                            "runtime": "opencode",
                            "status": "online",
                            "capabilities": ["code"],
                            "cost_tier": "C",
                        },
                    )
                    urn = r.json()["urn"]
                    bad = await c.post(
                        "/v1/jobs",
                        json={
                            "kind": "loop",
                            "description": "bad dep",
                            "submitter_urn": urn,
                            "depends_on": ["job-that-does-not-exist"],
                        },
                    )
                    good = await c.post("/v1/jobs", json={"kind": "loop", "description": "base", "submitter_urn": urn})
                    gid = good.json()["id"]
                    chained = await c.post(
                        "/v1/jobs",
                        json={"kind": "loop", "description": "chained", "submitter_urn": urn, "depends_on": [gid]},
                    )
                    got = await c.get(f"/v1/jobs/{chained.json()['id']}")
                    return bad.status_code, bad.json(), chained.status_code, got.json()

            bad_code, bad_body, ok_code, ok_body = _run(_do())
        self.assertEqual(bad_code, 422)
        self.assertIn("unknown job ids", json.dumps(bad_body))
        self.assertIn(ok_code, (200, 201))
        self.assertEqual(ok_body["depends_on"], [ok_body["depends_on"][0]])

    def test_no_update_path_accepts_depends_on(self):
        """Invariant: edges are write-once — JobUpdate has no depends_on field, so no
        PATCH can add the second edge of a cycle. (JobCreate has no id field either,
        so a self-edge cannot even be expressed: the id is server-generated.)"""
        self.assertNotIn("depends_on", JobUpdate.model_fields)
        self.assertNotIn("id", JobCreate.model_fields)

    async def test_self_edge_blocks_forever(self):
        """Invariant: a self-edge (only constructible below the API) can never satisfy the gate."""
        repo = await self._repo(name="self-cycle.db")
        urn = await self._agent(repo)
        await self._mk(repo, "selfish", deps=["selfish"])
        self.assertFalse(await self._satis(repo, "selfish"))
        outcome, _ = await self._claim(repo, "selfish", urn)
        self.assertEqual(outcome, "blocked")

    async def test_two_cycle_blocks_both_sides_forever(self):
        """Invariant (FB54-1 steady state): A<->B below the API blocks both; nothing
        deadlocks the bus itself — the escape is purge/cancel, i.e. the release paths."""
        repo = await self._repo(name="two-cycle.db")
        urn = await self._agent(repo)
        await self._mk(repo, "a", deps=["b"])
        await self._mk(repo, "b", deps=["a"])
        self.assertFalse(await self._satis(repo, "a"))
        self.assertFalse(await self._satis(repo, "b"))
        self.assertEqual((await self._claim(repo, "a", urn))[0], "blocked")
        self.assertEqual((await self._claim(repo, "b", urn))[0], "blocked")
        # Escape hatch: cancelling one side releases the other as `cancelled`.
        await self._kill(repo, "b", dead_status="cancelled")
        self.assertEqual(await self._edge(repo, "a"), None)
        self.assertEqual((await self._claim(repo, "a", urn))[0], "claimed")

    async def _satis(self, repo, job_id):
        async with repo.connection() as tx:
            return await repo.dependencies_satisfied(tx, job_id)


class ChainTests(_RepoCase):
    async def test_chain_unblocks_progressively_in_order(self):
        """Invariant: C on B on A — only the head claims; completions propagate one link at a time."""
        repo = await self._repo(name="chain.db")
        urn = await self._agent(repo)
        await self._mk(repo, "a")
        await self._mk(repo, "b", deps=["a"])
        await self._mk(repo, "c", deps=["b"])
        self.assertEqual((await self._claim(repo, "b", urn))[0], "blocked")
        self.assertEqual((await self._claim(repo, "c", urn))[0], "blocked")
        self.assertEqual((await self._claim(repo, "a", urn))[0], "claimed")
        await self._complete(repo, "a")
        self.assertEqual((await self._claim(repo, "b", urn))[0], "claimed")
        # Out-of-order attempt changes nothing: C still waits on B (claimed, not done).
        self.assertEqual((await self._claim(repo, "c", urn))[0], "blocked")
        await self._complete(repo, "b")
        self.assertEqual((await self._claim(repo, "c", urn))[0], "claimed")
        # Provenance: completed deps keep their edges.
        self.assertEqual(await self._edge(repo, "c"), ["b"])

    async def test_diamond_needs_both_parents_and_survives_one_death(self):
        """Invariant: D on [B, C] (both on A) — one parent's death strips just that id; D still waits on the other."""
        repo = await self._repo(name="diamond.db")
        urn = await self._agent(repo)
        await self._mk(repo, "a")
        await self._mk(repo, "b", deps=["a"])
        await self._mk(repo, "c", deps=["a"])
        await self._mk(repo, "d", deps=["b", "c"])
        await self._kill(repo, "b", dead_status="failed")
        self.assertEqual(await self._edge(repo, "d"), ["c"])
        self.assertEqual((await self._claim(repo, "d", urn))[0], "blocked")
        await self._complete(repo, "a")
        await self._complete(repo, "c")
        self.assertEqual((await self._claim(repo, "d", urn))[0], "claimed")

    async def _complete(self, repo, job_id):
        async with repo.connection() as db:
            await db.execute("UPDATE jobs SET status='done' WHERE id=?", (job_id,))
            await db.commit()


class NeedsReviewDepTests(_RepoCase):
    async def test_needs_review_blocks_with_edge_kept_then_done_unblocks(self):
        """Invariant: needs-review is quarantine, not death — the dependent waits, then claims once the dep is done."""
        repo = await self._repo(name="nr.db")
        urn = await self._agent(repo)
        await self._mk(repo, "dep", status="needs-review")
        await self._mk(repo, "dependent", deps=["dep"])
        self.assertEqual((await self._claim(repo, "dependent", urn))[0], "blocked")
        self.assertEqual(await self._edge(repo, "dependent"), ["dep"])
        await self._complete(repo, "dep")
        self.assertEqual((await self._claim(repo, "dependent", urn))[0], "claimed")
        self.assertEqual(await self._edge(repo, "dependent"), ["dep"])

    async def test_needs_review_then_failed_releases(self):
        """Invariant: a quarantined dep that later dies releases normally."""
        repo = await self._repo(name="nr-death.db")
        urn = await self._agent(repo)
        await self._mk(repo, "dep", status="needs-review")
        await self._mk(repo, "dependent", deps=["dep"])
        await self._kill(repo, "dep", dead_status="failed")
        self.assertIsNone(await self._edge(repo, "dependent"))
        self.assertEqual((await self._claim(repo, "dependent", urn))[0], "claimed")

    async def _complete(self, repo, job_id):
        async with repo.connection() as db:
            await db.execute("UPDATE jobs SET status='done' WHERE id=?", (job_id,))
            await db.commit()


if __name__ == "__main__":
    unittest.main()
