"""Round-trip + worker-read of the optional ``base_branch`` column.

Bus lineage graeae-hive @ aad39cd (pre-fix) silently DROPPED ``base_branch``
on POST /v1/jobs (proved by job 01a15ffa-bf19-71d8-8280-5fbb21689e09: sent
base_branch, stored null). The canonical lineage
(deploy/hive-canonical-20261003 @ 4d4fc85) carries the column on its
``JobCreate`` / ``JobOut`` / INSERTs / SELECTs and the matching
``hive_worker.py`` reads it as ``job.get("base_branch")`` when it cannot find
``head_branch`` upstream (cloning base_branch -> creating head_branch
locally).

This test class pins the round-trip on the LIVE-LIKE sqlite path (full ASGI
app) and the storage-shape parity with the worker's claim key:

  * POST /v1/jobs with ``base_branch`` round-trips to GET /v1/jobs/{id}
    and to GET /v1/jobs (list endpoint).
  * The /v1/jobs/next claimed payload includes ``base_branch`` under
    EXACTLY the key ``base_branch`` (the worker reads
    ``job.get("base_branch")`` -- the key name MUST match).
  * An OLD client that omits ``base_branch`` keeps working: the column is
    optional + nullable, and its absence shows up as ``None`` end-to-end.
  * The Oracle migration adds the column idempotently on a legacy
    HIVE_JOBS without one and is verified by the existing
    ``test_oracle_populated_core_schema_is_converged_and_verified``
    gateway (which uses ``_ORACLE_JOB_ADDITIONS``).
"""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")
os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")
os.environ.setdefault("HIVE_BUS_ADMIN_TOKEN", "test-admin-token")

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus.deps import hive_repo as _default_hive_repo  # noqa: E402
from hive.bus.schemas import JobCreate, JobOut  # noqa: E402
from hive.persistence.oracle import (  # noqa: E402
    _ORACLE_JOB_ADDITIONS,
    OracleHiveRepository,
)

SUBMITTER = "urn:agent:opencode:studio:s1"
WORKER = "urn:agent:zeroclaw:studio:w1"


class BaseBranchRoundTripTests(unittest.IsolatedAsyncioTestCase):
    """Full ASGI app against a temp sqlite DB (no orchestrator drift)."""

    async def asyncSetUp(self):
        self._td = tempfile.TemporaryDirectory()
        os.environ["AGENT_BUS_DB"] = os.path.join(self._td.name, "base_branch.db")
        # Force the app/repo to re-read AGENT_BUS_DB on each test:
        await _default_hive_repo().init()
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=agent_bus.app),
            base_url="http://t",
        )

    async def asyncTearDown(self):
        await self.client.aclose()
        self._td.cleanup()

    async def _register(self, urn_kind: str, host: str) -> str:
        runtime = "opencode" if urn_kind == "opencode" else "zeroclaw"
        r = await self.client.post(
            "/v1/agents/register",
            json={
                "kind": urn_kind,
                "host": host,
                "runtime": runtime,
                "status": "online",
                "capabilities": ["code"],
                "cost_tier": "B",
                "auth_method": "api",
            },
        )
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()["urn"]

    async def test_post_with_base_branch_round_trips_to_get_and_list(self):
        c = self.client
        submitter = await self._register("opencode", "studio")
        await self._register("zeroclaw", "studio")

        submit_resp = await c.post(
            "/v1/jobs",
            json={
                "kind": "code-edit",
                "description": "base-branch-roundtrip",
                "submitter_urn": submitter,
                "site": "internal",
                "repo": "org/repo",
                "head_branch": "fix/example",
                "base_branch": "main",
                "priority": 5,
            },
        )
        self.assertEqual(submit_resp.status_code, 200, submit_resp.text)
        jid = submit_resp.json()["id"]

        # GET /v1/jobs/{id} must round-trip the new column.
        got = (await c.get(f"/v1/jobs/{jid}")).json()
        self.assertEqual(got["head_branch"], "fix/example")
        self.assertEqual(got["base_branch"], "main")
        self.assertEqual(got["repo"], "org/repo")

        # GET /v1/jobs list endpoint must also include it.
        listed = (await c.get("/v1/jobs?limit=50")).json()["jobs"]
        row = next(j for j in listed if j["id"] == jid)
        self.assertEqual(row["base_branch"], "main")

        # GET /v1/jobs/{id}/preview is not applicable (no preview payload), but
        # we still need to confirm the SELECT behind it tolerates a missing
        # base_branch. PATCH to done with a minimal done-result and reuse:
        # we only assert the SELECT column list shape accepts the new column
        # via a repo-level read.
        async with _default_hive_repo().connection() as tx:
            async with tx.execute(
                "SELECT head_branch, base_branch, pr_number FROM jobs WHERE id=?",
                (jid,),
            ) as cur:
                row = await cur.fetchone()
        self.assertEqual(row[0], "fix/example")
        self.assertEqual(row[1], "main")
        self.assertIsNone(row[2])

    async def test_old_client_omitting_base_branch_still_works(self):
        """Backward compatibility: pre-fix submitters (no base_branch field)
        must keep accepting jobs and reading them as None."""
        c = self.client
        submitter = await self._register("opencode", "studio")

        submit_resp = await c.post(
            "/v1/jobs",
            json={
                "kind": "code-edit",
                "description": "no-base-branch",
                "submitter_urn": submitter,
                "site": "internal",
                "repo": "org/repo",
                "head_branch": "fix/example",
                "priority": 5,
            },
        )
        self.assertEqual(submit_resp.status_code, 200, submit_resp.text)
        jid = submit_resp.json()["id"]

        got = (await c.get(f"/v1/jobs/{jid}")).json()
        self.assertEqual(got["head_branch"], "fix/example")
        self.assertIsNone(got["base_branch"])

    async def test_claim_returns_base_branch_with_exact_key_worker_reads(self):
        """The /v1/jobs/next claimed payload must expose ``base_branch`` under
        EXACTLY the key the canonical worker reads
        (``job.get("base_branch")`` on hive_worker.py 4d4fc85).

        A worker that has never seen the field must still get a key it can
        index into directly.
        """
        c = self.client
        submitter = await self._register("opencode", "studio")
        worker = await self._register("zeroclaw", "studio")

        submit_resp = await c.post(
            "/v1/jobs",
            json={
                "kind": "code-edit",
                "description": "worker-read",
                "submitter_urn": submitter,
                "site": "internal",
                "repo": "org/repo",
                "head_branch": "fix/example",
                "base_branch": "main",
                "eligible_kinds": ["zeroclaw"],
                "priority": 5,
            },
        )
        self.assertEqual(submit_resp.status_code, 200, submit_resp.text)

        claim = await c.post(f"/v1/jobs/next?agent_urn={worker}")
        self.assertEqual(claim.status_code, 200, claim.text)
        job = claim.json()
        self.assertEqual(job["status"], "claimed")
        # The key name `base_branch` is THE contract with hive_worker.py.
        self.assertIn("base_branch", job)
        self.assertEqual(job["base_branch"], "main")
        self.assertEqual(job["head_branch"], "fix/example")

    async def test_claim_omits_base_branch_when_not_set(self):
        c = self.client
        submitter = await self._register("opencode", "studio")
        worker = await self._register("zeroclaw", "studio")

        await c.post(
            "/v1/jobs",
            json={
                "kind": "code-edit",
                "description": "worker-read-no-bb",
                "submitter_urn": submitter,
                "site": "internal",
                "repo": "org/repo",
                "head_branch": "fix/example",
                "eligible_kinds": ["zeroclaw"],
            },
        )
        claim = await c.post(f"/v1/jobs/next?agent_urn={worker}")
        self.assertEqual(claim.status_code, 200, claim.text)
        job = claim.json()
        # Still MUST be present as a key (None/null); the worker's
        # `job.get("base_branch")` calls must not crash.
        self.assertIn("base_branch", job)
        self.assertIsNone(job["base_branch"])


class BaseBranchSchemaTests(unittest.TestCase):
    """Schema-level pinning: the fields are DECLARED on JobCreate + JobOut
    so a future serializer regression that drops one fails here."""

    def test_job_create_declares_base_branch(self):
        fields_set = set(JobCreate.model_fields.keys())
        self.assertIn("base_branch", fields_set)

    def test_job_out_declares_base_branch(self):
        fields_set = set(JobOut.model_fields.keys())
        self.assertIn("base_branch", fields_set)

    def test_job_create_accepts_base_branch_round_trip(self):
        j = JobCreate(
            submitter_urn="urn:submitter",
            kind="code-edit",
            head_branch="fix/x",
            base_branch="main",
        )
        self.assertEqual(j.base_branch, "main")
        dumped = j.model_dump()
        self.assertEqual(dumped["base_branch"], "main")

    def test_job_create_base_branch_defaults_to_none(self):
        # Critical for backward compatibility -- existing callers that omit
        # the field must keep working.
        j = JobCreate(submitter_urn="urn:submitter", kind="code-edit")
        self.assertIsNone(j.base_branch)


class OracleBaseBranchMigrationTests(unittest.IsolatedAsyncioTestCase):
    """Verify the Oracle ALTER TABLE migration behaviour using the same
    fake-driver pattern the existing Oracle tests use (test_defect_fixes).

    The migration (oracle.py::OracleHiveRepository._apply_migrations) must
    add ``base_branch`` IF AND ONLY IF the live HIVE_JOBS is missing it --
    no DDL noise on a fresh schema, no crash on a legacy live HIVE_JOBS.
    """

    async def test_oracle_migration_adds_base_branch_if_missing(self):
        repo = OracleHiveRepository("oracle://user:pass@db/TEST")
        columns: dict = {"STATUS": ("VARCHAR2", 32)}
        added: list[str] = []

        async def fake_execute(_raw, sql, params=()):
            upper = sql.upper()
            if "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH" in upper:
                return [(name, data_type, length) for name, (data_type, length) in columns.items()]
            if "SELECT INDEX_NAME FROM USER_INDEXES" in upper:
                return []
            if "SELECT CONSTRAINT_NAME" in upper:
                # Two checks: one is a NOT-NULL, one is the status enum; both
                # are intentionally returned here so the run comes out clean.
                return [
                    ("CK_HIVE_JOBS_STATUS_RUNTIME", "status IN ('queued','done')"),
                    ("SYS_C_STATUS_NN", '"STATUS" IS NOT NULL'),
                ]
            if upper.startswith("ALTER TABLE HIVE_JOBS ADD ("):
                for name in _ORACLE_JOB_ADDITIONS:
                    if name in upper and name not in columns:
                        # Match the column mapping rule the live oracle test
                        # uses (CLOB / NUMBER / VARCHAR2 — see
                        # test_oracle_populated_core_schema_is_converged_and_verified)
                        if name in {
                            "RETRY_COUNT",
                            "MAX_RETRIES",
                            "RETRY_BACKOFF_UNTIL",
                            "LAST_UPDATE_AT",
                            "CLAIM_LEASE_EXPIRES_AT",
                            "DECLINE_COUNT",
                        }:
                            columns[name] = ("NUMBER", None)
                        elif name in {"REQUIRED_AUTONOMY", "PROJECT", "DEDUP_HASH", "BASE_BRANCH"}:
                            columns[name] = (
                                "VARCHAR2",
                                {"REQUIRED_AUTONOMY": 32, "PROJECT": 120, "DEDUP_HASH": 64, "BASE_BRANCH": 255}[name],
                            )
                        else:
                            columns[name] = ("CLOB", None)
                        added.append(name)
                        break
                return []
            if upper.startswith("SELECT ID, DEDUP_HASH FROM HIVE_JOBS"):
                return []
            if upper.startswith("CREATE INDEX") or "CREATE UNIQUE INDEX" in upper:
                return []
            return []

        with mock.patch("hive.persistence.oracle._raw_execute", side_effect=fake_execute):
            await repo._apply_migrations(object())
        # The new column is in the migration map.
        self.assertIn("BASE_BRANCH", _ORACLE_JOB_ADDITIONS)
        # And it was added exactly once for a legacy HIVE_JOBS that lacked it.
        self.assertIn("BASE_BRANCH", added)
        # And it ends up in the converged column set.
        self.assertIn("BASE_BRANCH", columns)
        self.assertEqual(columns["BASE_BRANCH"], ("VARCHAR2", 255))

    async def test_oracle_migration_skips_base_branch_when_already_present(self):
        """Idempotent: a fresh HIVE_JOBS that already has ``BASE_BRANCH`` must
        NOT get a duplicate ALTER TABLE ADD."""
        repo = OracleHiveRepository("oracle://user:pass@db/TEST")
        columns = {
            "STATUS": ("VARCHAR2", 32),
            "BASE_BRANCH": ("VARCHAR2", 255),
        }
        added: list[str] = []

        async def fake_execute(_raw, sql, params=()):
            upper = sql.upper()
            if "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH" in upper:
                return [(name, dt, ln) for name, (dt, ln) in columns.items()]
            if "SELECT INDEX_NAME FROM USER_INDEXES" in upper:
                return []
            if "SELECT CONSTRAINT_NAME" in upper:
                return [
                    ("CK_HIVE_JOBS_STATUS_RUNTIME", "status IN ('queued','done')"),
                    ("SYS_C_STATUS_NN", '"STATUS" IS NOT NULL'),
                ]
            if upper.startswith("ALTER TABLE HIVE_JOBS ADD ("):
                for name in _ORACLE_JOB_ADDITIONS:
                    if name in upper and name not in columns:
                        added.append(name)
                        columns[name] = ("CLOB", None)
                        break
                return []
            if upper.startswith("CREATE INDEX") or "CREATE UNIQUE INDEX" in upper:
                return []
            return []

        with mock.patch("hive.persistence.oracle._raw_execute", side_effect=fake_execute):
            await repo._apply_migrations(object())
        # BASE_BRANCH was already there -> nothing added.
        self.assertNotIn("BASE_BRANCH", added)
        self.assertEqual(columns["BASE_BRANCH"], ("VARCHAR2", 255))
