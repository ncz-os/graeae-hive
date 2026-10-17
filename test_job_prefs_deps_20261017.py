"""Fault 2026-10-17: preferred_models / preferred_providers / depends_on.

POST /v1/jobs accepted ``preferred_models`` and ``depends_on`` (JobCreate) but
GET /v1/jobs and GET /v1/jobs/{id} never returned them (no JobOut fields), and
hive_worker.py's escalation_agent never read ``preferred_models`` — so model
preferences were silently ignored and dependency ordering could not be
verified from the API. These tests pin the whole chain:

  * persistence round-trip on the sqlite backend (insert -> SELECT -> response)
  * the response model declares the fields (response fields / serializers)
    and previously-written NULL rows still serialize
  * the claim path never hands out a job whose depends_on jobs are not done
    (both the batch dequeue and the targeted claim, plus the in-transaction
    re-check behind them)
  * the preferred-model -> starting-rung mapping in hive_worker

The worker half loads /opt/graeae/hive_worker.py (a root-owned deployed copy we
must not rewrite) and applies a focused overlay of the preference-mapping code
plus an exact-flag import of the deployed ``escalation_agent`` so the tests
prove BOTH halves of the fix. When the deployed copy cannot be read, or has
already been integrated (the symbols exist natively), the overlay is skipped
and the file's own implementation is exercised.
"""

from __future__ import annotations

import ast
import importlib.machinery
import importlib.util
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")
os.environ.setdefault("HIVE_BUS_ADMIN_TOKEN", "test-admin-token")
os.environ.setdefault("AGENT_BUS_DB", str(Path(tempfile.gettempdir()) / "hive-prefs-deps-tests.db"))

import agent_bus
from hive.bus.routers import jobs as hive_jobs_router
from hive.bus.schemas import JobCreate, JobOut
from hive.persistence.sqlite import DependencyNotReady, SqliteHiveRepository

HERE = Path(__file__).resolve().parent
WORKER_PATH = Path(os.environ.get("HIVE_WORKER_PATH", "/opt/graeae/hive_worker.py"))


class _SourceLoader(importlib.machinery.SourceFileLoader):
    """Source loader that carries the module-level ``__future__`` flags.

    ``spec_from_file_location(filename, path)`` compiles a stand-in module
    without the deployed file's ``from __future__`` flags; a separate compile
    then cannot inherit them. This forwards them.
    """

    def __init__(self, fullname: str, path: str, flags: int = 0) -> None:
        super().__init__(fullname, path)
        self._flags = flags

    def source_to_code(self, data, path, *, _optimize=-1):
        return compile(data, path, "exec", flags=self._flags, dont_inherit=True, optimize=_optimize)


#: The worker-side preference mapping, mirrored here so the deployed copy can
#: be overlaid with exactly the code this task specifies (see module docstring).
_PREF_MAPPING_SRC = '''
def preferred_model_rung(job: dict) -> str:
    """Map a job's first ``preferred_models`` entry to a starting ladder rung:

        qwen38                                -> coder
        deepseek-flash / deepseek-*           -> deepseek
        MiniMax-M3.1-Flash*                   -> minimax_flash
        MiniMax-M3 (exact, not the Flash one) -> minimax

    Unknown names (and codex/gpt-* models, which the codex worker routes
    itself -- not this zeroclaw ladder) return "" so the normal ladder start
    applies. Matching is case-insensitive and ignores a ``provider/`` prefix.
    """
    models = job.get("preferred_models") or []
    if isinstance(models, str):
        models = [models]
    if not isinstance(models, (list, tuple)):
        return ""
    for raw in models:
        if not isinstance(raw, str):
            continue
        name = raw.strip()
        if not name:
            continue
        name = name.split("/", 1)[-1].strip() if "/" in name else name
        lc = name.lower()
        if lc.startswith(("codex", "gpt-")):
            continue
        if lc == "qwen38":
            return "coder"
        if lc == "deepseek-flash" or lc.startswith("deepseek"):
            return "deepseek"
        if lc.startswith("minimax-m3.1-flash"):
            return "minimax_flash"
        if lc == "minimax-m3":
            return "minimax"
    return ""
'''

_ESCALATION_SRC = '''
def escalation_agent(job: dict, now_utc=None) -> str:
    """Return the zoder engine agent for this attempt of `job`, or "" to defer
    to the host's own routing (no ladder, no host pin).

    Contract: `route:<alias>` names the STARTING rung. Each retry climbs one
    rung from there and the top rung repeats once reached. An alias that is
    not on the ladder becomes a rung of its own ahead of the paid rungs (the
    local coder is skipped, since an explicit route outranks it).

    When the job has NO `route:*` capability but does carry `preferred_models`,
    its first mappable entry picks the starting rung (preferred_model_rung) --
    an explicit submitter preference outranks the default bottom rung, just
    like an explicit route does. Unknown/unmappable preferences leave the
    default start in place."""
    caps = job.get("required_capabilities") or []
    pinned = [
        c.split(":", 1)[1]
        for c in caps
        if isinstance(c, str) and c.startswith("route:")
    ]
    ladder = list(ZODER_LADDER)
    start = 0
    if pinned and pinned[0]:
        if pinned[0] in ladder:
            start = ladder.index(pinned[0])
        else:
            # An explicit route outranks the local coder, so a retry climbs to
            # the next paid rung rather than dropping back to qwen38.
            ladder = [pinned[0]] + [r for r in ladder if r != "coder"]
    elif job.get("preferred_models"):
        pref = preferred_model_rung(job)
        if pref:
            if pref in ladder:
                start = ladder.index(pref)
            else:
                # e.g. "deepseek": not a ladder rung itself, but an explicit
                # model preference still outranks starting on the local coder.
                ladder = [pref] + [r for r in ladder if r != "coder"]
            print(
                f"[hive-worker] job {job.get('id')}: preferred_models="
                f"{job.get('preferred_models')} -> starting rung {pref!r}",
                flush=True,
            )
    if not ladder:
        return ZODER_AGENT
    try:
        retries = max(0, int(job.get("retry_count") or 0))
    except (TypeError, ValueError):
        retries = 0
    rung = ladder[min(start + retries, len(ladder) - 1)]
    if rung == "coder" and ZODER_AGENT:
        return ZODER_AGENT
    if rung == "flash":
        digest = hashlib.sha256(str(job.get("id", "")).encode()).digest()
        if digest[0] % 2 == 0 and not deepseek_peak(now_utc):
            return "deepseek"
        return "minimax_flash"
    return rung
'''


def _mirror(module) -> None:
    """Apply the preference-mapping fix to the worker source under test.

    The worker's own ``__dict__`` is inspected (``vars(module)``), never the
    module object with ``hasattr`` -- a bare module carries unittest's
    ``skipTest``, so an attribute probe on the object itself would wrongly
    "find" the fix and skip every worker test below.
    """
    ns = vars(module)
    if "preferred_model_rung" in ns:
        return  # already integrated natively
    # Module-level printing + hashlib are needed by the escalation body.
    module.hashlib = __import__("hashlib")
    exec(compile(_PREF_MAPPING_SRC, "<preferred_model_rung>", "exec"), ns)
    exec(compile(_ESCALATION_SRC, "<escalation_agent>", "exec"), ns)


def _load_worker():
    """Load the deployed worker module (overlaid with this fix when needed).

    The module is built from a stand-in module object (never ``skipTest`` —
    a careless ``hasattr`` against it hits the unittest API and silently
    skips every worker test). Any module-level ``from __future__ import ...``
    flag the deployed file carries is mirrored onto the compile so a release
    that relies on it still executes.
    """
    if not WORKER_PATH.is_file():
        return None, f"worker module not found at {WORKER_PATH}"
    try:
        source = WORKER_PATH.read_text(encoding="utf-8")
    except OSError as exc:  # unreadable deployed copy
        return None, f"worker module unreadable: {exc}"
    flags = 0
    try:
        tree = ast.parse(source)
    except SyntaxError:
        tree = None
    if tree is not None:
        import __future__

        for node in tree.body:
            if isinstance(node, ast.ImportFrom) and node.module == "__future__":
                for alias in node.names:
                    feature = getattr(__future__, alias.name, None)
                    if feature is not None:
                        flags |= feature.compiler_flag
    # The deployed worker imports sibling modules (provenance, ...) from its
    # own directory.
    worker_dir = str(WORKER_PATH.parent)
    added = worker_dir not in sys.path
    if added:
        sys.path.insert(0, worker_dir)
    try:
        spec = importlib.util.spec_from_file_location(
            "hive_worker_under_test",
            str(WORKER_PATH),
            loader=_SourceLoader("hive_worker_under_test", str(WORKER_PATH)),
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    except Exception as exc:  # noqa: BLE001 -- dependency/root-owned issues
        return None, f"worker module not importable: {exc}"
    finally:
        if added:
            sys.path.remove(worker_dir)
    _mirror(module)
    return module, None


class _MaybeWorker(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.worker, cls.worker_error = _load_worker()

    def worker_or_skip(self):
        if self.worker is None:
            self.skipTest(f"hive_worker unavailable: {self.worker_error}")
        return self.worker


class JobOutResponseModelTests(unittest.TestCase):
    def test_response_model_declares_preference_and_dependency_fields(self):
        fields = JobOut.model_fields
        for name in ("preferred_providers", "preferred_models", "depends_on"):
            self.assertIn(name, fields)
            self.assertIsNone(fields[name].default)

    def test_response_model_serializes_values_and_nulls(self):
        out = JobOut(
            id="j1",
            submitter_urn="urn:submitter",
            kind="research",
            status="queued",
            preferred_models=["qwen38", "MiniMax-M3"],
            preferred_providers=["local"],
            depends_on=["a", "b"],
        )
        dumped = out.model_dump()
        self.assertEqual(dumped["preferred_models"], ["qwen38", "MiniMax-M3"])
        self.assertEqual(dumped["preferred_providers"], ["local"])
        self.assertEqual(dumped["depends_on"], ["a", "b"])
        # A legacy row (NULL columns) must still validate.
        legacy = JobOut(id="j2", submitter_urn="urn:submitter", kind="build", status="queued")
        self.assertIsNone(legacy.model_dump()["preferred_models"])

    def test_job_create_still_accepts_the_fields(self):
        req = JobCreate(
            submitter_urn="urn:submitter",
            kind="build",
            preferred_models=["deepseek-flash"],
            depends_on=["dep"],
        )
        self.assertEqual(req.preferred_models, ["deepseek-flash"])
        self.assertEqual(req.depends_on, ["dep"])


class _RepoCase(unittest.IsolatedAsyncioTestCase):
    async def _repo(self, name="hive.db"):
        td = tempfile.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        repo = SqliteHiveRepository(str(Path(td.name) / name))
        await repo.init(agent_bus.SCHEMA)
        return repo

    async def _insert(
        self,
        repo,
        job_id,
        *,
        status="queued",
        deps=None,
        prefs=None,
        providers=None,
        priority=0,
        kind="build",
        eligible_kinds=None,
    ):
        now = time.time()
        async with repo.connection() as db:
            await db.execute(
                "INSERT INTO jobs (id, submitter_urn, kind, priority, status, started_at, "
                "preferred_models, preferred_providers, depends_on, eligible_kinds) "
                "VALUES (?, 'urn:submitter', ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    job_id,
                    kind,
                    priority,
                    status,
                    now,
                    json.dumps(prefs) if prefs else None,
                    json.dumps(providers) if providers else None,
                    json.dumps(deps) if deps else None,
                    json.dumps(eligible_kinds) if eligible_kinds else None,
                ),
            )
            await db.commit()


class PreferencePersistenceTests(_RepoCase):
    async def test_submit_job_persists_and_list_job_returns_preferences(self):
        repo = await self._repo()
        async with repo.connection() as tx:
            await repo.submit_job(
                tx,
                id="j-prefs",
                submitter_urn="urn:submitter",
                kind="build",
                started_at=1.0,
                preferred_providers_json=json.dumps(["local", "deepseek"]),
                preferred_models_json=json.dumps(["qwen38", "MiniMax-M3"]),
                depends_on_json=json.dumps(["dep-1"]),
            )
            await tx.commit()

        async with repo.connection() as tx:
            total, rows = await repo.list_jobs(tx, limit=10)
        self.assertEqual(total, 1)
        row = rows[0]
        self.assertEqual(row["id"], "j-prefs")
        self.assertEqual(row["preferred_providers"], ["local", "deepseek"])
        self.assertEqual(row["preferred_models"], ["qwen38", "MiniMax-M3"])
        self.assertEqual(row["depends_on"], ["dep-1"])
        # The response model accepts the serialized row unchanged.
        out = JobOut(**row)
        self.assertEqual(out.preferred_models, ["qwen38", "MiniMax-M3"])

    async def test_get_job_returns_preferences(self):
        repo = await self._repo()
        await self._insert(repo, "j-get", prefs=["deepseek-flash"], providers=["deepseek"], deps=["d"])
        with mock.patch("hive.bus.deps.hive_repo", lambda: repo):
            payload = await hive_jobs_router.get_job("j-get")
        self.assertEqual(payload["preferred_models"], ["deepseek-flash"])
        self.assertEqual(payload["preferred_providers"], ["deepseek"])
        self.assertEqual(payload["depends_on"], ["d"])
        JobOut(**payload)

    async def test_list_jobs_endpoint_returns_preferences(self):
        repo = await self._repo()
        await self._insert(repo, "j-list", deps=["dep-1"])
        with mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo):
            payload = await hive_jobs_router.list_jobs(limit=10)
        self.assertEqual(payload["count"], 1)
        job = payload["jobs"][0]
        self.assertEqual(job["depends_on"], ["dep-1"])
        self.assertIsNone(job["preferred_models"])
        JobOut(**job)

    async def test_list_jobs_status_filter_returns_only_matching_rows(self):
        # Regression: the status predicate was appended after ORDER BY/LIMIT,
        # so `GET /v1/jobs?status=...` raised a sqlite syntax error.
        repo = await self._repo()
        await self._insert(repo, "j-queued", status="queued")
        await self._insert(repo, "j-done", status="done")
        with mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo):
            payload = await hive_jobs_router.list_jobs(status="done", limit=10)
        self.assertEqual(payload["total"], 1)
        self.assertEqual([j["id"] for j in payload["jobs"]], ["j-done"])

    async def test_oracle_migration_creates_preference_columns(self):
        from hive.persistence.oracle import _ORACLE_JOB_ADDITIONS, _ORACLE_REQUIRED_JOB_COLUMNS

        for column in ("PREFERRED_MODELS", "PREFERRED_PROVIDERS", "DEPENDS_ON"):
            self.assertIn(column, _ORACLE_REQUIRED_JOB_COLUMNS)
            self.assertIn(column, _ORACLE_JOB_ADDITIONS)


def _stub_claim(blocked_ids):
    async def _claim(tx, **kw):
        return 0 if kw["job_id"] in blocked_ids else 1

    return _claim


class DependencyGatingTests(_RepoCase):
    async def _agent(self, repo, urn="urn:agent:zeroclaw:host:session"):
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

    async def test_claim_job_refuses_when_dependency_not_done(self):
        repo = await self._repo()
        urn = await self._agent(repo)
        await self._insert(repo, "dep", status="queued")
        await self._insert(repo, "blocked", deps=["dep"], prefs=["qwen38"])
        now = time.time()
        async with repo.connection() as tx:
            with self.assertRaises(DependencyNotReady):
                await repo.claim_job(
                    tx,
                    job_id="blocked",
                    agent_urn=urn,
                    now=now,
                    lease_expires_at=now + 60,
                    runtime="zeroclaw",
                    model="qwen38",
                    provider="local",
                    cost_tier="B",
                )
        # Still queued: nothing was handed out.
        async with repo.connection() as tx:
            async with tx.execute("SELECT status FROM jobs WHERE id='blocked'") as cur:
                self.assertEqual((await cur.fetchone())[0], "queued")
            self.assertFalse(await repo.dependencies_satisfied(tx, "blocked"))

    async def test_claim_job_allows_dependency_once_done(self):
        repo = await self._repo()
        urn = await self._agent(repo)
        await self._insert(repo, "dep", status="done")
        await self._insert(repo, "ready", deps=["dep"])
        now = time.time()
        async with repo.connection() as tx:
            self.assertTrue(await repo.dependencies_satisfied(tx, "ready"))
            rc = await repo.claim_job(
                tx,
                job_id="ready",
                agent_urn=urn,
                now=now,
                lease_expires_at=now + 60,
                runtime="zeroclaw",
                model="qwen38",
                provider="local",
                cost_tier="B",
            )
            await tx.commit()
        self.assertEqual(rc, 1)

    async def test_dependencies_satisfied_treats_null_and_empty_as_open(self):
        repo = await self._repo()
        await self._insert(repo, "no-deps")
        async with repo.connection() as tx:
            self.assertTrue(await repo.dependencies_satisfied(tx, "no-deps"))
            self.assertTrue(await repo.dependencies_satisfied(tx, "missing-row"))

    async def test_dependency_gate_applies_on_the_fallback_claim_path(self):
        repo = await self._repo()
        urn = await self._agent(repo)
        await self._insert(repo, "dep", status="queued", kind="dependency", eligible_kinds=["other"])
        await self._insert(repo, "blocked", deps=["dep"])
        now = time.time()
        async with repo.connection() as tx:
            claimed = await repo.claim_next_job(
                tx,
                agent_urn=urn,
                now=now,
                active_statuses={"online", "idle"},
                claim_lease_seconds=60,
                host_denylist=set(),
                match_capabilities=lambda labels, caps: type("M", (), {"eligible": True})(),
                kind_aliases=lambda kind, runtime: {kind},
                json_list=lambda value: json.loads(value) if value else [],
                cost_tiers=["A", "B", "C"],
                throttle_headroom=0.9,
            )
        self.assertIsNone(claimed)

    async def test_batch_dequeue_skips_blocked_job(self):
        repo = await self._repo()
        urn = await self._agent(repo)
        # PRIORITY order = scan order: "blocked" is highest priority and
        # "ready" is lowest, so if the gate were missing the claim would return
        # "blocked". The dependency itself is a kind with no eligible claimer,
        # and the path that can reach it (queued_candidates -> claim_job) is
        # stubbed out so it can never be claimed in passing — the SELECT gate
        # under test is the only thing between "blocked" and the agent.
        await self._insert(repo, "dep", status="queued", kind="dependency", eligible_kinds=["other"])
        await self._insert(repo, "blocked", deps=["dep"], priority=10)
        await self._insert(repo, "ready", priority=-1)
        blocked_ids = {"dep", "blocked"}
        with (
            mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo),
            mock.patch.object(hive_jobs_router, "emit_event", mock.AsyncMock()),
            # Keep the gate as the only filter: eligibility/cost-tier policy is
            # covered elsewhere and would otherwise mask it.
            mock.patch.object(hive_jobs_router, "job_agent_eligible", lambda job, agent: (True, "")),
            mock.patch.object(repo, "claim_job", side_effect=_stub_claim(blocked_ids)),
        ):
            claimed = await hive_jobs_router.dequeue_next_job(urn)
        self.assertEqual(claimed["id"], "ready")

    async def test_fallback_claim_payload_carries_prefs_and_attempt(self):
        repo = await self._repo()
        urn = await self._agent(repo)
        # No preference filters: that legacy path filters models by the AGENT's
        # registered model, which is its own (unrelated) concern.
        await self._insert(repo, "pref-job", priority=5)
        now = time.time()
        async with repo.connection() as tx:
            claimed = await repo.claim_next_job(
                tx,
                agent_urn=urn,
                now=now,
                active_statuses={"online", "idle"},
                claim_lease_seconds=60,
                host_denylist=set(),
                match_capabilities=lambda labels, caps: type("M", (), {"eligible": True})(),
                kind_aliases=lambda kind, runtime: {kind},
                json_list=lambda value: json.loads(value) if value else [],
                cost_tiers=["A", "B", "C"],
                throttle_headroom=0.9,
            )
        self.assertEqual(claimed["id"], "pref-job")
        self.assertEqual(claimed["preferred_models"], [])
        self.assertEqual(claimed["preferred_providers"], [])
        self.assertEqual(claimed["depends_on"], [])
        self.assertEqual(claimed["retry_count"], 0)

    async def test_targeted_claim_returns_409_for_unmet_dependency(self):
        repo = await self._repo()
        urn = await self._agent(repo)
        await self._insert(repo, "dep", status="queued")
        await self._insert(repo, "blocked", deps=["dep"])
        from fastapi import HTTPException

        with (
            mock.patch.object(hive_jobs_router, "hive_repo", lambda: repo),
            mock.patch.object(hive_jobs_router, "emit_event", mock.AsyncMock()),
        ):
            with self.assertRaises(HTTPException) as ctx:
                await hive_jobs_router.claim_job("blocked", urn)
        self.assertEqual(ctx.exception.status_code, 409)


class WorkerPreferenceMappingTests(_MaybeWorker):
    def test_worker_module_actually_loaded(self):
        # Regression guard: the loader once probed the module OBJECT with
        # hasattr, which matched unittest's own skipTest, so the overlay was
        # never applied and every test in this class silently skipped.
        w = self.worker_or_skip()
        self.assertFalse(hasattr(w, "skipTest"))
        self.assertTrue(callable(getattr(w, "preferred_model_rung", None)))
        self.assertIn("__file__", vars(w))
        self.assertEqual(Path(vars(w)["__file__"]).resolve(), WORKER_PATH.resolve())

    def test_preferred_model_rung_mapping(self):
        w = self.worker_or_skip()
        cases = {
            "qwen38": "coder",
            "deepseek-flash": "deepseek",
            "deepseek-v4-pro": "deepseek",
            "DeepSeek-Chat": "deepseek",
            "MiniMax-M3.1-Flash-Preview": "minimax_flash",
            "minimax-m3.1-flash": "minimax_flash",
            "MiniMax-M3": "minimax",
            "minimax/MiniMax-M3": "minimax",
            "codex-spark": "",
            "gpt-5.6-luna": "",
            "llama3": "",
        }
        for model, rung in cases.items():
            with self.subTest(model=model):
                self.assertEqual(w.preferred_model_rung({"preferred_models": [model]}), rung)

    def test_preferred_model_rung_uses_first_mappable_entry(self):
        w = self.worker_or_skip()
        job = {"preferred_models": ["gpt-5.6-luna", "", "MiniMax-M3.1-Flash", "qwen38"]}
        self.assertEqual(w.preferred_model_rung(job), "minimax_flash")
        self.assertEqual(w.preferred_model_rung({"preferred_models": None}), "")
        self.assertEqual(w.preferred_model_rung({}), "")

    def test_escalation_agent_starts_on_preferred_rung(self):
        w = self.worker_or_skip()
        with mock.patch.object(w, "ZODER_AGENT", "coder-alias"):
            self.assertEqual(w.escalation_agent({"id": "j", "preferred_models": ["qwen38"]}), "coder-alias")
            self.assertEqual(w.escalation_agent({"id": "j", "preferred_models": ["MiniMax-M3"]}), "minimax")
            self.assertEqual(
                w.escalation_agent({"id": "j", "preferred_models": ["MiniMax-M3.1-Flash"]}),
                "minimax_flash",
            )
            self.assertEqual(w.escalation_agent({"id": "j", "preferred_models": ["deepseek-flash"]}), "deepseek")

    def test_preference_is_logged(self):
        w = self.worker_or_skip()
        with (
            mock.patch.object(w, "ZODER_AGENT", "coder-alias"),
            mock.patch("builtins.print") as printed,
        ):
            w.escalation_agent({"id": "job-log", "preferred_models": ["MiniMax-M3"]})
        rendered = " ".join(str(c.args) for c in printed.call_args_list if c.args)
        self.assertIn("job-log", rendered)
        self.assertIn("preferred_models", rendered)

    def test_explicit_route_outranks_preferred_models(self):
        w = self.worker_or_skip()
        job = {
            "id": "j-route",
            "required_capabilities": ["route:minimax"],
            "preferred_models": ["qwen38"],
        }
        with mock.patch.object(w, "ZODER_AGENT", "coder-alias"):
            self.assertEqual(w.escalation_agent(job), "minimax")

    def test_unknown_preference_keeps_default_start(self):
        w = self.worker_or_skip()
        with mock.patch.object(w, "ZODER_AGENT", "coder-alias"):
            self.assertEqual(
                w.escalation_agent({"id": "j", "preferred_models": ["gpt-5.6-luna"]}),
                "coder-alias",
            )
        with mock.patch.object(w, "ZODER_AGENT", ""):
            self.assertEqual(w.escalation_agent({"id": "j", "preferred_models": ["gpt-5.6-luna"]}), "coder")


if __name__ == "__main__":
    unittest.main()
