"""Three submitter-facing defects at the bus, pinned end-to-end or at the unit
boundary. 2026-10-25.

Defect 1 -- cancel asymmetry.
    PATCH /v1/jobs/{id} answered 403 "job update requires claimed_by to match
    the current claimant" for the job's OWN submitter, so a submitter could not
    cancel or supersede its own claimed job (an orchestrator had to force-DELETE
    the row and kill the worker process by hand). Now the submitter_urn, and an
    agent registered with the ``orchestrator`` capability, may set
    ``status=cancelled`` + ``reason``; every other field stays claimant-only;
    the cancel is audited and emitted as ``job.cancelled``.
    NEGATIVE CONTROL: a non-submitter with no capability still gets 403.

Defect 2 -- silent preferred_models fallback.
    A forbidden model, a typo, or a model no eligible worker serves used to fall
    back silently (e.g. to qwen38). Now a forbidden model is 422 and an unserved
    name yields ``warnings[]``; the model actually used is recorded on the job.
    NEGATIVE CONTROL: valid models produce NO warnings.

Defect 3 -- missing head_branch.
    Nine jobs died because head_branch did not exist on the ARGONAS remote.
    Submit now probes: present -> accept; missing with a usable base_branch ->
    accept and record ``branch_created_from``; neither -> 422 naming the ref.
    NEGATIVE CONTROL: a non-missing branch is never rejected.

The probes are injected (``prober=``/``resolver=``) or the git call is mocked,
so the suite never touches ARGONAS and never needs ssh.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
import uuid
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")
os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")
os.environ.setdefault("HIVE_BUS_ADMIN_TOKEN", "test-admin-token")

import httpx  # noqa: E402
from fastapi import HTTPException  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus import deps  # noqa: E402
from hive.bus.deps import hive_repo as _default_hive_repo  # noqa: E402
from hive.bus.domain import branch_admission as ba  # noqa: E402
from hive.bus.domain.model_admission import (  # noqa: E402
    FORBIDDEN_MODEL_PROVIDERS,
    FORBIDDEN_MODELS,
    admission_warnings,
    forbidden_model,
    served_model_names,
)
from hive.bus.routers import jobs as routers_jobs  # noqa: E402
from hive.persistence import sqlite as sqlite_mod  # noqa: E402
from hive.persistence.db2 import _DB2_ADDITIVE_COLUMNS  # noqa: E402
from hive.persistence.oracle import _ORACLE_JOB_ADDITIONS  # noqa: E402
from hive.persistence.sqlite import DependencyNotReady  # noqa: E402


class _BusCase(unittest.IsolatedAsyncioTestCase):
    """Full ASGI app against a temp sqlite DB, and admission stubbed neutral.

    Branch admission is neutralized by default so the OTHER two defects can be
    exercised without a remote; the branch tests re-enable it explicitly.
    """

    async def asyncSetUp(self):
        self._td = tempfile.TemporaryDirectory()
        os.environ["AGENT_BUS_DB"] = os.path.join(self._td.name, "submitter.db")
        await _default_hive_repo().init()
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=agent_bus.app),
            base_url="http://t",
        )
        self._branch_patch = mock.patch(
            "hive.bus.routers.jobs.branch_admission",
            return_value={
                "ok": True,
                "reason": None,
                "detail": None,
                "state": "head_present",
                "url": None,
                "branch_created_from": None,
                "warnings": [],
            },
        )
        self._branch_patch.start()

    async def asyncTearDown(self):
        self._branch_patch.stop()
        await self.client.aclose()
        self._td.cleanup()

    async def _register(
        self,
        kind: str,
        host: str,
        capabilities: list[str] | None = None,
        model: str | None = None,
    ) -> str:
        runtime = "opencode" if kind == "opencode" else "zeroclaw"
        body = {
            "kind": kind,
            "host": host,
            "runtime": runtime,
            "status": "online",
            "capabilities": capabilities or ["code"],
            "cost_tier": "B",
            "auth_method": "api",
        }
        if model:
            body["model"] = model
        r = await self.client.post("/v1/agents/register", json=body)
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()["urn"]

    async def _submit(self, submitter: str, **extra) -> dict:
        body = {
            "kind": "code-edit",
            # Unique per submit: the bus coalesces identical submissions, and a
            # coalesced reply is an existing job (possibly already claimed).
            "description": f"d-{uuid.uuid4()}",
            "submitter_urn": submitter,
            "site": "internal",
            "repo": "graeae-hive",
            "head_branch": "fix/x",
            "priority": 5,
            **extra,
        }
        r = await self.client.post("/v1/jobs", json=body)
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()

    async def _claim(self, worker: str, jid: str) -> None:
        # A PATCH cannot claim (the bus rejects claim bypasses with 409, by
        # design); the sanctioned path is the explicit claim endpoint.
        r = await self.client.post(f"/v1/jobs/{jid}/claim", params={"by": worker})
        self.assertEqual(r.status_code, 200, r.text)


class SubmitterCancelTests(_BusCase):
    """Defect 1: the submitter (or an orchestrator) can stop a claimed job."""

    async def test_submitter_cancels_own_claimed_job_with_reason(self):
        submitter = await self._register("opencode", "studio")
        worker = await self._register("zeroclaw", "studio")
        job = await self._submit(submitter)
        await self._claim(worker, job["id"])

        r = await self.client.patch(
            f"/v1/jobs/{job['id']}",
            json={"status": "cancelled", "cancel_actor": submitter, "reason": "superseded by a fix"},
        )
        self.assertEqual(r.status_code, 200, r.text)
        # PATCH returns {ok, ts, ...}; the job state is read back via GET.
        got = (await self.client.get(f"/v1/jobs/{job['id']}")).json()
        self.assertEqual(got["status"], "cancelled")

        # The stop is audited with WHO and WHY.
        async with _default_hive_repo().connection() as tx:
            async with tx.execute(
                "SELECT actor_urn, new_status, patch FROM job_audit_log WHERE job_id=? ORDER BY ts DESC",
                (job["id"],),
            ) as cur:
                row = await cur.fetchone()
        self.assertEqual(row[0], submitter)
        self.assertEqual(row[1], "cancelled")
        self.assertIn("superseded by a fix", row[2])

    async def test_claimant_observes_cancellation_on_next_update(self):
        """The claimant's next heartbeat/update must see the stop, not a 200."""
        submitter = await self._register("opencode", "studio")
        worker = await self._register("zeroclaw", "studio")
        job = await self._submit(submitter)
        await self._claim(worker, job["id"])

        await self.client.patch(
            f"/v1/jobs/{job['id']}",
            json={"status": "cancelled", "cancel_actor": submitter, "reason": "stop"},
        )
        # A renewal after cancellation is terminal -> 409, which is what
        # hive_worker treats as "stop cleanly".
        r = await self.client.patch(
            f"/v1/jobs/{job['id']}",
            json={"status": "running", "claimed_by": worker},
        )
        self.assertEqual(r.status_code, 409, r.text)

    async def test_orchestrator_capability_can_cancel_someone_elses_job(self):
        submitter = await self._register("opencode", "studio")
        worker = await self._register("zeroclaw", "studio")
        orchestrator = await self._register("opencode", "console", capabilities=["code", "orchestrator"])
        job = await self._submit(submitter)
        await self._claim(worker, job["id"])

        r = await self.client.patch(
            f"/v1/jobs/{job['id']}",
            json={"status": "cancelled", "cancel_actor": orchestrator, "reason": "operator stop"},
        )
        self.assertEqual(r.status_code, 200, r.text)

    async def test_negative_control_non_submitter_still_gets_403(self):
        submitter = await self._register("opencode", "studio")
        worker = await self._register("zeroclaw", "studio")
        stranger = await self._register("opencode", "elsewhere")  # no orchestrator capability
        job = await self._submit(submitter)
        await self._claim(worker, job["id"])

        r = await self.client.patch(
            f"/v1/jobs/{job['id']}",
            json={"status": "cancelled", "cancel_actor": stranger, "reason": "not mine"},
        )
        self.assertEqual(r.status_code, 403, r.text)
        self.assertEqual((await self.client.get(f"/v1/jobs/{job['id']}")).json()["status"], "claimed")

    async def test_negative_control_submitter_cannot_write_claimant_fields(self):
        """The elevation is ONE field. Smuggling a result alongside is refused."""
        submitter = await self._register("opencode", "studio")
        worker = await self._register("zeroclaw", "studio")
        job = await self._submit(submitter)
        await self._claim(worker, job["id"])

        r = await self.client.patch(
            f"/v1/jobs/{job['id']}",
            json={
                "status": "cancelled",
                "cancel_actor": submitter,
                "reason": "sneaky",
                "result": {"output": "forged"},
            },
        )
        self.assertEqual(r.status_code, 403, r.text)
        got = (await self.client.get(f"/v1/jobs/{job['id']}")).json()
        self.assertEqual(got["status"], "claimed")

    async def test_negative_control_cancel_cannot_smuggle_routing_fields(self):
        """ALLOWLIST, not denylist: a cancel may carry ONLY status/reason/cancel_actor.

        A denylist let unenumerated fields (priority, repo, head_branch, ...)
        through, so a non-claimant could mutate routing/identity under the
        guise of a cancel.
        """
        submitter = await self._register("opencode", "studio")
        worker = await self._register("zeroclaw", "studio")
        # Only fields JobUpdate actually declares can be smuggled (unknown keys
        # are dropped by the schema, so priority/repo/head_branch are not
        # mutable via PATCH at all). Cover the full declared surface.
        for smuggling in (
            {"eligible_kinds": ["code-edit", "research"]},
            {"eligible_hosts": ["elsewhere"]},
            {"claimed_model": "qwen38"},
            {"claimed_provider": "siliconflow"},
            {"claimed_runtime": "opencode"},
            {"claimed_cost_tier": "A"},
            {"tokens_in": 100},
            {"tokens_out": 100},
            {"result_mnemos_id": "mem_forged"},
        ):
            job = await self._submit(submitter)
            await self._claim(worker, job["id"])
            r = await self.client.patch(
                f"/v1/jobs/{job['id']}",
                json={"status": "cancelled", "cancel_actor": submitter, "reason": "x", **smuggling},
            )
            self.assertEqual(r.status_code, 403, f"{smuggling} -> {r.text}")
            got = (await self.client.get(f"/v1/jobs/{job['id']}")).json()
            self.assertEqual(got["status"], "claimed", smuggling)

    async def test_negative_control_forged_submitter_urn_in_body_is_ignored(self):
        """submitter_urn on the PATCH is request data, never an auth source."""
        submitter = await self._register("opencode", "studio")
        worker = await self._register("zeroclaw", "studio")
        await self._register("opencode", "elsewhere")
        job = await self._submit(submitter)
        await self._claim(worker, job["id"])

        r = await self.client.patch(
            f"/v1/jobs/{job['id']}",
            json={"status": "cancelled", "submitter_urn": submitter, "reason": "forged"},
        )
        self.assertEqual(r.status_code, 403, r.text)

    async def test_negative_control_non_submitter_may_still_update_nothing(self):
        submitter = await self._register("opencode", "studio")
        worker = await self._register("zeroclaw", "studio")
        job = await self._submit(submitter)
        await self._claim(worker, job["id"])
        r = await self.client.patch(
            f"/v1/jobs/{job['id']}",
            json={"status": "running", "claimed_by": "urn:agent:zeroclaw:studio:imposter"},
        )
        self.assertEqual(r.status_code, 403, r.text)

    def test_has_orchestrator_capability_is_defined(self):
        """Regression: the previous attempt referenced this and it did not exist."""
        from hive.bus.routers import jobs as jobs_module

        self.assertTrue(callable(jobs_module._has_orchestrator_capability))


class PreferredModelTests(_BusCase):
    """Defect 2: no silent fallback to an unrequested model."""

    async def test_forbidden_model_is_422(self):
        submitter = await self._register("opencode", "studio")
        r = await self.client.post(
            "/v1/jobs",
            json={
                "kind": "code-edit",
                "description": "d",
                "submitter_urn": submitter,
                "site": "internal",
                "repo": "graeae-hive",
                "head_branch": "fix/x",
                "preferred_models": ["siliconflow/deepseek-v3"],
            },
        )
        self.assertEqual(r.status_code, 422, r.text)
        self.assertIn("forbidden", r.text.lower())

    async def test_unserved_model_is_accepted_with_warning(self):
        submitter = await self._register("opencode", "studio")
        await self._register("zeroclaw", "studio")  # registers no model preference
        job = await self._submit(submitter, preferred_models=["totally-not-a-real-model"])
        self.assertTrue(job.get("warnings"), job)
        self.assertIn("totally-not-a-real-model", " ".join(job["warnings"]))

    async def test_negative_control_valid_model_produces_no_warnings(self):
        submitter = await self._register("opencode", "studio")
        # A worker REGISTERS the model it serves; that registration is what
        # makes a preferred_model "served" and therefore warning-free.
        await self._register("zeroclaw", "studio", model="qwen38")
        job = await self._submit(submitter, preferred_models=["qwen38"])
        served_warnings = [w for w in job.get("warnings", []) if "qwen38" in w]
        self.assertEqual(served_warnings, [], job.get("warnings"))

    def test_deepseek_direct_is_forbidden_as_a_provider(self):
        self.assertIn("deepseek-direct", FORBIDDEN_MODEL_PROVIDERS)
        self.assertNotIn("deepseek-direct", FORBIDDEN_MODELS)
        self.assertTrue(forbidden_model("deepseek-direct/flash"))
        self.assertTrue(forbidden_model("deepseek-direct/deepseek-pro"))
        # The sanctioned route stays legal.
        self.assertFalse(forbidden_model("deepseek/flash"))

    def test_provider_qualified_name_needs_full_match(self):
        """A bare served leaf must not silently bless another provider's key."""
        # qwen38 is served bare; someprovider/qwen38 is NOT.
        self.assertEqual(admission_warnings(["qwen38"], {"qwen38"}), [])
        self.assertEqual(len(admission_warnings(["someprovider/qwen38"], {"qwen38"})), 1)
        # A provider-qualified key the fleet really serves stays warning-free.
        self.assertEqual(admission_warnings(["siliconflow/x"], {"siliconflow/x"}), [])

    def test_served_model_names_carries_qualified_keys(self):
        """Regression: a worker's (provider, model) pair must produce the
        QUALIFIED key in the served set, or a `provider/model` preference the
        fleet really serves would warn (the _is_served qualified branch is
        dead code without it)."""
        agents = [{"model": "qwen38", "provider": "tydeus"}]
        served = served_model_names(agents)
        self.assertIn("qwen38", served)
        self.assertIn("tydeus/qwen38", served, served)
        # The qualified preference matches its exact provider, no warning.
        self.assertEqual(admission_warnings(["tydeus/qwen38"], served), [])
        # ... but a DIFFERENT provider's qualified key for the same leaf does
        # not silently match.
        self.assertEqual(len(admission_warnings(["someother/qwen38"], served)), 1)

    def test_warnings_dedupe_on_normalized_key(self):
        out = admission_warnings(["MiniMax-M3", "minimax-m3"], set())
        self.assertEqual(len(out), 1, out)
        self.assertEqual(admission_warnings(["qwen38"], {"qwen38"}), [])


class BranchAdmissionTests(unittest.TestCase):
    """Defect 3: never accept a job whose branch cannot start."""

    def _stub_prober(self, present: set[str], transport: set[str] | None = None):
        transport = transport or set()

        def prober(url, branch):
            self.assertTrue(ba.valid_branch_name(branch), branch)
            if branch in transport:
                return "transport_error"
            return "present" if branch in present else "absent"

        return prober

    def test_head_present_is_accepted(self):
        out = ba.branch_admission(
            repo="graeae-hive",
            head_branch="fix/x",
            base_branch="main",
            prober=self._stub_prober({"fix/x"}),
            resolver=lambda repo: "ssh://argona@argonas/hive",
        )
        self.assertTrue(out["ok"])
        self.assertEqual(out["state"], "head_present")
        self.assertIsNone(out["branch_created_from"])

    def test_missing_head_with_base_is_accepted_and_records_created_from(self):
        out = ba.branch_admission(
            repo="graeae-hive",
            head_branch="fix/gone",
            base_branch="main",
            prober=self._stub_prober({"main"}),
            resolver=lambda repo: "ssh://argona@argonas/hive",
        )
        self.assertTrue(out["ok"], out)
        self.assertEqual(out["state"], "head_creatable_from_base")
        self.assertEqual(out["branch_created_from"], "main")

    def test_missing_head_without_base_is_rejected_naming_the_ref(self):
        out = ba.branch_admission(
            repo="graeae-hive",
            head_branch="fix/gone",
            base_branch=None,
            prober=self._stub_prober(set()),
            resolver=lambda repo: "ssh://argona@argonas/hive",
        )
        self.assertFalse(out["ok"])
        self.assertEqual(out["reason"], "missing_ref")
        self.assertIn("fix/gone", out["detail"])

    def test_invalid_probe_input_is_not_reported_as_absent(self):
        """An unusable URL/ref must not masquerade as 'the branch is missing'."""
        ba.clear_probe_cache()
        try:
            self.assertNotEqual(ba.probe_remote_branch("ext::sh -c 'rm -rf /'", "main"), "absent")
            self.assertEqual(ba.probe_remote_branch("ext::sh -c 'rm -rf /'", "main"), "transport_error")
            self.assertEqual(ba.probe_remote_branch("ssh://argona@argonas/h", "bad ref"), "transport_error")
        finally:
            ba.clear_probe_cache()

    def test_result_has_fixed_shape_on_every_path(self):
        """head_state/base_state exist even on early returns (no KeyError)."""
        for kwargs in (
            {"repo": "graeae-hive", "head_branch": None, "base_branch": None},
            {"repo": "", "head_branch": "fix/x", "base_branch": None},
            {"repo": "graeae-hive", "head_branch": "-bad", "base_branch": None},
        ):
            out = ba.branch_admission(prober=self._stub_prober(set()), resolver=lambda r: None, **kwargs)
            for key in ("ok", "state", "warnings", "branch_created_from", "head_state", "base_state"):
                self.assertIn(key, out, (key, kwargs))

    def test_head_branch_none_is_its_own_state_not_head_present(self):
        out = ba.branch_admission(
            repo="graeae-hive",
            head_branch=None,
            base_branch=None,
            prober=self._stub_prober(set()),
            resolver=lambda repo: "ssh://argona@argonas/hive",
        )
        self.assertTrue(out["ok"])
        self.assertEqual(out["state"], "no_head_branch")
        self.assertNotEqual(out["state"], "head_present")

    def test_transport_error_fails_open(self):
        out = ba.branch_admission(
            repo="graeae-hive",
            head_branch="fix/x",
            base_branch=None,
            prober=self._stub_prober(set(), transport={"fix/x"}),
            resolver=lambda repo: "ssh://argona@argonas/hive",
        )
        self.assertTrue(out["ok"], out)
        self.assertEqual(out["state"], "unreachable")
        self.assertTrue(out["warnings"])

    def test_unresolvable_repo_is_not_probed_and_warns(self):
        out = ba.branch_admission(
            repo="hive",  # the known ambiguous alias
            head_branch="fix/x",
            base_branch=None,
            prober=self._stub_prober(set()),
            resolver=lambda repo: None,
        )
        self.assertTrue(out["ok"])
        self.assertEqual(out["state"], "unprobed")

    def test_invalid_branch_name_is_rejected_before_git(self):
        for bad in ("-rf", "a b", "a;rm", "x..y", "a@{b", "../etc", "a.lock", "/abs", "@", "@main"):
            out = ba.branch_admission(
                repo="graeae-hive",
                head_branch=bad,
                base_branch=None,
                prober=self._stub_prober(set()),
                resolver=lambda repo: "ssh://argona@argonas/hive",
            )
            self.assertFalse(out["ok"], bad)
            self.assertEqual(out["reason"], "invalid_ref", bad)

    def test_valid_branch_names_accepted(self):
        for good in ("main", "fix/hive-submitter-controls-20261025", "release/2026.10", "a_b+c(d)e"):
            self.assertTrue(ba.valid_branch_name(good), good)

    def test_hostile_remote_urls_are_refused(self):
        for bad in (
            "ext::sh -c 'rm -rf /'",
            "fd::3",
            "file:///etc/passwd",
            "-oProxyCommand=touch /tmp/x",
            "ssh://a b/c",
            "ssh://evil/; rm -rf /",
            "",
            # 2026-10-25: bare absolute paths are accepted ONLY under the
            # ARGONAS NFS mount prefix, never arbitrary local paths.
            "/etc",
            "/root/.ssh/known_hosts",
            "/srv/git/hive.git",
            "/tmp/hive.git",
        ):
            self.assertFalse(ba.valid_remote_url(bad), bad)
        for good in (
            "ssh://argona@argonas/srv/git/hive.git",
            "git@argonas:hive.git",
            "/mnt/datapool/git/hive.git",
            "/mnt/datapool/git/graeae-hive.git",
        ):
            self.assertTrue(ba.valid_remote_url(good), good)

    def test_probe_passes_argv_list_never_a_shell(self):
        cache_before = dict(ba._PROBE_CACHE)
        ba.clear_probe_cache()
        try:
            done = mock.Mock(returncode=0, stdout="abc123\trefs/heads/main\n", stderr="")
            with mock.patch.object(ba.subprocess, "run", return_value=done) as run:
                self.assertEqual(ba.probe_remote_branch("ssh://argona@argonas/h", "main"), "present")
            _args, kwargs = run.call_args
            argv = _args[0]
            self.assertIsInstance(argv, list)
            self.assertEqual(argv[:3], ["git", "ls-remote", "--heads"])
            self.assertIn("shell", kwargs)
            self.assertFalse(kwargs["shell"], "git must never be run through a shell")
        finally:
            ba.clear_probe_cache()
            ba._PROBE_CACHE.update(cache_before)

    def test_transport_error_is_not_cached(self):
        ba.clear_probe_cache()
        try:
            fail = mock.Mock(returncode=128, stdout="", stderr="ssh: connect failed")
            ok = mock.Mock(returncode=0, stdout="abc\trefs/heads/main\n", stderr="")
            with mock.patch.object(ba.subprocess, "run", side_effect=[fail, ok]) as run:
                self.assertEqual(ba.probe_remote_branch("ssh://argona@argonas/h2", "main"), "transport_error")
                # A transient failure must be re-probed, not served from cache.
                self.assertEqual(ba.probe_remote_branch("ssh://argona@argonas/h2", "main"), "present")
            self.assertEqual(run.call_count, 2)
        finally:
            ba.clear_probe_cache()

    def test_bad_probe_timeout_env_does_not_crash(self):
        with mock.patch.dict(os.environ, {"HIVE_BRANCH_PROBE_TIMEOUT": "not-a-number"}):
            self.assertEqual(ba._env_float("HIVE_BRANCH_PROBE_TIMEOUT", 15.0, minimum=1.0, maximum=120.0), 15.0)
        with mock.patch.dict(os.environ, {"HIVE_BRANCH_PROBE_TIMEOUT": "9999"}):
            self.assertEqual(ba._env_float("HIVE_BRANCH_PROBE_TIMEOUT", 15.0, minimum=1.0, maximum=120.0), 120.0)


class SubmitBranchEndToEndTests(unittest.IsolatedAsyncioTestCase):
    """The REAL router path (branch_admission not stubbed) on a private DB.

    Kept in its own class because it rebinds AGENT_BUS_DB: running it inside
    the shared-DB classes made every sibling test order-dependent.
    """

    async def asyncSetUp(self):
        self._td = tempfile.TemporaryDirectory()
        self._old_db = os.environ.get("AGENT_BUS_DB")
        self._old_repo = deps._HIVE_REPO
        os.environ["AGENT_BUS_DB"] = os.path.join(self._td.name, "e2e.db")
        # hive.bus.deps caches the repository in a MODULE GLOBAL. Rebinding the
        # env var alone does NOT take effect once another class has resolved
        # it, which made this class order-dependent. Drop the cache so the new
        # AGENT_BUS_DB is honoured, and restore it afterwards.
        deps._HIVE_REPO = None
        await _default_hive_repo().init()

    async def asyncTearDown(self):
        if self._old_db is None:
            os.environ.pop("AGENT_BUS_DB", None)
        else:
            os.environ["AGENT_BUS_DB"] = self._old_db
        deps._HIVE_REPO = self._old_repo
        ba.clear_probe_cache()
        self._td.cleanup()

    async def test_missing_head_branch_is_422_and_creatable_becomes_200(self):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t") as client:
            reg = await client.post(
                "/v1/agents/register",
                json={
                    "kind": "opencode",
                    "host": "studio",
                    "runtime": "opencode",
                    "status": "online",
                    "capabilities": ["code"],
                    "cost_tier": "B",
                    "auth_method": "api",
                },
            )
            submitter = reg.json()["urn"]

            # branch_admission's prober/resolver defaults are bound at def-time,
            # so patching the module names does nothing. Patch the ROUTER's
            # imported symbol with a partial that supplies our stubs directly.
            def _fake_admission(*args, **kwargs):
                repo = kwargs.get("repo", args[0] if len(args) > 0 else None)
                head_branch = kwargs.get("head_branch", args[1] if len(args) > 1 else None)
                base_branch = kwargs.get("base_branch", args[2] if len(args) > 2 else None)
                return ba.branch_admission(
                    repo=repo,
                    head_branch=head_branch,
                    base_branch=base_branch,
                    prober=lambda url, b: "present" if b == "main" else "absent",
                    resolver=lambda repo: "ssh://argona@argonas/h",
                )

            with mock.patch("hive.bus.routers.jobs.branch_admission", side_effect=_fake_admission):
                # head missing, no base -> 422 naming the ref.
                bad = await client.post(
                    "/v1/jobs",
                    json={
                        "kind": "code-edit",
                        "description": "d",
                        "submitter_urn": submitter,
                        "site": "internal",
                        "repo": "graeae-hive",
                        "head_branch": "fix/does-not-exist",
                    },
                )
                self.assertEqual(bad.status_code, 422, bad.text)
                self.assertIn("fix/does-not-exist", bad.text)

                # head missing, base present -> 200, branch_created_from set.
                good = await client.post(
                    "/v1/jobs",
                    json={
                        "kind": "code-edit",
                        "description": "d",
                        "submitter_urn": submitter,
                        "site": "internal",
                        "repo": "graeae-hive",
                        "head_branch": "fix/does-not-exist",
                        "base_branch": "main",
                    },
                )
                self.assertEqual(good.status_code, 200, good.text)
                self.assertEqual(good.json()["branch_created_from"], "main")


class PersistenceMigrationTests(unittest.TestCase):
    """Reviewer finding: branch_created_from must actually reach both backends."""

    def test_sqlite_declares_branch_created_from(self):
        """sqlite's ensure_schema carries the column in its additive migration list."""
        import inspect

        from hive.persistence import sqlite as sqlite_mod

        src = inspect.getsource(sqlite_mod)
        self.assertIn('("jobs", "branch_created_from", "branch_created_from TEXT")', src)

    def test_db2_declares_branch_created_from(self):
        names = {n for n, _ in _DB2_ADDITIVE_COLUMNS.get("HIVE_JOBS", ())}
        self.assertIn("BRANCH_CREATED_FROM", names)

    def test_oracle_declares_branch_created_from(self):
        self.assertIn("BRANCH_CREATED_FROM", _ORACLE_JOB_ADDITIONS)


class ClaimDependencyGateTests(unittest.IsolatedAsyncioTestCase):
    """depends_on is enforced at CLAIM time (fault 2026-10-25).

    Two jobs whose dependency had not finished were claimed anyway. The
    dequeue path's candidate pre-read was advisory only, and the targeted
    claim's pre-read was fail-OPEN on malformed ``depends_on`` (the router's
    ``json_list`` swallows bad JSON to []). Now:

      * the pre-scan excludes jobs with unsatisfied deps BEFORE the
        LIMIT-bound window, so a high-priority blocked job cannot crowd
        eligible ones out of the window and take the claim slot;
      * ``claim_job``'s in-transaction re-check (fail-closed) is the binding
        gate on BOTH claim paths;
      * the targeted claim answers 409 for an unmet, malformed, or missing
        dep instead of handing out the job.

    The repo is constructed directly (same pattern as
    test_job_prefs_deps_20261017) so no live DB is touched; the router half
    is covered by the targeted-claim tests, which drive the real
    ``claim_job``.
    """

    async def _tmp_repo(self):
        import tempfile as _tf
        from pathlib import Path as _P

        td = _tf.TemporaryDirectory()
        self.addCleanup(td.cleanup)
        repo = sqlite_mod.SqliteHiveRepository(str(_P(td.name) / "claimdeps.db"))
        await repo.init(agent_bus.SCHEMA)
        return repo

    async def _mk(self, repo, case, *rows):
        """rows: (id, status, deps_json)"""
        for job_id, status, deps_json in rows:
            await case._insert(repo, job_id, status=status)
            if deps_json is not None:
                async with repo.connection() as db:
                    await db.execute("UPDATE jobs SET depends_on=? WHERE id=?", (deps_json, job_id))
                    await db.commit()

    async def test_dequeue_pref_scan_excludes_blocked_high_priority(self):
        """A blocked job with HIGHER priority must not consume the claim slot.

        Regression for the 2026-10-25 incident: the pre-scan ran
        LIMIT-bounded and the blocked job was only filtered in the router
        loop, so if more blocked/high-priority jobs than the window could
        absorb sat ahead of it, an eligible job fell out of the window
        entirely. The pre-scan must exclude unsatisfied deps BEFORE the LIMIT.
        """
        repo = await self._tmp_repo()
        case = _RepoCaseShim(self)
        await self._mk(
            repo,
            case,
            ("dep", "queued", None),
            ("blocked", "queued", json.dumps(["dep"])),
            ("ready", "queued", None),
        )
        # blocked is highest priority, ready lowest.
        async with repo.connection() as db:
            await db.execute("UPDATE jobs SET priority=10 WHERE id='blocked'")
            await db.execute("UPDATE jobs SET priority=-1 WHERE id='ready'")
            await db.commit()

        now = 1_000_000.0
        async with repo.connection() as tx:
            blocked = await repo.jobs_with_unsatisfied_deps(tx, ids=["dep", "blocked", "ready"])
            self.assertEqual(blocked, {"blocked"}, blocked)
            # With the hint: blocked is gone, dep and ready are returned.
            cands = await repo.queued_candidates(
                tx,
                agent_urn="urn:agent:zeroclaw:studio:claimdeps",
                now=now,
                limit=10,
                unsatisfied_dep_job_ids=blocked,
            )
            # 'dep' itself has no depends_on, so it stays a candidate; the
            # point of the test is that the HIGH-PRIORITY blocked job is out.
            self.assertEqual([r[0] for r in cands], ["dep", "ready"], [r[0] for r in cands])
            # Without the hint (legacy call): blocked is present -- the
            # advisory nature is documented, the binding gate is claim_job.
            cands_unhinted = await repo.queued_candidates(
                tx,
                agent_urn="urn:agent:zeroclaw:studio:claimdeps",
                now=now,
                limit=10,
            )
            self.assertIn("blocked", [r[0] for r in cands_unhinted])

    async def test_claim_job_refuses_unmet_dependency_in_transaction(self):
        """The binding gate: claim_job refuses even when the caller pre-read said OK."""
        repo = await self._tmp_repo()
        case = _RepoCaseShim(self)
        await self._mk(repo, case, ("dep", "queued", None), ("blocked", "queued", None))
        async with repo.connection() as db:
            await db.execute(
                "UPDATE jobs SET depends_on=? WHERE id='blocked'",
                (json.dumps(["dep"]),),
            )
            await db.commit()

        now = time.time()
        async with repo.connection() as tx:
            with self.assertRaises(DependencyNotReady):
                await repo.claim_job(
                    tx,
                    job_id="blocked",
                    agent_urn="urn:agent:zeroclaw:studio:claimdeps",
                    now=now,
                    lease_expires_at=now + 60,
                    runtime="zeroclaw",
                    model="qwen38",
                    provider="local",
                    cost_tier="B",
                )
            async with tx.execute("SELECT status FROM jobs WHERE id='blocked'") as cur:
                self.assertEqual((await cur.fetchone())[0], "queued")

    async def test_claim_job_refuses_dependency_finished_between_preread_and_claim(self):
        """The dep was done at pre-scan time, then re-queued: claim still fails."""
        repo = await self._tmp_repo()
        case = _RepoCaseShim(self)
        await self._mk(repo, case, ("dep", "done", None), ("blocked", "queued", json.dumps(["dep"])))
        now = time.time()
        async with repo.connection() as tx:
            # Pre-scan would see the dep as done ...
            pre = await repo.jobs_with_unsatisfied_deps(tx, ids=["blocked"])
            self.assertEqual(pre, set(), pre)
            # ... then the dep is re-queued (operator retry, a failed dep, etc.)
            await tx.execute("UPDATE jobs SET status='queued' WHERE id='dep'")
            # ... and the in-transaction gate must still refuse.
            with self.assertRaises(DependencyNotReady):
                await repo.claim_job(
                    tx,
                    job_id="blocked",
                    agent_urn="urn:agent:zeroclaw:studio:claimdeps",
                    now=now,
                    lease_expires_at=now + 60,
                    runtime="zeroclaw",
                    model="qwen38",
                    provider="local",
                    cost_tier="B",
                )

    async def test_jobs_with_unsatisfied_deps_fails_closed(self):
        """Malformed JSON and missing dep rows are unsatisfied, not open."""
        repo = await self._tmp_repo()
        case = _RepoCaseShim(self)
        await self._mk(
            repo,
            case,
            ("open", "queued", None),
            ("bad-json", "queued", None),
            ("not-list", "queued", None),
            ("missing-dep", "queued", json.dumps(["ghost"])),
            ("dep", "queued", None),
            ("open2", "queued", None),
            ("dup-open", "queued", json.dumps(["dep", "open2"])),
        )
        async with repo.connection() as db:
            await db.execute("UPDATE jobs SET depends_on='{not json' WHERE id='bad-json'")
            await db.execute(
                "UPDATE jobs SET depends_on=? WHERE id=?",
                ('{"a": 1}', "not-list"),
            )
            await db.commit()
        async with repo.connection() as tx:
            unsat = await repo.jobs_with_unsatisfied_deps(
                tx,
                ids=["open", "bad-json", "not-list", "missing-dep", "dup-open"],
            )
        # open (no deps) is the only open case; malformed JSON, a missing dep
        # row, and a well-formed list of not-yet-done deps are all unsatisfied.
        self.assertEqual(unsat, {"bad-json", "not-list", "missing-dep", "dup-open"}, unsat)

    async def test_jobs_with_unsatisfied_deps_empty_or_null_is_open(self):
        """NULL/empty depends_on is satisfied (negative control)."""
        repo = await self._tmp_repo()
        case = _RepoCaseShim(self)
        await self._mk(
            repo,
            case,
            ("no-deps", "queued", None),
            ("empty-list", "queued", "[]"),
            ("done-dep", "done", None),
            ("satisfied", "queued", None),
        )
        async with repo.connection() as db:
            await db.execute(
                "UPDATE jobs SET depends_on=? WHERE id='satisfied'",
                (json.dumps(["done-dep"]),),
            )
            await db.commit()
        async with repo.connection() as tx:
            unsat = await repo.jobs_with_unsatisfied_deps(tx, ids=["no-deps", "empty-list", "satisfied"])
        self.assertEqual(unsat, set(), unsat)

    async def test_targeted_claim_409_for_unmet_dependency(self):
        """The real router path: /v1/jobs/{id}/claim -> 409 when the dep is not done."""
        repo = await self._tmp_repo()
        case = _RepoCaseShim(self)
        await self._mk(repo, case, ("dep", "queued", None), ("blocked", "queued", json.dumps(["dep"])))
        urn = "urn:agent:zeroclaw:studio:claimdeps"
        now = 1_000_000.0
        async with repo.connection() as db:
            await db.execute(
                "INSERT INTO agents (urn, kind, host, session_id, capabilities, started_at, "
                "last_heartbeat, status, runtime, cost_tier) "
                "VALUES (?, 'zeroclaw', 'studio', 'claimdeps', '[]', ?, ?, 'online', 'zeroclaw', 'B')",
                (urn, now, now),
            )
            await db.commit()

        with (
            mock.patch.object(routers_jobs, "hive_repo", lambda: repo),
            mock.patch.object(routers_jobs, "emit_event", mock.AsyncMock()),
            mock.patch.object(routers_jobs, "job_agent_eligible", lambda job, agent: (True, "")),
        ):
            with self.assertRaises(HTTPException) as ctx:
                await routers_jobs.claim_job("blocked", by=urn)
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertIn("DAG dependencies", str(ctx.exception.detail))

    async def test_targeted_claim_negative_control_open_dependency(self):
        """Negative control: a done dep -> the claim goes through (200)."""
        repo = await self._tmp_repo()
        case = _RepoCaseShim(self)
        await self._mk(repo, case, ("dep", "done", None), ("ready", "queued", json.dumps(["dep"])))
        urn = "urn:agent:zeroclaw:studio:claimdeps"
        now = time.time()
        async with repo.connection() as db:
            await db.execute(
                "INSERT INTO agents (urn, kind, host, session_id, capabilities, started_at, "
                "last_heartbeat, status, runtime, cost_tier) "
                "VALUES (?, 'zeroclaw', 'studio', 'claimdeps', '[]', ?, ?, 'online', 'zeroclaw', 'B')",
                (urn, now, now),
            )
            await db.commit()

        with (
            mock.patch.object(routers_jobs, "hive_repo", lambda: repo),
            mock.patch.object(routers_jobs, "emit_event", mock.AsyncMock()),
            mock.patch.object(routers_jobs, "job_agent_eligible", lambda job, agent: (True, "")),
        ):
            out = await routers_jobs.claim_job("ready", by=urn)
        self.assertTrue(out["claimed"])


class _RepoCaseShim:
    """Minimal stand-in for test_job_prefs_deps_20261017's _RepoCase helpers."""

    def __init__(self, case):
        self._case = case

    async def _insert(self, repo, job_id, *, status="queued", **_kw):
        now = time.time()
        async with repo.connection() as db:
            await db.execute(
                "INSERT INTO jobs (id, submitter_urn, kind, priority, status, started_at) "
                "VALUES (?, 'urn:submitter', 'build', 0, ?, ?)",
                (job_id, status, now),
            )
            await db.commit()


if __name__ == "__main__":
    unittest.main()
