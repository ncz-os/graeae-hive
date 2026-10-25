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

import os
import tempfile
import unittest
import uuid
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")
os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")
os.environ.setdefault("HIVE_BUS_ADMIN_TOKEN", "test-admin-token")

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus import deps  # noqa: E402
from hive.bus.deps import hive_repo as _default_hive_repo  # noqa: E402
from hive.bus.domain import branch_admission as ba  # noqa: E402
from hive.bus.domain.model_admission import (  # noqa: E402
    FORBIDDEN_MODEL_PROVIDERS,
    FORBIDDEN_MODELS,
    admission_warnings,
    forbidden_model,
)
from hive.persistence.db2 import _DB2_ADDITIVE_COLUMNS  # noqa: E402
from hive.persistence.oracle import _ORACLE_JOB_ADDITIONS  # noqa: E402


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
        for bad in ("-rf", "a b", "a;rm", "x..y", "a@{b", "../etc", "a.lock", "/abs"):
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
        ):
            self.assertFalse(ba.valid_remote_url(bad), bad)
        for good in ("ssh://argona@argonas/srv/git/hive.git", "git@argonas:hive.git", "/srv/git/hive.git"):
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


if __name__ == "__main__":
    unittest.main()
