"""Round 7: the findings muse-spark-1.3-contributor returned against r6.

Each finding was verified against the source before anything changed. A REAL
finding has a test that fails on the r6 tree; a DISPROVED one has a pinning
test that passes on both trees and fails if the correct behaviour regresses.

* HIGH "healthy_alternative does not catch health-probe exceptions" --
  DISPROVED. The probe call is wrapped (``except Exception: continue``); a
  raising probe reads as unhealthy and the sweep does not crash.
* HIGH "the INFRA_RETRIES plan does not set the override, so the admin requeue
  is refused again" -- DISPROVED for the plan: it carries
  ``override=True`` and a non-empty ``override_reason``, the doctor's executor
  posts them as ``override_max_retries``/``override_reason``, and the REAL
  admin route requeues a spent row with that body. Verifying it surfaced a
  REAL defect one layer down: the route answers ``200`` even when it refused
  the row (``skipped_retry_budget``/``skipped_conflict``), and
  ``remediate_one`` judged success on the HTTP code alone, so a refused requeue
  was recorded as APPLIED and spent an attempt. It now reads the body.
* HIGH "MISSING_BRANCH preserves a valid-but-missing head branch" -- REAL. The
  plan probed only the base branch; a head branch that does not exist on the
  remote was kept and the row requeued onto it. The head is now probed: a
  missing head is repaired by pushing the preserved workspace HEAD to it, or
  the row is not requeued at all.
* MEDIUM "inconsistent case normalization of capped_routes" -- DISPROVED as a
  behaviour: every consumer (``model_problem``, ``healthy_alternative``)
  lower-cases both sides before comparing, so the spelling of a route never
  changes the classification or the chosen alternative.
* INFO "is_composite is broader than its comment" -- REAL. It accepted any
  36-char hex with a letter in it (and rejected an all-digit composite). It
  now matches the real shape: 36 hex chars, checked against
  ``head_sha[:28]`` / the workspace uuid tail when the caller knows them, and
  never a value the repository resolves.
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from hive import delivery_ref
from hive.workers import doctor_remediation as dr
from hive.workers.doctor_state import DurableStateStore, MemoryBackend

FIXTURE = Path(__file__).parent / "tests" / "fixtures" / "doctor_remediation_rows.json"
BASE = "deploy/integrate-20261017"
ROSTER = [
    {"id": "tydeus-qwen38", "model_id": "qwen38", "family": "alibaba", "cost_class": "A", "concurrency": 4},
    {"id": "cerberus-gemma", "model_id": "gemma4-31b", "family": "google", "cost_class": "A", "concurrency": 2},
    {"id": "minimax-m3", "model_id": "MiniMax-M3", "family": "minimax", "cost_class": "B", "concurrency": 2},
]


def row(name: str, **overrides) -> dict:
    data = json.loads(FIXTURE.read_text())
    base = json.loads(json.dumps(data[name]))
    result_overrides = overrides.pop("result", None)
    base.update(overrides)
    if result_overrides is not None:
        base["result"] = {**base.get("result", {}), **result_overrides}
    return base


@pytest.fixture
def state():
    return DurableStateStore(MemoryBackend(), refresh_secs=0.0)


class Fake:
    """The injected side effects, recorded in ONE ordered log."""

    def __init__(self, reclassify_resp=None, push_ok=True):
        self.log: list[tuple] = []
        self._reclassify_resp = reclassify_resp
        self._push_ok = push_ok

    def apply_fields(self, jid, changes, reason):
        self.log.append(("fields", jid, changes))
        return 200, {"ok": [jid]}

    def reclassify(self, jid, status, reason, override, override_reason):
        self.log.append(("reclassify", jid, status, override, override_reason))
        return 200, (self._reclassify_resp if self._reclassify_resp is not None else {"ok": [jid]})

    def escalate(self, job, why):
        self.log.append(("escalate", str(job.get("id")), why))

    def rescue(self, job, branch):
        self.log.append(("rescue", str(job.get("id")), branch))
        return (True, "pushed") if self._push_ok else (False, "push rejected")

    def kinds(self):
        return [entry[0] for entry in self.log]

    def run(self, job, state, **kw):
        kw.setdefault("default_branch", lambda _j: BASE)
        kw.setdefault("roster", ROSTER)
        return dr.remediate_one(
            job,
            state=state,
            apply_fields=self.apply_fields,
            reclassify=self.reclassify,
            escalate=self.escalate,
            rescue_workspace=self.rescue,
            now=1_800_000_000.0,
            **kw,
        )


# ── HIGH 1 (disproved): a raising health probe is unhealthy, not a crash ────


def _raises_for(*route_ids):
    def probe(route):
        if route.get("id") in route_ids:
            raise TimeoutError("health probe timed out")
        return True

    return probe


def test_r7_disproved_healthy_alternative_treats_a_raising_probe_as_unhealthy():
    job = row("unavailable_model")
    # The cheapest route raises: it must be skipped, not chosen and not fatal.
    chosen = dr.healthy_alternative(job, roster=ROSTER, health=_raises_for("cerberus-gemma"))
    assert chosen is not None and chosen["id"] == "tydeus-qwen38"
    chosen = dr.healthy_alternative(job, roster=ROSTER, health=_raises_for("tydeus-qwen38"))
    assert chosen["id"] == "cerberus-gemma"
    # Every probe raises: no route is proven healthy, so there is no alternative.
    assert dr.healthy_alternative(job, roster=ROSTER, health=_raises_for(*[r["id"] for r in ROSTER])) is None


def test_r7_disproved_a_raising_probe_does_not_crash_the_sweep(state):
    fake = Fake()

    def always_raises(_route):
        raise RuntimeError("probe exploded")

    summary = dr.sweep(
        [row("unavailable_model")],
        state=state,
        apply_fields=fake.apply_fields,
        reclassify=fake.reclassify,
        escalate=fake.escalate,
        default_branch=lambda _j: BASE,
        roster=ROSTER,
        health=always_raises,
        now=1_800_000_000.0,
    )
    assert "errors" not in summary, summary
    assert summary[dr.APPLIED] == 0
    assert summary.get(dr.NO_CHANGE) == 1, summary
    assert fake.log == []


# ── HIGH 2 (disproved for the plan): the override is set and honoured ───────


def test_r7_disproved_the_infra_plan_carries_the_override_the_route_requires():
    job = row("infra_retries")
    cause = dr.classify(job)
    assert cause is not None and cause.name == dr.INFRA_RETRIES
    plan = dr.plan_for(job, cause, default_branch=BASE)
    assert plan.requeue is True
    assert plan.override is True
    assert plan.override_reason.strip(), "the route refuses an override with an empty reason"


def test_r7_disproved_the_doctor_executor_posts_override_max_retries():
    from hive.workers import doctor

    posted = []
    with mock.patch.object(doctor, "_http", lambda m, p, b=None, timeout=0: posted.append((m, p, b)) or (200, {})):
        doctor._remediate_reclassify("j1", "queued", "infra_retry:http_503", True, "infra_retry:http_503")
    method, path, body = posted[0]
    assert (method, path) == ("POST", "/v1/admin/jobs/reclassify")
    assert body["status"] == "queued"
    assert body["override_max_retries"] is True
    assert body["override_reason"] == "infra_retry:http_503"


def test_r7_real_a_200_that_refused_the_requeue_is_not_recorded_as_applied(state):
    """The route answers 200 with the row under ``skipped_retry_budget`` when
    it refuses a requeue. That is not a fix: no attempt is spent, no marker is
    written, and the outcome says what the bus said. Fails on r6 (APPLIED)."""
    job = row("infra_retries")
    jid = job["id"]
    fake = Fake(reclassify_resp={"ok": [], "skipped_retry_budget": [jid]})
    outcome = fake.run(job, state)
    assert outcome.status == dr.FAILED, outcome
    assert "skipped_retry_budget" in outcome.error
    assert dr.attempts(state, jid) == 0
    # The next sweep is free to try again (the refusal fixed nothing).
    again = Fake().run(job, state)
    assert again.status == dr.APPLIED


def test_r7_a_response_without_a_job_list_is_still_judged_by_its_code(state):
    """Pin: a 2xx body that carries no per-row lists (an older bus, a fake) is
    not second-guessed; only an explicit refusal of THIS row fails."""
    job = row("infra_retries")
    outcome = Fake(reclassify_resp={}).run(job, state)
    assert outcome.status == dr.APPLIED


class TestInfraOverrideAgainstTheRealRoute:
    """The doctor's own executor body, driven through the live admin route."""

    @classmethod
    def setup_class(cls):
        os.environ.setdefault("HIVE_BACKEND", "sqlite")
        import agent_bus
        from hive.bus import deps as _deps
        from hive.bus.deps import hive_repo

        cls._td = tempfile.TemporaryDirectory()
        cls._env = mock.patch.dict(
            os.environ,
            {"AGENT_BUS_DB": os.path.join(cls._td.name, "r7.db"), "HIVE_BACKEND": "sqlite"},
        )
        cls._env.start()
        for var in ("MNEMOS_URL", "MNEMOS_TOKEN", "HIVE_SHARED_SECRET", "HIVE_BUS_ADMIN_TOKEN"):
            os.environ.pop(var, None)
        cls._repo_patch = mock.patch.object(_deps, "_HIVE_REPO", None)
        cls._repo_patch.start()
        cls.app = agent_bus.app
        cls.hive_repo = staticmethod(hive_repo)
        cls._run(hive_repo().init())
        reg = {
            "kind": "opencode",
            "host": "m1",
            "runtime": "opencode",
            "status": "online",
            "capabilities": ["code"],
            "cost_tier": "B",
        }
        cls.urn = cls._call("POST", "/v1/agents/register", reg)[1]["urn"]

    @classmethod
    def teardown_class(cls):
        cls._repo_patch.stop()
        cls._env.stop()
        cls._td.cleanup()

    @staticmethod
    def _run(coro):
        return asyncio.new_event_loop().run_until_complete(coro)

    @classmethod
    def _call(cls, method, path, body=None):
        import httpx

        async def _do():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=cls.app), base_url="http://t") as c:
                headers = {"X-Hive-Actor": cls.urn} if getattr(cls, "urn", None) else {}
                r = await c.request(method, path, json=body, headers=headers)
                return r.status_code, r.json()

        return cls._run(_do())

    def _spent_row(self, status):
        code, data = self._call(
            "POST", "/v1/jobs", {"kind": "loop", "description": "spent budget", "submitter_urn": self.urn}
        )
        assert code in (200, 201), data
        jid = data["id"]
        repo = self.hive_repo()

        async def _seed():
            async with repo.connection() as tx:
                await repo.patch_job_status(
                    tx, jid, sets={"status": status, "max_retries": 2, "retry_count": 2, "result": None}
                )
                await tx.commit()

        self._run(_seed())
        return jid

    def _doctor_reclassify(self, jid, override):
        from hive.workers import doctor

        job = row("infra_retries", id=jid)
        plan = dr.plan_for(job, dr.classify(job), default_branch=BASE)
        bus = lambda m, p, b=None, timeout=0: self._call(m, p, b)  # noqa: E731
        with mock.patch.object(doctor, "_http", bus):
            return doctor._remediate_reclassify(
                jid, "queued", plan.reason, plan.override if override else False, plan.override_reason
            )

    @pytest.mark.parametrize("status", ["needs-review", "failed", "dead-letter"])
    def test_r7_disproved_the_doctor_override_requeues_a_spent_row(self, status):
        jid = self._spent_row(status)
        code, data = self._doctor_reclassify(jid, override=True)
        assert code == 200, data
        assert data["ok"] == [jid], data
        assert self._call("GET", f"/v1/jobs/{jid}")[1]["status"] == "queued"

    def test_r7_without_the_override_the_route_refuses_with_a_200(self):
        """Why the body must be read: the refusal is a 200 too."""
        jid = self._spent_row("failed")
        code, data = self._doctor_reclassify(jid, override=False)
        assert code == 200
        assert data["skipped_retry_budget"] == [jid] and data["ok"] == []
        assert self._call("GET", f"/v1/jobs/{jid}")[1]["status"] == "failed"


# ── HIGH 3 (real): a valid head branch that does not exist on the remote ────


def _remote(*branches):
    """A branch probe that knows exactly ``branches``, recording every ask."""
    asked: list[str] = []

    def exists(name):
        asked.append(name)
        return name in branches

    exists.asked = asked
    return exists


def test_r7_real_a_missing_head_branch_without_a_workspace_is_not_requeued(state):
    job = row("missing_base")
    probe = _remote(BASE)  # the base exists; the head branch does not
    fake = Fake()
    outcome = fake.run(job, state, branch_exists=probe)
    assert job["head_branch"] in probe.asked, "the head branch must be checked on the remote"
    assert outcome.status == dr.NO_CHANGE, outcome
    assert "reclassify" not in fake.kinds(), "a row whose head branch is missing must not be requeued"
    assert "fields" not in fake.kinds()


def test_r7_real_a_missing_head_branch_is_repaired_from_the_workspace_head(state, tmp_path):
    workspace = str(tmp_path / "ws" / "01a18c60-4d18-7a33-8f2b-6c1e9a04d7b3")
    job = row("missing_base", result={"workspace_preserved": workspace})
    fake = Fake()
    outcome = fake.run(job, state, branch_exists=_remote(BASE))
    assert outcome.status == dr.APPLIED, outcome
    # The workspace HEAD is pushed to the head branch BEFORE the row records
    # ``workspace_pushed`` and before the requeue, so the queued row points at a
    # branch that now exists (see the closing section for why the push leads).
    assert fake.kinds() == ["rescue", "fields", "reclassify"], fake.log
    assert fake.log[0] == ("rescue", job["id"], job["head_branch"])
    assert fake.log[2][2] == "queued"
    changes = fake.log[1][2]
    assert changes["base_branch"]["after"] == BASE
    assert changes["workspace_pushed"] == {"before": False, "after": True}


def test_r7_real_a_failed_workspace_push_does_not_requeue(state, tmp_path):
    job = row("missing_base", result={"workspace_preserved": str(tmp_path / "ws")})
    fake = Fake(push_ok=False)
    outcome = fake.run(job, state, branch_exists=_remote(BASE))
    assert outcome.status == dr.FAILED
    assert "reclassify" not in fake.kinds()


def test_r7_real_an_unreadable_head_probe_is_not_proof_the_head_exists(state):
    job = row("missing_base")

    def probe(name):
        if name == BASE:
            return True
        raise OSError("ls-remote failed")

    fake = Fake()
    outcome = fake.run(job, state, branch_exists=probe)
    assert outcome.status == dr.NO_CHANGE
    assert "reclassify" not in fake.kinds()


def test_r7_pin_an_existing_head_branch_is_kept_and_requeued(state):
    job = row("missing_base")
    probe = _remote(BASE, job["head_branch"])
    fake = Fake()
    outcome = fake.run(job, state, branch_exists=probe)
    assert outcome.status == dr.APPLIED
    assert probe.asked[0] == BASE
    changes = fake.log[0][2]
    assert "head_branch" not in changes and "workspace_pushed" not in changes
    assert "rescue" not in fake.kinds()


# ── MEDIUM (disproved): capped_routes spelling never changes the outcome ────


@pytest.mark.parametrize(
    "job_capped,caller_capped",
    [
        (["gpt-5.3-codex-spark"], {"tydeus-qwen38"}),
        (["GPT-5.3-Codex-Spark"], {"TYDEUS-QWEN38"}),
        (["Gpt-5.3-CODEX-spark"], {"Tydeus-Qwen38"}),
    ],
)
def test_r7_disproved_capped_route_spelling_does_not_change_the_fix(state, job_capped, caller_capped):
    roster = [*ROSTER, {"id": "spark", "model_id": "gpt-5.3-codex-spark", "family": "openai", "cost_class": "B"}]
    job = row("unavailable_model", capped_routes=job_capped)
    cause = dr.classify(job, roster=roster, capped=caller_capped)
    assert (cause.name, cause.detail) == (dr.UNAVAILABLE_MODEL, "capped_model")
    fake = Fake()
    outcome = fake.run(job, state, roster=roster, capped=caller_capped)
    assert outcome.status == dr.APPLIED
    # tydeus (capped by the caller, in any case) is never chosen: gemma is.
    assert fake.log[0][2]["preferred_models"]["after"] == ["gemma4-31b"]


# ── INFO (real): is_composite matches the real composite shape ──────────────

HEAD = "4b8c1e0a7d92f36b58a0c1d4e7f9b2a3c6d8e0f1"
UUID = "01a18c8a-4c37-7b99-a3d5-8e1f2a04c649"
COMPOSITE = HEAD[:28] + UUID.replace("-", "")[-8:]


def test_r7_real_is_composite_checks_the_parts_it_is_given():
    assert len(COMPOSITE) == delivery_ref.COMPOSITE_LEN == 36
    assert delivery_ref.is_composite(COMPOSITE, head=HEAD, workspace=UUID)
    other_head = "f" * 40
    assert not delivery_ref.is_composite(COMPOSITE, head=other_head), "the prefix must be head_sha[:28]"
    assert not delivery_ref.is_composite(COMPOSITE, workspace="01a18c8a-4c37-7b99-a3d5-000000000000")


def test_r7_real_a_resolvable_36_char_abbreviation_is_not_a_composite():
    # Git resolves a 36-char abbreviation of a real commit: that is a commit.
    abbrev = HEAD[:36]
    assert not delivery_ref.is_composite(abbrev, ref_exists=lambda ref: HEAD.startswith(ref))
    assert delivery_ref.is_composite(COMPOSITE, ref_exists=lambda ref: HEAD.startswith(ref))


def test_r7_real_an_all_digit_composite_is_still_a_composite():
    head = "1234567890" * 4
    uuid = "00000000-0000-7000-8000-001122334455"
    composite = head[:28] + uuid.replace("-", "")[-8:]
    assert composite.isdigit()
    assert delivery_ref.is_composite(composite, head=head, workspace=uuid)


def test_r7_pin_is_composite_rejects_every_other_shape():
    assert not delivery_ref.is_composite(HEAD)  # 40 chars: a commit
    assert not delivery_ref.is_composite(COMPOSITE[:-1])
    assert not delivery_ref.is_composite(COMPOSITE[:-1] + "g")
    assert not delivery_ref.is_composite("")
    assert not delivery_ref.is_composite(None)
    # An unreadable probe falls back to the shape answer (not proof either way).
    assert delivery_ref.is_composite(COMPOSITE, ref_exists=lambda _r: (_ for _ in ()).throw(OSError()))


# ── Closing review (real): nothing is written before the workspace push ─────
#
# The plan for a push records ``workspace_pushed: True``. Written ahead of a
# push that then failed (or had no rescuer wired in), that claim made
# ``_fields_unchanged`` report the row as already fixed on every later sweep:
# the work was never pushed and the row never moved again.


def _land(job, changes):
    """The row as the bus holds it after ``apply_fields`` wrote ``changes``."""
    landed = json.loads(json.dumps(job))
    for name, change in changes.items():
        if name in landed:
            landed[name] = change["after"]
        else:
            landed.setdefault("result", {})[name] = change["after"]
    return landed


def test_close_a_failed_push_writes_nothing_and_the_next_sweep_retries(state, tmp_path):
    job = row("missing_base", result={"workspace_preserved": str(tmp_path / "ws")})
    failed = Fake(push_ok=False)
    first = failed.run(job, state, branch_exists=_remote(BASE))
    assert first.status == dr.FAILED
    assert failed.kinds() == ["rescue"], "no field may claim a push that did not happen"
    landed = job
    for entry in failed.log:
        if entry[0] == "fields":
            landed = _land(landed, entry[2])
    retry = Fake()
    second = retry.run(landed, state, branch_exists=_remote(BASE))
    assert second.status == dr.APPLIED, second
    assert retry.kinds() == ["rescue", "fields", "reclassify"]


def test_close_a_stranded_row_with_a_failed_push_is_retried(state, tmp_path):
    job = row("stranded_wip", result={"workspace_preserved": str(tmp_path / "ws")})
    assert dr.classify(job).name == dr.STRANDED_WIP
    failed = Fake(push_ok=False)
    assert failed.run(job, state).status == dr.FAILED
    assert "fields" not in failed.kinds()
    retry = Fake()
    assert retry.run(job, state).status == dr.APPLIED
    assert retry.kinds() == ["rescue", "fields", "reclassify"]


def test_close_no_rescuer_writes_nothing(state, tmp_path):
    job = row("stranded_wip", result={"workspace_preserved": str(tmp_path / "ws")})
    assert dr.classify(job).name == dr.STRANDED_WIP
    fake = Fake()
    outcome = dr.remediate_one(
        job,
        state=state,
        apply_fields=fake.apply_fields,
        reclassify=fake.reclassify,
        escalate=fake.escalate,
        default_branch=lambda _j: BASE,
        roster=ROSTER,
        now=1_800_000_000.0,
    )
    assert outcome.status == dr.FAILED and outcome.error == "no_workspace_rescuer"
    assert fake.log == []
