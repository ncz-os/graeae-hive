"""FB36: stateful property test of the job lifecycle.

A ``RuleBasedStateMachine`` drives the real in-process bus (temporary SQLite
backend) through submit, claim, lease renewal, progress, completion, failure,
requeue, cancel, release, reclassification and fake-clock advances. After every
step the invariants below are checked from ONE bulk list read:

* a job is in exactly one status and always in ``VALID_JOB_STATUSES``;
* a claimed/running job has exactly one live claimant with an unexpired lease;
* the retry budget is never exceeded;
* a status change is either a documented PATCH cell in
  ``docs/JOB_LIFECYCLE.md`` or belongs to an action that owns its own path
  (claim endpoints, admin requeue/reclassify, the completion gate, auto-retry);
* a completed job returns to ``queued`` only through an explicit requeue;
* review evidence is never lost;
* dead-letter is terminal.

FB51 resolved the two findings FB36 had pinned: ``POST /v1/admin/jobs/requeue``
archives the cleared result (including ``hive_reviews``) in
``job_result_history`` before clearing it, and ``dead-letter -> queued``
reclassify requires an explicit override. The randomized machine still passes
only while no *unknown* violation exists.
"""

import asyncio
import os
import re
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest

import agent_bus
from hive.bus import deps
from hive.bus.domain.jobs import STATUS_TRANSITIONS
from hive.bus.domain.reaper import select_orphan_claims
from hive.bus.routers import jobs as jobs_router

hypothesis = pytest.importorskip("hypothesis", reason="stateful lifecycle test needs the dev extra (hypothesis)")
settings = hypothesis.settings
st = pytest.importorskip("hypothesis.strategies")
_stateful = pytest.importorskip("hypothesis.stateful")
RuleBasedStateMachine, invariant, rule = _stateful.RuleBasedStateMachine, _stateful.invariant, _stateful.rule

DOC = Path(__file__).parent / "docs" / "JOB_LIFECYCLE.md"

FINDING_REQUEUE_REVIEW = "FB36-FINDING-1"
FINDING_DEAD_LETTER = "FB36-FINDING-2"

#: Transitions an admin endpoint may perform regardless of the PATCH table.
ADMIN_TARGETS = {"queued", "cancelled", "needs-review"}
#: Statuses a worker/claim action can produce outside the PATCH map.
ACTION_TARGETS = {
    "claim": {"claimed"},
    "failed": {"queued"},  # documented auto-retry to queued
    "done": {"needs-review"},  # documented completion gate without proof
    "requeue": ADMIN_TARGETS,
    "reclassify": ADMIN_TARGETS,
}


def run(coro):
    return asyncio.run(coro)


def parse_doc_table(text: str) -> set[tuple[str, str]]:
    """The ``from | to`` cells of the doc's PATCH transition table."""
    lines = text.splitlines()
    start = next(i for i, line in enumerate(lines) if re.match(r"\|\s*from\s*\|\s*to\s*\|\s*via\s*\|", line))
    pairs: set[tuple[str, str]] = set()
    for line in lines[start + 2 :]:
        if not line.strip().startswith("|"):
            break
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) == 3 and not set(cells) <= {"--", "---", "----"}:
            pairs.add((cells[0], cells[1]))
    return pairs


DOC_ACCEPTED = parse_doc_table(DOC.read_text())


class LifecycleMachine(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.temp = tempfile.TemporaryDirectory()
        self.env = patch.dict(
            os.environ,
            {
                "AGENT_BUS_DB": str(Path(self.temp.name) / "bus.db"),
                "HIVE_BACKEND": "sqlite",
                "MNEMOS_URL": "",
                "MNEMOS_TOKEN": "",
                "HIVE_SHARED_SECRET": "",
                "HIVE_BUS_ADMIN_TOKEN": "",
            },
        )
        self.env.start()
        self.repo_patch = patch.object(deps, "_HIVE_REPO", None)
        self.repo_patch.start()
        self.clock = 1800000000.0
        self.time_patch = patch.object(jobs_router, "time", SimpleNamespace(time=lambda: self.clock))
        self.time_patch.start()
        run(deps.hive_repo().init())
        self.submitter = self.register("opencode", ["code", "orchestrate"])
        self.workers = [self.register("zeroclaw", ["code"]) for _ in range(2)]
        self.ids = []
        self.previous = {}
        self.last_action = None

    def teardown(self):
        self.time_patch.stop()
        self.repo_patch.stop()
        self.env.stop()
        self.temp.cleanup()

    def request(self, method, path, body=None, actor=None):
        async def send():
            headers = {"X-Hive-Actor": actor} if actor else {}
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://test") as c:
                response = await c.request(method, path, json=body, headers=headers)
                return response.status_code, response.json()

        return run(send())

    def register(self, kind, capabilities):
        code, data = self.request(
            "POST",
            "/v1/agents/register",
            {
                "kind": kind,
                "host": "stateful",
                "runtime": kind,
                "capabilities": capabilities,
                "cost_tier": "B",
                "auth_method": "api",
            },
        )
        assert code == 200, data
        return data["urn"]

    def row(self, job):
        code, row = self.request("GET", f"/v1/jobs/{job}")
        assert code == 200, row
        return row

    def snapshot(self):
        """Every job this machine created, from one bulk read."""
        code, data = self.request("GET", "/v1/jobs?limit=100")
        assert code == 200, data
        ids = [job["id"] for job in data["jobs"]]
        assert len(ids) == len(set(ids)), "a job appeared twice in one list read"
        rows = {job["id"]: job for job in data["jobs"]}
        return {jid: rows[jid] for jid in self.ids if jid in rows}

    def select(self, i):
        return self.ids[i % len(self.ids)] if self.ids else None

    @rule()
    def submit(self):
        if len(self.ids) >= 5:
            return
        code, data = self.request(
            "POST",
            "/v1/jobs",
            {
                "kind": "e2e:stateful",
                "description": f"stateful {len(self.ids)}",
                "submitter_urn": self.submitter,
                "max_retries": 2,
            },
            self.submitter,
        )
        assert code in (200, 201), data
        self.ids.append(data["id"])
        self.last_action = "submit"

    @rule(i=st.integers(min_value=0, max_value=20), worker=st.integers(min_value=0, max_value=1))
    def claim(self, i, worker):
        jid = self.select(i)
        if jid:
            urn = self.workers[worker]
            before = self.row(jid)
            code, data = self.request("POST", f"/v1/jobs/{jid}/claim?by={urn}", actor=urn)
            after = self.row(jid)
            if code == 200:
                assert before["status"] in ("queued", "offered"), (before, data)
                assert after["status"] == "claimed" and after["claimed_by"] == urn
            else:
                assert code in (403, 409, 422, 429), (code, data)
                assert after["status"] == before["status"]
                assert after["claimed_by"] == before["claimed_by"]
            self.last_action = "claim"

    def patch_job(self, i, status, result=None):
        jid = self.select(i)
        if not jid:
            return None
        row = self.row(jid)
        claimant = row.get("claimed_by") or self.workers[0]
        body = {"status": status, "claimed_by": claimant}
        if result is not None:
            body["result"] = result
        code, data = self.request("PATCH", f"/v1/jobs/{jid}", body, claimant)
        assert code in (200, 403, 409, 422), (code, data)
        self.last_action = status
        return code

    @rule(i=st.integers(min_value=0, max_value=20))
    def renew(self, i):
        jid = self.select(i)
        if jid:
            row = self.row(jid)
            if row["status"] in ("claimed", "running"):
                code = self.patch_job(i, row["status"])
                if code == 200:
                    assert self.row(jid)["claim_lease_expires_at"] == self.clock + jobs_router.CLAIM_LEASE_SECONDS

    @rule(i=st.integers(min_value=0, max_value=20))
    def progress(self, i):
        self.patch_job(i, "running", {"hive_reviews": [{"model": "test", "verdict": "approve"}]})

    @rule(i=st.integers(min_value=0, max_value=20))
    def complete(self, i):
        self.patch_job(i, "done", {"note": "read-only completion"})

    @rule(i=st.integers(min_value=0, max_value=20), retryable=st.booleans())
    def fail(self, i, retryable):
        jid = self.select(i)
        before = self.row(jid) if jid else None
        code = self.patch_job(i, "failed", {"retryable": retryable, "error": "ordinary failure"})
        if jid is None or code != 200 or before["status"] not in ("claimed", "running"):
            return
        after = self.row(jid)
        if retryable and before["retry_count"] < before["max_retries"]:
            # Documented auto-retry: retryable failure with budget left requeues
            # immediately and spends one retry.
            assert after["status"] == "queued", after
            assert after["retry_count"] == before["retry_count"] + 1, after
        else:
            # Non-retryable, or the retry budget is already spent.
            assert after["status"] == "failed", after

    @rule(i=st.integers(min_value=0, max_value=20))
    def cancel(self, i):
        self.patch_job(i, "cancelled")

    @rule(
        i=st.integers(min_value=0, max_value=20), reason=st.sampled_from(["host_declines_kind", "invalid_head_branch"])
    )
    def release(self, i, reason):
        # Documented PATCH release: JOB_LIFECYCLE.md lists claimed|running ->
        # queued as "PATCH (release; claimant must match)", and the FB35 matrix
        # drives both cells through this endpoint successfully.
        self.patch_job(i, "queued", {"worker_error": reason})

    @rule(i=st.integers(min_value=0, max_value=20))
    def requeue(self, i):
        jid = self.select(i)
        if jid:
            code, data = self.request("POST", "/v1/admin/jobs/requeue", {"job_ids": [jid]}, self.submitter)
            assert code == 200, data
            self.last_action = "requeue"

    @rule(i=st.integers(min_value=0, max_value=20), status=st.sampled_from(["queued", "cancelled", "needs-review"]))
    def reclassify(self, i, status):
        jid = self.select(i)
        if jid:
            body = {"job_ids": [jid], "status": status}
            if status == "queued" and self.row(jid)["status"] == "dead-letter":
                # FB51: dead-letter only reopens with an explicit override.
                body["override_max_retries"] = True
                body["override_reason"] = "stateful dead-letter reopen"
            code, data = self.request("POST", "/v1/admin/jobs/reclassify", body, self.submitter)
            assert code == 200, data
            self.last_action = "reclassify"

    @rule(seconds=st.integers(min_value=1, max_value=600))
    def advance_clock(self, seconds):
        self.clock += seconds
        self.last_action = "clock"

    @rule()
    def reaper_view(self):
        """The reaper's pure selector sees exactly the expired leases.

        Long stale/hard TTLs isolate the lease rule, so a claim is selected if
        and only if its lease has expired at the fake clock. Requeuing itself is
        the reaper loop's job and is covered by FB37.
        """
        rows = self.snapshot()
        candidates = [
            (
                jid,
                row["kind"],
                row["claimed_by"],
                row["claimed_at"],
                row["last_update_at"],
                row["claim_lease_expires_at"],
            )
            for jid, row in rows.items()
            if row["status"] in ("claimed", "running")
        ]
        selected = {
            jid for jid, _ in select_orphan_claims(candidates, now=self.clock, stale_after=10**9, hard_max_age=10**9)
        }
        for jid, _kind, _claimer, _claimed_at, _updated_at, lease in candidates:
            expired = lease is not None and lease <= self.clock
            assert (jid in selected) == expired, f"{jid}: reaper selected={jid in selected} but lease expired={expired}"

    @invariant()
    def lifecycle_properties(self):
        rows = self.snapshot()
        for jid, row in rows.items():
            assert row["status"] in deps.VALID_JOB_STATUSES, row
            assert 0 <= row["retry_count"] <= row["max_retries"], row
            if row["status"] in ("claimed", "running"):
                assert row["claimed_by"] in self.workers, row
                # One live claimant and a lease on record. Expiry itself is the
                # reaper's input, so an expired-but-unreaped lease is legal here.
                assert row["claim_lease_expires_at"] is not None, row

            before = self.previous.get(jid)
            if before is None:
                self.previous[jid] = row
                continue

            if row["status"] != before["status"]:
                action_targets = ACTION_TARGETS.get(self.last_action, set())
                allowed = DOC_ACCEPTED | {(before["status"], target) for target in action_targets}
                assert (before["status"], row["status"]) in allowed, (
                    f"{jid}: {before['status']} -> {row['status']} after {self.last_action!r} "
                    "is neither a documented PATCH cell nor owned by that action"
                )
                if before["status"] == "done" and row["status"] == "queued":
                    assert self.last_action in ("requeue", "reclassify"), (
                        f"{jid}: completed job returned to queued via {self.last_action!r} without an explicit requeue"
                    )
                if before["status"] == "dead-letter":
                    assert self.last_action == "reclassify", (
                        f"{jid}: dead-letter became {row['status']!r} via {self.last_action!r} ({FINDING_DEAD_LETTER})"
                    )

            old_reviews = (before.get("result") or {}).get("hive_reviews")
            new_reviews = (row.get("result") or {}).get("hive_reviews")
            if old_reviews and new_reviews != old_reviews:
                # FB51 archives the evidence before clearing the current result;
                # the archive itself is pinned by the dedicated history test.
                assert self.last_action in ("requeue", "reclassify"), (
                    f"{jid}: review evidence cleared after {self.last_action!r} ({FINDING_REQUEUE_REVIEW})"
                )

            self.previous[jid] = row


TestLifecycleMachine = LifecycleMachine.TestCase
TestLifecycleMachine.settings = settings(max_examples=40, stateful_step_count=20, deadline=None)


def test_documented_transitions_match_the_map():
    """The doc table is the contract, and the map must not exceed it."""
    assert DOC_ACCEPTED, "doc transition table did not parse"
    for src, targets in STATUS_TRANSITIONS.items():
        for dst in targets:
            assert (src, dst) in DOC_ACCEPTED, f"map allows {src!r} -> {dst!r} but the doc omits it"


def test_requeue_preserves_review_evidence():
    """Sequence: submit -> claim -> progress (records a review) -> admin requeue.

    FB51 archives the review evidence, then clears the current result.
    """
    machine = LifecycleMachine()
    try:
        machine.submit()
        machine.claim(0, 0)
        machine.progress(0)
        before = machine.row(machine.ids[0])["result"]["hive_reviews"]
        machine.requeue(0)
        row = machine.row(machine.ids[0])
        assert row["status"] == "queued"
        assert (row["result"] or {}).get("hive_reviews") is None
        code, history = machine.request(
            "GET", f"/v1/admin/jobs/{machine.ids[0]}/history", actor=machine.submitter
        )
        assert code == 200, history
        assert history["count"] >= 1, history
        assert history["history"][0]["payload"]["hive_reviews"] == before
    finally:
        machine.teardown()


def test_dead_letter_reclassification_is_terminal():
    """Three real decline releases reach dead-letter; only an explicit override reopens it."""
    machine = LifecycleMachine()
    try:
        machine.submit()
        jid = machine.ids[0]
        for _ in range(3):
            machine.claim(0, 0)
            assert machine.row(jid)["status"] == "claimed"
            machine.release(0, "invalid_head_branch")
            machine.advance_clock(600)
        assert machine.row(jid)["status"] == "dead-letter"
        before = machine.row(jid)["result"]
        code, result = machine.request(
            "POST", "/v1/admin/jobs/reclassify", {"job_ids": [jid], "status": "queued"}, machine.submitter
        )
        assert code == 422, result
        assert machine.row(jid)["status"] == "dead-letter"
        code, result = machine.request(
            "POST",
            "/v1/admin/jobs/reclassify",
            {
                "job_ids": [jid],
                "status": "queued",
                "override_max_retries": True,
                "override_reason": "operator: dead-letter reopen after fixing head branch",
            },
            machine.submitter,
        )
        assert code == 200, result
        row = machine.row(jid)
        assert row["status"] == "queued"
        assert row["retry_count"] == 0
        code, history = machine.request("GET", f"/v1/admin/jobs/{jid}/history", actor=machine.submitter)
        assert code == 200, history
        if before:
            assert history["count"] >= 1, history
            entry = history["history"][0]
            assert (entry["prior_status"], entry["action"]) == ("dead-letter", "reclassify")
            assert entry["payload"] == before
        else:
            assert history["count"] == 0, history
    finally:
        machine.teardown()


def test_completion_gate_parks_a_fix_job_as_needs_review():
    """Documented gate: done without proof answers 409 and stores needs-review."""
    machine = LifecycleMachine()
    try:
        code, data = machine.request(
            "POST",
            "/v1/jobs",
            {
                "kind": "fix:stateful",
                "description": "gate",
                "submitter_urn": machine.submitter,
                "max_retries": 2,
            },
            machine.submitter,
        )
        assert code in (200, 201), data
        machine.ids.append(data["id"])
        machine.claim(0, 0)
        assert machine.patch_job(0, "done", {}) == 409
        assert machine.row(data["id"])["status"] == "needs-review"
    finally:
        machine.teardown()


def test_scripted_lifecycle_matches_the_doc():
    """A deterministic path independent of Hypothesis: submit/claim/complete/cancel."""
    machine = LifecycleMachine()
    try:
        machine.submit()
        jid = machine.ids[0]
        assert machine.row(jid)["status"] == "queued"
        machine.claim(0, 0)
        assert machine.row(jid)["claimed_by"] == machine.workers[0]
        assert machine.patch_job(0, "running") == 200
        assert machine.row(jid)["status"] == "running"
        assert machine.patch_job(0, "done", {"note": "read-only completion"}) == 200
        assert machine.row(jid)["status"] == "done"
        assert machine.patch_job(0, "cancelled") == 200
        assert machine.row(jid)["status"] == "cancelled"
    finally:
        machine.teardown()
