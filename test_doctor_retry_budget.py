"""A job with no retry budget is never requeued by the doctor (operator 2026-10-26).

Evidence: the PYTHIA doctor triage re-queued failed jobs 01a178cd and 01a17930
(twice) although both were submitted with max_retries 0. Every doctor path that
requeues a failed job, and the bus endpoints it calls, now refuse a job whose
max_retries is 0 or whose retry_count has reached max_retries. The bus accepts
such a requeue only with an explicit operator override that is logged.
"""

import asyncio
import json
import logging
import tempfile
from pathlib import Path
from unittest import mock

import pytest

import agent_bus
from hive.bus.routers import admin as hive_admin_router
from hive.persistence.sqlite import SqliteHiveRepository
from hive.workers import doctor


def _failed(jid, kind="build:x", max_retries=0, retry_count=0, **extra):
    return {
        "id": jid,
        "kind": kind,
        "status": "failed",
        "max_retries": max_retries,
        "retry_count": retry_count,
        **extra,
    }


# ── the budget predicate ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "job,expected",
    [
        ({"max_retries": 0, "retry_count": 0}, True),
        ({"max_retries": 2, "retry_count": 2}, True),
        ({"max_retries": 2, "retry_count": 3}, True),
        ({"max_retries": 2, "retry_count": 1}, False),
        ({"max_retries": 2, "retry_count": None}, False),
        ({"max_retries": "2", "retry_count": "0"}, False),
        ({}, True),  # unreadable budget fails closed
        ({"max_retries": None, "retry_count": 0}, True),
        ({"max_retries": "x", "retry_count": 0}, True),
        ({"max_retries": True, "retry_count": 0}, True),
    ],
)
def test_retry_budget_exhausted(job, expected):
    assert doctor.retry_budget_exhausted(job) is expected


# ── bulk resubmit (triage wrappers, LLM release, SOP resubmit_jobs) ─────────


def _bulk_http(jobs, calls):
    def _http(method, path, body=None, **_kw):
        calls.append((method, path, body))
        if method == "GET":
            return 200, {"jobs": jobs}
        return 200, {"ok": list((body or {}).get("job_ids", []))}

    return _http


def _resubmit_http(jobs, calls, new_id="replacement-1"):
    """GET failed jobs; POST /v1/jobs mints a child; PATCH marks the original.

    The bulk fake below answers a submit with `{"ok": [...]}`, which is the
    requeue shape; a submit answers with an id, so the resubmit-as-new path needs
    its own fake to be exercised at all.
    """

    def _http(method, path, body=None, **_kw):
        calls.append((method, path, body))
        if method == "GET":
            return 200, {"jobs": jobs}
        if method == "POST" and path == "/v1/jobs":
            return 201, {"id": new_id}
        if method == "PATCH":
            return 200, {}
        return 200, {"ok": list((body or {}).get("job_ids", []))}

    return _http


def test_failed_job_with_max_retries_zero_is_not_requeued(monkeypatch):
    """Operator rule 2026-10-26: a no-budget row is never REQUEUED.

    Decision (F3, 2026-10-29): these assertions predated the 2026-10-27 change in
    `action_resubmit_jobs` (operator: "why is the doctor not acting on these?").
    A budget-spent failure is now re-dispatched as a NEW job — new id, real
    budget, parented to the original, one generation only — instead of becoming a
    needs-review row nobody can action. It is still never requeued: no
    `/v1/admin/jobs/requeue` call and no operator override, which is what the
    2026-10-26 rule is about. These assertions now pin that distinction rather
    than pinning "nothing happens".
    """
    calls, messages = [], []
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "_http", _resubmit_http([_failed("01a178cd-dead", max_retries=0)], calls))
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda t, p: messages.append((t, p)))
    text = doctor.action_resubmit_jobs("build:x")

    # 1. Never a requeue, and never the operator override that would bypass the rule.
    assert [c for c in calls if c[1] == "/v1/admin/jobs/requeue"] == [], "no requeue may reach the bus"
    assert "override_max_retries" not in json.dumps([c[2] for c in calls if c[0] == "POST"])
    # 2. Exactly one replacement: parented, real budget, stable idempotency key.
    posts = [c for c in calls if c[0] == "POST" and c[1] == "/v1/jobs"]
    assert len(posts) == 1
    payload = posts[0][2]
    assert payload["parent_job_id"] == "01a178cd-dead"
    assert payload["max_retries"] == 2
    assert payload["idempotency_key"] == "doctor-resubmit-01a178cd-dead"
    assert payload["result"]["resubmitted_from"] == "01a178cd-dead"
    # 3. The original points at its replacement, so the next sweep skips it.
    patches = [c for c in calls if c[0] == "PATCH"]
    assert patches and patches[0][1] == "/v1/jobs/01a178cd-dead"
    assert patches[0][2]["result"]["resubmitted_as"] == "replacement-1"
    # 4. The operator is told both halves: held, then what was submitted.
    assert [m[0] for m in messages] == ["doctor.needs-review", "doctor.resubmitted"]
    assert messages[0][1]["job_ids"] == ["01a178cd-dead"]
    assert messages[1][1]["submitted"] == ["replacement-1"]
    assert "resubmitted 1 budget-spent job(s) of build:x as new" in text


def test_exhausted_budget_is_not_requeued_but_jobs_with_retries_left_are(monkeypatch):
    calls = []
    jobs = [
        _failed("spent", max_retries=2, retry_count=2),
        _failed("zero", max_retries=0),
        _failed("left", max_retries=2, retry_count=1),
    ]
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "_http", _bulk_http(jobs, calls))
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    text = doctor.action_resubmit_jobs("build:x")
    posts = [c for c in calls if c[0] == "POST"]
    assert len(posts) == 1
    assert posts[0][1] == "/v1/admin/jobs/requeue"
    assert posts[0][2]["job_ids"] == ["left"]
    assert "override_max_retries" not in posts[0][2], "the doctor never sends the operator override"
    assert text.startswith("requeued 1 jobs of build:x (api=200)")
    assert "held 2" in text


def test_requeue_record_regex_still_matches_with_held_note(monkeypatch):
    """The v2/LLM paths record a same-signature resubmit off this text."""
    import re

    calls = []
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "_http", _bulk_http([_failed("a", max_retries=1), _failed("b", max_retries=0)], calls))
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    text = doctor.action_resubmit_jobs("build:x")
    assert re.search(r"requeued [1-9]\d* jobs .*\(api=20[01]\)", text)
    calls.clear()
    monkeypatch.setattr(doctor, "_http", _bulk_http([_failed("b", max_retries=0)], calls))
    text = doctor.action_resubmit_jobs("build:x")
    assert not re.search(r"requeued [1-9]\d* jobs .*\(api=20[01]\)", text)


def test_triage_release_of_max_retries_zero_job_does_not_requeue(monkeypatch):
    """The exact incident path: a triage:<kind> wrapper deciding release_to_queue.

    Same decision as above: not released into the queue (no requeue, `released`
    False), but re-dispatched once as a parented replacement.
    """
    calls = []
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "_http", _resubmit_http([_failed("01a17930", max_retries=0)], calls))
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    text, released = doctor.apply_release("wrapper", True, "build:x", None)
    assert released is False, "a no-budget row is not released back into the queue"
    assert [c for c in calls if c[1] == "/v1/admin/jobs/requeue"] == []
    posts = [c for c in calls if c[0] == "POST" and c[1] == "/v1/jobs"]
    assert len(posts) == 1 and posts[0][2]["parent_job_id"] == "01a17930"
    assert "resubmitted 1 budget-spent job(s)" in text


# ── SOP executor release_to_queue of the original failed job ───────────────


def test_sop_release_of_original_with_no_budget_is_refused(monkeypatch):
    calls = []

    def _http(method, path, body=None, **_kw):
        calls.append((method, path, body))
        if method == "GET":
            return 200, _failed("orig", max_retries=0)
        return 200, {}

    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "_http", _http)
    released = []
    monkeypatch.setattr(doctor, "action_release_to_queue", lambda *a, **k: released.append(a) or "released")
    out = doctor.execute_sop_decision(
        {
            "id": "exec",
            "kind": "doctor:execute",
            "description": json.dumps(
                {
                    "version": 1,
                    "job_id": "orig",
                    "action": {"action": "release_to_queue", "release_eligible_kinds": ["zeroclaw"]},
                }
            ),
        }
    )
    assert released == []
    assert "needs-review" in json.dumps(out)


def test_sop_release_of_original_with_budget_still_releases(monkeypatch):
    def _http(method, path, body=None, **_kw):
        if method == "GET":
            return 200, _failed("orig", max_retries=2, retry_count=0)
        return 200, {}

    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "_http", _http)
    released = []
    monkeypatch.setattr(doctor, "action_release_to_queue", lambda *a, **k: released.append(a) or "released")
    doctor.execute_sop_decision(
        {
            "id": "exec",
            "kind": "doctor:execute",
            "description": json.dumps(
                {
                    "version": 1,
                    "job_id": "orig",
                    "action": {"action": "release_to_queue", "release_eligible_kinds": ["zeroclaw"]},
                }
            ),
        }
    )
    assert released and released[0][0] == "orig"


# ── dead-letter remediation ────────────────────────────────────────────────


def _dl(jid, **budget):
    return {"id": jid, "kind": "zeroclaw", "result": {"error": "timeout"}, **budget}


def test_dlq_transient_failure_with_no_budget_becomes_needs_review():
    decision, reason = doctor.classify_dead_letter(_dl("d0", max_retries=0, retry_count=0))
    assert decision == "needs-review"
    assert reason.startswith("retry-budget-exhausted")
    assert doctor.classify_dead_letter(_dl("d1", max_retries=2, retry_count=0))[0] == "resubmit"


def test_dlq_remediate_never_requeues_a_job_with_no_budget(monkeypatch):
    calls = []
    monkeypatch.setattr(doctor, "DLQ_APPLY", True)
    def http(*args, **kwargs):
        calls.append(args)
        if args[0] == "GET":
            return 200, {"jobs": [{"id": "existing-review", "kind": "review:budget"}]}
        return 200, {"ok": ["d0"]}

    monkeypatch.setattr(doctor, "_http", http)
    out = doctor.remediate_dead_letter(_dl("d0", max_retries=0, retry_count=0), "resubmit", "forced")
    assert [c for c in calls if c[1] == "/v1/admin/jobs/requeue"] == []
    assert [c[1] for c in calls if c[0] != "GET"] == ["/v1/admin/jobs/reclassify"]
    assert calls[0][2]["status"] == "needs-review"
    assert "needs-review" in out


def test_dlq_remediate_requeues_a_job_with_budget(monkeypatch):
    calls = []
    monkeypatch.setattr(doctor, "DLQ_APPLY", True)
    monkeypatch.setattr(doctor, "_http", lambda *a, **k: calls.append(a) or (200, {"ok": ["d1"]}))
    out = doctor.remediate_dead_letter(_dl("d1", max_retries=2, retry_count=1), "resubmit", "transient")
    assert [c[1] for c in calls] == ["/v1/admin/jobs/requeue"]
    assert out.startswith("resubmitted")


# ── the bus admin endpoints ─────────────────────────────────────────────────


async def _seed(repo, rows):
    async with repo.connection() as db:
        for jid, status, max_retries, retry_count in rows:
            await db.execute(
                "INSERT INTO jobs (id, submitter_urn, kind, status, started_at, max_retries, retry_count) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (jid, "urn:submitter", "build:x", status, 1.0, max_retries, retry_count),
            )
        await db.commit()


async def _status(repo, jid):
    async with repo.connection() as db:
        async with db.execute("SELECT status, retry_count FROM jobs WHERE id=?", (jid,)) as cur:
            return tuple(await cur.fetchone())


async def _events(repo, kind):
    async with repo.connection() as db:
        async with db.execute("SELECT payload FROM events WHERE kind=?", (kind,)) as cur:
            return [json.loads(r[0]) for r in await cur.fetchall()]


def _run(coro):
    return asyncio.run(coro)


def test_bus_requeue_refuses_no_budget_without_override(caplog):
    async def body():
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "b.db"))
            await repo.init(agent_bus.SCHEMA)
            await _seed(repo, [("zero", "failed", 0, 0), ("spent", "failed", 2, 2), ("left", "failed", 2, 1)])
            with mock.patch.object(hive_admin_router, "hive_repo", lambda: repo):
                res = await hive_admin_router.admin_requeue_jobs(
                    agent_bus.RequeueRequest(job_ids=["zero", "spent", "left"], reason="doctor-resubmit:build:x"),
                    actor="urn:agent:doctor:pythia",
                )
            assert res["ok"] == ["left"]
            assert res["skipped_retry_budget"] == ["zero", "spent"]
            assert (await _status(repo, "zero"))[0] == "failed"
            assert (await _status(repo, "spent"))[0] == "failed"
            assert await _status(repo, "left") == ("queued", 0)
            assert await _events(repo, "job.requeue_override") == []

    with caplog.at_level(logging.WARNING, logger="hive.bus.admin"):
        _run(body())
    assert any("refused: no retry budget" in r.getMessage() and "zero" in r.getMessage() for r in caplog.records)


def test_bus_requeue_override_needs_a_reason():
    async def body():
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "b.db"))
            await repo.init(agent_bus.SCHEMA)
            await _seed(repo, [("zero", "failed", 0, 0)])
            with mock.patch.object(hive_admin_router, "hive_repo", lambda: repo):
                res = await hive_admin_router.admin_requeue_jobs(
                    agent_bus.RequeueRequest(job_ids=["zero"], override_max_retries=True, override_reason="  "),
                    actor="urn:operator",
                )
            assert res["ok"] == [] and res["skipped_retry_budget"] == ["zero"]

    _run(body())


def test_bus_requeue_override_requeues_and_is_logged(caplog):
    async def body():
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "b.db"))
            await repo.init(agent_bus.SCHEMA)
            await _seed(repo, [("zero", "failed", 0, 0)])
            with mock.patch.object(hive_admin_router, "hive_repo", lambda: repo):
                res = await hive_admin_router.admin_requeue_jobs(
                    agent_bus.RequeueRequest(
                        job_ids=["zero"],
                        reason="manual-requeue",
                        override_max_retries=True,
                        override_reason="operator: one more attempt after brief fix",
                    ),
                    actor="urn:operator:jason",
                )
            assert res["ok"] == ["zero"]
            assert (await _status(repo, "zero"))[0] == "queued"
            events = await _events(repo, "job.requeue_override")
            assert len(events) == 1
            assert events[0]["id"] == "zero"
            assert events[0]["actor"] == "urn:operator:jason"
            assert events[0]["override_reason"] == "operator: one more attempt after brief fix"
            assert events[0]["max_retries"] == 0

    with caplog.at_level(logging.WARNING, logger="hive.bus.admin"):
        _run(body())
    msgs = [r.getMessage() for r in caplog.records]
    assert any("OVERRIDES its retry budget" in m and "urn:operator:jason" in m for m in msgs)


def test_bus_reclassify_to_queued_obeys_the_same_rule():
    async def body():
        with tempfile.TemporaryDirectory() as td:
            repo = SqliteHiveRepository(str(Path(td) / "b.db"))
            await repo.init(agent_bus.SCHEMA)
            await _seed(repo, [("zero", "dead-letter", 0, 0), ("left", "dead-letter", 2, 0)])
            with mock.patch.object(hive_admin_router, "hive_repo", lambda: repo):
                res = await hive_admin_router.admin_reclassify_jobs(
                    agent_bus.ReclassifyRequest(job_ids=["zero", "left"], status="queued"),
                    actor="urn:x",
                )
                assert res["ok"] == ["left"] and res["skipped_retry_budget"] == ["zero"]
                # needs-review is not a requeue: always allowed.
                res2 = await hive_admin_router.admin_reclassify_jobs(
                    agent_bus.ReclassifyRequest(job_ids=["zero"], status="needs-review"),
                    actor="urn:x",
                )
                assert res2["ok"] == ["zero"]
                assert (await _status(repo, "zero"))[0] == "needs-review"

    _run(body())
