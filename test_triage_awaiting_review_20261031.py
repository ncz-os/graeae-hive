"""Triage visibility for awaiting_review rows (operator rule 2026-10-31).

The operator: "queue should be part of triage queue". There is NO separate
queue, table or status namespace: an ``awaiting_review`` row is a Hive job row
in ``needs-review``, and it has to show up in the SAME triage listing the
operator already reads -- the weekend triage check (``scripts/hive_status.py``),
GET /v1/jobs and the doctor logs -- with its reason and ``next_attempt_at``
visible.

The API/dashboard half is exercised through the real ASGI app on a temporary
SQLite database; the weekend-triage half through the offline ``--rows`` mode
of the status script.
"""

import time

import pytest
from hive.review_queue import make_review_state


def _needs_review_row(rows=()):
    return {
        "id": "source",
        "kind": "fix:thing",
        "status": "needs-review",
        "repo": "graeae-hive",
        "head_branch": "fix/thing",
        "base_branch": "main",
        "result": {
            "head_sha": "a" * 40,
            "commits": ["a" * 40],
            "review_state": make_review_state(
                "qwen38",
                [{"model_id": "gpt-oss-20b", "failure": "review_busy"}],
                attempt=1,
                next_attempt_at=int(time.time()) + 900,
                now=int(time.time()) - 60,
            ),
        },
        **dict(rows),
    }


def _rows():
    row = _needs_review_row()
    return {
        "jobs": [
            {k: row[k] for k in ("id", "kind", "status", "repo", "head_branch", "base_branch", "result")},
            {"id": "other", "kind": "fix:other", "status": "needs-review", "result": {}},
            {"id": "run", "kind": "fix:run", "status": "running"},
        ],
        "agents": [],
    }


def test_weekend_triage_lists_the_awaiting_row_with_reason_and_next_attempt():
    from scripts import hive_status

    snapshot = hive_status.build_status(_rows(), now=time.time())
    by_id = {row["id"]: row for row in snapshot["attention"]}
    assert "source" in by_id and "other" in by_id
    row = by_id["source"]
    assert row["review_state"] == "awaiting_review"
    assert row["family"] == "alibaba"
    assert "review_busy" in row["review_reason"]
    assert row["next_attempt_at"]
    text = hive_status.render_text(snapshot)
    assert "awaiting_review" in text and "review_busy" in text


def _app(tmp_path):
    """The real ASGI app on a temporary SQLite database (no network, no fleet)."""
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    pytest.importorskip("sse_starlette")
    import os

    os.environ["AGENT_BUS_DB"] = str(tmp_path / "c.db")
    os.environ["HIVE_BACKEND"] = "sqlite"
    from hive.bus import deps as _deps

    _deps._HIVE_REPO = None
    import agent_bus

    return agent_bus


def _request(app_module, method, path, body=None, actor=None):
    import asyncio

    import httpx

    async def _do():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_module.app), base_url="http://t") as c:
            headers = {"X-Hive-Actor": actor} if actor else None
            return await c.request(method, path, json=body, headers=headers)

    return asyncio.run(_do())


def _register(app_module):
    resp = _request(
        app_module,
        "POST",
        "/v1/agents/register",
        {"kind": "opencode", "host": "ctr", "runtime": "opencode", "status": "online",
         "capabilities": ["code", "orchestrate"], "cost_tier": "C"},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["urn"]


def _awaiting_row(app_module):
    """One needs-review row carrying an awaiting_review block, via the real API."""
    submitter = _register(app_module)
    resp = _request(
        app_module,
        "POST",
        "/v1/jobs",
        {"kind": "loop", "description": "triage visibility 20261031", "submitter_urn": submitter, "priority": 50},
    )
    assert resp.status_code in (200, 201), resp.text
    job_id = resp.json()["id"]
    patched = _request(app_module, "PATCH", f"/v1/jobs/{job_id}", {"status": "needs-review", "result": _needs_review_row()["result"]})
    assert patched.status_code == 200, patched.text
    return job_id


def test_jobs_filter_selects_awaiting_review_rows(tmp_path):
    app_module = _app(tmp_path)
    job_id = _awaiting_row(app_module)
    listed = _request(app_module, "GET", "/v1/jobs?status=needs-review&review_state=awaiting_review")
    assert listed.status_code == 200, listed.text
    ids = [job["id"] for job in listed.json()["jobs"]]
    assert ids == [job_id]
    # Without the filter the same row is still there (it IS a needs-review row)...
    everything = _request(app_module, "GET", "/v1/jobs?status=needs-review")
    assert job_id in [job["id"] for job in everything.json()["jobs"]]
    # ...and an unknown value is refused, never read as "everything".
    bad = _request(app_module, "GET", "/v1/jobs?review_state=nonsense")
    assert bad.status_code == 400


def test_summary_counts_the_awaiting_queue(tmp_path):
    app_module = _app(tmp_path)
    _awaiting_row(app_module)
    summary = _request(app_module, "GET", "/v1/jobs/summary")
    assert summary.status_code == 200, summary.text
    body = summary.json()
    assert body["needs_review"]["n"] == 1
    assert body["awaiting_review"]["n"] == 1
    assert body["awaiting_review"]["oldest_age_sec"] is not None
