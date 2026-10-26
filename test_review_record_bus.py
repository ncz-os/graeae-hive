"""The bus keeps one reviewer record per job (2026-10-26).

Job 01a1793d's row named cerberus-reviewer in result.review_evidence (the
structured zoder verdict), nemotron in result.models.reviewers (the worker's host
ledger) and only the hive-reviewer's pre-dispatch brief review in
result.hive_reviews. The bus now warns when a submitted result disagrees with
itself, derives every reviewer field from the verdict (hive/review_record.py,
the same file the worker line ships), adds a `stage: delivery` entry to
hive_reviews next to the brief review, and the dashboard reads the same record.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("HIVE_BACKEND", "sqlite")
os.environ.setdefault("AGENT_BUS_DB", os.path.join(tempfile.mkdtemp(prefix="hive-review-record-"), "t.db"))

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive import review_record  # noqa: E402
from hive.bus.deps import hive_repo  # noqa: E402
from hive.bus.routers import jobs as jobs_router  # noqa: E402

HERE = Path(__file__).resolve().parent
BRIEF = {"model": "nvidia/nemotron-3-ultra-550b-a55b", "review_text": "VERDICT: APPROVE", "reviewed_at": 1.0}


def _evidence(model="cerberus-reviewer", verdict="approve"):
    entry = {
        "model": model,
        "role": "",
        "display": "GPT-OSS-20B (cerberus-reviewer)",
        "family": "openai",
        "verdict": verdict,
        "ok": True,
        "findings": {"total": 0},
        "escalation_reason": None,
        "forced_family_conflict": False,
    }
    return {"requested": "", "reviewers": [entry], "first": entry, "secondaries": [], "skipped": [], "warnings": []}


def _incident_result():
    return {
        "summary": "ok",
        "commits": ["abc123"],
        "files_changed": ["a.py"],
        "models": {"author": "deepseek-flash", "reviewers": ["nvidia/nemotron-3-ultra-550b-a55b"], "source": "ledger"},
        "review_evidence": _evidence(),
    }


class ReconcileTests(unittest.TestCase):
    def test_disagreeing_submission_is_logged_and_reconciled(self):
        result = _incident_result()
        result["hive_reviews"] = [dict(BRIEF)]
        with self.assertLogs("hive.bus.jobs", level=logging.WARNING) as logs:
            jobs_router._reconcile_reviewer_record("job-x", result)
        self.assertTrue(any("disagreeing reviewer fields" in m and "job-x" in m for m in logs.output))
        self.assertEqual(review_record.disagreements(result), [])
        self.assertEqual(
            review_record.reviewer_fields(result),
            {
                "reviewer": "cerberus-reviewer",
                "review_evidence": "cerberus-reviewer",
                "models": "cerberus-reviewer",
                "hive_reviews": "cerberus-reviewer",
            },
        )
        self.assertEqual(result["hive_reviews"][0], BRIEF, "the brief review is kept as it was")
        self.assertEqual(result["models"]["observed_reviewers"], ["nvidia/nemotron-3-ultra-550b-a55b"])

    def test_consistent_submission_logs_nothing(self):
        result = _incident_result()
        result["models"]["reviewers"] = ["cerberus-reviewer"]
        with self.assertNoLogs("hive.bus.jobs", level=logging.WARNING):
            jobs_router._reconcile_reviewer_record("job-y", result)
        self.assertEqual(result["reviewer"]["source"], "zoder_verdict")

    def test_older_worker_result_without_a_record_is_not_rewritten(self):
        result = {"models": {"author": "qwen38", "reviewers": ["gemma4-26b-a4b"]}, "hive_reviews": [dict(BRIEF)]}
        before = json.loads(json.dumps(result))
        jobs_router._reconcile_reviewer_record("job-z", result)
        self.assertEqual(result, before)

    def test_failure_result_is_null_safe(self):
        for result in ({}, {"error": "boom"}, {"review_evidence": None}, {"review_evidence": {"reviewers": []}}):
            jobs_router._reconcile_reviewer_record("job-f", result)
            self.assertEqual(review_record.disagreements(result), [])
        jobs_router._reconcile_reviewer_record("job-f", None)
        jobs_router._reconcile_reviewer_record("job-f", "not a dict")

    def test_a_broken_record_never_fails_the_write(self):
        result = _incident_result()
        result["review_evidence"]["reviewers"] = [None, 5, "x"]
        jobs_router._reconcile_reviewer_record("job-b", result)


class BusEndToEndTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await hive_repo().init()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t")

    async def asyncTearDown(self):
        await self.client.aclose()

    async def test_worker_result_and_brief_review_end_up_consistent(self):
        c = self.client
        r = await c.post(
            "/v1/agents/register",
            json={
                "kind": "opencode",
                "host": "studio",
                "runtime": "opencode",
                "status": "online",
                "capabilities": ["code", "orchestrate"],
                "cost_tier": "C",
                "auth_method": "api",
            },
        )
        submitter = r.json()["urn"]
        r = await c.post(
            "/v1/agents/register",
            json={
                "kind": "zeroclaw",
                "host": "studio",
                "runtime": "zeroclaw",
                "status": "online",
                "capabilities": ["code", "bash", "python"],
                "cost_tier": "B",
                "auth_method": "api",
            },
        )
        worker = r.json()["urn"]
        await c.post("/v1/agents/heartbeat", json={"urn": worker})
        r = await c.post(
            "/v1/jobs",
            json={
                "kind": "test:review-record",
                "description": "REVIEW RECORD TEST",
                "submitter_urn": submitter,
                "priority": 99,
                "max_retries": 0,
            },
        )
        self.assertEqual(r.status_code, 200, r.text)
        jid = r.json()["id"]
        r = await c.post(f"/v1/jobs/next?agent_urn={worker}")
        self.assertEqual(r.json()["id"], jid)
        # the hive-reviewer timer's pre-dispatch brief review, stored on the row
        async with hive_repo().connection() as tx:
            await tx.execute(
                "UPDATE jobs SET status=?, result=? WHERE id=?",
                ("running", json.dumps({"hive_reviews": [BRIEF]}), jid),
            )
            await tx.commit()
        with self.assertLogs("hive.bus.jobs", level=logging.WARNING):
            r = await c.patch(
                f"/v1/jobs/{jid}",
                json={"status": "done", "claimed_by": worker, "result": _incident_result()},
            )
        self.assertEqual(r.status_code, 200, r.text)
        res = (await c.get(f"/v1/jobs/{jid}")).json()["result"]
        if isinstance(res, str):
            res = json.loads(res)
        self.assertEqual(review_record.disagreements(res), [])
        self.assertEqual(review_record.reviewer_fields(res)["hive_reviews"], "cerberus-reviewer")
        self.assertEqual(res["models"]["reviewers"], ["cerberus-reviewer"])
        self.assertEqual(res["models"]["author"], "deepseek-flash")
        self.assertEqual(res["hive_reviews"][0]["review_text"], "VERDICT: APPROVE")
        self.assertEqual(res["review_evidence"]["reviewers"][0]["model"], "cerberus-reviewer")


def _job_models(job):
    js = (HERE / "static" / "dashboard.js").read_text(encoding="utf-8")
    start = js.index("function jobReviewers(")
    end = js.index("// A worker that registered as plain")
    script = js[start:end] + f"\nconsole.log(JSON.stringify(jobModels({json.dumps(job)})));\n"
    out = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=30, check=True)
    return json.loads(out.stdout)


@unittest.skipUnless(shutil.which("node"), "node not installed")
class DashboardReadsTheRecordTests(unittest.TestCase):
    def test_card_reviewers_come_from_the_record(self):
        res = _incident_result()
        review_record.apply(res, hive_reviews=True)
        self.assertEqual(_job_models({"result": res}), {"author": "deepseek-flash", "reviewers": ["cerberus-reviewer"]})

    def test_unreconciled_row_still_prefers_the_verdict_over_the_ledger(self):
        self.assertEqual(
            _job_models({"result": _incident_result()}),
            {"author": "deepseek-flash", "reviewers": ["cerberus-reviewer"]},
        )

    def test_rows_before_the_record_render_as_before(self):
        job = {"result": {"models": {"author": "qwen38", "reviewers": ["gemma4-26b-a4b"]}}}
        self.assertEqual(_job_models(job), {"author": "qwen38", "reviewers": ["gemma4-26b-a4b"]})
        self.assertIsNone(_job_models({"result": {}}))


if __name__ == "__main__":
    unittest.main()
