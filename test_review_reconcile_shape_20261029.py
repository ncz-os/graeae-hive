"""A completed ``review:*`` child must close, annotate or visibly refuse its parent.

Bug (found on the live bus, 2026-10-29): ``reconcile_review_parent`` required a
top-level ``result.verdict`` dict on the child. The real worker result for a
review child has NO such key (it is null); the verdict is
``result.reviewer.verdict`` (derived from ``result.review_evidence`` by
``hive/review_record.py``). Every such child was ignored SILENTLY: the parent
stayed ``needs-review`` with only the stale text "auto review submitted <id>".
In all observed pairs the child's ``base_sha`` equalled the parent's delivered
``head_sha`` while the child's own head differed: the review job advanced the
branch (it is a review-and-fix loop).

These tests run the REAL bus app in-process (ASGI, temp sqlite, the harness of
``test_agent_lifecycle.py`` / ``test_doctor_e2e_bus_20261029.py``) with
real-shape fixtures: anonymised, fake shas, the key list the worker emits.

(a) legacy dict verdict still closes an exact-head approved parent
(b) worker-shape approve, matching head, valid proof -> parent ``done``
(c) worker-shape approve where the child advanced the branch -> ``needs-review``
    with ``head_matches: false`` evidence, never ``done``
(d) request_changes via ``reviewer.verdict`` -> ``needs-review`` with evidence
(e) neither shape -> evidence "no usable verdict", status unchanged, ONE log line
(f) protected row / ``triage:`` parent / other owner: unchanged
(g) idempotent: reconciling the same child twice writes no second audit row/event
(h) the exact worker result dict (producer/consumer contract)
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import tempfile
import time
import unittest
import uuid
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive import review_record  # noqa: E402
from hive.bus import deps as _deps  # noqa: E402
from hive.bus.deps import emit_event, hive_repo  # noqa: E402
from hive.bus.domain.review_dispatch import reconcile_review_parent  # noqa: E402

DELIVERED = "1a2b3c4d" * 5  # the parent's delivered head (fake sha)
ADVANCED = "9e8d7c6b" * 5  # the head the review job pushed (fake sha)
OTHER = "5f4e3d2c" * 5
LOGGER = "hive.bus.review_dispatch"


def _run(coro):
    return asyncio.run(coro)


def worker_review_result(
    *,
    verdict="approve",
    head=DELIVERED,
    base=DELIVERED,
    remote_sha=None,
    remote_verified=True,
    top_level_verdict=None,
):
    """The result a worker PATCHes for a ``review:*`` child (anonymised).

    Key list from the live bus: ``verdict`` is null; the approval lives in
    ``reviewer`` (derived from ``review_evidence``), next to head/remote/base
    shas, ``loop_result.teardown`` and ``exit_code``. ``commits`` /
    ``files_changed`` / ``worktree_clean`` are the git proof the completion gate
    needs when the review job pushed.
    """
    evidence_verdict = verdict
    entry = {
        "model": "cerberus-reviewer",
        "role": "",
        "family": "openai",
        "verdict": evidence_verdict,
        "ok": True,
        "findings": {"total": 0},
    }
    return {
        "verdict": top_level_verdict,
        "reviewer": {
            "model": "cerberus-reviewer",
            "models": ["cerberus-reviewer"],
            "verdict": verdict,
            "source": "zoder_verdict",
        },
        "review_evidence": {
            "requested": "",
            "reviewers": [entry] if verdict is not None else [],
            "first": entry if verdict is not None else None,
            "secondaries": [],
            "skipped": [],
            "warnings": [],
        },
        "head_sha": head,
        "remote_sha": remote_sha if remote_sha is not None else head,
        "base_sha": base,
        "remote_verified": remote_verified,
        "loop_result": {
            "teardown": {
                "final_verdict": verdict,
                "reason": "approved" if verdict == "approve" else "changes_requested",
                "resolved": verdict == "approve",
                "last_good_head_sha": head,
            }
        },
        "exit_code": 0,
        "commits": [head],
        "files_changed": ["pkg/widget.py"],
        "worktree_clean": True,
    }


class ReviewReconcileShapeBase(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        env = mock.patch.dict(
            os.environ, {"AGENT_BUS_DB": os.path.join(self._td.name, "rv.db"), "HIVE_BACKEND": "sqlite"}
        )
        env.start()
        self.addCleanup(env.stop)
        repo = mock.patch.object(_deps, "_HIVE_REPO", None)
        repo.start()
        self.addCleanup(repo.stop)
        _run(hive_repo().init())
        self.submitter = self._register(
            {
                "kind": "opencode",
                "host": "e2e",
                "runtime": "opencode",
                "status": "online",
                "capabilities": ["code", "orchestrate"],
                "cost_tier": "C",
                "auth_method": "api",
            }
        )
        self.worker = self._register(
            {
                "kind": "zeroclaw",
                "host": "e2e",
                "runtime": "zeroclaw",
                "status": "online",
                "capabilities": ["code", "bash", "python"],
                "cost_tier": "B",
                "auth_method": "api",
            }
        )

    # ---- bus helpers ----
    def _request(self, method, path, body=None, actor=None):
        async def _do():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t") as c:
                headers = {"X-Hive-Actor": actor} if actor and method in ("POST", "PATCH", "PUT", "DELETE") else None
                resp = await c.request(method, path, json=body, headers=headers)
                try:
                    return resp.status_code, resp.json()
                except Exception:
                    return resp.status_code, resp.text

        return _run(_do())

    def _register(self, payload):
        code, data = self._request("POST", "/v1/agents/register", payload)
        self.assertEqual(code, 200, data)
        return data["urn"]

    def _get(self, jid):
        code, data = self._request("GET", f"/v1/jobs/{jid}")
        self.assertEqual(code, 200, data)
        return data

    def _sql(self, sql, params=()):
        async def _do():
            async with hive_repo().connection() as tx:
                async with tx.execute(sql, params) as cur:
                    rows = await cur.fetchall()
                await tx.commit()
                return rows

        return _run(_do())

    def _claim(self, jid):
        code, data = self._request("POST", f"/v1/jobs/{jid}/claim?by={self.worker}", None, actor=self.worker)
        self.assertEqual(code, 200, data)

    def _submit(self, kind, *, parent=None, description="REVIEW RECONCILE SHAPE TEST"):
        body = {
            "kind": kind,
            "description": description,
            "submitter_urn": self.submitter,
            "priority": 200,
        }
        # No repo routing: repo-bearing jobs must resolve to a trusted catalog remote,
        # and only the parent link and the result shapes matter to the reconcile.
        if parent:
            body["parent_job_id"] = parent
        code, data = self._request("POST", "/v1/jobs", body, actor=self.worker)
        self.assertIn(code, (200, 201), data)
        return data["id"]

    def parent_in_review(self, kind="fix:widget", *, head=DELIVERED, proof=True, extra=None):
        """Submit, claim and park a parent in needs-review, as a worker would."""
        jid = self._submit(kind)
        self._claim(jid)
        result = {"head_sha": head, "commits": [head], "files_changed": ["pkg/widget.py"], "summary": "delivered"}
        if proof:
            result["worktree_clean"] = True
        result.update(extra or {})
        code, data = self._request(
            "PATCH",
            f"/v1/jobs/{jid}",
            {"status": "needs-review", "claimed_by": self.worker, "result": result},
            actor=self.worker,
        )
        self.assertEqual(code, 200, data)
        self.assertEqual(self._get(jid)["status"], "needs-review")
        return jid

    def auto_child(self, parent):
        rows = self._sql("SELECT id FROM jobs WHERE parent_job_id=? AND kind LIKE 'review:%'", (parent,))
        self.assertEqual(len(rows), 1, "dispatch must have created exactly one review child")
        return rows[0][0]

    def manual_child(self, parent):
        # Unique description: identical submissions are coalesced by the bus.
        return self._submit("review:widget", parent=parent, description=f"HAND-MADE REVIEW CHILD {uuid.uuid4()}")

    def complete_child(self, child, result, *, claim=True):
        """PATCH the child done through the real endpoint (fires the real reconcile call)."""
        if claim:
            self._claim(child)
        code, data = self._request(
            "PATCH",
            f"/v1/jobs/{child}",
            {"status": "done", "claimed_by": self.worker, "result": result},
            actor=self.worker,
        )
        self.assertEqual(code, 200, data)

    def repatch_child_done(self, child, result):
        code, data = self._request(
            "PATCH",
            f"/v1/jobs/{child}",
            {"status": "done", "claimed_by": self.worker, "result": result},
            actor=self.worker,
        )
        self.assertEqual(code, 200, data)

    def audit_count(self, parent):
        return self._sql(
            "SELECT COUNT(*) FROM job_audit_log WHERE job_id=? AND patch LIKE '%review_job_id%'", (parent,)
        )[0][0]

    def event_count(self, parent):
        rows = self._sql("SELECT payload FROM events WHERE kind='job.review_completed'")
        return sum(1 for (payload,) in rows if json.loads(payload).get("id") == parent)

    def reconcile_directly(self, child):
        async def _do():
            async with hive_repo().connection() as tx:
                out = await reconcile_review_parent(tx, hive_repo(), child, now=time.time(), emit_event=emit_event)
                await tx.commit()
                return out

        return _run(_do())

    def set_child_done_raw(self, child, result):
        """Store a child result verbatim, bypassing the PATCH path (no reconcile fires)."""
        self._sql("UPDATE jobs SET status='done', result=? WHERE id=?", (json.dumps(result), child))

    def evidence(self, parent):
        return (self._get(parent).get("result") or {}).get("review_evidence")


def short(sha):
    return sha[:12]


ADVANCED_REASON = (
    f"approved, but the review job advanced the branch: reviewed head {short(ADVANCED)} differs from delivered head {short(DELIVERED)}; "
    "a human or the owner must confirm the branch tip before merging"
)


class LegacyAndWorkerShapeTests(ReviewReconcileShapeBase):
    def test_a_legacy_dict_verdict_still_closes_exact_head_parent(self):
        parent = self.parent_in_review()
        child = self.auto_child(parent)
        self.complete_child(
            child, {"verdict": {"verdict": "approve", "head": DELIVERED}, "deliverable": "Reviewed exact commit."}
        )
        row = self._get(parent)
        self.assertEqual(row["status"], "done")
        ev = row["result"]["review_evidence"]
        self.assertEqual(ev["verdict"], {"verdict": "approve", "head": DELIVERED})
        self.assertTrue(ev["head_matches"])
        self.assertEqual(ev["reason"], "approved exact delivered HEAD")

    def test_a_legacy_dict_verdict_takes_precedence_over_reviewer_shape(self):
        parent = self.parent_in_review()
        child = self.auto_child(parent)
        result = worker_review_result(verdict="request_changes", head=DELIVERED)
        result["verdict"] = {"verdict": "approve", "head": DELIVERED}
        self.complete_child(child, result)
        self.assertEqual(self._get(parent)["status"], "done")

    def test_b_worker_shape_approve_matching_head_closes_parent(self):
        parent = self.parent_in_review()
        child = self.auto_child(parent)
        self.complete_child(child, worker_review_result(verdict="approve", head=DELIVERED, base=DELIVERED))
        row = self._get(parent)
        self.assertEqual(row["status"], "done", row["result"])
        ev = row["result"]["review_evidence"]
        self.assertEqual(ev["review_job_id"], child)
        self.assertEqual(ev["verdict"], {"verdict": "approve", "head": DELIVERED})
        self.assertTrue(ev["head_matches"])
        self.assertTrue(ev["artifact_valid"])
        self.assertEqual(row["result"]["doctor_reason"], "approved exact delivered HEAD")
        self.assertFalse(row["result"]["needs_review"])

    def test_b_close_rule_not_loosened_without_parent_proof(self):
        parent = self.parent_in_review(proof=False)
        child = self.auto_child(parent)
        self.complete_child(child, worker_review_result(verdict="approve", head=DELIVERED, base=DELIVERED))
        row = self._get(parent)
        self.assertEqual(row["status"], "needs-review")
        self.assertEqual(row["result"]["review_evidence"]["reason"], "parent artifact evidence incomplete")
        self.assertFalse(row["result"]["review_evidence"]["artifact_valid"])

    def test_b_reviewed_head_selection_and_case_insensitive_verdict(self):
        cases = [
            # (child fields, expected status, expected reviewed head)
            (dict(head=OTHER, remote_sha=DELIVERED, remote_verified=True), "done", DELIVERED),
            (dict(head=DELIVERED, remote_sha=OTHER, remote_verified=False), "done", DELIVERED),
            (dict(head=DELIVERED, remote_sha=OTHER, remote_verified=True), "needs-review", OTHER),
            (dict(head=OTHER, remote_sha=DELIVERED, remote_verified=False), "needs-review", OTHER),
            (dict(head=DELIVERED, remote_sha="", remote_verified=True), "done", DELIVERED),
        ]
        for fields, expected, reviewed in cases:
            for spelling in ("approve", "APPROVE", " Approve "):
                with self.subTest(fields=fields, spelling=spelling):
                    parent = self.parent_in_review()
                    child = self.auto_child(parent)
                    result = worker_review_result(**fields)
                    result["reviewer"]["verdict"] = spelling
                    result["remote_sha"] = fields["remote_sha"]
                    self.set_child_done_raw(child, result)
                    self.assertEqual(self.reconcile_directly(child), parent)
                    row = self._get(parent)
                    self.assertEqual(row["status"], expected)
                    self.assertEqual(row["result"]["review_evidence"]["verdict"]["head"], reviewed)
                    self.assertEqual(row["result"]["review_evidence"]["verdict"]["verdict"], "approve")

    def test_c_child_advanced_branch_stays_needs_review_with_evidence(self):
        parent = self.parent_in_review()
        child = self.auto_child(parent)
        self.complete_child(child, worker_review_result(verdict="approve", head=ADVANCED, base=DELIVERED))
        row = self._get(parent)
        self.assertEqual(row["status"], "needs-review", "a different head must NEVER close the parent")
        result = row["result"]
        ev = result["review_evidence"]
        self.assertEqual(ev["review_job_id"], child)
        self.assertEqual(ev["verdict"], {"verdict": "approve", "head": ADVANCED})
        self.assertIs(ev["head_matches"], False)
        self.assertEqual(ev["reviewed_head"], ADVANCED)
        self.assertEqual(ev["delivered_head"], DELIVERED)
        self.assertIs(ev["child_advanced_branch"], True)
        self.assertEqual(ev["approved_unmerged_tip"], ADVANCED)
        self.assertEqual(ev["reason"], ADVANCED_REASON)
        self.assertEqual(result["doctor_reason"], ADVANCED_REASON)
        self.assertIs(result["needs_review"], True)
        self.assertEqual(result["auto_review_job_id"], child)
        self.assertEqual(result["head_sha"], DELIVERED, "the delivered head is never rewritten")

    def test_c_not_advanced_when_child_base_is_not_the_delivered_head(self):
        parent = self.parent_in_review()
        child = self.auto_child(parent)
        self.complete_child(child, worker_review_result(verdict="approve", head=ADVANCED, base=OTHER))
        row = self._get(parent)
        self.assertEqual(row["status"], "needs-review")
        ev = row["result"]["review_evidence"]
        self.assertIs(ev["child_advanced_branch"], False)
        self.assertNotIn("approved_unmerged_tip", ev)
        self.assertIs(ev["head_matches"], False)

    def test_c_legacy_dict_with_different_head_gets_the_same_outcome(self):
        parent = self.parent_in_review()
        child = self.auto_child(parent)
        self.complete_child(
            child, {"verdict": {"verdict": "approve", "head": ADVANCED}, "deliverable": "Reviewed another commit."}
        )
        row = self._get(parent)
        self.assertEqual(row["status"], "needs-review")
        self.assertIs(row["result"]["review_evidence"]["head_matches"], False)
        self.assertEqual(row["result"]["review_evidence"]["reviewed_head"], ADVANCED)

    def test_d_request_changes_via_reviewer_verdict_stays_needs_review(self):
        parent = self.parent_in_review()
        child = self.auto_child(parent)
        self.complete_child(child, worker_review_result(verdict="request_changes", head=DELIVERED, base=DELIVERED))
        row = self._get(parent)
        self.assertEqual(row["status"], "needs-review")
        ev = row["result"]["review_evidence"]
        self.assertEqual(ev["review_job_id"], child)
        self.assertEqual(ev["verdict"]["verdict"], "request_changes")
        self.assertEqual(ev["reason"], "review did not approve")
        self.assertEqual(row["result"]["doctor_reason"], "review did not approve")
        self.assertIs(row["result"]["needs_review"], True)


class NoUsableVerdictTests(ReviewReconcileShapeBase):
    NO_VERDICT_REASON = "review result has no usable verdict"

    def _no_verdict_result(self):
        return worker_review_result(verdict=None, head=ADVANCED, base=DELIVERED)

    def test_e_no_usable_verdict_records_evidence_and_logs_once(self):
        parent = self.parent_in_review()
        child = self.auto_child(parent)
        before = self._get(parent)["result"]
        with self.assertLogs(LOGGER, level="WARNING") as logs:
            self.complete_child(child, self._no_verdict_result())
            self.repatch_child_done(child, self._no_verdict_result())  # second reconcile: no second line
        row = self._get(parent)
        self.assertEqual(row["status"], "needs-review")
        self.assertEqual(
            row["result"]["review_evidence"],
            {"review_job_id": child, "verdict": None, "reason": self.NO_VERDICT_REASON},
        )
        self.assertEqual(row["result"]["doctor_reason"], before["doctor_reason"], "doctor_reason untouched")
        lines = [r.getMessage() for r in logs.records if child in r.getMessage()]
        self.assertEqual(len(lines), 1, lines)
        self.assertIn("review_job_id=" + child, lines[0])
        self.assertIn("reason=" + json.dumps(self.NO_VERDICT_REASON), lines[0])

    def test_e_unusable_reviewer_shapes_are_all_flagged(self):
        for label, mutate in {
            "reviewer null": lambda r: r.update(reviewer=None),
            "reviewer verdict null": lambda r: r["reviewer"].update(verdict=None),
            "reviewer verdict blank": lambda r: r["reviewer"].update(verdict="  "),
            "reviewer verdict not a string": lambda r: r["reviewer"].update(verdict=3),
            "legacy verdict without string": lambda r: r.update(verdict={"verdict": None}),
            "no reviewer key": lambda r: r.pop("reviewer"),
        }.items():
            with self.subTest(label):
                parent = self.parent_in_review()
                child = self.auto_child(parent)
                result = worker_review_result(verdict="approve")
                mutate(result)
                result["review_evidence"] = {}  # bus must not re-derive a reviewer from this
                self.set_child_done_raw(child, result)
                with self.assertLogs(LOGGER, level="WARNING"):
                    self.assertEqual(self.reconcile_directly(child), parent)
                row = self._get(parent)
                self.assertEqual(row["status"], "needs-review")
                self.assertEqual(
                    row["result"]["review_evidence"],
                    {"review_job_id": child, "verdict": None, "reason": self.NO_VERDICT_REASON},
                )

    def test_e_log_line_is_per_review_job_and_reason(self):
        parents = [self.parent_in_review() for _ in range(2)]
        children = [self.auto_child(p) for p in parents]
        with self.assertLogs(LOGGER, level="WARNING") as logs:
            for child in children:
                self.set_child_done_raw(child, self._no_verdict_result())
                self.reconcile_directly(child)
                self.reconcile_directly(child)
        for child in children:
            self.assertEqual(len([r for r in logs.records if child in r.getMessage()]), 1)


class GuardTests(ReviewReconcileShapeBase):
    """(f) the same guards as before apply to every shape, including no-verdict."""

    def _guarded_parent(self, label):
        if label == "protected":
            return self.parent_in_review(extra={"failure_reason": "dlq_protected:other-owner"})
        if label == "triage":
            return self.parent_in_review(kind="triage:widget")
        return self.parent_in_review(extra={"auto_review_job_id": "some-other-review-job"})

    def test_f_guarded_parents_are_unchanged_for_every_child_shape(self):
        shapes = {
            "worker approve exact": worker_review_result(verdict="approve", head=DELIVERED, base=DELIVERED),
            "worker approve advanced": worker_review_result(verdict="approve", head=ADVANCED, base=DELIVERED),
            "worker request_changes": worker_review_result(verdict="request_changes"),
            "no usable verdict": worker_review_result(verdict=None),
            "legacy dict": {"verdict": {"verdict": "approve", "head": DELIVERED}, "deliverable": "Reviewed."},
        }
        for label in ("protected", "triage", "other owner"):
            for shape, result in shapes.items():
                with self.subTest(parent=label, child=shape):
                    parent = self._guarded_parent(label)
                    before = self._get(parent)
                    audits = self._sql("SELECT COUNT(*) FROM job_audit_log WHERE job_id=?", (parent,))[0][0]
                    child = self.manual_child(parent)
                    self.complete_child(child, copy.deepcopy(result))
                    self.assertEqual(self._get(child)["status"], "done")
                    after = self._get(parent)
                    self.assertEqual(after["status"], "needs-review")
                    self.assertEqual(after["result"], before["result"], "guarded parent result must not change")
                    self.assertNotIn("review_evidence", after["result"])
                    self.assertEqual(
                        self._sql("SELECT COUNT(*) FROM job_audit_log WHERE job_id=?", (parent,))[0][0], audits
                    )
                    self.assertEqual(self.event_count(parent), 0)

    def test_f_parent_not_needs_review_is_untouched(self):
        parent = self.parent_in_review()
        child = self.auto_child(parent)
        self.complete_child(child, worker_review_result(verdict="approve", head=DELIVERED, base=DELIVERED))
        self.assertEqual(self._get(parent)["status"], "done")
        before = self._get(parent)
        second = self.manual_child(parent)
        self.complete_child(second, worker_review_result(verdict=None))
        self.assertEqual(self._get(parent)["result"], before["result"])
        self.assertEqual(self._get(parent)["status"], "done")


class IdempotencyTests(ReviewReconcileShapeBase):
    def _assert_idempotent(self, parent, child, result):
        self.complete_child(child, result)
        row = self._get(parent)
        audits, events = self.audit_count(parent), self.event_count(parent)
        self.assertEqual(audits, 1)
        self.assertEqual(events, 1)
        self.repatch_child_done(child, result)
        self.assertEqual(self.reconcile_directly(child), parent)
        self.assertEqual(self.audit_count(parent), audits, "no second audit row")
        self.assertEqual(self.event_count(parent), events, "no second event")
        self.assertEqual(self._get(parent)["result"], row["result"])
        self.assertEqual(self._get(parent)["status"], row["status"])

    def test_g_idempotent_for_each_outcome(self):
        shapes = {
            "closed": worker_review_result(verdict="approve", head=DELIVERED, base=DELIVERED),
            "advanced": worker_review_result(verdict="approve", head=ADVANCED, base=DELIVERED),
            "request_changes": worker_review_result(verdict="request_changes"),
            "no usable verdict": worker_review_result(verdict=None, head=ADVANCED),
        }
        for label, result in shapes.items():
            with self.subTest(label):
                parent = self.parent_in_review()
                self._assert_idempotent(parent, self.auto_child(parent), result)

    def test_g_changed_verdict_for_same_review_job_is_recorded_again(self):
        parent = self.parent_in_review()
        child = self.auto_child(parent)
        self.complete_child(child, worker_review_result(verdict="request_changes"))
        self.assertEqual(self.audit_count(parent), 1)
        self.repatch_child_done(child, worker_review_result(verdict="approve", head=DELIVERED, base=DELIVERED))
        self.assertEqual(self._get(parent)["status"], "done")
        self.assertEqual(self.audit_count(parent), 2)

    def test_g_a_different_later_review_job_still_loses_to_ownership(self):
        parent = self.parent_in_review()
        owner = self.auto_child(parent)
        self.complete_child(owner, worker_review_result(verdict="approve", head=ADVANCED, base=DELIVERED))
        before = self._get(parent)
        audits = self.audit_count(parent)
        late = self.manual_child(parent)
        self.complete_child(late, worker_review_result(verdict="approve", head=DELIVERED, base=DELIVERED))
        after = self._get(parent)
        self.assertEqual(after["status"], "needs-review", "a non-owner review must not close the parent")
        self.assertEqual(after["result"], before["result"])
        self.assertEqual(after["result"]["review_evidence"]["review_job_id"], owner)
        self.assertEqual(self.audit_count(parent), audits)
        late_nv = self.manual_child(parent)
        self.complete_child(late_nv, worker_review_result(verdict=None))
        self.assertEqual(self._get(parent)["result"], before["result"])


class ContractTests(ReviewReconcileShapeBase):
    """(h) the exact worker result shape: producer/consumer drift fails here."""

    # Verbatim key set of a live review-child result, values anonymised.
    WORKER_RESULT = {
        "verdict": None,
        "reviewer": {
            "model": "cerberus-reviewer",
            "models": ["cerberus-reviewer"],
            "verdict": "approve",
            "source": "zoder_verdict",
        },
        "review_evidence": {
            "requested": "",
            "reviewers": [
                {"model": "cerberus-reviewer", "role": "", "family": "openai", "verdict": "approve", "ok": True}
            ],
            "first": {"model": "cerberus-reviewer", "verdict": "approve"},
            "secondaries": [],
            "skipped": [],
            "warnings": [],
        },
        "head_sha": ADVANCED,
        "remote_sha": ADVANCED,
        "base_sha": DELIVERED,
        "remote_verified": True,
        "loop_result": {
            "teardown": {
                "final_verdict": "approve",
                "reason": "approved",
                "resolved": True,
                "last_good_head_sha": ADVANCED,
            }
        },
        "exit_code": 0,
        "commits": [ADVANCED],
        "files_changed": ["pkg/widget.py"],
        "worktree_clean": True,
    }

    def test_h_exact_worker_result_shape_is_consumed(self):
        parent = self.parent_in_review()
        child = self.auto_child(parent)
        self.complete_child(child, copy.deepcopy(self.WORKER_RESULT))
        stored = self._get(child)["result"]
        # What the bus stored for the child still has no usable top-level verdict ...
        self.assertIsNone(stored["verdict"])
        self.assertEqual(stored["reviewer"]["verdict"], "approve")
        # ... and the consumer still reads it.
        row = self._get(parent)
        self.assertEqual(row["status"], "needs-review")
        ev = row["result"]["review_evidence"]
        self.assertEqual(ev["verdict"], {"verdict": "approve", "head": ADVANCED})
        self.assertIs(ev["child_advanced_branch"], True)

    def test_h_same_shape_with_matching_head_closes(self):
        parent = self.parent_in_review()
        child = self.auto_child(parent)
        result = copy.deepcopy(self.WORKER_RESULT)
        result.update(head_sha=DELIVERED, remote_sha=DELIVERED, commits=[DELIVERED])
        self.complete_child(child, result)
        self.assertEqual(self._get(parent)["status"], "done")

    def test_h_producer_reviewer_record_matches_the_consumer_contract(self):
        # The producer is hive/review_record.py (shared by worker and bus): the
        # reviewer record it derives from a structured verdict must carry the
        # keys and the verdict the consumer reads from ``result.reviewer``.
        produced = {"review_evidence": copy.deepcopy(self.WORKER_RESULT["review_evidence"])}
        review_record.apply(produced)
        self.assertEqual(produced["reviewer"], self.WORKER_RESULT["reviewer"])
        self.assertEqual(set(produced["reviewer"]), {"model", "models", "verdict", "source"})
        self.assertNotIn("verdict", produced, "the producer does not emit a top-level verdict")

    def test_h_fixture_has_the_documented_key_list(self):
        self.assertEqual(
            set(self.WORKER_RESULT),
            {
                "verdict",
                "reviewer",
                "review_evidence",
                "head_sha",
                "remote_sha",
                "base_sha",
                "remote_verified",
                "loop_result",
                "exit_code",
                "commits",
                "files_changed",
                "worktree_clean",
            },
        )
        self.assertEqual(
            set(self.WORKER_RESULT["loop_result"]["teardown"]),
            {"final_verdict", "reason", "resolved", "last_good_head_sha"},
        )
        self.assertEqual(set(self.WORKER_RESULT), set(worker_review_result()), "fixture factory drifted")


if __name__ == "__main__":
    logging.basicConfig()
    unittest.main()
