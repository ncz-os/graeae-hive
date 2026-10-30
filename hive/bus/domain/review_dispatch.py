"""Transactional review dispatch when a job enters needs-review.

The caller owns commit/rollback. The doctor reconciles older rows and uses the
same idempotency key, so a simultaneous submission cannot create two reviews.
"""

from __future__ import annotations

import json
import logging
import uuid

from hive.workers import doctor_decline_paths
from queue_logic import dedup_key

log = logging.getLogger("hive.bus.review_dispatch")

NO_USABLE_VERDICT = "review result has no usable verdict"
_SHORT = 12
# (review_job_id, reason) pairs already logged by this process. A completed
# child can be re-reconciled (idempotent PATCH, doctor sweep); the log line must
# say it once. Bounded: cleared wholesale rather than growing without limit.
_LOGGED: set[tuple[str, str]] = set()
_LOGGED_MAX = 4096


def protected_review_row(job: dict) -> bool:
    return doctor_decline_paths.review_is_never_act(job)


async def dispatch_needs_review(tx, repository, job_id: str, *, now: float, emit_event=None) -> str | None:
    """Insert child + parent marker atomically, preserving delivered evidence.

    This internal queue repair deliberately has no author/provider family pin.
    A unique-dedup conflict propagates: the enclosing transition rolls back and
    can be retried; never commit a needs-review transition with half a dispatch.
    """
    async with tx.execute(
        "SELECT status, kind, result, repo, head_branch, base_branch, project, priority, submitter_urn FROM jobs WHERE id=?",
        (job_id,),
    ) as cursor:
        row = await cursor.fetchone()
    if not row or row[0] != "needs-review":
        return None
    status, kind, raw, repo, branch, base, project, priority, submitter = row
    # Triage rows are remediation reports, not branches to review.
    if str(kind or "").startswith("triage:"):
        return None
    result = json.loads(raw) if isinstance(raw, str) and raw else (raw or {})
    if not isinstance(result, dict):
        result = {}
    job = {"id": job_id, "kind": kind, "status": status, "project": project, "result": result}
    if protected_review_row(job) or result.get("review_evidence") or result.get("verdict"):
        return None
    if result.get("auto_review_job_id"):
        return str(result["auto_review_job_id"])
    total, children = await repository.list_jobs(tx, parent_job_id=job_id, limit=1000)
    child_id = next((child["id"] for child in children if str(child.get("kind") or "").startswith("review:")), None)
    if total > len(children) and not child_id:
        raise RuntimeError("incomplete review child listing")
    if not child_id:
        child_id = str(uuid.uuid4())
        head = result.get("head_sha") or "(resolve and report)"
        review_kind = "review:" + str(kind).removeprefix("review:")
        description = (
            "READ-ONLY review. Do not modify any file, branch, or commit. "
            f"Review branch {branch or '(see job)'} at HEAD {head} against base {base or '(default)'}. "
            "Return structured verdict JSON with verdict (approve | request-changes | blocked), "
            f"reason and head_sha (the exact commit reviewed). Reviewed job id: {job_id}. No commits expected."
        )
        await repository.submit_job(
            tx,
            id=child_id,
            submitter_urn=submitter,
            parent_job_id=job_id,
            kind=review_kind,
            description=description,
            priority=max(100, priority or 0),
            started_at=now,
            repo=repo,
            head_branch=branch,
            base_branch=base,
            project=project,
            max_retries=2,
            max_cost_tier="C",
            dedup_hash=dedup_key(idempotency_key=f"needs-review-followup:{job_id}"),
        )
        if emit_event:
            await emit_event(
                tx,
                "job.queued",
                {"id": child_id, "kind": review_kind, "parent_job_id": job_id, "priority": max(100, priority or 0)},
                commit=False,
            )
    result["auto_review_job_id"] = child_id
    result["doctor_reason"] = f"auto review submitted {child_id}"
    updated = await repository.patch_job_status(
        tx, job_id, sets={"result": json.dumps(result)}, expected_status="needs-review"
    )
    if not updated:
        raise RuntimeError("review parent status changed concurrently")
    return child_id


def _result(raw) -> dict:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
            return value if isinstance(value, dict) else {}
        except (TypeError, ValueError):
            return {}
    return {}


def _log_once(review_job_id: str, parent_id: str, reason: str, outcome: str) -> None:
    key = (review_job_id, reason)
    if key in _LOGGED:
        return
    if len(_LOGGED) >= _LOGGED_MAX:
        _LOGGED.clear()
    _LOGGED.add(key)
    log.warning(
        "review_reconcile review_job_id=%s parent_job_id=%s reason=%s outcome=%s",
        review_job_id,
        parent_id,
        json.dumps(reason),
        outcome,
    )


def _text(value) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def child_verdict(child_result: dict) -> dict | None:
    """The review child's verdict as ``{"verdict": str, "head": str | None}``, or None.

    Two producer shapes exist. The legacy one is a top-level ``result.verdict``
    dict with a string ``verdict`` (returned unchanged). The real worker result
    has ``result.verdict`` null and the verdict at ``result.reviewer.verdict``
    (``hive/review_record.py``), next to the shas of the commit the review job
    left the branch at: ``remote_sha`` when ``remote_verified`` is true (what
    the remote really holds), else ``head_sha``. Anything else is unusable.
    """
    verdict = child_result.get("verdict")
    if isinstance(verdict, dict) and _text(verdict.get("verdict")):
        return verdict
    reviewer = child_result.get("reviewer")
    text = _text(reviewer.get("verdict")) if isinstance(reviewer, dict) else None
    if not text:
        return None
    remote = _text(child_result.get("remote_sha")) if child_result.get("remote_verified") is True else None
    return {"verdict": text.lower(), "head": remote or _text(child_result.get("head_sha"))}


def _parent_skip_reason(parent, result: dict, review_job_id: str) -> str | None:
    """Why this review must not touch the parent (None: it may)."""
    if not parent or parent[0] != "needs-review":
        return "parent is not needs-review"
    # An old review child must not resolve a triage remediation report.
    if str(parent[1] or "").startswith("triage:"):
        return "parent is a triage report"
    if protected_review_row({"kind": parent[1], "project": parent[3], "result": result}):
        return "parent is a protected row"
    if result.get("auto_review_job_id") and result["auto_review_job_id"] != review_job_id:
        return "a different review owns this parent"
    return None


async def reconcile_review_parent(tx, repository, review_job_id: str, *, now: float, emit_event=None) -> str | None:
    """Consume a completed review, never equating submission with approval.

    Approval completes code work only for the exact delivered HEAD with valid
    parent artifact proof. Rejection, mismatched HEAD or missing artifact proof
    remains needs-review with the child's verdict and the blocking reason.
    An approval of a DIFFERENT head (a review job that advanced the branch)
    also stays needs-review: the owner must confirm the tip before merging. A
    child with no usable verdict is never silent: it is logged once and
    recorded as evidence on the parent (status unchanged).
    """
    from hive.bus.domain.jobs import valid_completion_proof

    async with tx.execute(
        "SELECT status, kind, parent_job_id, result, claimed_by, project FROM jobs WHERE id=?",
        (review_job_id,),
    ) as cursor:
        child = await cursor.fetchone()
    if not child or child[0] != "done" or not str(child[1]).startswith("review:") or not child[2]:
        return None
    child_result = _result(child[3])
    verdict = child_verdict(child_result)
    parent_id = child[2]
    async with tx.execute(
        "SELECT status, kind, result, project, claimed_by FROM jobs WHERE id=?", (parent_id,)
    ) as cursor:
        parent = await cursor.fetchone()
    result = _result(parent[2]) if parent else {}
    skip = _parent_skip_reason(parent, result, review_job_id)
    if skip:
        if verdict is None:
            _log_once(review_job_id, parent_id, f"{NO_USABLE_VERDICT}; parent untouched: {skip}", "ignored")
        return None
    previous = result.get("review_evidence")
    if verdict is None:
        evidence = {"review_job_id": review_job_id, "verdict": None, "reason": NO_USABLE_VERDICT}
        if previous == evidence:
            return parent_id
        _log_once(review_job_id, parent_id, NO_USABLE_VERDICT, "evidence_recorded")
        result["review_evidence"] = evidence
        return await _write_evidence(
            tx,
            repository,
            review_job_id,
            parent_id,
            parent,
            child,
            result,
            evidence["reason"],
            "needs-review",
            now=now,
            emit_event=emit_event,
        )
    reviewed_head = verdict.get("head") or verdict.get("head_sha")
    delivered_head = result.get("head_sha")
    exact_head = bool(reviewed_head and reviewed_head == delivered_head)
    artifact_valid = valid_completion_proof(result)
    approved = verdict["verdict"].strip().lower() == "approve"
    complete = approved and exact_head and artifact_valid
    reason = (
        "approved exact delivered HEAD"
        if complete
        else "review did not approve"
        if not approved
        else "review HEAD does not match delivered HEAD"
        if not exact_head
        else "parent artifact evidence incomplete"
    )
    evidence = {
        "review_job_id": review_job_id,
        "verdict": verdict,
        "head_matches": exact_head,
        "artifact_valid": artifact_valid,
        "reason": reason,
    }
    if approved and not exact_head and _text(reviewed_head) and _text(delivered_head):
        # Approved, but not the commit that was delivered. A review-and-fix loop
        # moves the branch: its base is the delivered head, its head is new.
        advanced = child_result.get("base_sha") == delivered_head
        shown = f"reviewed head {reviewed_head[:_SHORT]} differs from delivered head {delivered_head[:_SHORT]}"
        reason = (
            f"approved, but the review job advanced the branch: {shown}; " if advanced else f"approved, but {shown}; "
        ) + "a human or the owner must confirm the branch tip before merging"
        evidence.update(
            reason=reason,
            reviewed_head=reviewed_head,
            delivered_head=delivered_head,
            child_advanced_branch=advanced,
        )
        if advanced:
            # Documented evidence field, NOT a job status: the approved tip that is not merged.
            evidence["approved_unmerged_tip"] = reviewed_head
    if previous == evidence:
        return parent_id
    result["auto_review_job_id"] = review_job_id
    result["review_evidence"] = evidence
    result["needs_review"] = not complete
    result["doctor_reason"] = reason
    new_status = "done" if complete else "needs-review"
    return await _write_evidence(
        tx,
        repository,
        review_job_id,
        parent_id,
        parent,
        child,
        result,
        reason,
        new_status,
        now=now,
        emit_event=emit_event,
    )


async def _write_evidence(
    tx, repository, review_job_id, parent_id, parent, child, result, reason, new_status, *, now, emit_event
) -> str:
    sets = {"result": json.dumps(result)}
    if new_status == "done":
        sets.update(status="done", ended_at=now, last_update_at=now)
        result["status"] = "done"
        sets["result"] = json.dumps(result)
    if not await repository.patch_job_status(tx, parent_id, sets=sets, expected_status="needs-review"):
        raise RuntimeError("review parent changed during verdict reconciliation")
    await repository.insert_job_audit(
        tx,
        job_id=parent_id,
        ts=now,
        actor_urn=child[4],
        old_status="needs-review",
        new_status=new_status,
        old_claimed_by=parent[4],
        new_claimed_by=parent[4],
        patch_json=json.dumps({"review_job_id": review_job_id, "reason": reason}),
    )
    if emit_event:
        await emit_event(
            tx,
            "job.review_completed",
            {"id": parent_id, "review_job_id": review_job_id, "status": new_status, "reason": reason},
            commit=False,
        )
    return parent_id
