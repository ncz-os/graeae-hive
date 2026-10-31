# ── review queue inside the triage queue (operator 2026-10-31) ─────────────
# The review queue IS the triage queue: the Hive job rows in needs-review this
# sweep already reads. A row whose loop authored work but got no reviewer (the
# endpoint was busy, timed out, answered prose, or returned 503) is a
# needs-review row carrying result.review_state.state='awaiting_review' -- the
# worker puts it there instead of failing it. This sweep consumes those rows:
# it picks a CROSS-FAMILY reviewer from hive/core/reviewer-roster.json for the
# author's family, submits ONE read-only review job naming that reviewer,
# records the attempt on the same row, and on a structured APPROVE hands the
# row to the merge path, on request_changes back to the author path, and on a
# reviewer failure schedules the next attempt with exponential backoff bounded
# by a per-route concurrency cap. A row with no cross-family reviewer available
# stays awaiting_review (parked): never self-reviewed, never failed.


def _same_model(a: Any, b: Any) -> bool:
    return str(a or "").strip().lower() == str(b or "").strip().lower()


def _author_model(job: dict, result: dict) -> str:
    """The author model of a row: result.review_state, models, or the claim."""
    state = review_queue.review_state(job) or {}
    for candidate in (
        state.get("author"),
        (result.get("models") or {}).get("author") if isinstance(result.get("models"), dict) else None,
        result.get("author_model"),
        job.get("claimed_model"),
    ):
        if str(candidate or "").strip():
            return str(candidate).strip()
    return ""


def _review_head(job: dict, result: dict) -> str:
    return review_verdict.delivered_head(job, result) or str(result.get("head_sha") or job.get("head_sha") or "")


def _excluded_families(author: str, *others: Any) -> set:
    """Every family a reviewer may not have: the author's and each co-author's."""
    families = {review_queue.family_of(m) for m in [author, *others] if str(m or "").strip()}
    families.discard("")
    return families


def interpret_review_outcome(raw: Any, *, head_sha: str | None = None) -> dict:
    """Read a review job's output as a structured verdict -- or refuse it.

    Only a structured JSON verdict naming ``reviewers[].model`` counts
    (operator 2026-10-31). Prose, an empty body, a truncated fragment, a
    503-style error object or JSON without a reviewer list is NOT an approval:
    every one raises ValueError with the failure kind the sweep records on the
    row (``review_prose_only``, ``review_empty``, ``review_truncated``,
    ``review_http_503``, ``review_non_json``). An approve for a head other than
    the delivered one is ``review_head_mismatch``.
    """
    payload = raw
    if isinstance(payload, str):
        text = payload.strip()
        if not text:
            raise ValueError("review_empty")
        if text[:1] not in "{[":
            raise ValueError("review_truncated" if text[-1:] not in "}]" else "review_prose_only")
        try:
            payload = json.loads(text)
        except ValueError:
            raise ValueError("review_truncated" if text[-1:] not in "}]" else "review_non_json") from None
    if isinstance(payload, dict) and isinstance(payload.get("result"), dict):
        payload = payload["result"]  # a raw job row: read its result
    if not isinstance(payload, dict):
        raise ValueError("review_non_json")
    if payload.get("http_status") == 503 or payload.get("status_code") == 503:
        raise ValueError("review_http_503")
    entries = payload.get("reviewers")
    if not isinstance(entries, list) or not entries:
        raise ValueError("review_no_reviewers")
    reviewers, verdicts = [], []
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("review_no_reviewers")
        model = str(entry.get("model") or entry.get("model_id") or "").strip()
        verdict = review_verdict.normalize_verdict(entry.get("verdict"))
        if not model or not verdict:
            raise ValueError("review_no_reviewers")
        reviewers.append(model)
        verdicts.append(verdict)
    unreviewed = all(not review_queue.family_of(m) for m in reviewers)
    reviewing = [v for m, v in zip(reviewers, verdicts) if review_queue.family_of(m)]
    reviewed_head = str(payload.get("head_sha") or "").strip()
    if "approve" in reviewing:
        if not head_sha or review_verdict.same_sha(reviewed_head, head_sha):
            kind = "approve"
        else:
            raise ValueError("review_head_mismatch")
    elif "request-changes" in reviewing:
        kind = "request_changes"
    elif review_verdict.normalize_verdict(payload.get("verdict")) == "request-changes":
        kind = "request_changes"
    elif reviewing and all(v == "blocked" for v in reviewing):
        kind = "reviewer_failure"
    else:
        raise ValueError("review_undecided")
    return {
        "kind": kind,
        "reviewers": reviewers,
        "head_sha": reviewed_head or head_sha or "",
        "verdicts": verdicts,
        "unreviewed": unreviewed,
    }


def merge_handoff(job: dict) -> str:
    """The merge path's verdict on a row: approved rows are ready to merge.

    ``doctor.review_approved`` names the row and the approving reviewers; the
    merge train (``hive/workers/merge_train.py``) consumes needs-review rows,
    so an approved row stays exactly where it was -- in the triage queue.
    """
    state = review_queue.review_state(job) or {}
    approved = state.get("approved")
    if not isinstance(approved, dict) or not approved.get("reviewers"):
        return "needs_review"
    if approved.get("head_sha") and not review_verdict.same_sha(approved["head_sha"], _review_head(job, _row_result_dict(job))):
        return "stale_approval"
    if review_followup_protected(job):
        return "protected"
    return "ready_to_merge"


def _review_queue_rows(rows: list[dict]) -> list[dict]:
    """The awaiting_review rows of a needs-review listing (they ARE the queue)."""
    return [row for row in rows if review_queue.review_state(row) is not None]


def _review_child_outcome(child: dict) -> dict | None:
    """Read one child review job's output; None while it has not finished."""
    status = str(child.get("status") or "")
    if status not in _REVIEW_CHILD_TERMINAL:
        return None
    proof = _row_result_dict(child)
    sources = []
    for key in ("review_evidence", "verdict"):
        if proof.get(key):
            sources.append(proof[key])
    tail = proof.get("tail")
    if isinstance(tail, str) and tail.strip():
        sources.append(tail)
    if result_text := proof.get("stdout"):
        sources.append(result_text) if isinstance(result_text, str) else None
    if not sources:
        return {"kind": "reviewer_failure", "failure": "review_empty", "reviewers": []}
    if status != "done" and not proof.get("verdict"):
        return {"kind": "reviewer_failure", "failure": "review_http_503" if "503" in str(tail) else "review_failed", "reviewers": []}
    failure = None
    for candidate in sources:
        try:
            return interpret_review_outcome(candidate, head_sha=_review_head_by_state(child))
        except ValueError as exc:
            failure = str(exc)
    return {"kind": "reviewer_failure", "failure": failure or "review_no_reviewers", "reviewers": []}


def _review_head_by_state(source: dict) -> str:
    result = _row_result_dict(source)
    return str(result.get("head_sha") or source.get("head_sha") or "")


def _tried_entry(entry: dict, failure: str, now: float) -> dict:
    return {"model_id": entry.get("model_id"), "family": entry.get("family"), "failure": failure, "at": now}


def _patch_review_state(job: dict, state: dict, *, verdict: dict | None = None) -> bool:
    """Record the queue state on the SAME row (still needs-review: the queue IS triage)."""
    jid = str(job.get("id") or "")
    res = _row_result_dict(job)
    body_result = {**res, "review_state": state}
    body: dict = {"status": "needs-review", "result": body_result}
    if verdict is not None:
        body_result["verdict"] = verdict
        body_result["review_evidence"] = {
            **(body_result.get("review_evidence") if isinstance(body_result.get("review_evidence"), dict) else {}),
            **verdict,
            "review_job_id": state.get("review_job_id"),
        }
        review_record.apply(body_result, launched=verdict.get("reviewers", [""])[0] if verdict.get("reviewers") else None)
    if job.get("claimed_by"):
        body["claimed_by"] = job["claimed_by"]
    code, _ = _http("PATCH", f"/v1/jobs/{jid}", body)
    if code not in (200, 201, 204):
        broadcast_doctor_message(
            "doctor.sweep_error", {"sweep": "review-queue", "job_id": jid, "error": f"annotation failed http={code}"}
        )
        return False
    return True


def _resume_state(state: dict) -> dict:
    """Clear a transient parked reason before a new attempt (a retry is possible)."""
    out = dict(state)
    if out.get("parked_reason") == "in_backoff_or_parked_not_supported":
        out.pop("parked_reason", None)
    return out


def _review_queue_submit(job: dict, state: dict, reviewers: list[dict], *, now: float) -> tuple[str, dict]:
    """Submit ONE read-only review job naming the cross-family pick(s)."""
    jid = str(job.get("id") or "")
    res = _row_result_dict(job)
    kind = str(job.get("kind") or "").strip()
    base_kind = kind[len("review:") :] if kind.startswith("review:") else (kind[len("triage:") :] if kind.startswith("triage:") else _base_kind(kind))
    date = time.strftime("%Y%m%d", time.gmtime())
    review_kind = f"review:{base_kind}-{date}" if base_kind else f"review:{date}"
    head = job.get("head_branch") or ""
    head_sha = _review_head(job, res)
    base = job.get("base_branch") or ""
    author = state.get("author") or ""
    names = [r["model_id"] for r in reviewers]
    desc = (
        "READ-ONLY review. Do not modify any file, branch, or commit. "
        f"Review the branch {head or '(see job)'} at HEAD {head_sha or '(resolve and report)'} against base {base or '(default)'} "
        "with a reviewer from a DIFFERENT model family than the author "
        f"(author {author or 'unknown'}, family {state.get('author_family') or 'unknown'}; "
        f"review with {', '.join(names)}). "
        "Return a structured verdict JSON with a 'reviewers' list of {model, verdict} entries "
        "(verdict: approve | request-changes | blocked), a 'head_sha' field containing the exact reviewed commit, "
        f"and a 'reason' field. Prose is not a verdict. Reviewed job id: {jid}. No commits expected."
    )
    attempt = int(state.get("attempt") or 0)
    spec = {
        "kind": review_kind,
        "description": desc,
        "repo": job.get("repo"),
        "head_branch": head or None,
        "base_branch": base or None,
        "eligible_kinds": list(REVIEW_FOLLOWUP_ELIGIBLE_KINDS),
        "preferred_models": names[:1],
        "max_cost_tier": REVIEW_FOLLOWUP_MAX_COST_TIER,
        "priority": max(REVIEW_FOLLOWUP_PRIORITY, job.get("priority") or 0),
        "project": job.get("project"),
        "parent_job_id": jid,
        "idempotency_key": f"review-queue:{jid}:{attempt}:{head_sha[:12] or 'nohead'}",
    }
    code, resp = submit_job_spec(spec)
    new_id = (resp or {}).get("id") if isinstance(resp, dict) else None
    if code in (200, 201) and new_id:
        return str(new_id), spec
    raise RuntimeError(f"review submit failed code={code} resp={str(resp)[:160]}")


def process_awaiting_review_row(
    job: dict, *, now: float | None = None, health=None, load=None, roster: list[dict] | None = None
) -> tuple[str, dict | None, dict | None]:
    """Advance ONE awaiting_review row. Returns (outcome, new_state, verdict).

    Outcomes: ``skipped_status``/``skipped_triage``/``skipped_protected``,
    ``deferred`` (backoff not elapsed), ``awaited`` (a review job is in
    flight), ``submitted``, ``parked`` (no cross-family reviewer),
    ``exhausted`` (attempt budget spent: the row waits for an owner),
    ``approved``, ``request_changes`` and ``review_failed``.

    The row never leaves needs-review: there is no separate review status.
    """
    now = time.time() if now is None else float(now)
    jid = str(job.get("id") or "")
    if not jid or job.get("status") != "needs-review":
        return "skipped_status", None, None
    if str(job.get("kind") or "").startswith("triage:"):
        return "skipped_triage", None, None
    state = review_queue.review_state(job)
    if state is None:
        return "skipped_status", None, None
    if review_followup_protected(job):
        return "skipped_protected", None, None
    result = _row_result_dict(job)
    head = _review_head(job, result)
    # A submitted review in flight: read its outcome, never submit a second one.
    in_flight = str(state.get("review_job_id") or result.get("auto_review_job_id") or "")
    if in_flight:
        children = _needs_review_child_review_jobs(job)
        child = next((c for c in children if str(c.get("id")) == in_flight), None)
        if child is not None:
            outcome = _review_child_outcome(child)
            if outcome is None:
                return "awaited", state, None
            if outcome["kind"] == "approve":
                approved = {
                    "reviewers": outcome["reviewers"],
                    "head_sha": head,
                    "review_job_id": in_flight,
                    "at": now,
                }
                new_state = {**state, "review_job_id": None, "approved": approved, "updated_at": now}
                _patch_review_state(job, new_state, verdict={"reviewers": [{"model": m, "verdict": "approve"} for m in outcome["reviewers"]], "head_sha": head})
                broadcast_doctor_message(
                    "doctor.review_approved",
                    {
                        "host": AGENT_HOST,
                        "job_id": jid,
                        "reviewers": outcome["reviewers"],
                        "review_job_id": in_flight,
                        "merge": merge_handoff({**job, "result": {**result, "review_state": new_state}}),
                    },
                )
                return "approved", new_state, outcome
            if outcome["kind"] == "request_changes":
                changes = {"reviewers": outcome["reviewers"], "head_sha": outcome["head_sha"] or head, "review_job_id": in_flight, "at": now}
                new_state = {**state, "review_job_id": None, "request_changes": changes, "updated_at": now}
                _patch_review_state(
                    job,
                    new_state,
                    verdict={
                        "reviewers": [{"model": m, "verdict": "request-changes"} for m in outcome["reviewers"]],
                        "head_sha": outcome["head_sha"] or head,
                    },
                )
                broadcast_doctor_message(
                    "doctor.review_request_changes",
                    {"host": AGENT_HOST, "job_id": jid, "reviewers": outcome["reviewers"], "review_job_id": in_flight, "head_sha": outcome["head_sha"] or head},
                )
                return "request_changes", new_state, outcome
            failure = outcome.get("failure") or "review_no_reviewers"
            tried = list(state.get("tried") or [])
            for model in outcome.get("reviewers") or []:
                entry = next((r for r in (roster or review_queue.load_roster()) if _same_model(r.get("model_id"), model)), None)
                tried.append(_tried_entry(entry or {"model_id": model}, failure, now))
            return _schedule_retry(job, state, tried, failure, now=now, review_job_id=in_flight)
        # The child is gone from the listing: fall through to a new attempt.
    if review_queue.in_backoff(state, now=now):
        return "deferred", state, None
    if not review_queue.attempts_remain(state):
        return _park_exhausted(job, state, now)
    author = state.get("author") or _author_model(job, result)
    models = result.get("models") if isinstance(result.get("models"), dict) else {}
    co_authors = [*(models.get("reviewers") if isinstance(models.get("reviewers"), list) else []), *state.get("co_authors", [])]
    cohort = _excluded_families(author, *co_authors)
    roster = list(roster if roster is not None else review_queue.load_roster())
    health = health or _review_route_health()
    load = load or review_queue_load_reader()
    picked = [
        route
        for route in pick_reviewers(
            author,
            roster,
            health=health,
            load=load,
            co_authors=[m for m in co_authors if str(m or "").strip()],
            repo=job.get("repo"),
            author_routes=[str(job.get("claimed_agent") or "")] if job.get("claimed_agent") else None,
        )
    ]
    picked = [route for route in _sort_routes_by_family_load(picked, load) if route["family"] not in cohort]
    if not picked:
        new_state = {
            **_resume_state(state),
            "parked_reason": "no_cross_family_reviewer",
            "next_attempt_at": now + review_queue.backoff_seconds(int(state.get("attempt") or 0)),
            "author": author,
            "author_family": review_queue.family_of(author),
            "updated_at": now,
        }
        _patch_review_state(job, new_state)
        broadcast_doctor_message(
            "doctor.review_queue_parked",
            {"host": AGENT_HOST, "job_id": jid, "author": author, "author_family": new_state["author_family"], "reason": "no_cross_family_reviewer"},
        )
        return "parked", new_state, None
    try:
        review_id, spec = _review_queue_submit(job, {**state, "author": author}, picked, now=now)
    except RuntimeError as exc:
        broadcast_doctor_message("doctor.sweep_error", {"sweep": "review-queue", "job_id": jid, "error": str(exc)})
        return _schedule_retry(job, state, list(state.get("tried") or []), "review_submit_failed", now=now, review_job_id="")
    tried = list(state.get("tried") or []) + [_tried_entry(picked[0], "review_submitted", now)]
    new_state = {
        **_resume_state(state),
        "author": author,
        "author_family": review_queue.family_of(author),
        "tried": tried,
        "attempt": int(state.get("attempt") or 0) + 1,
        "review_job_id": review_id,
        "review_kind": spec["kind"],
        "next_attempt_at": None,
        "last_failure": None,
        "updated_at": now,
    }
    _patch_review_state(job, new_state)
    broadcast_doctor_message(
        "doctor.review_queue",
        {"host": AGENT_HOST, "job_id": jid, "review_job_id": review_id, "reviewers": [picked[0]["model_id"]], "author_family": new_state["author_family"]},
    )
    return "submitted", new_state, None


def _sort_routes_by_family_load(routes: list[dict], load) -> list[dict]:
    """Keep the roster's deterministic order; it already sorts slow routes last."""

    def family_load(route):
        try:
            value = load(route)
        except Exception:  # noqa: BLE001
            return 1e9
        return value if isinstance(value, (int, float)) else 1e9

    return sorted(routes, key=lambda r: family_load(r))


def _schedule_retry(
    job: dict, state: dict, tried: list[dict], failure: str, *, now: float, review_job_id: str
) -> tuple[str, dict, None]:
    attempt = int(state.get("attempt") or 0)
    if attempt >= int(state.get("max_attempts") or review_queue.MAX_REVIEW_ATTEMPTS):
        return _park_exhausted(job, {**state, "tried": tried, "last_failure": failure}, now)
    new_state = {
        **_resume_state(state),
        "tried": tried,
        "last_failure": failure,
        "review_job_id": None if review_job_id else state.get("review_job_id"),
        "claimed_failure": failure,
        "next_attempt_at": now + review_queue.backoff_seconds(attempt),
        "updated_at": now,
    }
    _patch_review_state(job, new_state)
    broadcast_doctor_message(
        "doctor.review_retry",
        {"host": AGENT_HOST, "job_id": job.get("id"), "failure": failure, "attempt": attempt, "next_attempt_at": new_state["next_attempt_at"]},
    )
    return "review_failed", new_state, None


def _park_exhausted(job: dict, state: dict, now: float) -> tuple[str, dict, None]:
    """Attempts spent: stay needs-review, with the owner-wait reason visible."""
    new_state = {
        **_resume_state(state),
        "parked_reason": "attempts_exhausted_owner_wait",
        "next_attempt_at": None,
        "updated_at": now,
    }
    _patch_review_state(job, new_state)
    broadcast_doctor_message(
        "doctor.review_queue_exhausted",
        {"host": AGENT_HOST, "job_id": job.get("id"), "attempts": state.get("attempt"), "last_failure": state.get("last_failure")},
    )
    return "exhausted", new_state, None


def run_review_queue_sweep(*, rows: list[dict] | None = None, now: float | None = None, **kwargs) -> dict:
    """The review-queue half of the needs-review sweep (one pass).

    Rows come from the SAME needs-review listing the triage report reads --
    there is no separate queue. Outcomes are counted per row so one bad row
    cannot stop the sweep; a listing failure is raised, never read as empty.
    """
    now = time.time() if now is None else float(now)
    if rows is None:
        rows = fetch_needs_review_jobs(limit=1000)
    rows = sorted(
        _review_queue_rows(rows),
        key=lambda j: (review_queue.review_state(j) or {}).get("next_attempt_at") or 0,
    )
    summary = {
        "scanned": len(rows),
        "submitted": 0,
        "awaited": 0,
        "deferred": 0,
        "parked": 0,
        "exhausted": 0,
        "approved": 0,
        "request_changes": 0,
        "review_failed": 0,
    }
    for job in rows:
        try:
            outcome, _, _ = process_awaiting_review_row(job, now=now, **kwargs)
        except Exception as exc:  # noqa: BLE001 -- one row must not stop the sweep
            summary["errors"] = summary.get("errors", 0) + 1
            log.error("review queue failed job=%s: %s", str(job.get("id"))[:12], exc)
            broadcast_doctor_message(
                "doctor.sweep_error", {"sweep": "review-queue", "job_id": job.get("id"), "error": str(exc)}
            )
            continue
        summary[outcome] = summary.get(outcome, 0) + 1
    return summary


def review_queue_triage_row(job: dict, now: float | None = None) -> dict | None:
    """The triage view of an awaiting_review row (reason, next_attempt_at, states)."""
    state = review_queue.review_state(job)
    if state is None:
        return None
    now = time.time() if now is None else float(now)
    reason = review_queue.awaiting_review_reason(job)
    if state.get("approved"):
        action = f"approved by {', '.join(state['approved'].get('reviewers') or [])}; hand to the merge path"
    elif state.get("request_changes"):
        action = f"changes requested by {', '.join(state['request_changes'].get('reviewers') or [])}; back to the author path"
    elif review_queue.is_parked(state):
        action = f"awaiting cross-family review: parked, no cross-family reviewer is available ({reason})"
    elif state.get("parked_reason") == "attempts_exhausted_owner_wait":
        action = f"awaiting cross-family review: attempts spent, waiting for an owner ({reason})"
    elif review_queue.in_backoff(state, now=now):
        action = f"awaiting cross-family review: next attempt scheduled ({reason})"
    elif state.get("review_job_id"):
        action = f"awaiting cross-family review: review job {state['review_job_id'][:12]} in flight"
    else:
        action = f"awaiting cross-family review ({reason})"
    return {
        "review_state": review_queue.AWAITING_REVIEW,
        "review_reason": reason,
        "next_attempt_at": state.get("next_attempt_at"),
        "review_attempt": int(state.get("attempt") or 0),
        "review_parked": review_queue.is_parked(state),
        "review_exhausted": state.get("parked_reason") == "attempts_exhausted_owner_wait",
        "review_in_backoff": review_queue.in_backoff(state, now=now),
        "suggested_action": action,
        # Attempts remain (backoff not elapsed, a review in flight, or a parked
        # pick to retry): NOT stale, never escalated while the queue owns it.
        "stale": False,
    }


def _gate_review_queue_stuck(row: dict, job: dict, now: float | None) -> None:
    """A triage row must not suggest an action for a row the queue still owns."""
    view = review_queue_triage_row(job, now=now)
    if view is None:
        return
    row.update({k: v for k, v in view.items() if k != "suggested_action"})
    row["suggested_action"] = view["suggested_action"]
