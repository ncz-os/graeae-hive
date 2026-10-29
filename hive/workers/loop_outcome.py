"""Classify why a zoder loop job failed, and decide what the doctor does about it.

A failed loop row used to look the same whatever went wrong, so the doctor either
requeued it blindly or spawned an LLM triage job per cluster, and the triage queue
filled with rows that only needed the reviewer to come back. The loop now reports
its teardown reason (``ZODER_LOOP_RESULT`` stored by the worker as
``result.loop_result``, and ``projects/zoder-loop-events`` memories in MNEMOS);
this module turns that into a class and a policy. Everything here is fail-soft:
classification never raises and an unknown outcome keeps today's behaviour.
"""

from __future__ import annotations

import json
import os
import urllib.request
from collections.abc import Callable

REVIEWER_UNAVAILABLE = "reviewer_unavailable"
AUTHOR_FAILED = "author_failed"
TIMEOUT = "timeout"
CRASH = "crash"
EXHAUSTED = "exhausted_request_changes"
NOTHING_TO_COMMIT = "nothing_to_commit"
APPROVED_UNMERGED = "approved_unmerged"
KILLED_BY_SIGNAL = "killed_by_signal"
CLONE_FAILED = "clone_failed"
EMPTY_DIFF = "empty_diff"
UNKNOWN = "unknown"

# Cap on automatic requeues of one job for the classes that are not the job's fault.
MAX_AUTO_REQUEUES = 2

# Why an EMPTY_DIFF row is parked: the doctor puts this text on the parked-for-review message.
EMPTY_DIFF_REASON = "author made no changes; the branch may already satisfy the task"

# What zoder's loop says when the author left nothing to review. This is not a
# reviewer outage, but the loop used to tear down as ``reviewer_unavailable``.
_EMPTY_DIFF_MARKERS = ("no review chunks were generated", "review incomplete: no review chunks")

_TEARDOWN_TO_CLASS = {
    "approved": APPROVED_UNMERGED,
    "reviewer_unavailable": REVIEWER_UNAVAILABLE,
    "author_failed": AUTHOR_FAILED,
    "timeout": TIMEOUT,
    "crash": CRASH,
    "exhausted": EXHAUSTED,
    "nothing_to_commit": NOTHING_TO_COMMIT,
}


def _seq(event: dict) -> int:
    """A sortable event sequence number, whatever the store put there.

    MNEMOS metadata is free-form and has been seen with ``seq`` as a string. A
    mixed str/int list makes ``sorted`` raise ``TypeError``, and because the whole
    fetcher is best-effort that used to return None — i.e. ONE mistyped event
    discarded the entire trail and the row classified as ``unknown`` instead of
    ``crash``/``timeout``. A value that cannot be an int sorts first rather than
    taking the trail with it.
    """
    try:
        return int(event.get("_seq") or 0)
    except (TypeError, ValueError):
        return 0


def _result_dict(job: dict) -> dict:
    res = job.get("result")
    if isinstance(res, str):
        try:
            res = json.loads(res)
        except ValueError:
            return {"raw": res}
    return res if isinstance(res, dict) else {}


# Exit codes of a worker stopped by SIGTERM / SIGKILL: the shell convention
# (128 + signal) and the negative form subprocess reports.
_SIGNAL_EXIT_CODES = frozenset({143, 137, -15, -9})
_SIGNAL_TEXT = ("killed by signal", "sigterm", "sigkill")
_LOCAL_ONLY_STATUS = "committed_local_stopped"


def _finalizer(res: dict) -> dict:
    fin = res.get("finalizer")
    return fin if isinstance(fin, dict) else {}


def _local_only(res: dict) -> bool:
    fin = _finalizer(res)
    return fin.get("status") == _LOCAL_ONLY_STATUS and fin.get("pushed") is not True


def has_local_only_commits(job: dict) -> bool:
    """True when the finalizer says commits exist only in the preserved workspace.

    That is ``committed_local_stopped`` with ``pushed`` not true (a missing flag is
    treated as not pushed: the safe reading). Never raises.
    """
    try:
        return _local_only(_result_dict(job))
    except Exception:  # noqa: BLE001
        return False


def _field_texts(job: dict, res: dict, keys: tuple[str, ...]) -> list[str]:
    """Lower-cased, stripped string values of ``keys`` from the result and the row."""
    out = []
    for src in (res, job):
        for k in keys:
            v = src.get(k)
            if isinstance(v, str) and v.strip():
                out.append(v.strip().lower())
    return out


def _structured_class(job: dict, res: dict) -> str | None:
    """KILLED_BY_SIGNAL / CLONE_FAILED from the structured result fields, else None."""
    code = res.get("exit_code")
    if isinstance(code, int) and not isinstance(code, bool) and code in _SIGNAL_EXIT_CODES:
        return KILLED_BY_SIGNAL
    if _local_only(res):
        return KILLED_BY_SIGNAL  # stopped with work only in the workspace: never requeue blind
    if any(t.startswith(CLONE_FAILED) for t in _field_texts(job, res, ("failure_reason", "error"))):
        return CLONE_FAILED
    if any(sig in t for t in _field_texts(job, res, ("failure_reason",)) for sig in _SIGNAL_TEXT):
        return KILLED_BY_SIGNAL
    return None


def _says_empty_diff(*values: object) -> bool:
    """True when any string among ``values`` carries an empty-diff marker."""
    return any(isinstance(v, str) and any(m in v.lower() for m in _EMPTY_DIFF_MARKERS) for v in values)


def _teardown_says_empty_diff(teardown: object) -> bool:
    if isinstance(teardown, dict):
        return _says_empty_diff(*teardown.values())
    return _says_empty_diff(teardown)


def _events_say_empty_diff(events: list[dict] | None) -> bool:
    """A ``review_response`` event whose error/summary/tail names the empty diff."""
    for ev in events or []:
        if (
            isinstance(ev, dict)
            and ev.get("phase") == "review_response"
            and _says_empty_diff(ev.get("error"), ev.get("summary"), ev.get("tail"))
        ):
            return True
    return False


def _iteration_is_empty(it: object) -> bool:
    """One loop iteration in which the author changed nothing.

    Only fields the loop reports: ``check_passed`` must be explicitly false, and
    the iteration must say no file/diff content exists (``files_changed`` 0 or an
    empty list, ``diff_lines`` 0, or ``substance`` "empty"). A missing field proves
    nothing, so an iteration with no such signal is not empty.
    """
    if not isinstance(it, dict) or it.get("check_passed") is not False:
        return False
    signals = []
    fc = it.get("files_changed")
    if isinstance(fc, list):
        signals.append(len(fc) == 0)
    elif isinstance(fc, int) and not isinstance(fc, bool):
        signals.append(fc == 0)
    dl = it.get("diff_lines")
    if isinstance(dl, int) and not isinstance(dl, bool):
        signals.append(dl == 0)
    if isinstance(it.get("substance"), str):
        signals.append(it["substance"].strip().lower() == "empty")
    return bool(signals) and all(signals)


def _empty_iterations_no_commits(res: dict, loop_result: dict) -> bool:
    """Every iteration changed nothing and the row recorded no commits."""
    its = loop_result.get("iterations") or loop_result.get("log")
    if not isinstance(its, list) or not its or not all(_iteration_is_empty(i) for i in its):
        return False
    if res.get("commits") or loop_result.get("commits"):
        return False
    if _finalizer(res).get("committed") is True:
        return False
    # A spend cap also leaves empty iterations + reviewer_unavailable; that is a real outage.
    return "budget exceeded" not in json.dumps(res, default=str).lower()


def fetch_loop_events(job_id: str, timeout: float = 4.0) -> list[dict] | None:
    """The job's ``zoder-loop-events`` from MNEMOS, oldest first; None if unavailable."""
    base, token = os.environ.get("MNEMOS_URL"), os.environ.get("MNEMOS_TOKEN")
    if not base or not token or not job_id:
        return None
    try:
        req = urllib.request.Request(
            base.rstrip("/") + "/v1/memories/search",
            data=json.dumps({"query": job_id, "subcategory": "zoder-loop-events", "limit": 50}).encode(),
            method="POST",
            headers={"Content-Type": "application/json", "Authorization": "Bearer " + token},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - operator-configured URL
            body = json.load(resp)
        if isinstance(body, dict):
            rows = body.get("memories") or body.get("results") or []
        elif isinstance(body, list):
            rows = body
        else:
            rows = []
        events = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            meta = row.get("metadata") or {}
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except ValueError:
                    meta = {}
            if meta.get("job_id") != job_id:
                continue
            try:
                payload = json.loads(row.get("content") or "{}")
            except ValueError:
                continue
            if isinstance(payload, dict):
                payload.setdefault("phase", meta.get("phase"))
                payload["_seq"] = meta.get("seq", 0)
                events.append(payload)
        return sorted(events, key=_seq)
    except Exception:  # noqa: BLE001 - diagnosis must never break the sweep
        return None


def classify(job: dict, events_fetcher: Callable[[str], list[dict] | None] = fetch_loop_events) -> str:
    """One class for a failed loop row. Never raises."""
    try:
        res = _result_dict(job)
        loop_result = res.get("loop_result")
        loop_result = loop_result if isinstance(loop_result, dict) else {}
        teardown = loop_result.get("teardown")
        # Empty diff first: the loop reports it as reviewer_unavailable, and every
        # rule below would then wait for a reviewer that is not the problem.
        if _teardown_says_empty_diff(teardown):
            return EMPTY_DIFF
        tear_reason = teardown.get("reason") if isinstance(teardown, dict) else None
        if tear_reason == "reviewer_unavailable":
            if _empty_iterations_no_commits(res, loop_result):
                return EMPTY_DIFF
            if _events_say_empty_diff(events_fetcher(str(job.get("id") or ""))):
                return EMPTY_DIFF
        if tear_reason in _TEARDOWN_TO_CLASS:
            return _TEARDOWN_TO_CLASS[tear_reason]
        structured = _structured_class(job, res)
        if structured:
            return structured
        events = events_fetcher(str(job.get("id") or ""))
        if _events_say_empty_diff(events):
            return EMPTY_DIFF
        if events:
            for ev in reversed(events):
                if ev.get("phase") == "teardown" and ev.get("reason") in _TEARDOWN_TO_CLASS:
                    return _TEARDOWN_TO_CLASS[ev["reason"]]
            phases = [e.get("phase") for e in events]
            if "teardown" not in phases:
                return CRASH  # a trail with no teardown: the loop died mid-way
        text = " ".join(str(res.get(k) or "") for k in ("error", "failure_reason", "tail", "raw")).lower()
        if _says_empty_diff(text):
            return EMPTY_DIFF
        if "reviewer_unavailable" in text:
            return REVIEWER_UNAVAILABLE
        if "nothing_to_commit" in text:
            return NOTHING_TO_COMMIT
        reasons = " ".join(_field_texts(job, res, ("error", "failure_reason")))
        if any(sig in reasons for sig in _SIGNAL_TEXT):
            return KILLED_BY_SIGNAL
        if "timed out" in text or "timeout" in text:
            return TIMEOUT
    except Exception:  # noqa: BLE001
        pass
    return UNKNOWN


def reviewer_healthy(url: str | None = None, timeout: float = 3.0) -> bool:
    """Bounded probe of the production reviewer.

    The URL comes from ``DOCTOR_REVIEWER_HEALTH_URL`` (no baked-in address). With
    no URL configured there is nothing to probe, so the reviewer is assumed
    healthy and the requeue cap alone bounds retries. Unreachable = unhealthy.
    """
    url = url or os.environ.get("DOCTOR_REVIEWER_HEALTH_URL", "").strip()
    if not url:
        return True
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310 - operator-configured URL
            return 200 <= resp.status < 300
    except Exception:  # noqa: BLE001
        return False


def decide(
    cls: str,
    requeues_so_far: int,
    reviewer_ok: Callable[[], bool] = reviewer_healthy,
    local_commits_only: bool = False,
) -> str:
    """What the sweep does with a failed row of this class.

    ``requeue``      resubmit it (existing resubmit path)
    ``hold``         do nothing this pass (cause is outside the job; retry later)
    ``needs_review`` stop automation, a human or the review lane looks once
    ``skip``         not the auto-requeue path (approved work waits for a merge)
    ``default``      unknown outcome: today's behaviour

    ``local_commits_only`` (see ``has_local_only_commits``): the work exists only in
    the preserved workspace, so a killed row is parked for a rescue and never requeued.
    """
    if cls == KILLED_BY_SIGNAL:
        # Not the job's fault, but a requeue would start from a fresh clone and
        # strand commits that were never pushed.
        if local_commits_only or requeues_so_far >= MAX_AUTO_REQUEUES:
            return "needs_review"
        return "requeue"
    if cls == CLONE_FAILED:  # transient failure to reach the git remote
        return "needs_review" if requeues_so_far >= MAX_AUTO_REQUEUES else "requeue"
    if cls == REVIEWER_UNAVAILABLE:
        if requeues_so_far >= MAX_AUTO_REQUEUES:
            return "needs_review"
        return "requeue" if reviewer_ok() else "hold"
    if cls in (AUTHOR_FAILED, TIMEOUT, CRASH):
        return "needs_review" if requeues_so_far >= MAX_AUTO_REQUEUES else "requeue"
    if cls in (EXHAUSTED, NOTHING_TO_COMMIT, EMPTY_DIFF):  # EMPTY_DIFF: a retry sees the same diff
        return "needs_review"
    if cls == APPROVED_UNMERGED:
        return "skip"
    return "default"


def cluster_is_infrastructure(classes: list[str]) -> bool:
    """True when every failure in a cluster is a class the doctor handles
    deterministically, so no LLM triage job should be created for it."""
    return bool(classes) and all(c != UNKNOWN for c in classes)
