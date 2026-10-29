"""One terminal PATCH per (job id, status) per worker process.

F2 idempotency audit (2026-10-29). The bus's auto-retry branch fires on
``req.status == "failed"`` — not only on a transition — so a second ``failed``
PATCH increments ``retry_count`` and requeues the job. ``run_job``'s
always-terminal except path patches ``failed`` after ``_run_job_inner`` may
already have failed the same row, which spends a retry the job never used.

Renewals are deliberately not deduped: repeating ``claimed``/``running`` is how a
claim lease is kept alive.
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import types

import pytest


def _stub_hive_budget() -> None:
    """`hive_budget` is a stdlib-only sibling that ships with the deployment, not
    this repo (tests/test_worker_build_busy.py stubs it the same way). The worker
    reads only ``FAILURE_REASON`` at import; the rest are runtime helpers these
    tests never reach, so a minimal module is enough to import patch_job.
    """
    if "hive_budget" in sys.modules:
        return
    budget = types.ModuleType("hive_budget")
    budget.FAILURE_REASON = "budget_exceeded"
    for name in ("from_text", "message_line", "iso_utc", "summary", "reset_at"):
        setattr(budget, name, lambda *a, **k: None)
    sys.modules["hive_budget"] = budget


def _load_worker():
    root = pathlib.Path(__file__).parent
    # deploy/ holds the worker's sibling modules (`provenance`, `retry_policy`, …)
    # and the repo root holds `hive`; without both, exec_module raises and the whole
    # module skips — which silently turns these regression tests into no-ops.
    for entry in (str(root), str(root / "deploy")):
        if entry not in sys.path:
            sys.path.insert(0, entry)
    _stub_hive_budget()
    path = root / "deploy" / "hive_worker.py"
    spec = importlib.util.spec_from_file_location("hive_worker_patch_under_test", path)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except BaseException as exc:  # noqa: BLE001 - the worker needs env/host bits on some hosts
        pytest.skip(f"hive_worker not importable here: {exc}")
    return module


@pytest.fixture
def worker(monkeypatch):
    module = _load_worker()
    module._terminal_patched.clear()
    calls: list[tuple] = []

    def _http(method, path, body=None, **_kw):
        calls.append((method, path, body))
        return 200, {}

    monkeypatch.setattr(module, "_http", _http)
    monkeypatch.setattr(module, "_claim_provenance", lambda job_id: {})
    return module, calls


def _patched_statuses(calls) -> list[str]:
    return [body["status"] for _m, _p, body in calls if isinstance(body, dict) and "status" in body]


def test_a_second_failed_patch_is_not_sent(worker):
    module, calls = worker
    assert module.patch_job("job-1", "failed", {"error": "boom"}) is True
    assert module.patch_job("job-1", "failed", {"error": "boom"}) is True
    assert _patched_statuses(calls) == ["failed"], (
        "the second failed PATCH would make the bus auto-retry the job and spend a retry"
    )


def test_the_exception_path_after_an_inner_failure_is_a_no_op(worker):
    """The exact regression: inner run fails the job, then raises, run_job re-fails it."""
    module, calls = worker
    module.patch_job("job-2", "failed", {"failure_reason": "loop_failed"})
    # run_job's except path:
    module.patch_job("job-2", "failed", {"failure_reason": "unhandled_exception"})
    assert _patched_statuses(calls) == ["failed"]


def test_a_different_terminal_status_still_goes_out(worker):
    module, calls = worker
    module.patch_job("job-3", "failed", {"error": "x"})
    assert module.patch_job("job-3", "needs-review", {"error": "x"}) is True
    assert _patched_statuses(calls) == ["failed", "needs-review"]


def test_renewals_are_not_deduped(worker):
    module, calls = worker
    module.patch_job("job-4", "running", {})
    module.patch_job("job-4", "running", {})
    assert _patched_statuses(calls) == ["running", "running"], "a claim renewal must keep repeating"


def test_another_job_is_unaffected(worker):
    module, calls = worker
    module.patch_job("job-5", "failed", {})
    module.patch_job("job-6", "failed", {})
    assert _patched_statuses(calls) == ["failed", "failed"]


def test_a_failed_patch_is_not_remembered(monkeypatch):
    """Only a 200 counts: a rejected patch must stay retryable."""
    module = _load_worker()
    module._terminal_patched.clear()
    calls: list[tuple] = []

    def _http(method, path, body=None, **_kw):
        calls.append((method, path, body))
        return 500, {}

    _stub_hive_budget()
    monkeypatch.setattr(module, "_http", _http)
    monkeypatch.setattr(module, "_claim_provenance", lambda job_id: {})
    assert module.patch_job("job-7", "failed", {}) is False
    assert module.patch_job("job-7", "failed", {}) is False
    assert len(calls) == 2, "a failed terminal patch must be retryable"
    assert ("job-7", "failed") not in module._terminal_patched
