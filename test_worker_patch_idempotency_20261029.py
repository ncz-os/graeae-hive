"""One terminal PATCH per (job id, status) per worker process.

F2 idempotency audit (2026-10-29). The bus's auto-retry branch fires on
``req.status == "failed"`` — not only on a transition — so a second ``failed``
PATCH increments ``retry_count`` and requeues the job. ``run_job``'s
always-terminal except path patches ``failed`` after ``_run_job_inner`` may
already have failed the same row, which spends a retry the job never used.

Renewals are deliberately not deduped: repeating ``claimed``/``running`` is how a
claim lease is kept alive.

Loading note: ``deploy/hive_worker.py`` imports sidecars that ship with the
deployment and are deliberately not tracked in this repo (``hive_budget``,
``provenance``, the ``hive.*`` delivery modules). They are stubbed here the way
``tests/test_terra_worker_delivery.py`` stubs them. Without the stubs ``exec_module``
raises and every case below becomes a silent skip — which is exactly how these
regression tests first managed to pass by not running.
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import types

import pytest

ROOT = pathlib.Path(__file__).parent


def _stub(monkeypatch, name: str, **attrs) -> None:
    """A stand-in module: real attributes where the worker reads one at import,
    a no-op callable for anything else."""
    module = types.ModuleType(name)
    module.__getattr__ = lambda _attr: lambda *a, **k: None  # noqa: ARG005
    for key, value in attrs.items():
        setattr(module, key, value)
    monkeypatch.setitem(sys.modules, name, module)


def _load_worker(monkeypatch):
    for entry in (str(ROOT), str(ROOT / "deploy")):
        if entry not in sys.path:
            sys.path.insert(0, entry)
    _stub(monkeypatch, "hive_budget", FAILURE_REASON="budget_exceeded")
    _stub(monkeypatch, "provenance", STATIC_AGENT_MODELS={}, KNOWN_COST_TIERS=set())
    _stub(monkeypatch, "hive.codex_delivery", fleet_git_env=lambda *a, **k: {})
    _stub(monkeypatch, "hive.llm_roles")
    _stub(monkeypatch, "hive.push_policy", argonas_url_for_slug=lambda slug: slug)
    _stub(monkeypatch, "hive.review_evidence")
    _stub(monkeypatch, "hive.review_record")
    spec = importlib.util.spec_from_file_location("hive_worker_patch_under_test", ROOT / "deploy" / "hive_worker.py")
    module = importlib.util.module_from_spec(spec)
    # No skip: a worker that cannot import here is a broken test, not a passing one.
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def worker(monkeypatch):
    module = _load_worker(monkeypatch)
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


def test_a_failed_patch_is_not_remembered(worker, monkeypatch):
    """Only a 200 counts: a rejected patch must stay retryable."""
    module, calls = worker

    def _http(method, path, body=None, **_kw):
        calls.append((method, path, body))
        return 500, {}

    monkeypatch.setattr(module, "_http", _http)
    assert module.patch_job("job-7", "failed", {}) is False
    assert module.patch_job("job-7", "failed", {}) is False
    assert len(calls) == 2, "a failed terminal patch must be retryable"
    assert ("job-7", "failed") not in module._terminal_patched
