"""Hermetic tests for the worker build/pipeline busy gate (2026-10-31).

These tests used to spawn real processes and read the host's real ``/proc``
(and skip on any host without one), so their result depended on what else was
running on the machine. They now install a fake process table and a fake
``/proc`` reader inside the loaded worker module, point ``HOME`` at a temporary
directory before the module is imported, and pass the clock explicitly where
the gate consults it. Nothing here reads host state: a build-orchestrator or
pipeline process running on the real host cannot make any of these tests pass
or fail.
"""

import importlib.util
import io
import os
import subprocess
import sys
import time
import types
from pathlib import Path

import pytest

WORKER_SRC = Path(__file__).resolve().parent.parent / "deploy" / "hive_worker.py"


@pytest.fixture
def worker(monkeypatch, tmp_path):
    # The module resolves ~ at import time (BUSY_DIR, DRAIN_FILE, zoder paths),
    # so HOME must point at an empty directory before it is loaded.
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.delenv("HIVE_WORKER_BUSY_DIR", raising=False)

    # These deployed sidecars are absent from this checkout. Delivery itself
    # uses real Git below; only unrelated import-time dependencies are stubbed.
    budget = types.ModuleType("hive_budget")
    budget.FAILURE_REASON = "budget_exceeded"
    provenance = types.ModuleType("provenance")
    provenance.STATIC_AGENT_MODELS = {}
    provenance.KNOWN_COST_TIERS = set()
    monkeypatch.setitem(sys.modules, "hive_budget", budget)
    monkeypatch.setitem(sys.modules, "provenance", provenance)
    for name in ("codex_delivery", "llm_roles", "push_policy", "review_evidence"):
        module = types.ModuleType("hive." + name)
        if name == "codex_delivery":
            module.fleet_git_env = lambda *a, **kw: {}
        if name == "push_policy":
            module.argonas_url_for_slug = lambda slug: slug
        monkeypatch.setitem(sys.modules, "hive." + name, module)
    spec = importlib.util.spec_from_file_location("terra_worker", WORKER_SRC)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeProcTable:
    """A pid -> (argv, uid) table standing in for the host's process list."""

    def __init__(self):
        self.procs: dict[int, tuple[list[str], int]] = {}

    def add(self, pid: int, argv: list[str], uid: int = 0) -> int:
        self.procs[pid] = (argv, uid)
        return pid


def install_fake_proc(monkeypatch, worker, table: FakeProcTable) -> None:
    """Point the module's process scan at ``table`` instead of the host.

    ``build_busy`` walks ``os.listdir('/proc')``, opens ``/proc/<pid>/cmdline``
    and stats ``/proc/<pid>``. Those three names are rebound on the loaded
    module only, so the real host processes are never consulted.
    """
    real_os = os
    procs = table.procs

    def _pid_of(path) -> int | None:
        if not isinstance(path, str):
            return None
        parts = path.split("/")
        if len(parts) >= 3 and parts[1] == "proc" and parts[2].isdigit():
            return int(parts[2])
        return None

    class ProcOS:
        def listdir(self, path):
            if path == "/proc":
                return [str(pid) for pid in procs]
            return real_os.listdir(path)

        def stat(self, path):
            pid = _pid_of(path)
            if pid is not None and pid in procs:
                return types.SimpleNamespace(st_uid=procs[pid][1])
            return real_os.stat(path)

        def __getattr__(self, name):
            return getattr(real_os, name)

    real_open = open

    def fake_open(path, *args, **kwargs):
        pid = _pid_of(path)
        if pid is not None and str(path).endswith("/cmdline") and pid in procs:
            argv = procs[pid][0]
            return io.BytesIO(b"\0".join(a.encode() for a in argv) + b"\0")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(worker, "os", ProcOS())
    monkeypatch.setattr(worker, "open", fake_open, raising=False)


@pytest.fixture
def proc_table(monkeypatch, worker):
    table = FakeProcTable()
    install_fake_proc(monkeypatch, worker, table)
    return table


def test_idle_host_is_not_busy(worker, tmp_path, monkeypatch, proc_table):
    monkeypatch.setattr(worker, "BUSY_DIR", str(tmp_path / "none"))
    monkeypatch.setattr(worker, "BUSY_PROCS", ())
    monkeypatch.setattr(worker, "PIPELINE_PROCS", ())
    monkeypatch.setattr(worker, "_pipeline_uids", lambda: {})
    assert worker.build_busy() == ""


def test_marker_with_live_pid_is_busy_and_dead_pid_is_stale(worker, tmp_path, monkeypatch, proc_table):
    monkeypatch.setattr(worker, "BUSY_DIR", str(tmp_path))
    monkeypatch.setattr(worker, "BUSY_PROCS", ())
    monkeypatch.setattr(worker, "PIPELINE_PROCS", ())
    monkeypatch.setattr(worker, "_pipeline_uids", lambda: {})
    marker = tmp_path / "image-build"
    marker.write_text(str(os.getpid()) + "\n")
    assert "image-build" in worker.build_busy()
    done = subprocess.Popen([sys.executable, "-c", "pass"])
    done.wait()
    marker.write_text(str(done.pid) + "\n")
    assert worker.build_busy() == ""


def test_pidless_marker_expires_after_ttl(worker, tmp_path, monkeypatch, proc_table):
    monkeypatch.setattr(worker, "BUSY_DIR", str(tmp_path))
    monkeypatch.setattr(worker, "BUSY_PROCS", ())
    monkeypatch.setattr(worker, "PIPELINE_PROCS", ())
    monkeypatch.setattr(worker, "_pipeline_uids", lambda: {})
    marker = tmp_path / "manual"
    marker.write_text("rebuilding kernel\n")
    assert "manual" in worker.build_busy()
    assert worker.build_busy(now=time.time() + worker.BUSY_MARKER_TTL + 5) == ""


def test_running_build_orchestrator_makes_the_host_busy(worker, tmp_path, monkeypatch, proc_table):
    """argv[1] of an interpreter-run script is a build orchestrator name."""
    monkeypatch.setattr(worker, "BUSY_DIR", str(tmp_path / "none"))
    monkeypatch.setattr(worker, "BUSY_PROCS", ("hive-busy-test-build.sh",))
    monkeypatch.setattr(worker, "PIPELINE_PROCS", ())
    monkeypatch.setattr(worker, "_pipeline_uids", lambda: {})
    assert worker.build_busy() == ""

    proc_table.add(4242, ["sh", "hive-busy-test-build.sh"])
    reason = worker.build_busy()
    assert reason.startswith("process hive-busy-test-build.sh (pid 4242)")


def test_named_pipeline_process_makes_the_host_busy_as_a_pipeline(worker, tmp_path, monkeypatch, proc_table):
    monkeypatch.setattr(worker, "BUSY_DIR", str(tmp_path / "none"))
    monkeypatch.setattr(worker, "BUSY_PROCS", ())
    monkeypatch.setattr(worker, "PIPELINE_PROCS", ("hive-pipeline-test-stage.sh",))
    monkeypatch.setattr(worker, "_pipeline_uids", lambda: {})
    assert worker.build_busy() == ""

    proc_table.add(4343, ["sh", "hive-pipeline-test-stage.sh"])

    reason = worker.build_busy()
    assert reason.startswith("pipeline ") and "hive-pipeline-test-stage.sh" in reason


def test_ci_user_process_is_a_pipeline_job_but_the_runner_daemon_is_not(worker, tmp_path, monkeypatch, proc_table):
    monkeypatch.setattr(worker, "BUSY_DIR", str(tmp_path / "none"))
    monkeypatch.setattr(worker, "BUSY_PROCS", ())
    monkeypatch.setattr(worker, "PIPELINE_PROCS", ())
    monkeypatch.setattr(worker, "_pipeline_uids", lambda: {1234: "ci-user"})
    proc_table.add(4444, ["gitlab-runner", "run"], uid=1234)
    assert "gitlab-runner" not in worker.build_busy()

    proc_table.add(4445, ["python", "job.py"], uid=1234)

    reason = worker.build_busy()
    assert reason.startswith("pipeline CI job as ci-user")


def test_python_dash_m_module_counts_through_argv_two(worker, tmp_path, monkeypatch, proc_table):
    monkeypatch.setattr(worker, "BUSY_DIR", str(tmp_path / "none"))
    monkeypatch.setattr(worker, "BUSY_PROCS", ())
    monkeypatch.setattr(worker, "PIPELINE_PROCS", ("pipeline.py",))
    monkeypatch.setattr(worker, "_pipeline_uids", lambda: {})
    assert worker.build_busy() == ""

    proc_table.add(4545, ["python", "-m", "pipeline.py"])

    assert worker.build_busy().startswith("pipeline pipeline.py (pid 4545)")


def test_real_host_processes_are_ignored(worker, tmp_path, monkeypatch, proc_table):
    """A real process named like a pipeline stage cannot influence the gate.

    The fake table is empty, so a scan that leaked to the host would find the
    process this test starts and report busy.
    """
    monkeypatch.setattr(worker, "BUSY_DIR", str(tmp_path / "none"))
    monkeypatch.setattr(worker, "BUSY_PROCS", ("hive-pipeline-test-stage.sh",))
    monkeypatch.setattr(worker, "PIPELINE_PROCS", ("hive-pipeline-test-stage.sh",))
    monkeypatch.setattr(worker, "_pipeline_uids", lambda: {})

    script = tmp_path / "hive-pipeline-test-stage.sh"
    script.write_text("sleep 30\n")
    proc = subprocess.Popen(["sh", str(script)])
    try:
        time.sleep(0.3)
        assert worker.build_busy() == ""
    finally:
        proc.kill()
        proc.wait()
