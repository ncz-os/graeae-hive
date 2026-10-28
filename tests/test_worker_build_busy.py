import importlib.util
import os
import subprocess
import sys
import time
import types
from pathlib import Path

import pytest


@pytest.fixture
def worker(monkeypatch):
    # These deployed sidecars are absent from this checkout. Delivery itself
    # uses real Git below; only unrelated import-time dependencies are stubbed.
    budget = types.ModuleType('hive_budget')
    budget.FAILURE_REASON = 'budget_exceeded'
    provenance = types.ModuleType('provenance')
    provenance.STATIC_AGENT_MODELS = {}
    provenance.KNOWN_COST_TIERS = set()
    monkeypatch.setitem(sys.modules, 'hive_budget', budget)
    monkeypatch.setitem(sys.modules, 'provenance', provenance)
    for name in ('codex_delivery', 'llm_roles', 'push_policy', 'review_evidence'):
        module = types.ModuleType('hive.' + name)
        if name == 'codex_delivery':
            module.fleet_git_env = lambda *a, **kw: {}
        if name == 'push_policy':
            module.argonas_url_for_slug = lambda slug: slug
        monkeypatch.setitem(sys.modules, 'hive.' + name, module)
    spec = importlib.util.spec_from_file_location('terra_worker', Path(__file__).resolve().parent.parent / 'deploy' / 'hive_worker.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_idle_host_is_not_busy(worker, tmp_path, monkeypatch):
    monkeypatch.setattr(worker, 'BUSY_DIR', str(tmp_path / 'none'))
    monkeypatch.setattr(worker, 'BUSY_PROCS', ())
    assert worker.build_busy() == ''


def test_marker_with_live_pid_is_busy_and_dead_pid_is_stale(worker, tmp_path, monkeypatch):
    monkeypatch.setattr(worker, 'BUSY_DIR', str(tmp_path))
    monkeypatch.setattr(worker, 'BUSY_PROCS', ())
    marker = tmp_path / 'image-build'
    marker.write_text(str(os.getpid()) + '\n')
    assert 'image-build' in worker.build_busy()
    done = subprocess.Popen([sys.executable, '-c', 'pass'])
    done.wait()
    marker.write_text(str(done.pid) + '\n')
    assert worker.build_busy() == ''


def test_pidless_marker_expires_after_ttl(worker, tmp_path, monkeypatch):
    monkeypatch.setattr(worker, 'BUSY_DIR', str(tmp_path))
    monkeypatch.setattr(worker, 'BUSY_PROCS', ())
    marker = tmp_path / 'manual'
    marker.write_text('rebuilding kernel\n')
    assert 'manual' in worker.build_busy()
    assert worker.build_busy(now=time.time() + worker.BUSY_MARKER_TTL + 5) == ''


@pytest.mark.skipif(not os.path.isdir('/proc'), reason='process scan reads /proc (the workers are Linux)')
def test_running_build_orchestrator_makes_the_host_busy(worker, tmp_path, monkeypatch):
    """The real-process path: a script named like a build orchestrator, run via
    its interpreter, is seen through argv[1]."""
    script = tmp_path / 'hive-busy-test-build.sh'
    script.write_text('sleep 30; true\n')
    monkeypatch.setattr(worker, 'BUSY_DIR', str(tmp_path / 'none'))
    monkeypatch.setattr(worker, 'BUSY_PROCS', ('hive-busy-test-build.sh',))
    assert worker.build_busy() == ''
    proc = subprocess.Popen(['sh', str(script)])
    try:
        time.sleep(0.3)
        assert 'hive-busy-test-build.sh' in worker.build_busy()
    finally:
        proc.kill()
        proc.wait()
    assert worker.build_busy() == ''


needs_proc = pytest.mark.skipif(not os.path.isdir('/proc'), reason='process scan reads /proc (the workers are Linux)')


@needs_proc
def test_named_pipeline_process_makes_the_host_busy_as_a_pipeline(worker, tmp_path, monkeypatch):
    script = tmp_path / 'hive-pipeline-test-stage.sh'
    script.write_text('sleep 30; true\n')
    monkeypatch.setattr(worker, 'BUSY_DIR', str(tmp_path / 'none'))
    monkeypatch.setattr(worker, 'BUSY_PROCS', ())
    monkeypatch.setattr(worker, 'PIPELINE_PROCS', ('hive-pipeline-test-stage.sh',))
    monkeypatch.setattr(worker, '_pipeline_uids', lambda: {})
    assert worker.build_busy() == ''
    proc = subprocess.Popen(['sh', str(script)])
    try:
        time.sleep(0.3)
        reason = worker.build_busy()
        assert reason.startswith('pipeline ') and 'hive-pipeline-test-stage.sh' in reason
    finally:
        proc.kill()
        proc.wait()


@needs_proc
def test_ci_user_process_is_a_pipeline_job_but_the_runner_daemon_is_not(worker, tmp_path, monkeypatch):
    monkeypatch.setattr(worker, 'BUSY_DIR', str(tmp_path / 'none'))
    monkeypatch.setattr(worker, 'BUSY_PROCS', ())
    monkeypatch.setattr(worker, 'PIPELINE_PROCS', ())
    monkeypatch.setattr(worker, '_pipeline_uids', lambda: {os.getuid(): 'ci-user'})
    # idle: the test process itself is owned by the CI uid but is not a job
    # child of anything we start, so scan only what we spawn by pausing the check
    # to processes that are not this pytest run.
    me = os.getpid()
    real_listdir = os.listdir
    monkeypatch.setattr(worker.os, 'listdir', lambda p: [x for x in real_listdir(p) if x != str(me)] if p == '/proc' else real_listdir(p))
    daemon_dir = tmp_path / 'd'
    daemon_dir.mkdir()
    (daemon_dir / 'run').write_text('import time\ntime.sleep(30)\n')
    fake = tmp_path / 'gitlab-runner'
    fake.symlink_to(sys.executable)
    job = tmp_path / 'job'
    job.symlink_to(sys.executable)
    (daemon_dir / 'job.py').write_text('import time\ntime.sleep(30)\n')
    daemon = subprocess.Popen([str(fake), 'run'], cwd=daemon_dir)
    try:
        time.sleep(0.5)
        # the daemon is ignored; other processes of the uid (the shell, the IDE)
        # may exist on a dev box, so assert on the reason only when it names us
        assert 'gitlab-runner' not in worker.build_busy()
    finally:
        daemon.kill()
        daemon.wait()
    jobproc = subprocess.Popen([str(job), 'job.py'], cwd=daemon_dir)
    try:
        time.sleep(0.5)
        reason = worker.build_busy()
        assert reason.startswith('pipeline CI job as ci-user')
    finally:
        jobproc.kill()
        jobproc.wait()
