"""Exercise the deployed worker's delivery gate with real local Git state."""
import importlib.util
import os
import subprocess
import sys
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
    spec = importlib.util.spec_from_file_location('terra_worker', Path('deploy/hive_worker.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_worker_delivery_accepts_untracked_but_rejects_tracked_changes(worker, tmp_path, capsys):
    repo = tmp_path / 'work'
    remote = tmp_path / 'remote.git'
    repo.mkdir()
    def git(*args):
        return subprocess.check_output(['git', '-C', str(repo), *args], stderr=subprocess.STDOUT, text=True).strip()
    subprocess.check_call(['git', 'init', '--bare', str(remote)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    git('init')
    git('config', 'user.name', 'Test')
    git('config', 'user.email', 'test@example.invalid')
    (repo / 'tracked').write_text('base')
    git('add', 'tracked')
    git('commit', '-m', 'base')
    base = git('rev-parse', 'HEAD')
    git('checkout', '-b', 'task')
    (repo / 'tracked').write_text('delivered')
    git('commit', '-am', 'deliver')
    git('remote', 'add', 'origin', str(remote))
    git('push', '-u', 'origin', 'task')
    (repo / 'scratch').write_text('leftover')
    proof = worker.completion_evidence(str(repo), base, os.environ.copy())
    assert worker.completion_ready(proof)
    assert proof['untracked_files'] == ['scratch']
    assert proof['pushed'] and proof['remote_sha'] == proof['head_sha']
    assert worker.verify_delivery(str(repo), base) == 0
    assert '"delivery_ready": true' in capsys.readouterr().out
    (repo / 'tracked').write_text('uncommitted')
    assert not worker.completion_ready(worker.completion_evidence(str(repo), base, os.environ.copy()))
    assert worker.verify_delivery(str(repo), base) == 1
    git('checkout', '--', 'tracked')
    (repo / 'tracked').write_text('local only')
    git('commit', '-am', 'local only')
    assert worker.verify_delivery(str(repo), base) == 1


def test_worker_preflight_stops_before_driver_when_daemon_socket_is_stale(worker, tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setattr(worker, 'WORKER_KIND', 'zeroclaw')
    reports = []
    monkeypatch.setattr(worker, 'patch_job', lambda *args: reports.append(args))
    monkeypatch.setattr(worker, '_find_driver', lambda name: pytest.fail('driver started'))
    class Socket:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def settimeout(self, timeout): pass
        def connect(self, path): raise ConnectionRefusedError(111, 'Connection refused')
    monkeypatch.setattr(worker.socket, 'socket', lambda *args: Socket())
    worker.run_job({'id': '01234567-89ab-cdef-0123-456789abcdef'})
    assert reports[0][1] == 'failed'
    assert reports[0][2]['failure_reason'] == 'daemon_lost'
    assert reports[0][2]['review_iterations'] == 0


def test_worker_preflight_lets_a_cold_daemon_through(worker, tmp_path, monkeypatch):
    """No socket file = idle host whose zoder cold-starts the daemon. It must
    not fail the job as daemon_lost (the 2026-10-28 queue churn)."""
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setattr(worker, 'WORKER_KIND', 'zeroclaw')
    failures, releases = [], []
    monkeypatch.setattr(worker, 'patch_job', lambda *args: failures.append(args))
    monkeypatch.setattr(worker, 'resolve_repo_url', lambda job: None)
    monkeypatch.setattr(worker, 'release_job', lambda *args: releases.append(args))
    class Socket:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def settimeout(self, timeout): pass
        def connect(self, path): raise FileNotFoundError(2, 'No such file or directory')
    monkeypatch.setattr(worker.socket, 'socket', lambda *args: Socket())
    worker.run_job({'id': '01234567-89ab-cdef-0123-456789abcdef'})
    assert failures == []
    assert releases[0][1] == 'no_repo_mapping'


def test_worker_preflight_accepts_connectable_socket(worker, tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setattr(worker, 'WORKER_KIND', 'zeroclaw')
    releases = []
    monkeypatch.setattr(worker, 'resolve_repo_url', lambda job: None)
    monkeypatch.setattr(worker, 'release_job', lambda *args: releases.append(args))
    class Socket:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def settimeout(self, timeout): pass
        def connect(self, path): pass
    monkeypatch.setattr(worker.socket, 'socket', lambda *args: Socket())
    worker.run_job({'id': '01234567-89ab-cdef-0123-456789abcdef'})
    assert releases[0][1] == 'no_repo_mapping'
