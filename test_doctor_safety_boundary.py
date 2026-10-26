"""Regressions at the actual doctor/executor and persistence boundaries."""

import json
import subprocess
from unittest.mock import Mock

import pytest

from hive.workers import doctor, doctor_gates, merge_train
from hive.workers.doctor_state import BusBackend, DurableStateStore, FileBackend, MemoryBackend, state_store_from_env


@pytest.mark.parametrize("action", ["cancel_jobs", "restart_service", "dispatch_codex_fix"])
def test_model_cannot_reach_privileged_executor(monkeypatch, action):
    monkeypatch.setattr(doctor, "fetch_failed_jobs_for_kind", lambda *a, **k: [])
    monkeypatch.setattr(doctor, "fetch_agent_registry_summary", lambda: {})
    monkeypatch.setattr(doctor, "_match_known_signature", lambda *a: None)
    monkeypatch.setattr(
        doctor,
        "invoke_doctor_agent",
        lambda *a, **k: (
            True,
            json.dumps(
                {
                    "action_type": action,
                    "confidence": 1,
                    "codex_task": "restart a host",
                    "target_host": "hydra",
                    "service_name": "zeroclaw-worker.service",
                }
            ),
        ),
    )
    executor = Mock(return_value="executed")
    monkeypatch.setattr(doctor, "action_" + action, executor)
    result = doctor.process_triage_job({"id": "j", "kind": "triage:zeroclaw"})
    assert result["action"]["action_type"] == "escalate"
    executor.assert_not_called()


@pytest.mark.parametrize("confidence", [0.3, 2.0, float("nan"), "invalid"])
def test_model_release_requires_valid_confidence(monkeypatch, confidence):
    monkeypatch.setattr(doctor, "fetch_failed_jobs_for_kind", lambda *a, **k: [])
    monkeypatch.setattr(doctor, "fetch_agent_registry_summary", lambda: {})
    monkeypatch.setattr(doctor, "_match_known_signature", lambda *a: None)
    monkeypatch.setattr(
        doctor,
        "invoke_doctor_agent",
        lambda *a, **k: (
            True,
            json.dumps(
                {
                    "action_type": "release_to_queue",
                    "confidence": confidence,
                }
            ),
        ),
    )
    release = Mock(return_value=("released", True))
    monkeypatch.setattr(doctor, "apply_release", release)
    result = doctor.process_triage_job({"id": "j", "kind": "triage:z"})
    assert result["action"]["action_type"] == "escalate"
    release.assert_not_called()


@pytest.mark.parametrize("options", [{"gate_enabled": False}, {"auto_approve": True}])
def test_unscoped_switch_is_not_an_approval(options):
    gate = doctor_gates.ApprovalGate(DurableStateStore(MemoryBackend()), **options)
    assert gate.require("cancel_jobs", job_ids=["j"]) is not None


@pytest.mark.parametrize("response", [(503, None), (200, {}), (200, {"messages": "bad"})])
def test_bus_failed_read_never_authorizes_a_save(response):
    http = Mock(return_value=response)
    state = DurableStateStore(BusBackend(http))
    state.mark("ssh:all", ttl=3600)
    assert not state.persist_ok
    assert all(call.args[0] == "GET" for call in http.call_args_list)


@pytest.mark.parametrize(
    "raw",
    [
        '{"doctor.state":',
        "[]",
        '{"doctor.state": []}',
        '{"doctor.state":{"ssh:all":{"timestamps":"broken"}}}',
        '{"doctor.state":{"ssh:all":{"timestamps":[NaN]}}}',
        '{"doctor.state":{"approval:a":{"at":Infinity}}}',
    ],
)
def test_corrupt_state_is_not_overwritten(tmp_path, raw):
    path = tmp_path / "state.json"
    path.write_text(raw)
    state = DurableStateStore(FileBackend(str(path)))
    state.mark("loop:new", ttl=1800)
    assert not state.persist_ok
    assert path.read_text() == raw


def test_stale_file_writer_cannot_erase_another_writer(tmp_path):
    path = str(tmp_path / "state.json")
    a, b = [DurableStateStore(FileBackend(path), refresh_secs=3600) for _ in range(2)]
    a.keys()
    b.mark("loop:b", ttl=1800)
    a.mark("loop:a", ttl=1800)
    assert not a.persist_ok
    assert DurableStateStore(FileBackend(path)).seen("loop:b", ttl=1800)


def test_retrying_stale_counter_preserves_both_writers(tmp_path):
    path = str(tmp_path / "state.json")
    clock = [100.0]
    a, b = [DurableStateStore(FileBackend(path), refresh_secs=3600, time_fn=lambda: clock[0]) for _ in range(2)]
    a.touch("ssh:all", ttl=3600)
    b.keys()
    clock[0] = 101
    a.touch("ssh:all", ttl=3600)
    clock[0] = 102
    b.touch("ssh:all", ttl=3600)
    assert not b.persist_ok
    assert b.timestamps("ssh:all", ttl=3600) == [100, 101, 102]


def test_short_touch_does_not_erase_the_canonical_history():
    state = DurableStateStore(MemoryBackend(), time_fn=lambda: 1000)
    state.touch("loop:j", ttl=1800, at=100)
    assert state.touch("loop:j", ttl=10, at=1000) == [1000]
    assert state.timestamps("loop:j", ttl=1800) == [100, 1000]


def test_approval_read_failure_refuses_cached_grant():
    backend = MemoryBackend()
    state = DurableStateStore(backend, refresh_secs=0)
    gate = doctor_gates.ApprovalGate(state)
    digest = doctor_gates.action_digest("dlq_remove", job_ids=["j"])
    gate.record_approval("dlq_remove", digest)
    backend.load = Mock(side_effect=OSError("offline"))
    assert gate.require("dlq_remove", job_ids=["j"]) is not None


def test_failed_write_does_not_resurrect_unmodified_approval():
    backend = MemoryBackend()
    state = DurableStateStore(backend, refresh_secs=0, time_fn=lambda: 100)
    state.mark("approval:old", ttl=900)
    save = backend.save
    backend.save = Mock(return_value=False)
    state.mark("loop:new", ttl=1800)
    # Another writer removes the approval; our unrelated failed write is not a grant.
    backend._records = {"loop:other": {"at": 100}}
    assert not state.seen("approval:old", ttl=900)
    backend.save = save


def test_empty_successful_load_honors_external_revocation():
    backend = MemoryBackend()
    state = DurableStateStore(backend, refresh_secs=0, time_fn=lambda: 100)
    state.mark("approval:old", ttl=900)
    backend._records = {}
    assert not state.seen("approval:old", ttl=900)


def test_restart_reserves_durable_budget_before_ssh(monkeypatch):
    backend = MemoryBackend()
    state = DurableStateStore(backend, refresh_secs=0)
    monkeypatch.setattr(doctor, "STATE", state)
    gate = doctor_gates.ApprovalGate(state)
    monkeypatch.setattr(doctor, "APPROVALS", gate)
    gate.record_approval(
        "restart_service",
        doctor_gates.action_digest("restart_service", host="hydra", service="zeroclaw-worker.service"),
    )
    backend.save = Mock(return_value=False)
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    ssh = Mock(return_value=subprocess.CompletedProcess([], 0))
    monkeypatch.setattr(doctor.subprocess, "run", ssh)
    result = doctor.action_restart_service("hydra", "zeroclaw-worker.service")
    assert result.startswith("REFUSED"), result
    ssh.assert_not_called()


def test_auto_triage_cannot_submit_before_cooldown_persists(monkeypatch):
    state = doctor.STATE
    state.backend.save = Mock(return_value=False)
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    http = Mock(return_value=(201, {"id": "new"}))
    monkeypatch.setattr(doctor, "_http", http)
    assert doctor.submit_auto_triage("z", 5, "original") is None
    http.assert_not_called()


@pytest.mark.parametrize(
    "urls",
    [
        "ssh://git@private.example/repo.git",
        "ssh://git@argonas/repo.git\nssh://git@github.com/repo.git",
        "",
    ],
)
def test_merge_train_requires_every_resolved_destination_to_be_argonas(monkeypatch, urls):
    calls = []

    def run(args, **kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, urls if args[1] == "remote" else "", "")

    monkeypatch.setattr(merge_train, "_sh", run)
    result = merge_train.fast_forward("unused", "main", "a" * 40, dry_run=False)
    assert result.startswith("REFUSED"), result
    assert not any(args[1] == "push" for args in calls)


def test_repository_failures_reach_every_record():
    outcome = merge_train.RepoOutcome(
        "r",
        failures=["branch b unreadable"],
        reports=[merge_train.BranchReport("r", "a", "fast_forward", "argonas", sha="a" * 40)],
    )
    assert outcome.to_records()[0]["failures"] == outcome.failures


@pytest.mark.parametrize("bucket", ["cherry_pick", "divergent", "conflict"])
def test_salvage_buckets_do_not_request_outward_work(monkeypatch, bucket):
    monkeypatch.setattr(merge_train, "ALLOW_PUBLIC_FORGE", True)
    report = merge_train.BranchReport("r", "hive/x", bucket, "gitlab.com")
    assert merge_train.route_outward(report) is None


def test_sop_job_executes_structured_decision_without_a_model(monkeypatch):
    model = Mock(return_value=(False, "unexpected model call"))
    monkeypatch.setattr(doctor, "invoke_doctor_agent", model)
    monkeypatch.setattr(doctor, "fetch_failed_jobs_for_kind", lambda *a, **k: [])
    monkeypatch.setattr(doctor, "fetch_agent_registry_summary", lambda: {})
    execute = Mock(return_value="requeued 2")
    monkeypatch.setattr(doctor, "action_resubmit_jobs", execute)
    result = doctor.process_triage_job(
        {
            "id": "executor",
            "kind": "doctor:execute",
            "description": json.dumps(
                {"version": 1, "job_id": "original", "action": {"action": "resubmit_jobs", "base_kind": "z", "max": 2}}
            ),
        }
    )
    model.assert_not_called()
    execute.assert_called_once_with("z", 2)
    assert result["exit_code"] == 0


def test_known_fix_task_also_needs_scoped_approval(monkeypatch):
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    http = Mock(return_value=(200, {"id": "fix"}))
    monkeypatch.setattr(doctor, "_http", http)
    assert doctor.action_dispatch_codex_fix("restart services", "j").startswith("REFUSED")
    http.assert_not_called()


def test_bulk_executor_caps_and_excludes_protected_jobs(monkeypatch):
    # Bus job records always carry the retry budget; these still have retries left.
    jobs = [
        {"id": str(i), "kind": "z", "project": "protected" if i < 2 else "normal", "max_retries": 2, "retry_count": 0}
        for i in range(50)
    ]
    monkeypatch.setattr(doctor, "DLQ_PROTECT_PROJECTS", {"protected"})
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    http = Mock(side_effect=[(200, {"jobs": jobs}), (200, {})])
    monkeypatch.setattr(doctor, "_http", http)
    doctor.action_resubmit_jobs("z", 999)
    ids = http.call_args.args[2]["job_ids"]
    assert len(ids) == 20
    assert "0" not in ids and "1" not in ids


def test_corrupt_cadence_cannot_run_or_be_overwritten(tmp_path):
    path = tmp_path / "cadence.json"
    path.write_text("{broken")
    env = {"MERGE_TRAIN_STATE_PATH": str(path)}
    assert not merge_train.due_for_run(env)[0]
    assert not merge_train.record_cycle_end(env=env)
    assert path.read_text() == "{broken"


def test_cli_cannot_apply_without_persisting_cadence(monkeypatch):
    monkeypatch.setattr(merge_train, "due_for_run", lambda: (True, 0))
    monkeypatch.setattr(merge_train, "run_cycle", lambda **kw: [])
    monkeypatch.setattr(merge_train, "record_cycle_end", lambda: False)
    apply = Mock(return_value=[])
    monkeypatch.setattr(merge_train, "fast_forward_candidates", apply)
    assert merge_train.main(["--apply"]) == 1
    apply.assert_not_called()


def test_runtime_defaults_to_durable_file_not_memory_or_message_journal(tmp_path):
    state = state_store_from_env({"HOME": str(tmp_path), "HIVE_URL": "http://unused"})
    assert isinstance(state.backend, FileBackend)
    assert not list(tmp_path.iterdir()), "constructing the store must do no I/O"


@pytest.mark.parametrize("backend", ["bus", "memory"])
def test_runtime_refuses_nondurable_or_bounded_journal_configuration(backend):
    with pytest.raises(ValueError):
        state_store_from_env({"DOCTOR_STATE_BACKEND": backend})


def test_test_environment_cannot_reload_a_live_state_path(tmp_path):
    import os

    assert os.environ.get("DOCTOR_STATE_PATH", "").startswith(str(tmp_path))


def test_import_does_not_install_signal_handlers():
    result = subprocess.run(
        [
            "python3",
            "-c",
            "import signal; before = signal.getsignal(signal.SIGTERM); "
            "import hive.workers.doctor; assert signal.getsignal(signal.SIGTERM) == before",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def _signature_match(monkeypatch):
    monkeypatch.setattr(doctor, "fetch_failed_jobs_for_kind", lambda *a, **k: [])
    monkeypatch.setattr(doctor, "fetch_agent_registry_summary", lambda: {})
    monkeypatch.setattr(doctor, "_doctor_v2_decision", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(
        doctor,
        "_match_known_signature",
        lambda *a: {
            "action_type": "dispatch_codex_fix",
            "confidence": 1.0,
            "reason": "token mismatch",
            "codex_task": "repair the pairing",
            "target_host": "hydra",
            "signature": "token_mismatch",
            "release_eligible_kinds": None,
        },
    )


@pytest.mark.parametrize("dispatch_result", ["REFUSED: approval required", "codex submit FAILED code=500 resp=x"])
def test_known_signature_fix_not_dispatched_is_neither_announced_nor_released(monkeypatch, dispatch_result):
    _signature_match(monkeypatch)
    monkeypatch.setattr(doctor, "action_dispatch_codex_fix", lambda *a, **k: dispatch_result)
    release = Mock(return_value=("released", True))
    monkeypatch.setattr(doctor, "apply_release", release)
    sent = []
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda topic, payload, *a, **k: sent.append(topic))
    doctor.process_triage_job({"id": "j", "kind": "triage:zeroclaw"})
    release.assert_not_called()
    assert "doctor.fix.token_mismatch" not in sent
    assert "doctor.escalate.token_mismatch" in sent


def test_known_signature_fix_dispatched_is_announced_and_released(monkeypatch):
    _signature_match(monkeypatch)
    monkeypatch.setattr(doctor, "action_dispatch_codex_fix", lambda *a, **k: "codex sub-job submitted id=abc")
    release = Mock(return_value=("released", True))
    monkeypatch.setattr(doctor, "apply_release", release)
    sent = []
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda topic, payload, *a, **k: sent.append(topic))
    doctor.process_triage_job({"id": "j", "kind": "triage:zeroclaw"})
    release.assert_called_once()
    assert "doctor.fix.token_mismatch" in sent
