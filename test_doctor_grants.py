import json
from concurrent.futures import ThreadPoolExecutor

from hive.workers.doctor_gates import ApprovalGate, action_digest
from hive.workers.doctor_state import DurableStateStore, FileBackend


def gate(path):
    return ApprovalGate(DurableStateStore(FileBackend(str(path)), refresh_secs=0), ttl=900)


def test_exact_single_use_and_restart(tmp_path):
    path = tmp_path / "state.json"
    first = gate(path)
    digest = action_digest("dlq_remove", job_ids=["a"])
    first.record_approval("dlq_remove", digest, by="operator:alice")
    assert first.require("dlq_remove", job_ids=["b"])
    assert first.require("dlq_remove", job_ids=["a"]) is None
    assert gate(path).require("dlq_remove", job_ids=["a"])
    record = json.loads(path.read_text())["doctor.state"][f"approval:{digest}"]["value"]
    assert record["approver"] == "operator:alice"
    assert record["used"] is True
    assert record["expires_at"] - record["granted_at"] == 900


def test_revoke_survives_restart(tmp_path):
    path = tmp_path / "state.json"
    first = gate(path)
    digest = action_digest("dlq_remove", job_ids=["a"])
    first.record_approval("dlq_remove", digest, by="operator:alice")
    first.revoke(digest)
    assert gate(path).require("dlq_remove", job_ids=["a"])


def test_concurrent_consume_once(tmp_path):
    path = tmp_path / "state.json"
    digest = action_digest("dlq_remove", job_ids=["a"])
    gate(path).record_approval("dlq_remove", digest, by="operator:alice")
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: gate(path).require("dlq_remove", job_ids=["a"]), range(8)))
    assert results.count(None) == 1


def test_legacy_marker_and_payload_flag_cannot_grant(tmp_path):
    path = tmp_path / "state.json"
    first = gate(path)
    digest = action_digest("dlq_remove", job_ids=["a"])
    first.store.mark(f"approval:{digest}", ttl=900)
    assert gate(path).require("dlq_remove", job_ids=["a"], approved=True)
    assert gate(path).require("dlq_remove", job_ids=["a"])


def test_cli_issue_list_revoke(tmp_path, capsys):
    from hive.workers.doctor_grants import main

    path = tmp_path / "state.json"
    action = json.dumps({"kind": "dlq_remove", "parts": {"job_ids": ["a"]}})
    assert main(["--state-path", str(path), "issue", "--action", action, "--approver", "operator:alice"]) == 0
    digest = json.loads(capsys.readouterr().out)["digest"]
    assert main(["--state-path", str(path), "list"]) == 0
    assert digest in capsys.readouterr().out
    assert main(["--state-path", str(path), "revoke", digest]) == 0
    capsys.readouterr()
    assert gate(path).require("dlq_remove", job_ids=["a"])
    assert main(["--state-path", str(path), "issue", "--action", action, "--approver", "model"]) == 1


def test_expired_grant_refused(tmp_path):
    path = tmp_path / "state.json"
    first = gate(path)
    digest = action_digest("dlq_remove", job_ids=["a"])
    first.record_approval("dlq_remove", digest, by="operator:alice")
    # Edit the isolated test fixture to simulate a grant that expired before use.
    blob = json.loads(path.read_text())
    blob["doctor.state"][f"approval:{digest}"]["value"]["expires_at"] = 1
    path.write_text(json.dumps(blob))
    assert gate(path).require("dlq_remove", job_ids=["a"])
