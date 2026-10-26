import json

import pytest

from hive.workers.doctor_state_migrate import inspect_and_import


def source(tmp_path):
    path = tmp_path / "source.json"
    records = {
        "loop:a": {"timestamps": [100.0, 101.0], "tombstone": False},
        "approval:revoked": {"at": 102.0, "tombstone": True},
        "approval:used": {
            "at": 103.0,
            "value": {
                "digest": "used",
                "approver": "operator:a",
                "granted_at": 100.0,
                "expires_at": 200.0,
                "used": True,
            },
        },
    }
    path.write_text(json.dumps({"doctor.state": records}))
    return path, records


def test_dry_run_writes_nothing_and_round_trip(tmp_path):
    src, records = source(tmp_path)
    target = tmp_path / "target.json"
    report = inspect_and_import(source_file=str(src), target_file=str(target))
    assert report["would_write"] == {"doctor.state": records}
    assert not target.exists()
    inspect_and_import(source_file=str(src), target_file=str(target), apply=True)
    assert json.loads(target.read_text())["doctor.state"] == records


def test_nonempty_target_refused(tmp_path):
    src, _ = source(tmp_path)
    target = tmp_path / "target.json"
    target.write_text(json.dumps({"doctor.state": {"loop:old": {"at": 20}}}))
    with pytest.raises(ValueError, match="non-empty"):
        inspect_and_import(source_file=str(src), target_file=str(target), apply=True)
    assert "loop:old" in target.read_text()


def test_failed_or_truncated_bus_read_refused(tmp_path):
    target = tmp_path / "target.json"
    with pytest.raises((OSError, ValueError)):
        inspect_and_import(bus_http=lambda *_: (503, {}), target_file=str(target), apply=True, bus_limit=2)
    messages = [{"ts": n, "payload": {"key": f"loop:{n}", "value": {"at": n}}} for n in range(3)]
    with pytest.raises(ValueError, match="truncated"):
        inspect_and_import(
            bus_http=lambda *_: (200, {"messages": messages}), target_file=str(target), apply=True, bus_limit=2
        )
    assert not target.exists()


def test_bus_tombstone_survives(tmp_path):
    target = tmp_path / "target.json"
    messages = [
        {"ts": 1, "payload": {"key": "approval:abc", "value": {"at": 1, "value": {"digest": "abc", "used": False}}}},
        {"ts": 2, "payload": {"key": "approval:abc", "value": {"at": 2, "tombstone": True}}},
    ]
    inspect_and_import(bus_http=lambda *_: (200, {"messages": messages}), target_file=str(target), apply=True)
    assert json.loads(target.read_text())["doctor.state"]["approval:abc"]["tombstone"] is True


def test_bus_inspection_uses_get_only(tmp_path):
    calls = []

    def fake(method, path):
        calls.append((method, path))
        return 200, {"messages": [{"ts": 1, "payload": {"key": "loop:a", "value": {"at": 1}}}]}

    target = tmp_path / "target.json"
    report = inspect_and_import(bus_http=fake, target_file=str(target))
    assert report["would_write"]["doctor.state"]["loop:a"] == {"at": 1}
    assert calls == [("GET", "/v1/messages?topic=doctor.state&limit=501")]
    assert not target.exists()


def test_empty_bus_cannot_be_imported_as_replacement(tmp_path):
    target = tmp_path / "target.json"
    with pytest.raises(ValueError, match="empty"):
        inspect_and_import(bus_http=lambda *_: (200, {"messages": []}), target_file=str(target), apply=True)
    assert not target.exists()
