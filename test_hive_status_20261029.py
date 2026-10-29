"""Tests for scripts/hive_status.py (offline --rows mode only, no network)."""

from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from pathlib import Path

from scripts import hive_status

FIXTURE = Path(__file__).parent / "tests" / "fixtures" / "hive_status.sample.json"
NOW = 1793315905.0  # fixed clock so ages are deterministic


def _rows():
    return json.loads(FIXTURE.read_text())


def test_counts_and_active_rows():
    snap = hive_status.build_status(_rows(), older_than_hours=24.0, now=NOW)
    assert snap["queue"]["by_status"]["running"] == 1
    assert snap["queue"]["by_status"]["failed"] == 11
    assert snap["queue"]["by_status"]["needs-review"] == 1
    assert snap["agents"]["by_kind"]["zeroclaw"] == 2
    assert snap["agents"]["by_status"]["offline"] == 1
    by_id = {r["id"]: r for r in snap["active"]}
    assert by_id["11111111-0000-4000-8000-000000000001"]["host"] == "h1"
    assert by_id["11111111-0000-4000-8000-000000000002"]["host"] == "h2"
    assert by_id["11111111-0000-4000-8000-000000000001"]["head_branch"] == "sonnet/aaa"


def test_every_outcome_class_has_a_planned_action():
    snap = hive_status.build_status(_rows(), older_than_hours=24.0, now=NOW)
    got = {(r["kind"], r["class"], r["doctor_action"]) for r in snap["attention"]}
    assert ("test:author", "author_failed", "requeue") in got
    assert ("test:reviewer", "reviewer_unavailable", "requeue") in got
    assert ("test:timeout", "timeout", "requeue") in got
    assert ("test:crash", "crash", "requeue") in got
    assert ("test:exhausted", "exhausted_request_changes", "needs_review") in got
    assert ("test:nothing", "nothing_to_commit", "needs_review") in got
    assert ("test:approved", "approved_unmerged", "skip") in got
    assert ("test:signal", "killed_by_signal", "needs_review") in got
    assert ("test:clone", "clone_failed", "requeue") in got
    assert ("test:empty", "empty_diff", "needs_review") in got
    assert ("test:mystery", "unknown", "default") in got
    assert ("test:parked", "author_failed", "requeue") in got


def test_flags_stale_and_offline_claiming():
    snap = hive_status.build_status(_rows(), older_than_hours=1.0, now=NOW)
    stale_ids = {r["id"] for r in snap["flags"]["stale"]}
    assert "11111111-0000-4000-8000-000000000002" in stale_ids  # claimed 4h ago
    assert "11111111-0000-4000-8000-000000000001" not in stale_ids  # running, fresh
    assert snap["flags"]["offline_claiming"] == [{"urn": "urn:agent:zeroclaw:h2:222", "kind": "zeroclaw", "host": "h2"}]


def test_empty_queue():
    snap = hive_status.build_status({"jobs": [], "agents": []}, now=NOW)
    assert snap["queue"]["total"] == 0
    assert snap["active"] == []
    assert snap["attention"] == []
    assert snap["flags"] == {"stale": [], "offline_claiming": []}
    text = hive_status.render_text(snap)
    assert "(none)" in text


def test_json_output_and_no_token_leak(monkeypatch, tmp_path):
    out = tmp_path / "out.json"
    rows = _rows()
    rows_path = tmp_path / "rows.json"
    rows_path.write_text(json.dumps(rows))
    monkeypatch.setenv("HIVE_BUS_ADMIN_TOKEN", "super-secret-token-value")
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = hive_status.main(["--json", "--rows", str(rows_path)])
    assert rc == 0
    out.write_text(buf.getvalue())
    payload = json.loads(out.read_text())
    assert payload["queue"]["total"] == len(rows["jobs"])
    assert "super-secret-token-value" not in out.read_text()


def test_cli_missing_hive_url_errors(monkeypatch):
    monkeypatch.delenv("HIVE_URL", raising=False)
    rc = hive_status.main(["--older-than-hours", "1"])
    assert rc == 2
