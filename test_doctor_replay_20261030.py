"""FB39: the doctor v1-vs-v2 replay harness and its pins.

Covers the harness itself (agreement mapping, table output, allowlist
enforcement incl. stale pins) and the fixture set (every loop_outcome class
represented). The harness must never touch the network or write anything.
"""

from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from hive.workers import loop_outcome  # noqa: E402
from scripts import doctor_replay  # noqa: E402

ROOT = Path(__file__).resolve().parent
CASES = ROOT / "scripts" / "doctor_replay_cases.json"
ALLOWLIST = ROOT / "scripts" / "doctor_replay_allowlist.json"
RULES = ROOT / "docs" / "doctor_v2_rules.toml"


def _case(**overrides):
    base = {
        "id": "t",
        "job": {"id": "t", "kind": "fix:t", "status": "failed", "retry_count": 0, "max_retries": 2, "result": {}},
        "events": [],
    }
    base.update(overrides)
    return base


# ── the agreement mapping ──────────────────────────────────────────────────


def test_agree_maps_operational_equivalents():
    assert doctor_replay.agree("requeue", "release_to_queue")
    assert doctor_replay.agree("needs_review", "escalate")
    assert doctor_replay.agree("needs_review", "needs_review")
    assert doctor_replay.agree("skip", "no_action")
    assert doctor_replay.agree("parked", "escalate")
    assert not doctor_replay.agree("requeue", "escalate")
    assert not doctor_replay.agree("hold", "release_to_queue")
    assert not doctor_replay.agree("default", "escalate")


# ── the fixture set ────────────────────────────────────────────────────────


def test_every_loop_outcome_class_is_covered():
    """Each of the 11 loop_outcome classes appears in the replay sequence."""
    cases = json.loads(CASES.read_text())["cases"]
    seen = set()
    for case in cases:
        if case["job"].get("status") == "failed":
            seen.add(
                loop_outcome.classify(case["job"], events_fetcher=lambda _jid, _case=case: _case.get("events") or [])
            )
    expected = {
        loop_outcome.REVIEWER_UNAVAILABLE,
        loop_outcome.AUTHOR_FAILED,
        loop_outcome.TIMEOUT,
        loop_outcome.CRASH,
        loop_outcome.EXHAUSTED,
        loop_outcome.NOTHING_TO_COMMIT,
        loop_outcome.APPROVED_UNMERGED,
        loop_outcome.KILLED_BY_SIGNAL,
        loop_outcome.CLONE_FAILED,
        loop_outcome.EMPTY_DIFF,
        loop_outcome.UNKNOWN,
    }
    assert expected <= seen, f"missing classes: {expected - seen}"


def test_review_stuck_kill_and_empty_diff_shapes_are_present():
    ids = {c["id"] for c in json.loads(CASES.read_text())["cases"]}
    assert "review-stuck-needs-review" in ids
    assert "sigterm-143" in ids
    assert "sigkill-local-commits" in ids
    assert "empty-diff-marker" in ids
    assert "empty-iterations" in ids
    assert "request-changes-exhausted" in ids


# ── allowlist enforcement ──────────────────────────────────────────────────


def test_unexpected_disagreement_exits_nonzero(tmp_path, capsys):
    cases = tmp_path / "cases.json"
    cases.write_text(json.dumps([_case(id="boom")]))
    allow = tmp_path / "allow.json"
    allow.write_text("[]")
    assert doctor_replay.main(["--cases", str(cases), "--allowlist", str(allow), "--rules", str(RULES)]) == 1
    capsys.readouterr()


def test_allowlisted_disagreement_passes(tmp_path, capsys):
    row = {
        "id": "boom",
        "v1_class": "unknown",
        "v1_action": "default",
        "v2_rule": "R10",
        "v2_action": "escalate",
        "agree": False,
    }
    cases = tmp_path / "cases.json"
    cases.write_text(json.dumps([_case(id="boom")]))
    allow = tmp_path / "allow.json"
    allow.write_text(json.dumps([{k: row[k] for k in ("id", "v1_action", "v2_action", "v2_rule")} | {"reason": "t"}]))
    assert doctor_replay.main(["--cases", str(cases), "--allowlist", str(allow), "--rules", str(RULES)]) == 0
    capsys.readouterr()


def test_stale_allowlist_entry_exits_nonzero(tmp_path, capsys):
    cases = tmp_path / "cases.json"
    cases.write_text(
        json.dumps(
            [
                _case(
                    id="fine",
                    job={
                        "id": "fine",
                        "kind": "fix:t",
                        "status": "failed",
                        "retry_count": 0,
                        "max_retries": 2,
                        "result": {"loop_result": {"teardown": {"reason": "approved"}}},
                    },
                )
            ]
        )
    )
    allow = tmp_path / "allow.json"
    allow.write_text(
        json.dumps(
            [{"id": "gone", "v1_action": "default", "v2_action": "escalate", "v2_rule": "R10", "reason": "stale"}]
        )
    )
    assert doctor_replay.main(["--cases", str(cases), "--allowlist", str(allow), "--rules", str(RULES)]) == 1
    capsys.readouterr()


# ── the shipped sequence is clean ──────────────────────────────────────────


def test_shipped_sequence_agrees_or_is_pinned(capsys):
    assert doctor_replay.main([]) == 0
    capsys.readouterr()


def test_harness_never_touches_the_network(monkeypatch, capsys):
    """MNEMOS set and urlopen forbidden: the replay must stay offline."""

    def _forbidden(*_a, **_k):  # pragma: no cover - must never run
        raise AssertionError("the replay harness reached the network")

    monkeypatch.setattr(urllib.request, "urlopen", _forbidden)
    monkeypatch.setenv("MNEMOS_URL", "http://127.0.0.1:9")
    monkeypatch.setenv("MNEMOS_TOKEN", "would-be-used")
    monkeypatch.setenv("DOCTOR_REVIEWER_HEALTH_URL", "http://127.0.0.1:9")
    assert doctor_replay.main([]) == 0
    capsys.readouterr()


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
