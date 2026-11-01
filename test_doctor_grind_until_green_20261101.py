"""Doctor reviews GRIND UNTIL GREEN (operator 2026-11-01, amended after soundness review).

The two-automatic-rounds-then-escalate rule is replaced by a BOUNDED loop of
alternating fix-in-place reviewers. GREEN is objective:

* the merged-tree suite is clean (no failing test, no error, nothing skipped),
* the import preflight is clean (every changed Python file compiles),
* a structured APPROVE on the exact head from a STRONG reviewer family
  (muse, deepseek, codex review-only per the ladder, opus). Local routes
  (cerberus-reviewer/gpt-oss-20b, qwen38, gemma) may review but never count
  toward green; astra never.

Only ``high`` and ``critical`` findings block. A medium/low finding is recorded
as a follow-up row (``parent_job_id`` = the work row), never re-opens the loop.
Each round reviews the DELTA since the last reviewed head plus whether the
prior blocking findings were resolved. Rotation: the reviewer family differs
from the branch author's and from the previous round's reviewer; a fixer never
approves its own commit, so the round after a fix is reviewed by a family that
did not fix.

Hard limits: ``HIVE_DOCTOR_GRIND_MAX_ROUNDS`` (6), a per-row spend cap, the
global hourly brake ``HIVE_DOCTOR_CONTINUE_MAX_PER_HOUR`` and stall detection
by finding fingerprint (the same fingerprint in two rounds). Any limit escalates
ONCE with the evidence and stops. The test-integrity guard rejects a fix commit
that deletes or weakens assertions, skips/xfails tests, or grows the diff past
``HIVE_DOCTOR_GRIND_DIFF_BUDGET`` (default +400 lines).

The rows are the fleet's own shapes: ``tests/fixtures/doctor_continuation_rows.json``
and the live 2026-11-01 work row of ``tests/fixtures/doctor_review_chain_rows.json``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")

import pytest

from hive import review_queue as rq
from hive.workers import doctor_remediation as dr
from hive.workers.doctor_state import DurableStateStore, MemoryBackend
from test_doctor_continuation_rounds_20261101 import (  # noqa: F401 -- table_only is an autouse fixture
    NOW,
    ROSTER,
    TIP,
    Fake,
    child,
    continuation_row,
    quiet_work_row,
    rc_on,
    table_only,
    work_row,
)

ROOT = Path(__file__).parent
FIXTURES = ROOT / "tests" / "fixtures"
#: The head ``rc_verdict_only`` names: the commit that adds its own verdict.
VERDICT_ONLY_HEAD = "9e1b7c3a5f20d48e6c1a0b9f8e7d6c5b4a392817"


@pytest.fixture
def state():
    return DurableStateStore(MemoryBackend(), refresh_secs=0.0)


# ── a fix commit the guard can inspect ──────────────────────────────────────


class Repo_:
    """A throwaway git repo: a base commit and a working tree to patch."""

    def __init__(self, path: Path):
        self.path = path
        self.git("init", "-q")
        self.git("config", "user.email", "test@example.invalid")
        self.git("config", "user.name", "test")
        (path / "mod.py").write_text(
            "def add(a, b):\n    return a + b\n\n\ndef test_add():\n    assert add(1, 2) == 3\n"
            "    assert add(0, 0) == 0\n"
        )
        self.git("add", "mod.py")
        self.git("commit", "-q", "-m", "base")
        self.base = self.git("rev-parse", "HEAD").strip()

    def git(self, *args: str) -> str:
        return subprocess.run(["git", *args], cwd=self.path, capture_output=True, text=True, check=True).stdout

    def patch(self, text: str, *, extra_lines: int = 0) -> None:
        body = text + ("\n# filler\n" * extra_lines)
        (self.path / "mod.py").write_text(body)
        if extra_lines:
            (self.path / "extra.py").write_text("# padded\n" * 0)
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "fix")

    def head(self) -> str:
        return self.git("rev-parse", "HEAD").strip()


@pytest.fixture
def repo(tmp_path):
    return Repo_(tmp_path)


# ── the strong-reviewer ladder ──────────────────────────────────────────────


def test_the_strong_reviewer_ladder_is_the_operator_list():
    assert dr.STRONG_REVIEW_FAMILIES == ("deepseek", "meta", "openai", "opus")
    assert dr.is_strong_reviewer("muse-spark-1.3")
    assert dr.is_strong_reviewer("deepseek-flash")
    assert dr.is_strong_reviewer("gpt-6-luna")  # codex, review-only, on the ladder
    assert dr.is_strong_reviewer("gpt-6.1-terra")  # the rest of the codex ladder
    assert dr.is_strong_reviewer("claude-opus-4-6")


@pytest.mark.parametrize(
    "model",
    [
        "gpt-oss-20b",  # cerberus-reviewer: local, may review, never green
        "qwen38",
        "gemma4-31b",
        "nvidia/nemotron-3-ultra-550b-a55b",
        "MiniMax-M3",
        "gpt-6-astra",  # the codex AUTHORIZATION model: never a reviewer
        "",
        None,
    ],
)
def test_a_local_or_forbidden_model_is_never_strong(model):
    assert not dr.is_strong_reviewer(model)


def test_astra_never_counts_even_with_an_operator_authorization():
    """``gpt-6-astra`` is the codex authorization model, never auto-selected."""
    assert not dr.is_strong_reviewer("gpt-6-astra", job={"codex_model_authorization": "gpt-6-astra"})


# ── GREEN is objective ──────────────────────────────────────────────────────


def green_review(child_row: dict) -> dict:
    """The reading of a strong approve on TIP with every objective gate clean."""
    reading = dr.read_review(work_row(), child_row, tip=TIP)
    assert reading is not None and reading["verdict"] == "approve"
    return {**reading, "gates": {"suite": "clean", "import_preflight": "clean"}}


def test_a_strong_approve_on_the_exact_head_with_clean_gates_is_green():
    verdict = dr.green_verdict([green_review(child("approve_google", claimed_model="muse-spark-1.3"))])
    assert verdict["green"] is True
    # The verdict record names gemma (google); the job's CLAIM (muse) rides along.
    assert verdict["review"]["families"] == ["google", "meta"]


def test_a_local_approve_is_never_green():
    """cerberus-reviewer/gpt-oss-20b and qwen38 approve; neither counts."""
    for model in ("gpt-oss-20b", "qwen38", "gemma4-31b"):
        local = dr.read_review(work_row(), child("approve_google", claimed_model=model), tip=TIP)
        if local is None:
            # qwen38 is this row's AUTHOR: its approve is not independent and
            # never even reads as a review, let alone as green.
            assert rq.family_of(model) == rq.family_of(dr.author_model(work_row())), model
            continue
        assert local["strong"] is False, model
        verdict = dr.green_verdict([{**local, "gates": {"suite": "clean", "import_preflight": "clean"}}])
        assert verdict["green"] is False and verdict["why"] == "no_strong_approve", model


def test_a_strong_approve_with_a_failing_suite_is_not_green():
    review = green_review(child("approve_google", claimed_model="muse-spark-1.3"))
    review["gates"] = {"suite": "1 failed, 812 passed", "import_preflight": "clean"}
    verdict = dr.green_verdict([review])
    assert verdict["green"] is False and verdict["why"] == "suite_not_clean"


def test_a_strong_approve_with_a_broken_import_is_not_green():
    review = green_review(child("approve_google", claimed_model="muse-spark-1.3"))
    review["gates"] = {"suite": "clean", "import_preflight": "hive/x.py: SyntaxError"}
    verdict = dr.green_verdict([review])
    assert verdict["green"] is False and verdict["why"] == "import_preflight_not_clean"


def test_gates_that_were_never_run_are_not_clean():
    review = green_review(child("approve_google", claimed_model="muse-spark-1.3"))
    review.pop("gates")
    verdict = dr.green_verdict([review])
    assert verdict["green"] is False and verdict["why"] == "gates_unknown"


# ── the merged-tree gates themselves ────────────────────────────────────────


def test_the_suite_gate_reads_a_clean_run_and_rejects_a_skip():
    clean = dr.suite_gate({"returncode": 0, "summary": "812 passed in 41.20s"})
    assert clean["clean"] is True, clean
    for summary in ("1 failed, 811 passed", "812 passed, 1 skipped", "812 passed, 2 xfailed", "1 error in 3s"):
        assert dr.suite_gate({"returncode": 0, "summary": summary})["clean"] is False, summary
        assert dr.suite_gate({"returncode": 1, "summary": summary})["clean"] is False, summary
    assert dr.suite_gate({})["clean"] is False
    assert dr.suite_gate({"returncode": 0, "summary": "no tests ran in 0.01s"})["clean"] is False


def test_the_import_preflight_compiles_every_changed_python_file(repo, tmp_path):
    ok = dr.import_preflight(repo.path, base=repo.base, head=repo.head())
    assert ok["clean"] is True, ok
    (repo.path / "broken.py").write_text("def f(:\n")
    repo.git("add", "broken.py")
    repo.git("commit", "-q", "-m", "break")
    bad = dr.import_preflight(repo.path, base=repo.base, head=repo.head())
    assert bad["clean"] is False and "broken.py" in bad["detail"], bad


def test_the_import_preflight_is_not_clean_when_it_cannot_run(repo):
    """No git, no proof: fail closed, never "nothing to check"."""
    out = dr.import_preflight(repo.path / "does-not-exist", base="0" * 40, head="1" * 40)
    assert out["clean"] is False, out


# ── the test-integrity guard ────────────────────────────────────────────────


GOOD_FIX = (
    "def add(a, b):\n    return a + b\n\n\ndef test_add():\n    assert add(1, 2) == 3\n"
    "    assert add(0, 0) == 0\n    assert add(-1, 1) == 0\n"
)


def test_a_fix_that_adds_an_assertion_is_accepted(repo):
    repo.patch(GOOD_FIX)
    out = dr.test_integrity(repo.path, base=repo.base, head=repo.head(), budget=400)
    assert out["ok"] is True, out


def test_a_fix_that_deletes_an_assertion_is_rejected(repo):
    repo.patch("def add(a, b):\n    return a + b\n\n\ndef test_add():\n    assert add(1, 2) == 3\n")
    out = dr.test_integrity(repo.path, base=repo.base, head=repo.head(), budget=400)
    assert out["ok"] is False and "assertion" in out["why"], out


def test_a_fix_that_deletes_a_test_file_is_rejected(repo):
    (repo.path / "test_other.py").write_text("def test_x():\n    assert 1 == 1\n")
    repo.git("add", "test_other.py")
    repo.git("commit", "-q", "-m", "add test")
    repo.base = repo.head()
    repo.git("rm", "-q", "test_other.py")
    repo.git("commit", "-q", "-m", "delete the test")
    out = dr.test_integrity(repo.path, base=repo.base, head=repo.head(), budget=400)
    assert out["ok"] is False and "deleted" in out["why"], out


def test_a_fix_that_skips_a_test_is_rejected(repo):
    repo.patch(
        "def add(a, b):\n    return a + b\n\n\nimport pytest\n\n\n"
        "def test_add():\n    assert add(1, 2) == 3\n    assert add(0, 0) == 0\n"
        "    pytest.skip('later')\n"
    )
    out = dr.test_integrity(repo.path, base=repo.base, head=repo.head(), budget=400)
    assert out["ok"] is False and "skip" in out["why"], out


def test_a_fix_that_xfails_a_test_is_rejected(repo):
    repo.patch(
        "def add(a, b):\n    return a + b\n\n\nimport pytest\n\n\n@pytest.mark.xfail\n"
        "def test_add():\n    assert add(1, 2) == 3\n    assert add(0, 0) == 0\n"
    )
    out = dr.test_integrity(repo.path, base=repo.base, head=repo.head(), budget=400)
    assert out["ok"] is False and "skip" in out["why"], out


def test_a_fix_over_the_diff_budget_is_rejected(repo):
    padded = "def add(a, b):\n    return a + b\n" + "".join(f"V{i} = {i}\n" for i in range(60))
    repo.patch(padded, extra_lines=60)
    out = dr.test_integrity(repo.path, base=repo.base, head=repo.head(), budget=40)
    assert out["ok"] is False and "budget" in out["why"], out
    assert out["lines"] > 40


def test_a_fix_that_cannot_be_read_is_rejected(repo):
    out = dr.test_integrity(repo.path, base="0" * 40, head="1" * 40, budget=400)
    assert out["ok"] is False, out


# ── the loop ────────────────────────────────────────────────────────────────


def rc_with(row: dict, severity: str, *, model: str = "muse-spark-1.3", round_no: int = 1) -> dict:
    """A request_changes review of ``row``'s tip with ONE located finding."""
    rc = rc_on(row, "rc_meta")
    rc["claimed_model"] = model
    rc["result"]["reviewer"] = {"model": model, "models": [model], "verdict": "request_changes"}
    rc["result"].pop("review_evidence", None)
    findings = [
        {"severity": severity, "location": "hive/review_queue.py:830", "title": f"issue {round_no}", "body": "b"}
    ]
    rc["result"]["verdict"] = {
        "verdict": "request_changes",
        "head": rc["result"]["head_sha"],
        "findings": findings,
    }
    # The reviewer record carries its OWN copy: a caller that rewrites the
    # verdict's findings must not leave a stale duplicate on the reviewer.
    rc["result"]["reviewer"]["findings"] = [dict(f) for f in findings]
    return rc


def approve_by(row: dict, model: str) -> dict:
    ap = rc_on(row, "approve_google")
    ap["claimed_model"] = model
    ap["result"]["reviewer"] = {"model": model, "models": [model], "verdict": "approve"}
    ap["result"].pop("review_evidence", None)
    ap["result"]["verdict"] = {"verdict": "approve", "head": ap["result"]["head_sha"]}
    return ap


class Loop(Fake):
    """A Fake whose gates can be scripted per (head)."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.gates: dict[str, dict] = {}
        self.followups: list[dict] = []

    def gate(self, head: str) -> dict:
        return dict(self.gates.get(head[:12], {"suite": "clean", "import_preflight": "clean"}))

    def add_followup(self, spec: dict) -> tuple[int, dict]:
        self.followups.append(spec)
        return 201, {"id": f"01a1c000-0000-7000-8000-{len(self.followups):012d}"}

    def sweep(self, rows, state, **kw):
        kw.setdefault("roster", ROSTER)
        kw.setdefault("now", NOW)
        kw.setdefault("enabled", True)
        kw.setdefault("dry_run", False)
        return dr.continuation_sweep(
            rows,
            state=state,
            children=self.children,
            submit=self.submit,
            escalate=self.escalate,
            gates=self.gate,
            add_followup=self.add_followup,
            **kw,
        )


def test_a_strong_approve_on_the_exact_head_after_a_fix_is_green(state):
    row = continuation_row(1)
    row["result"]["head_sha"] = row["result"]["commits"][0] = "1" * 40
    fake = Loop({row["id"]: [rc_with(row, "high"), approve_by(row, "muse-spark-1.3")]})
    summary = fake.sweep([row], state)
    assert summary.get(dr.GREEN) == 1, summary
    assert fake.specs == []
    assert dr.green_marker(state, row["id"])["head"] == "1" * 40


def test_a_verdict_only_head_advance_keeps_the_green_review(state):
    """VERDICT-ONLY HEAD ADVANCE: the approve names the commit that adds its verdict."""
    row = work_row()
    fake = Loop({row["id"]: [child("approve_google", claimed_model="muse-spark-1.3")]})
    # The reviewer reports the commit that ADDS its verdict, not the reviewed
    # tip; the callback below is what proves that head is the tip plus a
    # verdict artifact.
    approve = fake.children_of[row["id"]][0]
    approve["result"]["head_sha"] = VERDICT_ONLY_HEAD
    approve["result"]["verdict"] = {"verdict": "approve", "head": VERDICT_ONLY_HEAD}
    seen: list[tuple[str, str]] = []

    def verdict_only(_row, _child, tip, reviewed):
        seen.append((tip, reviewed))
        return tip == TIP and reviewed == VERDICT_ONLY_HEAD

    summary = fake.sweep([row], state, head_is_verdict_only=verdict_only)
    assert seen == [(TIP, VERDICT_ONLY_HEAD)]
    assert summary.get(dr.GREEN) == 1, summary


def test_an_approve_from_another_family_after_a_family_requested_changes_still_counts(state):
    """REQUEST_CHANGES FROM TWO FAMILIES: the delta is fixed, a third family approves."""
    row = continuation_row(1)
    tip = row["result"]["head_sha"]
    fake = Loop({row["id"]: [rc_with(row, "high", round_no=1), approve_by(row, "deepseek-flash")]})
    fake.gates[tip[:12]] = {"suite": "clean", "import_preflight": "clean"}
    summary = fake.sweep([row], state)
    assert summary.get(dr.GREEN) == 1, summary
    assert [s["preferred_models"] for s in fake.specs] == []


def test_a_blocking_finding_gets_a_fix_job_in_place_on_the_branch(state):
    row = continuation_row(1)
    fake = Loop({row["id"]: [rc_with(row, "high", round_no=1)]})
    summary = fake.sweep([row], state)
    assert summary.get(dr.APPLIED) == 1, summary
    spec = fake.specs[0]
    # fix IN PLACE: a continuation commit on the row's own branch, not a new one.
    assert spec["head_branch"] == row["head_branch"]
    assert spec["base_branch"] == row["base_branch"]
    assert spec["parent_job_id"] == row["id"]
    assert ":fix:" in spec["idempotency_key"]
    assert dr.CONTINUATION_INSTRUCTION in spec["description"]
    assert "hive/review_queue.py:830" in spec["description"]


def test_the_next_round_is_reviewed_by_a_family_that_did_not_fix(state):
    """FIXER-THEN-REVIEWER ROTATION: round 2's reviewer is neither author nor fixer."""
    row = continuation_row(1)
    fake = Loop({row["id"]: [rc_with(row, "high", round_no=1)]})
    fake.sweep([row], state)
    fixer = fake.specs[0]["preferred_models"][0]
    # The fix lands; the review of the new head comes from the fixer's own family.
    new_tip = "7" * 40
    row["result"]["head_sha"] = row["result"]["commits"][0] = new_tip
    same_family = rc_with(row, "high", model=fixer, round_no=2)
    fake.children_of[row["id"]].append(same_family)
    fake.sweep([row], state, now=NOW + 60)
    assert len(fake.specs) == 1, "a fixer's own review must not drive another round"
    # A different family's review of that head does drive a round.
    other = rc_with(row, "high", model="qwen38", round_no=2)
    fake.children_of[row["id"]].append(other)
    fake.sweep([row], state, now=NOW + 120)
    assert len(fake.specs) == 2
    assert rq.family_of(fake.specs[1]["preferred_models"][0]) not in {rq.family_of(fixer), "alibaba"}


def test_a_reviewer_never_approves_its_own_fix(state):
    """A fixer's approve of the head it wrote is not green."""
    row = continuation_row(1)
    fake = Loop({row["id"]: [rc_with(row, "high", round_no=1)]})
    fake.sweep([row], state)
    fixer = fake.specs[0]["preferred_models"][0]
    row["result"]["head_sha"] = row["result"]["commits"][0] = "7" * 40
    fake.children_of[row["id"]].append(approve_by(row, fixer))
    summary = fake.sweep([row], state, now=NOW + 60)
    assert summary.get(dr.GREEN, 0) == 0, summary


def test_medium_only_findings_are_recorded_and_never_block(state):
    row = work_row()
    fake = Loop({row["id"]: [rc_with(row, "medium", round_no=1), rc_with(row, "low", round_no=1)]})
    summary = fake.sweep([row], state)
    assert fake.specs == [], "a medium-only round never re-opens the loop"
    assert summary[dr.FOLLOW_UP] == 2, summary
    assert [f["parent_job_id"] for f in fake.followups] == [row["id"], row["id"]]
    assert all(":followup:" in f["idempotency_key"] for f in fake.followups)


def test_a_follow_up_is_written_once_per_finding(state):
    row = work_row()
    fake = Loop({row["id"]: [rc_with(row, "medium", round_no=1)]})
    fake.sweep([row], state)
    fake.sweep([row], state, now=NOW + 60)
    assert len(fake.followups) == 1


def test_a_fix_that_deletes_a_test_fails_the_round_and_never_merges(state):
    row = continuation_row(1)
    fake = Loop({row["id"]: [rc_with(row, "high", round_no=1)]})
    fake.sweep([row], state)
    rejected = {"ok": False, "why": "assertion_removed: mod.py:5", "lines": 3}
    summary = fake.sweep([row], state, now=NOW + 60, integrity=lambda *_a, **_k: rejected)
    assert summary.get(dr.REJECTED) == 1, summary
    assert dr.green_marker(state, row["id"]) is None
    assert fake.escalated and "test_integrity" in fake.escalated[-1][1]


def test_the_same_fingerprint_in_two_rounds_is_a_stall(state):
    row = continuation_row(1)
    fake = Loop({row["id"]: [rc_with(row, "high", round_no=1)]})
    fake.sweep([row], state)
    row["result"]["head_sha"] = row["result"]["commits"][0] = "7" * 40
    again = rc_with(row, "high", round_no=2)
    again["result"]["verdict"]["findings"] = [
        {"severity": "high", "location": "hive/review_queue.py:830", "title": "issue 1", "body": "b"}
    ]
    again["result"]["reviewer"]["findings"] = [dict(f) for f in again["result"]["verdict"]["findings"]]
    fake.children_of[row["id"]].append(again)
    summary = fake.sweep([row], state, now=NOW + 60)
    assert summary.get(dr.STALLED) == 1, summary
    assert len(fake.specs) == 1
    assert "stall" in fake.escalated[0][1]


def test_a_different_finding_is_not_a_stall(state):
    row = continuation_row(1)
    fake = Loop({row["id"]: [rc_with(row, "high", round_no=1)]})
    fake.sweep([row], state)
    row["result"]["head_sha"] = row["result"]["commits"][0] = "7" * 40
    fake.children_of[row["id"]].append(rc_with(row, "high", round_no=2))
    summary = fake.sweep([row], state, now=NOW + 60)
    assert summary.get(dr.STALLED, 0) == 0 and len(fake.specs) == 2, summary


def test_the_round_cap_is_six_and_escalates_once(state):
    assert dr.GRIND_MAX_ROUNDS == 6
    row = continuation_row(6)
    fake = Loop({row["id"]: [rc_with(row, "high", round_no=6)]})
    first = fake.sweep([row], state)
    fake.sweep([row], state, now=NOW + 60)
    assert fake.specs == []
    assert first.get(dr.ESCALATED) == 1, first
    assert len(fake.escalated) == 1 and "6" in fake.escalated[0][1]


@pytest.mark.parametrize("value, expected", [("2", 2), ("0", 0), ("-3", 0), ("junk", 6), ("", 6)])
def test_the_round_cap_reads_its_env(value, expected):
    env = {**os.environ, "HIVE_DOCTOR_GRIND_MAX_ROUNDS": value}
    out = subprocess.run(
        [sys.executable, "-c", "from hive.workers import doctor_remediation as d; print(d.GRIND_MAX_ROUNDS)"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout.strip() == str(expected)


def test_the_spend_cap_escalates_once(state, monkeypatch):
    monkeypatch.setattr(dr, "GRIND_MAX_SPEND_USD", 1.0)
    row = continuation_row(1)
    fake = Loop({row["id"]: [rc_with(row, "high", round_no=1)]})
    dr.record_spend(state, row["id"], 1.5, now=NOW)
    summary = fake.sweep([row], state)
    assert fake.specs == [] and summary.get(dr.ESCALATED) == 1, summary
    assert "spend" in fake.escalated[0][1]


def test_the_hourly_brake_still_bounds_creation(state, monkeypatch):
    monkeypatch.setattr(dr, "CONTINUE_MAX_PER_HOUR", 1)
    rows, kids = [], {}
    for i in range(2):
        r = continuation_row(1, id=f"01a1b222-0000-7000-8000-00000000000{i}", ended_at=NOW + i)
        r["result"]["head_sha"] = r["result"]["commits"][0] = f"{i + 1}" * 40
        rows.append(r)
        kids[r["id"]] = [rc_with(r, "high", round_no=1)]
    fake = Loop(kids)
    summary = fake.sweep(rows, state)
    assert len(fake.specs) == 1 and summary.get(dr.THROTTLED) == 1, summary


def test_dry_run_fixes_nothing_and_marks_nothing(state):
    row = continuation_row(1)
    fake = Loop({row["id"]: [rc_with(row, "high", round_no=1)]})
    summary = fake.sweep([row], state, dry_run=True)
    assert fake.specs == [] and summary.get(dr.DRY_WOULD_APPLY) == 1, summary
    assert dr.green_marker(state, row["id"]) is None


def test_the_kill_switch_stops_the_loop(state, monkeypatch):
    monkeypatch.setattr(dr, "REMEDIATE_ENABLED", False)
    row = continuation_row(1)
    fake = Loop({row["id"]: [rc_with(row, "high", round_no=1)]})
    summary = dr.continuation_sweep(
        [row],
        state=state,
        children=fake.children,
        submit=fake.submit,
        escalate=fake.escalate,
        roster=ROSTER,
        now=NOW,
    )
    assert summary["enabled"] is False and fake.specs == []


def test_a_review_of_a_review_is_never_reviewed(state):
    row = work_row(kind="review:build:zeroclaw-20261101")
    fake = Loop({row["id"]: [rc_with(row, "high")]})
    fake.sweep([row], state)
    assert fake.specs == [] and fake.lookups == []
