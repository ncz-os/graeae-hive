"""Review-queue hardening: partial blocks and defensive env parsing (2026-10-31).

The structured review of the adoption work returned ``request_changes`` with two
findings. Both are verified here against the source before anything changed:

**high -- a partial ``review_state`` blocks adoption forever.** The sweep reads
``usable_review_state``; that answered None for a block missing ``author``,
``attempt`` or ``max_attempts``, and ``needs_adoption`` then declined the row
because ``review_state()`` said "a block is present". The row was therefore
neither advanced (no usable block) nor adopted (looks owned): it sat in
needs-review forever. A PARTIAL or MALFORMED block is a repair case -- adopt it
again, from the row, and correct the block.

**high -- an unguarded ``int()`` on an env var crashes module import.** Every
``int(os.environ.get(...))`` at import time raises ValueError on a typo
(``HIVE_DOCTOR_REVIEW_SUBMIT_MAX=three``) and takes the whole doctor module
down, and a malformed value INSIDE a row's ``review_state`` raised out of the
pure helpers (``attempts_remain`` on ``attempt='zero'``). Every parse is
defensive now: default, plus a logged warning.

Everything is hermetic: no bus, no probes, no deployed resolver modules.
"""

import importlib
import os
import sys
import warnings

import pytest

from hive import review_queue as rq

HEAD = "a" * 40
OLD_HEAD = "b" * 40


# ── finding 1: a partial/malformed review_state is still adopted ────────────


def _row(state_block, *, jid="partial-1", **extra):
    """A needs-review row carrying ``state_block`` verbatim (legacy otherwise)."""
    row = {
        "id": jid,
        "kind": "fix:thing",
        "status": "needs-review",
        "repo": "graeae-hive",
        "project": "graeae-hive",
        "head_branch": f"hv/{jid}",
        "claimed_model": "gpt-oss-20b",
        "result": {"head_sha": HEAD, "commits": [HEAD]},
    }
    if state_block is not None:
        row["result"]["review_state"] = state_block
    row.update(extra)
    return row


PARTIAL_BLOCKS = {
    "missing attempt": {"state": "awaiting_review", "author": "gpt-oss-20b"},
    "missing author": {"state": "awaiting_review", "attempt": 0, "max_attempts": 3},
    "missing max_attempts": {"state": "awaiting_review", "author": "gpt-oss-20b", "attempt": 0},
    "attempt is a word": {"state": "awaiting_review", "author": "a", "attempt": "zero", "max_attempts": 3},
    "attempt is a list": {"state": "awaiting_review", "author": "a", "attempt": [1], "max_attempts": 3},
    "max_attempts is a word": {"state": "awaiting_review", "author": "a", "attempt": 0, "max_attempts": "three"},
    "tried is not a list": {"state": "awaiting_review", "author": "a", "attempt": 0, "max_attempts": 3, "tried": "qwen"},
    "empty block": {},
}


@pytest.mark.parametrize("label", sorted(PARTIAL_BLOCKS))
def test_a_partial_or_malformed_review_state_is_still_adopted(label):
    """Regression (review 2026-10-31, high): partial blocks must not be skipped."""
    block = PARTIAL_BLOCKS[label]
    row = _row(block)

    # Not usable: the sweep cannot advance it as it stands.
    assert rq.usable_review_state(row) is None, label

    # But it IS adoptable -- repaired from the row -- and the repair is complete.
    adopt, _reason = rq.needs_adoption(row)
    assert adopt is True, f"{label}: a partial block must not block adoption"
    repaired = rq.adoption_review_state(row, now=1000.0)
    assert repaired is not None, label
    for field in rq.ADOPTION_REQUIRED_FIELDS:
        assert field in repaired, f"{label}: repaired block missing {field}"
    assert repaired["attempt"] == 0 and repaired["max_attempts"] == rq.MAX_REVIEW_ATTEMPTS
    assert repaired["author"] == "gpt-oss-20b", label
    assert isinstance(repaired["tried"], list), label
    # The repaired block is complete, so the NEXT sweep sees a usable row.
    assert rq.usable_review_state({"result": {"review_state": repaired}}) is not None


def test_a_partial_block_is_adopted_by_the_doctor_without_touching_status():
    """The doctor's adoption path repairs the block and keeps needs-review."""
    from hive.workers import doctor

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(doctor, "HIVE_DOCTOR_ADOPT_LEGACY_REVIEWS", True)
        # No review child listing: the row has no live children.
        monkeypatch.setattr(doctor, "_review_is_in_flight", lambda _job: False)
        row = _row({"state": "awaiting_review", "author": "gpt-oss-20b"})
        assert doctor.adopt_legacy_review_row(row, now=1000.0) is True
        state = rq.review_state(row)
        assert state is not None and state["attempt"] == 0
        assert state["max_attempts"] == rq.MAX_REVIEW_ATTEMPTS
        assert row["status"] == "needs-review"
        # Idempotent: a second sweep finds nothing to repair.
        assert doctor.adopt_legacy_review_row(row, now=1001.0) is False
    finally:
        monkeypatch.undo()


def test_a_complete_block_is_never_re_adopted():
    """The positive control: the fix must not make every row a repair case."""
    row = _row(rq.make_review_state("gpt-oss-20b", [], now=1000.0))
    assert rq.usable_review_state(row) is not None
    assert rq.needs_adoption(row) == (False, "")
    assert rq.adoption_review_state(row, now=1000.0) is None


def test_a_block_with_a_live_review_is_not_repaired_or_duplicated():
    """A partial block whose review is in flight is still never a second review."""
    row = _row({"state": "awaiting_review", "author": "a", "review_job_id": "review-7"})
    assert rq.needs_adoption(row) == (False, "")


def test_the_pure_helpers_survive_malformed_block_values():
    """A malformed value inside a block must not raise out of the helpers."""
    state = {"state": "awaiting_review", "author": "a", "attempt": "zero", "max_attempts": "three"}
    assert rq.attempts_remain(state) is True  # unreadable attempt -> 0 of 3
    assert rq.in_backoff({**state, "next_attempt_at": "soon"}, now=0) is False
    assert rq.is_parked(state) is False and rq.is_resolved(state) is False
    # The triage listing renders it instead of crashing the listing.
    text = rq.awaiting_review_reason({"result": {"review_state": {**state, "tried": ["qwen"]}}})
    assert "author family" in text


# ── finding 2: env parsing is defensive and logged ─────────────────────────


def _fresh_doctor(monkeypatch, env):
    """Import ``hive.workers.doctor`` from scratch under ``env``."""
    for name in ("HIVE_DOCTOR_ADOPT_LEGACY_REVIEWS", "HIVE_DOCTOR_REVIEW_SUBMIT_MAX", "HIVE_DOCTOR_ADOPT_DRY_RUN"):
        monkeypatch.delenv(name, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    from hive.workers import doctor

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        importlib.reload(doctor)
    return doctor, [str(w.message) for w in caught]


def test_a_malformed_cap_env_var_does_not_crash_the_import():
    """Regression (review 2026-10-31, high): ``int()`` on an env var at import."""
    monkeypatch = pytest.MonkeyPatch()
    try:
        doctor, messages = _fresh_doctor(monkeypatch, {"HIVE_DOCTOR_REVIEW_SUBMIT_MAX": "three"})
        assert doctor.HIVE_DOCTOR_REVIEW_SUBMIT_MAX == 3, "the default must be used"
        assert any("HIVE_DOCTOR_REVIEW_SUBMIT_MAX" in m for m in messages), messages
    finally:
        monkeypatch.undo()
        importlib.reload(__import__("hive.workers.doctor", fromlist=["doctor"]))


def test_a_negative_cap_is_clamped_to_zero():
    monkeypatch = pytest.MonkeyPatch()
    try:
        doctor, _messages = _fresh_doctor(monkeypatch, {"HIVE_DOCTOR_REVIEW_SUBMIT_MAX": "-4"})
        assert doctor.HIVE_DOCTOR_REVIEW_SUBMIT_MAX == 0
        doctor, _messages = _fresh_doctor(monkeypatch, {"HIVE_DOCTOR_REVIEW_SUBMIT_MAX": "7"})
        assert doctor.HIVE_DOCTOR_REVIEW_SUBMIT_MAX == 7
    finally:
        monkeypatch.undo()
        importlib.reload(__import__("hive.workers.doctor", fromlist=["doctor"]))


def test_every_import_time_env_int_in_the_doctor_is_guarded():
    """No bare ``int(os.environ...`` may remain at the doctor's module scope."""
    import ast
    import inspect
    import re

    from hive.workers import doctor

    source = inspect.getsource(doctor)
    tree = ast.parse(source)
    offenders = []
    for node in tree.body:  # module scope only: import time is what crashes
        for sub in ast.walk(node):
            if not isinstance(sub, ast.Call) or not sub.args:
                continue
            func = sub.func
            if not (isinstance(func, ast.Name) and func.id == "int"):
                continue
            text = ast.get_source_segment(source, sub) or ""
            if "environ" in text:
                offenders.append(text)
    assert not offenders, f"unguarded env int() at import time: {offenders}"
    assert re.search(r"def _env_int\(", source), "the defensive parser must exist"


def test_the_defensive_env_parser_reads_each_shape():
    """``_env_int``: default, bounds, and a warning naming the variable."""
    from hive.workers import doctor

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert doctor._env_int("__HIVE_TEST_UNSET__", 5) == 5
        os.environ["__HIVE_TEST_INT__"] = " 9 "
        try:
            assert doctor._env_int("__HIVE_TEST_INT__", 5) == 9
            os.environ["__HIVE_TEST_INT__"] = "nope"
            assert doctor._env_int("__HIVE_TEST_INT__", 5) == 5
            assert any("__HIVE_TEST_INT__" in str(w.message) for w in caught)
            os.environ["__HIVE_TEST_INT__"] = "-2"
            assert doctor._env_int("__HIVE_TEST_INT__", 5, minimum=0) == 0
        finally:
            del os.environ["__HIVE_TEST_INT__"]
            sys.modules.pop("__nope__", None)
