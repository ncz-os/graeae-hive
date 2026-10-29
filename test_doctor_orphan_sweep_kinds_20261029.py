"""The orphan-failure sweep must not recycle rows no live worker kind can take.

Operator 2026-10-29: "Codex is at quota for several days, so jobs should not go
to codex". The sweep requeued a failed build:codex row once a minute: its
"requeued N jobs" result was never marked as handled, and the only workers that
could claim it were codex workers that failed it again at once.
"""

from __future__ import annotations

import hashlib
import os
import unittest
from unittest import mock

os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")

from hive.workers import doctor


def _failed(jid="f1", kind="build:codex", kinds=("codex",)):
    return {
        "id": jid,
        "kind": kind,
        "eligible_kinds": list(kinds) if kinds is not None else None,
        "parent_job_id": None,
        "max_retries": 2,
        "retry_count": 0,
    }


class _FakeState:
    """Mirrors DurableStateStore: last_mark() never expires, seen() applies the window."""

    def __init__(self, now=1_000_000.0):
        self.marks = {}
        self.now = now

    def last_mark(self, key):
        return self.marks.get(key)

    def mark(self, key, ttl=None):
        self.marks[key] = self.now

    def seen(self, key, ttl):
        last = self.marks.get(key)
        return last is not None and ttl > 0 and (self.now - last) <= ttl


def _run(failed, agents, resubmit_text="requeued 1 jobs of build:codex (api=200)", state=None):
    resubmitted = []

    def fake_http(method, path, payload=None, timeout=10.0):
        if path.startswith("/v1/jobs?status=failed"):
            return 200, {"jobs": failed}
        if path.startswith("/v1/jobs?status=queued"):
            return 200, {"jobs": []}
        if path.startswith("/v1/agents"):
            return (200, {"agents": agents}) if agents is not None else (503, None)
        return 200, {}

    def fake_resubmit(base_kind, *a, **k):
        resubmitted.append(base_kind)
        return resubmit_text

    state = state or _FakeState()
    with (
        mock.patch.object(doctor, "_http", fake_http),
        mock.patch.object(doctor, "dlq_protected", lambda j: False),
        mock.patch.object(doctor, "action_resubmit_jobs", fake_resubmit),
        mock.patch.object(doctor, "STATE", state),
    ):
        out = doctor.sweep_orphan_failures()
    return out, resubmitted, state


def _set_key(*ids, base="build:zeroclaw"):
    """The cooldown key the sweep uses: base kind plus a digest of the failed row ids."""
    digest = hashlib.sha1(",".join(sorted(ids)).encode()).hexdigest()[:12]
    return f"orphan_resubmit:{base}:{digest}"


class OrphanSweepKindTests(unittest.TestCase):
    def test_codex_only_row_is_skipped_when_no_codex_worker_is_online(self):
        _, resubmitted, _ = _run([_failed()], [{"kind": "zeroclaw", "host": "hydra"}])
        self.assertEqual(resubmitted, [])

    def test_codex_only_row_is_requeued_when_a_codex_worker_is_online(self):
        _, resubmitted, _ = _run([_failed()], [{"kind": "codex", "host": "achilles"}])
        self.assertEqual(resubmitted, ["build:codex"])

    def test_row_without_eligible_kinds_is_still_requeued(self):
        _, resubmitted, _ = _run([_failed(kind="build:zeroclaw", kinds=None)], [])
        self.assertEqual(resubmitted, ["build:zeroclaw"])

    def test_corrupt_eligible_kinds_is_not_requeued(self):
        row = _failed()
        row["eligible_kinds"] = "[codex"
        _, resubmitted, _ = _run([row], [{"kind": "codex"}])
        self.assertEqual(resubmitted, [])

    def test_non_list_eligible_kinds_is_not_requeued(self):
        row = _failed()
        row["eligible_kinds"] = 5
        _, resubmitted, _ = _run([row], [{"kind": "codex"}])
        self.assertEqual(resubmitted, [])

    def test_unknown_agent_list_does_not_block_the_sweep(self):
        _, resubmitted, _ = _run([_failed()], None)
        self.assertEqual(resubmitted, ["build:codex"])

    def test_requeued_result_marks_the_base_kind(self):
        _, _, state = _run(
            [_failed(kind="build:zeroclaw", kinds=("zeroclaw",))],
            [{"kind": "zeroclaw"}],
            resubmit_text="requeued 1 jobs of build:zeroclaw (api=200)",
        )
        self.assertIn(_set_key("f1"), state.marks)

    def test_refused_requeue_leaves_the_base_kind_unmarked(self):
        _, _, state = _run(
            [_failed(kind="build:zeroclaw", kinds=("zeroclaw",))],
            [{"kind": "zeroclaw"}],
            resubmit_text="requeued 1 jobs of build:zeroclaw (api=500)",
        )
        self.assertNotIn(_set_key("f1"), state.marks)

    def test_a_mark_older_than_the_window_does_not_suppress_new_failures(self):
        """Regression 2026-10-29: last_mark() never expires, so a stale mark blocked the kind forever."""
        state = _FakeState()
        state.marks[_set_key("f1")] = state.now - (doctor.ORPHAN_RESUBMIT_TTL_SEC + 60)
        _, resubmitted, _ = _run(
            [_failed(kind="build:zeroclaw", kinds=("zeroclaw",))],
            [{"kind": "zeroclaw"}],
            resubmit_text="requeued 1 jobs of build:zeroclaw (api=200)",
            state=state,
        )
        self.assertEqual(resubmitted, ["build:zeroclaw"])

    def test_a_fresh_mark_still_throttles_the_same_kind(self):
        state = _FakeState()
        state.marks[_set_key("f1")] = state.now - 30
        _, resubmitted, _ = _run(
            [_failed(kind="build:zeroclaw", kinds=("zeroclaw",))], [{"kind": "zeroclaw"}], state=state
        )
        self.assertEqual(resubmitted, [])

    def test_a_new_failure_of_the_same_kind_is_acted_on_inside_the_window(self):
        """2026-10-29: one requeue used to hold back every other failure of the kind for 10 minutes."""
        state = _FakeState()
        state.marks[_set_key("f1")] = state.now - 30  # f1 was just handled
        rows = [
            _failed(jid="f1", kind="build:zeroclaw", kinds=("zeroclaw",)),
            _failed(jid="f2", kind="build:zeroclaw", kinds=("zeroclaw",)),
        ]
        _, resubmitted, state = _run(rows, [{"kind": "zeroclaw"}], state=state)
        self.assertEqual(resubmitted, ["build:zeroclaw"])
        self.assertIn(_set_key("f1", "f2"), state.marks)


if __name__ == "__main__":
    unittest.main()
