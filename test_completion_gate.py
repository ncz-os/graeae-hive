"""Completion gate — read-only kinds must not be held to commit evidence.

Regression 2026-10-16: a ``review:...`` job that set ``repo`` (to clone the code
under review) was forced through the commit-evidence gate, so a clean, verified,
commitless run was rejected as ``needs-review`` with
``failure_reason=completion_evidence_missing``. ``commit_required`` keyed off
``bool(job_repo)`` alone.
"""

from __future__ import annotations

import unittest

from hive.bus.domain.jobs import kind_requires_commit


class CompletionGateTests(unittest.TestCase):
    def test_readonly_kinds_with_repo_do_not_require_commit(self):
        for kind in (
            "review:graeae-hive-usage-ledger-parity",
            "investigation:magnetar-installer-pool-online",
            "analysis:something",
            "triage:build:zeroclaw",
            "docs:whatever",
            "research:topic",
            "design:architecture",
            "ops:check",
            "diag:host",
        ):
            self.assertFalse(kind_requires_commit(kind, "graeae-hive"), f"{kind} is read-only")

    def test_code_kinds_still_require_commit(self):
        for kind in ("feat:knemon", "fix:auto-restore-policy", "mnemos:foo", "ncz-os-thing", "riskyeats:site"):
            self.assertTrue(kind_requires_commit(kind, None))
            self.assertTrue(kind_requires_commit(kind, "any"))

    def test_bare_code_kinds(self):
        self.assertTrue(kind_requires_commit("code", None))
        self.assertTrue(kind_requires_commit("riskyeats", None))

    def test_repo_presence_still_requires_commit_for_generic_kinds(self):
        self.assertTrue(kind_requires_commit("some-generic-kind", "some-repo"))

    def test_no_repo_no_commit_kind(self):
        self.assertFalse(kind_requires_commit("some-generic-kind", None))
        self.assertFalse(kind_requires_commit("", None))


if __name__ == "__main__":
    unittest.main()
