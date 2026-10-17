"""Completion gate — read-only kinds must not be held to commit evidence.

Regression 2026-10-16: a ``review:...`` job that set ``repo`` (to clone the code
under review) was forced through the commit-evidence gate, so a clean, verified,
commitless run was rejected as ``needs-review`` with
``failure_reason=completion_evidence_missing``. ``commit_required`` keyed off
``bool(job_repo)`` alone.

Follow-up (audit BLOCKING-2, 2026-10-16): the exemption must match a read-only
kind at a TOKEN BOUNDARY. A bare ``startswith`` made code kinds named
``analysis-pipeline-fix`` / ``reviewer-migration`` / ``design-system-update``
(and friends) exempt even with a repo set. A kind is read-only only when it
EQUALS a bare prefix or starts with ``<prefix>:``.
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
            "architecture:octopus",
            "ops:check",
            "diag:host",
            "track:thing",
            "ping:host",
        ):
            self.assertFalse(kind_requires_commit(kind, "graeae-hive"), f"{kind} is read-only")
            self.assertFalse(kind_requires_commit(kind, None), f"{kind} is read-only (no repo)")

    def test_bare_readonly_kinds_equal_the_prefix(self):
        # A kind that EQUALS the bare prefix is read-only; no delimiter needed.
        for kind in (
            "review",
            "analysis",
            "investigation",
            "triage",
            "design",
            "research",
            "architecture",
            "hive-stats",
            "dream-walker",
        ):
            self.assertFalse(kind_requires_commit(kind, "graeae-hive"), f"{kind} is read-only")
            self.assertFalse(kind_requires_commit(kind, None), f"{kind} is read-only (no repo)")

    def test_readonly_lookalike_code_kinds_are_gated(self):
        """BLOCKING-2: a char after the prefix that is not ':' means NOT exempt."""
        for kind in (
            "analysis-pipeline-fix",
            "reviewer-migration",
            "design-system-update",
            "research-notes",
            "triage-build",
            "investigation-x",
            "architecture-doc",
            "analysispipeline",
            "reviewer",
            "designsystem",
        ):
            self.assertTrue(kind_requires_commit(kind, "graeae-hive"), f"{kind} is a code kind")
            self.assertTrue(kind_requires_commit(kind, "any"), f"{kind} is a code kind")

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
