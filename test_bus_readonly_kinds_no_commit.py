"""Read-only kinds (review:/audit:/investigate:/probe:) need no commit to complete.

Determinism-gaps class 2 (2026-10-27): ``kind_requires_commit`` sent every job
with a repo through the commit-evidence gate, so a read-only review that
legitimately produced no commit (its deliverable is a structured verdict) was
rejected as ``needs-review`` with ``completion_evidence_missing`` — 17 rows in
24 h (``codex_delivery_failed`` x12, "produced no workspace edits" x5).

The fix:

* :data:`READONLY_KINDS` — a documented, prefix-only allowlist
  (``review:``, ``audit:``, ``investigate:``, ``probe:``), shared by the
  doctor and these tests via import, never duplicated.
* ``kind_requires_commit`` exempts those kinds at a TOKEN BOUNDARY:
  case-insensitive, prefix-only — ``Reviewing:`` does NOT match, and a code
  kind like ``build:x`` is never touched.
* the completion proof for those kinds is EITHER the existing git proof (a
  read-only run that did commit) OR the read-only proof: ``result.verdict`` a
  dict with a non-empty ``verdict`` string, plus a non-empty
  ``result.deliverable`` string. An empty result is rejected on BOTH paths.
"""

from __future__ import annotations

import unittest

from hive.bus.domain.jobs import (
    READONLY_KINDS,
    kind_requires_commit,
    valid_completion_proof,
    valid_readonly_completion_proof,
    valid_readonly_proof,
)

# A full, self-consistent git proof: a real run with one commit.
_HEAD = "a" * 40
_GIT_PROOF = {
    "head_sha": _HEAD,
    "commits": [_HEAD],
    "files_changed": ["hive/bus/domain/jobs.py"],
    "worktree_clean": True,
}

# The read-only proof shape: a structured verdict + a non-empty deliverable.
_READONLY_PROOF = {
    "verdict": {"verdict": "pass", "confidence": 0.9, "findings": []},
    "deliverable": "Review complete: parity holds, no drift found.",
}


class ReadonlyKindAllowlistTests(unittest.TestCase):
    def test_allowlist_is_the_documented_prefix_tuple(self):
        # Shared by the doctor and the tests via import; pin its exact shape.
        self.assertEqual(READONLY_KINDS, ("review:", "audit:", "investigate:", "probe:"))

    def test_prefix_kinds_do_not_require_commit(self):
        for p in READONLY_KINDS:
            for kind in (p + "anything", p):  # "review:x" and bare "review:"
                self.assertFalse(kind_requires_commit(kind, "graeae-hive"), f"{kind} is read-only")
                self.assertFalse(kind_requires_commit(kind, None), f"{kind} is read-only (no repo)")

    def test_case_insensitive(self):
        for kind in ("REVIEW:usage-ledger-parity", "Review:audit", "AUDIT:x", "Investigate:y", "PROBE:z", "rEvIeW:"):
            self.assertFalse(kind_requires_commit(kind, "graeae-hive"), f"{kind} is read-only (case)")
            self.assertFalse(kind_requires_commit(kind, None), f"{kind} is read-only (case, no repo)")

    def test_prefix_only_not_lookalike(self):
        """``reviewing:``/``reviewer-x`` do NOT match ``review:`` — they stay gated.

        The prefix is the whole ``"review:"``; a kind only matches when the
        kind STARTS WITH that exact 7-character prefix, so the character after
        the ``r`` must be ``:``.
        """
        for kind in (
            "reviewing:ledger",
            "reviewer-migration",
            "auditor:tool",
            "investigates:x",
            "prober:thing",
        ):
            self.assertTrue(kind_requires_commit(kind, "graeae-hive"), f"{kind} is a code kind")

    def test_substring_prefix_is_a_match(self):
        # Prefix-only means STARTS WITH the prefix: "audit:x" matches "audit:".
        for kind in ("audit:graeae-hive", "investigate:bus-gate", "probe:host-latency"):
            self.assertFalse(kind_requires_commit(kind, "graeae-hive"), f"{kind} is read-only")


class CodeKindsUnchangedTests(unittest.TestCase):
    def test_code_kinds_still_require_commit(self):
        for kind in ("build:zeroclaw", "feat:knemon", "fix:auto-restore", "code", "riskyeats"):
            self.assertTrue(kind_requires_commit(kind, "graeae-hive"), f"{kind} is a code kind")
        # The gate is untouched for code kinds; note a no-repo ``build:`` job
        # was never commit-gated either (repo presence drives the generic path),
        # and that is unchanged by this fix.
        self.assertTrue(kind_requires_commit("code", None))
        self.assertTrue(kind_requires_commit("riskyeats", None))

    def test_generic_kind_with_repo_still_gated(self):
        self.assertTrue(kind_requires_commit("some-generic-kind", "some-repo"))

    def test_generic_kind_no_repo_not_gated(self):
        self.assertFalse(kind_requires_commit("some-generic-kind", None))


class ReadonlyCompletionProofTests(unittest.TestCase):
    def test_review_job_completes_with_readonly_proof_and_no_commits(self):
        # No git evidence at all — the verdict IS the deliverable.
        self.assertFalse(valid_completion_proof(_READONLY_PROOF), "verdict is not a git proof")
        self.assertTrue(valid_readonly_completion_proof("review:graeae-hive-usage-ledger-parity", _READONLY_PROOF))

    def test_build_job_with_same_proof_is_still_rejected(self):
        # kind_requires_commit stays True for build:, and the read-only proof
        # is kind-gated: the helper itself refuses it for a code kind.
        self.assertTrue(kind_requires_commit("build:zeroclaw", "graeae-hive"))
        self.assertFalse(valid_readonly_completion_proof("build:zeroclaw", _READONLY_PROOF))
        self.assertFalse(valid_completion_proof(_READONLY_PROOF))

    def test_review_job_with_empty_result_is_rejected(self):
        self.assertFalse(valid_readonly_completion_proof("review:whatever", {}))
        self.assertFalse(valid_readonly_completion_proof("review:whatever", None))
        self.assertFalse(valid_readonly_proof({}))

    def test_empty_verdict_or_deliverable_is_rejected(self):
        self.assertFalse(valid_readonly_proof({"verdict": {"verdict": ""}, "deliverable": "x"}))
        self.assertFalse(valid_readonly_proof({"verdict": {"verdict": "   "}, "deliverable": "x"}))
        self.assertFalse(valid_readonly_proof({"verdict": {"verdict": "pass"}, "deliverable": ""}))
        self.assertFalse(valid_readonly_proof({"verdict": {"verdict": "pass"}, "deliverable": "  "}))
        self.assertFalse(valid_readonly_proof({"verdict": "pass", "deliverable": "x"}))  # not a dict
        self.assertFalse(valid_readonly_proof({"verdict": {"confidence": 1.0}, "deliverable": "x"}))  # no verdict str
        self.assertFalse(valid_readonly_proof({"verdict": {"verdict": 1}, "deliverable": "x"}))  # not a str

    def test_review_job_with_git_proof_still_passes(self):
        # A read-only run that also committed is still valid via the git path.
        self.assertTrue(valid_readonly_completion_proof("review:ledger", _GIT_PROOF))
        self.assertTrue(valid_completion_proof(_GIT_PROOF))

    def test_incomplete_git_proof_is_not_a_git_path(self):
        # Files changed but no commit — neither path accepts it.
        broken = {"head_sha": _HEAD, "commits": [], "files_changed": ["a.py"], "worktree_clean": True}
        self.assertFalse(valid_readonly_completion_proof("review:x", broken))
        self.assertFalse(valid_readonly_completion_proof("audit:x", broken))

    def test_empty_verdict_dict_rejected(self):
        self.assertFalse(valid_readonly_proof({"verdict": {}, "deliverable": "x"}))

    def test_verdict_dict_missing_verdict_key_does_not_raise(self):
        # Reviewer finding: a verdict dict without the inner "verdict" key
        # (malformed worker payload) must fail closed, not raise KeyError.
        self.assertFalse(valid_readonly_proof({"verdict": {"confidence": 0.9}, "deliverable": "x"}))
        self.assertFalse(valid_readonly_proof({"verdict": {"finding": "pass"}, "deliverable": "x"}))
        self.assertFalse(valid_readonly_proof({"verdict": {"verdict": None}, "deliverable": "x"}))
        self.assertFalse(valid_readonly_completion_proof("review:x", {"verdict": {"confidence": 0.9}, "deliverable": "x"}))


if __name__ == "__main__":
    unittest.main()
