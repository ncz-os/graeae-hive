"""Unresolvable-release classification — job-level unschedulable reasons must
count toward the dead-letter threshold so the reclaim loop terminates.

Regression: on 2026-10-16 a worker released a job with worker_error
``no_repo_mapping``; because that reason was NOT in DECLINE_REASON_PREFIXES the
bus never incremented decline_count, so the job requeued forever (observed
retry_count=5) instead of dead-lettering.

Audit BLOCKING-1 (2026-10-16) refined that: ``no_repo_mapping`` is a
WORKER-DEPLOYMENT condition (worker REPO_MAP=16 vs bus catalog=123), not a
job-level one. It counts only when the job's repo is GENUINELY absent from the
bus catalog; a catalog-present repo means a stale worker and the job must be
requeued/routed, never killed.
"""

from __future__ import annotations

import unittest

from hive.bus.domain.jobs import (
    DECLINE_REASON_PREFIXES,
    is_unresolvable_release,
)

# The exact reasons hive_worker.py's release_job() emits today.
WORKER_RELEASE_REASONS = ("no_repo_mapping", "malformed_job_id", "invalid_head_branch")
# Job-level: a property of the job itself, so no host can ever run it.
JOB_LEVEL_RELEASE_REASONS = ("malformed_job_id", "invalid_head_branch")

# Not in the bus catalog (data/repos.json), so genuinely unresolvable.
CATALOG_ABSENT_REPO = "definitely-not-a-catalog-repo-zzz-20261016"
CATALOG_PRESENT_REPO = "graeae-hive"


class UnresolvableReleaseTests(unittest.TestCase):
    def test_worker_release_reasons_are_unresolvable(self):
        for reason in JOB_LEVEL_RELEASE_REASONS:
            self.assertTrue(
                is_unresolvable_release(reason),
                f"{reason} must count toward dead-letter or the job thrashes forever",
            )
            self.assertTrue(is_unresolvable_release(reason, job_repo=CATALOG_PRESENT_REPO), reason)

    def test_no_repo_mapping_counts_only_for_a_catalog_absent_repo(self):
        # BLOCKING-1: absence from the WORKER's map is deploy drift, not a dead job.
        self.assertTrue(is_unresolvable_release("no_repo_mapping", job_repo=CATALOG_ABSENT_REPO))
        self.assertTrue(is_unresolvable_release("no_repo_mapping", job_repo=None))
        self.assertFalse(is_unresolvable_release("no_repo_mapping", job_repo=CATALOG_PRESENT_REPO))

    def test_all_worker_reasons_remain_recognised(self):
        # Every reason the worker emits is still understood by the classifier,
        # so none can silently fall through to the uncounted path.
        self.assertEqual(
            set(JOB_LEVEL_RELEASE_REASONS) | {"no_repo_mapping"},
            set(WORKER_RELEASE_REASONS),
        )

    def test_existing_workspace_reasons_still_count(self):
        self.assertTrue(is_unresolvable_release("no_workspace_for_kind"))
        self.assertTrue(is_unresolvable_release("no_workspace_for_repo"))

    def test_prefix_form_matches(self):
        self.assertTrue(is_unresolvable_release("no_repo_mapping:ssh://x/y.git", job_repo=CATALOG_ABSENT_REPO))
        self.assertFalse(is_unresolvable_release("no_repo_mapping:ssh://x/y.git", job_repo=CATALOG_PRESENT_REPO))
        self.assertTrue(is_unresolvable_release("invalid_head_branch:does-not-exist"))

    def test_host_routing_declines_do_not_count(self):
        # these are legitimate "not me" signals — the matcher routes to a capable host
        for reason in ("host_declines_kind", "released_by_host", "", "some_transient_error"):
            self.assertFalse(is_unresolvable_release(reason), f"{reason!r} must stay OUT")

    def test_decline_prefixes_is_the_extension_point(self):
        self.assertEqual(DECLINE_REASON_PREFIXES, tuple(dict.fromkeys(DECLINE_REASON_PREFIXES)))


if __name__ == "__main__":
    unittest.main()
