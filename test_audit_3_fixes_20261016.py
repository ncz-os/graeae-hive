"""Adversarial audit of the three 2026-10-16 hive fixes — executable findings.

Companion to docs/audit-3-fixes-20261016.md. Tests assert the DESIRED behaviour
for the findings that have been remediated, and pin the genuinely-correct parts
of each fix so a later change cannot regress them silently.

Remediated in this branch:

  * BLOCKING-1 (fix A) — ``no_repo_mapping`` is a WORKER-DEPLOYMENT condition
    (worker ``REPO_MAP``=16 vs bus catalog=123), not a job-level one. It now
    counts toward the decline/dead-letter threshold ONLY when the job's repo is
    genuinely absent from the bus catalog (``hive.repos``, in-process — no
    hot-path HTTP). A catalog-present repo is requeued/routed, never killed.
    ``malformed_job_id`` / ``invalid_head_branch`` keep counting.

  * BLOCKING-2 (fix B) — ``kind_requires_commit`` matches read-only kinds at a
    TOKEN BOUNDARY: a kind is exempt only when it EQUALS a bare prefix or starts
    with ``<prefix>:``. Code kinds wearing a read-only name
    (``analysis-pipeline-fix``, ``reviewer-migration``,
    ``design-system-update``, ...) are GATED again.

Run: /usr/bin/python3 -m pytest -q test_audit_3_fixes_20261016.py
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("HIVE_BACKEND", "sqlite")
os.environ["AGENT_BUS_DB"] = os.path.join(tempfile.mkdtemp(prefix="hive-audit3-"), "t.db")

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus.deps import hive_repo  # noqa: E402
from hive.bus.domain.jobs import (  # noqa: E402
    MAX_DECLINE_REQUEUES,
    NO_REPO_MAPPING_DECLINE_BUDGET,
    is_readonly_kind,
    is_unresolvable_release,
    kind_requires_commit,
    repo_in_catalog,
)
from hive.bus.domain.workspace import ADMISSION_NONCOMMIT_PREFIXES  # noqa: E402

# Catalog-present / catalog-absent fixtures for the BLOCKING-1 assertions.
CATALOG_PRESENT_REPO = "graeae-hive"
CATALOG_ABSENT_REPO = "definitely-not-a-catalog-repo-zzz-20261016"


# Code job names that merely BEGIN with a read-only family word. These are the
# kinds a submitter could plausibly choose for real code work.
CODE_KIND_READONLY_LOOKALIKE = (
    "analysis-pipeline-fix",
    "reviewer-migration",
    "design-system-update",
    "research-notes",
    "triage-build",
    "investigation-x",
    "architecture-doc",
)


# ───────────────────────────── Fix A: unresolvable release ─────────────────────────────
def _resolve_fuzzy(query: str) -> list:
    from hive.repos import resolve

    return resolve(query, limit=1)


class UnresolvableReleaseAudit(unittest.TestCase):
    """fix/hive-unresolvable-release-20261016."""

    def test_fixed_job_level_reasons_do_count(self):
        # CORRECT part of fix A — these two really are properties of the job alone.
        for reason in ("malformed_job_id", "invalid_head_branch"):
            self.assertTrue(is_unresolvable_release(reason), f"{reason} is job-level unresolvable")

    def test_host_routing_declines_still_excluded(self):
        # CORRECT: counting these would false-dead-letter a job another host can run.
        for reason in ("host_declines_kind", "released_by_host", "some_transient_error"):
            self.assertFalse(is_unresolvable_release(reason))

    def test_PREFIX_DETAIL_form_counts(self):
        self.assertTrue(is_unresolvable_release("invalid_head_branch:does-not-exist"))

    def test_BLOCKING1_catalog_present_repo_is_NOT_counted(self):
        """BLOCKING-1: a catalog-present repo + no_repo_mapping must NOT count.

        The deployed worker resolves 16 repos; the bus catalog has 123. A job on
        one of the 107 other repos gets ``no_repo_mapping`` from a STALE worker
        (deploy drift) -- it must be requeued/routed, never dead-lettered. Before
        the fix it was counted and killed at decline 3 (~70 s).
        """
        repo = CATALOG_PRESENT_REPO
        self.assertTrue(repo_in_catalog(repo), f"{repo!r} must be a catalog repo")
        self.assertFalse(is_unresolvable_release("no_repo_mapping", job_repo=repo))
        self.assertFalse(is_unresolvable_release("no_repo_mapping:ssh://x/y.git", job_repo=repo))

    def test_BLOCKING1_catalog_absent_repo_IS_counted(self):
        """BLOCKING-1: a genuinely absent repo must still be terminable.

        Otherwise the job requeues forever (the original thrash). It counts on
        the larger ``NO_REPO_MAPPING_DECLINE_BUDGET`` -- still bounded/visible.
        """
        repo = CATALOG_ABSENT_REPO
        self.assertFalse(repo_in_catalog(repo), f"{repo!r} must NOT be a catalog repo")
        self.assertTrue(is_unresolvable_release("no_repo_mapping", job_repo=repo))
        self.assertTrue(is_unresolvable_release("no_repo_mapping:ssh://x/y.git", job_repo=repo))
        # a repo-less job can never be resolved by a worker: same as absent
        self.assertTrue(is_unresolvable_release("no_repo_mapping", job_repo=None))
        self.assertTrue(is_unresolvable_release("no_repo_mapping", job_repo=""))
        self.assertGreater(NO_REPO_MAPPING_DECLINE_BUDGET, 3)

    def test_BLOCKING1_malformed_and_bad_branch_still_count(self):
        """The other two worker reasons ARE job-level: keep counting them.

        Counted with no repo, a catalog repo, or an absent repo -- no qualification.
        """
        for reason in ("malformed_job_id", "invalid_head_branch", "invalid_head_branch:nope"):
            self.assertTrue(is_unresolvable_release(reason), reason)
            self.assertTrue(is_unresolvable_release(reason, job_repo=CATALOG_PRESENT_REPO), reason)
            self.assertTrue(is_unresolvable_release(reason, job_repo=CATALOG_ABSENT_REPO), reason)
            self.assertTrue(is_unresolvable_release(reason, job_repo=None), reason)

    def test_BLOCKING1_substring_only_repo_IS_counted(self):
        """Follow-up defect: the catalog check fell back to fuzzy resolve().

        resolve() ranks prefix AND substring hits, so ``"raeae-hive"`` matched
        the catalog entry ``graeae-hive`` and the repo looked catalog-present ->
        no_repo_mapping was never counted -> the job requeued forever. Exact
        matching only.
        """
        # NOTE: 'hive' alone is an exact ALIAS of graeae-hive-mind in the live
        # catalog, so it legitimately resolves. The substring-only probe is a
        # proper substring that is not any id/alias, e.g. 'raeae-hive'.
        repo = "raeae-hive"
        self.assertTrue(
            repo_in_catalog(CATALOG_PRESENT_REPO),
            f"{CATALOG_PRESENT_REPO!r} must be a catalog repo",
        )
        self.assertTrue(
            _resolve_fuzzy(repo),
            f"sanity: {repo!r} IS a fuzzy substring hit, so this is the regression case",
        )
        self.assertFalse(repo_in_catalog(repo), f"{repo!r} must not count as catalog-present")
        self.assertTrue(is_unresolvable_release("no_repo_mapping", job_repo=repo))
        self.assertTrue(is_unresolvable_release("no_repo_mapping:ssh://x/y.git", job_repo=repo))

    def test_BLOCKING1_exact_id_and_alias_are_NOT_counted(self):
        """Exact id and exact (case-insensitive) alias still count as present."""
        from hive.repos import find_exact

        entry = find_exact(CATALOG_PRESENT_REPO)
        self.assertIsNotNone(entry)
        for repo in (CATALOG_PRESENT_REPO, CATALOG_PRESENT_REPO.upper()):
            self.assertTrue(repo_in_catalog(repo), f"{repo!r} is the exact id")
            self.assertFalse(is_unresolvable_release("no_repo_mapping", job_repo=repo))
        alias = (entry.get("aliases") or [None])[0]
        if alias:
            for repo in (alias, alias.upper()):
                self.assertTrue(repo_in_catalog(repo), f"{repo!r} is an exact alias")
                self.assertFalse(is_unresolvable_release("no_repo_mapping", job_repo=repo))
        # and nothing resolves fuzzily in repo_in_catalog any more
        src = (Path(__file__).parent / "hive" / "bus" / "domain" / "jobs.py").read_text()
        fn = src.split("def repo_in_catalog", 1)[1].split("\ndef ", 1)[0]
        self.assertNotIn("_repo_catalog.resolve", fn, "repo_in_catalog must not fall back to the fuzzy ranker")

    def test_BLOCKING1_workspace_reason_detail_form_still_counts(self):
        """Regression: only the bare workspace reason counted after 23446a7."""
        for reason in ("no_workspace_for_kind:x", "no_workspace_for_kind:code",
                       "no_workspace_for_repo:graeae-hive", "no_workspace_for_kind",
                       "no_workspace_for_repo"):
            self.assertTrue(is_unresolvable_release(reason), reason)

    def test_BLOCKING1_catalog_lookup_is_in_process(self):
        """No hot-path HTTP: the catalog comes from hive.repos (a local file)."""
        src = (Path(__file__).parent / "hive" / "bus" / "domain" / "jobs.py").read_text()
        fn = src.split("def repo_in_catalog", 1)[1].split("\ndef ", 1)[0]
        for banned in ("httpx", "urllib", "requests", "http://", "https://"):
            self.assertNotIn(banned, fn, f"catalog lookup must not use {banned}")
        self.assertIn("hive import repos", fn)


class UnresolvableReleaseEndToEndAudit(unittest.IsolatedAsyncioTestCase):
    """BLOCKING-1 through the real PATCH release path (claimed -> queued).

    The unit tests above pin the classifier; this pins the actual dead-letter
    decision, which is where the ~70s kill happened.
    """

    async def asyncSetUp(self):
        await hive_repo().init()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t")
        self.worker = (
            await self.client.post(
                "/v1/agents/register",
                json={"kind": "zeroclaw", "host": "audit3", "runtime": "zeroclaw",
                      "capabilities": ["code"], "cost_tier": "C", "auth_method": "api"},
            )
        ).json()["urn"]
        self.orchestrator = (
            await self.client.post(
                "/v1/agents/register",
                json={"kind": "opencode", "host": "audit3", "runtime": "opencode",
                      "capabilities": ["code"], "cost_tier": "C", "auth_method": "api"},
            )
        ).json()["urn"]

    async def asyncTearDown(self):
        await self.client.aclose()

    async def _submit(self, repo: str) -> str:
        r = await self.client.post(
            "/v1/jobs",
            json={
                "submitter_urn": self.orchestrator,
                "kind": "fix:x",
                "description": f"audit3 blocking-1 {repo}",
                "repo": repo,
                "eligible_hosts": ["audit3"],
                "required_autonomy": "full",
            },
        )
        self.assertLess(r.status_code, 300, r.text)
        return r.json()["id"]

    async def _row(self, job_id: str):
        async with hive_repo().connection() as tx:
            cur = await tx.execute("SELECT status, decline_count FROM jobs WHERE id=?", (job_id,))
            return await cur.fetchone()

    async def _release(self, job_id: str) -> None:
        """Claim then release with worker_error=no_repo_mapping (the worker's path)."""
        async with hive_repo().connection() as tx:  # skip the release backoff
            await hive_repo().patch_job_status(
                tx, job_id, sets={"retry_backoff_until": 0}, expected_status="queued"
            )
            await tx.commit()
        claimed = await self.client.post(f"/v1/jobs/{job_id}/claim?by={self.worker}")
        self.assertEqual(claimed.status_code, 200, claimed.text)
        released = await self.client.patch(
            f"/v1/jobs/{job_id}",
            json={
                "status": "queued",
                "claimed_by": self.worker,
                "result": {"worker_error": "no_repo_mapping", "via": "hive_worker"},
            },
        )
        self.assertEqual(released.status_code, 200, released.text)

    async def test_BLOCKING1_catalog_present_job_survives_repeated_releases(self):
        """A stale worker naming a real catalog repo must NEVER dead-letter it."""
        job_id = await self._submit(CATALOG_PRESENT_REPO)
        for _ in range(NO_REPO_MAPPING_DECLINE_BUDGET + 3):
            await self._release(job_id)
            status, decline_count = await self._row(job_id)
            self.assertEqual(status, "queued", "legitimate job was dead-lettered")
            self.assertIn(decline_count, (0, None), "catalog-present release was counted as a decline")

    async def test_BLOCKING1_catalog_absent_job_is_terminated_at_the_larger_budget(self):
        """A genuinely absent repo still terminates -- bounded, just not at 70s."""
        job_id = await self._submit(CATALOG_ABSENT_REPO)
        for _ in range(MAX_DECLINE_REQUEUES):
            await self._release(job_id)
            status, _ = await self._row(job_id)
            self.assertEqual(status, "queued", "absent-repo job killed at the normal threshold")
        for _ in range(NO_REPO_MAPPING_DECLINE_BUDGET):
            status, _ = await self._row(job_id)
            if status == "dead-letter":
                break
            await self._release(job_id)
        status, decline_count = await self._row(job_id)
        self.assertEqual(status, "dead-letter", "absent-repo job requeued forever")
        self.assertEqual(decline_count, NO_REPO_MAPPING_DECLINE_BUDGET)

    async def test_malformed_job_id_still_dead_letters_at_the_normal_threshold(self):
        """Job-level reasons keep the original 3-strike bound."""
        job_id = await self._submit(CATALOG_PRESENT_REPO)
        for _ in range(MAX_DECLINE_REQUEUES):
            async with hive_repo().connection() as tx:
                await hive_repo().patch_job_status(
                    tx, job_id, sets={"retry_backoff_until": 0}, expected_status="queued"
                )
                await tx.commit()
            claimed = await self.client.post(f"/v1/jobs/{job_id}/claim?by={self.worker}")
            self.assertEqual(claimed.status_code, 200, claimed.text)
            r = await self.client.patch(
                f"/v1/jobs/{job_id}",
                json={
                    "status": "queued",
                    "claimed_by": self.worker,
                    "result": {"worker_error": "malformed_job_id", "via": "hive_worker"},
                },
            )
            self.assertEqual(r.status_code, 200, r.text)
        status, decline_count = await self._row(job_id)
        self.assertEqual(status, "dead-letter")
        self.assertEqual(decline_count, MAX_DECLINE_REQUEUES)


# ─────────────────────── Fix B: completion gate / noncommit ───────────────────────
class CompletionGateAudit(unittest.TestCase):
    """fix/hive-completion-gate-noncommit-20261016."""

    def test_readonly_kinds_really_are_exempt(self):
        # CORRECT part of fix B — the regression it targets is genuinely fixed.
        for kind in ("review:x", "analysis:x", "investigation:x", "triage:x", "docs:x"):
            self.assertFalse(kind_requires_commit(kind, "graeae-hive"), kind)

    def test_code_kinds_still_gated(self):
        # CORRECT: the code prefix list is copied verbatim; nothing slipped.
        for kind in ("feat:x", "fix:x", "mnemos:x", "ncz-os-x", "riskyeats:x", "code", "riskyeats"):
            self.assertTrue(kind_requires_commit(kind, "graeae-hive"), kind)
            self.assertTrue(kind_requires_commit(kind, None), kind)

    def test_PREFIX_DELIMITER_gap_is_the_root_cause(self):
        """The bare entries in ADMISSION_NONCOMMIT_PREFIXES have no ':' delimiter."""
        bare = [p for p in ADMISSION_NONCOMMIT_PREFIXES if not p.endswith(":")]
        self.assertTrue(bare, "expected bare (undelimited) read-only prefixes")
        for p in bare:
            self.assertNotIn(":", p)

    def test_BLOCKING2_code_job_lookalike_kind_is_gated(self):
        """BLOCKING-2: a code kind wearing a read-only name must NOT skip the gate.

        A plain startswith() let 'analysis-pipeline-fix' match the 'analysis'
        prefix and be exempt EVEN WITH A REPO SET, so such a job PATCHed to
        'done' with an empty result (no head_sha/commits/files_changed). The
        exemption now requires a token boundary: the kind must EQUAL the bare
        prefix or be followed by ':'.
        """
        for kind in CODE_KIND_READONLY_LOOKALIKE:
            self.assertTrue(
                kind_requires_commit(kind, "graeae-hive"),
                f"BLOCKING-2: code job {kind!r} with a repo set skips the commit-evidence gate",
            )
            self.assertTrue(
                kind_requires_commit(kind, "any"),
                f"BLOCKING-2: code job {kind!r} with any repo skips the commit-evidence gate",
            )
            # A non-':' delimiter (or any other char) after the prefix -> not exempt.
            self.assertFalse(
                is_readonly_kind(kind),
                f"BLOCKING-2: {kind!r} must not be classified read-only",
            )

    def test_BLOCKING2_legitimate_prefix_topic_forms_stay_exempt(self):
        """'<prefix>:topic' and the bare prefix itself remain exempt."""
        for kind in (
            "review:topic",
            "analysis:topic",
            "investigation:topic",
            "triage:topic",
            "design:topic",
            "research:topic",
            "architecture:topic",
            "docs:topic",
            "track:topic",
            "ops:topic",
            "diag:topic",
            "ping:topic",
            "review",
            "analysis",
            "investigation",
            "triage",
            "design",
            "research",
            "architecture",
        ):
            self.assertTrue(is_readonly_kind(kind), f"{kind} must be read-only")
            self.assertFalse(kind_requires_commit(kind, "graeae-hive"), kind)
            self.assertFalse(kind_requires_commit(kind, None), kind)

    def test_BLOCKING2_repo_less_lookalike_stays_exempt(self):
        """No repo and not a code prefix -> exempt, unchanged from before."""
        for kind in CODE_KIND_READONLY_LOOKALIKE:
            self.assertFalse(kind_requires_commit(kind, None), kind)

    def test_BLOCKING2_gate_boundary_characters(self):
        """Only ':' (or end-of-string) may follow a bare read-only prefix."""
        self.assertTrue(is_readonly_kind("review:"))
        self.assertTrue(is_readonly_kind("review"))
        for kind in ("review/", "review.x", "review_x", "reviewer", "reviews", "designs", "triage-"):
            self.assertFalse(is_readonly_kind(kind), kind)
            self.assertTrue(kind_requires_commit(kind, "graeae-hive"), kind)


# ─────────────────────────── Fix C: unknown agent 409 ───────────────────────────
class UnknownAgent409Audit(unittest.IsolatedAsyncioTestCase):
    """fix/hive-unknown-agent-409-20261016."""

    async def asyncSetUp(self):
        await hive_repo().init()
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t"
        )
        self.ghost = "urn:agent:doctor:audit3:00000000-0000-0000-0000-000000000000"

    async def asyncTearDown(self):
        await self.client.aclose()

    async def test_unknown_agent_is_409_on_heartbeat(self):
        # CORRECT part of fix C — and the reason BLOCKING-3 exists.
        r = await self.client.post("/v1/agents/heartbeat", json={"urn": self.ghost})
        self.assertEqual(r.status_code, 409, r.text)

    async def test_unknown_agent_is_409_on_dequeue(self):
        r = await self.client.post(f"/v1/jobs/next?agent_urn={self.ghost}")
        self.assertEqual(r.status_code, 409, r.text)

    async def test_409_does_not_mask_the_status_error(self):
        # CORRECT: an agent that EXISTS but is not active must still report its
        # own distinct 409 — the change must not swallow the real error.
        r = await self.client.post(
            "/v1/agents/register",
            json={"kind": "zeroclaw", "host": "studio", "runtime": "zeroclaw",
                  "capabilities": ["code"], "cost_tier": "C", "auth_method": "api"},
        )
        urn = r.json()["urn"]
        # A drain/status change is applied via heartbeat (there is no PATCH
        # /v1/agents/{urn} route); 'stale' is a valid non-active status.
        await self.client.post("/v1/agents/heartbeat", json={"urn": urn, "status": "stale"})
        cl = await self.client.post(f"/v1/jobs/next?agent_urn={urn}")
        self.assertEqual(cl.status_code, 409, cl.text)
        self.assertIn("agent status", cl.text, "must name the status error, not 'not registered'")

    async def test_BLOCKING3_doctor_heartbeat_selfheals_on_409(self):
        """BLOCKING-3 (fixed): the doctor re-registered only on 404, but the bus
        returns 409 for an unknown agent URN, so its heartbeat spun forever.
        The doctor now re-registers on both codes (test_doctor_reregister.py
        covers the behaviour)."""
        src = (Path(__file__).parent / "hive" / "workers" / "doctor.py").read_text()
        self.assertIn("REREGISTER_CODES = (404, 409)", src)
        heartbeat = src[src.index("def heartbeat(") : src.index("\ndef ", src.index("def heartbeat("))]
        self.assertIn("if code in REREGISTER_CODES:", heartbeat)


if __name__ == "__main__":
    unittest.main()
