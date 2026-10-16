"""Reaper orphan-claim selection — the queue-health TTL logic."""

from __future__ import annotations

import unittest

from hive.bus.domain.reaper import select_orphan_claims

NOW = 1_000_000.0


def cand(jid, kind="feat:x", claimer="urn:a", c_at=NOW - 5, u_at=NOW - 5, lease=None):
    """Build a candidate tuple (job_id, kind, claimed_by, claimed_at, last_update_at, lease)."""
    return (jid, kind, claimer, c_at, u_at, lease)


class SelectOrphanClaimsTests(unittest.TestCase):
    def _sel(self, cands, **kw):
        base = dict(now=NOW, stale_after=1800.0, hard_max_age=14400.0)
        base.update(kw)
        return select_orphan_claims(cands, **base)

    def test_fresh_claim_not_reaped(self):
        self.assertEqual(self._sel([cand("j1")]), [])

    def test_stale_by_last_update_reaped(self):
        # last_update 31 min ago > 30 min ttl
        self.assertEqual(self._sel([cand("j1", u_at=NOW - 1860)]), [("j1", "urn:a")])

    def test_progress_keeps_reaper_at_bay(self):
        # old claim, but last_update is fresh -> NOT reaped (still PATCHing)
        self.assertEqual(self._sel([cand("j1", c_at=NOW - 10_000, u_at=NOW - 10)]), [])

    def test_expired_lease_reaped_immediately(self):
        self.assertEqual(self._sel([cand("j1", u_at=NOW - 1, lease=NOW - 1)]), [("j1", "urn:a")])

    def test_hard_cap_unrefreshable(self):
        # lease + last_update fresh, but claimed 5h ago (> 4h hard cap)
        got = self._sel([cand("j1", c_at=NOW - 18000, u_at=NOW - 1, lease=NOW + 999)])
        self.assertEqual(got, [("j1", "urn:a")])

    def test_kind_specific_ttl_extends(self):
        # 31 min stale, but 'investigation' has a 1h ttl -> not reaped.
        # The override key is the BASE kind; the real kind carries a suffix.
        c = cand("j1", kind="investigation:image-build-preflight", u_at=NOW - 1860)
        self.assertEqual(self._sel([c], stale_by_kind={"investigation": 3600.0}), [])
        # same candidate with the default ttl -> reaped
        self.assertEqual(self._sel([c]), [("j1", "urn:a")])

    def test_hard_cap_kind_override_base_match(self):
        # 'migration' raises the hard cap to 6h; a 5h-old claim is NOT reaped
        c = cand("j1", kind="migration:fleet", c_at=NOW - 18000, u_at=NOW - 1)
        self.assertEqual(self._sel([c], hard_by_kind={"migration": 21600.0}), [])

    def test_phantom_claim_null_claimed_at_reaped(self):
        # claimed_at NULL (corrupt) -> latest=0 -> stale -> reaped
        self.assertEqual(self._sel([cand("j1", c_at=None, u_at=None)]), [("j1", "urn:a")])

    def test_mixed_batch(self):
        cands = [
            cand("fresh"),
            cand("stale", u_at=NOW - 2000),
            cand("leased", lease=NOW - 5),
        ]
        got = self._sel(cands)
        self.assertEqual(sorted(j for j, _ in got), ["leased", "stale"])


if __name__ == "__main__":
    unittest.main()
