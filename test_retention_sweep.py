"""Terminal-job retention sweep — unit tests (fake bus, no live DB).

The PURE planner (``hive.workers.retention.plan_retention_sweep``) is tested
directly against in-memory rows. The DOCTOR-side orchestration
(``run_retention_sweep``: fetch → re-read → snapshot → purge → verify) is
tested against a FAKE bus (an in-memory dict of rows standing in for the bus
HTTP API) — nothing here touches a live bus or a real database.

Test names name the protected set explicitly (requirement: active statuses and
non-triage done rows must NEVER be touched).
"""

from __future__ import annotations

import json
import os
import tempfile
import time
import unittest

from hive.workers import retention
from hive.workers import doctor

NOW = time.time()
OLD = NOW - 86400  # a day old — past every TTL below
YOUNG = NOW - 60   # 60 s old — under every TTL below


def _job(jid: str, status: str, kind: str = "zeroclaw", *, ended: float | None = None,
         last_update: float | None = None, claimed_by: str | None = None,
         result=None) -> dict:
    return {
        "id": jid,
        "kind": kind,
        "status": status,
        "claimed_by": claimed_by,
        "ended_at": ended,
        "last_update_at": last_update,
        "result": result,
    }


class RetentionPlannerTests(unittest.TestCase):
    """Pure planner — exact predicates, ordering, protected set, invariant."""

    def test_25_dead_rows_oldest_first_leaves_at_most_max_dead(self):
        """25 young dead rows (all under TTL) → the cap forces exactly the 15
        oldest out (``cap-dead``), leaving exactly 10 dead rows — the
        invariant ``dead <= max_dead`` holds.
        """
        jobs = [_job(f"j{i:02d}", "failed", ended=NOW - 60 - i) for i in range(25)]
        plan = retention.plan_retention_sweep(jobs, dead_ttl=3600.0, triage_ttl=600.0,
                                              max_dead=10, now=NOW)
        self.assertEqual(len(plan["purge_ids"]), 15, "25 dead - cap 10 = 15 cap-forced")
        # The 15 oldest (j24..j10) are cap-forced, in oldest-first order.
        self.assertEqual(plan["purge_ids"], [f"j{i:02d}" for i in range(24, 9, -1)],
                         "oldest 15 are cap-forced, in order")
        self.assertEqual(set(plan["reasons"].values()), {"cap-dead"},
                         "all 15 are cap-forced (all under TTL)")
        survivors = [j["id"] for j in jobs if j["id"] not in set(plan["purge_ids"])]
        self.assertEqual(len(survivors), 10, "invariant: dead count == max_dead")

    def test_cap_forces_oldest_first_when_over_cap(self):
        """15 young dead rows (all under TTL) → the cap forces exactly the 5
        oldest out (``cap-dead``), leaving exactly 10 dead rows. TTL never
        fires because nothing is past the dead TTL."""
        # Rows 1 s apart, all ~60 s old: j14 is the oldest (largest age).
        jobs = [_job(f"j{i:02d}", "failed", ended=NOW - 60 - i) for i in range(15)]
        plan = retention.plan_retention_sweep(jobs, dead_ttl=3600.0, triage_ttl=600.0,
                                              max_dead=10, now=NOW)
        # The 5 oldest (j14..j10) are cap-forced, in oldest-first order.
        self.assertEqual(plan["purge_ids"], ["j14", "j13", "j12", "j11", "j10"])
        self.assertEqual(set(plan["reasons"].values()), {"cap-dead"})
        survivors = [j["id"] for j in jobs if j["id"] not in set(plan["purge_ids"])]
        self.assertEqual(len(survivors), 10, "invariant: dead count == max_dead")

    def test_ttl_respected_young_dead_row_survives_until_ttl_fires(self):
        """10 old + 1 young (11 > cap 10): the cap forces the OLDEST row out
        (``cap-dead``), then the TTL clears the remaining 9 old rows
        (``ttl-dead``). The young row — under the TTL and no longer over the
        cap — SURVIVES: TTL is respected, the cap is the only rule that can
        move a young row, and the cap has already been satisfied."""
        jobs = [_job(f"old{i}", "failed", ended=OLD + i) for i in range(10)]
        jobs.append(_job("young", "failed", ended=YOUNG))
        plan = retention.plan_retention_sweep(jobs, dead_ttl=3600.0, triage_ttl=600.0,
                                              max_dead=10, now=NOW)
        # The cap forced exactly 1 row out (11 - 10 = 1): the oldest.
        self.assertEqual(plan["reasons"].get("old0"), "cap-dead")
        # The remaining 9 old rows are TTL-expired.
        self.assertEqual(
            [j["id"] for j in jobs if plan["reasons"].get(j["id"]) == "ttl-dead"],
            [f"old{i}" for i in range(1, 10)],
        )
        # The young row survived.
        self.assertNotIn("young", plan["purge_ids"],
                         "young row survives: cap satisfied, TTL not expired")
        survivors = [j["id"] for j in jobs if j["id"] not in set(plan["purge_ids"])]
        self.assertEqual(survivors, ["young"], "only the young row remains")
        self.assertLessEqual(len(survivors), 10, "invariant holds")

    def test_young_dead_row_survives_when_under_the_cap(self):
        """Below the cap, TTL alone decides — a young row stays, and old rows
        past the dead TTL are purged (survivors end up under the cap)."""
        jobs = [_job(f"old{i}", "failed", ended=OLD + i) for i in range(3)]
        jobs.append(_job("young", "failed", ended=YOUNG))
        plan = retention.plan_retention_sweep(jobs, dead_ttl=3600.0, triage_ttl=600.0,
                                              max_dead=10, now=NOW)
        self.assertNotIn("young", plan["purge_ids"], "under the cap the TTL is the only rule")
        self.assertEqual(plan["purge_ids"], ["old0", "old1", "old2"])
        self.assertEqual(set(plan["reasons"].values()), {"ttl-dead"})

    def test_protected_set_active_statuses_and_non_triage_done_never_touched(self):
        """PROTECTED SET (explicit): rows in queued/offered/claimed/running/
        needs-review AND non-triage done rows are NEVER purged — even ancient,
        even when the dead cap is breached by other rows."""
        protected = [
            _job("a-queued", "queued", ended=OLD),
            _job("a-offered", "offered", ended=OLD),
            _job("a-claimed", "claimed", ended=OLD),
            _job("a-running", "running", ended=OLD),
            _job("a-needs-review", "needs-review", ended=OLD),
            _job("d-work", "done", kind="zeroclaw", ended=OLD),      # non-triage done = proof-of-work
            _job("d-work2", "done", kind="codex:build", ended=OLD),  # non-triage done = proof-of-work
        ]
        # 25 YOUNG dead rows (under TTL) so only the cap is in play:
        # 25 - 10 = 15 cap-forced, 10 survivors, nothing TTL-eligible.
        dead = [_job(f"j{i:02d}", "failed", ended=YOUNG + i) for i in range(25)]
        jobs = protected + dead
        plan = retention.plan_retention_sweep(jobs, dead_ttl=3600.0, triage_ttl=600.0,
                                              max_dead=10, now=NOW)
        purged = set(plan["purge_ids"])
        for j in protected:
            self.assertNotIn(j["id"], purged, f"protected row {j['id']} must never be purged")
        self.assertEqual(len(purged), 15, "exactly the cap overflow, all cap-forced")
        self.assertEqual(set(plan["reasons"].values()), {"cap-dead"})

    def test_failed_completion_is_protected(self):
        """failed_completion is a worker-side retryable state, not a dead row."""
        jobs = [_job("fc1", "failed_completion", ended=OLD)]
        plan = retention.plan_retention_sweep(jobs, dead_ttl=3600.0, triage_ttl=600.0,
                                              max_dead=10, now=NOW)
        self.assertEqual(plan["purge_ids"], [])

    def test_triage_rows_past_ttl_purged_running_triage_not(self):
        """Triage work orders: past TTL (done/cancelled/failed/dead-letter) are
        purged; a triage row still RUNNING is never touched; a fresh triage
        row under its TTL survives."""
        old_ts = NOW - 3600  # an hour old > 600 s triage TTL
        jobs = [
            _job("t-done", "done", kind="triage:zeroclaw", ended=old_ts),
            _job("t-cancelled", "cancelled", kind="triage:zeroclaw", ended=old_ts),
            _job("t-failed", "failed", kind="triage:zeroclaw", ended=old_ts),
            _job("t-dl", "dead-letter", kind="triage:zeroclaw", ended=old_ts),
            _job("t-running", "running", kind="triage:zeroclaw", ended=old_ts),
            _job("t-fresh-done", "done", kind="triage:zeroclaw", ended=NOW - 60),
        ]
        plan = retention.plan_retention_sweep(jobs, dead_ttl=3600.0, triage_ttl=600.0,
                                              max_dead=10, now=NOW)
        self.assertEqual(
            set(plan["purge_ids"]),
            {"t-done", "t-cancelled", "t-dl", "t-failed"},
            "the four terminal triage rows past TTL are purged",
        )
        self.assertNotIn("t-running", plan["purge_ids"], "a running triage is ACTIVE — protected")
        self.assertNotIn("t-fresh-done", plan["purge_ids"], "fresh triage under its TTL survives")
        # Dead set (t-cancelled/t-failed/t-dl = 3) is under the cap → no cap
        # picks; all four are purged by the triage TTL rule.
        self.assertEqual(set(plan["reasons"].values()), {"ttl-triage"})
        self.assertEqual(plan["reasons"]["t-failed"], "ttl-triage")
        self.assertEqual(plan["reasons"]["t-done"], "ttl-triage")

    def test_invariant_holds_on_second_sweep_same_data(self):
        """Idempotency: running the planner over the rows that SURVIVE the first
        sweep produces an empty plan — the dead count is at most the cap and
        nothing surviving is TTL-expired (or the TTL survivors are still
        capped)."""
        # 15 young dead rows: sweep 1 cap-forces the 5 oldest, leaves 10 young.
        jobs = [_job(f"j{i:02d}", "failed", ended=NOW - 60 - i) for i in range(15)]
        plan1 = retention.plan_retention_sweep(jobs, dead_ttl=3600.0, triage_ttl=600.0,
                                               max_dead=10, now=NOW)
        self.assertEqual(len(plan1["purge_ids"]), 5)
        purged1 = set(plan1["purge_ids"])
        survivors = [j for j in jobs if j["id"] not in purged1]
        self.assertLessEqual(len(survivors), 10, "invariant after sweep 1")
        # The second sweep over the same (surviving) data.
        plan2 = retention.plan_retention_sweep(survivors, dead_ttl=3600.0, triage_ttl=600.0,
                                               max_dead=10, now=NOW)
        self.assertEqual(plan2["purge_ids"], [], "second sweep is a no-op (idempotent)")

    def test_dead_set_count_includes_cancelled_and_dead_letter(self):
        """The invariant counts failed + dead-letter + cancelled together:
        4 failed + 4 cancelled + 4 dead-letter = 12 dead > cap 10 → the 2
        oldest are cap-forced, leaving exactly 10."""
        jobs = (
            [_job(f"f{i}", "failed", ended=NOW - 60 - i) for i in range(4)]
            + [_job(f"c{i}", "cancelled", ended=NOW - 60 - i) for i in range(4)]
            + [_job(f"d{i}", "dead-letter", ended=NOW - 60 - i) for i in range(4)]
        )
        plan = retention.plan_retention_sweep(jobs, dead_ttl=3600.0, triage_ttl=600.0,
                                              max_dead=10, now=NOW)
        self.assertEqual(len(plan["purge_ids"]), 2, "12 dead > cap 10 → 2 oldest purged")
        for r in plan["reasons"].values():
            self.assertEqual(r, "cap-dead", "both are cap-forced (all under TTL)")
        self.assertEqual(len(jobs) - len(plan["purge_ids"]), 10)

    def test_age_fallback_last_update_at(self):
        """ended_at missing → age falls back to last_update_at."""
        jobs = [_job("x", "failed", ended=None, last_update=OLD)]
        plan = retention.plan_retention_sweep(jobs, dead_ttl=3600.0, triage_ttl=600.0,
                                              max_dead=10, now=NOW)
        self.assertEqual(plan["purge_ids"], ["x"])
        self.assertEqual(plan["reasons"]["x"], "ttl-dead")

    def test_unknown_status_never_purged(self):
        """Anything outside the dead/triage sets (e.g. 'released') is protected."""
        jobs = [_job("r1", "released", ended=OLD)]
        plan = retention.plan_retention_sweep(jobs, dead_ttl=3600.0, triage_ttl=600.0,
                                              max_dead=0, now=NOW)
        self.assertEqual(plan["purge_ids"], [])


class FakeBus:
    """In-memory fake of the bus HTTP surface the sweep uses.

    Records every purge call; supports making a row disappear (404) or move
    status between listing and re-read, to exercise the safety paths.
    """

    def __init__(self, rows: list[dict]):
        self.rows = {j["id"]: dict(j) for j in rows}
        self.purge_calls: list[list[str]] = []
        self.messages: list[tuple[str, dict]] = []
        self.vanished: set[str] = set()
        self.moved: dict[str, str] = {}  # id -> status (seen on re-read)

    # -- bus listing surface (as the doctor's _http would return) --
    def list_by_status(self, status: str, limit: int = 1000) -> tuple[int, dict | None]:
        jobs = [r for r in self.rows.values() if r.get("status") == status][:limit]
        return 200, {"count": len(jobs), "total": len(jobs), "jobs": jobs}

    def get_job(self, jid: str) -> tuple[int, dict | None]:
        if jid in self.vanished or jid not in self.rows:
            return 404, None
        r = self.rows[jid]
        if jid in self.moved:
            r = {**r, "status": self.moved[jid]}
        return 200, r

    def purge(self, body: dict) -> tuple[int, dict]:
        ids = body.get("ids") or []
        self.purge_calls.append(list(ids))
        deleted = []
        for jid in ids:
            r = self.rows.get(jid)
            if r is None:
                continue
            if r.get("status") in ("queued", "offered", "claimed", "running", "needs-review"):
                continue  # endpoint's own active guard
            del self.rows[jid]
            deleted.append(jid)
        return 200, {"deleted": len(deleted), "matched_ids": deleted}

    def post_message(self, body: dict) -> tuple[int, dict | None]:
        self.messages.append((body.get("topic"), body.get("payload") or {}))
        return 200, None

    def status_total(self, status: str) -> int:
        return sum(1 for r in self.rows.values() if r.get("status") == status)


def _run_doctor_sweep(rows: list[dict], *, dry_run: bool = False,
                      vanished: set[str] | None = None,
                      moved: dict[str, str] | None = None,
                      listing_fail: bool = False) -> tuple[dict | None, FakeBus, str]:
    """Wire the doctor against a FakeBus and run one sweep.
    Returns (report, bus, snapshot_dir) where snapshot_dir is the temp
    directory used for the retention snapshot (for test assertions)."""
    bus = FakeBus(rows)
    if vanished:
        bus.vanished |= vanished
    if moved:
        bus.moved.update(moved)

    def fake_http(method, path, body=None, timeout=15.0):
        # -- reads --
        if method == "GET" and path.startswith("/v1/jobs?status="):
            if listing_fail:
                return 500, None
            status = path.split("status=")[1].split("&")[0]
            return bus.list_by_status(status)
        if method == "GET" and path.startswith("/v1/jobs/"):
            jid = path.rsplit("/", 1)[-1]
            return bus.get_job(jid)
        if method == "POST" and path == "/v1/jobs/purge":
            return bus.purge(body or {})
        if method == "POST" and path == "/v1/messages":
            return bus.post_message(body or {})
        raise AssertionError(f"unexpected call {method} {path}")

    old_http = doctor._http
    old_urn = doctor._urn
    old_dry = doctor.RETENTION_DRY_RUN
    old_dir = os.environ.get("HIVE_RETENTION_DIR")
    snap_dir = tempfile.mkdtemp(prefix="retention-snap-")
    os.environ["HIVE_RETENTION_DIR"] = snap_dir
    os.environ["RETENTION_DEAD_TTL_SEC"] = "3600"
    os.environ["RETENTION_TRIAGE_TTL_SEC"] = "600"
    os.environ["RETENTION_MAX_DEAD"] = "10"
    try:
        doctor._http = fake_http
        doctor._urn = "urn:agent:doctor:pythia:test"
        doctor.RETENTION_DRY_RUN = dry_run
        report = doctor.run_retention_sweep()
    finally:
        doctor._http = old_http
        doctor._urn = old_urn
        doctor.RETENTION_DRY_RUN = old_dry
        if old_dir is None:
            os.environ.pop("HIVE_RETENTION_DIR", None)
        else:
            os.environ["HIVE_RETENTION_DIR"] = old_dir
    return report, bus, snap_dir


class RetentionDoctorSweepTests(unittest.TestCase):
    """Doctor-side orchestration against the FAKE bus (no live bus, no DB)."""

    def test_dry_run_purges_nothing_but_reports(self):
        rows = [_job(f"j{i:02d}", "failed", ended=OLD + i) for i in range(25)]
        report, bus, _ = _run_doctor_sweep(rows, dry_run=True)
        self.assertIsNotNone(report)
        self.assertTrue(report["dry_run"])
        self.assertEqual(report["purged"], 0)
        self.assertEqual(report["would_purge"], 25)
        self.assertEqual(len(bus.purge_calls), 0, "dry-run must never call the purge endpoint")
        # nothing left the fake bus
        self.assertEqual(len(bus.rows), 25)

    def test_snapshot_written_before_deletion_contains_every_removed_id(self):
        rows = [_job(f"j{i:02d}", "failed", ended=OLD + i, result={"error": "boom"}) for i in range(25)]
        report, bus, snap_dir = _run_doctor_sweep(rows)
        self.assertIsNotNone(report)
        self.assertEqual(report["purged"], 25)
        # The report should carry the snapshot path; if not, find it in
        # the temp dir the harness created.
        snap_path = report.get("snapshot_file")
        if snap_path is None:
            files = [p for p in os.listdir(snap_dir) if p.endswith(".jsonl")]
            self.assertEqual(len(files), 1, "exactly one snapshot file per sweep")
            snap_path = os.path.join(snap_dir, files[0])
        self.assertTrue(os.path.exists(snap_path), "snapshot file must exist")
        with open(snap_path, encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]
        header = lines[0]
        self.assertEqual(header["event"], "retention-sweep-snapshot")
        self.assertEqual(header["count"], 25)
        entry_ids = {line["id"] for line in lines[1:]}
        self.assertEqual(len(entry_ids), 25)
        removed = {f"j{i:02d}" for i in range(25)}
        self.assertEqual(entry_ids, removed, "snapshot must contain every removed id")
        # spot-check the fields the operator asked for
        e = next(line for line in lines[1:] if line["id"] == "j00")
        for field in ("id", "kind", "status", "claimed_by", "ended_at", "result_summary"):
            self.assertIn(field, e)
        self.assertEqual(e["status"], "failed")
        self.assertIn("boom", e["result_summary"])
        # and the rows actually vanished from the bus
        self.assertEqual(len(bus.rows), 0)

    def test_invariant_holds_on_second_sweep_same_data(self):
        rows = [_job(f"j{i:02d}", "failed", ended=OLD + i) for i in range(25)]
        report1, bus, _ = _run_doctor_sweep(rows)
        self.assertEqual(report1["purged"], 25)
        self.assertTrue(report1["invariant_satisfied"])
        self.assertEqual(report1["dead_after_sweep"], 0)
        # second sweep over the same (now surviving) data — a no-op purge, but
        # the sweep still reports and the invariant is verified against the
        # live bus on EVERY sweep, deleted rows or not.
        report2, _, _ = _run_doctor_sweep(list(bus.rows.values()))
        self.assertIsNotNone(report2, "a no-op sweep still reports the queue state")
        self.assertEqual(report2["purged"], 0, "nothing left to purge")
        self.assertTrue(report2["invariant_satisfied"],
                        "the invariant must hold after EVERY sweep — no-op included")
        self.assertEqual(report2["dead_after_sweep"], 0)
        # invariant still holds
        dead = sum(bus.status_total(s) for s in ("failed", "dead-letter", "cancelled"))
        self.assertLessEqual(dead, 10)

    def test_protected_rows_survive_doctor_sweep(self):
        """End-to-end protected set: active rows + non-triage done rows are
        untouched even while 25 dead rows are purged down to the cap."""
        protected = [
            _job("a-queued", "queued", ended=OLD),
            _job("a-offered", "offered", ended=OLD),
            _job("a-claimed", "claimed", ended=OLD),
            _job("a-running", "running", ended=OLD),
            _job("a-needs-review", "needs-review", ended=OLD),
            _job("d-work", "done", kind="zeroclaw", ended=OLD),
        ]
        rows = protected + [_job(f"j{i:02d}", "failed", ended=OLD + i) for i in range(25)]
        report, bus, _ = _run_doctor_sweep(rows)
        self.assertEqual(report["purged"], 25)
        for j in protected:
            self.assertIn(j["id"], bus.rows, f"protected row {j['id']} must survive")
        self.assertEqual(len(bus.rows), 6, "only the protected rows remain")

    def test_row_vanishing_between_listing_and_purge_is_skipped(self):
        """A row disappearing between listing and re-read → skipped (404)."""
        rows = [_job(f"j{i:02d}", "failed", ended=OLD + i) for i in range(25)]
        report, bus, _ = _run_doctor_sweep(rows, vanished={"j00"})
        self.assertIsNotNone(report)
        self.assertEqual(report["skipped"], 1)
        self.assertEqual(report["purged"], 24)
        purged_flat = [j for call in bus.purge_calls for j in call]
        self.assertNotIn("j00", purged_flat)
        # j00 vanished from the fake bus before the sweep → 1 dead row
        # remains (the 25th, which the sweep saw but did not re-read as
        # vanished). Invariant recount reflects the actual bus state.
        self.assertEqual(report["dead_after_sweep"], 1)
        self.assertTrue(report["invariant_satisfied"],
                        "1 dead row <= cap 10 → invariant satisfied")

    def test_row_status_change_between_listing_and_purge_is_skipped(self):
        """A row that moves to an active status before the re-read is never
        purged (safety: never purge a job another path may still be writing)."""
        rows = [_job(f"j{i:02d}", "failed", ended=OLD + i) for i in range(25)]
        report, bus, _ = _run_doctor_sweep(rows, moved={"j00": "queued"})
        self.assertIsNotNone(report)
        self.assertEqual(report["skipped"], 1)
        self.assertIn("j00", bus.rows, "moved row must survive")
        purged_flat = [j for call in bus.purge_calls for j in call]
        self.assertNotIn("j00", purged_flat)

    def test_one_hive_message_per_sweep_doctor_retention_topic(self):
        rows = [_job(f"j{i:02d}", "failed", ended=OLD + i) for i in range(12)]
        report, bus, _ = _run_doctor_sweep(rows)
        topics = [t for t, _ in bus.messages]
        self.assertEqual([t for t in topics if t == "doctor.retention"], ["doctor.retention"])
        payload = next(p for t, p in bus.messages if t == "doctor.retention")
        self.assertEqual(payload["purged"], 12)
        self.assertTrue(payload["invariant_satisfied"])
        self.assertIn("counts_by_status", payload)
        self.assertIn("oldest_purged_sec", payload)
        self.assertIn("newest_purged_sec", payload)
        # a no-op sweep still posts its doctor.retention report (one message
        # PER sweep, not per deletion) — with the invariant verified against
        # the live queue even though nothing was purged.
        report2, bus2, _ = _run_doctor_sweep(list(bus.rows.values()))
        self.assertIsNotNone(report2, "a no-op sweep still reports the queue state")
        self.assertEqual(report2["purged"], 0)
        self.assertTrue(report2["invariant_satisfied"],
                        "the invariant must hold after EVERY sweep — no-op included")
        self.assertEqual(
            [t for t, _ in bus2.messages if t == "doctor.retention"], ["doctor.retention"],
            "exactly one report message for the no-op sweep",
        )

    def test_cap_forces_young_rows_end_to_end(self):
        """TTL not expired but count over the cap → oldest dead purged first
        (the operator's 'busy period' rule)."""
        # 12 recent dead rows (all under the 1h TTL) — cap 10 forces 2 out.
        rows = [_job(f"j{i:02d}", "failed", ended=NOW - 60 * i) for i in range(12)]
        report, bus, _ = _run_doctor_sweep(rows)
        self.assertIsNotNone(report)
        self.assertEqual(report["purged"], 2)
        self.assertEqual(report["counts_by_reason"], {"cap-dead": 2})
        # oldest two (j11, j10) went first — they have the largest ages
        self.assertEqual(sorted(bus.purge_calls[0] or []), ["j10", "j11"])
        self.assertEqual(report["dead_after_sweep"], 10)
        self.assertTrue(report["invariant_satisfied"])

    def test_incomplete_listing_skips_sweep(self):
        """If a status listing fails, the sweep is skipped entirely (no cap
        purge on an incomplete picture)."""
        rows = [_job(f"j{i:02d}", "failed", ended=OLD + i) for i in range(25)]
        report, bus, _ = _run_doctor_sweep(rows, listing_fail=True)
        self.assertEqual(report, {"error": "incomplete listing", "scanned": None, "purged": 0})
        self.assertEqual(len(bus.purge_calls), 0)
        self.assertEqual(len(bus.rows), 25)


if __name__ == "__main__":
    unittest.main()
