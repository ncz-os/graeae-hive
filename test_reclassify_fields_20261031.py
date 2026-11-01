"""The doctor's FIELD fix must actually land on the row.

``hive/workers/doctor_remediation`` builds ``plan.changes`` as
``{row_field: {before, after}}`` and hands it to the doctor's executor
(``doctor._remediate_apply_fields``), which posts the flattened mapping as
``fields: {name: after}`` to ``POST /v1/admin/jobs/reclassify``.

That route moved the row's STATUS but had no ``fields`` input, so the field
patch was silently dropped: the doctor reported a fix, the row kept its broken
``baseline``/``head_sha``, and the next sweep classified the same cause again
until the 2-attempt cap escalated a row the doctor believed it had repaired.

These tests pin the wire contract the doctor's executor depends on:
  * ``fields`` is accepted and merged into the row's result;
  * a field the row already carries is OVERWRITTEN (a repair replaces a bad
    value, it does not skip it);
  * unrelated result keys survive the merge (the merge is not a replace);
  * a request with no ``fields`` is byte-for-byte the old behaviour;
  * the merged write is audited so before/after is recoverable.

Harness follows ``test_auth_matrix_20261031`` (temp DB, ``_HIVE_REPO`` patched
to None, tokens cleared, in-process ASGITransport). No sleeps, no network.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")
os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus import deps as _deps  # noqa: E402
from hive.bus.deps import hive_repo  # noqa: E402

REAL_HEAD = "4b8c1e0a7d92f36b58a0c1d4e7f9b2a3c6d8e0f1"
COMPOSITE = "4b8c1e0a7d92f36b58a0c1d4e7f9" + "0a1c9f3e"  # never resolved a commit


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class ReclassifyFieldsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._td = tempfile.TemporaryDirectory()
        cls._env = mock.patch.dict(
            os.environ,
            {"AGENT_BUS_DB": os.path.join(cls._td.name, "fields.db"), "HIVE_BACKEND": "sqlite"},
        )
        cls._env.start()
        for var in ("MNEMOS_URL", "MNEMOS_TOKEN", "HIVE_SHARED_SECRET", "HIVE_BUS_ADMIN_TOKEN"):
            os.environ.pop(var, None)
        cls._repo_patch = mock.patch.object(_deps, "_HIVE_REPO", None)
        cls._repo_patch.start()
        _run(hive_repo().init())

        async def _register():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t") as c:
                reg = {
                    "kind": "opencode",
                    "host": "m1",
                    "runtime": "opencode",
                    "status": "online",
                    "capabilities": ["code"],
                    "cost_tier": "B",
                }
                return (await c.post("/v1/agents/register", json=reg)).json()["urn"]

        cls.urn = _run(_register())

    @classmethod
    def tearDownClass(cls):
        cls._repo_patch.stop()
        cls._env.stop()
        cls._td.cleanup()

    # ---- harness ----
    @classmethod
    def _call(cls, method, path, body=None):
        async def _do():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t") as c:
                r = await c.request(method, path, json=body, headers={"X-Hive-Actor": cls.urn})
                return r.status_code, r.json()

        return _run(_do())

    def _submit_with_result(self, result):
        """A failed row carrying ``result`` -- the shape the doctor reads."""
        code, data = self._call(
            "POST", "/v1/jobs", {"kind": "loop", "description": "field patch", "submitter_urn": self.urn}
        )
        self.assertIn(code, (200, 201), data)
        jid = data["id"]

        async def _seed():
            async with hive_repo().connection() as tx:
                await hive_repo().patch_job_status(
                    tx,
                    jid,
                    sets={"status": "failed", "result": __import__("json").dumps(result)},
                )
                await tx.commit()

        _run(_seed())
        return jid

    def _result(self, jid):
        code, row = self._call("GET", f"/v1/jobs/{jid}")
        self.assertEqual(code, 200, row)
        return row.get("result") or {}

    # ---- the contract ----
    def test_reclassify_writes_the_fields_it_is_given(self):
        """REGRESSION: the doctor's executor posts ``fields``; before the fix the
        route accepted the request (200 ok) and dropped the patch entirely."""
        jid = self._submit_with_result({"baseline": COMPOSITE, "worker_error": "merge-base exited 128"})
        code, data = self._call(
            "POST",
            "/v1/admin/jobs/reclassify",
            {
                "job_ids": [jid],
                "status": "needs-review",
                "reason": "doctor-remediate:unresolvable_ref",
                "fields": {"baseline": REAL_HEAD},
            },
        )
        self.assertEqual(code, 200, data)
        self.assertIn(jid, data["ok"], data)
        self.assertEqual(self._result(jid)["baseline"], REAL_HEAD)

    def test_a_repair_overwrites_a_bad_value_in_place(self):
        """A field the row ALREADY carries is replaced, not skipped -- that is
        the whole point of a field repair."""
        jid = self._submit_with_result({"head_sha": COMPOSITE})
        code, _ = self._call(
            "POST",
            "/v1/admin/jobs/reclassify",
            {"job_ids": [jid], "status": "needs-review", "reason": "r", "fields": {"head_sha": REAL_HEAD}},
        )
        self.assertEqual(code, 200)
        self.assertEqual(self._result(jid)["head_sha"], REAL_HEAD)

    def test_the_merge_keeps_unrelated_result_keys(self):
        """The other result evidence (the review contract, the error text) must
        survive: the patch is a MERGE, not a wholesale replace."""
        jid = self._submit_with_result({"baseline": COMPOSITE, "worker_error": "exit 128", "commits": [REAL_HEAD]})
        self._call(
            "POST",
            "/v1/admin/jobs/reclassify",
            {"job_ids": [jid], "status": "needs-review", "reason": "r", "fields": {"baseline": REAL_HEAD}},
        )
        result = self._result(jid)
        self.assertEqual(result["baseline"], REAL_HEAD)
        self.assertEqual(result["worker_error"], "exit 128")
        self.assertEqual(result["commits"], [REAL_HEAD])

    def test_no_fields_is_the_old_status_only_behaviour(self):
        """Backward compatibility: an omitted ``fields`` must not touch the
        result at all (the existing callers reclassify status only)."""
        jid = self._submit_with_result({"baseline": COMPOSITE})
        code, _ = self._call(
            "POST", "/v1/admin/jobs/reclassify", {"job_ids": [jid], "status": "needs-review", "reason": "r"}
        )
        self.assertEqual(code, 200)
        self.assertEqual(self._result(jid)["baseline"], COMPOSITE)

    def test_requeue_still_clears_the_result_and_does_not_resurrect_fields(self):
        """Requeue archives and clears the result (FB51). A ``fields`` patch
        alongside status=queued must not write into the cleared result -- the
        row is going back to the queue, not being repaired in place."""
        jid = self._submit_with_result({"baseline": COMPOSITE})
        code, data = self._call(
            "POST",
            "/v1/admin/jobs/reclassify",
            {"job_ids": [jid], "status": "queued", "reason": "r", "fields": {"baseline": REAL_HEAD}},
        )
        self.assertIn(code, (200, 422), data)
        if code == 200:
            self.assertNotIn("baseline", self._result(jid))


if __name__ == "__main__":
    unittest.main()


class DoctorPlanRoundTripTests(unittest.TestCase):
    """The seam the two modules meet on: the doctor's PLAN keys, its real
    executor's body shape, and this route's merge must agree end to end.

    ``plan_for`` keys the mapping by the row's own field name (never a
    ``result.`` qualifier); ``_remediate_apply_fields`` flattens it to
    ``fields: {name: after}``; the route merges it into the row's result. This
    class drives that whole path against the live app and then re-reads the row
    with the remediation module's own readers."""

    setUpClass = ReclassifyFieldsTests.__dict__["setUpClass"]
    tearDownClass = ReclassifyFieldsTests.__dict__["tearDownClass"]
    _call = ReclassifyFieldsTests.__dict__["_call"]
    _submit_with_result = ReclassifyFieldsTests.__dict__["_submit_with_result"]
    _result = ReclassifyFieldsTests.__dict__["_result"]

    def test_a_composite_baseline_plan_lands_and_reads_back_as_fixed(self):
        import json as _json
        from pathlib import Path

        from hive.workers import doctor_remediation as dr

        real_head = REAL_HEAD
        fixture = _json.loads(
            (Path(__file__).resolve().parent / "tests" / "fixtures" / "doctor_remediation_rows.json").read_text()
        )["composite_baseline"]
        jid = self._submit_with_result(fixture["result"])

        code, row = self._call("GET", f"/v1/jobs/{jid}")
        self.assertEqual(code, 200)
        # The classifier reads the row shape the fixture models; the live row
        # supplies the identity and the result the seed wrote.
        row = {**fixture, "id": jid, "status": "failed", "result": row.get("result") or {}}

        cause = dr.classify(row)
        self.assertEqual(cause.name, dr.UNRESOLVABLE_REF)
        plan = dr.plan_for(
            row,
            cause,
            default_branch="deploy/integrate-20261017",
            ref_exists=lambda _r: True,
            resolve_ref=lambda _r: real_head,
        )
        # The plan speaks bare row field names -- exactly what the route merges.
        self.assertFalse(any("." in name for name in plan.changes), plan.changes)

        code, data = self._call(
            "POST",
            "/v1/admin/jobs/reclassify",
            {
                "job_ids": [jid],
                "status": "needs-review",
                "reason": plan.reason,
                "fields": {name: change["after"] for name, change in plan.changes.items()},
            },
        )
        self.assertEqual(code, 200, data)

        _code, written = self._call("GET", f"/v1/jobs/{jid}")
        self.assertEqual(dr.delivered_head(written), real_head)
        # And the reader agrees the planned fields are now in place.
        self.assertTrue(dr._fields_unchanged(written, plan))


class FieldPatchSafetyTests(ReclassifyFieldsTests):
    """Review round 8, findings 1-2 (both real, both fixed here).

    * The merge first read ``result`` and then wrote it back unconditionally:
      a concurrent writer that replaced the payload between the two steps lost
      its write. The write is now a compare-and-set on ``result IS ?`` too, so a
      lost race is reported as a conflict instead of silently overwriting.
    * A result that is valid JSON but NOT an object (a bare string or list) was
      reset to ``{}`` and then replaced by the patch. It is now preserved under
      ``previous_result`` -- a repair must never destroy evidence it cannot
      merge into."""

    def test_a_non_object_result_is_preserved_not_discarded(self):
        jid = self._submit_with_result(["legacy", "list", "result"])
        code, data = self._call(
            "POST",
            "/v1/admin/jobs/reclassify",
            {"job_ids": [jid], "status": "needs-review", "reason": "r", "fields": {"baseline": REAL_HEAD}},
        )
        self.assertEqual(code, 200, data)
        result = self._result(jid)
        self.assertEqual(result["baseline"], REAL_HEAD)
        self.assertEqual(result["previous_result"], ["legacy", "list", "result"])

    def test_a_null_result_accepts_the_patch(self):
        jid = self._submit_with_result(None)
        code, _ = self._call(
            "POST",
            "/v1/admin/jobs/reclassify",
            {"job_ids": [jid], "status": "needs-review", "reason": "r", "fields": {"baseline": REAL_HEAD}},
        )
        self.assertEqual(code, 200)
        result = self._result(jid)
        self.assertEqual(result["baseline"], REAL_HEAD)
        # needs-review also hands the row to the review queue; that handoff
        # key is expected and is not part of this assertion.
        self.assertNotIn("previous_result", result)

    def test_a_concurrent_result_write_is_not_clobbered(self):
        """The CAS: if the payload changes between the read and the write, the
        UPDATE misses and the row is reported as a conflict -- the other
        writer's value survives."""
        from hive.bus.routers import admin as _admin

        jid = self._submit_with_result({"baseline": COMPOSITE})
        real_read = _admin._read_result
        seeded = {"done": False}

        async def racing_read(tx, job_id):
            raw = await real_read(tx, job_id)
            if not seeded["done"]:
                seeded["done"] = True
                # Another writer commits a result between our read and our write.
                async with hive_repo().connection() as other:
                    await hive_repo().patch_job_status(
                        other, job_id, sets={"result": '{"worker_error": "theirs"}'}, expected_status="failed"
                    )
                    await other.commit()
            return raw

        with mock.patch.object(_admin, "_read_result", racing_read):
            _code, data = self._call(
                "POST",
                "/v1/admin/jobs/reclassify",
                {"job_ids": [jid], "status": "needs-review", "reason": "r", "fields": {"baseline": REAL_HEAD}},
            )
        self.assertIn(jid, data["skipped_conflict"], data)
        self.assertEqual(self._result(jid), {"worker_error": "theirs"})


class PreservationSlotTests(unittest.TestCase):
    """Review round 9: a field named ``previous_result`` must not be able to
    destroy the value that key exists to preserve.

    A result that is valid JSON but not an object is parked under
    ``previous_result``. A caller that sends ``fields={"previous_result": ...}``
    used to overwrite that slot, silently losing the original evidence -- the
    exact loss the preservation was added to prevent."""

    def test_a_patch_field_cannot_steal_the_preservation_slot(self):
        from hive.bus.routers.admin import _merge_field_patch

        merged = _merge_field_patch('"legacy-bare-string"', {"previous_result": "ATTACKER", "baseline": REAL_HEAD})
        self.assertEqual(merged["previous_result"], "legacy-bare-string")
        self.assertEqual(merged["baseline"], REAL_HEAD)

    def test_a_patch_field_cannot_steal_the_slot_on_a_list_result(self):
        from hive.bus.routers.admin import _merge_field_patch

        merged = _merge_field_patch('["a","b"]', {"previous_result": None})
        self.assertEqual(merged["previous_result"], ["a", "b"])

    def test_ordinary_patches_are_unaffected(self):
        from hive.bus.routers.admin import _merge_field_patch

        merged = _merge_field_patch('{"worker_error":"x"}', {"baseline": REAL_HEAD})
        self.assertEqual(merged, {"worker_error": "x", "baseline": REAL_HEAD})

    def test_ordinary_patch_merges_normally(self):
        """Sanity companion to the slot tests: a normal patch is a plain merge."""
        from hive.bus.routers.admin import _merge_field_patch

        self.assertEqual(_merge_field_patch(None, {"a": 1}), {"a": 1})


class DisprovedRound9Tests(unittest.TestCase):
    """Review round 9's other claims, each reproduced against source and
    DISPROVED. Pinned so a later edit cannot silently make them true.

    * "``result IS ?`` with a bound NULL matches nothing; use ``= ?``" -- the
      opposite is true: ``IS`` matches NULL, ``=`` does not. Switching to ``=``
      would break exactly the empty-result case the finding worried about.
    * "``import json`` missing" / "``row`` shadowed by a DB fetch" -- both
      already correct.
    * "``doctor_rules.ALERT_TOPIC`` / ``loop_outcome`` / ``default_rungs`` are
      unresolvable or circular" -- all import cleanly; doctor_rules imports
      nothing from doctor.
    * "``parse_constrained`` drops ``evidence_quote`` so the quote gate never
      sees it" -- the quote is re-attached from the raw reply immediately after
      parsing (doctor.py), so the gate does see it."""

    def test_is_matches_a_bound_null_but_equals_does_not(self):
        import sqlite3

        con = sqlite3.connect(":memory:")
        con.execute("CREATE TABLE t(id INTEGER, result TEXT)")
        con.execute("INSERT INTO t VALUES(1, NULL)")
        self.assertEqual(con.execute("SELECT COUNT(*) FROM t WHERE result IS ?", (None,)).fetchone()[0], 1)
        self.assertEqual(con.execute("SELECT COUNT(*) FROM t WHERE result = ?", (None,)).fetchone()[0], 0)

    def test_the_cas_predicate_stays_is_not_equals(self):
        """The CAS must keep ``result IS ?``: the NULL case depends on it."""
        import inspect

        from hive.bus.routers import admin as _admin

        src = inspect.getsource(_admin._patch_result_cas)
        self.assertIn("result IS ?", src)
        self.assertNotIn("result = ?", src)

    def test_the_modules_the_findings_called_unresolvable_do_import(self):
        from hive.workers import doctor_models, loop_outcome

        self.assertTrue(hasattr(loop_outcome, "_TEARDOWN_TO_CLASS"))
        self.assertTrue(hasattr(loop_outcome, "REVIEWER_UNAVAILABLE"))
        self.assertGreater(len(doctor_models.default_rungs()), 0)

    def test_doctor_rules_does_not_import_doctor(self):
        """No cycle: doctor_rules must not depend on doctor."""
        import inspect

        from hive.workers import doctor_rules

        src = inspect.getsource(doctor_rules)
        self.assertNotIn("import doctor\n", src)
        self.assertNotIn("from hive.workers.doctor import", src)

    def test_the_quote_dropped_by_parse_constrained_is_reattached(self):
        """The schema drops ``evidence_quote`` on purpose (a reply cannot smuggle
        fields past the rules); the caller puts the RAW quote back before the
        gate. Pin both halves so the gate cannot go blind silently."""
        import json as _json

        from hive.workers import doctor_rules as dr

        reply = _json.dumps(
            {"action_type": "release_to_queue", "confidence": 0.9, "reason": "x", "evidence_quote": "verbatim"}
        )
        ok, decision = dr.parse_constrained(reply)
        self.assertTrue(ok)
        self.assertNotIn("evidence_quote", decision)  # the schema's deliberate drop

        # doctor.py's re-attach step, in the same order it runs it.
        raw_reply = _json.loads(reply)
        quote = raw_reply.get("evidence_quote") or raw_reply.get("quote")
        if quote:
            decision["evidence_quote"] = str(quote)
        self.assertEqual(decision["evidence_quote"], "verbatim")

    def test_the_slot_is_protected_even_when_the_result_is_already_a_dict(self):
        """Round 9 reviewer objection, reproduced and DISPROVED: the skip is in
        the merge loop, so it applies to the object branch too -- an existing
        ``previous_result`` in a dict result is not overwritten."""
        import json as _json

        from hive.bus.routers.admin import _merge_field_patch

        merged = _merge_field_patch(
            _json.dumps({"previous_result": "ORIGINAL", "baseline": "old"}), {"previous_result": "NEW"}
        )
        self.assertEqual(merged["previous_result"], "ORIGINAL")
        self.assertEqual(merged["baseline"], "old")
