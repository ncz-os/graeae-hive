"""The doctor's field-patch call must be a request the bus actually accepts.

The first live remediation sweep on PYTHIA (2026-11-01) failed every row with
``field_patch_http=422``: ``_remediate_apply_fields`` posted
``{job_ids, fields, reason}`` to ``/v1/admin/jobs/reclassify`` but
``ReclassifyRequest.status`` is required (a field repair rides a status move).
Every remediation test faked the HTTP layer, so none of them ever validated
the body against the route's own schema.
"""

import unittest
from unittest import mock

from hive.bus.routers.admin import RECLASSIFY_ALLOWED_TARGETS
from hive.bus.schemas import ReclassifyRequest
from hive.workers import doctor


class FieldPatchPayloadTests(unittest.TestCase):
    def _posted_body(self):
        calls = []

        def fake_http(method, path, body=None, **kwargs):
            calls.append((method, path, body))
            return 200, {"ok": []}

        with mock.patch.object(doctor, "_http", fake_http):
            code, _ = doctor._remediate_apply_fields(
                "01a19737-c9c7-7a30-b557-ce7d668a5f38",
                {"review_state": {"before": None, "after": "awaiting_review"}},
                "review_busy:loop_unresolved",
            )
        self.assertEqual(code, 200)
        self.assertEqual(len(calls), 1)
        method, path, body = calls[0]
        self.assertEqual((method, path), ("POST", "/v1/admin/jobs/reclassify"))
        return body

    def test_the_body_validates_against_the_routes_own_schema(self):
        body = self._posted_body()
        request = ReclassifyRequest(**body)  # raises on the unfixed code: status missing
        self.assertEqual(request.fields, {"review_state": "awaiting_review"})

    def test_the_status_is_one_the_route_allows_and_keeps_the_row_out_of_the_queue(self):
        body = self._posted_body()
        self.assertIn(body["status"], RECLASSIFY_ALLOWED_TARGETS)
        # fields are only merged when the row stays out of the queue (a requeue
        # archives and clears the result), so the patch must not name "queued".
        self.assertNotEqual(body["status"], "queued")

    def test_the_audit_reason_rides_along(self):
        self.assertEqual(self._posted_body()["reason"], "doctor-remediate:review_busy:loop_unresolved")


if __name__ == "__main__":
    unittest.main()
