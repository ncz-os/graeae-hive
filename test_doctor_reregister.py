"""Doctor self-heal on an unknown agent URN (audit BLOCKING-3 / ADVISORY-2).

Companion to docs/audit-3-fixes-20261016.md. The bus returns 409 (not 404) for
an unknown agent URN, so the doctor's heartbeat must re-register on 404 OR 409 —
otherwise a ghost doctor never self-heals and spins forever — and the claim poll
must treat the self-heal code as expected, without a warning per poll. 404
handling is kept for buses that still answer 404.

Run: /usr/bin/python3 -m pytest -q test_doctor_reregister.py
"""

from __future__ import annotations

import unittest
from unittest import mock

import hive.workers.doctor as doctor


class DoctorReregisterTest(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.object(doctor, "_urn", "urn:agent:doctor:t:ghost")
        patcher.start()
        self.addCleanup(patcher.stop)

    def _heartbeat(self, code: int) -> tuple[mock.Mock, mock.Mock]:
        with mock.patch.object(doctor, "_http", return_value=(code, None)) as http, \
                mock.patch.object(doctor, "register") as register:
            doctor.heartbeat()
        return http, register

    def test_BLOCKING3_heartbeat_reregisters_on_409(self):
        """BLOCKING-3: an unknown URN now yields 409 — the doctor must self-heal."""
        _, register = self._heartbeat(409)
        register.assert_called_once_with()

    def test_heartbeat_still_reregisters_on_404(self):
        # Older buses answer 404; that handling is kept, not replaced.
        _, register = self._heartbeat(404)
        register.assert_called_once_with()

    def test_heartbeat_is_silent_on_success(self):
        _, register = self._heartbeat(200)
        register.assert_not_called()

    def test_heartbeat_does_not_reregister_on_other_errors(self):
        # A 500 must not trigger a registration storm.
        _, register = self._heartbeat(500)
        register.assert_not_called()

    def _claim(self, code: int) -> tuple[mock.Mock, mock.Mock]:
        with mock.patch.object(doctor, "_http", return_value=(code, None)), \
                mock.patch.object(doctor, "log") as log:
            job = doctor.claim_next_job()
        return job, log

    def test_ADVISORY2_claim_409_is_not_a_per_poll_warning(self):
        job, log = self._claim(409)
        self.assertIsNone(job)
        log.warning.assert_not_called()

    def test_claim_404_is_not_a_per_poll_warning(self):
        job, log = self._claim(404)
        self.assertIsNone(job)
        log.warning.assert_not_called()

    def test_claim_other_error_still_warns(self):
        job, log = self._claim(500)
        self.assertIsNone(job)
        log.warning.assert_called_once()

    def test_claim_204_is_idle(self):
        job, log = self._claim(204)
        self.assertIsNone(job)
        log.warning.assert_not_called()


if __name__ == "__main__":
    unittest.main()
