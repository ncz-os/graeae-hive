"""Tests for the Phase-5 capacity ledger + admission control."""

import time
import unittest
from datetime import UTC, datetime

from hive.bus.capacity import (
    DEEPSEEK_PEAK_MULTIPLIER,
    CapacityLedger,
    ServiceCapacity,
    time_multiplier,
)


def _ledger(**caps):
    led = CapacityLedger()
    for _name, c in caps.items():
        led.register(c)
    return led


class TimeMultiplierTests(unittest.TestCase):
    def test_deepseek_peak_is_2x_weekday_mornings(self):
        # Wed 2026-10-14 02:00 UTC -> peak
        peak = datetime(2026, 10, 14, 2, 0, tzinfo=UTC).timestamp()
        self.assertEqual(time_multiplier("deepseek", peak), DEEPSEEK_PEAK_MULTIPLIER)
        # Wed 12:00 UTC -> offpeak
        off = datetime(2026, 10, 14, 12, 0, tzinfo=UTC).timestamp()
        self.assertEqual(time_multiplier("deepseek", off), 1.0)
        # Sunday 02:00 -> offpeak (weekend)
        sun = datetime(2026, 10, 18, 2, 0, tzinfo=UTC).timestamp()
        self.assertEqual(time_multiplier("deepseek", sun), 1.0)

    def test_non_deepseek_is_always_1x(self):
        peak = datetime(2026, 10, 14, 2, 0, tzinfo=UTC).timestamp()
        self.assertEqual(time_multiplier("minimax", peak), 1.0)


class AdmissionTests(unittest.TestCase):
    def test_unknown_service_fails_closed(self):
        led = CapacityLedger()
        ok, reason = led.headroom("nope")
        self.assertFalse(ok)
        self.assertIn("unknown service", reason)

    def test_slot_cap(self):
        led = _ledger(minimax=ServiceCapacity(service="minimax", slots=5))
        for _ in range(5):
            self.assertTrue(led.headroom("minimax")[0])
            led.acquire("minimax")
        ok, reason = led.headroom("minimax")
        self.assertFalse(ok)
        self.assertIn("global cap", reason)
        led.release("minimax")
        self.assertTrue(led.headroom("minimax")[0])

    def test_credit_pool_with_peak_multiplier(self):
        led = _ledger(deepseek=ServiceCapacity(service="deepseek", credit_cap_usd=10.0))
        peak = datetime(2026, 10, 14, 2, 0, tzinfo=UTC).timestamp()
        # $6 at peak = $12 charged -> would exceed a $10 pool
        ok, reason = led.headroom("deepseek", est_cost_usd=6.0, now=peak)
        self.assertFalse(ok)
        self.assertIn("credit pool", reason)
        # same $6 offpeak = $6 -> fits
        off = datetime(2026, 10, 14, 12, 0, tzinfo=UTC).timestamp()
        self.assertTrue(led.headroom("deepseek", est_cost_usd=6.0, now=off)[0])

    def test_token_rate_ceiling(self):
        led = _ledger(openai=ServiceCapacity(service="openai", tpm=1000))
        led.record_usage("openai", tokens=900)
        self.assertFalse(led.headroom("openai", est_tokens=200)[0])
        self.assertTrue(led.headroom("openai", est_tokens=50)[0])

    def test_limited_until_denies(self):
        led = _ledger(minimax=ServiceCapacity(service="minimax", slots=5))
        now = time.time()
        led.mark_limited("minimax", until=now + 60)
        ok, reason = led.headroom("minimax", now=now)
        self.assertFalse(ok)
        self.assertIn("limited until", reason)
        self.assertTrue(led.headroom("minimax", now=now + 61)[0])

    def test_reset_window_clears(self):
        led = _ledger(deepseek=ServiceCapacity(service="deepseek", credit_cap_usd=1.0))
        led.record_usage("deepseek", cost_usd=0.9)
        self.assertFalse(led.headroom("deepseek", est_cost_usd=0.5)[0])
        led.reset_window("deepseek")
        self.assertTrue(led.headroom("deepseek", est_cost_usd=0.5)[0])


if __name__ == "__main__":
    unittest.main()
