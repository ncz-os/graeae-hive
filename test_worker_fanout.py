"""Capacity-derived worker fan-out — dynamic, not a fixed count."""

from __future__ import annotations

import unittest

from hive.workers.fanout import desired_worker_count


class WorkerFanoutTests(unittest.TestCase):
    def test_small_host_runs_fewer(self):
        # 4 cores / 8 GB -> 2 workers (cpu axis: 4//2=2, ram axis: 8//2=4 -> 2)
        self.assertEqual(desired_worker_count(cpu_count=4, mem_gb=8), 2)

    def test_big_host_runs_more(self):
        # 32 cores / 128 GB -> min(16, 16) = 16
        self.assertEqual(desired_worker_count(cpu_count=32, mem_gb=128), 16)

    def test_ram_axis_can_bind(self):
        # 16 cores but only 8 GB -> ram axis binds -> 4
        self.assertEqual(desired_worker_count(cpu_count=16, mem_gb=8), 4)

    def test_gpus_add_concurrency(self):
        base = desired_worker_count(cpu_count=16, mem_gb=64)  # min(8,32)=8
        withgpu = desired_worker_count(cpu_count=16, mem_gb=64, gpus=2)
        self.assertEqual(withgpu, base + 2)

    def test_dynamic_shed_under_load(self):
        calm = desired_worker_count(cpu_count=16, mem_gb=64, load_per_cpu=0.1)  # 8
        warm = desired_worker_count(cpu_count=16, mem_gb=64, load_per_cpu=0.75)  # 7
        hot = desired_worker_count(cpu_count=16, mem_gb=64, load_per_cpu=0.95)  # 6
        self.assertEqual((calm, warm, hot), (8, 7, 6))

    def test_never_below_floor(self):
        # tiny + critically loaded -> floor
        self.assertEqual(desired_worker_count(cpu_count=2, mem_gb=2, load_per_cpu=5.0, min_workers=1), 1)
        self.assertEqual(
            desired_worker_count(cpu_count=2, mem_gb=2, load_per_cpu=5.0, min_workers=4), 4
        )

    def test_capped(self):
        self.assertLessEqual(desired_worker_count(cpu_count=256, mem_gb=2048, gpus=16), 16)


if __name__ == "__main__":
    unittest.main()
