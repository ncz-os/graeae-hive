"""Capacity-derived zeroclaw fan-out — how many agents a host should run.

The number of worker *sessions* a host runs is not a fixed constant: it is a
function of the host's capacity (CPU cores, RAM, accelerators) and — dynamically
— its current load. A big build box runs many; a small box runs few; a host under
load sheds sessions so it never oversubscribes.

The system-watcher computes this from its live specs + load each heartbeat and
advertises it as ``target_workers``; the host's worker reconcile starts/stops
``hive-worker-zeroclaw@N`` units to match. Pure + dependency-free so it is
directly unit-testable.
"""

from __future__ import annotations

# Per-session budget: a zeroclaw session is CPU + a few hundred MB when idle but
# spikes under a real job; ~2 cores and ~2 GB per concurrent session is the fleet
# rule of thumb (CLAUDE.md oversubscription gotcha: ARGOS load=92 incident).
CORES_PER_WORKER = 2
GB_PER_WORKER = 2
LOAD_SHED_THRESHOLD = 0.7  # 1-min load/cpu above which we start shedding
LOAD_CRITICAL = 0.9  # above which we shed aggressively
DEFAULT_MAX_WORKERS = 16


def desired_worker_count(
    *,
    cpu_count: int,
    mem_gb: float,
    load_per_cpu: float = 0.0,
    gpus: int = 0,
    min_workers: int = 1,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> int:
    """How many zeroclaw sessions ``host`` should run right now.

    Base = the tighter of the CPU axis (cores / CORES_PER_WORKER) and the RAM axis
    (GB / GB_PER_WORKER); each GPU permits one extra concurrent session. Under
    load the count sheds (dynamic), never below ``min_workers`` and never above
    ``max_workers``.
    """
    cpu_count = max(1, int(cpu_count or 1))
    by_cpu = max(1, cpu_count // CORES_PER_WORKER)
    by_ram = max(1, int((mem_gb or 0) // GB_PER_WORKER))
    base = min(by_cpu, by_ram)
    if gpus and gpus > 0:
        base = min(base + int(gpus), max_workers)
    # dynamic shedding under live load
    if load_per_cpu >= LOAD_CRITICAL:
        base -= 2
    elif load_per_cpu >= LOAD_SHED_THRESHOLD:
        base -= 1
    return max(int(min_workers), min(int(base), int(max_workers)))
