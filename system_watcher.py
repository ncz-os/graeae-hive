#!/usr/bin/env python3
"""Compatibility shim — the fleet system watcher now lives in hive.workers.watcher."""

from hive.workers.watcher import *  # noqa: F401,F403
from hive.workers.watcher import main  # noqa: F401

if __name__ == "__main__":
    main()
