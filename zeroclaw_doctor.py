#!/usr/bin/env python3
"""Compatibility shim — the GRAEAE doctor now lives in hive.workers.doctor."""

from hive.workers.doctor import *  # noqa: F401,F403
from hive.workers.doctor import main  # noqa: F401

if __name__ == "__main__":
    main()
