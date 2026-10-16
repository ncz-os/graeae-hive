"""Compatibility shim.

The GRAEAE Hive Mind app now lives in ``hive.bus.app`` (Phase 1d). This module
is kept as the stable entrypoint so ``uvicorn agent_bus:app`` and existing
imports (``agent_bus.<name>``) keep working. All real code is in the package.
"""

from hive.bus.app import *  # noqa: F401,F403
