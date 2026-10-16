"""Compatibility shim — the MCP SSE bridge now lives in ``hive.mcp.bridge``.

Kept as the systemd entrypoint (``python /srv/agent-bus/mcp_bridge.py``) and for
existing imports. All real code is in the package.
"""

import os

import uvicorn

from hive.mcp.bridge import *  # noqa: F401,F403
from hive.mcp.bridge import app  # noqa: F401

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "5006")))
