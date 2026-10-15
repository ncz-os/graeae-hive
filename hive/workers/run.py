"""Unified harness worker loop.

One loop for every harness. The per-harness differences live in the Phase-B
adapters (`hive/harness/*`): the loop only does register → heartbeat → claim →
execute → complete/fail, and folds telemetry into the job result.

Transport: REST via the adapter (sync urllib). MCP is the *interactive* surface
(a human session calling hive_job_* tools), not the worker mechanism. WSS is not
used (the fleet dropped it).

Usage:
    from hive.harness import get_harness_adapter
    from hive.workers.run import HarnessWorker

    adapter = get_harness_adapter("codex")
    adapter._executor = codex_executor(binary="codex")   # CLI hook
    HarnessWorker(adapter, host="hydra").run_forever()
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from hive.harness.base import HarnessRegistration
from hive.harness.generic import GenericAdapter

logger = logging.getLogger("hive.worker")


class HarnessWorker:
    """Drive one harness adapter as an autonomous worker."""

    def __init__(
        self,
        adapter: GenericAdapter,
        *,
        urn: str | None = None,
        host: str = "",
        capabilities: list[str] | None = None,
        version: str = "0.1.0",
        poll_interval: float = 30.0,
        heartbeat_interval: float = 15.0,
        claim_timeout_retries: int = 3,
    ) -> None:
        self.adapter = adapter
        self.urn = urn
        self.host = host
        self.capabilities = capabilities or []
        self.version = version
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self.claim_timeout_retries = claim_timeout_retries
        self._last_heartbeat = 0.0
        self._last_message_ts = 0.0
        # Optional peer-message callback (awareness/coordination): called with
        # each new message addressed to this worker (+ broadcasts).
        self.on_message: Callable[[dict[str, Any]], None] | None = None

    # -- lifecycle ----------------------------------------------------------

    def register(self) -> str:
        if self.urn:
            return self.urn
        reg = HarnessRegistration(
            harness_kind=self.adapter.harness_kind,
            harness_version=self.version,
            capabilities=self.capabilities,
            cost_tier=self.adapter.cost_tier,
        )
        self.urn = self.adapter.register(reg)
        self._last_heartbeat = time.time()
        logger.info("registered %s as %s", self.adapter.harness_kind, self.urn)
        return self.urn

    def _maybe_heartbeat(self, *, force: bool = False) -> None:
        now = time.time()
        if force or now - self._last_heartbeat >= self.heartbeat_interval:
            try:
                self.adapter.heartbeat(self.urn, {"status": "online", "host": self.host})
                self._last_heartbeat = now
            except Exception as e:  # noqa: BLE001 - keep the loop alive
                logger.warning("heartbeat failed for %s: %s", self.urn, e)

    # -- awareness / communication / coordination ---------------------------

    def announce(self, topic: str, payload: dict[str, Any], *, to_urn: str | None = None) -> None:
        """Publish a Hive message (to_urn=None broadcasts) so peers can react."""
        try:
            self.adapter.publish_message(from_urn=self.urn, topic=topic, payload=payload, to_urn=to_urn)
        except Exception as e:  # noqa: BLE001
            logger.warning("announce(%s) failed: %s", topic, e)

    def poll_messages(self) -> list[dict[str, Any]]:
        """Fetch messages for this worker (+ broadcasts) since the last poll.

        Each new message is delivered to ``self.on_message`` if set. Returns the
        raw list. Never raises — a message-poll failure must not stop work.
        """
        try:
            msgs = self.adapter.list_messages(
                to_urn=self.urn,
                since=self._last_message_ts or None,
                limit=50,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("message poll failed: %s", e)
            return []
        for m in msgs:
            self._last_message_ts = max(self._last_message_ts, float(m.get("ts") or 0))
            if self.on_message is not None:
                try:
                    self.on_message(m)
                except Exception:  # noqa: BLE001
                    logger.exception("on_message handler raised for %r", m.get("id"))
        return msgs

    # -- one unit of work ---------------------------------------------------

    def run_once(self) -> bool:
        """Claim and run one job.

        Returns True if a job was CLAIMED — whether it then succeeded or failed
        (a handled job means the loop should poll again immediately, no sleep).
        Returns False only when nothing was claimable.
        """
        self._maybe_heartbeat()
        job = self.adapter.claim(self.urn)
        if not job:
            return False
        job_id = job.get("id")
        if not job_id:
            logger.warning("claim returned a job without id: %r", job)
            return False
        logger.info("claimed %s (%s)", job_id, job.get("kind"))
        try:
            result = self.adapter.execute(self.urn, job)
            self.adapter.complete(self.urn, job_id, result)
            logger.info("completed %s", job_id)
        except Exception as e:  # noqa: BLE001 - report failure, don't crash the loop
            logger.exception("job %s failed", job_id)
            try:
                self.adapter.fail(self.urn, job_id, str(e))
            except Exception as e2:  # noqa: BLE001
                logger.error("fail-report for %s also failed: %s", job_id, e2)
        return True

    def run_forever(self, *, max_iterations: int | None = None) -> None:
        """Poll-claim-execute until stopped. Sleeps ``poll_interval`` when idle."""
        self.register()
        self._maybe_heartbeat(force=True)
        self.announce(
            "worker.online",
            {"host": self.host, "capabilities": self.capabilities, "urn": self.urn},
        )
        iterations = 0
        while max_iterations is None or iterations < max_iterations:
            iterations += 1
            self.poll_messages()
            try:
                handled = self.run_once()
            except KeyboardInterrupt:
                logger.info("worker interrupted; exiting")
                return
            except Exception as e:  # noqa: BLE001 - never let the loop die
                logger.exception("run_once error: %s", e)
                handled = False
            if not handled:
                time.sleep(self.poll_interval)


def main(adapter_factory: Callable[[], GenericAdapter], argv: list[str] | None = None) -> int:
    """Thin CLI entrypoint: build an adapter, run the loop.

    Each harness's console script passes its own ``adapter_factory`` (codex,
    claude, opencode, zoder). Kept dependency-free so it works under any harness.
    """
    import argparse

    ap = argparse.ArgumentParser(description="Hive harness worker")
    ap.add_argument("--urn", default=None, help="reuse an existing agent URN (skip register)")
    ap.add_argument("--host", default="", help="hostname for registration/heartbeat")
    ap.add_argument("--poll", type=float, default=30.0)
    ap.add_argument("--heartbeat", type=float, default=15.0)
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    worker = HarnessWorker(
        adapter_factory(),
        urn=args.urn,
        host=args.host,
        poll_interval=args.poll,
        heartbeat_interval=args.heartbeat,
    )
    worker.run_forever()
    return 0
