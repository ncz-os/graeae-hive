"""GenericAdapter — the fallback/base harness adapter.

This is the worker-side contract: a harness worker instantiates its adapter to
register with the bus, heartbeat, claim, execute, and report. The four
first-class harnesses (zoder, opencode, codex, claude) subclass this and only
override their capability declaration + engine hook.

Transport: synchronous HTTP via ``urllib`` — mirrors the live worker pattern in
``system_watcher.py``. Harness workers are CLI/subprocess processes, so sync is
correct here; the *bus* (agent_bus.py) stays async and never instantiates these.

Engine wiring (actually running the harness on a job) is Phase D. ``execute``
delegates to an injected ``executor`` callable so this module has no opinion
about how any given harness runs; without one it fails loudly, never silently.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any

from hive.harness.base import HarnessCapabilities, HarnessRegistration

DEFAULT_BUS_URL = "http://127.0.0.1:5005"

# Map the adapter auth_scheme vocabulary onto the bus's AUTH_METHODS
# ({"subscription","api","free","unknown"}). See agent_bus.AUTH_METHODS.
_AUTH_METHOD_MAP: dict[str, str] = {
    "bearer": "api",  # pay-per-token
    "api_key": "api",
    "oauth": "subscription",  # OAuth subscription (e.g. Claude Pro)
    "none": "free",
}


def _host_from_urn(urn: str) -> str | None:
    """Extract host from ``urn:agent:<kind>:<host>:<session>`` (None if not that shape)."""
    parts = urn.split(":")
    if len(parts) >= 5 and parts[0] == "urn" and parts[1] == "agent":
        return parts[3]
    return None


def job_prompt(job: dict[str, Any]) -> str:
    """Render a job into the prompt string a harness CLI is invoked with."""
    head = job.get("kind") or "task"
    desc = job.get("description") or ""
    return f"[hive job {job.get('id')}] {head}\n\n{desc}".strip()


# transport(method, url, token, body) -> (status_code, parsed_json_or_None)
Transport = Callable[[str, str, str | None, dict | None], tuple[int, dict | None]]


def _urllib_transport(method: str, url: str, token: str | None, body: dict | None) -> tuple[int, dict | None]:
    """Default sync transport (system_watcher.py pattern)."""
    data = json.dumps(body).encode() if body is not None else None
    headers = {"content-type": "application/json"}
    if token:
        headers["authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:  # noqa: S310
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, None
    except Exception:
        return 0, None


class GenericAdapter:
    """Base worker-side harness adapter. Subclass to specialize."""

    harness_kind: str = "generic"
    priority: int = 99
    cost_tier: str = "metered"
    auth_scheme: str = "bearer"
    base_capabilities: frozenset[str] = frozenset()

    def __init__(
        self,
        *,
        bus_url: str | None = None,
        token: str | None = None,
        transport: Transport | None = None,
        extra_capabilities: list[str] | None = None,
        executor: Callable[[str, dict[str, Any]], dict[str, Any]] | None = None,
    ) -> None:
        self.bus_url = (bus_url or os.environ.get("HIVE_URL") or DEFAULT_BUS_URL).rstrip("/")
        self.token = token if token is not None else os.environ.get("HIVE_BUS_TOKEN")
        self._transport = transport or _urllib_transport
        self._extra_capabilities = frozenset(extra_capabilities or ())
        self._executor = executor

    # -- capability contract --------------------------------------------------

    def capabilities(self) -> HarnessCapabilities:
        return HarnessCapabilities(
            capabilities=self.base_capabilities | self._extra_capabilities,
            cost_tier=self.cost_tier,
            priority=self.priority,
            auth_scheme=self.auth_scheme,
        )

    def is_enabled(self) -> bool:
        """Whether this harness may run. Default True; codex overrides."""
        return True

    def auth_method(self) -> str:
        """Bus AUTH_METHODS value for this adapter (derived from auth_scheme)."""
        return _AUTH_METHOD_MAP.get(self.auth_scheme, "unknown")

    # -- transport ------------------------------------------------------------

    def _call(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict | None]:
        return self._transport(method, f"{self.bus_url}{path}", self.token, body)

    # -- bus protocol (worker-side) ------------------------------------------

    def register(self, registration: HarnessRegistration) -> str:
        """POST /v1/agents/register. Returns the assigned agent URN."""
        body = {
            "kind": registration.harness_kind,
            "runtime": registration.harness_kind,
            "version": registration.harness_version,
            "capabilities": list(
                self.base_capabilities | self._extra_capabilities | frozenset(registration.capabilities)
            ),
            "cost_tier": registration.cost_tier,
            "auth_method": self.auth_method(),
            "metadata": registration.metadata,
        }
        status, resp = self._call("POST", "/v1/agents/register", body)
        if status != 200 or not resp or not resp.get("urn"):
            raise RuntimeError(f"register failed for harness {self.harness_kind!r}: status={status} resp={resp!r}")
        return str(resp["urn"])

    def heartbeat(self, urn: str, payload: dict[str, Any]) -> dict[str, Any]:
        """POST /v1/agents/heartbeat. A 200-with-body or 204-empty both succeed."""
        body = {"urn": urn, **payload}
        status, resp = self._call("POST", "/v1/agents/heartbeat", body)
        if status not in (200, 204):
            raise RuntimeError(f"heartbeat failed for {urn!r}: status={status}")
        return resp or {}

    def claim(self, urn: str, jobs: list[dict[str, Any]] | None = None) -> dict[str, Any] | None:
        """POST /v1/jobs/next?agent_urn=<urn>. Returns a claimed job or None.

        ``jobs`` is accepted for Protocol conformance (some transports offer a
        candidate list); the canonical bus dequeues server-side, so it is unused
        by the default transport.
        """
        _ = jobs
        q = urllib.parse.quote(urn, safe="")
        status, resp = self._call("POST", f"/v1/jobs/next?agent_urn={q}")
        if status == 204 or not resp:
            return None
        if status != 200:
            raise RuntimeError(f"claim failed for {urn!r}: status={status}")
        return resp

    def execute(self, urn: str, job: dict[str, Any]) -> dict[str, Any]:
        """Run the harness's native engine on ``job`` and return a result dict.

        Phase D injects the real engine via ``executor``. Without one, fail
        loudly rather than reporting a bogus success.
        """
        if self._executor is None:
            raise NotImplementedError(
                f"{type(self).__name__} has no executor wired; pass executor=<callable(urn, job) -> result> (Phase D)."
            )
        return self._executor(urn, job)

    def complete(self, urn: str, job_id: str, result: dict[str, Any]) -> None:
        """PATCH /v1/jobs/<id> status=done with result."""
        status, _ = self._call("PATCH", f"/v1/jobs/{job_id}", {"status": "done", "result": result, "claimed_by": urn})
        if status != 200:
            raise RuntimeError(f"complete failed for {job_id!r}: status={status}")

    def fail(self, urn: str, job_id: str, error: str) -> None:
        """PATCH /v1/jobs/<id> status=failed."""
        status, _ = self._call(
            "PATCH",
            f"/v1/jobs/{job_id}",
            {"status": "failed", "result": {"error": error}, "claimed_by": urn},
        )
        if status != 200:
            raise RuntimeError(f"fail-report failed for {job_id!r}: status={status}")

    def status(self, urn: str) -> dict[str, Any]:
        """Return this agent's row (model/provider/caps/last_heartbeat/...).

        The bus ``GET /v1/agents`` takes ``host`` but **not** ``urn`` (verified:
        its params are status/kind/host/runtime/pid/cost_tier/include_offline).
        A URN encodes its host, so narrow server-side by host and match the exact
        URN client-side. Absent ⇒ {}. An unexpected response shape raises rather
        than being masked as "not found".
        """
        query = "/v1/agents?include_offline=true"
        host = _host_from_urn(urn)
        if host:
            query += f"&host={urllib.parse.quote(host, safe='')}"
        status, resp = self._call("GET", query)
        if status not in (200, 204):
            raise RuntimeError(f"status failed for {urn!r}: status={status}")
        if resp is None:
            return {}
        if not isinstance(resp, dict):
            raise RuntimeError(f"status for {urn!r}: unexpected response type {type(resp).__name__}")
        for agent in resp.get("agents", []):
            if isinstance(agent, dict) and agent.get("urn") == urn:
                return agent
        return {}

    # -- agent-to-agent: awareness / communication / coordination --------------

    def roster(self, *, include_offline: bool = False) -> list[dict[str, Any]]:
        """List registered agents so a worker can see its peers (awareness)."""
        query = "/v1/agents?include_offline=true" if include_offline else "/v1/agents"
        status, resp = self._call("GET", query)
        if status not in (200, 204):
            raise RuntimeError(f"roster failed: status={status}")
        return (resp or {}).get("agents", [])

    def publish_message(
        self,
        *,
        from_urn: str,
        topic: str,
        payload: dict[str, Any],
        to_urn: str | None = None,
        in_reply_to: str | None = None,
    ) -> dict[str, Any]:
        """Publish a Hive message. ``to_urn=None`` broadcasts to every worker."""
        body = {
            "from_urn": from_urn,
            "topic": topic,
            "payload": payload,
            "to_urn": to_urn,
            "in_reply_to": in_reply_to,
        }
        status, resp = self._call("POST", "/v1/messages", body)
        if status != 200:
            raise RuntimeError(f"publish_message failed: status={status}")
        return resp or {}

    def list_messages(
        self,
        *,
        to_urn: str | None = None,
        topic: str | None = None,
        since: float | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Poll messages addressed to this worker (+ broadcasts since ``since``)."""
        params = {
            k: v for k, v in {"to_urn": to_urn, "topic": topic, "since": since, "limit": limit}.items() if v is not None
        }
        query = urllib.parse.urlencode(params)
        status, resp = self._call("GET", f"/v1/messages?{query}")
        if status not in (200, 204):
            raise RuntimeError(f"list_messages failed: status={status}")
        return (resp or {}).get("messages", [])

    # -- delegation + working-set awareness -----------------------------------

    def submit_job(
        self,
        *,
        submitter_urn: str,
        kind: str,
        description: str | None = None,
        parent_job_id: str | None = None,
        required_capabilities: list[str] | None = None,
        eligible_kinds: list[str] | None = None,
        project: str | None = None,
        max_cost_tier: str = "B",
        **extra: Any,
    ) -> dict[str, Any]:
        """Submit a job for another worker to claim (offload/delegation).

        Routing is capability-based: set ``required_capabilities`` /
        ``eligible_kinds`` to steer it, and ``parent_job_id`` to record the
        offload lineage. The bus arbitrates the claim (no double-work).
        """
        body: dict[str, Any] = {
            "submitter_urn": submitter_urn,
            "kind": kind,
            "description": description,
            "parent_job_id": parent_job_id,
            "required_capabilities": required_capabilities,
            "eligible_kinds": eligible_kinds,
            "project": project,
            "max_cost_tier": max_cost_tier,
        }
        body.update(extra)
        status, resp = self._call("POST", "/v1/jobs", {k: v for k, v in body.items() if v is not None})
        if status != 200:
            raise RuntimeError(f"submit_job failed: status={status}")
        return resp or {}

    def peers_working_set(self) -> dict[str, dict[str, Any]]:
        """Map urn -> advertised ``working_set`` for every peer that reports one.

        A working_set is arbitrary metadata a worker advertises on heartbeat
        (e.g. ``{"repo": "graeae-hive-mind", "languages": ["python"],
        "architectures": ["arm64"], "files": ["agent_bus.py"]}``). Lets a worker
        see what files/architectures/languages peers are touching before it
        edits or offloads.
        """
        out: dict[str, dict[str, Any]] = {}
        for agent in self.roster():
            meta = agent.get("metadata") or {}
            ws = meta.get("working_set") if isinstance(meta, dict) else None
            if isinstance(ws, dict):
                out[agent.get("urn", "")] = ws
        return out
