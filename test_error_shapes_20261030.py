"""FB44: one error envelope for every route in the OpenAPI schema.

The contract (`docs/API_REFERENCE.md`, section "Pagination and errors") says a
non-2xx JSON body is either `{"detail": "<string>"}` (a route's own refusal) or
`{"detail": [{loc, msg, type, ...}]}` (the framework's query/body validation),
and that no body carries a traceback, a filesystem path, a SQL fragment or a
driver message.

This file proves that for EVERY route in `app.openapi()`, not for a hand-picked
few:

* `test_every_schema_route_is_exercised` — walks the schema; a route added
  without a refusal probe fails here rather than escaping the contract.
* `test_every_error_body_uses_the_standard_detail_envelope` — every 4xx from
  those probes has one of the two shapes above.
* `test_no_error_body_leaks_paths_tracebacks_or_sql` — the same bodies contain
  none of the needles a stack/SQL/path leak would leave.
* `test_*_is_reachable` — each of 400/401/403/404/409/422 is triggered on
  purpose and carries a detail, so "one shape" is not proved only on 404s.

Documented exceptions, not violations: `GET /health` answers 503 with the
health document (a `detail` field plus `ts`/`service`/`version`/`status`), and
`POST /mcp` is JSON-RPC. The MCP route's two envelope violations are real
defects and are pinned as strict xfails at the bottom.
"""

import asyncio
import contextlib
import json
import os
import re
import tempfile
from pathlib import Path
from unittest import mock

import httpx
import pytest

import agent_bus
from hive.bus import deps

#: Routes this contract cannot probe with a refused request, with the reason.
EXEMPT = {
    ("GET", "/"): "the dashboard: text/html, not a JSON route",
    ("GET", "/v1/events"): "server-sent events: an open stream, not a JSON body",
    ("POST", "/mcp"): 'JSON-RPC: documented `{"error": ...}` exception, pinned below',
    # 503 carries the health document rather than an error envelope; it has its
    # own shape test instead of being silently exempt from one.
    ("GET", "/health"): "503 is the health document (ts/service/version/status/detail), not an error",
}

LEAK_NEEDLES = (
    "Traceback (most recent call last)",
    'File "',
    "site-packages",
    "/Users/",
    "/home/",
    "/srv/",
    '.py", line',
    "SELECT ",
    "INSERT ",
    "UPDATE ",
    "sqlite3.",
    "ORA-",
    "psycopg",
    "asyncpg",
    "pydantic_core",
)


@contextlib.contextmanager
def bus():
    """A throwaway SQLite bus, no network, no fleet, no admin token."""
    with tempfile.TemporaryDirectory() as tmp:
        env = {
            "AGENT_BUS_DB": str(Path(tmp) / "bus.db"),
            "HIVE_BACKEND": "sqlite",
            "MNEMOS_URL": "",
            "MNEMOS_TOKEN": "",
            "HIVE_SHARED_SECRET": "",
            "HIVE_BUS_ADMIN_TOKEN": "",
        }
        with mock.patch.dict(os.environ, env), mock.patch.object(deps, "_HIVE_REPO", None):
            asyncio.run(deps.hive_repo().init())
            yield


def run(scenario):
    async def main():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://test"
        ) as client:
            return await scenario(client)

    return asyncio.run(main())


def schema_routes():
    """Every (METHOD, path) the app publishes, minus the GET/HEAD alias of one."""
    out = set()
    for path, ops in agent_bus.app.openapi()["paths"].items():
        for method in ops:
            if method.lower() in ("head", "options"):
                continue
            out.add((method.upper(), path))
    return out


def shape_of(response):
    """Classify an error body: the envelope kinds the contract allows, or the fault."""
    try:
        body = response.json()
    except (ValueError, json.JSONDecodeError):
        return "not-json"
    if not isinstance(body, dict):
        return "not-an-object"
    keys = set(body)
    if keys == {"detail"}:
        detail = body["detail"]
        if isinstance(detail, str) and detail.strip():
            return "detail-string"
        if (
            isinstance(detail, list)
            and detail
            and all(isinstance(item, dict) and {"loc", "msg", "type"} <= set(item) for item in detail)
        ):
            return "detail-list"
        return "detail-malformed"
    if "error" in keys:
        return "error-key:" + ",".join(sorted(keys))
    return "other:" + ",".join(sorted(keys))


def _substitute(path):
    """A concrete path whose ids do not exist: the refusal path for every route."""

    def repl(match):
        name = match.group(1)
        return "urn:agent:ghost:00000000-0000-0000-0000-000000000000" if "urn" in name else "no-such-id"

    return re.sub(r"\{([^}]+)\}", repl, path)


def refusal(method, path):
    """A request this route must refuse, as `(method, url, kwargs)`."""
    url = _substitute(path)
    if method == "GET":
        return method, url, {}
    if method == "DELETE":
        return method, url, {}
    if path.endswith("/heartbeat"):
        return method, url, {"json": {}}
    return method, url, {"json": {}}


async def _register(client, kind="opencode", capabilities=("code",)):
    response = await client.post(
        "/v1/agents/register",
        json={
            "kind": kind,
            "host": "contract",
            "runtime": kind,
            "capabilities": list(capabilities),
            "cost_tier": "B",
            "auth_method": "api",
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["urn"]


_submitted = 0


async def _job(client, submitter, kind="e2e:contract"):
    global _submitted
    _submitted += 1
    response = await client.post(
        "/v1/jobs",
        json={"kind": kind, "description": f"contract-{_submitted}", "submitter_urn": submitter},
        headers={"X-Hive-Actor": submitter},
    )
    assert response.status_code in (200, 201), response.text
    return response.json()["id"]


# ── the schema walk ─────────────────────────────────────────────────────────


def test_every_schema_route_is_exercised():
    """No route may be added to the API without a refused request in this contract."""
    routes = schema_routes()
    assert len(routes) > 30, routes
    # The walk builds a refusal from the schema itself, so what this pins is that
    # every route ANSWERED: a route removed from the app, or one whose parameter
    # substitution is malformed, shows up here instead of silently escaping.
    with bus():
        seen = run(lambda client: _walk(client, routes))
    assert set(seen) == routes, f"unprobed: {sorted(routes - set(seen))}"
    answered = {route: code for route, code in seen.items() if route not in EXEMPT}
    assert all(code > 0 for code in answered.values()), seen


async def _walk(client, routes):
    seen = {}
    for method, path in sorted(routes):
        if (method, path) in EXEMPT:
            seen[(method, path)] = 0
            continue
        m, url, kwargs = refusal(method, path)
        response = await client.request(m, url, **kwargs)
        seen[(method, path)] = response.status_code
    return seen


def test_every_error_body_uses_the_standard_detail_envelope():
    """Every refusal from every route is `{"detail": str}` or `{"detail": [validation]}`."""
    with bus():
        offenders = run(_collect_bad_shapes)
    assert offenders == [], "routes with a non-standard error body:\n" + "\n".join(offenders)


async def _collect_bad_shapes(client):
    offenders = []
    for method, path in sorted(schema_routes()):
        if (method, path) in EXEMPT:
            continue
        m, url, kwargs = refusal(method, path)
        response = await client.request(m, url, **kwargs)
        if response.status_code < 400:
            continue
        shape = shape_of(response)
        if shape not in ("detail-string", "detail-list"):
            offenders.append(f"{m} {path} -> {response.status_code} {shape}: {response.text[:160]}")
        if response.status_code >= 500:
            offenders.append(f"{m} {path} -> {response.status_code} is a server fault, not a refusal")
    return offenders


def test_no_error_body_leaks_paths_tracebacks_or_sql():
    """The refusal bodies carry no internals."""
    with bus():
        leaks = run(_collect_leaks)
    assert leaks == [], "error bodies leaking internals:\n" + "\n".join(leaks)


async def _collect_leaks(client):
    leaks = []
    for method, path in sorted(schema_routes()):
        if (method, path) in EXEMPT:
            continue
        m, url, kwargs = refusal(method, path)
        response = await client.request(m, url, **kwargs)
        if response.status_code < 400:
            continue
        text = response.text
        for needle in LEAK_NEEDLES:
            if needle in text:
                leaks.append(f"{m} {path} -> {response.status_code} contains {needle!r}: {text[:160]}")
                break
    return leaks


# ── each documented code, on purpose ────────────────────────────────────────


def test_400_is_reachable_and_names_the_valid_values():
    with bus():

        async def scenario(client):
            for url in ("/v1/jobs?status=bogus", "/v1/jobs?review_state=bogus"):
                response = await client.get(url)
                assert response.status_code == 400, (url, response.status_code, response.text)
                assert shape_of(response) == "detail-string"
                assert "valid:" in response.json()["detail"], response.json()

        run(scenario)


def test_401_is_reachable_on_a_mutating_route():
    with bus():

        async def scenario(client):
            response = await client.post("/v1/agents/urn:agent:x:y/deregister")
            assert response.status_code == 401, response.text
            assert shape_of(response) == "detail-string"

        run(scenario)


def test_403_is_reachable_for_an_actor_that_does_not_own_the_row():
    with bus():

        async def scenario(client):
            owner = await _register(client, "opencode")
            stranger = await _register(client, "zeroclaw")
            response = await client.post(f"/v1/agents/{owner}/deregister", headers={"X-Hive-Actor": stranger})
            assert response.status_code == 403, response.text
            assert shape_of(response) == "detail-string"
            assert stranger not in response.json()["detail"], "the refusal must not echo the caller's identity"

        run(scenario)


def test_404_is_reachable_for_an_unknown_id():
    with bus():

        async def scenario(client):
            for url in ("/v1/jobs/no-such-id", "/v1/jobs/no-such-id/preview", "/v1/repos/no-such-id"):
                response = await client.get(url)
                assert response.status_code == 404, (url, response.status_code, response.text)
                assert shape_of(response) == "detail-string"

        run(scenario)


def test_409_is_reachable_for_an_illegal_transition():
    with bus():

        async def scenario(client):
            worker = await _register(client)
            jid = await _job(client, worker)
            headers = {"X-Hive-Actor": worker}
            claimed = await client.post(f"/v1/jobs/next?agent_urn={worker}", headers=headers)
            assert claimed.status_code == 200, claimed.text
            assert claimed.json()["id"] == jid, claimed.text
            done = await client.patch(f"/v1/jobs/{jid}", json={"status": "done", "claimed_by": worker}, headers=headers)
            assert done.status_code == 200, done.text
            reopen = await client.patch(
                f"/v1/jobs/{jid}", json={"status": "running", "claimed_by": worker}, headers=headers
            )
            assert reopen.status_code == 409, reopen.text
            assert shape_of(reopen) == "detail-string"
            assert "transition" in reopen.json()["detail"], reopen.json()

        run(scenario)


def test_422_is_reachable_for_a_bad_body_and_a_bad_query():
    with bus():

        async def scenario(client):
            body = await client.post("/v1/jobs", json={})
            assert body.status_code == 422, body.text
            assert shape_of(body) == "detail-list"
            assert {item["loc"][-1] for item in body.json()["detail"]} >= {"kind", "submitter_urn"}

            query = await client.get("/v1/jobs?limit=abc")
            assert query.status_code == 422, query.text
            assert shape_of(query) == "detail-list"
            assert query.json()["detail"][0]["loc"] == ["query", "limit"]

            out_of_range = await client.get(
                "/v1/admin/jobs/no-such-id/history?limit=501",
                headers={"X-Hive-Actor": await _register(client)},
            )
            assert out_of_range.status_code in (404, 422), out_of_range.text
            if out_of_range.status_code == 422:
                assert shape_of(out_of_range) == "detail-string"

        run(scenario)


def test_health_503_keeps_the_health_document_shape():
    """The documented exception: 503 carries the health document, not a bare error."""
    with bus():

        async def scenario(client):
            response = await client.get("/health")
            assert response.status_code in (200, 503), response.text
            body = response.json()
            assert {"ts", "service", "version", "status"} <= set(body), body
            assert isinstance(body["ts"], (int, float))

        run(scenario)


# ── findings ────────────────────────────────────────────────────────────────

MCP_FINDING = (
    'FB44-FINDING-1: POST /mcp answers `{"error": ...}` instead of the `{"detail": ...}` every other route uses'
)


@pytest.mark.xfail(strict=True, reason=MCP_FINDING + " (unknown method, 404)")
def test_mcp_unknown_method_uses_the_detail_envelope():
    with bus():

        async def scenario(client):
            response = await client.post("/mcp", json={"method": "nope", "params": {}})
            assert response.status_code == 404, response.text
            assert shape_of(response) == "detail-string", response.text

        run(scenario)


@pytest.mark.xfail(strict=True, reason=MCP_FINDING + " (invalid params, 400)")
def test_mcp_invalid_params_uses_the_detail_envelope():
    with bus():

        async def scenario(client):
            response = await client.post("/mcp", json={"method": "job.create", "params": {"kind": 5}})
            assert response.status_code == 400, response.text
            assert shape_of(response) == "detail-string", response.text

        run(scenario)


@pytest.mark.xfail(
    strict=True,
    reason="FB44-FINDING-2: POST /mcp catches every exception and answers 400 with str(e), so an internal "
    "fault is reported as a client mistake and its message (paths included) reaches the caller",
)
def test_mcp_internal_fault_is_not_a_400_with_its_message():
    with bus():

        async def scenario(client):
            def boom(**kwargs):
                raise RuntimeError("connection lost: /srv/agent-bus/bus.db is locked")

            with mock.patch("hive.bus.routers.mcp.list_jobs", boom):
                response = await client.post("/mcp", json={"method": "job.list", "params": {}})
            assert response.status_code >= 500 or "/srv/" not in response.text, response.text
            assert response.status_code != 400, response.text

        run(scenario)
