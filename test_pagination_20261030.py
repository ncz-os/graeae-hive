"""FB44: the pagination contract for every list route, at 3x its maximum page.

The contract is stated in `docs/API_REFERENCE.md`, section "Pagination and
errors". A page parameter is an integer query parameter named `limit` (or
`top_n` for the two aggregates):

* absent, `0` and negative all mean the route's default — never "no rows";
* above the max is clamped to the max, never rejected;
* not an integer is a 422 from the framework's query validation;
* ordering is deterministic, and `limit=N` is a prefix of `limit=M > N`;
* only `/v1/events/log` has a cursor, and its `last_id` is the next `since_id`.

Rows are seeded directly through the repository's transaction (3 000 jobs,
messages and events for a max of 1 000) so the cap is exercised rather than
assumed: a `limit` bug that silently returns the default, drops the last page
or restarts from the top is visible here.
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

SEED = 3000  # 3x the 1000-row max on jobs/messages/events
DEFAULT = 100
DOC = Path(__file__).resolve().parent / "docs" / "API_REFERENCE.md"
DOC_ROW = re.compile(r"^\| `(GET|POST|PATCH|DELETE) ([^`]+)` \| `(limit|top_n)` \| (\d+) \| (\d+) \|", re.M)


def documented(route):
    """`(param, default, max)` for a list route, read from the generated reference."""
    rows = {
        f"{m.group(1)} {m.group(2)}": (m.group(3), int(m.group(4)), int(m.group(5)))
        for m in DOC_ROW.finditer(DOC.read_text())
    }
    return rows[route]


@contextlib.contextmanager
def bus():
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


async def seed_jobs(count=SEED):
    """Direct rows: 3 000 queued jobs, started_at ascending so order is checkable."""
    base = 1_800_000_000.0
    async with deps.hive_repo().connection() as tx:
        await tx.executemany(
            "INSERT INTO jobs (id, submitter_urn, kind, description, priority, status, started_at, "
            "created_at, last_update_at, max_retries) VALUES (?,?,?,?,?,?,?,?,?,?)"
            if False
            else "INSERT INTO jobs (id, submitter_urn, kind, description, priority, status, "
            "started_at, max_retries) VALUES (?,?,?,?,?,?,?,?)",
            [
                (f"job-{i:05d}", "urn:agent:seed", "e2e:page", f"page-{i}", 5, "queued", base + i, 2)
                for i in range(count)
            ],
        )
        await tx.commit()


async def seed_messages(count=SEED):
    base = 1_800_000_000.0
    async with deps.hive_repo().connection() as tx:
        await tx.executemany(
            "INSERT INTO messages (id, from_urn, to_urn, topic, payload, ts) VALUES (?,?,?,?,?,?)",
            [
                (f"msg-{i:05d}", "urn:agent:seed", None, "seed.page", json.dumps({"i": i}), base + i)
                for i in range(count)
            ],
        )
        await tx.commit()


async def seed_events(count=SEED):
    async with deps.hive_repo().connection() as tx:
        await tx.executemany(
            "INSERT INTO events (ts, kind, payload, agent_urn) VALUES (?,?,?,?)",
            [(1_800_000_000.0 + i, "seed.page", json.dumps({"i": i}), None) for i in range(count)],
        )
        await tx.commit()


async def seed_all():
    await seed_jobs()
    await seed_messages()
    await seed_events()


def _ids(payload, key):
    return [row["id"] for row in payload[key]]


# ── defaults, zero, negative ────────────────────────────────────────────────


def test_absent_zero_and_negative_limit_all_mean_the_default():
    """`limit=0` is the default page, not an empty page and not unbounded."""
    with bus():

        async def scenario(client):
            await seed_all()
            for query in ("", "?limit=0", "?limit=-5"):
                response = await client.get("/v1/jobs" + query)
                assert response.status_code == 200, response.text
                body = response.json()
                assert body["count"] == DEFAULT, (query, body["count"])
                assert body["total"] == SEED, (query, body["total"])

            for query in ("", "?limit=0"):
                messages = (await client.get("/v1/messages" + query)).json()
                assert messages["count"] == DEFAULT, query
                events = (await client.get("/v1/events/log" + query)).json()
                assert events["count"] == DEFAULT, query

        run(scenario)


# ── the cap ─────────────────────────────────────────────────────────────────


def test_jobs_limit_is_clamped_to_the_max_and_total_still_reports_every_row():
    """The max the reference promises is the max the route honours, at 3x that many rows."""
    _, default, max_limit = documented("GET /v1/jobs")
    assert (default, max_limit) == (100, 1000)
    with bus():

        async def scenario(client):
            await seed_all()
            response = await client.get("/v1/jobs?limit=1000000")
            assert response.status_code == 200, response.text
            body = response.json()
            assert body["count"] == max_limit, body["count"]
            assert len(body["jobs"]) == max_limit
            assert body["total"] == SEED, "the cap is a page size, not a filter"

            exact = (await client.get(f"/v1/jobs?limit={max_limit}")).json()
            assert [row["id"] for row in exact["jobs"]] == [row["id"] for row in body["jobs"]], (
                "asking for more than the max must return exactly the max page"
            )

        run(scenario)


def test_messages_and_events_limits_are_clamped_to_the_max():
    with bus():

        async def scenario(client):
            await seed_all()
            _, _, messages_max = documented("GET /v1/messages")
            messages = (await client.get("/v1/messages?limit=1000000")).json()
            assert messages["count"] == messages_max, messages["count"]
            _, _, events_max = documented("GET /v1/events/log")
            events = (await client.get("/v1/events/log?limit=1000000")).json()
            assert events["count"] == events_max, events["count"]
            assert len(events["events"]) == events_max

        run(scenario)


def test_the_reference_documents_every_paged_route_with_its_real_default():
    """A new list route with a page parameter cannot ship undocumented or with a stale default."""
    doc = DOC.read_text()
    assert "## Pagination and errors" in doc, "the generated section is missing"
    rows = {f"{m.group(1)} {m.group(2)}": (m.group(3), int(m.group(4))) for m in DOC_ROW.finditer(doc)}

    paged = {}
    for path, ops in agent_bus.app.openapi()["paths"].items():
        for method, node in ops.items():
            for param in node.get("parameters", []):
                if param.get("in") != "query" or param.get("name") not in ("limit", "top_n"):
                    continue
                key = f"{method.upper()} {path}"
                paged[key] = (param["name"], param.get("schema", {}).get("default"))

    missing = sorted(set(paged) - set(rows))
    assert missing == [], f"paged routes missing from the reference: {missing}"
    for key, (name, default) in paged.items():
        assert rows[key][0] == name, (key, rows[key], name)
        assert rows[key][1] == default, f"{key}: the reference says {rows[key][1]}, the schema says {default}"


# ── ordering and prefix stability ───────────────────────────────────────────


def test_a_smaller_page_is_a_prefix_of_a_larger_page_on_every_list_route():
    with bus():

        async def scenario(client):
            await seed_all()
            for url, key in (
                ("/v1/jobs", "jobs"),
                ("/v1/messages", "messages"),
                ("/v1/events/log", "events"),
            ):
                small = _ids((await client.get(f"{url}?limit=7")).json(), key)
                large = _ids((await client.get(f"{url}?limit=50")).json(), key)
                assert len(small) == 7, (url, small)
                assert small == large[:7], f"{url}: limit=7 is not a prefix of limit=50"
                again = _ids((await client.get(f"{url}?limit=7")).json(), key)
                assert again == small, f"{url}: the same request returned a different order"

        run(scenario)


def test_messages_are_newest_first():
    """`ts DESC` is the documented order, and the seeded ts is monotonically increasing."""
    with bus():

        async def scenario(client):
            await seed_messages(count=5)
            body = (await client.get("/v1/messages?limit=5")).json()
            stamps = [row["ts"] for row in body["messages"]]
            assert stamps == sorted(stamps, reverse=True), stamps

        run(scenario)


def test_jobs_listing_is_newest_first_and_claims_are_oldest_high_priority_first():
    """The listing and the dispatcher order differently, and both are documented."""
    with bus():

        async def scenario(client):
            await seed_jobs(count=5)
            body = (await client.get("/v1/jobs?limit=5")).json()
            assert [row["id"] for row in body["jobs"]] == [f"job-{i:05d}" for i in reversed(range(5))], (
                "GET /v1/jobs is started_at DESC: newest first"
            )

            # The claim path is priority DESC then started_at ASC: the oldest row of
            # the highest priority wins, which is the opposite end of the same table.
            worker = await client.post(
                "/v1/agents/register",
                json={
                    "kind": "zeroclaw",
                    "host": "page",
                    "runtime": "zeroclaw",
                    "capabilities": ["code"],
                    "cost_tier": "B",
                    "auth_method": "api",
                },
            )
            urn = worker.json()["urn"]
            claimed = await client.post(f"/v1/jobs/next?agent_urn={urn}", headers={"X-Hive-Actor": urn})
            assert claimed.status_code == 200, claimed.text
            assert claimed.json()["id"] == "job-00000", claimed.text

        run(scenario)


# ── the cursor ──────────────────────────────────────────────────────────────


def test_events_cursor_walks_the_whole_tail_exactly_once():
    """`since_id` pages forward: strictly increasing ids, no duplicates, no gaps, and the end is empty."""
    with bus():

        async def scenario(client):
            await seed_events(count=SEED)
            seen = []
            cursor = 0
            pages = 0
            while True:
                body = (await client.get(f"/v1/events/log?since_id={cursor}&limit=1000")).json()
                ids = [row["id"] for row in body["events"]]
                if not ids:
                    break
                assert ids == sorted(ids), "the cursor page must be in id order"
                assert all(i > cursor for i in ids), (cursor, ids[:3])
                seen.extend(ids)
                cursor = body["last_id"]
                pages += 1
                assert pages <= 10, "the cursor is not advancing"
            assert len(seen) == len(set(seen)) == SEED
            assert seen == sorted(seen)
            assert (await client.get(f"/v1/events/log?since_id={cursor}")).json()["events"] == []

        run(scenario)


# ── invalid values ──────────────────────────────────────────────────────────


@pytest.mark.parametrize("route", ["/v1/jobs", "/v1/messages", "/v1/events/log"])
def test_a_non_integer_limit_is_a_422_with_the_standard_shape(route):
    with bus():

        async def scenario(client):
            for value in ("abc", "1.5", "10;DROP TABLE jobs"):
                response = await client.get(f"{route}?limit={value}")
                assert response.status_code == 422, (route, value, response.status_code, response.text)
                body = response.json()
                assert set(body) == {"detail"}, body
                assert body["detail"][0]["loc"] == ["query", "limit"], body["detail"]

        run(scenario)


def test_history_limit_is_validated_not_clamped():
    """The one route that rejects an out-of-range limit instead of clamping it: the doc says so."""
    with bus():

        async def scenario(client):
            register = await client.post(
                "/v1/agents/register",
                json={
                    "kind": "opencode",
                    "host": "page",
                    "runtime": "opencode",
                    "capabilities": ["code"],
                    "cost_tier": "B",
                    "auth_method": "api",
                },
            )
            actor = register.json()["urn"]
            headers = {"X-Hive-Actor": actor}
            for value in ("0", "-1", "501"):
                response = await client.get(f"/v1/admin/jobs/no-such-id/history?limit={value}", headers=headers)
                assert response.status_code == 422, (value, response.status_code, response.text)
                assert "limit must be between" in response.json()["detail"]
            ok = await client.get("/v1/admin/jobs/no-such-id/history?limit=50", headers=headers)
            assert ok.status_code == 200, ok.text
            assert ok.json()["count"] == 0

        run(scenario)


def test_unpaged_collections_ignore_a_limit_they_do_not_declare():
    """`GET /v1/agents` takes no page parameter: an extra `limit` must not change the answer."""
    with bus():

        async def scenario(client):
            for _ in range(3):
                await client.post(
                    "/v1/agents/register",
                    json={
                        "kind": "zeroclaw",
                        "host": "page",
                        "runtime": "zeroclaw",
                        "capabilities": ["code"],
                        "cost_tier": "B",
                        "auth_method": "api",
                    },
                )
            plain = (await client.get("/v1/agents?include_offline=true")).json()
            with_limit = (await client.get("/v1/agents?include_offline=true&limit=1")).json()
            assert len(plain["agents"]) >= 3
            assert len(with_limit["agents"]) == len(plain["agents"])

        run(scenario)


def test_pagination_does_not_change_the_response_envelope():
    """The page is still `{count, total, jobs}` / `{count, messages}` / `{count, events, last_id}`."""
    with bus():

        async def scenario(client):
            await seed_all()
            jobs = (await client.get("/v1/jobs?limit=2")).json()
            assert set(jobs) == {"count", "total", "jobs"}, jobs.keys()
            messages = (await client.get("/v1/messages?limit=2")).json()
            assert set(messages) == {"count", "messages"}, messages.keys()
            events = (await client.get("/v1/events/log?limit=2")).json()
            assert set(events) == {"count", "events", "last_id"}, events.keys()
            assert events["last_id"] == events["events"][-1]["id"]

        run(scenario)
