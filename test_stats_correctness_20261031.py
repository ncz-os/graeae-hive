"""FB57: stats endpoints correctness — independent recomputation from raw rows.

Each test builds a known dataset, recomputes the statistic in plain Python
straight from the raw tables (NOT via the endpoint's SQL), and compares with
what the endpoint returns:

* costs: every group_by (provider/model/runtime/cost_tier/claimed_by/day/kind)
  over rows with null cost, zero tokens, mixed models, an exact-cutoff
  boundary row, and excluded pre-cutoff/NULL-ended rows;
* workers: real submit→claim→PATCH lifecycles (done w/ tokens, failed,
  cancelled) on two workers; counts/token sums recomputed from the jobs table,
  cost via estimate_cost on the same effective inputs, plus kind filter,
  ordering, top_n, and the system-urn exclusion;
* cache: cache_store/record_hit plus cache_hit job results, recomputed
  independently; empty-DB zeros and top_n truncation.

Binary-clean decimals only (multiples of 0.25 / .5 durations) so Python and
SQLite rounding agree; duration/last_run (wall-clock at PATCH time) are range
assertions. Harness follows test_api_contract_20261029 (temp DB file,
_HIVE_REPO patched to None, secret env vars cleared).
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import unittest
from datetime import UTC, datetime
from unittest import mock

os.environ.setdefault("HIVE_BACKEND", "sqlite")
os.environ.setdefault("HIVE_BUS_TOKEN", "test-bus-token")

import time as _time  # noqa: E402

import httpx  # noqa: E402

import agent_bus  # noqa: E402
from hive.bus import deps as _deps  # noqa: E402
from hive.bus.deps import hive_repo  # noqa: E402
from hive.bus.domain.jobs import estimate_cost, stable_worker_id  # noqa: E402

NOW = _time.time()
SHA = "a" * 40
PROOF = {"head_sha": SHA, "commits": [SHA], "files_changed": ["f.py"], "worktree_clean": True}


def utcdate(ts):
    return datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d")


def utcdt(ts):
    return datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")


def provider_bucket(result_json, claimed_provider):
    try:
        alias = (json.loads(result_json or "null") or {}).get("agent_alias") or ""
    except Exception:
        alias = ""
    if alias in ("hive_deepseek_pro_1", "hive_groq_1", "hive_xai_1", "hive_nvidia_1"):
        return "codex"
    for prefix, name in (
        ("hive_deepseek", "deepseek-direct"),
        ("hive_siliconflow", "siliconflow"),
        ("hive_xai", "xai"),
        ("hive_minimax", "minimax"),
        ("hive_groq", "groq"),
        ("hive_nvidia", "nvidia"),
    ):
        if alias.startswith(prefix):
            return name
    return claimed_provider or "unknown"


def _run(coro):
    return asyncio.run(coro)


class _Harness(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._td = tempfile.TemporaryDirectory()
        cls._env = mock.patch.dict(
            os.environ, {"AGENT_BUS_DB": os.path.join(cls._td.name, "stats.db"), "HIVE_BACKEND": "sqlite"}
        )
        cls._env.start()
        for var in ("MNEMOS_URL", "MNEMOS_TOKEN", "HIVE_SHARED_SECRET", "HIVE_BUS_ADMIN_TOKEN"):
            os.environ.pop(var, None)
        cls._repo_patch = mock.patch.object(_deps, "_HIVE_REPO", None)
        cls._repo_patch.start()
        _run(hive_repo().init())
        cls.orch = cls._register(
            {
                "kind": "opencode",
                "host": "st",
                "runtime": "opencode",
                "status": "online",
                "capabilities": ["code"],
                "cost_tier": "B",
            }
        )
        cls.w1 = cls._register(
            {
                "kind": "zeroclaw",
                "host": "st",
                "runtime": "zeroclaw",
                "status": "online",
                "capabilities": ["code"],
                "cost_tier": "B",
            }
        )
        cls.w2 = cls._register(
            {
                "kind": "zeroclaw",
                "host": "st2",
                "runtime": "zeroclaw",
                "status": "online",
                "capabilities": ["code"],
                "cost_tier": "B",
            }
        )

    @classmethod
    def tearDownClass(cls):
        cls._repo_patch.stop()
        cls._env.stop()
        cls._td.cleanup()

    @classmethod
    def _register(cls, payload):
        async def _do():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t") as c:
                r = await c.post("/v1/agents/register", json=payload)
                assert r.status_code == 200, r.text
                return r.json()["urn"]

        return _run(_do())

    def _call(self, method, path, json_body=None, params=None):
        async def _do():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t") as c:
                r = await c.request(method, path, json=json_body, params=params)
                return r.status_code, r.json()

        return _run(_do())


COST_JOBS = [
    # id, ended, kind, model, provider, runtime, tier, by, in, out, cost, alias
    ("c1", NOW - 100, "loop", "m-a", "p-a", "r-a", "B", "w1", 100, 50, 1.5, "hive_deepseek_pro_1"),
    ("c2", NOW - 200, "loop", "m-a", "p-a", "r-a", "B", "w1", 0, 0, 0.0, "hive_deepseek-x"),
    ("c3", NOW - 300, "build", "m-b", "p-b", "r-b", "C", "w2", 400, 100, None, "hive_minimax-1"),
    ("c4", NOW - 400, "build", None, None, None, None, None, None, None, 2.25, None),
    ("c5", NOW - 100, "loop", "m-b", "p-b", "r-b", "B", "w2", 200, 200, 0.5, "other"),
    ("c-bound", NOW - 3590, "loop", "m-a", "p-a", "r-a", "B", "w1", 8, 8, 0.25, None),
    ("c-old", NOW - 7200, "loop", "m-a", "p-a", "r-a", "B", "w1", 999, 999, 9.0, None),
    ("c-open", None, "loop", "m-a", "p-a", "r-a", "B", "w1", 999, 999, 9.0, None),
]


class CostStatsTests(_Harness):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        repo = hive_repo()

        async def _mk():
            async with repo.connection() as db:
                for jid, ended, kind, model, prov, rt, tier, by, tin, tout, cost, alias in COST_JOBS:
                    res = json.dumps({"agent_alias": alias}) if alias else None
                    await db.execute(
                        "INSERT INTO jobs (id, submitter_urn, kind, status, started_at, ended_at, "
                        "tokens_in, tokens_out, estimated_cost_usd, claimed_model, claimed_provider, "
                        "claimed_runtime, claimed_cost_tier, claimed_by, result) "
                        "VALUES (?, ?, ?, 'done', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            jid,
                            cls.orch,
                            kind,
                            NOW - 1000,
                            ended,
                            tin,
                            tout,
                            cost,
                            model,
                            prov,
                            rt,
                            tier,
                            (cls.w1 if by == "w1" else cls.w2) if by else None,
                            res,
                        ),
                    )
                await db.commit()

        _run(_mk())

    def _expected(self, group_by, cutoff):
        buckets = {}
        for _jid, ended, kind, model, prov, rt, tier, by, tin, tout, cost, alias in COST_JOBS:
            if ended is None or ended < cutoff:
                continue
            res = json.dumps({"agent_alias": alias}) if alias else None
            if group_by == "day":
                key = utcdate(ended)
            elif group_by == "provider":
                key = provider_bucket(res, prov)
            elif group_by == "model":
                key = model or "unknown"
            elif group_by == "runtime":
                key = rt or "unknown"
            elif group_by == "cost_tier":
                key = tier or "unknown"
            elif group_by == "claimed_by":
                key = (self.w1 if by == "w1" else self.w2) if by else "unknown"
            else:
                key = kind
            b = buckets.setdefault(key, [0, 0, 0, 0.0, 0.0, 0])
            b[0] += 1
            b[1] += tin or 0
            b[2] += tout or 0
            b[3] += cost or 0
            b[4] += ended - (NOW - 1000)
            b[5] += 1
        return {
            k: {
                "job_count": v[0],
                "tokens_in": v[1],
                "tokens_out": v[2],
                "estimated_cost_usd": round(v[3], 4),
                "avg_duration_sec": round(v[4] / v[5], 2),
            }
            for k, v in buckets.items()
        }

    def test_every_group_by_matches_independent_recomputation(self):
        """Invariant: each bucket's count/sums/avg equal a from-scratch
        recomputation from the jobs rows — null cost contributes 0 but still
        counts, zero-token rows are counted, the exact-cutoff row is included,        pre-cutoff and NULL-ended rows are excluded."""
        import time as _t

        # Wide window; the endpoint's cutoff is real-now based, so recompute
        # against the same real-now cutoff it used.
        real_cutoff = _t.time() - 100 * 3600
        for group_by in ("provider", "model", "runtime", "cost_tier", "claimed_by", "day", "kind"):
            with self.subTest(group_by=group_by):
                code, data = self._call("GET", "/v1/stats/costs", params={"since_hours": 100, "group_by": group_by})
                self.assertEqual(code, 200, data)
                exp = self._expected(group_by, real_cutoff)
                got = {r["bucket"]: r for r in data["buckets"]}
                self.assertEqual(set(got), set(exp), group_by)
                for bucket, e in exp.items():
                    for field in ("job_count", "tokens_in", "tokens_out", "estimated_cost_usd", "avg_duration_sec"):
                        self.assertEqual(got[bucket][field], e[field], (group_by, bucket, field))
                totals = data["totals"]
                self.assertEqual(totals["job_count"], sum(e["job_count"] for e in exp.values()))
                self.assertEqual(totals["tokens_in"], sum(e["tokens_in"] for e in exp.values()))
                self.assertEqual(totals["tokens_out"], sum(e["tokens_out"] for e in exp.values()))
                self.assertEqual(
                    totals["estimated_cost_usd"], round(sum(e["estimated_cost_usd"] for e in exp.values()), 4)
                )

    def test_null_cost_and_zero_tokens_counted_with_zero_sums(self):
        """Invariant: c3 (null cost) and c2 (zero tokens) appear with 0 sums."""
        code, data = self._call("GET", "/v1/stats/costs", params={"since_hours": 100, "group_by": "model"})
        self.assertEqual(code, 200)
        got = {r["bucket"]: r for r in data["buckets"]}
        self.assertEqual(got["m-b"]["tokens_in"], 600)  # 400 + 200
        self.assertEqual(got["m-b"]["estimated_cost_usd"], 0.5)  # None → 0
        self.assertEqual(got["unknown"]["job_count"], 1)  # null-model row
        self.assertEqual(got["unknown"]["estimated_cost_usd"], 2.25)

    def test_bad_group_by_is_422(self):
        code, _ = self._call("GET", "/v1/stats/costs", params={"group_by": "nope"})
        self.assertEqual(code, 422)

    def test_cutoff_boundary_is_inclusive(self):
        """Invariant: ended_at == cutoff is included (>=), ended_at one
        second older is excluded — asserted at the repository level with an
        exact cutoff (the HTTP cutoff is live time.time() and cannot be hit
        exactly)."""
        repo = hive_repo()

        async def _q(cutoff):
            async with repo.connection() as tx:
                return await repo.cost_stats_rows(tx, group_by="model", cutoff=cutoff)

        # m-a rows in window: c1 (NOW-100), c2 (NOW-200), c-bound (NOW-3590).
        at = {r["bucket"]: r["job_count"] for r in _run(_q(NOW - 3590))}
        after = {r["bucket"]: r["job_count"] for r in _run(_q(NOW - 3589))}
        self.assertEqual(at.get("m-a"), 3)
        self.assertEqual(after.get("m-a"), 2)


class WorkerStatsTests(_Harness):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # W1: 2 done (tokens), 1 failed, 1 cancelled — kind loop.
        # W2: 1 done — kind loop. All through the real lifecycle.
        cls.jobs = []  # (jid, by_urn, kind, tin, tout, terminal)
        cls._n = 0  # unique-description counter (the bus coalesces resubmissions)
        cls._drive()

    @classmethod
    def _submit_claim(cls, worker):
        cls._n += 1
        tag = cls._n

        async def _do():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t") as c:
                # Unique description: the bus coalesces identical
                # (kind, description) resubmissions onto the live row.
                r = await c.post(
                    "/v1/jobs", json={"kind": "loop", "description": f"stats w {tag}", "submitter_urn": cls.orch}
                )
                assert r.status_code in (200, 201), r.text
                jid = r.json()["id"]
                r = await c.post(f"/v1/jobs/{jid}/claim", params={"by": worker})
                assert r.status_code == 200, r.text
                return jid

        return _run(_do())

    @classmethod
    def _patch(cls, jid, body):
        async def _do():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t") as c:
                r = await c.patch(f"/v1/jobs/{jid}", json=body)
                assert r.status_code == 200, r.text
                return r.json()

        return _run(_do())

    @classmethod
    def _drive(cls):
        w1, w2, orch = cls.w1, cls.w2, cls.orch
        for tin, tout in ((100, 50), (200, 0)):
            jid = cls._submit_claim(w1)
            cls._patch(
                jid,
                {
                    "status": "done",
                    "claimed_by": w1,
                    "tokens_in": tin,
                    "tokens_out": tout,
                    "result": dict(PROOF, exit_code=0),
                },
            )
            cls.jobs.append((jid, w1, "loop", tin, tout, "done"))
        jid = cls._submit_claim(w1)
        cls._patch(
            jid,
            {
                "status": "failed",
                "claimed_by": w1,
                "tokens_in": 40,
                "tokens_out": 10,
                "result": {"retryable": False, "failure_reason": "x"},
            },
        )
        cls.jobs.append((jid, w1, "loop", 40, 10, "failed"))
        jid = cls._submit_claim(w1)
        cls._patch(jid, {"status": "cancelled", "cancel_actor": orch, "reason": "stats"})
        cls.jobs.append((jid, w1, "loop", 0, 0, "cancelled"))
        jid = cls._submit_claim(w2)
        cls._patch(
            jid,
            {
                "status": "done",
                "claimed_by": w2,
                "tokens_in": 300,
                "tokens_out": 300,
                "result": dict(PROOF, exit_code=0),
            },
        )
        cls.jobs.append((jid, w2, "loop", 300, 300, "done"))

    def test_counts_and_token_sums_match_jobs_table(self):
        """Invariant: per (stable worker, kind), success/fail/cancelled counts
        and token sums equal the jobs rows the lifecycles wrote — the rollup
        neither loses nor double-counts a terminal transition."""
        code, data = self._call("GET", "/v1/stats/workers")
        self.assertEqual(code, 200, data)
        k1 = stable_worker_id(self.w1)
        got = {(r["urn"], r["kind"]): r for r in data["workers"]}
        r1 = got[(k1, "loop")]
        self.assertEqual((r1["success_count"], r1["fail_count"], r1["cancelled_count"]), (2, 1, 1))
        self.assertEqual((r1["total_tokens_in"], r1["total_tokens_out"]), (340, 60))
        self.assertEqual(r1["success_pct"], round(100.0 * 2 / 4, 1))
        r2 = got[(stable_worker_id(self.w2), "loop")]
        self.assertEqual((r2["success_count"], r2["fail_count"], r2["cancelled_count"]), (1, 0, 0))
        self.assertEqual((r2["total_tokens_in"], r2["total_tokens_out"]), (300, 300))
        # Ordering: success_count DESC — W1 first.
        self.assertEqual(data["workers"][0]["urn"], k1)
        # Cost is estimate_cost on the effective inputs summed per worker.
        exp1 = sum(
            estimate_cost("unknown", "unknown", tin, tout)
            for (_, _, _, tin, tout, _) in [j for j in self.jobs if j[1] == self.w1]
        )
        self.assertAlmostEqual(r1["total_cost_usd"], round(exp1, 4))
        # Durations are wall-clock ranges, but must be sane.
        self.assertGreaterEqual(r1["avg_duration_sec"], 0)
        self.assertLess(r1["avg_duration_sec"], 3600)

    def test_kind_filter_top_n_and_system_exclusion(self):
        """Invariant: kind=? filters, top_n truncates, and system-kind URNs
        are excluded by default but visible with include_system=true."""
        repo = hive_repo()

        async def _mk():
            async with repo.connection() as tx:
                await repo.upsert_worker_kind_stats(
                    tx,
                    urn="urn:agent:system:mon:abc",
                    kind="loop",
                    column="success_count",
                    tokens_in=1,
                    tokens_out=1,
                    cost_usd=0,
                    duration_sec=1,
                    last_run=NOW,
                )

        _run(_mk())
        code, data = self._call("GET", "/v1/stats/workers")
        self.assertNotIn("urn:agent:system:mon", [r["urn"] for r in data["workers"]])
        code, data = self._call("GET", "/v1/stats/workers", params={"include_system": True})
        self.assertIn("urn:agent:system:mon:abc", [r["urn"] for r in data["workers"]])
        code, data = self._call("GET", "/v1/stats/workers", params={"kind": "loop", "top_n": 1})
        self.assertEqual(len(data["workers"]), 1)
        self.assertEqual(data["workers"][0]["urn"], stable_worker_id(self.w1))
        code, data = self._call("GET", "/v1/stats/workers", params={"kind": "no-such-kind"})
        self.assertEqual(data["workers"], [])


class CacheStatsTests(_Harness):
    def test_cache_totals_entries_hits_and_empty(self):
        """Invariant: COUNT/SUM over hive_cache plus the LIKE-count of cache_hit
        job results, reproduced independently; empty DB reports zeros; top_n
        truncates; cache_key ships truncated to 16 chars."""
        repo = hive_repo()

        async def _mk():
            async with repo.connection() as db:
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, status, started_at, result) "
                    "VALUES ('h1', ?, 'loop', 'done', 1.0, ?)",
                    (self.orch, json.dumps({"cache_hit": True})),
                )
                await db.execute(
                    "INSERT INTO jobs (id, submitter_urn, kind, status, started_at, result) "
                    "VALUES ('h2', ?, 'loop', 'done', 1.0, ?)",
                    (self.orch, json.dumps({"other": 1})),
                )
                await db.commit()
                async with repo.connection() as tx:
                    await repo.cache_store(
                        tx,
                        cache_key="k" * 40,
                        source_job_id="h2",
                        result_json="{}",
                        result_mnemos_id=None,
                        model="m",
                        provider="p",
                        cached_at=100.0,
                    )
                    await repo.cache_store(
                        tx,
                        cache_key="z" * 40,
                        source_job_id="h1",
                        result_json="{}",
                        result_mnemos_id=None,
                        model="m",
                        provider="p",
                        cached_at=200.0,
                    )
                    await repo.cache_record_hit(tx, cache_key="k" * 40, cost_saved_usd=1.25, ts=300.0)

        _run(_mk())
        code, data = self._call("GET", "/v1/stats/cache")
        self.assertEqual(code, 200, data)
        self.assertEqual(data["totals"]["cached_jobs"], 2)
        self.assertEqual(data["totals"]["total_hits"], 1)
        self.assertEqual(data["totals"]["total_cost_saved_usd"], 1.25)
        self.assertEqual(data["totals"]["jobs_short_circuited"], 1)
        self.assertEqual(len(data["top_entries"]), 2)
        self.assertEqual(data["top_entries"][0]["hit_count"], 1)
        self.assertEqual(len(data["top_entries"][0]["cache_key"]), 16)
        self.assertEqual(data["top_entries"][0]["cached_at"], utcdt(100.0))
        code, data = self._call("GET", "/v1/stats/cache", params={"top_n": 1})
        self.assertEqual(len(data["top_entries"]), 1)

    def test_empty_cache_reports_zeros(self):
        """Invariant: COUNT/SUM over an empty cache table report zeros, not nulls."""
        repo = hive_repo()

        async def _wipe():
            async with repo.connection() as db:
                await db.execute("DELETE FROM hive_cache")
                await db.execute("DELETE FROM jobs")
                await db.commit()

        _run(_wipe())
        code, data = self._call("GET", "/v1/stats/cache")
        self.assertEqual(code, 200, data)
        self.assertEqual(
            data["totals"], {"cached_jobs": 0, "total_hits": 0, "total_cost_saved_usd": 0, "jobs_short_circuited": 0}
        )
        self.assertEqual(data["top_entries"], [])


if __name__ == "__main__":
    unittest.main()
