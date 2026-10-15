"""Tests for the unified harness worker loop + telemetry capture."""

import unittest
from unittest import mock

from hive.core import quota
from hive.harness.codex import codex_executor
from hive.workers.run import HarnessWorker


class FakeAdapter:
    harness_kind = "fake"
    cost_tier = "metered"

    def __init__(self, job=None, execute_raises=None):
        self._job = job
        self._execute_raises = execute_raises
        self.calls = []

    def register(self, reg):
        self.calls.append(("register", reg))
        return "urn:agent:fake:h:s"

    def heartbeat(self, urn, payload):
        self.calls.append(("heartbeat", urn))
        return {}

    def claim(self, urn, jobs=None):
        self.calls.append(("claim", urn))
        return self._job

    def execute(self, urn, job):
        self.calls.append(("execute", job.get("id")))
        if self._execute_raises:
            raise self._execute_raises
        return {"summary": "ok"}

    def complete(self, urn, job_id, result):
        self.calls.append(("complete", job_id, result))

    def fail(self, urn, job_id, error):
        self.calls.append(("fail", job_id, error))


class WorkerLoopTests(unittest.TestCase):
    def test_register_sets_urn(self):
        w = HarnessWorker(FakeAdapter())
        self.assertEqual(w.register(), "urn:agent:fake:h:s")
        self.assertEqual(w.urn, "urn:agent:fake:h:s")

    def test_run_once_claims_executes_completes(self):
        a = FakeAdapter(job={"id": "job-1", "kind": "feat:edit"})
        w = HarnessWorker(a, urn="urn:x", heartbeat_interval=0)
        self.assertTrue(w.run_once())
        kinds = [c[0] for c in a.calls]
        self.assertIn("execute", kinds)
        self.assertIn("complete", kinds)
        self.assertNotIn("fail", kinds)

    def test_run_once_no_job_returns_false(self):
        a = FakeAdapter(job=None)
        w = HarnessWorker(a, urn="urn:x")
        self.assertFalse(w.run_once())
        self.assertNotIn("execute", [c[0] for c in a.calls])

    def test_run_once_reports_failure_without_crashing(self):
        a = FakeAdapter(job={"id": "job-1"}, execute_raises=RuntimeError("boom"))
        w = HarnessWorker(a, urn="urn:x", heartbeat_interval=0)
        self.assertTrue(w.run_once())
        fails = [c for c in a.calls if c[0] == "fail"]
        self.assertEqual(len(fails), 1)
        self.assertIn("boom", fails[0][2])

    def test_run_forever_bounded(self):
        a = FakeAdapter(job=None)
        w = HarnessWorker(a, urn="urn:x", poll_interval=0)
        with mock.patch("hive.workers.run.time.sleep"):
            w.run_forever(max_iterations=3)
        self.assertEqual(len([c for c in a.calls if c[0] == "claim"]), 3)


class TelemetryTests(unittest.TestCase):
    def test_ratelimit_headers_parsed(self):
        out = quota.parse_ratelimit_headers(
            {
                "x-ratelimit-remaining-tokens": "31877",
                "x-ratelimit-limit-tokens": "40000",
                "anthropic-ratelimit-tokens-remaining": "95000",
                "content-type": "application/json",
            }
        )
        self.assertEqual(out["remaining-tokens"], 31877)
        self.assertEqual(out["limit-tokens"], 40000)
        self.assertEqual(out["tokens-remaining"], 95000)
        self.assertNotIn("content-type", out)

    def test_extract_tokens_shapes(self):
        self.assertEqual(
            quota.extract_tokens({"usage": {"prompt_tokens": 5, "completion_tokens": 7}}), {"in": 5, "out": 7}
        )
        self.assertEqual(quota.extract_tokens({"input_tokens": 3, "output_tokens": 4}), {"in": 3, "out": 4})
        self.assertEqual(quota.extract_tokens({"tokens_in": 1, "tokens_out": 2}), {"in": 1, "out": 2})
        self.assertIsNone(quota.extract_tokens({"nope": 1}))

    def test_parse_cli_json_embedded(self):
        self.assertEqual(
            quota.parse_cli_json('log line\n{"usage":{"prompt_tokens":2,"completion_tokens":3}}\ntrailing'),
            {"usage": {"prompt_tokens": 2, "completion_tokens": 3}},
        )
        self.assertEqual(quota.parse_cli_json("no json here"), {})

    def test_quota_block_shape(self):
        b = quota.quota_block(
            provider="openai", model="codex", tokens_in=2, tokens_out=3, ratelimit={"remaining-tokens": 9}
        )
        self.assertEqual(b["provider"], "openai")
        self.assertEqual(b["tokens"], {"in": 2, "out": 3})
        self.assertEqual(b["ratelimit"], {"remaining-tokens": 9})


class ExecutorTests(unittest.TestCase):
    def test_codex_executor_returns_quota_block(self):
        proc = mock.Mock(returncode=0, stdout='{"usage":{"prompt_tokens":10,"completion_tokens":20}}', stderr="")
        with mock.patch("hive.harness.codex.subprocess.run", return_value=proc) as run:
            result = codex_executor()("urn:x", {"id": "j1", "kind": "feat:test", "description": "do it"})
        self.assertEqual(result["harness"], "codex")
        self.assertEqual(result["quota"]["tokens"], {"in": 10, "out": 20})
        self.assertEqual(result["quota"]["provider"], "openai")
        # headless invocation shape
        self.assertEqual(run.call_args.args[0][:2], ["codex", "exec"])


if __name__ == "__main__":
    unittest.main()
