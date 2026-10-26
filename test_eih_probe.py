"""Tests for hive.tools.eih_probe — a fake transport, never a real request.

Every test injects a transport (or patches ``http_transport``) so nothing here
touches the network, the real key, or a model provider.
"""

from __future__ import annotations

import json
import os
import stat
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from hive.tools import eih_probe as probe

# A syntactically plausible but fake key, assembled at runtime so no complete
# token literal exists in source (same convention as test_hive_secrets.py's
# FAKE_* fixtures -- see .gitguardian.yaml). It must never appear in any row.
_KP = "nvapi-"
FAKE_KEY = _KP + "TESTSECRET" + "0" * 40
_BEARER_ECHO = "nvapi-ABCDEF" + "123456"

NO_SKIP = datetime(2026, 10, 26, 13, 7, tzinfo=UTC)  # 13 % 6 != 0 -> tiny
MEDIUM_TIME = datetime(2026, 10, 26, 12, 7, tzinfo=UTC)  # 12 % 6 == 0 -> medium
WINDOW_TIME = datetime(2026, 10, 26, 22, 7, tzinfo=UTC)


def ok_result(**overrides) -> probe.StreamResult:
    result = probe.StreamResult(
        http_status=200,
        headers_ms=120.0,
        first_token_ms=240.0,
        latency_ms=900.0,
        content=str(17 * 23 + 19),
        finish_reason="stop",
        tokens_in=40,
        tokens_out=1,
        resolved_ip="10.1.2.3",
        resolved_ip_source="socket_peer",
    )
    for key, value in overrides.items():
        setattr(result, key, value)
    return result


class FakeTransport:
    """Scripted transport: returns the queued results, one per call."""

    def __init__(self, *results: probe.StreamResult) -> None:
        self.results = list(results)
        self.calls: list[dict] = []

    def __call__(self, *, base_url, payload, api_key, first_token_timeout, total_timeout):
        self.calls.append(
            {
                "base_url": base_url,
                "payload": payload,
                "api_key": api_key,
                "first_token_timeout": first_token_timeout,
                "total_timeout": total_timeout,
            }
        )
        if not self.results:
            raise AssertionError("transport called more times than scripted")
        return self.results.pop(0)


def run_with(transport, *, now=NO_SKIP, models=("model/one",), **kwargs):
    return probe.run_probe(
        now=now,
        transport=transport,
        api_key=FAKE_KEY,
        models=models,
        host="testhost",
        host_load1=0.5,
        cpu_count=8,
        sleep=lambda _secs: None,
        **kwargs,
    )


class PromptClassTests(unittest.TestCase):
    def test_tiny_outside_medium_hours(self):
        for hour in (0, 1, 5, 7, 13, 23):
            with self.subTest(hour=hour):
                moment = datetime(2026, 10, 26, hour, 7, tzinfo=UTC)
                expected = "medium" if hour % 6 == 0 else "tiny"
                self.assertEqual(probe.prompt_class_for(moment), expected)

    def test_medium_only_at_hour_mod_six(self):
        for hour in range(24):
            with self.subTest(hour=hour):
                moment = datetime(2026, 10, 26, hour, 30, tzinfo=UTC)
                self.assertEqual(probe.prompt_class_for(moment) == "medium", hour % 6 == 0)

    def test_medium_class_switches_max_tokens_and_prompt(self):
        transport = FakeTransport(ok_result())
        run = run_with(transport, now=MEDIUM_TIME)
        self.assertEqual(run.rows[0]["prompt_class"], "medium")
        sent = transport.calls[0]["payload"]
        self.assertEqual(sent["max_tokens"], probe.MEDIUM_MAX_TOKENS)
        self.assertTrue(sent["stream"])
        self.assertGreater(len(sent["messages"][0]["content"]), 20_000)

    def test_tiny_class_payload(self):
        transport = FakeTransport(ok_result())
        run_with(transport)
        sent = transport.calls[0]["payload"]
        self.assertEqual(sent["max_tokens"], probe.TINY_MAX_TOKENS)
        self.assertIs(sent["stream"], True)
        self.assertEqual(sent["model"], "model/one")
        # The answer is checked against a computed value, not a literal guess.
        self.assertEqual(probe.TINY_PROMPT_ANSWER, str(17 * 23 + 19))

    def test_medium_prompt_is_deterministic(self):
        self.assertEqual(probe.build_medium_prompt(), probe.build_medium_prompt())


class ClassificationTests(unittest.TestCase):
    def observed(self, result: probe.StreamResult) -> str:
        """Outcome of attempt 1; retryable classes get a second scripted result."""
        transport = FakeTransport(result, result)
        return run_with(transport).rows[0]["outcome"]

    def test_ok(self):
        self.assertEqual(self.observed(ok_result()), "ok")

    def test_budget_exhausted_empty_content_and_length(self):
        result = ok_result(content="", finish_reason="length", tokens_reasoning=512)
        self.assertEqual(self.observed(result), "budget_exhausted")

    def test_budget_exhausted_is_not_a_failure(self):
        result = ok_result(content="", finish_reason="length", tokens_reasoning=400)
        run = run_with(FakeTransport(result))
        self.assertEqual(run.rows[0]["outcome"], "budget_exhausted")
        self.assertEqual(run.rows[0]["tokens_reasoning"], 400)
        self.assertIsNone(run.rows[0]["error_excerpt"])

    def test_empty_content_with_stop_is_ok_not_budget(self):
        self.assertEqual(self.observed(ok_result(content="", finish_reason="stop")), "ok")

    def test_429(self):
        result = probe.StreamResult(http_status=429, body='{"error":"rate limited"}', latency_ms=100.0)
        self.assertEqual(self.observed(result), "429")

    def test_503(self):
        result = probe.StreamResult(http_status=503, body="Service Unavailable", latency_ms=90.0)
        self.assertEqual(self.observed(result), "503")

    def test_504(self):
        result = probe.StreamResult(http_status=504, body="gateway timeout", latency_ms=300_000.0)
        self.assertEqual(self.observed(result), "504")

    def test_529(self):
        result = probe.StreamResult(http_status=529, body="overloaded", latency_ms=110.0)
        self.assertEqual(self.observed(result), "529")

    def test_404_function_not_found_is_unprovisioned(self):
        body = '{"type":"about:blank","title":"Not Found","detail":"Function not found: model/x"}'
        result = probe.StreamResult(http_status=404, body=body, latency_ms=80.0)
        self.assertEqual(self.observed(result), "404_unprovisioned")

    def test_404_without_function_not_found_is_generic_http(self):
        result = probe.StreamResult(http_status=404, body="no such route", latency_ms=80.0)
        self.assertEqual(self.observed(result), "http_404")

    def test_other_http_code(self):
        result = probe.StreamResult(http_status=400, body="bad request", latency_ms=50.0)
        self.assertEqual(self.observed(result), "http_400")

    def test_timeout(self):
        result = probe.StreamResult(
            http_status=None, failure="timeout", failure_detail="first token deadline exceeded", latency_ms=60_000.0
        )
        self.assertEqual(self.observed(result), "timeout")

    def test_transport_error(self):
        result = probe.StreamResult(
            http_status=None, failure="transport", failure_detail="ConnectionResetError: reset", latency_ms=12.0
        )
        self.assertEqual(self.observed(result), "transport")


class RetryTests(unittest.TestCase):
    def test_retry_is_a_separate_row_with_attempt_two(self):
        first = probe.StreamResult(http_status=503, body="unavailable", latency_ms=100.0)
        second = ok_result()
        transport = FakeTransport(first, second)
        rows = run_with(transport).rows
        self.assertEqual(len(rows), 2)
        self.assertEqual([row["attempt"] for row in rows], [1, 2])
        self.assertEqual([row["outcome"] for row in rows], ["503", "ok"])

    def test_retry_sleeps_two_seconds(self):
        delays: list[float] = []
        first = probe.StreamResult(http_status=429, body="limited", latency_ms=10.0)
        probe.run_probe(
            now=NO_SKIP,
            transport=FakeTransport(first, ok_result()),
            api_key=FAKE_KEY,
            models=("model/one",),
            host="testhost",
            host_load1=0.1,
            cpu_count=8,
            sleep=delays.append,
        )
        self.assertEqual(delays, [probe.RETRY_DELAY_SECS])

    def test_timeout_and_transport_are_retried(self):
        for failure in ("timeout", "transport"):
            with self.subTest(failure=failure):
                first = probe.StreamResult(http_status=None, failure=failure, failure_detail="x", latency_ms=1.0)
                transport = FakeTransport(first, ok_result())
                rows = run_with(transport).rows
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[0]["attempt"], 1)

    def test_no_retry_for_budget_exhausted(self):
        result = ok_result(content="", finish_reason="length")
        transport = FakeTransport(result)
        rows = run_with(transport).rows
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(transport.calls), 1)

    def test_no_retry_for_unprovisioned(self):
        result = probe.StreamResult(http_status=404, body="Function not found", latency_ms=5.0)
        transport = FakeTransport(result)
        self.assertEqual(len(run_with(transport).rows), 1)
        self.assertEqual(len(transport.calls), 1)

    def test_at_most_two_attempts(self):
        failing = probe.StreamResult(http_status=503, body="down", latency_ms=5.0)
        transport = FakeTransport(failing, probe.StreamResult(http_status=503, body="down", latency_ms=5.0))
        rows = run_with(transport).rows
        self.assertEqual([row["attempt"] for row in rows], [1, 2])
        self.assertEqual(len(transport.calls), 2)


class SkipGateTests(unittest.TestCase):
    def test_skipped_load_writes_rows_and_makes_no_request(self):
        transport = FakeTransport()  # any call would fail the test
        run = probe.run_probe(
            now=NO_SKIP,
            transport=transport,
            api_key=FAKE_KEY,
            models=("a/one", "b/two"),
            host="testhost",
            host_load1=9.5,
            cpu_count=8,
            sleep=lambda _s: None,
        )
        self.assertEqual(transport.calls, [])
        self.assertEqual(run.skipped, "skipped_load")
        self.assertEqual([row["outcome"] for row in run.rows], ["skipped_load", "skipped_load"])
        self.assertEqual([row["host_load1"] for row in run.rows], [9.5, 9.5])

    def test_load_equal_to_cpu_count_does_not_skip(self):
        transport = FakeTransport(ok_result())
        run = probe.run_probe(
            now=NO_SKIP,
            transport=transport,
            api_key=FAKE_KEY,
            models=("a/one",),
            host="testhost",
            host_load1=8.0,
            cpu_count=8,
        )
        self.assertIsNone(run.skipped)
        self.assertEqual(run.rows[0]["outcome"], "ok")

    def test_skipped_window_writes_rows_and_makes_no_request(self):
        transport = FakeTransport()
        run = probe.run_probe(
            now=WINDOW_TIME,
            transport=transport,
            api_key=FAKE_KEY,
            models=("a/one", "b/two", "c/three"),
            host="testhost",
            host_load1=0.1,
            cpu_count=8,
        )
        self.assertEqual(transport.calls, [])
        self.assertEqual(run.skipped, "skipped_window")
        self.assertEqual(len(run.rows), 3)
        self.assertTrue(all(row["outcome"] == "skipped_window" for row in run.rows))

    def test_publish_window_boundaries(self):
        def at(hour, minute):
            return probe.in_publish_window(datetime(2026, 10, 26, hour, minute, tzinfo=UTC))

        self.assertFalse(at(21, 54))
        self.assertTrue(at(21, 55))
        self.assertTrue(at(22, 0))
        self.assertTrue(at(22, 29))
        self.assertFalse(at(22, 30))
        self.assertFalse(at(13, 7))


class SequentialTests(unittest.TestCase):
    def test_never_more_than_one_request_in_flight(self):
        in_flight = 0
        peak = 0

        def transport(**_kwargs):
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            in_flight -= 1
            return ok_result()

        models = ("a/one", "b/two", "c/three", "d/four")
        run = probe.run_probe(
            now=NO_SKIP,
            transport=transport,
            api_key=FAKE_KEY,
            models=models,
            host="testhost",
            host_load1=0.1,
            cpu_count=8,
        )
        self.assertEqual(peak, 1)
        self.assertEqual(len(run.rows), 4)
        self.assertEqual([row["model_id"] for row in run.rows], list(models))

    def test_one_request_per_model_on_success(self):
        transport = FakeTransport(*[ok_result() for _ in range(3)])
        run_with(transport, models=("a/one", "b/two", "c/three"))
        self.assertEqual(len(transport.calls), 3)


class BannedModelTests(unittest.TestCase):
    def test_default_models_are_all_permitted(self):
        for model_id in probe.DEFAULT_MODELS:
            with self.subTest(model=model_id):
                self.assertFalse(probe.is_banned_model(model_id))

    def test_banned_families_rejected(self):
        banned = [
            "deepseek-v4-pro",
            "deepseek-flash",
            "minimax/minimax-m2",
            "openai/gpt-oss-120b",
            "anthropic/claude-sonnet-4-6",
            "google/gemini-3-pro",
            "x-ai/grok-5",
        ]
        for model_id in banned:
            with self.subTest(model=model_id):
                self.assertTrue(probe.is_banned_model(model_id))
                with self.assertRaises(probe.ConfigError) as ctx:
                    probe.resolve_models({"EIH_PROBE_MODELS": f"nvidia/nemotron-3-ultra-550b-a55b,{model_id}"})
                self.assertIn(model_id, str(ctx.exception))

    def test_override_accepts_permitted_models(self):
        models = probe.resolve_models({"EIH_PROBE_MODELS": "nvidia/nemotron-3-ultra-550b-a55b,z-ai/glm-5.3"})
        self.assertEqual(models, ("nvidia/nemotron-3-ultra-550b-a55b", "z-ai/glm-5.3"))

    def test_default_without_env(self):
        self.assertEqual(probe.resolve_models({}), probe.DEFAULT_MODELS)

    def test_main_exits_non_zero_on_banned_model(self):
        env = {"EIH_PROBE_MODELS": "deepseek-v4-pro", "EIH_API_KEY": FAKE_KEY}
        self.assertNotEqual(probe.main([], env=env), 0)


class RedactionTests(unittest.TestCase):
    def test_key_never_reaches_a_row_when_server_echoes_it(self):
        echoed = f'{{"error":"invalid api key {FAKE_KEY} for request","authorization":"Bearer {FAKE_KEY}"}}'
        result = probe.StreamResult(http_status=401, body=echoed, latency_ms=40.0)
        run = run_with(FakeTransport(result))
        blob = json.dumps(run.rows)
        self.assertNotIn(FAKE_KEY, blob)
        self.assertIn("[REDACTED]", run.rows[0]["error_excerpt"])

    def test_key_never_reaches_a_row_when_failure_detail_echoes_it(self):
        result = probe.StreamResult(
            http_status=None,
            failure="transport",
            failure_detail=f"OSError: connection closed, bearer {FAKE_KEY}",
            latency_ms=5.0,
        )
        run = run_with(FakeTransport(result, result))
        self.assertNotIn(FAKE_KEY, json.dumps(run.rows))

    def test_bare_nvapi_shape_redacted_without_known_secret(self):
        text = probe.error_excerpt(f"upstream said {_BEARER_ECHO} nope")
        self.assertNotIn(_BEARER_ECHO, text)

    def test_bearer_token_redacted(self):
        text = probe.error_excerpt("Authorization: Bearer abc.def.ghi rejected")
        self.assertNotIn("abc.def.ghi", text)
        self.assertIn("[REDACTED]", text)

    def test_excerpt_truncated_to_300_chars(self):
        excerpt = probe.error_excerpt("x" * 1000)
        self.assertEqual(len(excerpt), probe.ERROR_EXCERPT_LIMIT)

    def test_excerpt_none_for_empty(self):
        self.assertIsNone(probe.error_excerpt(""))
        self.assertIsNone(probe.error_excerpt(None))


class RowShapeTests(unittest.TestCase):
    def test_field_set_is_exactly_the_contract(self):
        row = run_with(FakeTransport(ok_result())).rows[0]
        self.assertEqual(tuple(row), probe.ROW_FIELDS)
        self.assertEqual(
            set(row),
            {
                "measured_at",
                "host",
                "resolved_ip",
                "resolved_ip_source",
                "provider_id",
                "model_id",
                "prompt_class",
                "probe_tool",
                "attempt",
                "latency_ms",
                "first_token_ms",
                "headers_ms",
                "http_status",
                "outcome",
                "finish_reason",
                "tokens_in",
                "tokens_out",
                "tokens_reasoning",
                "host_load1",
                "error_excerpt",
                "tool_version",
            },
        )

    def test_missing_numbers_are_null_not_zero(self):
        result = probe.StreamResult(http_status=None, failure="transport", failure_detail="boom")
        row = run_with(FakeTransport(result, result)).rows[0]
        for name in ("latency_ms", "first_token_ms", "headers_ms", "http_status", "tokens_in", "tokens_out"):
            with self.subTest(field=name):
                self.assertIsNone(row[name])

    def test_skip_row_numbers_are_null(self):
        run = probe.run_probe(
            now=WINDOW_TIME,
            transport=FakeTransport(),
            api_key=FAKE_KEY,
            models=("a/one",),
            host="testhost",
            host_load1=0.2,
            cpu_count=8,
        )
        row = run.rows[0]
        self.assertIsNone(row["latency_ms"])
        self.assertIsNone(row["first_token_ms"])
        self.assertIsNone(row["http_status"])
        self.assertEqual(row["host_load1"], 0.2)

    def test_constant_fields(self):
        row = run_with(FakeTransport(ok_result())).rows[0]
        self.assertEqual(row["provider_id"], "nvidia-eih")
        self.assertEqual(row["probe_tool"], "direct-interim")
        self.assertEqual(row["tool_version"], probe.TOOL_VERSION)
        self.assertTrue(row["measured_at"].endswith("Z"))

    def test_resolved_ip_source_recorded(self):
        row = run_with(FakeTransport(ok_result())).rows[0]
        self.assertEqual(row["resolved_ip"], "10.1.2.3")
        self.assertEqual(row["resolved_ip_source"], "socket_peer")


class OutputFileTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = Path(self._tmp.name) / "eih-probe"

    def _row(self, when, outcome="ok"):
        return probe.make_row(
            now=when,
            model_id="a/one",
            prompt_class="tiny",
            outcome=outcome,
            host="testhost",
            host_load1=0.1,
            metric=ok_result(),
            secret=FAKE_KEY,
        )

    def test_directory_created_0700_and_file_0600(self):
        writer = probe.RowWriter(self.dir)
        writer.write(self._row(NO_SKIP), NO_SKIP)
        self.assertEqual(stat.S_IMODE(self.dir.stat().st_mode), 0o700)
        target = self.dir / "eih-probe-2026-10-26.jsonl"
        self.assertTrue(target.exists())
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)

    def test_appends_one_json_object_per_line(self):
        writer = probe.RowWriter(self.dir)
        writer.write(self._row(NO_SKIP), NO_SKIP)
        writer.write(self._row(NO_SKIP), NO_SKIP)
        target = self.dir / "eih-probe-2026-10-26.jsonl"
        lines = target.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 2)
        for line in lines:
            self.assertEqual(tuple(json.loads(line)), probe.ROW_FIELDS)

    def test_daily_rotation_by_utc_date(self):
        writer = probe.RowWriter(self.dir)
        writer.write(self._row(NO_SKIP), NO_SKIP)
        next_day = datetime(2026, 10, 27, 1, 7, tzinfo=UTC)
        writer.write(self._row(next_day), next_day)
        names = sorted(p.name for p in self.dir.glob("eih-probe-*.jsonl"))
        self.assertEqual(names, ["eih-probe-2026-10-26.jsonl", "eih-probe-2026-10-27.jsonl"])

    def test_size_cap_stops_writing_and_logs_once(self):
        messages: list[str] = []
        writer = probe.RowWriter(self.dir, max_bytes=600, log=messages.append)
        row = self._row(NO_SKIP)
        one_line = len(json.dumps(row, ensure_ascii=False)) + 1
        written = [writer.write(row, NO_SKIP) for _ in range(600 // max(one_line, 1) + 5)]
        self.assertIn(False, written)
        size = (self.dir / "eih-probe-2026-10-26.jsonl").stat().st_size
        self.assertLessEqual(size, 600)
        self.assertEqual(sum("cap" in m for m in messages), 1, messages)

    def test_retention_deletes_files_older_than_30_days(self):
        writer = probe.RowWriter(self.dir)
        writer._ensure_dir()
        old = self.dir / "eih-probe-2026-08-01.jsonl"
        old.write_text("{}\n", encoding="utf-8")
        recent = self.dir / "eih-probe-2026-10-20.jsonl"
        recent.write_text("{}\n", encoding="utf-8")
        unrelated = self.dir / "notes.txt"
        unrelated.write_text("keep\n", encoding="utf-8")
        removed = writer.prune(NO_SKIP)
        self.assertEqual([p.name for p in removed], ["eih-probe-2026-08-01.jsonl"])
        self.assertFalse(old.exists())
        self.assertTrue(recent.exists())
        self.assertTrue(unrelated.exists())

    def test_output_dir_default_and_env_override(self):
        self.assertEqual(probe.output_dir({}), Path("~/Projects/eih-probe/").expanduser())
        self.assertEqual(probe.output_dir({"EIH_PROBE_DIR": "/srv/probe"}), Path("/srv/probe"))

    def test_main_writes_rows_and_exits_zero_on_probe_failure(self):
        failing = probe.StreamResult(http_status=503, body="down", latency_ms=20.0)
        original = probe.http_transport
        scripted = len(probe.DEFAULT_MODELS) * 2  # each 503 retried once
        probe.http_transport = FakeTransport(*[failing for _ in range(scripted)])  # type: ignore[assignment]
        self.addCleanup(setattr, probe, "http_transport", original)
        env = {"EIH_API_KEY": FAKE_KEY, "EIH_PROBE_DIR": str(self.dir)}
        code = probe.main([], env=env)
        self.assertEqual(code, 0)
        rows = [json.loads(line) for line in (self.dir / "eih-probe-2026-10-26.jsonl").read_text().splitlines()]
        self.assertEqual(len(rows), len(probe.DEFAULT_MODELS) * 2)  # each 503 retried once
        self.assertTrue(all(row["outcome"] == "503" for row in rows))


class KeyLoadingTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)

    def test_env_wins(self):
        self.assertEqual(probe.load_api_key({"EIH_API_KEY": _KP + "FROMENV"}), _KP + "FROMENV")

    def test_key_file_via_env(self):
        path = self.root / "key.env"
        path.write_text(f"MINIMAX_API_KEY=nope\nEIH_API_KEY={_KP}FROMFILE\n", encoding="utf-8")
        self.assertEqual(probe.load_api_key({"EIH_KEY_FILE": str(path)}), _KP + "FROMFILE")

    def test_zoder_config_fallback(self):
        home = self.root / "home"
        (home / ".zoder").mkdir(parents=True)
        (home / ".zoder" / "config.toml").write_text(
            "[providers.models.custom.eih_reviewer]\n"
            f'api_key = "{_KP}FROMZODER"\n'
            'uri = "https://integrate.api.nvidia.com/v1"\n',
            encoding="utf-8",
        )
        previous = os.environ.get("HOME")
        os.environ["HOME"] = str(home)
        self.addCleanup(
            lambda: os.environ.__setitem__("HOME", previous) if previous is not None else os.environ.pop("HOME", None)
        )
        self.assertEqual(probe.load_api_key({}), _KP + "FROMZODER")

    def test_missing_key_is_a_config_error(self):
        path = self.root / "empty.env"
        path.write_text("MINIMAX_API_KEY=nope\n", encoding="utf-8")
        home = self.root / "emptyhome"
        home.mkdir()
        previous = os.environ.get("HOME")
        os.environ["HOME"] = str(home)
        self.addCleanup(
            lambda: os.environ.__setitem__("HOME", previous) if previous is not None else os.environ.pop("HOME", None)
        )
        with self.assertRaises(probe.ConfigError):
            probe.load_api_key({"EIH_KEY_FILE": str(path)})

    def test_main_exits_non_zero_without_key(self):
        home = self.root / "nokey"
        home.mkdir()
        previous = os.environ.get("HOME")
        os.environ["HOME"] = str(home)
        self.addCleanup(
            lambda: os.environ.__setitem__("HOME", previous) if previous is not None else os.environ.pop("HOME", None)
        )
        code = probe.main([], env={"EIH_PROBE_DIR": str(self.root / "out")})
        self.assertNotEqual(code, 0)
        self.assertFalse((self.root / "out").exists())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
