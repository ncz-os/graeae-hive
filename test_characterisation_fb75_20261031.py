"""Characterisation tests for the three least-tested large graeae-hive modules (FB75).

Base: ``deploy/integrate-20261017``. Method: production modules of at least 200
lines ranked by how many test files reference them; the three largest modules
referenced by exactly ONE test file are pinned here — ``hive/tools/eih_probe.py``
(921 lines), ``system_watcher.py`` (708) and ``hive/tools/deepseek_spend_alert.py``
(689). ``hive/workers/*`` and ``hive/doctor*.py`` are excluded by the brief.

These tests characterise TODAY's behaviour. They are not a specification: where
the behaviour looks wrong it is pinned as ``xfail(strict=True)`` with the reason
below, so fixing the module flips the xfail into a failure that is meant to be
seen and the test promoted.

Suspected defects pinned as xfail (see PLAN-FB75.md):

1. ``eih_probe.classify`` reports ``ok`` for a stream that broke mid-body: only
   ``timeout`` is special-cased ahead of the status checks, so a transport
   failure with status 200 and no content falls into the success branch.
2. ``eih_probe.resolve_models`` accepts an id that is not ``vendor/name`` shaped
   even though its docstring promises ``ConfigError`` for exactly that case.
3. ``system_watcher.capabilities_from_specs`` emits duplicate tags when a host
   has more than one GPU of the same vendor.
4. ``system_watcher._find_bin`` returns a NON-executable file from its extra
   directories (plain ``os.path.exists``), unlike ``shutil.which``.

Nothing here touches the network, ``/sys``, the bus or a real service.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
import sys

import pytest

from hive.tools import deepseek_spend_alert as spend
from hive.tools import eih_probe as eih

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import system_watcher as watcher  # noqa: E402  (root-level satellite module)

UTC = dt.UTC
MIB = 1024 * 1024
GIB = 1024 * MIB


def utc(*args: int) -> dt.datetime:
    return dt.datetime(*args, tzinfo=UTC)


# --------------------------------------------------------------------------- #
# hive/tools/eih_probe.py
# --------------------------------------------------------------------------- #
class EihModelPolicyTests:
    @pytest.mark.parametrize(
        "model_id",
        ["deepseek-v3", "DeepSeek/deepseek-chat", "minimax-01", "openai/gpt-5", "anthropic/claude-x", "google/gemini-2", "x-ai/grok-4"],
    )
    def test_banned_families_are_refused_case_insensitively(self, model_id):
        assert eih.is_banned_model(model_id) is True

    @pytest.mark.parametrize("model_id", ["nvidia/nemotron-3-ultra-550b-a55b", "z-ai/glm-5.3", ""])
    def test_permitted_and_empty_ids_are_not_banned(self, model_id):
        assert eih.is_banned_model(model_id) is False

    def test_defaults_are_the_four_permitted_models(self):
        models = eih.resolve_models({})
        assert models == eih.DEFAULT_MODELS
        assert models[0] == "nvidia/nemotron-3-ultra-550b-a55b"
        assert len(models) == 4

    def test_blank_override_falls_back_to_the_defaults(self):
        assert eih.resolve_models({"EIH_PROBE_MODELS": "   "}) == eih.DEFAULT_MODELS

    def test_override_is_split_trimmed_and_returned_as_a_tuple(self):
        models = eih.resolve_models({"EIH_PROBE_MODELS": " nvidia/a , ,z-ai/b "})
        assert models == ("nvidia/a", "z-ai/b")
        assert isinstance(models, tuple)

    def test_a_banned_override_is_a_config_error(self):
        with pytest.raises(eih.ConfigError) as excinfo:
            eih.resolve_models({"EIH_PROBE_MODELS": "nvidia/a,openai/gpt-5"})
        assert "may not be probed" in str(excinfo.value)

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "FB75 finding 2: resolve_models documents ConfigError for an id that is "
            "not vendor/name shaped, but only ban-ness and emptiness are validated, "
            "so 'not-a-model' is returned and later posted to EIH."
        ),
    )
    def test_a_malformed_id_is_a_config_error(self):
        with pytest.raises(eih.ConfigError):
            eih.resolve_models({"EIH_PROBE_MODELS": "not-a-model"})


class EihPromptTests:
    @pytest.mark.parametrize("hour", [0, 6, 12, 18])
    def test_medium_runs_on_utc_hours_divisible_by_six(self, hour):
        assert eih.prompt_class_for(utc(2026, 10, 31, hour)) == "medium"

    @pytest.mark.parametrize("hour", [1, 5, 7, 11, 13, 23])
    def test_every_other_hour_is_tiny(self, hour):
        assert eih.prompt_class_for(utc(2026, 10, 31, hour)) == "tiny"

    def test_the_hour_is_read_in_utc_not_local_time(self):
        # 01:00 at UTC-5 is 06:00 UTC, so a "local 01:00" run is a medium run.
        minus_five = dt.timezone(dt.timedelta(hours=-5))
        assert eih.prompt_class_for(dt.datetime(2026, 10, 31, 1, tzinfo=minus_five)) == "medium"

    def test_the_medium_prompt_is_byte_identical_across_calls(self):
        assert eih.build_medium_prompt() == eih.build_medium_prompt()

    def test_the_medium_prompt_shape(self):
        prompt = eih.build_medium_prompt(target_tokens=10)
        assert prompt.startswith(eih.MEDIUM_TASK)
        assert "# block 0000" in prompt
        assert prompt.rstrip().endswith("```")
        # Roughly four characters per token, and at least one block.
        assert len(prompt) >= 10 * 4

    def test_a_smaller_target_yields_a_shorter_prompt(self):
        assert len(eih.build_medium_prompt(target_tokens=1)) < len(eih.build_medium_prompt(target_tokens=50))

    def test_prompt_for_medium_uses_the_expanded_prompt(self):
        assert eih.prompt_for("medium") == eih.build_medium_prompt()

    @pytest.mark.parametrize("prompt_class", ["tiny", "TINY", "unknown", ""])
    def test_everything_else_gets_the_tiny_prompt(self, prompt_class):
        assert eih.prompt_for(prompt_class) == eih.TINY_PROMPT

    def test_the_tiny_prompt_answer_is_pinned(self):
        assert eih.TINY_PROMPT_ANSWER == "410"


class EihKeyDiscoveryTests:
    def test_key_file_reads_the_named_variables_and_strips_quotes(self, tmp_path):
        path = tmp_path / "secrets.env"
        path.write_text('# comment\nOTHER=1\nEIH_API_KEY="nvapi-from-file"\n')
        assert eih.key_from_file(path) == "nvapi-from-file"

    def test_key_file_accepts_the_nvidia_variable(self, tmp_path):
        path = tmp_path / "k"
        path.write_text("NVIDIA_API_KEY=nvapi-nvidia\n")
        assert eih.key_from_file(path) == "nvapi-nvidia"

    def test_a_missing_key_file_is_none(self, tmp_path):
        assert eih.key_from_file(tmp_path / "absent") is None

    def test_zoder_config_takes_the_key_of_an_eih_block(self, tmp_path):
        path = tmp_path / "config.toml"
        path.write_text('[provider.eih_reviewer]\napi_key = "nvapi-zoder"\n\n[provider.other]\napi_key = "nvapi-other"\n')
        assert eih.key_from_zoder_config(path) == "nvapi-zoder"

    def test_zoder_config_ignores_a_non_nvapi_key(self, tmp_path):
        path = tmp_path / "config.toml"
        path.write_text('[provider.eih_reviewer]\napi_key = "sk-not-nvidia"\n')
        assert eih.key_from_zoder_config(path) is None

    def test_the_environment_wins(self, tmp_path):
        assert eih.load_api_key({"EIH_API_KEY": "  nvapi-env  "}) == "nvapi-env"

    def test_the_key_file_is_used_when_the_environment_is_empty(self, tmp_path, monkeypatch):
        path = tmp_path / "secrets.env"
        path.write_text("EIH_API_KEY=nvapi-file\n")
        monkeypatch.setattr(eih, "DEFAULT_KEY_FILE", str(tmp_path / "absent"))
        assert eih.load_api_key({"EIH_KEY_FILE": str(path)}) == "nvapi-file"

    def test_a_missing_key_everywhere_is_a_config_error(self, tmp_path, monkeypatch):
        monkeypatch.setattr(eih, "DEFAULT_KEY_FILE", str(tmp_path / "absent"))
        monkeypatch.setattr(eih, "ZODER_CONFIG_FILES", ())
        with pytest.raises(eih.ConfigError) as excinfo:
            eih.load_api_key({"EIH_KEY_FILE": str(tmp_path / "also-absent")})
        assert "no EIH API key found" in str(excinfo.value)


class EihRedactionTests:
    def test_an_explicit_secret_is_masked(self):
        assert eih.redact("token abc123 here", "abc123") == "token [REDACTED] here"

    def test_nvapi_and_bearer_shapes_are_masked_without_a_secret(self):
        assert eih.redact("key nvapi-ABC_9") == "key [REDACTED]"
        # The Bearer pattern swallows the scheme word itself.
        assert eih.redact("Authorization: Bearer abc.def") == "Authorization: [REDACTED]"

    @pytest.mark.parametrize("value", [None, ""])
    def test_falsey_text_passes_through(self, value):
        assert eih.redact(value) == value

    def test_an_excerpt_collapses_whitespace(self):
        assert eih.error_excerpt("a\n\n  b\tc") == "a b c"

    def test_an_excerpt_is_capped_at_three_hundred_characters(self):
        excerpt = eih.error_excerpt("x " * 400)
        assert len(excerpt) == eih.ERROR_EXCERPT_LIMIT == 300
        assert excerpt.endswith("…")

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_an_empty_excerpt_is_none(self, value):
        assert eih.error_excerpt(value) is None


class EihClassificationTests:
    def test_a_timeout(self):
        assert eih.classify(eih.StreamResult(failure="timeout")) == eih.OUTCOME_TIMEOUT

    def test_a_transport_failure_before_any_status(self):
        assert eih.classify(eih.StreamResult(failure="transport")) == eih.OUTCOME_TRANSPORT

    def test_an_unprovisioned_function_is_its_own_outcome(self):
        result = eih.StreamResult(http_status=404, body='{"error": "Function not found"}')
        assert eih.classify(result) == eih.OUTCOME_404

    def test_a_plain_404_is_a_plain_http_error(self):
        assert eih.classify(eih.StreamResult(http_status=404, body="no such route")) == "http_404"

    @pytest.mark.parametrize("status", [429, 503, 504, 529])
    def test_the_transient_gateway_statuses_keep_their_number(self, status):
        assert eih.classify(eih.StreamResult(http_status=status)) == str(status)

    def test_a_200_with_content_is_ok(self):
        assert eih.classify(eih.StreamResult(http_status=200, content="410")) == eih.OUTCOME_OK

    def test_an_empty_length_stopped_body_is_budget_exhausted(self):
        result = eih.StreamResult(http_status=200, content="   ", finish_reason="length")
        assert eih.classify(result) == eih.OUTCOME_BUDGET

    def test_an_empty_body_that_did_not_stop_on_length_is_still_ok(self):
        assert eih.classify(eih.StreamResult(http_status=200, content="")) == eih.OUTCOME_OK

    def test_a_server_error_is_an_http_error(self):
        assert eih.classify(eih.StreamResult(http_status=500)) == "http_500"

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "FB75 finding 1: a transport failure during the SSE body (status already "
            "200, no content) falls through to the success branch, so a truncated "
            "probe is recorded as ok. Only timeout is special-cased before the status."
        ),
    )
    def test_a_stream_that_broke_mid_body_is_not_ok(self):
        assert eih.classify(eih.StreamResult(http_status=200, failure="transport", content="")) != eih.OUTCOME_OK

    @pytest.mark.parametrize(
        ("outcome", "retried"),
        [
            ("429", True),
            ("503", True),
            ("504", True),
            ("529", True),
            (eih.OUTCOME_TIMEOUT, True),
            (eih.OUTCOME_TRANSPORT, True),
            (eih.OUTCOME_404, False),
            (eih.OUTCOME_BUDGET, False),
            (eih.OUTCOME_SKIPPED_LOAD, False),
            (eih.OUTCOME_SKIPPED_WINDOW, False),
            (eih.OUTCOME_OK, False),
            ("http_400", False),
        ],
    )
    def test_only_the_transient_classes_are_retried(self, outcome, retried):
        assert eih.should_retry(outcome) is retried


class EihRowTests:
    def test_a_row_has_exactly_the_contract_fields_in_order(self):
        row = eih.make_row(
            now=utc(2026, 10, 31, 12), model_id="nvidia/x", prompt_class="tiny", outcome="ok", host="h", host_load1=0.5
        )
        assert list(row) == list(eih.ROW_FIELDS)
        assert row["provider_id"] == eih.PROVIDER_ID == "nvidia-eih"
        assert row["probe_tool"] == eih.PROBE_TOOL
        assert row["tool_version"] == eih.TOOL_VERSION

    def test_unobserved_numbers_default_to_null(self):
        row = eih.make_row(
            now=utc(2026, 10, 31, 12), model_id="m", prompt_class="tiny", outcome="ok", host="h", host_load1=None
        )
        for field in ("http_status", "latency_ms", "first_token_ms", "headers_ms", "tokens_in", "tokens_out"):
            assert row[field] is None

    def test_the_timestamp_is_second_precision_utc(self):
        row = eih.make_row(
            now=dt.datetime(2026, 10, 31, 12, 0, 0, 123456, tzinfo=UTC),
            model_id="m",
            prompt_class="tiny",
            outcome="ok",
            host="h",
            host_load1=None,
        )
        assert row["measured_at"] == "2026-10-31T12:00:00Z"

    def test_the_secret_never_reaches_the_row(self):
        metric = eih.StreamResult(http_status=500, body="failed with nvapi-SECRETVALUE")
        row = eih.make_row(
            now=utc(2026, 10, 31, 12),
            model_id="m",
            prompt_class="tiny",
            outcome="http_500",
            host="h",
            host_load1=None,
            metric=metric,
            secret="nvapi-SECRETVALUE",
        )
        assert "SECRETVALUE" not in json.dumps(row)
        assert "[REDACTED]" in row["error_excerpt"]

    def test_an_error_body_becomes_the_excerpt(self):
        metric = eih.StreamResult(http_status=400, body="bad request: model unknown")
        row = eih.make_row(
            now=utc(2026, 10, 31, 12),
            model_id="m",
            prompt_class="tiny",
            outcome="http_400",
            host="h",
            host_load1=None,
            metric=metric,
        )
        assert row["error_excerpt"] == "bad request: model unknown"

    def test_the_publish_window_edges(self):
        assert eih.in_publish_window(utc(2026, 10, 31, 21, 54)) is False
        assert eih.in_publish_window(utc(2026, 10, 31, 21, 55)) is True
        assert eih.in_publish_window(utc(2026, 10, 31, 22, 29)) is True
        assert eih.in_publish_window(utc(2026, 10, 31, 22, 30)) is False

    @pytest.mark.parametrize(
        ("load1", "cpus", "gated"),
        [(8.1, 8, True), (8.0, 8, False), (None, 8, False), (9.0, 0, False), (9.0, None, False)],
    )
    def test_the_load_gate(self, load1, cpus, gated):
        assert eih.load_gate(load1, cpus) is gated


class EihRowWriterTests:
    def _writer(self, tmp_path, **kwargs):
        return eih.RowWriter(tmp_path, log=kwargs.pop("log", lambda _m: None), **kwargs)

    def test_the_file_name_is_the_utc_date(self, tmp_path):
        writer = self._writer(tmp_path)
        assert writer.path_for(utc(2026, 10, 31, 23, 59)).name == "eih-probe-2026-10-31.jsonl"

    def test_a_written_row_is_0600_and_append_only(self, tmp_path):
        writer = self._writer(tmp_path)
        now = utc(2026, 10, 31, 12)
        assert writer.write({"a": 1}, now) is True
        assert writer.write({"a": 2}, now) is True
        path = writer.path_for(now)
        assert oct(path.stat().st_mode & 0o777) == "0o600"
        assert [json.loads(line)["a"] for line in path.read_text().splitlines()] == [1, 2]

    def test_the_size_cap_refuses_the_row_and_logs_once(self, tmp_path):
        messages: list[str] = []
        writer = self._writer(tmp_path, max_bytes=5, log=messages.append)
        now = utc(2026, 10, 31, 12)
        assert writer.write({"a": 1}, now) is False
        assert writer.write({"a": 1}, now) is False
        assert len(messages) == 1
        assert not writer.path_for(now).exists()

    def test_prune_removes_exactly_the_expired_probe_files(self, tmp_path):
        writer = self._writer(tmp_path, retention_days=30)
        for name in ("eih-probe-2026-09-30.jsonl", "eih-probe-2026-10-01.jsonl", "eih-probe-nope.jsonl", "other.jsonl"):
            (tmp_path / name).write_text("{}\n")
        removed = {path.name for path in writer.prune(now=utc(2026, 10, 31, 12))}
        assert removed == {"eih-probe-2026-09-30.jsonl"}
        assert sorted(p.name for p in tmp_path.iterdir()) == [
            "eih-probe-2026-10-01.jsonl",
            "eih-probe-nope.jsonl",
            "other.jsonl",
        ]

    def test_prune_on_a_missing_directory_is_empty(self, tmp_path):
        assert self._writer(tmp_path / "absent").prune(now=utc(2026, 10, 31)) == []


# --------------------------------------------------------------------------- #
# hive/tools/deepseek_spend_alert.py
# --------------------------------------------------------------------------- #
def snapshot(ts: dt.datetime, *, total: float, topped_up: float = 0.0, granted: float = 0.0) -> dict:
    return {
        "ts": ts,
        "ts_utc": spend.fmt_ts(ts),
        "total": total,
        "granted": granted,
        "topped_up": topped_up,
        "currency": "USD",
    }


class SpendConfigTests:
    def test_defaults(self):
        config = spend.Config({})
        assert config.currency == "USD"
        assert config.ceiling == 100.0
        assert config.thresholds == (70.0, 90.0)
        assert config.baseline_month == "2026-10"
        assert config.baseline_total == 252.11
        assert config.baseline_topped_up == 240.71

    def test_the_currency_is_upper_cased_and_the_bus_url_trimmed(self):
        config = spend.Config({"DEEPSEEK_CURRENCY": " eur ", "HIVE_BUS_URL": "http://bus.local:8000/", "DEEPSEEK_API_KEY": "x"})
        assert config.currency == "EUR"
        assert config.bus_url == "http://bus.local:8000"
        assert config.api_key == "x"

    def test_the_state_file_lives_beside_the_snapshots(self, tmp_path):
        config = spend.Config({"DEEPSEEK_SNAPSHOT_PATH": str(tmp_path / "bal.jsonl")})
        assert config.state_path == tmp_path / "deepseek-spend-alert-state.json"

    @pytest.mark.parametrize("value", ["abc", "0", "-5"])
    def test_a_bad_ceiling_is_a_config_error(self, value):
        with pytest.raises(spend.ConfigError):
            spend.Config({"DEEPSEEK_MONTHLY_CEILING_USD": value})

    @pytest.mark.parametrize("value", ["200 abc", "0", "-10"])
    def test_bad_thresholds_are_a_config_error(self, value):
        with pytest.raises(spend.ConfigError):
            spend.Config({"DEEPSEEK_ALERT_THRESHOLDS": value})

    def test_thresholds_are_sorted_and_de_duplicated(self):
        config = spend.Config({"DEEPSEEK_ALERT_THRESHOLDS": "200,50 100 50"})
        assert config.thresholds == (50.0, 100.0, 200.0)


class SpendTimeHelperTests:
    def test_z_suffix_and_naive_values_are_utc(self):
        assert spend.parse_ts("2026-10-25T23:30:00Z") == utc(2026, 10, 25, 23, 30)
        assert spend.parse_ts("2026-10-25T23:30:00") == utc(2026, 10, 25, 23, 30)

    def test_an_explicit_offset_is_converted(self):
        assert spend.parse_ts("2026-10-26T01:30:00+02:00") == utc(2026, 10, 25, 23, 30)

    def test_format_round_trips(self):
        moment = utc(2026, 10, 25, 23, 30)
        assert spend.fmt_ts(moment) == "2026-10-25T23:30:00Z"
        assert spend.parse_ts(spend.fmt_ts(moment)) == moment

    def test_month_key_and_start(self):
        assert spend.month_key(utc(2026, 10, 31, 23, 59)) == "2026-10"
        assert spend.month_start(utc(2026, 10, 31, 23, 59)) == utc(2026, 10, 1)


class SpendSnapshotTests:
    def test_a_missing_file_is_empty(self, tmp_path):
        assert spend.read_snapshots(tmp_path / "absent.jsonl", "USD") == []

    def test_only_valid_rows_of_the_currency_survive(self, tmp_path):
        path = tmp_path / "bal.jsonl"
        good = {"ts_utc": "2026-10-01T00:00:00Z", "total": 10, "granted": 1, "topped_up": 9, "currency": "usd"}
        other = dict(good, currency="EUR")
        bad_json = "{not json"
        missing = {"ts_utc": "2026-10-01T00:00:00Z", "total": 1}
        bad_number = dict(good, total="lots")
        path.write_text("\n".join([json.dumps(good), bad_json, "", json.dumps(other), json.dumps(missing), json.dumps(bad_number)]))
        rows = spend.read_snapshots(path, "USD")
        assert len(rows) == 1
        assert rows[0]["total"] == 10.0
        assert rows[0]["ts"] == utc(2026, 10, 1)
        assert rows[0]["ts_utc"] == "2026-10-01T00:00:00Z"

    def test_append_writes_only_the_contract_fields_sorted(self, tmp_path):
        path = tmp_path / "sub" / "bal.jsonl"
        spend.append_snapshot(path, {**snapshot(utc(2026, 10, 1), total=5), "extra": "ignored"})
        written = json.loads(path.read_text().strip())
        assert list(written) == sorted(spend.SNAPSHOT_FIELDS)
        assert "extra" not in written

    def test_save_and_load_state_round_trip(self, tmp_path):
        path = tmp_path / "state.json"
        spend.save_state(path, "2026-10", [90.0, 70.0, 70.0])
        assert spend.load_state(path) == {"month": "2026-10", "sent": [70.0, 90.0]}

    def test_a_corrupt_state_file_reads_as_empty(self, tmp_path):
        path = tmp_path / "state.json"
        path.write_text("{not json")
        assert spend.load_state(path) == {}

    def test_a_list_state_file_reads_as_empty(self, tmp_path):
        path = tmp_path / "state.json"
        path.write_text("[1, 2]")
        assert spend.load_state(path) == {}


class SpendArithmeticTests:
    def test_topups_count_only_positive_steps(self):
        snaps = [
            snapshot(utc(2026, 10, 2), total=90, topped_up=15.0),
            snapshot(utc(2026, 10, 3), total=80, topped_up=5.0),
            snapshot(utc(2026, 10, 4), total=70, topped_up=25.0),
        ]
        assert spend.topups_between(snaps, utc(2026, 10, 1), utc(2026, 10, 5), 10.0) == 25.0

    def test_snapshots_outside_the_window_do_not_move_the_anchor(self):
        snaps = [
            snapshot(utc(2026, 9, 20), total=50, topped_up=999.0),
            snapshot(utc(2026, 10, 2), total=90, topped_up=15.0),
        ]
        assert spend.topups_between(snaps, utc(2026, 10, 1), utc(2026, 10, 5), 10.0) == 5.0

    def test_the_month_start_total_prefers_a_baseline_naming_this_month(self):
        config = spend.Config({"DEEPSEEK_BASELINE_MONTH": "2026-10", "DEEPSEEK_BASELINE_TS": "2026-10-01T00:00:00Z", "DEEPSEEK_BASELINE_TOTAL": "300"})
        total, source = spend.month_start_total([snapshot(utc(2026, 10, 2), total=250)], utc(2026, 10, 1), utc(2026, 10, 5), config)
        assert (total, source) == (300.0, "baseline")

    def test_otherwise_the_first_snapshot_inside_the_month(self):
        config = spend.Config({"DEEPSEEK_BASELINE_MONTH": "1999-01"})
        total, source = spend.month_start_total([snapshot(utc(2026, 10, 3), total=250)], utc(2026, 10, 1), utc(2026, 10, 5), config)
        assert total == 250.0
        assert source.startswith("first snapshot")

    def test_otherwise_the_newest_snapshot_before_the_month(self):
        config = spend.Config({"DEEPSEEK_BASELINE_MONTH": "1999-01"})
        total, source = spend.month_start_total([snapshot(utc(2026, 9, 30, 23), total=200)], utc(2026, 10, 1), utc(2026, 10, 5), config)
        assert total == 200.0
        assert source.startswith("last snapshot before month start")

    def test_no_figure_at_all_is_a_config_error(self):
        config = spend.Config({"DEEPSEEK_BASELINE_MONTH": "1999-01"})
        with pytest.raises(spend.ConfigError):
            spend.month_start_total([], utc(2026, 10, 1), utc(2026, 10, 5), config)

    def _spend_config(self, **env):
        base = {"DEEPSEEK_MONTHLY_CEILING_USD": "100", "DEEPSEEK_BASELINE_MONTH": "1999-01"}
        return spend.Config({**base, **env})

    def test_month_to_date_spend_is_start_plus_topups_minus_current(self):
        snaps = [snapshot(utc(2026, 10, 1), total=100.0, topped_up=100.0), snapshot(utc(2026, 10, 5), total=70.0, topped_up=120.0)]
        result = spend.compute_spend(snaps, utc(2026, 10, 6), self._spend_config())
        assert result["spend"] == 50.0
        assert result["topups"] == 20.0
        assert result["start_total"] == 100.0
        assert result["current_total"] == 70.0
        assert result["pct"] == 50.0
        assert result["month"] == "2026-10"

    def test_a_balance_above_the_month_start_clamps_to_zero_spend(self):
        snaps = [snapshot(utc(2026, 10, 1), total=10.0), snapshot(utc(2026, 10, 5), total=99.0)]
        assert spend.compute_spend(snaps, utc(2026, 10, 6), self._spend_config())["spend"] == 0.0

    def test_no_snapshot_to_price_against_is_a_config_error(self):
        with pytest.raises(spend.ConfigError):
            spend.compute_spend([], utc(2026, 10, 6), self._spend_config())

    def test_the_alert_sentence(self):
        result = {"spend": 50.0, "pct": 50.4, "current_total": 70.0, "topups": 20.0}
        assert spend.format_alert_message(result, 100.0) == (
            "DeepSeek spend $50.00 this month (50% of $100 ceiling); "
            "balance $70.00; top-ups since month start $20.00"
        )

    def test_thresholds_already_sent_this_month_are_suppressed(self):
        assert spend.thresholds_due(60.0, (50.0, 80.0), {"month": "2026-10", "sent": [50.0]}, "2026-10") == []
        assert spend.thresholds_due(85.0, (50.0, 80.0), {"month": "2026-10", "sent": [50.0]}, "2026-10") == [80.0]

    def test_a_stale_month_reopens_every_reached_threshold(self):
        assert spend.thresholds_due(90.0, (50.0, 80.0), {"month": "2026-09", "sent": [50.0, 80.0]}, "2026-10") == [50.0, 80.0]

    def test_nothing_is_due_below_the_lowest_threshold(self):
        assert spend.thresholds_due(10.0, (50.0, 80.0), {}, "2026-10") == []


# --------------------------------------------------------------------------- #
# system_watcher.py
# --------------------------------------------------------------------------- #
class WatcherGpuEntryTests:
    def test_an_apu_folds_gtt_into_the_memory_figures(self):
        entry = watcher._amd_entry_from_bytes(
            name="card0", util=5.0, vram_used=512 * MIB, vram_total=512 * MIB, gtt_used=2 * GIB, gtt_total=8 * GIB, temp_c=None, power_w=None
        )
        assert entry["unified_memory"] is True
        assert entry["mem_used_mib"] == 512 + 2048
        assert entry["mem_total_mib"] == 512 + 8192
        assert entry["vram_total_mib"] == 512
        assert entry["gtt_total_mib"] == 8192

    def test_a_discrete_card_keeps_vram_only_figures(self):
        entry = watcher._amd_entry_from_bytes(
            name="c1", util=None, vram_used=GIB, vram_total=8 * GIB, gtt_used=0, gtt_total=0, temp_c=40.0, power_w=12.0
        )
        assert entry["unified_memory"] is False
        assert entry["mem_used_mib"] == 1024
        assert entry["mem_total_mib"] == 8192
        assert (entry["util_pct"], entry["temp_c"], entry["power_w"]) == (None, 40.0, 12.0)

    def test_a_card_with_no_vram_reading_is_not_unified(self):
        entry = watcher._amd_entry_from_bytes(name="z", util=None, vram_used=0, vram_total=0, gtt_used=10, gtt_total=20, temp_c=None, power_w=None)
        assert entry["unified_memory"] is False
        assert entry["mem_total_mib"] == 0

    def test_rocm_json_keeps_names_percentages_and_watts(self):
        payload = {
            "card0": {
                "Card SKU": "gfx1103",
                "GPU use (%)": "42%",
                "VRAM Total Memory (B)": "536870912",
                "VRAM Total Used Memory (B)": "1024",
                "GTT Total Memory (B)": "8589934592",
                "GTT Total Used Memory (B)": "2048",
                "Temperature (Sensor edge) (C)": "51.0",
                "Average Graphics Package Power (W)": "17.5W",
            },
            "not-a-card": "ignored",
        }
        entries = watcher._parse_rocm_json(payload)
        assert len(entries) == 1
        entry = entries[0]
        assert entry["name"] == "gfx1103"
        assert entry["util_pct"] == 42.0
        assert entry["vram_total_mib"] == 512
        assert entry["unified_memory"] is True
        assert entry["temp_c"] == 51.0
        assert entry["power_w"] == 17.5

    def test_rocm_json_defaults_missing_numbers_to_zero(self):
        entry = watcher._parse_rocm_json({"card0": {"Card series": "Navi"}})[0]
        assert entry["name"] == "Navi"
        # A missing percentage reads as 0.0, not "unknown".
        assert entry["util_pct"] == 0.0
        assert (entry["vram_used_mib"], entry["vram_total_mib"]) == (0, 0)

    def test_sysfs_fallback_reads_vram_and_gtt(self, tmp_path):
        device = tmp_path / "class" / "drm" / "card0" / "device"
        device.mkdir(parents=True)
        (device / "mem_info_vram_total").write_text(str(512 * MIB))
        (device / "mem_info_vram_used").write_text(str(256 * MIB))
        (device / "mem_info_gtt_total").write_text(str(8 * GIB))
        (device / "mem_info_gtt_used").write_text(str(GIB))
        (device / "gpu_busy_percent").write_text("7")
        # A second card with no vram files is skipped, as on a non-amdgpu node.
        (tmp_path / "class" / "drm" / "card1" / "device").mkdir(parents=True)
        entries = watcher._apu_mem_from_sysfs(str(tmp_path))
        assert len(entries) == 1
        entry = entries[0]
        assert entry["name"] == "card0"
        assert entry["util_pct"] == 7.0
        assert entry["unified_memory"] is True
        assert entry["mem_total_mib"] == 512 + 8192


class WatcherCapabilityTests:
    def test_the_base_tags_are_always_present(self):
        assert watcher.capabilities_from_specs({})[:2] == ["system", "host"]

    def test_tool_presence_becomes_tags(self):
        caps = watcher.capabilities_from_specs({"has_docker": True, "has_podman": True, "has_buildx": True, "has_npu": True})
        for tag in ("docker", "podman", "build", "buildx", "multi-arch-build", "npu", "xdna"):
            assert tag in caps

    @pytest.mark.parametrize(
        ("vendor", "expected"),
        [
            ("nvidia", ["gpu", "nvidia-gpu", "cuda"]),
            ("amd", ["gpu", "amd-gpu", "rocm", "vulkan-compute"]),
            ("intel", ["gpu", "intel-igpu"]),
            ("apple", ["gpu", "apple-gpu", "metal", "mlx"]),
        ],
    )
    def test_gpu_vendors_become_tags(self, vendor, expected):
        caps = watcher.capabilities_from_specs({"gpus": [{"vendor": vendor}]})
        assert [tag for tag in caps if tag in expected] == expected

    def test_an_unknown_vendor_adds_no_gpu_tag(self):
        assert "gpu" not in watcher.capabilities_from_specs({"gpus": [{"vendor": "matrox"}]})

    def test_size_and_cpu_thresholds(self):
        caps = watcher.capabilities_from_specs({"ram_gb": 32, "cpu_count": 16})
        assert "ram-32g+" in caps and "ram-64g+" not in caps
        assert "cpu-16t+" in caps and "cpu-32t+" not in caps
        big = watcher.capabilities_from_specs({"ram_gb": 128, "cpu_count": 64})
        assert {"ram-32g+", "ram-64g+", "ram-128g+", "cpu-16t+", "cpu-32t+"} <= set(big)

    def test_platform_tags(self):
        caps = watcher.capabilities_from_specs({"os": "Darwin", "arch": "arm64"})
        assert "macos" in caps and "arm64" in caps

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "FB75 finding 3: capabilities_from_specs appends the vendor tags once per GPU, "
            "so a two-AMD-GPU host advertises gpu/amd-gpu/rocm/vulkan-compute twice."
        ),
    )
    def test_capability_tags_are_unique(self):
        caps = watcher.capabilities_from_specs({"gpus": [{"vendor": "amd"}, {"vendor": "amd"}]})
        assert len(caps) == len(set(caps))


class WatcherHelperTests:
    def test_find_bin_prefers_path(self):
        found = watcher._find_bin("sh")
        assert found and os.path.basename(found) == "sh"

    def test_find_bin_falls_back_to_the_extra_directories(self, tmp_path):
        busybox = tmp_path / "rocm-smi"
        busybox.write_text("#!/bin/sh\n")
        busybox.chmod(0o755)
        assert watcher._find_bin("rocm-smi", [str(tmp_path)]) == str(busybox)

    def test_find_bin_returns_none_when_absent(self, tmp_path):
        assert watcher._find_bin("definitely-not-installed-fb75", [str(tmp_path)]) is None

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "FB75 finding 4: the extra-directory fallback uses os.path.exists, not an "
            "executable check, so a stray 0644 file would be returned and handed to subprocess."
        ),
    )
    def test_find_bin_does_not_return_a_non_executable_file(self, tmp_path):
        stray = tmp_path / "rocm-smi"
        stray.write_text("not a program\n")
        stray.chmod(0o644)
        assert watcher._find_bin("rocm-smi", [str(tmp_path)]) is None

    def test_the_worker_target_is_a_bounded_integer(self):
        assert isinstance(watcher._target_workers({"cpu_count": 8, "ram_gb": 32}, {}), int)
        assert watcher._target_workers({"cpu_count": 1, "ram_gb": 1}, {"load_1min": 40}) >= 1

    def test_a_saturated_host_never_targets_more_workers_than_an_idle_one(self):
        specs = {"cpu_count": 16, "ram_gb": 64, "gpus": [{"vendor": "amd"}]}
        idle = watcher._target_workers(specs, {"load_1min": 0.1})
        saturated = watcher._target_workers(specs, {"load_1min": 16 * 0.95})
        assert saturated <= idle
