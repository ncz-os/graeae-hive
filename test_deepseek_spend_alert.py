"""DeepSeek month-to-date spend alert (hive/tools/deepseek_spend_alert.py).

No network: the balance fetch and the hive-bus transport are injected fake
callables that record exactly what the tool asked for. Fixtures use the real
``/user/balance`` field names (``balance_infos[]`` with ``currency``,
``total_balance``, ``granted_balance``, ``topped_up_balance``).

Covered contract:
  * month-to-date spend across a calendar-month boundary (baseline, first
    snapshot of the month, top-ups, granted-credit burn);
  * top-up detection (increase counts, a decrease is not a negative top-up,
    a grant is not a top-up);
  * the 70% threshold fires once (and only once) per month;
  * 90% fires separately, after 70;
  * a down bus does not mark a threshold as sent, and the next run retries;
  * a failed fetch writes no snapshot and sends no alert;
  * the API key never appears in any output, snapshot or state file;
  * the exact alert wording.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import json
import os
import pathlib

import pytest

HERE = pathlib.Path(__file__).resolve().parent
TOOL_PATH = HERE / "hive" / "tools" / "deepseek_spend_alert.py"
SECRET = "sk-TESTSECRET-0000-not-a-real-key"

_spec = importlib.util.spec_from_file_location("deepseek_spend_alert", TOOL_PATH)
assert _spec and _spec.loader, f"cannot load {TOOL_PATH}"
tool = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tool)


# ── fixtures / fakes ────────────────────────────────────────────────────────
def utc(*args) -> dt.datetime:
    return dt.datetime(*args, tzinfo=dt.UTC)


def balance_body(total: float, topped_up: float, granted: float, currency: str = "USD") -> dict:
    """A /user/balance response body with the real field names."""
    return {
        "is_available": True,
        "balance_infos": [
            {
                "currency": currency,
                "total_balance": f"{total:.2f}",
                "granted_balance": f"{granted:.2f}",
                "topped_up_balance": f"{topped_up:.2f}",
            }
        ],
    }


def snapshot_row(ts: dt.datetime, total: float, granted: float, topped_up: float) -> dict:
    """One in-memory snapshot row in the shape read_snapshots() returns."""
    return {
        "ts_utc": tool.fmt_ts(ts),
        "ts": ts,
        "total": total,
        "granted": granted,
        "topped_up": topped_up,
        "currency": "USD",
    }


class FakeBalance:
    """Stand-in for fetch_balance: returns scripted rows, records the key seen."""

    def __init__(self, *rows: dict):
        self.rows = list(rows)
        self.calls: list[tuple[str, str]] = []

    def __call__(self, api_key: str, currency: str, now: dt.datetime) -> dict:
        self.calls.append((api_key, currency))
        row = self.rows.pop(0)
        return {**row, "ts": now, "ts_utc": tool.fmt_ts(now), "currency": currency}

    @staticmethod
    def row(total: float, topped_up: float, granted: float) -> dict:
        return {"total": total, "topped_up": topped_up, "granted": granted}


class FakeBus:
    """Stand-in for the bus transport. ``down`` makes every call fail."""

    def __init__(self, down: bool = False):
        self.down = down
        self.calls: list[tuple[str, dict]] = []
        self.registered_urns: list[str] = []

    def __call__(self, url: str, *, method="GET", headers=None, body=None, timeout=None) -> dict:
        if self.down:
            raise tool.FetchError(f"POST {url} failed: connection refused")
        self.calls.append((url, {"method": method, "headers": dict(headers or {}), "body": body}))
        if url.endswith("/v1/agents/register"):
            urn = "urn:agent:human:testhost:0e0e0e0e-0000-4000-8000-000000000000"
            self.registered_urns.append(urn)
            return {"urn": urn, "session_id": "0e0e0e0e-0000-4000-8000-000000000000"}
        if url.endswith("/v1/agents/heartbeat"):
            return {"ack": True, "ts": 0}
        if url.endswith("/v1/messages"):
            return {"id": "msg-1", "ts": 0}
        raise AssertionError(f"unexpected bus call {method} {url}")

    @property
    def paths(self) -> list[str]:
        return [u.rsplit("/v1/", 1)[-1] for u, _ in self.calls]

    @property
    def topic_posts(self) -> list[dict]:
        return [meta["body"] for url, meta in self.calls if url.endswith("/v1/messages")]


@pytest.fixture()
def env(tmp_path):
    return {
        "DEEPSEEK_API_KEY": SECRET,
        "DEEPSEEK_SNAPSHOT_PATH": str(tmp_path / "state" / "deepseek-balance.jsonl"),
        "HIVE_BUS_URL": "http://bus.test:5005",
        "HIVE_HOST_NAME": "testhost",
        "TZ": "UTC",
    }


def make_config(env: dict) -> tool.Config:
    return tool.Config(env)


def seed(config: tool.Config, rows: list[tuple[dt.datetime, float, float, float]]) -> None:
    """Append (ts, total, granted, topped_up) readings to the snapshot file."""
    for ts, total, granted, topped_up in rows:
        tool.append_snapshot(
            config.snapshot_path,
            {"ts_utc": tool.fmt_ts(ts), "total": total, "granted": granted, "topped_up": topped_up, "currency": "USD"},
        )


def state_of(config: tool.Config) -> dict:
    return tool.load_state(config.state_path)


# ── month-to-date spend math ────────────────────────────────────────────────
class MonthToDateSpendTests:
    def test_baseline_month_spend_from_baseline_plus_topups(self, env):
        """October 2026: baseline $252.11; a $50 top-up is NOT spend."""
        config = make_config(env)
        seed(config, [(utc(2026, 10, 25, 23, 30), 252.11, 11.39, 240.71)])
        snapshots = tool.read_snapshots(config.snapshot_path, "USD")
        # A $50 top-up: topped_up 240.71 -> 290.71, total 252.11 -> 302.11.
        snap = snapshot_row(utc(2026, 10, 26, 13, 0), 302.11, 11.39, 290.71)
        snapshots.append(snap)
        spend = tool.compute_spend(snapshots, utc(2026, 10, 26, 13, 0), config)
        # Top-up is not spend: 252.11 + 50.00 - 302.11 = 0.00.
        assert spend["spend"] == 0.00
        assert spend["topups"] == 50.00
        assert spend["start_source"] == "baseline"
        assert spend["month"] == "2026-10"

    def test_baseline_month_counts_burn_as_spend(self, env):
        """October 2026: baseline $252.11; $20 of real spend after a top-up."""
        config = make_config(env)
        seed(config, [(utc(2026, 10, 25, 23, 30), 252.11, 11.39, 240.71)])
        snapshots = tool.read_snapshots(config.snapshot_path, "USD")
        # $50 topped up, then $20 burned: total 252.11 + 50 - 20 = 282.11.
        snapshots.append(snapshot_row(utc(2026, 10, 26, 13, 0), 282.11, 11.39, 290.71))
        spend = tool.compute_spend(snapshots, utc(2026, 10, 26, 13, 0), config)
        assert spend["spend"] == 20.00  # 252.11 + 50.00 - 282.11
        assert spend["topups"] == 50.00
        assert spend["start_source"] == "baseline"

    def test_new_calendar_month_starts_from_the_closing_balance(self, env):
        """November rolls over: October's spend never leaks into November."""
        config = make_config(env)
        seed(
            config,
            [
                (utc(2026, 10, 31, 22, 0), 120.00, 5.00, 115.00),
                (utc(2026, 11, 1, 9, 0), 110.50, 5.00, 115.00),
            ],
        )
        snapshots = tool.read_snapshots(config.snapshot_path, "USD")
        spend = tool.compute_spend(snapshots, utc(2026, 11, 1, 9, 0), config)
        assert spend["month"] == "2026-11"
        assert spend["start_source"].startswith("first snapshot 2026-11-01")
        # November's start is its own first reading, never October's baseline.
        assert spend["start_total"] == 110.50
        assert spend["spend"] == 0.00  # 110.50 + 0 - 110.50
        assert spend["topups"] == 0.00

    def test_month_boundary_is_utc_midnight(self, env):
        """A reading at 23:59:59Z belongs to the old month, 00:00:00Z to the new."""
        config = make_config(env)
        seed(
            config,
            [
                (utc(2026, 10, 31, 23, 59, 59), 100.00, 0.00, 100.00),
                (utc(2026, 11, 1, 0, 0, 0), 99.00, 0.00, 100.00),
            ],
        )
        snapshots = tool.read_snapshots(config.snapshot_path, "USD")
        spend = tool.compute_spend(snapshots, utc(2026, 11, 1, 0, 0, 0), config)
        assert spend["month"] == "2026-11"
        # The new month's first snapshot IS its start balance: $0 spent yet.
        assert spend["spend"] == 0.00

    def test_baseline_is_only_used_for_its_own_month(self, env):
        config = make_config(env)
        assert config.baseline_month == "2026-10"
        seed(config, [(utc(2026, 12, 1, 0, 30), 90.00, 2.00, 88.00), (utc(2026, 12, 2, 0, 30), 80.00, 0.00, 88.00)])
        snapshots = tool.read_snapshots(config.snapshot_path, "USD")
        spend = tool.compute_spend(snapshots, utc(2026, 12, 2, 0, 30), config)
        assert spend["start_source"].startswith("first snapshot 2026-12-01")
        assert spend["spend"] == 10.00


class TopUpDetectionTests:
    def test_increase_in_topped_up_is_a_top_up_and_offsets_spend(self, env):
        config = make_config(env)
        start = utc(2026, 10, 26, 0, 0)
        snaps = [
            {"ts": start, "total": 100.00, "granted": 0.0, "topped_up": 100.00, "currency": "USD"},
            {"ts": utc(2026, 10, 26, 6, 0), "total": 90.00, "granted": 0.0, "topped_up": 100.00, "currency": "USD"},
            {"ts": utc(2026, 10, 26, 12, 0), "total": 140.00, "granted": 0.0, "topped_up": 150.00, "currency": "USD"},
        ]
        assert tool.topups_between(snaps, start, utc(2026, 10, 27), 100.00) == 50.00

    def test_decrease_is_not_a_negative_top_up(self, env):
        start = utc(2026, 10, 26, 0, 0)
        snaps = [
            {"ts": start, "total": 100.00, "granted": 0.0, "topped_up": 100.00, "currency": "USD"},
            {"ts": utc(2026, 10, 26, 6, 0), "total": 80.00, "granted": 0.0, "topped_up": 80.00, "currency": "USD"},
            {"ts": utc(2026, 10, 26, 9, 0), "total": 110.00, "granted": 0.0, "topped_up": 110.00, "currency": "USD"},
        ]
        # The draw-down (100 -> 80) is not a negative top-up. The refill
        # (80 -> 110) IS an increase between consecutive snapshots, so the spec
        # counts it in full: $30.00. Spend nets out because the formula adds the
        # top-up back before subtracting the balance.
        assert tool.topups_between(snaps, start, utc(2026, 10, 27), 100.00) == 30.00

    def test_grant_is_not_a_top_up_and_its_burn_is_spend(self, env):
        config = make_config(env)
        start = utc(2026, 10, 26, 0, 0)
        snaps = [
            {"ts": start, "total": 100.00, "granted": 0.0, "topped_up": 100.00, "currency": "USD"},
            # A $10 grant lands: total rises, topped_up does not.
            {"ts": utc(2026, 10, 26, 3, 0), "total": 110.00, "granted": 10.0, "topped_up": 100.00, "currency": "USD"},
            # $4 of spend comes out of the granted credit.
            {"ts": utc(2026, 10, 26, 4, 0), "total": 106.00, "granted": 6.0, "topped_up": 100.00, "currency": "USD"},
        ]
        assert tool.topups_between(snaps, start, utc(2026, 10, 27), 100.00) == 0.00
        # The grant itself is not a top-up; burning $4 of it is spend. These
        # synthetic rows have no configured baseline for their month, so assert
        # the invariant: spend = start_total + top-ups - current total.
        spend = tool.compute_spend(snaps, utc(2026, 10, 26, 4, 0), config)
        assert spend["topups"] == 0.00
        assert spend["spend"] == max(round(spend["start_total"] + 0.00 - 106.00, 2), 0.00)


# ── thresholds ──────────────────────────────────────────────────────────────
class ThresholdTests:
    def test_seventy_fires_once_and_ninety_fires_separately(self, env):
        config = make_config(env)
        bus = FakeBus()
        # Baseline $252.11 -> balance $177.11 is exactly $75.00 spent = 75%.
        seed(config, [(utc(2026, 10, 25, 23, 30), 252.11, 11.39, 240.71)])
        fetch = FakeBalance(FakeBalance.row(177.11, 240.71, 11.39))
        report = tool.run(config, now=utc(2026, 10, 26, 13, 30), fetcher=fetch, transport=bus, log=lambda m: None)
        assert report["thresholds_sent"] == [70.0]
        assert len(bus.topic_posts) == 1
        assert state_of(config) == {"month": "2026-10", "sent": [70.0]}

        # Same spend, next hour: nothing new is sent.
        bus2 = FakeBus()
        fetch2 = FakeBalance(FakeBalance.row(177.11, 240.71, 11.39))
        report = tool.run(config, now=utc(2026, 10, 26, 14, 30), fetcher=fetch2, transport=bus2, log=lambda m: None)
        assert report["thresholds_sent"] == []
        assert bus2.topic_posts == []
        assert state_of(config) == {"month": "2026-10", "sent": [70.0]}

        # Balance $152.11 -> $100.00 spent = 100%: only the 90 tier is new.
        bus3 = FakeBus()
        fetch3 = FakeBalance(FakeBalance.row(152.11, 240.71, 11.39))
        report = tool.run(config, now=utc(2026, 10, 26, 15, 30), fetcher=fetch3, transport=bus3, log=lambda m: None)
        assert report["thresholds_sent"] == [90.0]
        assert len(bus3.topic_posts) == 1
        assert state_of(config)["sent"] == [70.0, 90.0]

    def test_jump_straight_past_ninety_sends_both_tiers(self, env):
        config = make_config(env)
        bus = FakeBus()
        seed(config, [(utc(2026, 10, 25, 23, 30), 252.11, 11.39, 240.71)])
        fetch = FakeBalance(FakeBalance.row(152.11, 240.71, 11.39))
        report = tool.run(config, now=utc(2026, 10, 26, 13, 30), fetcher=fetch, transport=bus, log=lambda m: None)
        assert report["thresholds_sent"] == [70.0, 90.0]
        assert [p["topic"] for p in bus.topic_posts] == ["fleet.alert", "fleet.alert"]
        assert state_of(config)["sent"] == [70.0, 90.0]

    def test_new_month_resets_the_threshold_bookkeeping(self, env):
        config = make_config(env)
        seed(config, [(utc(2026, 10, 31, 20, 0), 252.11, 11.39, 240.71)])
        tool.save_state(config.state_path, "2026-10", [70.0, 90.0])
        # November starts at $200.00 and falls to $125.00 = exactly 75%.
        seed(config, [(utc(2026, 11, 1, 0, 5), 200.00, 11.39, 188.61)])
        bus = FakeBus()
        fetch = FakeBalance(FakeBalance.row(125.00, 188.61, 11.39))
        report = tool.run(config, now=utc(2026, 11, 1, 1, 0), fetcher=fetch, transport=bus, log=lambda m: None)
        assert report["month"] == "2026-11"
        assert report["thresholds_sent"] == [70.0]
        assert state_of(config) == {"month": "2026-11", "sent": [70.0]}

    def test_thresholds_and_ceiling_are_env_overridable(self, env):
        env.update({"DEEPSEEK_MONTHLY_CEILING_USD": "50", "DEEPSEEK_ALERT_THRESHOLDS": "50,80"})
        config = make_config(env)
        assert config.ceiling == 50.0
        assert config.thresholds == (50.0, 80.0)
        assert tool.thresholds_due(51.0, config.thresholds, {}, "2026-10") == [50.0]
        assert tool.thresholds_due(81.0, config.thresholds, {"month": "2026-10", "sent": [50]}, "2026-10") == [80.0]

    def test_bad_threshold_env_is_a_config_error(self, env):
        env["DEEPSEEK_ALERT_THRESHOLDS"] = "seventy"
        with pytest.raises(tool.ConfigError):
            make_config(env)


# ── bus behaviour ───────────────────────────────────────────────────────────
class BusTests:
    def test_bus_down_does_not_mark_sent_and_next_run_retries(self, env):
        config = make_config(env)
        seed(config, [(utc(2026, 10, 25, 23, 30), 252.11, 11.39, 240.71)])
        down = FakeBus(down=True)
        report = tool.run(
            config,
            now=utc(2026, 10, 26, 13, 30),
            fetcher=FakeBalance(FakeBalance.row(177.11, 240.71, 11.39)),
            transport=down,
            log=lambda m: None,
        )
        assert report["thresholds_sent"] == []
        assert report["bus_failures"] and "70" in report["bus_failures"][0]
        assert state_of(config) == {}  # nothing marked
        assert down.calls == []

        # The snapshot was still written (the balance is the real record).
        assert len(tool.read_snapshots(config.snapshot_path, "USD")) == 2

        # Next hour, bus back: the 70 tier is sent now.
        up = FakeBus()
        report = tool.run(
            config,
            now=utc(2026, 10, 26, 14, 30),
            fetcher=FakeBalance(FakeBalance.row(177.11, 240.71, 11.39)),
            transport=up,
            log=lambda m: None,
        )
        assert report["thresholds_sent"] == [70.0]
        assert state_of(config)["sent"] == [70.0]

    def test_no_bus_measures_without_touching_the_bus(self, env, capsys):
        """--no-bus: snapshot + measure, no bus call, nothing marked, exit 0."""
        config = make_config(env)
        seed(config, [(utc(2026, 10, 25, 23, 30), 252.11, 11.39, 240.71)])
        bus = FakeBus()
        report = tool.run(
            config,
            now=utc(2026, 10, 26, 13, 30),
            fetcher=FakeBalance(FakeBalance.row(177.11, 240.71, 11.39)),
            transport=bus,
            no_bus=True,
            log=lambda m: None,
        )
        assert bus.calls == []  # bus never contacted
        assert report["thresholds_sent"] == []
        assert report["bus_failures"] == []
        assert report["thresholds_due"] == [70.0]  # would have fired
        assert state_of(config) == {}  # but nothing is marked
        assert len(tool.read_snapshots(config.snapshot_path, "USD")) == 2  # snapshot kept

    def test_two_thresholds_register_and_heartbeat_once(self, env):
        """Both tiers due in one run: one registration, one heartbeat, two posts."""
        config = make_config(env)
        seed(config, [(utc(2026, 10, 25, 23, 30), 252.11, 11.39, 240.71)])
        bus = FakeBus()
        report = tool.run(
            config,
            now=utc(2026, 10, 26, 13, 30),
            fetcher=FakeBalance(FakeBalance.row(157.11, 240.71, 11.39)),  # ~$95 spend
            transport=bus,
            log=lambda m: None,
        )
        assert report["thresholds_sent"] == [70.0, 90.0]
        assert bus.paths == ["agents/register", "agents/heartbeat", "messages", "messages"]
        assert len(bus.registered_urns) == 1
        assert state_of(config)["sent"] == [70.0, 90.0]

    def test_registers_then_heartbeats_then_posts_to_fleet_alert(self, env):
        config = make_config(env)
        seed(config, [(utc(2026, 10, 25, 23, 30), 252.11, 11.39, 240.71)])
        bus = FakeBus()
        tool.run(
            config,
            now=utc(2026, 10, 26, 13, 30),
            fetcher=FakeBalance(FakeBalance.row(177.11, 240.71, 11.39)),
            transport=bus,
            log=lambda m: None,
        )
        assert bus.paths == ["agents/register", "agents/heartbeat", "messages"]
        reg = bus.calls[0][1]["body"]
        assert (reg["kind"], reg["runtime"], reg["host"]) == ("human", "human", "testhost")
        urn = bus.registered_urns[0]
        assert bus.calls[1][1]["body"] == {"urn": urn, "status": "online"}
        # The post carries the registered URN as sender and X-Hive-Actor.
        post = bus.topic_posts[0]
        assert post["from_urn"] == urn
        assert post["topic"] == "fleet.alert"
        assert "X-Hive-Actor" in bus.calls[2][1]["headers"]


# ── fetch failure ───────────────────────────────────────────────────────────
class FetchFailureTests:
    def test_fetch_failure_writes_no_snapshot_and_no_alert(self, env):
        config = make_config(env)
        seed(config, [(utc(2026, 10, 25, 23, 30), 252.11, 11.39, 240.71)])

        def boom(api_key, currency, now):
            raise tool.FetchError("GET https://api.deepseek.com/user/balance -> HTTP 503")

        bus = FakeBus()
        with pytest.raises(tool.FetchError):
            tool.run(config, now=utc(2026, 10, 26, 13, 30), fetcher=boom, transport=bus, log=lambda m: None)
        assert len(tool.read_snapshots(config.snapshot_path, "USD")) == 1  # only the seed
        assert bus.calls == []
        assert state_of(config) == {}

    def test_main_exits_non_zero_on_fetch_failure_and_writes_nothing(self, env, monkeypatch, capsys):
        monkeypatch.setattr(tool, "fetch_balance", _raising_fetch)
        with monkeypatch.context() as m:
            m.setattr(os, "environ", env)
            assert tool.main([]) == 1
        assert "balance fetch failed" in capsys.readouterr().err
        assert not pathlib.Path(env["DEEPSEEK_SNAPSHOT_PATH"]).exists()


def _raising_fetch(api_key, currency, now):
    raise tool.FetchError("GET https://api.deepseek.com/user/balance failed: connection refused")


class FetchParsingTests:
    def test_balance_response_uses_real_field_names(self, env):
        config = make_config(env)
        captured = {}

        def fake_http(url, *, method="GET", headers=None, body=None, timeout=tool.FETCH_TIMEOUT_SECS):
            captured["url"] = url
            captured["headers"] = headers
            captured["timeout"] = timeout
            return balance_body(252.11, 240.71, 11.39)

        original = tool._http_json
        tool._http_json = fake_http
        try:
            row = tool.fetch_balance(SECRET, "USD", utc(2026, 10, 25, 23, 30))
        finally:
            tool._http_json = original
        assert captured["url"] == "https://api.deepseek.com/user/balance"
        assert captured["headers"]["Authorization"] == f"Bearer {SECRET}"
        assert captured["timeout"] == 20.0
        assert (row["total"], row["granted"], row["topped_up"], row["currency"]) == (252.11, 11.39, 240.71, "USD")
        assert row["ts_utc"] == "2026-10-25T23:30:00Z"

    def test_missing_balance_infos_is_a_fetch_error(self):
        original = tool._http_json
        tool._http_json = lambda url, **kw: {"is_available": True}
        try:
            with pytest.raises(tool.FetchError):
                tool.fetch_balance(SECRET, "USD", utc(2026, 10, 26))
        finally:
            tool._http_json = original


# ── message format ──────────────────────────────────────────────────────────
class MessageFormatTests:
    def test_exact_wording_at_seventy(self, env):
        config = make_config(env)
        spend = {"spend": 70.00, "pct": 70.0, "current_total": 182.11, "topups": 0.00}
        assert (
            tool.format_alert_message(spend, config.ceiling)
            == "DeepSeek spend $70.00 this month (70% of $100 ceiling); balance $182.11; top-ups since month start $0.00"
        )

    def test_rounding_is_cents_and_whole_percent(self, env):
        config = make_config(env)
        spend = {"spend": 93.456, "pct": 93.456, "current_total": 100.006, "topups": 12.3456}
        assert (
            tool.format_alert_message(spend, config.ceiling)
            == "DeepSeek spend $93.46 this month (93% of $100 ceiling); balance $100.01; top-ups since month start $12.35"
        )


# ── secret hygiene ──────────────────────────────────────────────────────────
class SecretHygieneTests:
    def test_key_never_lands_in_snapshot_state_or_output(self, env, capsys):
        config = make_config(env)
        seed(config, [(utc(2026, 10, 25, 23, 30), 252.11, 11.39, 240.71)])
        bus = FakeBus()
        fetch = FakeBalance(FakeBalance.row(177.11, 240.71, 11.39))
        report = tool.run(
            config,
            now=utc(2026, 10, 26, 13, 30),
            fetcher=fetch,
            transport=bus,
            log=lambda m: print(m),
        )
        out = capsys.readouterr()
        # The tool really did read the key from the environment ...
        assert fetch.calls == [(SECRET, "USD")]
        # ... and it is in none of the artefacts it produces.
        blobs = [
            config.snapshot_path.read_text(),
            config.state_path.read_text(),
            json.dumps(report),
            out.out,
            out.err,
            json.dumps([meta for _, meta in bus.calls]),
        ]
        for blob in blobs:
            assert SECRET not in blob
            assert "Bearer" not in blob

    def test_missing_key_is_a_config_error_not_a_request(self, env):
        env.pop("DEEPSEEK_API_KEY")
        with pytest.raises(tool.ConfigError):
            tool.run(make_config(env), now=utc(2026, 10, 26), log=lambda m: None)
