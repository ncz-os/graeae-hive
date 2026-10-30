"""scripts/offload_queue.py: the planner and submit helper for a single-seat, hour-metered agent.

Pure planning is tested against fake files (dicts), submit/release against a
recording fake bus, and submit/release once more against the REAL bus app
(in-process ASGI, temp sqlite) the way test_agent_lifecycle.py drives it. No
network.
"""

from __future__ import annotations

import asyncio
import io
import json
import os
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import pytest

os.environ.setdefault("HIVE_BACKEND", "sqlite")

from scripts import offload_queue as oq  # noqa: E402

NOW = datetime(2026, 10, 30, 12, 0, tzinfo=UTC).timestamp()
RESET = "2026-10-30T20:00:00Z"  # 8h after NOW
REPO = "org/approved"
CHECK = "pytest -q tests/test_x.py"
GATES = "no-assertion-weakening, no-new-skips, suite-before-after"

MODELS = {
    "models": [
        {"id": "m1", "displayName": "One", "available": True, "costMultiplier": 1},
        {"id": "m2", "displayName": "Two", "available": True, "costMultiplier": 2},
        {"id": "m3", "displayName": "Three", "available": True, "costMultiplier": 3},
        {"id": "m3-alt", "displayName": "Three alt", "available": True, "costMultiplier": 3},
        {"id": "m3-off", "displayName": "Three off", "available": False, "costMultiplier": 3},
        {"id": "m4", "displayName": "Four", "available": True, "costMultiplier": 4},
        {"id": "m5", "displayName": "Five", "available": True, "costMultiplier": 5},
        {"id": "m-null", "displayName": "No price", "available": True, "costMultiplier": None},
    ]
}


def cfg(**over) -> oq.Config:
    raw = {"allowedRepos": [REPO], **over}
    return oq.config_from_dict(raw)


def models() -> oq.Models:
    return oq.models_from_dict(MODELS)


def balance(daily=20.0, wallet=0.0, reset=RESET, updated_ago_min=1.0, session_model=None, state=None) -> oq.Balance:
    return oq.balance_from_dict(
        {
            "balance": {
                "dailyRemainingHours": daily,
                "walletHours": wallet,
                "spendableHours": None if daily is None else daily + (wallet or 0),
                "dailyResetAt": reset,
                "updatedAt": None if updated_ago_min is None else NOW - updated_ago_min * 60,
            },
            "sessionModel": session_model,
            "sessionMultiplier": None,
            "sessionStartedAt": None if session_model is None else NOW - 600,
            "state": state,
        }
    )


def brief(branch="offload/x", check=CHECK, gates=GATES, extra=""):
    lines = ["Do the thing.", extra]
    if check:
        lines.append(f"check: {check}")
    if branch:
        lines.append(f"branch: {branch}")
    if gates:
        lines.append(f"gates: {gates}")
    return "\n".join(x for x in lines if x)


def task(**over):
    base = {"title": "Add tests", "repo": REPO, "class": "test-write", "brief": brief()}
    base.update(over)
    return base


def mkjob(
    jid, model="m3", cls="test-write", minutes=None, prio=0, queued=0, kind="freebuff", status="queued", ab=False
):
    meta = {
        "title": jid,
        "class": cls,
        "model": model,
        "estimate-minutes": minutes,
        "ab_test": "true" if ab else None,
    }
    return {
        "id": jid,
        "kind": kind,
        "status": status,
        "priority": prio,
        "started_at": NOW - 3600 + queued,
        "preferred_models": [model],
        "description": oq.build_description(meta, "brief"),
    }


def plan(jobs, balance_=None, cfg_=None, evergreen=None, now=NOW):
    return oq.build_plan(
        cfg_ or cfg(), models(), balance_ or balance(), jobs, now=now, evergreen=evergreen or [], evergreen_blocked=0
    )


def verdict_of(p):
    return p["verdict"]


# --------------------------------------------------------------- the fake bus


class FakeBus:
    """Records every call; answers like the bus for the routes the planner uses."""

    def __init__(self, jobs=None, honours_priority=True):
        self.jobs = {j["id"]: dict(j) for j in (jobs or [])}
        self.calls: list[tuple[str, str, dict | None]] = []
        self.honours_priority = honours_priority
        self.n = 0

    def __call__(self, method, path, body=None):
        self.calls.append((method, path, body))
        if method == "GET" and path.startswith("/v1/jobs?"):
            status = path.split("status=")[1].split("&")[0]
            rows = [j for j in self.jobs.values() if j["status"] == status]
            return 200, {"count": len(rows), "total": len(rows), "jobs": rows}
        if method == "GET" and path.startswith("/v1/jobs/"):
            job = self.jobs.get(path.rsplit("/", 1)[1])
            return (200, dict(job)) if job else (404, {"detail": "job not found"})
        if method == "POST" and path == "/v1/jobs":
            for j in self.jobs.values():
                if j.get("idempotency_key") == body.get("idempotency_key") and j["status"] == "queued":
                    return 200, {"id": j["id"], "coalesced": True}
            self.n += 1
            jid = f"job-{self.n}"
            self.jobs[jid] = {**body, "id": jid, "status": "queued", "started_at": NOW}
            return 200, {"id": jid, "warnings": []}
        if method == "PATCH":
            job = self.jobs[path.rsplit("/", 1)[1]]
            if self.honours_priority and "priority" in body:
                job["priority"] = body["priority"]
            return 200, dict(job)
        return 500, None

    def writes(self):
        return [c for c in self.calls if c[0] in ("POST", "PATCH")]


def ctx(bus=None, cfg_=None, balance_=None):
    return oq.Context(
        cfg=cfg_ or cfg(),
        models=models(),
        balance=balance_ or balance(),
        transport=bus,
        now=NOW,
        actor="urn:test:actor",
    )


# ------------------------------------------------------------------ submit


def test_empty_allowed_repos_refuses_every_submit():
    for c in (oq.Config(), oq.config_from_dict({}), oq.config_from_dict({"allowedRepos": []})):
        res = oq.validate_task(c, models(), task())
        assert not res["ok"]
        assert oq.REASON_REPO in res["reasons"]
    # and the full submit path refuses without touching the bus
    bus = FakeBus()
    out = oq.submit_task(ctx(bus, cfg_=oq.Config()), task(), dry_run=False)
    assert out["ok"] is False and "repo not approved for offload" in out["refused"]
    assert bus.writes() == []


def test_unapproved_repo_is_refused_and_approved_is_accepted():
    assert oq.REASON_REPO in oq.validate_task(cfg(), models(), task(repo="org/other"))["reasons"]
    assert oq.validate_task(cfg(), models(), task())["ok"]


def test_brief_size_limit_is_48_kib_exclusive():
    pad = "x" * (48 * 1024)
    big = brief(extra=pad)
    res = oq.validate_task(cfg(), models(), task(brief=big))
    assert not res["ok"] and any("48 KiB" in r for r in res["reasons"])
    # a brief one byte under the limit passes
    head = brief()
    fill = "y" * (48 * 1024 - 1 - len(head.encode()) - 1)
    ok_brief = head + "\n" + fill
    assert len(ok_brief.encode()) == 48 * 1024 - 1
    assert oq.validate_task(cfg(), models(), task(brief=ok_brief))["ok"]
    assert not oq.validate_task(cfg(), models(), task(brief=ok_brief + "z"))["ok"]


def test_brief_needs_a_check_line_and_a_branch_name():
    no_check = oq.validate_task(cfg(), models(), task(brief=brief(check=None)))
    assert any("no check: line" in r for r in no_check["reasons"])
    no_branch = oq.validate_task(cfg(), models(), task(brief=brief(branch=None)))
    assert any("no branch: line" in r for r in no_branch["reasons"])
    bad = oq.validate_task(cfg(), models(), task(brief=brief(branch="has space..bad")))
    assert "branch name is not valid" in bad["reasons"]
    # an empty value is not a check
    assert not oq.validate_task(cfg(), models(), task(brief=brief().replace(f"check: {CHECK}", "check:")))["ok"]
    two = brief() + "\ncheck: a different command"
    assert any(
        "more than one distinct check" in r for r in oq.validate_task(cfg(), models(), task(brief=two))["reasons"]
    )


def test_refused_classes_each_carry_the_reason():
    for cls in oq.REFUSED_CLASSES:
        res = oq.validate_task(cfg(), models(), task(**{"class": cls}))
        assert not res["ok"]
        assert "class not suitable for the offload seat" in res["reasons"], cls
    for cls in oq.ITERATION_CLASSES:
        res = oq.validate_task(cfg(), models(), task(**{"class": cls}))
        assert oq.REASON_CLASS not in res["reasons"], cls
    unknown = oq.validate_task(cfg(), models(), task(**{"class": "vibes"}))
    assert not unknown["ok"] and "task class missing or unknown" in unknown["reasons"]


def test_gates_are_required_for_gated_classes_and_validated():
    for cls in oq.GATED_CLASSES:
        res = oq.validate_task(cfg(), models(), task(**{"class": cls, "brief": brief(gates=None)}))
        assert any("needs a gates: line" in r for r in res["reasons"]), cls
    bad = oq.validate_task(cfg(), models(), task(brief=brief(gates="no-assertion-weakening, trust-me")))
    assert "unknown gate(s) in gates: line" in bad["reasons"]
    for cls in set(oq.ITERATION_CLASSES) - set(oq.GATED_CLASSES):
        assert oq.validate_task(cfg(), models(), task(**{"class": cls, "brief": brief(gates=None)}))["ok"], cls
    for gate in oq.GATES:
        assert oq.validate_task(cfg(), models(), task(brief=brief(gates=gate)))["ok"], gate


def test_declared_gates_travel_in_the_job_for_the_verifier():
    out = oq.submit_task(ctx(FakeBus()), task(), dry_run=True)
    assert out["ok"]
    head = oq.parse_header(out["payload"]["description"])
    assert head["gates"] == "no-assertion-weakening,no-new-skips,suite-before-after"
    assert head["check"] == CHECK and head["branch"] == "offload/x" and head["class"] == "test-write"
    assert out["payload"]["head_branch"] == "offload/x"


def test_model_must_be_in_snapshot_available_and_in_band():
    assert "model not in the models snapshot" in oq.validate_task(cfg(), models(), task(model="nope"))["reasons"]
    assert "model not available" in oq.validate_task(cfg(), models(), task(model="m3-off"))["reasons"]
    assert "model multiplier unknown" in oq.validate_task(cfg(), models(), task(model="m-null"))["reasons"]
    for good in ("m2", "m3", "m4"):
        res = oq.validate_task(cfg(), models(), task(model=good))
        assert res["ok"], (good, res)


BAND = "model outside the approved multiplier band (2x floor, 4x cap)"


def test_cap_refuses_a_model_above_max_multiplier():
    res = oq.validate_task(cfg(), models(), task(model="m5"))
    assert not res["ok"] and BAND in res["reasons"]
    # a lowered cap moves the line
    res = oq.validate_task(cfg(maxMultiplier=3, defaultMultiplier=3), models(), task(model="m4"))
    assert "model outside the approved multiplier band (2x floor, 3x cap)" in res["reasons"]


def test_floor_refuses_a_model_below_min_multiplier():
    res = oq.validate_task(cfg(), models(), task(model="m1"))
    assert not res["ok"] and BAND in res["reasons"]
    res = oq.validate_task(cfg(minMultiplier=3), models(), task(model="m2"))
    assert "model outside the approved multiplier band (3x floor, 4x cap)" in res["reasons"]


def test_no_model_gets_the_default_tier():
    res = oq.validate_task(cfg(), models(), task())
    assert res["ok"] and res["model"] == "m3" and res["multiplier"] == 3  # lowest available id at 3x
    res = oq.validate_task(cfg(defaultMultiplier=2), models(), task())
    assert res["model"] == "m2"
    payload = oq.submit_task(ctx(FakeBus()), task(), dry_run=True)["payload"]
    assert payload["preferred_models"] == ["m3"]
    # nothing available at the default tier: refuse, never guess
    res = oq.validate_task(
        cfg(minMultiplier=2, maxMultiplier=4, defaultMultiplier=4), oq.models_from_dict({"models": []}), task()
    )
    assert "no available model at the default multiplier" in res["reasons"]


def test_one_x_needs_allow_ab_test_and_the_job_label():
    # label without the config switch, config switch without the label: both refused
    assert BAND in oq.validate_task(cfg(), models(), task(model="m1", ab_test=True))["reasons"]
    assert BAND in oq.validate_task(cfg(allowAbTest=True), models(), task(model="m1"))["reasons"]
    res = oq.validate_task(cfg(allowAbTest=True), models(), task(model="m1", ab_test=True))
    assert res["ok"] and res["ab_test"] is True
    # the label is carried in the job
    payload = oq.submit_task(ctx(FakeBus(), cfg_=cfg(allowAbTest=True)), task(model="m1", ab_test=True), dry_run=True)[
        "payload"
    ]
    assert oq.parse_header(payload["description"])["ab_test"] == "true"
    # the A/B switch does not open anything else below the floor
    res = oq.validate_task(cfg(allowAbTest=True, minMultiplier=3), models(), task(model="m2", ab_test=True))
    assert "model outside the approved multiplier band (3x floor, 4x cap)" in res["reasons"]


def test_above_cap_needs_operator_approval():
    assert BAND in oq.validate_task(cfg(), models(), task(model="m5"))["reasons"]
    assert BAND in oq.validate_task(cfg(approvedAboveCap=["m4"]), models(), task(model="m5"))["reasons"]
    assert oq.validate_task(cfg(approvedAboveCap=["m5"]), models(), task(model="m5"))["ok"]
    # approval does not reach an unavailable model
    c = cfg(approvedAboveCap=["m5"])
    assert not oq.validate_task(
        c, oq.models_from_dict({"models": [{"id": "m5", "available": False, "costMultiplier": 5}]}), task(model="m5")
    )["ok"]


def test_depends_on_must_exist():
    bus = FakeBus([mkjob("dep-1")])
    ok = oq.submit_task(ctx(bus), task(depends_on=["dep-1"]), dry_run=True)
    assert ok["ok"] and ok["payload"]["depends_on"] == ["dep-1"]
    bad = oq.submit_task(ctx(bus), task(depends_on=["dep-1", "ghost"]), dry_run=True)
    assert not bad["ok"] and "depends_on job does not exist" in bad["refused"]
    assert "ghost" in " ".join(bad["details"])


def test_submit_sets_routing_priority_and_a_stable_key():
    bus = FakeBus()
    first = oq.submit_task(ctx(bus), task(), dry_run=False)
    assert first["ok"] and not first["coalesced"]
    job = bus.jobs[first["id"]]
    assert job["eligible_kinds"] == ["freebuff"] and job["preferred_models"] == ["m3"]
    assert job["kind"] == "freebuff" and job["repo"] == REPO and job["head_branch"] == "offload/x"
    assert job["priority"] == 90  # the only block is the first block: top band
    again = oq.submit_task(ctx(bus), task(), dry_run=False)
    assert again["id"] == first["id"] and again["coalesced"] is True
    assert len([j for j in bus.jobs.values()]) == 1
    # a different task gets a different key
    other = oq.submit_task(ctx(bus), task(title="Other", brief=brief(branch="offload/y")), dry_run=False)
    assert other["id"] != first["id"]


def test_second_model_lands_in_a_lower_band_than_the_first_block():
    bus = FakeBus()
    oq.submit_task(ctx(bus), task(model="m3"), dry_run=False)
    res = oq.submit_task(ctx(bus), task(title="B", model="m2", brief=brief(branch="offload/b")), dry_run=False)
    assert bus.jobs[res["id"]]["priority"] == 80


def test_dry_run_prints_the_job_and_creates_nothing():
    bus = FakeBus()
    out = oq.submit_task(ctx(bus), task(), dry_run=True)
    assert out["ok"] and out["payload"]["submitter_urn"].startswith("<from")  # no identity in the output
    assert bus.writes() == []


def test_kind_name_is_configuration():
    assert oq.Config().kind == "freebuff"
    payload = oq.submit_task(ctx(FakeBus(), cfg_=cfg(kind="other-seat")), task(), dry_run=True)["payload"]
    assert payload["kind"] == "other-seat" and payload["eligible_kinds"] == ["other-seat"]


# -------------------------------------------------------------------- plan


def test_blocks_group_by_model_so_the_plan_switches_once_not_three_times():
    # claim order is A B A B: the naive run switches model three times
    jobs = [
        mkjob("a1", "m3", queued=1),
        mkjob("b1", "m2", queued=2),
        mkjob("a2", "m3", queued=3),
        mkjob("b2", "m2", queued=4),
    ]
    p = plan(jobs)
    assert p["switches"] == {"naive": 3, "planned": 1}
    assert [b["model"] for b in p["blocks"]] == ["m3", "m2"]
    assert [sorted(b["taskIds"]) for b in p["blocks"]] == [["a1", "a2"], ["b1", "b2"]]


def test_open_session_model_goes_first_and_costs_no_switch():
    jobs = [mkjob("b1", "m2", queued=1), mkjob("a1", "m3", queued=2)]
    p = plan(jobs, balance(session_model="m3", state="active"))
    assert p["blocks"][0]["model"] == "m3" and p["blocks"][0]["continuesOpenSession"]
    assert p["switches"]["planned"] == 1  # m3 (open) -> m2
    p = plan(jobs)  # no open session: the older job's model first
    assert p["blocks"][0]["model"] == "m2"


def test_hour_arithmetic_for_a_3x_and_a_1x_block():
    # 3 tasks x 30 min = 90 min -> 2 whole hours -> 6h at 3x; one 20 min 1x task -> 1h minimum -> 1h
    jobs = [mkjob(f"t{i}", "m3", minutes=30, queued=i) for i in range(3)] + [
        mkjob("ab", "m1", minutes=20, queued=10, ab=True)
    ]
    p = plan(jobs, cfg_=cfg(allowAbTest=True))
    by = {b["model"]: b for b in p["blocks"]}
    assert by["m3"]["minutes"] == 90 and by["m3"]["estimatedSessionHours"] == 2 and by["m3"]["estimatedCostHours"] == 6
    assert by["m1"]["estimatedSessionHours"] == 1 and by["m1"]["estimatedCostHours"] == 1
    assert p["pool"]["plannedHours"] == 7
    assert p["pool"]["unusedHoursAtReset"] == 13  # 20 remaining - 7


def test_rounding_up_to_whole_session_hours():
    for minutes, hours in ((60, 1), (61, 2), (119, 2), (120, 2), (121, 3)):
        p = plan([mkjob("t", "m2", minutes=minutes)])
        assert p["blocks"][0]["estimatedSessionHours"] == hours, minutes
        assert p["blocks"][0]["estimatedCostHours"] == hours * 2


def test_session_minimum_applies_per_session():
    p = plan([mkjob("t", "m3", minutes=10)], cfg_=cfg(sessionMinimumHours=2))
    assert p["blocks"][0]["estimatedSessionHours"] == 2 and p["blocks"][0]["estimatedCostHours"] == 6
    p = plan([mkjob("t", "m3", minutes=10)])
    assert p["blocks"][0]["estimatedCostHours"] == 3  # one-hour minimum x 3


def test_minutes_per_task_default_by_class_and_config_override():
    p = plan([mkjob("t", "m2", cls="soak-loop")])  # default 90 min -> 2h
    assert p["blocks"][0]["minutes"] == 90
    p = plan([mkjob("t", "m2", cls="soak-loop")], cfg_=cfg(minutesPerTask={"soak-loop": 200}))
    assert p["blocks"][0]["minutes"] == 200 and p["blocks"][0]["estimatedSessionHours"] == 4
    assert plan([mkjob("t", "m2", cls="soak-loop", minutes=15)])["blocks"][0]["minutes"] == 15  # the job's own estimate


def test_verdict_under_used_suggests_evergreen_items_and_says_how_many():
    jobs = [mkjob("t", "m3", minutes=30)]
    evergreen = [
        oq.Item(f"evergreen:{n}", f"E{n}", "doc-from-code", "m3", 3.0, 30.0, 0, NOW + n, n, virtual=True)
        for n in range(8)
    ]
    p = plan(jobs, balance(daily=10), evergreen=evergreen)
    # 10h at 3x = 3 whole session-hours = 180 min = 6 tasks of 30 min; one is already queued
    assert p["verdict"] == "UNDER-USED"
    assert p["suggestion"]["pull"] == 5
    assert p["pool"]["unusedHoursAtReset"] == 7
    assert "pull 5 item(s)" in oq.render_plan(p)


def test_under_used_with_nothing_queued_says_no_useful_work_and_invents_none():
    p = plan([], balance(daily=10))
    assert p["verdict"] == "UNDER-USED"
    assert p["note"] == "no useful work queued"
    assert p["suggestion"]["pull"] == 0 and "do not invent work" in p["suggestion"]["message"]
    text = oq.render_plan(p)
    assert "no useful work queued" in text and p["blocks"] == []


def test_verdict_balanced():
    p = plan([mkjob("a", "m3", minutes=60), mkjob("b", "m3", minutes=60, queued=1)], balance(daily=7))
    assert p["pool"]["plannedHours"] == 6 and p["pool"]["unusedHoursAtReset"] == 1
    assert p["verdict"] == "BALANCED" and p["suggestion"] is None


def test_verdict_over_lists_what_does_not_fit_and_defers_it():
    jobs = [mkjob(f"t{i}", "m3", minutes=30, queued=i) for i in range(6)]  # 180 min -> 9h at 3x
    p = plan(jobs, balance(daily=7))
    assert p["verdict"] == "OVER"
    assert p["pool"]["plannedHours"] == 6  # 2 session-hours at 3x
    assert [d["id"] for d in p["deferred"]] == ["t4", "t5"]
    assert all(d["reason"] == oq.REASON_FIT for d in p["deferred"])
    assert p["bands"]["t4"] == p["bands"]["t5"] == 0 and p["bands"]["t0"] == 90
    text = oq.render_plan(p)
    assert "deferred (will not fit)" in text and "t5" in text


def test_verdict_cannot_start_when_the_hold_exceeds_the_pool():
    p = plan([mkjob("t", "m3")], balance(daily=2.5))
    assert p["verdict"] == "CANNOT-START"
    assert "start-of-session hold exceeds the remaining pool" in p["reasons"]
    assert p["blocks"][0]["state"] == "not-started" and p["pool"]["plannedHours"] == 0
    # exactly the hold is enough
    assert plan([mkjob("t", "m3")], balance(daily=3))["verdict"] != "CANNOT-START"


def test_cannot_start_when_the_session_minimum_exceeds_the_pool():
    p = plan([mkjob("t", "m3")], balance(daily=5), cfg_=cfg(sessionMinimumHours=2))  # hold 3 <= 5 < minimum 6
    assert p["verdict"] == "CANNOT-START" and p["reasons"] == [oq.REASON_MINIMUM]


def test_last_session_rule_first_task_must_finish_before_reset_minus_margin():
    reset = "2026-10-30T12:40:00Z"  # 40 min away; margin 30 leaves 10 usable minutes
    p = plan([mkjob("t", "m2", minutes=30)], balance(daily=10, reset=reset))
    assert p["verdict"] == "CANNOT-START" and p["reasons"] == [oq.REASON_TIME]
    p = plan([mkjob("t", "m2", minutes=30)], balance(daily=10, reset=reset), cfg_=cfg(resetMarginMinutes=0))
    assert p["verdict"] != "CANNOT-START"  # 40 usable minutes now
    # a later task that would run past the cutoff is deferred, the earlier one is kept
    jobs = [mkjob("a", "m2", minutes=20, queued=1), mkjob("b", "m2", minutes=20, queued=2)]
    p = plan(jobs, balance(daily=10, reset="2026-10-30T13:00:00Z"))  # 60 min - 30 margin = 30 usable
    assert [b["taskIds"] for b in p["blocks"]] == [["a"]] and [d["id"] for d in p["deferred"]] == ["b"]
    assert p["verdict"] == "OVER"


def test_the_wallet_is_never_planned():
    wallet = 500.0
    cases = [
        plan([mkjob("t", "m3")], balance(daily=1, wallet=wallet)),  # cannot start on the pool alone
        plan([mkjob(f"t{i}", "m3", minutes=60, queued=i) for i in range(6)], balance(daily=7, wallet=wallet)),  # over
        plan([], balance(daily=10, wallet=wallet)),  # under-used
    ]
    for p in cases:
        assert p["pool"]["plannedHours"] <= p["pool"]["dailyRemainingHours"], p
        assert p["walletPolicy"] == "reserve"
        blob = json.dumps(p) + oq.render_plan(p)
        assert "spend the wallet" not in blob and "use the wallet" not in blob
        assert "(reserved, not planned)" in oq.render_plan(p)
    assert cases[0]["verdict"] == "CANNOT-START" and cases[1]["verdict"] == "OVER"
    # the same plan with and without a wallet
    a = plan([mkjob("t", "m3")], balance(daily=10, wallet=0))
    b = plan([mkjob("t", "m3")], balance(daily=10, wallet=wallet))
    assert a["pool"]["plannedHours"] == b["pool"]["plannedHours"] and a["verdict"] == b["verdict"]
    # no other wallet policy is accepted
    with pytest.raises(oq.ConfigError):
        oq.config_from_dict({"walletPolicy": "spend"})


def test_null_and_unknown_fields_are_reported_not_computed():
    nulls = oq.balance_from_dict(
        {
            "balance": {
                "dailyRemainingHours": None,
                "walletHours": None,
                "spendableHours": None,
                "dailyResetAt": None,
                "updatedAt": None,
            },
            "sessionModel": None,
            "sessionMultiplier": None,
            "sessionStartedAt": None,
            "state": None,
        }
    )
    p = plan([mkjob("t", "m3")], nulls)
    assert p["verdict"] == "UNKNOWN"
    cannot = " ".join(p["cannotCompute"])
    assert "dailyRemainingHours is null" in cannot and "dailyResetAt is unknown" in cannot
    assert p["pool"]["unusedHoursAtReset"] is None and p["blocks"][0]["estimatedCostHours"] == 3
    assert any("updatedAt is null" in w for w in p["warnings"])
    assert "cannot compute:" in oq.render_plan(p)
    # an unreadable balance file and an unreadable models file
    gone = oq.load_balance("/nonexistent/balance.json")
    assert gone.problem and plan([mkjob("t", "m3")], gone)["verdict"] == "UNKNOWN"
    nomodels = oq.load_models("/nonexistent/models.json")
    p = oq.build_plan(cfg(), nomodels, balance(), [mkjob("t", "m3")], now=NOW)
    assert p["blocks"] == [] and p["excluded"] and any("model multipliers" in c for c in p["cannotCompute"])
    assert p["note"] == "no useful work queued"
    # a stale reset: the snapshot predates it
    p = plan([mkjob("t", "m3")], balance(reset="2026-10-30T09:00:00Z"))
    assert p["verdict"] == "UNKNOWN" and any("in the past" in w for w in p["warnings"])


def test_snapshot_older_than_fifteen_minutes_warns():
    fresh = plan([], balance(updated_ago_min=14))
    stale = plan([], balance(updated_ago_min=16))
    assert not any("stale" in w for w in fresh["warnings"])
    assert any("balance snapshot is stale: 16 min old" in w for w in stale["warnings"])
    assert any(
        "stale" in w for w in plan([], balance(updated_ago_min=40), cfg_=cfg(staleSnapshotMinutes=30))["warnings"]
    )


def test_orchestration_only_seat_warns_about_build_heavy_classes():
    heavy = [
        mkjob("s", "m3", cls="soak-loop"),
        mkjob("f", "m3", cls="flake-hunt"),
        mkjob("m", "m3", cls="mechanical-migration"),
    ]
    p = plan(heavy, cfg_=cfg(where="orchestration-only host class"))
    warn = [w for w in p["warnings"] if "orchestration-only" in w]
    assert len(warn) == 1
    for cls in ("soak-loop", "flake-hunt", "mechanical-migration"):
        assert cls in warn[0]
    # light classes: no warning; build host: no warning
    assert not [
        w
        for w in plan([mkjob("t", "m3")], cfg_=cfg(where="orchestration-only"))["warnings"]
        if "orchestration-only" in w
    ]
    assert not [w for w in plan(heavy, cfg_=cfg(where="build host class"))["warnings"] if "orchestration-only" in w]
    assert "warning: seat is marked orchestration-only" in oq.render_plan(p)


def test_queued_jobs_outside_the_policy_are_not_planned():
    jobs = [mkjob("ok", "m3"), mkjob("hot", "m5"), mkjob("cheap", "m1"), mkjob("design", "m3", cls="design")]
    p = plan(jobs)
    reasons = {e["id"]: e["reason"] for e in p["excluded"]}
    assert reasons == {"hot": BAND, "cheap": BAND, "design": oq.REASON_CLASS}
    assert [t for b in p["blocks"] for t in b["taskIds"]] == ["ok"]


def test_json_plan_round_trips():
    p = plan([mkjob("t", "m3")])
    assert json.loads(json.dumps(p)) == p


# ----------------------------------------------------------------- release


def _release_jobs():
    return [
        mkjob("a1", "m3", prio=5, queued=1),
        mkjob("a2", "m3", prio=5, queued=2),
        mkjob("b1", "m2", prio=0, queued=3),
        mkjob("other", "m3", kind="zeroclaw", prio=7, queued=0),
    ]


def test_release_assigns_bands_and_only_touches_the_kind():
    bus = FakeBus(_release_jobs())
    res = oq.release(ctx(bus), dry_run=False)
    assert res["ok"] and res["activeBlock"] == "m3"
    assert {j: bus.jobs[j]["priority"] for j in ("a1", "a2", "b1")} == {"a1": 90, "a2": 90, "b1": 80}
    patched = [c[1] for c in bus.writes()]
    assert sorted(patched) == ["/v1/jobs/a1", "/v1/jobs/a2", "/v1/jobs/b1"]
    assert bus.jobs["other"]["priority"] == 7  # another kind: never touched
    assert all(not c[1].endswith("/other") for c in bus.calls if c[0] == "PATCH")
    # idempotent: a second release changes nothing
    bus.calls.clear()
    again = oq.release(ctx(bus), dry_run=False)
    assert bus.writes() == [] and {c["action"] for c in again["changes"]} == {"unchanged"}


def test_release_dry_run_writes_nothing():
    bus = FakeBus(_release_jobs())
    res = oq.release(ctx(bus), dry_run=True)
    assert bus.writes() == []
    assert {c["action"] for c in res["changes"]} == {"would-patch"}
    assert bus.jobs["a1"]["priority"] == 5


def test_release_refuses_a_job_id_of_another_kind():
    bus = FakeBus(_release_jobs())
    res = oq.release(ctx(bus), dry_run=False, only_ids=["other", "a1"])
    assert [r["id"] for r in res["refused"]] == ["other"]
    assert [c[1] for c in bus.writes()] == ["/v1/jobs/a1"]


def test_release_has_nothing_to_release_without_an_active_block():
    bus = FakeBus([mkjob("t", "m3")])
    res = oq.release(ctx(bus, balance_=balance(daily=1)), dry_run=False)
    assert res["ok"] is False and "no active block" in res["error"] and bus.writes() == []


def test_release_reports_a_bus_that_does_not_apply_priority():
    bus = FakeBus(_release_jobs(), honours_priority=False)
    res = oq.release(ctx(bus), dry_run=False)
    assert res["ok"] is False and "did not change priority" in res["error"]
    assert {c["action"] for c in res["changes"]} == {"not-applied"}


# ---------------------------------------------------------------- evergreen


def _backlog(tmp_path: Path):
    items = [
        {
            "title": "Approved one",
            "repo": REPO,
            "class": "doc-from-code",
            "brief": "Write docs.",
            "check": CHECK,
            "estimateMinutes": 30,
        },
        {"title": "Elsewhere", "repo": "org/not-approved", "class": "doc-from-code", "brief": "x", "check": CHECK},
        {"title": "Design thing", "repo": REPO, "class": "design", "brief": "x", "check": CHECK},
        {
            "title": "Tests",
            "repo": REPO,
            "class": "test-write",
            "brief": "Write tests.",
            "check": CHECK,
            "gates": ["no-assertion-weakening", "red-on-base"],
        },
        {"title": "Tests no gates", "repo": REPO, "class": "test-write", "brief": "x", "check": CHECK},
        {
            "title": "Approved two",
            "repo": REPO,
            "class": "dead-code",
            "brief": "Remove.",
            "check": CHECK,
            "gates": "suite-before-after",
        },
    ]
    path = tmp_path / "evergreen.json"
    path.write_text(json.dumps({"items": items}))
    return path


def test_evergreen_pull_never_submits_for_repos_not_in_allowed_repos(tmp_path):
    c = cfg()
    c.evergreen_file = str(_backlog(tmp_path))
    bus = FakeBus()
    res = oq.evergreen_pull(ctx(bus, cfg_=c), 10, dry_run=False)
    titles = [s["title"] for s in res["submitted"]]
    assert titles == ["Approved one", "Tests", "Approved two"]
    skipped = {s["title"]: s["reasons"] for s in res["skipped"]}
    assert skipped["Elsewhere"] == ["repo not approved for offload"]
    assert skipped["Design thing"] == ["class not suitable for the offload seat"]
    assert any("gates:" in r for r in skipped["Tests no gates"])
    assert all(j["repo"] == REPO for j in bus.jobs.values())
    # an empty allowedRepos pulls nothing at all
    empty = oq.config_from_dict({"evergreenFile": str(_backlog(tmp_path))})
    bus2 = FakeBus()
    res = oq.evergreen_pull(ctx(bus2, cfg_=empty), 10, dry_run=False)
    assert res["submitted"] == [] and bus2.writes() == []


def test_evergreen_pull_n_counts_new_jobs_and_skips_those_already_queued(tmp_path):
    c = cfg()
    c.evergreen_file = str(_backlog(tmp_path))
    bus = FakeBus()
    first = oq.evergreen_pull(ctx(bus, cfg_=c), 1, dry_run=False)
    assert [s["title"] for s in first["submitted"]] == ["Approved one"]
    second = oq.evergreen_pull(ctx(bus, cfg_=c), 1, dry_run=False)
    assert [e["title"] for e in second["existing"]] == ["Approved one"]
    assert [s["title"] for s in second["submitted"]] == ["Tests"]
    dry = oq.evergreen_pull(ctx(FakeBus(), cfg_=c), 2, dry_run=True)
    assert len(dry["submitted"]) == 2


def test_evergreen_plain_text_backlog(tmp_path):
    path = tmp_path / "evergreen.txt"
    path.write_text(
        "title: Doc the CLI\nrepo: org/approved\nclass: doc-from-code\nestimateMinutes: 30\n"
        f"check: {CHECK}\nbrief: Write the usage section.\nKeep it short.\n---\n"
        "title: Second\nrepo: org/approved\nclass: doc-from-code\ncheck: true\nbrief: Another.\n"
    )
    rows = oq.load_evergreen(str(path))
    assert [r["title"] for r in rows] == ["Doc the CLI", "Second"]
    assert rows[0]["brief"] == "Write the usage section.\nKeep it short."
    c = cfg()
    c.evergreen_file = str(path)
    res = oq.evergreen_pull(ctx(FakeBus(), cfg_=c), 5, dry_run=True)
    assert len(res["submitted"]) == 2


# ------------------------------------------------------------------ status


def test_status_reports_depth_spend_snapshot_age_stuck_claims_and_evergreen(tmp_path, capsys):
    eg = _backlog(tmp_path)
    jobs = [
        mkjob("q1", "m3"),
        {**mkjob("c1", "m3", status="claimed"), "claimed_at": NOW - 7200, "claim_lease_expires_at": NOW - 600},
        {**mkjob("c2", "m3", status="claimed"), "claimed_at": NOW - 60, "claim_lease_expires_at": NOW + 600},
        {**mkjob("r1", "m3", status="running"), "claimed_at": NOW - 7200},  # no lease field: claimed_at + lease_seconds
        {**mkjob("d1", "m3", status="done", minutes=60), "ended_at": NOW - 3600},
        {**mkjob("d0", "m3", status="done", minutes=60), "ended_at": NOW - 40 * 3600},  # before the window
        mkjob("other", "m3", kind="zeroclaw"),
    ]
    c = cfg()
    c.evergreen_file = str(eg)
    bal = tmp_path / "bal.json"
    bal.write_text(
        json.dumps(
            {
                "balance": {
                    "dailyRemainingHours": 12,
                    "walletHours": 5,
                    "spendableHours": 17,
                    "dailyResetAt": RESET,
                    "updatedAt": NOW - 20 * 60,
                },
                "sessionModel": None,
            }
        )
    )
    mod = tmp_path / "models.json"
    mod.write_text(json.dumps(MODELS))
    conf = tmp_path / "cfg.json"
    conf.write_text(json.dumps({"allowedRepos": [REPO], "evergreenFile": str(eg)}))
    rows = tmp_path / "jobs.json"
    rows.write_text(json.dumps({"jobs": jobs}))
    out, err = io.StringIO(), io.StringIO()
    rc = oq.main(
        ["status", "--json", "--jobs", str(rows), "--now", RESET.replace("20:00", "12:00")],
        env={"OFFLOAD_CONFIG": str(conf), "OFFLOAD_BALANCE_FILE": str(bal), "OFFLOAD_MODELS_FILE": str(mod)},
        stdout=out,
        stderr=err,
    )
    assert rc == 0, err.getvalue()
    st = json.loads(out.getvalue())
    assert st["queue"] == {"queued": 1, "claimed": 2, "running": 1, "done": 2}
    assert sorted(s["id"] for s in st["stuckClaimed"]) == ["c1", "r1"]
    assert st["snapshotStale"] is True and round(st["snapshotAgeMinutes"]) == 20
    assert st["spentHoursEstimated"] == 3  # one done job in the window: 60 min at 3x
    assert st["plannedHours"] == 3
    assert st["evergreen"] == {"total": 6, "usable": 3, "skipped": 3}
    assert any("stale" in w for w in st["warnings"]) or st["snapshotStale"]


# --------------------------------------------------------------------- CLI


def test_cli_plan_text_and_json_and_submit_dry_run(tmp_path):
    mod = tmp_path / "models.json"
    mod.write_text(json.dumps(MODELS))
    bal = tmp_path / "bal.json"
    bal.write_text(
        json.dumps(
            {
                "balance": {
                    "dailyRemainingHours": 9,
                    "walletHours": 0,
                    "spendableHours": 9,
                    "dailyResetAt": RESET,
                    "updatedAt": NOW,
                }
            }
        )
    )
    conf = tmp_path / "cfg.json"
    conf.write_text(json.dumps({"allowedRepos": [REPO], "where": "orchestration-only"}))
    rows = tmp_path / "jobs.json"
    rows.write_text(json.dumps({"jobs": [mkjob("a", "m3", cls="soak-loop")]}))
    env = {"OFFLOAD_CONFIG": str(conf), "OFFLOAD_BALANCE_FILE": str(bal), "OFFLOAD_MODELS_FILE": str(mod)}
    common = ["--jobs", str(rows), "--now", "2026-10-30T12:00:00Z"]
    out = io.StringIO()
    assert oq.main(["plan", *common], env=env, stdout=out, stderr=io.StringIO()) == 0
    assert "verdict:" in out.getvalue() and "orchestration-only" in out.getvalue()
    out = io.StringIO()
    assert oq.main(["plan", "--json", *common], env=env, stdout=out, stderr=io.StringIO()) == 0
    assert json.loads(out.getvalue())["blocks"][0]["model"] == "m3"
    tf = tmp_path / "task.json"
    tf.write_text(json.dumps(task()))
    out = io.StringIO()
    assert oq.main(["submit", "--task", str(tf), "--dry-run", *common], env=env, stdout=out, stderr=io.StringIO()) == 0
    assert "dry run" in out.getvalue()
    tf.write_text(json.dumps(task(repo="org/nope")))
    out = io.StringIO()
    assert oq.main(["submit", "--task", str(tf), "--dry-run", *common], env=env, stdout=out, stderr=io.StringIO()) == 3
    assert "REFUSED: repo not approved for offload" in out.getvalue()
    # a missing bus URL with no --jobs is a clear error, not a crash
    err = io.StringIO()
    assert oq.main(["plan"], env={**env}, stdout=io.StringIO(), stderr=err) == 2
    assert "HIVE_URL is not set" in err.getvalue()


def test_config_rejects_nonsense_and_names_unknown_keys():
    with pytest.raises(oq.ConfigError):
        oq.config_from_dict({"minMultiplier": 5, "maxMultiplier": 4})
    with pytest.raises(oq.ConfigError):
        oq.config_from_dict({"allowedRepos": "org/x"})
    p = plan([], cfg_=oq.config_from_dict({"allowedRepo": ["org/x"]}))  # a typo must be loud
    assert any("allowedRepo" in w for w in p["warnings"])


def test_script_carries_no_address_token_or_urn():
    text = (Path(__file__).parent / "scripts" / "offload_queue.py").read_text()
    import re

    assert not re.search(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", text)
    assert "urn:agent" not in text and "Bearer " + "ey" not in text
    assert "http://" not in text.replace("http://<", "")


# ------------------------------------------- against the REAL bus (in-process)


class RealBusTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import httpx

        import agent_bus
        from hive.bus import deps
        from hive.bus.domain import branch_admission as ba

        self._td = tempfile.TemporaryDirectory()
        self.addCleanup(self._td.cleanup)
        env = mock.patch.dict(os.environ, {"AGENT_BUS_DB": os.path.join(self._td.name, "offload.db")})
        env.start()
        self.addCleanup(env.stop)
        repo = mock.patch.object(deps, "_HIVE_REPO", None)
        repo.start()
        self.addCleanup(repo.stop)
        await deps.hive_repo().init()
        for patcher in (
            mock.patch.object(ba, "resolve_remote_url", return_value="ssh://git@example.test/repo"),
            mock.patch(
                "hive.bus.routers.jobs.branch_admission",
                side_effect=lambda **kw: ba.branch_admission(
                    **kw,
                    resolver=lambda r: "ssh://git@example.test/repo",
                    prober=lambda url, ref: "present" if ref == "main" else "absent",
                ),
            ),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=agent_bus.app), base_url="http://t")
        self.addAsyncCleanup(self.client.aclose)
        reg = {"host": "studio", "status": "online", "capabilities": ["code", "orchestrate"], "auth_method": "api"}
        r = await self.client.post(
            "/v1/agents/register", json={**reg, "kind": "opencode", "runtime": "opencode", "cost_tier": "C"}
        )
        self.assertEqual(r.status_code, 200, r.text)
        self.actor = r.json()["urn"]
        r = await self.client.post("/v1/agents/register", json={**reg, "kind": "freebuff", "runtime": "freebuff"})
        self.assertEqual(r.status_code, 200, r.text)
        self.seat = r.json()["urn"]
        self.loop = asyncio.get_running_loop()

    def transport(self, method, path, body=None):
        """A synchronous transport onto the in-process app, called from a worker thread."""

        async def go():
            r = await self.client.request(method, path, json=body)
            try:
                return r.status_code, r.json()
            except ValueError:
                return r.status_code, None

        return asyncio.run_coroutine_threadsafe(go(), self.loop).result(timeout=30)

    def context(self):
        return oq.Context(
            cfg=cfg(), models=models(), balance=balance(), transport=self.transport, now=NOW, actor=self.actor
        )

    async def test_submit_is_idempotent_and_the_job_has_the_contract_fields(self):
        c = self.context()
        first = await asyncio.to_thread(oq.submit_task, c, task(), dry_run=False)
        self.assertTrue(first["ok"], first)
        again = await asyncio.to_thread(oq.submit_task, c, task(), dry_run=False)
        self.assertEqual(again["id"], first["id"])
        self.assertTrue(again["coalesced"])
        r = await self.client.get(f"/v1/jobs/{first['id']}")
        job = r.json()
        self.assertEqual(job["kind"], "freebuff")
        self.assertEqual(job["eligible_kinds"], ["freebuff"])
        self.assertEqual(job["preferred_models"], ["m3"])
        self.assertEqual(job["priority"], 90)
        self.assertEqual(job["head_branch"], "offload/x")
        self.assertEqual(
            oq.parse_header(job["description"])["gates"], "no-assertion-weakening,no-new-skips,suite-before-after"
        )
        listed = (await self.client.get("/v1/jobs?status=queued")).json()["jobs"]
        self.assertEqual(len([j for j in listed if j["kind"] == "freebuff"]), 1)

    async def test_depends_on_must_exist_on_the_bus(self):
        c = self.context()
        bad = await asyncio.to_thread(
            oq.submit_task, c, task(depends_on=["00000000-0000-4000-8000-000000000000"]), dry_run=False
        )
        self.assertFalse(bad["ok"])
        self.assertIn("depends_on job does not exist", bad["refused"])
        first = await asyncio.to_thread(oq.submit_task, c, task(), dry_run=False)
        ok = await asyncio.to_thread(
            oq.submit_task,
            c,
            task(title="Next", brief=brief(branch="offload/next"), depends_on=[first["id"]]),
            dry_run=False,
        )
        self.assertTrue(ok["ok"], ok)

    async def test_release_touches_only_the_kind_and_reports_the_missing_priority_patch(self):
        c = self.context()
        a = await asyncio.to_thread(oq.submit_task, c, task(model="m3"), dry_run=False)
        b = await asyncio.to_thread(
            oq.submit_task, c, task(title="B", model="m2", brief=brief(branch="offload/b")), dry_run=False
        )
        other = await self.client.post(
            "/v1/jobs",
            json={
                "kind": "freebuff",
                "description": "foreign row of the same kind, no header",
                "submitter_urn": self.actor,
                "eligible_kinds": ["freebuff"],
                "priority": 7,
            },
        )
        self.assertEqual(other.status_code, 200, other.text)
        dry = await asyncio.to_thread(oq.release, c, dry_run=True)
        self.assertEqual(dry["activeBlock"], "m3")
        got = (await self.client.get(f"/v1/jobs/{a['id']}")).json()
        self.assertEqual(got["priority"], 90)
        # the real bus: PATCH accepts the call but JobUpdate has no priority field
        c.balance = balance(session_model="m2", state="active")  # the open session makes m2 the active block
        res = await asyncio.to_thread(oq.release, c, dry_run=False)
        self.assertEqual(res["activeBlock"], "m2")
        self.assertFalse(res["ok"])
        self.assertIn("did not change priority", res["error"])
        for jid, want in ((a["id"], 90), (b["id"], 80)):
            self.assertEqual((await self.client.get(f"/v1/jobs/{jid}")).json()["priority"], want)
        # a job of another kind is never listed for release
        other_kind = await self.client.post(
            "/v1/jobs",
            json={
                "kind": "test:other",
                "description": "x",
                "submitter_urn": self.actor,
                "eligible_kinds": ["freebuff"],
            },
        )
        self.assertEqual(other_kind.status_code, 200, other_kind.text)
        again = await asyncio.to_thread(oq.release, c, dry_run=True)
        self.assertNotIn(other_kind.json()["id"], [x["id"] for x in again["changes"]])


if __name__ == "__main__":
    unittest.main()
