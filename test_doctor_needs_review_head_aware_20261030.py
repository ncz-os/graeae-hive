"""A needs-review row is only "reviewed" when the evidence covers its CURRENT head.

Shape observed on the live bus (anonymised here, fake ids and fake shas): a zoder
loop rescued unresolved work and parked the row in needs-review. Earlier rounds
had a reviewer, who said request_changes; in the final round the reviewer went
away. ``result.review_evidence`` and ``result.reviewer`` come from the earlier
rounds, ``result.head_sha`` is the final pushed head, and the loop's teardown says
``reviewer_unavailable`` and ``resolved: false``. The doctor used to read the old
evidence as "already reviewed" and did nothing for as long as the row existed.

No network: ``_http`` is a stateful in-memory fake of the bus.
"""

from __future__ import annotations

import copy

import pytest

from hive.workers import doctor
from hive.workers.doctor_state import DurableStateStore, MemoryBackend

JOB_ID = "job-fake-0001"
EARLIER = "ab" * 20
MIDDLE = "cd" * 20
FINAL = "fe" * 20
CHILD_PREFIX = "review-child-"


def live_row_result() -> dict:
    """The needs-review row's result as the loop left it (fake ids and shas)."""
    return {
        "failure_reason": "loop_unresolved_work_rescued",
        "reviewer": {"model": "fake-reviewer", "verdict": "request_changes", "source": "zoder_verdict"},
        "review_evidence": {
            "requested": "",
            "reviewers": [{"model": "fake-reviewer", "role": "", "verdict": "request_changes", "ok": True}],
            "first": {"model": "fake-reviewer", "verdict": "request_changes"},
            "secondaries": [],
            "skipped": [],
            "warnings": [],
        },
        "loop_result": {
            "verdicts": ["request_changes", "request_changes", "request_changes", "reviewer_unavailable"],
            "teardown": {
                "final_verdict": "reviewer_unavailable",
                "resolved": False,
                "last_good_head_sha": FINAL,
            },
        },
        "head_sha": FINAL,
        "commits": [EARLIER, MIDDLE, FINAL],
    }


def make_row(result: dict | None = None, **extra) -> dict:
    row = {
        "id": JOB_ID,
        "kind": "build:fake-task",
        "status": "needs-review",
        "repo": "owner/fake-repo",
        "head_branch": "feat/fake-branch",
        "base_branch": "main",
        "project": "fake-project",
        "result": live_row_result() if result is None else result,
    }
    row.update(extra)
    return row


class FakeBus:
    """Just enough bus for the follow-up sweep: listing, child listing, submit, patch."""

    def __init__(self, row: dict):
        self.row = row
        self.children: list[dict] = []
        self.calls: list[tuple] = []
        self.posted: list[dict] = []
        self.messages: list[tuple[str, dict]] = []

    def http(self, method, path, body=None, **kwargs):
        self.calls.append((method, path, body))
        if method == "GET":
            if "parent_job_id=" in path:
                return 200, {"jobs": [dict(c) for c in self.children]}
            return 200, {"jobs": [self.row], "total": 1}
        if method == "POST" and path == "/v1/jobs":
            self.posted.append(body)
            child = {"id": f"{CHILD_PREFIX}{len(self.posted)}", "kind": body["kind"], "status": "queued"}
            self.children.append(child)
            return 201, {"id": child["id"]}
        if method == "PATCH" and path == f"/v1/jobs/{self.row['id']}":
            self.row["result"] = copy.deepcopy(body["result"])
            return 200, {}
        return 200, {}

    def writes(self) -> list[tuple]:
        return [c for c in self.calls if c[0] in {"POST", "PATCH", "PUT", "DELETE"}]


@pytest.fixture
def bus(monkeypatch):
    fake = FakeBus(make_row())
    monkeypatch.setattr(doctor, "_http", fake.http)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda topic, body: fake.messages.append((topic, body)))
    return fake


def sweep() -> dict:
    return doctor.run_review_followup_sweep()


def restart_doctor(monkeypatch) -> None:
    """A fresh process: the durable markers of the old one are not there."""
    monkeypatch.setattr(doctor, "STATE", DurableStateStore(MemoryBackend(), refresh_secs=0))


# ── the live row ───────────────────────────────────────────────────────────


def test_live_row_shape_gets_a_re_review(bus):
    report = sweep()
    assert report["submitted"] == 1 and report.get("skipped_verdict", 0) == 0
    assert len(bus.posted) == 1
    spec = bus.posted[0]
    assert spec["parent_job_id"] == JOB_ID
    assert spec["kind"].startswith("review:")
    assert FINAL in spec["description"], "the review must name the final head"
    assert spec["idempotency_key"] == f"needs-review-followup:{JOB_ID}:{FINAL[:12]}"
    result = bus.row["result"]
    assert result["auto_review_job_id"] == f"{CHILD_PREFIX}1"
    assert result["re_review_head"] == FINAL
    assert result["doctor_reason"].startswith("re-review: ")
    assert "final round had no reviewer" in result["doctor_reason"]
    assert result["review_evidence"] == live_row_result()["review_evidence"], "earlier evidence is kept"
    announced = [body for topic, body in bus.messages if topic == "doctor.needs_review"]
    assert announced and announced[0]["auto_review_job_id"] == f"{CHILD_PREFIX}1"
    assert "re_review" in announced[0]


def test_second_pass_is_a_no_op_even_after_a_doctor_restart(bus, monkeypatch):
    assert sweep()["submitted"] == 1
    writes_after_first = len(bus.writes())
    again = sweep()
    assert again["submitted"] == 0 and again["skipped_marker"] == 1
    # a restarted doctor has no markers: the row itself (auto id + re_review_head) is the cap
    restart_doctor(monkeypatch)
    assert sweep()["submitted"] == 0, "child still queued: a review is in flight"
    bus.children[0]["status"] = "failed"
    restarted = sweep()
    assert restarted["submitted"] == 0 and restarted["skipped_marker"] == 1, "one re-review per (row, head)"
    assert len(bus.writes()) == writes_after_first
    assert len(bus.posted) == 1


def test_a_new_head_earns_a_new_re_review_once(bus, monkeypatch):
    assert sweep()["submitted"] == 1
    bus.children[0]["status"] = "failed"
    newer = "99" * 20
    bus.row["result"]["head_sha"] = newer
    bus.row["result"]["commits"].append(newer)
    restart_doctor(monkeypatch)
    assert sweep()["submitted"] == 1
    assert bus.posted[1]["idempotency_key"].endswith(newer[:12])
    assert sweep()["submitted"] == 0
    assert len(bus.posted) == 2


def test_evidence_for_the_current_head_is_not_re_reviewed(bus):
    bus.row["result"]["review_evidence"]["reviewed_head"] = FINAL
    report = sweep()
    assert report["submitted"] == 0 and report["skipped_verdict"] == 1
    assert bus.writes() == []


@pytest.mark.parametrize(
    "mutate",
    [
        lambda r: r["review_evidence"].update(head_matches=True),
        lambda r: r["review_evidence"].update(verdict={"verdict": "approve", "head": FINAL}),
        lambda r: r["reviewer"].update(head=FINAL[:12]),
        lambda r: r["loop_result"]["teardown"].update(resolved=True),
        lambda r: r["loop_result"].update(
            verdicts=["request_changes", "approve"], teardown={"final_verdict": "approve"}
        ),
        lambda r: r["review_evidence"].update(child_advanced_branch=True, reviewed_head=MIDDLE, head_matches=False),
    ],
)
def test_evidence_that_covers_the_head_leaves_the_row_alone(bus, mutate):
    mutate(bus.row["result"])
    assert sweep()["submitted"] == 0
    assert bus.writes() == []


def test_same_row_with_no_loop_outage_and_no_head_info_is_still_treated_as_reviewed(bus):
    # what the base code did for every row with evidence: unchanged when nothing says otherwise
    result = bus.row["result"]
    del result["loop_result"]
    assert sweep()["skipped_verdict"] == 1
    assert bus.writes() == []


def test_evidence_for_an_older_head_is_re_reviewed(bus):
    result = bus.row["result"]
    del result["loop_result"]
    result["review_evidence"]["reviewed_head"] = MIDDLE
    assert sweep()["submitted"] == 1
    assert "older head" in bus.row["result"]["doctor_reason"]


def test_evidence_with_a_verdict_head_for_an_older_head_is_re_reviewed(bus):
    result = bus.row["result"]
    del result["loop_result"]
    result["review_evidence"]["verdict"] = {"verdict": "approve", "head": EARLIER}
    assert sweep()["submitted"] == 1
    assert "older head" in bus.row["result"]["doctor_reason"]


def test_last_verdict_alone_marks_the_final_round_as_unreviewed(bus):
    result = bus.row["result"]
    del result["loop_result"]["teardown"]
    assert sweep()["submitted"] == 1
    assert "final round had no reviewer" in bus.row["result"]["doctor_reason"]


def test_request_changes_with_newer_commits_is_re_reviewed(bus):
    result = bus.row["result"]
    del result["loop_result"]
    del result["head_sha"]
    result["review_evidence"]["reviewed_head"] = MIDDLE
    assert sweep()["submitted"] == 1
    assert "newer commit" in bus.row["result"]["doctor_reason"]


def test_request_changes_on_the_last_commit_is_not_re_reviewed(bus):
    result = bus.row["result"]
    del result["loop_result"]
    del result["head_sha"]
    result["review_evidence"]["reviewed_head"] = FINAL
    assert sweep()["submitted"] == 0
    assert bus.writes() == []


def test_empty_diff_teardown_is_not_a_reviewer_outage(bus):
    result = bus.row["result"]
    result["loop_result"]["teardown"].update(reason="reviewer_unavailable", detail="no review chunks were generated")
    assert sweep()["submitted"] == 0
    assert bus.writes() == []


def test_row_without_any_evidence_still_gets_the_plain_review(bus):
    result = bus.row["result"]
    for key in ("review_evidence", "reviewer", "loop_result"):
        del result[key]
    assert sweep()["submitted"] == 1
    assert bus.posted[0]["idempotency_key"] == f"needs-review-followup:{JOB_ID}"
    assert "re_review_head" not in bus.row["result"]
    assert bus.row["result"]["doctor_reason"].startswith("auto review submitted")


# ── never loops, never overrides a guard ───────────────────────────────────


def test_a_review_in_flight_blocks_the_re_review(bus):
    bus.children.append({"id": "review-child-existing", "kind": "review:fake", "status": "running"})
    bus.row["result"]["auto_review_job_id"] = "review-child-existing"
    assert sweep()["skipped_verdict"] == 1
    assert bus.writes() == []


def test_allow_submit_false_defers_and_writes_nothing(bus):
    assert doctor.ensure_review_followup(bus.row, allow_submit=False) == "deferred"
    assert bus.writes() == []


def test_submitted_marker_blocks_the_re_review(bus):
    doctor.STATE.mark(f"review:submitted:{JOB_ID}", ttl=doctor.REVIEW_FOLLOWUP_TTL_SEC)
    assert sweep()["skipped_marker"] == 1
    assert bus.writes() == []


def test_durable_state_failure_refuses_the_re_review(bus, monkeypatch):
    monkeypatch.setattr(doctor.STATE, "persist_ok", False)
    report = sweep()
    assert report["errors"] == 1 and report["submitted"] == 0
    assert bus.writes() == []


def test_failed_submit_is_retried_with_the_same_key_and_leaves_no_marker(bus, monkeypatch):
    specs = []

    def lost(method, path, body=None, **kwargs):
        if (method, path) == ("POST", "/v1/jobs"):
            specs.append(body)
            return 0, None
        return bus.http(method, path, body, **kwargs)

    monkeypatch.setattr(doctor, "_http", lost)
    sweep()
    sweep()
    assert len(specs) == 2 and specs[0]["idempotency_key"] == specs[1]["idempotency_key"]
    assert not doctor.review_followup_submitted(JOB_ID)
    assert "re_review_head" not in bus.row["result"]


def test_protected_project_row_is_untouched(bus, monkeypatch):
    monkeypatch.setattr(doctor, "DLQ_PROTECT_PROJECTS", frozenset({"fake-project"}))
    assert sweep()["skipped_protected"] == 1
    assert bus.writes() == []


def test_protected_kind_row_is_untouched(bus, monkeypatch):
    monkeypatch.setattr(doctor, "DLQ_PROTECT_KIND_PREFIXES", ("build:",))
    assert sweep()["skipped_protected"] == 1
    assert bus.writes() == []


def test_routing_signal_row_is_untouched(bus):
    bus.row["result"]["worker_error"] = "released_by_host"
    assert sweep()["skipped_protected"] == 1
    assert bus.writes() == []


def test_triage_row_is_untouched(bus):
    bus.row["kind"] = "triage:build:fake-task"
    assert sweep()["skipped_triage"] == 1
    assert bus.writes() == []


def test_row_level_verdict_is_untouched(bus):
    bus.row["result"]["verdict"] = {"verdict": "approve"}
    assert sweep()["skipped_verdict"] == 1
    assert bus.writes() == []


# ── a review the bus already recorded for an older head ────────────────────


def reconciled_old_head_row(bus: FakeBus, child_status: str = "done") -> dict:
    """Parent after the bus reconciled a finished review of an OLDER head."""
    proof = {"verdict": {"verdict": "approve", "head": MIDDLE}, "deliverable": "Reviewed."}
    bus.children.append({"id": "review-child-old", "kind": "review:fake", "status": child_status, "result": proof})
    result = bus.row["result"]
    del result["loop_result"]
    result["auto_review_job_id"] = "review-child-old"
    result["review_evidence"] = {
        "review_job_id": "review-child-old",
        "verdict": proof["verdict"],
        "head_matches": False,
        "artifact_valid": True,
        "reason": "review HEAD does not match delivered HEAD",
    }
    return proof


def test_recorded_review_of_an_older_head_is_re_reviewed_not_replayed_forever(bus):
    reconciled_old_head_row(bus)
    report = sweep()
    assert report.get("replayed_verdict", 0) == 0
    assert report["submitted"] == 1
    assert [c for c in bus.writes() if c[1] == "/v1/jobs/review-child-old"] == []
    assert bus.posted[0]["idempotency_key"] == f"needs-review-followup:{JOB_ID}:{FINAL[:12]}"
    assert bus.row["result"]["auto_review_job_id"] == f"{CHILD_PREFIX}1"
    assert sweep()["submitted"] == 0, "and then it stops"


def test_recorded_review_that_is_still_in_flight_is_not_duplicated(bus):
    reconciled_old_head_row(bus, child_status="running")
    assert sweep()["submitted"] == 0
    assert bus.writes() == []
