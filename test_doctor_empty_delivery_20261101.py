"""An empty delivery is an author failure, never a review (live 2026-11-01).

12 work rows ended needs-review with ``commits == []`` and ``head_sha ==
base_sha`` (the author died on the zeroclaw 'Session limit reached' fault).
The doctor filed them ``waiting_review``; a reviewer then approved the EMPTY
diff or returned prose, and each row burned its review attempts. The right
action is to rerun the author. Row shapes are the real ones from the bus.
"""

from __future__ import annotations

from hive.workers import doctor_remediation as dr

BASE_SHA = "1cbe935698dd517e207b580ccb54d1fab681c3ea"


def empty_row(**overrides) -> dict:
    row = {
        "id": "01a198b2-54f2-7387-9908-5878958ca9a3",
        "kind": "build:zeroclaw",
        "status": "needs-review",
        "repo": "zoder",
        "project": "zoder",
        "head_branch": "hv/cf2-zf-cancel-streak-bug-20261031",
        "base_branch": "master",
        "max_retries": 2,
        "retry_count": 0,
        "result": {
            "commits": [],
            "head_sha": BASE_SHA,
            "base_sha": BASE_SHA,
            "files_changed": [],
            "exit_code": 1,
            "remote_verified": True,
            "pushed": True,
            "failure_reason": "loop_unresolved",
        },
    }
    result = overrides.pop("result", None)
    row.update(overrides)
    if result:
        row["result"] = {**row["result"], **result}
    return row


def test_a_work_row_with_no_commits_and_head_equal_base_is_an_empty_delivery():
    cause = dr.classify(empty_row())
    assert cause is not None and cause.name == dr.EMPTY_DELIVERY, cause


def test_the_live_failed_shape_is_empty_too():
    assert dr.classify(empty_row(status="failed")).name == dr.EMPTY_DELIVERY


def test_a_row_with_commits_is_not_empty():
    row = empty_row(result={"commits": ["a" * 40], "head_sha": "a" * 40, "files_changed": ["x.py"]})
    cause = dr.classify(row)
    assert cause is None or cause.name != dr.EMPTY_DELIVERY


def test_a_head_that_moved_off_base_is_not_empty():
    row = empty_row(result={"head_sha": "b" * 40})
    cause = dr.classify(row)
    assert cause is None or cause.name != dr.EMPTY_DELIVERY


def test_files_changed_means_it_delivered_something():
    row = empty_row(result={"files_changed": ["hive/x.py"]})
    cause = dr.classify(row)
    assert cause is None or cause.name != dr.EMPTY_DELIVERY


def test_a_review_job_is_never_an_empty_delivery():
    cause = dr.classify(empty_row(kind="review:build:zeroclaw-20261101"))
    assert cause is None or cause.name != dr.EMPTY_DELIVERY


def test_a_verify_job_is_never_an_empty_delivery():
    assert dr.classify(empty_row(kind="verify:build:zeroclaw")) is None


def test_a_row_with_no_recorded_head_or_base_is_not_called_empty():
    row = empty_row()
    row["result"].pop("head_sha")
    row["result"].pop("base_sha")
    cause = dr.classify(row)
    assert cause is None or cause.name != dr.EMPTY_DELIVERY


def test_the_plan_requeues_the_author_and_never_hands_it_to_review():
    row = empty_row()
    cause = dr.classify(row)
    plan = dr.plan_for(row, cause, default_branch="master")
    assert plan is not None
    assert plan.requeue is True and plan.needs_review is False
    assert plan.override is False  # a retry is still left


def test_a_spent_retry_budget_is_raised_by_one_so_the_rerun_can_happen():
    row = empty_row(max_retries=2, retry_count=2)
    cause = dr.classify(row)
    assert cause.name == dr.EMPTY_DELIVERY
    plan = dr.plan_for(row, cause, default_branch="master")
    assert plan.requeue is True and plan.override is True
    assert plan.changes["max_retries"]["after"] == 3


def test_an_empty_row_is_not_reviewable():
    assert dr.is_empty_delivery(empty_row()) is True
    assert dr.is_empty_delivery(empty_row(result={"commits": ["a" * 40]})) is False


def test_no_review_child_is_ever_created_for_an_empty_delivery():
    """The shared choke point of both review submit paths refuses it."""
    from hive.workers import doctor

    reason = doctor._review_child_refusal(empty_row())
    assert reason and "empty_delivery" in reason
    delivered = empty_row(result={"commits": ["a" * 40], "head_sha": "a" * 40, "files_changed": ["x.py"]})
    assert "empty_delivery" not in doctor._review_child_refusal(delivered)
