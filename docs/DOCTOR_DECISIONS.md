# Doctor loop-outcome decisions

What the orphan-failure sweep does with each failed-loop outcome class from
`hive/workers/loop_outcome.py`. Source of truth is code: `classify()` picks
the class, `decide()` picks the verdict, `hive/workers/doctor.py` acts on it
(requeue / hold / park / skip). A test (`test_doctor_decisions_20261029.py`)
parses the table below and checks every row against `decide()`. Do not edit
the table by hand without running the test.

Constants: `MAX_AUTO_REQUEUES = 2` (`loop_outcome.py`). Requeue marks live
7 days (`LOOP_REQUEUE_TTL_SEC`), parks 30 days (`LOOP_PARK_TTL_SEC`), and the
sweep's requeue action is gated per failed-row set for 600 s
(`ORPHAN_RESUBMIT_TTL_SEC`), so a requeue verdict does not always mean an
immediate requeue.

Verdicts: `requeue` (resubmit via the normal path), `hold` (do nothing this
pass — the cause is outside the job), `needs-review` (park once as
`needs-review` with a `loop-outcome:<class>` reason and a
`doctor.needs-review` broadcast), `skip` (not the auto-requeue path —
approved work waits for its merge), `default` (unknown class: today's
normal per-kind resubmit behaviour, unchanged).

| outcome class | requeues 0-1, reviewer up | requeues 0-1, reviewer down | requeues 2+ | local-only commits | requeue cap | cooldown | operator sees |
| --- | --- | --- | --- | --- | --- | --- | --- |
| reviewer_unavailable | requeue | hold | needs_review | -- | 2 | orphan sweep 600s per failed set | requeued; held (log only); parked + broadcast |
| author_failed | requeue | requeue | needs_review | -- | 2 | orphan sweep 600s per failed set | requeued; parked + broadcast |
| timeout | requeue | requeue | needs_review | -- | 2 | orphan sweep 600s per failed set | requeued; parked + broadcast |
| crash | requeue | requeue | needs_review | -- | 2 | orphan sweep 600s per failed set | requeued; parked + broadcast |
| exhausted_request_changes | needs_review | needs_review | needs_review | -- | 0 | park once per 30d | parked + broadcast |
| nothing_to_commit | needs_review | needs_review | needs_review | -- | 0 | park once per 30d | parked + broadcast |
| approved_unmerged | skip | skip | skip | -- | -- | none | excluded silently (log only) |
| killed_by_signal | requeue | requeue | needs_review | needs_review | 2 (0 when local-only) | orphan sweep 600s per failed set | requeued; parked + broadcast |
| clone_failed | requeue | requeue | needs_review | -- | 2 | orphan sweep 600s per failed set | requeued; parked + broadcast |
| empty_diff | needs_review | needs_review | needs_review | -- | 0 | park once per 30d | parked with empty-diff reason + broadcast |
| unknown | default | default | default | -- | -- | normal resubmit path | unchanged behaviour |
