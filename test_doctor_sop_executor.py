"""What the doctor SOP's decisions need from the executor (2026-10-22).

Three defects, each with a test that fails on the base commit:

1. `cancel_jobs` hard-deleted up to 20 jobs plus every child at any confidence,
   and a DLQ `remove` did the same for one job, with no operator in the loop.
2. Two purge routes with two payload keys (`ids` on `/v1/jobs/purge`, `job_ids` on
   `/v1/admin/jobs/purge`), and the DLQ path reported `removed <id>` for any 2xx
   — including `ok: []` with the job in `skipped_not_dead_letter`, i.e. a removal
   that never happened.
3. `cixmini` was offered in `DOCTOR_SYSTEM` as a restart target, had a whole
   branch in `action_restart_service`, and was not in `FLEET_HOSTS`, so the
   function answered `unknown host 'cixmini'` before reaching that branch. Dead
   code behind a prompt that invites it.

Plus the durability half: a pod restart used to reset the loop counter, the
per-kind cooldown, the DLQ-seen set and the SSH rate limiter.
"""

import json
import time

import pytest

from hive.workers import doctor, doctor_gates
from hive.workers.doctor_state import (
    COUNTER_TTLS,
    DurableStateStore,
    FileBackend,
    MemoryBackend,
    state_store_from_env,
)

# ── fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def store(tmp_path):
    """A file-backed store, so durability is real rather than asserted."""
    clock = {"now": 1_000_000.0}
    backend = FileBackend(str(tmp_path / "doctor-state.json"))
    return DurableStateStore(backend, refresh_secs=0.0, time_fn=lambda: clock["now"]), clock


@pytest.fixture
def gate(store):
    state, _clock = store
    return doctor_gates.ApprovalGate(state, gate_enabled=True, auto_approve=False, ttl=900.0)


@pytest.fixture
def doctor_state(monkeypatch, tmp_path):
    """Point the doctor at a durable store and a gate that starts closed."""
    clock = {"now": 2_000_000.0}
    state = DurableStateStore(FileBackend(str(tmp_path / "state.json")), refresh_secs=0.0, time_fn=lambda: clock["now"])
    monkeypatch.setattr(doctor, "STATE", state)
    monkeypatch.setattr(doctor, "APPROVALS", doctor_gates.ApprovalGate(state, gate_enabled=True))
    monkeypatch.setattr(doctor, "_seen_job_attempts", {})
    monkeypatch.setattr(doctor, "_recent_auto_triage", {})
    monkeypatch.setattr(doctor, "_dlq_seen", {})
    monkeypatch.setattr(doctor, "_ssh_actions_log", type(doctor._ssh_actions_log)())
    monkeypatch.setattr(doctor, "_host_last_action", {})
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    return state, clock


def _http_stub(recorder, jobs=None):
    """A `_http` that answers the two routes the tests need."""

    def http(method, path, body=None, timeout=15.0):
        recorder.append((method, path, body))
        if method == "GET" and path.startswith("/v1/jobs?status=failed"):
            return 200, {"jobs": jobs if jobs is not None else []}
        if method == "GET" and path.startswith("/v1/jobs?parent_job_id="):
            return 200, {"jobs": []}
        return 200, {"deleted": len((body or {}).get("ids", [])), "matched_ids": (body or {}).get("ids", [])}

    return http


# ── 1. destructive actions wait for an operator ──────────────────────────────


def test_cancel_jobs_is_refused_without_an_approval(monkeypatch, doctor_state):
    state, _clock = doctor_state
    calls = []
    monkeypatch.setattr(doctor, "_http", _http_stub(calls, jobs=[{"id": "j1", "kind": "zeroclaw"}]))
    out = doctor.action_cancel_jobs("zeroclaw")
    assert out.startswith("REFUSED"), out
    posted = [call for call in calls if call[0] == "POST"]
    assert posted == [], f"nothing may be purged without an approval, got {posted}"
    assert "digest" in out, f"the refusal must say what to approve: {out}"


def test_cancel_jobs_proceeds_once_that_exact_action_is_approved(monkeypatch, doctor_state):
    state, _clock = doctor_state
    calls = []
    monkeypatch.setattr(doctor, "_http", _http_stub(calls, jobs=[{"id": "j1", "kind": "zeroclaw"}]))
    ids = ["j1"]
    digest = doctor_gates.action_digest("cancel_jobs", base_kind="zeroclaw", job_ids=sorted(ids))
    doctor.APPROVALS.record_approval("cancel_jobs", digest)

    out = doctor.action_cancel_jobs("zeroclaw")
    assert not out.startswith("REFUSED"), out
    posted = [call for call in calls if call[0] == "POST"]
    assert len(posted) == 1
    _method, path, body = posted[0]
    assert path == "/v1/jobs/purge"
    assert body["ids"] == ids
    assert body["status"] == "failed"
    assert "cancelled 1/1" in out


def test_an_approval_for_one_cancel_does_not_approve_the_next(monkeypatch, doctor_state):
    """The approval is scoped to the id set, which is the whole point."""
    state, _clock = doctor_state
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    digest = doctor_gates.action_digest("cancel_jobs", base_kind="zeroclaw", job_ids=["j1"])
    doctor.APPROVALS.record_approval("cancel_jobs", digest)

    calls = []
    monkeypatch.setattr(doctor, "_http", _http_stub(calls, jobs=[{"id": "j1", "kind": "zeroclaw"}]))
    assert not doctor.action_cancel_jobs("zeroclaw").startswith("REFUSED")

    # A different failure set — the next cycle, one more job — is a different
    # action, so the old approval does not carry over.
    calls.clear()
    monkeypatch.setattr(
        doctor, "_http", _http_stub(calls, jobs=[{"id": "j1", "kind": "zeroclaw"}, {"id": "j2", "kind": "zeroclaw"}])
    )
    assert doctor.action_cancel_jobs("zeroclaw").startswith("REFUSED")


def test_the_standing_auto_approve_is_off_unless_configured():
    state = DurableStateStore(MemoryBackend())
    assert doctor_gates.AUTO_APPROVE_DESTRUCTIVE is False, "the standing auto-approve ships OFF"
    off = doctor_gates.ApprovalGate(state, gate_enabled=True, auto_approve=False)
    assert off.require("cancel_jobs", job_ids=["j"]) is not None
    on = doctor_gates.ApprovalGate(state, gate_enabled=True, auto_approve=True)
    assert on.require("cancel_jobs", job_ids=["j"]) is None


def test_a_non_destructive_action_is_never_gated(gate):
    for kind in ("release_to_queue", "resubmit_jobs", "dispatch_codex_fix", "no_action"):
        assert gate.require(kind, anything=1) is None, kind
    for kind in sorted(doctor_gates.DESTRUCTIVE_ACTIONS):
        assert gate.require(kind, anything=1) is not None, kind


def test_an_approval_expires(gate):
    digest = doctor_gates.action_digest("dlq_remove", job_ids=["j"])
    gate.record_approval("dlq_remove", digest)
    assert gate.approved("dlq_remove", digest)
    # TTL is enforced by the store's window, so a fresh gate over the same state
    # with a zero TTL sees nothing.
    tight = doctor_gates.ApprovalGate(gate.store, gate_enabled=True, ttl=0.0)
    assert not tight.approved("dlq_remove", digest)


def test_a_dlq_remove_is_refused_without_an_approval(monkeypatch, doctor_state):
    monkeypatch.setattr(doctor, "DLQ_APPLY", True)
    calls = []

    def http(method, path, body=None, timeout=15.0):
        calls.append((method, path, body))
        return 200, {"ok": [], "not_found": [], "skipped_not_dead_letter": ["dead-1"]}

    monkeypatch.setattr(doctor, "_http", http)
    job = {"id": "dead-1", "kind": "zeroclaw", "result": {"last_decline_reason": "no_repo_mapping"}}
    out = doctor.remediate_dead_letter(job, "remove", "unschedulable")
    assert out.startswith("REFUSED"), out
    assert [c for c in calls if c[0] == "POST"] == []


# ── 2. one purge contract, and an honest success report ──────────────────────


def test_the_two_purge_intents_pair_the_right_route_with_the_right_key():
    cancel_path, cancel_body = doctor_gates.purge_request("cancel_failed", ["a", "b"], "why")
    assert cancel_path == "/v1/jobs/purge"
    assert cancel_body["ids"] == ["a", "b"]
    assert cancel_body["status"] == "failed", "the bulk route needs the status filter to stay narrow"
    assert "job_ids" not in cancel_body

    dlq_path, dlq_body = doctor_gates.purge_request("dlq_remove", ["c"], "why")
    assert dlq_path == "/v1/admin/jobs/purge"
    assert dlq_body["job_ids"] == ["c"]
    assert "ids" not in dlq_body
    assert "status" not in dlq_body, "the DLQ route only touches dead-letter jobs"
    assert cancel_body["reason"] == dlq_body["reason"] == "why"


def test_an_unknown_purge_intent_raises_rather_than_defaulting():
    with pytest.raises(KeyError):
        doctor_gates.purge_request("delete_everything", ["a"], "why")


def test_a_dlq_remove_that_deleted_nothing_is_not_reported_as_removed(monkeypatch, doctor_state):
    monkeypatch.setattr(doctor, "DLQ_APPLY", True)
    """The base commit said `removed <id> via admin purge` for any 2xx."""
    digest = doctor_gates.action_digest("dlq_remove", job_ids=["dead-1"])
    doctor.APPROVALS.record_approval("dlq_remove", digest)

    def http(method, path, body=None, timeout=15.0):
        return 200, {"ok": [], "not_found": [], "skipped_not_dead_letter": ["dead-1"]}

    monkeypatch.setattr(doctor, "_http", http)
    job = {"id": "dead-1", "kind": "zeroclaw"}
    out = doctor.remediate_dead_letter(job, "remove", "unschedulable")
    assert "removed dead-1" not in out, out
    assert "REFUSED" in out and "nothing deleted" in out, out


def test_a_dlq_remove_that_really_deleted_says_so(monkeypatch, doctor_state):
    monkeypatch.setattr(doctor, "DLQ_APPLY", True)
    digest = doctor_gates.action_digest("dlq_remove", job_ids=["dead-1"])
    doctor.APPROVALS.record_approval("dlq_remove", digest)
    monkeypatch.setattr(
        doctor,
        "_http",
        lambda *a, **k: (200, {"ok": ["dead-1"], "not_found": [], "skipped_not_dead_letter": []}),
    )
    out = doctor.remediate_dead_letter({"id": "dead-1", "kind": "zeroclaw"}, "remove", "unschedulable")
    assert out == "removed dead-1 via admin purge"


def test_a_partial_cancel_is_reported_as_partial(monkeypatch, doctor_state):
    calls = []

    def http(method, path, body=None, timeout=15.0):
        calls.append((method, path, body))
        if method == "GET" and path.startswith("/v1/jobs?status=failed"):
            return 200, {"jobs": [{"id": "j1", "kind": "zeroclaw"}, {"id": "j2", "kind": "zeroclaw"}]}
        if method == "GET":
            return 200, {"jobs": []}
        # The bus deleted one of two.
        return 200, {"deleted": 1, "matched_ids": ["j1"]}

    monkeypatch.setattr(doctor, "_http", http)
    digest = doctor_gates.action_digest("cancel_jobs", base_kind="zeroclaw", job_ids=["j1", "j2"])
    doctor.APPROVALS.record_approval("cancel_jobs", digest)
    out = doctor.action_cancel_jobs("zeroclaw")
    assert "cancelled 1/2" in out, out
    assert "PARTIAL" in out, out


def test_a_purge_response_that_names_nothing_is_unconfirmed():
    outcome = doctor_gates.purge_result("dlq_remove", {"ok": [], "not_found": [], "skipped_not_dead_letter": []}, ["j"])
    assert outcome["removed"] == 0
    assert outcome["partial"] is True


def test_a_non_json_purge_response_is_not_a_success():
    outcome = doctor_gates.purge_result("cancel_failed", None, ["a", "b"])
    assert outcome["removed"] == 0
    assert sorted(outcome["skipped"]) == ["a", "b"]


# ── 3. one restart allowlist, cixmini consistently in or out ──────────────────


def test_cixmini_is_offered_and_resolvable_when_configured():
    assert "cixmini" in doctor_gates.RESTART_TARGETS
    env = {"CIXMINI_IP": "192.0.2.99", "CIXMINI_SSH_PASSWORD": "hunter2"}
    target, reason = doctor_gates.resolve_restart_target("cixmini", env=env)
    assert target is not None, reason
    assert target["address"] == "192.0.2.99"
    assert target["user"] == "mini"
    assert target["auth"] == "password"


def test_cixmini_without_a_recorded_address_is_refused_by_name():
    """The base commit answered `unknown host 'cixmini'`, which is a lie: it is a
    known host with nothing recorded. The refusal has to name what to set."""
    target, reason = doctor_gates.resolve_restart_target("cixmini", env={})
    assert target is None
    assert "cixmini" in reason
    assert "CIXMINI_IP" in reason
    assert "not a restart target" not in reason


def test_cixmini_without_a_password_is_refused_before_any_ssh():
    target, reason = doctor_gates.resolve_restart_target("cixmini", env={"CIXMINI_IP": "192.0.2.99"})
    assert target is None
    assert "CIXMINI_SSH_PASSWORD" in reason


def test_the_restart_prompt_and_the_executor_read_one_table():
    """Every host the prompt offers must be one the executor can resolve, or be
    a known host whose configuration is missing (which it names)."""
    import re

    offered = re.search(r'"target_host": fleet hostname or null \(([^)]*)\)', doctor.DOCTOR_SYSTEM)
    assert offered, "DOCTOR_SYSTEM must list its target hosts"
    names = {name.strip() for name in offered.group(1).replace("\n", "").split("|") if name.strip()}
    known = set(doctor_gates.RESTART_TARGETS)
    assert names == known, (
        f"DOCTOR_SYSTEM offers {sorted(names)} but the executor knows {sorted(known)}; "
        f"a host the prompt offers and the executor refuses is the cixmini defect"
    )


def test_a_blocked_host_is_refused_even_when_allowlisted():
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setitem(doctor_gates.RESTART_TARGETS, "argos", {"address": "192.0.2.1", "user": "x", "auth": "key"})
    try:
        target, reason = doctor_gates.resolve_restart_target("argos", env={})
    finally:
        monkeypatch.undo()
    assert target is None
    assert "blocklist" in reason


def test_a_restart_needs_an_approval_for_that_host_and_service(monkeypatch, doctor_state):
    monkeypatch.setattr(doctor, "DRY_RUN", True)
    out = doctor.action_restart_service("cerberus", "zeroclaw-doctor.service")
    assert out.startswith("REFUSED"), out

    digest = doctor_gates.action_digest("restart_service", host="cerberus", service="zeroclaw-doctor.service")
    doctor.APPROVALS.record_approval("restart_service", digest)
    out = doctor.action_restart_service("cerberus", "zeroclaw-doctor.service")
    assert out.startswith("DRY_RUN: would restart"), out
    # A different service is a different action.
    assert doctor.action_restart_service("cerberus", "zeroclaw-fanout.service").startswith("REFUSED")


def test_a_restart_outside_the_allowlist_never_shells_out(monkeypatch, doctor_state):
    def boom(*args, **kwargs):
        raise AssertionError("subprocess must not run for a host the table refuses")

    monkeypatch.setattr(doctor.subprocess, "run", boom)
    out = doctor.action_restart_service("nowhere", "zeroclaw-doctor.service")
    assert "not a restart target" in out, out
    out = doctor.action_restart_service("argos", "zeroclaw-doctor.service")
    assert "blocklist" in out, out


def test_the_service_allowlist_still_applies(monkeypatch, doctor_state):
    out = doctor.action_restart_service("cerberus", "nginx.service")
    assert "not in allowlist" in out, out


# ── 4. durable safety state ──────────────────────────────────────────────────


def test_loop_history_survives_a_restart(store, tmp_path):
    state, clock = store
    for _ in range(3):
        state.touch("loop:j1", ttl=COUNTER_TTLS["loop"], at=clock["now"])
    clock["now"] += 60
    assert len(state.touch("loop:j1", ttl=COUNTER_TTLS["loop"], at=clock["now"])) == 4

    # A NEW process: a new store object over the same file. The count is still 4,
    # which is what stops a restart handing a looping job a clean sheet.
    reborn = DurableStateStore(
        FileBackend(str(tmp_path / "doctor-state.json")), refresh_secs=0.0, time_fn=lambda: clock["now"]
    )
    assert len(reborn.touch("loop:j1", ttl=COUNTER_TTLS["loop"], at=clock["now"])) == 5


def test_a_fresh_store_does_not_erase_what_the_last_one_wrote(tmp_path):
    """The write path has to READ before it writes.

    A store that saves its cache without loading first holds an empty cache on a
    fresh process, so its first `mark` would persist `{}` and destroy every limit
    the previous process recorded — while looking perfectly durable. This is the
    same defect the module exists to remove, relocated from the read side to the
    write side, so it is pinned directly.
    """
    path = str(tmp_path / "s.json")
    clock = {"now": 500_000.0}
    first = DurableStateStore(FileBackend(path), refresh_secs=0.0, time_fn=lambda: clock["now"])
    first.mark("cooldown:zeroclaw", ttl=3600.0, at=clock["now"])
    first.mark("dlq:dead-1", ttl=21600.0, at=clock["now"])

    reborn = DurableStateStore(FileBackend(path), refresh_secs=0.0, time_fn=lambda: clock["now"])
    reborn.mark("ssh:cerberus", ttl=3600.0, at=clock["now"])  # the FIRST write after restart

    reread = DurableStateStore(FileBackend(path), refresh_secs=0.0, time_fn=lambda: clock["now"])
    assert reread.seen("cooldown:zeroclaw", ttl=3600.0), "the cooldown survived the restart"
    assert reread.seen("dlq:dead-1", ttl=21600.0), "the DLQ-seen entry survived the restart"
    assert reread.seen("ssh:cerberus", ttl=3600.0), "and the new record landed"


def test_loop_history_expires_outside_its_window(store):
    state, clock = store
    state.touch("loop:j1", ttl=COUNTER_TTLS["loop"], at=clock["now"])
    clock["now"] += COUNTER_TTLS["loop"] + 1
    assert state.touch("loop:j1", ttl=COUNTER_TTLS["loop"], at=clock["now"]) == [clock["now"]]


def test_the_ssh_rate_limiter_survives_a_restart(monkeypatch, tmp_path):
    """The base commit's deque was empty after a restart, so the first restart
    action always looked like the first one."""
    path = str(tmp_path / "state.json")
    clock = {"now": 3_000_000.0}
    first = DurableStateStore(FileBackend(path), refresh_secs=0.0, time_fn=lambda: clock["now"])
    monkeypatch.setattr(doctor, "STATE", first)
    monkeypatch.setattr(doctor, "MAX_SSH_PER_HOUR", 3)
    monkeypatch.setattr(doctor, "_ssh_actions_log", type(doctor._ssh_actions_log)())
    monkeypatch.setattr(doctor, "_host_last_action", {})
    monkeypatch.setattr(doctor, "DRY_RUN", True)
    monkeypatch.setattr(doctor, "MIN_HOST_COOLDOWN", 0.0)
    monkeypatch.setattr(doctor, "APPROVALS", doctor_gates.ApprovalGate(first, gate_enabled=True))

    digest = doctor_gates.action_digest("restart_service", host="cerberus", service="zeroclaw-doctor.service")
    doctor.APPROVALS.record_approval("restart_service", digest)
    doctor.action_restart_service("cerberus", "zeroclaw-doctor.service")
    assert doctor.ssh_actions_total(doctor.SSH_RATE_WINDOW_SEC) == 1

    reborn = DurableStateStore(FileBackend(path), refresh_secs=0.0, time_fn=lambda: clock["now"])
    assert reborn.timestamps(f"{doctor.KEY_SSH}:all", ttl=doctor.SSH_RATE_WINDOW_SEC) != []
    assert len(reborn.timestamps(f"{doctor.KEY_SSH}:all", ttl=doctor.SSH_RATE_WINDOW_SEC)) == 1


def test_the_fleet_rate_cap_counts_across_hosts(store):
    state, clock = store
    for host in ("cerberus", "hydra", "proteus"):
        state.mark(f"ssh:{host}", ttl=3600.0, at=clock["now"])
        state.touch("ssh:all", ttl=3600.0, at=clock["now"])
    assert len(state.timestamps("ssh:all", ttl=3600.0)) == 3
    clock["now"] += 3601
    assert state.timestamps("ssh:all", ttl=3600.0) == []


def test_a_backend_outage_degrades_durability_not_the_limit(store):
    """The cache keeps serving the limit when the backend stops answering."""
    state, clock = store

    class BrokenBackend(FileBackend):
        def load(self, namespace):
            raise OSError("bus is down")

        def save(self, namespace, records):
            raise OSError("bus is down")

    state.backend = BrokenBackend(state.backend.path)
    # `touch` is the counter shape: two attempts inside the window is the loop
    # detector's evidence, and it must still read as 2 with the backend dead.
    state.touch("loop:j1", ttl=1800.0, at=clock["now"])
    state.touch("loop:j1", ttl=1800.0, at=clock["now"])
    # `refresh_secs=0.0` means every read re-loads, so this read IS the reload —
    # and a reload that fails must not empty a cache that holds the limit.
    assert len(state.timestamps("loop:j1", ttl=1800.0)) == 2, "the limit must not relax"

    # An EXPLICIT `refresh()` is the operator's "believe the backend from scratch",
    # so after it a dead backend legitimately yields nothing. Documented, not
    # accidental: the guarantee above is about the periodic reload, not this.
    state.refresh()
    assert state.timestamps("loop:j1", ttl=1800.0) == []


def test_a_corrupt_state_file_is_ignored_and_overwritten(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("{not json", encoding="utf-8")
    backend = FileBackend(str(path))
    assert backend.load("doctor.state") == {}
    assert backend.save("doctor.state", {"doctor.state/loop:j": {"timestamps": [1.0]}}) is True
    assert backend.load("doctor.state")["doctor.state/loop:j"]["timestamps"] == [1.0]


def test_the_backend_choice_is_explicit_and_the_memory_one_says_so():
    assert isinstance(state_store_from_env({"DOCTOR_STATE_BACKEND": "memory"}).backend, MemoryBackend)
    assert isinstance(
        state_store_from_env({"DOCTOR_STATE_BACKEND": "file", "DOCTOR_STATE_PATH": "/tmp/x"}).backend, FileBackend
    )
    # `auto` prefers a path over a bus URL, and a durable option over memory.
    assert isinstance(state_store_from_env({"DOCTOR_STATE_PATH": "/tmp/x"}).backend, FileBackend)
    with pytest.raises(ValueError):
        state_store_from_env({"DOCTOR_STATE_BACKEND": "postgres"})


def test_every_durable_counter_has_a_ttl():
    """If a counter has no window, a restart cannot ever clear it and the state
    grows forever. Listing them makes an un-timed counter a failing test."""
    assert set(COUNTER_TTLS) == {
        "loop",
        "cooldown",
        "dlq",
        "ssh",
        "host_cooldown",
        "token_fix",
        "failures",
    }
    assert all(ttl > 0 for ttl in COUNTER_TTLS.values())


def test_the_doctor_writes_through_the_store(store, monkeypatch, tmp_path):
    """The doctor's own helpers land in the store, not only in a module dict."""
    state, clock = store
    monkeypatch.setattr(doctor, "STATE", state)
    monkeypatch.setattr(doctor, "_seen_job_attempts", {})
    monkeypatch.setattr(doctor, "_recent_auto_triage", {})

    doctor.loop_attempts("j9", window=doctor.LOOP_WINDOW_SEC, at=clock["now"])
    doctor.note_triage("zeroclaw", ttl=doctor.AUTO_TRIAGE_COOLDOWN, at=clock["now"])
    doctor.record_ssh_action("cerberus", at=clock["now"])
    assert doctor.cooldown_active(f"{doctor.KEY_COOLDOWN}:zeroclaw", doctor.AUTO_TRIAGE_COOLDOWN)
    assert doctor.ssh_actions_total(doctor.SSH_RATE_WINDOW_SEC) == 1
    assert doctor.dlq_marked("dead-1", ttl=doctor.DLQ_SEEN_TTL, at=clock["now"]) is False
    assert doctor.dlq_marked("dead-1", ttl=doctor.DLQ_SEEN_TTL, at=clock["now"]) is True

    reborn = DurableStateStore(
        FileBackend(str(tmp_path / "doctor-state.json")), refresh_secs=0.0, time_fn=lambda: clock["now"]
    )
    assert len(reborn.timestamps(f"{doctor.KEY_LOOP}:j9", ttl=1800.0)) == 1
    assert reborn.seen(f"{doctor.KEY_DLQ}:dead-1", ttl=21600.0)


def test_the_approval_record_is_json_and_carries_no_secret(store):
    """A state file is world-readable by default; nothing secret may land in it."""
    state, clock = store
    state.mark("approval:abc", ttl=900.0, at=clock["now"])
    state.set_value("failures:repo", {"reason": "GLTOK is not set"}, ttl=86400.0)
    blob = json.dumps(state._cache, sort_keys=True)  # noqa: SLF001 - the file is the subject
    assert "GLTOK" in blob, "a recorded failure may name a missing VARIABLE"
    for forbidden in ("Bearer", "password", "PRIVATE-TOKEN", "hunter2"):
        assert forbidden not in blob


def test_cooldown_and_dlq_helpers_use_the_documented_windows(store, monkeypatch):
    state, clock = store
    monkeypatch.setattr(doctor, "STATE", state)
    clock["now"] += COUNTER_TTLS["cooldown"] + 1
    doctor.note_triage("zeroclaw", at=clock["now"] - COUNTER_TTLS["cooldown"] - 1)
    assert not doctor.cooldown_active(f"{doctor.KEY_COOLDOWN}:zeroclaw", COUNTER_TTLS["cooldown"])


def test_state_reflects_the_time_the_module_last_read_it(store):
    """A test that does not freeze the clock proves nothing about a window."""
    state, clock = store
    assert state._time() == clock["now"]  # noqa: SLF001
    assert time.time() > clock["now"], "the fixture's clock is frozen on purpose"


# ── 5. review round 1: findings verified against the source ─────────────────


def test_a_purge_that_confirmed_a_count_but_named_no_ids_is_not_reported_as_a_total_failure():
    """`matched_ids` absent + an integer `deleted` used to yield
    `skipped == requested`, so a successful purge read as having deleted nothing.

    The count is what the route CONFIRMED, so it is the removal count; the shortfall
    against what was asked for is the partial.
    """
    outcome = doctor_gates.purge_result("cancel_failed", {"deleted": 1}, ["a", "b", "c"])
    assert outcome["removed"] == 1
    assert outcome["partial"] is True
    assert sorted(outcome["skipped"]) == ["b", "c"], outcome
    # And a complete purge that simply does not echo the ids is not partial.
    full = doctor_gates.purge_result("cancel_failed", {"deleted": 2}, ["a", "b"])
    assert full["removed"] == 2
    assert full["partial"] is False
    assert full["skipped"] == []


def test_a_purge_naming_its_ids_reports_the_unnamed_ones_as_skipped():
    outcome = doctor_gates.purge_result("cancel_failed", {"deleted": 1, "matched_ids": ["a"]}, ["a", "b"])
    assert outcome["removed"] == 1
    assert outcome["skipped"] == ["b"]


def test_a_restart_target_with_no_recorded_user_is_refused_by_name(monkeypatch):
    """`entry["user"]` would KeyError. Now the refusal names what to record."""
    monkeypatch.setitem(doctor_gates.RESTART_TARGETS, "ghost", {"address": "10.0.0.1"})
    target, reason = doctor_gates.resolve_restart_target("ghost", env={})
    assert target is None
    assert "no user is recorded" in reason
    target, reason = doctor_gates.resolve_restart_target("ghost", env={"GHOST_USER": "g"})
    assert target is None, "no user_env key means the env read does not apply"
    assert "no user is recorded" in reason


def test_the_cixmini_sudo_user_is_read_at_resolve_time():
    """Not at import: a later config change must not need a restart to take."""
    env = {"CIXMINI_IP": "192.0.2.99", "CIXMINI_SSH_PASSWORD": "x"}
    assert doctor_gates.resolve_restart_target("cixmini", env=env)[0]["user"] == "mini"
    env["DOCTOR_CIXMINI_SUDO_USER"] = "operator"
    assert doctor_gates.resolve_restart_target("cixmini", env=env)[0]["user"] == "operator"


def test_disabling_the_gate_allows_the_action_rather_than_refusing_it():
    """`approved()` used to answer for a disabled gate, so `False` read as a
    refusal when the rail was simply off.

    `approved()` now answers only "is there an approval"; the switch lives in
    `require()`. Both readings are pinned, because the failure mode is an operator
    setting DOCTOR_GATE_DESTRUCTIVE=0 to turn the rail OFF and getting the reverse.
    """
    state = DurableStateStore(MemoryBackend())
    off = doctor_gates.ApprovalGate(state, gate_enabled=False, auto_approve=False)
    for kind in sorted(doctor_gates.DESTRUCTIVE_ACTIONS):
        assert off.require(kind, x=1) is None, f"{kind} with the gate off must be allowed"
    digest = doctor_gates.action_digest("cancel_jobs", job_ids=["a"])
    assert off.approved("cancel_jobs", digest) is False, "no approval was recorded"
    assert off.require("cancel_jobs", job_ids=["a"]) is None, "and the gate is off"

    on = doctor_gates.ApprovalGate(state, gate_enabled=True, auto_approve=False)
    assert on.require("cancel_jobs", job_ids=["a"]) is not None
    on.record_approval("cancel_jobs", digest)
    assert on.approved("cancel_jobs", digest) is True
    assert on.require("cancel_jobs", job_ids=["a"]) is None


def test_a_safety_limit_that_cannot_be_persisted_fails_the_ssh_rate_limiter_closed(monkeypatch, doctor_state):
    """A failed write leaves the cache ahead of the backend, so a restart would
    hand out a fresh SSH budget.

    The cache is still newer (an in-process limit is never looser), but the limiter
    refuses outright rather than pretend the record exists.
    """
    state, _clock = doctor_state
    monkeypatch.setattr(doctor, "MIN_HOST_COOLDOWN", 0.0)
    assert doctor._ssh_rate_ok("cerberus") is True, "a healthy store allows one"

    state.persist_ok = False
    assert doctor._ssh_rate_ok("cerberus") is False, "an unpersistable limiter allows nothing"
    assert doctor._ssh_rate_ok("hydra") is False, "on any host"

    state.persist_ok = True
    assert doctor._ssh_rate_ok("cerberus") is True, "and recovers when the store writes again"


def test_the_store_reports_whether_its_last_write_landed(store):
    state, clock = store
    assert state.persist_ok is True
    state.touch("loop:j", ttl=1800.0, at=clock["now"])

    class RefusingBackend:
        name = "file"

        def load(self, namespace):
            return {}

        def save(self, namespace, records):
            return False

    state.backend = RefusingBackend()
    state.touch("loop:j", ttl=1800.0, at=clock["now"])
    assert state.persist_ok is False, "a backend that says no must be believed"
    # The cache still holds the newer value: the limit is not loosened in-process.
    assert len(state.timestamps("loop:j", ttl=1800.0)) == 2


def test_touch_returns_oldest_first_even_for_a_backdated_stamp(store):
    """`touch` sorted before appending, so a caller-supplied `at` in the past
    returned an unsorted list against an oldest-first contract."""
    state, clock = store
    clock["now"] = 10_000.0
    state.touch("loop:j", ttl=1800.0, at=9_000.0)
    stamps = state.touch("loop:j", ttl=1800.0, at=9_500.0)
    assert stamps == sorted(stamps), stamps
    assert stamps == [9_000.0, 9_500.0]
