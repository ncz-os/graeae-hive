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
import subprocess
import time

import pytest

from hive.workers import doctor, doctor_gates
from hive.workers import merge_train as mt
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


def git(*args, cwd):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True)


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
    assert doctor.dlq_decided("dead-1", ttl=doctor.DLQ_SEEN_TTL) is False
    doctor.dlq_record_decision("dead-1", ttl=doctor.DLQ_SEEN_TTL, at=clock["now"])
    assert doctor.dlq_decided("dead-1", ttl=doctor.DLQ_SEEN_TTL) is True

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


def test_a_purge_result_never_iterates_a_string():
    """A route that answered with the string "ok" would otherwise yield
    ["o", "k"] and report two deleted jobs that do not exist."""
    outcome = doctor_gates.purge_result("dlq_remove", {"ok": "ok", "not_found": "yes"}, ["j"])
    assert outcome["removed"] == 0
    assert outcome["not_found"] == []
    # A malformed id list is fail-closed: nothing is read as confirmed removed.
    bulk = doctor_gates.purge_result("cancel_failed", {"matched_ids": "a"}, ["a", "b"])
    assert bulk["removed"] == 0
    assert sorted(bulk["skipped"]) == ["a", "b"], bulk


def test_an_integer_count_on_the_dlq_route_is_honoured():
    """`ok` is a list today; a bare count is still a confirmation."""
    outcome = doctor_gates.purge_result("dlq_remove", {"ok": 1}, ["a"])
    assert outcome["removed"] == 1
    assert outcome["partial"] is False


def test_an_empty_counter_table_does_not_crash_the_write_path(store, monkeypatch):
    """`_ttl_for` runs inside the write path; `max()` over an empty table raises."""
    import hive.workers.doctor_state as state_module

    state, clock = store
    monkeypatch.setattr(state_module, "COUNTER_TTLS", {})
    # No prefix matches and the table is empty: the record must still be written.
    state.mark("unlisted-prefix", ttl=60.0, at=clock["now"])
    assert state.last_mark("unlisted-prefix") == clock["now"]


def test_a_short_window_write_cannot_truncate_a_keys_canonical_history(store):
    """A second caller touching the same key with a shorter window must not erase
    the first caller's evidence: pruning never shortens."""
    state, clock = store
    for _ in range(3):
        state.touch("loop:j", ttl=COUNTER_TTLS["loop"], at=clock["now"])
    assert len(state.timestamps("loop:j", ttl=COUNTER_TTLS["loop"])) == 3
    # A short-window write: the record must still carry the three.
    state.touch("loop:j", ttl=5.0, at=clock["now"])
    assert len(state.timestamps("loop:j", ttl=COUNTER_TTLS["loop"])) == 4


def test_an_unlisted_key_prefix_is_pruned_rather_than_growing_forever(store):
    """Infinity for an unrecognised prefix makes a typo'd prefix a silent leak.

    Two different windows are in play and the test names both: `COUNTER_TTLS` (and
    its default) governs how long a record SURVIVES a pruning write, while
    `seen`/`timestamps` apply the caller's own window at read time. The bounded
    one is the pruning window, so that is what is asserted here.
    """
    import hive.workers.doctor_state as state_module

    state, clock = store
    state.mark("typo-prefx:j", ttl=60.0, at=clock["now"])
    assert state.last_mark("typo-prefx:j") == clock["now"]

    assert state_module._prune_ttl_of("typo-prefx:j", 1.0) == state_module.DEFAULT_TTL_SECS
    assert state_module._prune_ttl_of("loop:j", 9999.0) == 9999.0, "pruning never shortens"

    # Inside the default window a write keeps it; past it, the next write prunes it.
    state.mark("loop:other", ttl=10.0, at=clock["now"])
    assert state.last_mark("typo-prefx:j") == clock["now"]
    clock["now"] += state_module.DEFAULT_TTL_SECS + 1
    state.mark("loop:other", ttl=10.0, at=clock["now"])
    assert state.last_mark("typo-prefx:j") is None, "an unlisted prefix must still be pruned"


def test_an_attributed_skip_is_flagged_as_attributed_and_not_printed_as_observed():
    """The bus confirmed a count and named no ids, so which ids it kept is not
    knowable — and the doctor's summary must say so rather than list them."""
    outcome = doctor_gates.purge_result("cancel_failed", {"deleted": 1}, ["a", "b", "c"])
    assert outcome["attributed"] is True
    assert outcome["partial"] is True
    exact = doctor_gates.purge_result("cancel_failed", {"deleted": 1, "matched_ids": ["a"]}, ["a", "b"])
    assert exact["attributed"] is False


def test_a_job_cut_off_by_the_per_cycle_cap_is_not_marked_decided(monkeypatch, doctor_state):
    """REGRESSION FIX: marking durably at LOOK time meant a job the per-cycle cap
    never reached was marked decided and then never remediated for the whole seen
    window — the base commit marked at the END of the loop, which is the order
    that matters."""
    jobs = [{"id": f"dead-{n}", "kind": "zeroclaw", "result": {"error": "timeout"}} for n in range(5)]
    monkeypatch.setattr(doctor, "fetch_dead_letter_jobs", lambda: list(jobs))
    monkeypatch.setattr(doctor, "DLQ_APPLY", False)
    monkeypatch.setattr(doctor, "DLQ_MAX_PER_CYCLE", 2)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)

    first = doctor.handle_dead_letter_queue()
    assert first["resubmit"] == 2, first
    # `deferred` is the number of jobs this cycle did not reach, so the backlog
    # behind the cap is visible in one number rather than implied.
    assert first.get("deferred") == 3, first
    for job in jobs[2:]:
        assert not doctor.dlq_decided(job["id"]), f"{job['id']} was never decided"

    # The next cycle picks the undecided ones up rather than skipping them, and the
    # job the cap kept cutting off is still undecided — which is the property that
    # matters: it is never silently lost, and it is never marked decided either.
    second = doctor.handle_dead_letter_queue()
    assert second["resubmit"] == 2, second
    assert second.get("deferred") == 1, second
    assert not doctor.dlq_decided("dead-4"), "still undecided after two capped cycles"
    third = doctor.handle_dead_letter_queue()
    assert third["resubmit"] == 1, third
    assert doctor.dlq_decided("dead-4"), "and decided once a cycle reaches it"


def test_a_protected_job_is_reported_every_cycle_and_never_remediated(monkeypatch, doctor_state):
    """A protected lane is REPORTED, never remediated — so it must not be recorded
    as decided, or the owner stops hearing about it after one cycle."""
    monkeypatch.setattr(
        doctor,
        "fetch_dead_letter_jobs",
        lambda: [{"id": "dead-p", "kind": "riskyeats:x", "project": "riskyeats", "result": {}}],
    )
    monkeypatch.setattr(doctor, "DLQ_PROTECT_PROJECTS", frozenset({"riskyeats"}))
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    for cycle in range(3):
        summary = doctor.handle_dead_letter_queue()
        assert summary.get("protected") == 1, f"cycle {cycle}: {summary}"
        assert summary["resubmit"] == 0 and summary["remove"] == 0
        assert not doctor.dlq_decided("dead-p"), "a protected job is never 'decided'"
    assert summary["skipped_seen"] == 0


def test_an_approval_refusal_does_not_spend_the_cycle_budget(monkeypatch, doctor_state):
    """Twenty unapproved purges must not crowd out every job behind them."""
    jobs = [
        {"id": f"dead-{n}", "kind": "zeroclaw", "result": {"last_decline_reason": "no_repo_mapping"}} for n in range(5)
    ]
    monkeypatch.setattr(doctor, "fetch_dead_letter_jobs", lambda: list(jobs))
    monkeypatch.setattr(doctor, "DLQ_APPLY", True)
    monkeypatch.setattr(doctor, "DLQ_MAX_PER_CYCLE", 2)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    monkeypatch.setattr(
        doctor, "_http", lambda *a, **k: (200, {"ok": [], "not_found": [], "skipped_not_dead_letter": []})
    )

    summary = doctor.handle_dead_letter_queue()
    assert summary["remove"] == 5, f"refusals must not spend the cap: {summary}"
    assert summary.get("deferred") in (None, 0), summary


def test_a_refusal_from_the_approval_gate_is_not_recorded_as_decided(monkeypatch, doctor_state):
    """The doctor wanted to act and was not allowed to. That is not a decision: the
    operator's approval is a future event, so recording it would hide the job from
    every later cycle for the whole seen window — and the operator would approve
    something that never comes back round."""
    monkeypatch.setattr(
        doctor,
        "fetch_dead_letter_jobs",
        lambda: [{"id": "dead-r", "kind": "zeroclaw", "result": {"last_decline_reason": "no_repo_mapping"}}],
    )
    monkeypatch.setattr(doctor, "DLQ_APPLY", True)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    calls = []
    monkeypatch.setattr(
        doctor,
        "_http",
        lambda *a, **k: calls.append(a) or (200, {"ok": [], "not_found": [], "skipped_not_dead_letter": []}),
    )
    summary = doctor.handle_dead_letter_queue()
    assert summary["remove"] == 1, summary
    assert calls == [], "a refusal must not reach the bus"
    assert not doctor.dlq_decided("dead-r"), "a refusal is not a decision"

    # Once the approval exists, the next cycle reaches the bus and records.
    digest = doctor_gates.action_digest("dlq_remove", job_ids=["dead-r"])
    doctor.APPROVALS.record_approval("dlq_remove", digest)
    calls.clear()
    doctor.handle_dead_letter_queue()
    assert len(calls) == 1, calls
    assert doctor.dlq_decided("dead-r"), "and the acted decision IS recorded"


def test_a_dry_run_is_a_decision_and_is_recorded(monkeypatch, doctor_state):
    """DLQ_APPLY=0 is the configured behaviour, not a refusal — recording it is what
    stops every cycle re-deciding the same dead-letter jobs."""
    monkeypatch.setattr(
        doctor,
        "fetch_dead_letter_jobs",
        lambda: [{"id": "dead-d", "kind": "zeroclaw", "result": {"error": "timeout"}}],
    )
    monkeypatch.setattr(doctor, "DLQ_APPLY", False)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    summary = doctor.handle_dead_letter_queue()
    assert summary["resubmit"] == 1, summary
    assert doctor.dlq_decided("dead-d")
    again = doctor.handle_dead_letter_queue()
    assert again["skipped_seen"] == 1, again


def test_a_revoked_approval_does_not_come_back_from_the_journal(gate, tmp_path):
    """A bus backend is an append-only journal: dropping the key from the cache
    leaves the record already in the journal, and the next replay resurrects the
    approval somebody just revoked. `forget` writes a tombstone instead."""
    state = gate.store
    digest = doctor_gates.action_digest("cancel_jobs", job_ids=["a"])
    gate.record_approval("cancel_jobs", digest)
    assert gate.approved("cancel_jobs", digest)

    gate.revoke(digest)
    assert not gate.approved("cancel_jobs", digest), "revoked immediately"
    assert state._cache[f"approval:{digest}"].get("tombstone") is True

    # A rebuild from the durable source — a restart — must agree.
    reborn = DurableStateStore(type(state.backend)(state.backend.path), refresh_secs=0.0, time_fn=state._time)
    assert not reborn.seen(f"approval:{digest}", ttl=900.0), "a restart must not resurrect it"

    # And a later approval supersedes the retraction.
    gate.record_approval("cancel_jobs", digest)
    assert gate.approved("cancel_jobs", digest)


def test_a_tombstone_eventually_expires(store):
    """Otherwise a retracted key is a permanent record."""
    state, clock = store
    state.set_value("loop:j", {"x": 1}, ttl=1800.0)
    state.forget("loop:j")
    assert state.value("loop:j", "gone") == "gone"
    assert "loop:j" in state.keys(""), "the tombstone is a record, not an omission"
    clock["now"] += 86_401
    state.touch("loop:other", ttl=10.0, at=clock["now"])
    assert "loop:j" not in state.keys(""), "and it ages out like any other"


def test_a_truncated_bus_read_is_not_treated_as_a_smaller_state(tmp_path):
    """A bounded read of a longer journal drops the OLDEST records, which are the
    ones a safety window is made of. A truncated load must be treated as no answer
    rather than as a smaller state."""
    clock = {"now": 7_000_000.0}
    limit = 4

    class Journal:
        """An append-only log that outgrows one read."""

        name = "bus"

        def __init__(self):
            self.entries = []

        def load(self, namespace):
            window = self.entries[-limit:]
            self.truncated = len(self.entries) >= limit
            newest = {}
            for key, value in window:
                newest[key] = value
            return newest

        def save(self, namespace, records):
            for key, value in records.items():
                self.entries.append((key, value))
            return True

    journal = Journal()
    state = DurableStateStore(journal, refresh_secs=0.0, time_fn=lambda: clock["now"])
    state.mark("loop:j1", ttl=1800.0, at=clock["now"])
    for n in range(limit):
        state.mark(f"filler:{n}", ttl=1800.0, at=clock["now"])
    # `loop:j1` is now older than one read's window.
    assert journal.entries[0][0] == "loop:j1"

    # `refresh_secs=0.0` means this read IS a reload — the periodic kind, not the
    # explicit `refresh()` the operator asks for when they want the backend's answer
    # whatever it is. A truncated reload must keep the live state it already holds.
    assert state.last_mark("loop:j1") is not None, "a truncated read must not drop live state"
    assert journal.truncated is True

    # The explicit `refresh()` is the escape hatch and it DOES believe the backend,
    # so after it the truncated answer is all there is. Documented, not accidental:
    # the guarantee above is about the periodic reload, not this.
    state.refresh()
    assert state.last_mark("loop:j1") is None


def test_a_password_auth_restart_is_refused_when_the_password_is_absent(monkeypatch, doctor_state):
    """Raised four times by review; asserted here rather than argued.

    `resolve_restart_target` refuses when the password variable is unset or EMPTY
    (`not (env.get(...) or "")` is true for `""`), and it runs before the subprocess
    is built — so the second read of the variable can only see the same value.
    """
    monkeypatch.setenv("CIXMINI_IP", "192.0.2.99")
    monkeypatch.delenv("CIXMINI_SSH_PASSWORD", raising=False)
    target, reason = doctor_gates.resolve_restart_target("cixmini")
    assert target is None
    assert "CIXMINI_SSH_PASSWORD" in reason

    # Empty is as absent as unset.
    monkeypatch.setenv("CIXMINI_SSH_PASSWORD", "")
    assert doctor_gates.resolve_restart_target("cixmini")[0] is None

    # And through the executor, with the gate satisfied so nothing else refuses.
    monkeypatch.setenv("CIXMINI_SSH_PASSWORD", "")
    monkeypatch.setattr(doctor, "DRY_RUN", False)
    digest = doctor_gates.action_digest("restart_service", host="cixmini", service="zeroclaw-doctor.service")
    doctor.APPROVALS.record_approval("restart_service", digest)

    def boom(*args, **kwargs):
        raise AssertionError("no subprocess may be built without a password")

    monkeypatch.setattr(doctor.subprocess, "run", boom)
    out = doctor.action_restart_service("cixmini", "zeroclaw-doctor.service")
    assert "CIXMINI_SSH_PASSWORD" in out, out


def test_each_cancel_test_starts_with_no_approval(tmp_path):
    """The approval now lives in a DURABLE store, so a test that does not clear it
    could inherit one recorded by the test before it. The conftest fixture swaps in a
    fresh store per test; this proves the swap happened rather than assuming it."""
    from hive.workers.doctor_state import MemoryBackend

    assert isinstance(doctor.STATE.backend, MemoryBackend), type(doctor.STATE.backend)
    assert doctor.STATE.keys("") == [], doctor.STATE.keys("")


def test_a_failed_first_load_cannot_overwrite_the_durable_records(tmp_path):
    """A cache that has never been loaded holds only this process's writes, so
    persisting it over a backend that was merely unreachable would delete every
    record the previous process left. Nothing is written until a load succeeds."""
    path = str(tmp_path / "s.json")
    clock = {"now": 4_000_000.0}
    good = DurableStateStore(FileBackend(path), refresh_secs=0.0, time_fn=lambda: clock["now"])
    good.mark("cooldown:zeroclaw", ttl=3600.0, at=clock["now"])
    good.mark("dlq:dead-1", ttl=21600.0, at=clock["now"])

    class Unreachable(FileBackend):
        def load(self, namespace):
            raise OSError("bus is down")

    fresh = DurableStateStore(Unreachable(path), refresh_secs=0.0, time_fn=lambda: clock["now"])
    fresh.touch("loop:j", ttl=1800.0, at=clock["now"])
    assert fresh.persist_ok is False, "an unloaded store must not claim it wrote"
    assert len(fresh.timestamps("loop:j", ttl=1800.0)) == 1, "the in-process limit still holds"

    # The durable records are untouched.
    reread = DurableStateStore(FileBackend(path), refresh_secs=0.0, time_fn=lambda: clock["now"])
    assert reread.seen("cooldown:zeroclaw", ttl=3600.0)
    assert reread.seen("dlq:dead-1", ttl=21600.0)
    assert not reread.seen("loop:j", ttl=1800.0), "the unwritten record is absent, as it should be"

    # And once the backend answers, writing resumes.
    healed = DurableStateStore(FileBackend(path), refresh_secs=0.0, time_fn=lambda: clock["now"])
    healed.touch("loop:j", ttl=1800.0, at=clock["now"])
    assert healed.persist_ok is True
    again = DurableStateStore(FileBackend(path), refresh_secs=0.0, time_fn=lambda: clock["now"])
    assert again.seen("cooldown:zeroclaw", ttl=3600.0), "the existing records survived"
    assert again.seen("loop:j", ttl=1800.0), "and the new one landed"


# ── 7. review round 2: findings verified against the source ───────────────────
#
# Every test below names a deepseek-flash finding from the round-2 review and
# either disproves it (the disproof is asserted, not argued) or pins the fix.


def test_a_refusal_storm_bounds_the_work_one_cycle_does(monkeypatch, doctor_state):
    """`DLQ_MAX_PER_CYCLE` bounds ACTIONS; nothing bounded the WORK.

    A DLQ whose remediation is refused for want of an approval never advances
    `acted`, so the cap was never reached and one cycle classified, attempted and
    broadcast every job in the queue — a full pass per cycle over a queue that
    cannot drain. The attempt counter bounds the pass, and it is above the action
    budget so the action budget still binds first whenever the doctor is making
    progress (and so refusals still do not crowd out the jobs behind them).
    """
    jobs = [
        {"id": f"dead-{n:03d}", "kind": "zeroclaw", "result": {"last_decline_reason": "no_repo_mapping"}}
        for n in range(180)
    ]
    monkeypatch.setattr(doctor, "fetch_dead_letter_jobs", lambda: list(jobs))
    monkeypatch.setattr(doctor, "DLQ_APPLY", True)
    monkeypatch.setattr(doctor, "DLQ_MAX_PER_CYCLE", 2)
    monkeypatch.setattr(doctor, "DLQ_MAX_ATTEMPTS_PER_CYCLE", 5)
    monkeypatch.setattr(doctor, "broadcast_doctor_message", lambda *a, **k: None)
    monkeypatch.setattr(doctor, "_http", lambda *a, **k: (200, {"ok": []}))
    attempted: list[str] = []
    monkeypatch.setattr(
        doctor,
        "classify_dead_letter",
        lambda job: attempted.append(job["id"]) or ("remove", "no repo mapping"),
    )

    summary = doctor.handle_dead_letter_queue()

    assert len(attempted) == 5, f"one cycle must not attempt all {len(jobs)} jobs: {len(attempted)}"
    assert summary.get("deferred") == len(jobs) - len(attempted), (
        f"deferred must be the backlog the cap left: {summary}"
    )
    # And a refusal still does not spend the ACTION budget: with the attempt cap
    # above it, the five refused jobs were all reached rather than the cap breaking
    # after the first two.
    assert summary["remove"] == 5, summary
    assert not any(doctor.dlq_decided(job["id"]) for job in jobs[:5]), "a refusal is still not a decision"


def test_the_attempt_budget_is_never_below_the_action_budget():
    """An attempt cap under the action cap would make the action budget
    unreachable, which is the same defect in the other direction."""
    assert doctor.DLQ_MAX_ATTEMPTS_PER_CYCLE >= doctor.DLQ_MAX_PER_CYCLE, (
        doctor.DLQ_MAX_ATTEMPTS_PER_CYCLE,
        doctor.DLQ_MAX_PER_CYCLE,
    )


def test_a_truncated_bus_read_is_not_believed_to_have_loaded(tmp_path):
    """The load itself is the defect, not just what it returns.

    `_ensure_loaded` set `_loaded_ok` before the truncation check, so a truncated
    read counted as a SUCCESSFUL load: `_loaded_at` was stamped (the store stopped
    retrying for the whole refresh window) and `_persist_locked` was permitted to
    write the partial cache over the journal — destroying exactly the records the
    bounded read dropped. A truncated read is a failed read: the cache is kept, the
    next read retries, and nothing is written until the backend answers whole.
    """

    class Journal:
        name = "bus"

        def __init__(self):
            self.entries = []
            self.truncated = False
            self.saves = 0

        def load(self, namespace):
            self.truncated = len(self.entries) >= 2
            newest: dict = {}
            for key, value in self.entries[-2:]:
                newest[key] = value
            return newest

        def save(self, namespace, records):
            self.saves += 1
            for key, value in records.items():
                self.entries.append((key, value))
            return True

    clock = {"now": 8_000_000.0}
    journal = Journal()
    state = DurableStateStore(journal, refresh_secs=0.0, time_fn=lambda: clock["now"])

    state.mark("loop:a", ttl=1800.0, at=clock["now"])
    state.mark("loop:b", ttl=1800.0, at=clock["now"])
    saves_before = journal.saves

    # Every read from here on is truncated, so every write must be refused: the
    # cache holds three records and the read can only ever show two of them.
    state.mark("loop:c", ttl=1800.0, at=clock["now"])
    assert journal.truncated is True, "the journal is longer than one read"
    assert state.persist_ok is False, "a truncated read must not be believed: it would be written back"
    assert journal.saves == saves_before, "and nothing may be written over the journal"

    # The in-process limits still hold — that is the point of keeping the cache.
    assert state.last_mark("loop:c") == clock["now"]
    assert state.last_mark("loop:a") == clock["now"]


def test_the_bus_backend_posts_only_what_changed(monkeypatch):
    """`save` re-posted the whole cache on every write.

    The journal is append-only and the read is bounded, so re-publishing every
    record per write grows the journal by the size of the state per write until
    every read is truncated — which, with the truncation treated as a load, stops
    the store loading at all. `save` posts the records that changed and seeds
    itself from the records a complete `load` returned.
    """
    from hive.workers.doctor_state import BusBackend

    posted: list[dict] = []
    journal: list[dict] = [
        {"ts": 1.0, "payload": {"key": "loop:a", "value": {"timestamps": [1.0]}}},
        {"ts": 2.0, "payload": {"key": "loop:b", "value": {"timestamps": [2.0]}}},
        {"ts": 3.0, "payload": {"key": "loop:c", "value": {"timestamps": [3.0]}}},
    ]

    def http(method, path, body=None):
        if method == "GET":
            return 200, {"messages": list(journal)}
        posted.append(body["payload"])
        journal.append({"ts": float(len(journal)), "payload": body["payload"]})
        return 201, {}

    clock = {"now": 9_000_000.0}
    backend = BusBackend(http, limit=50)
    state = DurableStateStore(backend, namespace="doctor.safety", refresh_secs=0.0, time_fn=lambda: clock["now"])

    # The store holds three keys, so a save that re-posted the cache would publish
    # three records per write.
    state.mark("loop:a", ttl=1800.0, at=clock["now"])
    assert [entry["key"] for entry in posted] == ["loop:a"], posted

    state.mark("loop:b", ttl=1800.0, at=clock["now"])
    assert [entry["key"] for entry in posted] == ["loop:a", "loop:b"], posted

    state.mark("loop:c", ttl=1800.0, at=clock["now"])
    assert [entry["key"] for entry in posted] == ["loop:a", "loop:b", "loop:c"], posted
    assert state.persist_ok is True

    # A brand new backend over the same journal posts what it is given, because it
    # knows nothing about what is already there.
    fresh = BusBackend(http, limit=50)
    fresh.save("doctor.safety", {"loop:d": {"at": 9.0}})
    assert posted[-1]["key"] == "loop:d"


def test_one_malformed_bus_message_does_not_discard_the_whole_read():
    """`float(message["ts"])` raised on a non-numeric timestamp, and the exception
    was caught a frame up as "the backend failed" — so one bad message threw away
    every good record in the journal, which is the same failure as the truncation
    guard it sat next to."""
    from hive.workers.doctor_state import BusBackend

    def http(method, path, body=None):
        assert method == "GET"
        return 200, {
            "messages": [
                {"ts": "2026-10-22T00:00:00Z", "payload": {"key": "loop:a", "value": {"at": 1.0}}},
                {"ts": 2.0, "payload": {"key": "loop:b", "value": {"at": 2.0}}},
            ]
        }

    backend = BusBackend(http, limit=50)
    assert backend.load("doctor.safety") == {"loop:a": {"at": 1.0}, "loop:b": {"at": 2.0}}
    assert backend.truncated is False


def test_clearing_the_safety_state_is_one_write_not_one_per_key(store, monkeypatch):
    """`reset_safety_state` forgot every key in turn, which is a round-trip to the
    backend per key and, on a file backend, a full rewrite of the state file each
    time — for a store whose whole purpose is to hold a lot of keys."""
    state, clock = store
    for n in range(50):
        state.mark(f"loop:j{n}", ttl=1800.0, at=clock["now"])
    saves = {"n": 0}
    original = state.backend.save

    def counting_save(namespace, records):
        saves["n"] += 1
        return original(namespace, records)

    monkeypatch.setattr(state.backend, "save", counting_save)
    doctor.reset_safety_state(state)

    assert saves["n"] == 1, f"one clear is one write, not fifty: {saves['n']}"
    assert [key for key in state.keys("") if not state._cache[key].get("tombstone")] == [], state.keys("")

    # And a restart agrees: every key was retracted durably, not just in memory.
    reborn = DurableStateStore(FileBackend(state.backend.path), refresh_secs=0.0, time_fn=state._time)
    assert reborn.timestamps("loop:j0", ttl=1800.0) == []
    assert not reborn.seen("loop:j7", ttl=1800.0)


def test_the_ssh_mirror_is_trimmed_on_write_not_on_read(monkeypatch, doctor_state):
    """`_ssh_actions_log` is a write-only mirror, so trimming it inside the rate
    check was dead work on every check and made the mirror something the check
    mutates. It is trimmed where it grows instead — and it is still bounded."""
    _state, clock = doctor_state
    monkeypatch.setattr(doctor, "SSH_RATE_WINDOW_SEC", 3600.0)
    doctor.record_ssh_action("cerberus", at=clock["now"])
    assert len(doctor._ssh_actions_log) == 1

    # A read does not mutate the mirror.
    snapshot = list(doctor._ssh_actions_log)
    doctor._ssh_rate_ok("cerberus")
    assert list(doctor._ssh_actions_log) == snapshot, "a rate check must not prune the mirror"

    # An old entry ages out on the next write, so the mirror cannot grow forever.
    doctor.record_ssh_action("cerberus", at=clock["now"] + 3601.0)
    assert len(doctor._ssh_actions_log) == 1, doctor._ssh_actions_log
    doctor.record_ssh_action("cerberus", at=clock["now"] + 7100.0)
    assert len(doctor._ssh_actions_log) == 2, doctor._ssh_actions_log


def test_the_auto_triage_cooldown_reader_and_writer_agree(monkeypatch, doctor_state):
    """Raised four times by review, and disproved here rather than argued: the
    reader asks for `cooldown:<base kind>` and `note_triage(base_kind)` marks
    `cooldown:<kind>`. The point of the test is the SECOND scan — the durable
    cooldown is the only thing stopping every cycle re-submitting the same cluster.
    """
    _state, clock = doctor_state
    monkeypatch.setattr(doctor, "CLUSTER_THRESHOLD", 2)
    monkeypatch.setattr(doctor, "_active_triage_exists", lambda base_kind: False)
    # `ended_at` is compared against the WALL clock, so the jobs are recent in real
    # time; the store's clock (below) is what the cooldown is measured on.
    jobs = [{"id": f"j{n}", "kind": "zeroclaw:build", "ended_at": time.time()} for n in range(3)]
    submitted: list[str] = []
    monkeypatch.setattr(
        doctor,
        "_http",
        lambda method, path, *a, **k: (
            (200, {"jobs": list(jobs)}) if method == "GET" else submitted.append(path) or (201, {"id": "triage-1"})
        ),
    )

    clusters = doctor.scan_for_failure_clusters()
    assert [cluster["base_kind"] for cluster in clusters] == ["zeroclaw:build"], clusters
    for cluster in clusters:
        doctor.submit_auto_triage(cluster["base_kind"], cluster["count"], cluster["sample_job_id"])
    assert len(submitted) == 1, submitted

    # The next cycle inside the cooldown sees nothing to do.
    again = doctor.scan_for_failure_clusters()
    assert again == [], f"the durable cooldown must suppress the second cycle: {again}"
    assert len(submitted) == 1, f"and nothing may be submitted again: {submitted}"

    # Past the window it comes back — so the pin above is the cooldown, not a
    # permanently dead path.
    clock["now"] += doctor.AUTO_TRIAGE_COOLDOWN + 1
    assert [cluster["base_kind"] for cluster in doctor.scan_for_failure_clusters()] == ["zeroclaw:build"]


def test_a_journal_of_exactly_one_page_is_not_truncation(tmp_path):
    """A journal of exactly `limit` messages is COMPLETE, not a page of a longer
    one. Treating a full page as truncation refused to load from the exact moment
    the store became safe to read — and, with `_loaded_ok` false, wrote nothing."""
    from hive.workers.doctor_state import BusBackend

    journal = [{"ts": float(n), "payload": {"key": f"loop:j{n}", "value": {"at": float(n)}}} for n in range(4)]

    def http(method, path, body=None):
        assert method == "GET", method
        window = int(path.split("limit=")[1])
        return 200, {"messages": journal[:window]}

    backend = BusBackend(http, limit=4)
    loaded = backend.load("doctor.safety")
    assert backend.truncated is False, "exactly one page is the whole journal"
    assert set(loaded) == {"loop:j0", "loop:j1", "loop:j2", "loop:j3"}

    # One more message and it IS longer than one read.
    journal.append({"ts": 9.0, "payload": {"key": "loop:j4", "value": {"at": 9.0}}})
    backend.load("doctor.safety")
    assert backend.truncated is True


def test_a_push_that_names_the_canonical_remote_anywhere_is_still_refused():
    """`git push gitlab argonas:main` contains the canonical name and must not pass:
    the remote is the token AFTER `push`, and a whole-line membership test cannot
    tell the two apart."""
    with pytest.raises(ValueError, match="must name"):
        mt.assert_no_forced_or_public_writes(["git", "push", "gitlab", "argonas:main"])
    with pytest.raises(ValueError, match="must name"):
        mt.assert_no_forced_or_public_writes(["git", "push", "origin", "main"])
    mt.assert_no_forced_or_public_writes(["git", "push", "argonas", "abc:refs/heads/main"])


def test_an_unreadable_git_root_is_a_failed_cycle_not_an_empty_one(tmp_path, monkeypatch, capsys):
    """`discover_repos` returned `[]` for an unreadable root as well as for an empty
    one, so a cycle that looked at nothing printed nothing and exited zero."""
    missing = tmp_path / "not-there"
    with pytest.raises(mt.MeasurementError):
        mt.discover_repos(str(missing))
    outcomes = mt.run_cycle(git_root=str(missing))
    assert outcomes and outcomes[0].failed, outcomes
    assert "git root" in outcomes[0].failures[0], outcomes[0].failures


def test_purge_counts_beside_named_ids_do_not_overstate(tmp_path):
    """A route that names the ids it deleted is the exact truth; a count beside it
    that disagrees must not print "5 removed, 3 skipped" for one request."""
    names = ["a", "b"]
    outcome = doctor_gates.purge_result(
        "cancel_failed",
        {"deleted": 5, "matched_ids": names},
        names + ["c", "d", "e"],
    )
    assert outcome["removed"] == 2, outcome
    assert outcome["skipped"] == ["c", "d", "e"], outcome
    assert outcome["partial"] is True, outcome


# ── 8. review round 3: a reload must not forget what the outage refused ────────
#
# The one real finding round 2 left open, in `doctor_state.py:_ensure_loaded`:
# a write made while the backend was unreachable is refused by `_persist_locked`
# and therefore lives only in the cache, and the first load that SUCCEEDED then
# replaced the cache wholesale — dropping every mark the doctor had made during
# the outage, in a process that never restarted. The reload merges those writes
# back over what loaded; newest `at` wins per key and a tombstone merges like any
# other record, so the merge cannot hand a key back that another process has since
# retracted.


class FlakyBackend(FileBackend):
    """A durable backend that can be taken away for reads, or for writes, or both.

    The two halves are separate because a bus that answers a GET and drops a POST is
    the ordinary shape of a partial outage, and it loses durability exactly the way a
    dead backend does — from the cache's point of view the record is in one place
    only, and that is what the merge has to remember.
    """

    def __init__(self, path):
        super().__init__(path)
        self.readable = True
        self.writable = True

    def load(self, namespace):
        if not self.readable:
            raise OSError("bus is down")
        return super().load(namespace)

    def save(self, namespace, records):
        if not self.writable:
            raise OSError("bus is down")
        return super().save(namespace, records)


def test_a_mark_made_during_the_outage_survives_the_reload(tmp_path):
    """The reload after the outage is the moment the limit used to reset."""
    path = str(tmp_path / "s.json")
    clock = {"now": 9_000_000.0}
    backend = FlakyBackend(path)
    state = DurableStateStore(backend, refresh_secs=0.0, time_fn=lambda: clock["now"])

    state.mark("cooldown:zeroclaw", ttl=3600.0, at=clock["now"])
    assert state.persist_ok is True, "the healthy path persists"

    backend.readable = False
    clock["now"] += 30
    outage_stamp = clock["now"]
    state.touch("loop:j1", ttl=1800.0, at=clock["now"])
    assert state.persist_ok is False, "the outage refuses the write"
    # The in-process limit holds throughout the outage.
    assert len(state.timestamps("loop:j1", ttl=1800.0)) == 1

    # `refresh_secs=0.0` makes every read a reload, so this read IS the first
    # successful load since the outage — the replacement that used to lose it.
    backend.readable = True
    assert len(state.timestamps("loop:j1", ttl=1800.0)) == 1, (
        "the reload must not forget a mark the outage refused to write"
    )
    assert state.seen("cooldown:zeroclaw", ttl=3600.0), "the merge displaces nothing"

    # And the merge is still merged at the NEXT reload: a load is not what makes a
    # record durable, so the record is still refused until a save is accepted.
    clock["now"] += 5
    assert len(state.timestamps("loop:j1", ttl=1800.0)) == 1, "and must survive it too"

    state.mark("dlq:dead-1", ttl=21600.0, at=clock["now"])
    assert state.persist_ok is True
    reread = DurableStateStore(FileBackend(path), refresh_secs=0.0, time_fn=lambda: clock["now"])
    assert reread.seen("cooldown:zeroclaw", ttl=3600.0), "the existing records survived"
    assert reread.timestamps("loop:j1", ttl=1800.0) == [outage_stamp], "and the merged one landed"
    assert reread.seen("dlq:dead-1", ttl=21600.0)


def test_a_backend_record_newer_than_the_refused_write_wins(tmp_path):
    """The merge is newest-`at`-wins, not cache-always-wins.

    Somebody else retracted this key after this process's own mark was refused but
    before the backend came back. That retraction is the newest thing said about
    the key, so a merge that let the cache always win would hand the key back and
    the loop counter would start again.
    """
    path = str(tmp_path / "s.json")
    clock = {"now": 9_500_000.0}
    backend = FlakyBackend(path)
    state = DurableStateStore(backend, refresh_secs=0.0, time_fn=lambda: clock["now"])
    state.mark("loop:j1", ttl=1800.0, at=clock["now"])

    backend.readable = False
    clock["now"] += 10
    state.touch("loop:j1", ttl=1800.0, at=clock["now"])  # refused; cache at = clock["now"]
    assert state.persist_ok is False

    # Another process retracts the key DURING the outage, at a later stamp.
    clock["now"] += 10
    other = DurableStateStore(FileBackend(path), refresh_secs=0.0, time_fn=lambda: clock["now"])
    other.forget("loop:j1")

    backend.readable = True
    assert state.seen("loop:j1", ttl=1800.0) is False, (
        f"the newer retraction must stand; cached at {state.last_mark('loop:j1')}"
    )
    assert state._cache["loop:j1"].get("tombstone") is True, state._cache["loop:j1"]


def test_a_tombstone_written_during_the_outage_still_retracts(tmp_path):
    """A retraction is refused exactly like a mark, so the outage drops it too —
    and the load that heals the outage would otherwise resurrect the approval."""
    path = str(tmp_path / "s.json")
    clock = {"now": 9_800_000.0}
    backend = FlakyBackend(path)
    state = DurableStateStore(backend, refresh_secs=0.0, time_fn=lambda: clock["now"])
    state.mark("cooldown:zeroclaw", ttl=3600.0, at=clock["now"])
    state.mark("dlq:dead-1", ttl=21600.0, at=clock["now"])
    assert state.persist_ok is True

    backend.readable = False
    clock["now"] += 30
    state.forget("cooldown:zeroclaw")
    assert state.persist_ok is False
    assert state.seen("cooldown:zeroclaw", ttl=3600.0) is False, "the retraction holds in-process"

    backend.readable = True
    assert state.seen("cooldown:zeroclaw", ttl=3600.0) is False, (
        "the healed reload must not resurrect the retracted key"
    )
    assert state.seen("dlq:dead-1", ttl=21600.0), "and must leave the other records alone"

    # The retraction reaches the backend on the next accepted write.
    clock["now"] += 5
    state.mark("ssh:cerberus", ttl=3600.0, at=clock["now"])
    assert state.persist_ok is True
    reread = DurableStateStore(FileBackend(path), refresh_secs=0.0, time_fn=lambda: clock["now"])
    assert reread.seen("cooldown:zeroclaw", ttl=3600.0) is False
    assert reread.seen("dlq:dead-1", ttl=21600.0)


def test_a_healthy_reload_is_the_backends_answer_and_nothing_else(tmp_path):
    """No outage, nothing refused, nothing to merge: the reload must be exactly
    what loaded. A merge that fires on every load would be a second source of
    truth for a store whose whole point is that the backend is the one."""
    path = str(tmp_path / "s.json")
    clock = {"now": 10_000_000.0}
    good = DurableStateStore(FileBackend(path), refresh_secs=0.0, time_fn=lambda: clock["now"])
    good.mark("cooldown:zeroclaw", ttl=3600.0, at=clock["now"])
    good.mark("dlq:dead-1", ttl=21600.0, at=clock["now"])

    healed = DurableStateStore(FileBackend(path), refresh_secs=0.0, time_fn=lambda: clock["now"])
    # The merge is only ever fed by refused writes, and this process refused none:
    # so the set that feeds it is empty from the first read to the last.
    assert healed._written_since_load == {}, "nothing was refused, so nothing is merged"
    assert healed.keys("") == ["cooldown:zeroclaw", "dlq:dead-1"], healed.keys("")
    assert healed.seen("cooldown:zeroclaw", ttl=3600.0)
    assert healed.seen("dlq:dead-1", ttl=21600.0)
    assert healed._written_since_load == {}, "and a successful save keeps it that way"

    # A retraction that was actually persisted still retracts after a reload.
    healed.forget("dlq:dead-1")
    assert healed.seen("dlq:dead-1", ttl=21600.0) is False
    assert healed.seen("cooldown:zeroclaw", ttl=3600.0), "and only that one"


def test_a_write_the_backend_refused_survives_the_next_reload(tmp_path):
    """A save the backend did not accept loses durability exactly as a load that
    failed does, and it has to be remembered the same way.

    The read side keeps answering — a bus that serves a GET and drops a POST is the
    ordinary shape of a partial outage — so the next reload succeeds, replaces the
    cache, and would hand the process a clean sheet for every limit recorded in
    between. `persist_ok` says the write did not land; the merge is what stops the
    process's own enforcement from quietly relaxing because of it.
    """
    path = str(tmp_path / "s.json")
    clock = {"now": 11_000_000.0}
    backend = FlakyBackend(path)
    state = DurableStateStore(backend, refresh_secs=0.0, time_fn=lambda: clock["now"])
    state.mark("cooldown:zeroclaw", ttl=3600.0, at=clock["now"])
    assert state.persist_ok is True

    backend.writable = False
    clock["now"] += 30
    stamp = clock["now"]
    state.touch("ssh:all", ttl=3600.0, at=clock["now"])
    assert state.persist_ok is False, "the save was refused"
    assert len(state.timestamps("ssh:all", ttl=3600.0)) == 1, "and the limit holds in-process"

    backend.writable = True
    assert len(state.timestamps("ssh:all", ttl=3600.0)) == 1, (
        "a successful reload must not drop a record the backend never accepted"
    )
    # Once a save is accepted the whole state, merged record included, is durable.
    clock["now"] += 5
    state.mark("dlq:dead-1", ttl=21600.0, at=clock["now"])
    assert state.persist_ok is True
    reread = DurableStateStore(FileBackend(path), refresh_secs=0.0, time_fn=lambda: clock["now"])
    assert reread.timestamps("ssh:all", ttl=3600.0) == [stamp], reread.timestamps("ssh:all", ttl=3600.0)
    assert reread.seen("cooldown:zeroclaw", ttl=3600.0)


def test_refresh_drops_the_unpersisted_records_with_the_cache(tmp_path):
    """`refresh()` is the operator's "believe the backend from scratch", so the
    records the backend never accepted go with the cache: a merge that survived it
    would put back exactly what it was asked to drop, under the one method whose
    documented job is to stop serving them."""
    path = str(tmp_path / "s.json")
    clock = {"now": 11_500_000.0}
    backend = FlakyBackend(path)
    state = DurableStateStore(backend, refresh_secs=0.0, time_fn=lambda: clock["now"])

    backend.readable = False
    state.touch("loop:j1", ttl=1800.0, at=clock["now"])
    assert state.persist_ok is False
    assert state._written_since_load, "the refused write was remembered"

    state.refresh()
    assert state._written_since_load == {}, "refresh() drops what the cache drops"
    backend.readable = True
    assert state.timestamps("loop:j1", ttl=1800.0) == [], "the operator asked for the backend's answer"


def test_a_pruned_record_is_not_carried_by_the_snapshot(tmp_path):
    """The snapshot of what the backend refused was accumulated with `update`.

    `update` only ever adds, so a key the write path pruned as expired stayed in the
    snapshot, and the merge handed it back over the loaded state. Every read that
    passes a TTL filters it again, so no limit is extended — but `value()` reads
    with no window and returns the record, and the next accepted write persists it
    again. The snapshot is not a running total: at the moment of a refusal the whole
    cache IS what the backend has not taken, so it is taken, not accumulated.
    """
    path = str(tmp_path / "s.json")
    clock = {"now": 12_000_000.0}
    backend = FlakyBackend(path)
    state = DurableStateStore(backend, refresh_secs=0.0, time_fn=lambda: clock["now"])
    state.mark("cooldown:zeroclaw", ttl=3600.0, at=clock["now"])
    assert state.persist_ok is True

    backend.writable = False
    clock["now"] += 10
    state.set_value("failures:hydra", "cycle failed", ttl=COUNTER_TTLS["failures"])
    assert state.persist_ok is False
    assert state.value("failures:hydra") == "cycle failed"

    # Past the value's window, a write to another key prunes it from the cache.
    clock["now"] += COUNTER_TTLS["failures"] + 1
    state.mark("ssh:cerberus", ttl=3600.0, at=clock["now"])
    assert "failures:hydra" not in state._cache, "the write path expired it"
    assert "failures:hydra" not in state._written_since_load, "and the snapshot dropped it with it"

    # A successful reload must not hand it back: `value()` reads with no window, so
    # an resurrected record would be served as live.
    backend.writable = True
    assert state.value("failures:hydra") is None, "an expired record came back from the merge"
    assert state.seen("ssh:cerberus", ttl=3600.0), "and the live one is still there"
