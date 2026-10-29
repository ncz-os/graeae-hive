"""H6: ``scripts/submit_job.py`` — the operator's rules, enforced offline.

Covered, per the brief: the defaults (no pin, no preferred_models, zeroclaw
kinds), the refusals (pin without a reason, codex without the flag, an
off-builder build job, an unknown key, a paid model without ``--reason``), the
content-derived idempotency key, and the fact that a dry run never reaches the
network while ``--send`` needs exactly ``HIVE_URL`` + ``HIVE_ACTOR_URN``.
"""

from __future__ import annotations

import importlib.util
import io
import json
import pathlib
import urllib.request

import pytest

from hive.bus.schemas import JobCreate

ROOT = pathlib.Path(__file__).parent
TASK = ROOT / "tests" / "fixtures" / "submit_task.sample.json"


def _load_submit():
    """Load scripts/submit_job.py without installing anything."""
    spec = importlib.util.spec_from_file_location("submit_job", ROOT / "scripts" / "submit_job.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mod = _load_submit()

#: An inventory holding every builder, minos and a host that is neither.
INVENTORY = {name: {"address": "192.0.2.10"} for name in ("proteus", "hydra", "achilles", "argos", "minos", "cerberus")}


def _task(**overrides) -> dict:
    task = {"kind": "code-edit", "description": "fix the thing"}
    task.update(overrides)
    return task


def _build(task: dict, **kwargs) -> tuple[dict, list[str]]:
    canonical = json.dumps(task, sort_keys=True, separators=(",", ":"))
    kwargs.setdefault("inventory", INVENTORY)
    return mod.build_payload(task, canonical, **kwargs)


def _write(tmp_path, task: dict) -> str:
    path = tmp_path / "task.json"
    path.write_text(json.dumps(task, indent=2))
    return str(path)


# ── the defaults, straight from the operator rules ──────────────────────────


def test_defaults_are_the_quiet_rules():
    payload, warnings = _build(_task())
    assert payload["eligible_kinds"] == ["zeroclaw"]
    assert "eligible_hosts" not in payload
    assert "preferred_models" not in payload  # so the host ladder starts at qwen38
    assert payload["idempotency_key"].startswith("submit-job:")
    assert warnings == []


def test_the_rules_block_is_prepended_to_the_description():
    payload, _ = _build(_task())
    description = payload["description"]
    assert description.startswith(mod.RULES_BLOCK)
    assert description.endswith("fix the thing")
    for clause in (
        "Work only inside the job workspace",
        "Push to ARGONAS only",
        "No GitHub, no GitLab, no PRs",
        "structured non-author review",
        "Claude verifies and merges",
    ):
        assert clause in description, clause


def test_the_payload_is_something_the_bus_accepts():
    payload, _ = _build(_task())
    JobCreate(**payload)  # raises if the script built a payload the bus would 422


# ── idempotency ─────────────────────────────────────────────────────────────


def test_idempotency_key_follows_content_not_formatting():
    task = _task()
    again = dict(reversed(list(task.items())))  # same content, other order
    assert mod.idempotency_key(json.dumps(task, sort_keys=True)) == mod.idempotency_key(
        json.dumps(again, sort_keys=True)
    )
    assert mod.idempotency_key(json.dumps(task, sort_keys=True)) != mod.idempotency_key(
        json.dumps(_task(description="different"), sort_keys=True)
    )


def test_a_second_run_of_one_task_file_reuses_the_key(tmp_path):
    path = _write(tmp_path, _task())
    first, _ = mod.load_task(path)
    second, _ = mod.load_task(path)
    assert _build(first)[0]["idempotency_key"] == _build(second)[0]["idempotency_key"]


def test_an_explicit_idempotency_key_in_the_task_file_wins():
    payload, _ = _build(_task(idempotency_key="operator-forced-rerun"))
    assert payload["idempotency_key"] == "operator-forced-rerun"


# ── refusals ────────────────────────────────────────────────────────────────


def test_a_host_pin_without_a_reason_is_refused():
    with pytest.raises(ValueError, match="pin-reason"):
        _build(_task(eligible_hosts=["hydra"]))
    payload, _ = _build(_task(eligible_hosts=["hydra"]), pin_reason="needs the 4090")
    assert payload["pin_reason"] == "needs the 4090"


def test_a_pin_reason_with_no_pin_is_refused():
    with pytest.raises(ValueError, match="no eligible_hosts"):
        _build(_task(), pin_reason="because")


def test_codex_is_refused_unless_the_flag_says_otherwise():
    with pytest.raises(ValueError, match="out of quota"):
        _build(_task(eligible_kinds=["codex"]))
    payload, _ = _build(_task(eligible_kinds=["codex", "zeroclaw"]), allow_codex=True)
    assert payload["eligible_kinds"] == ["codex", "zeroclaw"]


def test_an_unknown_task_key_is_a_typo_not_a_new_feature():
    with pytest.raises(ValueError, match="unknown task key"):
        _build(_task(elgible_hosts=["hydra"]))
    with pytest.raises(ValueError, match="unknown task key"):
        _build(_task(submitter_urn="urn:agent:me:myself"))


def test_kind_and_description_are_required():
    with pytest.raises(ValueError, match='"kind"'):
        _build({"description": "x"})
    with pytest.raises(ValueError, match='"description"'):
        _build({"kind": "code-edit"})


def test_a_malformed_task_file_is_a_message(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{not json")
    with pytest.raises(ValueError, match="not valid JSON"):
        mod.load_task(bad)
    with pytest.raises(ValueError, match="cannot read"):
        mod.load_task(tmp_path / "missing.json")
    lst = tmp_path / "list.json"
    lst.write_text("[]")
    with pytest.raises(ValueError, match="must be a JSON object"):
        mod.load_task(lst)


# ── build jobs: builders only, by name, never minos ─────────────────────────


def test_a_build_job_pins_to_the_builders_from_the_inventory():
    payload, warnings = _build(_task(kind="build:ncz-os-image"))
    assert payload["eligible_hosts"] == ["proteus", "hydra", "achilles", "argos"]
    assert "minos" not in payload["eligible_hosts"]
    assert payload["pin_reason"]  # the bus's unexplained-pin warning stays silent
    assert warnings == []


def test_minos_is_refused_for_a_build_job_even_when_the_inventory_lists_it():
    with pytest.raises(ValueError, match="never a builder"):
        _build(_task(kind="build:ncz-os-image", eligible_hosts=["minos"]), pin_reason="asked")


def test_a_non_builder_is_refused_for_a_build_job():
    with pytest.raises(ValueError, match="not a builder"):
        _build(_task(kind="build:ncz-os-image", eligible_hosts=["cerberus"]), pin_reason="asked")


def test_a_builder_absent_from_the_inventory_is_refused():
    without = {k: v for k, v in INVENTORY.items() if k != "achilles"}
    with pytest.raises(ValueError, match="not in the fleet inventory"):
        _build(_task(kind="build:ncz-os-image", eligible_hosts=["achilles"]), pin_reason="asked", inventory=without)


def test_a_build_job_with_no_builders_known_refuses_to_guess():
    with pytest.raises(ValueError, match="refusing to guess"):
        _build(_task(kind="build:ncz-os-image"), inventory={})


def test_a_capability_marked_build_is_treated_as_a_build_job():
    payload, _ = _build(_task(required_capabilities=["hard:build-rust"]))
    assert set(payload["eligible_hosts"]) == {"proteus", "hydra", "achilles", "argos"}


def test_an_ordinary_job_is_not_pinned():
    payload, _ = _build(_task(kind="research"))
    assert "eligible_hosts" not in payload


# ── model rules ─────────────────────────────────────────────────────────────


def test_a_paid_model_warns_without_a_reason_and_is_kept_with_one():
    task = _task(preferred_models=["anthropic/claude-opus-4-7"])
    payload, warnings = _build(task)
    assert payload["preferred_models"] == ["anthropic/claude-opus-4-7"]
    assert any("--reason" in w for w in warnings)

    payload, warnings = _build(task, reason="the operator asked for opus on this one")
    assert payload["preferred_models"] == ["anthropic/claude-opus-4-7"]
    assert not any(w.startswith("preferred_models names paid") for w in warnings)
    assert any(w.startswith("paid model(s) kept") for w in warnings)


def test_a_free_model_is_not_cried_down_as_paid():
    _, warnings = _build(_task(preferred_models=["qwen38"]))
    assert warnings == []
    assert mod.paid_model_tier("qwen38") not in ("B", "C")


def test_paid_detection_uses_the_rates_table_for_bare_names():
    assert mod.paid_model_tier("gpt-5.5") in ("B", "C")
    assert mod.paid_model_tier("nvidia/nemotron-3-ultra") not in ("B", "C")


# ── dry run and send ────────────────────────────────────────────────────────


def test_dry_run_is_the_default_and_never_sends(monkeypatch, tmp_path, capsys):
    def _forbidden(*_a, **_k):  # pragma: no cover - must never run
        raise AssertionError("submit_job reached the network in a dry run")

    monkeypatch.setattr(urllib.request, "urlopen", _forbidden)
    path = _write(tmp_path, _task())
    assert mod.main([path]) == 0
    captured = capsys.readouterr()
    printed = json.loads(captured.out)  # stdout is the payload and nothing else
    assert printed["kind"] == "code-edit"
    assert printed["eligible_kinds"] == ["zeroclaw"]
    assert "dry run" in captured.err


def test_send_needs_both_env_vars(monkeypatch, tmp_path, capsys):
    path = _write(tmp_path, _task())
    monkeypatch.delenv("HIVE_ACTOR_URN", raising=False)
    monkeypatch.setenv("HIVE_URL", "http://bus.example.invalid:5005")
    assert mod.main([path, "--send"]) == 2
    assert "HIVE_ACTOR_URN is not set" in capsys.readouterr().err

    monkeypatch.setenv("HIVE_ACTOR_URN", "urn:agent:test:actor")
    monkeypatch.delenv("HIVE_URL", raising=False)
    assert mod.main([path, "--send"]) == 2
    assert "HIVE_URL is not set" in capsys.readouterr().err


def test_send_posts_the_payload_to_the_bus(monkeypatch, tmp_path, capsys):
    captured: list = []

    class _Answer:
        def __init__(self):
            self._body = io.BytesIO(json.dumps({"id": "01a17c99-submitted"}).encode())

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def read(self, *_a):
            return self._body.read()

    def _fake_urlopen(req, timeout=None):  # noqa: ARG001 - signature matches urllib
        captured.append(req)
        return _Answer()

    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)
    monkeypatch.setenv("HIVE_URL", "http://bus.example.invalid:5005")
    monkeypatch.setenv("HIVE_ACTOR_URN", "urn:agent:test:actor")
    path = _write(tmp_path, _task())
    assert mod.main([path, "--send"]) == 0
    assert "01a17c99-submitted" in capsys.readouterr().out

    req = captured[0]
    assert req.get_method() == "POST"
    assert req.full_url.endswith("/v1/jobs")
    body = json.loads(req.data)
    assert body["submitter_urn"] == "urn:agent:test:actor"
    assert body["idempotency_key"].startswith("submit-job:")
    JobCreate(**body)


def test_a_refusal_exits_two_with_the_reason_on_stderr(monkeypatch, tmp_path, capsys):
    path = _write(tmp_path, _task(eligible_hosts=["hydra"]))
    assert mod.main([path]) == 2
    err = capsys.readouterr().err
    assert "submit_job:" in err and "pin-reason" in err


def test_the_sample_task_file_submits_as_a_build(tmp_path, capsys):
    payload, warnings = mod.build_payload(*mod.load_task(TASK), inventory=INVENTORY)
    assert payload["kind"] == "build:ncz-os-image"
    assert set(payload["eligible_hosts"]) == {"proteus", "hydra", "achilles", "argos"}
    assert warnings == []
    JobCreate(**payload)
