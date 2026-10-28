from hive.bus.domain.eligibility import host_pin_warnings


def test_unpinned_job_is_quiet():
    assert host_pin_warnings("build:zeroclaw", None) == []
    assert host_pin_warnings("build:zeroclaw", []) == []
    assert host_pin_warnings("build:zeroclaw", ["*"]) == []


def test_pin_without_reason_warns():
    out = host_pin_warnings("build:zeroclaw", ["achilles", "cerberus"])
    assert len(out) == 1
    assert "pin_reason" in out[0] and "achilles" in out[0]


def test_single_host_pin_also_warns_about_stranding():
    out = host_pin_warnings("build:zeroclaw", ["hydra"])
    assert len(out) == 2
    assert "re-offer" in out[1]


def test_pin_reason_silences_the_warning():
    assert host_pin_warnings("build:zeroclaw", ["hydra"], "needs the 4090") == []
    assert len(host_pin_warnings("build:zeroclaw", ["hydra"], "   ")) == 2


def test_hardware_bound_kinds_and_narrow_hosts_are_exempt():
    assert host_pin_warnings("ncz-os-image", ["achilles"]) == []
    assert host_pin_warnings("fleet-infra:x", ["achilles"]) == []
    assert host_pin_warnings("build:zeroclaw", ["cixmini"]) == []
