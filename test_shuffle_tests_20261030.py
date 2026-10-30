"""Tests for scripts/shuffle_tests.py (the order-dependence hunt).

No real suite is run here: the runner is faked, except for the timeout tests,
which start tiny processes of their own on purpose.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from scripts import shuffle_tests as sh  # noqa: E402


class FakeRunner:
    """Records the sequence it was asked to run and returns scripted statuses."""

    def __init__(self, statuses: dict[str, str]):
        self.statuses = statuses
        self.seen: list[str] = []

    def __call__(self, file: str):
        self.seen.append(file)
        return self.statuses.get(file, sh.PASS), f"output for {file}"


def test_make_orders_is_deterministic_and_covers_every_file():
    files = [f"test_{i}.py" for i in range(10)]
    first = sh.make_orders(files, seed=7, orders=3)
    again = sh.make_orders(files, seed=7, orders=3)
    assert first == again, "the same seed must produce the same orders"
    assert len(first) == 3
    for order in first:
        assert sorted(order) == sorted(files), "every order is a permutation"
    other = sh.make_orders(files, seed=8, orders=3)
    assert other != first, "a different seed must produce a different schedule"


def test_run_order_records_one_status_per_file_in_sequence():
    files = ["test_a.py", "test_b.py", "test_c.py"]
    runner = FakeRunner({"test_b.py": sh.FAIL, "test_c.py": sh.TIMEOUT})
    seen_positions: list[int] = []

    def on_result(index, total, file, status, output):
        seen_positions.append(index)
        assert total == len(files)
        assert output == f"output for {file}"

    results = sh.run_order(files, runner, on_result)

    assert runner.seen == files, "files must run in the order given"
    assert results == {
        "test_a.py": sh.PASS,
        "test_b.py": sh.FAIL,
        "test_c.py": sh.TIMEOUT,
    }
    assert seen_positions == [1, 2, 3]


def test_run_order_accepts_a_status_only_runner():
    results = sh.run_order(["test_x.py"], lambda f: sh.FAIL)
    assert results == {"test_x.py": sh.FAIL}


def test_summarize_keeps_discovery_order_and_differing_flags_only_changes():
    files = ["test_a.py", "test_b.py", "test_c.py"]
    per_order = [
        {"test_a.py": sh.PASS, "test_b.py": sh.PASS, "test_c.py": sh.FAIL},
        {"test_a.py": sh.PASS, "test_b.py": sh.TIMEOUT, "test_c.py": sh.FAIL},
    ]
    statuses = sh.summarize(files, per_order)
    assert list(statuses) == files
    assert statuses["test_a.py"] == [sh.PASS, sh.PASS]
    assert sh.differing(statuses) == {"test_b.py": [sh.PASS, sh.TIMEOUT]}


def test_pytest_cmd_runs_exactly_one_file():
    cmd = sh.pytest_cmd("test_api_contract_20261029.py")
    assert cmd[1:3] == ["-m", "pytest"]
    assert cmd[-1] == "test_api_contract_20261029.py"
    assert "-p" in cmd and "no:cacheprovider" in cmd


def test_top_level_tests_are_top_level_only():
    files = sh.top_level_tests(sh.ROOT)
    assert files, "the repo has top-level test files"
    assert all("/" not in f for f in files), files
    assert all(f.startswith("test_") and f.endswith(".py") for f in files), files
    assert "tests/test_doctor_v2_rules.py" not in files


def test_run_cmd_timeout_kills_the_whole_process_group(tmp_path):
    """A hung file must be killed and reported as a timeout, children included."""
    pids = tmp_path / "pids"
    script = tmp_path / "hang.py"
    script.write_text(
        "import os, subprocess, sys, time\n"
        f"open({str(pids)!r}, 'w').write(str(os.getpid()))\n"
        "grandchild = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
        f"open({str(pids)!r}, 'a').write(' ' + str(grandchild.pid))\n"
        "time.sleep(60)\n"
    )
    started = time.monotonic()
    status, output = sh.run_cmd([sys.executable, str(script)], timeout=5.0)
    elapsed = time.monotonic() - started

    assert status == sh.TIMEOUT, output
    assert "timeout after 5s" in output
    assert elapsed < 20, f"the timeout must not wait for the hung process ({elapsed:.1f}s)"
    assert pids.exists(), "the child never started"
    for pid in (int(p) for p in pids.read_text().split()):
        assert_reaped(pid)


def assert_reaped(pid: int, wait: float = 3.0) -> None:
    """A killed process disappears; give the OS a moment to reap it first."""
    deadline = time.monotonic() + wait
    while True:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        if time.monotonic() > deadline:
            raise AssertionError(f"pid {pid} still alive after the timeout")
        time.sleep(0.1)


def test_run_cmd_reports_pass_and_fail(tmp_path):
    status, _ = sh.run_cmd([sys.executable, "-c", "raise SystemExit(0)"], timeout=30)
    assert status == sh.PASS
    status, output = sh.run_cmd([sys.executable, "-c", "raise SystemExit(3)"], timeout=30)
    assert status == sh.FAIL
    assert output == ""


def test_pytest_exit_5_maps_to_empty_not_fail(tmp_path):
    """A file that collects no tests is EMPTY, not a failure."""
    collecting_nothing = tmp_path / "test_nothing_here.py"
    collecting_nothing.write_text("ANSWER = 42\n")
    status, _ = sh.run_cmd(
        [sys.executable, "-m", "pytest", str(collecting_nothing)], timeout=60
    )
    assert status == sh.EMPTY


def test_main_prints_the_seed_and_the_differing_files(tmp_path, monkeypatch, capsys):
    """End to end through main() with a faked runner: no pytest, no suite.

    Two fixed orders; ``test_b`` fails whenever ``test_a`` ran before it, so its
    result differs between the orders and must be reported as a difference.
    """
    files = ["test_a.py", "test_b.py", "test_c.py"]
    fixed = [["test_a.py", "test_b.py", "test_c.py"], ["test_b.py", "test_a.py", "test_c.py"]]
    started: list[str] = []

    def fake_make_orders(population, seed, orders):
        assert sorted(population) == sorted(files)
        return [list(o) for o in fixed[:orders]]

    def fake_run_order(order, runner, on_result=None):
        # test_b fails whenever test_a ran before it in the same order, which is
        # what a real order leak between two files looks like.
        results = {}
        for index, file in enumerate(order, start=1):
            failed = file == "test_b.py" and "test_a.py" in order[: index - 1]
            results[file] = sh.FAIL if failed else sh.PASS
            started.append(file)
            if on_result is not None:
                on_result(index, len(order), file, results[file], "")
        return results

    monkeypatch.setattr(sh, "make_orders", fake_make_orders)
    monkeypatch.setattr(sh, "run_order", fake_run_order)
    out_json = tmp_path / "results.json"
    rc = sh.main(
        [
            "--seed", "5",
            "--orders", "2",
            "--files", *files,
            "--json", str(out_json),
        ]
    )
    captured = capsys.readouterr().out
    assert rc == 0
    assert "seed=5" in captured
    assert "files=3" in captured
    assert "DIFF test_b.py: order 1=fail (pos 2), order 2=pass (pos 1)" in captured, captured
    assert "order after test_a.py -> fail" in captured, captured
    assert "order after (first) -> pass" in captured, captured

    payload = json.loads(out_json.read_text())
    assert payload["seed"] == 5
    assert payload["orders"] == 2
    assert set(payload["results"][0]) == set(files)
    assert payload["differing"] == {"test_b.py": [sh.FAIL, sh.PASS]}
    assert started == ["test_a.py", "test_b.py", "test_c.py"] + ["test_b.py", "test_a.py", "test_c.py"]


def test_main_reports_no_differences_when_everything_is_stable(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(sh, "run_cmd", lambda cmd, timeout, cwd=None: (sh.PASS, ""))
    rc = sh.main(["--seed", "1", "--orders", "2", "--files", "test_a.py", "test_b.py"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "none: every file had the same result in every order" in out
