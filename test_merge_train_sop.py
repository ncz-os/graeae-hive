"""What merge-train may write, and how it records what it could not (2026-10-22).

The engine this replaces (`~/merge-train/apply.py` on PYTHIA) pushed `main` to
ARGONAS **and** GitLab on every fast-forward, opened `[DOCTOR]` merge requests
through the GitLab API, ran every 5 minutes for 17-21 minutes, and reported a bare
`run failed` when a single repository raised — because `os.environ["GLTOK"]` was
read at import time.

Every test here builds real git repositories in `tmp_path`: the fast-forward
refusals are only meaningful against a real merge base, a real non-ancestor and a
real `--force`.
"""

import ast
import json
import os
import re
import subprocess

import pytest

from hive.workers import merge_train as mt


def source_of(module=mt) -> str:
    with open(module.__file__, encoding="utf-8") as handle:
        return handle.read()


def git(*args, cwd):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True)


@pytest.fixture
def repo(tmp_path):
    """A bare repo whose `main` has four interesting branches against it.

    Built so each bucket is real rather than mocked: `hive/ff` is a strict
    descendant of the FINAL `main` (a genuine fast-forward), `hive/behind` hangs
    off the old base (a cherry-pick candidate), `hive/conflict` and `main` edit the
    same line differently, and `hive/orphan` shares no history at all.
    """
    work = tmp_path / "work"
    work.mkdir()
    git("init", "-q", "-b", "main", cwd=work)
    git("config", "user.email", "jperlow@gmail.com", cwd=work)
    git("config", "user.name", "Jason Perlow", cwd=work)
    (work / "README").write_text("one\n", encoding="utf-8")
    git("add", "README", cwd=work)
    git("commit", "-q", "-m", "base", cwd=work)
    base = git("rev-parse", "HEAD", cwd=work).stdout.strip()

    # main moves on: adds a file and rewrites the shared line.
    (work / "OTHER").write_text("main moved\n", encoding="utf-8")
    (work / "README").write_text("main version\n", encoding="utf-8")
    git("add", "OTHER", cwd=work)
    git("commit", "-qam", "main moves on", cwd=work)
    main_sha = git("rev-parse", "HEAD", cwd=work).stdout.strip()

    # A strict descendant of the FINAL main: the genuine fast-forward candidate.
    git("checkout", "-q", "-b", "hive/ff", cwd=work)
    (work / "FF").write_text("ff\n", encoding="utf-8")
    git("add", "FF", cwd=work)
    git("commit", "-qm", "ff work", cwd=work)
    ff_sha = git("rev-parse", "HEAD", cwd=work).stdout.strip()

    # Off the OLD base: behind main, small, and touching NEITHER file main touched,
    # so the merge is clean -> a cherry-pick candidate rather than a conflict.
    git("checkout", "-q", "-b", "hive/behind", base, cwd=work)
    (work / "CHERRY").write_text("cherry\n", encoding="utf-8")
    git("add", "CHERRY", cwd=work)
    git("commit", "-qm", "cherry", cwd=work)
    cherry_sha = git("rev-parse", "HEAD", cwd=work).stdout.strip()

    # Off the old base too, but editing the line main also edited -> conflict.
    git("checkout", "-q", "-b", "hive/conflict", base, cwd=work)
    (work / "README").write_text("conflicting\n", encoding="utf-8")
    git("commit", "-qam", "conflicting edit", cwd=work)

    # No history at all.
    git("checkout", "-q", "--orphan", "hive/orphan", cwd=work)
    git("rm", "-q", "-rf", ".", cwd=work)
    (work / "NEW").write_text("orphan\n", encoding="utf-8")
    git("add", "NEW", cwd=work)
    git("commit", "-qm", "orphan", cwd=work)

    git("checkout", "-q", "main", cwd=work)
    bare = tmp_path / "mirror.git"
    subprocess.run(["git", "clone", "-q", "--bare", str(work), str(bare)], check=True, capture_output=True)
    return {
        "bare": str(bare),
        "work": str(work),
        "base": base,
        "main_sha": main_sha,
        "ff_sha": ff_sha,
        "cherry_sha": cherry_sha,
    }


# ── the write ────────────────────────────────────────────────────────────────


def test_a_fast_forward_advances_the_canonical_remote(repo, tmp_path):
    upstream = tmp_path / "upstream.git"
    subprocess.run(["git", "init", "-q", "--bare", str(upstream)], check=True, capture_output=True)
    git("remote", "add", "argonas", str(upstream), cwd=repo["bare"])
    git("fetch", "-q", "argonas", cwd=repo["bare"])

    out = mt.fast_forward(repo["bare"], "main", repo["ff_sha"], dry_run=False)
    assert "advanced main" in out, out
    assert "REFUSED" not in out
    got = subprocess.run(
        ["git", "rev-parse", "main"], cwd=str(upstream), capture_output=True, text=True, check=True
    ).stdout.strip()
    assert got == repo["ff_sha"]


def test_a_dry_run_writes_nothing(repo, tmp_path):
    upstream = tmp_path / "upstream.git"
    subprocess.run(["git", "init", "-q", "--bare", str(upstream)], check=True, capture_output=True)
    git("remote", "add", "argonas", str(upstream), cwd=repo["bare"])
    out = mt.fast_forward(repo["bare"], "main", repo["ff_sha"], dry_run=True)
    assert "DRY-RUN" in out, out
    refs = subprocess.run(["git", "for-each-ref", "refs/heads"], cwd=str(upstream), capture_output=True, text=True)
    assert refs.stdout.strip() == ""


def test_a_non_fast_forward_is_refused(repo):
    """`main` moved on, so advancing it to the wip sha is not a fast-forward."""
    out = mt.fast_forward(repo["bare"], "main", repo["cherry_sha"], dry_run=False)
    assert out.startswith("REFUSED"), out
    assert "not a fast-forward" in out, out


def test_a_push_to_anything_but_argonas_is_refused(repo):
    for remote in ("gitlab", "origin", "github"):
        out = mt.fast_forward(repo["bare"], "main", repo["ff_sha"], dry_run=False, remote=remote)
        assert out.startswith("REFUSED"), f"{remote}: {out}"
        assert "outward_send" in out, f"{remote}: {out}"


def test_an_empty_or_unknown_sha_is_refused(repo):
    for sha in ("", "   ", "deadbeefdeadbeef"):
        out = mt.fast_forward(repo["bare"], "main", sha, dry_run=False)
        assert out.startswith("REFUSED"), f"{sha!r}: {out}"


def test_the_single_choke_point_refuses_a_force_push(repo):
    for args in (
        ["git", "push", "--force", "argonas", "main"],
        ["git", "push", "argonas", "+main"],
        ["git", "push", "--mirror", "argonas"],
        ["git", "push", "argonas", "+main"],
        ["git", "push", "argonas", "+abc:refs/heads/main"],
        ["git", "push", "argonas", "--delete", "hive/ff"],
    ):
        with pytest.raises(ValueError):
            mt.assert_no_forced_or_public_writes(args)
    with pytest.raises(ValueError):
        mt.assert_no_forced_or_public_writes(["git", "push", "origin", "main"])
    # The one allowed shape passes.
    mt.assert_no_forced_or_public_writes(["git", "push", "argonas", "abc:refs/heads/main"])


def test_the_default_remote_is_argonas():
    assert mt.CANONICAL_REMOTE == "argonas"


# ── the opt-in ───────────────────────────────────────────────────────────────


def test_the_public_forge_path_is_off_by_default():
    assert mt.ALLOW_PUBLIC_FORGE is False
    report = mt.BranchReport(repo="proj", branch="hive/ff", bucket=mt.BUCKET_FAST_FORWARD, target="gitlab.com")
    assert mt.route_outward(report) is None, "nothing may be requested with the opt-in off"


def test_an_enabled_opt_in_requests_rather_than_sends(monkeypatch):
    monkeypatch.setattr(mt, "ALLOW_PUBLIC_FORGE", True)
    report = mt.BranchReport(repo="proj", branch="hive/ff", bucket=mt.BUCKET_DIVERGENT, target="gitlab.com")
    request = mt.route_outward(report)
    assert request is not None
    assert request["kind"] == "push_public_forge"
    assert request["target"] == "gitlab.com"
    # The request carries NO gate result: `outward_send`'s default for an absent
    # gate is the unadjudicated finding, which routes to the operator. A request
    # that asserted a clean gate would be a request the decision reads as cleared.
    for gate in ("secret_gate", "attribution_scan", "soak", "rate"):
        assert gate not in request, f"{gate} must be left at its default, not asserted"
    assert "approved" not in request, "no approval may ride along with the request"
    assert request["plain_text"] is True


def test_a_merge_request_is_requested_as_pr_open(monkeypatch):
    monkeypatch.setattr(mt, "ALLOW_PUBLIC_FORGE", True)
    report = mt.BranchReport(
        repo="proj",
        branch="hive/wip",
        bucket=mt.BUCKET_DIVERGENT,
        target="argonas",
        opens_mr=True,
    )
    assert mt.route_outward(report)["kind"] == "pr_open"


def test_no_code_path_in_this_module_pushes_to_a_public_forge():
    """The only `git push` is inside `fast_forward`, and it refuses a
    non-canonical remote before reaching it."""
    source = source_of()
    # Count real invocations (`_sh([...])`), not every mention of the word: the
    # choke point's own guard mentions "push" too.
    pushes = re.findall(r'_sh\(\[[^\]]*"push"[^\]]*', source)
    assert pushes == ['_sh(["git", "push", remote, f"{sha}:refs/heads/{base}"'], (
        f"exactly one git push may exist in this module, and it must be the guarded one: {pushes}"
    )
    for forbidden in ("urllib", "gitlab.com/api", "merge_requests", "PRIVATE-TOKEN"):
        assert forbidden not in source, f"{forbidden} may not appear in this module"


# ── cadence ──────────────────────────────────────────────────────────────────


def test_the_daily_floor_defers_a_five_minute_timer():
    cadence = mt.Cadence(last_run=1000.0, interval=86_400.0, now=1300.0)
    assert not cadence.due(), "300s after the last run is not a day"
    assert cadence.seconds_until_due() == 86_400.0 - 300.0


def test_a_push_event_exempts_the_floor():
    cadence = mt.Cadence(last_run=1000.0, interval=86_400.0, now=1300.0)
    assert cadence.due(on_push_event=True)


def test_never_run_is_due():
    assert mt.Cadence(last_run=None, now=1300.0).due()


def test_the_five_minute_timer_is_still_a_five_minute_floor():
    """The floor is the operator's; a 300s floor still defers at 299s."""
    cadence = mt.Cadence(last_run=1000.0, interval=300.0, now=1299.0)
    assert not cadence.due()
    assert mt.Cadence(last_run=1000.0, interval=300.0, now=1300.0).due()


def test_the_record_is_read_from_the_environment(tmp_path):
    path = tmp_path / "state.json"
    env = {"MERGE_TRAIN_STATE_PATH": str(path)}
    due, _wait = mt.due_for_run(env, now=1000.0)
    assert due, "no record means never run, not 'ran just now'"

    path.write_text(json.dumps({"last_run": 1000.0}), encoding="utf-8")
    env["MERGE_TRAIN_MIN_INTERVAL"] = "86400"
    due, wait = mt.due_for_run(env, now=1300.0)
    assert not due
    assert wait == 86_400.0 - 300.0


def test_an_unreadable_record_is_treated_as_never_run(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("{not json", encoding="utf-8")
    due, _wait = mt.due_for_run({"MERGE_TRAIN_STATE_PATH": str(path)}, now=1000.0)
    assert due


def test_the_default_interval_is_a_day():
    assert mt.MIN_INTERVAL_SECS == 86_400.0
    assert "86400" in source_of()


# ── classification ───────────────────────────────────────────────────────────


def test_the_buckets_are_the_ones_the_sop_decision_parses(repo):
    """The two lists are written out separately (this module runs on PYTHIA
    without the zeroclaw crate), so they are asserted to agree."""
    expected = {
        "done",
        "fast_forward",
        "cherry_pick",
        "divergent",
        "conflict",
        "no_base",
    }
    assert set(mt.BUCKETS) == expected
    # And the same set as `TrainBucket::ALL` on the zeroclaw side.
    so_puckets = expected
    assert len(so_puckets) == len(mt.BUCKETS)


def test_a_clean_fast_forward_is_classified_as_one(repo):
    bucket, stats = mt.classify(repo["bare"], "main", "hive/ff")
    assert bucket == mt.BUCKET_FAST_FORWARD, (bucket, stats)
    assert stats.ahead == 1 and stats.behind == 0


def test_a_branch_behind_main_is_not_a_fast_forward(repo):
    bucket, stats = mt.classify(repo["bare"], "main", "hive/behind")
    assert bucket == mt.BUCKET_CHERRY, (bucket, stats)
    assert stats.behind >= 1


def test_an_orphan_branch_has_no_merge_base(repo):
    bucket, stats = mt.classify(repo["bare"], "main", "hive/orphan")
    assert bucket == mt.BUCKET_NO_BASE
    assert stats.ahead == 0


def test_a_merged_branch_is_done(repo):
    # A branch pointing at main itself: nothing to merge.
    git("branch", "hive/done", repo["main_sha"], cwd=repo["bare"])
    bucket, _stats = mt.classify(repo["bare"], "main", "hive/done")
    assert bucket == mt.BUCKET_DONE


def test_a_conflict_is_classified_as_one(repo):
    bucket, stats = mt.classify(repo["bare"], "main", "hive/conflict")
    assert bucket == mt.BUCKET_CONFLICT, (bucket, stats)


def test_candidate_branches_respect_the_pattern(repo):
    branches = mt.candidate_branches(repo["bare"], "main")
    assert "hive/ff" in branches
    assert "main" not in branches, "the base is never a candidate"
    assert all("hive/" in branch for branch in branches)


# ── the record: why each repository failed ───────────────────────────────────


def test_a_failure_is_recorded_with_its_reason_not_a_bare_run_failed(tmp_path):
    # One repository that measures cleanly, one that cannot be read at all.
    good = tmp_path / "good.git"
    work = tmp_path / "w"
    work.mkdir()
    git("init", "-q", "-b", "main", cwd=work)
    git("config", "user.email", "jperlow@gmail.com", cwd=work)
    git("config", "user.name", "Jason Perlow", cwd=work)
    (work / "f").write_text("x\n", encoding="utf-8")
    git("add", "f", cwd=work)
    git("commit", "-qm", "base", cwd=work)
    (work / "g").write_text("y\n", encoding="utf-8")
    git("add", "g", cwd=work)
    git("commit", "-qm", "wip", cwd=work)
    subprocess.run(["git", "clone", "-q", "--bare", str(work), str(good)], check=True, capture_output=True)
    subprocess.run(["git", "push", "-q", str(good), "main:hive/wip"], cwd=work, check=True, capture_output=True)
    bad = tmp_path / "bad.git"
    bad.mkdir()

    outward = []
    outcomes = mt.run_cycle(repos=[str(good), str(bad)], outward_reports=outward)

    assert len(outcomes) == 2
    failed = [outcome for outcome in outcomes if outcome.failed]
    assert len(failed) == 1, [o.summary() for o in outcomes]
    reasons = failed[0].failures
    assert reasons and all("bad.git" in reason for reason in reasons), reasons
    # The reason is specific, and it lands in the record the SOP decides on.
    record = failed[0].to_record()
    assert record["failures"] == reasons
    assert "run failed" not in json.dumps(record)


def test_one_bad_branch_does_not_end_the_cycle(tmp_path, monkeypatch):
    bare = tmp_path / "r.git"
    subprocess.run(["git", "init", "-q", "--bare", str(bare)], check=True, capture_output=True)
    work = tmp_path / "w"
    work.mkdir()
    git("init", "-q", "-b", "main", cwd=work)
    git("config", "user.email", "jperlow@gmail.com", cwd=work)
    git("config", "user.name", "Jason Perlow", cwd=work)
    (work / "f").write_text("x\n", encoding="utf-8")
    git("add", "f", cwd=work)
    git("commit", "-qm", "base", cwd=work)
    # Both branches hang off the SAME main commit, one commit ahead, so both are
    # genuine fast-forward candidates and the only thing that separates them is
    # whether `classify` raises.
    for name in ("hive/good", "hive/bad"):
        git("checkout", "-q", "-b", name, cwd=work)
        (work / name.split("/")[-1]).write_text(name + "\n", encoding="utf-8")
        git("add", name.split("/")[-1], cwd=work)
        git("commit", "-qm", name, cwd=work)
    git("checkout", "-q", "main", cwd=work)
    # `cwd=work` matters: a push with no cwd would run against THIS repository.
    subprocess.run(
        # The branch TIPS, not `main`'s sha: pushing main's sha into hive/good
        # would make that branch DONE and prove nothing.
        [
            "git",
            "push",
            "-q",
            str(bare),
            "main:main",
            "hive/good:hive/good",
            "hive/bad:hive/bad",
        ],
        cwd=work,
        check=True,
    )

    real_classify = mt.classify

    def flaky(repo, base, branch):
        if branch == "hive/bad":
            raise RuntimeError("git exploded")
        return real_classify(repo, base, branch)

    monkeypatch.setattr(mt, "classify", flaky)

    outcome = mt.measure_repo(str(bare))
    buckets = {report.branch: report.bucket for report in outcome.reports}
    assert buckets["hive/good"] == mt.BUCKET_FAST_FORWARD
    assert buckets["hive/bad"] == mt.BUCKET_NO_BASE
    assert any("git exploded" in failure for failure in outcome.failures), outcome.failures


def test_a_repository_with_no_candidates_is_reported(tmp_path):
    bare = tmp_path / "empty.git"
    subprocess.run(["git", "init", "-q", "--bare", str(bare)], check=True, capture_output=True)
    outcome = mt.measure_repo(str(bare))
    assert outcome.failed
    assert "no branch matched" in outcome.failures[0]


def test_a_missing_repository_is_reported_not_raised(tmp_path):
    outcome = mt.measure_repo(str(tmp_path / "nope.git"))
    assert outcome.failed
    assert "not a directory" in outcome.failures[0]


def test_the_cycle_never_raises(tmp_path, monkeypatch):
    """`run_cycle` catches per repository; the whole cycle cannot die on one."""
    monkeypatch.setattr(mt, "measure_repo", lambda repo: (_ for _ in ()).throw(RuntimeError("boom")))
    outcomes = mt.run_cycle(repos=["/a/one.git", "/a/two.git"])
    assert len(outcomes) == 2
    assert all(outcome.failed for outcome in outcomes)
    assert "RuntimeError: boom" in outcomes[0].failures[0]


def test_a_failed_repository_is_never_fast_forwarded(repo, tmp_path):
    """A repository whose measurement failed must not be written to, even if it
    also reported a fast-forward bucket."""
    upstream = tmp_path / "upstream.git"
    subprocess.run(["git", "init", "-q", "--bare", str(upstream)], check=True, capture_output=True)
    git("remote", "add", "argonas", str(upstream), cwd=repo["bare"])
    outcome = mt.measure_repo(repo["bare"])
    outcome.failures.append("one branch could not be read")
    lines = mt.fast_forward_candidates([outcome], dry_run=False)
    assert any("SKIPPED" in line for line in lines), lines
    refs = subprocess.run(
        ["git", "for-each-ref", "refs/heads"], cwd=str(upstream), capture_output=True, text=True
    ).stdout.strip()
    assert refs == "", f"nothing may be written from a failed repository: {refs}"


def test_the_record_field_names_match_the_sop_contract(repo):
    """`merge_train.decide` parses `record{repo,branch,bucket,target,sha,opens_mr,
    ahead,behind,clobbered,failures}`; a renamed field is a silent escalation."""
    outcome = mt.measure_repo(repo["bare"])
    record = outcome.to_record()
    assert set(record) == {
        "repo",
        "branch",
        "bucket",
        "target",
        "sha",
        "opens_mr",
        "ahead",
        "behind",
        "clobbered",
        "failures",
    }


def test_a_replayed_sop_decision_refuses_the_gitlab_push(repo, monkeypatch):
    """End of the chain: the engine's record goes to the decision, and the
    decision's verdict is what the engine is allowed to act on.

    The decision function itself is Rust and lives in zeroclaw; what is pinned
    here is the CONTRACT — that a record whose target is a public forge comes back
    with nothing to write, and that the engine's only writer refuses it anyway.
    """
    outcome = mt.measure_repo(repo["bare"])
    record = outcome.to_record()
    record["target"] = "gitlab"
    assert record["target"] != mt.CANONICAL_REMOTE
    # The engine would emit an outward REQUEST for it, never a push.
    monkeypatch.setattr(mt, "ALLOW_PUBLIC_FORGE", False)
    assert mt.route_outward(mt.BranchReport(**{**record, "opens_mr": False})) is None
    # And if something tried to push there, the choke point refuses.
    with pytest.raises(ValueError):
        mt.assert_no_forced_or_public_writes(["git", "push", "gitlab", "main"])


def _code_without_docstrings(module=mt) -> str:
    """The module's code with every docstring removed.

    The prose in this file deliberately NAMES the defects it removes ("the base
    commit did `os.environ["GLTOK"]` at import time"), so a plain text search for
    the offending string finds the explanation, not the offence. Walking the AST
    separates the two.
    """
    tree = ast.parse(source_of(module))
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                node.body = body[1:] or [ast.Pass()]
    return ast.unparse(tree)


def test_no_token_is_read_at_import_time():
    """The base commit did `TOK = os.environ["GLTOK"]` at module scope, so one
    missing variable aborted the whole cycle before it measured anything."""
    subscripts = [
        node
        for node in ast.walk(ast.parse(source_of()))
        if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Attribute) and node.value.attr == "environ"
    ]
    assert subscripts == [], (
        f"every os.environ read must go through .get() with a default, never a subscript: {subscripts}"
    )


def test_the_module_needs_no_gitlab_token_at_all():
    """No forge credential is read anywhere in this file's CODE."""
    code = _code_without_docstrings()
    for forbidden in ("PRIVATE-TOKEN", "oauth2", "GLTOK", "gitlab.com/api", "Authorization"):
        assert forbidden not in code, forbidden


def test_importing_the_module_does_not_need_the_environment(monkeypatch):
    """Import must not raise on a host with none of its variables set.

    `monkeypatch.delenv` rather than `os.environ.clear()` and putting it back by
    hand: clearing the whole environment means a failure between the clear and the
    restore leaves the rest of the session running on an empty environment, which
    surfaces as some unrelated test failing somewhere else entirely.
    """
    import importlib

    for key in tuple(os.environ):
        if key.startswith("MERGE_TRAIN"):
            monkeypatch.delenv(key, raising=False)
    reloaded = importlib.reload(mt)
    assert reloaded.ALLOW_PUBLIC_FORGE is False
    assert reloaded.MIN_INTERVAL_SECS == 86_400.0
    assert reloaded.CANONICAL_REMOTE == "argonas"
    monkeypatch.undo()
    importlib.reload(mt)


def test_the_record_carries_the_base_the_classification_used(repo):
    """Review finding: `fast_forward_candidates` re-resolved `default_branch` per
    candidate — a second git call per branch, and one that could name a different
    branch than the one that was classified. The base now travels on the report."""
    outcome = mt.measure_repo(repo["bare"])
    assert outcome.reports
    assert all(report.base == "main" for report in outcome.reports), [(r.branch, r.base) for r in outcome.reports]
    # And it is NOT in the record the SOP decides on: a payload field the decision
    # ignores is a field that can drift unnoticed.
    assert "base" not in outcome.to_record()


def test_a_fast_forward_uses_the_recorded_base_not_a_fresh_resolution(repo, monkeypatch, tmp_path):
    upstream = tmp_path / "upstream.git"
    subprocess.run(["git", "init", "-q", "--bare", str(upstream)], check=True, capture_output=True)
    git("remote", "add", "argonas", str(upstream), cwd=repo["bare"])
    outcome = mt.measure_repo(repo["bare"])

    def explode(*args, **kwargs):
        raise AssertionError("default_branch must not be re-resolved per candidate")

    monkeypatch.setattr(mt, "default_branch", explode)
    lines = mt.fast_forward_candidates([outcome], dry_run=False)
    assert any("advanced main" in line for line in lines), lines


def test_apply_writes_to_each_repository_measured_not_to_one_named_path(repo, tmp_path):
    """`main()` used to pass no path at all, so `--apply` took the "not applied"
    branch, wrote nothing, and reported success. The outcome carries its own path."""
    upstream = tmp_path / "upstream.git"
    subprocess.run(["git", "init", "-q", "--bare", str(upstream)], check=True, capture_output=True)
    git("remote", "add", "argonas", str(upstream), cwd=repo["bare"])
    outcome = mt.measure_repo(repo["bare"])
    assert outcome.path == repo["bare"], "the outcome must remember where it was measured"

    lines = mt.fast_forward_candidates([outcome], dry_run=False)
    assert any("advanced main" in line for line in lines), lines
    refs = subprocess.run(
        ["git", "for-each-ref", "refs/heads"], cwd=str(upstream), capture_output=True, text=True
    ).stdout.strip()
    assert "main" in refs, f"--apply must actually write: {refs}"


def test_an_outcome_with_no_recorded_path_is_skipped_not_written(repo):
    """No path means there is nothing to write; it must say so rather than guess."""
    outcome = mt.measure_repo(repo["bare"])
    outcome.path = ""
    lines = mt.fast_forward_candidates([outcome], dry_run=False)
    assert lines and all("no recorded repository path" in line for line in lines), lines


def test_a_binary_change_is_never_a_fast_forward(repo, tmp_path):
    """`--numstat` prints `-` for both columns of a binary file, which counted as
    zero lines — so a branch adding a multi-gigabyte blob was classified
    fast-forward and pushed with no human ever seeing its size."""
    git("checkout", "-q", "hive/ff", cwd=repo["work"])
    with open(os.path.join(repo["work"], "big.bin"), "wb") as handle:
        handle.write(bytes(4096))
    git("add", "big.bin", cwd=repo["work"])
    git("commit", "-qm", "add a binary blob", cwd=repo["work"])
    # The mirror is a clone OF the work tree, so the work tree has no remote: push
    # to the mirror by path.
    subprocess.run(["git", "push", "-q", repo["bare"], "hive/ff"], cwd=repo["work"], check=True)

    bucket, stats = mt.classify(repo["bare"], "main", "hive/ff")
    assert stats.binary == 1, stats
    assert bucket == mt.BUCKET_DIVERGENT, (bucket, stats)


def test_a_completed_cycle_records_when_it_ran(tmp_path):
    """Without this the record file is never written, `due_for_run` reads
    'never run' forever, and the daily floor never engages — the 5-minute timer
    defect with a one-day number in front of it."""
    path = str(tmp_path / "mt.json")
    env = {"MERGE_TRAIN_STATE_PATH": path}
    assert mt.record_cycle_end(at=1_000.0, env=env) is True
    due, wait = mt.due_for_run(env, now=1_300.0)
    assert not due, "300s after a recorded run is not a day"
    assert wait == 86_400.0 - 300.0


def test_a_failed_cycle_is_still_recorded(tmp_path):
    """The cycle RAN; what it failed at is in the outcome records. Not recording a
    failed cycle would mean a broken repository re-runs every five minutes."""
    path = str(tmp_path / "mt.json")
    assert mt.record_cycle_end(at=1_000.0, env={"MERGE_TRAIN_STATE_PATH": path}) is True
    due, _wait = mt.due_for_run({"MERGE_TRAIN_STATE_PATH": path}, now=1_300.0)
    assert not due


def test_recording_without_a_path_is_a_no_op_not_a_crash():
    """With the default path disabled and nothing configured there is nowhere to
    write, and that says so rather than pretending the cycle was recorded."""
    assert mt.record_cycle_end(at=1.0, env={"MERGE_TRAIN_NO_DEFAULT_STATE": "1"}) is False


def test_a_failed_git_count_is_not_a_clean_done(repo, monkeypatch):
    """`_git` returns "" for a failed command as readily as for zero, so
    `int(... or 0)` read a broken measurement as DONE — a clean record for a
    measurement that never happened."""
    real_sh = mt._sh

    def explode(args, **kwargs):
        if args[:2] == ["git", "rev-list"]:
            return subprocess.CompletedProcess(args, 128, "", "fatal: bad object\n")
        return real_sh(args, **kwargs)

    monkeypatch.setattr(mt, "_sh", explode)
    with pytest.raises(mt.MeasurementError):
        mt.classify(repo["bare"], "main", "hive/ff")

    # And `measure_repo` turns that into a recorded failure rather than a bucket.
    outcome = mt.measure_repo(repo["bare"])
    assert outcome.failed, outcome.failures
    assert any("rev-list" in failure for failure in outcome.failures), outcome.failures
    assert all(report.bucket != mt.BUCKET_DONE for report in outcome.reports), [
        (r.branch, r.bucket) for r in outcome.reports
    ]


def test_every_measured_branch_gets_a_record(repo):
    """`merge_train.decide` is called once per record, so a caller with only
    `to_record()` decides the first branch and silently drops the rest."""
    outcome = mt.measure_repo(repo["bare"])
    assert len(outcome.reports) >= 4, [r.branch for r in outcome.reports]
    records = outcome.to_records()
    assert len(records) == len(outcome.reports)
    assert {record["branch"] for record in records} == {r.branch for r in outcome.reports}
    # `to_record()` is the one-branch shortcut and agrees with the first record.
    assert records[0] == outcome.to_record()


def test_an_unreadable_repository_is_not_reported_as_having_no_branches(repo, monkeypatch):
    """`for-each-ref` returning nothing read as "no branch matched the pattern" —
    a clean statement about a repository nothing could look at."""
    real = mt._sh
    monkeypatch.setattr(
        mt,
        "_sh",
        lambda a, **k: (
            subprocess.CompletedProcess(a, 128, "", "fatal: not a git repository\n")
            if a[:2] == ["git", "for-each-ref"]
            else real(a, **k)
        ),
    )
    outcome = mt.measure_repo(repo["bare"])
    assert outcome.failed, outcome.failures
    assert any("for-each-ref" in failure for failure in outcome.failures), outcome.failures
    assert not any("no branch matched" in failure for failure in outcome.failures), outcome.failures


def test_a_failing_merge_base_is_not_reported_as_no_merge_base(repo, monkeypatch):
    """git exits 1 for "no merge base" and 128 for "fatal: bad object", and `_git`
    returns "" for both — two different jobs with different owners."""
    real = mt._sh
    monkeypatch.setattr(
        mt,
        "_sh",
        lambda a, **k: (
            subprocess.CompletedProcess(a, 128, "", "fatal: bad object\n")
            if a[:2] == ["git", "merge-base"]
            else real(a, **k)
        ),
    )
    with pytest.raises(mt.MeasurementError):
        mt.classify(repo["bare"], "main", "hive/ff")

    outcome = mt.measure_repo(repo["bare"])
    assert any("merge-base" in failure for failure in outcome.failures), outcome.failures
    # The claim is the FAILURE, not the placeholder bucket: an unmeasurable branch is
    # filed under the one bucket that escalates, and the repository is refused by the
    # writer outright.
    assert outcome.failed
    lines = mt.fast_forward_candidates([outcome], dry_run=False)
    assert any("SKIPPED" in line for line in lines), lines


def test_the_cadence_record_has_a_default_path(tmp_path, monkeypatch):
    """With nothing configured and nothing written, `due_for_run` is permanently
    due — the timer defect with a one-day number in front of it."""
    monkeypatch.delenv("MERGE_TRAIN_STATE_PATH", raising=False)
    monkeypatch.delenv("MERGE_TRAIN_NO_DEFAULT_STATE", raising=False)
    monkeypatch.delenv("XDG_STATE_HOME", raising=False)
    path = mt.state_path({"XDG_STATE_HOME": str(tmp_path)})
    assert path.startswith(str(tmp_path)), path
    assert path.endswith(os.path.join("merge-train", "state.json"))
    # The SUPPLIED env is the only one that decides: an empty one yields no path
    # rather than a path into the real user's home.
    assert mt.state_path({}) == "", "the real HOME must not be reachable from a scoped env"
    assert mt.state_path({"HOME": str(tmp_path)}).startswith(str(tmp_path))

    env = {"XDG_STATE_HOME": str(tmp_path)}
    assert mt.record_cycle_end(at=1_000.0, env=env) is True
    due, wait = mt.due_for_run({**env}, now=1_300.0)
    assert not due, "the default path has to make the floor real"
    assert wait == 86_400.0 - 300.0

    # The default is opt-out, and opting out says so rather than pretending.
    off = mt.state_path({"XDG_STATE_HOME": str(tmp_path), "MERGE_TRAIN_NO_DEFAULT_STATE": "1"})
    assert off == ""
    assert mt.record_cycle_end(at=1.0, env={"MERGE_TRAIN_NO_DEFAULT_STATE": "1"}) is False


def test_a_failed_merge_tree_is_not_a_clean_fast_forward(repo, monkeypatch):
    """`merge-tree` exits 1 for a conflict and 128 for a bad object. Treating 128
    as "no conflict" made `behind == 0` reachable from a repository nobody could
    read — an unattended push."""
    real = mt._sh
    monkeypatch.setattr(
        mt,
        "_sh",
        lambda a, **k: (
            subprocess.CompletedProcess(a, 128, "", "fatal: bad object\n")
            if a[:2] == ["git", "merge-tree"]
            else real(a, **k)
        ),
    )
    with pytest.raises(mt.MeasurementError):
        mt.classify(repo["bare"], "main", "hive/ff")
    outcome = mt.measure_repo(repo["bare"])
    assert any("merge-tree" in failure for failure in outcome.failures), outcome.failures
    assert all(report.bucket != mt.BUCKET_FAST_FORWARD for report in outcome.reports), [
        (r.branch, r.bucket) for r in outcome.reports
    ]


def test_json_output_emits_one_line_per_branch(tmp_path, capsys, repo, monkeypatch):
    """One line per repository would drop every candidate but the first — silently,
    because the output still looks well-formed."""
    monkeypatch.setattr(mt, "discover_repos", lambda *a, **k: [repo["bare"]])
    monkeypatch.setattr(mt, "record_cycle_end", lambda *a, **k: False)
    monkeypatch.delenv("MERGE_TRAIN_NO_DEFAULT_STATE", raising=False)
    assert mt.main(["--json"]) in (0, 1)
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    branches = {json.loads(line)["branch"] for line in lines}
    assert branches == {report.branch for report in mt.measure_repo(repo["bare"]).reports}


def test_exclusions_accept_bare_and_dot_git_spellings(tmp_path):
    """An exclusion that silently does nothing leaves a repository nobody meant to
    touch in the train."""
    root = tmp_path / "gitroot"
    root.mkdir()
    for name in ("openclaw.git", "riskyeats.git", "wanted.git"):
        subprocess.run(["git", "init", "-q", "--bare", str(root / name)], check=True, capture_output=True)
    for exclude in ({"openclaw"}, {"openclaw.git"}, {"OpenClaw.Git"}):
        found = [os.path.basename(path) for path in mt.discover_repos(str(root), frozenset(exclude))]
        assert "openclaw.git" not in found, exclude
        assert "riskyeats.git" in found, exclude
        assert "wanted.git" in found, exclude
    assert len(mt.discover_repos(str(root), frozenset())) == 3


def test_a_failed_diff_is_not_nothing_clobbered(repo, monkeypatch):
    """Two empty sets intersect to nothing, so a failed clobber check read as
    "nothing is clobbered" — one of the three conditions that stop a branch being
    fast-forwarded and pushed unattended."""
    real = mt._sh
    monkeypatch.setattr(
        mt,
        "_sh",
        lambda a, **k: (
            subprocess.CompletedProcess(a, 128, "", "fatal: bad object\n") if a[:2] == ["git", "diff"] else real(a, **k)
        ),
    )
    # `hive/behind` is the branch that actually REACHES the diff: a fast-forward
    # returns before it, so the clobber check only runs for a branch that is behind
    # — where a failure used to read as "nothing is clobbered" and then as a
    # cherry-pick by size.
    with pytest.raises(mt.MeasurementError):
        mt.classify(repo["bare"], "main", "hive/behind")
    outcome = mt.measure_repo(repo["bare"])
    assert any("diff --name-only" in f or "diff --numstat" in f for f in outcome.failures), outcome.failures
    measured = {report.branch: report.bucket for report in outcome.reports}
    assert measured["hive/behind"] != mt.BUCKET_CHERRY, measured["hive/behind"]


def test_a_repository_level_failure_reaches_the_record(tmp_path):
    """It is why the whole repository is skipped; a record carrying only the first
    branch's failures would let the SOP decide a branch the run never looked at."""
    bare = tmp_path / "empty.git"
    subprocess.run(["git", "init", "-q", "--bare", str(bare)], check=True, capture_output=True)
    outcome = mt.measure_repo(str(bare))
    assert outcome.failed
    record = outcome.to_record()
    assert record["failures"] == outcome.failures, record
    assert "no branch matched" in record["failures"][0]


def test_a_failed_repository_still_appears_in_the_json_output(tmp_path, capsys, monkeypatch):
    """Its failures are the whole reason it is in the output; an empty list would
    mean the record the SOP escalates on never exists."""
    monkeypatch.setattr(mt, "discover_repos", lambda *a, **k: [str(tmp_path / "not-a-repo.git")])
    monkeypatch.setattr(mt, "record_cycle_end", lambda *a, **k: False)
    assert mt.main(["--json"]) == 1
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert len(lines) == 1, lines
    record = json.loads(lines[0])
    assert record["failures"], record
    assert "not a directory" in record["failures"][0], record


def test_only_a_bucket_that_wants_a_write_asks_outward(repo, monkeypatch):
    """A `done` branch needs nothing outward, and a request for it would put a
    phantom action in front of the operator's approval queue."""
    monkeypatch.setattr(mt, "ALLOW_PUBLIC_FORGE", True)
    outward: list[dict] = []
    outcomes = mt.run_cycle(repos=[repo["bare"]], outward_reports=outward)
    measured = {report.branch: report.bucket for report in outcomes[0].reports}
    for request in outward:
        branch = next(
            (report for report in outcomes[0].reports if report.repo in request["text"]),
            None,
        )
        assert branch is not None
        assert branch.bucket in mt.OUTWARD_CANDIDATE_BUCKETS, branch
    # Nothing that is `done` or `no_base` is ever in the bucket set the engine asks
    # from, whatever the target says.
    assert mt.BUCKET_DONE not in mt.OUTWARD_CANDIDATE_BUCKETS
    assert mt.BUCKET_NO_BASE not in mt.OUTWARD_CANDIDATE_BUCKETS
    assert measured, measured


def test_an_outward_request_never_names_the_canonical_remote(repo, monkeypatch):
    """Every report's target is ARGONAS by default, and naming ARGONAS in an
    `outward_send` request would ask an operator to approve the one write that needs
    no approval."""
    monkeypatch.setattr(mt, "ALLOW_PUBLIC_FORGE", True)
    outward: list[dict] = []
    mt.run_cycle(repos=[repo["bare"]], outward_reports=outward)
    assert outward == [], outward

    # A genuinely outward target does produce a request.
    report = mt.BranchReport(repo="proj", branch="hive/x", bucket=mt.BUCKET_DIVERGENT, target="gitlab.com")
    request = mt.route_outward(report)
    assert request is not None
    assert request["target"] == "gitlab.com"


def test_a_failed_repository_enqueues_no_outward_request(tmp_path, monkeypatch):
    """A request built from a measurement that did not happen is a request for an
    action nobody can describe."""
    monkeypatch.setattr(mt, "ALLOW_PUBLIC_FORGE", True)
    outward: list[dict] = []
    mt.run_cycle(repos=[str(tmp_path / "not-a-repo.git")], outward_reports=outward)
    assert outward == [], outward
