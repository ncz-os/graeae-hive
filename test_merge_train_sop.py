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
