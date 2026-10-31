"""FB63 mutation-lite: single-operator mutants in a temp copy, never the real tree.

For each mutant: copy the repo (no .git) to a temp dir, apply exactly one
string replacement (asserted unique), run the target's test files with the
same interpreter, record KILLED (tests failed) vs SURVIVED (still green).
Prints a table. Exit 0 unless the harness itself breaks; survivors are the
deliverable, not a failure.

Usage: python scripts/mutation_lite_fb63.py [--only DECIDE|PROOF|RETRY|VERDICT] [--list]
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).parents[1]
PY = sys.executable
BASE_ARGS = ["-m", "pytest", "-q", "-p", "no:randomly", "-x"]


def m(mid, group, tests, path, old, new, note):
    return {"id": mid, "group": group, "tests": tests, "path": path, "old": old, "new": new, "note": note}


DECIDE_TESTS = ["test_doctor_loop_outcomes_20261029.py", "test_replay_loop_outcomes.py"]
PROOF_TESTS = ["test_bus_readonly_kinds_no_commit.py"]
RETRY_TESTS = ["test_auto_retry_protection.py"]
VERDICT_TESTS = ["test_review_verdict_fuzz_20261030.py", "test_doctor_verdict_completion.py"]
LOOP = "hive/workers/loop_outcome.py"

MUTANTS = [
    # ── loop_outcome.decide (D1..D12) ──
    m(
        "D1",
        "DECIDE",
        DECIDE_TESTS,
        LOOP,
        "if local_commits_only or requeues_so_far >= MAX_AUTO_REQUEUES:",
        "if local_commits_only or requeues_so_far > MAX_AUTO_REQUEUES:",
        "cap boundary off by one",
    ),
    m(
        "D2",
        "DECIDE",
        DECIDE_TESTS,
        LOOP,
        "if local_commits_only or requeues_so_far >= MAX_AUTO_REQUEUES:",
        "if requeues_so_far >= MAX_AUTO_REQUEUES:",
        "drop local-commits guard",
    ),
    m(
        "D3",
        "DECIDE",
        DECIDE_TESTS,
        LOOP,
        'if cls == CLONE_FAILED:  # transient failure to reach the git remote\n        return "needs_review" if requeues_so_far >= MAX_AUTO_REQUEUES else "requeue"',
        'if cls == CLONE_FAILED:  # transient failure to reach the git remote\n        return "needs_review" if requeues_so_far > MAX_AUTO_REQUEUES else "requeue"',
        "clone-failed cap boundary",
    ),
    m(
        "D4",
        "DECIDE",
        DECIDE_TESTS,
        LOOP,
        'if cls == CLONE_FAILED:  # transient failure to reach the git remote\n        return "needs_review" if requeues_so_far >= MAX_AUTO_REQUEUES else "requeue"',
        'if cls == CLONE_FAILED:  # transient failure to reach the git remote\n        return "requeue" if requeues_so_far >= MAX_AUTO_REQUEUES else "needs_review"',
        "swap clone-failed outcomes",
    ),
    m(
        "D5",
        "DECIDE",
        DECIDE_TESTS,
        LOOP,
        'return "requeue" if reviewer_ok() else "hold"',
        'return "requeue" if not reviewer_ok() else "hold"',
        "flip reviewer health",
    ),
    m(
        "D6",
        "DECIDE",
        DECIDE_TESTS,
        LOOP,
        "if cls in (AUTHOR_FAILED, TIMEOUT, CRASH):",
        "if cls in (AUTHOR_FAILED, TIMEOUT):",
        "drop CRASH class",
    ),
    m(
        "D7",
        "DECIDE",
        DECIDE_TESTS,
        LOOP,
        "if cls in (EXHAUSTED, NOTHING_TO_COMMIT, EMPTY_DIFF):  # EMPTY_DIFF: a retry sees the same diff",
        "if cls in (EXHAUSTED, NOTHING_TO_COMMIT):  # EMPTY_DIFF: a retry sees the same diff",
        "drop EMPTY_DIFF terminal",
    ),
    m(
        "D8",
        "DECIDE",
        DECIDE_TESTS,
        LOOP,
        'if cls == APPROVED_UNMERGED:\n        return "skip"',
        'if cls == APPROVED_UNMERGED:\n        return "default"',
        "approved-unmerged no longer skips",
    ),
    m(
        "D9",
        "DECIDE",
        DECIDE_TESTS,
        LOOP,
        '        return "skip"\n    return "default"',
        '        return "skip"\n    return "needs_review"',
        "unknown class escalates",
    ),
    m(
        "D10",
        "DECIDE",
        DECIDE_TESTS,
        LOOP,
        '        if local_commits_only or requeues_so_far >= MAX_AUTO_REQUEUES:\n            return "needs_review"\n        return "requeue"',
        '        if local_commits_only or requeues_so_far >= MAX_AUTO_REQUEUES:\n            return "needs_review"\n        return "hold"',
        "killed-row parks instead of requeue",
    ),
    m(
        "D11",
        "DECIDE",
        DECIDE_TESTS,
        LOOP,
        "if cls == REVIEWER_UNAVAILABLE:",
        "if cls != REVIEWER_UNAVAILABLE and cls in (REVIEWER_UNAVAILABLE, TIMEOUT):",
        "reviewer branch unreachable",
    ),
    m(
        "D12",
        "DECIDE",
        DECIDE_TESTS,
        LOOP,
        "if cls in (AUTHOR_FAILED, TIMEOUT, CRASH):",
        "if cls in (AUTHOR_FAILED, TIMEOUT, CRASH, REVIEWER_UNAVAILABLE):",
        "reviewer folded into plain requeue",
    ),
    # ── valid_completion_proof (P1..P12) ──
    m(
        "P1",
        "PROOF",
        PROOF_TESTS,
        "hive/bus/domain/jobs.py",
        're.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", sha)',
        're.fullmatch(r"[0-9a-f]{41}|[0-9a-f]{64}", sha)',
        "40-char sha rejected",
    ),
    m(
        "P2",
        "PROOF",
        PROOF_TESTS,
        "hive/bus/domain/jobs.py",
        "        and sha in commits\n",
        "        and True  # sha in commits\n",
        "drop head-in-commits",
    ),
    m(
        "P3",
        "PROOF",
        PROOF_TESTS,
        "hive/bus/domain/jobs.py",
        "        and isinstance(files, list)\n        and files\n",
        "        and isinstance(files, list)\n",
        "empty files_changed accepted",
    ),
    m(
        "P4",
        "PROOF",
        PROOF_TESTS,
        "hive/bus/domain/jobs.py",
        'proof.get("worktree_clean") is True',
        'proof.get("worktree_clean") is False',
        "require dirty worktree",
    ),
    m(
        "P5",
        "PROOF",
        PROOF_TESTS,
        "hive/bus/domain/jobs.py",
        '                and proof.get("pushed") is True\n',
        "",
        "drop pushed requirement",
    ),
    m(
        "P6",
        "PROOF",
        PROOF_TESTS,
        "hive/bus/domain/jobs.py",
        '                and proof.get("remote_verified") is True\n',
        '                and proof.get("remote_verified") is False\n',
        "unverified remote accepted",
    ),
    m(
        "P7",
        "PROOF",
        PROOF_TESTS,
        "hive/bus/domain/jobs.py",
        '                and proof.get("remote_sha") == sha\n',
        '                and proof.get("remote_sha") != sha\n',
        "wrong remote sha accepted",
    ),
    m(
        "P8",
        "PROOF",
        PROOF_TESTS,
        "hive/bus/domain/jobs.py",
        'and all(isinstance(c, str) and re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", c) for c in commits)',
        'and any(isinstance(c, str) and re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", c) for c in commits)',
        "one valid commit suffices",
    ),
    m(
        "P9",
        "PROOF",
        PROOF_TESTS,
        "hive/bus/domain/jobs.py",
        "        and all(isinstance(f, str) and f for f in files)\n",
        "",
        "drop non-empty filenames",
    ),
    m(
        "P10",
        "PROOF",
        PROOF_TESTS,
        "hive/bus/domain/jobs.py",
        "        and isinstance(commits, list)\n",
        "        and isinstance(commits, tuple)\n",
        "commits must be tuple",
    ),
    m(
        "P11",
        "PROOF",
        PROOF_TESTS,
        "hive/bus/domain/jobs.py",
        're.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", c) for c in commits)',
        're.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{63}", c) for c in commits)',
        "64-char commit rejected",
    ),
    m(
        "P12",
        "PROOF",
        PROOF_TESTS,
        "hive/bus/domain/jobs.py",
        '                and isinstance(proof.get("untracked_files"), list)\n',
        "",
        "drop untracked list check",
    ),
    # ── auto_retry_allowed (R1..R12) ──
    m(
        "R1",
        "RETRY",
        RETRY_TESTS,
        "retry_policy.py",
        'if job.get("status") != "failed":',
        'if job.get("status") == "failed":',
        "non-failed passes status gate",
    ),
    m(
        "R2",
        "RETRY",
        RETRY_TESTS,
        "retry_policy.py",
        "return isinstance(val, int) and not isinstance(val, bool) and val > 0",
        "return isinstance(val, int) and not isinstance(val, bool) and val >= 0",
        "zero max_retries allowed",
    ),
    m(
        "R3",
        "RETRY",
        RETRY_TESTS,
        "retry_policy.py",
        "return isinstance(val, int) and not isinstance(val, bool) and val >= 0",
        "return isinstance(val, int) and not isinstance(val, bool) and val > 0",
        "zero retry_count rejected",
    ),
    m(
        "R4",
        "RETRY",
        RETRY_TESTS,
        "retry_policy.py",
        "    if retry_count >= max_retries:",
        "    if retry_count > max_retries:",
        "one extra retry past budget",
    ),
    m(
        "R5",
        "RETRY",
        RETRY_TESTS,
        "retry_policy.py",
        '        if result.get("retryable") is not True:\n            return False',
        '        if result.get("retryable") is True:\n            return False',
        "build retryable inverted",
    ),
    m(
        "R6",
        "RETRY",
        RETRY_TESTS,
        "retry_policy.py",
        '        if isinstance(result, dict) and result.get("retryable") is False:\n            return False',
        "",
        "explicit opt-out ignored",
    ),
    m(
        "R7",
        "RETRY",
        RETRY_TESTS,
        "retry_policy.py",
        "    if not isinstance(result, dict):\n        return True",
        "    if not isinstance(result, dict):\n        return False",
        "non-dict result allowed",
    ),
    m(
        "R8",
        "RETRY",
        RETRY_TESTS,
        "retry_policy.py",
        '        if val is not None and val is not False and val != "":',
        "        if val is not None and val is not False:",
        "empty-string hold fires",
    ),
    m(
        "R9",
        "RETRY",
        RETRY_TESTS,
        "retry_policy.py",
        '            if norm in ("hold", "held") or any(',
        "            if any(",
        "bare hold no longer blocks",
    ),
    m(
        "R10",
        "RETRY",
        RETRY_TESTS,
        "retry_policy.py",
        "    return isinstance(val, int) and not isinstance(val, bool) and val > 0\n\n\ndef _is_non_negative_int",
        "    return isinstance(val, int) and val > 0\n\n\ndef _is_non_negative_int",
        "bool counts as positive int",
    ),
    m(
        "R11",
        "RETRY",
        RETRY_TESTS,
        "retry_policy.py",
        '    if kind.startswith("build"):',
        '    if kind.startswith("buildx"):',
        "build kinds take lenient path",
    ),
    m(
        "R12",
        "RETRY",
        RETRY_TESTS,
        "retry_policy.py",
        '    else:\n        # Non-build failed jobs may preserve retry eligibility only when budget and hold checks pass.\n        # Explicit retryable=False blocks any kind.\n        if isinstance(result, dict) and result.get("retryable") is False:\n            return False\n\n    return True',
        '    else:\n        # Non-build failed jobs may preserve retry eligibility only when budget and hold checks pass.\n        # Explicit retryable=False blocks any kind.\n        if isinstance(result, dict) and result.get("retryable") is False:\n            return False\n\n    return False',
        "blanket deny",
    ),
    # ── review_verdict.row_verdict (V1..V12) ──
    m(
        "V1",
        "VERDICT",
        VERDICT_TESTS,
        "hive/review_verdict.py",
        '        if text and (text != "approve" or named_model(reviewer.get("model"))):\n            return text\n    evidence = result.get("review_evidence")',
        '        if text:\n            return text\n    evidence = result.get("review_evidence")',
        "model-less reviewer approve surfaces",
    ),
    m(
        "V2",
        "VERDICT",
        VERDICT_TESTS,
        "hive/review_verdict.py",
        '            if text and (text != "approve" or named_model(verdict.get("model"))):\n                return text',
        "            if text:\n                return text",
        "model-less evidence approve surfaces",
    ),
    m(
        "V3",
        "VERDICT",
        VERDICT_TESTS,
        "hive/review_verdict.py",
        '            if text and text != "approve":\n                return text',
        "            if text:\n                return text",
        "bare approve string surfaces",
    ),
    m(
        "V4",
        "VERDICT",
        VERDICT_TESTS,
        "hive/review_verdict.py",
        '    return (text_value(value) or "").lower().replace("-", "_")',
        '    return (text_value(value) or "").lower().replace("_", "-")',
        "verdict dashes flipped",
    ),
    m(
        "V5",
        "VERDICT",
        VERDICT_TESTS,
        "hive/review_verdict.py",
        '        for entry in (evidence.get("first"), *(evidence.get("reviewers") or [])):',
        '        for entry in (*(evidence.get("reviewers") or []),):',
        "first reviewer entry ignored",
    ),
    m(
        "V6",
        "VERDICT",
        VERDICT_TESTS,
        "hive/review_verdict.py",
        "            if not isinstance(entry, dict):\n                continue",
        "            if not isinstance(entry, dict):\n                return text",
        "non-dict entry returns garbage",
    ),
    m(
        "V7",
        "VERDICT",
        VERDICT_TESTS,
        "hive/review_verdict.py",
        '        for entry in (evidence.get("first"), *(evidence.get("reviewers") or [])):\n            if not isinstance(entry, dict):\n                continue\n            text = normalize_verdict(entry.get("verdict"))\n            if text and (text != "approve" or named_model(entry.get("model"))):\n                return text\n    return ""',
        '        for entry in (evidence.get("first"), *(evidence.get("reviewers") or [])):\n            if not isinstance(entry, dict):\n                continue\n            text = normalize_verdict(entry.get("verdict"))\n            if text and (text != "approve" or named_model(entry.get("model"))):\n                return text\n    return "approve"',
        "no verdict defaults approve",
    ),
    m(
        "V8",
        "VERDICT",
        VERDICT_TESTS,
        "hive/review_verdict.py",
        '    reviewer = result.get("reviewer")\n    if isinstance(reviewer, dict):\n        text = normalize_verdict(reviewer.get("verdict"))',
        '    reviewer = result.get("reviewer")\n    if True:  # isinstance(reviewer, dict):\n        text = normalize_verdict(reviewer.get("verdict"))',
        "reviewer gate removed (may raise)",
    ),
    m(
        "V9",
        "VERDICT",
        VERDICT_TESTS,
        "hive/review_verdict.py",
        '            text = normalize_verdict(entry.get("verdict"))\n            if text and (text != "approve" or named_model(entry.get("model"))):',
        '            text = normalize_verdict(entry.get("verdict"))\n            if text:',
        "model-less loop approve surfaces",
    ),
    m(
        "V10",
        "VERDICT",
        VERDICT_TESTS,
        "hive/review_verdict.py",
        '    return (text_value(value) or "").lower().replace("-", "_")',
        '    return (text_value(value) or "").replace("-", "_")',
        "verdict case no longer folded",
    ),
    m(
        "V11",
        "VERDICT",
        VERDICT_TESTS,
        "hive/review_verdict.py",
        '    reviewer = result.get("reviewer")\n    if isinstance(reviewer, dict):\n        text = normalize_verdict(reviewer.get("verdict"))\n        if text and (text != "approve" or named_model(reviewer.get("model"))):',
        '    reviewer = result.get("reviewer")\n    if isinstance(reviewer, list):\n        text = normalize_verdict(reviewer.get("verdict"))\n        if text and (text != "approve" or named_model(reviewer.get("model"))):',
        "reviewer dict never read",
    ),
    m(
        "V12",
        "VERDICT",
        VERDICT_TESTS,
        "hive/review_verdict.py",
        '        else:\n            text = normalize_verdict(verdict)\n            if text and text != "approve":\n                return text',
        "        else:\n            text = normalize_verdict(verdict)\n            if text:\n                return text",
        "bare evidence approve surfaces",
    ),
]


KILLERS = ["test_mutation_killers_20261031.py"]


def run_one(mutant: dict, with_killers: bool = False) -> str:
    tmp = Path(tempfile.mkdtemp(prefix="fb63mut_"))
    try:
        shutil.copytree(
            ROOT, tmp / "repo", ignore=shutil.ignore_patterns(".git", "__pycache__", ".pytest_cache"), symlinks=False
        )
        target = tmp / "repo" / mutant["path"]
        text = target.read_text()
        if text.count(mutant["old"]) != 1:
            return f"HARNESS-FAIL: {mutant['old']!r} occurs {text.count(mutant['old'])}x"
        target.write_text(text.replace(mutant["old"], mutant["new"]))
        tests = [*mutant["tests"], *KILLERS] if with_killers else mutant["tests"]
        proc = subprocess.run(
            [PY, *BASE_ARGS, *tests],
            cwd=tmp / "repo",
            capture_output=True,
            text=True,
            timeout=180,
            env={
                "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
                "HOME": "/tmp",
                "PYTHONPATH": str(tmp / "repo"),
                "PYTHONDONTWRITEBYTECODE": "1",
            },
        )
        if proc.returncode == 0:
            return "SURVIVED"
        return "KILLED"
    except subprocess.TimeoutExpired:
        return "TIMEOUT(=survived?)"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> None:
    args = set(sys.argv[1:])
    if "--list" in args:
        for m_ in MUTANTS:
            print(f"{m_['id']:>4}  [{m_['group']:<7}] {m_['note']}")
        return
    only = None
    ids = None
    for a in args:
        if a.startswith("--only="):
            only = a.split("=", 1)[1]
        if a.startswith("--ids="):
            ids = set(a.split("=", 1)[1].split(","))
    with_killers = "--with-killers" in args
    results = []
    for m_ in MUTANTS:
        if only and m_["group"] != only:
            continue
        if ids and m_["id"] not in ids:
            continue
        status = run_one(m_, with_killers)
        results.append((m_["id"], m_["group"], status, m_["note"]))
        print(f"{m_['id']:>4}  {status:<12} {m_['note']}", flush=True)
    print()
    survived = [r for r in results if r[2] != "KILLED"]
    print(f"killed={len(results) - len(survived)} survived={len(survived)} total={len(results)}")
    for sid, group, status, note in survived:
        print(f"  SURVIVOR {sid} [{group}] {status}: {note}")


if __name__ == "__main__":
    main()
