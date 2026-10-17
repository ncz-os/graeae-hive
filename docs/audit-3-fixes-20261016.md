# Independent adversarial audit — three hive fixes live on the bus (2026-10-16)

**Auditor:** independent review (adversarial, read-only against the fix branches)
**Date:** 2026-10-16  **Repo:** graeae-hive
**Branches under review:**

| # | Branch | Change under review |
|---|--------|---------------------|
| A | `fix/hive-unresolvable-release-20261016` | worker job-level release reasons count toward dead-letter |
| B | `fix/hive-completion-gate-noncommit-20261016` | read-only/noncommit kinds exempt from commit evidence |
| C | `fix/hive-unknown-agent-409-20261016` | unknown agent URN returns 409 so workers re-register |

**Verdict:** A is directionally right but shipped a live false-dead-letter hazard
(BLOCKING-1 — since remediated, see below). B contains a real commit-gate bypass (BLOCKING-2). C regresses the
doctor's heartbeat self-heal (BLOCKING-3). Full suite is green — none of these are
caught by tests.

No source file of the three fixes was modified by this audit (read-only mandate);
findings below carry the exact fix.

### Remediation status (branch `fix/hive-unknown-agent-409-20261016`)

**BLOCKING-1 — FIXED.** `no_repo_mapping` no longer counts toward the
dead-letter threshold on its own; it counts **only** when the job's repo is
genuinely absent from the bus catalog. The catalog is consulted **in-process**
(`hive.repos` over `data/repos.json`) — no hot-path HTTP.

| Condition | Counts? | Threshold | Outcome |
|---|---|---|---|
| repo present in catalog + `no_repo_mapping` | **no** | — | requeued / routed (stale worker, deploy drift) |
| repo absent from catalog + `no_repo_mapping` | yes | `NO_REPO_MAPPING_DECLINE_BUDGET` (20) | bounded, operator-visible dead-letter |
| job has no repo + `no_repo_mapping` | yes | `NO_REPO_MAPPING_DECLINE_BUDGET` (20) | bounded dead-letter |
| `malformed_job_id` / `invalid_head_branch` | yes | `MAX_DECLINE_REQUEUES` (3) | unchanged |

- `hive/bus/domain/jobs.py`: `JOB_LEVEL_DECLINE_REASONS` (malformed_job_id,
  invalid_head_branch) is excluded from `DECLINE_REASON_PREFIXES`;
  `is_unresolvable_release(reason, *, job_repo=None)` is now a pure classifier;
  new `repo_in_catalog()` reads the in-process catalog and **fails open** if the
  catalog is unreadable (never dead-letter a repo we could not look up).
- `hive/bus/routers/jobs.py`: classification and the count-threshold are
  separate decisions — a catalog-present `no_repo_mapping` is dropped (job
  requeues), a catalog-absent one increments `decline_count` against the larger
  budget. The dead-letter event reports the threshold actually applied.
- Tests: `test_audit_3_fixes_20261016.py` (unit + end-to-end through
  claimed→queued) and `test_decline_reasons.py` assert present/absent counting.

---

## Executive summary

| ID | Severity | Fix | Finding |
|----|----------|-----|---------|
| BLOCKING-1 | High | A | **FIXED** — `no_repo_mapping` is counted as job-level-unresolvable, but it is a **worker-deployment** condition. The worker resolves 16 repos; the bus catalog has 123. Every job naming one of the other 107 repos is dead-lettered in **~70 s** while the registry-resolution fix is still only "recommended". Legitimate jobs are killed. |
| BLOCKING-2 | High | B | `kind_requires_commit` exempts by `kind.startswith(prefix)` against prefixes that have **no delimiter** (`review`, `analysis`, `design`, `research`, `triage`, `investigation`, `architecture`). A code job named `analysis-pipeline-fix` / `reviewer-migration` / `design-system-update` skips the commit-evidence gate **even with a repo set**. Verified end-to-end: such a job reaches `done` with an empty result payload. |
| BLOCKING-3 | Medium | C | The 409 change breaks the doctor's heartbeat self-heal: `hive/workers/doctor.py:291` re-registers only on `code == 404`. A ghost doctor's heartbeat now returns 409, self-heal never fires, and the doctor silently spins forever — the exact zombie the fix set out to cure, moved to another process. |
| ADVISORY-1 | Low | C | 409 conflates "unknown agent" with "agent exists but is not online/idle" on `/v1/jobs/next`. `hive_worker.py:1523` re-registers on *any* 409, so an intentional drain (`status=offline`) is undone by the worker itself. Pre-existing, but the fix widens its blast radius. |
| ADVISORY-2 | Low | C | `hive/workers/doctor.py:303` treats non-`(0,204,404)` as noteworthy, so an unregistered doctor now logs a claim warning on every poll. |

---

## Fix A — unresolvable worker releases count toward dead-letter

**Files:** `hive/bus/domain/jobs.py:100-118`, `hive/bus/routers/jobs.py:733-736,889-911`

### What is correct

- The refactor is clean: `is_unresolvable_release()` (`hive/bus/domain/jobs.py:114`)
  is a faithful extraction of the previous inline expression at
  `hive/bus/routers/jobs.py:733`, semantics unchanged for the two pre-existing
  prefixes.
- The thrash the fix targets is real. Before it, a job released with
  `worker_error=no_repo_mapping` requeued forever — `decline_count` stayed 0 so the
  dead-letter threshold at `MAX_DECLINE_REQUEUES=3`
  (`hive/bus/domain/jobs.py:99`, applied `hive/bus/routers/jobs.py:891`) could never
  be reached. An unbounded requeue loop is strictly worse than a terminal state.
- The guard is correctly *narrow*: the decline counter only advances on
  `release_to_queue` (`hive/bus/routers/jobs.py:888`), i.e. a genuine
  claimed→queued release by the current claimant. A worker cannot burn another
  agent's decline budget, and a status field that merely happens to read `queued`
  cannot.
- `host_declines_kind` / `released_by_host` correctly stay out — those are
  per-host routing signals and counting them would false-dead-letter a job that a
  *different* host could run. `test_decline_reasons.py:33` pins this.

### BLOCKING-1 — false dead-letter of legitimate jobs (~70 s)

The fix's premise is that the three reasons are **job-level**. Two of the three are
(`malformed_job_id`, `invalid_head_branch` — a property of the job's own id/branch
field, `hive_worker.py:1205,1246`). **`no_repo_mapping` is not.**

It is emitted at `hive_worker.py:1214` when `resolve_repo_url()` misses. That
function consults `REPO_MAP`, a hardcoded literal in the deployed worker file
(`hive_worker.py:~450-470`) with **16 entries**. The bus's authoritative,
auto-updating catalog (`hive/registry.py`, served at `GET /v1/repos`) holds
**123**. Measured:

```
worker REPO_MAP entries:            16
bus catalog entries:               123
in catalog, NOT in worker map:     107
```

So `no_repo_mapping` is a statement about *this host's deployed worker file*, not
about the job. It is exactly as deployment-dependent as the fleet's worker version
skew — and `docs/investigation-no-repo-mapping-20261016.md` §4 documents that the
live worker blob matches no committed blob, i.e. on-host drift is already present.

The compounding factor is the release backoff at
`hive/bus/routers/jobs.py:846` (`5.0 * 2**retry_count`, capped 300 s). Three
declines land at:

```
decline 1: backoff  10.0s
decline 2: backoff  20.0s   (cumulative  30s)
decline 3: backoff  40.0s   (cumulative  70s)
```

**70 seconds.** That is the entire grace period a fleet operator has to notice a
wrongly-dead-lettered job and use the admin override at
`hive/bus/routers/admin.py:261`. The registry-resolution fix that would make
`no_repo_mapping` genuinely job-level is listed as **"recommended"** in the
investigation doc's own fix table — i.e. not shipped. Shipping the dead-letter
counter before its prerequisite inverts the safe order: today an unmapped job
loudly requeues forever (bad but recoverable and visible); after this change it
silently dies in a minute.

The live symptom that motivated the fix — `retry_count=5`, `status=queued`,
`worker_error=no_repo_mapping` — is a *fix-the-worker* signal. Routing it to a
terminal state converts a deployment bug into data loss.

**Required fix** — keep the thrash bound, but do not terminate on a
deployment-dependent reason:

```python
# hive/bus/domain/jobs.py
# Reasons that are a property of the JOB alone: no worker on any host can
# ever run it. Deployment-dependent reasons are deliberately excluded.
DECLINE_REASON_PREFIXES: tuple[str, ...] = (
    "no_workspace_for_kind",
    "no_workspace_for_repo",
    "malformed_job_id",
    "invalid_head_branch",
)
```

and give `no_repo_mapping` a *bounded, operator-visible* home instead of silence —
either a much larger dedicated budget, or (preferred, and what the investigation
doc already recommends) land the `/v1/repos/{id}` resolution in the worker first
so the reason genuinely becomes job-level and can then join this tuple. Keep the
existing `retry_count` climb so the job still cannot hot-loop.

### Regressions checked and cleared

- **Counter abuse:** a worker PATCHing `worker_error` on a job it does not hold is
  rejected upstream — `claimed_by` must equal the current claimant
  (`hive/bus/routers/jobs.py:795-803`). Cannot dead-letter someone else's job.
- **Dead-letter state machine:** `dead-letter` is terminal and self-transition-only
  (`hive/bus/domain/jobs.py:149`), is in `TERMINAL_JOB_STATUSES`
  (`hive/bus/deps.py:162`), and the CAS uses `expected_status="queued"`
  (`hive/bus/routers/jobs.py:908`) so it cannot clobber a job that a racing worker
  has already claimed. Correct.
- **Evidence retention:** the dead-letter payload keeps `last_decline_result`
  (`hive/bus/routers/jobs.py:898`), so the operator still sees why. Correct.
- **Recovery path exists:** `hive/bus/routers/admin.py:261` overrides
  dead-letter → a reviewable status. Correct — but manual, hence the 70 s finding.

---

## Fix B — read-only kinds exempt from the commit-evidence gate

**Files:** `hive/bus/domain/jobs.py:154-172`, `hive/bus/routers/jobs.py:817-824`

### What is correct

- The motivating regression is real and the diagnosis is right. `commit_required`
  keyed off `bool(job_repo)` alone, so a `review:...` job that set `repo` (to clone
  the code under review) was forced through `valid_completion_proof()` and landed in
  `needs-review` with `failure_reason=completion_evidence_missing` despite a clean
  run. Extracting the predicate to a testable pure function is the right shape.
- Genuinely read-only kinds are exempted correctly. Verified end-to-end that
  `docs:`, `analysis:`, `investigation:` etc. now complete, while `feat:knemon-x`
  and `fix:codex-x` still correctly go to `needs-review`.
- The extraction is behaviour-preserving for every code kind: the `riskyeats`/`code`
  exact-match and the ten code prefixes are copied verbatim from the original
  (`hive/bus/routers/jobs.py:812-819` pre-change).
- Reusing `ADMISSION_NONCOMMIT_PREFIXES` rather than re-listing kinds is good
  hygiene — the admission-time and completion-time notions of "noncommit" stay in
  sync.

### BLOCKING-2 — a code job can skip the commit gate via its kind name

`ADMISSION_NONCOMMIT_PREFIXES` (`hive/bus/domain/workspace.py:32-47`) mixes two
kinds of entry:

```
delimited:  "docs:", "track:", "ops:", "diag:", "ping:"
bare:       "review", "architecture", "triage",
            "investigation", "hive-stats", "dream-walker",
            "research", "analysis", "design"
```

That distinction is invisible at admission (where a bare prefix is a deliberate
"kind family starts with this word" shorthand) but **is** the whole security
property here. `kind_requires_commit` applies
`any(k.startswith(p) for p in ADMISSION_NONCOMMIT_PREFIXES)`
(`hive/bus/domain/jobs.py:164`) with no delimiter boundary and no repo
qualification, so any kind merely *beginning* with one of the bare words is
exempt — including a kind that carries code work. Measured:

```
'review:foo'            requires_commit=False   (intended)
'reviewer-migration'    requires_commit=False   <-- BYPASS
'analysis-pipeline-fix' requires_commit=False   <-- BYPASS
'design-system-update'  requires_commit=False   <-- BYPASS
'research-notes'        requires_commit=False   <-- BYPASS
'triage-build'          requires_commit=False   <-- BYPASS
'investigation-x'       requires_commit=False   <-- BYPASS
'architecture-doc'      requires_commit=False   <-- BYPASS
```

Confirmed end-to-end against the real bus (register → submit → claim → PATCH
`status=done` with an **empty** result — no `head_sha`, no `commits`, no
`files_changed`, `worktree_clean` absent — and `repo=graeae-hive` set):

```
  feat:knemon-x              -> ('needs-review', 'completion_evidence_missing')   gated, correct
  fix:codex-x                -> ('needs-review', 'completion_evidence_missing')   gated, correct
  docs:ship-code             -> ('done', None)                                     exempt
  analysis-pipeline-fix      -> ('done', None)                                     BYPASS
  reviewer-migration         -> ('done', None)                                     BYPASS
  design-system-update       -> ('done', None)                                     BYPASS
  research-notes             -> ('done', None)                                     BYPASS
  track:thing                -> ('done', None)                                     exempt
  triage-build               -> ('done', None)                                     BYPASS
  investigation-x            -> ('done', None)                                     BYPASS
```

A job that produced no commit at all reports `done` with `failure_reason=None`. The
gate exists precisely to make that impossible, and it is now bypassable by choosing
a kind name. Note the asymmetry that makes this sharp: the submitter picks the kind,
so this is not an accidental collision — `analysis-pipeline-fix` is a perfectly
natural name for a real code change, and nothing in the submit path warns.

There is a **second, quieter regression** in the same three lines: the exemption is
unqualified by `repo`. `review:...` jobs that legitimately *do* clone and commit
(review-with-a-fix, `docs:`-that-edits-files) now bypass the gate too — the fix
trades one false rejection for a class of false acceptances.

**Required fix** — exempt read-only kinds, but (a) require a token boundary so a
bare prefix cannot swallow an unrelated kind, and (b) keep the gate whenever a repo
is actually present and the run could have committed:

```python
# hive/bus/domain/jobs.py
_READONLY_PREFIXES = tuple(
    p for p in ADMISSION_NONCOMMIT_PREFIXES
)
# Only treat a kind as read-only at a TOKEN boundary: "review" must match
# "review:..." or "review", never "reviewer-migration".
def _is_readonly_kind(kind: str) -> bool:
    k = str(kind or "")
    return any(k == p or k.startswith(p) and (p.endswith(":") or k[len(p)] == ":")
               for p in _READONLY_PREFIXES)

def kind_requires_commit(job_kind: str, job_repo: str | None) -> bool:
    k = str(job_kind or "")
    if not job_repo and _is_readonly_kind(k):
        return False
    return (bool(job_repo) or k in ("riskyeats", "code") or k.startswith(...))
```

The `not job_repo` qualifier is the deliberate second half: a read-only kind that
*did* resolve a repo is being asked to clone, and cloning means it can commit, so
it must be able to prove it. This keeps the original bug fixed (a repo-less
`review:` job is exempt — which was the actual regression) while closing the
bypass. If a stricter posture is wanted, invert it: require `commit_required` for
read-only kinds with a repo and accept that such jobs must produce a commit.

Add a test that asserts the *negative* cases (`analysis-pipeline-fix` with a repo
**must** require commit evidence). `test_completion_gate.py` currently asserts only
the intended exemptions, which is why the bypass is green.

### Regressions checked and cleared

- **Code jobs cannot skip the gate.** Every code prefix (`feat:`, `fix:`,
  `mnemos:`, `riskyeats:`, `ic-engine`, `investorclaw`, `ncz-os`, `argonaut:`,
  `riskybiz:`) is disjoint from the read-only set — verified. The only bypass is
  via a read-only-prefixed *name*, per BLOCKING-2.
- **The `hallucination_guard` is unaffected** — it runs before the commit gate
  (`hive/bus/routers/jobs.py:710-724`) and still forces `exit_code=-2` on a
  `done` PATCH for all kinds. Good: a read-only job cannot launder a hallucinated
  claim through the exemption.
- **No import cycle:** `domain/jobs.py` → `domain/workspace.py` is a leaf import
  (`workspace.py` imports nothing from `domain`). Suite confirms.
- **Repo-less generic kinds** (`kind_requires_commit("some-generic-kind", None)`)
  stay exempt — unchanged from before, not a regression.

---

## Fix C — unknown agent URN returns 409

**Files:** `hive/bus/routers/agents.py:229-233`, `hive/bus/routers/jobs.py:462-465,1145-1146`

### What is correct

- The premise is right. `hive_worker.py:1523-1526` re-registers on **409 only**; a
  404 fell through to `time.sleep(POLL_INTERVAL)` and the worker polled forever
  against a URN the bus had dropped. The hydra zombie is real.
- The change is applied at all three sites that gate on agent existence —
  heartbeat (`agents.py:229`), dequeue (`jobs.py:462`), targeted claim
  (`jobs.py:1145`) — so the three stay consistent. Consistency here matters: a
  worker that healed on claim but not on heartbeat would flap.
- 409 is semantically defensible: the request conflicts with current server state
  (the registration the client believes it holds is gone), which is exactly what
  "re-register" means. 404 ("no such resource") was the weaker choice.
- `test_agent_lifecycle.py:166-175` covers both new 409 sites.
- It does **not** mask the neighbouring real error: the "status is not
  online/idle" 409 at `jobs.py:474` and `jobs.py:1154` is a separate, correctly
  distinct branch reached only when `agent_row` *exists*. Verified: an unknown URN
  yields `agent not registered`, a `stale` agent yields `agent status is 'stale'`.

### BLOCKING-3 — the doctor's heartbeat self-heal is now dead code

The fix's stated intent is "so workers re-register". It works for `hive_worker.py`
because that client's re-register trigger is 409. **The doctor keys on 404:**

```python
# hive/workers/doctor.py:289-293
def heartbeat():
    code, _ = _http("POST", "/v1/agents/heartbeat", {"urn": _urn, "status": "online"})
    if code == 404:
        log.warning("heartbeat 404 — re-registering")
        register()
```

Verified against the real bus after the change:

```
heartbeat(unknown) -> 409
claim(unknown)     -> 409
doctor.py:291 keys on 404 -> self-heal BROKEN (now 409)
```

So when the bus drops a doctor's URN, the doctor's `main()` loop
(`doctor.py:1370-1371`) calls `heartbeat()`, gets 409, takes **no** branch, and
continues — forever. Previously it self-healed on 404. The zombie-worker failure
mode was fixed for `hive_worker.py` and *introduced* for the doctor.

Note the doctor's `claim_next_job()` was already 409-blind in the same way but did
not regress, because `/v1/jobs/next` already returned 409 for the not-online case
before this fix — the doctor's claim path had no 404 self-heal to lose.

**Required fix** — teach the doctor's self-heal both codes (one line, and it is
strictly more robust than before):

```python
# hive/workers/doctor.py:291
if code in (404, 409):
    log.warning("heartbeat %s — re-registering", code)
    register()
```

and update `claim_next_job()`'s noteworthy set at `doctor.py:303` to match.

Two further client surfaces were checked and are **not** broken, but should be
audited on the next deploy:

- `hive/harness/generic.py:150-152` — `heartbeat()` raises on any non-200/204.
  Under the old 404 it raised too, so no regression, but it now raises on a code
  that means "re-register"; there is no re-register on that path at all.
- `tests/fixtures/zeroclaw_wss_worker_v2.py:334` — returns a bare bool, ignores the
  code. No regression.

### ADVISORY-1 — 409 now conflates drain with unknown-agent

`hive_worker.py:1523` re-registers on **any** 409 from `/v1/jobs/next`. That branch
previously meant only "you are not online/idle"; it now also means "you don't
exist". The worker cannot tell them apart and self-heals in both cases. So an
operator drain — `POST /v1/agents/{urn}` with `status=offline`
(`hive/bus/routers/agents.py:239-260`) — is undone within one poll interval by the
worker re-registering and setting itself back to `online`
(`hive/persistence/sqlite.py:473`, `status='online'` hardcoded in the INSERT).

This is pre-existing behaviour, but the fix widens it: an unregistered agent and a
deliberately-drained agent are now indistinguishable at the client. Recommended
follow-up (non-blocking): make the worker re-register only when the 409 body
carries `agent not registered`, and leave a drained agent drained.

---

## Suite

```
$ /usr/bin/python3 -m pytest -q
............................................sssssss................ [ 34%]
..........ss........................................... [ 62%]
................................................................. [ 96%]
.......                                                                  [100%]
```

**All green.** Note what that means for this audit: none of BLOCKING-1/2/3 is
detected by the existing suite. `test_decline_reasons.py` and
`test_completion_gate.py` test only the *intended* direction of each change; there
is no negative test for a code job wearing a read-only kind name, no test that a
`no_repo_mapping` job is still recoverable, and no test that a 409 heartbeating
client re-registers.

### Recommended test additions

1. `test_completion_gate.py` — `analysis-pipeline-fix` / `reviewer-migration` /
   `design-system-update` **with a repo** must require commit evidence. (Closes
   BLOCKING-2.)
2. `test_decline_reasons.py` — a `no_repo_mapping` release must NOT drive a job to
   `dead-letter` within the backoff window, or an explicit test pinning the chosen
   budget. (Closes BLOCKING-1.)
3. `test_agent_lifecycle.py` — a client whose heartbeat receives 409 must
   re-register; assert via the doctor, or add a shared "unregistered client
   self-heals" contract test both clients implement. (Closes BLOCKING-3.)

---

## Recommended merge order

1. **Fix BLOCKING-3** (doctor one-liner) — smallest, no behavioural risk.
2. **Fix BLOCKING-2** (token boundary + repo qualifier in `kind_requires_commit`),
   with the negative tests.
3. **Land the worker `/v1/repos/{id}` resolution** so `no_repo_mapping` becomes
   genuinely job-level.
4. **Only then** let `no_repo_mapping` count toward dead-letter (BLOCKING-1).

Fixes B and C as they stand are worth landing after items 1–2; fix A should not be
promoted to a deploy branch before item 3.

## Reproduce

```sh
# BLOCKING-2 (no source change needed — empty proof, repo set)
/usr/bin/python3 -m pytest -q test_completion_gate.py   # green: bypass is untested
# and directly:
#   register -> submit kind="analysis-pipeline-fix" repo="graeae-hive"
#     -> claim -> PATCH status=done result={"exit_code":0}   => status "done"

# BLOCKING-1 (worker vs catalog drift)
/usr/bin/python3 -c "import json;print(len(json.load(open('data/repos.json'))['repos']))"
grep -c '^\s*"[a-z0-9-]*":\s*"ssh' /opt/graeae/hive_worker.py

# BLOCKING-3
sed -n '289,293p' hive/workers/doctor.py     # keys on 404, bus now returns 409
```