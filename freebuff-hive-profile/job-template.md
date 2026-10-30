# Job description template for a Freebuff session

Copy the block below into the `description` of the task file. Fill every field; a message that needs a question
back will stall, because nobody answers an unattended session.

```
GOAL
<one sentence: what is done when this job is finished>

REPO AND BRANCH
repo: <org>/<repo>
head_branch: <branch to push; create it from the base branch>
base_branch: <usually main>

STEPS
1. <first concrete step>
2. <second concrete step>

CHECK
Run exactly this and do not finish until it passes:
<one command, e.g. python3 -m pytest -q path/to/tests>

DO NOT
- touch files outside <paths>
- push to any branch other than head_branch
- ask a question: if the brief is not enough, stop and say what is missing

REPORT
When finished, say: the branch pushed, the output tail of the CHECK command, and the list of files changed.
```

Rules for a good job:

- One goal. Split anything bigger into jobs chained with `depends_on`.
- The check is a command, not an opinion. If there is no command, say how to see the result.
- Keep it short; the message is capped at 64 KiB, and long background belongs in a file in the repo.
- Privacy: only give work that is fine to send to a hosted service. It may use the data for training. No secrets,
  no customer data, no private keys, no credentials in the description or in the repo the session can read.
- Use a non-commit kind (for example `docs:*`) unless the result is reported with commit evidence. See the README
  limits.
