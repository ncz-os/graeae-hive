"""Dashboard shows who reviewed a job (2026-10-26).

result.review_evidence (written by the worker from the zoder review JSON) is
rendered with the FIRST reviewer prominently, escalations and second reviewers
in order beneath it, the chain's skipped candidates, and any warning. Runs the
real functions from static/dashboard.js under node; skipped when node is absent.
"""

import json
import shutil
import subprocess
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
JS = (HERE / "static" / "dashboard.js").read_text(encoding="utf-8")
START = "// ── reviewer record"
END = "// ── end reviewer record"


def _run(job_or_result, fn):
    esc = next(line for line in JS.splitlines() if line.startswith("function esc("))
    section = JS[JS.index(START) : JS.index(END)]
    script = (
        esc
        + "\n"
        + section
        + f"\nconst input = {json.dumps(job_or_result)};\n"
        + f"console.log(JSON.stringify({fn}(input)));\n"
    )
    out = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=30, check=True)
    return json.loads(out.stdout)


@unittest.skipUnless(shutil.which("node"), "node not installed")
class ReviewerBlockTests(unittest.TestCase):
    def test_first_reviewer_prominent_then_secondaries_skips_and_warnings(self):
        job = {
            "result": {
                "review_evidence": {
                    "reviewers": [
                        {
                            "model": "cerberus-narrator",
                            "role": "cerberus-reviewer",
                            "display": "GPT-OSS-20B (cerberus-reviewer)",
                            "family": "openai",
                            "verdict": "request_changes",
                            "findings": {"high": 1, "total": 1},
                            "escalation_reason": None,
                        },
                        {
                            "model": "nvidia/nemotron-3-ultra-550b-a55b",
                            "display": "Nemotron 3 Ultra",
                            "family": "nvidia",
                            "verdict": "approve",
                            "findings": {"total": 0},
                            "escalation_reason": "invalid_verdict",
                        },
                    ],
                    "skipped": [{"candidate": "tydeus-reviewer", "reason": "family_excluded"}],
                    "warnings": [],
                }
            }
        }
        html = _run(job, "reviewerBlock")
        first = html.index("GPT-OSS-20B (cerberus-reviewer)")
        second = html.index("Nemotron 3 Ultra")
        self.assertLess(first, second)
        self.assertIn("font-weight:600", html[:first])  # the first one is prominent
        self.assertIn("escalated: invalid_verdict", html)
        self.assertIn("tydeus-reviewer skipped: family_excluded", html)
        self.assertIn("1 finding", html)
        # The served model id is a tooltip, never the label.
        self.assertIn('title="served model cerberus-narrator', html)

    def test_missing_named_reviewer_warns(self):
        html = _run(
            {"result": {"review_evidence": {"reviewers": [], "warnings": ["no_named_reviewer: x"]}}}, "reviewerBlock"
        )
        self.assertIn("no named reviewer recorded", html)
        self.assertIn("no_named_reviewer: x", html)

    def test_no_evidence_renders_nothing_and_text_is_escaped(self):
        self.assertEqual(_run({"result": {}}, "reviewerBlock"), "")
        html = _run(
            {"result": {"review_evidence": {"reviewers": [{"display": "<img src=x>", "verdict": "approve"}]}}},
            "reviewerBlock",
        )
        self.assertNotIn("<img", html)
        self.assertIn("&lt;img", html)

    def test_modal_text_lists_first_then_others(self):
        text = _run(
            {
                "review_evidence": {
                    "reviewers": [
                        {"display": "A (x-reviewer)", "model": "m1", "family": "f", "verdict": "approve"},
                        {"display": "B", "model": "m2", "verdict": "approve", "escalation_reason": "timeout"},
                    ],
                    "chunking": {"mode": "single", "diff_bytes": 99},
                }
            },
            "reviewerText",
        )
        self.assertTrue(text.startswith("FIRST A (x-reviewer)"))
        self.assertIn("THEN  B", text)
        self.assertIn("escalated: timeout", text)
        self.assertIn("review diff: 99 bytes, single", text)


if __name__ == "__main__":
    unittest.main()
