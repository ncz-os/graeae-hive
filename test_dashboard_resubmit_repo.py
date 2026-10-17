"""Regression test: dashboard resubmit must carry the source job's repo routing.

Historic defect (2026-10-17, 03:1xZ): the dashboard 'Resubmit' and
'Triage & Resubmit' buttons on a needs-review job POSTed to /v1/jobs with
no ``repo`` (and no head_branch/required_capabilities/max_retries/
eligible_hosts/parent_job_id), so workers released the children with
``no_repo_mapping`` x3 and they dead-lettered in ~70s
(jobs 01a147d6-7ae9 triage, 01a147d6-8e2f).

Additionally, ``triageAndResubmit()`` targeted ``eligible_kinds:
['codex','claude']`` — Codex must never be a default target (operator
rule: Codex runs only on explicit request) and no 'claude' workers
exist, so every triage job dead-lettered.

Fix contract:
  * submitJob() POST body includes repo / head_branch (from the new
    #f-repo / #f-head-branch form fields, omitted when empty) and the
    hidden carry-over fields required_capabilities / max_retries /
    eligible_hosts / parent_job_id (omitted when empty);
  * the submit panel (dashboard.html) has input#f-repo and
    input#f-head-branch;
  * fillFormForEdit / resubmitFailed / triageAndResubmit pre-fill the
    form from the source job (fillFormFromJob) and set the carry-over
    (setCarryover); resubmit/triage set parent_job_id to the source id;
  * triageAndResubmit() creates a read-only triage job of kind
    'triage:<original kind>' with eligible_kinds ['zeroclaw'] and
    required_capabilities ['route:flash'], carrying repo/head_branch
    and parent_job_id — and never references 'codex' or 'claude' as an
    eligible kind;
  * submitJob warns via setStatus when resubmitting a source job that
    had a repo with the form's repo left empty.
"""

import re
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
JS_PATH = HERE / "static" / "dashboard.js"
HTML_PATH = HERE / "dashboard.html"


def _func_source(src: str, name: str) -> str:
    """Return the body of a top-level (async) function, brace-matched."""
    m = re.search(r"(?:async\s+)?function\s+" + name + r"\s*\([^)]*\)\s*\{", src)
    assert m, f"{name}() not found in dashboard.js"
    depth, i = 0, m.end() - 1
    while i < len(src):
        if src[i] == "{":
            depth += 1
        elif src[i] == "}":
            depth -= 1
            if depth == 0:
                return src[m.start():i + 1]
        i += 1
    raise AssertionError(f"unbalanced braces in {name}()")


class DashboardResubmitRepoTest(unittest.TestCase):
    def setUp(self):
        self.js = JS_PATH.read_text(encoding="utf-8")
        self.html = HTML_PATH.read_text(encoding="utf-8")
        self.submit = _func_source(self.js, "submitJob")
        self.triage = _func_source(self.js, "triageAndResubmit")
        self.resubmit = _func_source(self.js, "resubmitFailed")
        self.edit = _func_source(self.js, "fillFormForEdit")

    # ── (1) submitJob POST body ─────────────────────────────────────────
    def test_submit_job_body_includes_repo_and_routing_fields(self):
        """repo/head_branch come from the form; carry-over fields from state."""
        for field in ("repo:", "head_branch:", "required_capabilities:",
                      "max_retries:", "eligible_hosts:", "parent_job_id:"):
            self.assertIn(field, self.submit, f"submitJob body omits {field}")
        self.assertIn("f-repo", self.submit, "submitJob does not read #f-repo")
        self.assertIn("f-head-branch", self.submit, "submitJob does not read #f-head-branch")
        # repo/head_branch are omitted when empty (undefined, not "")
        self.assertRegex(self.submit, r"repo\s*:\s*repo\s*\|\|\s*undefined")
        self.assertRegex(self.submit, r"head_branch\s*:\s*headBranch\s*\|\|\s*undefined")
        # carry-over fields are omitted when absent (undefined)
        self.assertRegex(self.submit, r"required_capabilities\s*:\s*_co\s*&&\s*_co\.required_capabilities\s*\|\|\s*undefined")
        self.assertRegex(self.submit, r"parent_job_id\s*:\s*_co\s*&&\s*_co\.parent_job_id\s*\|\|\s*undefined")
        self.assertRegex(self.submit, r"eligible_hosts\s*:\s*_co\s*&&\s*_co\.eligible_hosts\s*\|\|\s*undefined")
        self.assertRegex(self.submit, r"max_retries\s*:\s*_co\s*&&\s*typeof\s+_co\.max_retries\s*===\s*'number'\s*\?\s*_co\.max_retries\s*:\s*undefined")

    # ── (1) submit panel fields ─────────────────────────────────────────
    def test_submit_panel_has_repo_and_head_branch_inputs(self):
        for fid in ("f-repo", "f-head-branch"):
            m = re.search(rf'<input[^>]*id="{fid}"[^>]*>', self.html)
            self.assertIsNotNone(m, f"submit panel missing input#{fid} in dashboard.html")
        # both live in the submit panel (before the panel's closing form-actions)
        self.assertLess(
            self.html.index('id="f-repo"'), self.html.index('id="submit-btn"'),
            "repo field not inside the submit panel")
        self.assertLess(
            self.html.index('id="f-head-branch"'), self.html.index('id="submit-btn"'),
            "head_branch field not inside the submit panel")

    # ── (1) source-job pre-fill in all three entry points ───────────────
    def test_source_job_is_copied_into_form_and_carryover(self):
        """edit/resubmit/triage all pre-fill repo routing from the source job."""
        for fn_name, fn in (("fillFormForEdit", self.edit),
                            ("resubmitFailed", self.resubmit),
                            ("triageAndResubmit", self.triage)):
            self.assertIn("fillFormFromJob", fn, f"{fn_name} does not pre-fill the form from the source job")
            self.assertIn("setCarryover", fn, f"{fn_name} does not set the hidden carry-over state")
        # fillFormFromJob actually copies repo + head_branch
        fill = _func_source(self.js, "fillFormFromJob")
        self.assertIn("job.repo", fill)
        self.assertIn("job.head_branch", fill)
        # resubmit + triage parent the new job to the source job
        self.assertRegex(self.resubmit, r"setCarryover\(j\s*,\s*\{\s*parent_job_id\s*:\s*j\.id\s*\}\s*\)")
        self.assertRegex(self.triage, r"parent_job_id\s*:\s*j\.id")

    # ── (2) triage job kind/routing ─────────────────────────────────────
    def test_triage_job_is_readonly_flash_route_never_codex_or_claude(self):
        # kind: 'triage:' + original kind, read-only and commit-gate exempt
        self.assertRegex(self.triage, r"kind\s*:\s*'triage:'\s*\+\s*\(\s*j\.kind")
        # eligible_kinds: zeroclaw only
        self.assertRegex(self.triage, r"eligible_kinds\s*:\s*\[\s*'zeroclaw'\s*\]")
        self.assertNotIn("'codex'", self.triage, "triage job targets codex by default (operator rule)")
        self.assertNotIn("'claude'", self.triage, "triage job targets nonexistent claude workers")
        self.assertNotRegex(self.triage, r"\bcodex\b", "no codex reference anywhere in triageAndResubmit")
        self.assertNotRegex(self.triage, r"\bclaude\b", "no claude reference anywhere in triageAndResubmit")
        # required_capabilities: route:flash
        self.assertRegex(self.triage, r"required_capabilities\s*:\s*\[\s*'route:flash'\s*\]")
        # carries repo/head_branch + parent from the source job
        self.assertRegex(self.triage, r"repo\s*:\s*j\.repo\s*\|\|\s*undefined")
        self.assertRegex(self.triage, r"head_branch\s*:\s*j\.head_branch\s*\|\|\s*undefined")
        # The old ['codex','claude'] array is gone from the whole file
        self.assertNotRegex(self.js, r"eligible_kinds\s*:\s*\[\s*'codex'\s*,\s*'claude'\s*\]")

    # ── (3) empty-repo warning on resubmit ──────────────────────────────
    def test_submit_warns_when_source_had_repo_but_form_repo_is_empty(self):
        self.assertIn("setStatus", self.submit)
        # detection: carry-over parent whose source job HAD a repo + repo empty
        self.assertRegex(
            self.submit,
            r"_warnedRepo\s*=\s*!!\(\s*_co\s*&&\s*_co\.source_repo\s*&&\s*repo\s*===\s*''\s*\)",
        )
        self.assertRegex(self.js, r"source_repo\s*:\s*\(\s*job\s*&&\s*job\.repo\s*\)\s*\|\|\s*''")
        # and an error-styled warning is rendered
        self.assertRegex(self.submit, r"_warnedRepo\s*\)?\s*setStatus\(.*WITHOUT a repo", re.S)
        self.assertRegex(self.submit, r"setStatus\(.*WITHOUT a repo.*,\s*true\)", re.S)

    def test_warning_is_not_wiped_by_clear_form(self):
        """clearForm() ends with setStatus(''), so it must run before the
        queued/warning messages or the operator never sees them."""
        clear_at = self.submit.index("clearForm();")
        self.assertLess(clear_at, self.submit.index("WITHOUT a repo"))
        self.assertLess(clear_at, self.submit.index("queued"))

    def test_edit_is_not_parented_and_closing_drops_carryover(self):
        self.assertIn("setCarryover(job,{parent_job_id:null})", self.js)
        self.assertIn("('parent_job_id' in opts)?opts.parent_job_id", self.js)
        toggle = self.js[self.js.index("function toggleSubmit") : self.js.index("function setStatus")]
        self.assertIn("if(!_submitOpen)cancelEdit();", toggle)

    def test_priority_zero_survives_fill(self):
        self.assertIn("job.priority??50", self.js)

    # ── sanity ──────────────────────────────────────────────────────────
    def test_js_still_parses(self):
        """Sanity: dashboard.js remains syntactically coherent (balanced braces)."""
        self.assertEqual(self.js.count("{"), self.js.count("}"))
        self.assertIn("function submitJob", self.js)
        self.assertIn("function triageAndResubmit", self.js)
        self.assertIn("function resubmitFailed", self.js)


if __name__ == "__main__":
    unittest.main()
