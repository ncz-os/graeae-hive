"""Regression test: dashboard GPU panel must not render a red dot for a vendor.

Historic defect (2026-10-17): ``refreshGPU()`` in static/dashboard.js mapped
vendor to emoji dots (nvidia=🟢, amd=🔴, other=⚪), so every healthy AMD GPU
(HYDRA, ACHILLES) showed a RED dot that operators read as a failure.

Fix contract:
  * the GPU vendor is rendered as a neutral text badge (<span class="gpu-vendor">),
    styled with a neutral color in static/dashboard.css — never red;
  * a separate health dot (gpu-health-dot) is green when the GPU reported
    runtime metrics in the latest heartbeat (row present with a real util_pct),
    grey when there is no runtime data — and is never red;
  * a zero/unknown VRAM total (mem_total_mib 0 or null) renders '-' instead of
    '0.0/0.0 GiB'.
"""

import json
import re
import subprocess
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
JS_PATH = HERE / "static" / "dashboard.js"
CSS_PATH = HERE / "static" / "dashboard.css"


def _gpu_func_source(src: str) -> str:
    """Return the body of the refreshGPU() function (brace-matched)."""
    m = re.search(r"(async\s+)?function\s+refreshGPU\s*\(\s*\)\s*\{", src)
    assert m, "refreshGPU() not found in dashboard.js"
    depth, i = 0, m.end() - 1
    while i < len(src):
        if src[i] == "{":
            depth += 1
        elif src[i] == "}":
            depth -= 1
            if depth == 0:
                return src[m.start():i + 1]
        i += 1
    raise AssertionError("unbalanced braces in refreshGPU()")


def _js_strings(node_src: str):
    """Yield the literal text of all JS string literals (single/double/backtick)."""
    out, i, n = [], 0, len(node_src)
    while i < n:
        c = node_src[i]
        if c in ("'", '"', "`"):
            j = i + 1
            while j < n:
                if node_src[j] == "\\":
                    j += 2
                    continue
                if node_src[j] == c:
                    break
                j += 1
            out.append(node_src[i + 1:j])
            i = j + 1
        else:
            i += 1
    return out


class DashboardGpuVendorBadgeTest(unittest.TestCase):
    def setUp(self):
        self.js = JS_PATH.read_text(encoding="utf-8")
        self.css = CSS_PATH.read_text(encoding="utf-8")
        self.gpu = _gpu_func_source(self.js)

    def _assert_no_red_for_health(self, html: str):
        """A vendor/health cell may never carry a red color or red dot."""
        self.assertNotIn("\U0001F534", html, "red emoji rendered in GPU row")
        self.assertNotIn("\U0001F535", html, "red circle emoji rendered in GPU row")
        for lit in _js_strings(html):
            self.assertNotIn("#da3633", lit.lower(), "red color used in GPU row HTML")
            self.assertNotIn("#f85149", lit.lower(), "red color used in GPU row HTML")

    def test_no_red_for_vendor_or_health_in_refresh_gpu(self):
        """Vendor and health indicators may never be red, anywhere in refreshGPU()."""
        # No colored-dot emojis at all in the GPU panel (health is a CSS class now).
        for emoji in ("\U0001F534", "\U0001F535", "\U0001F532", "\U0001F533", "\u26AA", "\u26AB"):
            self.assertNotIn(emoji, self.gpu, f"colored dot emoji {emoji!r} in refreshGPU()")
        # The old vendorIcon ternary (vendor → colored emoji) is gone.
        self.assertNotRegex(self.gpu, r"vendorIcon")
        self.assertNotRegex(self.gpu, r"vendor\s*===?\s*'nvidia'\s*\?\s*'[\U0001F530-\U0001F53F\u26AA\u26AB]")
        # No red indicators in the vendor or health cell construction.
        # (A red temp threshold may remain — high temp is a real warning, not a
        # vendor/health-status indicator.)
        for line in self.gpu.splitlines():
            if "vendor" in line or "healthDot" in line or "health" in line:
                self.assertNotIn("#da3633", line.lower(), f"red color in vendor/health line: {line!r}")
                self.assertNotIn("#f85149", line.lower(), f"red color in vendor/health line: {line!r}")
                self.assertNotIn("#ff7b72", line.lower(), f"red color in vendor/health line: {line!r}")

    def test_neutral_vendor_badge_renders_for_all_vendors(self):
        """Vendor renders as a neutral .gpu-vendor text badge for nvidia/amd/other."""
        self.assertIn('class="gpu-vendor"', self.gpu)
        self.assertIn("NVIDIA", self.gpu)
        self.assertIn("AMD", self.gpu)

        cases = {
            "nvidia": {"vendor": "nvidia", "name": "GeForce RTX 4090", "util_pct": 12.0,
                       "mem_used_mib": 2048, "mem_total_mib": 24564, "temp_c": 45.0, "power_w": 60.0},
            "amd": {"vendor": "amd", "name": "Radeon PRO W7900", "util_pct": 7.0,
                    "mem_used_mib": 1024, "mem_total_mib": 49140, "temp_c": 50.0, "power_w": 40.0},
            "other": {"vendor": "intel", "name": "Arc A770", "util_pct": 3.0,
                      "mem_used_mib": 512, "mem_total_mib": 16384, "temp_c": None, "power_w": None},
        }
        for label, gpu in cases.items():
            html = self._render_row(gpu)
            self.assertIn('class="gpu-vendor"', html, label)
            self.assertNotIn("\U0001F534", html, f"{label}: red emoji rendered")
            self._assert_no_red_for_health(html)
            if label == "nvidia":
                self.assertIn(">NVIDIA</span>", html)
            elif label == "amd":
                self.assertIn(">AMD</span>", html)

    def test_health_dot_green_when_reporting_grey_when_no_runtime_data(self):
        """Green dot when util_pct is present, grey when no runtime data."""
        reporting = self._render_row({"vendor": "nvidia", "name": "RTX 4090", "util_pct": 0.0,
                                      "mem_used_mib": None, "mem_total_mib": None,
                                      "temp_c": None, "power_w": None})
        self.assertIn("gpu-health-ok", reporting)
        self.assertNotIn("gpu-health-none", reporting)

        no_data = self._render_row({"vendor": "amd", "name": "MI250X", "util_pct": None,
                                    "mem_used_mib": None, "mem_total_mib": None,
                                    "temp_c": None, "power_w": None})
        self.assertIn("gpu-health-none", no_data)
        self.assertNotIn("gpu-health-ok", no_data)

        for html in (reporting, no_data):
            self._assert_no_red_for_health(html)

    def test_zero_or_null_mem_total_renders_dash(self):
        """mem_total_mib 0 or null must render '-', never '0.0/0.0 GiB'."""
        for mt in (0, None):
            html = self._render_row({"vendor": "nvidia", "name": "RTX 4090", "util_pct": 1.0,
                                     "mem_used_mib": None, "mem_total_mib": mt,
                                     "temp_c": None, "power_w": None})
            self.assertNotIn("0.0/0.0 GiB", html, f"mem_total_mib={mt!r} rendered 0.0/0.0 GiB")
            self.assertNotIn("GiB", html, f"mem_total_mib={mt!r} still rendered a GiB cell")
            self.assertIn("gpu-vendor", html)  # row still renders its other cells

    def _render_row(self, gpu):
        """Execute refreshGPU() in a stubbed DOM and return the 2nd cell of row 0."""
        return _render_gpu_row(gpu)

    def test_gpu_health_classes_are_neutral_in_css(self):
        """CSS: vendor badge is neutral; health dot uses only green/grey, never red."""
        css = re.sub(r"/\*.*?\*/", "", self.css, flags=re.S).lower()
        def rule(cls):
            return re.search(rf"\.{cls}\s*\{{([^}}]*)\}}", css)
        v = rule("gpu-vendor")
        self.assertIsNotNone(v, ".gpu-vendor rule missing in dashboard.css")
        for red in ("#da3633", "#f85149", "#ff7b72", "red"):
            self.assertNotIn(red, v.group(1), f"red in .gpu-vendor: {v.group(1)}")
        for cls in ("gpu-health-ok", "gpu-health-none"):
            r = rule(cls)
            self.assertIsNotNone(r, f".{cls} rule missing in dashboard.css")
            for red in ("#da3633", "#f85149", "#ff7b72", "red"):
                self.assertNotIn(red, r.group(1), f"red in .{cls}: {r.group(1)}")
        # ok = green-ish, none = grey-ish
        self.assertIn("#7ee787", rule("gpu-health-ok").group(1))
        self.assertIn("#6e7681", rule("gpu-health-none").group(1))

    def test_js_still_parses(self):
        """Sanity: dashboard.js remains syntactically coherent (balanced braces)."""
        self.assertEqual(self.js.count("{"), self.js.count("}"))
        self.assertIn("function refreshGPU", self.js)


# ── DOM-stub harness for executing the real refreshGPU() in Node ──────────────
_JS_TEMPLATE = """
'use strict';
function _el(){
  return {
    className:'', textContent:'', innerHTML:'',
    children:[],
    appendChild(c){ this.children.push(c); },
    insertRow(){ const r={cells:[], appendChild(c){r.cells.push(c);}}; this.children.push(r); return r; },
  };
}
const _els = new Map();
function getEl(sel){ if(!_els.has(sel)) _els.set(sel,_el()); return _els.get(sel); }
const _tbody = _el();
global.document = {
  getElementById: getEl,
  querySelector: (sel) => (sel === '#gpu-tbl tbody' ? _tbody : getEl(sel)),
  createElement: () => _el(),
};
global.Node = class Node {};
const _payload = %PAYLOAD%;
global.api = (method, path, body, retry) => Promise.resolve({ hosts: _payload });
global.unchanged = (key, data) => false;
global.esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
global.td = (html, cls) => {
  const t = document.createElement("td");
  t.className = cls || "";
  t.innerHTML = html;
  return t;
};

%GPU_FUNC%

refreshGPU().then(() => {
  const row0 = _tbody.children[0];
  const cells = row0 ? row0.cells.map(c => c.innerHTML) : [];
  process.stdout.write(JSON.stringify({ cells, count: _tbody.children.length }));
}).catch(e => { process.stderr.write(String((e && e.stack) || e)); process.exit(1); });
"""


def _render_gpu_row(gpu: dict) -> str:
    """Render the GPU-name cell (2nd column) of a single refreshGPU() row in Node."""
    if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
        raise unittest.SkipTest("node not available for JS render test")
    src = JS_PATH.read_text(encoding="utf-8")
    gpu_func = _gpu_func_source(src)
    payload = json.dumps([{"host": "test-host", "gpus_runtime": [gpu]}])
    prog = _JS_TEMPLATE.replace("%PAYLOAD%", payload).replace("%GPU_FUNC%", gpu_func)
    res = subprocess.run(["node", "-e", prog], capture_output=True, text=True, timeout=30)
    if res.returncode != 0:
        raise AssertionError(f"node render failed: {res.stderr}")
    out = json.loads(res.stdout)
    cells = out["cells"]
    assert len(cells) >= 2, f"expected >=2 cells, got {out!r}"
    return cells[1]


if __name__ == "__main__":
    unittest.main()
