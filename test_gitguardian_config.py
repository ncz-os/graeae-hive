"""Self-audit the repo-root ``.gitguardian.yaml`` configuration.

Task 01a1481e-bb57-7201-bd9f-ca5de739c71f: GitGuardian flagged two synthetic
fixtures in ``test_hive_secrets.py`` as 'GitLab Token' / 'SSH Credentials' on
commit 6dbaf1d. The fix is twofold -- (1) every token-shaped fixture in the
test file is now built at runtime from string fragments so no complete token
literal exists in source, and (2) the repo-root config keeps ``ignored-paths``
scoped to the test fixture files that intentionally contain token SHAPES.

These tests guard the second half so a future drift gets caught at pytest
time, not at GitGuardian's nightly scan. They are deliberately tolerant of
future expansions: a NEW test fixture that legitimately contains token shapes
may grow the ``ignored-paths`` set as long as it names real files (and the
header still references this task so the rationale stays discoverable).
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path


# Task identifier that anchors the rationale of this config file.
TASK_ID = "01a1481e-bb57-7201-bd9f-ca5de739c71f"


def _repo_root() -> Path:
    return Path(__file__).resolve().parent


def _config_text() -> str:
    return (_repo_root() / ".gitguardian.yaml").read_text(encoding="utf-8")


class GitGuardianConfigShapeTests(unittest.TestCase):
    """The config must parse, declare v2, scope ignored-paths, and cite the task."""

    def test_config_file_exists(self):
        self.assertTrue(
            (_repo_root() / ".gitguardian.yaml").is_file(),
            ".gitguardian.yaml must live at the repo root",
        )

    def test_config_declares_version_2(self):
        try:
            import yaml  # local import: optional dep in some environments
        except ModuleNotFoundError:
            self.skipTest("pyyaml is not installed in this environment (and is not a declared "
                        "project dependency): pip install pyyaml to run this config assertion")

        cfg = yaml.safe_load(_config_text())
        self.assertEqual(cfg.get("version"), 2, "version: 2 is required")

    def test_ignored_paths_is_present_and_iterable(self):
        try:
            import yaml
        except ModuleNotFoundError:
            self.skipTest("pyyaml is not installed in this environment (and is not a declared "
                        "project dependency): pip install pyyaml to run this config assertion")

        cfg = yaml.safe_load(_config_text())
        secret = cfg.get("secret") or {}
        ignored = secret.get("ignored-paths")
        self.assertIsNotNone(ignored, "secret.ignored-paths must be defined")
        self.assertIsInstance(ignored, list)
        self.assertGreater(len(ignored), 0, "ignored-paths must list at least one file")

    def test_ignored_paths_is_scoped_to_test_fixtures(self):
        # Every path in ignored-paths must (a) exist in the repo and (b) live
        # under the test layout. A future expansion is allowed; production
        # code never earns an entry here.
        try:
            import yaml
        except ModuleNotFoundError:
            self.skipTest("pyyaml is not installed in this environment (and is not a declared "
                        "project dependency): pip install pyyaml to run this config assertion")

        cfg = yaml.safe_load(_config_text())
        ignored = (cfg.get("secret") or {}).get("ignored-paths") or []
        for rel in ignored:
            with self.subTest(path=rel):
                self.assertFalse(
                    rel.startswith("hive/"),
                    f"production code path {rel!r} must not be in ignored-paths",
                )
                self.assertTrue(
                    rel.startswith("test_") or rel.startswith("tests/"),
                    f"ignored path {rel!r} must live under the test layout",
                )
                self.assertTrue(
                    (_repo_root() / rel).is_file(),
                    f"ignored path {rel!r} does not exist in the repo",
                )

    def test_header_cites_the_task(self):
        # The leading comment line must reference the task identifier so a
        # operator opening the file can see WHY the ignores exist.
        text = _config_text()
        first_line = text.splitlines()[0] if text else ""
        self.assertTrue(first_line.startswith("#"), "config must open with a # comment")
        self.assertIn(
            TASK_ID,
            text,
            f"the config must mention task {TASK_ID} in its header",
        )


class GitGuardianConfigIntegrityTests(unittest.TestCase):
    """The config itself must remain scanner-safe (no complete token literal)."""

    FORBIDDEN_HEAD = re.compile("gl" + "pat" + "-" + r"[0-9A-Za-z_-]{18,}")
    FORBIDDEN_SSHPASS = re.compile("ssh" + "pass" + r"\s+" + "-p" + r"\s+['\"]?[^\s'\"]")

    def test_no_complete_token_literal_in_config(self):
        # Even if a future operator hand-edits the comment block, no GitGuardian
        # detector pattern may complete itself inside the YAML source.
        text = _config_text()
        self.assertIsNone(self.FORBIDDEN_HEAD.search(text))
        self.assertIsNone(self.FORBIDDEN_SSHPASS.search(text))


if __name__ == "__main__":
    unittest.main()
