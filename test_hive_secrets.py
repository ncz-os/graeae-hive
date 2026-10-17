"""Tests for hive.core.secrets (credential scan/redact).

All token-shaped fixtures are assembled at runtime from fragments so that no
complete credential literal exists in the source tree. Secret scanners (e.g.
GitGuardian) flag a source file that contains a token-shaped string; these are
synthetic test values only, so we keep the *shape* (which the detector must
still recognise) but never the literal. Bodies use obviously-fake text
("FAKE"/"EXAMPLE"/repeated "x") and stay long enough to satisfy the detector's
minimum-length quantifiers -- see hive/core/secrets.py.
"""

import re
import unittest

from hive.core import secrets

# --- runtime-built fixture fragments (no complete token literal in source) ---
# Each value below is concatenated from string LITERALS, never a single token
# literal in source. A scanner that greps for "<prefix><body>" cannot match a
# source that has the prefix split across a + and the body broken by "+".
FAKE_GLPAT = "glpat" + "-" + "FAKE" + "x" * 16  # 18+ chars after the dash
FAKE_GITHUB_PAT = "github" + "_pat_" + "11" + ("FAKE" + "0" * 12) + "_" + "x" * 6
FAKE_GITHUB_TOKEN = "gho" + "_" + "x" * 36
FAKE_GHP_TOKEN = "gh" + "p_" + "x" * 36  # ghp_ form (also covered by gh[pousr]_)
FAKE_XAI_KEY = "xai" + "-" + "FAKE" + "x" * 20  # 20+ chars of [0-9A-Za-z]
FAKE_OPENAI_KEY = "sk" + "-" + "proj" + "-" + "FAKE" + "x" * 20
FAKE_AWS_AKIA = "AKIA" + "EXAMPLE" + "0" * 9  # AKIA + 16 uppercase/digits
FAKE_GOOGLE_API = "AIza" + "Sy" + "FAKE" + "x" * 29  # AIza + 35 of [0-9A-Za-z_-]
FAKE_SLACK = "xoxb" + "-" + "FAKE" + "x" * 15  # xoxb- + 10+ chars
# PEM private key: assembled from small fragments with non-key characters
# interleaved so the BEGIN/END marker never appears as a contiguous run in
# source. At runtime we strip the interleaved characters back out so the
# detector (which reads the assembled value) still recognises it.
_PEM_NOISE = "@"  # stripped at runtime; not a character the detector matches
FAKE_PEM_KEY = (
    "-----B" + _PEM_NOISE + "EGIN " + "RSA " + "PRIV" + _PEM_NOISE + "ATE "
    + "KEY" + _PEM_NOISE + "-----\n"
    + "FAKE" + "x" * 60 + "\n"
    + "-----END " + "RSA " + "PRIVATE " + "KEY" + _PEM_NOISE + "-----"
).replace(_PEM_NOISE, "")
FAKE_SSHPASS_PW = "not" + "-a" + "-real" + "-password"
FAKE_PW_KV_PW = "not" + "-a" + "-real" + "-password"
FAKE_BASIC_AUTH = "https://" + "user" + ":" + "s3cr3t" + "@" + "gitlab.com/x.git"

_SAMPLES = {
    "github_pat": "token " + FAKE_GITHUB_PAT,
    "github_token": FAKE_GITHUB_TOKEN,
    "gitlab_pat": FAKE_GLPAT,
    "openai_key": FAKE_OPENAI_KEY,
    "aws_akia": FAKE_AWS_AKIA,
    "google_api": FAKE_GOOGLE_API,
    "slack": FAKE_SLACK,
    "pem_private": FAKE_PEM_KEY,
    "sshpass": 'sshpass ' + "-p " + '"' + FAKE_SSHPASS_PW + '" ssh root@host',
    "pw_kv": "root " + "pw: " + '"' + FAKE_PW_KV_PW + '"',
    "basic_auth_uri": FAKE_BASIC_AUTH,
}


class FixtureShapeTests(unittest.TestCase):
    """Guard the fixtures themselves: they must stay shape-valid but fake."""

    def test_glpat_fixture_satisfies_detector_body_length(self):
        # body after the "glpat" + "-" prefix must be >= 18 chars of [0-9A-Za-z_-]
        self.assertGreaterEqual(len(FAKE_GLPAT.split("-", 1)[1]), 18)
        self.assertIn("FAKE", FAKE_GLPAT)

    def test_aws_fixture_is_example_shaped(self):
        self.assertEqual(len(FAKE_AWS_AKIA), 20)
        self.assertTrue(FAKE_AWS_AKIA.startswith("AKIA"))
        self.assertIn("EXAMPLE", FAKE_AWS_AKIA)

    def test_xai_fixture_is_fake_shaped(self):
        # body after the "xai" + "-" prefix must be >= 20 chars of [0-9A-Za-z]
        body = FAKE_XAI_KEY.split("-", 1)[1]
        self.assertGreaterEqual(len(body), 20)
        self.assertIn("FAKE", FAKE_XAI_KEY)

    def test_google_api_fixture_is_fake_shaped(self):
        # body after the "AIza" prefix must be >= 35 chars
        body = FAKE_GOOGLE_API[len("AIza"):]
        self.assertGreaterEqual(len(body), 35)
        self.assertIn("FAKE", FAKE_GOOGLE_API)

    def test_slack_fixture_is_fake_shaped(self):
        # body after the "xoxb" + "-" prefix must be >= 10 chars
        body = FAKE_SLACK.split("-", 1)[1]
        self.assertGreaterEqual(len(body), 10)
        self.assertIn("FAKE", FAKE_SLACK)

    def test_pem_fixture_brackets_are_assembled(self):
        # the BEGIN marker must exist in the runtime value but never as a
        # contiguous literal in the file's source text (the source must spell
        # it across string fragments so a scanner cannot match it).
        self.assertIn("BEGIN", FAKE_PEM_KEY)
        self.assertIn("PRIVATE", FAKE_PEM_KEY)
        self.assertIn("KEY-----", FAKE_PEM_KEY)
        with open(__file__, encoding="utf-8") as fh:
            src = fh.read()
        # The detector regex is "-----BEGIN [A-Z ]*PRIVATE KEY-----" with the
        # five-dash bracket pairs on either side. That full bracketed run must
        # NOT appear contiguously in source -- including inside this very
        # test's own assertion strings, which we build at runtime.
        forbidden_begin = "-----" + "BEGIN " + "RSA " + "PRIVATE " + "KEY-----"
        forbidden_end = "-----" + "END " + "RSA " + "PRIVATE " + "KEY-----"
        self.assertNotIn(forbidden_begin, src)
        self.assertNotIn(forbidden_end, src)


class SourceHygieneTests(unittest.TestCase):
    """A complete token literal must never come back into this file's source."""

    # Token SHAPES, matched against the raw source text. Keep each pattern
    # split so this guard cannot itself trip a scanner.
    FORBIDDEN = (
        re.compile("gl" + "pat" + "-" + r"[0-9A-Za-z_-]{18,}"),
        re.compile("gh" + "[pousr]" + "_" + r"[0-9A-Za-z]{30,}"),
        re.compile("github" + "_pat_" + r"[0-9A-Za-z_]{20,}"),
        re.compile(r"\bsk-" + r"(?:proj-|ant-|or-v1-|live-|test-)?[0-9A-Za-z_-]{20,}"),
        re.compile("xA" + r"i-[0-9A-Za-z]{20,}"),
        re.compile("AKIA" + r"[0-9A-Z]{16}\b"),
        re.compile("AIza" + r"[0-9A-Za-z_-]{35}"),
        re.compile("xox" + r"[baprs]-[0-9A-Za-z-]{10,}"),
        re.compile("ssh" + "pass" + r"\s+" + "-p" + r"\s+['\"]?[^\s'\"]"),
        # the BEGIN ... PRIVATE KEY run must never appear contiguously in source
        re.compile("-----BEGIN " + r"[A-Z ]*PRIVATE KEY-----"),
    )

    def test_no_complete_token_literal_in_source(self):
        with open(__file__, encoding="utf-8") as fh:
            src = fh.read()
        for pat in self.FORBIDDEN:
            with self.subTest(pattern=pat.pattern):
                self.assertIsNone(pat.search(src), f"token literal found: {pat.pattern}")

    def test_fixtures_are_runtime_assembled(self):
        # every fixture must exist as a value
        for name in (
            "FAKE_GLPAT",
            "FAKE_GITHUB_PAT",
            "FAKE_GITHUB_TOKEN",
            "FAKE_GHP_TOKEN",
            "FAKE_XAI_KEY",
            "FAKE_OPENAI_KEY",
            "FAKE_AWS_AKIA",
            "FAKE_GOOGLE_API",
            "FAKE_SLACK",
            "FAKE_PEM_KEY",
            "FAKE_SSHPASS_PW",
            "FAKE_PW_KV_PW",
            "FAKE_BASIC_AUTH",
        ):
            with self.subTest(fixture=name):
                self.assertIsInstance(globals().get(name), str)


class ScanTests(unittest.TestCase):
    def test_detects_high_confidence_shapes(self):
        for kind, text in _SAMPLES.items():
            with self.subTest(kind=kind):
                kinds = {f.kind for f in secrets.scan(text)}
                self.assertIn(kind, kinds, f"{kind} not detected in {text!r}")

    def test_plain_prose_is_clean(self):
        text = "the quick brown fox jumps over the lazy dog; status=online host=pythia"
        self.assertEqual(secrets.scan(text), [])
        self.assertFalse(secrets.contains_secret(text))

    def test_redact_masks_values_and_keeps_context(self):
        text = "token=" + FAKE_GLPAT + " host=pythia"
        self.assertEqual(secrets.redact(text), "token=[REDACTED:gitlab_pat] host=pythia")

    def test_redact_no_secret_is_identity(self):
        text = "nothing here"
        self.assertEqual(secrets.redact(text), text)

    def test_non_overlapping(self):
        # a gitlab pat inside a basic-auth uri should yield ONE span, not two
        text = "https://" + "x" + ":" + FAKE_GLPAT + "@" + "host/x"
        self.assertEqual(len(secrets.scan(text)), 1)
        self.assertNotIn(FAKE_GLPAT, secrets.redact(text))


if __name__ == "__main__":
    unittest.main()