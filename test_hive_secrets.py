"""Tests for hive.core.secrets (credential scan/redact).

All token-shaped fixtures are assembled at runtime from fragments so that no
complete credential literal exists in the source tree. Secret scanners (e.g.
GitGuardian) flag a source file that contains a token-shaped string; these are
synthetic test values only, so we keep the *shape* (which the detector must
still recognise) but never the literal. Bodies use obviously-fake text
("FAKE"/"EXAMPLE"/repeated "x") and stay long enough to satisfy the detector's
minimum-length quantifiers -- see hive/core/secrets.py.
"""

import unittest

from hive.core import secrets

# --- runtime-built fixture fragments (no complete token literal in source) ---
FAKE_GLPAT = "glpat" + "-" + "FAKE" + "x" * 16  # 18+ chars after the dash
FAKE_GITHUB_PAT = "github" + "_pat_" + "11" + ("FAKE" + "0" * 12) + "_" + "x" * 6
FAKE_GITHUB_TOKEN = "gho" + "_" + "x" * 36
FAKE_OPENAI_KEY = "sk" + "-" + "proj" + "-" + "FAKE" + "x" * 20
FAKE_AWS_AKIA = "AKIA" + "EXAMPLE" + "0" * 9  # AKIA + 16 uppercase/digits
FAKE_SSHPASS_PW = "not" + "-a" + "-real" + "-password"
FAKE_BASIC_AUTH = "https://" + "user" + ":" + "s3cr3t" + "@" + "gitlab.com/x.git"

_SAMPLES = {
    "github_pat": "token " + FAKE_GITHUB_PAT,
    "github_token": FAKE_GITHUB_TOKEN,
    "gitlab_pat": FAKE_GLPAT,
    "openai_key": FAKE_OPENAI_KEY,
    "aws_akia": FAKE_AWS_AKIA,
    "sshpass": 'sshpass ' + "-p " + '"' + FAKE_SSHPASS_PW + '" ssh root@host',
    "basic_auth_uri": FAKE_BASIC_AUTH,
}


class FixtureShapeTests(unittest.TestCase):
    """Guard the fixtures themselves: they must stay shape-valid but fake."""

    def test_glpat_fixture_satisfies_detector_body_length(self):
        # body after "glpat-" must be >= 18 chars of [0-9A-Za-z_-]
        self.assertGreaterEqual(len(FAKE_GLPAT.split("-", 1)[1]), 18)
        self.assertIn("FAKE", FAKE_GLPAT)

    def test_aws_fixture_is_example_shaped(self):
        self.assertEqual(len(FAKE_AWS_AKIA), 20)
        self.assertTrue(FAKE_AWS_AKIA.startswith("AKIA"))
        self.assertIn("EXAMPLE", FAKE_AWS_AKIA)


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
