"""Tests for hive.core.secrets (credential scan/redact)."""

import unittest

from hive.core import secrets


class ScanTests(unittest.TestCase):
    def test_detects_high_confidence_shapes(self):
        samples = {
            "github_pat": "token github_pat_11ABCDEFG0123456789_abcdef",
            "github_token": "gho_" + "A" * 36,
            "gitlab_pat": "glpat-abcdefghij1234567890",
            "openai_key": "sk-proj-abcdefghij0123456789ABCDEF",
            "aws_akia": "AKIAIOSFODNN7EXAMPLE",
            "sshpass": 'sshpass -p "S3cretPass" ssh root@host',
            "basic_auth_uri": "https://user:s3cr3t@gitlab.com/x.git",
        }
        for kind, text in samples.items():
            with self.subTest(kind=kind):
                kinds = {f.kind for f in secrets.scan(text)}
                self.assertIn(kind, kinds, f"{kind} not detected in {text!r}")

    def test_plain_prose_is_clean(self):
        text = "the quick brown fox jumps over the lazy dog; status=online host=pythia"
        self.assertEqual(secrets.scan(text), [])
        self.assertFalse(secrets.contains_secret(text))

    def test_redact_masks_values_and_keeps_context(self):
        text = "token=glpat-abcdefghij1234567890 host=pythia"
        self.assertEqual(secrets.redact(text), "token=[REDACTED:gitlab_pat] host=pythia")

    def test_redact_no_secret_is_identity(self):
        text = "nothing here"
        self.assertEqual(secrets.redact(text), text)

    def test_non_overlapping(self):
        # a gitlab pat inside a basic-auth uri should yield ONE span, not two
        text = "https://x:glpat-abcdefghij1234567890@host/x"
        self.assertEqual(len(secrets.scan(text)), 1)
        self.assertNotIn("glpat-abcdefghij1234567890", secrets.redact(text))


if __name__ == "__main__":
    unittest.main()
