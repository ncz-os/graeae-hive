"""Credential-shape detection — scan text for secrets and redact them.

Adapted (reduced) from MNEMOS ``mnemos/core/secret_detection.py`` — see
``docs/analysis-mnemos-reuse-20261015.md``. MNEMOS's full module is an 852-line
vault classifier with denylists + namespace semantics; the hive only needs the
high-confidence detector + a redact helper, e.g. to scrub job descriptions,
registry notes, logs, and the auto-commit daemon's plaintext credentials.

Deliberately conservative: only HIGH-confidence shapes match, so ordinary prose
is left alone. This is a redaction aid, not a DLP guarantee.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# (kind, pattern). High-confidence issuer-specific shapes only.
_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("github_pat", re.compile(r"\bgithub_pat_[0-9A-Za-z_]{20,}")),
    ("github_token", re.compile(r"\bgh[pousr]_[0-9A-Za-z]{30,}")),
    ("gitlab_pat", re.compile(r"\bglpat-[0-9A-Za-z_-]{18,}")),
    ("openai_key", re.compile(r"\bsk-(?:proj-|ant-|or-v1-|live-|test-)?[0-9A-Za-z_-]{20,}")),
    ("xai_key", re.compile(r"\bxai-[0-9A-Za-z]{20,}")),
    ("aws_akia", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("google_api", re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b")),
    ("slack", re.compile(r"\bxox[baprs]-[0-9A-Za-z-]{10,}")),
    ("pem_private", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    # sshpass -p <pw>  /  root pw: <pw>
    ("sshpass", re.compile(r"(?i)\bsshpass\s+-p\s*(['\"]?)([^\s'\"]{1,})\1")),
    ("pw_kv", re.compile(r"(?i)\b(?:root[ \t]+pw|pw|sudo[ \t]+pw)\b[ \t]*[:=]?[ \t]*(['\"]?)([^\s'\"]{3,})\1")),
    # URL-embedded basic auth: scheme://user:pass@host
    ("basic_auth_uri", re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://[^:@/\s]+:[^@/\s]+@[^\s'\")\]}>,;]+")),
]


@dataclass(frozen=True)
class Finding:
    kind: str
    start: int
    end: int

    def __str__(self) -> str:  # pragma: no cover - convenience
        return f"{self.kind}@{self.start}:{self.end}"


def scan(text: str) -> list[Finding]:
    """Return every high-confidence credential span in ``text`` (non-overlapping)."""
    if not text:
        return []
    found: list[Finding] = []
    for kind, pat in _PATTERNS:
        for m in pat.finditer(text):
            found.append(Finding(kind, m.start(), m.end()))
    found.sort(key=lambda f: f.start)
    # drop overlaps, keeping the earliest/longest
    out: list[Finding] = []
    for f in found:
        if out and f.start < out[-1].end:
            continue
        out.append(f)
    return out


def contains_secret(text: str) -> bool:
    return bool(scan(text))


def redact(text: str, *, replacement: str = "[REDACTED:{kind}]") -> str:
    """Replace every detected credential span with a typed placeholder."""
    if not text:
        return text
    findings = scan(text)
    if not findings:
        return text
    pieces: list[str] = []
    cursor = 0
    for f in findings:
        pieces.append(text[cursor : f.start])
        pieces.append(replacement.format(kind=f.kind))
        cursor = f.end
    pieces.append(text[cursor:])
    return "".join(pieces)
