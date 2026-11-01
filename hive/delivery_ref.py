"""The composite-baseline builder and the commit validator (2026-10-31).

The observed defect: a review/verify job carried a 36-character "baseline"
that named no commit — ``head_sha[:28] + workspace_uuid[-8:]``. Git resolves
the delivery boundary with ``git merge-base --is-ancestor <baseline> HEAD``;
that call exited 128 on the composite value and ``completion_evidence``
aborted before its own delivery check, so every such job failed as
"unverified delivery" no matter how good the work was.

The builder and the validator live here so the ONE implementation is
importable by the worker, the bus lines and the doctor (which repairs rows
already carrying the bad value), instead of each growing its own copy of the
36-character arithmetic.
"""

from __future__ import annotations

import re

COMPOSITE_HEAD_PREFIX_LEN = 28
COMPOSITE_SUFFIX_LEN = 8
COMPOSITE_LEN = COMPOSITE_HEAD_PREFIX_LEN + COMPOSITE_SUFFIX_LEN

_SHA40_RE = re.compile(r"[0-9a-f]{40}")
_SHA64_RE = re.compile(r"[0-9a-f]{64}")
_SHA_PREFIX_RE = re.compile(r"[0-9a-f]{7,40}")
_HEX36_RE = re.compile(r"^[0-9a-f]{36}$")
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)


def normalize_head(value: object) -> str:
    """The lowercase, whitespace-free commit a caller passed, or ``""``."""
    text = str(value or "").strip().lower()
    return text if _SHA40_RE.fullmatch(text) or _SHA64_RE.fullmatch(text) else ""


def workspace_suffix(workspace: object) -> str:
    """The last 8 characters of a workspace UUID (the composite's tail)."""
    text = str(workspace or "")
    match = _UUID_RE.search(text)
    if match:
        return match.group(0).replace("-", "")[-COMPOSITE_SUFFIX_LEN:].lower()
    stripped = text.strip().lower()
    return stripped if re.fullmatch(r"[0-9a-f]{8}", stripped) else ""


def build_boundary(workspace: object, head: object) -> str:
    """The delivery boundary for a review/verify job: the commit ALONE.

    Returns the normalised head sha, or ``""`` when no commit was given. It
    never composes a shorter identifier: a boundary that is not a commit cannot
    be compared with ``git merge-base --is-ancestor``, and a workspace UUID
    identifies a directory, not a revision."""
    return normalize_head(head)


def is_composite(value: object, *, head: object = None, workspace: object = None, ref_exists=None) -> bool:
    """Whether ``value`` is the builder bug's ``head_sha[:28] + uuid[-8:]`` value.

    The shape is exactly 36 lowercase hex characters (never 40/64, so never a
    full commit). Each part is checked when the caller can name it: ``head``
    must supply the first 28 characters and ``workspace`` (a uuid or its 8-char
    tail) the last 8. ``ref_exists`` rules out a value the repository RESOLVES
    -- git accepts a 36-char abbreviation of a real commit, and that is a
    commit, not a composite. An unreadable probe falls back to the shape."""
    text = str(value or "").strip().lower()
    if not _HEX36_RE.match(text):
        return False
    head_text = normalize_head(head)
    if head_text and text[:COMPOSITE_HEAD_PREFIX_LEN] != head_text[:COMPOSITE_HEAD_PREFIX_LEN]:
        return False
    suffix = workspace_suffix(workspace) if workspace is not None else ""
    if suffix and text[-COMPOSITE_SUFFIX_LEN:] != suffix:
        return False
    if ref_exists is not None:
        try:
            if ref_exists(text):
                return False
        except Exception:  # noqa: BLE001 -- a broken probe is not evidence either way
            pass
    return True


def is_commit(value: object, *, ref_exists=None) -> bool:
    """Whether ``value`` is a commit this repository actually holds.

    Shape: 40- or 64-character lowercase hex. ``ref_exists`` (optional) is
    asked when provided; an unreadable probe is not proof of absence, so this
    falls back to the shape answer rather than reporting a valid commit as
    broken."""
    text = normalize_head(value)
    if not text:
        return False
    if ref_exists is None:
        return True
    try:
        return bool(ref_exists(text))
    except Exception:  # noqa: BLE001 -- a broken probe is not evidence of absence
        return True


def repair_candidate(value: object, candidates: list | tuple) -> str:
    """The first real commit in ``candidates`` when ``value`` is a composite."""
    if not is_composite(value):
        return ""
    for candidate in candidates or ():
        text = normalize_head(candidate)
        if text:
            return text
    return ""


def short(value: object, length: int = 12) -> str:
    """A safe display form of a ref-like value (never raises on None)."""
    match = _SHA_PREFIX_RE.match(str(value or "").strip().lower())
    return match.group(0)[:length] if match else ""
