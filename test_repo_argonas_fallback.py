"""ARGONAS live fallback: repos created after the catalog build still resolve.

``data/repos.json`` is a build artifact (last built 2026-10-15) and there is no
add-repo API, so a bare repo created afterwards used to 404 on
``GET /v1/repos/{slug}`` and jobs for it died with ``no_repo_mapping`` /
``no_private_remote``. On a catalog MISS only, ``_raw_entry`` and ``find_exact``
now synthesize an entry from a bare ``<slug>.git`` under the ARGONAS root as
this host sees it (``HIVE_ARGONAS_GIT_ROOT``).

All offline: the root is a ``tmp_path`` directory, never the real mount.

``HitTests`` is the regression proof -- it fails on the pre-fallback code (the
lookup returns None) and passes on the fallback.
"""

import asyncio
import importlib.util
import json
import os
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from hive import repos
from hive.registry import (
    ARGONAS_GIT_PATH_NFS,
    ARGONAS_GIT_PATH_SSH,
    ARGONAS_SSH_HOST,
    ARGONAS_SSH_USER,
    argonas_nfs_path,
    argonas_ssh_url,
)

CATALOG = {
    "_meta": {"generated_by": "test"},
    "repos": [
        {
            "id": "cataloged",
            "display_name": "Cataloged",
            "aliases": ["cat-alias"],
            "sources": ["gitlab", "argonas"],
            "argonas_bare": f"{ARGONAS_GIT_PATH_NFS}/cataloged.git",
            "clone_ssh": f"ssh://{ARGONAS_SSH_USER}@{ARGONAS_SSH_HOST}{ARGONAS_GIT_PATH_SSH}/cataloged.git",
            "gitlab": {"url": "https://gitlab.com/ncz-os/cataloged", "default_branch": "trunk"},
        },
        {
            # A bare repo of the SAME name exists on the "mount" too: the built
            # catalog must win, or every curated field (gitlab facet, branch)
            # would be replaced by a synthesized one.
            "id": "shadowed",
            "display_name": "Shadowed From Catalog",
            "sources": ["overlay"],
            "default_branch": "release",
        },
    ],
}


def _make_bare(root: Path, slug: str, head: str | None = "ref: refs/heads/master\n") -> Path:
    """A minimal bare repo directory (no git needed: only HEAD is read)."""
    bare = Path(root, f"{slug}.git")
    if bare.exists():  # re-render HEAD in place for a repeat call
        if head is not None:
            (bare / "HEAD").write_text(head)
        return bare
    (bare / "refs" / "heads").mkdir(parents=True)
    (bare / "objects").mkdir()
    (bare / "config").write_text("[core]\n\tbare = true\n")
    if head is not None:
        (bare / "HEAD").write_text(head)
    return bare


class _FallbackCase(unittest.TestCase):
    """Temp ARGONAS root + temp catalog, both installed for one test."""

    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        base = Path(self.td.name)
        self.root = base / "argonas_git"
        self.root.mkdir()
        self.outside = base / "outside"  # never inside the root
        self.outside.mkdir()
        catalog = base / "repos.json"
        catalog.write_text(json.dumps(CATALOG))
        self._env = mock.patch.dict(
            os.environ,
            {
                "HIVE_REPO_CATALOG": str(catalog),
                "HIVE_ARGONAS_GIT_ROOT": str(self.root),
            },
        )
        self._env.start()
        # Present on the fallback code only; the old-code contrast run (see
        # OldCodeContrastTests) has no such cache to clear.
        if hasattr(repos, "clear_fallback_cache"):
            repos.clear_fallback_cache()
        repos.load_catalog(force=True)

    def tearDown(self):
        if hasattr(repos, "clear_fallback_cache"):
            repos.clear_fallback_cache()
        self._env.stop()
        repos.load_catalog(force=True)  # drop the temp catalog from the cache
        self.td.cleanup()


class HitTests(_FallbackCase):
    """The regression: a repo the catalog has never seen but ARGONAS has."""

    def test_live_repo_resolves_from_argonas(self):
        _make_bare(self.root, "created-after-build")
        entry = repos.find_exact("created-after-build")
        self.assertIsNotNone(entry, "a bare repo on ARGONAS must resolve even with no catalog row")
        self.assertEqual(entry["id"], "created-after-build")
        self.assertEqual(entry["display_name"], "created-after-build")
        self.assertEqual(entry["aliases"], [])
        self.assertEqual(entry["sources"], ["argonas-live"])
        self.assertEqual(entry["status"], "active")
        self.assertEqual(entry["argonas_bare"], str(self.root / "created-after-build.git"))
        # The push destination is the ARGONAS SSH URL -- never a local path,
        # which is what a worker would otherwise be handed.
        self.assertEqual(
            entry["clone_ssh"],
            "ssh://jasonperlow@192.0.2.101/mnt/datapool/git/created-after-build.git",
        )
        self.assertEqual(entry["clone_nfs"], "/mnt/argonas_git/created-after-build.git")

    def test_fallback_is_derived_from_the_shared_argonas_constants(self):
        # No second hard-coded copy of user/host/paths: the synthesized URLs
        # come from hive.registry, so they can never drift from the catalog's.
        _make_bare(self.root, "const-check")
        entry = repos._probe_argonas_live("const-check")
        self.assertEqual(entry["clone_ssh"], argonas_ssh_url("const-check"))
        self.assertEqual(entry["clone_nfs"], argonas_nfs_path("const-check"))
        self.assertIn(ARGONAS_SSH_HOST, entry["clone_ssh"])

    def test_get_repo_and_default_branch_come_through_to_dict(self):
        _make_bare(self.root, "branched", head="ref: refs/heads/feat/new-thing\n")
        payload = repos.get_repo("branched")
        self.assertEqual(payload["id"], "branched")
        self.assertEqual(payload["default_branch"], "feat/new-thing")
        self.assertEqual(payload["clone"]["ssh"], "ssh://jasonperlow@192.0.2.101/mnt/datapool/git/branched.git")
        self.assertEqual(payload["sources"], ["argonas-live"])

    def test_default_branch_falls_back_to_master(self):
        _make_bare(self.root, "nohead", head=None)
        self.assertEqual(repos.find_exact("nohead")["default_branch"], "master")
        _make_bare(self.root, "detached", head="0123456789abcdef0123456789abcdef01234567\n")
        self.assertEqual(repos.find_exact("detached")["default_branch"], "master")
        _make_bare(self.root, "traversal", head="ref: refs/heads/../evil\n")
        self.assertEqual(repos.find_exact("traversal")["default_branch"], "master")
        _make_bare(self.root, "redirect", head="ref: refs/heads/../../etc/passwd\n")
        self.assertEqual(repos.find_exact("redirect")["default_branch"], "master")

    def test_resolving_sees_the_fallback(self):
        """``repo_in_catalog`` uses find_exact, so a live repo stops dead-lettering."""
        _make_bare(self.root, "live-job-repo")
        from hive.bus.domain.jobs import repo_in_catalog

        self.assertTrue(repo_in_catalog("live-job-repo"))
        self.assertFalse(repo_in_catalog("never-existed-anywhere"))
        self.assertFalse(repo_in_catalog("../etc/passwd"))


class MissTests(_FallbackCase):
    def test_unknown_slug_is_still_none(self):
        self.assertIsNone(repos.find_exact("nope"))
        # a bare dir that merely CONTAINS the name is not a hit
        _make_bare(self.root, "actual")
        self.assertIsNone(repos.find_exact("actua"))
        self.assertIsNone(repos.find_exact("actual.git.git"))

    def test_file_is_not_a_repo(self):
        (self.root / "afile.git").write_text("not a directory")
        self.assertIsNone(repos.find_exact("afile"))


class HostileSlugTests(_FallbackCase):
    """The slug comes from a job string: treat it as hostile."""

    def setUp(self):
        super().setUp()
        self.outside.mkdir(exist_ok=True)
        _make_bare(self.outside, "escaped")
        _make_bare(self.root, "legit")

    def test_dotdot_is_refused(self):
        self.assertIsNone(repos.find_exact("../x"))
        self.assertIsNone(repos.find_exact("../outside/escaped"))
        self.assertIsNone(repos.find_exact("legit/../legit"))
        self.assertIsNone(repos.find_exact("legit..git"))
        self.assertIsNone(repos._probe_argonas_live("../x"))

    def test_separators_are_refused(self):
        self.assertIsNone(repos.find_exact("a/b"))
        self.assertIsNone(repos.find_exact("/legit"))
        self.assertIsNone(repos.find_exact("legit/"))
        self.assertIsNone(repos.find_exact("legit\x00"))

    def test_git_suffix_is_refused(self):
        # callers pass the bare NAME; ``x.git`` would build ``x.git.git`` and
        # let an alias-ish string dodge the catalog
        self.assertIsNone(repos.find_exact("x.git"))
        self.assertIsNone(repos._probe_argonas_live("legit.git"))

    def test_empty_and_non_string(self):
        self.assertIsNone(repos.find_exact(""))
        self.assertIsNone(repos.find_exact("   "))
        self.assertIsNone(repos.find_exact(None))
        self.assertIsNone(repos._probe_argonas_live(""))

    def test_length_bounds(self):
        self.assertEqual(repos._SLUG_RE.pattern, r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")
        self.assertIsNone(repos._probe_argonas_live("a" * 101))
        # 100 chars is the boundary the regex allows (1 + 99) -- allowed, and a
        # miss because nothing on disk has that name.
        self.assertIsNone(repos._probe_argonas_live("a" * 100))
        _make_bare(self.root, "b" * 100)
        self.assertIsNotNone(repos._probe_argonas_live("b" * 100))

    def test_leading_punctuation_is_refused(self):
        self.assertIsNone(repos._probe_argonas_live("-rf"))
        self.assertIsNone(repos._probe_argonas_live(".hidden"))

    def test_symlink_escaping_the_root_is_refused(self):
        os.symlink(self.outside / "escaped.git", self.root / "escape.git")
        self.assertIsNone(repos.find_exact("escape"))
        self.assertIsNone(repos._probe_argonas_live("escape"))

    def test_symlink_inside_the_root_is_refused(self):
        # A link is not a repository, even when it points INSIDE the root: the
        # directory test must be lstat, not isdir (isdir follows the link and
        # would advertise whatever it points at). In-root links are refused
        # too, so there is no "allowed symlink" category to reason about.
        _make_bare(self.root, "real-one", head="ref: refs/heads/main\n")
        os.symlink(self.root / "real-one.git", self.root / "alias-one.git")
        self.assertIsNone(repos._probe_argonas_live("alias-one"))
        self.assertIsNone(repos.find_exact("alias-one"))
        # the real directory is still served, by its own name
        entry = repos._probe_argonas_live("real-one")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["default_branch"], "main")

    def test_whitespace_branch_names_are_rejected_through_the_probe(self):
        # Exercises the full _probe_argonas_live path (not just the helper), so
        # a mistake in the boolean expression cannot hide behind a unit call.
        for bad in ("a b", "a\tb"):
            _make_bare(self.root, "wscheck", head=f"ref: refs/heads/{bad}\n")
            self.assertEqual(repos._probe_argonas_live("wscheck")["default_branch"], "master")
        _make_bare(self.root, "wscheck", head="ref: refs/heads/feature/ok-1\n")
        self.assertEqual(repos._probe_argonas_live("wscheck")["default_branch"], "feature/ok-1")

    def test_forbidden_ref_chars_include_the_backslash(self):
        # The ref-name character set is spelled out as a set: the backslash
        # and the DEL byte must BOTH be rejected (a quoting mistake in a string
        # literal silently dropped the backslash once already).
        self.assertIn("\\", repos._FORBIDDEN_REF_CHARS)
        self.assertIn("\x7f", repos._FORBIDDEN_REF_CHARS)
        for bad in ("a\\b", "a~b", "a^b", "a:b", "a?b", "a*b", "a[b", "a\x7fb", "a b", "a\x00b", "a\x01b"):
            _make_bare(self.root, "refcheck", head=f"ref: refs/heads/{bad}\n")
            self.assertEqual(
                repos._probe_argonas_live("refcheck")["default_branch"],
                "master",
                f"{bad!r} must not be accepted as a branch name",
            )
        _make_bare(self.root, "refcheck", head="ref: refs/heads/feat/fine-name_1\n")
        self.assertEqual(repos._probe_argonas_live("refcheck")["default_branch"], "feat/fine-name_1")

    def test_symlinked_root_still_finds_its_children(self):
        # The ROOT being a symlink is legitimate (the NFS mount is one on some
        # hosts). The root is resolved ONCE per process (memoized) so the
        # child path is <real-root>/<slug>.git -- inside the root by
        # construction, and the lstat sees the actual directory.
        _make_bare(self.root, "behind-link", head="ref: refs/heads/trunk\n")
        link = self.outside / "root-link"
        link.symlink_to(self.root)
        with mock.patch.dict(os.environ, {"HIVE_ARGONAS_GIT_ROOT": str(link)}):
            repos.clear_fallback_cache()  # drop the memoized realpath of the old root
            entry = repos._probe_argonas_live("behind-link")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["argonas_bare"], str(self.root / "behind-link.git"))
        self.assertEqual(entry["id"], "behind-link")
        self.assertEqual(entry["default_branch"], "trunk")  # README read through the link

    def test_symlinked_root_is_its_own_realpath(self):
        # A legitimately symlinked MOUNT is fine: the child is joined onto the
        # root as given, so it is inside by construction.
        link = Path(self.td.name) / "argonas_link"
        link.symlink_to(self.root)
        with mock.patch.dict(os.environ, {"HIVE_ARGONAS_GIT_ROOT": str(link)}):
            self.assertIsNotNone(repos._probe_argonas_live("legit"))


class ContainmentTests(_FallbackCase):
    """The root may be ``/`` (or a symlinked mount): containment must still hold."""

    def test_containment_helper(self):
        self.assertTrue(repos._contained("/srv/git/a.git", "/srv/git"))
        self.assertTrue(repos._contained("/srv/git", "/srv/git"))
        self.assertFalse(repos._contained("/srv/git-other/a.git", "/srv/git"))  # prefix, not child
        self.assertFalse(repos._contained("/etc/passwd", "/srv/git"))
        # The string form `startswith(root + os.sep)` turns "/" into "//" and
        # would have declared EVERY absolute path contained.
        self.assertTrue(repos._contained("/foo.git", "/"))
        self.assertTrue(repos._contained("/a/b/c.git", "/"))

    def test_root_of_slash_does_not_accept_any_absolute_path(self):
        # With the root at "/", `real_bare` is whatever came back from
        # realpath -- including a path OUTSIDE the root's namespace on another
        # mount. The string form compared against "//" and passed everything;
        # the real containment check must still refuse.
        with (
            mock.patch.dict(os.environ, {"HIVE_ARGONAS_GIT_ROOT": "/"}),
            mock.patch("hive.repos.os.path.realpath", return_value="/elsewhere/x.git"),
        ):
            self.assertIsNone(repos._probe_argonas_live("x"))

    def test_bare_dir_that_is_the_root_itself_is_not_a_repo(self):
        # root intentionally points at a directory whose name ends in `.git`
        # via a slug of nothing -- only the real child path counts.
        inner = self.root / "nested"
        inner.mkdir()
        _make_bare(inner, "realdir")
        with mock.patch.dict(os.environ, {"HIVE_ARGONAS_GIT_ROOT": str(self.root)}):
            self.assertIsNone(repos._probe_argonas_live("nested"))


class StatBudgetTests(_FallbackCase):
    """The fallback must cost ONE stat call: the mount may be slow or stalled."""

    def _spy(self, owner, name):
        seen: list[str] = []
        real = getattr(owner, name)

        def spy(path, *a, **k):
            seen.append(os.fspath(path))
            return real(path, *a, **k)

        return seen, spy

    def test_hit_costs_exactly_one_stat_and_one_read(self):
        _make_bare(self.root, "budgeted", head="ref: refs/heads/main\n")
        # Pre-resolve the root so realpath's component-walk is outside the spy.
        # The probe lstats the bare repo TWICE (first check + re-confirm at the
        # open) and reads HEAD once; that is the steady-state cost.
        root_str = str(self.root)
        with mock.patch("hive.repos._realpath_cached", return_value=os.path.realpath(root_str)):
            stats, lstat_spy = self._spy(repos.os, "lstat")
            files: list[str] = []
            real_read = repos._read_text

            def read_text(path):
                files.append(path)
                return real_read(path)

            with (
                mock.patch("hive.repos.os.lstat", side_effect=lstat_spy),
                mock.patch("hive.repos._read_text", side_effect=read_text),
            ):
                entry = repos._probe_argonas_live("budgeted")
        self.assertIsNotNone(entry)
        self.assertEqual(len(stats), 2, f"expected two lstat (check + re-confirm), saw {stats}")
        self.assertEqual(len(files), 1, f"expected one read, saw {files}")
        self.assertEqual(entry["default_branch"], "main")

    def test_miss_costs_one_stat_and_no_read(self):
        root_str = str(self.root)
        with mock.patch("hive.repos._realpath_cached", return_value=os.path.realpath(root_str)):
            stats, lstat_spy = self._spy(repos.os, "lstat")
            files: list[str] = []
            with (
                mock.patch("hive.repos.os.lstat", side_effect=lstat_spy),
                mock.patch("hive.repos._read_text", side_effect=lambda p: files.append(p)),
            ):
                self.assertIsNone(repos._probe_argonas_live("absent"))
        self.assertEqual(len(stats), 1)  # absent: the first lstat fails, no re-confirm
        self.assertEqual(files, [])

    def test_symlink_refused_at_the_same_one_stat(self):
        os.symlink(self.outside / "escaped.git", self.root / "escape2.git")
        root_str = str(self.root)
        with mock.patch("hive.repos._realpath_cached", return_value=os.path.realpath(root_str)):
            stats, lstat_spy = self._spy(repos.os, "lstat")
            with mock.patch("hive.repos.os.lstat", side_effect=lstat_spy):
                self.assertIsNone(repos._probe_argonas_live("escape2"))
        self.assertEqual(len(stats), 1)  # refused by the very stat that found it


class NormalizationTests(_FallbackCase):
    def test_padded_slug_uses_one_cache_key(self):
        _make_bare(self.root, "padded")
        self.assertIsNotNone(repos.find_exact("  padded  "))
        self.assertEqual(list(repos._FALLBACK._entries), ["padded"])
        self.assertIsNotNone(repos.find_exact("padded"))
        self.assertEqual(list(repos._FALLBACK._entries), ["padded"])

    def test_padded_negative_is_cached_once(self):
        with mock.patch("hive.repos._probe_argonas_live", wraps=repos._probe_argonas_live) as probe:
            self.assertIsNone(repos.find_exact("  late  "))
            _make_bare(self.root, "late")
            # still the cached NEGATIVE, for both spellings, not a second probe
            self.assertIsNone(repos.find_exact("late"))
            self.assertIsNone(repos.find_exact("  late  "))
            self.assertEqual(probe.call_count, 1)

    def test_non_string_slugs_answer_none(self):
        for bad in (None, 7, b"bytes", ""):
            self.assertIsNone(repos._fallback_entry(bad))
            self.assertIsNone(repos._probe_argonas_live(bad))

    def test_normalize_is_strip_only(self):
        # Case and inner characters are ARGONAS-significant: never rewritten.
        self.assertEqual(repos._normalize_slug("  Mixed.Case-1  "), "Mixed.Case-1")
        self.assertEqual(repos._normalize_slug("x"), "x")


class NormalizationBeforeValidationTests(_FallbackCase):
    """Validation must see the NORMALIZED slug: one key, one decision."""

    def test_hostile_slug_is_validated_after_stripping(self):
        with mock.patch("hive.repos._probe_argonas_live", wraps=repos._probe_argonas_live) as probe:
            self.assertIsNone(repos.find_exact("  ../etc/passwd  "))
            # the probe received the STRIPPED string, so validation judged it
            self.assertEqual(probe.call_args.args, ("../etc/passwd",))
        self.assertEqual(list(repos._FALLBACK._entries), ["../etc/passwd"])

    def test_padded_hostile_and_bare_hostile_share_one_negative_entry(self):
        with mock.patch("hive.repos._probe_argonas_live", wraps=repos._probe_argonas_live) as probe:
            self.assertIsNone(repos.find_exact("  ../etc/passwd  "))
            self.assertIsNone(repos.find_exact("../etc/passwd"))
            self.assertIsNone(repos.find_exact(" ../etc/passwd"))
            self.assertEqual(probe.call_count, 1)  # cached once, under one key
        self.assertEqual(list(repos._FALLBACK._entries), ["../etc/passwd"])

    def test_a_hostile_name_cannot_be_smuggled_past_validation(self):
        # Whatever the padding, a separator / traversal / .git suffix still
        # fails: the normalized form is what the regex and the folder test see.
        for hostile in ("  a/b  ", "  ../x  ", "  x.git  ", "\t../x\t", "  .hidden  "):
            self.assertIsNone(repos.find_exact(hostile), hostile)
        self.assertEqual([k for k in repos._FALLBACK._entries if k == "legit"], [])


class ProbeSyscallAndHeadTests(_FallbackCase):
    def test_realpath_is_memoized_not_per_lookup(self):
        """The root is resolved ONCE (memoized), not once per slug lookup.

        The old code called ``os.path.abspath`` (no stat) per lookup to keep a
        strict one-stat budget; the new code resolves the root via ``realpath``
        but caches it process-wide, so the per-lookup cost is still one lstat
        of the bare repo. This test proves the memo is hit (realpath is NOT
        re-called on the second lookup of the same root).
        """
        _make_bare(self.root, "statme")
        calls: list[str] = []
        real_realpath = os.path.realpath

        def spy_realpath(p):
            calls.append(os.fspath(p))
            return real_realpath(p)

        with mock.patch("hive.repos.os.path.realpath", side_effect=spy_realpath):
            repos.clear_fallback_cache()
            e1 = repos._probe_argonas_live("statme")
            first_count = len(calls)
            e2 = repos._probe_argonas_live("statme")
            second_count = len(calls)
        self.assertIsNotNone(e1)
        self.assertIsNotNone(e2)
        # The first lookup resolves the root (realpath called); the second
        # lookup hits the memo (realpath NOT called again for the same root).
        self.assertGreaterEqual(first_count, 1)
        self.assertEqual(second_count, first_count, "realpath was re-called per lookup")

    def test_symlink_target_is_never_resolved(self):
        """A symlinked bare repo is refused by lstat, not by resolving its target.

        The root is resolved (memoized), but the BARE REPO path is lstat'ed --
        and lstat does NOT follow symlinks, so a link to anywhere (in-root or
        out-of-root) is refused. This test proves the probe never calls
        realpath on the bare-repo path itself.
        """
        calls: list[tuple[str, str]] = []
        real_lstat, real_realpath = os.lstat, os.path.realpath
        _make_bare(self.root, "target")
        os.symlink(self.outside / "escaped.git", self.root / "link.git")
        os.symlink(self.root / "target.git", self.root / "inner-link.git")
        with (
            mock.patch(
                "hive.repos.os.lstat",
                side_effect=lambda p: (calls.append(("lstat", os.fspath(p))), real_lstat(p))[1],
            ),
            mock.patch(
                "hive.repos.os.path.realpath",
                side_effect=lambda p: (calls.append(("realpath", os.fspath(p))), real_realpath(p))[1],
            ),
        ):
            repos.clear_fallback_cache()
            self.assertIsNone(repos._probe_argonas_live("link"))
            self.assertIsNone(repos._probe_argonas_live("inner-link"))
        # realpath was called only for the ROOT (memoized), never for the
        # bare-repo path. (lstat is also used inside realpath's own
        # component-walk, so we assert on the realpath calls, not lstat.)
        realpath_paths = [p for kind, p in calls if kind == "realpath"]
        self.assertFalse(
            any(p.endswith(".git") for p in realpath_paths),
            f"realpath on a bare repo: {realpath_paths}",
        )

    def test_head_is_parsed_not_split(self):
        cases = {
            "ref: refs/heads/ok-1\n": "ok-1",
            "ref: refs/heads/feature/sub_2\n": "feature/sub_2",
            "HEAD detached at abc1234\n": "master",
            "ref: refs/tags/v1\n": "master",  # not a branch ref
            "ref: refs/heads/..\n": "master",
            "ref: refs/heads/a..b\n": "master",
            "ref: refs/heads/.hidden\n": "master",
            "ref: refs/heads/x.lock\n": "master",
            "ref: refs/heads/HEAD\n": "master",
            "ref: refs/heads/a\x00b\n": "master",
            "ref: refs/heads/a b\n": "master",
            "ref: refs/heads/trailing.\n": "master",
            "": "master",
        }
        for head, expected in cases.items():
            _make_bare(self.root, "headcase", head=head)
            self.assertEqual(
                repos._probe_argonas_live("headcase")["default_branch"],
                expected,
                f"HEAD {head!r}",
            )


class IdContractTests(_FallbackCase):
    """What id a caller gets back: the normalized slug, and padding is the only tolerance."""

    def test_padded_lookup_returns_the_normalized_id(self):
        _make_bare(self.root, "normal-id")
        entry = repos.find_exact("  normal-id  ")
        self.assertEqual(entry["id"], "normal-id")
        # and that id is what get_repo/the router can resolve again
        self.assertEqual(repos.get_repo(entry["id"])["id"], "normal-id")
        self.assertEqual(repos.get_repo("  normal-id  ")["id"], "normal-id")

    def test_case_is_not_a_tolerance_for_the_fallback(self):
        # Catalog lookups are case-insensitive; the filesystem is NOT, so the
        # fallback must not silently answer for a differently-cased name.
        _make_bare(self.root, "lowercase")
        self.assertIsNone(repos.find_exact("LowerCase"))
        self.assertIsNone(repos.get_repo("LOWERCASE"))
        self.assertIsNotNone(repos.find_exact("lowercase"))

    def test_padded_value_still_reports_as_in_catalog_for_jobs(self):
        # Documented effect: repo_in_catalog sees the fallback, padded spelling
        # included, which is what makes a no_repo_mapping decline drift, not rot.
        from hive.bus.domain import jobs

        _make_bare(self.root, "late-for-jobs")
        self.assertTrue(jobs.repo_in_catalog("late-for-jobs"))
        self.assertTrue(jobs.repo_in_catalog("  late-for-jobs  "))
        self.assertFalse(jobs.repo_in_catalog("never-existed"))


class SymlinkEscapeTests(_FallbackCase):
    """A planted link must never make the bus advertise an outside repo."""

    def _plant(self, name: str, inside: bool) -> Path:
        target = (self.root if inside else self.outside) / "victim.git"
        (target / "refs" / "heads").mkdir(parents=True, exist_ok=True)
        (target / "HEAD").write_text("ref: refs/heads/evil\n")
        os.symlink(target, self.root / f"{name}.git")
        return target

    def test_out_of_root_link_is_never_advertised(self):
        self._plant("escapee", inside=False)
        self.assertIsNone(repos._probe_argonas_live("escapee"))
        self.assertIsNone(repos.find_exact("escapee"))
        self.assertIsNone(repos.get_repo("escapee"))
        # nothing was cached as a hit, and no ARGONAS URL was ever produced
        self.assertNotIn("escapee", [k for k, item in repos._FALLBACK._entries.items() if item[1] is not None])

    def test_in_root_link_is_never_advertised(self):
        self._plant("inside-link", inside=True)
        self.assertIsNone(repos._probe_argonas_live("inside-link"))
        self.assertIsNone(repos.find_exact("inside-link"))

    def test_link_to_a_directory_stack_is_still_refused(self):
        # link -> link -> real dir: lstat only ever sees the first link.
        real = self.root / "deep-real.git"
        (real / "refs" / "heads").mkdir(parents=True)
        (real / "HEAD").write_text("ref: refs/heads/trunk\n")
        os.symlink(self.root / "deep-link.git", self.root / "deep-hop.git")
        os.symlink(real, self.root / "deep-link.git")
        self.assertIsNone(repos._probe_argonas_live("deep-hop"))
        self.assertIsNone(repos._probe_argonas_live("deep-link"))
        self.assertIsNotNone(repos._probe_argonas_live("deep-real"))

    def test_probe_never_reads_through_a_link(self):
        calls: list[str] = []
        self._plant("noread", inside=False)
        with mock.patch("hive.repos._read_text", side_effect=lambda p: calls.append(p)):
            self.assertIsNone(repos._probe_argonas_live("noread"))
        self.assertEqual(calls, [])  # rejected before any open()


class StoreIfAbsentTests(unittest.TestCase):
    def test_a_stale_negative_never_overwrites_a_fresh_answer(self):
        cache = repos._FallbackCache()
        cache.store("slug", 60, {"id": "fresh"}, only_if_absent=True)
        stored = cache.store("slug", 10, None, only_if_absent=True)
        self.assertFalse(stored)
        self.assertEqual(cache.lookup("slug"), (True, {"id": "fresh"}))

    def test_a_positive_answer_upgrades_a_live_negative(self):
        # The Negative->Positive direction is allowed: a repo that appeared
        # while this thread was inside the mount is served at once, instead of
        # being denied until the negative TTL runs out.
        cache = repos._FallbackCache()
        cache.store("slug", 10, None)
        self.assertTrue(cache.store("slug", 60, {"id": "slug"}, only_if_absent=True))
        self.assertEqual(cache.lookup("slug"), (True, {"id": "slug"}))

    def test_a_negative_answer_never_displaces_a_live_positive(self):
        cache = repos._FallbackCache()
        cache.store("slug", 60, {"id": "slug"})
        self.assertFalse(cache.store("slug", 10, None, only_if_absent=True))
        self.assertEqual(cache.lookup("slug"), (True, {"id": "slug"}))

    def test_identical_answers_are_a_noop(self):
        cache = repos._FallbackCache()
        cache.store("pos", 60, {"id": "pos"})
        self.assertFalse(cache.store("pos", 60, {"id": "pos"}, only_if_absent=True))
        cache.store("neg", 10, None)
        # Two negative answers to the same probe: the existing entry is kept,
        # its TTL refreshed to the newer store's clock (a confirm, not a win).
        self.assertFalse(cache.store("neg", 10, None, only_if_absent=True))
        self.assertEqual(cache.lookup("neg"), (True, None))

    def test_an_expired_negative_is_refreshed(self):
        # An EXPIRED NEGATIVE is confirmed by a fresh negative probe: kept, with
        # its TTL refreshed to the newer store's clock (a confirm, not a win).
        cache = repos._FallbackCache()
        cache.store("slug", 10, None)
        real = time.monotonic
        with mock.patch("hive.repos.time.monotonic", lambda: real() + 11):
            self.assertFalse(cache.store("slug", 60, None, only_if_absent=True))
        self.assertEqual(cache.lookup("slug"), (True, None))

    def test_expired_positive_is_never_replaced_by_negative(self):
        # An EXPIRED POSITIVE is NOT replaceable by a negative: the repo exists,
        # the "absent" is stale, so the negative refreshes the positive instead
        # (see FallbackCacheTests.test_negative_never_displaces_expired_positive
        # for the full assertion set).
        cache = repos._FallbackCache()
        cache.store("slug", 10, {"id": "old"})
        real = time.monotonic
        with mock.patch("hive.repos.time.monotonic", lambda: real() + 11):
            self.assertFalse(cache.store("slug", 60, None, only_if_absent=True))
        self.assertEqual(cache.lookup("slug"), (True, {"id": "old"}))

    def test_losing_the_race_returns_the_other_threads_answer(self):
        """Probe A started before the repo existed; B finished first with a hit."""
        _make_bare = None
        with mock.patch.object(repos, "_FALLBACK", repos._FallbackCache()):
            repos._FALLBACK.store("raced", 60, {"id": "raced", "winner": "B"})
            # A's probe says "miss", and its store must not win.
            with mock.patch("hive.repos._probe_argonas_live", return_value=None):
                repos._FALLBACK.lookup("raced")  # warm, so no probe happens at all
                answer = repos._fallback_entry("raced")
            self.assertEqual(answer["winner"], "B")

    def test_eviction_removes_one_entry_per_insert(self):
        cache = repos._FallbackCache(max_entries=2)
        cache.store("a", 60, {})
        cache.store("b", 60, {})
        cache.store("c", 60, {})
        self.assertEqual(len(cache), 2)
        self.assertEqual(list(cache._entries), ["b", "c"])


class CatalogWinsTests(_FallbackCase):
    def test_catalog_entry_beats_the_filesystem(self):
        _make_bare(self.root, "shadowed", head="ref: refs/heads/from-disk\n")
        entry = repos.find_exact("shadowed")
        self.assertEqual(entry["display_name"], "Shadowed From Catalog")
        self.assertEqual(entry["sources"], ["overlay"])
        self.assertEqual(entry["default_branch"], "release")
        self.assertEqual(repos.get_repo("shadowed")["default_branch"], "release")

    def test_alias_resolves_to_the_catalog_entry_not_a_live_repo(self):
        _make_bare(self.root, "cat-alias")
        self.assertEqual(repos.find_exact("cat-alias")["id"], "cataloged")
        self.assertEqual(repos.get_repo("cat-alias")["id"], "cataloged")

    def test_list_and_resolve_stay_catalog_only(self):
        _make_bare(self.root, "live-but-unlisted")
        ids = [e["id"] for e in repos.list_repos()]
        self.assertNotIn("live-but-unlisted", ids)
        self.assertEqual(repos.resolve("live-but-unlisted"), [])

    def test_corrupt_catalog_raises_catalog_corruption_error(self):
        """A malformed repos.json must raise CatalogCorruptionError, not a raw
        json.JSONDecodeError -- so a bus endpoint can distinguish 'the build
        artifact is broken, rebuild it' from a transient I/O fault.
        """
        import tempfile as _tf

        td = _tf.TemporaryDirectory()
        try:
            catalog = Path(td.name) / "repos.json"
            catalog.write_text("{ this is not valid json ]")
            with mock.patch.dict(os.environ, {"HIVE_REPO_CATALOG": str(catalog)}):
                repos.clear_fallback_cache()
                with self.assertRaises(repos.CatalogCorruptionError) as ctx:
                    repos.load_catalog(force=True)
            self.assertIn("corrupt JSON", str(ctx.exception))
        finally:
            repos.clear_fallback_cache()
            td.cleanup()


class UnreadableRootTests(unittest.TestCase):
    def test_missing_root_returns_none_and_never_raises(self):
        td = tempfile.TemporaryDirectory()
        try:
            catalog = Path(td.name) / "repos.json"
            catalog.write_text(json.dumps(CATALOG))
            with mock.patch.dict(os.environ, {"HIVE_REPO_CATALOG": str(catalog)}):
                repos.load_catalog(force=True)
                repos.clear_fallback_cache()
                with mock.patch.dict(os.environ, {"HIVE_ARGONAS_GIT_ROOT": str(Path(td.name) / "nope")}):
                    self.assertIsNone(repos._probe_argonas_live("anything"))
                    self.assertIsNone(repos.find_exact("anything"))
        finally:
            repos.clear_fallback_cache()
            td.cleanup()

    def test_root_that_is_a_file_returns_none(self):
        td = tempfile.TemporaryDirectory()
        try:
            root_file = Path(td.name) / "git-root"
            root_file.write_text("")
            with mock.patch.dict(os.environ, {"HIVE_ARGONAS_GIT_ROOT": str(root_file)}):
                self.assertIsNone(repos._probe_argonas_live("anything"))
        finally:
            repos.clear_fallback_cache()
            repos.load_catalog(force=True)
            td.cleanup()

    def test_permission_error_returns_none(self):
        with mock.patch("hive.repos.os.path.isdir", side_effect=PermissionError("denied")):
            self.assertIsNone(repos._probe_argonas_live("anything"))
        with mock.patch("hive.repos.os.path.realpath", side_effect=OSError("stale mount")):
            self.assertIsNone(repos._probe_argonas_live("anything"))

    def test_default_root_is_the_nfs_mount(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("HIVE_ARGONAS_GIT_ROOT", None)
            self.assertEqual(repos.argonas_git_root(), ARGONAS_GIT_PATH_NFS)


class CacheTests(_FallbackCase):
    def test_hit_is_cached(self):
        _make_bare(self.root, "cached")
        with mock.patch("hive.repos._probe_argonas_live", wraps=repos._probe_argonas_live) as probe:
            for _ in range(5):
                self.assertIsNotNone(repos.find_exact("cached"))
            self.assertEqual(probe.call_count, 1)

    def test_negative_result_expires(self):
        with mock.patch("hive.repos._probe_argonas_live", wraps=repos._probe_argonas_live) as probe:
            self.assertIsNone(repos.find_exact("late-repo"))
            # a repo created a moment later is still denied -- the negative
            # cache is what keeps a hostile job string from costing a stat per
            # claim -- until its (short) TTL runs out.
            _make_bare(self.root, "late-repo")
            self.assertIsNone(repos.find_exact("late-repo"))
            self.assertEqual(probe.call_count, 1)
            real = time.monotonic
            with mock.patch("hive.repos.time.monotonic", lambda: real() + repos._NEGATIVE_TTL + 0.01):
                self.assertIsNotNone(repos.find_exact("late-repo"))
            self.assertEqual(probe.call_count, 2)

    def test_positive_result_expires(self):
        self.assertEqual(repos._POSITIVE_TTL, 60.0)
        self.assertEqual(repos._NEGATIVE_TTL, 10.0)
        bare = _make_bare(self.root, "vanishing")
        self.assertIsNotNone(repos.find_exact("vanishing"))
        self.assertEqual(len(repos._FALLBACK), 1)  # one stat-backed cache entry
        bare.rename(bare.with_name("vanishing-gone.git"))
        # still cached: the whole point of the positive TTL
        self.assertIsNotNone(repos.find_exact("vanishing"))
        real = time.monotonic
        with mock.patch("hive.repos.time.monotonic", lambda: real() + repos._POSITIVE_TTL + 0.01):
            self.assertIsNone(repos.find_exact("vanishing"))

    def test_clear_drops_everything(self):
        repos.find_exact("whatever")
        self.assertTrue(len(repos._FALLBACK))
        repos.clear_fallback_cache()
        self.assertEqual(len(repos._FALLBACK), 0)

    def test_probe_runs_with_the_cache_lock_free(self):
        """The mount I/O must happen with NO cache lock held.

        A stalled NFS lookup parks inside the probe; if the cache lock were
        held there, every other slug would queue behind it. Proven without
        timing: the probe takes the cache lock itself and must get it at once.
        """
        observed = {}

        def probe(slug):
            acquired = repos._FALLBACK._lock.acquire(timeout=1.0)
            observed["lock_free"] = acquired
            if acquired:
                repos._FALLBACK._lock.release()
            return None

        with mock.patch("hive.repos._probe_argonas_live", side_effect=probe):
            repos.find_exact("some-slug")
        self.assertTrue(observed.get("lock_free"), "probe ran with the cache lock held")

    def test_a_slow_probe_does_not_block_other_slugs(self):
        """A parked probe on slug A leaves B's cached hit immediate."""
        _make_bare(self.root, "warm")
        self.assertIsNotNone(repos.find_exact("warm"))  # cached positive
        inside = threading.Event()
        release = threading.Event()

        def parked(slug):
            inside.set()
            release.wait(5)
            return None

        with mock.patch("hive.repos._probe_argonas_live", side_effect=parked):
            slow = threading.Thread(target=repos.find_exact, args=("stalled",), daemon=True)
            slow.start()
            self.assertTrue(inside.wait(2), "probe never started")
            started = time.monotonic()
            self.assertIsNotNone(repos.find_exact("warm"))
            self.assertLess(time.monotonic() - started, 1.0)
            release.set()
            slow.join(5)


class FallbackCacheTests(unittest.TestCase):
    """The cache is one lock over an ordered dict: no second lock, no race."""

    def test_lookup_returns_the_entry_not_the_bookkeeping(self):
        cache = repos._FallbackCache()
        self.assertEqual(cache.lookup("nope"), (False, None))
        cache.store("hit", 60, {"id": "hit"})
        self.assertEqual(cache.lookup("hit"), (True, {"id": "hit"}))
        cache.store("miss", 10, None)  # a NEGATIVE result is cached too
        self.assertEqual(cache.lookup("miss"), (True, None))  # hit, answer is None
        self.assertEqual(cache.lookup("hit"), (True, {"id": "hit"}))  # still a hit

    def test_expiry_is_dropped_on_read(self):
        cache = repos._FallbackCache()
        cache.store("soon", 10, {"id": "soon"})
        real = time.monotonic
        with mock.patch("hive.repos.time.monotonic", lambda: real() + 10.01):
            self.assertEqual(cache.lookup("soon"), (False, None))
        self.assertEqual(len(cache), 0)

    def test_one_lock_only(self):
        # A second lock is exactly what allowed the eviction race; there must be
        # no lock on the cache other than the one guarding the dict.
        cache = repos._FallbackCache()
        locks = [n for n, v in vars(cache).items() if isinstance(v, type(threading.Lock()))]
        self.assertEqual(locks, ["_lock"])

    def test_evicts_exactly_one_lru_entry(self):
        cache = repos._FallbackCache(max_entries=3)
        for i in range(3):
            cache.store(f"slug-{i}", 60, {"id": f"slug-{i}"})
        self.assertEqual(len(cache), 3)
        cache.lookup("slug-0")  # touch the oldest: it is now the most recent
        cache.store("slug-3", 60, {"id": "slug-3"})
        self.assertEqual(len(cache), 3)
        self.assertTrue(cache.lookup("slug-0")[0])
        self.assertTrue(cache.lookup("slug-3")[0])
        self.assertFalse(cache.lookup("slug-1")[0])  # only the LRU end went
        self.assertTrue(cache.lookup("slug-2")[0])

    def test_concurrent_stores_at_capacity_evict_one_per_store(self):
        """Regression: the eviction loop was `while`, so N concurrent stores
        at capacity evicted N entries, not one per store.

        With the `if` fix, after filling the cache and running many concurrent
        stores, the cache is still at capacity and the survivors are the most
        recently stored -- exactly one LRU entry is dropped per store.
        """
        cache = repos._FallbackCache(max_entries=4)
        # Fill to capacity.
        for i in range(4):
            cache.store(f"slug-{i}", 60, {"id": f"slug-{i}"})
        self.assertEqual(len(cache), 4)

        # Hammer with concurrent stores of NEW keys. Each store evicts at most
        # one entry, so after 10 new stores the cache holds the 4 most recent.
        barrier = threading.Barrier(4)

        def hammer(worker: int) -> None:
            barrier.wait()
            for i in range(10):
                cache.store(f"w{worker}-new-{i}", 60, {"w": worker, "i": i})

        threads = [threading.Thread(target=hammer, args=(w,)) for w in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(10)
        # The cache must be exactly at capacity -- never over, never under
        # (under would mean the `while` bug evicted more than one per store).
        self.assertEqual(len(cache), 4, f"cache at {len(cache)}, expected 4")

    def test_ttl_refresh_on_cas_lose(self):
        """Regression: when two threads race to store the SAME positive answer,
        the CAS loser used to leave the winner's entry on its (shorter) TTL.
        Now the loser refreshes the TTL to its own (longer) value.

        The probe path always stores a POSITIVE entry with a 60s TTL, so the
        race is two positive stores of the same answer: the first (the thread
        that probed first) cached it, the second (the thread that probed
        slightly later) "loses" the CAS but must refresh the TTL.
        """
        cache = repos._FallbackCache()
        real = time.monotonic
        t0 = real()
        # Thread 1 stores the positive entry at t0 with a 60s TTL.
        with mock.patch("hive.repos.time.monotonic", lambda: t0):
            cache.store("race", 60.0, {"id": "race"}, only_if_absent=True)
        # Thread 2 (a concurrent probe of the same repo) stores the SAME
        # positive answer 30s later via only_if_absent. The existing entry is
        # live and positive, so the CAS "loses" (returns False) -- but the
        # entry's TTL must be refreshed to t0+30+60, not left at t0+60.
        with mock.patch("hive.repos.time.monotonic", lambda: t0 + 30.0):
            stored = cache.store("race", 60.0, {"id": "race"}, only_if_absent=True)
        self.assertFalse(stored, "CAS must lose when a live positive already exists")
        # Without the refresh the entry expires at t0+60; with it, at t0+90.
        with mock.patch("hive.repos.time.monotonic", lambda: t0 + 70.0):
            self.assertTrue(cache.lookup("race")[0], "TTL was not refreshed by the CAS loser")
        with mock.patch("hive.repos.time.monotonic", lambda: t0 + 91.0):
            self.assertFalse(cache.lookup("race")[0], "refreshed TTL should expire at t0+90")

    def test_negative_still_never_displaces_live_positive(self):
        """The asymmetric CAS rule: a NEGATIVE answer never displaces a live
        POSITIVE one, even via only_if_absent.
        """
        cache = repos._FallbackCache()
        cache.store("repo", 60, {"id": "repo"}, only_if_absent=True)
        # A negative probe (repo "disappeared" between probe and store) must
        # NOT overwrite the live positive.
        stored = cache.store("repo", 10, None, only_if_absent=True)
        self.assertFalse(stored)
        hit, entry = cache.lookup("repo")
        self.assertTrue(hit)
        self.assertEqual(entry, {"id": "repo"})

    def test_negative_never_displaces_expired_positive(self):
        """Regression: a NEGATIVE answer must not displace a POSITIVE one merely
        because its TTL lapsed.

        The positive TTL is 60s; if a concurrent negative probe's store runs
        after that window, the old code saw the positive as "expired" and let
        the negative overwrite it -- making a real repo intermittently 404
        across a TTL boundary. Now the negative is refused (the positive's TTL
        is refreshed instead), because the repo still exists on the mount.
        """
        cache = repos._FallbackCache()
        real = time.monotonic
        t0 = real()
        with mock.patch("hive.repos.time.monotonic", lambda: t0):
            cache.store("repo", 60.0, {"id": "repo"}, only_if_absent=True)
        # 61s later the positive has expired; a negative probe arrives.
        with mock.patch("hive.repos.time.monotonic", lambda: t0 + 61.0):
            stored = cache.store("repo", 10.0, None, only_if_absent=True)
        self.assertFalse(stored, "a negative must not displace an expired positive")
        # The positive was REFRESHED, not replaced: it still serves and now
        # outlives t0+61.
        with mock.patch("hive.repos.time.monotonic", lambda: t0 + 62.0):
            hit, entry = cache.lookup("repo")
        self.assertTrue(hit, "expired positive should have been refreshed, not evicted")
        self.assertEqual(entry, {"id": "repo"})
        # And a POSITIVE re-probe confirms the (now refreshed) positive: a
        # confirm is a no-op (keeps the existing value, refreshes the TTL), and
        # the asymmetry is about value, not clock -- a positive never "wins"
        # back what a negative could not take.
        with mock.patch("hive.repos.time.monotonic", lambda: t0 + 62.0):
            self.assertFalse(cache.store("repo", 60.0, {"id": "repo"}, only_if_absent=True))
        with mock.patch("hive.repos.time.monotonic", lambda: t0 + 63.0):
            self.assertEqual(cache.lookup("repo"), (True, {"id": "repo"}))

    def test_negative_confirms_expired_negative(self):
        """A negative confirms an expired negative: kept, TTL refreshed (no win)."""
        cache = repos._FallbackCache()
        real = time.monotonic
        t0 = real()
        with mock.patch("hive.repos.time.monotonic", lambda: t0):
            cache.store("ghost", 10.0, None, only_if_absent=True)
        with mock.patch("hive.repos.time.monotonic", lambda: t0 + 11.0):
            stored = cache.store("ghost", 10.0, None, only_if_absent=True)
        self.assertFalse(stored, "identical answers are a no-op")
        # Refreshed: still a hit just past the original 10s window.
        with mock.patch("hive.repos.time.monotonic", lambda: t0 + 12.0):
            self.assertEqual(cache.lookup("ghost"), (True, None))

    def test_eviction_keeps_warm_entries_within_bounds(self):
        cache = repos._FallbackCache(max_entries=4)
        for i in range(10):
            cache.store(f"slug-{i}", 60, {"id": f"slug-{i}"})
        self.assertEqual(len(cache), 4)
        for i in (6, 7, 8, 9):  # the survivors are still warm
            self.assertTrue(cache.lookup(f"slug-{i}")[0])

    def test_concurrent_store_and_lookup_never_raise(self):
        """Regression for the two-lock eviction race (KeyError / wrong victim)."""
        cache = repos._FallbackCache(max_entries=8)
        errors: list[str] = []

        def hammer(worker: int) -> None:
            try:
                for i in range(3000):
                    cache.store(f"slug-{i % 23}", 60, {"worker": worker, "i": i})
                    cache.lookup(f"slug-{(i * 7) % 23}")
                    len(cache)
            except Exception as exc:  # noqa: BLE001 - the point is that nothing raises
                errors.append(repr(exc))

        threads = [threading.Thread(target=hammer, args=(w,)) for w in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        self.assertEqual(errors, [])
        self.assertLessEqual(len(cache), 8)

    def test_through_the_public_lookup_path_is_bounded(self):
        with mock.patch.object(repos, "_FALLBACK", repos._FallbackCache(max_entries=4)):
            for i in range(10):
                repos.find_exact(f"miss-{i}")
            self.assertEqual(len(repos._FALLBACK), 4)


class OldCodeContrastTests(unittest.TestCase):
    """Shows the declared property fails without the fallback.

    Runs the pre-fallback ``hive/repos.py`` next to this one with the same temp
    root. The old module must MISS; the checked-out module must HIT. Skipped
    when the baseline revision is not available (shallow export, no git).

    The baseline is the last commit whose ``hive/repos.py`` has NO
    ``_fallback_entry`` (``113a802`` at the time this test was written).
    ``origin/deploy/integrate-20261017`` is NOT usable: it moves over time, and
    once the ARGONAS fallback landed on it (2026-10-26) the "old" module
    resolved post-build repos too, so the contrast silently proved nothing.
    """

    BASELINE = "113a802"

    def _load_baseline(self, path: Path):
        spec = importlib.util.spec_from_file_location("baseline_repos", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_old_lookup_misses_what_the_new_one_resolves(self):
        repo_root = Path(__file__).resolve().parent
        try:
            source = subprocess.check_output(
                ["git", "-C", str(repo_root), "show", f"{self.BASELINE}:hive/repos.py"],
                stderr=subprocess.DEVNULL,
            )
        except (subprocess.CalledProcessError, FileNotFoundError, OSError):
            # No git binary (FileNotFoundError/OSError) or no such revision
            # (shallow clone, bare export): an environment gap, not a defect.
            self.skipTest(f"{self.BASELINE} not available")
        # Guard the premise: the pinned baseline must be a pre-fallback module,
        # or the contrast would pass vacuously (see class docstring).
        self.assertNotIn(b"_fallback_entry", source, f"baseline {self.BASELINE} already has the fallback")
        # The contrast needs an ARGONAS root the baseline module can probe.
        # tempfile's default is /tmp, which is tmpfs on this host; a repo under
        # tmpfs is a fixture the fleet rule (no repo on tmpfs) does not intend
        # to cover, so this test builds its fixture under the job workspace
        # instead (never /tmp).
        base = Path(tempfile.mkdtemp(prefix=".argonas_contrast-", dir=str(Path(__file__).resolve().parent)))
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(base)], check=False))
        try:
            root = base / "argonas_git"
            root.mkdir()
            _make_bare(root, "post-build-repo")
            catalog = base / "repos.json"
            catalog.write_text(json.dumps(CATALOG))
            old_path = base / "old_repos.py"
            old_path.write_bytes(source)
            env = {
                "HIVE_REPO_CATALOG": str(catalog),
                "HIVE_ARGONAS_GIT_ROOT": str(root),
            }
            with mock.patch.dict(os.environ, env):
                old = self._load_baseline(old_path)
                self.assertIsNone(
                    old.find_exact("post-build-repo"),
                    "baseline code unexpectedly resolved an uncatalogued repo",
                )
                if hasattr(repos, "clear_fallback_cache"):  # absent on the baseline itself
                    repos.clear_fallback_cache()
                repos.load_catalog(force=True)
                self.assertIsNotNone(repos.find_exact("post-build-repo"))
        finally:
            if hasattr(repos, "clear_fallback_cache"):
                repos.clear_fallback_cache()
            repos.load_catalog(force=True)


class ImportSurfaceTests(unittest.TestCase):
    def test_no_second_hardcoded_argonas_spelling(self):
        """The host/user/path live in hive.registry only, never copied here."""
        source = (Path(__file__).resolve().parent / "hive" / "repos.py").read_text()
        for literal in (ARGONAS_SSH_HOST, f"{ARGONAS_SSH_USER}@", ARGONAS_GIT_PATH_SSH, ARGONAS_GIT_PATH_NFS):
            self.assertNotIn(literal, source)

    def test_bus_router_serves_the_fallback(self):
        """The worker's actual path: GET /v1/repos/{slug} -> get_repo."""
        td = tempfile.TemporaryDirectory()
        try:
            base = Path(td.name)
            root = base / "argonas_git"
            root.mkdir()
            _make_bare(root, "from-a-job")
            catalog = base / "repos.json"
            catalog.write_text(json.dumps(CATALOG))
            with mock.patch.dict(
                os.environ,
                {"HIVE_REPO_CATALOG": str(catalog), "HIVE_ARGONAS_GIT_ROOT": str(root)},
            ):
                if importlib.util.find_spec("fastapi") is None:
                    self.skipTest("fastapi not installed")
                from hive.bus.routers import repos as router_module

                # The endpoint is a plain async def; call it directly rather
                # than standing up the app (no DB/DI involved).
                result = asyncio.run(router_module.get_repo("from-a-job"))
                self.assertEqual(result["id"], "from-a-job")
                self.assertIn("/mnt/datapool/git/from-a-job.git", result["clone"]["ssh"])
        finally:
            repos.clear_fallback_cache()
            repos.load_catalog(force=True)
            td.cleanup()


if __name__ == "__main__":
    unittest.main()
