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
        self.outside = Path(self.td.name) / "outside"
        self.outside.mkdir()
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

    def test_symlink_inside_the_root_still_works(self):
        # A link that stays inside the root is followed and synthesized with
        # the ID THE CALLER ASKED FOR -- never the link target's name.
        _make_bare(self.root, "real-one", head="ref: refs/heads/main\n")
        os.symlink(self.root / "real-one.git", self.root / "alias-one.git")
        entry = repos._probe_argonas_live("alias-one")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["id"], "alias-one")
        self.assertEqual(entry["display_name"], "alias-one")
        self.assertEqual(entry["argonas_bare"], str(self.root / "alias-one.git"))
        self.assertEqual(
            entry["clone_ssh"],
            "ssh://jasonperlow@192.0.2.101/mnt/datapool/git/alias-one.git",
        )
        self.assertEqual(entry["default_branch"], "main")  # read through the link
        self.assertEqual(entry["sources"], ["argonas-live"])
        self.assertEqual(entry["status"], "active")

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
        for bad in ("a\\b", "a~b", "a^b", "a:b", "a?b", "a*b", "a[b", "a\x7fb", "a b"):
            _make_bare(self.root, "refcheck", head=f"ref: refs/heads/{bad}\n")
            self.assertEqual(
                repos._probe_argonas_live("refcheck")["default_branch"],
                "master",
                f"{bad!r} must not be accepted as a branch name",
            )
        _make_bare(self.root, "refcheck", head="ref: refs/heads/feat/fine-name_1\n")
        self.assertEqual(repos._probe_argonas_live("refcheck")["default_branch"], "feat/fine-name_1")

    def test_symlinked_root_is_its_own_realpath(self):
        # A legitimately symlinked MOUNT is fine: containment is compared in
        # realpath space, not against the unresolved spelling.
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

    Runs the BASELINE's ``hive/repos.py`` (git show of ``origin/deploy/
    integrate-20261017``) next to this one with the same temp root. The old
    module must MISS; the checked-out module must HIT. Skipped when the
    baseline revision is not available (shallow export, no git).
    """

    BASELINE = "origin/deploy/integrate-20261017"

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
        td = tempfile.TemporaryDirectory()
        try:
            base = Path(td.name)
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
            td.cleanup()


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
