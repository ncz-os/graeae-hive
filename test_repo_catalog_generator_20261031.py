"""FB59: repo catalog generator — determinism, no-drop, missing-remote flag,
bus round-trip, and the file<->generator drift check.

``hive.registry.build_registry`` merges five live sources with the curated
overlay into ``data/repos.json``; ``hive.repos`` serves the built file to the
bus. All live sources are mocked here (no network, no creds); subprocesses
get an empty HOME so no real ``~/.api_keys_master.json`` is read.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import hive.registry as regmod  # noqa: E402
from hive.core.config import get_settings  # noqa: E402

ROOT = Path(__file__).resolve().parent
SHIPPED = ROOT / "data" / "repos.json"
SHIPPED_OVERLAY = ROOT / "data" / "repos_overlay.json"

GL = {"g-only": {"gitlab": {"path_with_namespace": "ncz-os/g-only"}}}
GH = {"h-only": {"github": {"full_name": "o/h-only"}}}
BK = {"b-only": {"buildkite": {"org": "ncz-os", "pipeline": "b-only"}}}
FAKE_DISCOVER = {
    "discover_gitlab": GL,
    "discover_github": GH,
    "discover_buildkite": BK,
    "discover_argonas": {"a-only"},
    "discover_r2": [{"name": "r2-bucket"}],
}
OVERLAY = {"repos": {"o-only": {"notes": "overlay only entry"}, "g-only": {"notes": "curated note"}}}
ALL_LIVE = ("gitlab", "github", "argonas", "buildkite", "r2")

CLEAN_ENV = {
    "GITLAB_PAT": "",
    "GITHUB_PAT": "",
    "BUILDKITE_API_TOKEN": "",
    "R2_ACCOUNT_ID": "",
    "CLOUDFLARE_R2_API_TOKEN": "",
    "ARGONAS_SSH_PASS": "",
    "ARGONAS_SSH_HOST": "",
}


def _clean_env():
    p = mock.patch.dict(os.environ, CLEAN_ENV)
    p.start()
    return p


def _build(overlay=OVERLAY, skip=(), discoverers=None, creds=None, called=None):
    """build_registry with mocked sources and a temp overlay file.

    Returns ``(registry, {discover_name: mock})`` so a caller can assert a
    source was actually consulted; pass ``called`` to receive that same dict
    instead of the returned copy.

    A source is only consulted when its credential is set (see
    ``build_registry``): no PAT, no ``discover_*`` call. The test must not
    depend on which host's ``~/.api_keys_master.json`` is reachable, so it
    supplies a fixed, fake credential for every forge source and pins the
    operator-config module constants it reads -- a box without the key file
    (the Linux build host) or with a different one (macOS) then builds the
    same registry.
    """
    td = tempfile.TemporaryDirectory()
    op = Path(td.name) / "overlay.json"
    op.write_text(json.dumps(overlay))
    fake_creds = {
        "gitlab_pat": "test-gitlab-pat",
        "github_token": "test-github-token",
        "buildkite_token": "test-buildkite-token",
        "r2_account_id": "test-r2-account",
        "r2_token": "test-r2-token",
        "argonas_password": None,
        **(creds or {}),
    }
    record: dict[str, mock.Mock] = called if called is not None else {}
    patches = [
        mock.patch.object(regmod, "ARGONAS_SSH_HOST", ""),
        mock.patch.object(regmod, "_keys", return_value={}),
        mock.patch.object(
            regmod,
            "get_settings",
            return_value=SimpleNamespace(
                gitlab_pat="", github_pat="", buildkite_api_token="", r2_account_id="", argonas_ssh_pass=""
            ),
        ),
    ]
    for name in FAKE_DISCOVER:
        value = (discoverers or {}).get(name, FAKE_DISCOVER[name])
        record[name] = mock.Mock(return_value=value)
        patches.append(mock.patch.object(regmod, name, record[name]))
    for p in patches:
        p.start()
    try:
        return regmod.build_registry(overlay_path=op, skip=skip, **fake_creds), record
    finally:
        for p in reversed(patches):
            p.stop()
        td.cleanup()


def canonical_bytes(reg):
    return (json.dumps(reg, indent=2, sort_keys=True) + "\n").encode()


class DeterminismTests(unittest.TestCase):
    def test_double_build_is_byte_identical(self):
        """Invariant: the same inputs always produce the same bytes."""
        self.assertEqual(canonical_bytes(_build()[0]), canonical_bytes(_build()[0]))

    def test_byte_identical_across_hash_seeds(self):
        """Invariant: no set-ordering hazard — two fresh interpreters with
        different PYTHONHASHSEED emit identical bytes (overlay-only inputs,
        so no network is possible)."""
        with tempfile.TemporaryDirectory() as td:
            op = Path(td) / "overlay.json"
            op.write_text(json.dumps(OVERLAY))
            # Concatenation, not % formatting: a % in a temp path must not break it.
            helper = (
                "import json,sys; sys.path.insert(0, " + json.dumps(str(ROOT)) + "); "
                "from hive.registry import build_registry; "
                "reg = build_registry(overlay_path=" + json.dumps(str(op)) + ", "
                "skip=('gitlab','github','argonas','buildkite','r2')); "
                "sys.stdout.write(json.dumps(reg, indent=2, sort_keys=True) + chr(10))"
            )
            home = Path(td) / "home"
            home.mkdir()
            outs = []
            for seed in ("0", "1"):
                env = {
                    **os.environ,
                    "PYTHONHASHSEED": seed,
                    "HOME": str(home),
                    **CLEAN_ENV,
                    "HIVE_REPO_CATALOG": os.devnull,
                }
                r = subprocess.run(
                    [sys.executable, "-c", helper],
                    capture_output=True,
                    text=True,
                    env=env,
                    timeout=120,
                )
                self.assertEqual(r.returncode, 0, r.stderr[-2000:])
                outs.append(r.stdout)
            self.assertEqual(outs[0], outs[1])
            self.assertTrue(json.loads(outs[0])["repos"])


class NoDropTests(unittest.TestCase):
    def setUp(self):
        self._env = _clean_env()
        self.addCleanup(self._env.stop)
        if hasattr(get_settings, "cache_clear"):
            get_settings.cache_clear()
            self.addCleanup(get_settings.cache_clear)

    def test_every_source_slug_survives(self):
        """Invariant: a slug known to exactly one source is in the output
        with the right source tag — the merge drops nothing.

        The call count assertions keep a skipped source from passing
        vacuously: a forge with no credential is never discovered, so its
        slug would come back overlay-only and the tag check below would be
        asserting against the one source it excluded."""
        reg, called = _build()
        by_id = {e["id"]: e for e in reg["repos"]}
        for slug, tag in (
            ("g-only", "gitlab"),
            ("h-only", "github"),
            ("a-only", "argonas"),
            ("b-only", "buildkite"),
            ("o-only", "overlay"),
        ):
            self.assertIn(slug, by_id, slug)
            self.assertIn(tag, by_id[slug]["sources"], slug)
        # A forge discoverer takes its credential positionally; the
        # credential-free sources take no argument.
        for name in ("discover_gitlab", "discover_github", "discover_buildkite"):
            called[name].assert_called_once()
        called["discover_argonas"].assert_called_once_with(password=None)
        called["discover_r2"].assert_called_once()
        self.assertEqual(reg["_meta"]["r2_buckets"], ["r2-bucket"])

    def test_overlay_curated_fields_land_on_discovered_entries(self):
        """Invariant: curated overlay fields attach to the entry DISCOVERY
        built; the source tag proves the entry was discovered, not merely
        created by ``merge_overlay``."""
        reg, called = _build()
        by_id = {e["id"]: e for e in reg["repos"]}
        self.assertEqual(by_id["g-only"]["notes"], "curated note")
        self.assertIn("gitlab", by_id["g-only"]["sources"])
        called["discover_gitlab"].assert_called_once_with("test-gitlab-pat")


class MissingRemoteTests(unittest.TestCase):
    def setUp(self):
        self._env = _clean_env()
        self.addCleanup(self._env.stop)
        if hasattr(get_settings, "cache_clear"):
            get_settings.cache_clear()
            self.addCleanup(get_settings.cache_clear)

    def test_overlay_only_entry_has_no_clone_recipe(self):
        """Invariant (the flag): an entry with no live remote is marked
        sources==["overlay"] and carries NO clone_ssh/clone_nfs — it cannot
        be mistaken for a healthy, clonable repo."""
        reg, _ = _build()
        entry = next(e for e in reg["repos"] if e["id"] == "o-only")
        self.assertEqual(entry["sources"], ["overlay"])
        self.assertNotIn("clone_ssh", entry)
        self.assertNotIn("clone_nfs", entry)

    def test_unreachable_source_does_not_kill_the_build(self):
        """Invariant: a dead remote (the discover_* failure shape is an empty
        result — they swallow HTTP/transport errors internally) still yields a
        successful build of the rest; the gap stays visible as overlay-only."""
        reg, _ = _build(
            discoverers={
                "discover_gitlab": {},
                "discover_github": {},
                "discover_buildkite": {},
                "discover_argonas": set(),
                "discover_r2": [],
            }
        )
        by_id = {e["id"]: e for e in reg["repos"]}
        self.assertEqual(set(by_id), {"o-only", "g-only"})
        self.assertEqual(reg["_meta"]["r2_buckets"], [])


class BusRoundTripTests(unittest.TestCase):
    def test_generated_ids_are_bus_knowledge(self):
        """Invariant: every generated id loads through the bus catalog and
        resolves via find_exact — the generator output IS bus knowledge."""
        import hive.repos as repos_mod

        reg, _ = _build()
        with tempfile.TemporaryDirectory() as td:
            cat = Path(td) / "repos.json"
            cat.write_bytes(canonical_bytes(reg))
            with mock.patch.dict(os.environ, {"HIVE_REPO_CATALOG": str(cat)}):
                catalog = repos_mod.load_catalog(force=True)
                self.assertEqual(set(catalog), {e["id"] for e in reg["repos"]})
                for e in reg["repos"]:
                    found = repos_mod.find_exact(e["id"])
                    self.assertIsNotNone(found, e["id"])
                    self.assertEqual(found["id"], e["id"])


class DriftTests(unittest.TestCase):
    def test_shipped_file_is_canonical(self):
        """Invariant: data/repos.json is in canonical form (indent=2,
        sort_keys) with a consistent count and id-sorted rows — a hand-edit
        that is not a generator write fails this check."""
        raw = SHIPPED.read_bytes()
        reg = json.loads(raw)
        self.assertEqual(raw, canonical_bytes(reg))
        self.assertEqual(reg["_meta"]["count"], len(reg["repos"]))
        self.assertEqual(reg["_meta"]["generated_by"], "hive.registry.build_registry")
        ids = [e["id"] for e in reg["repos"]]
        self.assertEqual(ids, sorted(ids))

    def test_shipped_file_agrees_with_shipped_overlay(self):
        """Invariant (the drift check): regenerating with all live sources
        skipped — i.e. overlay alone — must reproduce every overlay slug and
        curated field in the shipped file. Editing repos_overlay.json without
        rebuilding repos.json fails here."""
        import hive.repos as repos_mod
        from hive.registry import slugify

        shipped = repos_mod.load_catalog(path=SHIPPED, force=True)
        overlay_repos = json.loads(SHIPPED_OVERLAY.read_text())["repos"]
        self.assertTrue(overlay_repos, "overlay is empty — check vacuously true?")
        # The generator slugifies overlay keys (merge_overlay), so the drift
        # check compares slugified — e.g. overlay "InvestorClaw" ships as
        # "investorclaw".
        missing = [s for s in overlay_repos if slugify(s) not in shipped]
        self.assertEqual(missing, [])
        for slug, curated in overlay_repos.items():
            for key, value in curated.items():
                self.assertEqual(shipped[slugify(slug)].get(key), value, (slug, key))


if __name__ == "__main__":
    unittest.main()
