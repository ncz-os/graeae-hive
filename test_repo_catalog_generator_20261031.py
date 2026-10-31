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


def _build(overlay=OVERLAY, skip=(), discoverers=None):
    """build_registry with mocked sources and a temp overlay file."""
    td = tempfile.TemporaryDirectory()
    op = Path(td.name) / "overlay.json"
    op.write_text(json.dumps(overlay))
    patches = [mock.patch.object(regmod, "ARGONAS_SSH_HOST", "")]
    for name in FAKE_DISCOVER:
        value = (discoverers or {}).get(name, FAKE_DISCOVER[name])
        patches.append(mock.patch.object(regmod, name, return_value=value))
    for p in patches:
        p.start()
    try:
        return regmod.build_registry(overlay_path=op, skip=skip)
    finally:
        for p in reversed(patches):
            p.stop()
        td.cleanup()


def canonical_bytes(reg):
    return (json.dumps(reg, indent=2, sort_keys=True) + "\n").encode()


class DeterminismTests(unittest.TestCase):
    def test_double_build_is_byte_identical(self):
        """Invariant: the same inputs always produce the same bytes."""
        self.assertEqual(canonical_bytes(_build()), canonical_bytes(_build()))

    def test_byte_identical_across_hash_seeds(self):
        """Invariant: no set-ordering hazard — two fresh interpreters with
        different PYTHONHASHSEED emit identical bytes (overlay-only inputs,
        so no network is possible)."""
        helper = (
            "import json,sys; sys.path.insert(0, %r); "
            "from hive.registry import build_registry; "
            "reg = build_registry(overlay_path=%r, skip=('gitlab','github','argonas','buildkite','r2')); "
            "sys.stdout.write(json.dumps(reg, indent=2, sort_keys=True) + chr(10))"
        )
        with tempfile.TemporaryDirectory() as td:
            op = Path(td) / "overlay.json"
            op.write_text(json.dumps(OVERLAY))
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
                    [sys.executable, "-c", helper % (str(ROOT), str(op))],
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
        with the right source tag — the merge drops nothing."""
        reg = _build()
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
        self.assertEqual(reg["_meta"]["r2_buckets"], ["r2-bucket"])

    def test_overlay_curated_fields_land_on_discovered_entries(self):
        reg = _build()
        by_id = {e["id"]: e for e in reg["repos"]}
        self.assertEqual(by_id["g-only"]["notes"], "curated note")
        self.assertIn("gitlab", by_id["g-only"]["sources"])


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
        reg = _build()
        entry = next(e for e in reg["repos"] if e["id"] == "o-only")
        self.assertEqual(entry["sources"], ["overlay"])
        self.assertNotIn("clone_ssh", entry)
        self.assertNotIn("clone_nfs", entry)

    def test_unreachable_source_does_not_kill_the_build(self):
        """Invariant: a dead remote (the discover_* failure shape is an empty
        result — they swallow HTTP/transport errors internally) still yields a
        successful build of the rest; the gap stays visible as overlay-only."""
        reg = _build(
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

        reg = _build()
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
