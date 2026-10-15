"""Tests for the repo registry (hive.repos loader + hive.registry discovery).

All offline: discovery sources are exercised with synthetic data; the loader is
exercised against a temp catalog. No network, no PATs.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path

from hive import repos
from hive.registry import correlate, merge_overlay, slugify

CATALOG = {
    "_meta": {"generated_by": "test"},
    "repos": [
        {
            "id": "graeae-hive-mind",
            "display_name": "GRAEAE Hive Mind",
            "aliases": ["hive", "agent-bus"],
            "sources": ["gitlab", "argonas", "overlay"],
            "argonas_bare": "/mnt/argonas_git/graeae-hive-mind.git",
            "clone_ssh": "ssh://jasonperlow@192.0.2.101/mnt/datapool/git/graeae-hive-mind.git",
            "gitlab": {"url": "https://gitlab.com/ncz-os/graeae-hive-mind", "default_branch": "master"},
            "live": [{"host": "pythia", "path": "/srv/agent-bus", "service": "graeae-hive.service"}],
            "language": "python",
            "commands": {"test": "python3 -m unittest", "lint": "ruff check --fix"},
            "deps": ["mnemos"],
            "mnemos": {"required": True, "tags": ["hive"], "queries": ["hive architecture", "hive job lifecycle"]},
        },
        {
            "id": "mnemos",
            "display_name": "MNEMOS",
            "sources": ["gitlab", "overlay"],
            "gitlab": {"url": "https://gitlab.com/ncz-os/mnemos", "default_branch": "feat/db2-native"},
            "mnemos": {"required": False, "tags": ["mnemos"]},
        },
    ],
}


class LoaderTests(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.path = Path(self.td.name) / "repos.json"
        self.path.write_text(json.dumps(CATALOG))
        self._old = os.environ.get("HIVE_REPO_CATALOG")
        os.environ["HIVE_REPO_CATALOG"] = str(self.path)
        repos.load_catalog(force=True)

    def tearDown(self):
        if self._old is None:
            os.environ.pop("HIVE_REPO_CATALOG", None)
        else:
            os.environ["HIVE_REPO_CATALOG"] = self._old
        repos.load_catalog(force=True)  # drop the temp catalog from cache
        self.td.cleanup()

    def test_get_by_id_alias_and_miss(self):
        self.assertEqual(repos.get_repo("graeae-hive-mind")["id"], "graeae-hive-mind")
        self.assertEqual(repos.get_repo("hive")["id"], "graeae-hive-mind")  # alias
        self.assertEqual(repos.get_repo("agent-bus")["id"], "graeae-hive-mind")
        self.assertIsNone(repos.get_repo("nope"))

    def test_load_for_host_live_vs_clone(self):
        live = repos.get_repo("graeae-hive-mind", host="pythia")["load_for_host"]
        self.assertEqual(live["mode"], "live")
        self.assertEqual(live["path"], "/srv/agent-bus")
        clone = repos.get_repo("graeae-hive-mind", host="studio")["load_for_host"]
        self.assertEqual(clone["mode"], "clone")
        self.assertIn("/mnt/datapool/git", clone["ssh"])  # clone from the ARGONAS bare repo

    def test_default_branch_from_forge_facet(self):
        self.assertEqual(repos.get_repo("mnemos")["default_branch"], "feat/db2-native")

    def test_required_mnemos_queries(self):
        self.assertEqual(
            repos.required_mnemos_queries("graeae-hive-mind"),
            ["hive architecture", "hive job lifecycle"],
        )
        self.assertEqual(repos.required_mnemos_queries("mnemos"), [])  # required=False

    def test_to_dict_emits_required_mnemos_queries(self):
        # the bus endpoint AND the MCP bridge both read this key off the payload
        entry = repos.get_repo("graeae-hive-mind")
        self.assertEqual(entry["required_mnemos_queries"], ["hive architecture", "hive job lifecycle"])
        self.assertEqual(repos.get_repo("mnemos")["required_mnemos_queries"], [])

    def test_resolve_is_capped(self):
        # the cap is the property under test; don't couple it to catalog size
        self.assertGreaterEqual(len(repos.resolve("e", limit=100)), 1)  # sanity
        self.assertLessEqual(len(repos.resolve("e", limit=1)), 1)

    def test_aliases_unique_across_entries(self):
        # no two entries may share an alias (else get_repo/resolve is ambiguous)
        seen: dict[str, str] = {}
        for entry in repos.list_repos():
            for alias in entry["aliases"]:
                self.assertNotIn(alias, seen, f"alias {alias!r} on both {seen.get(alias)} and {entry['id']}")
                seen[alias] = entry["id"]

    def test_alias_does_not_collide_with_own_id(self):
        # every alias that exists must not equal another entry's id (ambiguity rule)
        ids = {e["id"] for e in repos.list_repos()}
        for entry in repos.list_repos():
            for alias in entry["aliases"]:
                self.assertNotIn(alias, ids - {entry["id"]}, f"alias {alias!r} collides with a repo id")

    def test_resolve_ranks_exact_then_prefix(self):
        ids = [e["id"] for e in repos.resolve("hive")]
        self.assertEqual(ids[0], "graeae-hive-mind")

    def test_filters(self):
        self.assertEqual([e["id"] for e in repos.list_repos(language="python")], ["graeae-hive-mind"])
        self.assertEqual([e["id"] for e in repos.list_repos(tag="hive")], ["graeae-hive-mind"])
        self.assertEqual(len(repos.list_repos(source="argonas")), 1)

    def test_catalog_meta(self):
        meta = repos.catalog_meta()
        self.assertEqual(meta["count"], 2)


class DiscoveryTests(unittest.TestCase):
    def test_slugify(self):
        self.assertEqual(slugify("ncz-os/graeae-hive-mind"), "graeae-hive-mind")
        self.assertEqual(slugify("git@github.com:ncz-os/zeroclaw.git"), "zeroclaw")
        self.assertEqual(slugify("InvestorClaw.git"), "investorclaw")
        self.assertEqual(slugify("debs.git"), "debs")

    def test_correlate_merges_by_slug_and_tracks_sources(self):
        gitlab = {"zeroclaw": {"gitlab": {"url": "gl"}}, "solo": {"gitlab": {"url": "x"}}}
        github = {"zeroclaw": {"github": {"url": "gh"}}}
        merged = correlate(gitlab, github)
        self.assertEqual(merged["zeroclaw"]["gitlab"], {"url": "gl"})
        self.assertEqual(merged["zeroclaw"]["github"], {"url": "gh"})
        self.assertEqual(merged["zeroclaw"]["sources"], ["gitlab", "github"])
        self.assertEqual(merged["solo"]["sources"], ["gitlab"])

    def test_merge_overlay_slugifies_keys_and_preserves_discovered(self):
        entries = {"investorclaw": {"id": "investorclaw", "sources": ["argonas"]}}
        overlay = {"InvestorClaw": {"display_name": "InvestorClaw", "notes": "read-only"}}
        out = merge_overlay(entries, overlay)
        self.assertEqual(out["investorclaw"]["display_name"], "InvestorClaw")
        self.assertEqual(out["investorclaw"]["notes"], "read-only")
        # discovered source preserved, overlay appended once
        self.assertIn("argonas", out["investorclaw"]["sources"])
        self.assertIn("overlay", out["investorclaw"]["sources"])

    def test_merge_overlay_adds_overlay_only_entries(self):
        out = merge_overlay({}, {"orphan-repo": {"status": "deferred"}})
        self.assertEqual(out["orphan-repo"]["id"], "orphan-repo")
        self.assertEqual(out["orphan-repo"]["sources"], ["overlay"])


class BuiltCatalogTests(unittest.TestCase):
    """Sanity-check the shipped catalog IF it has been built."""

    def test_shipped_catalog_shape(self):
        path = Path(__file__).resolve().parent / "data" / "repos.json"
        if not path.exists():
            self.skipTest("data/repos.json not built yet")
        data = json.loads(path.read_text())
        self.assertIn("repos", data)
        ids = {e["id"] for e in data["repos"]}
        self.assertIn("graeae-hive-mind", ids)
        for e in data["repos"]:
            self.assertIn("id", e)
            self.assertIn("sources", e)
            self.assertIsInstance(e["sources"], list)

    def test_shipped_catalog_has_no_alias_id_collisions(self):
        path = Path(__file__).resolve().parent / "data" / "repos.json"
        if not path.exists():
            self.skipTest("data/repos.json not built yet")
        data = json.loads(path.read_text())
        ids = {e["id"] for e in data["repos"]}
        seen_aliases: dict[str, str] = {}
        for e in data["repos"]:
            for alias in e.get("aliases", []):
                self.assertNotIn(alias, ids - {e["id"]}, f"alias {alias!r} collides with a repo id")
                self.assertNotIn(
                    alias, seen_aliases, f"alias {alias!r} shared by {seen_aliases.get(alias)} and {e['id']}"
                )
                seen_aliases[alias] = e["id"]


if __name__ == "__main__":
    unittest.main()
