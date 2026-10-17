"""Phase 6 — model/router registry drift + invariant tests.

The registry (hive/core/model-registry.toml) is the ONE source; these tests
prove the committed consumers (hive/core/models.py tables + the per-host zoder /
zeroclaw TOML fragments) match it byte-for-byte, and that the fleet rules —
Nemotron EIH-only, DeepSeek direct-only — hold as CODE, not comments.
"""

from __future__ import annotations

import subprocess
import sys
import tomllib
import unittest
from pathlib import Path

from hive.core import model_registry as mr
from hive.core import models
from hive.core.models import (
    KNEMON_PROVIDERS,
    LLM_RATES,
    PROVIDER_COST_TIER,
    knemon_candidates,
    rate_for,
)

# The bus prefers zoder's live pricing catalog over the built-in table. Pin it to
# a non-existent file so these tests assert the REGISTRY's numbers, not whatever
# catalog happens to be installed on the machine running them.
models.ZODER_PRICING_JSON = Path("/nonexistent/zoder-pricing.json")
models._zoder_catalog.cache_clear()

ROOT = Path(__file__).resolve().parent
REGISTRY = mr.DEFAULT_REGISTRY


def _write_registry(tmp: Path, text: str) -> Path:
    p = tmp / "model-registry.toml"
    p.write_text(text)
    return p


class TestDrift(unittest.TestCase):
    """--check must pass against the committed tables (no hand-edited drift)."""

    def setUp(self):
        self.reg = mr.load_registry(REGISTRY)

    def test_cli_check_passes_against_committed_tables(self):
        rc = mr.main(["--check", "--registry", str(REGISTRY)])
        self.assertEqual(rc, 0, "--check failed: committed consumers drifted")

    def test_cli_check_subprocess_exit_zero(self):
        out = subprocess.run(
            [sys.executable, "-m", "hive.core.model_registry", "--check"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
        )
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)

    def test_bus_tables_byte_equal_committed_block(self):
        block = mr.emit_bus_tables(self.reg)
        text = mr.MODELS_PY.read_text()
        self.assertIn(mr.BUS_BEGIN, text)
        self.assertIn(mr.BUS_END, text)
        start = text.index(mr.BUS_BEGIN)
        end = text.index(mr.BUS_END) + len(mr.BUS_END)
        self.assertEqual(text[start:end], block.rstrip("\n"))

    def test_committed_tables_match_registry(self):
        self.assertEqual(PROVIDER_COST_TIER, self.reg.providers)
        want_rates = {
            (m.provider, m.model): (m.price_in, m.price_out)
            for m in self.reg.models
            if m.emit_rate
        }
        self.assertEqual(dict(LLM_RATES), want_rates)
        want_knemon = [
            {
                "provider": m.provider,
                "model": m.model,
                "alias": m.knemon_alias,
                "tier": m.knemon_tier,
            }
            for m in sorted(
                (m for m in self.reg.models if "knemon" in m.routes),
                key=lambda m: m.knemon_order or 0,
            )
        ]
        self.assertEqual(KNEMON_PROVIDERS, want_knemon)

    def test_generated_fragments_committed_and_parseable(self):
        for host in self.reg.hosts:
            for name, fn in (("zoder.toml", mr.emit_zoder_toml), ("zeroclaw.toml", mr.emit_zeroclaw_toml)):
                path = mr.GENERATED_DIR / host / name
                self.assertTrue(path.exists(), f"missing generated fragment {path}")
                self.assertEqual(path.read_text(), fn(self.reg, host))
                tomllib.loads(path.read_text())  # must be valid TOML

    def test_behavior_preserving_known_values(self):
        """Spot-check today's values (Phase 6 makes NO routing change)."""
        self.assertEqual(PROVIDER_COST_TIER["groq"], "B")
        self.assertEqual(PROVIDER_COST_TIER["anthropic"], "C")
        self.assertEqual(PROVIDER_COST_TIER["unknown"], "A")
        self.assertEqual(LLM_RATES[("minimax", "minimax-m3")], (0.40, 1.20))
        self.assertEqual(LLM_RATES[("together", "deepseek-v4-pro")], (2.10, 4.40))
        self.assertEqual(rate_for("minimax", "minimax-m3"), (0.40, 1.20))
        names = [k["alias"] for k in KNEMON_PROVIDERS]
        self.assertEqual(
            names,
            ["hive_groq_1", "hive_gemini_1", "hive_minimax_1", "hive_xai_1", "hive_nova_1", "hive_nova_2"],
        )
        # cheapest-acceptable-first ordering is preserved under a B ceiling
        out = [(c["provider"], c["model"]) for c in knemon_candidates("B")]
        self.assertEqual(
            out,
            [
                ("bedrock", "amazon.nova-micro-v1:0"),   # 0.14 /MTok out
                ("bedrock", "amazon.nova-lite-v1:0"),    # 0.24
                ("groq", "gpt-oss-20b"),                 # 0.30
                ("gemini", "gemini-2.5-flash-lite"),     # 0.40
                ("xai", "grok-4.1-fast"),                # 0.50
                ("minimax", "minimax-m3"),               # 1.20
            ],
        )
        # a C ceiling adds nothing today (no tier-C knemon candidates)
        self.assertEqual([(c["provider"], c["model"]) for c in knemon_candidates("C")], out)
        # an A ceiling admits nothing (every knemon candidate is tier B)
        self.assertEqual(knemon_candidates("A"), [])

    def test_deepseek_and_siliconflow_not_routed(self):
        """Operator 2026-09-07: deepseek/siliconflow never enter the pool."""
        providers = {k["provider"] for k in KNEMON_PROVIDERS}
        self.assertNotIn("deepseek", providers)
        self.assertNotIn("deepseek-direct", providers)
        self.assertNotIn("siliconflow", providers)

    def test_write_is_idempotent(self):
        before = mr.MODELS_PY.read_text()
        rc = mr.main(["--write", "--registry", str(REGISTRY)])
        self.assertEqual(rc, 0)
        self.assertEqual(mr.MODELS_PY.read_text(), before)


class TestInvariants(unittest.TestCase):
    """Fleet rules enforced in code — a bad registry never loads."""

    def setUp(self):
        self.reg = mr.load_registry(REGISTRY)

    # ── Nemotron = EIH-only backup reviewer ─────────────────────────────────
    def test_nemotron_is_eih_only_backup_reviewer(self):
        nemo = [m for m in self.reg.models if "nemotron" in m.model.lower()]
        self.assertEqual([m.key for m in nemo], [mr.NEMOTRON_MODEL])
        m = nemo[0]
        self.assertEqual(m.routes, ("zoder.reviewer.backup",))
        self.assertTrue(m.endpoint.startswith("http://eih:"))
        self.assertEqual(self.reg.hosts["eih"]["zoder_reviewer_backup"], mr.NEMOTRON_MODEL)

    def test_nemotron_never_in_bus_router_or_coder(self):
        self.assertNotIn(mr.NEMOTRON_MODEL, {k["model"] for k in KNEMON_PROVIDERS})
        for m in self.reg.models:
            if m.key == mr.NEMOTRON_MODEL:
                continue
            self.assertNotIn("nemotron", m.model.lower())
        # ...and no host may install it as a coder
        for host, assigns in self.reg.hosts.items():
            for slot, key in assigns.items():
                if "nemotron" in key:
                    self.assertNotEqual(slot, "zoder_coder", host)

    def test_registry_rejects_nemotron_as_knemon_candidate(self):
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            text = REGISTRY.read_text()
            bad = text.replace(
                'routes = ["zoder.reviewer.backup"]\nnotes = "EIH-only backup reviewer. '
                'Fleet rule: never a knemon/coder candidate."',
                'routes = ["zoder.reviewer.backup", "knemon"]\n'
                'knemon_alias = "hive_nemotron_1"\n'
                'knemon_tier = "A"\n'
                'knemon_order = 99',
            )
            self.assertNotEqual(bad, text)
            with self.assertRaises(mr.RegistryError) as ctx:
                mr.load_registry(_write_registry(Path(td), bad))
            self.assertIn("Nemotron", str(ctx.exception))

    def test_registry_rejects_non_eih_nemotron(self):
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            text = REGISTRY.read_text()
            bad = text.replace('endpoint = "http://eih:8000/v1"', 'endpoint = "http://pythia:8000/v1"')
            self.assertNotEqual(bad, text)
            with self.assertRaises(mr.RegistryError) as ctx:
                mr.load_registry(_write_registry(Path(td), bad))
            self.assertIn("EIH", str(ctx.exception))

    def test_registry_rejects_stray_second_nemotron(self):
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            text = REGISTRY.read_text() + (
                '[[model]]\nkey = "local/nemotron-mini"\nin = 0.0\nout = 0.0\n'
                'endpoint = "http://tydeus:8000/v1"\n'
            )
            with self.assertRaises(mr.RegistryError) as ctx:
                mr.load_registry(_write_registry(Path(td), text))
            self.assertIn("Nemotron", str(ctx.exception))

    # ── DeepSeek = direct only ──────────────────────────────────────────────
    def test_deepseek_is_direct_only(self):
        routed_third_party = []
        for m in self.reg.models:
            if "deepseek" not in m.model.lower():
                continue
            if m.provider in mr.DEEPSEEK_DIRECT_PROVIDERS:
                continue
            # allowed ONLY as a pricing record (together), never as a route
            self.assertEqual(m.routes, (), f"{m.key} may not carry routes")
            routed_third_party.append(m.key)
        self.assertEqual(routed_third_party, ["together/deepseek-v4-pro"])

    def test_registry_rejects_deepseek_via_third_party(self):
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            text = REGISTRY.read_text().replace(
                'notes = "PRICING RECORD ONLY — deepseek is direct-only, so this entry carries no routes"',
                'routes = ["zoder.coder"]',
            )
            with self.assertRaises(mr.RegistryError) as ctx:
                mr.load_registry(_write_registry(Path(td), text))
            self.assertIn("direct-only", str(ctx.exception))

    # ── structural validation ───────────────────────────────────────────────
    def test_rejects_unknown_route(self):
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            text = REGISTRY.read_text().replace(
                'routes = ["knemon"]', 'routes = ["totally-not-a-consumer"]', 1
            )
            with self.assertRaises(mr.RegistryError) as ctx:
                mr.load_registry(_write_registry(Path(td), text))
            self.assertIn("unknown route", str(ctx.exception))

    def test_rejects_unsupported_schema_version(self):
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            text = REGISTRY.read_text().replace("schema_version = 1", "schema_version = 99", 1)
            with self.assertRaises(mr.RegistryError):
                mr.load_registry(_write_registry(Path(td), text))

    def test_unknown_host_raises(self):
        with self.assertRaises(mr.RegistryError):
            mr.emit_zoder_toml(self.reg, "no-such-host")

    def test_every_host_slot_references_a_declared_model(self):
        for host, assigns in self.reg.hosts.items():
            for slot, key in assigns.items():
                self.assertIsNotNone(self.reg.by_key(key), f"{host}.{slot} -> {key}")


if __name__ == "__main__":
    unittest.main()