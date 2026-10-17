"""Phase 6 — model/router registry drift + invariant tests.

The registry (hive/core/model-registry.toml) is the ONE source; these tests
prove the committed consumers (hive/core/models.py tables + the per-host zoder /
zeroclaw TOML fragments) match it byte-for-byte, and that the fleet rules —
Nemotron EIH-only, DeepSeek direct-only — hold as CODE, not comments.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
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
        self.assertTrue(mr._is_eih_endpoint(m.endpoint), m.endpoint)
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
                'routes = ["zoder.reviewer.backup"]\nnotes = "EIH-only backup '
                'reviewer (NVIDIA EIH, integrate.api.nvidia.com). Fleet rule: never a '
                'knemon/coder candidate."',
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
            bad = text.replace(
                'endpoint = "https://integrate.api.nvidia.com/v1"',
                'endpoint = "http://pythia:8000/v1"',
            )
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

    # ── host.<name>.<slot> placement (review §3.1) ──────────────────────────
    def test_committed_registry_loads_with_host_slot_validation(self):
        """Positive: the committed registry satisfies every host-slot rule."""
        reg = mr.load_registry(REGISTRY)  # raises RegistryError on any violation
        self.assertEqual(
            reg.hosts["eih"]["zoder_reviewer_backup"], mr.NEMOTRON_MODEL
        )
        for host, assigns in reg.hosts.items():
            for slot, key in assigns.items():
                entry = reg.by_key(key)
                self.assertIn(mr._slot_route(slot), entry.routes, f"{host}.{slot}")

    def test_rejects_nemotron_on_tydeus_coder_slot(self):
        """§3.1 (2): the review's exact mutation — Nemotron as TYDEUS coder."""
        with tempfile.TemporaryDirectory() as td:
            text = REGISTRY.read_text().replace(
                '[host.tydeus]\nzoder_coder = "local/qwen38"',
                f'[host.tydeus]\nzoder_coder = "{mr.NEMOTRON_MODEL}"',
            )
            self.assertNotEqual(text, REGISTRY.read_text())
            with self.assertRaises(mr.RegistryError) as ctx:
                mr.load_registry(_write_registry(Path(td), text))
            msg = str(ctx.exception)
            self.assertIn("tydeus", msg)
            self.assertIn("zoder_coder", msg)
            self.assertIn(mr.NEMOTRON_MODEL, msg)
            self.assertIn("Nemotron", msg)

    def test_rejects_nemotron_in_eih_coder_slot(self):
        """Nemotron may only be the backup reviewer, even on EIH."""
        with tempfile.TemporaryDirectory() as td:
            text = REGISTRY.read_text().replace(
                'zoder_reviewer_backup = "nvidia/nemotron-3-ultra-550b-a55b"',
                'zoder_coder = "nvidia/nemotron-3-ultra-550b-a55b"',
            )
            self.assertNotEqual(text, REGISTRY.read_text())
            with self.assertRaises(mr.RegistryError) as ctx:
                mr.load_registry(_write_registry(Path(td), text))
            self.assertIn("Nemotron", str(ctx.exception))

    def test_rejects_deepseek_through_third_party_slot(self):
        """§3.1 (3): a slot may not resolve a DeepSeek model off the direct provider."""
        extra = (
            '[[model]]\nkey = "together/deepseek-v4-flash"\nin = 0.14\nout = 0.28\n'
            'routes = ["zoder.coder"]\n'
        )
        with tempfile.TemporaryDirectory() as td:
            text = REGISTRY.read_text().replace('[host.tydeus]', extra + '[host.tydeus]')
            text = text.replace(
                'zoder_coder = "local/qwen38"', 'zoder_coder = "together/deepseek-v4-flash"'
            )
            self.assertNotEqual(text, REGISTRY.read_text())
            with self.assertRaises(mr.RegistryError) as ctx:
                mr.load_registry(_write_registry(Path(td), text))
            msg = str(ctx.exception)
            self.assertIn("DeepSeek", msg)
            self.assertIn("direct-only", msg)
            self.assertIn("together/deepseek-v4-flash", msg)

    def test_rejects_deepseek_slot_pointing_at_siliconflow(self):
        """A direct-provider key is not enough: the endpoint must be api.deepseek.com."""
        extra = (
            '[[model]]\nkey = "deepseek-direct/deepseek-v4-orion"\nin = 0.1\nout = 0.2\n'
            'endpoint = "https://api.siliconflow.cn/v1"\nroutes = ["zoder.coder"]\n'
        )
        with tempfile.TemporaryDirectory() as td:
            text = REGISTRY.read_text().replace('[host.tydeus]', extra + '[host.tydeus]')
            text = text.replace(
                'zoder_coder = "local/qwen38"',
                'zoder_coder = "deepseek-direct/deepseek-v4-orion"',
            )
            self.assertNotEqual(text, REGISTRY.read_text())
            with self.assertRaises(mr.RegistryError) as ctx:
                mr.load_registry(_write_registry(Path(td), text))
            msg = str(ctx.exception)
            self.assertIn("https://api.deepseek.com/v1", msg)
            self.assertIn("deepseek-direct/deepseek-v4-orion", msg)

    def test_rejects_retired_tydeus_8002_endpoint_from_a_slot(self):
        """§3.1 (4): the retired TYDEUS :8002 nemotron35 coder endpoint."""
        with tempfile.TemporaryDirectory() as td:
            text = REGISTRY.read_text().replace(
                'endpoint = "http://tydeus:8006/v1"',
                'endpoint = "http://tydeus:8002/v1/nemotron35"',
            )
            self.assertNotEqual(text, REGISTRY.read_text())
            with self.assertRaises(mr.RegistryError) as ctx:
                mr.load_registry(_write_registry(Path(td), text))
            msg = str(ctx.exception)
            self.assertIn(":8002", msg)
            self.assertIn("local/qwen38", msg)
            self.assertIn("tydeus", msg)

    def test_rejects_retired_4100_endpoint_from_a_slot(self):
        """§3.1 (4): the legacy EIH gateway :4100 may never be a host slot endpoint."""
        with tempfile.TemporaryDirectory() as td:
            text = REGISTRY.read_text().replace(
                'endpoint = "http://cerberus:8080/v1"', 'endpoint = "http://eih:4100/v1"'
            )
            self.assertNotEqual(text, REGISTRY.read_text())
            with self.assertRaises(mr.RegistryError) as ctx:
                mr.load_registry(_write_registry(Path(td), text))
            msg = str(ctx.exception)
            self.assertIn(":4100", msg)
            self.assertIn("local/gemma4-31b", msg)

    def test_rejects_slot_outside_the_models_allowed_routes(self):
        """§3.1 (1): the slot must be one of that model's routes/roles."""
        with tempfile.TemporaryDirectory() as td:
            text = REGISTRY.read_text().replace(
                'zoder_coder = "local/qwen38"', 'zoder_coder = "local/gemma4-31b"'
            )
            self.assertNotEqual(text, REGISTRY.read_text())
            with self.assertRaises(mr.RegistryError) as ctx:
                mr.load_registry(_write_registry(Path(td), text))
            msg = str(ctx.exception)
            self.assertIn("local/gemma4-31b", msg)
            self.assertIn("zoder_coder", msg)

    def test_rejects_slot_referencing_unknown_model(self):
        """§3.1 (1): the slot's model must exist in the registry."""
        with tempfile.TemporaryDirectory() as td:
            text = REGISTRY.read_text().replace(
                'zoder_coder = "local/qwen38"', 'zoder_coder = "local/does-not-exist"'
            )
            self.assertNotEqual(text, REGISTRY.read_text())
            with self.assertRaises(mr.RegistryError) as ctx:
                mr.load_registry(_write_registry(Path(td), text))
            msg = str(ctx.exception)
            self.assertIn("tydeus", msg)
            self.assertIn("zoder_coder", msg)
            self.assertIn("local/does-not-exist", msg)

    def test_slot_route_names_map_to_routes(self):
        self.assertEqual(mr._slot_route("zoder_coder"), "zoder.coder")
        self.assertEqual(mr._slot_route("zoder_reviewer"), "zoder.reviewer")
        self.assertEqual(mr._slot_route("zoder_reviewer_backup"), "zoder.reviewer.backup")
        self.assertEqual(mr._slot_route("zeroclaw_default"), "zeroclaw.default")

    def test_validate_invariants_has_no_unbound_names(self):
        """Regression: the host pass must not depend on a name that can vanish.

        A `where` that is only bound by the function preamble is fine at
        runtime, but it is fragile — one edit to the preamble turns every
        `[host.*]` entry into a NameError instead of a RegistryError. Assert the
        host pass binds its own message prefix and that validate_invariants has
        no name it references but never binds.
        """
        import ast
        import builtins

        tree = ast.parse(Path(mr.__file__).read_text())
        fn = next(
            n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "validate_invariants"
        )
        bound = {a.arg for a in fn.args.args}
        for n in ast.walk(fn):
            if isinstance(n, ast.Assign):
                bound |= {t.id for t in n.targets if isinstance(t, ast.Name)}
            elif isinstance(n, (ast.AnnAssign, ast.AugAssign)) and isinstance(n.target, ast.Name):
                bound.add(n.target.id)
            elif isinstance(n, (ast.For, ast.comprehension)):
                bound |= {t.id for t in ast.walk(n.target) if isinstance(t, ast.Name)}
            elif isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store):
                bound.add(n.id)
        module_names = set(dir(builtins))
        module_names |= {
            n.name for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef))
        }
        module_names |= {
            t.id
            for n in tree.body
            if isinstance(n, ast.Assign)
            for t in n.targets
            if isinstance(t, ast.Name)
        }
        loads = {n.id for n in ast.walk(fn) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}
        self.assertEqual(sorted(loads - bound - module_names), [])

        # The host pass passes a locally-bound prefix, not the preamble's name.
        host_pass = next(
            n for n in ast.walk(fn) if isinstance(n, ast.For) and "hosts" in ast.dump(n)
        )
        self.assertIn("hosts_where", {n.id for n in ast.walk(host_pass) if isinstance(n, ast.Name)})

    def test_load_registry_raises_registry_error_never_name_error(self):
        """Every violating host slot fails as RegistryError (not a crash)."""
        cases = {
            "nemotron": (
                'zoder_coder = "local/qwen38"',
                f'zoder_coder = "{mr.NEMOTRON_MODEL}"',
            ),
            "retired": ('endpoint = "http://tydeus:8006/v1"', 'endpoint = "http://tydeus:8002/v1"'),
        }
        for label, (old, new) in cases.items():
            with self.subTest(case=label):
                text = REGISTRY.read_text().replace(old, new)
                self.assertNotEqual(text, REGISTRY.read_text())
                with tempfile.TemporaryDirectory() as td:
                    try:
                        mr.load_registry(_write_registry(Path(td), text))
                    except mr.RegistryError:
                        pass
                    except NameError as exc:  # pragma: no cover - the regression
                        self.fail(f"{label}: NameError instead of RegistryError: {exc}")
                    else:  # pragma: no cover - the regression
                        self.fail(f"{label}: violating registry loaded")

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


class TestEihPlacementPolicy(unittest.TestCase):
    """Operator policy 2026-10-17: EIH serves Nemotron or open-weight only."""

    def _registry_with(self, model_block: str) -> str:
        return REGISTRY.read_text() + model_block

    def _load(self, block: str):
        with tempfile.TemporaryDirectory() as td:
            return mr.load_registry(_write_registry(Path(td), self._registry_with(block)))

    def test_shipped_registry_has_no_ngc_proxy_rates(self):
        self.assertNotIn(("ngc-proxy", "gpt-5.5"), LLM_RATES)
        self.assertNotIn(("ngc-proxy", "*"), LLM_RATES)
        self.assertIsNone(self.reg_by_key("ngc-proxy/gpt-5.5"))

    def reg_by_key(self, key: str):
        return mr.load_registry(REGISTRY).by_key(key)

    def test_shipped_nemotron_uses_the_eih_api_host(self):
        entry = self.reg_by_key(mr.NEMOTRON_MODEL)
        self.assertEqual(entry.endpoint, "https://integrate.api.nvidia.com/v1")

    def test_rejects_gpt_5_5_on_eih(self):
        block = (
            '[[model]]\nkey = "openai/gpt-5.5-eih"\nin = 0.0\nout = 0.0\n'
            'endpoint = "https://integrate.api.nvidia.com/v1"\n'
        )
        with self.assertRaises(mr.RegistryError) as ctx:
            self._load(block)
        self.assertIn("EIH placement policy", str(ctx.exception))

    def test_rejects_anthropic_on_eih(self):
        block = (
            '[[model]]\nkey = "anthropic/claude-opus-4-7-eih"\nin = 0.0\nout = 0.0\n'
            'endpoint = "https://integrate.api.nvidia.com/v1"\n'
        )
        with self.assertRaises(mr.RegistryError) as ctx:
            self._load(block)
        self.assertIn("anthropic/claude-opus-4-7-eih", str(ctx.exception))

    def test_rejects_deepseek_on_eih(self):
        block = (
            '[[model]]\nkey = "deepseek-direct/deepseek-v4-flash-eih"\nin = 0.0\nout = 0.0\n'
            'endpoint = "https://integrate.api.nvidia.com/v1"\n'
        )
        with self.assertRaises(mr.RegistryError) as ctx:
            self._load(block)
        self.assertIn("direct-only", str(ctx.exception))

    def test_accepts_nemotron_and_open_weight_on_eih(self):
        block = (
            '[[model]]\nkey = "nvidia/nemotron-3-ultra-550b-a55b-alt"\nin = 0.0\nout = 0.0\n'
            'endpoint = "https://integrate.api.nvidia.com/v1"\n'
        )
        # A second Nemotron is rejected by the one-and-only Nemotron rule, so
        # assert the EIH pass itself accepts Nemotron-shaped placement.
        with self.assertRaises(mr.RegistryError) as ctx:
            self._load(block)
        self.assertNotIn("EIH placement policy", str(ctx.exception))

    def test_accepts_zai_glm_on_eih(self):
        block = (
            '[[model]]\nkey = "z-ai/glm-5.3"\nin = 0.0\nout = 0.0\n'
            'endpoint = "https://integrate.api.nvidia.com/v1"\n'
        )
        reg = self._load(block)
        self.assertIsNotNone(reg.by_key("z-ai/glm-5.3"))

    def test_open_weight_allowlist_is_a_regex_tuple(self):
        self.assertIsInstance(mr.OPEN_WEIGHT_EIH_ALLOWLIST, tuple)
        self.assertTrue(all(hasattr(p, "search") for p in mr.OPEN_WEIGHT_EIH_ALLOWLIST))
        for key in (
            "z-ai/glm-5.3",
            "moonshotai/kimi-k2.6",
            "qwen/qwen3-32b",
            "meta/llama-3.3-70b",
            "mistralai/mistral-large",
            "openai/gpt-oss-120b",
            "google/gemma-4-31b",
        ):
            entry = mr.ModelEntry(
                key=key, provider=key.split("/", 1)[0], model=key.split("/", 1)[1],
                price_in=0.0, price_out=0.0,
            )
            self.assertTrue(mr._is_open_weight_eih(entry), key)
        for key in ("openai/gpt-5.5", "anthropic/claude-opus-4-7", "xai/grok-4.3"):
            entry = mr.ModelEntry(
                key=key, provider=key.split("/", 1)[0], model=key.split("/", 1)[1],
                price_in=0.0, price_out=0.0,
            )
            self.assertFalse(mr._is_open_weight_eih(entry), key)
