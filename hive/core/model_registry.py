"""Phase 6 — central model + router registry generator.

One declarative source (``hive/core/model-registry.toml``) GENERATES the bus
routing tables in ``hive/core/models.py`` and the per-host zoder/zeroclaw TOML
fragments. The generated consumers are committed; ``--check`` fails when they
drift from the source. There is NO runtime dependency on the registry — the bus
still imports plain dicts/lists from ``models.py``.

This first cut is BEHAVIOR-PRESERVING: the registry re-states today's values
exactly, so regenerating is a no-op and routing is unchanged.

Design + guardrails: docs/decision-phase6-model-router-registry-20261016.md

CLI::

    python -m hive.core.model_registry --check     # exit 1 on drift
    python -m hive.core.model_registry --write     # rewrite consumers

Invariants enforced (as code, not comments):

* Nemotron — ``nvidia/nemotron-3-ultra-550b-a55b`` is the only Nemotron served
  through NVIDIA EIH, and is a backup REVIEWER. It may never be a knemon (bus
  router) candidate or a coder route.
* Nemotron placement is NOT EIH-exclusive (operator correction 2026-10-17):
  a Nemotron model may sit in ANY ``[host.<name>]`` slot whose routes it
  allows, on ANY host — TYDEUS serves a Nemotron backup model under test
  (served name ``nemotron35``, vLLM on TYDEUS ``:8010``).
* DeepSeek — deepseek models are DIRECT-only: they may never carry a route
  through any other provider (``together`` may carry a pricing record for
  deepseek, but no routes).
* Host placement — every ``[host.<name>]`` slot is validated against the same
  rules (review §3.1): the slot's model must exist and allow that slot as a
  route, a DeepSeek model must resolve to the direct provider, and a retired
  endpoint (TYDEUS ``:8002`` or ``:4100``) may never be referenced.
"""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
DEFAULT_REGISTRY = HERE / "model-registry.toml"
MODELS_PY = HERE / "models.py"
GENERATED_DIR = HERE / "generated"

# Markers delimiting the generated block inside hive/core/models.py. Keep in
# sync with the block itself (models.py owns the constants).
BUS_BEGIN = "# --- BEGIN GENERATED (hive.core.model_registry) ---"
BUS_END = "# --- END GENERATED (hive.core.model_registry) ---"

# The one legal Nemotron in the registry (operator correction 2026-10-17: it is
# the only Nemotron this registry knows, a backup REVIEWER, and its placement is
# NOT EIH-only — any host slot whose route it allows may map it; TYDEUS serves a
# Nemotron backup under test as served name 'nemotron35' on vLLM :8010).
NEMOTRON_MODEL = "nvidia/nemotron-3-ultra-550b-a55b"
EIH_HOST = "eih"

# ── NVIDIA EIH placement policy (operator 2026-10-17) ───────────────────────
# Anything routed through NVIDIA EIH may serve ONLY Nemotron or an open-weight
# model. Closed frontier models (openai/gpt-5.x, codex, anthropic, azure/*,
# gemini, grok) are forbidden on EIH. DeepSeek is direct-only and may NEVER be
# an EIH model.
EIH_ENDPOINT_HOST = "integrate.api.nvidia.com"
EIH_PROVIDERS = frozenset({"eih", "nvidia", "ngc", "nvidia-ngc"})
# Model-key regexes (matched against the full "<provider>/<model>" key, case
# insensitively) for open-weight families that are allowed to sit on EIH.
OPEN_WEIGHT_EIH_ALLOWLIST: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE)
    for p in (
        r"^z-ai/glm-",
        r"^moonshotai/kimi-",
        r"^qwen/",
        r"^meta/llama",
        r"^mistralai/",
        r"^openai/gpt-oss-",
        r"^google/gemma",
    )
)
NEMOTRON_EIH_ENDPOINTS = (EIH_ENDPOINT_HOST, f"http://{EIH_HOST}:")
NEMOTRON_ALLOWED_ROUTES = frozenset({"zoder.reviewer.backup"})
DEEPSEEK_DIRECT_PROVIDERS = frozenset({"deepseek", "deepseek-direct"})
KNOWN_ROUTES = frozenset(
    {"knemon", "zoder.coder", "zoder.reviewer", "zoder.reviewer.backup", "zeroclaw.default"}
)
DEEPSEEK_DIRECT_ENDPOINT = "https://api.deepseek.com/v1"
# Retired endpoints: TYDEUS :8002 (the old coder route) and :4100 (the legacy
# EIH gateway). Any host slot still pointing at one is an error. (The TYDEUS
# served name "nemotron35" on vLLM :8010 is NOT retired — see the Nemotron
# note above.) No retired endpoint tokens remain: the only tokens ever
# registered were retired with the Nemotron host/slot rule (operator
# correction 2026-10-17).
RETIRED_ENDPOINT_PORTS = (
    ("8002", "TYDEUS :8002 (retired coder route)"),
    ("4100", "legacy EIH gateway :4100 (retired)"),
)
RETIRED_ENDPOINT_TOKENS: tuple[str, ...] = ()


class RegistryError(ValueError):
    """The registry is malformed or violates a fleet invariant."""


@dataclass(frozen=True)
class ModelEntry:
    key: str
    provider: str
    model: str
    price_in: float
    price_out: float
    routes: tuple[str, ...] = ()
    endpoint: str = ""
    auth: str = ""
    notes: str = ""
    knemon_alias: str = ""
    knemon_tier: str = ""
    knemon_order: int | None = None
    emit_rate: bool = True


@dataclass(frozen=True)
class Registry:
    path: Path
    schema_version: int
    providers: dict[str, str] = field(default_factory=dict)
    models: list[ModelEntry] = field(default_factory=list)
    hosts: dict[str, dict[str, str]] = field(default_factory=dict)

    def by_key(self, key: str) -> ModelEntry | None:
        for m in self.models:
            if m.key == key:
                return m
        return None


def load_registry(path: str | Path = DEFAULT_REGISTRY) -> Registry:
    """Load + validate the declarative registry. Raises RegistryError on drift."""
    p = Path(path)
    if not p.is_absolute():
        p = (Path.cwd() / p).resolve()
    try:
        raw = tomllib.loads(p.read_text())
    except FileNotFoundError as exc:
        raise RegistryError(f"registry not found: {p}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise RegistryError(f"{p}: invalid TOML: {exc}") from exc

    version = int(raw.get("schema_version", 0))
    if version != 1:
        raise RegistryError(f"{p}: unsupported schema_version {version!r} (expected 1)")

    providers: dict[str, str] = {}
    for row in raw.get("provider") or []:
        name = str(row.get("name") or "").strip().lower()
        tier = str(row.get("cost_tier") or "").strip().upper()
        if not name or tier not in ("A", "B", "C"):
            raise RegistryError(f"{p}: bad provider entry {row!r}")
        if name in providers:
            raise RegistryError(f"{p}: duplicate provider {name!r}")
        providers[name] = tier

    models: list[ModelEntry] = []
    seen: set[str] = set()
    for row in raw.get("model") or []:
        key = str(row.get("key") or "").strip()
        if "/" not in key:
            raise RegistryError(f"{p}: model key must be '<provider>/<model>': {key!r}")
        if key in seen:
            raise RegistryError(f"{p}: duplicate model key {key!r}")
        seen.add(key)
        provider, model = key.split("/", 1)
        provider = provider.lower()
        routes = tuple(str(r).strip() for r in (row.get("routes") or ()))
        for r in routes:
            if r not in KNOWN_ROUTES:
                raise RegistryError(f"{p}: {key}: unknown route {r!r}")
        models.append(
            ModelEntry(
                key=key,
                provider=provider,
                model=model,
                price_in=float(row.get("in", 0.0)),
                price_out=float(row.get("out", 0.0)),
                routes=routes,
                endpoint=str(row.get("endpoint") or "").strip(),
                auth=str(row.get("auth") or "").strip(),
                notes=str(row.get("notes") or "").strip(),
                knemon_alias=str(row.get("knemon_alias") or "").strip(),
                knemon_tier=str(row.get("knemon_tier") or "").strip().upper(),
                knemon_order=(
                    int(row["knemon_order"]) if row.get("knemon_order") is not None else None
                ),
                emit_rate=bool(row.get("emit_rate", True)),
            )
        )

    hosts: dict[str, dict[str, str]] = {}
    for host, assigns in (raw.get("host") or {}).items():
        hosts[str(host)] = {str(k): str(v) for k, v in assigns.items()}

    reg = Registry(path=p, schema_version=version, providers=providers, models=models, hosts=hosts)
    validate_invariants(reg)
    return reg


def validate_invariants(reg: Registry) -> None:
    """Fleet rules encoded as code — a registry that breaks them never loads."""
    where = str(reg.path)

    # ── Nemotron: exactly one, backup reviewer only ──────────────────────────
    nemo = [m for m in reg.models if "nemotron" in m.model.lower() or "nemotron" in m.provider.lower()]
    if [m.key for m in nemo] != [NEMOTRON_MODEL]:
        raise RegistryError(
            f"{where}: Nemotron rule — the only Nemotron must be {NEMOTRON_MODEL}, got "
            f"{[m.key for m in nemo]}"
        )
    nm = nemo[0]
    if set(nm.routes) - NEMOTRON_ALLOWED_ROUTES:
        raise RegistryError(
            f"{where}: Nemotron rule — {NEMOTRON_MODEL} is a backup reviewer only; "
            f"forbidden routes "
            f"{sorted(set(nm.routes) - NEMOTRON_ALLOWED_ROUTES)}"
        )
    if not _is_eih_endpoint(nm.endpoint):
        raise RegistryError(
            f"{where}: {NEMOTRON_MODEL} endpoint must be EIH "
            f"({EIH_ENDPOINT_HOST} or {EIH_HOST}:), got {nm.endpoint!r}"
        )

    # Nemotron must never be a bus-router candidate or a coder route.
    for m in reg.models:
        if "nemotron" in m.model.lower() and m.key != NEMOTRON_MODEL:
            raise RegistryError(f"{where}: stray Nemotron {m.key!r}")

    # ── EIH placement: Nemotron or open-weight only (operator 2026-10-17) ───
    for m in reg.models:
        if not _is_eih_model(m):
            continue
        if "deepseek" in m.model.lower():
            raise RegistryError(
                f"{where}: DeepSeek is direct-only — {m.key!r} may never run on EIH "
                f"({EIH_ENDPOINT_HOST})"
            )
        if _is_nemotron(m) or _is_open_weight_eih(m):
            continue
        raise RegistryError(
            f"{where}: EIH placement policy — {m.key!r} on EIH ({m.endpoint or m.provider}) "
            f"must be a Nemotron model or match OPEN_WEIGHT_EIH_ALLOWLIST; closed "
            f"frontier models are forbidden on EIH"
        )

    # ── DeepSeek: direct-only ────────────────────────────────────────────────
    for m in reg.models:
        is_deepseek_model = "deepseek" in m.model.lower()
        if is_deepseek_model and m.provider not in DEEPSEEK_DIRECT_PROVIDERS:
            if m.routes:
                raise RegistryError(
                    f"{where}: DeepSeek is direct-only — {m.key!r} may not carry routes "
                    f"{sorted(m.routes)}"
                )

    # ── KNEMON entries are fully specified + uniquely ordered ─────────────────
    orders: set[int] = set()
    aliases: set[str] = set()
    for m in reg.models:
        if "knemon" not in m.routes:
            continue
        if not (m.knemon_alias and m.knemon_tier):
            raise RegistryError(f"{where}: knemon route {m.key!r} needs knemon_alias + knemon_tier")
        if m.knemon_order is None:
            raise RegistryError(f"{where}: knemon route {m.key!r} needs knemon_order")
        if m.knemon_order in orders:
            raise RegistryError(f"{where}: duplicate knemon_order {m.knemon_order}")
        if m.knemon_alias in aliases:
            raise RegistryError(f"{where}: duplicate knemon_alias {m.knemon_alias!r}")
        orders.add(m.knemon_order)
        aliases.add(m.knemon_alias)

    # ── Host placement: every [host.<name>] slot obeys the same rules ────────
    # Bind the message prefix locally rather than leaning on the `where` defined
    # at the top of this function: the host pass must build its own
    # "<path>: host .. slot .. -> model .." context even if the preamble moves.
    hosts_where = str(reg.path)
    for host, assigns in reg.hosts.items():
        for slot, key in assigns.items():
            _validate_host_slot(reg, hosts_where, host, slot, key)


def _is_eih_endpoint(endpoint: str) -> bool:
    """True when an endpoint URL is served by NVIDIA EIH."""
    ep = (endpoint or "").strip().lower()
    return any(prefix in ep for prefix in NEMOTRON_EIH_ENDPOINTS)


def _is_eih_model(m: ModelEntry) -> bool:
    """True when a model is routed through NVIDIA EIH (API host or EIH provider)."""
    return _is_eih_endpoint(m.endpoint) or m.provider in EIH_PROVIDERS


def _is_open_weight_eih(m: ModelEntry) -> bool:
    """True when a model key matches OPEN_WEIGHT_EIH_ALLOWLIST."""
    return any(p.search(m.key) for p in OPEN_WEIGHT_EIH_ALLOWLIST)


def _is_nemotron(m: ModelEntry) -> bool:
    return "nemotron" in m.model.lower() or "nemotron" in m.provider.lower()


def _slot_route(slot: str) -> str:
    """Map a ``[host.<name>]`` slot name to the route it selects.

    zoder slots are written ``zoder_<role>`` in TOML (``zoder_reviewer_backup``)
    and named ``zoder.<role>`` as routes (``zoder.reviewer.backup``).
    """
    group, _, role = slot.partition("_")
    if not group or not role:
        return slot
    return f"{group}.{role.replace('_', '.')}"


def _retired_endpoint(endpoint: str) -> str | None:
    """Return the retired-endpoint label if ``endpoint`` points at one."""
    ep = endpoint.strip().lower()
    if not ep:
        return None
    for token in RETIRED_ENDPOINT_TOKENS:
        if token in ep:
            return f"retired endpoint token {token!r}"
    for port, label in RETIRED_ENDPOINT_PORTS:
        if re.search(rf":{port}(?!\d)", ep):
            return label
    return None


def _validate_host_slot(reg: Registry, where: str, host: str, slot: str, key: str) -> None:
    """Reject a host slot that is unknown or points at a banned route.

    Covers review §3.1: a registry mapping ``[host.tydeus] zoder_reviewer =
    'local/gemma4-31b'`` must fail to load, not emit a TOML. Nemotron is NOT
    host-restricted — it may sit in any slot whose route it allows, on any host.
    """
    site = f"{where}: host {host!r} slot {slot!r} -> model {key!r}"

    entry = reg.by_key(key)
    if entry is None:
        raise RegistryError(f"{site}: unknown model (not declared in the registry)")

    # (2) a DeepSeek model in a slot must resolve to the direct provider.
    if "deepseek" in entry.model.lower():
        endpoint = entry.endpoint.rstrip("/")
        if entry.provider not in DEEPSEEK_DIRECT_PROVIDERS or endpoint != DEEPSEEK_DIRECT_ENDPOINT:
            raise RegistryError(
                f"{site}: DeepSeek is direct-only — slot model must resolve to "
                f"provider {sorted(DEEPSEEK_DIRECT_PROVIDERS)} with endpoint "
                f"{DEEPSEEK_DIRECT_ENDPOINT}, got provider {entry.provider!r} "
                f"endpoint {entry.endpoint!r}"
            )

    # (3) a retired endpoint may never be referenced from a host slot.
    retired = _retired_endpoint(entry.endpoint)
    if retired:
        raise RegistryError(f"{site}: references the {retired} ({entry.endpoint!r})")

    # The slot must name one of the model's allowed routes/roles. Checked last
    # so the fleet rules above own the message when they also apply (a DeepSeek
    # or retired-endpoint slot is that violation first).
    route = _slot_route(slot)
    if route not in entry.routes:
        raise RegistryError(
            f"{site}: slot {slot!r} (route {route!r}) is not an allowed route of "
            f"{key!r}; allowed routes {sorted(entry.routes)}"
        )


# ── emit: bus tables ────────────────────────────────────────────────────────
def _fmt_rate(v: float) -> str:
    text = repr(float(v))
    return text


def emit_bus_tables(registry: Registry) -> str:
    """Render the generated block for hive/core/models.py (between the markers)."""
    lines: list[str] = [BUS_BEGIN, "# Generated by hive.core.model_registry from"]
    lines.append(f"# {registry.path.name}. DO NOT EDIT BY HAND — run --write.")
    lines.append("")
    lines.append("# ── cost tiers (A = free/local, B = mid, C = premium) ─────────────")
    lines.append("PROVIDER_COST_TIER: dict[str, str] = {")
    for name, tier in registry.providers.items():
        lines.append(f'    "{name}": "{tier}",')
    lines.append("}")
    lines.append("COST_TIERS = [\"A\", \"B\", \"C\"]")
    lines.append("")
    lines.append("# ── per-MTok rates (USD): (input, output) ───────────────────────────")
    lines.append("LLM_RATES: dict[tuple[str, str], tuple[float, float]] = {")
    for m in registry.models:
        if not m.emit_rate:
            continue
        suffix = f"  # {m.notes}" if m.notes else ""
        lines.append(f'    ("{m.provider}", "{m.model}"): ({_fmt_rate(m.price_in)}, {_fmt_rate(m.price_out)}),{suffix}')
    lines.append("}")
    lines.append("")
    lines.append("# ── routing corpus (cheapest-acceptable ordering happens at call time) ─")
    lines.append("KNEMON_PROVIDERS: list[dict[str, Any]] = [")
    knemon = sorted(
        (m for m in registry.models if "knemon" in m.routes),
        key=lambda m: m.knemon_order or 0,
    )
    for m in knemon:
        if m.notes:
            lines.append(f"    # {m.notes}")
        lines.append("    {")
        lines.append(f'        "provider": "{m.provider}",')
        lines.append(f'        "model": "{m.model}",')
        lines.append(f'        "alias": "{m.knemon_alias}",')
        lines.append(f'        "tier": "{m.knemon_tier}",')
        lines.append("    },")
    lines.append("]")
    lines.append(BUS_END)
    return "\n".join(lines) + "\n"


def _replace_bus_block(models_py_text: str, block: str) -> str:
    start = models_py_text.index(BUS_BEGIN)
    end = models_py_text.index(BUS_END) + len(BUS_END)
    # keep the newline the original block ended with
    tail = "\n" if models_py_text[end : end + 1] == "\n" else ""
    return models_py_text[:start] + block.rstrip("\n") + tail + models_py_text[end + len(tail) :]


# ── emit: per-host TOML fragments ───────────────────────────────────────────
def _host_models(reg: Registry, host: str) -> list[tuple[str, ModelEntry]]:
    assigns = reg.hosts.get(host)
    if assigns is None:
        raise RegistryError(f"{reg.path}: unknown host {host!r} (known: {sorted(reg.hosts)})")
    out: list[tuple[str, ModelEntry]] = []
    for slot, key in assigns.items():
        entry = reg.by_key(key)
        if entry is None:
            raise RegistryError(f"{reg.path}: host {host} slot {slot!r} references unknown model {key!r}")
        out.append((slot, entry))
    return out


def _header(reg: Registry, kind: str, host: str) -> list[str]:
    return [
        f"# GENERATED by hive.core.model_registry ({kind}) from {reg.path.name}",
        f"# host: {host}. DO NOT EDIT BY HAND.",
        "",
    ]


def emit_zoder_toml(registry: Registry, host: str) -> str:
    """zoder TOML fragment for `host` — the engine slots plus router providers."""
    rows = _host_models(registry, host)
    lines = _header(registry, "zoder", host)
    lines.append("zoder_version = 1")
    for slot, m in rows:
        lines.append("")
        lines.append(f"[agents.{slot}]")
        lines.append(f'model = "{m.model}"')
        lines.append(f'provider = "{m.provider}"')
        lines.append(f'endpoint = "{m.endpoint}"')
        if m.auth:
            lines.append(f'auth = "{m.auth}"')
        lines.append(f'cost_tier = "{registry.providers.get(m.provider, "C")}"')
        if m.notes:
            lines.append(f'notes = "{m.notes}"')
    knemon = sorted(
        (m for m in registry.models if "knemon" in m.routes),
        key=lambda m: m.knemon_order or 0,
    )
    lines.append("")
    lines.append("# bus-router providers (KNEMON_PROVIDERS order)")
    for i, m in enumerate(knemon):
        lines.append("")
        lines.append(f"[router.providers.{i}]")
        lines.append(f'alias = "{m.knemon_alias}"')
        lines.append(f'provider = "{m.provider}"')
        lines.append(f'model = "{m.model}"')
        lines.append(f'tier = "{m.knemon_tier}"')
    return "\n".join(lines) + "\n"


def emit_zeroclaw_toml(registry: Registry, host: str) -> str:
    """zeroclaw TOML fragment for `host` — [providers.models.*]."""
    rows = _host_models(registry, host)
    lines = _header(registry, "zeroclaw", host)
    for slot, m in rows:
        if "zeroclaw.default" not in m.routes:
            continue
        lines.append(f"[providers.models.{m.model}]")
        lines.append(f'name = "{m.key}"')
        lines.append(f'endpoint = "{m.endpoint}"')
        if m.auth:
            lines.append(f'auth = "{m.auth}"')
        lines.append(f'cost_tier = "{registry.providers.get(m.provider, "C")}"')
        lines.append("")
    if len(lines) == 4:  # header only
        lines.append("# no zeroclaw.default model is assigned to this host")
        lines.append("")
    return "\n".join(lines) + "\n"


# ── committed outputs + CLI ─────────────────────────────────────────────────
def targets(registry_path: Path) -> dict[Path, str]:
    """Every committed consumer, keyed by path — what --write/--check compare."""
    reg = load_registry(registry_path)
    out: dict[Path, str] = {MODELS_PY: _replace_bus_block(MODELS_PY.read_text(), emit_bus_tables(reg))}
    for host in reg.hosts:
        out[GENERATED_DIR / host / "zoder.toml"] = emit_zoder_toml(reg, host)
        out[GENERATED_DIR / host / "zeroclaw.toml"] = emit_zeroclaw_toml(reg, host)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--registry", default=str(DEFAULT_REGISTRY), help="registry TOML (default: the shipped one)")
    ap.add_argument("--check", action="store_true", help="fail if committed consumers drifted (default)")
    ap.add_argument("--write", action="store_true", help="rewrite the committed consumers")
    args = ap.parse_args(argv)
    if args.check and args.write:
        ap.error("--check and --write are mutually exclusive")

    reg_path = Path(args.registry)
    try:
        wanted = targets(reg_path)
    except RegistryError as exc:
        print(f"registry error: {exc}", file=sys.stderr)
        return 2

    drift: list[str] = []
    for path, want in wanted.items():
        have = path.read_text() if path.exists() else None
        if have == want:
            continue
        if args.write:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(want)
            print(f"wrote {path}")
        else:
            drift.append(str(path))

    if drift:
        print("model registry drift in:\n  " + "\n  ".join(drift), file=sys.stderr)
        print("run: python -m hive.core.model_registry --write", file=sys.stderr)
        return 1
    if not args.write:
        print(f"model registry OK ({len(wanted)} generated consumers match {reg_path.name})")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())