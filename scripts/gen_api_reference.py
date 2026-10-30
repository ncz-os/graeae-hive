#!/usr/bin/env python3
"""Generate docs/API_REFERENCE.md from the live bus FastAPI app.

Reads ``app.openapi()`` (every route, method, path/query/body fields) and
cross-references the router source (``hive/bus/deps.py`` + ``routers/``) for
the auth tier each route needs and the status codes the code raises
explicitly, plus the job/agent status values. No addresses, no tokens;
examples use a placeholder host.

    scripts/gen_api_reference.py [--out docs/API_REFERENCE.md]
    scripts/gen_api_reference.py --check [--out docs/API_REFERENCE.md]
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import importlib
import inspect
import pkgutil
import sys
from pathlib import Path

PLACEHOLDER_HOST = "http://<bus-host>:5005"

MUTATING = {"POST", "PUT", "PATCH", "DELETE"}


def _load_app():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import agent_bus

    return agent_bus.app


def _openapi_path(path: str) -> str:
    import re

    return re.sub(r"\{(\w+)(:[^}]+)?\}", r"{\1}", path)


def _iter_routes():
    """Yield (path, method, APIRoute) from every bus router module."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import hive.bus.routers as pkg

    out = []
    for mod in pkgutil.iter_modules(pkg.__path__):
        module = importlib.import_module(f"hive.bus.routers.{mod.name}")
        router = getattr(module, "router", None)
        routes = getattr(router, "routes", None)
        if not routes:
            continue
        for route in routes:
            methods = sorted(getattr(route, "methods", None) or [])
            endpoint = getattr(route, "endpoint", None)
            if not methods or endpoint is None:
                continue
            for method in methods:
                if method == "HEAD":
                    continue
                out.append((_openapi_path(route.path), method, route))
    out.sort(key=lambda t: (t[0], t[1]))
    return out


def _codes_in_call(call: ast.Call) -> set[int]:
    """Status codes carried by one ``HTTPException(...)`` raise or
    ``JSONResponse(..., status_code=N)`` return. Best effort."""
    func_name = ""
    if isinstance(call.func, ast.Name):
        func_name = call.func.id
    elif isinstance(call.func, ast.Attribute):
        func_name = call.func.attr
    codes: set[int] = set()
    if func_name == "HTTPException":
        if call.args and isinstance(call.args[0], ast.Constant):
            with contextlib.suppress(TypeError, ValueError):
                codes.add(int(call.args[0].value))
        for kw in call.keywords:
            if kw.arg == "status_code" and isinstance(kw.value, ast.Constant):
                with contextlib.suppress(TypeError, ValueError):
                    codes.add(int(kw.value.value))
    elif func_name == "JSONResponse" or func_name.endswith("Response"):
        # JSONResponse(payload, status_code=503), HTMLResponse(html, status_code=404), ...
        if len(call.args) >= 2 and isinstance(call.args[1], ast.Constant):
            with contextlib.suppress(TypeError, ValueError):
                codes.add(int(call.args[1].value))
        for kw in call.keywords:
            if kw.arg == "status_code" and isinstance(kw.value, ast.Constant):
                with contextlib.suppress(TypeError, ValueError):
                    codes.add(int(kw.value.value))
    return codes


def _source_of(func) -> str:
    try:
        return inspect.getsource(func)
    except (OSError, TypeError):
        return ""


def _called_helpers(func, tree) -> list:
    out = []
    seen = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            helper = getattr(func, "__globals__", {}).get(node.func.id)
            if callable(helper) and helper is not func and id(helper) not in seen:
                seen.add(id(helper))
                out.append(helper)
    return out


def _scan_tree(tree) -> set[int]:
    codes: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call):
            codes |= _codes_in_call(node.exc)
        elif isinstance(node, ast.Return) and isinstance(node.value, ast.Call):
            codes |= _codes_in_call(node.value)
    return codes


def _raises_in_source(func) -> list[int]:
    """HTTP status codes the endpoint raises/returns explicitly.

    Covers the endpoint body, helpers it calls directly (one level, e.g.
    ``_authorize_deregister``), and — via :func:`_dep_gate_codes` at the call
    site — the ``Depends`` gate callables (e.g. the 401 from
    ``require_mutation_actor``). Best effort, never raises.
    """
    source = _source_of(func)
    if not source:
        return []
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    codes = _scan_tree(tree)
    for helper in _called_helpers(func, tree):
        hsource = _source_of(helper)
        if "HTTPException" not in hsource and "JSONResponse" not in hsource:
            continue
        try:
            codes |= _scan_tree(ast.parse(hsource))
        except SyntaxError:
            continue
    return sorted(codes)


def _dep_gate_codes(route) -> list[int]:
    """Status codes raised by the route's ``Depends`` gate callables."""
    names: set[str] = set()
    for dep in getattr(route, "dependencies", None) or []:
        fn = getattr(dep, "dependency", None)
        if fn is not None and hasattr(fn, "__globals__"):
            names.add(fn.__name__)
    dependant = getattr(route, "dependant", None)
    for dep in getattr(dependant, "dependencies", None) or [] if dependant else []:
        fn = getattr(getattr(dep, "call", None), "__name__", "") or getattr(
            getattr(dep, "dependency", None), "__name__", ""
        )
        if fn:
            names.add(fn)
    if not names:
        return []
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from hive.bus import deps as _deps

    codes: set[int] = set()
    for name in names:
        fn = getattr(_deps, name, None)
        if callable(fn):
            codes.update(_raises_in_source(fn))
    return sorted(codes)


def _auth_tier(path: str, method: str, route) -> str:
    """One of: admin tier, X-Hive-Actor, X-Hive-Token, none (with the why)."""
    endpoint = route.endpoint
    try:
        source = inspect.getsource(endpoint)
    except (OSError, TypeError):
        source = ""
    dep_names = set()
    for dep in getattr(route, "dependencies", None) or []:
        fn = getattr(dep, "dependency", None)
        if fn is not None:
            dep_names.add(getattr(fn, "__name__", ""))
    dependant = getattr(route, "dependant", None)
    for dep in getattr(dependant, "dependencies", None) or [] if dependant else []:
        fn = getattr(getattr(dep, "call", None), "__name__", "") or getattr(
            getattr(dep, "dependency", None), "__name__", ""
        )
        if fn:
            dep_names.add(fn)
    try:
        params = set(inspect.signature(endpoint).parameters)
    except (TypeError, ValueError):
        params = set()
    admin_in_source = "is_admin_tier" in source
    if path.startswith("/v1/admin/"):
        return "X-Hive-Actor (admin operation; must name a registered, active agent)"
    if "require_mutation_actor" in dep_names:
        extra = ""
        if admin_in_source:
            extra = "; admin tier (Authorization: Bearer) accepted where the code checks is_admin_tier"
        if "x_hive_token" in params:
            extra += "; X-Hive-Token required when HIVE_SHARED_SECRET is set"
        return "X-Hive-Actor (must name a registered, active agent)" + extra
    if admin_in_source:
        return "admin tier (Authorization: Bearer) or self via X-Hive-Actor"
    if "x_hive_token" in params:
        return "X-Hive-Token (when HIVE_SHARED_SECRET is set) + X-Hive-Actor"
    if "x_hive_actor" in params:
        return "X-Hive-Actor"
    if method in MUTATING:
        return "none (payload-gated: e.g. claimed_by must match; no header)"
    return "none"


def _schema_type(schema: dict) -> str:
    if not isinstance(schema, dict):
        return "-"
    if "$ref" in schema:
        return schema["$ref"].split("/")[-1]
    if "anyOf" in schema:
        return " | ".join(_schema_type(s) for s in schema["anyOf"])
    text = schema.get("type", "-")
    if "default" in schema:
        text += f" (default {schema['default']!r})"
    return text


def _resolve(ref: str, spec: dict) -> dict:
    node: object = spec
    for part in ref.lstrip("#/").split("/"):
        if isinstance(node, dict):
            node = node.get(part, {})
        else:
            return {}
    return node if isinstance(node, dict) else {}


def generate() -> str:
    app = _load_app()
    spec = app.openapi()
    paths = spec.get("paths", {})
    components = spec.get("components", {})
    _ = components  # resolved per-route below via spec
    routes = _iter_routes()
    info = spec.get("info", {})

    lines = [
        "# Hive Bus API Reference",
        "",
        "Generated from the live FastAPI app (`app.openapi()` cross-checked",
        "against `hive/bus/deps.py` and `hive/bus/routers/`). Do not edit by",
        "hand: run `scripts/gen_api_reference.py` (or `--check` in tests).",
        "No addresses, no tokens; examples use a placeholder host.",
        "",
        f"Base URL: `{PLACEHOLDER_HOST}`",
        f"App: {info.get('title', 'GRAEAE Hive Mind')} {info.get('version', '')}".rstrip(),
        "",
        "Auth tiers used below:",
        "",
        "- `admin tier`: `Authorization: Bearer` with the bus admin token.",
        "- `X-Hive-Actor`: header naming a registered, active agent.",
        "- `X-Hive-Token`: required with `X-Hive-Actor` when `HIVE_SHARED_SECRET` is set.",
        "- `none`: no header; the route gates on its payload instead.",
        "",
    ]

    by_route = {(path, method): route for path, method, route in routes}
    for path in sorted(paths):
        for method in sorted(m for m in paths[path] if m.lower() != "head"):
            route = by_route.get((path, method.upper()))
            node = paths[path][method]
            lines.append(f"## `{method.upper()} {path}`")
            lines.append("")
            handler = route.endpoint.__name__ if route is not None else "-"
            summary = (node.get("summary") or handler).strip()
            if node.get("description"):
                summary += " — " + " ".join(node["description"].split())[:200]
            lines.append(f"Handler: `{handler}`. {summary}")
            lines.append("")
            lines.append(
                f"Auth: {_auth_tier(path, method.upper(), route) if route is not None else 'none (not in routers/*; served by middleware/static)'}"
            )
            lines.append("")
            params = node.get("parameters", [])
            if params:
                lines.append("Parameters:")
                lines.append("")
                lines.append("| name | in | required | type |")
                lines.append("| --- | --- | --- | --- |")
                for param in sorted(params, key=lambda p: (p.get("in", ""), p.get("name", ""))):
                    lines.append(
                        f"| `{param.get('name')}` | {param.get('in')} "
                        f"| {param.get('required', False)} | {_schema_type(param.get('schema', {}))} |"
                    )
                lines.append("")
            else:
                lines.append("Parameters: none.")
                lines.append("")
            body = (node.get("requestBody") or {}).get("content", {}).get("application/json", {}).get("schema", {})
            if body:
                if "$ref" in body:
                    schema = _resolve(body["$ref"], spec)
                    props = schema.get("properties", {})
                    required = set(schema.get("required", []))
                    if props:
                        lines.append(f"Body (`{body['$ref'].split('/')[-1]}`):")
                        lines.append("")
                        lines.append("| field | required | type |")
                        lines.append("| --- | --- | --- |")
                        for name in sorted(props):
                            lines.append(f"| `{name}` | {name in required} | {_schema_type(props[name])} |")
                        lines.append("")
                    else:
                        lines.append(f"Body: `{body['$ref'].split('/')[-1]}`.")
                        lines.append("")
                else:
                    lines.append(f"Body type: {_schema_type(body)}.")
                    lines.append("")
            else:
                lines.append("Body: none.")
                lines.append("")
            explicit = (
                sorted(set(_raises_in_source(route.endpoint)) | set(_dep_gate_codes(route)))
                if route is not None
                else []
            )
            responses = node.get("responses", {})
            codes = sorted({str(c) for c in list(responses) + [str(c) for c in explicit]})
            lines.append(
                f"Status codes: {', '.join(codes) if codes else '-'}"
                + (f" (raised explicitly in code: {', '.join(map(str, explicit))})" if explicit else "")
            )
            lines.append("")
            lines.append(f"Example: `curl {PLACEHOLDER_HOST}{path}`")
            lines.append("")

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from hive.bus import deps as _deps

    lines.append("## Status values")
    lines.append("")
    for name in (
        "VALID_JOB_STATUSES",
        "TERMINAL_JOB_STATUSES",
        "IN_FLIGHT_JOB_STATUSES",
        "DEP_DEAD_STATUSES",
        "VALID_AGENT_STATUSES",
    ):
        value = getattr(_deps, name, None)
        try:
            shown = sorted(value)
        except TypeError:
            shown = list(value) if value is not None else []
        lines.append(f"- `{name}`: {', '.join(shown)}")
    lines.append("")
    lines.append("Job lifecycle: queued → offered → claimed → running → done/failed;")
    lines.append("failed rows may be requeued to queued (budget permitting) or parked")
    lines.append("as needs-review; dead-letter/cancelled are terminal.")
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate docs/API_REFERENCE.md from app.openapi().")
    parser.add_argument("--out", default="docs/API_REFERENCE.md")
    parser.add_argument("--check", action="store_true", help="Fail when the committed file is out of date.")
    args = parser.parse_args(argv)
    text = generate()
    out = Path(args.out)
    if args.check:
        try:
            current = out.read_text()
        except OSError:
            print(f"{out} is missing; run scripts/gen_api_reference.py", file=sys.stderr)
            return 1
        if current != text:
            print(f"{out} is out of date; run scripts/gen_api_reference.py", file=sys.stderr)
            return 1
        return 0
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
