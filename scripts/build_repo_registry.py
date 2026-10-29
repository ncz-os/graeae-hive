#!/usr/bin/env python3
"""Build data/repos.json from the live sources + curated overlay.

Sources (all optional; missing creds just skip that source):
  gitlab   GITLAB_PAT            or ~/.api_keys_master.json gitlab.ncz_os_pat
  github   GITHUB_PAT            or ...github.oauth_token_perlowja
  argonas  ARGONAS_SSH_HOST + ARGONAS_SSH_PASS  (ssh root@$ARGONAS_SSH_HOST 'ls *.git')
  buildkite BUILDKITE_API_TOKEN  or ...buildkite.api_access_token
  r2       R2_ACCOUNT_ID + CLOUDFLARE_R2_API_TOKEN (or ~/.api_keys_master.json cloudflare.r2_*)

Usage:
    scripts/build_repo_registry.py                 # write data/repos.json
    scripts/build_repo_registry.py --dry-run       # print summary, write nothing
    scripts/build_repo_registry.py --skip r2 argonas
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hive.registry import build_registry  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "data" / "repos.json"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--skip", nargs="*", default=[], help="sources to skip: gitlab github argonas buildkite r2")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    reg = build_registry(skip=tuple(args.skip))
    repos = reg["repos"]
    by_source: dict[str, int] = {}
    for e in repos:
        for s in e.get("sources", []):
            by_source[s] = by_source.get(s, 0) + 1

    print(f"built registry: {len(repos)} repos")
    print(f"  by source: {json.dumps(by_source, sort_keys=True)}")
    print(f"  r2 buckets: {reg['_meta'].get('r2_buckets')}")
    only_overlay = [e["id"] for e in repos if e.get("sources") == ["overlay"]]
    if only_overlay:
        print(f"  overlay-only (no live source): {len(only_overlay)} -> {only_overlay[:12]}")

    if args.dry_run:
        print("dry-run: not writing")
        return 0

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(reg, indent=2, sort_keys=True) + "\n")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
