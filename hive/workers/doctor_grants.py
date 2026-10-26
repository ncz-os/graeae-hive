"""Operator-only CLI for scoped doctor grants; no model or SOP payload can issue one.

After approving the exact action at the SOP checkpoint, the operator copies its
kind and parts into --action. The CLI records the digest, identity, TTL and use
state in the same file as doctor safety counters. Review the printed digest against
the SOP decision before invoking the executor. No background wiring is installed.
"""

from __future__ import annotations

import argparse
import json
import sys

from .doctor_gates import APPROVAL_TTL_SECS, DESTRUCTIVE_ACTIONS, ApprovalGate, action_digest
from .doctor_state import DurableStateStore, FileBackend, state_store_from_env


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Issue, inspect or revoke scoped doctor grants")
    parser.add_argument("--state-path", help="explicit file backend path (default: doctor state path)")
    commands = parser.add_subparsers(dest="command", required=True)
    issue = commands.add_parser("issue")
    issue.add_argument("--action", required=True, help='exact JSON: {"kind":...,"parts":{...}}')
    issue.add_argument("--approver", required=True, help="operator:<identity> from checkpoint")
    issue.add_argument("--ttl", type=float, default=APPROVAL_TTL_SECS)
    commands.add_parser("list")
    revoke = commands.add_parser("revoke")
    revoke.add_argument("digest")
    args = parser.parse_args(argv)
    try:
        state = (
            DurableStateStore(FileBackend(args.state_path), refresh_secs=0)
            if args.state_path
            else state_store_from_env()
        )
        if not isinstance(state.backend, FileBackend):
            raise ValueError("grant CLI requires a file state backend")
        if args.command == "issue":
            action = json.loads(args.action)
            if (
                not isinstance(action, dict)
                or set(action) != {"kind", "parts"}
                or not isinstance(action["parts"], dict)
            ):
                raise ValueError("action must contain only kind and parts object")
            kind = action["kind"]
            if kind not in DESTRUCTIVE_ACTIONS:
                raise ValueError("unknown destructive action")
            if not 0 < args.ttl <= 900:
                raise ValueError("TTL must be in (0, 900] seconds")
            digest = action_digest(kind, **action["parts"])
            ApprovalGate(state, ttl=args.ttl).record_approval(kind, digest, by=args.approver)
            print(json.dumps({"digest": digest, "approver": args.approver}))
        elif args.command == "list":
            state.reload()
            if not state.loaded_ok:
                raise OSError("state read failed")
            for key in state.keys("approval:"):
                grant = state.value(key)
                if isinstance(grant, dict) and grant.get("digest") == key.removeprefix("approval:"):
                    print(json.dumps(grant, sort_keys=True))
        else:
            if len(args.digest) != 32 or any(char not in "0123456789abcdef" for char in args.digest):
                raise ValueError("invalid digest")
            ApprovalGate(state).revoke(args.digest)
            if not state.persist_ok:
                raise OSError("revocation was not persisted")
            print(json.dumps({"revoked": args.digest}))
    except (OSError, ValueError, TypeError) as error:
        print(f"grant operation refused: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
