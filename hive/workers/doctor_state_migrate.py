"""Offline, read-only inspection and guarded import of doctor safety state.

Stop the old writer first. Dry run prints the exact JSON namespace to import.
Only --apply writes, only to an absent or empty named target. Source is never
changed. The bus path performs GET only and refuses incomplete journals.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from .doctor_state import DEFAULT_NAMESPACE, BusBackend, FileBackend


def _validate(records: dict) -> None:
    if not isinstance(records, dict) or any(
        not isinstance(key, str) or not isinstance(value, dict) for key, value in records.items()
    ):
        raise ValueError("source state has invalid records")
    for value in records.values():
        if "at" not in value and "timestamps" not in value:
            raise ValueError("source state has a record without timestamp")
        if value.get("tombstone") not in (None, True, False):
            raise ValueError("source state has an invalid tombstone")


def _bus_getter(bus_url: str):
    parsed = urlsplit(bus_url)
    if (
        parsed.scheme not in ("http", "https")
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("bus URL must be an HTTP base URL without credentials or query")
    base = bus_url.rstrip("/")

    def get(method, path, body=None):
        if method != "GET" or body is not None or not path.startswith("/v1/messages?"):
            raise ValueError("migration bus access is GET-only")
        request = Request(base + path, headers={"X-Hive-Actor": "urn:operator:doctor-state-migrate"}, method="GET")
        try:
            with urlopen(request, timeout=15) as response:  # noqa: S310 - explicit operator URL
                raw = response.read(8_388_609)
                if len(raw) > 8_388_608:
                    raise ValueError("bus response truncated by size limit")
                return response.status, json.loads(raw)
        except OSError:
            raise OSError("bus GET failed") from None

    return get


def inspect_and_import(
    *,
    source_file: str | None = None,
    bus_url: str | None = None,
    bus_http=None,
    target_file: str,
    apply: bool = False,
    bus_limit: int = 500,
) -> dict:
    """Return exact would-write report; apply only after complete source/target reads."""
    if sum(value is not None for value in (source_file, bus_url, bus_http)) != 1:
        raise ValueError("choose exactly one source file or bus journal")
    if not target_file:
        raise ValueError("explicit target file path required")
    if source_file is not None:
        if not os.path.isfile(source_file):
            raise OSError("source file is absent or unreadable")
        source = FileBackend(source_file)
        records = source.load(DEFAULT_NAMESPACE)
        if source._read_bytes is None:
            raise OSError("source file disappeared during read")
        origin = "file"
    else:
        if not 0 < bus_limit <= 100_000:
            raise ValueError("bus limit out of range")
        http = bus_http if bus_http is not None else _bus_getter(bus_url)

        def strict_get(method, path, body=None):
            if method != "GET":
                raise ValueError("migration bus access is GET-only")
            status, response = http(method, path) if body is None else http(method, path, body)
            if status == 200 and isinstance(response, dict) and isinstance(response.get("messages"), list):
                for message in response["messages"]:
                    if not isinstance(message, dict) or not isinstance(message.get("payload"), dict):
                        raise ValueError("bus journal contains malformed message")
                    if type(message.get("ts")) not in (int, float) or not math.isfinite(message["ts"]):
                        raise ValueError("bus journal contains unordered state message")
                    payload = message["payload"]
                    if not isinstance(payload.get("key"), str) or not isinstance(payload.get("value"), dict):
                        raise ValueError("bus journal contains malformed state record")
            return status, response

        backend = BusBackend(strict_get, limit=bus_limit)
        records = backend.load(DEFAULT_NAMESPACE)
        if backend.truncated:
            raise ValueError("bus journal read truncated; increase --bus-limit")
        origin = "bus"
    _validate(records)
    if apply and not records:
        raise ValueError("source is empty; refusing an empty import")
    target = FileBackend(target_file)
    # Validate target with the same reader the executor uses. The raw object must
    # be entirely empty, including other namespaces, before this import owns it.
    try:
        with open(target_file, "rb") as handle:
            existing = json.load(handle)
    except FileNotFoundError:
        existing = {}
    if not isinstance(existing, dict):
        raise ValueError("target file is invalid")
    if existing:
        raise ValueError("target file is non-empty; refusing overwrite")
    target.load(DEFAULT_NAMESPACE)
    if target._read_bytes is not None and json.loads(target._read_bytes):
        raise ValueError("target file became non-empty; refusing overwrite")
    report = {"source": origin, "records": len(records), "would_write": {DEFAULT_NAMESPACE: records}}
    if apply:
        if not target.save(DEFAULT_NAMESPACE, records):
            raise OSError("target changed or could not be written")
        report["applied"] = True
    else:
        report["applied"] = False
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--source-file")
    source.add_argument("--bus-url")
    parser.add_argument("--target", required=True)
    parser.add_argument("--bus-limit", type=int, default=500)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    try:
        report = inspect_and_import(
            source_file=args.source_file,
            bus_url=args.bus_url,
            target_file=args.target,
            apply=args.apply,
            bus_limit=args.bus_limit,
        )
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"migration refused: {error}", file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
