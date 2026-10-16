"""Backend contract — the ABC must be fully implemented on EVERY supported backend.

Two guarantees:
  1. STRUCTURAL (always runs): every supported backend resolves from the factory
     and its repo class implements the whole HiveMindRepository ABC (nothing left
     abstract). Adding a method to the ABC without implementing it on a backend
     fails here — so a new ABC call cannot silently break a backend.
  2. DIALECT (always runs): the pure translation helpers produce the right
     postgres/mysql SQL + DDL.

A live behavioural conformance run (the ABC executed against a real instance) is
enabled per backend by setting its DSN; without infra it is skipped.
"""

from __future__ import annotations

import importlib
import inspect
import os
import unittest

os.environ.setdefault("HIVE_BACKEND", "sqlite")

from hive.persistence.base import HiveMindRepository  # noqa: E402
from hive.persistence.factory import get_hive_repository  # noqa: E402

BACKENDS = [
    ("sqlite", "hive.persistence.sqlite", "SqliteHiveRepository"),
    ("oracle", "hive.persistence.oracle", "OracleHiveRepository"),
    ("db2", "hive.persistence.db2", "Db2HiveRepository"),
    ("postgres", "hive.persistence.postgres", "PostgresHiveRepository"),
    ("mysql", "hive.persistence.mysql", "MySqlHiveRepository"),
]


class BackendStructuralContractTests(unittest.TestCase):
    def test_backend_module_classes_are_concrete(self):
        for name, mod, cls in BACKENDS:
            m = importlib.import_module(mod)
            klass = getattr(m, cls)
            self.assertTrue(issubclass(klass, HiveMindRepository), f"{name}: {cls} must implement the ABC")
            # a class with unimplemented abstract methods cannot be instantiated
            self.assertFalse(inspect.isabstract(klass), f"{name}: {cls} leaves ABC methods unimplemented")

    def test_factory_resolves_every_supported_backend(self):
        for name, *_ in BACKENDS:
            repo = get_hive_repository(backend=name)
            self.assertIsInstance(repo, HiveMindRepository, f"factory did not return a repo for {name!r}")

    def test_every_abc_method_is_present_on_each_backend(self):
        # each concrete backend either implements or inherits every ABC method
        abc = {n for n in dir(HiveMindRepository) if not n.startswith("_") and callable(getattr(HiveMindRepository, n))}
        for name, mod, cls in BACKENDS:
            klass = getattr(importlib.import_module(mod), cls)
            missing = {n for n in abc if not hasattr(klass, n)}
            self.assertEqual(missing, set(), f"{name}: missing ABC methods {sorted(missing)}")

    def test_unknown_backend_rejected(self):
        with self.assertRaises(RuntimeError):
            get_hive_repository(backend="not-a-backend")


class DialectTranslationTests(unittest.TestCase):
    def test_numbered_placeholders(self):
        from hive.persistence.dialect import to_numbered

        self.assertEqual(to_numbered("SELECT * FROM t WHERE a=? AND b=?"), "SELECT * FROM t WHERE a=$1 AND b=$2")

    def test_format_placeholders(self):
        from hive.persistence.dialect import to_format

        self.assertEqual(to_format("SELECT * FROM t WHERE a=?"), "SELECT * FROM t WHERE a=%s")

    def test_mysql_upsert_rewrite(self):
        from hive.persistence.dialect import mysql_upsert

        got = mysql_upsert(
            "INSERT INTO hive_cache (cache_key, result_json) VALUES (?, ?) "
            "ON CONFLICT(cache_key) DO UPDATE SET result_json=excluded.result_json"
        )
        self.assertIn("ON DUPLICATE KEY UPDATE", got)
        self.assertIn("result_json=VALUES(result_json)", got)
        self.assertNotIn("ON CONFLICT", got)

    def test_postgres_ddl_types_and_identity(self):
        from hive.persistence.dialect import postgres_ddl

        sqlite = "CREATE TABLE IF NOT EXISTS events (\n  id INTEGER PRIMARY KEY AUTOINCREMENT,\n  ts REAL,\n  kind TEXT\n);"
        pg = postgres_ddl(sqlite)
        self.assertIn("BIGSERIAL PRIMARY KEY", pg)
        self.assertIn("double precision", pg)
        self.assertIn("text", pg)
        self.assertNotIn("AUTOINCREMENT", pg)

    def test_mysql_ddl_types_and_identity(self):
        from hive.persistence.dialect import mysql_ddl

        sqlite = "CREATE TABLE IF NOT EXISTS events (\n  id INTEGER PRIMARY KEY AUTOINCREMENT,\n  ts REAL,\n  kind TEXT\n);"
        my = mysql_ddl(sqlite)
        self.assertIn("BIGINT AUTO_INCREMENT PRIMARY KEY", my)
        self.assertIn("double", my)


if __name__ == "__main__":
    unittest.main()
