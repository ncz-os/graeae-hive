"""Dialect contract for the shared repository SQL (JSON_VALUE).

Production runs behind translating backends (Oracle, PostgreSQL, MySQL, DB2) that subclass
SqliteHiveRepository, so the SQL in sqlite.py is the COMMON dialect. 2026-10-30 regression: a "portable"
rewrite to ``json_extract`` made GET /v1/stats/costs fail on the Oracle backend
(ORA-00904 "JSON_EXTRACT": invalid identifier) while passing on stock SQLite. These tests pin the
contract: the shared SQL uses JSON_VALUE, SQLite provides that function, and the Oracle translator
leaves it intact.
"""
from __future__ import annotations

import asyncio
import json
import unittest

from hive.persistence import oracle as oracle_backend
from hive.persistence.sqlite import SqliteHiveRepository, _json_value

GROUP_BYS = ("day", "provider", "model", "runtime", "cost_tier", "claimed_by")


class _Cursor:
    def __init__(self, sink, sql, params):
        sink.append((sql, params))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def __aiter__(self):
        async def gen():
            if False:
                yield None

        return gen()


class _RecordingTx:
    def __init__(self):
        self.statements = []

    def execute(self, sql, params=()):
        return _Cursor(self.statements, sql, params)


def _captured_sql():
    repo = SqliteHiveRepository.__new__(SqliteHiveRepository)
    out = {}
    for group_by in GROUP_BYS:
        tx = _RecordingTx()
        asyncio.run(repo.cost_stats_rows(tx, group_by=group_by, cutoff=0.0))
        out[group_by] = tx.statements[0][0]
    return out


class JsonValueUdfTests(unittest.TestCase):
    def test_scalar_nested_and_index(self):
        doc = json.dumps({"a": {"b": "x"}, "n": 3, "l": [{"k": "v"}, 2]})
        self.assertEqual(_json_value(doc, "$.a.b"), "x")
        self.assertEqual(_json_value(doc, "$.n"), 3)
        self.assertEqual(_json_value(doc, "$.l[0].k"), "v")
        self.assertEqual(_json_value(doc, "$.l[1]"), 2)

    def test_missing_invalid_and_bad_inputs_are_null(self):
        self.assertIsNone(_json_value('{"a":1}', "$.b"))
        self.assertIsNone(_json_value('{"l":[1]}', "$.l[5]"))
        self.assertIsNone(_json_value("not json", "$.a"))
        self.assertIsNone(_json_value(None, "$.a"))
        self.assertIsNone(_json_value('{"a":1}', "a"))
        self.assertIsNone(_json_value('{"a":1}', None))

    def test_objects_arrays_and_booleans(self):
        doc = '{"o":{"x":1},"l":[1,2],"t":true,"f":false}'
        self.assertEqual(json.loads(_json_value(doc, "$.o")), {"x": 1})
        self.assertEqual(json.loads(_json_value(doc, "$.l")), [1, 2])
        self.assertEqual(_json_value(doc, "$.t"), "true")
        self.assertEqual(_json_value(doc, "$.f"), "false")


class DialectContractTests(unittest.TestCase):
    def test_shared_sql_uses_the_standard_function_not_a_sqlite_only_one(self):
        for group_by, sql in _captured_sql().items():
            with self.subTest(group_by=group_by):
                self.assertNotIn("json_extract", sql.lower(), "common-dialect SQL must not use json_extract")

    def test_provider_query_uses_json_value(self):
        self.assertIn("JSON_VALUE(", _captured_sql()["provider"])

    def test_oracle_translation_keeps_the_function_and_never_introduces_json_extract(self):
        for group_by, sql in _captured_sql().items():
            translated, _ = oracle_backend._translate_sql(sql, (0.0,))
            with self.subTest(group_by=group_by):
                self.assertIsNotNone(translated)
                self.assertNotIn("json_extract", translated.lower())
                if group_by == "provider":
                    self.assertIn("JSON_VALUE(", translated)


if __name__ == "__main__":
    unittest.main()
