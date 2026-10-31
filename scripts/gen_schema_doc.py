"""Generate the mechanical section of docs/SCHEMA.md from the DDL sources.

Stdlib only. Parses the sqlite reference DDL, the postgres/mysql
translations, the db2 DDL plus its additive map, and the in-code oracle
fragments, then emits one markdown section per backend. Run from the repo
root: ``python scripts/gen_schema_doc.py`` (rewrites docs/SCHEMA.md between
the GENERATED markers, preserving the hand-written findings section after
them).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))

from hive.persistence import db2 as db2mod  # noqa: E402
from hive.persistence import oracle as oraclemod  # noqa: E402
from hive.persistence.dialect import mysql_ddl, postgres_ddl  # noqa: E402
from hive.persistence.sqlite import DDL as SQLITE_DDL  # noqa: E402

SQLITE_SRC = (ROOT / "hive" / "persistence" / "sqlite.py").read_text()

BEGIN = "<!-- GENERATED-BEGIN: do not edit between the markers -->"
END = "<!-- GENERATED-END -->"


def strip_comments(ddl: str) -> str:
    return re.sub(r"--[^\n]*", "", ddl)


def split_top_commas(body: str) -> list[str]:
    parts, depth, cur = [], 0, ""
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur)
    return parts


def parse_tables(ddl: str) -> dict[str, dict[str, str]]:
    """Table -> {column: TYPE} with the raw (still backend-spelled) type."""
    tables: dict[str, dict[str, str]] = {}
    for m in re.finditer(r"CREATE TABLE (?:IF NOT EXISTS )?(\w+)\s*\((.*?)\);", strip_comments(ddl), re.I | re.S):
        cols: dict[str, str] = {}
        for part in split_top_commas(m.group(2)):
            p = part.strip()
            if not p or re.match(r"(?i)^(PRIMARY KEY|FOREIGN KEY|CHECK|CONSTRAINT|UNIQUE)\b", p):
                continue
            cm = re.match(r'"?(\w+)"?\s+(.*)', p, re.S)
            if not cm:
                continue
            cname, rest = cm.group(1).lower(), cm.group(2).strip()
            tm = re.match(r"([A-Za-z ]+?)(?:\s*\(\s*[\d,\s]+\s*\))?(?=\s|,|$)", rest)
            ctype = tm.group(1).strip().upper() if tm else rest.split()[0].upper()
            cols[cname] = re.sub(r"\s+", " ", ctype)
        tables[m.group(1).lower()] = cols
    return tables


def parse_indexes(*sources: str) -> dict[str, list[str]]:
    """Index name -> [table, ...] from CREATE INDEX statements."""
    out: dict[str, list[str]] = {}
    for src in sources:
        for m in re.finditer(
            r"CREATE\s+(UNIQUE\s+)?INDEX\s+(?:IF NOT EXISTS\s+)?(\w+)\s+ON\s+(\w+)",
            strip_comments(src),
            re.I,
        ):
            out.setdefault(m.group(2).lower(), []).append(m.group(3).lower())
    return out


def sqlite_live_columns() -> dict[str, dict[str, str]]:
    """The migrator's additive columns: the (table, column, fragment) tuples
    plus the later `_ensure_column(db, table, column, fragment)` calls."""
    live: dict[str, dict[str, str]] = {}
    found = re.findall(r'\(\s*"(\w+)",\s*"(\w+)",\s*"([^"]+)"\s*\)', SQLITE_SRC)
    found += re.findall(r'_ensure_column\(\s*\w+,\s*"(\w+)",\s*"(\w+)",\s*"([^"]+)"', SQLITE_SRC)
    for table, col, frag in found:
        tm = re.match(r"\w+\s+([A-Za-z]+)", frag.strip())
        if tm:
            live.setdefault(table, {})[col] = re.sub(r"\s+", " ", tm.group(1).strip().upper())
    return live


def sqlite_live_indexes() -> dict[str, list[str]]:
    stmts = re.findall(r'"(CREATE\s+(?:UNIQUE\s+)?INDEX[^"]+)"', SQLITE_SRC)
    return parse_indexes(*stmts)


def oracle_job_fragments() -> dict[str, str]:
    frags = {}
    for name, definition in oraclemod._ORACLE_JOB_ADDITIONS.items():
        tm = re.match(r"\w+\s+([A-Za-z]+(?:\s*\(\s*[\d,\s]+\s*\))?)", definition.strip())
        if tm:
            frags[name.lower()] = re.sub(r"\s+", " ", tm.group(1).strip().upper())
    return frags


def emit() -> str:
    ref = parse_tables(SQLITE_DDL)
    pg = parse_tables(postgres_ddl(SQLITE_DDL))
    my = parse_tables(mysql_ddl(SQLITE_DDL))
    db = parse_tables(db2mod.DB2_DDL)
    lines = [BEGIN, ""]
    lines.append(
        f"_Reference: sqlite DDL ({len(ref)} tables). Generated from the sources; findings are hand-written below the markers._"
    )
    lines.append("")
    for backend, tables in (
        ("sqlite (reference)", ref),
        ("postgres (`postgres_ddl`)", pg),
        ("mysql (`mysql_ddl`)", my),
        ("db2 (`DB2_DDL`)", db),
    ):
        lines.append(f"## {backend}")
        lines.append("")
        for table in sorted(tables):
            cols = tables[table]
            lines.append(f"### {table} ({len(cols)} columns)")
            lines.append("")
            lines.append("| column | type |")
            lines.append("| --- | --- |")
            for col, ctype in sorted(cols.items()):
                lines.append(f"| {col} | {ctype} |")
            lines.append("")
    lines.append("## oracle (in-code fragments only)")
    lines.append("")
    lines.append("No full DDL in code; the canonical schema is external. In-code evidence:")
    lines.append("")
    lines.append(f"- `_TABLE_MAP`: {', '.join(f'{k} → {v}' for k, v in sorted(oraclemod._TABLE_MAP.items()))}")
    lines.append(
        f"- `_ORACLE_JOB_ADDITIONS` ({len(oracle_job_fragments())} columns), "
        f"`_ORACLE_REQUIRED_JOB_COLUMNS` ({len(oraclemod._ORACLE_REQUIRED_JOB_COLUMNS)} columns)"
    )
    lines.append("- inline `capacity_ledger` / `capacity_usage` / `hive_job_result_history` CREATEs")
    lines.append("")
    lines.append(END)
    return "\n".join(lines) + "\n"


def assemble(pre: str, generated: str, post: str) -> str:
    """Join the regions with exactly two blank lines before findings."""
    post = post.strip("\n")
    return pre + generated + ("\n\n" + post + "\n" if post else "")


def main() -> None:
    doc = ROOT / "docs" / "SCHEMA.md"
    text = doc.read_text() if doc.exists() else "# Schema\n\n" + BEGIN + "\n" + END + "\n"
    if BEGIN not in text or END not in text:
        text = "# Schema\n\n" + BEGIN + "\n" + END + "\n"
    pre, _, rest = text.partition(BEGIN)
    _, _, post = rest.partition(END)
    doc.write_text(assemble(pre, emit(), post))
    print(f"wrote {doc}")


if __name__ == "__main__":
    main()
