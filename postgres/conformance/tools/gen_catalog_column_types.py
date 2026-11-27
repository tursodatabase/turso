#!/usr/bin/env python3
"""Write postgres/frontend/catalog_column_types.rs from PostgreSQL 18.

It lists, for each catalog table of the frontend, the columns whose declared
type stands for the type PostgreSQL gives the column: INTEGER for oid,
integer, smallint and bigint; BIGINT for bigint; TEXT for text, name and
"char"; BOOLEAN for boolean; REAL for real and double precision; TEXT[] for
text[] and name[]. The declared types come from the CREATE TABLE statements
in postgres/frontend/catalog.rs, the PostgreSQL types from a PostgreSQL 18
server.

    gen_catalog_column_types.py --port 54329           # print the file
    gen_catalog_column_types.py --port 54329 --write   # replace the file
"""

import argparse
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
FRONTEND = os.path.join(HERE, "..", "..", "frontend")
CATALOG_RS = os.path.join(FRONTEND, "catalog.rs")
OUTPUT = os.path.join(FRONTEND, "catalog_column_types.rs")

STANDS_FOR = {
    "INTEGER": {"oid", "integer", "smallint", "bigint"},
    "BIGINT": {"bigint"},
    "TEXT": {"text", "name", '"char"'},
    "BOOLEAN": {"boolean"},
    "REAL": {"real", "double precision"},
    "TEXT[]": {"text[]", "name[]"},
}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--port", default="5432")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--user", default="postgres")
    ap.add_argument("--psql", default="psql")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    postgres = postgres_column_types(args)
    tables = {}
    for table, body in re.findall(r'CREATE TABLE (pg_\w+)\s*\((.*?)\)"', open(CATALOG_RS).read(), re.S):
        for column in body.split(","):
            parts = column.split()
            if len(parts) < 2:
                continue
            name, declared = parts[0], parts[1].upper()
            if postgres.get((table, name)) in STANDS_FOR.get(declared, set()):
                tables.setdefault(table, []).append(name)

    lines = [
        "//! The columns of the catalog tables whose declared type stands for the type",
        "//! PostgreSQL gives them. postgres/conformance/tools/gen_catalog_column_types.py",
        "//! writes this file.",
        "",
        "/// FIXME(registered-types): The catalog tables declare SQLite types, so only these columns",
        "/// are typed by their declared type. Catalog columns of registered types replace this list.",
        "#[rustfmt::skip]",
        "pub(crate) const COLUMNS_OF_THEIR_POSTGRES_TYPE: &[(&str, &[&str])] = &[",
    ]
    for table in sorted(tables):
        columns = ", ".join(f'"{name}"' for name in tables[table])
        lines.append(f'    ("{table}", &[{columns}]),')
    lines.append("];")
    lines += [
        "",
        "/// Whether `column` of the catalog table that `create_sql` creates holds",
        "/// values of the type PostgreSQL gives the column.",
        "pub(crate) fn has_postgres_type(create_sql: &str, column: &str) -> bool {",
        "    let table = create_sql",
        "        .trim_start()",
        '        .trim_start_matches("CREATE TABLE")',
        "        .split(|c: char| c == '(' || c.is_whitespace())",
        "        .find(|word| !word.is_empty());",
        "    COLUMNS_OF_THEIR_POSTGRES_TYPE",
        "        .iter()",
        "        .find(|(name, _)| Some(*name) == table)",
        "        .is_some_and(|(_, columns)| columns.contains(&column))",
        "}",
    ]
    text = "\n".join(lines) + "\n"
    if args.write:
        open(OUTPUT, "w").write(text)
    else:
        sys.stdout.write(text)


def postgres_column_types(args):
    sql = """SELECT c.relname, a.attname, format_type(a.atttypid, NULL)
             FROM pg_attribute a
             JOIN pg_class c ON c.oid = a.attrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'pg_catalog' AND a.attnum > 0 AND NOT a.attisdropped"""
    cmd = [args.psql, "-h", args.host, "-p", str(args.port), "-U", args.user, "-At",
           "-F", "\t", "-c", sql]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(result.stderr)
    version = subprocess.run(cmd[:-1] + ["SHOW server_version"], capture_output=True, text=True)
    if not version.stdout.startswith("18."):
        sys.exit(f"need PostgreSQL 18, got {version.stdout.strip()}")
    types = {}
    for line in result.stdout.splitlines():
        table, column, ty = line.split("\t")
        types[(table, column)] = ty
    return types


if __name__ == "__main__":
    main()
