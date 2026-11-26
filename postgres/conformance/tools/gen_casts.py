#!/usr/bin/env python3
"""Write the PG_CASTS table of postgres/frontend/casts.rs from PostgreSQL 18.

The rows come from postgres/frontend/resolve/testdata/pg_cast.json. A row is
kept when its source and its target are both types of the frontend's pg_type.

    gen_casts.py            # print the table
    gen_casts.py --write    # replace it in casts.rs
    gen_casts.py --sqltest  # print the catalog.sqltest test
"""

import argparse
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
FRONTEND = os.path.join(HERE, "..", "..", "frontend")
FIXTURES = os.path.join(FRONTEND, "resolve", "testdata")
CASTS_RS = os.path.join(FRONTEND, "casts.rs")

TYPES_OF_FRONTEND = {
    16, 17, 20, 21, 23, 25, 26, 114, 650, 700, 701, 829, 869, 1043, 1082, 1114, 1184, 1700,
    2950, 3802,
}
TYPES_WITHOUT_ARRAY = {1082, 1114, 1184}
CONTEXTS = {"i": "Implicit", "a": "Assignment", "e": "Explicit"}
METHODS = {"f": "Function", "i": "InputOutput", "b": "Binary"}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--sqltest", action="store_true")
    args = ap.parse_args()

    types = {row["oid"]: row for row in read_rows("pg_type.json")}
    array_types = {
        types[oid]["typarray"] for oid in TYPES_OF_FRONTEND - TYPES_WITHOUT_ARRAY
    }
    frontend_types = TYPES_OF_FRONTEND | array_types
    casts = [
        row
        for row in read_rows("pg_cast.json")
        if row["castsource"] in frontend_types and row["casttarget"] in frontend_types
    ]
    casts.sort(key=lambda row: row["oid"])

    if args.sqltest:
        print_sqltest(casts, types, frontend_types)
        return

    table = "".join(render(row) for row in casts)
    if not args.write:
        sys.stdout.write(table)
        return
    source = open(CASTS_RS).read()
    pattern = re.compile(
        r"(pub\(crate\) const PG_CASTS: &\[PgCast\] = pg_casts!\[\n).*?(^\];)", re.S | re.M
    )
    if not pattern.search(source):
        sys.exit("cannot find the PG_CASTS table in casts.rs")
    open(CASTS_RS, "w").write(pattern.sub(lambda m: m.group(1) + table + m.group(2), source))


def read_rows(name):
    with open(os.path.join(FIXTURES, name)) as f:
        return [json.loads(line.rstrip(",\n")) for line in f if line.startswith("{")]


def render(row):
    return (
        f'    ({row["oid"]}, {row["castsource"]}, {row["casttarget"]}, {row["castfunc"]}, '
        f'{CONTEXTS[row["castcontext"]]}, {METHODS[row["castmethod"]]}),\n'
    )


def print_sqltest(casts, types, frontend_types):
    names = sorted(types[oid]["typname"] for oid in frontend_types)
    quoted = ", ".join(f"'{name}'" for name in names)
    print("test pg-cast-has-the-rows-of-postgresql-between-the-types-of-pg-type {")
    print("    SELECT c.oid, s.typname, t.typname, c.castfunc, c.castcontext, c.castmethod")
    print("    FROM pg_cast c")
    print("    JOIN pg_type s ON s.oid = c.castsource")
    print("    JOIN pg_type t ON t.oid = c.casttarget")
    for column, keyword in (("s", "WHERE"), ("t", "  AND")):
        print(f"    {keyword} {column}.typname IN (")
        for line in wrap(quoted, 88):
            print("        " + line)
        print("    )")
    print("    ORDER BY c.oid;")
    print("}")
    print("expect {")
    for row in casts:
        cells = [
            row["oid"], types[row["castsource"]]["typname"], types[row["casttarget"]]["typname"],
            row["castfunc"], row["castcontext"], row["castmethod"],
        ]
        print("    " + "|".join(map(str, cells)))
    print("}")


def wrap(text, width):
    lines, line = [], ""
    for part in text.split(", "):
        piece = part + ","
        if len(line) + len(piece) + 1 > width and line:
            lines.append(line)
            line = ""
        line = (line + " " + piece).strip()
    lines.append(line.rstrip(","))
    return lines


if __name__ == "__main__":
    main()
