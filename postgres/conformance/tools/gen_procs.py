#!/usr/bin/env python3
"""Write the PROCS table of postgres/frontend/procs.rs from PostgreSQL 18.

The rows come from testdata/pg_proc.json (oid, name, argument types, result,
prosrc); the flags that file does not have (leakproof, volatility, parallel,
cost, rows, set-returning) come from a PostgreSQL 18 server. A row is kept when
every argument type is a type of the frontend's pg_type, the function is not
variadic, has no default arguments and is written in the internal language.

    gen_procs.py --through bytea --port 54361            # print the table
    gen_procs.py --through bytea --port 54361 --write    # replace it in procs.rs
    gen_procs.py --through bytea --port 54361 --sqltest bytea   # print the test
"""

import argparse
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
FRONTEND = os.path.join(HERE, "..", "..", "frontend")
FIXTURE = os.path.join(FRONTEND, "resolve", "testdata", "pg_proc.json")
PROCS_RS = os.path.join(FRONTEND, "procs.rs")

TYPES_OF_FRONTEND = {
    16, 17, 20, 21, 23, 25, 26, 114, 650, 700, 701, 829, 869, 1043, 1082, 1083, 1114,
    1184, 1700, 2950, 3802,
}
BYTEA = 17

SEED_OIDS = [
    1081, 1282, 1299, 1387, 1394, 1395, 1396, 1397, 1398, 1705, 2100, 2101, 2102, 2103,
    2104, 2105, 2107, 2108, 2109, 2110, 2111, 2114, 2115, 2116, 2117, 2118, 2119, 2120,
    2122, 2126, 2127, 2129, 2130, 2131, 2132, 2133, 2134, 2135, 2136, 2138, 2142, 2143,
    2145, 2146, 2147, 2154, 2155, 2156, 2157, 2158, 2159, 2508, 2803, 3538, 3545, 3810,
    6395, 6396,
]

# Each family: names, and whether its rows are those with bytea arguments
# ("only"), those without ("without"), or all ("all").
FAMILIES = [
    ("seed", {"oids": SEED_OIDS}),
    ("math", {"names": """abs ceil ceiling degrees exp floor ln log mod pi power radians random
        round sign sqrt trunc""".split(), "bytea": "all"}),
    ("string", {"names": """btrim char_length character_length chr length lower lpad ltrim
        octet_length position quote_ident quote_literal repeat replace reverse rpad rtrim
        strpos substr substring upper""".split(), "bytea": "without"}),
]


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--through", required=True, choices=[f for f, _ in FAMILIES])
    ap.add_argument("--port", default="5432")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--user", default="postgres")
    ap.add_argument("--psql", default="psql")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--sqltest", choices=[f for f, _ in FAMILIES])
    ap.add_argument("--left-out", action="store_true", help="list the overloads left out")
    args = ap.parse_args()

    fixture = {row["oid"]: row for row in map(json.loads, read_rows())}
    flags = server_flags(args)
    chosen, left_out = choose_rows(fixture, flags, args.through)

    if args.left_out:
        for family, row in left_out:
            print(family, describe(row))
        return

    if args.sqltest:
        print_sqltest(args.sqltest, chosen, flags)
        return

    table = "".join(
        render(chosen[oid][1], flags[oid]) for oid in sorted(chosen)
    )
    if not args.write:
        sys.stdout.write(table)
        return
    source = open(PROCS_RS).read()
    pattern = re.compile(r"(pub\(crate\) const PROCS: &\[Proc\] = procs!\[\n).*?(^\];)", re.S | re.M)
    if not pattern.search(source):
        sys.exit("cannot find the PROCS table in procs.rs")
    open(PROCS_RS, "w").write(pattern.sub(lambda m: m.group(1) + table + m.group(2), source))


def choose_rows(fixture, flags, through):
    chosen = {}
    left_out = []
    for family, spec in FAMILIES:
        for row in fixture.values():
            if row["oid"] in chosen or not wanted(row, spec):
                continue
            if "oids" in spec or keep(row, flags[row["oid"]]):
                chosen[row["oid"]] = (family, row)
            else:
                left_out.append((family, row))
        if family == through:
            break
    return chosen, left_out


def read_rows():
    with open(FIXTURE) as f:
        return [line.rstrip(",\n") for line in f if line.startswith("{")]


def wanted(row, spec):
    if "oids" in spec:
        return row["oid"] in spec["oids"]
    if row["proname"] not in spec["names"] or row["prokind"] not in ("f", "a"):
        return False
    has_bytea = BYTEA in row["proargtypes"]
    return {"all": True, "only": has_bytea, "without": not has_bytea}[spec["bytea"]]


def keep(row, flag):
    return (
        flag["internal"]
        and all(t in TYPES_OF_FRONTEND for t in row["proargtypes"])
        and row["provariadic"] == 0
        and row["pronargdefaults"] == 0
    )


def describe(row):
    return f'{row["proname"]}({",".join(map(str, row["proargtypes"]))}) oid {row["oid"]}'


def psql(args, sql):
    cmd = [args.psql, "-h", args.host, "-p", str(args.port), "-U", args.user, "-At",
           "-F", "\t", "-c", sql]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(result.stderr)
    return result.stdout


def server_flags(args):
    version = psql(args, "SHOW server_version").strip()
    if not version.startswith("18."):
        sys.exit(f"need PostgreSQL 18, got {version}")
    out = psql(
        args,
        """SELECT oid::int, prolang = 12, proleakproof, proretset, provolatile::text, proparallel::text,
                  procost::float8, prorows::float8
           FROM pg_proc WHERE pronamespace = 'pg_catalog'::regnamespace""",
    )
    flags = {}
    for line in out.splitlines():
        oid, internal, leak, retset, volatility, parallel, cost, rows = line.split("\t")
        flags[int(oid)] = {
            "internal": internal == "t",
            "leakproof": leak == "t", "returns_set": retset == "t", "volatility": volatility,
            "parallel": parallel, "cost": float(cost), "rows": float(rows),
        }
    return flags


def rust_bool(value):
    return "true" if value else "false"


def render(row, flag):
    types = ", ".join(map(str, row["proargtypes"]))
    return (
        f'    ({row["oid"]}, "{row["proname"]}", "{row["prokind"]}", '
        f'{rust_bool(flag["leakproof"])}, {rust_bool(row["proisstrict"])}, '
        f'{rust_bool(flag["returns_set"])}, "{flag["volatility"]}", "{flag["parallel"]}", '
        f'{float_literal(flag["cost"])}, {float_literal(flag["rows"])}, {row["provariadic"]}, '
        f'&[{types}], {row["prorettype"]}, "{row["prosrc"]}"),\n'
    )


def float_literal(value):
    text = repr(value)
    return text if "." in text or "e" in text else text + ".0"


def print_sqltest(family, chosen, flags):
    oids = sorted(oid for oid, (f, _) in chosen.items() if f == family)
    items = ", ".join(map(str, oids))
    wrapped = wrap(items, 96)
    print(f"# The rows of pg_proc for the {family} functions match PostgreSQL 18.")
    print(f"test pg-proc-{family}-rows-match-postgresql {{")
    print("    SELECT oid, proname, prokind, proleakproof, proisstrict, proretset, provolatile,")
    print("           proparallel, procost::int, prorows::int, provariadic, proargtypes::text,")
    print("           prorettype, prosrc")
    print("    FROM pg_proc")
    print("    WHERE oid IN (")
    for line in wrapped:
        print("        " + line)
    print("    )")
    print("    ORDER BY oid;")
    print("}")
    print("expect {")
    for oid in oids:
        row = chosen[oid][1]
        flag = flags[oid]
        cells = [
            oid, row["proname"], row["prokind"], tf(flag["leakproof"]), tf(row["proisstrict"]),
            tf(flag["returns_set"]), flag["volatility"], flag["parallel"], int(flag["cost"]),
            int(flag["rows"]), row["provariadic"], " ".join(map(str, row["proargtypes"])),
            row["prorettype"], row["prosrc"],
        ]
        print("    " + "|".join(map(str, cells)))
    print("}")


def tf(value):
    return "t" if value else "f"


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
