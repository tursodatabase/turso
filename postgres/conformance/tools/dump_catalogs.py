#!/usr/bin/env python3
"""Dump the PostgreSQL 18 catalogs the resolver reads into JSON fixtures.

Writes pg_cast.json, pg_type.json, pg_range.json, pg_proc.json and
pg_operator.json into postgres/frontend/resolve/testdata/ (or --out). Only pg_catalog objects are
dumped. Re-run against a PostgreSQL 18 server:

    python3 postgres/conformance/tools/dump_catalogs.py \
        --host 127.0.0.1 --port 54341 --user postgres [--psql /path/to/psql]

Each file is a JSON array with one row per line.
"""

import argparse
import json
import os
import subprocess
import sys

QUERIES = {
    "pg_cast.json": """
        SELECT oid::int AS oid, castsource::int AS castsource, casttarget::int AS casttarget,
               castfunc::int AS castfunc, castcontext::text AS castcontext,
               castmethod::text AS castmethod
        FROM pg_cast ORDER BY oid""",
    "pg_type.json": """
        SELECT oid::int AS oid, typname::text AS typname, typcategory::text AS typcategory,
               typispreferred, typtype::text AS typtype, typelem::int AS typelem,
               typarray::int AS typarray, typbasetype::int AS typbasetype,
               typsubscript::text AS typsubscript
        FROM pg_type WHERE typnamespace = 'pg_catalog'::regnamespace ORDER BY oid""",
    "pg_range.json": """
        SELECT rngtypid::int AS rngtypid, rngsubtype::int AS rngsubtype,
               rngmultitypid::int AS rngmultitypid
        FROM pg_range ORDER BY rngtypid""",
    "pg_proc.json": """
        SELECT oid::int AS oid, proname::text AS proname,
               proargtypes::oid[]::int[] AS proargtypes, prorettype::int AS prorettype,
               proisstrict, provariadic::int AS provariadic, pronargdefaults::int AS pronargdefaults,
               prokind::text AS prokind, prosrc
        FROM pg_proc
        WHERE pronamespace = 'pg_catalog'::regnamespace AND prokind IN ('f', 'a')
        ORDER BY oid""",
    "pg_operator.json": """
        SELECT oid::int AS oid, oprname::text AS oprname, oprkind::text AS oprkind,
               oprleft::int AS oprleft, oprright::int AS oprright, oprresult::int AS oprresult,
               oprcode::oid::int AS oprcode
        FROM pg_operator WHERE oprnamespace = 'pg_catalog'::regnamespace ORDER BY oid""",
}


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    default_out = os.path.join(here, "..", "..", "frontend", "resolve", "testdata")
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", default="5432")
    ap.add_argument("--user", default="postgres")
    ap.add_argument("--dbname", default="postgres")
    ap.add_argument("--psql", default="psql")
    ap.add_argument("--out", default=default_out)
    args = ap.parse_args()

    version = psql(args, "SHOW server_version").strip()
    if not version.startswith("18."):
        sys.exit(f"expected PostgreSQL 18, got {version}")
    os.makedirs(args.out, exist_ok=True)
    for name, query in QUERIES.items():
        rows = json.loads(psql(args, f"SELECT coalesce(json_agg(t), '[]') FROM ({query}) t"))
        with open(os.path.join(args.out, name), "w") as f:
            f.write("[\n" + ",\n".join(json.dumps(r, separators=(",", ":")) for r in rows) + "\n]\n")
        print(f"{name}: {len(rows)} rows (server {version})")


def psql(args, sql):
    cmd = [args.psql, "-h", args.host, "-p", str(args.port), "-U", args.user, "-d", args.dbname,
           "-At", "-q", "-X", "-v", "ON_ERROR_STOP=1", "-c", sql]
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout


if __name__ == "__main__":
    main()
