#!/usr/bin/env -S python3 -u

import json

import turso
from antithesis.assertions import always
from antithesis.random import get_random

# Get initial state
try:
    con_init = turso.connect("init_state.db")
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(0)

cur_init = con_init.cursor()

# Get all existing tables from schemas
existing_schemas = cur_init.execute("SELECT tbl, schema FROM schemas").fetchall()
if not existing_schemas:
    print("No tables found in schemas")
    exit(0)

# Select a random table
selected_idx = get_random() % len(existing_schemas)
selected_tbl, schema_json = existing_schemas[selected_idx]
tbl_schema = json.loads(schema_json)

tbl_name = f"tbl_{selected_tbl}"

try:
    con = turso.connect("stress_composer.db")
except Exception as e:
    print(f"Failed to open stress_composer.db. Exiting... {e}")
    exit(0)

cur = con.cursor()

try:
    cur.execute("SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = '" + tbl_name + "'")

    result = cur.fetchone()

    if result is None:
        print(f"Table {tbl_name} not found")
        exit(0)
    else:
        schema_before = result[0]

    cur.execute("BEGIN TRANSACTION")

    cur.execute("ALTER TABLE " + tbl_name + " RENAME TO " + tbl_name + "_old")

    con.rollback()

    cur = con.cursor()
    cur.execute("SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = '" + tbl_name + "'")

    result_after = cur.fetchone()
    if result_after is None:
        print(f"Table {tbl_name} dropped in parallel after rollback")
        exit(0)

    schema_after = result_after[0]

    always(schema_before == schema_after, "schema should be the same after rollback", {})
except turso.ProgrammingError as e:
    print(f"Table {tbl_name} dropped in parallel: {e}")
    con.rollback()
    exit(0)
