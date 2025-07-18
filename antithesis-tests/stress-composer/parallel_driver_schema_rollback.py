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

tbl_len = cur_init.execute("SELECT count FROM tables").fetchone()[0]
selected_tbl = get_random() % tbl_len
tbl_schema = json.loads(cur_init.execute(f"SELECT schema FROM schemas WHERE tbl = {selected_tbl}").fetchone()[0])

tbl_name = f"tbl_{selected_tbl}"

try:
    con = turso.connect("stress_composer.db")
except Exception as e:
    print(f"Failed to open stress_composer.db. Exiting... {e}")
    exit(0)

cur = con.cursor()
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

schema_after = cur.fetchone()[0]

always(schema_before == schema_after, "schema should be the same after rollback", {})
