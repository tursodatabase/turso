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
cols = ", ".join([f"col_{col}" for col in range(tbl_schema["colCount"])])

try:
    con = turso.connect("stress_composer.db")
except Exception as e:
    print(f"Failed to open stress_composer.db. Exiting... {e}")
    exit(0)
cur = con.cursor()

print("Running integrity check...")

result = cur.execute("PRAGMA integrity_check")
row = result.fetchone()
always(row == ("ok",), f"Integrity check failed: {row}", {})
