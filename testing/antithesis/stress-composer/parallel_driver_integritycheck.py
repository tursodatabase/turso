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
