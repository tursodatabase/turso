#!/usr/bin/env -S python3 -u


import turso
from antithesis.random import get_random

# Get initial state
try:
    con_init = turso.connect("init_state.db")
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(0)

cur_init = con_init.cursor()

# Get all existing tables from schemas
existing_schemas = cur_init.execute("SELECT tbl FROM schemas").fetchall()
if not existing_schemas:
    print("No tables found in schemas")
    exit(0)

# Select a random table
selected_idx = get_random() % len(existing_schemas)
selected_tbl = existing_schemas[selected_idx][0]

try:
    con = turso.connect("stress_composer.db")
except Exception as e:
    print(f"Failed to open stress_composer.db. Exiting... {e}")
    exit(0)

cur = con.cursor()

try:
    cur.execute(f"DROP TABLE tbl_{selected_tbl}")
    con.commit()
    print(f"Successfully dropped table tbl_{selected_tbl}")
except turso.ProgrammingError as e:
    # Table might have been dropped in parallel - this is expected
    print(f"Table tbl_{selected_tbl} already dropped in parallel: {e}")
    con.rollback()

con.close()

cur_init.execute("DELETE FROM schemas WHERE tbl = ?", (selected_tbl,))

con_init.commit()

con_init.close()
