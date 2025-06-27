#!/usr/bin/env -S python3 -u

import json

import turso
from antithesis.random import get_random
from utils import generate_random_value

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

# insert up to 100 rows in the selected table
insertions = get_random() % 100
print(f"Inserting {insertions} rows...")

for i in range(insertions):
    values = [generate_random_value(tbl_schema[f"col_{col}"]["data_type"]) for col in range(tbl_schema["colCount"])]
    try:
        cur.execute(f"""
            INSERT INTO tbl_{selected_tbl} ({cols})
            VALUES ({", ".join(values)})
        """)
    except turso.OperationalError as e:
        if "UNIQUE constraint failed" in str(e):
            # Ignore UNIQUE constraint violations
            pass
        else:
            # Re-raise other operational errors
            raise

print("Rolling back...")
con.rollback()
