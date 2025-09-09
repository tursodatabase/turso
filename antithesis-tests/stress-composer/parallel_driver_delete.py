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

# Get all existing tables from schemas
existing_schemas = cur_init.execute("SELECT tbl, schema FROM schemas").fetchall()
if not existing_schemas:
    print("No tables found in schemas")
    exit(0)

# Select a random table
selected_idx = get_random() % len(existing_schemas)
selected_tbl, schema_json = existing_schemas[selected_idx]
tbl_schema = json.loads(schema_json)

# get primary key column
pk = tbl_schema["pk"]
# get non-pk columns
cols = [f"col_{col}" for col in range(tbl_schema["colCount"]) if col != pk]

try:
    con = turso.connect("stress_composer.db")
except Exception as e:
    print(f"Failed to open stress_composer.db. Exiting... {e}")
    exit(0)
cur = con.cursor()

deletions = get_random() % 100
print(f"Attempt to delete {deletions} rows in tbl_{selected_tbl}...")

for i in range(deletions):
    where_clause = f"col_{pk} = {generate_random_value(tbl_schema[f'col_{pk}']['data_type'])}"

    try:
        cur.execute(f"""
            DELETE FROM tbl_{selected_tbl} WHERE {where_clause}
        """)
    except turso.OperationalError:
        con.rollback()
        # Re-raise other operational errors
        raise

con.commit()
