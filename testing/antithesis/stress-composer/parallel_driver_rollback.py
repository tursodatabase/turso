#!/usr/bin/env -S python3 -u

import json

import turso
from antithesis.random import get_random
from helper_utils import generate_random_value

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
    except turso.ProgrammingError:
        # Table/column might have been dropped in parallel - this is expected
        con.rollback()
        break
    except turso.IntegrityError:
        # Ignore constraint violations - can happen with random values
        pass

print("Rolling back...")
con.rollback()
