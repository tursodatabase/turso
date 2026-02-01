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

# get primary key column
pk = tbl_schema["pk"]
# get non-pk columns
cols = [f"col_{col}" for col in range(tbl_schema["colCount"]) if col != pk]
# print(cols)
try:
    con = turso.connect("stress_composer.db")
except Exception as e:
    print(f"Failed to open stress_composer.db. Exiting... {e}")
    exit(0)
cur = con.cursor()

# insert up to 100 rows in the selected table
updates = get_random() % 100
print(f"Attempt to update {updates} rows in tbl_{selected_tbl}...")

for i in range(updates):
    set_clause = ""
    if tbl_schema["colCount"] == 1:
        set_clause = f"col_{pk} = {generate_random_value(tbl_schema[f'col_{pk}']['data_type'])}"
    else:
        values = []
        for col in cols:
            # print(col)
            values.append(f"{col} = {generate_random_value(tbl_schema[col]['data_type'])}")
        set_clause = ", ".join(values)

    where_clause = f"col_{pk} = {generate_random_value(tbl_schema[f'col_{pk}']['data_type'])}"
    # print(where_clause)

    try:
        cur.execute(f"""
            UPDATE tbl_{selected_tbl} SET {set_clause} WHERE {where_clause}
        """)
    except turso.ProgrammingError:
        # Table/column might have been dropped in parallel - this is expected
        con.rollback()
        break
    except turso.IntegrityError:
        # Ignore constraint violations - can happen with random values
        pass

con.commit()
