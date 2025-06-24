#!/usr/bin/env -S python3 -u

import json

import limbo
from antithesis.random import get_random
from utils import generate_random_value

# Get initial state
try:
    con_init = limbo.connect("init_state.db")
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(0)

cur_init = con_init.cursor()

tbl_len = cur_init.execute("SELECT count FROM tables").fetchone()[0]
selected_tbl = get_random() % tbl_len
tbl_schema = json.loads(cur_init.execute(f"SELECT schema FROM schemas WHERE tbl = {selected_tbl}").fetchone()[0])

# get primary key column
pk = tbl_schema["pk"]
# get non-pk columns
cols = [f"col_{col}" for col in range(tbl_schema["colCount"]) if col != pk]
# print(cols)
try:
    con = limbo.connect("stress_composer.db")
except limbo.OperationalError as e:
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
    except limbo.OperationalError as e:
        if "UNIQUE constraint failed" in str(e):
            # Ignore UNIQUE constraint violations
            pass
        else:
            # Re-raise other operational errors
            raise
