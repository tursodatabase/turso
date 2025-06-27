#!/usr/bin/env -S python3 -u

import glob
import json
import os

import turso
from antithesis.random import get_random, random_choice

constraints = ["NOT NULL", ""]
data_type = ["INTEGER", "REAL", "TEXT", "BLOB", "NUMERIC"]

# remove any existing db files
for f in glob.glob("*.db"):
    try:
        os.remove(f)
    except OSError:
        pass

for f in glob.glob("*.db-wal"):
    try:
        os.remove(f)
    except OSError:
        pass

# store initial states in a separate db
try:
    con_init = turso.connect("init_state.db")
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(0)

cur_init = con_init.cursor()
cur_init.execute("CREATE TABLE schemas (schema TEXT, tbl INT)")
cur_init.execute("CREATE TABLE tables (count INT)")

try:
    con = turso.connect("stress_composer.db")
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(0)

cur = con.cursor()

tbl_count = max(1, get_random() % 10)

cur_init.execute(f"INSERT INTO tables (count) VALUES ({tbl_count})")

schemas = []
for i in range(tbl_count):
    col_count = max(1, get_random() % 10)
    pk = get_random() % col_count

    schema = {"table": i, "colCount": col_count, "pk": pk}

    cols = []
    cols_str = ""
    for j in range(col_count):
        col_data_type = random_choice(data_type)
        col_constraint_1 = random_choice(constraints)
        col_constraint_2 = random_choice(constraints)

        col = (
            f"col_{j} {col_data_type} {col_constraint_1} {col_constraint_2 if col_constraint_2 != col_constraint_1 else ''}"  # noqa: E501
            if j != pk
            else f"col_{j} {col_data_type}"
        )

        cols.append(col)

        schema[f"col_{j}"] = {
            "data_type": col_data_type,
            "constraint1": col_constraint_1 if j != pk else "",
            "constraint2": col_constraint_2 if col_constraint_1 != col_constraint_2 else "" if j != pk else "NOT NULL",
        }

        cols_str = ", ".join(cols)

    schemas.append(schema)
    cur_init.execute(f"INSERT INTO schemas (schema, tbl) VALUES ('{json.dumps(schema)}', {i})")

    cur.execute(f"""
        CREATE TABLE tbl_{i} ({cols_str})
    """)

print(f"DB Schemas\n------------\n{json.dumps(schemas, indent=2)}")
