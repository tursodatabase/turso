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
cur_init.execute("CREATE TABLE indexes (idx_name TEXT, tbl_name TEXT, idx_type TEXT, cols TEXT)")

try:
    con = turso.connect("stress_composer.db")
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(0)

cur = con.cursor()

tbl_count = max(1, get_random() % 10)

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

    # Create indexes for this table
    indexes_created = []

    # Create single-column indexes for non-PK columns
    for j in range(col_count):
        if j != pk and get_random() % 3 == 0:  # Create index with 33% probability
            col_name = f"col_{j}"
            col_info = schema[col_name]

            # Choose index type based on column properties
            if col_info["data_type"] in ["INTEGER", "REAL", "NUMERIC"]:
                idx_name = f"idx_tbl{i}_col{j}_numeric"
                cur.execute(f"CREATE INDEX {idx_name} ON tbl_{i} ({col_name})")
                indexes_created.append({"name": idx_name, "type": "single", "cols": col_name})
            elif col_info["data_type"] == "TEXT" and "NOT NULL" in col_info.get("constraint1", "") + col_info.get(
                "constraint2", ""
            ):
                idx_name = f"idx_tbl{i}_col{j}_text"
                cur.execute(f"CREATE INDEX {idx_name} ON tbl_{i} ({col_name})")
                indexes_created.append({"name": idx_name, "type": "single", "cols": col_name})

    # Create a composite index if table has multiple columns
    if col_count > 2 and get_random() % 2 == 0:  # 50% chance for composite index
        # Select 2-3 columns for composite index
        num_cols = min(3, col_count)
        selected_cols = []
        available_cols = [j for j in range(col_count) if j != pk]

        if len(available_cols) >= 2:
            for _ in range(min(num_cols, len(available_cols))):
                if available_cols:
                    idx = get_random() % len(available_cols)
                    selected_cols.append(available_cols.pop(idx))

            if len(selected_cols) >= 2:
                col_names = [f"col_{j}" for j in selected_cols]
                idx_name = f"idx_tbl{i}_composite"
                cur.execute(f"CREATE INDEX {idx_name} ON tbl_{i} ({', '.join(col_names)})")
                indexes_created.append({"name": idx_name, "type": "composite", "cols": ", ".join(col_names)})

    # Store index information
    for idx in indexes_created:
        values = f"'{idx['name']}', 'tbl_{i}', '{idx['type']}', '{idx['cols']}'"
        cur_init.execute(
            f"INSERT INTO indexes (idx_name, tbl_name, idx_type, cols) VALUES ({values})"
        )

con_init.commit()

con.commit()

# Retrieve and display created indexes
cur_init.execute("SELECT * FROM indexes")
indexes = cur_init.fetchall()

print(f"DB Schemas\n------------\n{json.dumps(schemas, indent=2)}")
print("\nIndexes Created\n------------")
for idx in indexes:
    print(f"Index: {idx[0]} on {idx[1]} (type: {idx[2]}, columns: {idx[3]})")
