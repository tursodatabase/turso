#!/usr/bin/env -S python3 -u

import json
import string

import turso
from antithesis.random import get_random, random_choice

# Get initial state
try:
    con_init = turso.connect("init_state.db")
except Exception as e:
    print(f"Error connecting to database: {e}")
    exit(0)

cur_init = con_init.cursor()


# Connect to the main database
try:
    con = turso.connect("stress_composer.db")
except Exception as e:
    print(f"Failed to open stress_composer.db. Exiting... {e}")
    exit(0)

cur = con.cursor()

# Find the next available table number
existing_tables = cur.execute("""
    SELECT name FROM sqlite_master
    WHERE type = 'table' AND name LIKE 'tbl_%'
""").fetchall()

# Extract table numbers
table_numbers = set()
for (name,) in existing_tables:
    if name.startswith("tbl_"):
        try:
            table_numbers.add(int(name[4:]))
        except ValueError:
            pass

# Find next available table number
next_table_num = 0
while next_table_num in table_numbers:
    next_table_num += 1

print(f"Creating new table: tbl_{next_table_num}")

# Define possible data types and constraints
data_types = ["INTEGER", "REAL", "TEXT", "BLOB", "NUMERIC"]
constraints = ["", "NOT NULL", "DEFAULT 0", "DEFAULT ''", "UNIQUE"]

# Generate random number of columns (2-10)
col_count = 2 + (get_random() % 9)

# Select primary key column
pk = get_random() % col_count

# Build schema
schema = {"table": next_table_num, "colCount": col_count, "pk": pk}
cols = []

for j in range(col_count):
    col_data_type = random_choice(data_types)
    col_constraint_1 = random_choice(constraints)
    col_constraint_2 = random_choice(constraints)

    # Primary key handling
    if j == pk:
        col_def = f"col_{j} {col_data_type} PRIMARY KEY"
        schema[f"col_{j}"] = {
            "data_type": col_data_type,
            "constraint1": "",
            "constraint2": "NOT NULL",
        }
    else:
        # Ensure constraints are different if both are selected
        if col_constraint_2 == col_constraint_1:
            col_constraint_2 = ""

        col_def = f"col_{j} {col_data_type}"
        if col_constraint_1:
            col_def += f" {col_constraint_1}"
        if col_constraint_2:
            col_def += f" {col_constraint_2}"

        schema[f"col_{j}"] = {
            "data_type": col_data_type,
            "constraint1": col_constraint_1,
            "constraint2": col_constraint_2,
        }

    cols.append(col_def)

cols_str = ", ".join(cols)

try:
    # Create the table
    create_stmt = f"CREATE TABLE tbl_{next_table_num} ({cols_str})"
    print(f"Creating table with {col_count} columns")
    print(f"Schema: {create_stmt}")
    cur.execute(create_stmt)


    # Store schema information
    cur_init.execute(f"""
        INSERT INTO schemas (schema, tbl)
        VALUES ('{json.dumps(schema)}', {next_table_num})
    """)

    con_init.commit()
    con.commit()

    print(f"Successfully created table tbl_{next_table_num}")

    # Optionally create some initial indexes (20% chance per non-PK column)
    indexes_created = 0
    for j in range(col_count):
        if j != pk and get_random() % 5 == 0:  # 20% chance
            col_name = f"col_{j}"
            col_info = schema[col_name]
            index_suffix = "".join(random_choice(string.ascii_lowercase) for _ in range(4))

            if col_info["data_type"] in ["INTEGER", "REAL", "NUMERIC"]:
                idx_name = f"idx_tbl{next_table_num}_col{j}_{index_suffix}"
            else:
                idx_name = f"idx_tbl{next_table_num}_col{j}_text_{index_suffix}"

            try:
                cur.execute(f"CREATE INDEX {idx_name} ON tbl_{next_table_num} ({col_name})")
                cur_init.execute(f"""
                    INSERT INTO indexes (idx_name, tbl_name, idx_type, cols)
                    VALUES ('{idx_name}', 'tbl_{next_table_num}', 'single', '{col_name}')
                """)
                indexes_created += 1
                print(f"Created index: {idx_name}")
            except Exception as e:
                print(f"Failed to create index {idx_name}: {e}")

    if indexes_created > 0:
        con_init.commit()
        con.commit()
        print(f"Created {indexes_created} initial indexes")

except turso.OperationalError as e:
    print(f"Failed to create table: {e}")
    con.rollback()
    con_init.rollback()

con.close()
con_init.close()
