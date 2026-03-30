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

# Get all existing tables from schemas
existing_schemas = cur_init.execute(
    "SELECT tbl, schema FROM schemas").fetchall()
if not existing_schemas:
    print("No tables available for index creation")
    exit(0)

# Select a random table
selected_idx = get_random() % len(existing_schemas)
selected_tbl, schema_json = existing_schemas[selected_idx]
tbl_schema = json.loads(schema_json)

# Connect to the main database
try:
    con = turso.connect("stress_composer.db")
except Exception as e:
    print(f"Failed to open stress_composer.db. Exiting... {e}")
    exit(0)

cur = con.cursor()

# Check existing indexes on this table
existing_indexes = cur.execute(f"""
    SELECT name FROM sqlite_master
    WHERE type = 'index' AND tbl_name = 'tbl_{selected_tbl}'
    AND sql IS NOT NULL
""").fetchall()
existing_index_names = {idx[0] for idx in existing_indexes}

print(
    f"Selected table: tbl_{selected_tbl} with {tbl_schema['colCount']} columns")
print(f"Existing indexes: {len(existing_indexes)}")

# Decide whether to create a single-column or composite index
create_composite = tbl_schema["colCount"] > 2 and get_random(
) % 3 == 0  # 33% chance for composite

if create_composite:
    # Create composite index
    num_cols = 2 + (get_random() %
                    min(2, tbl_schema["colCount"] - 1))  # 2-3 columns
    selected_cols = []
    available_cols = list(range(tbl_schema["colCount"]))

    for _ in range(min(num_cols, len(available_cols))):
        if available_cols:
            idx = get_random() % len(available_cols)
            selected_cols.append(available_cols.pop(idx))

    # Randomly decide sort order for each column (25% chance of DESC)
    col_specs = []
    for col in sorted(selected_cols):
        if get_random() % 4 == 0:  # 25% chance
            col_specs.append(f"col_{col} DESC")
        else:
            col_specs.append(f"col_{col}")

    cols_suffix = "_".join(str(c) for c in sorted(selected_cols))
    random_suffix = "".join(random_choice(string.ascii_lowercase) for _ in range(4))
    index_name = f"idx_tbl{selected_tbl}_{cols_suffix}_{random_suffix}"

    # Check if this combination already has an index
    if index_name not in existing_index_names:
        try:
            create_stmt = f"CREATE INDEX {index_name} ON tbl_{selected_tbl} ({', '.join(col_specs)})"
            print(f"Creating composite index: {create_stmt}")
            cur.execute(create_stmt)

            # Store index information in init_state.db
            cur_init.execute(f"""
                INSERT INTO indexes (idx_name, tbl_name, idx_type, cols)
                VALUES ('{index_name}', 'tbl_{selected_tbl}', 'composite', '{", ".join(col_specs)}')
            """)
            con_init.commit()
            print(f"Successfully created composite index: {index_name}")
        except turso.ProgrammingError as e:
            print(f"Table/column might have been dropped in parallel: {e}")
            con.rollback()
        except turso.OperationalError as e:
            print(f"Failed to create composite index: {e}")
            con.rollback()
    else:
        print(f"Index {index_name} already exists, skipping")
else:
    # Create single-column index
    # Select a random column (avoiding primary key which is usually col_0)
    available_cols = list(range(1, tbl_schema["colCount"]))
    if not available_cols:
        available_cols = [0]  # Fall back to first column if only one exists

    selected_col = available_cols[get_random() % len(available_cols)]
    col_name = f"col_{selected_col}"

    # Randomly decide sort order (25% chance of DESC)
    col_spec = col_name
    if get_random() % 4 == 0:  # 25% chance
        col_spec = f"{col_name} DESC"

    # Determine index type based on column data type
    col_type = tbl_schema[col_name]["data_type"]
    index_suffix = "".join(random_choice(string.ascii_lowercase)
                           for _ in range(4))

    if col_type == "TEXT" and tbl_schema[col_name].get("unique", False):
        # Create unique index for unique text columns
        index_name = f"idx_tbl{selected_tbl}_col{selected_col}_unique_{index_suffix}"
        create_stmt = f"CREATE UNIQUE INDEX {index_name} ON tbl_{selected_tbl} ({col_spec})"
    else:
        # Create regular index
        index_name = f"idx_tbl{selected_tbl}_col{selected_col}_{index_suffix}"
        create_stmt = f"CREATE INDEX {index_name} ON tbl_{selected_tbl} ({col_spec})"

    if index_name not in existing_index_names:
        try:
            print(f"Creating single-column index: {create_stmt}")
            cur.execute(create_stmt)

            # Store index information in init_state.db
            idx_type = "unique" if "UNIQUE" in create_stmt else "single"
            cur_init.execute(f"""
                INSERT INTO indexes (idx_name, tbl_name, idx_type, cols)
                VALUES ('{index_name}', 'tbl_{selected_tbl}', '{idx_type}', '{col_spec}')
            """)
            con_init.commit()
            print(f"Successfully created {idx_type} index: {index_name}")
        except turso.ProgrammingError as e:
            print(f"Table/column might have been dropped in parallel: {e}")
            con.rollback()
        except turso.OperationalError as e:
            print(f"Failed to create index: {e}")
            con.rollback()
    else:
        print(f"Index {index_name} already exists, skipping")

con.commit()
con.close()
con_init.close()
