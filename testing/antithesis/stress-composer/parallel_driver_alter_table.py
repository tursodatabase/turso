#!/usr/bin/env -S python3 -u

import json

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
existing_schemas = cur_init.execute("SELECT tbl, schema FROM schemas").fetchall()
if not existing_schemas:
    print("No tables found in schemas")
    exit(0)

# Select a random table
selected_idx = get_random() % len(existing_schemas)
selected_tbl, schema_json = existing_schemas[selected_idx]
tbl_schema = json.loads(schema_json)

print(f"Selected table: tbl_{selected_tbl} with {tbl_schema['colCount']} columns")

# Connect to the main database
try:
    con = turso.connect("stress_composer.db")
except Exception as e:
    print(f"Failed to open stress_composer.db. Exiting... {e}")
    exit(0)

cur = con.cursor()

# Define possible data types and constraints for new columns
data_types = ["INTEGER", "REAL", "TEXT", "BLOB", "NUMERIC"]
# When adding columns to existing tables, NOT NULL without DEFAULT is not allowed
constraints = ["", "DEFAULT 0", "DEFAULT ''", "DEFAULT NULL"]

# Choose ALTER TABLE operation
# Weight ADD_COLUMN more heavily (60% chance)
rand_val = get_random() % 100
if rand_val < 60:
    operation = "ADD_COLUMN"
elif rand_val < 80:
    operation = "RENAME_TABLE"
else:
    operation = "RENAME_COLUMN"

try:
    if operation == "ADD_COLUMN":
        # Add a new column
        new_col_num = tbl_schema["colCount"]
        new_col_name = f"col_{new_col_num}"

        # Choose data type and constraint
        col_data_type = random_choice(data_types)
        col_constraint = random_choice(constraints)

        alter_stmt = f"ALTER TABLE tbl_{selected_tbl} ADD COLUMN {new_col_name} {col_data_type}"
        if col_constraint:
            alter_stmt += f" {col_constraint}"

        print(f"Adding column: {alter_stmt}")
        cur.execute(alter_stmt)

        # Update schema in init_state.db
        tbl_schema["colCount"] += 1
        tbl_schema[new_col_name] = {
            "data_type": col_data_type,
            "constraint1": col_constraint,
            "constraint2": ""
        }

        cur_init.execute(
            "UPDATE schemas SET schema = ? WHERE tbl = ?",
            (json.dumps(tbl_schema), selected_tbl)
        )
        print(f"Successfully added column {new_col_name} to tbl_{selected_tbl}")

    elif operation == "RENAME_TABLE":
        new_table = len(existing_schemas) + 1

        alter_stmt = f"ALTER TABLE tbl_{selected_tbl} RENAME TO tbl_{new_table}"
        print(f"Renaming table: {alter_stmt}")
        cur.execute(alter_stmt)

        # Update schemas table - change the tbl identifier to match the new name
        # Extract the new table number/identifier from the new name
        cur_init.execute(
            "UPDATE schemas SET tbl = ? WHERE tbl = ?",
            (new_table, selected_tbl)
        )

        print(f"Successfully renamed table tbl_{selected_tbl} to tbl_{new_table}")

    elif operation == "RENAME_COLUMN":
        # Select a random non-primary key column to rename
        available_cols = []
        pk_col = tbl_schema.get("pk", 0)

        for i in range(tbl_schema["colCount"]):
            if i != pk_col:
                available_cols.append(i)

        if available_cols:
            selected_col = random_choice(available_cols)
            old_col_name = f"col_{selected_col}"
            new_col_name = f"col_{selected_col}_renamed"

            alter_stmt = f"ALTER TABLE tbl_{selected_tbl} RENAME COLUMN {old_col_name} TO {new_col_name}"
            print(f"Renaming column: {alter_stmt}")
            cur.execute(alter_stmt)

            # Rename it back to maintain consistency
            cur.execute(f"ALTER TABLE tbl_{selected_tbl} RENAME COLUMN {new_col_name} TO {old_col_name}")

            print(f"Successfully renamed column {old_col_name} (and renamed back)")
        else:
            print("No non-primary key columns available to rename")

    con.commit()
    con_init.commit()

except turso.ProgrammingError as e:
    print(f"Table/column might have been dropped in parallel: {e}")
    con.rollback()
    con_init.rollback()
except turso.OperationalError as e:
    print(f"Failed to alter table: {e}")
    con.rollback()
    con_init.rollback()
except Exception as e:
    print(f"Unexpected error: {e}")
    con.rollback()
    con_init.rollback()

con.close()
con_init.close()
