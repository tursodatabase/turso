#!/usr/bin/env -S python3 -u


import turso
from antithesis.random import get_random

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

# Get all user-created indexes (excluding automatic indexes)
existing_indexes = cur.execute("""
    SELECT name, tbl_name FROM sqlite_master
    WHERE type = 'index'
    AND sql IS NOT NULL
    AND name NOT LIKE 'sqlite_%'
""").fetchall()

if not existing_indexes:
    print("No indexes available to drop")
    exit(0)

# Select a random index to drop
selected_idx = get_random() % len(existing_indexes)
index_name, table_name = existing_indexes[selected_idx]

print(f"Selected index: {index_name} on table {table_name}")
print(f"Total indexes available: {len(existing_indexes)}")

try:
    # Drop the index
    drop_stmt = f"DROP INDEX {index_name}"
    print(f"Dropping index: {drop_stmt}")
    cur.execute(drop_stmt)

    # Remove index information from init_state.db
    cur_init.execute(f"""
        DELETE FROM indexes
        WHERE idx_name = '{index_name}'
    """)
    con_init.commit()

    print(f"Successfully dropped index: {index_name}")
except turso.ProgrammingError as e:
    print(f"Index {index_name} already dropped in parallel: {e}")
    con.rollback()
except turso.OperationalError as e:
    print(f"Failed to drop index: {e}")
    con.rollback()
except Exception as e:
    print(f"Warning: Could not remove index from metadata: {e}")

con.commit()
con.close()
con_init.close()
