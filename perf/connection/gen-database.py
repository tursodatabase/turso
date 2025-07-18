#!/usr/bin/env python3
"""Generate SQLite databases with specified number of tables for benchmarking."""

import argparse
import sqlite3


def main() -> None:
    """Generate database with specified number of tables."""
    parser = argparse.ArgumentParser()
    parser.add_argument("filename")
    parser.add_argument("-t", "--tables", type=int, help="Number of tables to create")

    args = parser.parse_args()

    conn = sqlite3.connect(args.filename)
    cursor = conn.cursor()

    # Enable WAL mode
    cursor.execute("PRAGMA journal_mode=WAL")

    # Create the specified number of tables
    for i in range(args.tables):
        table_name = f"table_{i}"
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert a small amount of data in each table for realism
        cursor.execute(
            f"INSERT INTO {table_name} (name, value) VALUES (?, ?)",
            (f"item_{i}", i),
        )

    print(f"Created {args.tables} tables in {args.filename}")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
