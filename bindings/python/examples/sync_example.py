#!/usr/bin/env python3
"""
Turso Database Sync example with Turso Cloud (with optional remote encryption)

Environment variables:
  TURSO_REMOTE_URL          - Remote database URL (default: http://localhost:8080)
  TURSO_AUTH_TOKEN          - Auth token (optional)
  TURSO_REMOTE_ENCRYPTION_KEY - Base64-encoded encryption key (optional)
"""

import os

from turso.sync import connect


def main():
    remote_url = os.environ.get("TURSO_REMOTE_URL", "http://localhost:8080")
    auth_token = os.environ.get("TURSO_AUTH_TOKEN")
    encryption_key = os.environ.get("TURSO_REMOTE_ENCRYPTION_KEY")

    print(f"Remote URL: {remote_url}")
    print(f"Auth Token: {auth_token is not None}")
    print(f"Encryption: {encryption_key is not None}")

    # Connect to the sync database
    conn = connect(
        path=":memory:",
        remote_url=remote_url,
        auth_token=auth_token,
        remote_encryption_key=encryption_key,
    )

    # Create table
    conn.execute("CREATE TABLE IF NOT EXISTS t (x TEXT)")
    conn.commit()

    # Get current row count and insert next numbered row
    cur = conn.execute("SELECT COUNT(*) FROM t")
    row = cur.fetchone()
    count = row[0] if row else 0
    next_num = count + 1

    conn.execute(f"INSERT INTO t VALUES ('hello sync #{next_num}')")
    conn.commit()
    conn.push()

    # Query test table contents
    print("\nTest table contents:")
    cur = conn.execute("SELECT * FROM t")
    for row in cur.fetchall():
        print(f"  Row: {row[0]}")

    # Query sqlite_master for all tables
    print("\nDatabase tables:")
    cur = conn.execute("SELECT name, type FROM sqlite_master WHERE type='table'")
    for row in cur.fetchall():
        print(f"  - {row[1]}: {row[0]}")

    # Show database stats
    stats = conn.stats()
    print("\nDatabase stats:")
    print(f"  Network received: {stats.network_received_bytes} bytes")
    print(f"  Network sent: {stats.network_sent_bytes} bytes")
    print(f"  Main WAL size: {stats.main_wal_size} bytes")

    print("\nDone!")


if __name__ == "__main__":
    main()
