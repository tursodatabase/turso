#!/usr/bin/env python3
"""
Local Database Encryption Example

This example demonstrates how to use local database encryption
with the Turso Python SDK.

Supported ciphers:
  - aes128gcm
  - aes256gcm
  - aegis256
  - aegis256x2
  - aegis128l
  - aegis128x2
  - aegis128x4
"""

import os

import turso

DB_PATH = "encrypted.db"
# 32-byte hex key for aegis256 (256 bits = 32 bytes = 64 hex chars)
ENCRYPTION_KEY = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327"


def main():
    print("=== Turso Local Encryption Example ===\n")

    # Create an encrypted database
    print("1. Creating encrypted database...")
    conn = turso.connect(
        DB_PATH,
        experimental_features="encryption",
        encryption=turso.EncryptionOpts(
            cipher="aegis256",
            hexkey=ENCRYPTION_KEY,
        ),
    )

    # Create a table and insert sensitive data
    print("2. Creating table and inserting data...")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, ssn TEXT)")
    cursor.execute("INSERT INTO users (name, ssn) VALUES ('Alice', '123-45-6789')")
    cursor.execute("INSERT INTO users (name, ssn) VALUES ('Bob', '987-65-4321')")
    conn.commit()

    # Checkpoint to flush data to disk
    cursor.execute("PRAGMA wal_checkpoint(truncate)")
    conn.commit()

    # Query the data
    print("3. Querying data...")
    cursor.execute("SELECT * FROM users")
    for row in cursor.fetchall():
        print(f"   User: id={row[0]}, name={row[1]}, ssn={row[2]}")

    conn.close()

    # Verify the data is encrypted on disk
    print("\n4. Verifying encryption...")
    with open(DB_PATH, "rb") as f:
        raw_content = f.read()

    contains_plaintext = b"Alice" in raw_content or b"123-45-6789" in raw_content

    if contains_plaintext:
        print("   WARNING: Data appears to be unencrypted!")
    else:
        print("   Data is encrypted on disk (plaintext not found)")

    # Reopen with the same key
    print("\n5. Reopening database with correct key...")
    conn2 = turso.connect(
        DB_PATH,
        experimental_features="encryption",
        encryption=turso.EncryptionOpts(
            cipher="aegis256",
            hexkey=ENCRYPTION_KEY,
        ),
    )
    cursor2 = conn2.cursor()
    cursor2.execute("SELECT name FROM users")
    names = [row[0] for row in cursor2.fetchall()]
    print(f"   Successfully read users: {names}")
    conn2.close()

    # Demonstrate that wrong key fails
    print("\n6. Attempting to open with wrong key (should fail)...")
    try:
        conn3 = turso.connect(
            DB_PATH,
            experimental_features="encryption",
            encryption=turso.EncryptionOpts(
                cipher="aegis256",
                hexkey="aaaaaaa4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
            ),
        )
        cursor3 = conn3.cursor()
        cursor3.execute("SELECT * FROM users")
        print("   ERROR: Should have failed with wrong key!")
    except Exception as e:
        print(f"   Correctly failed: {e}")

    # Cleanup
    os.remove(DB_PATH)
    print("\n=== Example completed successfully ===")


if __name__ == "__main__":
    main()
