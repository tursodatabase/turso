#!/usr/bin/env python3
"""
Turso Database Sync example with Turso Cloud (with optional remote encryption)

Environment variables:
  TURSO_REMOTE_URL              - Remote database URL (default: http://localhost:8080)
  TURSO_AUTH_TOKEN              - Auth token (optional)
  TURSO_REMOTE_ENCRYPTION_KEY   - Base64-encoded encryption key (optional)
  TURSO_REMOTE_ENCRYPTION_CIPHER - Cipher name (default: Aes256Gcm)
    Supported: Aes256Gcm, Aes128Gcm, ChaCha20Poly1305,
               Aegis128L, Aegis128X2, Aegis128X4,
               Aegis256, Aegis256X2, Aegis256X4
               docs - https://docs.turso.tech/cloud/encryption
"""

import os

from turso.sync import RemoteEncryptionCipher, connect

# Map cipher string to enum
CIPHER_MAP = {
    "aes256gcm": RemoteEncryptionCipher.Aes256Gcm,
    "aes128gcm": RemoteEncryptionCipher.Aes128Gcm,
    "chacha20poly1305": RemoteEncryptionCipher.ChaCha20Poly1305,
    "aegis128l": RemoteEncryptionCipher.Aegis128L,
    "aegis128x2": RemoteEncryptionCipher.Aegis128X2,
    "aegis128x4": RemoteEncryptionCipher.Aegis128X4,
    "aegis256": RemoteEncryptionCipher.Aegis256,
    "aegis256x2": RemoteEncryptionCipher.Aegis256X2,
    "aegis256x4": RemoteEncryptionCipher.Aegis256X4,
}


def main():
    remote_url = os.environ.get("TURSO_REMOTE_URL", "http://localhost:8080")
    auth_token = os.environ.get("TURSO_AUTH_TOKEN")
    encryption_key = os.environ.get("TURSO_REMOTE_ENCRYPTION_KEY")
    encryption_cipher_str = os.environ.get("TURSO_REMOTE_ENCRYPTION_CIPHER", "aes256gcm").lower()
    encryption_cipher = CIPHER_MAP.get(encryption_cipher_str)
    if encryption_cipher is None:
        raise ValueError(f"Unknown cipher: {encryption_cipher_str}. Supported: {', '.join(CIPHER_MAP.keys())}")

    print(f"Remote URL: {remote_url}")
    print(f"Auth Token: {auth_token is not None}")
    print(f"Encryption: {encryption_key is not None}")
    if encryption_key:
        print(f"Cipher: {encryption_cipher}")

    # Connect to the sync database
    conn = connect(
        path=":memory:",
        remote_url=remote_url,
        auth_token=auth_token,
        remote_encryption_key=encryption_key,
        remote_encryption_cipher=encryption_cipher if encryption_key else None,
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
