---
display_name: "encryption at-rest"
---

# Encryption - At-Rest Database Encryption

## Overview

Turso supports transparent at-rest encryption to protect your database files from unauthorized access. Database pages, WAL frames, and MVCC logical logs are encrypted and decrypted with the credentials supplied when opening the database.

New encrypted databases bind each page to its page number, so a page that moves to a different position in the database file or the WAL fails to decrypt. This check does not detect an older copy of a page at the same position. It also does not detect a page from another database that uses the same key, such as a backup, a copy, or the temporary database of an in-place VACUUM, when that page replaces the page at the same position. Encrypted databases that earlier versions of Turso created keep their original encryption format, which does not have this check. Earlier versions of Turso cannot open a database in the new format: they report `Decryption failed for page=1`, which is the same error as for a wrong key.

## Supported Ciphers

Turso supports multiple encryption algorithms with different performance and security characteristics:

### AES-GCM Family
- **`aes128gcm`** - AES-128 in Galois/Counter Mode (16-byte key)
- **`aes256gcm`** - AES-256 in Galois/Counter Mode (32-byte key)

### AEGIS Family (High Performance)
- **`aegis256`** - AEGIS-256 (32-byte key) - Recommended for most use cases
- **`aegis128l`** - AEGIS-128L (16-byte key)
- **`aegis128x2`** - AEGIS-128 with 2x parallelization (16-byte key)
- **`aegis128x4`** - AEGIS-128 with 4x parallelization (16-byte key)
- **`aegis256x2`** - AEGIS-256 with 2x parallelization (32-byte key)
- **`aegis256x4`** - AEGIS-256 with 4x parallelization (32-byte key)

**Note:** AEGIS ciphers generally offer better performance than AES-GCM while maintaining excellent security properties. AEGIS-256 is recommended as the default choice.

## Generating Encryption Keys

Generate a secure encryption key using OpenSSL:

```bash
# For 32-byte key (256-bit) - use with aes256gcm, aegis256, etc.
openssl rand -hex 32

# For 16-byte key (128-bit) - use with aes128gcm, aegis128l, etc.
openssl rand -hex 16
```

Example output:
```
2d7a30108d3eb3e45c90a732041fe54778bdcf707c76749fab7da335d1b39c1d
```

**Important:** Store your encryption key securely. If you lose the key, your encrypted data cannot be recovered.

## Creating an Encrypted Database

Encryption must be selected when the database is opened. Supply both the cipher and key before creating a connection; `PRAGMA cipher = ...` and `PRAGMA hexkey = ...` assignments are rejected, including on empty databases.

Specify encryption parameters directly in the database URI:

```bash
tursodb --experimental-encryption "file:database.db?cipher=aegis256&hexkey=2d7a30108d3eb3e45c90a732041fe54778bdcf707c76749fab7da335d1b39c1d"
```

The key-bearing URI is operationally unsafe: it can expose the key through process arguments, shell history, and command-line recording. A protected secret-input channel is not currently provided by this CLI flow.

After opening, create tables and insert data normally. Encryption identity, page size, and reserved bytes are fixed for the lifetime of the encrypted database object. Opening a live plaintext database with encryption options is rejected, even before its first write.

## Opening an Encrypted Database

**Important:** To open an existing encrypted database, you MUST provide the cipher and key as URI parameters:

```bash
tursodb --experimental-encryption "file:database.db?cipher=aegis256&hexkey=2d7a30108d3eb3e45c90a732041fe54778bdcf707c76749fab7da335d1b39c1d"
```

Attempting to open an encrypted database without the correct cipher and key will fail.

## Troubleshooting

### "Database is encrypted or is not a database"
This error occurs when:
- Opening an encrypted database without providing cipher/key
- Using the wrong cipher or key
- The database file is corrupted

### "Invalid hex string"
- Ensure your key is valid hexadecimal (0-9, a-f)
- Check the key length matches your cipher (32 hex chars for 16 bytes, 64 for 32 bytes)

## Attached Databases and Maintenance

The main and attached databases select encryption independently. A plaintext main database can attach encrypted targets, and an encrypted main database can attach plaintext targets. Provide each encrypted target's own credentials in its URI:

```sql
ATTACH 'file:aux.db?cipher=aegis256&hexkey=YOUR_HEX_KEY' AS aux;
```

`ATTACH ... KEY` rejects non-NULL values before opening a target. Use URI credentials for encryption; `KEY NULL` is accepted. Serialize other opens of a new target until ATTACH completes.

In-place VACUUM preserves encryption and page layout for the main database. In-place VACUUM of an encrypted attached database is rejected. VACUUM INTO writes a plaintext destination. External restore hooks reject built-in encrypted databases. Sync encryption is not supported.

The process-wide database manager authenticates every open independently, and each connection and attachment authenticates page 1 with its own pager. Missing keys, wrong keys, and mismatched ciphers are refused before returning a database or connection. Existing encrypted files retain their format and credentials.

Callers using raw opens, custom storage, or separate processes must serialize the first initialization of a zero-byte encrypted database.
