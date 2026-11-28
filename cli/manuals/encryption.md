---
display_name: "encryption at-rest"
---

# Encryption - At-Rest Database Encryption

## Overview

Turso supports transparent at-rest encryption to protect your database files from unauthorized access. Database pages, WAL frames, and MVCC logical logs are encrypted and decrypted with the cipher and key that you give when you open the database, or with `PRAGMA cipher` and `PRAGMA hexkey` when you create it.

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

Encryption is an experimental feature that must be explicitly enabled with `--experimental-encryption`.

### Method 1: Using PRAGMAs

Open a new database file:

```bash
tursodb --experimental-encryption database.db
```

Then, in the SQL shell, set the cipher and the key before any other statement:

```sql
PRAGMA cipher = 'aegis256';
PRAGMA hexkey = '2d7a30108d3eb3e45c90a732041fe54778bdcf707c76749fab7da335d1b39c1d';

-- Now create your tables and insert data
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users VALUES (1, 'Alice');
```

The PRAGMAs can encrypt only a new, empty database that no other connection uses, and only outside a transaction. They return an error and change nothing on a database that already has pages, on an attached database (`PRAGMA aux.hexkey`), and with MVCC (run `PRAGMA journal_mode = 'mvcc'` after them). After the first of the two PRAGMAs, the shell refuses other statements until you set the second one, so a missing key cannot give a plaintext database. Run `PRAGMA page_size` before the two PRAGMAs if you want a page size other than 4096.

The shell does not write lines that contain `hexkey` to its history file.

### Method 2: Using URI Parameters

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

In a shell that was started with `--experimental-encryption`, `.open` accepts the same URI:

```
.open "file:database.db?cipher=aegis256&hexkey=2d7a30108d3eb3e45c90a732041fe54778bdcf707c76749fab7da335d1b39c1d"
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
