---
display_name: "encryption at-rest"
---

# Encryption - At-Rest Database Encryption

## Overview

Turso supports transparent at-rest encryption to protect your database files from unauthorized access. When enabled, all data written to disk is automatically encrypted, and decrypted when read, with no changes required to your application code.

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

### Method 1: Using PRAGMAs

Start Turso and set encryption parameters before creating tables:

```bash
tursodb database.db
```

Then in the SQL shell:
```sql
PRAGMA cipher = 'aegis256';
PRAGMA hexkey = '2d7a30108d3eb3e45c90a732041fe54778bdcf707c76749fab7da335d1b39c1d';

-- Now create your tables and insert data
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users VALUES (1, 'Alice');
```

### Method 2: Using URI Parameters

Specify encryption parameters directly in the database URI:

```bash
tursodb "file:database.db?cipher=aegis256&hexkey=2d7a30108d3eb3e45c90a732041fe54778bdcf707c76749fab7da335d1b39c1d"
```

## Opening an Encrypted Database

**Important:** To open an existing encrypted database, you MUST provide the cipher and key as URI parameters:

```bash
tursodb "file:database.db?cipher=aegis256&hexkey=2d7a30108d3eb3e45c90a732041fe54778bdcf707c76749fab7da335d1b39c1d"
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
