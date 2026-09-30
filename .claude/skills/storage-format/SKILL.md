---
name: storage-format
description: SQLite file format, B-trees, pages, cells, overflow, freelist that is used in tursodb
---
# Storage Format Guide

## Database File Structure

```
┌─────────────────────────────┐
│ Page 1: Header + Schema     │  ← First 100 bytes = DB header
├─────────────────────────────┤
│ Page 2..N: B-tree pages     │  ← Tables and indexes
│            Overflow pages   │
│            Freelist pages   │
└─────────────────────────────┘
```

Page size: power of 2, 512-65536 bytes. Default 4096.

## Database Header (First 100 Bytes)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 16 | Magic: `"SQLite format 3\0"` |
| 16 | 2 | Page size (big-endian) |
| 18 | 1 | Write format version (1=rollback, 2=WAL) |
| 19 | 1 | Read format version |
| 24 | 4 | Change counter |
| 28 | 4 | Database size in pages |
| 32 | 4 | First freelist trunk page |
| 36 | 4 | Total freelist pages |
| 40 | 4 | Schema cookie |
| 56 | 4 | Text encoding (1=UTF8, 2=UTF16LE, 3=UTF16BE) |

All multi-byte integers: **big-endian**.

## Page Types

| Flag | Type | Purpose |
|------|------|---------|
| 0x02 | Interior index | Index B-tree internal node |
| 0x05 | Interior table | Table B-tree internal node |
| 0x0a | Leaf index | Index B-tree leaf |
| 0x0d | Leaf table | Table B-tree leaf |
| - | Overflow | Payload exceeding cell capacity |
| - | Freelist | Unused pages (trunk or leaf) |

## B-tree Structure

Two B-tree types:
- **Table B-tree**: 64-bit rowid keys, stores row data
- **Index B-tree**: Arbitrary keys (index columns + rowid)

```
Interior page:  [ptr0] key1 [ptr1] key2 [ptr2] ...
                   │         │         │
                   ▼         ▼         ▼
               child     child     child
               pages     pages     pages

Leaf page:     key1:data  key2:data  key3:data ...
```

Page 1 always root of `sqlite_schema` table.

## Cell Format

### Table Leaf Cell
```
[payload_size: varint] [rowid: varint] [payload] [overflow_ptr: u32?]
```

### Table Interior Cell
```
[left_child_page: u32] [rowid: varint]
```

### Index Cells
Similar but key is arbitrary (columns + rowid), not just rowid.

## Record Format (Payload)

```
[header_size: varint] [type1: varint] [type2: varint] ... [data1] [data2] ...
```

Serial types:
| Type | Meaning |
|------|---------|
| 0 | NULL |
| 1-4 | 1/2/3/4 byte signed int |
| 5 | 6 byte signed int |
| 6 | 8 byte signed int |
| 7 | IEEE 754 float |
| 8 | Integer 0 |
| 9 | Integer 1 |
| ≥12 even | BLOB, length=(N-12)/2 |
| ≥13 odd | Text, length=(N-13)/2 |

## Overflow Pages

When payload exceeds threshold, excess stored in overflow chain:
```
[next_page: u32] [data...]
```
Last page has next_page=0.

## Freelist

Linked list of trunk pages, each containing leaf page numbers:
```
Trunk: [next_trunk: u32] [leaf_count: u32] [leaf_pages: u32...]
```

## Turso Implementation

Key files:
- `core/storage/sqlite3_ondisk.rs` - On-disk format, `PageType` enum
- `core/storage/btree.rs` - B-tree operations (large file)
- `core/storage/pager.rs` - Page management
- `core/storage/buffer_pool.rs` - Page caching

## Debugging Storage

```bash
# Integrity check
cargo run --bin tursodb test.db "PRAGMA integrity_check;"

# Page count
cargo run --bin tursodb test.db "PRAGMA page_count;"

# Freelist info
cargo run --bin tursodb test.db "PRAGMA freelist_count;"
```

## References

- [SQLite File Format](https://sqlite.org/fileformat.html)
- [SQLite B-Tree Module](https://sqlite.org/btreemodule.html)
- [SQLite Internals: Pages & B-trees](https://fly.io/blog/sqlite-internals-btree/)
