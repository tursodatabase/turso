# Portable MVCC Log Metadata

The MVCC recovery log is the only operation stream. Portable metadata lets a
raw-log consumer interpret that recovery stream outside the original database
instance, where the in-memory schema and MVCC table-id map are no longer
available.

## Goals

- Keep MVCC recovery compact and authoritative.
- Avoid duplicating recovery rows, schema rows, and header updates in a second
  portable operation stream.
- Resolve negative MVCC table ids and uncheckpointed rootpages into portable
  object names.
- Keep sync-specific fields out of the core format by using generic transaction
  metadata.
- Encrypt and authenticate portable metadata with the recovery payload when
  database encryption is enabled.
- Keep the encoding owned by `turso_core`, without depending on sync-engine or
  server protocol types.

## Frame Layout

An MVCC transaction frame has a recovery payload and may have an extension
block:

```text
tx header:
  payload_size            // recovery payload bytes
  op_count
  commit_ts
  extension_size
  extension_record_count
  frame_flags

body:
  extension records
  recovery ops

trailer:
  crc/auth tag/end magic
```

Local recovery reads only the recovery ops. Raw-log consumers read the same
frame in order: portable metadata first, then recovery ops whose table ids are
resolved through that metadata.

When encryption is enabled, the encrypted plaintext is:

```text
extension_block || recovery_payload
```

Portable names, metadata, and operation-local extension bytes are therefore not
plaintext side data.

The transaction-scoped extension block intentionally precedes the recovery
payload so streaming consumers can load the string table and object map before
reading the first recovery op.

## Recovery Ops

The recovery payload describes all committed effects:

- table upserts: `rowid_varint || table_record_bytes`
- table deletes: `rowid_varint`
- index upserts/deletes
- database-header updates
- `sqlite_schema` row upserts/deletes for DDL

Schema changes and header changes are not repeated as portable operations.
Schema DDL is represented by the recovery stream's `sqlite_schema` row changes,
including the SQL stored in those rows. Header updates are represented by
recovery header ops.

## Transaction Extension

The transaction extension is a protobuf-style envelope encoded inside
`turso_core`. It currently contains only:

- `string_table`: frame-local interned strings.
- `object_map`: bindings from MVCC table ids to interned names.
- `meta`: generic string key/value transaction metadata.

There are no portable row, schema, or header operations.

```text
PortableTxn {
  end_offset: uint64          // added by the frame wrapper
  commit_ts: uint64           // added by the frame wrapper
  repeated string_table: bytes
  repeated object_map: ObjectMap
  repeated meta: MetaField
}

ObjectMap {
  mv_table_id: sint64
  name_ref: uint64
}

MetaField {
  key_ref: uint64
  value_ref: uint64
}
```

`mv_table_id` is the id used by recovery ops in the same frame. It may be
negative because MVCC uses negative rootpages for objects that have not been
checkpointed.

`name_ref` points into the frame string table. Row recovery ops keep the compact
MVCC table id, and external readers resolve that id through the same-frame object
map.

## Operation Extensions

Recovery ops reserve `OP_FLAG_PORTABLE_EXTENSION` for operation-local protobuf
bytes:

```text
op {
  tag
  flags                  // includes OP_FLAG_PORTABLE_EXTENSION
  table_id
  payload_len
  payload
  extension_len
  extension_bytes
}
```

The extension is attached to one recovery op, not to the frame as a parallel
portable operation stream. Recovery must be able to ignore operation-local
extensions. If data is required for crash recovery, it belongs in the main
recovery payload.

Current delete extension fields:

```text
DeleteExtension {
  identity_record: bytes  // field 1, only for sqlite_schema deletes
  pk_record: bytes        // field 2, SQLite record of primary-key values
  rowid: sint64           // field 3, present for rowid-table deletes
}
```

For ordinary user-table deletes, core computes a primary-key projection while the
committing connection still has schema context. The extension carries only that
projected key record plus the rowid, not the full deleted row. For tables
without an explicit primary key, `pk_record` is omitted and rowid is the delete
identity. For `INTEGER PRIMARY KEY` aliases, the projected key uses the rowid
value rather than the stored table-record slot, matching SQLite rowid semantics.

`sqlite_schema` deletes are the exception. They attach the old schema record as
`identity_record` because object-map construction needs the deleted object's
name and rootpage.

The same operation-extension mechanism can later carry fields such as DDL
statement text for ALTER-style operations or trigger provenance, without
changing the recovery op shape.

## Construction

At commit time:

1. Serialize each recovery op, attaching operation-local portable bytes only
   when the operation needs local identity metadata.
2. Build the recovery payload.
3. Parse that payload with the recovery parser.
4. Decode `sqlite_schema` row changes only to discover object-map entries.
5. Resolve table row ops to object-map entries while core still has schema
   context.
6. Snapshot all per-connection MVCC log metadata as generic key/value fields.
7. Emit the string table, object maps, and metadata as the transaction extension
   payload.

If portable logging is enabled and a user table mutation cannot be resolved to a
portable object map, commit fails. Silently omitting a user table mapping would
make external replay ambiguous.

## Filtering

The portable extension skips implementation-owned objects:

- `sqlite_*`
- `__turso_internal_*`
- `turso_sync_*`
- reserved CDC bookkeeping tables

These objects are SQLite implementation state, Turso local state, or sync/CDC
bookkeeping that should be rebuilt by the receiving database. They are not part
of the user schema contract the portable extension exposes.

`sqlite_sequence` deserves special handling by consumers: it is an internal
SQLite table, but sequence state is derived from user-visible inserts into
`AUTOINCREMENT` tables. Replaying the user table writes is the portable source of
truth; copying a producer's `sqlite_sequence` row would import local allocator
state and can be wrong for a different receiver.

## Review Concerns Addressed

- Memory: there is no second portable row-op list and no side buffer for every
  deleted row. Delete identity is projected during op serialization.
- Log size: upsert rows, schema rows, and header updates are not duplicated.
  Data-table deletes carry only primary-key projection and rowid, not full row
  images.
- Encryption: extension records are encrypted and authenticated with recovery
  bytes.
- Table ids: every user table id used by recovery has an explicit same-frame
  object-map entry.
- Names: names are interned once per frame and referenced from object maps.
- Metadata: all connection metadata is preserved as generic key/value fields;
  `"client"` is not special in the format.
