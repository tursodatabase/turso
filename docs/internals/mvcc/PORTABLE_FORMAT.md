# Portable MVCC Logical Changes

This design extends the MVCC logical log with portable change metadata for sync.
The recovery log remains the source of truth for local crash recovery. Portable
changes are an additional per-commit view that lets a raw-log consumer replay
committed user-visible changes without access to the original database schema,
connection state, or in-memory MVCC table-id map.

## Goals

- Keep MVCC recovery compact and authoritative.
- Make row changes portable: require no additional context for consumers to replay logical transactions from the log 
- Avoid excessive memory usage.
- Avoid duplicating large row records in the portable extension when the same
  bytes already exist in the recovery payload.
- Keep table names and SQL out of repeated row operations.
- Encrypt and authenticate portable metadata together with recovery records when
  database encryption is enabled.
- Keep the encoding owned by `turso_core`, without depending on sync-engine or
  server protocol types.

## Frame Placement

Portable changes live in an extension block appended to an MVCC transaction
frame:

```text
TX_EXT_HEADER
  payload_size            // recovery payload bytes only
  op_count
  commit_ts
  extension_size          // extension block bytes
  extension_record_count
  frame_flags

BODY
  recovery payload
  extension block

TRAILER
  chained crc
  end magic
```

The recovery payload is still parsed and replayed by MVCC recovery. The
extension block is ignored by local recovery and consumed only by raw-log readers
that ask for portable changes.

When encryption is enabled, the recovery payload and extension block are
encrypted as one body:

```text
plaintext = recovery_payload || extension_block
on disk   = encrypted_chunks(plaintext)
```

The encrypted chunk AAD includes the log salt, op count, commit timestamp, chunk
index, and final plaintext size. As a result, portable table names, SQL text,
origin metadata, and row-delete records are not plaintext side data and cannot
be modified independently of the recovery frame.

## Portable Transaction Envelope

The extension payload is a small protobuf-style envelope encoded inside
`turso_core`. It contains:

- `end_offset`: the raw-log cursor after this frame.
- `commit_ts`: the MVCC commit timestamp.
- `origin_client_id`: optional explicit origin metadata.
- `string_table`: frame-local interned strings.
- `object_map`: frame-local bindings from MVCC recovery ids to portable object
  identities.
- `ops`: portable logical operations.

The string table and object map are the core portability mechanism, not just
compression.

## String Table

Names and SQL are interned once per frame. Schema operations and object-map
entries reference strings by index.

This avoids repeating object names and `CREATE ...` SQL in every operation. More
importantly, row operations never switch between "sometimes name" and "sometimes
id" encodings. Rows always carry an MVCC table id; the object map gives that id
portable meaning.

## Object Map

Each object-map entry binds the local id used by recovery records in this frame
to a portable object identity:

```text
object_map {
  mv_table_id: sint64
  logical_object_id: uint64
  kind: table | index | trigger | view
  name_ref: string table index
  schema_epoch: commit timestamp
  create_sql_ref: optional string table index
  rootpage_debug: sint64
}
```

`mv_table_id` is the id used by recovery ops. It may be negative because MVCC
uses negative root pages for objects that exist only in the MV store.

`logical_object_id` is stable for rootpage-backed objects. For non-zero root
pages it is `abs(rootpage)`, so an uncheckpointed negative rootpage and its later
checkpointed positive rootpage refer to the same portable object.

Objects with rootpage `0`, such as views, triggers, and some virtual table
schema rows, do not have rootpage-backed row identity. Their portable identity
comes from schema events: kind, name, and SQL.

`rootpage_debug` exists only to make producer behavior auditable. Consumers must
not treat it as the portable identity.

## Portable Operations

Portable row operations are table mutations. Index recovery records are not
portable mutations; a destination database maintains indexes by applying table
changes normally.

```text
upsert_row {
  mv_table_id: sint64
  rowid: sint64
}

delete_row {
  mv_table_id: sint64
  rowid: sint64
  old_record: optional bytes
}
```

Upserts do not duplicate the row record in the portable extension. The same row
image already exists in the recovery payload in this frame. A raw-log consumer
reads the recovery table op for bytes and resolves `mv_table_id` through the
object map.

Deletes include the old row record when core still has it. This lets sync derive
primary-key values for tables whose sync identity is not SQLite rowid. If no old
record is available, the operation still has rowid fallback semantics.

Schema operations describe user-visible object lifecycle:

```text
schema {
  action: create | drop | refresh
  kind: table | index | trigger | view
  stable_table_id: optional uint64
  name_ref: string table index
  sql_ref: optional string table index
}
```

Header operations carry database-header fields relevant to sync:

```text
update_header {
  user_version: uint64
  application_id: uint64
}
```

Row writes are modeled as idempotent upserts and deletes. The portable contract
does not distinguish insert from update; it records the committed final state
needed for replay.

## Commit-Time Construction

The commit path builds the recovery payload first. Portable metadata is derived
after that payload exists:

1. Parse the recovery payload with the same parser used by recovery.
2. Decode `sqlite_schema` row changes into schema events.
3. Retain only old row images needed for schema drops and delete primary-key
   derivation. The writer does not keep a duplicate `Vec<RowVersion>` for all
   committed changes.
4. Resolve every table row mutation to an object-map entry while core still has
   schema context.
5. Emit row ops as references to `mv_table_id`.
6. Emit the string table, object map, schema/header operations, row operations,
   and optional origin metadata into the extension payload.

If portable logging is enabled and core cannot decode required schema context or
resolve a table mutation to an object-map entry, commit fails. Silently omitting
a portable mutation would make sync consumers diverge from local recovery.

## Filtering And Metadata

Portable changes filter internal implementation tables: SQLite internal objects,
Turso internal objects, and reserved CDC tables. These are not part of the user
schema contract that sync should replay on clients.

Origin metadata is explicit per-connection metadata. It is snapshot at commit
time, not consumed, and setting metadata does not enable portable logging by
itself. This avoids surprising behavior where metadata mutation changes log
format or disappears after one commit.

Current trigger behavior is effect-based: portable row operations describe the
final committed effects of the transaction, including trigger-produced writes.
The log does not mark whether an individual write came directly from client DML
or from trigger execution. Adding that provenance would require VDBE-level
origin tracking and is outside this format change.

Memory growth is limited by deriving portable changes from the serialized
recovery payload and retaining only old records needed for deletes/schema drops.
The old design cloned every row version into a second structure.

Log growth is limited by not duplicating upsert row images and by interning names
and SQL once per frame. Portable row ops are compact references plus optional
old records for deletes.

Encryption is handled by placing the extension block inside the encrypted body
instead of appending plaintext metadata after the recovery payload.

Table identity is made explicit through the object map. Row ops use one
consistent representation, `mv_table_id`, and raw consumers resolve it through
the same-frame map to a stable logical object id and interned name/SQL.
