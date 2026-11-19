# How sync depends on table ids

The specs in this folder do not model sync. Sync reads the logical log and
the `rootpage` column of `sqlite_schema`. It uses the same rule as recovery:
the id of a table is `-rootpage`. So any change to table ids, for example
step 3 of `SIMPLIFY.md`, must keep sync working. This file records what sync
expects. Line numbers are from commit 0c9fe0dd9.

## Where sync uses table ids

### The object map in each commit frame

When portable changes are on, each commit frame in the logical log has an
object map. The object map is a list of pairs: a table id and a table name.
Sync uses it to turn log records into row changes with a table name.

1. Core builds the object map in the commit path. It takes the `rootpage`
   of the `sqlite_schema` row and removes the sign. Then it calls
   `portable_table_id_from_rootpage` (`core/mvcc/database/mod.rs:634`,
   called at `mod.rs:2974`), which returns `-rootpage`.
2. The sync client reads the object map in `portable_object_names`
   (`sync/engine/src/database_sync_operations.rs:1141`).
3. `decode_recovery_ops_to_logical_txn` (`database_sync_operations.rs:1319`)
   reads the raw `i32` table id of each log record. It finds the name of the
   table through the object map of the same commit.

The record id and the object map id must use the same form, or the lookup
fails. Today both are `-rootpage`, because `canonical_log_id`
(`mod.rs:2458`) rewrites record ids after the table gets a page.

### The stable table id in the pull protocol

A row change that the client pulls can name its table by `stable_table_id`,
not by name (`sync/engine/src/client_proto.rs:91`). The client keeps a map
from stable id to name in its sync metadata
(`logical_table_names_by_stable_id`, `sync/engine/src/types.rs:194`).
`resolve_row_table_name` (`database_sync_operations.rs:457`) reads it.

After a pull that copies pages, the client builds the map again
(`read_logical_replay_table_map`, `database_sync_operations.rs:2588`). It
reads `SELECT rootpage, name FROM sqlite_schema` and uses
`rootpage.unsigned_abs()` as the stable id (`:2612`). So the stable id in the protocol is the absolute value of the
root page.

## What a permanent table id changes

Example: table `t` gets id -5 at `CREATE`, and its first checkpoint gives it
page 6.

| Value | Today | With a permanent id |
|---|---|---|
| Table id in a log record | -5 before the publish, -6 after it | -5 always |
| Object map id (core) | `-abs(rootpage)`: -5, then -6 | -5. Core must emit the table id. |
| `stable_table_id` in the protocol | `abs(rootpage)`: 5, then 6 | 6, if the `rootpage` column in the B-tree holds the page |
| Map that the client builds again | `abs(rootpage)` from `sqlite_schema` | No change, with the same condition |

The rules for a permanent id:

- Core must write the table id into the object map, with no conversion from
  the root page. The sync decoder does not change.
- The `rootpage` column in the B-tree must hold the real page. The pull
  protocol and the client map depend on it. This is a second reason to
  decide carefully what the MVCC version of a `sqlite_schema` row holds.
- The map from table id to page lives in the database file. A pull that
  copies pages, or an external restore
  (`reset_logical_log_after_external_restore`,
  `sync/engine/src/database_sync_engine.rs:2399`), brings the map with the
  pages.

## Open questions

These are not verified.

1. Possible id collision in the client map. Before its first checkpoint, a
   table has `rootpage = -N`. `unsigned_abs()` turns it into N. A new table
   at -5 and an old table at page 5 then get the same stable id 5. This is
   the same pattern as #11501. To be a bug, a pull that copies pages must
   see a table that no checkpoint published yet. That was not checked.
2. The stable id changes at the first checkpoint of a table, from 5 to 6.
   No code that handles this change was found. The sync tests were not
   searched for it.

## Next step

Add the object map to `MvccPassiveSchema.qnt`. Each commit gets a map from
id to name. A new invariant says that each log record finds the correct
table name through the map of its own commit. Sync replays the log like
recovery does, so this checks the promise that sync depends on. The same
run checks question 1 for the code today and for a permanent id. The pull
protocol stays out of the model. Its rule is: the stable id is the page
number in the `rootpage` column of the B-tree.
