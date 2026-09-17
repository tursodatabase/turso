# Append pages to the database, log only pointers

Status: design + prototype. See "What is not built yet" at the end.

## The idea

In WAL mode every page is written to disk twice. A commit copies the page into
the `-wal` file, and a later checkpoint copies the same bytes again into the
database file. The log holds whole page bodies, so the log grows as fast as
the data does.

This mode writes the page body once, straight into the database file, past the
end of the part the database currently uses. The log keeps only a pointer per
page: "logical page 42 now lives in slot 9137". A commit appends those pointers
and syncs once. The page bodies are in the same sync.

## One sync per commit

The obvious way to build this needs two syncs:

1. Write the page bodies into the database file.
2. Sync the database file.
3. Append the pointers to a separate log file.
4. Sync the log file.

Step 2 is there because a durable pointer must never name a body that is not
on disk. That is undetectable corruption.

But that is a detection problem, not an ordering problem. Put the CRC of the
body into the pointer record. Recovery can then tell a body that is on disk from one
that is not, and the barrier at step 2 is unnecessary.

One requirement is left: the bodies and the pointers must be in the same sync.
A sync covers one file. So **the pointer log lives inside the database file.**

A commit becomes:

1. Write the page bodies into fresh tail slots. Write one **commit block** into
   the tail with them.
2. Sync the database file.

That is one sync, over exactly the bytes that have to be written anyway. It is
the same barrier count as WAL mode under `synchronous=FULL`, and it writes each
page body once instead of twice.

A crash during the sync leaves an arbitrary subset of those writes on disk.
Recovery copes because of two checks:

- The commit block holds a CRC of every body it names. A body that is not on disk
  fails its CRC, so the transaction is discarded.
- The commit block holds a CRC of itself. A torn or missing commit block is not
  a valid block, so the transaction is discarded.

The commit block is the single point of atomicity. The transaction committed if
and only if its commit block is on disk, is valid, and every body it names
verifies. Nothing is ever updated in place, so there is no pointer write that a
crash can catch half-done.

## Layout

The database file is an array of `page_size` slots.

```
slot:      0        1      ...  db_size-1  db_size   ...
        +--------+--------+-----+--------+---------+---------+---------+------+
        | page 1 | page 2 | ... | page N | commit  | body    | body    | ...  |
        |        |        |     |        | block   |         |         |      |
        +--------+--------+-----+--------+---------+---------+---------+------+
        |<-------- canonical region ---->|<-------------- tail ------------->|
```

Slots below `db_size` are the **canonical region**: slot `N-1` holds logical
page `N`, which is the plain SQLite layout. Slots at or above `db_size` are the
**tail**.

The canonical region is never touched between compactions, so the database file
stays a valid, if stale, SQLite database the whole time. That is the property
the database file already has between checkpoints today.

Each transaction claims one contiguous run of tail slots. The first slot of the
run holds the commit block. The rest hold page bodies.

A commit block holds:

| field | meaning |
|---|---|
| magic, format | identifies a commit block |
| `commit_id` | increases by one per commit |
| `slot_count` | how many slots this transaction claimed, the block included |
| `db_size` | database size in pages after this commit |
| `prev_block` | slot of the previous commit block |
| entries | one `(page_no, slot, body_crc)` per page in this transaction |
| `crc` | CRC over the whole block |

At a 4 KiB page size a block holds about 340 entries. A transaction that dirties
more pages than one block holds claims more blocks, chained by `prev_block`.

## Recovery

Each commit block says how many slots its transaction claimed, and the block is
the first slot of that run. So the next block sits at `this_slot + slot_count`.
Recovery walks that chain forward:

1. Read the anchor from the database header: the slot of the newest commit block
   known at the last compaction.
2. Read the block at that slot. Check the magic, the format and the CRC. Check
   that the generation salt matches the header, and that `commit_id` is exactly
   one more than the previous block's. A leftover block from an earlier
   generation can sit at the right slot and still pass its own CRC, so these two
   checks are what reject it.
3. Read and CRC every body the block names. If one fails, stop.
4. Apply the block's entries to the map, take its `db_size`, and move to
   `this_slot + slot_count`.
5. Stop at the first slot that does not hold a valid block.

The walk reads one block per transaction plus the bodies it has to verify, and
it stops at the first gap. The anchor is an optimization: a stale anchor only
makes the walk longer. Correctness comes from the walk. The anchor is only ever
written after a compaction sync, so it can never point past real data.

## Reads

`max_frame` keeps the meaning it has in WAL mode: the highest log position this
connection may see. Here a position is `(commit_id, entry index)`. A reader takes
it at `begin_read_tx` and holds it for the transaction.

Resolving a page is one in-memory lookup and one read:

```
page_no --(map)--> slot --> pread at slot * page_size
```

The map is rebuilt by the recovery walk and maintained at commit, exactly as the
WAL index is today. A page the map does not name has not been rewritten since
the last compaction, so it is read from its canonical slot.

A read costs one `pread` of the database file, which is the I/O count a WAL
frame read costs today.

## Space reclaim

Slots are claimed in contiguous runs and are never reused piecemeal, so there is
no free list. A slot stops being useful when a newer version of its page is
committed and no live reader can still read the old version. That is the
read-mark rule the WAL index already implements: the floor is the lowest
position any live reader holds.

Dead slots are reclaimed only by compaction. Dropping the free list keeps the
runs contiguous, which is what makes the forward walk in recovery work.

## Compaction

The tail grows forever if nothing moves pages back, so compaction plays the role
checkpointing plays in WAL mode:

1. Move any live body that sits below the new `db_size` up to a free high slot.
   After this pass every source slot is at or above `db_size`, so the next pass
   cannot overwrite a body it has not copied yet.
2. Copy every live body from its tail slot into canonical slot `page_no - 1`.
3. Sync the database file.
4. Write the new anchor and an empty map into the header, sync, and truncate the
   tail away.

The copy is database-file to database-file, so it can use `copy_file_range` and
never crosses into another file. The pressure to run it at all is far lower than
the pressure to checkpoint a WAL, because the thing that grows is a few bytes
per page rather than a page per page.

## What this costs

**Locality.** This is the real price. Pages that are logically next to each other
scatter across the tail in write order, so a range scan that used to be one
sequential run becomes random reads until compaction puts the pages back.

**File size.** The file is larger than the data by the size of the live tail.

**A CRC per page at commit.** Hardware CRC32C runs at several GB/s, so this is
well under a microsecond for a 4 KiB page. Turso already checksums every WAL
frame body today, so this is not new work.

## Where the 2x becomes 1x: the map as a b-tree

Compaction as described still writes each page body twice: once into the tail,
once back into the canonical slot. The win so far is only the size of the log.

The larger prize is to stop copying bodies back. Then compaction only has to
fold the pointers into something durable, and a page body is written once.

That durable something wants to be a b-tree in the database file, keyed by page
number. The obvious objection is that it eats its own tail: to find the pages of
the map you need the map.

Copy-on-write breaks that. Writing to the map builds new pages from the leaf up
to the root, and the only thing that needs an update in place is one root
pointer. Keep two copies of the root pointer in the file header, write them
alternately, and give each a commit id and a CRC. Recovery reads both and takes
the newer valid one. A crash during that write leaves the older copy intact, so
nothing is ever half-updated.

This is the arrangement LMDB uses, and it is worth being clear about what it
means: **with a copy-on-write map, the pointer log is unnecessary.** A commit is
write the bodies, write the new map path, flip the root. Recovery is two header
reads and no scan at all.

It is not free. Every commit rewrites the map from leaf to root, so a
transaction that dirties one page writes that page plus three or four map pages.
For small transactions that is worse than the log, and small transactions are
the common case.

So the two structures are good at opposite things, and the useful arrangement is
both:

| | commit blocks | b-tree map |
|---|---|---|
| bytes per dirty page at commit | 12 | one page per tree level |
| recovery | forward walk over the tail | two header reads |
| needs periodic folding | yes | no |

The commit blocks take the writes. The b-tree map is where they are folded, in
place of copying bodies. Compaction stops moving pages and starts moving
pointers only, which is where 2x becomes 1x.

One thing makes the b-tree cheaper here than the numbers suggest: the map only
names pages that are **not** in their canonical slot. Right after a fold it is
empty, and it never has to describe the whole database. A shallow tree means the
copy-on-write path is one or two pages, not four.

### Can it be an actual SQLite table?

A b-tree in the file, yes. A table in the schema, no.

The map has to be readable by the code that resolves a page read, and a table in
the schema is resolved through the pager, which is the thing asking. Map pages
must be found through the copy-on-write root instead, so this is the b-tree
layer without the SQL layer above it.

Exposing it read-only as a virtual table, so `SELECT * FROM turso_page_map`
works, costs almost nothing and is worth having for debugging. The storage path
must not go through it.

SQLite already has a version of this. An auto-vacuum database keeps **ptrmap
pages**: pages inside the database file whose only job is to record where other
pages are. They are updated in place, and they are crash-safe only because the
journal or the WAL covers them. That is the same conclusion this document starts
from, reached from the other direction.

## What is not built yet

- The durable map described directly above, so compaction still copies bodies.
- Group commit, which shares one sync across several transactions.
- More than one writer, and coordination across processes.
- VACUUM, MVCC, the sync engine, and encryption do not know about the
  indirection.
