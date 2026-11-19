---
name: transaction-correctness
description: How WAL mechanics, checkpointing, concurrency rules, recovery work in tursodb
---
# Transaction correctness

Journal modes: `wal` (default; files `.db`, `.db-wal`) and `mvcc` (`.db-log`, see the mvcc skill). There is no `.db-shm`: the WAL index is in memory. With `--experimental-multiprocess-wal`, processes coordinate through a `.tshm` file.

## WAL

- A writer appends frames to the WAL. A commit is a frame with a non-zero db size in its header. The main DB file is unchanged until a checkpoint.
- A reader takes a read mark (the last valid commit frame). For each page it looks in the WAL up to that frame, then in the main DB.
- One writer at a time. Readers and the writer do not block each other.
- A checkpoint cannot copy past frames that an active reader still needs.
- A commit is fsynced before success is returned. Readers never see a partial transaction.

## Checkpoint modes

Auto-checkpoint starts when the WAL reaches 1000 frames. It is set per connection with `PRAGMA wal_autocheckpoint`.

- PASSIVE: never blocks readers or writers; stops at frames that active readers need.
- FULL: copies all frames to the DB file; blocks new writers while pending.
- RESTART: like FULL, then waits until all readers use the DB file only, so the next writer restarts the WAL from the beginning.
- TRUNCATE: like RESTART, then truncates the WAL file to zero bytes.

## Recovery

When a database is opened, the WAL file is scanned: frame checksums are verified and the frame index is rebuilt up to the last valid commit frame. Frames after that are ignored.
