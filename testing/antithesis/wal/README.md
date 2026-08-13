# wal: WAL commit + checkpoint + restart workload

This template runs two copies of a small dedicated workload binary
(`wal_workload`, source in `src/main.rs`) as **separate processes** over one
shared database file opened with `experimental_multiprocess_wal`. It is
modeled on the workload that caught the upstream SQLite WAL-reset bug
(<https://antithesis.com/blog/2026/wal-reset-bug/>): each writer runs a
continuous mix of small write transactions (~70%), WAL checkpoints in all four
modes (~20%, PASSIVE-heavy), and correctness sweeps (~10%).

## The lost-write oracle

Each write transaction atomically commits, in one transaction:

- a new `t(writer, seq)` row with `seq = committed_seq + 1`,
- a bump of the per-writer `ctr` counter,
- updates to a few `churn` rows (so the WAL keeps growing and checkpoints
  have real work), and
- `progress.committed_seq = seq` — the durable watermark.

Because data and watermark commit together, `COUNT(*)` and `MAX(seq)` for a
writer must **exactly equal** the watermark at every commit point, across
crashes and restarts. A checkpoint that loses or skips WAL frames (the
WAL-reset bug class: a stale backfill count published into a restarted WAL
generation) breaks that equality — or the read-your-writes check right after
commit — long before `PRAGMA integrity_check` notices anything.

Writers are namespaced by `WRITER_ID`, so the oracle needs no cross-process
coordination and stays exact under arbitrary interleaving.

## Files

- `first_setup.sh` — creates the database and schema (`wal_workload init`).
- `singleton_driver_writer_a.sh` / `singleton_driver_writer_b.sh` — the two
  writer processes (`WRITER_ID=0/1 wal_workload run`).
- `finally_validate.sh` — final integrity check plus the per-writer
  no-lost-writes check straight from the durable `progress` table
  (`wal_workload validate`).

## Running locally

```sh
cargo build -p turso_wal_workload
export DB_PATH=/tmp/wal-workload.db
target/debug/wal_workload init
WRITER_ID=0 target/debug/wal_workload run &   # terminal 1
WRITER_ID=1 target/debug/wal_workload run &   # terminal 2
# later:
target/debug/wal_workload validate
```

Without Antithesis the SDK assertions fall back to local evaluation; set
`ANTITHESIS_SDK_LOCAL_OUTPUT=/tmp/wal-workload-sdk.jsonl` to capture assertion
verdicts (look for `"condition":false`).
