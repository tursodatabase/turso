# MVCC FTS writer investigation

Build the MVCC REPL and feed it the two-connection scenario:

```bash
cargo run -p turso_cli --features mvcc_repl -- \
  --mvcc --experimental-index-method /tmp/turso-fts-mvcc.db \
  < examples/fts/mvcc-repl.txt
```

Both connections start at independent MVCC snapshots and try to write the same
FTS index. The second insert reports a write-write conflict before constructing
a Tantivy writer and rolls back its complete transaction, including the
base-table row. The script commits the first transaction, starts a new
transaction on the second connection, replays its insert, and checks base-table
and FTS visibility from both connections.

Use a fresh database path for each run.
