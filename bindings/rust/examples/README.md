# Rust Examples

```bash
cargo run --package turso --example example
cargo run --package turso --example example_struct
cargo run --package turso --example concurrent_writes
cargo run --package turso --example sync_example --features sync  # requires Turso Cloud
```

## Examples

| Example | Description |
|---------|-------------|
| `example` | Basic queries, prepared statements, and pragma usage |
| `example_struct` | Mapping rows to structs using transactions |
| `concurrent_writes` | MVCC mode: 16 concurrent writers using `BEGIN CONCURRENT` |
| `sync_example` | Syncing with Turso Cloud (set `TURSO_REMOTE_URL` / `TURSO_AUTH_TOKEN`) |
