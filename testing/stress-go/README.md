# Limbo Stress Test (Go)

A Go stress testing application for the Limbo SQLite driver using Gorm ORM.

## Prerequisites

- Go 1.24+
- The `libturso_sync_sdk_kit` native library (built from the parent Turso project)

## Building

```bash
# Build the native library (from limbo root)
cd ..
cargo build --release -p turso_sync_sdk_kit

# Build the stress test
cd stress-go
go build -o stress-go .
```

## Running

```bash
# Set library path and run
DYLD_LIBRARY_PATH=../target/release ./stress-go

# Capture full output to a file (recommended for debugging panics)
DYLD_LIBRARY_PATH=../target/release ./stress-go 2>&1 | tee stress.log
```

On Linux, use `LD_LIBRARY_PATH` instead of `DYLD_LIBRARY_PATH`.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_PATH` | `stress_test.db` | Path to the SQLite database file |
| `PORT` | `8080` | HTTP server port |
| `NUM_WORKERS` | `10` | Number of concurrent stress workers |
| `CHECKPOINT_INTERVAL_MS` | `1000` | Interval between WAL checkpoints (ms) |

## HTTP Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/insert` | POST | Insert a single random record |
| `/update` | POST | Update a random existing record |
| `/delete` | POST | Delete a random record |
| `/select` | GET | Select 10 random records by ID |
| `/bulk` | POST | Insert 100 records in a transaction |
| `/random` | GET/POST | Perform a random operation |
| `/stats` | GET | Get operation statistics |
| `/health` | GET | Health check |

## How It Works

1. **HTTP Server** - Serves CRUD endpoints for database operations
2. **Stress Workers** - Background goroutines that continuously call HTTP endpoints with weighted probability
3. **Checkpoint Worker** - Periodically runs `PRAGMA wal_checkpoint()` with random modes (TRUNCATE, RESTART, FULL, PASSIVE)
4. **Logging** - All SQL statements are logged with worker ID and execution time

## Example Output

```
2024/01/15 10:30:00 Database initialized at stress_test.db
2024/01/15 10:30:00 Checkpoint interval: 1s
2024/01/15 10:30:00 Server starting on port 8080
2024/01/15 10:30:00 Starting 10 stress workers
[worker-3] [1.234ms] [rows:1] INSERT INTO `records` ...
[worker-7] [0.567ms] [rows:0] [ERROR: record not found] SELECT * FROM `records` WHERE ...
[checkpoint] Executing: PRAGMA wal_checkpoint(TRUNCATE)
[checkpoint] Success: TRUNCATE
```
