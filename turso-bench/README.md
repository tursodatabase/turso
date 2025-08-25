# TursoDB Benchmark Tool

A high-performance benchmarking tool for TursoDB, compatible with mobibench functionality but using TursoDB's core database engine instead of SQLite.

## Features

- **SQLite-compatible Operations**: Insert, Update, Delete operations
- **Multi-threading**: Support for concurrent database operations
- **Performance Metrics**: Latency measurement, IOPS tracking, throughput analysis
- **Flexible Configuration**: Multiple databases, tables, and transaction patterns
- **TursoDB Integration**: Uses TursoDB's core engine with MVCC, indexes, and views support

## Installation

1. Clone the repository and navigate to the turso-bench directory:
```bash
cd turso-bench
```

2. Build the project:
```bash
cargo build --release
```

3. Run the benchmark:
```bash
cargo run --release -- --help
```

## Usage

### Basic Usage

```bash
# Run a simple insert benchmark
./target/release/turso-bench -a 0 -n 1000

# Run update operations with 4 threads
./target/release/turso-bench -a 1 -t 4 -n 500

# Run delete operations with latency measurement
./target/release/turso-bench -a 2 -n 1000 -L latency.txt
```

### Command Line Options

```
Usage: turso-bench [OPTIONS]

Options:
  -p, --path <PATH>                Database path [default: ./turso-bench]
  -f, --file-size-kb <FILE_SIZE_KB>    File size per thread in KB [default: 1024]
  -r, --record-size-kb <RECORD_SIZE_KB> Record size in KB [default: 4]
  -a, --access-mode <ACCESS_MODE>      Access mode: 0=Insert, 1=Update, 2=Delete [default: 0]
  -t, --num-threads <NUM_THREADS>      Number of threads [default: 1]
  -n, --transactions <TRANSACTIONS>    Number of database transactions per thread [default: 10]
  -T, --num-tables <NUM_TABLES>        Number of tables to create [default: 3]
  -D, --num-databases <NUM_DATABASES>  Number of databases [default: 1]
  -i, --interval-ms <INTERVAL_MS>      Time interval between transactions in milliseconds [default: 0]
  -q, --quiet                          Enable quiet mode (no progress output)
  -L, --latency-file <LATENCY_FILE>    Enable latency measurement and output to file
  -k, --iops-file <IOPS_FILE>          Enable IOPS measurement and output to file
  -v, --overlap-ratio <OVERLAP_RATIO>  Overlap ratio for random operations (0-100%) [default: 0]
  -R, --random-insert                  Use random insert order
      --enable-mvcc                    Enable MVCC (Multi-Version Concurrency Control)
      --enable-indexes                 Enable indexes [default: true]
      --enable-views                   Enable views
  -h, --help                           Print help
  -V, --version                        Print version
```

### Examples

#### 1. Basic Insert Performance Test
```bash
./target/release/turso-bench \
    --access-mode 0 \
    --transactions 10000 \
    --num-threads 1 \
    --path "./test.db"
```

#### 2. Multi-threaded Update Benchmark
```bash
./target/release/turso-bench \
    --access-mode 1 \
    --transactions 5000 \
    --num-threads 8 \
    --num-tables 5 \
    --latency-file "update_latency.txt"
```

#### 3. In-Memory Database Test
```bash
./target/release/turso-bench \
    --access-mode 0 \
    --transactions 50000 \
    --num-threads 4 \
    --path ":memory:" \
    --enable-mvcc
```

#### 4. Comprehensive Benchmark with All Metrics
```bash
./target/release/turso-bench \
    --access-mode 0 \
    --transactions 10000 \
    --num-threads 16 \
    --num-tables 10 \
    --num-databases 3 \
    --latency-file "comprehensive_latency.txt" \
    --iops-file "comprehensive_iops.txt" \
    --enable-mvcc \
    --enable-views
```

## Output

The benchmark tool provides detailed performance metrics:

```
-----------------------------------------
[TursoDB Benchmark Configuration]
-----------------------------------------
Operation Mode: Insert (0)
Database Path: ./turso-bench
Number of Threads: 4
Transactions per Thread: 1000
Number of Tables: 3
Number of Databases: 1
MVCC: Enabled
Indexes: Enabled
Views: Disabled

-----------------------------------------
[Measurement Result]
-----------------------------------------
[TIME] : 2.345 sec. 1705.32 Inserts/sec
Total operations: 4000 across 4 threads (426.33 ops/thread/sec)
[LATENCY] Min: 1.23ms, Max: 45.67ms, Avg: 2.34ms
```

## Performance Tuning

### For Maximum Throughput
- Use multiple threads (`-t`)
- Enable MVCC for better concurrency (`--enable-mvcc`)
- Use in-memory databases for CPU-bound tests (`-p :memory:`)

### For Latency Testing
- Use single thread (`-t 1`)
- Enable latency logging (`-L latency.txt`)
- Reduce transaction interval (`-i 0`)

### For Stress Testing
- Increase transaction count (`-n`)
- Use multiple databases (`-D`)
- Enable all features (`--enable-mvcc --enable-views`)

## Comparison with mobibench

This tool is designed to be compatible with mobibench workflows while providing the performance benefits of TursoDB:

| Feature | mobibench | turso-bench |
|---------|-----------|-------------|
| Database Engine | SQLite | TursoDB |
| Operations | Insert/Update/Delete | Insert/Update/Delete |
| Threading | ✓ | ✓ |
| Latency Measurement | ✓ | ✓ |
| IOPS Tracking | ✓ | ✓ |
| MVCC Support | - | ✓ |
| Views Support | - | ✓ |
| Async I/O | - | ✓ |

## Dependencies

- Rust 1.70+
- TursoDB core library
- Tokio async runtime

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.