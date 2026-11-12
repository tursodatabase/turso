//! Benchmarks one blocking MVCC checkpoint of a database whose MVCC store holds N
//! rows that no checkpoint has written. The inserts of these rows are setup and are
//! not measured.
//!
//! `PRAGMA wal_checkpoint(TRUNCATE)` starts the checkpoint in the `table` and
//! `table-with-index` groups. The commit of an `INSERT` of one row starts it in the
//! `after-insert` group.

use std::hint::black_box;
use std::sync::Arc;
use turso_sqlite::SqliteDialect;

#[cfg(not(feature = "codspeed"))]
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};

use turso_core::{ConnectOpts, Connection, Database, DatabaseOpts, OpenFlags, PlatformIO};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const ROWS_PER_INSERT: u64 = 1000;

/// A file-backed MVCC database with rows that no checkpoint has written.
struct UncheckpointedRows {
    _temp_dir: tempfile::TempDir,
    _db: Arc<Database>,
    conn: Arc<Connection>,
}

impl UncheckpointedRows {
    fn checkpoint(self) -> Self {
        self.conn
            .execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        self
    }

    /// Makes each commit that writes to the logical log start a checkpoint.
    fn checkpoint_after_each_commit(self) -> Self {
        self.conn
            .execute("PRAGMA mvcc_checkpoint_threshold = 0")
            .unwrap();
        self
    }

    fn insert_one_row(self, id: u64) -> Self {
        self.conn
            .execute(format!("INSERT INTO t(id, v) VALUES ({id}, {id})"))
            .unwrap();
        self
    }
}

fn insert_rows(num_rows: u64, create_table: &str) -> UncheckpointedRows {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let path = temp_dir
        .path()
        .join("checkpoint.db")
        .to_str()
        .unwrap()
        .to_string();
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        &path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")
        .unwrap();
    conn.execute("PRAGMA synchronous = OFF").unwrap();
    conn.execute(create_table).unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    let mut first = 0;
    while first < num_rows {
        let end = (first + ROWS_PER_INSERT).min(num_rows);
        let values = (first..end)
            .map(|id| format!("({id}, {id})"))
            .collect::<Vec<_>>()
            .join(",");
        conn.execute(format!("INSERT INTO t(id, v) VALUES {values}"))
            .unwrap();
        first = end;
    }
    conn.execute("COMMIT").unwrap();

    UncheckpointedRows {
        _temp_dir: temp_dir,
        _db: db,
        conn,
    }
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_checkpoint(c: &mut Criterion) {
    let cases = [
        (
            "mvcc-checkpoint/table",
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
            &[1_000, 10_000, 100_000][..],
        ),
        (
            "mvcc-checkpoint/table-with-index",
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER UNIQUE)",
            &[1_000, 10_000][..],
        ),
    ];
    for (group_name, create_table, row_counts) in cases {
        let mut group = c.benchmark_group(group_name);
        for &num_rows in row_counts {
            group.throughput(Throughput::Elements(num_rows));
            group.bench_with_input(
                BenchmarkId::from_parameter(num_rows),
                &num_rows,
                |b, &num_rows| {
                    b.iter_batched(
                        || insert_rows(num_rows, create_table),
                        |rows| black_box(rows.checkpoint()),
                        BatchSize::PerIteration,
                    )
                },
            );
        }
        group.finish();
    }

    let mut group = c.benchmark_group("mvcc-checkpoint/after-insert");
    for num_rows in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(num_rows));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_rows),
            &num_rows,
            |b, &num_rows| {
                b.iter_batched(
                    || {
                        insert_rows(
                            num_rows,
                            "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)",
                        )
                        .checkpoint_after_each_commit()
                    },
                    |rows| black_box(rows.insert_one_row(num_rows)),
                    BatchSize::PerIteration,
                )
            },
        );
    }
    group.finish();
}

#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10);
    targets = bench_checkpoint
}

#[cfg(feature = "codspeed")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_checkpoint
}

criterion_main!(benches);
