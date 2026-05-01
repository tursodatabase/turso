//! STRUCT and UNION type overhead benchmarks
//!
//! Each benchmark group compares a struct/union operation against the
//! equivalent flat-column operation on identical data, so the delta
//! isolates the cost of blob deserialization / field extraction.
//!
//! Run with: cargo bench --bench struct_union_benchmark

#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
#[cfg(not(feature = "codspeed"))]
use pprof::criterion::{Output, PProfProfiler};

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion,
};

use std::sync::Arc;
use turso_core::io::MemoryIO;
use turso_core::{Database, DatabaseOpts, OpenFlags, StepResult};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const N: usize = 10_000;

fn setup_db() -> (Arc<Database>, Arc<turso_core::Connection>) {
    let opts = DatabaseOpts::new().with_custom_types(true);
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(MemoryIO::new());
    let db =
        Database::open_file_with_flags(io, ":memory:", OpenFlags::default(), opts, None).unwrap();
    let conn = db.connect().unwrap();
    (db, conn)
}

fn execute(db: &Database, conn: &Arc<turso_core::Connection>, sql: &str) {
    let mut stmt = conn.prepare(sql).unwrap();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {}
            StepResult::IO => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
}

fn run_to_completion(db: &Database, stmt: &mut turso_core::Statement) {
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                black_box(stmt.row());
            }
            StepResult::IO => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
    stmt.reset().unwrap();
}

// ── SELECT one field: flat column vs struct field ────────────────────────
//
// flat:   SELECT x FROM t          (read integer directly from record)
// struct: SELECT val.x FROM t      (read blob, deserialize, extract field)

fn bench_select_one_field(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("select_one_field");

    // --- flat baseline ---
    let (db, conn) = setup_db();
    execute(
        &db,
        &conn,
        "CREATE TABLE flat1(x INTEGER, y INTEGER) STRICT",
    );
    for i in 1..=N {
        execute(
            &db,
            &conn,
            &format!("INSERT INTO flat1 VALUES ({i}, {})", i * 2),
        );
    }
    let mut flat_stmt = conn.prepare("SELECT x FROM flat1").unwrap();

    group.bench_function(BenchmarkId::new("flat", ""), |b| {
        b.iter(|| run_to_completion(&db, &mut flat_stmt));
    });

    // --- struct ---
    let (db2, conn2) = setup_db();
    execute(&db2, &conn2, "CREATE TYPE point1 AS STRUCT(x INT, y INT)");
    execute(&db2, &conn2, "CREATE TABLE struct1(val point1) STRICT");
    for i in 1..=N {
        execute(
            &db2,
            &conn2,
            &format!("INSERT INTO struct1 VALUES (struct_pack({i}, {}))", i * 2),
        );
    }
    let mut struct_stmt = conn2.prepare("SELECT val.x FROM struct1").unwrap();

    group.bench_function(BenchmarkId::new("struct", ""), |b| {
        b.iter(|| run_to_completion(&db2, &mut struct_stmt));
    });

    group.finish();
}

// ── SELECT two fields: flat columns vs struct fields ────────────────────
//
// flat:   SELECT x, y FROM t
// struct: SELECT val.x, val.y FROM t

fn bench_select_two_fields(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("select_two_fields");

    let (db, conn) = setup_db();
    execute(
        &db,
        &conn,
        "CREATE TABLE flat2(x INTEGER, y INTEGER) STRICT",
    );
    for i in 1..=N {
        execute(
            &db,
            &conn,
            &format!("INSERT INTO flat2 VALUES ({i}, {})", i * 2),
        );
    }
    let mut flat_stmt = conn.prepare("SELECT x, y FROM flat2").unwrap();

    group.bench_function(BenchmarkId::new("flat", ""), |b| {
        b.iter(|| run_to_completion(&db, &mut flat_stmt));
    });

    let (db2, conn2) = setup_db();
    execute(&db2, &conn2, "CREATE TYPE point2 AS STRUCT(x INT, y INT)");
    execute(&db2, &conn2, "CREATE TABLE struct2(val point2) STRICT");
    for i in 1..=N {
        execute(
            &db2,
            &conn2,
            &format!("INSERT INTO struct2 VALUES (struct_pack({i}, {}))", i * 2),
        );
    }
    let mut struct_stmt = conn2.prepare("SELECT val.x, val.y FROM struct2").unwrap();

    group.bench_function(BenchmarkId::new("struct", ""), |b| {
        b.iter(|| run_to_completion(&db2, &mut struct_stmt));
    });

    group.finish();
}

// ── WHERE filter: flat column vs struct field ───────────────────────────
//
// flat:   SELECT x FROM t WHERE x > 5000
// struct: SELECT val.x FROM t WHERE val.x > 5000
// Same data, same selectivity (~50%), isolates deserialization cost in the filter path.

fn bench_where_filter(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("where_filter");

    let (db, conn) = setup_db();
    execute(
        &db,
        &conn,
        "CREATE TABLE flat_w(x INTEGER, y INTEGER) STRICT",
    );
    for i in 1..=N {
        execute(
            &db,
            &conn,
            &format!("INSERT INTO flat_w VALUES ({i}, {})", i * 2),
        );
    }
    let mut flat_stmt = conn.prepare("SELECT x FROM flat_w WHERE x > 5000").unwrap();

    group.bench_function(BenchmarkId::new("flat", ""), |b| {
        b.iter(|| run_to_completion(&db, &mut flat_stmt));
    });

    let (db2, conn2) = setup_db();
    execute(&db2, &conn2, "CREATE TYPE point_w AS STRUCT(x INT, y INT)");
    execute(&db2, &conn2, "CREATE TABLE struct_w(val point_w) STRICT");
    for i in 1..=N {
        execute(
            &db2,
            &conn2,
            &format!("INSERT INTO struct_w VALUES (struct_pack({i}, {}))", i * 2),
        );
    }
    let mut struct_stmt = conn2
        .prepare("SELECT val.x FROM struct_w WHERE val.x > 5000")
        .unwrap();

    group.bench_function(BenchmarkId::new("struct", ""), |b| {
        b.iter(|| run_to_completion(&db2, &mut struct_stmt));
    });

    group.finish();
}

// ── INSERT: flat row vs struct_pack row ─────────────────────────────────
//
// flat:   INSERT INTO t VALUES (id, x, y)
// struct: INSERT INTO t VALUES (id, struct_pack(x, y))

fn bench_insert(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("insert");

    let (db, conn) = setup_db();
    execute(
        &db,
        &conn,
        "CREATE TABLE flat_ins(id INTEGER PRIMARY KEY, x INTEGER, y INTEGER) STRICT",
    );
    let mut flat_id = 1i64;
    group.bench_function(BenchmarkId::new("flat", ""), |b| {
        b.iter(|| {
            execute(
                &db,
                &conn,
                &format!(
                    "INSERT INTO flat_ins VALUES ({flat_id}, {flat_id}, {})",
                    flat_id * 2
                ),
            );
            flat_id += 1;
        });
    });

    let (db2, conn2) = setup_db();
    execute(
        &db2,
        &conn2,
        "CREATE TYPE point_ins AS STRUCT(x INT, y INT)",
    );
    execute(
        &db2,
        &conn2,
        "CREATE TABLE struct_ins(id INTEGER PRIMARY KEY, val point_ins) STRICT",
    );
    let mut struct_id = 1i64;
    group.bench_function(BenchmarkId::new("struct", ""), |b| {
        b.iter(|| {
            execute(
                &db2,
                &conn2,
                &format!(
                    "INSERT INTO struct_ins VALUES ({struct_id}, struct_pack({struct_id}, {}))",
                    struct_id * 2
                ),
            );
            struct_id += 1;
        });
    });

    group.finish();
}

// ── Union extract vs flat column read ──────────────────────────────────
//
// flat:   SELECT x FROM t                 (read integer column)
// union:  SELECT val.i FROM t             (read blob, check tag, extract value)
// All rows have the same tag so every extract succeeds.

fn bench_union_extract(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("union_extract");

    let (db, conn) = setup_db();
    execute(&db, &conn, "CREATE TABLE flat_u(x INTEGER) STRICT");
    for i in 1..=N {
        execute(&db, &conn, &format!("INSERT INTO flat_u VALUES ({i})"));
    }
    let mut flat_stmt = conn.prepare("SELECT x FROM flat_u").unwrap();

    group.bench_function(BenchmarkId::new("flat", ""), |b| {
        b.iter(|| run_to_completion(&db, &mut flat_stmt));
    });

    let (db2, conn2) = setup_db();
    execute(&db2, &conn2, "CREATE TYPE number_u AS UNION(i INT, f REAL)");
    execute(&db2, &conn2, "CREATE TABLE union_u(val number_u) STRICT");
    for i in 1..=N {
        execute(
            &db2,
            &conn2,
            &format!("INSERT INTO union_u VALUES (union_value('i', {i}))"),
        );
    }
    let mut union_stmt = conn2.prepare("SELECT val.i FROM union_u").unwrap();

    group.bench_function(BenchmarkId::new("union", ""), |b| {
        b.iter(|| run_to_completion(&db2, &mut union_stmt));
    });

    group.finish();
}

// ── Union tag scan vs typeof() on flat column ──────────────────────────
//
// flat:   SELECT typeof(x) FROM t         (return type string for a column)
// union:  SELECT union_tag(val) FROM t    (read blob, extract tag name)

fn bench_union_tag(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("union_tag");

    let (db, conn) = setup_db();
    execute(&db, &conn, "CREATE TABLE flat_t(x INTEGER) STRICT");
    for i in 1..=N {
        execute(&db, &conn, &format!("INSERT INTO flat_t VALUES ({i})"));
    }
    let mut flat_stmt = conn.prepare("SELECT typeof(x) FROM flat_t").unwrap();

    group.bench_function(BenchmarkId::new("typeof_flat", ""), |b| {
        b.iter(|| run_to_completion(&db, &mut flat_stmt));
    });

    let (db2, conn2) = setup_db();
    execute(&db2, &conn2, "CREATE TYPE number_t AS UNION(i INT, f REAL)");
    execute(&db2, &conn2, "CREATE TABLE union_t(val number_t) STRICT");
    for i in 1..=N {
        if i % 2 == 0 {
            execute(
                &db2,
                &conn2,
                &format!("INSERT INTO union_t VALUES (union_value('i', {i}))"),
            );
        } else {
            execute(
                &db2,
                &conn2,
                &format!("INSERT INTO union_t VALUES (union_value('f', {i}.5))"),
            );
        }
    }
    let mut union_stmt = conn2.prepare("SELECT union_tag(val) FROM union_t").unwrap();

    group.bench_function(BenchmarkId::new("union_tag", ""), |b| {
        b.iter(|| run_to_completion(&db2, &mut union_stmt));
    });

    group.finish();
}

// ── criterion wiring ───────────────────────────────────────────────────

#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = grp_select_one;
    config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
    targets = bench_select_one_field
}
#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = grp_select_two;
    config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
    targets = bench_select_two_fields
}
#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = grp_where;
    config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
    targets = bench_where_filter
}
#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = grp_insert;
    config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
    targets = bench_insert
}
#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = grp_union_extract;
    config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
    targets = bench_union_extract
}
#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = grp_union_tag;
    config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
    targets = bench_union_tag
}

#[cfg(feature = "codspeed")]
criterion_group!(grp_select_one, bench_select_one_field);
#[cfg(feature = "codspeed")]
criterion_group!(grp_select_two, bench_select_two_fields);
#[cfg(feature = "codspeed")]
criterion_group!(grp_where, bench_where_filter);
#[cfg(feature = "codspeed")]
criterion_group!(grp_insert, bench_insert);
#[cfg(feature = "codspeed")]
criterion_group!(grp_union_extract, bench_union_extract);
#[cfg(feature = "codspeed")]
criterion_group!(grp_union_tag, bench_union_tag);

criterion_main!(
    grp_select_one,
    grp_select_two,
    grp_where,
    grp_insert,
    grp_union_extract,
    grp_union_tag,
);
