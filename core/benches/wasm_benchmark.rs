use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use std::sync::Arc;
use turso_core::io::MemoryIO;
use turso_core::{Database, StepResult};

/// WAT source for add(a, b) -> a + b.
/// Integer args are passed as raw i64 in argv slots — minimal WASM overhead.
/// 64 pages (4MB) so the bump allocator handles thousands of calls per sample.
const ADD_WAT: &str = r#"
(module
  (memory (export "memory") 64)
  (global $bump (mut i32) (i32.const 1024))
  (func (export "turso_malloc") (param $size i32) (result i32)
    (local $ptr i32)
    global.get $bump
    local.set $ptr
    global.get $bump
    local.get $size
    i32.add
    global.set $bump
    local.get $ptr
  )
  (func (export "add") (param $argc i32) (param $argv i32) (result i64)
    ;; Reset bump allocator each call to avoid OOM across many iterations
    i32.const 1024
    global.set $bump
    local.get $argv
    i64.load
    local.get $argv
    i32.const 8
    i32.add
    i64.load
    i64.add
  )
)
"#;

/// WAT source for strlen(s) -> byte count.
/// Text args are marshalled as pointers to [TAG_TEXT][utf8 bytes][0x00].
/// Skips the tag byte, counts bytes until null terminator, returns count as raw i64.
const STRLEN_WAT: &str = r#"
(module
  (memory (export "memory") 64)
  (global $bump (mut i32) (i32.const 1024))
  (func (export "turso_malloc") (param $size i32) (result i32)
    (local $ptr i32)
    global.get $bump
    local.set $ptr
    global.get $bump
    local.get $size
    i32.add
    global.set $bump
    local.get $ptr
  )
  (func (export "strlen") (param $argc i32) (param $argv i32) (result i64)
    (local $ptr i32)
    (local $count i64)
    ;; Reset bump allocator each call to avoid OOM across many iterations
    i32.const 1024
    global.set $bump
    ;; Load pointer from argv[0]
    local.get $argv
    i64.load
    i32.wrap_i64
    local.set $ptr
    ;; Skip the TAG_TEXT byte (offset +1)
    local.get $ptr
    i32.const 1
    i32.add
    local.set $ptr
    ;; Count bytes until null terminator
    i64.const 0
    local.set $count
    (block $break
      (loop $loop
        local.get $ptr
        i32.load8_u
        i32.eqz
        br_if $break
        local.get $count
        i64.const 1
        i64.add
        local.set $count
        local.get $ptr
        i32.const 1
        i32.add
        local.set $ptr
        br $loop
      )
    )
    local.get $count
  )
)
"#;

const N: usize = 10000;

fn wat_to_wasm(wat: &str) -> Vec<u8> {
    wat::parse_str(wat).expect("Failed to parse WAT")
}

fn setup_memory_db() -> (Arc<Database>, Arc<turso_core::Connection>) {
    setup_memory_db_with_opts(turso_core::DatabaseOpts::new())
}

fn setup_memory_db_with_wasm() -> (Arc<Database>, Arc<turso_core::Connection>) {
    let runtime = turso_wasm_wasmtime::WasmtimeRuntime::new().unwrap();
    let opts = turso_core::DatabaseOpts::new().with_unstable_wasm_runtime(Arc::new(runtime));
    setup_memory_db_with_opts(opts)
}

fn setup_memory_db_with_opts(
    opts: turso_core::DatabaseOpts,
) -> (Arc<Database>, Arc<turso_core::Connection>) {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        turso_core::OpenFlags::default(),
        opts,
        None,
    )
    .unwrap();
    let conn = db.connect().unwrap();
    (db, conn)
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

fn register_wasm_add(db: &Database, conn: &Arc<turso_core::Connection>) {
    let wasm_bytes = wat_to_wasm(ADD_WAT);
    let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();
    execute(
        db,
        conn,
        &format!("CREATE FUNCTION wasm_add LANGUAGE wasm AS X'{hex}' EXPORT 'add'"),
    );
}

fn register_wasm_strlen(db: &Database, conn: &Arc<turso_core::Connection>) {
    let wasm_bytes = wat_to_wasm(STRLEN_WAT);
    let hex: String = wasm_bytes.iter().map(|b| format!("{b:02x}")).collect();
    execute(
        db,
        conn,
        &format!("CREATE FUNCTION wasm_strlen LANGUAGE wasm AS X'{hex}' EXPORT 'strlen'"),
    );
}

// ── Extension helpers ───────────────────────────────────────────────────────

const BENCH_EXT_WASM: &[u8] =
    include_bytes!("../../wasm-sdk/examples/bench-extension/bench_extension.wasm");

fn register_bench_extension(db: &Database, conn: &Arc<turso_core::Connection>) {
    let path = std::env::temp_dir().join("bench_extension.wasm");
    std::fs::write(&path, BENCH_EXT_WASM).unwrap();
    execute(
        db,
        conn,
        &format!(
            "CREATE EXTENSION bench_ext LANGUAGE wasm FROM '{}'",
            path.display()
        ),
    );
}

// ── UDF benchmarks (existing) ───────────────────────────────────────────────

fn bench_integer_add_native(criterion: &mut Criterion) {
    let (db, conn) = setup_memory_db();
    execute(&db, &conn, "CREATE TABLE data(a INTEGER, b INTEGER)");
    execute(
        &db,
        &conn,
        &format!("INSERT INTO data SELECT value, value * 2 FROM generate_series(1, {N})"),
    );
    let mut stmt = conn.prepare("SELECT a + b FROM data").unwrap();

    criterion.bench_function("integer_add_native", |b| {
        b.iter(|| run_to_completion(&db, &mut stmt));
    });
}

fn bench_integer_add_wasm(criterion: &mut Criterion) {
    let (db, conn) = setup_memory_db_with_wasm();
    execute(&db, &conn, "CREATE TABLE data(a INTEGER, b INTEGER)");
    execute(
        &db,
        &conn,
        &format!("INSERT INTO data SELECT value, value * 2 FROM generate_series(1, {N})"),
    );
    register_wasm_add(&db, &conn);
    let mut stmt = conn.prepare("SELECT wasm_add(a, b) FROM data").unwrap();

    criterion.bench_function("integer_add_wasm", |b| {
        b.iter(|| run_to_completion(&db, &mut stmt));
    });
}

fn bench_strlen_native(criterion: &mut Criterion) {
    let (db, conn) = setup_memory_db();
    execute(&db, &conn, "CREATE TABLE sdata(s TEXT)");
    execute(
        &db,
        &conn,
        &format!("INSERT INTO sdata SELECT 'hello_' || value FROM generate_series(1, {N})"),
    );
    let mut stmt = conn.prepare("SELECT length(s) FROM sdata").unwrap();

    criterion.bench_function("strlen_native", |b| {
        b.iter(|| run_to_completion(&db, &mut stmt));
    });
}

fn bench_strlen_wasm(criterion: &mut Criterion) {
    let (db, conn) = setup_memory_db_with_wasm();
    execute(&db, &conn, "CREATE TABLE sdata(s TEXT)");
    execute(
        &db,
        &conn,
        &format!("INSERT INTO sdata SELECT 'hello_' || value FROM generate_series(1, {N})"),
    );
    register_wasm_strlen(&db, &conn);
    let mut stmt = conn.prepare("SELECT wasm_strlen(s) FROM sdata").unwrap();

    criterion.bench_function("strlen_wasm", |b| {
        b.iter(|| run_to_completion(&db, &mut stmt));
    });
}

fn bench_filter_agg_native(criterion: &mut Criterion) {
    let (db, conn) = setup_memory_db();
    execute(&db, &conn, "CREATE TABLE data(a INTEGER, b INTEGER)");
    execute(
        &db,
        &conn,
        &format!("INSERT INTO data SELECT value, value * 2 FROM generate_series(1, {N})"),
    );
    let mut stmt = conn
        .prepare("SELECT count(*) FROM data WHERE a + b > 500")
        .unwrap();

    criterion.bench_function("filter_agg_native", |b| {
        b.iter(|| run_to_completion(&db, &mut stmt));
    });
}

fn bench_filter_agg_wasm(criterion: &mut Criterion) {
    let (db, conn) = setup_memory_db_with_wasm();
    execute(&db, &conn, "CREATE TABLE data(a INTEGER, b INTEGER)");
    execute(
        &db,
        &conn,
        &format!("INSERT INTO data SELECT value, value * 2 FROM generate_series(1, {N})"),
    );
    register_wasm_add(&db, &conn);
    let mut stmt = conn
        .prepare("SELECT count(*) FROM data WHERE wasm_add(a, b) > 500")
        .unwrap();

    criterion.bench_function("filter_agg_wasm", |b| {
        b.iter(|| run_to_completion(&db, &mut stmt));
    });
}

// ── Extension benchmarks (new) ──────────────────────────────────────────────

fn bench_ext_scalar_add(criterion: &mut Criterion) {
    let (db, conn) = setup_memory_db_with_wasm();
    execute(&db, &conn, "CREATE TABLE data(a INTEGER, b INTEGER)");
    execute(
        &db,
        &conn,
        &format!("INSERT INTO data SELECT value, value * 2 FROM generate_series(1, {N})"),
    );
    register_bench_extension(&db, &conn);
    let mut stmt = conn.prepare("SELECT bench_add(a, b) FROM data").unwrap();

    criterion.bench_function("ext_scalar_add", |b| {
        b.iter(|| run_to_completion(&db, &mut stmt));
    });
}

fn bench_vtab_native(criterion: &mut Criterion) {
    let (db, conn) = setup_memory_db();
    let mut stmt = conn
        .prepare(format!("SELECT value FROM generate_series(1, {N})"))
        .unwrap();

    criterion.bench_function("vtab_native", |b| {
        b.iter(|| run_to_completion(&db, &mut stmt));
    });
}

fn bench_vtab_extension(criterion: &mut Criterion) {
    let (db, conn) = setup_memory_db_with_wasm();
    register_bench_extension(&db, &conn);
    execute(&db, &conn, "CREATE VIRTUAL TABLE cnt USING bench_counter");
    let mut stmt = conn.prepare("SELECT value FROM cnt").unwrap();

    criterion.bench_function("vtab_extension", |b| {
        b.iter(|| run_to_completion(&db, &mut stmt));
    });
}

// ── One criterion_group per benchmark → one flamegraph each ─────────────────

criterion_group! {
    name = grp_integer_add_native;
    config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
    targets = bench_integer_add_native
}
criterion_group! {
    name = grp_integer_add_wasm;
    config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
    targets = bench_integer_add_wasm
}
criterion_group! {
    name = grp_strlen_native;
    config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
    targets = bench_strlen_native
}
criterion_group! {
    name = grp_strlen_wasm;
    config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
    targets = bench_strlen_wasm
}
criterion_group! {
    name = grp_filter_agg_native;
    config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
    targets = bench_filter_agg_native
}
criterion_group! {
    name = grp_filter_agg_wasm;
    config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
    targets = bench_filter_agg_wasm
}
criterion_group! {
    name = grp_ext_scalar_add;
    config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
    targets = bench_ext_scalar_add
}
criterion_group! {
    name = grp_vtab_native;
    config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
    targets = bench_vtab_native
}
criterion_group! {
    name = grp_vtab_extension;
    config = Criterion::default().with_profiler(PProfProfiler::new(1000, Output::Flamegraph(None)));
    targets = bench_vtab_extension
}
criterion_main!(
    grp_integer_add_native,
    grp_integer_add_wasm,
    grp_strlen_native,
    grp_strlen_wasm,
    grp_filter_agg_native,
    grp_filter_agg_wasm,
    grp_ext_scalar_add,
    grp_vtab_native,
    grp_vtab_extension,
);
