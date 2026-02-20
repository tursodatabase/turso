//! File format compatibility fuzz tests.
//!
//! These tests verify that Turso can correctly read SQLite-generated database
//! files by having SQLite create complex databases (with various page sizes,
//! many tables, indexes, overflow pages, large blobs, etc.) and then opening
//! them with Turso and comparing query results.
//!
//! Related issue: <https://github.com/tursodatabase/turso/issues/2576>
use core_tester::common::{
    TempDatabase, limbo_exec_rows, maybe_setup_tracing, rng_from_time_or_env, sqlite_exec_rows,
};
use rand::{Rng, SeedableRng, seq::SliceRandom};
use rand_chacha::ChaCha8Rng;
use tempfile::TempDir;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Column descriptor used when generating tables.
#[derive(Clone, Debug)]
struct ColSpec {
    name: String,
    ty: &'static str,
}

const ALL_TYPES: [&str; 5] = ["INTEGER", "TEXT", "REAL", "BLOB", "NUMERIC"];

const PAGE_SIZES: [u32; 6] = [512, 1024, 2048, 4096, 8192, 16384];

fn random_col_type<R: Rng>(rng: &mut R) -> &'static str {
    ALL_TYPES[rng.random_range(0..ALL_TYPES.len())]
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Generate a random SQL literal value for insertion.
///
/// Covers all SQLite serial types that appear on disk:
/// - NULL  (serial type 0)
/// - Small integers 0/1 (serial types 8/9)
/// - Various-width signed integers (serial types 1–6)
/// - IEEE 754 float (serial type 7)
/// - BLOB (serial type >= 12, even)
/// - Text (serial type >= 13, odd)
///
/// Floats are formatted with explicit decimal point so SQLite stores them as
/// REAL rather than converting them to INTEGER affinity.
fn random_value<R: Rng>(rng: &mut R, large_blob: bool) -> String {
    if large_blob {
        // Large blob forces overflow pages (chain of overflow page pointers).
        let size = rng.random_range(4096..=65536_usize);
        let bytes: Vec<u8> = (0..size).map(|_| rng.random_range(0..=255_u8)).collect();
        let hex: String = bytes.iter().map(|b| format!("{b:02X}")).collect();
        return format!("X'{hex}'");
    }
    match rng.random_range(0..10) {
        0 => "NULL".to_string(),
        // Serial types 8/9 on disk (single-bit flags)
        1 => "0".to_string(),
        2 => "1".to_string(),
        // Various-width signed integers (serial types 1–6)
        3 => rng.random_range(-128..=127_i64).to_string(),
        4 => rng.random_range(-32768..=32767_i64).to_string(),
        5 => rng
            .random_range(-1_000_000_000..=1_000_000_000_i64)
            .to_string(),
        // REAL (serial type 7): always include decimal point so SQLite stores as REAL
        6 => {
            let f: f64 = rng.random_range(-10000.0..10000.0_f64);
            // Always include a decimal point so the value is unambiguously REAL.
            // Avoid NaN/Inf which have inconsistent textual representations.
            if f.is_nan() || f.is_infinite() {
                "0.0".to_string()
            } else {
                format!("{f:.1}")
            }
        }
        // Small blob (serial type >= 12, even)
        7 => {
            let size = rng.random_range(1..=32_usize);
            let bytes: Vec<u8> = (0..size).map(|_| rng.random_range(0..=255_u8)).collect();
            let hex: String = bytes.iter().map(|b| format!("{b:02X}")).collect();
            format!("X'{hex}'")
        }
        // Short text (serial type >= 13, odd)
        8 => {
            let len = rng.random_range(1..=64_usize);
            let s: String = (0..len)
                .map(|_| (rng.random_range(b'a'..=b'z') as char))
                .collect();
            format!("'{s}'")
        }
        // Medium text to stress multi-cell B-tree pages
        _ => {
            let len = rng.random_range(64..=512_usize);
            let s: String = (0..len)
                .map(|_| (rng.random_range(b'A'..=b'Z') as char))
                .collect();
            format!("'{s}'")
        }
    }
}

/// Build a deterministic `SELECT *` with an `ORDER BY` over all columns so
/// both SQLite and Turso return rows in the same order.
fn make_full_select(tname: &str, col_count: usize) -> String {
    let order_cols: String = (1..=col_count)
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT * FROM {} ORDER BY {}",
        quote_ident(tname),
        order_cols
    )
}

// ── core scenario: SQLite generates, Turso reads ──────────────────────────────

/// Core test scenario used by all three test functions.
///
/// 1. SQLite creates a fresh database at `page_size` in WAL mode, writes
///    `n_tables` tables with random schemas and data (including overflow pages
///    from large blobs), creates indexes, then checkpoints to flush the WAL
///    into the main file.
/// 2. SQLite re-reads its own data (ground truth).
/// 3. Turso opens the SQLite-generated file and reads the same data.
/// 4. Every row of every table is compared.
fn run_sqlite_generates_turso_reads(seed: u64, page_size: u32) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    let tmp_dir = TempDir::new().expect("failed to create temp dir");
    let db_path = tmp_dir.path().join("generated.db");

    // ── phase 1: SQLite generates ─────────────────────────────────────────────
    {
        let sqlite = rusqlite::Connection::open(&db_path).expect("sqlite open");

        sqlite
            .pragma_update(None, "page_size", page_size)
            .expect("set page_size");
        sqlite
            .pragma_update(None, "journal_mode", "wal")
            .expect("set WAL");

        let n_tables = rng.random_range(3..=8_usize);

        for t in 0..n_tables {
            let tname = format!("t{t}");
            let n_cols = rng.random_range(1..=6_usize);
            let cols: Vec<ColSpec> = (0..n_cols)
                .map(|c| ColSpec {
                    name: format!("c{c}"),
                    ty: random_col_type(&mut rng),
                })
                .collect();

            let col_defs: String = cols
                .iter()
                .map(|c| format!("  {} {}", quote_ident(&c.name), c.ty))
                .collect::<Vec<_>>()
                .join(",\n");
            let create = format!("CREATE TABLE {} (\n{}\n);", quote_ident(&tname), col_defs);
            sqlite.execute_batch(&create).expect("CREATE TABLE");

            // Secondary index on first column (exercises index B-tree pages).
            let idx_name = format!("idx_{tname}");
            let first_col = quote_ident(&cols[0].name);
            sqlite
                .execute_batch(&format!(
                    "CREATE INDEX {} ON {}({});",
                    quote_ident(&idx_name),
                    quote_ident(&tname),
                    first_col
                ))
                .expect("CREATE INDEX");

            // Insert rows. 5 % chance of a large blob to trigger overflow pages.
            let n_rows = rng.random_range(50..=300_usize);
            let large_blob_prob = 0.05_f64;
            for _ in 0..n_rows {
                let vals: String = cols
                    .iter()
                    .map(|_| {
                        let use_large_blob = rng.random_bool(large_blob_prob);
                        random_value(&mut rng, use_large_blob)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let ins = format!("INSERT INTO {} VALUES ({});", quote_ident(&tname), vals);
                sqlite.execute(&ins, ()).expect("INSERT");
            }
        }

        // Flush WAL back into the main file so Turso reads a complete database.
        sqlite
            .pragma_update(None, "wal_checkpoint", "TRUNCATE")
            .expect("checkpoint");
    }

    // ── phase 2: SQLite reads back (ground truth) ─────────────────────────────
    let sqlite_verify = rusqlite::Connection::open(&db_path).expect("sqlite verify open");
    let mut expected: Vec<(String, Vec<Vec<rusqlite::types::Value>>)> = Vec::new();
    {
        let mut tnames: Vec<String> = sqlite_verify
            .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        tnames.shuffle(&mut rng);

        for tname in &tnames {
            let col_count = sqlite_verify
                .prepare(&format!("PRAGMA table_info({})", quote_ident(tname)))
                .unwrap()
                .query_map([], |_| Ok(()))
                .unwrap()
                .count();
            if col_count == 0 {
                continue;
            }
            let query = make_full_select(tname, col_count);
            let rows = sqlite_exec_rows(&sqlite_verify, &query);
            expected.push((query, rows));
        }
    }

    // ── phase 3: Turso opens the SQLite-generated file and reads it ───────────
    let turso_db = TempDatabase::builder().with_db_path(&db_path).build();
    let turso_conn = turso_db.connect_limbo();

    for (query, expected_rows) in &expected {
        let turso_rows = limbo_exec_rows(&turso_conn, query);
        assert_eq!(
            turso_rows, *expected_rows,
            "Mismatch reading SQLite-generated DB\n\
             seed={seed} page_size={page_size}\n\
             query: {query}\n\
             turso:  {turso_rows:?}\n\
             sqlite: {expected_rows:?}"
        );
    }
}

/// Variant of the core scenario using DELETE (rollback) journal mode.
///
/// When Turso opens a legacy (DELETE-journal) database it converts it to WAL
/// mode on first open.  This test exercises that legacy→WAL conversion path:
/// SQLite writes the DB in DELETE mode, Turso converts and reads it, and we
/// assert the data matches.
fn run_sqlite_generates_turso_reads_delete_journal(seed: u64, page_size: u32) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    let tmp_dir = TempDir::new().expect("failed to create temp dir");
    let db_path = tmp_dir.path().join("generated_delete.db");

    // ── phase 1: SQLite generates in DELETE journal mode ──────────────────────
    {
        let sqlite = rusqlite::Connection::open(&db_path).expect("sqlite open");
        sqlite
            .pragma_update(None, "page_size", page_size)
            .expect("set page_size");
        // Explicitly stay in DELETE (rollback journal) mode — no WAL file.
        sqlite
            .pragma_update(None, "journal_mode", "delete")
            .expect("set DELETE journal");

        let n_tables = rng.random_range(2..=6_usize);
        for t in 0..n_tables {
            let tname = format!("t{t}");
            let n_cols = rng.random_range(1..=5_usize);
            let cols: Vec<ColSpec> = (0..n_cols)
                .map(|c| ColSpec {
                    name: format!("c{c}"),
                    ty: random_col_type(&mut rng),
                })
                .collect();

            let col_defs: String = cols
                .iter()
                .map(|c| format!("  {} {}", quote_ident(&c.name), c.ty))
                .collect::<Vec<_>>()
                .join(",\n");
            sqlite
                .execute_batch(&format!(
                    "CREATE TABLE {} (\n{}\n);",
                    quote_ident(&tname),
                    col_defs
                ))
                .expect("CREATE TABLE");

            let n_rows = rng.random_range(20..=150_usize);
            for _ in 0..n_rows {
                let vals: String = cols
                    .iter()
                    .map(|_| random_value(&mut rng, false))
                    .collect::<Vec<_>>()
                    .join(", ");
                sqlite
                    .execute(
                        &format!("INSERT INTO {} VALUES ({});", quote_ident(&tname), vals),
                        (),
                    )
                    .expect("INSERT");
            }
        }
        // No checkpoint needed — DELETE mode writes directly to the main file.
    }

    // ── phase 2: SQLite reads back (ground truth) ─────────────────────────────
    let sqlite_verify = rusqlite::Connection::open(&db_path).expect("sqlite verify open");
    let mut expected: Vec<(String, Vec<Vec<rusqlite::types::Value>>)> = Vec::new();
    {
        let tnames: Vec<String> = sqlite_verify
            .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        for tname in &tnames {
            let col_count = sqlite_verify
                .prepare(&format!("PRAGMA table_info({})", quote_ident(tname)))
                .unwrap()
                .query_map([], |_| Ok(()))
                .unwrap()
                .count();
            if col_count == 0 {
                continue;
            }
            let query = make_full_select(tname, col_count);
            let rows = sqlite_exec_rows(&sqlite_verify, &query);
            expected.push((query, rows));
        }
    }

    // ── phase 3: Turso opens the SQLite-generated file (converts legacy→WAL) ──
    let turso_db = TempDatabase::builder().with_db_path(&db_path).build();
    let turso_conn = turso_db.connect_limbo();

    for (query, expected_rows) in &expected {
        let turso_rows = limbo_exec_rows(&turso_conn, query);
        assert_eq!(
            turso_rows, *expected_rows,
            "Mismatch reading SQLite DELETE-journal DB\n\
             seed={seed} page_size={page_size}\n\
             query: {query}\n\
             turso:  {turso_rows:?}\n\
             sqlite: {expected_rows:?}"
        );
    }
}

/// Scenario that specifically exercises deep overflow chains and many tables.
///
/// - Very large blobs (up to 256 KB) ensure overflow chains span many pages.
/// - 20–40 tables ensure `sqlite_schema` grows to an interior B-tree page.
/// - AUTOINCREMENT tables add a `sqlite_sequence` table that Turso must parse.
fn run_sqlite_generates_turso_reads_stress_layout(seed: u64, page_size: u32) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    let tmp_dir = TempDir::new().expect("failed to create temp dir");
    let db_path = tmp_dir.path().join("generated_stress.db");

    // ── phase 1: SQLite generates ─────────────────────────────────────────────
    {
        let sqlite = rusqlite::Connection::open(&db_path).expect("sqlite open");
        sqlite
            .pragma_update(None, "page_size", page_size)
            .expect("set page_size");
        sqlite
            .pragma_update(None, "journal_mode", "wal")
            .expect("set WAL");

        // Enough tables to push sqlite_schema past a single leaf page.
        let n_tables = rng.random_range(20..=40_usize);
        // One table uses AUTOINCREMENT to create the sqlite_sequence system table.
        let autoincrement_table = rng.random_range(0..n_tables);

        for t in 0..n_tables {
            let tname = format!("t{t}");
            let n_cols = rng.random_range(1..=4_usize);
            let extra_cols: Vec<ColSpec> = (0..n_cols)
                .map(|c| ColSpec {
                    name: format!("c{c}"),
                    ty: random_col_type(&mut rng),
                })
                .collect();

            // One table uses AUTOINCREMENT; the rest use a plain INTEGER primary key.
            let pk_clause = if t == autoincrement_table {
                "id INTEGER PRIMARY KEY AUTOINCREMENT"
            } else {
                "id INTEGER PRIMARY KEY"
            };

            let col_defs: String = std::iter::once(pk_clause.to_string())
                .chain(
                    extra_cols
                        .iter()
                        .map(|c| format!("{} {}", quote_ident(&c.name), c.ty)),
                )
                .collect::<Vec<_>>()
                .join(", ");

            sqlite
                .execute_batch(&format!(
                    "CREATE TABLE {} ({});",
                    quote_ident(&tname),
                    col_defs
                ))
                .expect("CREATE TABLE");

            // Every 5th table gets a deep overflow blob (up to 256 KB).
            let n_rows = rng.random_range(10..=60_usize);
            let large_blob_prob = if t % 5 == 0 { 0.3_f64 } else { 0.0_f64 };
            for _ in 0..n_rows {
                // Build value list excluding the id column (auto-assigned).
                let vals: String = extra_cols
                    .iter()
                    .map(|_| {
                        if large_blob_prob > 0.0 && rng.random_bool(large_blob_prob) {
                            // Deep overflow chain: blob spanning many pages.
                            let size = rng.random_range(65536..=262144_usize);
                            let bytes: Vec<u8> =
                                (0..size).map(|_| rng.random_range(0..=255_u8)).collect();
                            let hex: String = bytes.iter().map(|b| format!("{b:02X}")).collect();
                            format!("X'{hex}'")
                        } else {
                            random_value(&mut rng, false)
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");

                let col_names: String = extra_cols
                    .iter()
                    .map(|c| quote_ident(&c.name))
                    .collect::<Vec<_>>()
                    .join(", ");

                if extra_cols.is_empty() {
                    sqlite
                        .execute(
                            &format!("INSERT INTO {} DEFAULT VALUES;", quote_ident(&tname)),
                            (),
                        )
                        .expect("INSERT DEFAULT VALUES");
                } else {
                    sqlite
                        .execute(
                            &format!(
                                "INSERT INTO {} ({}) VALUES ({});",
                                quote_ident(&tname),
                                col_names,
                                vals
                            ),
                            (),
                        )
                        .expect("INSERT");
                }
            }
        }

        sqlite
            .pragma_update(None, "wal_checkpoint", "TRUNCATE")
            .expect("checkpoint");
    }

    // ── phase 2: SQLite reads back (ground truth) ─────────────────────────────
    let sqlite_verify = rusqlite::Connection::open(&db_path).expect("sqlite verify open");
    let mut expected: Vec<(String, Vec<Vec<rusqlite::types::Value>>)> = Vec::new();
    {
        // Include ALL tables (including sqlite_sequence) so Turso's parsing of
        // system tables is exercised.
        let tnames: Vec<String> = sqlite_verify
            .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        for tname in &tnames {
            let col_count = sqlite_verify
                .prepare(&format!("PRAGMA table_info({})", quote_ident(tname)))
                .unwrap()
                .query_map([], |_| Ok(()))
                .unwrap()
                .count();
            if col_count == 0 {
                continue;
            }
            let query = make_full_select(tname, col_count);
            let rows = sqlite_exec_rows(&sqlite_verify, &query);
            expected.push((query, rows));
        }
    }

    // ── phase 3: Turso opens the SQLite-generated file and reads it ───────────
    let turso_db = TempDatabase::builder().with_db_path(&db_path).build();
    let turso_conn = turso_db.connect_limbo();

    for (query, expected_rows) in &expected {
        let turso_rows = limbo_exec_rows(&turso_conn, query);
        assert_eq!(
            turso_rows, *expected_rows,
            "Mismatch reading SQLite stress-layout DB\n\
             seed={seed} page_size={page_size}\n\
             query: {query}\n\
             turso:  {turso_rows:?}\n\
             sqlite: {expected_rows:?}"
        );
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// For each supported page size, SQLite generates a complex database (WAL mode)
/// and Turso reads it back, comparing every row of every table.
///
/// Related issue: <https://github.com/tursodatabase/turso/issues/2576>
#[test]
fn fuzz_sqlite_generated_file_format() {
    maybe_setup_tracing();

    let (mut rng, base_seed) = rng_from_time_or_env();
    tracing::info!("fuzz_sqlite_generated_file_format base_seed={base_seed}");

    for &page_size in &PAGE_SIZES {
        let seed: u64 = rng.random();
        tracing::info!("fuzz_sqlite_generated_file_format page_size={page_size} seed={seed}");
        run_sqlite_generates_turso_reads(seed, page_size);
    }
}

/// Fixed-seed regression tests: always run in CI regardless of environment,
/// providing deterministic coverage of specific layout scenarios.
#[test]
fn fuzz_sqlite_generated_file_format_fixed_seeds() {
    maybe_setup_tracing();

    // (seed, page_size) pairs chosen to cover different file layout scenarios:
    // smallest page size (many overflow pages), default 4096, largest page size.
    let cases: &[(u64, u32)] = &[
        (0xdead_beef_cafe_babe, 512),
        (0x0102_0304_0506_0708, 4096),
        (0xfeed_face_cafe_beef, 8192),
        (0x1234_5678_90ab_cdef, 16384),
        (0xabcd_ef12_3456_7890, 1024),
        (0x0000_0000_0000_0001, 2048),
    ];

    for &(seed, page_size) in cases {
        tracing::info!(
            "fuzz_sqlite_generated_file_format_fixed_seeds \
             seed={seed:#018x} page_size={page_size}"
        );
        run_sqlite_generates_turso_reads(seed, page_size);
    }
}

/// Stress test: many random iterations rotating through page sizes to maximise
/// schema and data variety in a single run.
#[test]
fn fuzz_sqlite_generated_file_format_stress() {
    maybe_setup_tracing();

    const ITERATIONS: usize = 20;
    let (mut rng, base_seed) = rng_from_time_or_env();
    tracing::info!(
        "fuzz_sqlite_generated_file_format_stress \
         base_seed={base_seed} iterations={ITERATIONS}"
    );

    for i in 0..ITERATIONS {
        let seed: u64 = rng.random();
        let page_size = PAGE_SIZES[i % PAGE_SIZES.len()];
        tracing::info!(
            "fuzz_sqlite_generated_file_format_stress \
             iter={}/{ITERATIONS} seed={seed} page_size={page_size}",
            i + 1
        );
        run_sqlite_generates_turso_reads(seed, page_size);
    }
}

/// SQLite generates databases in DELETE (rollback journal) mode and Turso
/// reads them back.  Turso converts legacy-mode databases to WAL on first open;
/// this test exercises that legacy→WAL conversion path across all page sizes.
#[test]
fn fuzz_sqlite_generated_file_format_delete_journal() {
    maybe_setup_tracing();

    let (mut rng, base_seed) = rng_from_time_or_env();
    tracing::info!("fuzz_sqlite_generated_file_format_delete_journal base_seed={base_seed}");

    // Fixed seeds first (deterministic CI coverage).
    let fixed: &[(u64, u32)] = &[
        (0xaaaa_bbbb_cccc_dddd, 512),
        (0x1111_2222_3333_4444, 4096),
        (0x9876_5432_10fe_dcba, 16384),
    ];
    for &(seed, page_size) in fixed {
        tracing::info!(
            "fuzz_sqlite_generated_file_format_delete_journal \
             seed={seed:#018x} page_size={page_size}"
        );
        run_sqlite_generates_turso_reads_delete_journal(seed, page_size);
    }

    // Random seeds over all page sizes.
    for &page_size in &PAGE_SIZES {
        let seed: u64 = rng.random();
        tracing::info!(
            "fuzz_sqlite_generated_file_format_delete_journal \
             page_size={page_size} seed={seed}"
        );
        run_sqlite_generates_turso_reads_delete_journal(seed, page_size);
    }
}

/// SQLite generates databases with many tables (stressing `sqlite_schema`
/// interior B-tree pages), deep overflow chains (multi-page blobs), and
/// AUTOINCREMENT (exercising the `sqlite_sequence` system table).
#[test]
fn fuzz_sqlite_generated_file_format_complex_layout() {
    maybe_setup_tracing();

    let (mut rng, base_seed) = rng_from_time_or_env();
    tracing::info!("fuzz_sqlite_generated_file_format_complex_layout base_seed={base_seed}");

    // Fixed seeds for deterministic CI coverage.
    let fixed: &[(u64, u32)] = &[
        (0xf00d_cafe_babe_1234, 512),
        (0xdead_1234_5678_cafe, 4096),
        (0x0011_2233_4455_6677, 16384),
    ];
    for &(seed, page_size) in fixed {
        tracing::info!(
            "fuzz_sqlite_generated_file_format_complex_layout \
             seed={seed:#018x} page_size={page_size}"
        );
        run_sqlite_generates_turso_reads_stress_layout(seed, page_size);
    }

    // Random seed at default page size.
    let seed: u64 = rng.random();
    tracing::info!("fuzz_sqlite_generated_file_format_complex_layout seed={seed} page_size=4096");
    run_sqlite_generates_turso_reads_stress_layout(seed, 4096);
}
