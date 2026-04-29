use crate::common::{
    compute_dbhash, compute_dbhash_with_database_opts, compute_dbhash_with_options,
    compute_dbhash_with_options_and_database_opts, do_flush, ExecRows, TempDatabase,
};
use crate::queued_io::{QueuedIo, QueuedIoOpKind};
use rusqlite::Connection as SqliteConnection;
use std::{path::Path, sync::Arc};
use tempfile::TempDir;
use turso_core::{Connection, Database, DatabaseOpts, LimboError, StepResult, Value};
use turso_parser::{ast::Cmd, parser::Parser};

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedSchemaRow {
    object_type: String,
    name: String,
    table_name: String,
    sql: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlainVacuumInvariant {
    hash: String,
    normalized_schema: Vec<NormalizedSchemaRow>,
}

/// Helper to run integrity_check and return the result string
fn run_integrity_check(conn: &Arc<Connection>) -> String {
    let rows: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
    rows.into_iter()
        .map(|(text,)| text)
        .collect::<Vec<_>>()
        .join("\n")
}

fn normalize_sql_whitespace(sql: &str) -> String {
    sql.split_ascii_whitespace().collect::<Vec<_>>().join(" ")
}

fn canonicalize_schema_sql(sql: &str) -> String {
    let mut parser = Parser::new(sql.as_bytes());
    let Ok(Some(cmd)) = parser.next_cmd() else {
        return normalize_sql_whitespace(sql);
    };
    if !matches!(parser.next_cmd(), Ok(None)) {
        return normalize_sql_whitespace(sql);
    }
    match cmd {
        Cmd::Stmt(stmt) | Cmd::Explain(stmt) | Cmd::ExplainQueryPlan(stmt) => stmt.to_string(),
    }
}

fn normalized_schema_snapshot(conn: &Arc<Connection>) -> Vec<NormalizedSchemaRow> {
    let rows: Vec<(String, String, String, String)> = conn.exec_rows(
        "SELECT type, name, tbl_name, COALESCE(sql, '') \
         FROM sqlite_schema \
         ORDER BY type, name, tbl_name, COALESCE(sql, '')",
    );
    rows.into_iter()
        .map(|(object_type, name, table_name, sql)| NormalizedSchemaRow {
            object_type,
            name,
            table_name,
            sql: (!sql.is_empty()).then(|| canonicalize_schema_sql(&sql)),
        })
        .collect()
}

fn compute_default_dbhash(tmp_db: &TempDatabase) -> String {
    compute_dbhash_with_database_opts(tmp_db, tmp_db.db_opts).hash
}

fn compute_plain_vacuum_dbhash(tmp_db: &TempDatabase) -> PlainVacuumInvariant {
    let options = turso_dbhash::DbHashOptions {
        without_schema: true,
        ..Default::default()
    };
    let reopened = TempDatabase::new_with_existent_with_opts(&tmp_db.path, tmp_db.db_opts);
    let reopened_conn = reopened.connect_limbo();
    PlainVacuumInvariant {
        hash: compute_dbhash_with_options_and_database_opts(tmp_db, &options, tmp_db.db_opts).hash,
        normalized_schema: normalized_schema_snapshot(&reopened_conn),
    }
}

fn assert_plain_vacuum_integrity_and_hash(
    tmp_db: &TempDatabase,
    conn: &Arc<Connection>,
    expected: &PlainVacuumInvariant,
) {
    assert_eq!(run_integrity_check(conn), "ok");
    assert_eq!(
        compute_plain_vacuum_dbhash(tmp_db).hash,
        expected.hash,
        "plain VACUUM should preserve logical database content"
    );
    assert_eq!(
        normalized_schema_snapshot(conn),
        expected.normalized_schema,
        "plain VACUUM should preserve normalized sqlite_schema entries"
    );
}

fn run_plain_vacuum_and_assert_round_trip(
    tmp_db: &TempDatabase,
    conn: &Arc<Connection>,
) -> anyhow::Result<()> {
    checkpoint_if_mvcc(tmp_db, conn)?;
    let before_hash = compute_plain_vacuum_dbhash(tmp_db);
    conn.execute("VACUUM")?;
    assert_plain_vacuum_integrity_and_hash(tmp_db, conn, &before_hash);
    Ok(())
}

fn assert_vacuum_into_destination_integrity_and_default_hash(
    dest_db: &TempDatabase,
    dest_conn: &Arc<Connection>,
    expected_hash: &str,
) {
    assert_eq!(run_integrity_check(dest_conn), "ok");
    assert_eq!(
        compute_default_dbhash(dest_db),
        expected_hash,
        "VACUUM INTO should preserve logical database content"
    );
}

fn run_vacuum_into_and_assert_round_trip(
    tmp_db: &TempDatabase,
    conn: &Arc<Connection>,
    dest_path: &std::path::Path,
) -> anyhow::Result<(TempDatabase, Arc<Connection>)> {
    let source_hash = compute_default_dbhash(tmp_db);
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent_with_opts(dest_path, tmp_db.db_opts);
    let dest_conn = dest_db.connect_limbo();
    assert_vacuum_into_destination_integrity_and_default_hash(&dest_db, &dest_conn, &source_hash);
    Ok((dest_db, dest_conn))
}

fn checkpoint_if_mvcc(tmp_db: &TempDatabase, conn: &Arc<Connection>) -> anyhow::Result<()> {
    if tmp_db.enable_mvcc {
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    }
    Ok(())
}

fn connect_existing_with_opts(
    path: &Path,
    db_opts: DatabaseOpts,
) -> (TempDatabase, Arc<Connection>) {
    let db = TempDatabase::new_with_existent_with_opts(path, db_opts);
    let conn = db.connect_limbo();
    (db, conn)
}

fn escape_sqlite_string_literal(text: &str) -> String {
    text.replace('\'', "''")
}

fn scalar_i64(conn: &Arc<Connection>, sql: &str) -> i64 {
    let rows: Vec<(i64,)> = conn.exec_rows(sql);
    assert_eq!(rows.len(), 1, "expected one row for {sql}");
    rows[0].0
}

fn sqlite_scalar_i64(conn: &SqliteConnection, sql: &str) -> i64 {
    conn.query_row(sql, [], |row| row.get(0))
        .unwrap_or_else(|err| panic!("expected one row for {sql}: {err}"))
}

fn create_employees_with_check_constraints(conn: &Arc<Connection>) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL CHECK(length(name) > 0),
            age INTEGER CHECK(age >= 18 AND age <= 120),
            salary REAL CHECK(salary > 0),
            status TEXT CHECK(status IN ('active', 'inactive', 'pending'))
        )",
    )?;
    Ok(())
}

fn insert_valid_employee_rows(conn: &Arc<Connection>) -> anyhow::Result<()> {
    conn.execute("INSERT INTO employees VALUES (1, 'Alice', 30, 50000.0, 'active')")?;
    conn.execute("INSERT INTO employees VALUES (2, 'Bob', 45, 75000.0, 'inactive')")?;
    conn.execute("INSERT INTO employees VALUES (3, 'Charlie', 18, 35000.0, 'pending')")?;
    Ok(())
}

fn employee_rows(conn: &Arc<Connection>) -> Vec<(i64, String, i64, f64, String)> {
    conn.exec_rows("SELECT id, name, age, salary, status FROM employees ORDER BY id")
}

fn custom_type_sql_snapshot(conn: &Arc<Connection>) -> Vec<(String, String)> {
    let rows: Vec<(String, String)> =
        conn.exec_rows("SELECT name, sql FROM sqlite_turso_types ORDER BY name");
    rows.into_iter()
        .map(|(name, sql)| (name, normalize_sql_whitespace(&sql)))
        .collect()
}

fn create_custom_type_ordering_fixture(conn: &Arc<Connection>) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TYPE rev_cmp \
         BASE text \
         ENCODE string_reverse(value) \
         DECODE string_reverse(value) \
         OPERATOR '<' string_reverse",
    )?;
    conn.execute(
        "CREATE TABLE words(
            id INTEGER PRIMARY KEY,
            word rev_cmp NOT NULL DEFAULT 'banana'
        ) STRICT",
    )?;
    conn.execute("CREATE INDEX words_word_idx ON words(word)")?;
    conn.execute("INSERT INTO words(id, word) VALUES (1, 'cherry')")?;
    conn.execute("INSERT INTO words(id, word) VALUES (2, 'apple')")?;
    conn.execute("INSERT INTO words(id, word) VALUES (3, 'banana')")?;
    conn.execute("INSERT INTO words(id) VALUES (4)")?;
    Ok(())
}

fn assert_custom_type_ordering_fixture(conn: &Arc<Connection>) {
    let rows: Vec<(i64, String)> = conn.exec_rows("SELECT id, word FROM words ORDER BY id");
    assert_eq!(
        rows,
        vec![
            (1, "cherry".to_string()),
            (2, "apple".to_string()),
            (3, "banana".to_string()),
            (4, "banana".to_string()),
        ],
    );

    let ordered: Vec<(i64, String)> = conn.exec_rows(
        "SELECT id, word FROM words INDEXED BY words_word_idx ORDER BY word ASC, id ASC",
    );
    assert_eq!(
        ordered,
        vec![
            (2, "apple".to_string()),
            (3, "banana".to_string()),
            (4, "banana".to_string()),
            (1, "cherry".to_string()),
        ],
        "custom comparator order should survive when scanning the custom-type index",
    );

    let desc: Vec<(i64, String)> = conn.exec_rows(
        "SELECT id, word FROM words INDEXED BY words_word_idx ORDER BY word DESC, id DESC",
    );
    assert_eq!(
        desc,
        vec![
            (1, "cherry".to_string()),
            (4, "banana".to_string()),
            (3, "banana".to_string()),
            (2, "apple".to_string()),
        ],
        "descending scans over the custom-type index should preserve comparator semantics",
    );
}

fn assert_custom_type_reusable_for_new_schema(conn: &Arc<Connection>) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TABLE extra_words(
            id INTEGER PRIMARY KEY,
            word rev_cmp NOT NULL DEFAULT 'kiwi'
        ) STRICT",
    )?;
    conn.execute("CREATE INDEX extra_words_word_idx ON extra_words(word)")?;
    conn.execute("INSERT INTO extra_words(id) VALUES (1)")?;
    conn.execute("INSERT INTO extra_words(id, word) VALUES (2, 'apricot')")?;
    conn.execute("INSERT INTO extra_words(id, word) VALUES (3, 'pear')")?;

    let rows: Vec<(i64, String)> = conn.exec_rows(
        "SELECT id, word FROM extra_words INDEXED BY extra_words_word_idx ORDER BY word ASC, id ASC",
    );
    assert_eq!(
        rows,
        vec![
            (2, "apricot".to_string()),
            (1, "kiwi".to_string()),
            (3, "pear".to_string()),
        ],
        "the preserved custom type should remain usable in new tables and indexes after VACUUM",
    );
    Ok(())
}

fn wal_file_size(tmp_db: &TempDatabase) -> u64 {
    let wal_path = format!("{}-wal", tmp_db.path.display());
    std::fs::metadata(wal_path).map(|m| m.len()).unwrap_or(0)
}

fn mvcc_log_file_size(tmp_db: &TempDatabase) -> u64 {
    std::fs::metadata(tmp_db.path.with_extension("db-log"))
        .map(|m| m.len())
        .unwrap_or(0)
}

fn assert_plain_vacuum_folded_into_db_file(tmp_db: &TempDatabase, conn: &Arc<Connection>) {
    let page_count = scalar_i64(conn, "PRAGMA page_count") as u64;
    let page_size = scalar_i64(conn, "PRAGMA page_size") as u64;
    let db_size = std::fs::metadata(&tmp_db.path)
        .unwrap_or_else(|err| panic!("database file should exist after VACUUM: {err}"))
        .len();

    assert_eq!(
        db_size,
        page_count * page_size,
        "VACUUM should checkpoint the compacted image into the db file"
    );
    assert_eq!(
        wal_file_size(tmp_db),
        0,
        "VACUUM should truncate the source WAL after checkpoint"
    );
}

fn assert_plain_vacuum_preserves_content_hash(
    tmp_db: &TempDatabase,
    conn: &Arc<Connection>,
) -> anyhow::Result<()> {
    // Plain VACUUM can rebuild equivalent sqlite_schema SQL text with different
    // formatting, so compare a parser-normalized sqlite_schema snapshot rather
    // than raw schema SQL text. Integrity-check still covers the structural
    // side, and the data hash stays schema-free so formatting churn does not
    // create false failures.
    //
    // Intentionally do not `cacheflush()` after VACUUM here. Some callers
    // assert that plain VACUUM left the physical `-wal` folded into the DB
    // file; a post-VACUUM cacheflush can emit a fresh WAL frame from the live
    // connection and obscure that invariant.
    let hash_opts = turso_dbhash::DbHashOptions {
        without_schema: true,
        ..Default::default()
    };
    do_flush(conn, tmp_db)?;
    let before_schema = normalized_schema_snapshot(conn);
    let before = compute_dbhash_with_options_and_database_opts(tmp_db, &hash_opts, tmp_db.db_opts);

    conn.execute("VACUUM")?;

    let after = compute_dbhash_with_options_and_database_opts(tmp_db, &hash_opts, tmp_db.db_opts);
    assert_eq!(
        before.hash, after.hash,
        "plain VACUUM changed logical database content: before={}, after={}",
        before.hash, after.hash
    );
    assert_eq!(
        normalized_schema_snapshot(conn),
        before_schema,
        "plain VACUUM should preserve normalized sqlite_schema entries"
    );
    Ok(())
}

fn assert_plain_vacuum_preserves_autovacuum_mode(
    pragma_value: &str,
    expected_mode: i64,
) -> anyhow::Result<()> {
    let opts = DatabaseOpts::new()
        .with_encryption(true)
        .with_autovacuum(true);
    let (_temp_dir, tmp_db) = open_sqlite_autovacuum_db(pragma_value, opts)?;
    let conn = tmp_db.connect_limbo();

    assert_eq!(scalar_i64(&conn, "PRAGMA auto_vacuum"), expected_mode);

    checkpoint_if_mvcc(&tmp_db, &conn)?;
    let before_hash = compute_plain_vacuum_dbhash(&tmp_db);
    conn.execute("VACUUM")?;

    assert_eq!(scalar_i64(&conn, "PRAGMA auto_vacuum"), expected_mode);
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM t"), 120);
    assert_plain_vacuum_integrity_and_hash(&tmp_db, &conn, &before_hash);
    assert_plain_vacuum_folded_into_db_file(&tmp_db, &conn);

    let reopened = TempDatabase::new_with_existent_with_opts(&tmp_db.path, opts);
    let reopened_conn = reopened.connect_limbo();
    assert_eq!(
        scalar_i64(&reopened_conn, "PRAGMA auto_vacuum"),
        expected_mode
    );
    assert_eq!(run_integrity_check(&reopened_conn), "ok");

    Ok(())
}

fn assert_vacuum_into_preserves_autovacuum_mode(
    pragma_value: &str,
    expected_mode: i64,
) -> anyhow::Result<()> {
    let opts = DatabaseOpts::new()
        .with_encryption(true)
        .with_autovacuum(true);
    let (_temp_dir, tmp_db) = open_sqlite_autovacuum_db(pragma_value, opts)?;
    let conn = tmp_db.connect_limbo();

    assert_eq!(scalar_i64(&conn, "PRAGMA auto_vacuum"), expected_mode);

    let hash_opts = turso_dbhash::DbHashOptions {
        table_filter: Some("t".to_string()),
        without_schema: true,
        ..Default::default()
    };
    let source_hash = compute_dbhash_with_options_and_database_opts(&tmp_db, &hash_opts, opts);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuum-into-autovacuum.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
    let dest_conn = dest_db.connect_limbo();
    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(scalar_i64(&dest_conn, "PRAGMA auto_vacuum"), expected_mode);
    assert_eq!(scalar_i64(&dest_conn, "SELECT COUNT(*) FROM t"), 120);
    assert_eq!(
        compute_dbhash_with_options_and_database_opts(&dest_db, &hash_opts, opts).hash,
        source_hash.hash,
        "VACUUM INTO should preserve logical table content for auto-vacuum databases"
    );

    let reopened = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
    let reopened_conn = reopened.connect_limbo();
    assert_eq!(
        scalar_i64(&reopened_conn, "PRAGMA auto_vacuum"),
        expected_mode
    );
    assert_eq!(run_integrity_check(&reopened_conn), "ok");

    Ok(())
}

fn open_sqlite_autovacuum_db(
    pragma_value: &str,
    opts: DatabaseOpts,
) -> anyhow::Result<(TempDir, TempDatabase)> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("autovacuum.db");
    let sqlite_conn = SqliteConnection::open(&db_path)?;
    sqlite_conn.pragma_update(None, "page_size", 1024)?;
    sqlite_conn.pragma_update(None, "auto_vacuum", pragma_value)?;
    sqlite_conn.execute_batch("VACUUM")?;
    sqlite_conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT)", ())?;
    sqlite_conn.execute("CREATE INDEX idx_t_payload ON t(payload)", ())?;
    for i in 0..160 {
        sqlite_conn.execute(
            "INSERT INTO t VALUES(?1, ?2)",
            rusqlite::params![i, "autovacuum-payload-".repeat(20)],
        )?;
    }
    sqlite_conn.execute("DELETE FROM t WHERE id % 4 = 0", ())?;
    drop(sqlite_conn);

    let tmp_db = TempDatabase::new_with_existent_with_opts(&db_path, opts);
    Ok((temp_dir, tmp_db))
}

fn populate_until_page_count(
    conn: &Arc<Connection>,
    target_pages: i64,
    payload_len: usize,
) -> anyhow::Result<i64> {
    let payload = "x".repeat(payload_len);
    let mut next_id = 0_i64;

    loop {
        let page_count = scalar_i64(conn, "PRAGMA page_count");
        if page_count == target_pages {
            return Ok(next_id);
        }
        assert!(
            page_count < target_pages,
            "page_count skipped target {target_pages}; current={page_count}, rows={next_id}"
        );
        conn.execute(format!(
            "INSERT INTO t VALUES({next_id}, '{}')",
            escape_sqlite_string_literal(&payload)
        ))?;
        next_id += 1;
        assert!(
            next_id < target_pages * 20 + 100,
            "could not reach page_count {target_pages}"
        );
    }
}

#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(mvcc, init_sql = "CREATE TABLE t (a INTEGER, b TEXT, c BLOB);")]
fn test_vacuum_into_basic(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("INSERT INTO t VALUES (1, 'hello', X'DEADBEEF')")?;
    conn.execute("INSERT INTO t VALUES (2, 'world', X'CAFEBABE')")?;
    conn.execute("INSERT INTO t VALUES (3, 'test', NULL)")?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, tmp_db.db_opts);
    let dest_conn = dest_db.connect_limbo();

    let integrity_result = run_integrity_check(&dest_conn);
    assert_eq!(
        integrity_result, "ok",
        "Destination database should pass integrity check"
    );

    let dest_hash = compute_dbhash(&dest_db);
    if !tmp_db.enable_mvcc {
        // MVCC meta table is removed so content wont match
        assert_eq!(
            source_hash.hash, dest_hash.hash,
            "Source and destination databases should have the same content hash"
        );
    }

    // let's verify the data anyways (to alleviate any bugs in dbhash)
    let rows: Vec<(i64, String)> = dest_conn.exec_rows("SELECT a, b FROM t ORDER BY a, b");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1, "hello");
    assert_eq!(rows[1].0, 2);
    assert_eq!(rows[1].1, "world");
    assert_eq!(rows[2].0, 3);
    assert_eq!(rows[2].1, "test");

    let mut stmt = dest_conn.prepare("SELECT c FROM t ORDER BY a")?;
    let blob_values = stmt.run_collect_rows()?;
    assert_eq!(blob_values.len(), 3);
    assert_eq!(blob_values[0][0], Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    assert_eq!(blob_values[1][0], Value::Blob(vec![0xCA, 0xFE, 0xBA, 0xBE]));
    assert_eq!(blob_values[2][0], Value::Null);

    // verify destination also has zero reserved_space (the default value)
    {
        use std::fs::File;
        use std::io::{Read, Seek, SeekFrom};
        const RESERVED_SPACE_OFFSET: u64 = 20;

        let mut file = File::open(&dest_path)?;
        file.seek(SeekFrom::Start(RESERVED_SPACE_OFFSET))?;
        let mut buf = [0u8; 1];
        file.read_exact(&mut buf)?;
        assert_eq!(buf[0], 0, "Destination should have reserved_space=0");
    }

    Ok(())
}

/// Test VACUUM INTO error cases: plain VACUUM, existing file, within
/// transaction, and query_only mode.
#[turso_macros::test(mvcc)]
fn test_vacuum_into_error_cases(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE t (a INTEGER)")?;
    conn.execute("INSERT INTO t VALUES (1)")?;

    let dest_dir = TempDir::new()?;

    // 1. plain VACUUM should succeed; MVCC databases must checkpoint first.
    checkpoint_if_mvcc(&tmp_db, &conn)?;
    let result = conn.execute("VACUUM");
    assert!(result.is_ok(), "Plain VACUUM should succeed");

    // 2. VACUUM INTO existing file should fail
    let existing_path = dest_dir.path().join("existing.db");
    std::fs::write(&existing_path, b"existing content")?;
    let result = conn.execute(format!("VACUUM INTO '{}'", existing_path.to_str().unwrap()));
    assert!(result.is_err(), "VACUUM INTO existing file should fail");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("already exists") || err_msg.contains("output file"),
        "Error should mention file exists, got: {err_msg}"
    );

    // 3. VACUUM INTO within transaction should fail
    conn.execute("BEGIN")?;
    conn.execute("INSERT INTO t VALUES (2)")?;
    let txn_path = dest_dir.path().join("txn.db");
    let result = conn.execute(format!("VACUUM INTO '{}'", txn_path.to_str().unwrap()));
    assert!(
        result.is_err(),
        "VACUUM INTO within transaction should fail"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("transaction") || err_msg.contains("VACUUM"),
        "Error should mention transaction, got: {err_msg}"
    );
    assert!(!txn_path.exists(), "File should not be created on failure");

    // rollback and verify original data intact
    conn.execute("ROLLBACK")?;
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT a FROM t");
    assert_eq!(rows, vec![(1,)]);

    // 4. VACUUM / VACUUM INTO should fail in query_only mode with a
    // VACUUM-specific error message.
    conn.set_query_only(true);
    let query_only_path = dest_dir.path().join("query-only.db");
    let escaped_query_only_path = escape_sqlite_string_literal(query_only_path.to_str().unwrap());
    let result = conn.execute(format!("VACUUM INTO '{escaped_query_only_path}'"));
    assert!(
        result.is_err(),
        "VACUUM INTO should fail in query_only mode"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("VACUUM") && err_msg.contains("query_only"),
        "Error should mention VACUUM and query_only, got: {err_msg}"
    );
    assert!(
        !query_only_path.exists(),
        "File should not be created in query_only mode"
    );
    conn.set_query_only(false);

    Ok(())
}

/// Active-statement accounting matrix for VACUUM INTO:
///
/// - active other statement, no reprepare -> reject
/// - active other statement, with reprepare -> still reject
/// Active-statement accounting for VACUUM INTO:
///
/// - active other statement, no reprepare -> reject
/// - active other statement, with reprepare -> still reject
///
/// SQLite-compatible behavior: VACUUM INTO must reject if another statement is
/// still active on the same connection.
#[turso_macros::test(mvcc, init_sql = "CREATE TABLE t (a INTEGER);")]
fn test_vacuum_into_rejects_active_select_on_same_connection(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES (1), (2), (3)")?;

    let mut select_stmt = conn.prepare("SELECT a FROM t ORDER BY a")?;
    assert!(
        matches!(select_stmt.step()?, StepResult::Row),
        "SELECT should remain active after yielding its first row"
    );
    assert_eq!(
        select_stmt.row().unwrap().get_values().next(),
        Some(&Value::from_i64(1))
    );

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("active-select-vacuum-into.db");

    let err = conn
        .execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))
        .expect_err("VACUUM INTO should reject same-connection active statements");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("SQL statements in progress"),
        "error should mention active statements, got: {err_msg}"
    );
    assert!(
        !dest_path.exists(),
        "destination file should not be created on failure"
    );

    let mut seen = vec![1];
    loop {
        match select_stmt.step()? {
            StepResult::Row => {
                let row = select_stmt.row().expect("row should be present");
                match row.get_values().next() {
                    Some(v) => seen.push(v.as_int().expect("expected integer row value")),
                    other => panic!("unexpected row value after VACUUM INTO: {other:?}"),
                }
            }
            StepResult::Done => break,
            StepResult::IO => select_stmt.get_pager().io.step()?,
            StepResult::Busy => anyhow::bail!("unexpected Busy while draining SELECT"),
            StepResult::Interrupt => anyhow::bail!("unexpected Interrupt while draining SELECT"),
        }
    }

    assert_eq!(seen, vec![1, 2, 3]);
    Ok(())
}

/// Reprepared active root statements must remain counted so VACUUM INTO still
/// rejects them as "SQL statements in progress".
#[turso_macros::test(mvcc, init_sql = "CREATE TABLE t (a INTEGER);")]
fn test_vacuum_into_rejects_reprepared_active_select_on_same_connection(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES (1), (2), (3)")?;

    let mut select_stmt = conn.prepare("SELECT a FROM t ORDER BY a")?;
    conn.execute("PRAGMA foreign_keys = ON")?;

    assert!(
        matches!(select_stmt.step()?, StepResult::Row),
        "SELECT should remain active after reprepare and first row"
    );
    assert_eq!(
        select_stmt.row().unwrap().get_values().next(),
        Some(&Value::from_i64(1))
    );

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir
        .path()
        .join("reprepared-active-select-vacuum-into.db");

    let err = conn
        .execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))
        .expect_err("VACUUM INTO should reject re-prepared active statements");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("SQL statements in progress"),
        "error should mention active statements, got: {err_msg}"
    );
    assert!(
        !dest_path.exists(),
        "destination file should not be created on failure"
    );

    let mut seen = vec![1];
    loop {
        match select_stmt.step()? {
            StepResult::Row => {
                let row = select_stmt.row().expect("row should be present");
                match row.get_values().next() {
                    Some(v) => seen.push(v.as_int().expect("expected integer row value")),
                    other => panic!("unexpected row value after VACUUM INTO: {other:?}"),
                }
            }
            StepResult::Done => break,
            StepResult::IO => select_stmt.get_pager().io.step()?,
            StepResult::Busy => anyhow::bail!("unexpected Busy while draining SELECT"),
            StepResult::Interrupt => anyhow::bail!("unexpected Interrupt while draining SELECT"),
        }
    }

    assert_eq!(seen, vec![1, 2, 3]);
    Ok(())
}

/// Statement overlap: a live SELECT on one connection can continue after a
/// same-connection write, and it keeps reading its original snapshot.
#[turso_macros::test(mvcc, init_sql = "CREATE TABLE t (a INTEGER);")]
fn test_same_connection_select_then_write_then_continue_select(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES (1), (2), (3)")?;

    let mut select_stmt = conn.prepare("SELECT a FROM t ORDER BY a")?;
    assert!(
        matches!(select_stmt.step()?, StepResult::Row),
        "SELECT should remain active after yielding its first row"
    );
    assert_eq!(
        select_stmt.row().unwrap().get_values().next(),
        Some(&Value::from_i64(1))
    );

    conn.execute("INSERT INTO t VALUES (4)")?;

    let mut seen = vec![1];
    loop {
        match select_stmt.step()? {
            StepResult::Row => {
                let row = select_stmt.row().expect("row should be present");
                match row.get_values().next() {
                    Some(v) => seen.push(v.as_int().expect("expected integer row value")),
                    other => panic!("unexpected row value after INSERT: {other:?}"),
                }
            }
            StepResult::Done => break,
            StepResult::IO => select_stmt.get_pager().io.step()?,
            StepResult::Busy => anyhow::bail!("unexpected Busy while draining SELECT"),
            StepResult::Interrupt => anyhow::bail!("unexpected Interrupt while draining SELECT"),
        }
    }

    assert_eq!(
        seen,
        vec![1, 2, 3],
        "active SELECT should continue reading its original snapshot"
    );

    let fresh_rows: Vec<(i64,)> = conn.exec_rows("SELECT a FROM t ORDER BY a");
    assert_eq!(fresh_rows, vec![(1,), (2,), (3,), (4,)]);
    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_vacuum_into_multiple_tables(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t1 (a INTEGER)")?;
    conn.execute("CREATE TABLE t2 (b TEXT)")?;
    conn.execute("INSERT INTO t1 VALUES (1), (2), (3)")?;
    conn.execute("INSERT INTO t2 VALUES ('foo'), ('bar')")?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    let integrity_result = run_integrity_check(&dest_conn);
    assert_eq!(
        integrity_result, "ok",
        "Destination database should pass integrity check"
    );
    let dest_hash = compute_dbhash(&dest_db);
    if !tmp_db.enable_mvcc {
        assert_eq!(
            source_hash.hash, dest_hash.hash,
            "Source and destination databases should have the same content hash"
        );
    }

    let rows_t1: Vec<(i64,)> = dest_conn.exec_rows("SELECT a FROM t1 ORDER BY a");
    assert_eq!(rows_t1, vec![(1,), (2,), (3,)]);
    let rows_t2: Vec<(String,)> = dest_conn.exec_rows("SELECT b FROM t2 ORDER BY b");
    assert_eq!(rows_t2, vec![("bar".to_string(),), ("foo".to_string(),)]);
    Ok(())
}

#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(mvcc, init_sql = "CREATE TABLE t (a INTEGER);")]
fn test_vacuum_into_with_index(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE INDEX idx_t_a ON t (a)")?;
    conn.execute("INSERT INTO t VALUES (1), (2), (3)")?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    let integrity_result = run_integrity_check(&dest_conn);
    assert_eq!(
        integrity_result, "ok",
        "Destination database with index should pass integrity check"
    );
    let dest_hash = compute_dbhash(&dest_db);
    if !tmp_db.enable_mvcc {
        assert_eq!(
            source_hash.hash, dest_hash.hash,
            "Source and destination databases should have the same content hash"
        );
    }

    // lets verify that the index exists in the schema
    let schema: Vec<(String, String)> =
        dest_conn.exec_rows("SELECT type, name FROM sqlite_schema WHERE type = 'index'");
    assert!(
        schema.iter().any(|(_, name)| name == "idx_t_a"),
        "Index should exist in vacuumed database"
    );
    let rows: Vec<(i64,)> = dest_conn.exec_rows("SELECT a FROM t ORDER BY a");
    assert_eq!(rows, vec![(1,), (2,), (3,)]);

    Ok(())
}

/// Test VACUUM INTO with views (simple and complex views with aggregations)
/// Note: Views are not yet supported with MVCC
#[turso_macros::test]
fn test_vacuum_into_with_views(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute(
        "CREATE TABLE employees (id INTEGER, name TEXT, department TEXT, salary INTEGER)",
    )?;
    conn.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 100000)")?;
    conn.execute("INSERT INTO employees VALUES (2, 'Bob', 'Sales', 80000)")?;
    conn.execute("INSERT INTO employees VALUES (3, 'Charlie', 'Engineering', 120000)")?;
    conn.execute("INSERT INTO employees VALUES (4, 'Diana', 'HR', 70000)")?;

    // create multiple views: simple filter, complex filter, aggregation
    conn.execute(
        "CREATE VIEW engineering AS SELECT id, name, salary FROM employees WHERE department = 'Engineering'",
    )?;
    conn.execute(
        "CREATE VIEW high_earners AS SELECT name, salary FROM employees WHERE salary > 90000",
    )?;
    conn.execute(
        "CREATE VIEW dept_summary AS SELECT department, COUNT(*) as cnt FROM employees GROUP BY department",
    )?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    if !tmp_db.enable_mvcc {
        assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
    }

    // verify that all views exist
    let views: Vec<(String,)> =
        dest_conn.exec_rows("SELECT name FROM sqlite_schema WHERE type = 'view' ORDER BY name");
    assert_eq!(
        views,
        vec![
            ("dept_summary".to_string(),),
            ("engineering".to_string(),),
            ("high_earners".to_string(),)
        ]
    );

    // verify views query copied data correctly
    let eng: Vec<(i64, String, i64)> =
        dest_conn.exec_rows("SELECT id, name, salary FROM engineering ORDER BY id");
    assert_eq!(
        eng,
        vec![
            (1, "Alice".to_string(), 100000),
            (3, "Charlie".to_string(), 120000)
        ]
    );

    let high: Vec<(String, i64)> =
        dest_conn.exec_rows("SELECT name, salary FROM high_earners ORDER BY salary DESC");
    assert_eq!(
        high,
        vec![
            ("Charlie".to_string(), 120000),
            ("Alice".to_string(), 100000)
        ]
    );

    let summary: Vec<(String, i64)> =
        dest_conn.exec_rows("SELECT department, cnt FROM dept_summary ORDER BY department");
    assert_eq!(
        summary,
        vec![
            ("Engineering".to_string(), 2),
            ("HR".to_string(), 1),
            ("Sales".to_string(), 1)
        ]
    );

    Ok(())
}

/// Test VACUUM INTO with triggers (single and multiple).
/// Verifies that trigger definitions are preserved in the vacuumed database
/// and that data inserted by triggers during initial inserts is copied correctly.
/// Note: Triggers are not yet fully supported with MVCC
#[turso_macros::test]
fn test_vacuum_into_with_triggers(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, product_id INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE audit_log (action TEXT, tbl TEXT, record_id INTEGER)")
        .unwrap();

    conn.execute(
        "CREATE TRIGGER log_product AFTER INSERT ON products BEGIN
            INSERT INTO audit_log VALUES ('INSERT', 'products', NEW.id);
        END",
    )
    .unwrap();
    conn.execute(
        "CREATE TRIGGER log_order AFTER INSERT ON orders BEGIN
            INSERT INTO audit_log VALUES ('INSERT', 'orders', NEW.id);
        END",
    )
    .unwrap();

    // insert data (triggers will fire)
    conn.execute("INSERT INTO products VALUES (1, 'Item A'), (2, 'Item B')")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES (1, 1), (2, 2)")
        .unwrap();

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new().unwrap();
    let dest_path = dest_dir.path().join("vacuumed.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))
        .unwrap();

    let dest_opts = turso_core::DatabaseOpts::new();
    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, dest_opts);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);

    let triggers: Vec<(String,)> =
        dest_conn.exec_rows("SELECT name FROM sqlite_schema WHERE type = 'trigger' ORDER BY name");
    assert_eq!(
        triggers,
        vec![("log_order".to_string(),), ("log_product".to_string(),)]
    );

    // verify data copied (no duplicates from triggers firing during copy)
    let products: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT id, name FROM products ORDER BY id");
    assert_eq!(
        products,
        vec![(1, "Item A".to_string()), (2, "Item B".to_string())]
    );

    let audit: Vec<(String, String, i64)> =
        dest_conn.exec_rows("SELECT action, tbl, record_id FROM audit_log ORDER BY tbl, record_id");
    assert_eq!(
        audit,
        vec![
            ("INSERT".to_string(), "orders".to_string(), 1),
            ("INSERT".to_string(), "orders".to_string(), 2),
            ("INSERT".to_string(), "products".to_string(), 1),
            ("INSERT".to_string(), "products".to_string(), 2),
        ]
    );

    // verify triggers work for new inserts
    dest_conn
        .execute("INSERT INTO products VALUES (3, 'New')")
        .unwrap();
    dest_conn
        .execute("INSERT INTO orders VALUES (3, 3)")
        .unwrap();

    let new_audit: Vec<(String, String, i64)> = dest_conn
        .exec_rows("SELECT action, tbl, record_id FROM audit_log WHERE record_id = 3 ORDER BY tbl");
    assert_eq!(
        new_audit,
        vec![
            ("INSERT".to_string(), "orders".to_string(), 3),
            ("INSERT".to_string(), "products".to_string(), 3),
        ]
    );
}

/// Test VACUUM INTO preserves and bumps meta values like SQLite.
/// Note: Some pragmas don't work correctly with MVCC yet
#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(init_sql = "CREATE TABLE t (a INTEGER);")]
fn test_vacuum_into_preserves_meta_values(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES (1)")?;
    let dest_dir = TempDir::new()?;

    // Test 1: Normal positive values
    conn.execute("PRAGMA user_version = 42")?;
    conn.execute("PRAGMA application_id = 12345")?;
    let source_schema_version: Vec<(i64,)> = conn.exec_rows("PRAGMA schema_version");

    let source_hash1 = compute_dbhash(&tmp_db);
    let dest_path1 = dest_dir.path().join("vacuumed1.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path1.to_str().unwrap()))?;

    let dest_db1 = TempDatabase::new_with_existent(&dest_path1);
    let dest_conn1 = dest_db1.connect_limbo();
    assert_eq!(run_integrity_check(&dest_conn1), "ok");
    assert_eq!(source_hash1.hash, compute_dbhash(&dest_db1).hash);

    let uv: Vec<(i64,)> = dest_conn1.exec_rows("PRAGMA user_version");
    assert_eq!(uv, vec![(42,)], "user_version should be 42");
    let aid: Vec<(i64,)> = dest_conn1.exec_rows("PRAGMA application_id");
    assert_eq!(aid, vec![(12345,)], "application_id should be 12345");
    let schema_version: Vec<(i64,)> = dest_conn1.exec_rows("PRAGMA schema_version");
    assert_eq!(
        schema_version,
        vec![(source_schema_version[0].0 + 1,)],
        "schema_version should be source schema_version + 1"
    );

    // Test 2: Boundary values (negative user_version, max application_id)
    conn.execute("PRAGMA user_version = -1")?;
    conn.execute("PRAGMA application_id = 2147483647")?; // i32::MAX

    let source_hash2 = compute_dbhash(&tmp_db);
    let dest_path2 = dest_dir.path().join("vacuumed2.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path2.to_str().unwrap()))?;

    let dest_db2 = TempDatabase::new_with_existent(&dest_path2);
    let dest_conn2 = dest_db2.connect_limbo();
    assert_eq!(run_integrity_check(&dest_conn2), "ok");
    assert_eq!(source_hash2.hash, compute_dbhash(&dest_db2).hash);

    let uv: Vec<(i64,)> = dest_conn2.exec_rows("PRAGMA user_version");
    assert_eq!(uv, vec![(-1,)], "Negative user_version should be preserved");
    let aid: Vec<(i64,)> = dest_conn2.exec_rows("PRAGMA application_id");
    assert_eq!(
        aid,
        vec![(2147483647,)],
        "Max application_id should be preserved"
    );

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_vacuum_into_preserves_page_size(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    // create a new empty database and set page_size before creating tables
    let source_db = TempDatabase::new_empty();
    let conn = source_db.connect_limbo();
    // Set non-default page_size (must be done before any tables are created)
    conn.reset_page_size(8192)?;

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'hello'), (2, 'world')")?;
    let source_page_size: Vec<(i64,)> = conn.exec_rows("PRAGMA page_size");
    assert_eq!(
        source_page_size[0].0, 8192,
        "Source database should have page_size of 8192"
    );

    let source_hash = compute_dbhash(&source_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    let dest_page_size: Vec<(i64,)> = dest_conn.exec_rows("PRAGMA page_size");
    assert_eq!(
        dest_page_size[0].0, 8192,
        "page_size should be preserved as 8192 in destination database"
    );

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);

    let rows: Vec<(i64, String)> = dest_conn.exec_rows("SELECT a, b FROM t ORDER BY a, b");
    assert_eq!(
        rows,
        vec![(1, "hello".to_string()), (2, "world".to_string())]
    );
    Ok(())
}

/// Test VACUUM INTO with edge cases: empty tables with indexes, completely empty database
#[turso_macros::test(mvcc)]
fn test_vacuum_into_empty_edge_cases(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let dest_dir = TempDir::new()?;

    // Test 1: Completely empty database (no tables)
    {
        let empty_db = TempDatabase::new_empty();
        let conn = empty_db.connect_limbo();

        let schema: Vec<(String,)> =
            conn.exec_rows("SELECT name FROM sqlite_schema WHERE type = 'table'");
        assert!(schema.is_empty(), "Should have no tables");

        let dest_path = dest_dir.path().join("empty1.db");
        conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

        let dest_db = TempDatabase::new_with_existent(&dest_path);
        let dest_conn = dest_db.connect_limbo();
        assert_eq!(run_integrity_check(&dest_conn), "ok");

        // lets verify this db is usable
        dest_conn.execute("CREATE TABLE t (a INTEGER)")?;
        dest_conn.execute("INSERT INTO t VALUES (1)")?;
        let rows: Vec<(i64,)> = dest_conn.exec_rows("SELECT a FROM t");
        assert_eq!(rows, vec![(1,)]);
    }

    // Test 2: Empty tables with indexes (schema only, no data)
    {
        let conn = tmp_db.connect_limbo();
        conn.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")?;
        conn.execute("CREATE TABLE t2 (a INTEGER, b REAL)")?;
        conn.execute("CREATE INDEX idx_t1_name ON t1 (name)")?;
        conn.execute("CREATE UNIQUE INDEX idx_t2_a ON t2 (a)")?;

        let source_hash = compute_dbhash(&tmp_db);

        let dest_path = dest_dir.path().join("empty2.db");
        conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

        let dest_db = TempDatabase::new_with_existent(&dest_path);
        let dest_conn = dest_db.connect_limbo();

        assert_eq!(run_integrity_check(&dest_conn), "ok");
        if !tmp_db.enable_mvcc {
            assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
        }
        let cnt: Vec<(i64,)> = dest_conn.exec_rows("SELECT COUNT(*) FROM t1");
        assert_eq!(cnt, vec![(0,)]);

        // verify indexes exist and work
        let indexes: Vec<(String,)> = dest_conn
            .exec_rows("SELECT name FROM sqlite_schema WHERE type = 'index' ORDER BY name");
        assert_eq!(
            indexes,
            vec![("idx_t1_name".to_string(),), ("idx_t2_a".to_string(),)]
        );

        // verify unique constraint works
        dest_conn.execute("INSERT INTO t2 VALUES (1, 1.0)")?;
        let dup = dest_conn.execute("INSERT INTO t2 VALUES (1, 2.0)");
        assert!(dup.is_err(), "Unique index should prevent duplicate");
    }

    Ok(())
}

/// Test VACUUM INTO preserves AUTOINCREMENT counters (sqlite_sequence)
/// FIXME: enable for mvcc when autoincrement is fixed
#[turso_macros::test]
fn test_vacuum_into_preserves_autoincrement(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // create table with AUTOINCREMENT and insert some rows to advance the counter
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")?;
    conn.execute("INSERT INTO t (name) VALUES ('first')")?;
    conn.execute("INSERT INTO t (name) VALUES ('second')")?;
    conn.execute("INSERT INTO t (name) VALUES ('third')")?;

    // delete rows to create a gap
    conn.execute("DELETE FROM t WHERE id = 2")?;

    // verify sqlite_sequence has the counter
    let seq_before: Vec<(String, i64)> =
        conn.exec_rows("SELECT name, seq FROM sqlite_sequence WHERE name = 't'");
    assert_eq!(
        seq_before,
        vec![("t".to_string(), 3)],
        "sqlite_sequence should have counter value 3"
    );

    let source_hash = compute_dbhash(&tmp_db);
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    let dest_path_str = dest_path.to_str().unwrap();
    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    // verify integrity and dbhash (before modifying destination)
    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);

    // verify sqlite_sequence was copied
    let seq_after: Vec<(String, i64)> =
        dest_conn.exec_rows("SELECT name, seq FROM sqlite_sequence WHERE name = 't'");
    assert_eq!(
        seq_after,
        vec![("t".to_string(), 3)],
        "sqlite_sequence should be preserved in destination"
    );

    // insert a new row and verify it gets id = 4 (not 1 or 3)
    dest_conn.execute("INSERT INTO t (name) VALUES ('fourth')")?;
    let new_row: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT id, name FROM t WHERE name = 'fourth'");
    assert_eq!(
        new_row,
        vec![(4, "fourth".to_string())],
        "New row should get id = 4 (AUTOINCREMENT counter preserved)"
    );

    // verify integrity since we modified the db
    let integrity_result = run_integrity_check(&dest_conn);
    assert_eq!(integrity_result, "ok");

    Ok(())
}

/// Test VACUUM INTO preserves hidden rowid values for ordinary rowid tables.
#[turso_macros::test(mvcc)]
fn test_vacuum_into_preserves_rowid_for_rowid_tables(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (a TEXT)")?;
    conn.execute("INSERT INTO t(rowid, a) VALUES(5, 'x')")?;
    conn.execute("INSERT INTO t(rowid, a) VALUES(42, 'y')")?;

    let source_rows: Vec<(i64, String)> = conn.exec_rows("SELECT rowid, a FROM t ORDER BY rowid");
    assert_eq!(
        source_rows,
        vec![(5, "x".to_string()), (42, "y".to_string())]
    );

    let source_hash = compute_dbhash(&tmp_db);
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_rowid.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();
    assert_eq!(run_integrity_check(&dest_conn), "ok");
    if !tmp_db.enable_mvcc {
        assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
    }

    let dest_rows: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT rowid, a FROM t ORDER BY rowid");
    assert_eq!(dest_rows, vec![(5, "x".to_string()), (42, "y".to_string())]);

    Ok(())
}

/// Compare VACUUM INTO rowid behavior with SQLite reference output.
/// This captures the compatibility expectation that explicit rowids are preserved.
#[turso_macros::test(mvcc)]
fn test_vacuum_into_rowid_compat_with_sqlite_reference(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (a TEXT)")?;
    conn.execute("INSERT INTO t(rowid, a) VALUES(5, 'x')")?;
    conn.execute("INSERT INTO t(rowid, a) VALUES(42, 'y')")?;
    let hash_opts = turso_dbhash::DbHashOptions {
        table_filter: Some("t".to_string()),
        ..Default::default()
    };
    let source_hash =
        compute_dbhash_with_options_and_database_opts(&tmp_db, &hash_opts, tmp_db.db_opts);

    let dest_dir = TempDir::new()?;
    let turso_dest = dest_dir.path().join("turso_rowid_vacuum.db");
    conn.execute(format!("VACUUM INTO '{}'", turso_dest.to_str().unwrap()))?;
    let turso_dest_db = TempDatabase::new_with_existent_with_opts(&turso_dest, tmp_db.db_opts);
    let turso_dest_conn = turso_dest_db.connect_limbo();
    assert_eq!(run_integrity_check(&turso_dest_conn), "ok");
    assert_eq!(
        source_hash.hash,
        compute_dbhash_with_options_and_database_opts(&turso_dest_db, &hash_opts, tmp_db.db_opts)
            .hash
    );
    let turso_rows: Vec<(i64, String)> =
        turso_dest_conn.exec_rows("SELECT rowid, a FROM t ORDER BY rowid");

    let sqlite_src = dest_dir.path().join("sqlite_rowid_src.db");
    let sqlite_dest = dest_dir.path().join("sqlite_rowid_vacuum.db");
    let sqlite_conn = SqliteConnection::open(&sqlite_src)?;
    sqlite_conn.execute_batch(
        "CREATE TABLE t (a TEXT);
         INSERT INTO t(rowid, a) VALUES(5, 'x');
         INSERT INTO t(rowid, a) VALUES(42, 'y');",
    )?;
    let sqlite_dest_escaped = escape_sqlite_string_literal(sqlite_dest.to_str().unwrap());
    sqlite_conn.execute(&format!("VACUUM INTO '{sqlite_dest_escaped}'"), [])?;
    drop(sqlite_conn);

    let sqlite_dest_conn = SqliteConnection::open(&sqlite_dest)?;
    let mut sqlite_stmt = sqlite_dest_conn.prepare("SELECT rowid, a FROM t ORDER BY rowid")?;
    let sqlite_rows = sqlite_stmt
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(turso_rows, sqlite_rows);
    assert_eq!(
        turso_rows,
        vec![(5, "x".to_string()), (42, "y".to_string())]
    );

    Ok(())
}

/// Compare VACUUM INTO compatibility for explicit INTEGER PRIMARY KEY and indexed tables.
/// Includes normal, unique, and partial indexes to cover index-heavy table rewrites.
#[turso_macros::test(mvcc)]
fn test_vacuum_into_integer_pk_and_indexes_compat_with_sqlite(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute(
        "CREATE TABLE t_idx (id INTEGER PRIMARY KEY, a TEXT NOT NULL, b INTEGER NOT NULL)",
    )?;
    conn.execute("CREATE INDEX idx_t_idx_b ON t_idx(b)")?;
    conn.execute("CREATE UNIQUE INDEX idx_t_idx_a_unique ON t_idx(a)")?;
    conn.execute("CREATE INDEX idx_t_idx_partial ON t_idx(a) WHERE b >= 100")?;
    conn.execute("INSERT INTO t_idx(id, a, b) VALUES (10, 'alpha', 50)")?;
    conn.execute("INSERT INTO t_idx(id, a, b) VALUES (25, 'beta', 150)")?;
    conn.execute("INSERT INTO t_idx(id, a, b) VALUES (50, 'gamma', 200)")?;
    let hash_opts = turso_dbhash::DbHashOptions {
        table_filter: Some("t_idx".to_string()),
        ..Default::default()
    };
    let source_hash =
        compute_dbhash_with_options_and_database_opts(&tmp_db, &hash_opts, tmp_db.db_opts);

    let dest_dir = TempDir::new()?;
    let turso_dest = dest_dir.path().join("turso_index_vacuum.db");
    conn.execute(format!("VACUUM INTO '{}'", turso_dest.to_str().unwrap()))?;
    let turso_dest_db = TempDatabase::new_with_existent_with_opts(&turso_dest, tmp_db.db_opts);
    let turso_dest_conn = turso_dest_db.connect_limbo();
    assert_eq!(run_integrity_check(&turso_dest_conn), "ok");
    assert_eq!(
        source_hash.hash,
        compute_dbhash_with_options_and_database_opts(&turso_dest_db, &hash_opts, tmp_db.db_opts)
            .hash
    );
    let turso_rows: Vec<(i64, i64, String, i64)> =
        turso_dest_conn.exec_rows("SELECT rowid, id, a, b FROM t_idx ORDER BY id");
    let turso_index_names_rows: Vec<(String,)> = turso_dest_conn.exec_rows(
        "SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = 't_idx' ORDER BY name",
    );
    let turso_index_names: Vec<String> = turso_index_names_rows
        .into_iter()
        .map(|(name,)| name)
        .collect();

    let sqlite_src = dest_dir.path().join("sqlite_index_src.db");
    let sqlite_dest = dest_dir.path().join("sqlite_index_vacuum.db");
    let sqlite_conn = SqliteConnection::open(&sqlite_src)?;
    sqlite_conn.execute_batch(
        "CREATE TABLE t_idx (id INTEGER PRIMARY KEY, a TEXT NOT NULL, b INTEGER NOT NULL);
         CREATE INDEX idx_t_idx_b ON t_idx(b);
         CREATE UNIQUE INDEX idx_t_idx_a_unique ON t_idx(a);
         CREATE INDEX idx_t_idx_partial ON t_idx(a) WHERE b >= 100;
         INSERT INTO t_idx(id, a, b) VALUES (10, 'alpha', 50);
         INSERT INTO t_idx(id, a, b) VALUES (25, 'beta', 150);
         INSERT INTO t_idx(id, a, b) VALUES (50, 'gamma', 200);",
    )?;
    let sqlite_dest_escaped = escape_sqlite_string_literal(sqlite_dest.to_str().unwrap());
    sqlite_conn.execute(&format!("VACUUM INTO '{sqlite_dest_escaped}'"), [])?;
    drop(sqlite_conn);

    let sqlite_dest_conn = SqliteConnection::open(&sqlite_dest)?;
    let mut sqlite_rows_stmt =
        sqlite_dest_conn.prepare("SELECT rowid, id, a, b FROM t_idx ORDER BY id")?;
    let sqlite_rows = sqlite_rows_stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut sqlite_index_stmt = sqlite_dest_conn.prepare(
        "SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = 't_idx' ORDER BY name",
    )?;
    let sqlite_index_names = sqlite_index_stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(turso_rows, sqlite_rows);
    assert_eq!(
        turso_rows,
        vec![
            (10, 10, "alpha".to_string(), 50),
            (25, 25, "beta".to_string(), 150),
            (50, 50, "gamma".to_string(), 200),
        ]
    );
    assert_eq!(turso_index_names, sqlite_index_names);
    assert_eq!(
        turso_index_names,
        vec![
            "idx_t_idx_a_unique".to_string(),
            "idx_t_idx_b".to_string(),
            "idx_t_idx_partial".to_string(),
        ]
    );

    Ok(())
}

/// Test VACUUM INTO preserves hidden rowid values when "rowid" column name is shadowed.
#[turso_macros::test(mvcc)]
fn test_vacuum_into_preserves_rowid_when_rowid_alias_is_shadowed(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (rowid TEXT, a TEXT)")?;
    conn.execute("INSERT INTO t(_rowid_, rowid, a) VALUES(77, 'visible', 'x')")?;

    let source_rows: Vec<(i64, String, String)> =
        conn.exec_rows("SELECT _rowid_, rowid, a FROM t ORDER BY _rowid_");
    assert_eq!(
        source_rows,
        vec![(77, "visible".to_string(), "x".to_string())]
    );
    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_rowid_shadowed.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();
    assert_eq!(run_integrity_check(&dest_conn), "ok");
    if !tmp_db.enable_mvcc {
        assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
    }

    let dest_rows: Vec<(i64, String, String)> =
        dest_conn.exec_rows("SELECT _rowid_, rowid, a FROM t ORDER BY _rowid_");
    assert_eq!(
        dest_rows,
        vec![(77, "visible".to_string(), "x".to_string())]
    );

    Ok(())
}

/// Test VACUUM INTO succeeds when all hidden rowid aliases are shadowed by real columns.
#[turso_macros::test(mvcc)]
fn test_vacuum_into_when_all_rowid_aliases_are_shadowed(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (rowid TEXT, _rowid_ TEXT, oid TEXT, a TEXT)")?;
    conn.execute("INSERT INTO t(rowid, _rowid_, oid, a) VALUES('r1', 'u1', 'o1', 'x')")?;
    conn.execute("INSERT INTO t(rowid, _rowid_, oid, a) VALUES('r2', 'u2', 'o2', 'y')")?;

    let source_rows: Vec<(String, String, String, String)> =
        conn.exec_rows("SELECT rowid, _rowid_, oid, a FROM t ORDER BY a");
    assert_eq!(
        source_rows,
        vec![
            (
                "r1".to_string(),
                "u1".to_string(),
                "o1".to_string(),
                "x".to_string()
            ),
            (
                "r2".to_string(),
                "u2".to_string(),
                "o2".to_string(),
                "y".to_string()
            ),
        ]
    );
    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_all_aliases_shadowed.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();
    assert_eq!(run_integrity_check(&dest_conn), "ok");
    if !tmp_db.enable_mvcc {
        assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
    }

    let dest_rows: Vec<(String, String, String, String)> =
        dest_conn.exec_rows("SELECT rowid, _rowid_, oid, a FROM t ORDER BY a");
    assert_eq!(
        dest_rows,
        vec![
            (
                "r1".to_string(),
                "u1".to_string(),
                "o1".to_string(),
                "x".to_string()
            ),
            (
                "r2".to_string(),
                "u2".to_string(),
                "o2".to_string(),
                "y".to_string()
            ),
        ]
    );

    Ok(())
}

/// Test that a table with "sqlite_sequence" in its SQL (e.g., default value) is NOT skipped
#[turso_macros::test(mvcc)]
fn test_vacuum_into_table_with_sqlite_sequence_in_sql(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // create a table that mentions "sqlite_sequence" in a default value
    // this should NOT be skipped during schema copy
    conn.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, content TEXT DEFAULT 'see sqlite_sequence')",
    )?;
    conn.execute("INSERT INTO notes (id) VALUES (1)")?;
    conn.execute("INSERT INTO notes (id, content) VALUES (2, 'custom')")?;

    let source_hash = compute_dbhash(&tmp_db);
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;
    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    if !tmp_db.enable_mvcc {
        assert_eq!(
            source_hash.hash,
            compute_dbhash(&dest_db).hash,
            "Source and destination databases should have the same content hash"
        );
    }

    // verify the table was created and data was copied
    let rows: Vec<(i64, String)> = dest_conn.exec_rows("SELECT id, content FROM notes ORDER BY id");
    assert_eq!(
        rows,
        vec![
            (1, "see sqlite_sequence".to_string()),
            (2, "custom".to_string())
        ],
        "Table with 'sqlite_sequence' in SQL should be created and data copied"
    );
    Ok(())
}

/// Test VACUUM INTO with table names containing special characters
/// tests for: spaces, quotes, SQL keywords, unicode, numeric names, and mixed special chars
#[turso_macros::test(mvcc)]
fn test_vacuum_into_special_table_names(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // 1. Table with spaces
    conn.execute(r#"CREATE TABLE "table with spaces" (id INTEGER, value TEXT)"#)?;
    conn.execute(r#"INSERT INTO "table with spaces" VALUES (1, 'spaces work')"#)?;

    // 2. Table with double quotes
    conn.execute(r#"CREATE TABLE "table""quote" (id INTEGER, data TEXT)"#)?;
    conn.execute(r#"INSERT INTO "table""quote" VALUES (2, 'quotes work')"#)?;

    // 3. SQL reserved keyword as table name
    conn.execute(r#"CREATE TABLE "select" (id INTEGER, val TEXT)"#)?;
    conn.execute(r#"INSERT INTO "select" VALUES (3, 'keyword works')"#)?;

    // 4. Unicode table name
    conn.execute(r#"CREATE TABLE "表格_données_🎉" (id INTEGER, val TEXT)"#)?;
    conn.execute(r#"INSERT INTO "表格_données_🎉" VALUES (4, 'unicode works')"#)?;

    // 5. Numeric table name
    conn.execute(r#"CREATE TABLE "123" (id INTEGER, val TEXT)"#)?;
    conn.execute(r#"INSERT INTO "123" VALUES (5, 'numeric works')"#)?;

    // 6. Mixed special characters (multiple quotes, spaces, SQL-injection-like)
    conn.execute(r#"CREATE TABLE "table ""with"" many; DROP TABLE--" (id INTEGER, val TEXT)"#)?;
    conn.execute(r#"INSERT INTO "table ""with"" many; DROP TABLE--" VALUES (6, 'mixed works')"#)?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_tables.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(
        run_integrity_check(&dest_conn),
        "ok",
        "Destination should pass integrity check"
    );
    if !tmp_db.enable_mvcc {
        assert_eq!(
            source_hash.hash,
            compute_dbhash(&dest_db).hash,
            "Source and destination databases should have the same content hash"
        );
    }

    // verify all tables were copied correctly
    let r1: Vec<(i64, String)> = dest_conn.exec_rows(r#"SELECT * FROM "table with spaces""#);
    assert_eq!(r1, vec![(1, "spaces work".to_string())]);

    let r2: Vec<(i64, String)> = dest_conn.exec_rows(r#"SELECT * FROM "table""quote""#);
    assert_eq!(r2, vec![(2, "quotes work".to_string())]);

    let r3: Vec<(i64, String)> = dest_conn.exec_rows(r#"SELECT * FROM "select""#);
    assert_eq!(r3, vec![(3, "keyword works".to_string())]);

    let r4: Vec<(i64, String)> = dest_conn.exec_rows(r#"SELECT * FROM "表格_données_🎉""#);
    assert_eq!(r4, vec![(4, "unicode works".to_string())]);

    let r5: Vec<(i64, String)> = dest_conn.exec_rows(r#"SELECT * FROM "123""#);
    assert_eq!(r5, vec![(5, "numeric works".to_string())]);

    let r6: Vec<(i64, String)> =
        dest_conn.exec_rows(r#"SELECT * FROM "table ""with"" many; DROP TABLE--""#);
    assert_eq!(r6, vec![(6, "mixed works".to_string())]);

    Ok(())
}

/// Test VACUUM INTO preserves float precision
#[turso_macros::test(mvcc)]
fn test_vacuum_into_preserves_float_precision(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE floats (id INTEGER PRIMARY KEY, value REAL)")?;

    // Insert various floats that require high precision
    // These values are chosen to test edge cases in float representation
    let test_values: Vec<f64> = vec![
        0.1,                    // Classic binary representation issue
        0.123456789012345,      // Many decimal places
        1.0000000000000002,     // Smallest increment above 1.0
        std::f64::consts::PI,   // Pi (3.141592653589793)
        std::f64::consts::E,    // Euler's number (2.718281828459045)
        1e-10,                  // Very small number
        1e15,                   // Large number
        -0.999999999999999,     // Negative with many 9s
        123_456_789.123_456_79, // Large with decimals
        1.0,                    // Integer-like float (must stay float, not become int)
        -2.0,                   // Negative integer-like float
        0.0,                    // Zero as float
        100.0,                  // Larger integer-like float
    ];

    for (i, &val) in test_values.iter().enumerate() {
        conn.execute(format!(
            "INSERT INTO floats VALUES ({}, {:.17})",
            i + 1,
            val
        ))?;
    }

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();
    assert_eq!(run_integrity_check(&dest_conn), "ok");
    if !tmp_db.enable_mvcc {
        assert_eq!(
            source_hash.hash,
            compute_dbhash(&dest_db).hash,
            "Source and destination databases should have the same content hash"
        );
    }

    // verify float precision is preserved
    let rows: Vec<(i64, f64)> = dest_conn.exec_rows("SELECT id, value FROM floats ORDER BY id");
    assert_eq!(rows.len(), test_values.len());

    for (i, &expected) in test_values.iter().enumerate() {
        let actual = rows[i].1;
        assert!(
            (actual - expected).abs() < 1e-15 || actual == expected,
            "Float precision lost for value {}: expected {:.17}, got {:.17}",
            i + 1,
            expected,
            actual
        );
    }
    Ok(())
}

/// Test VACUUM INTO with column names containing special characters
/// Consolidates tests for: spaces, quotes, SQL keywords, unicode, numeric, dashes, dots,
/// mixed special chars, and indexes on special columns
#[turso_macros::test(mvcc)]
fn test_vacuum_into_special_column_names(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // table with various special column names covering all edge cases
    conn.execute(
        r#"CREATE TABLE special_cols (
            "column with spaces" INTEGER,
            "column""with""quotes" TEXT,
            "from" INTEGER,
            "列名_données_🎉" TEXT,
            "123numeric" REAL,
            "col.with" INTEGER,
            "SELECT * FROM t; --" TEXT
        )"#,
    )?;

    // create index on column with special name
    conn.execute(r#"CREATE INDEX "idx on special" ON special_cols ("column with spaces")"#)?;
    conn.execute(r#"CREATE INDEX "idx""quoted" ON special_cols ("column""with""quotes")"#)?;

    conn.execute(
        r#"INSERT INTO special_cols VALUES (1, 'quotes', 10, 'unicode', 1.5, 100, 'injection')"#,
    )?;
    conn.execute(r#"INSERT INTO special_cols VALUES (2, 'work', 20, 'works', 2.5, 200, 'safe')"#)?;

    let source_hash = compute_dbhash(&tmp_db);
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_cols.db");
    let dest_path_str = dest_path.to_str().unwrap();
    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(
        run_integrity_check(&dest_conn),
        "ok",
        "Destination should pass integrity check"
    );
    if !tmp_db.enable_mvcc {
        assert_eq!(
            source_hash.hash,
            compute_dbhash(&dest_db).hash,
            "Source and destination databases should have the same content hash"
        );
    }

    // verify all column data was copied correctly
    let rows: Vec<(i64, String, i64, String, f64, i64, String)> = dest_conn.exec_rows(
        r#"SELECT "column with spaces", "column""with""quotes", "from", "列名_données_🎉", "123numeric", "col.with", "SELECT * FROM t; --" FROM special_cols ORDER BY "column with spaces""#,
    );
    assert_eq!(
        rows,
        vec![
            (
                1,
                "quotes".to_string(),
                10,
                "unicode".to_string(),
                1.5,
                100,
                "injection".to_string()
            ),
            (
                2,
                "work".to_string(),
                20,
                "works".to_string(),
                2.5,
                200,
                "safe".to_string()
            )
        ]
    );

    let indexes: Vec<(String,)> = dest_conn.exec_rows(
        r#"SELECT name FROM sqlite_schema WHERE type = 'index' AND name LIKE 'idx%' ORDER BY name"#,
    );
    assert_eq!(
        indexes,
        vec![
            ("idx on special".to_string(),),
            ("idx\"quoted".to_string(),)
        ]
    );

    Ok(())
}

/// Test VACUUM INTO with large blobs that trigger overflow pages
/// Each 8KiB blob exceeds the 4KiB page size, forcing overflow page usage
/// Note: page_count pragma doesn't work correctly with MVCC yet
#[turso_macros::test]
fn test_vacuum_into_large_data_multi_page(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // Create table for large blob storage
    conn.execute("CREATE TABLE large_data (id INTEGER PRIMARY KEY, data BLOB)")?;

    // Insert 100 rows with 8KiB blobs each - larger than page_size (4096),
    // so each row requires overflow pages
    for i in 0..100 {
        conn.execute(format!(
            "INSERT INTO large_data VALUES ({i}, randomblob(8192))"
        ))?;
    }

    let source_hash = compute_dbhash(&tmp_db);
    let page_count: Vec<(i64,)> = conn.exec_rows("PRAGMA page_count");
    assert!(
        page_count[0].0 > 10,
        "Source should have multiple pages, got: {}",
        page_count[0].0
    );

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_large.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(
        run_integrity_check(&dest_conn),
        "ok",
        "Large database should pass integrity check"
    );
    if !tmp_db.enable_mvcc {
        assert_eq!(
            source_hash.hash,
            compute_dbhash(&dest_db).hash,
            "Source and destination should have same content hash"
        );
    }

    // Verify row count
    let count: Vec<(i64,)> = dest_conn.exec_rows("SELECT COUNT(*) FROM large_data");
    assert_eq!(count[0].0, 100, "All 100 rows should be copied");

    // Verify blob sizes are preserved
    let sizes: Vec<(i64,)> =
        dest_conn.exec_rows("SELECT length(data) FROM large_data WHERE id IN (0, 50, 99)");
    assert_eq!(sizes.len(), 3);
    for (size,) in sizes {
        assert_eq!(size, 8192, "Blob size should be preserved");
    }

    Ok(())
}

/// VACUUM INTO must leave the main destination file self-contained, even when
/// the copied image is larger than the WAL auto-checkpoint threshold.
#[turso_macros::test]
fn test_vacuum_into_large_destination_survives_without_wal(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE large_data (id INTEGER PRIMARY KEY, data BLOB)")?;
    conn.execute(
        "INSERT INTO large_data \
         SELECT value, randomblob(4096) \
         FROM generate_series(1, 1200)",
    )?;

    let source_pages: Vec<(i64,)> = conn.exec_rows("PRAGMA page_count");
    assert!(
        source_pages[0].0 > 1000,
        "source should exceed the auto-checkpoint threshold, got {} pages",
        source_pages[0].0
    );
    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_large_no_wal.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let copied_dir = TempDir::new()?;
    let copied_path = copied_dir.path().join("copied.db");
    std::fs::copy(&dest_path, &copied_path)?;
    assert!(
        !copied_dir.path().join("copied.db-wal").exists(),
        "test must copy only the destination database file"
    );

    let copied_db = TempDatabase::new_with_existent(&copied_path);
    let copied_conn = copied_db.connect_limbo();

    assert_eq!(run_integrity_check(&copied_conn), "ok");
    assert_eq!(source_hash.hash, compute_dbhash(&copied_db).hash);

    let copied_pages: Vec<(i64,)> = copied_conn.exec_rows("PRAGMA page_count");
    assert!(
        copied_pages[0].0 > 1000,
        "copied destination should remain large, got {} pages",
        copied_pages[0].0
    );

    let stats: Vec<(i64, i64, i64)> = copied_conn
        .exec_rows("SELECT COUNT(*), MIN(length(data)), MAX(length(data)) FROM large_data");
    assert_eq!(stats, vec![(1200, 4096, 4096)]);

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_vacuum_into_with_foreign_keys(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")?;
    conn.execute(
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category_id INTEGER REFERENCES categories(id) ON DELETE CASCADE
        )",
    )?;
    conn.execute(
        "INSERT INTO categories VALUES (1, 'Electronics'), (2, 'Books'), (3, 'Clothing')",
    )?;
    conn.execute("INSERT INTO products VALUES (1, 'Laptop', 1), (2, 'Phone', 1), (3, 'Novel', 2)")?;

    let source_hash = compute_dbhash(&tmp_db);
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_fk.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;
    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    if !tmp_db.enable_mvcc {
        assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
    }

    // verify schema includes foreign key
    let products_sql: Vec<(String,)> =
        dest_conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'products'");
    assert!(
        products_sql[0].0.contains("REFERENCES"),
        "Foreign key should be preserved in schema"
    );

    let categories: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT id, name FROM categories ORDER BY id");
    assert_eq!(
        categories,
        vec![
            (1, "Electronics".to_string()),
            (2, "Books".to_string()),
            (3, "Clothing".to_string())
        ]
    );

    let products: Vec<(i64, String, i64)> =
        dest_conn.exec_rows("SELECT id, name, category_id FROM products ORDER BY id");
    assert_eq!(
        products,
        vec![
            (1, "Laptop".to_string(), 1),
            (2, "Phone".to_string(), 1),
            (3, "Novel".to_string(), 2)
        ]
    );

    Ok(())
}

/// Test VACUUM INTO with composite primary keys
#[turso_macros::test(mvcc)]
fn test_vacuum_into_with_composite_primary_key(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute(
        "CREATE TABLE order_items (
            order_id INTEGER,
            item_id INTEGER,
            quantity INTEGER NOT NULL,
            price REAL NOT NULL,
            PRIMARY KEY (order_id, item_id)
        )",
    )?;

    // create a many-to-many relationship table
    conn.execute(
        "CREATE TABLE student_courses (
            student_id INTEGER,
            course_id INTEGER,
            enrollment_date TEXT,
            grade TEXT,
            PRIMARY KEY (student_id, course_id)
        )",
    )?;

    conn.execute(
        "INSERT INTO order_items VALUES (1, 1, 2, 10.0), (1, 2, 1, 20.0), (2, 1, 5, 10.0)",
    )?;
    conn.execute(
        "INSERT INTO student_courses VALUES (1, 101, '2024-01-15', 'A'), (1, 102, '2024-01-16', 'B'), (2, 101, '2024-01-15', 'A')",
    )?;

    let source_hash = compute_dbhash(&tmp_db);
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_composite.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    if !tmp_db.enable_mvcc {
        assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
    }

    let order_items_sql: Vec<(String,)> =
        dest_conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'order_items'");
    assert!(
        order_items_sql[0].0.contains("PRIMARY KEY"),
        "PRIMARY KEY should be in schema"
    );

    let order_items: Vec<(i64, i64, i64, f64)> = dest_conn.exec_rows(
        "SELECT order_id, item_id, quantity, price FROM order_items ORDER BY order_id, item_id",
    );
    assert_eq!(
        order_items,
        vec![(1, 1, 2, 10.0), (1, 2, 1, 20.0), (2, 1, 5, 10.0)]
    );

    let student_courses: Vec<(i64, i64, String, String)> = dest_conn.exec_rows(
        "SELECT student_id, course_id, enrollment_date, grade FROM student_courses ORDER BY student_id, course_id",
    );
    assert_eq!(
        student_courses,
        vec![
            (1, 101, "2024-01-15".to_string(), "A".to_string()),
            (1, 102, "2024-01-16".to_string(), "B".to_string()),
            (2, 101, "2024-01-15".to_string(), "A".to_string())
        ]
    );

    // verify composite primary key constraint is enforced on dest db
    let duplicate = dest_conn.execute("INSERT INTO order_items VALUES (1, 1, 3, 15.0)");
    assert!(
        duplicate.is_err(),
        "Composite primary key should reject duplicate (order_id, item_id)"
    );

    Ok(())
}

/// Test VACUUM INTO with data populated using table-valued functions
/// (generate_series, json_each, json_tree)
#[turso_macros::test(mvcc)]
fn test_vacuum_into_with_table_valued_functions(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE numbers (n INTEGER PRIMARY KEY)")?;
    conn.execute("INSERT INTO numbers SELECT value FROM generate_series(1, 100)")?;

    conn.execute("CREATE TABLE json_data (id INTEGER PRIMARY KEY, data TEXT)")?;
    conn.execute(r#"INSERT INTO json_data VALUES (1, '{"a": 1, "b": 2, "c": 3}')"#)?;
    conn.execute(r#"INSERT INTO json_data VALUES (2, '{"x": 10, "y": 20}')"#)?;

    conn.execute("CREATE TABLE json_keys (id INTEGER, key TEXT)")?;
    conn.execute("INSERT INTO json_keys SELECT j.id, e.key FROM json_data j, json_each(j.data) e")?;

    conn.execute("CREATE TABLE nested_json (id INTEGER PRIMARY KEY, data TEXT)")?;
    conn.execute(r#"INSERT INTO nested_json VALUES (1, '{"root": {"child": [1, 2, 3]}}')"#)?;

    conn.execute("CREATE TABLE json_paths (id INTEGER, fullkey TEXT, type TEXT)")?;
    conn.execute(
        "INSERT INTO json_paths SELECT n.id, t.fullkey, t.type FROM nested_json n, json_tree(n.data) t",
    )?;

    let source_hash = compute_dbhash(&tmp_db);
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_tvf.db");

    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;
    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    if !tmp_db.enable_mvcc {
        assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
    }

    // verify generate_series data
    let count: Vec<(i64,)> = dest_conn.exec_rows("SELECT COUNT(*) FROM numbers");
    assert_eq!(count[0].0, 100);
    let sum: Vec<(i64,)> = dest_conn.exec_rows("SELECT SUM(n) FROM numbers");
    assert_eq!(sum[0].0, 5050); // 1+2+...+100 = 5050

    // verify json_each data
    let json_keys: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT id, key FROM json_keys ORDER BY id, key");
    assert_eq!(
        json_keys,
        vec![
            (1, "a".to_string()),
            (1, "b".to_string()),
            (1, "c".to_string()),
            (2, "x".to_string()),
            (2, "y".to_string()),
        ]
    );

    // verify json_tree data
    let json_paths: Vec<(i64, String, String)> =
        dest_conn.exec_rows("SELECT id, fullkey, type FROM json_paths ORDER BY fullkey");
    assert!(!json_paths.is_empty(), "json_tree data should be copied");
    assert!(
        json_paths.iter().any(|(_, path, _)| path.contains("root")),
        "Should have root path"
    );

    Ok(())
}

/// VACUUM INTO must handle deferred index creation after a large
/// generate_series-backed data load.
#[turso_macros::test]
fn test_vacuum_into_large_generate_series_with_index(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT)")?;
    conn.execute(
        "INSERT INTO t1 \
         SELECT value, 'padding_padding_padding_padding_' || value \
         FROM generate_series(1, 10000)",
    )?;
    conn.execute("CREATE INDEX idx ON t1(b)")?;

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_generate_series_index.db");
    let (_dest_db, dest_conn) = run_vacuum_into_and_assert_round_trip(&tmp_db, &conn, &dest_path)?;

    let count: Vec<(i64,)> = dest_conn.exec_rows("SELECT COUNT(*) FROM t1");
    assert_eq!(count, vec![(10000,)]);

    let row: Vec<(i64, String)> = dest_conn.exec_rows(
        "SELECT a, b FROM t1 INDEXED BY idx \
         WHERE b = 'padding_padding_padding_padding_9999'",
    );
    assert_eq!(
        row,
        vec![(9999, "padding_padding_padding_padding_9999".to_string())]
    );

    Ok(())
}

/// Test VACUUM INTO preserves reserved_space bytes from source database.
/// Reserved space is used by encryption extensions and checksums.
#[turso_macros::test(mvcc)]
fn test_vacuum_into_preserves_reserved_space(tmp_db: TempDatabase) -> anyhow::Result<()> {
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};

    const RESERVED_BYTES: u8 = 32;
    const RESERVED_SPACE_OFFSET: u64 = 20; // Offset in SQLite header

    let conn = tmp_db.connect_limbo();

    // set reserved_bytes BEFORE any table creation (must be done on uninitialized db)
    conn.set_reserved_bytes(RESERVED_BYTES)?;
    conn.execute("CREATE TABLE encrypted_data (id INTEGER PRIMARY KEY, secret TEXT)")?;
    conn.execute("INSERT INTO encrypted_data VALUES (1, 'confidential')")?;
    conn.execute("INSERT INTO encrypted_data VALUES (2, 'private')")?;

    // verify source has reserved_space set
    assert_eq!(
        conn.get_reserved_bytes(),
        Some(RESERVED_BYTES),
        "Source should have reserved_bytes={RESERVED_BYTES}"
    );

    let source_hash = compute_dbhash(&tmp_db);
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_reserved.db");
    let dest_path_str = dest_path.to_str().unwrap();
    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    if !tmp_db.enable_mvcc {
        assert_eq!(
            source_hash.hash,
            compute_dbhash(&dest_db).hash,
            "Source and destination should have same content hash"
        );
    }

    assert_eq!(
        dest_conn.get_reserved_bytes(),
        Some(RESERVED_BYTES),
        "Destination should have reserved_bytes={RESERVED_BYTES}"
    );

    // verify reserved_space in destination file header
    {
        let mut file = File::open(&dest_path)?;
        file.seek(SeekFrom::Start(RESERVED_SPACE_OFFSET))?;
        let mut buf = [0u8; 1];
        file.read_exact(&mut buf)?;
        assert_eq!(
            buf[0], RESERVED_BYTES,
            "Destination file header should have reserved_space={RESERVED_BYTES}"
        );
    }

    let rows: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT id, secret FROM encrypted_data ORDER BY id");
    assert_eq!(
        rows,
        vec![(1, "confidential".to_string()), (2, "private".to_string())]
    );
    Ok(())
}

/// Test VACUUM INTO with partial indexes (CREATE INDEX ... WHERE)
/// Note: Partial indexes are not supported with MVCC
#[turso_macros::test]
fn test_vacuum_into_with_partial_indexes(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer TEXT,
            status TEXT,
            amount REAL
        )",
    )?;
    conn.execute(
        "CREATE INDEX idx_pending_orders ON orders (customer, amount) WHERE status = 'pending'",
    )
    .unwrap();
    // create another partial index for variety
    conn.execute("CREATE INDEX idx_large_orders ON orders (customer) WHERE amount > 1000")?;

    // insert data (some matching the partial index conditions, some not)
    conn.execute("INSERT INTO orders VALUES (1, 'Alice', 'pending', 500.0)")?;
    conn.execute("INSERT INTO orders VALUES (2, 'Bob', 'completed', 200.0)")?;
    conn.execute("INSERT INTO orders VALUES (3, 'Alice', 'pending', 1500.0)")?;
    conn.execute("INSERT INTO orders VALUES (4, 'Charlie', 'shipped', 2000.0)")?;
    conn.execute("INSERT INTO orders VALUES (5, 'Bob', 'pending', 100.0)")?;

    assert_eq!(run_integrity_check(&conn), "ok");

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_partial_idx.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");

    if !tmp_db.enable_mvcc {
        assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
    }

    // verify partial indexes exist with WHERE clause in schema
    let indexes: Vec<(String, String)> = dest_conn.exec_rows(
                "SELECT name, sql FROM sqlite_schema WHERE type = 'index' AND name LIKE 'idx_%' ORDER BY name",
            );
    assert_eq!(indexes.len(), 2);

    // verify idx_large_orders has WHERE clause
    let large_idx = indexes.iter().find(|(name, _)| name == "idx_large_orders");
    assert!(large_idx.is_some(), "idx_large_orders should exist");
    assert!(
        large_idx.unwrap().1.contains("WHERE"),
        "Partial index should have WHERE clause"
    );

    // verify idx_pending_orders has WHERE clause
    let pending_idx = indexes
        .iter()
        .find(|(name, _)| name == "idx_pending_orders");
    assert!(pending_idx.is_some(), "idx_pending_orders should exist");
    assert!(
        pending_idx.unwrap().1.contains("WHERE"),
        "Partial index should have WHERE clause"
    );

    // verify data was copied (table data is correct even if indexes are not)
    let orders: Vec<(i64, String, String, f64)> =
        dest_conn.exec_rows("SELECT id, customer, status, amount FROM orders ORDER BY id");
    assert_eq!(
        orders,
        vec![
            (1, "Alice".to_string(), "pending".to_string(), 500.0),
            (2, "Bob".to_string(), "completed".to_string(), 200.0),
            (3, "Alice".to_string(), "pending".to_string(), 1500.0),
            (4, "Charlie".to_string(), "shipped".to_string(), 2000.0),
            (5, "Bob".to_string(), "pending".to_string(), 100.0)
        ]
    );
    Ok(())
}

/// Test VACUUM INTO with mixed index types: normal, unique, partial, expression.
/// Note: Partial indexes are not supported with MVCC.
#[turso_macros::test]
fn test_vacuum_into_with_mixed_index_types(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute(
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL,
            stock INTEGER NOT NULL,
            discontinued INTEGER NOT NULL DEFAULT 0
        )",
    )?;

    conn.execute("CREATE UNIQUE INDEX idx_products_name ON products(name)")?;
    conn.execute("CREATE INDEX idx_products_category_price ON products(category, price)")?;
    conn.execute(
        "CREATE INDEX idx_products_instock ON products(category) WHERE stock > 0 AND discontinued = 0",
    )?;
    conn.execute("CREATE INDEX idx_products_name_expr ON products(lower(name), abs(price))")?;

    conn.execute("INSERT INTO products VALUES (1, 'Widget A', 'tools', 10.5, 5, 0)")?;
    conn.execute("INSERT INTO products VALUES (2, 'Widget B', 'tools', 12.0, 0, 0)")?;
    conn.execute("INSERT INTO products VALUES (3, 'Gadget C', 'electronics', 99.9, 10, 0)")?;
    conn.execute("INSERT INTO products VALUES (4, 'Legacy D', 'electronics', 49.5, 3, 1)")?;
    conn.execute("INSERT INTO products VALUES (5, 'Spare E', 'parts', 4.25, 25, 0)")?;

    assert_eq!(run_integrity_check(&conn), "ok");
    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_mixed_indexes.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    if !tmp_db.enable_mvcc {
        assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
    }

    let index_defs: Vec<(String, String)> = dest_conn.exec_rows(
        "SELECT name, sql FROM sqlite_schema
         WHERE type = 'index'
           AND name IN (
               'idx_products_name',
               'idx_products_category_price',
               'idx_products_instock',
               'idx_products_name_expr'
           )
         ORDER BY name",
    );
    assert_eq!(index_defs.len(), 4);

    let partial_idx_sql = &index_defs
        .iter()
        .find(|(name, _)| name == "idx_products_instock")
        .expect("partial index should exist")
        .1;
    let normalized_partial_sql = partial_idx_sql
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .to_lowercase();
    let has_expected_partial_predicate = normalized_partial_sql
        .contains("wherestock>0anddiscontinued=0")
        || normalized_partial_sql.contains("wherediscontinued=0andstock>0")
        || normalized_partial_sql.contains("where(stock>0)and(discontinued=0)")
        || normalized_partial_sql.contains("where(discontinued=0)and(stock>0)");
    assert!(
        has_expected_partial_predicate,
        "partial index WHERE predicate should be preserved"
    );

    let expr_idx_sql = &index_defs
        .iter()
        .find(|(name, _)| name == "idx_products_name_expr")
        .expect("expression index should exist")
        .1;
    let normalized_expr_sql = expr_idx_sql
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .to_lowercase();
    assert!(
        normalized_expr_sql.contains("lower(name)") && normalized_expr_sql.contains("abs(price)"),
        "expression index definition should be preserved"
    );

    let in_stock_products: Vec<(String,)> = dest_conn
        .exec_rows("SELECT name FROM products WHERE stock > 0 AND discontinued = 0 ORDER BY name");
    assert_eq!(
        in_stock_products,
        vec![
            ("Gadget C".to_string(),),
            ("Spare E".to_string(),),
            ("Widget A".to_string(),)
        ]
    );

    let expr_match: Vec<(i64,)> =
        dest_conn.exec_rows("SELECT id FROM products WHERE lower(name) = 'widget a'");
    assert_eq!(expr_match, vec![(1,)]);

    Ok(())
}

/// Test VACUUM INTO preserves MVCC journal mode from source database.
/// If source has mvcc enabled, destination should too.
#[turso_macros::test]
fn test_vacuum_into_preserves_mvcc_after_reopen(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("PRAGMA journal_mode = 'mvcc'")?;
    let source_mode: Vec<(String,)> = conn.exec_rows("PRAGMA journal_mode");
    assert_eq!(
        source_mode,
        vec![("mvcc".to_string(),)],
        "Source should have MVCC enabled"
    );

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'hello')")?;
    conn.execute("INSERT INTO t VALUES (2, 'world')")?;
    let hash_opts = turso_dbhash::DbHashOptions {
        table_filter: Some("t".to_string()),
        ..Default::default()
    };
    let source_hash =
        compute_dbhash_with_options_and_database_opts(&tmp_db, &hash_opts, tmp_db.db_opts);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_mvcc.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;
    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, tmp_db.db_opts);
    let dest_conn = dest_db.connect_limbo();
    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(
        source_hash.hash,
        compute_dbhash_with_options_and_database_opts(&dest_db, &hash_opts, tmp_db.db_opts).hash
    );
    let dest_mode: Vec<(String,)> = dest_conn.exec_rows("PRAGMA journal_mode");
    assert_eq!(
        dest_mode,
        vec![("mvcc".to_string(),)],
        "Destination should have MVCC enabled (inherited from source)"
    );

    // verify the .db-log file was created for destination
    let log_path = dest_dir.path().join("vacuumed_mvcc.db-log");
    assert!(
        log_path.exists(),
        "MVCC log file should exist at {log_path:?}"
    );

    // verify data was copied correctly
    let rows: Vec<(i64, String)> = dest_conn.exec_rows("SELECT id, data FROM t ORDER BY id");
    assert_eq!(
        rows,
        vec![(1, "hello".to_string()), (2, "world".to_string())]
    );

    drop(dest_conn);
    drop(dest_db);

    let reopened = TempDatabase::new_with_existent_with_opts(&dest_path, tmp_db.db_opts);
    let reopened_conn = reopened.connect_limbo();
    let reopened_mode: Vec<(String,)> = reopened_conn.exec_rows("PRAGMA journal_mode");
    assert_eq!(reopened_mode, vec![("mvcc".to_string(),)]);
    assert_eq!(run_integrity_check(&reopened_conn), "ok");
    let reopened_rows: Vec<(i64, String)> =
        reopened_conn.exec_rows("SELECT id, data FROM t ORDER BY id");
    assert_eq!(
        reopened_rows,
        vec![(1, "hello".to_string()), (2, "world".to_string())]
    );

    Ok(())
}

#[turso_macros::test]
fn test_plain_vacuum_preserves_mvcc_after_reopen(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("PRAGMA journal_mode = 'mvcc'")?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, data TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'hello'), (2, 'world')")?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;

    conn.execute("VACUUM")?;
    assert_eq!(run_integrity_check(&conn), "ok");
    let journal_mode: Vec<(String,)> = conn.exec_rows("PRAGMA journal_mode");
    assert_eq!(journal_mode, vec![("mvcc".to_string(),)]);

    let path = tmp_db.path.clone();
    let opts = tmp_db.db_opts;
    drop(conn);
    drop(tmp_db);

    let reopened = TempDatabase::new_with_existent_with_opts(&path, opts);
    let reopened_conn = reopened.connect_limbo();
    let journal_mode: Vec<(String,)> = reopened_conn.exec_rows("PRAGMA journal_mode");
    assert_eq!(journal_mode, vec![("mvcc".to_string(),)]);
    assert_eq!(run_integrity_check(&reopened_conn), "ok");
    assert_eq!(scalar_i64(&reopened_conn, "SELECT COUNT(*) FROM t"), 2);

    Ok(())
}

#[test]
fn test_vacuum_into_from_memory_database() -> anyhow::Result<()> {
    use std::sync::Arc;
    use turso_core::{Database, MemoryIO, OpenFlags};

    let _ = env_logger::try_init();

    // create an in-memory database
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:",
        OpenFlags::Create,
        turso_core::DatabaseOpts::new(),
        None,
    )?;
    let conn = db.connect()?;

    conn.execute("CREATE TABLE t (a INTEGER, b TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'hello')")?;
    conn.execute("INSERT INTO t VALUES (2, 'world')")?;
    conn.execute("INSERT INTO t VALUES (3, 'from memory')")?;

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_from_memory.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;
    assert!(
        dest_path.exists(),
        "Vacuumed file should exist on disk at {dest_path_str}"
    );

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();
    let integrity_result = run_integrity_check(&dest_conn);
    assert_eq!(
        integrity_result, "ok",
        "Destination database should pass integrity check"
    );

    // verify all data was copied correctly
    let rows: Vec<(i64, String)> = dest_conn.exec_rows("SELECT a, b FROM t ORDER BY a, b");
    assert_eq!(
        rows,
        vec![
            (1, "hello".to_string()),
            (2, "world".to_string()),
            (3, "from memory".to_string())
        ],
        "All data from in-memory database should be copied to disk"
    );

    Ok(())
}

// test for future stuff, as turso db does not support yet:
// 1. CHECK constraints
// 2. WITHOUT ROWID tables
// 3. table without any columns (which sqlite does kek)

/// Test VACUUM INTO with CHECK constraints.
#[turso_macros::test(mvcc)]
fn test_vacuum_into_with_check_constraints(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    create_employees_with_check_constraints(&conn)?;
    insert_valid_employee_rows(&conn)?;
    checkpoint_if_mvcc(&tmp_db, &conn)?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_check.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    if !tmp_db.enable_mvcc {
        assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
    }

    // Verify schema includes CHECK constraints
    let employees_sql: Vec<(String,)> =
        dest_conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'employees'");
    assert!(
        employees_sql[0].0.contains("CHECK"),
        "CHECK constraints should be preserved"
    );

    // Verify data was copied
    let rows: Vec<(i64, String, i64, f64, String)> =
        dest_conn.exec_rows("SELECT id, name, age, salary, status FROM employees ORDER BY id");
    assert_eq!(
        rows,
        vec![
            (1, "Alice".to_string(), 30, 50000.0, "active".to_string()),
            (2, "Bob".to_string(), 45, 75000.0, "inactive".to_string()),
            (3, "Charlie".to_string(), 18, 35000.0, "pending".to_string())
        ]
    );

    // Verify CHECK constraints are enforced in destination
    assert!(
        dest_conn
            .execute("INSERT INTO employees VALUES (4, 'Dan', 10, 40000.0, 'active')")
            .is_err(),
        "CHECK constraint on age should reject value < 18"
    );

    Ok(())
}

/// Plain VACUUM must preserve CHECK-constrained tables and keep those
/// constraints enforced after the rewrite.
#[turso_macros::test(mvcc)]
fn test_plain_vacuum_with_check_constraints(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    create_employees_with_check_constraints(&conn)?;
    insert_valid_employee_rows(&conn)?;
    checkpoint_if_mvcc(&tmp_db, &conn)?;
    let before_hash = compute_plain_vacuum_dbhash(&tmp_db);

    conn.execute("VACUUM")?;

    assert_plain_vacuum_integrity_and_hash(&tmp_db, &conn, &before_hash);

    let employees_sql: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'employees'");
    assert!(
        employees_sql[0].0.contains("CHECK"),
        "CHECK constraints should be preserved"
    );

    assert_eq!(
        employee_rows(&conn),
        vec![
            (1, "Alice".to_string(), 30, 50000.0, "active".to_string()),
            (2, "Bob".to_string(), 45, 75000.0, "inactive".to_string()),
            (3, "Charlie".to_string(), 18, 35000.0, "pending".to_string()),
        ]
    );

    assert!(
        conn.execute("INSERT INTO employees VALUES (4, 'Dan', 10, 40000.0, 'active')")
            .is_err(),
        "CHECK constraint on age should reject value < 18"
    );

    Ok(())
}

/// Rows inserted while CHECK constraints are ignored must survive a plain
/// VACUUM unchanged. The rewrite should preserve the data as-is without
/// re-validating historical rows, while still keeping constraints enabled for
/// future writes.
#[turso_macros::test(mvcc)]
fn test_plain_vacuum_with_ignored_check_constraint_rows(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    create_employees_with_check_constraints(&conn)?;
    insert_valid_employee_rows(&conn)?;
    conn.execute("PRAGMA ignore_check_constraints = ON")?;
    conn.execute("INSERT INTO employees VALUES (4, '', 16, -10.0, 'ghost')")?;
    conn.execute("INSERT INTO employees VALUES (5, 'Eve', 130, 12345.0, 'active')")?;
    conn.execute("PRAGMA ignore_check_constraints = OFF")?;
    checkpoint_if_mvcc(&tmp_db, &conn)?;

    let (_before_db, before_conn) = connect_existing_with_opts(&tmp_db.path, tmp_db.db_opts);
    let before_integrity = run_integrity_check(&before_conn);
    let before_hash = compute_plain_vacuum_dbhash(&tmp_db);

    conn.execute("VACUUM")?;

    let (_after_db, after_conn) = connect_existing_with_opts(&tmp_db.path, tmp_db.db_opts);
    assert_eq!(
        run_integrity_check(&after_conn),
        before_integrity,
        "plain VACUUM must preserve the existing integrity_check result"
    );
    assert_eq!(
        normalized_schema_snapshot(&after_conn),
        before_hash.normalized_schema,
        "plain VACUUM must preserve normalized sqlite_schema entries for rows inserted with ignored CHECK constraints"
    );
    assert_eq!(
        compute_plain_vacuum_dbhash(&tmp_db).hash,
        before_hash.hash,
        "plain VACUUM must preserve logical content even for rows inserted with ignored CHECK constraints"
    );
    assert_eq!(
        employee_rows(&after_conn),
        vec![
            (1, "Alice".to_string(), 30, 50000.0, "active".to_string()),
            (2, "Bob".to_string(), 45, 75000.0, "inactive".to_string()),
            (3, "Charlie".to_string(), 18, 35000.0, "pending".to_string()),
            (4, "".to_string(), 16, -10.0, "ghost".to_string()),
            (5, "Eve".to_string(), 130, 12345.0, "active".to_string()),
        ]
    );
    assert!(
        after_conn
            .execute("INSERT INTO employees VALUES (6, '', 16, -10.0, 'ghost')")
            .is_err(),
        "future violating inserts should still fail once CHECK constraints are re-enabled"
    );

    Ok(())
}

/// VACUUM INTO must also preserve rows that were inserted while CHECK
/// constraints were ignored, without re-validating them during the copy.
#[turso_macros::test(mvcc)]
fn test_vacuum_into_with_ignored_check_constraint_rows(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    create_employees_with_check_constraints(&conn)?;
    insert_valid_employee_rows(&conn)?;
    conn.execute("PRAGMA ignore_check_constraints = ON")?;
    conn.execute("INSERT INTO employees VALUES (4, '', 16, -10.0, 'ghost')")?;
    conn.execute("INSERT INTO employees VALUES (5, 'Eve', 130, 12345.0, 'active')")?;
    conn.execute("PRAGMA ignore_check_constraints = OFF")?;
    checkpoint_if_mvcc(&tmp_db, &conn)?;

    let (_source_db, source_conn) = connect_existing_with_opts(&tmp_db.path, tmp_db.db_opts);
    let source_integrity = run_integrity_check(&source_conn);
    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_ignored_check.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, tmp_db.db_opts);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(
        run_integrity_check(&dest_conn),
        source_integrity,
        "VACUUM INTO must preserve the existing integrity_check result"
    );
    if !tmp_db.enable_mvcc {
        assert_eq!(
            source_hash.hash,
            compute_dbhash(&dest_db).hash,
            "VACUUM INTO must preserve logical content even for rows inserted with ignored CHECK constraints"
        );
    }

    let employees_sql: Vec<(String,)> =
        dest_conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'employees'");
    assert!(
        employees_sql[0].0.contains("CHECK"),
        "CHECK constraints should be preserved in the destination schema"
    );
    assert_eq!(
        employee_rows(&dest_conn),
        vec![
            (1, "Alice".to_string(), 30, 50000.0, "active".to_string()),
            (2, "Bob".to_string(), 45, 75000.0, "inactive".to_string()),
            (3, "Charlie".to_string(), 18, 35000.0, "pending".to_string()),
            (4, "".to_string(), 16, -10.0, "ghost".to_string()),
            (5, "Eve".to_string(), 130, 12345.0, "active".to_string()),
        ]
    );
    assert!(
        dest_conn
            .execute("INSERT INTO employees VALUES (6, '', 16, -10.0, 'ghost')")
            .is_err(),
        "future violating inserts should still fail in the destination"
    );

    Ok(())
}

/// Test VACUUM INTO with WITHOUT ROWID tables
/// Skips if WITHOUT ROWID is not supported.
#[turso_macros::test]
fn test_vacuum_into_with_without_rowid(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // Skip if WITHOUT ROWID is not supported
    if conn
        .execute(
            "CREATE TABLE config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT
        ) WITHOUT ROWID",
        )
        .is_err()
    {
        return Ok(());
    }

    conn.execute("INSERT INTO config VALUES ('theme', 'dark', '2024-01-01')")?;
    conn.execute("INSERT INTO config VALUES ('language', 'en', '2024-01-02')")?;
    conn.execute("INSERT INTO config VALUES ('timezone', 'UTC', '2024-01-03')")?;

    // Also create a regular table
    conn.execute("CREATE TABLE regular (id INTEGER PRIMARY KEY, data TEXT)")?;
    conn.execute("INSERT INTO regular VALUES (1, 'test')")?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_without_rowid.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);

    // Verify schema includes WITHOUT ROWID
    let config_sql: Vec<(String,)> =
        dest_conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'config'");
    assert!(
        config_sql[0].0.contains("WITHOUT ROWID"),
        "WITHOUT ROWID should be preserved in schema"
    );

    // Verify data was copied
    let config: Vec<(String, String, String)> =
        dest_conn.exec_rows("SELECT key, value, updated_at FROM config ORDER BY key");
    assert_eq!(
        config,
        vec![
            (
                "language".to_string(),
                "en".to_string(),
                "2024-01-02".to_string()
            ),
            (
                "theme".to_string(),
                "dark".to_string(),
                "2024-01-01".to_string()
            ),
            (
                "timezone".to_string(),
                "UTC".to_string(),
                "2024-01-03".to_string()
            )
        ]
    );

    // Verify regular table also copied
    let regular: Vec<(i64, String)> = dest_conn.exec_rows("SELECT id, data FROM regular");
    assert_eq!(regular, vec![(1, "test".to_string())]);

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_vacuum_into_with_strict_table(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL,
            username TEXT NOT NULL,
            score INTEGER
        ) STRICT",
    )?;
    conn.execute("CREATE UNIQUE INDEX idx_users_email ON users (email)")?;
    conn.execute("CREATE INDEX idx_users_username ON users (username)")?;
    conn.execute("CREATE INDEX idx_users_score ON users (score)")?;

    conn.execute("INSERT INTO users VALUES (1, 'alice@example.com', 'alice', 100)")?;
    conn.execute("INSERT INTO users VALUES (2, 'bob@example.com', 'bob', 200)")?;
    conn.execute("INSERT INTO users VALUES (3, 'charlie@example.com', 'charlie', 150)")?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_strict.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    if !tmp_db.enable_mvcc {
        assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
    }

    let rows: Vec<(i64, String, String, i64)> =
        dest_conn.exec_rows("SELECT id, email, username, score FROM users ORDER BY id");
    assert_eq!(
        rows,
        vec![
            (1, "alice@example.com".to_string(), "alice".to_string(), 100),
            (2, "bob@example.com".to_string(), "bob".to_string(), 200),
            (
                3,
                "charlie@example.com".to_string(),
                "charlie".to_string(),
                150
            )
        ]
    );

    let indexes: Vec<(String,)> = dest_conn.exec_rows(
        "SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'users' ORDER BY name",
    );
    assert_eq!(indexes.len(), 3);
    assert_eq!(indexes[0].0, "idx_users_email");
    assert_eq!(indexes[1].0, "idx_users_score");
    assert_eq!(indexes[2].0, "idx_users_username");

    assert!(
        dest_conn
            .execute("INSERT INTO users VALUES (4, 'alice@example.com', 'alice2', 50)")
            .is_err(),
        "Unique index should reject duplicate email"
    );

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_vacuum_into_with_strict_without_rowid(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    if conn
        .execute(
            "CREATE TABLE settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at INTEGER
            ) STRICT, WITHOUT ROWID",
        )
        .is_err()
    {
        return Ok(());
    }

    conn.execute("INSERT INTO settings VALUES ('theme', 'dark', 1704067200)")?;
    conn.execute("INSERT INTO settings VALUES ('language', 'en', 1704153600)")?;
    conn.execute("INSERT INTO settings VALUES ('timezone', 'UTC', 1704240000)")?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_strict_without_rowid.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    if !tmp_db.enable_mvcc {
        assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
    }

    let settings_sql: Vec<(String,)> =
        dest_conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'settings'");
    assert!(
        settings_sql[0].0.contains("STRICT"),
        "STRICT should be preserved in schema"
    );
    assert!(
        settings_sql[0].0.contains("WITHOUT ROWID"),
        "WITHOUT ROWID should be preserved in schema"
    );

    let rows: Vec<(String, String, i64)> =
        dest_conn.exec_rows("SELECT key, value, updated_at FROM settings ORDER BY key");
    assert_eq!(
        rows,
        vec![
            ("language".to_string(), "en".to_string(), 1704153600),
            ("theme".to_string(), "dark".to_string(), 1704067200),
            ("timezone".to_string(), "UTC".to_string(), 1704240000)
        ]
    );

    assert!(
        dest_conn
            .execute("INSERT INTO settings VALUES ('test', 123, 'not_an_int')")
            .is_err(),
        "STRICT should reject wrong types"
    );

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_vacuum_into_with_multiple_strict_tables(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            total REAL NOT NULL
        ) STRICT",
    )?;

    conn.execute(
        "CREATE TABLE order_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_name TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            price REAL NOT NULL
        ) STRICT",
    )?;

    conn.execute("CREATE TABLE logs (id INTEGER PRIMARY KEY, message TEXT)")?;

    conn.execute("INSERT INTO orders VALUES (1, 100, 59.97)")?;
    conn.execute("INSERT INTO orders VALUES (2, 101, 29.99)")?;

    conn.execute("INSERT INTO order_items VALUES (1, 1, 'Widget', 2, 19.99)")?;
    conn.execute("INSERT INTO order_items VALUES (2, 1, 'Gadget', 1, 19.99)")?;
    conn.execute("INSERT INTO order_items VALUES (3, 2, 'Gizmo', 3, 9.99)")?;

    conn.execute("INSERT INTO logs VALUES (1, 'Order 1 created')")?;

    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed_multi_strict.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    if !tmp_db.enable_mvcc {
        assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
    }

    let orders_sql: Vec<(String,)> =
        dest_conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'orders'");
    assert!(orders_sql[0].0.contains("STRICT"));

    let items_sql: Vec<(String,)> =
        dest_conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'order_items'");
    assert!(items_sql[0].0.contains("STRICT"));

    let logs_sql: Vec<(String,)> =
        dest_conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'logs'");
    assert!(!logs_sql[0].0.contains("STRICT"));

    let orders: Vec<(i64, i64, f64)> =
        dest_conn.exec_rows("SELECT id, customer_id, total FROM orders ORDER BY id");
    assert_eq!(orders, vec![(1, 100, 59.97), (2, 101, 29.99)]);

    let items: Vec<(i64, i64, String, i64, f64)> = dest_conn.exec_rows(
        "SELECT id, order_id, product_name, quantity, price FROM order_items ORDER BY id",
    );
    assert_eq!(
        items,
        vec![
            (1, 1, "Widget".to_string(), 2, 19.99),
            (2, 1, "Gadget".to_string(), 1, 19.99),
            (3, 2, "Gizmo".to_string(), 3, 9.99)
        ]
    );

    let logs: Vec<(i64, String)> = dest_conn.exec_rows("SELECT id, message FROM logs");
    assert_eq!(logs, vec![(1, "Order 1 created".to_string())]);

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_vacuum_into_compacts_fragmented_database(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE fragmented_data (id INTEGER PRIMARY KEY, data BLOB)")?;

    for i in 0..100 {
        conn.execute(format!(
            "INSERT INTO fragmented_data VALUES ({i}, randomblob(4096))",
        ))?;
    }

    conn.execute("DELETE FROM fragmented_data WHERE id < 60")?;

    for i in 60..100 {
        conn.execute(format!(
            "UPDATE fragmented_data SET data = randomblob(4096) WHERE id = {i}",
        ))?;

        conn.execute(format!(
            "UPDATE fragmented_data SET data = randomblob(8192) WHERE id = {i}",
        ))?;
    }

    let count: Vec<(i64,)> = conn.exec_rows("SELECT COUNT(*) FROM fragmented_data");
    assert_eq!(count[0].0, 40, "Expected 40 rows after deletes");

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;

    let source_size = std::fs::metadata(&tmp_db.path)?.len();
    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuumed.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(
        run_integrity_check(&dest_conn),
        "ok",
        "Vacuumed database should pass integrity check"
    );

    if !tmp_db.enable_mvcc {
        assert_eq!(
            source_hash.hash,
            compute_dbhash(&dest_db).hash,
            "Source and vacuumed database should have same content hash"
        );
    }

    if !tmp_db.enable_mvcc {
        let source_pages: Vec<(i64,)> = conn.exec_rows("PRAGMA page_count");
        let dest_pages: Vec<(i64,)> = dest_conn.exec_rows("PRAGMA page_count");

        assert!(
            dest_pages[0].0 < source_pages[0].0,
            "Page count should reduce from {} to {}",
            source_pages[0].0,
            dest_pages[0].0
        );
    }

    let dest_count: Vec<(i64,)> = dest_conn.exec_rows("SELECT COUNT(*) FROM fragmented_data");
    assert_eq!(dest_count[0].0, 40, "Vacuumed db should have 40 rows");

    if !tmp_db.enable_mvcc {
        let dest_size = std::fs::metadata(&dest_path)?.len();
        assert!(
            dest_size < source_size,
            "VACUUM INTO should reduce file size. Source: {source_size} bytes, Destination: {dest_size} bytes"
        );
    }

    Ok(())
}

#[turso_macros::test(init_sql = "CREATE TABLE main_t(x INTEGER);")]
fn test_vacuum_into_attached_database(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO main_t VALUES (1), (2), (3)")?;

    let attached_path = tmp_db.path.with_file_name("attached.db");
    conn.execute(format!(
        "ATTACH DATABASE '{}' AS att",
        attached_path.display()
    ))?;
    conn.execute("CREATE TABLE att.att_t(y TEXT)")?;
    conn.execute("INSERT INTO att.att_t VALUES ('a'), ('b'), ('c')")?;

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("att_vacuumed.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM att INTO '{dest_path_str}'"))?;
    assert!(dest_path.exists(), "Destination file should exist");

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    let integrity = run_integrity_check(&dest_conn);
    assert_eq!(
        integrity, "ok",
        "Vacuumed attached database should pass integrity check"
    );

    let attached_db = TempDatabase::new_with_existent(&attached_path);
    let source_hash = compute_dbhash(&attached_db);
    let dest_hash = compute_dbhash(&dest_db);
    assert_eq!(
        source_hash.hash, dest_hash.hash,
        "Vacuumed database should have the same content hash as the attached source"
    );

    let rows: Vec<(String,)> = dest_conn.exec_rows("SELECT y FROM att_t ORDER BY y");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, "a");
    assert_eq!(rows[1].0, "b");
    assert_eq!(rows[2].0, "c");

    let result = dest_conn.execute("SELECT * FROM main_t");
    assert!(
        result.is_err(),
        "main_t should not exist in vacuumed attached database"
    );

    Ok(())
}

/// Column names containing commas must not confuse the
/// bind-parameter count in the generated INSERT statement.
#[turso_macros::test(mvcc)]
fn test_vacuum_into_column_name_with_comma(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t(\"a,b\" INTEGER, c TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'hello')")?;
    conn.execute("INSERT INTO t VALUES (2, 'world')")?;
    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("comma_col.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();
    if !tmp_db.enable_mvcc {
        assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
    }

    let rows: Vec<(i64, String)> = dest_conn.exec_rows("SELECT \"a,b\", c FROM t ORDER BY \"a,b\"");
    assert_eq!(
        rows,
        vec![(1, "hello".to_string()), (2, "world".to_string())]
    );

    Ok(())
}

// checksum feature changes reserved_space which breaks VACUUM INTO hash comparison
#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(init_sql = "CREATE TABLE main_t(x INTEGER);")]
fn test_vacuum_main_into(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO main_t VALUES (10), (20), (30)")?;

    let attached_path = tmp_db.path.with_file_name("attached.db");
    conn.execute(format!(
        "ATTACH DATABASE '{}' AS att",
        attached_path.display()
    ))?;
    conn.execute("CREATE TABLE att.att_t(z INTEGER)")?;
    conn.execute("INSERT INTO att.att_t VALUES (100)")?;

    // Hash content only (without_schema) because
    // vacuumed target (main) is created by init_sql
    // which is executed by rusqlite and produces different
    // schema than turso's VACUUM INTO
    let hash_opts = turso_dbhash::DbHashOptions {
        without_schema: true,
        ..Default::default()
    };
    let source_hash = compute_dbhash_with_options(&tmp_db, &hash_opts);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("main_vacuumed.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM main INTO '{dest_path_str}'"))?;
    assert!(dest_path.exists(), "Destination file should exist");

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    let integrity = run_integrity_check(&dest_conn);
    assert_eq!(
        integrity, "ok",
        "Vacuumed main database should pass integrity check"
    );

    let dest_hash = compute_dbhash_with_options(&dest_db, &hash_opts);
    assert_eq!(
        source_hash.hash, dest_hash.hash,
        "Vacuumed database should have the same content hash as main"
    );

    let rows: Vec<(i64,)> = dest_conn.exec_rows("SELECT x FROM main_t ORDER BY x");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, 10);
    assert_eq!(rows[1].0, 20);
    assert_eq!(rows[2].0, 30);

    let result = dest_conn.execute("SELECT * FROM att_t");
    assert!(
        result.is_err(),
        "att_t should not exist in vacuumed main database"
    );

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_vacuum_into_nonexistent_schema(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    let result = conn.execute("VACUUM nonexistent INTO 'out.db'");

    assert!(result.is_err(), "Should error on non-existent database");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("no such database: nonexistent"),
        "Error should indicate database not found: {err_msg}"
    );

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_vacuum_into_temp_is_noop(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("temp_vacuum.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM temp INTO '{dest_path_str}'"))?;
    assert!(
        !dest_path.exists(),
        "VACUUM temp INTO should not create a file"
    );

    Ok(())
}

/// VACUUM INTO must correctly handle deferred index creation.
/// Indexes are created after data copy for performance. Verify that the
/// destination still has working indexes with correct data.
#[turso_macros::test(mvcc)]
fn test_vacuum_into_deferred_indexes(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)")?;
    conn.execute("CREATE INDEX idx_a ON t (a)")?;
    conn.execute("CREATE UNIQUE INDEX idx_b ON t (b)")?;

    for i in 0..30 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, 'val_{i}', {i})"))?;
    }
    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("deferred_idx.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    if !tmp_db.enable_mvcc {
        assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
    }

    let indexes: Vec<(String,)> = dest_conn.exec_rows(
        "SELECT name FROM sqlite_schema WHERE type = 'index' AND sql IS NOT NULL ORDER BY name",
    );
    assert_eq!(indexes.len(), 2);
    assert_eq!(indexes[0].0, "idx_a");
    assert_eq!(indexes[1].0, "idx_b");

    let count: Vec<(i64,)> = dest_conn.exec_rows("SELECT COUNT(*) FROM t");
    assert_eq!(count[0].0, 30);

    let eqp_a: Vec<(i64, i64, i64, String)> =
        dest_conn.exec_rows("EXPLAIN QUERY PLAN SELECT id, a FROM t WHERE a = 'val_15'");
    assert!(
        eqp_a
            .iter()
            .any(|(_, _, _, detail)| detail.contains("INDEX") && detail.contains("idx_a")),
        "expected lookup by a to use idx_a, got plan: {eqp_a:?}",
    );
    let row: Vec<(i64, String)> = dest_conn.exec_rows("SELECT id, a FROM t WHERE a = 'val_15'");
    assert_eq!(row, vec![(15, "val_15".to_string())]);
    let row: Vec<(i64, String)> =
        dest_conn.exec_rows("SELECT id, a FROM t INDEXED BY idx_a WHERE a = 'val_15'");
    assert_eq!(row, vec![(15, "val_15".to_string())]);

    let eqp_b: Vec<(i64, i64, i64, String)> =
        dest_conn.exec_rows("EXPLAIN QUERY PLAN SELECT id, b FROM t WHERE b = 20");
    assert!(
        eqp_b
            .iter()
            .any(|(_, _, _, detail)| detail.contains("INDEX") && detail.contains("idx_b")),
        "expected lookup by b to use idx_b, got plan: {eqp_b:?}",
    );
    let row: Vec<(i64, i64)> = dest_conn.exec_rows("SELECT id, b FROM t WHERE b = 20");
    assert_eq!(row, vec![(20, 20)]);
    let row: Vec<(i64, i64)> =
        dest_conn.exec_rows("SELECT id, b FROM t INDEXED BY idx_b WHERE b = 20");
    assert_eq!(row, vec![(20, 20)]);

    Ok(())
}

/// VACUUM INTO must copy storage-backed internal tables such as sqlite_stat1.
#[turso_macros::test]
fn test_vacuum_into_preserves_sqlite_stat1(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT, value INTEGER)")?;
    conn.execute("CREATE INDEX idx_t_category_value ON t(category, value)")?;
    for i in 0..50 {
        let category = if i % 2 == 0 { "even" } else { "odd" };
        conn.execute(format!(
            "INSERT INTO t VALUES ({i}, '{category}', {})",
            i * 10
        ))?;
    }
    conn.execute("ANALYZE")?;

    let source_stats: Vec<(String, String, String)> =
        conn.exec_rows("SELECT tbl, COALESCE(idx, ''), stat FROM sqlite_stat1 ORDER BY tbl, idx");
    assert!(
        !source_stats.is_empty(),
        "ANALYZE should populate sqlite_stat1"
    );
    let hash_opts = turso_dbhash::DbHashOptions {
        table_filter: Some("t".to_string()),
        ..Default::default()
    };
    let source_hash =
        compute_dbhash_with_options_and_database_opts(&tmp_db, &hash_opts, tmp_db.db_opts);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("sqlite_stat1.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;
    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, tmp_db.db_opts);
    let dest_conn = dest_db.connect_limbo();
    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(
        source_hash.hash,
        compute_dbhash_with_options_and_database_opts(&dest_db, &hash_opts, tmp_db.db_opts).hash
    );
    let dest_stats: Vec<(String, String, String)> = dest_conn
        .exec_rows("SELECT tbl, COALESCE(idx, ''), stat FROM sqlite_stat1 ORDER BY tbl, idx");
    assert_eq!(dest_stats, source_stats);

    let count: Vec<(i64,)> = dest_conn.exec_rows("SELECT COUNT(*) FROM t WHERE category = 'even'");
    assert_eq!(count, vec![(25,)]);

    Ok(())
}

/// VACUUM INTO must preserve generated column values.
/// Generated columns are excluded from the data-copy column list (they're
/// computed, not stored), but the destination schema includes them and the
/// values should be recomputed correctly from the copied base columns.
#[test]
fn test_vacuum_into_preserves_generated_columns() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let opts = turso_core::DatabaseOpts::new().with_generated_columns(true);
    let tmp_db = TempDatabase::builder().with_opts(opts).build();
    let conn = tmp_db.connect_limbo();

    conn.execute(
        "CREATE TABLE t (
            a INTEGER,
            b INTEGER,
            c INTEGER GENERATED ALWAYS AS (a + b) VIRTUAL
        )",
    )?;
    conn.execute("INSERT INTO t (a, b) VALUES (10, 20)")?;
    conn.execute("INSERT INTO t (a, b) VALUES (100, 200)")?;

    // Verify generated column works on source
    let source_rows: Vec<(i64, i64, i64)> = conn.exec_rows("SELECT a, b, c FROM t ORDER BY a");
    assert_eq!(source_rows, vec![(10, 20, 30), (100, 200, 300)]);
    let source_hash = compute_dbhash_with_database_opts(&tmp_db, opts);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("gencol.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(
        source_hash.hash,
        compute_dbhash_with_database_opts(&dest_db, opts).hash
    );

    // Generated column values should be recomputed correctly on destination
    let dest_rows: Vec<(i64, i64, i64)> = dest_conn.exec_rows("SELECT a, b, c FROM t ORDER BY a");
    assert_eq!(dest_rows, vec![(10, 20, 30), (100, 200, 300)]);

    Ok(())
}

/// VACUUM INTO with generated columns that reference the rowid
/// alias, after deletes that create gaps. Verifies that rowid values are preserved
/// and the generated column recomputes correctly from the rowid on the destination.
#[test]
fn test_vacuum_into_generated_column_with_rowid_and_deletes() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let opts = turso_core::DatabaseOpts::new().with_generated_columns(true);
    let tmp_db = TempDatabase::builder().with_opts(opts).build();
    let conn = tmp_db.connect_limbo();

    // id is INTEGER PRIMARY KEY (rowid alias), label is generated from id
    conn.execute(
        "CREATE TABLE t (
            id INTEGER PRIMARY KEY,
            name TEXT,
            label TEXT GENERATED ALWAYS AS ('item_' || id) VIRTUAL
        )",
    )?;
    conn.execute("INSERT INTO t (id, name) VALUES (1, 'a')")?;
    conn.execute("INSERT INTO t (id, name) VALUES (2, 'b')")?;
    conn.execute("INSERT INTO t (id, name) VALUES (3, 'c')")?;
    conn.execute("INSERT INTO t (id, name) VALUES (4, 'd')")?;
    conn.execute("INSERT INTO t (id, name) VALUES (5, 'e')")?;

    // Delete some rows to create gaps in rowid space
    conn.execute("DELETE FROM t WHERE id IN (2, 4)")?;

    // Source should have rows 1, 3, 5 with correct generated labels
    let source_rows: Vec<(i64, String, String)> =
        conn.exec_rows("SELECT id, name, label FROM t ORDER BY id");
    assert_eq!(
        source_rows,
        vec![
            (1, "a".to_string(), "item_1".to_string()),
            (3, "c".to_string(), "item_3".to_string()),
            (5, "e".to_string(), "item_5".to_string()),
        ]
    );
    let source_hash = compute_dbhash_with_database_opts(&tmp_db, opts);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("gencol_rowid.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(
        source_hash.hash,
        compute_dbhash_with_database_opts(&dest_db, opts).hash
    );

    // Rowids must be preserved (not compacted), and generated labels must match
    let dest_rows: Vec<(i64, String, String)> =
        dest_conn.exec_rows("SELECT id, name, label FROM t ORDER BY id");
    assert_eq!(source_rows, dest_rows);

    Ok(())
}

/// VACUUM INTO must preserve custom type definitions so that
/// STRICT tables with custom type columns are correctly created on the destination
/// and encode/decode values properly.
#[test]
fn test_vacuum_into_preserves_custom_types() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
    let tmp_db = TempDatabase::builder().with_opts(opts).build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, amount cents) STRICT")?;
    conn.execute("INSERT INTO t VALUES (1, 42)")?;
    conn.execute("INSERT INTO t VALUES (2, 100)")?;

    // Verify decoded values on source
    let source_rows: Vec<(i64, i64)> = conn.exec_rows("SELECT id, amount FROM t ORDER BY id");
    assert_eq!(source_rows, vec![(1, 42), (2, 100)]);
    let hash_opts = turso_dbhash::DbHashOptions {
        table_filter: Some("t".to_string()),
        ..Default::default()
    };
    let source_hash = compute_dbhash_with_options_and_database_opts(&tmp_db, &hash_opts, opts);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("custom_types.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(
        source_hash.hash,
        compute_dbhash_with_options_and_database_opts(&dest_db, &hash_opts, opts).hash
    );

    // Destination must decode values correctly (42, 100 - not raw 4200, 10000)
    let dest_rows: Vec<(i64, i64)> = dest_conn.exec_rows("SELECT id, amount FROM t ORDER BY id");
    assert_eq!(dest_rows, source_rows);

    Ok(())
}

/// VACUUM INTO must preserve custom types that carry non-monotonic encoders,
/// custom comparators, and indexes. The copied destination must still order
/// through the preserved index and remain usable for new schema objects that
/// reference the copied type.
#[test]
fn test_vacuum_into_preserves_custom_type_ordering_and_schema_reuse() -> anyhow::Result<()> {
    let opts = turso_core::DatabaseOpts::new().with_custom_types(true);
    let tmp_db = TempDatabase::builder().with_opts(opts).build();
    let conn = tmp_db.connect_limbo();

    create_custom_type_ordering_fixture(&conn)?;
    assert_custom_type_ordering_fixture(&conn);
    let source_type_sqls = custom_type_sql_snapshot(&conn);
    let hash_opts = turso_dbhash::DbHashOptions {
        table_filter: Some("words".to_string()),
        ..Default::default()
    };
    let source_hash =
        compute_dbhash_with_options_and_database_opts(&tmp_db, &hash_opts, tmp_db.db_opts).hash;

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("custom_type_ordering.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
    let dest_conn = dest_db.connect_limbo();
    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(
        compute_dbhash_with_options_and_database_opts(&dest_db, &hash_opts, opts).hash,
        source_hash,
    );
    assert_eq!(custom_type_sql_snapshot(&dest_conn), source_type_sqls);
    assert_custom_type_ordering_fixture(&dest_conn);
    assert_custom_type_reusable_for_new_schema(&dest_conn)?;

    Ok(())
}

/// VACUUM INTO must preserve AUTOINCREMENT counters so that
/// new inserts on both source and destination get the same next rowid.
#[turso_macros::test]
fn test_vacuum_into_preserves_autoincrement_counter(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")?;
    conn.execute("INSERT INTO t (val) VALUES ('a')")?;
    conn.execute("INSERT INTO t (val) VALUES ('b')")?;
    conn.execute("INSERT INTO t (val) VALUES ('c')")?;
    // Delete some rows to create a gap - AUTOINCREMENT should never reuse rowids
    conn.execute("DELETE FROM t WHERE id = 2")?;
    let source_hash = compute_dbhash(&tmp_db);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("autoincr.db");
    let (dest_db, dest_conn) = run_vacuum_into_and_assert_round_trip(&tmp_db, &conn, &dest_path)?;

    // Insert a new row on the source - should get id=4 (not 2, because AUTOINCREMENT)
    conn.execute("INSERT INTO t (val) VALUES ('d')")?;
    let source_new: Vec<(i64, String)> = conn.exec_rows("SELECT id, val FROM t WHERE val = 'd'");
    assert_eq!(source_new, vec![(4, "d".to_string())]);

    // Insert a new row on the destination - should also get id=4
    assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);
    dest_conn.execute("INSERT INTO t (val) VALUES ('d')")?;
    let dest_new: Vec<(i64, String)> = dest_conn.exec_rows("SELECT id, val FROM t WHERE val = 'd'");
    assert_eq!(
        dest_new, source_new,
        "AUTOINCREMENT counter should produce the same next rowid on both databases"
    );

    Ok(())
}

/// VACUUM INTO preserves custom index-method indexes by replaying the
/// user-visible custom index after copying table data. The index method then
/// recreates and backfills its backing storage from destination rows.
#[turso_macros::test]
fn test_vacuum_into_preserves_custom_index_method(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE vectors (id INTEGER PRIMARY KEY, label TEXT, embedding BLOB)")?;
    conn.execute("CREATE INDEX vec_idx ON vectors USING toy_vector_sparse_ivf (embedding)")?;
    conn.execute("INSERT INTO vectors VALUES (1, 'cat', vector32_sparse('[1, 0, 0, 0]'))")?;
    conn.execute("INSERT INTO vectors VALUES (2, 'dog', vector32_sparse('[0, 1, 0, 0]'))")?;
    conn.execute("INSERT INTO vectors VALUES (3, 'fish', vector32_sparse('[0, 0, 1, 0]'))")?;
    let hash_opts = turso_dbhash::DbHashOptions {
        table_filter: Some("vectors".to_string()),
        ..Default::default()
    };
    let source_hash =
        compute_dbhash_with_options_and_database_opts(&tmp_db, &hash_opts, tmp_db.db_opts);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vectors.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;
    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, tmp_db.db_opts);
    let dest_conn = dest_db.connect_limbo();
    assert_eq!(
        source_hash.hash,
        compute_dbhash_with_options_and_database_opts(&dest_db, &hash_opts, tmp_db.db_opts).hash
    );

    let indexes: Vec<(String,)> = dest_conn.exec_rows(
        "SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'vectors' ORDER BY name",
    );
    assert_eq!(
        indexes,
        vec![
            ("vec_idx".to_string(),),
            ("vec_idx_inverted_index".to_string(),),
            ("vec_idx_stats".to_string(),),
        ],
    );

    let eqp: Vec<(i64, i64, i64, String)> = dest_conn.exec_rows(
        "EXPLAIN QUERY PLAN \
         SELECT id, label, vector_distance_jaccard(embedding, vector32_sparse('[1, 0, 0, 0]')) AS distance \
         FROM vectors ORDER BY distance LIMIT 1",
    );
    assert!(
        eqp.iter()
            .any(|(_, _, _, detail)| detail.contains("INDEX METHOD")),
        "nearest-neighbor query should use custom index method, got plan: {eqp:?}",
    );

    let nearest: Vec<(i64, String, f64)> = dest_conn.exec_rows(
        "SELECT id, label, vector_distance_jaccard(embedding, vector32_sparse('[1, 0, 0, 0]')) AS distance \
         FROM vectors ORDER BY distance LIMIT 1",
    );
    assert_eq!(nearest.len(), 1);
    assert_eq!(nearest[0].0, 1);
    assert_eq!(nearest[0].1, "cat");
    assert!(nearest[0].2.abs() < 1e-9);

    Ok(())
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_vacuum_into_preserves_fts_index(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")?;
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")?;
    conn.execute("INSERT INTO articles VALUES (1, 'Database Performance', 'Optimizing database queries is important for performance')")?;
    conn.execute("INSERT INTO articles VALUES (2, 'Web Development', 'Modern web applications use JavaScript and APIs')")?;
    conn.execute("INSERT INTO articles VALUES (3, 'Database Design', 'Good database design leads to better performance')")?;
    conn.execute("INSERT INTO articles VALUES (4, 'API Development', 'RESTful APIs are common in web services')")?;

    let source_database_matches: Vec<(i64,)> = conn
        .exec_rows("SELECT id FROM articles WHERE fts_match(title, body, 'database') ORDER BY id");
    assert_eq!(source_database_matches, vec![(1,), (3,)]);
    let hash_opts = turso_dbhash::DbHashOptions {
        table_filter: Some("articles".to_string()),
        ..Default::default()
    };
    let source_hash =
        compute_dbhash_with_options_and_database_opts(&tmp_db, &hash_opts, tmp_db.db_opts);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("fts_vacuumed.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;
    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, tmp_db.db_opts);
    let dest_conn = dest_db.connect_limbo();
    assert_eq!(
        source_hash.hash,
        compute_dbhash_with_options_and_database_opts(&dest_db, &hash_opts, tmp_db.db_opts).hash
    );

    let dest_database_matches: Vec<(i64,)> = dest_conn
        .exec_rows("SELECT id FROM articles WHERE fts_match(title, body, 'database') ORDER BY id");
    assert_eq!(dest_database_matches, source_database_matches);

    let dest_web_matches: Vec<(i64,)> = dest_conn
        .exec_rows("SELECT id FROM articles WHERE fts_match(title, body, 'web') ORDER BY id");
    assert_eq!(dest_web_matches, vec![(2,), (4,)]);

    let eqp: Vec<(i64, i64, i64, String)> = dest_conn.exec_rows(
        "EXPLAIN QUERY PLAN \
         SELECT id FROM articles WHERE fts_match(title, body, 'database')",
    );
    assert!(
        eqp.iter()
            .any(|(_, _, _, detail)| detail.contains("INDEX METHOD")),
        "FTS query should use an index method after VACUUM INTO, got plan: {eqp:?}",
    );

    dest_conn.execute(
        "INSERT INTO articles VALUES (5, 'Vacuum Export', 'Full text search stays usable after vacuum')",
    )?;
    let dest_vacuum_matches: Vec<(i64,)> = dest_conn
        .exec_rows("SELECT id FROM articles WHERE fts_match(title, body, 'vacuum') ORDER BY id");
    assert_eq!(dest_vacuum_matches, vec![(5,)]);

    Ok(())
}

/// Test VACUUM INTO preserves vector data stored as blobs (without a vector index).
/// Verifies that vector32() encoded blobs survive the vacuum round-trip
/// and produce the same distance calculations on the destination.
#[turso_macros::test]
fn test_vacuum_into_preserves_vector_blobs(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE vectors (id INTEGER PRIMARY KEY, label TEXT, embedding BLOB)")?;
    conn.execute("INSERT INTO vectors VALUES (1, 'cat', vector32('[1.0, 0.0, 0.0, 0.0]'))")?;
    conn.execute("INSERT INTO vectors VALUES (2, 'dog', vector32('[0.0, 1.0, 0.0, 0.0]'))")?;
    conn.execute("INSERT INTO vectors VALUES (3, 'fish', vector32('[0.0, 0.0, 1.0, 0.0]'))")?;
    let source_hash = compute_dbhash(&tmp_db);

    // Compute a distance on the source for comparison
    let source_dist: Vec<(f64,)> = conn.exec_rows(
        "SELECT vector_distance_cos(
            (SELECT embedding FROM vectors WHERE id = 1),
            (SELECT embedding FROM vectors WHERE id = 2)
        )",
    );

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vectors.db");
    let dest_path_str = dest_path.to_str().unwrap();

    conn.execute(format!("VACUUM INTO '{dest_path_str}'"))?;

    let dest_db = TempDatabase::new_with_existent(&dest_path);
    let dest_conn = dest_db.connect_limbo();

    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(source_hash.hash, compute_dbhash(&dest_db).hash);

    let count: Vec<(i64,)> = dest_conn.exec_rows("SELECT COUNT(*) FROM vectors");
    assert_eq!(count[0].0, 3);

    // Verify vector distance matches source - confirms blobs survived intact
    let dest_dist: Vec<(f64,)> = dest_conn.exec_rows(
        "SELECT vector_distance_cos(
            (SELECT embedding FROM vectors WHERE id = 1),
            (SELECT embedding FROM vectors WHERE id = 2)
        )",
    );
    assert_eq!(source_dist, dest_dist);

    Ok(())
}

// ---------------------------------------------------------------------------
// Plain VACUUM tests
// ---------------------------------------------------------------------------

/// Plain VACUUM must preserve generated column values.
#[test]
fn test_plain_vacuum_preserves_generated_columns() -> anyhow::Result<()> {
    let opts = DatabaseOpts::new().with_generated_columns(true);
    let tmp_db = TempDatabase::builder().with_opts(opts).build();
    let conn = tmp_db.connect_limbo();

    conn.execute(
        "CREATE TABLE t (
            a INTEGER,
            b INTEGER,
            c INTEGER GENERATED ALWAYS AS (a + b) VIRTUAL
        )",
    )?;
    conn.execute("INSERT INTO t (a, b) VALUES (10, 20), (100, 200), (7, 8)")?;
    conn.execute("DELETE FROM t WHERE a = 7")?;

    let before_rows: Vec<(i64, i64, i64)> = conn.exec_rows("SELECT a, b, c FROM t ORDER BY a");
    assert_eq!(before_rows, vec![(10, 20, 30), (100, 200, 300)]);

    assert_plain_vacuum_preserves_content_hash(&tmp_db, &conn)?;

    let after_rows: Vec<(i64, i64, i64)> = conn.exec_rows("SELECT a, b, c FROM t ORDER BY a");
    assert_eq!(after_rows, before_rows);
    assert_eq!(run_integrity_check(&conn), "ok");
    assert_plain_vacuum_folded_into_db_file(&tmp_db, &conn);

    Ok(())
}

/// Plain VACUUM must preserve custom type definitions and keep decoded values
/// usable on both the current and reopened connections.
#[test]
fn test_plain_vacuum_preserves_custom_types() -> anyhow::Result<()> {
    let opts = DatabaseOpts::new().with_custom_types(true);
    let tmp_db = TempDatabase::builder().with_opts(opts).build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TYPE cents BASE integer ENCODE value * 100 DECODE value / 100")?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, amount cents) STRICT")?;
    conn.execute("INSERT INTO t VALUES (1, 42), (2, 100)")?;

    let before_rows: Vec<(i64, i64)> = conn.exec_rows("SELECT id, amount FROM t ORDER BY id");
    assert_eq!(before_rows, vec![(1, 42), (2, 100)]);
    let before_hash = compute_plain_vacuum_dbhash(&tmp_db).hash;

    conn.execute("VACUUM")?;
    assert_plain_vacuum_folded_into_db_file(&tmp_db, &conn);
    assert_eq!(run_integrity_check(&conn), "ok");
    assert_eq!(compute_plain_vacuum_dbhash(&tmp_db).hash, before_hash);

    let after_rows: Vec<(i64, i64)> = conn.exec_rows("SELECT id, amount FROM t ORDER BY id");
    assert_eq!(after_rows, before_rows);
    assert_eq!(run_integrity_check(&conn), "ok");

    let reopened = TempDatabase::new_with_existent_with_opts(&tmp_db.path, opts);
    let reopened_conn = reopened.connect_limbo();
    assert_eq!(run_integrity_check(&reopened_conn), "ok");
    assert_eq!(compute_plain_vacuum_dbhash(&reopened).hash, before_hash);
    let reopened_rows: Vec<(i64, i64)> =
        reopened_conn.exec_rows("SELECT id, amount FROM t ORDER BY id");
    assert_eq!(reopened_rows, before_rows);

    Ok(())
}

/// Plain VACUUM reparses the live schema on the same connection. Custom types
/// that rely on a comparator-backed index must still order correctly after the
/// rewrite, and the same connection must remain able to use that type in new
/// DDL immediately afterward.
#[test]
fn test_plain_vacuum_preserves_custom_type_ordering_and_schema_reuse() -> anyhow::Result<()> {
    let opts = DatabaseOpts::new().with_custom_types(true);
    let tmp_db = TempDatabase::builder().with_opts(opts).build();
    let conn = tmp_db.connect_limbo();

    create_custom_type_ordering_fixture(&conn)?;
    assert_custom_type_ordering_fixture(&conn);
    let source_type_sqls = custom_type_sql_snapshot(&conn);
    let hash_opts = turso_dbhash::DbHashOptions {
        table_filter: Some("words".to_string()),
        ..Default::default()
    };
    let source_hash =
        compute_dbhash_with_options_and_database_opts(&tmp_db, &hash_opts, tmp_db.db_opts).hash;

    conn.execute("VACUUM")?;
    assert_plain_vacuum_folded_into_db_file(&tmp_db, &conn);
    assert_eq!(run_integrity_check(&conn), "ok");
    assert_eq!(
        compute_dbhash_with_options_and_database_opts(&tmp_db, &hash_opts, tmp_db.db_opts).hash,
        source_hash,
    );
    assert_eq!(custom_type_sql_snapshot(&conn), source_type_sqls);
    assert_custom_type_ordering_fixture(&conn);
    assert_custom_type_reusable_for_new_schema(&conn)?;

    Ok(())
}

/// Plain VACUUM reparses the main schema only; an initialized temp schema on
/// the same connection must remain usable across the reload.
#[turso_macros::test]
fn test_plain_vacuum_keeps_temp_schema_on_same_connection(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE main_t(id INTEGER PRIMARY KEY, payload TEXT)")?;
    for id in 0..80 {
        conn.execute(format!(
            "INSERT INTO main_t VALUES({id}, '{}')",
            "m".repeat(180)
        ))?;
    }
    conn.execute("DELETE FROM main_t WHERE id >= 20")?;

    conn.execute("CREATE TEMP TABLE temp_ids(id INTEGER PRIMARY KEY, note TEXT)")?;
    conn.execute("INSERT INTO temp.temp_ids VALUES (1, 'one'), (2, 'two')")?;

    assert_plain_vacuum_preserves_content_hash(&tmp_db, &conn)?;

    let temp_rows: Vec<(i64, String)> =
        conn.exec_rows("SELECT id, note FROM temp.temp_ids ORDER BY id");
    assert_eq!(
        temp_rows,
        vec![(1, "one".to_string()), (2, "two".to_string())]
    );
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM main_t"), 20);
    assert_eq!(run_integrity_check(&conn), "ok");
    assert_plain_vacuum_folded_into_db_file(&tmp_db, &conn);

    conn.execute("INSERT INTO temp.temp_ids VALUES (3, 'three')")?;
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM temp.temp_ids"), 3);

    Ok(())
}

/// Basic plain VACUUM: data survives the compaction round-trip.
#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(init_sql = "CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT, c REAL);")]
fn test_plain_vacuum_basic(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t1 VALUES(1, 'hello', 3.125)")?;
    conn.execute("INSERT INTO t1 VALUES(2, 'world', 2.725)")?;
    conn.execute("INSERT INTO t1 VALUES(3, 'test', 1.625)")?;
    conn.execute("DELETE FROM t1 WHERE a = 2")?;
    conn.execute("VACUUM")?;
    assert_eq!(run_integrity_check(&conn), "ok");

    let rows: Vec<(i64, String, f64)> = conn.exec_rows("SELECT a, b, c FROM t1 ORDER BY a");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "hello".into(), 3.125));
    assert_eq!(rows[1], (3, "test".into(), 1.625));

    Ok(())
}

/// Plain VACUUM preserves user_version and application_id metadata.
#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(init_sql = "CREATE TABLE t(a INTEGER);")]
fn test_plain_vacuum_preserves_metadata(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES(1)")?;
    conn.execute("PRAGMA user_version = 42")?;
    conn.execute("PRAGMA application_id = 99")?;

    let pre_schema_version: Vec<(i64,)> = conn.exec_rows("PRAGMA schema_version");

    conn.execute("VACUUM")?;
    assert_eq!(run_integrity_check(&conn), "ok");

    let user_version: Vec<(i64,)> = conn.exec_rows("PRAGMA user_version");
    assert_eq!(user_version[0].0, 42);

    let application_id: Vec<(i64,)> = conn.exec_rows("PRAGMA application_id");
    assert_eq!(application_id[0].0, 99);

    // Schema version should be bumped by 1.
    let post_schema_version: Vec<(i64,)> = conn.exec_rows("PRAGMA schema_version");
    assert_eq!(post_schema_version[0].0, pre_schema_version[0].0 + 1);

    let journal_mode: Vec<(String,)> = conn.exec_rows("PRAGMA journal_mode");
    assert_eq!(journal_mode[0].0, "wal");

    Ok(())
}

/// Plain VACUUM preserves AUTOINCREMENT counters.
#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(init_sql = "CREATE TABLE t1(a INTEGER PRIMARY KEY AUTOINCREMENT, b TEXT);")]
fn test_plain_vacuum_preserves_autoincrement(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t1(b) VALUES('one')")?;
    conn.execute("INSERT INTO t1(b) VALUES('two')")?;
    conn.execute("INSERT INTO t1(b) VALUES('three')")?;
    conn.execute("DELETE FROM t1 WHERE b = 'two'")?;
    conn.execute("VACUUM")?;
    assert_eq!(run_integrity_check(&conn), "ok");

    // Verify sqlite_sequence preserved
    let seq: Vec<(String, i64)> = conn.exec_rows("SELECT name, seq FROM sqlite_sequence");
    assert_eq!(seq.len(), 1);
    assert_eq!(seq[0].0, "t1");
    assert_eq!(seq[0].1, 3);

    // Next insert should continue from 4, not restart
    conn.execute("INSERT INTO t1(b) VALUES('four')")?;
    let rows: Vec<(i64, String)> = conn.exec_rows("SELECT a, b FROM t1 ORDER BY a");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (1, "one".into()));
    assert_eq!(rows[1], (3, "three".into()));
    assert_eq!(rows[2], (4, "four".into()));

    Ok(())
}

/// Plain VACUUM must preserve hidden rowid values for ordinary rowid tables.
#[turso_macros::test(mvcc)]
fn test_plain_vacuum_preserves_rowid_for_rowid_tables(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (a TEXT)")?;
    conn.execute("INSERT INTO t(rowid, a) VALUES(5, 'x')")?;
    conn.execute("INSERT INTO t(rowid, a) VALUES(42, 'y')")?;

    run_plain_vacuum_and_assert_round_trip(&tmp_db, &conn)?;

    let rows: Vec<(i64, String)> = conn.exec_rows("SELECT rowid, a FROM t ORDER BY rowid");
    assert_eq!(rows, vec![(5, "x".to_string()), (42, "y".to_string())]);

    Ok(())
}

/// Plain VACUUM must preserve hidden rowid values when the visible `rowid`
/// column name is shadowed by a real column.
#[turso_macros::test(mvcc)]
fn test_plain_vacuum_preserves_rowid_when_rowid_alias_is_shadowed(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (rowid TEXT, a TEXT)")?;
    conn.execute("INSERT INTO t(_rowid_, rowid, a) VALUES(77, 'visible', 'x')")?;

    run_plain_vacuum_and_assert_round_trip(&tmp_db, &conn)?;

    let rows: Vec<(i64, String, String)> =
        conn.exec_rows("SELECT _rowid_, rowid, a FROM t ORDER BY _rowid_");
    assert_eq!(rows, vec![(77, "visible".to_string(), "x".to_string())]);

    Ok(())
}

/// Plain VACUUM must succeed when all three hidden rowid aliases are shadowed
/// by user columns.
#[turso_macros::test(mvcc)]
fn test_plain_vacuum_when_all_rowid_aliases_are_shadowed(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (rowid TEXT, _rowid_ TEXT, oid TEXT, a TEXT)")?;
    conn.execute("INSERT INTO t(rowid, _rowid_, oid, a) VALUES('r1', 'u1', 'o1', 'x')")?;
    conn.execute("INSERT INTO t(rowid, _rowid_, oid, a) VALUES('r2', 'u2', 'o2', 'y')")?;

    run_plain_vacuum_and_assert_round_trip(&tmp_db, &conn)?;

    let rows: Vec<(String, String, String, String)> =
        conn.exec_rows("SELECT rowid, _rowid_, oid, a FROM t ORDER BY a");
    assert_eq!(
        rows,
        vec![
            (
                "r1".to_string(),
                "u1".to_string(),
                "o1".to_string(),
                "x".to_string()
            ),
            (
                "r2".to_string(),
                "u2".to_string(),
                "o2".to_string(),
                "y".to_string()
            ),
        ]
    );

    Ok(())
}

/// Plain VACUUM must preserve WITHOUT ROWID tables and their data.
#[turso_macros::test]
fn test_plain_vacuum_with_without_rowid(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    if conn
        .execute(
            "CREATE TABLE config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT
            ) WITHOUT ROWID",
        )
        .is_err()
    {
        return Ok(());
    }

    conn.execute("INSERT INTO config VALUES ('theme', 'dark', '2024-01-01')")?;
    conn.execute("INSERT INTO config VALUES ('language', 'en', '2024-01-02')")?;
    conn.execute("INSERT INTO config VALUES ('timezone', 'UTC', '2024-01-03')")?;

    conn.execute("CREATE TABLE regular (id INTEGER PRIMARY KEY, data TEXT)")?;
    conn.execute("INSERT INTO regular VALUES (1, 'test')")?;

    run_plain_vacuum_and_assert_round_trip(&tmp_db, &conn)?;

    let config_sql: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'config'");
    assert!(
        config_sql[0].0.contains("WITHOUT ROWID"),
        "WITHOUT ROWID should be preserved in schema"
    );

    let config: Vec<(String, String, String)> =
        conn.exec_rows("SELECT key, value, updated_at FROM config ORDER BY key");
    assert_eq!(
        config,
        vec![
            (
                "language".to_string(),
                "en".to_string(),
                "2024-01-02".to_string()
            ),
            (
                "theme".to_string(),
                "dark".to_string(),
                "2024-01-01".to_string()
            ),
            (
                "timezone".to_string(),
                "UTC".to_string(),
                "2024-01-03".to_string()
            ),
        ]
    );

    let regular: Vec<(i64, String)> = conn.exec_rows("SELECT id, data FROM regular");
    assert_eq!(regular, vec![(1, "test".to_string())]);

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_plain_vacuum_with_strict_table(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL,
            username TEXT NOT NULL,
            score INTEGER
        ) STRICT",
    )?;
    conn.execute("CREATE UNIQUE INDEX idx_users_email ON users (email)")?;
    conn.execute("CREATE INDEX idx_users_username ON users (username)")?;
    conn.execute("CREATE INDEX idx_users_score ON users (score)")?;

    conn.execute("INSERT INTO users VALUES (1, 'alice@example.com', 'alice', 100)")?;
    conn.execute("INSERT INTO users VALUES (2, 'bob@example.com', 'bob', 200)")?;
    conn.execute("INSERT INTO users VALUES (3, 'charlie@example.com', 'charlie', 150)")?;

    run_plain_vacuum_and_assert_round_trip(&tmp_db, &conn)?;

    let rows: Vec<(i64, String, String, i64)> =
        conn.exec_rows("SELECT id, email, username, score FROM users ORDER BY id");
    assert_eq!(
        rows,
        vec![
            (1, "alice@example.com".to_string(), "alice".to_string(), 100),
            (2, "bob@example.com".to_string(), "bob".to_string(), 200),
            (
                3,
                "charlie@example.com".to_string(),
                "charlie".to_string(),
                150
            ),
        ]
    );

    let indexes: Vec<(String,)> = conn.exec_rows(
        "SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'users' ORDER BY name",
    );
    assert_eq!(indexes.len(), 3);
    assert_eq!(indexes[0].0, "idx_users_email");
    assert_eq!(indexes[1].0, "idx_users_score");
    assert_eq!(indexes[2].0, "idx_users_username");

    assert!(
        conn.execute("INSERT INTO users VALUES (4, 'alice@example.com', 'alice2', 50)")
            .is_err(),
        "Unique index should reject duplicate email"
    );

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_plain_vacuum_with_strict_without_rowid(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    if conn
        .execute(
            "CREATE TABLE settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at INTEGER
            ) STRICT, WITHOUT ROWID",
        )
        .is_err()
    {
        return Ok(());
    }

    conn.execute("INSERT INTO settings VALUES ('theme', 'dark', 1704067200)")?;
    conn.execute("INSERT INTO settings VALUES ('language', 'en', 1704153600)")?;
    conn.execute("INSERT INTO settings VALUES ('timezone', 'UTC', 1704240000)")?;

    run_plain_vacuum_and_assert_round_trip(&tmp_db, &conn)?;

    let settings_sql: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'settings'");
    assert!(
        settings_sql[0].0.contains("STRICT"),
        "STRICT should be preserved in schema"
    );
    assert!(
        settings_sql[0].0.contains("WITHOUT ROWID"),
        "WITHOUT ROWID should be preserved in schema"
    );

    let rows: Vec<(String, String, i64)> =
        conn.exec_rows("SELECT key, value, updated_at FROM settings ORDER BY key");
    assert_eq!(
        rows,
        vec![
            ("language".to_string(), "en".to_string(), 1704153600),
            ("theme".to_string(), "dark".to_string(), 1704067200),
            ("timezone".to_string(), "UTC".to_string(), 1704240000),
        ]
    );

    assert!(
        conn.execute("INSERT INTO settings VALUES ('test', 123, 'not_an_int')")
            .is_err(),
        "STRICT should reject wrong types"
    );

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_plain_vacuum_with_multiple_strict_tables(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            total REAL NOT NULL
        ) STRICT",
    )?;

    conn.execute(
        "CREATE TABLE order_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_name TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            price REAL NOT NULL
        ) STRICT",
    )?;

    conn.execute("CREATE TABLE logs (id INTEGER PRIMARY KEY, message TEXT)")?;

    conn.execute("INSERT INTO orders VALUES (1, 100, 59.97)")?;
    conn.execute("INSERT INTO orders VALUES (2, 101, 29.99)")?;

    conn.execute("INSERT INTO order_items VALUES (1, 1, 'Widget', 2, 19.99)")?;
    conn.execute("INSERT INTO order_items VALUES (2, 1, 'Gadget', 1, 19.99)")?;
    conn.execute("INSERT INTO order_items VALUES (3, 2, 'Gizmo', 3, 9.99)")?;

    conn.execute("INSERT INTO logs VALUES (1, 'Order 1 created')")?;

    run_plain_vacuum_and_assert_round_trip(&tmp_db, &conn)?;

    let orders_sql: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'orders'");
    assert!(orders_sql[0].0.contains("STRICT"));

    let order_items_sql: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE name = 'order_items'");
    assert!(order_items_sql[0].0.contains("STRICT"));

    let order_rows: Vec<(i64, i64, f64)> =
        conn.exec_rows("SELECT id, customer_id, total FROM orders ORDER BY id");
    assert_eq!(order_rows, vec![(1, 100, 59.97), (2, 101, 29.99)]);

    let item_rows: Vec<(i64, i64, String, i64, f64)> = conn.exec_rows(
        "SELECT id, order_id, product_name, quantity, price FROM order_items ORDER BY id",
    );
    assert_eq!(
        item_rows,
        vec![
            (1, 1, "Widget".to_string(), 2, 19.99),
            (2, 1, "Gadget".to_string(), 1, 19.99),
            (3, 2, "Gizmo".to_string(), 3, 9.99),
        ]
    );

    let log_rows: Vec<(i64, String)> = conn.exec_rows("SELECT id, message FROM logs");
    assert_eq!(log_rows, vec![(1, "Order 1 created".to_string())]);

    assert!(
        conn.execute("INSERT INTO orders VALUES ('oops', 999, 1.0)")
            .is_err(),
        "STRICT tables should continue rejecting wrong types after plain VACUUM"
    );

    Ok(())
}

/// Plain VACUUM preserves indexes (queries using them still work).
#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(init_sql = "CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT);")]
fn test_plain_vacuum_preserves_indexes(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE INDEX idx_t1_b ON t1(b)")?;
    conn.execute("INSERT INTO t1 VALUES(1, 'alpha')")?;
    conn.execute("INSERT INTO t1 VALUES(2, 'beta')")?;
    conn.execute("INSERT INTO t1 VALUES(3, 'gamma')")?;
    conn.execute("DELETE FROM t1 WHERE a = 2")?;
    run_plain_vacuum_and_assert_round_trip(&tmp_db, &conn)?;

    let indexes: Vec<(String,)> = conn.exec_rows(
        "SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = 't1' ORDER BY name",
    );
    assert_eq!(indexes, vec![("idx_t1_b".to_string(),)]);

    let eqp: Vec<(i64, i64, i64, String)> = conn
        .exec_rows("EXPLAIN QUERY PLAN SELECT a, b FROM t1 INDEXED BY idx_t1_b WHERE b = 'gamma'");
    assert!(
        eqp.iter()
            .any(|(_, _, _, detail)| detail.contains("INDEX") && detail.contains("idx_t1_b")),
        "expected lookup to use idx_t1_b after plain VACUUM, got plan: {eqp:?}",
    );

    let rows: Vec<(i64, String)> =
        conn.exec_rows("SELECT a, b FROM t1 INDEXED BY idx_t1_b WHERE b = 'gamma'");
    assert_eq!(rows, vec![(3, "gamma".to_string())]);

    Ok(())
}

/// Plain VACUUM must preserve `sqlite_stat1` rows produced by ANALYZE.
#[turso_macros::test(mvcc)]
fn test_plain_vacuum_preserves_sqlite_stat1(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT, value INTEGER)")?;
    conn.execute("CREATE INDEX idx_t_category_value ON t(category, value)")?;
    for i in 0..50 {
        let category = if i % 2 == 0 { "even" } else { "odd" };
        conn.execute(format!(
            "INSERT INTO t VALUES ({i}, '{category}', {})",
            i * 10
        ))?;
    }
    conn.execute("ANALYZE")?;

    let source_stats: Vec<(String, String, String)> =
        conn.exec_rows("SELECT tbl, COALESCE(idx, ''), stat FROM sqlite_stat1 ORDER BY tbl, idx");
    assert!(
        !source_stats.is_empty(),
        "ANALYZE should populate sqlite_stat1"
    );

    run_plain_vacuum_and_assert_round_trip(&tmp_db, &conn)?;

    let dest_stats: Vec<(String, String, String)> =
        conn.exec_rows("SELECT tbl, COALESCE(idx, ''), stat FROM sqlite_stat1 ORDER BY tbl, idx");
    assert_eq!(dest_stats, source_stats);

    let count: Vec<(i64,)> = conn.exec_rows("SELECT COUNT(*) FROM t WHERE category = 'even'");
    assert_eq!(count, vec![(25,)]);

    Ok(())
}

/// Plain VACUUM must preserve custom index-method indexes on the source
/// connection after schema reload.
#[turso_macros::test]
fn test_plain_vacuum_preserves_custom_index_method(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE vectors (id INTEGER PRIMARY KEY, label TEXT, embedding BLOB)")?;
    conn.execute("CREATE INDEX vec_idx ON vectors USING toy_vector_sparse_ivf (embedding)")?;
    conn.execute("INSERT INTO vectors VALUES (1, 'cat', vector32_sparse('[1, 0, 0, 0]'))")?;
    conn.execute("INSERT INTO vectors VALUES (2, 'dog', vector32_sparse('[0, 1, 0, 0]'))")?;
    conn.execute("INSERT INTO vectors VALUES (3, 'fish', vector32_sparse('[0, 0, 1, 0]'))")?;
    let before_nearest: Vec<(i64, String, f64)> = conn.exec_rows(
        "SELECT id, label, vector_distance_jaccard(embedding, vector32_sparse('[1, 0, 0, 0]')) AS distance \
         FROM vectors ORDER BY distance LIMIT 1",
    );

    assert_plain_vacuum_preserves_content_hash(&tmp_db, &conn)?;

    let indexes: Vec<(String,)> = conn.exec_rows(
        "SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'vectors' ORDER BY name",
    );
    assert_eq!(
        indexes,
        vec![
            ("vec_idx".to_string(),),
            ("vec_idx_inverted_index".to_string(),),
            ("vec_idx_stats".to_string(),),
        ],
    );

    let eqp: Vec<(i64, i64, i64, String)> = conn.exec_rows(
        "EXPLAIN QUERY PLAN \
         SELECT id, label, vector_distance_jaccard(embedding, vector32_sparse('[1, 0, 0, 0]')) AS distance \
         FROM vectors ORDER BY distance LIMIT 1",
    );
    assert!(
        eqp.iter()
            .any(|(_, _, _, detail)| detail.contains("INDEX METHOD")),
        "nearest-neighbor query should use custom index method after plain VACUUM, got plan: {eqp:?}",
    );

    let nearest: Vec<(i64, String, f64)> = conn.exec_rows(
        "SELECT id, label, vector_distance_jaccard(embedding, vector32_sparse('[1, 0, 0, 0]')) AS distance \
         FROM vectors ORDER BY distance LIMIT 1",
    );
    assert_eq!(nearest, before_nearest);
    assert_plain_vacuum_folded_into_db_file(&tmp_db, &conn);

    Ok(())
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_plain_vacuum_preserves_fts_index(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")?;
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")?;
    conn.execute("INSERT INTO articles VALUES (1, 'Database Performance', 'Optimizing database queries is important for performance')")?;
    conn.execute("INSERT INTO articles VALUES (2, 'Web Development', 'Modern web applications use JavaScript and APIs')")?;
    conn.execute("INSERT INTO articles VALUES (3, 'Database Design', 'Good database design leads to better performance')")?;
    conn.execute("INSERT INTO articles VALUES (4, 'API Development', 'RESTful APIs are common in web services')")?;

    let before_database_matches: Vec<(i64,)> = conn
        .exec_rows("SELECT id FROM articles WHERE fts_match(title, body, 'database') ORDER BY id");

    assert_plain_vacuum_preserves_content_hash(&tmp_db, &conn)?;

    let after_database_matches: Vec<(i64,)> = conn
        .exec_rows("SELECT id FROM articles WHERE fts_match(title, body, 'database') ORDER BY id");
    assert_eq!(after_database_matches, before_database_matches);

    let web_matches: Vec<(i64,)> =
        conn.exec_rows("SELECT id FROM articles WHERE fts_match(title, body, 'web') ORDER BY id");
    assert_eq!(web_matches, vec![(2,), (4,)]);

    let eqp: Vec<(i64, i64, i64, String)> = conn.exec_rows(
        "EXPLAIN QUERY PLAN \
         SELECT id FROM articles WHERE fts_match(title, body, 'database')",
    );
    assert!(
        eqp.iter()
            .any(|(_, _, _, detail)| detail.contains("INDEX METHOD")),
        "FTS query should use an index method after plain VACUUM, got plan: {eqp:?}",
    );

    conn.execute(
        "INSERT INTO articles VALUES (5, 'Vacuum Export', 'Full text search stays usable after vacuum')",
    )?;
    let vacuum_matches: Vec<(i64,)> = conn
        .exec_rows("SELECT id FROM articles WHERE fts_match(title, body, 'vacuum') ORDER BY id");
    assert_eq!(vacuum_matches, vec![(5,)]);

    Ok(())
}

/// Plain VACUUM must preserve vector blobs stored as ordinary BLOB data.
#[turso_macros::test]
fn test_plain_vacuum_preserves_vector_blobs(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE vectors (id INTEGER PRIMARY KEY, label TEXT, embedding BLOB)")?;
    conn.execute("INSERT INTO vectors VALUES (1, 'cat', vector32('[1.0, 0.0, 0.0, 0.0]'))")?;
    conn.execute("INSERT INTO vectors VALUES (2, 'dog', vector32('[0.0, 1.0, 0.0, 0.0]'))")?;
    conn.execute("INSERT INTO vectors VALUES (3, 'fish', vector32('[0.0, 0.0, 1.0, 0.0]'))")?;

    let before_dist: Vec<(f64,)> = conn.exec_rows(
        "SELECT vector_distance_cos(
            (SELECT embedding FROM vectors WHERE id = 1),
            (SELECT embedding FROM vectors WHERE id = 2)
        )",
    );

    assert_plain_vacuum_preserves_content_hash(&tmp_db, &conn)?;

    let count: Vec<(i64,)> = conn.exec_rows("SELECT COUNT(*) FROM vectors");
    assert_eq!(count[0].0, 3);

    let after_dist: Vec<(f64,)> = conn.exec_rows(
        "SELECT vector_distance_cos(
            (SELECT embedding FROM vectors WHERE id = 1),
            (SELECT embedding FROM vectors WHERE id = 2)
        )",
    );
    assert_eq!(after_dist, before_dist);

    Ok(())
}

/// Plain VACUUM preserves views.
#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(views, init_sql = "CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT);")]
fn test_plain_vacuum_preserves_views(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE VIEW v1 AS SELECT * FROM t1 WHERE a > 1")?;
    conn.execute("INSERT INTO t1 VALUES(1, 'one')")?;
    conn.execute("INSERT INTO t1 VALUES(2, 'two')")?;
    conn.execute("INSERT INTO t1 VALUES(3, 'three')")?;
    conn.execute("DELETE FROM t1 WHERE a = 2")?;
    run_plain_vacuum_and_assert_round_trip(&tmp_db, &conn)?;

    let views: Vec<(String,)> =
        conn.exec_rows("SELECT name FROM sqlite_schema WHERE type = 'view' ORDER BY name");
    assert_eq!(views, vec![("v1".to_string(),)]);

    let rows: Vec<(i64, String)> = conn.exec_rows("SELECT a, b FROM v1 ORDER BY a");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], (3, "three".into()));

    Ok(())
}

/// Plain VACUUM rejects active transactions.
#[turso_macros::test(init_sql = "CREATE TABLE t(a INTEGER);")]
fn test_plain_vacuum_rejects_active_transaction(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("BEGIN")?;
    let err = conn.execute("VACUUM").unwrap_err();
    assert_eq!(
        err.to_string(),
        "Transaction error: cannot VACUUM from within a transaction",
        "unexpected error: {err}"
    );
    conn.execute("ROLLBACK")?;
    Ok(())
}

/// Plain VACUUM works on empty databases.
#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(init_sql = "CREATE TABLE t(a INTEGER);")]
fn test_plain_vacuum_empty_table(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("PRAGMA user_version = 42")?;
    conn.execute("PRAGMA application_id = 99")?;
    let pre_schema_version: Vec<(i64,)> = conn.exec_rows("PRAGMA schema_version");

    run_plain_vacuum_and_assert_round_trip(&tmp_db, &conn)?;

    let tables: Vec<(String, String)> =
        conn.exec_rows("SELECT name, sql FROM sqlite_schema WHERE type = 'table' AND name = 't'");
    assert_eq!(
        tables
            .into_iter()
            .map(|(name, sql)| (name, canonicalize_schema_sql(&sql)))
            .collect::<Vec<_>>(),
        vec![("t".to_string(), "CREATE TABLE t (a INTEGER)".to_string())]
    );

    let user_version: Vec<(i64,)> = conn.exec_rows("PRAGMA user_version");
    assert_eq!(user_version[0].0, 42);

    let application_id: Vec<(i64,)> = conn.exec_rows("PRAGMA application_id");
    assert_eq!(application_id[0].0, 99);

    let post_schema_version: Vec<(i64,)> = conn.exec_rows("PRAGMA schema_version");
    assert_eq!(post_schema_version[0].0, pre_schema_version[0].0 + 1);

    let rows: Vec<(i64,)> = conn.exec_rows("SELECT COUNT(*) FROM t");
    assert_eq!(rows[0].0, 0);
    Ok(())
}

/// Plain VACUUM preserves an empty table after heavy insert/update/delete churn.
#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(init_sql = "CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT);")]
fn test_plain_vacuum_empty_table_after_updates(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    let initial_payload = "a".repeat(300);
    let updated_even_payload = "b".repeat(700);
    let updated_odd_payload = "c".repeat(900);

    for i in 0..240 {
        conn.execute(format!("INSERT INTO t VALUES({i}, '{}')", initial_payload))?;
    }

    conn.execute(format!(
        "UPDATE t SET payload = '{}' WHERE id % 2 = 0",
        updated_even_payload
    ))?;
    conn.execute(format!(
        "UPDATE t SET payload = '{}' WHERE id % 2 = 1",
        updated_odd_payload
    ))?;
    conn.execute("DELETE FROM t")?;

    let pre_vacuum_pages = scalar_i64(&conn, "PRAGMA page_count");
    assert!(
        pre_vacuum_pages > 10,
        "workload should span many pages before VACUUM, got page_count={pre_vacuum_pages}"
    );

    run_plain_vacuum_and_assert_round_trip(&tmp_db, &conn)?;

    let tables: Vec<(String, String)> =
        conn.exec_rows("SELECT name, sql FROM sqlite_schema WHERE type = 'table' AND name = 't'");
    assert_eq!(
        tables
            .into_iter()
            .map(|(name, sql)| (name, canonicalize_schema_sql(&sql)))
            .collect::<Vec<_>>(),
        vec![(
            "t".to_string(),
            "CREATE TABLE t (id INTEGER PRIMARY KEY, payload TEXT)".to_string()
        )]
    );

    let rows: Vec<(i64,)> = conn.exec_rows("SELECT COUNT(*) FROM t");
    assert_eq!(rows[0].0, 0);

    let post_vacuum_pages = scalar_i64(&conn, "PRAGMA page_count");
    assert_eq!(
        post_vacuum_pages, 2,
        "VACUUM should reduce an emptied table back to the header page plus the empty table root page: before={pre_vacuum_pages}, after={post_vacuum_pages}"
    );

    Ok(())
}

/// Multiple VACUUMs in a row work correctly.
#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(init_sql = "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);")]
fn test_plain_vacuum_repeated(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES(1, 'one')")?;
    conn.execute("INSERT INTO t VALUES(2, 'two')")?;
    conn.execute("INSERT INTO t VALUES(3, 'three')")?;
    conn.execute("DELETE FROM t WHERE a = 2")?;
    run_plain_vacuum_and_assert_round_trip(&tmp_db, &conn)?;
    run_plain_vacuum_and_assert_round_trip(&tmp_db, &conn)?;
    run_plain_vacuum_and_assert_round_trip(&tmp_db, &conn)?;

    let rows: Vec<(i64, String)> = conn.exec_rows("SELECT a, b FROM t ORDER BY a");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "one".into()));
    assert_eq!(rows[1], (3, "three".into()));

    Ok(())
}

/// Writes after VACUUM work correctly.
#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(init_sql = "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);")]
fn test_plain_vacuum_then_write(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES(1, 'one')")?;
    conn.execute("INSERT INTO t VALUES(2, 'two')")?;
    conn.execute("DELETE FROM t WHERE a = 1")?;
    run_plain_vacuum_and_assert_round_trip(&tmp_db, &conn)?;
    conn.execute("INSERT INTO t VALUES(3, 'three')")?;
    conn.execute("INSERT INTO t VALUES(4, 'four')")?;

    let rows: Vec<(i64, String)> = conn.exec_rows("SELECT a, b FROM t ORDER BY a");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (2, "two".into()));
    assert_eq!(rows[1], (3, "three".into()));
    assert_eq!(rows[2], (4, "four".into()));

    Ok(())
}

/// Plain VACUUM rejects query_only mode.
#[turso_macros::test(init_sql = "CREATE TABLE t(a INTEGER);")]
fn test_plain_vacuum_rejects_query_only(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("PRAGMA query_only = 1")?;
    let err = conn.execute("VACUUM").unwrap_err();
    assert_eq!(
        err.to_string(),
        "Parse error: Cannot execute VACUUM in query_only mode",
        "unexpected error: {err}"
    );
    Ok(())
}

/// Plain VACUUM rejects active statements on same connection.
#[turso_macros::test(init_sql = "CREATE TABLE t(a INTEGER);")]
fn test_plain_vacuum_rejects_active_statement(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO t VALUES(1)")?;
    conn.execute("INSERT INTO t VALUES(2)")?;

    // Hold an active SELECT open
    let mut stmt = conn.prepare("SELECT * FROM t")?;
    let step_result = stmt.step()?;
    assert!(matches!(step_result, StepResult::Row));

    // VACUUM should fail while the SELECT is active
    let err = conn.execute("VACUUM").unwrap_err();
    assert_eq!(
        err.to_string(),
        "Transaction error: cannot VACUUM - SQL statements in progress",
        "unexpected error: {err}"
    );

    stmt.reset()?;
    Ok(())
}

/// Plain VACUUM reduces page_count when rows are deleted, and a subsequent
/// checkpoint truncates the .db file to the compacted size.
#[cfg_attr(feature = "checksum", ignore)]
#[turso_macros::test(init_sql = "CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT);")]
fn test_plain_vacuum_reduces_page_count(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();

    // Insert enough data to grow the database well past the minimum.
    for i in 0..200 {
        conn.execute(format!("INSERT INTO t VALUES({i}, '{}')", "x".repeat(100)))?;
    }

    let pre_pages: Vec<(i64,)> = conn.exec_rows("PRAGMA page_count");
    assert!(
        pre_pages[0].0 > 5,
        "source should have multiple pages, got: {}",
        pre_pages[0].0
    );

    // Delete most rows to create free pages.
    conn.execute("DELETE FROM t WHERE a >= 10")?;

    // page_count should not have decreased yet (deleted pages are on freelist).
    let after_delete_pages: Vec<(i64,)> = conn.exec_rows("PRAGMA page_count");
    assert_eq!(after_delete_pages[0].0, pre_pages[0].0);

    let before_hash = compute_plain_vacuum_dbhash(&tmp_db);
    conn.execute("VACUUM")?;

    // After VACUUM the compacted image should be smaller.
    let post_vacuum_pages: Vec<(i64,)> = conn.exec_rows("PRAGMA page_count");
    assert!(
        post_vacuum_pages[0].0 < pre_pages[0].0,
        "page_count should decrease after VACUUM: before={}, after={}",
        pre_pages[0].0,
        post_vacuum_pages[0].0
    );

    // VACUUM includes a TRUNCATE checkpoint, so the WAL should be empty.
    let wal_path = format!("{}-wal", tmp_db.path.display());
    let wal_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    assert_eq!(wal_size, 0, "WAL should be truncated to zero after VACUUM");

    // Data integrity: the 10 remaining rows should be intact.
    let rows: Vec<(i64,)> = conn.exec_rows("SELECT COUNT(*) FROM t");
    assert_eq!(rows[0].0, 10);
    assert_plain_vacuum_integrity_and_hash(&tmp_db, &conn, &before_hash);
    Ok(())
}

/// Plain VACUUM should compact the same fragmented workload down to the same
/// final page count as SQLite.
#[test]
fn test_plain_vacuum_matches_sqlite_page_count_after_updates_and_deletes() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    let sqlite_dir = TempDir::new()?;
    let sqlite_path = sqlite_dir.path().join("sqlite_vacuum_reference.db");
    let sqlite_conn = SqliteConnection::open(&sqlite_path)?;

    for sql in [
        "PRAGMA page_size = 1024",
        "CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
    ] {
        conn.execute(sql)?;
        sqlite_conn.execute_batch(sql)?;
    }

    conn.execute("BEGIN IMMEDIATE")?;
    sqlite_conn.execute_batch("BEGIN IMMEDIATE")?;

    for i in 0..3500_i64 {
        let payload = format!("seed-{i:04}-{}", "payload".repeat(35));
        conn.execute(format!(
            "INSERT INTO t VALUES({i}, '{}')",
            escape_sqlite_string_literal(&payload)
        ))?;
        sqlite_conn.execute(
            "INSERT INTO t VALUES(?1, ?2)",
            rusqlite::params![i, payload],
        )?;
    }

    for i in (0..3500_i64).step_by(3) {
        let payload = format!("wide-{i:04}-{}", "updated".repeat(55));
        conn.execute(format!(
            "UPDATE t SET payload = '{}' WHERE id = {i}",
            escape_sqlite_string_literal(&payload)
        ))?;
        sqlite_conn.execute(
            "UPDATE t SET payload = ?1 WHERE id = ?2",
            rusqlite::params![payload, i],
        )?;
    }

    conn.execute("DELETE FROM t WHERE id % 4 = 0")?;
    sqlite_conn.execute("DELETE FROM t WHERE id % 4 = 0", [])?;

    for i in (1..3500_i64).step_by(5) {
        let payload = format!("tail-{i:04}-{}", "rewrite".repeat(28));
        conn.execute(format!(
            "UPDATE t SET payload = '{}' WHERE id = {i} AND id % 4 != 0",
            escape_sqlite_string_literal(&payload)
        ))?;
        sqlite_conn.execute(
            "UPDATE t SET payload = ?1 WHERE id = ?2 AND id % 4 != 0",
            rusqlite::params![payload, i],
        )?;
    }

    conn.execute("COMMIT")?;
    sqlite_conn.execute_batch("COMMIT")?;

    let pre_pages = scalar_i64(&conn, "PRAGMA page_count");
    let pre_sqlite_pages = sqlite_scalar_i64(&sqlite_conn, "PRAGMA page_count");
    assert!(
        pre_pages > 100,
        "Turso reference workload should exceed 100 pages before VACUUM, got {pre_pages}"
    );
    assert!(
        pre_sqlite_pages > 100,
        "SQLite reference workload should exceed 100 pages before VACUUM, got {pre_sqlite_pages}"
    );

    let before_hash = compute_plain_vacuum_dbhash(&tmp_db);
    conn.execute("VACUUM")?;
    sqlite_conn.execute_batch("VACUUM")?;

    let post_pages = scalar_i64(&conn, "PRAGMA page_count");
    let post_sqlite_pages = sqlite_scalar_i64(&sqlite_conn, "PRAGMA page_count");
    assert_eq!(
        post_pages, post_sqlite_pages,
        "plain VACUUM should match SQLite final page count for the same fragmented workload"
    );
    assert_plain_vacuum_integrity_and_hash(&tmp_db, &conn, &before_hash);
    assert_plain_vacuum_folded_into_db_file(&tmp_db, &conn);

    Ok(())
}

/// Plain VACUUM copy-back batch boundaries. The core unit tests assert the
/// exact range math; this integration test builds compacted images around the
/// 64-page boundary so the read/write batch state machine crosses one-page,
/// exact-boundary, and one-over-boundary cases.
#[test]
fn test_plain_vacuum_copy_batch_page_count_boundaries() -> anyhow::Result<()> {
    for target_pages in [2_i64, 63, 64, 65, 128, 129] {
        let tmp_db = TempDatabase::new_empty();
        let conn = tmp_db.connect_limbo();
        conn.execute("PRAGMA page_size = 512")?;
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT)")?;

        let inserted = populate_until_page_count(&conn, target_pages, 350)?;
        assert!(
            inserted > 0 || target_pages == 2,
            "boundary workload should insert rows for target {target_pages}"
        );

        let before_hash = compute_plain_vacuum_dbhash(&tmp_db);
        conn.execute("VACUUM")?;

        assert_eq!(
            scalar_i64(&conn, "PRAGMA page_count"),
            target_pages,
            "VACUUM should preserve compacted page count for boundary target {target_pages}"
        );
        assert_eq!(
            scalar_i64(&conn, "SELECT COUNT(*) FROM t"),
            inserted,
            "row count should survive boundary VACUUM for target {target_pages}"
        );
        assert_plain_vacuum_integrity_and_hash(&tmp_db, &conn, &before_hash);
        assert_plain_vacuum_folded_into_db_file(&tmp_db, &conn);
    }

    Ok(())
}

/// Truly empty databases exercise the lower edge of the copy-back setup: there
/// may be no user schema pages to copy, but VACUUM still must leave the file
/// usable and folded.
#[test]
fn test_plain_vacuum_empty_schema_physical_contract() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    // A truly empty DB (page 1 never allocated) is not a valid VACUUM target —
    // an existing database that "looks empty" indicates a problem, not a no-op.
    let err = conn
        .execute("VACUUM")
        .expect_err("VACUUM on an uninitialized database should return an error");
    assert_eq!(
        err.to_string(),
        "Internal error: begin_blocking_tx can be done on an initialized database (page 1 must already be allocated)",
        "expected initialization error, got: {err}"
    );
    Ok(())
}

/// Initialized DB whose user schema has been fully torn down — page 1 exists,
/// but there are no user tables or indexes. VACUUM must succeed and leave the
/// DB queryable at the minimum page count.
///
/// FIXME: currently ignored because Turso's VACUUM does not reclaim the root
/// page of the dropped table. SQLite produces page_count=1 for the same
/// sequence; Turso produces page_count > 1. The integrity_check passes, so
/// this is a compaction/correctness gap in the target-pager rebuild rather
/// than a data-loss bug.
#[ignore = "VACUUM does not reclaim dropped-table root pages (diverges from SQLite)"]
#[test]
fn test_plain_vacuum_initialized_but_empty_schema() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE _scratch(i)")?;
    conn.execute("DROP TABLE _scratch")?;

    run_plain_vacuum_and_assert_round_trip(&tmp_db, &conn)?;

    assert!(
        scalar_i64(&conn, "PRAGMA page_count") <= 1,
        "empty schema should stay at the minimum page count"
    );
    let tables: Vec<(i64,)> =
        conn.exec_rows("SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table'");
    assert_eq!(tables[0].0, 0, "no user tables should remain after DROP");
    Ok(())
}

/// Plain VACUUM on a workload with several tables and indexes, large enough
/// to span multiple copy-back batches and exercise a non-monotonic
/// page→frame map in the temp WAL. This is the path that drives
/// `coalesce_frame_runs` to build more than one run per batch.
#[test]
fn test_plain_vacuum_multi_table_multi_batch() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE a(id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("CREATE TABLE b(id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("CREATE TABLE c(id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("CREATE INDEX idx_a_v ON a(v)")?;
    conn.execute("CREATE INDEX idx_b_v ON b(v)")?;
    conn.execute("CREATE INDEX idx_c_v ON c(v)")?;

    // Enough rows to push total_pages past VACUUM_COPY_BATCH_SIZE (64) so the
    // batch path is hit more than once, and to grow each table's b-tree
    // across several pages so target-build interleaves table/index frames.
    for i in 0..800 {
        conn.execute(format!("INSERT INTO a VALUES({i}, '{}')", "a".repeat(200)))?;
        conn.execute(format!("INSERT INTO b VALUES({i}, '{}')", "b".repeat(200)))?;
        conn.execute(format!("INSERT INTO c VALUES({i}, '{}')", "c".repeat(200)))?;
    }
    // Delete half of each table so the pre-VACUUM image has freelist holes.
    conn.execute("DELETE FROM a WHERE id % 2 = 0")?;
    conn.execute("DELETE FROM b WHERE id % 2 = 0")?;
    conn.execute("DELETE FROM c WHERE id % 2 = 0")?;

    let pre_pages: Vec<(i64,)> = conn.exec_rows("PRAGMA page_count");
    assert!(
        pre_pages[0].0 > 64,
        "workload should span multiple copy-back batches, got page_count={}",
        pre_pages[0].0
    );

    let before_hash = compute_plain_vacuum_dbhash(&tmp_db);
    conn.execute("VACUUM")?;

    // Row counts preserved.
    let counts: Vec<(i64,)> = conn.exec_rows(
        "SELECT (SELECT COUNT(*) FROM a) + (SELECT COUNT(*) FROM b) + (SELECT COUNT(*) FROM c)",
    );
    assert_eq!(counts[0].0, 1200);

    // Index-covered reads still work.
    let via_idx_a: Vec<(i64,)> =
        conn.exec_rows("SELECT id FROM a WHERE v = 'aaaaaaaa' ORDER BY id");
    assert_eq!(via_idx_a.len(), 0); // sanity: no row matches short value
    let via_idx_b: Vec<(i64,)> = conn.exec_rows("SELECT COUNT(*) FROM b WHERE v LIKE 'b%'");
    assert_eq!(via_idx_b[0].0, 400);

    // Spot-check specific rows round-trip.
    let a_one: Vec<(i64, String)> = conn.exec_rows("SELECT id, v FROM a WHERE id = 1");
    assert_eq!(a_one.len(), 1);
    assert_eq!(a_one[0], (1, "a".repeat(200)));
    let c_last: Vec<(i64, String)> = conn.exec_rows("SELECT id, v FROM c WHERE id = 799");
    assert_eq!(c_last.len(), 1);
    assert_eq!(c_last[0], (799, "c".repeat(200)));

    assert_plain_vacuum_integrity_and_hash(&tmp_db, &conn, &before_hash);
    Ok(())
}

/// Page-size coverage for the batched read/write path. Small pages force many
/// frames; the large page checks that frame sizing and DB-file truncation do
/// not assume the default 4096-byte page size.
#[test]
fn test_plain_vacuum_page_size_variants_fold_and_integrity() -> anyhow::Result<()> {
    for page_size in [512_i64, 1024, 4096, 65536] {
        let tmp_db = TempDatabase::new_empty();
        let conn = tmp_db.connect_limbo();
        conn.execute(format!("PRAGMA page_size = {page_size}"))?;
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB, tag TEXT)")?;
        conn.execute("CREATE INDEX idx_t_tag ON t(tag)")?;

        let rows = if page_size <= 1024 { 180 } else { 40 };
        let blob_size = if page_size <= 1024 { 1800 } else { 9000 };
        for i in 0..rows {
            conn.execute(format!(
                "INSERT INTO t VALUES({i}, zeroblob({}), 'tag-{}')",
                blob_size + i % 17,
                i % 11
            ))?;
        }
        conn.execute("DELETE FROM t WHERE id % 3 = 0")?;

        let pre_pages = scalar_i64(&conn, "PRAGMA page_count");
        let before_hash = compute_plain_vacuum_dbhash(&tmp_db);
        conn.execute("VACUUM")?;

        assert_eq!(scalar_i64(&conn, "PRAGMA page_size"), page_size);
        assert!(
            scalar_i64(&conn, "PRAGMA page_count") <= pre_pages,
            "VACUUM should not grow page_count for page_size={page_size}"
        );
        assert_eq!(
            scalar_i64(&conn, "SELECT COUNT(*) FROM t"),
            rows - (rows + 2) / 3
        );
        assert_eq!(
            scalar_i64(&conn, "SELECT COUNT(*) FROM t WHERE tag = 'tag-5'"),
            scalar_i64(
                &conn,
                "SELECT COUNT(*) FROM t NOT INDEXED WHERE tag = 'tag-5'"
            )
        );
        assert_plain_vacuum_integrity_and_hash(&tmp_db, &conn, &before_hash);
        assert_plain_vacuum_folded_into_db_file(&tmp_db, &conn);
    }

    Ok(())
}

/// Stress the storage shapes that make batched VACUUM risky: overflow chains,
/// freelist pages, table and index b-trees, triggers, views, and a second
/// primary-key table all in one compacted image.
#[test]
fn test_plain_vacuum_complex_batched_storage_shapes() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::builder().with_views(true).build();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA page_size = 1024")?;
    conn.execute(
        "CREATE TABLE docs(
            id INTEGER PRIMARY KEY,
            category TEXT NOT NULL,
            payload BLOB NOT NULL,
            note TEXT
        )",
    )?;
    conn.execute("CREATE INDEX idx_docs_category_note ON docs(category, note)")?;
    conn.execute("CREATE INDEX idx_docs_note_partial ON docs(note) WHERE category = 'keep'")?;
    conn.execute("CREATE TABLE audit(doc_id INTEGER, payload_len INTEGER)")?;
    conn.execute(
        "CREATE TRIGGER docs_ai AFTER INSERT ON docs BEGIN
            INSERT INTO audit VALUES(new.id, length(new.payload));
        END",
    )?;
    conn.execute("CREATE TABLE kv(k TEXT PRIMARY KEY, v TEXT)")?;
    conn.execute("CREATE VIEW kept_docs AS SELECT id, note FROM docs WHERE category = 'keep'")?;

    for i in 0..180 {
        let category = if i % 4 == 0 { "drop" } else { "keep" };
        conn.execute(format!(
            "INSERT INTO docs VALUES({i}, '{category}', zeroblob({}), 'note-{}')",
            2500 + (i % 13),
            i % 23
        ))?;
        conn.execute(format!(
            "INSERT INTO kv VALUES('k-{i:03}', '{}')",
            "v".repeat(90)
        ))?;
    }
    conn.execute("DELETE FROM docs WHERE category = 'drop'")?;
    conn.execute("DELETE FROM kv WHERE k > 'k-120'")?;

    let pre_pages = scalar_i64(&conn, "PRAGMA page_count");
    assert!(
        pre_pages > 64,
        "complex workload should force multiple copy-back batches, got {pre_pages}"
    );

    let before_hash = compute_plain_vacuum_dbhash(&tmp_db);
    conn.execute("VACUUM")?;

    let indexes: Vec<(String,)> = conn.exec_rows(
        "SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'docs' ORDER BY name",
    );
    assert_eq!(
        indexes,
        vec![
            ("idx_docs_category_note".to_string(),),
            ("idx_docs_note_partial".to_string(),),
        ]
    );

    let views: Vec<(String,)> =
        conn.exec_rows("SELECT name FROM sqlite_schema WHERE type = 'view' ORDER BY name");
    assert_eq!(views, vec![("kept_docs".to_string(),)]);

    let eqp_composite: Vec<(i64, i64, i64, String)> = conn.exec_rows(
        "EXPLAIN QUERY PLAN \
         SELECT id, note FROM docs INDEXED BY idx_docs_category_note \
         WHERE category = 'keep' AND note = 'note-7' ORDER BY id",
    );
    assert!(
        eqp_composite.iter().any(|(_, _, _, detail)| {
            detail.contains("INDEX") && detail.contains("idx_docs_category_note")
        }),
        "expected lookup to use idx_docs_category_note after plain VACUUM, got plan: {eqp_composite:?}",
    );

    let eqp_partial: Vec<(i64, i64, i64, String)> = conn.exec_rows(
        "EXPLAIN QUERY PLAN \
         SELECT id, note FROM docs INDEXED BY idx_docs_note_partial \
         WHERE category = 'keep' AND note = 'note-7' ORDER BY id",
    );
    assert!(
        eqp_partial
            .iter()
            .any(|(_, _, _, detail)| detail.contains("INDEX") && detail.contains("idx_docs_note_partial")),
        "expected lookup to use idx_docs_note_partial after plain VACUUM, got plan: {eqp_partial:?}",
    );

    let eqp_view: Vec<(i64, i64, i64, String)> = conn.exec_rows(
        "EXPLAIN QUERY PLAN \
         SELECT id, note FROM kept_docs WHERE note = 'note-7' ORDER BY id",
    );
    assert!(
        eqp_view.iter().any(|(_, _, _, detail)| {
            detail.contains("INDEX")
                && (detail.contains("idx_docs_category_note")
                    || detail.contains("idx_docs_note_partial"))
        }),
        "expected kept_docs query to remain plannable through a docs index after plain VACUUM, got plan: {eqp_view:?}",
    );

    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM docs"), 135);
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM kept_docs"), 135);
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM kv"), 121);
    let kept_note_rows: Vec<(i64, String)> =
        conn.exec_rows("SELECT id, note FROM kept_docs WHERE note = 'note-7' ORDER BY id");
    assert_eq!(
        kept_note_rows,
        vec![
            (7, "note-7".to_string()),
            (30, "note-7".to_string()),
            (53, "note-7".to_string()),
            (99, "note-7".to_string()),
            (122, "note-7".to_string()),
            (145, "note-7".to_string()),
        ]
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM docs WHERE category = 'keep' AND note = 'note-7'"
        ),
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM docs NOT INDEXED WHERE category = 'keep' AND note = 'note-7'"
        )
    );
    assert_eq!(
        scalar_i64(
            &conn,
            "SELECT SUM(length(payload)) FROM docs WHERE id BETWEEN 1 AND 20"
        ),
        scalar_i64(
            &conn,
            "SELECT SUM(length(payload)) FROM docs NOT INDEXED WHERE id BETWEEN 1 AND 20"
        )
    );
    assert_plain_vacuum_integrity_and_hash(&tmp_db, &conn, &before_hash);
    assert_plain_vacuum_folded_into_db_file(&tmp_db, &conn);

    conn.execute("INSERT INTO docs VALUES(1000, 'keep', zeroblob(4097), 'after-vacuum')")?;
    assert_eq!(
        scalar_i64(&conn, "SELECT payload_len FROM audit WHERE doc_id = 1000"),
        4097
    );
    assert_eq!(run_integrity_check(&conn), "ok");
    Ok(())
}

/// Plain VACUUM on an encrypted source database. Exercises the per-frame
/// decrypt branch inside `Wal::read_frames_batch`: encryption is
/// propagated to the temp pager (see `vacuum_temp_db_encryption`), so the
/// batch read path decrypts each frame before feeding it into the source
/// WAL write batch.
#[test]
fn test_plain_vacuum_encrypted() -> anyhow::Result<()> {
    const HEXKEY: &str = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";

    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute(format!("PRAGMA hexkey = '{HEXKEY}'"))?;
    conn.execute("PRAGMA cipher = 'aegis256'")?;

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
    for i in 0..150 {
        conn.execute(format!("INSERT INTO t VALUES({i}, '{}')", "z".repeat(80)))?;
    }
    conn.execute("DELETE FROM t WHERE id >= 30")?;

    conn.execute("VACUUM")?;
    assert_eq!(run_integrity_check(&conn), "ok");

    // Data round-trips through encrypted temp WAL → encrypted source WAL.
    let count: Vec<(i64,)> = conn.exec_rows("SELECT COUNT(*) FROM t");
    assert_eq!(count[0].0, 30);
    let first: Vec<(i64, String)> = conn.exec_rows("SELECT id, v FROM t WHERE id = 0");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0], (0, "z".repeat(80)));
    let last: Vec<(i64, String)> = conn.exec_rows("SELECT id, v FROM t WHERE id = 29");
    assert_eq!(last.len(), 1);
    assert_eq!(last[0], (29, "z".repeat(80)));

    Ok(())
}

/// Plain VACUUM on an existing encrypted database after the original
/// connection is closed. This exercises the restart path where a fresh
/// connection must reapply the key before reading the source image.
#[test]
fn test_plain_vacuum_encrypted_existing() -> anyhow::Result<()> {
    const HEXKEY: &str = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";

    let tmp_db = TempDatabase::new_empty();
    {
        let conn = tmp_db.connect_limbo();
        conn.execute(format!("PRAGMA hexkey = '{HEXKEY}'"))?;
        conn.execute("PRAGMA cipher = 'aegis256'")?;

        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
        for i in 0..150 {
            conn.execute(format!("INSERT INTO t VALUES({i}, '{}')", "z".repeat(80)))?;
        }
        conn.execute("DELETE FROM t WHERE id >= 30")?;
        do_flush(&conn, &tmp_db)?;
    }

    let reopened = TempDatabase::new_with_existent_with_opts(&tmp_db.path, tmp_db.db_opts);
    let reopened_conn = reopened.connect_limbo();
    reopened_conn.execute(format!("PRAGMA hexkey = '{HEXKEY}'"))?;
    reopened_conn.execute("PRAGMA cipher = 'aegis256'")?;

    reopened_conn.execute("VACUUM")?;
    assert_eq!(run_integrity_check(&reopened_conn), "ok");

    let count: Vec<(i64,)> = reopened_conn.exec_rows("SELECT COUNT(*) FROM t");
    assert_eq!(count[0].0, 30);
    let first: Vec<(i64, String)> = reopened_conn.exec_rows("SELECT id, v FROM t WHERE id = 0");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0], (0, "z".repeat(80)));
    let last: Vec<(i64, String)> = reopened_conn.exec_rows("SELECT id, v FROM t WHERE id = 29");
    assert_eq!(last.len(), 1);
    assert_eq!(last[0], (29, "z".repeat(80)));

    let post_vacuum = TempDatabase::new_with_existent_with_opts(&tmp_db.path, tmp_db.db_opts);
    let post_vacuum_conn = post_vacuum.connect_limbo();
    post_vacuum_conn.execute(format!("PRAGMA hexkey = '{HEXKEY}'"))?;
    post_vacuum_conn.execute("PRAGMA cipher = 'aegis256'")?;
    assert_eq!(run_integrity_check(&post_vacuum_conn), "ok");
    assert_eq!(scalar_i64(&post_vacuum_conn, "SELECT COUNT(*) FROM t"), 30);

    Ok(())
}

#[test]
fn test_plain_vacuum_preserves_full_autovacuum() -> anyhow::Result<()> {
    assert_plain_vacuum_preserves_autovacuum_mode("full", 1)
}

#[test]
fn test_mvcc_plain_vacuum_preserves_full_autovacuum_after_reopen() -> anyhow::Result<()> {
    let opts = DatabaseOpts::new().with_autovacuum(true);
    let (_temp_dir, tmp_db) = open_sqlite_autovacuum_db("full", opts)?;
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA journal_mode = 'mvcc'")?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_eq!(scalar_i64(&conn, "PRAGMA auto_vacuum"), 1);

    let before_hash = compute_plain_vacuum_dbhash(&tmp_db);
    conn.execute("VACUUM")?;
    let journal_mode: Vec<(String,)> = conn.exec_rows("PRAGMA journal_mode");
    assert_eq!(journal_mode, vec![("mvcc".to_string(),)]);
    assert_eq!(scalar_i64(&conn, "PRAGMA auto_vacuum"), 1);
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM t"), 120);
    assert_plain_vacuum_integrity_and_hash(&tmp_db, &conn, &before_hash);
    assert_plain_vacuum_folded_into_db_file(&tmp_db, &conn);

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let reopened = TempDatabase::new_with_existent_with_opts(&path, opts);
    let reopened_conn = reopened.connect_limbo();
    let journal_mode: Vec<(String,)> = reopened_conn.exec_rows("PRAGMA journal_mode");
    assert_eq!(journal_mode, vec![("mvcc".to_string(),)]);
    assert_eq!(scalar_i64(&reopened_conn, "PRAGMA auto_vacuum"), 1);
    assert_eq!(run_integrity_check(&reopened_conn), "ok");
    assert_eq!(scalar_i64(&reopened_conn, "SELECT COUNT(*) FROM t"), 120);

    Ok(())
}

#[test]
fn test_vacuum_into_preserves_full_autovacuum() -> anyhow::Result<()> {
    assert_vacuum_into_preserves_autovacuum_mode("full", 1)
}

#[test]
fn test_mvcc_vacuum_into_preserves_full_autovacuum_after_reopen() -> anyhow::Result<()> {
    let opts = DatabaseOpts::new().with_autovacuum(true);
    let (_temp_dir, tmp_db) = open_sqlite_autovacuum_db("full", opts)?;
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA journal_mode = 'mvcc'")?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_eq!(scalar_i64(&conn, "PRAGMA auto_vacuum"), 1);

    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("mvcc-vacuum-into-full-autovacuum.db");
    conn.execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))?;

    let dest_db = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
    let dest_conn = dest_db.connect_limbo();
    let journal_mode: Vec<(String,)> = dest_conn.exec_rows("PRAGMA journal_mode");
    assert_eq!(journal_mode, vec![("mvcc".to_string(),)]);
    assert_eq!(scalar_i64(&dest_conn, "PRAGMA auto_vacuum"), 1);
    assert_eq!(run_integrity_check(&dest_conn), "ok");
    assert_eq!(scalar_i64(&dest_conn, "SELECT COUNT(*) FROM t"), 120);

    drop(dest_conn);
    drop(dest_db);

    let reopened = TempDatabase::new_with_existent_with_opts(&dest_path, opts);
    let reopened_conn = reopened.connect_limbo();
    let journal_mode: Vec<(String,)> = reopened_conn.exec_rows("PRAGMA journal_mode");
    assert_eq!(journal_mode, vec![("mvcc".to_string(),)]);
    assert_eq!(scalar_i64(&reopened_conn, "PRAGMA auto_vacuum"), 1);
    assert_eq!(run_integrity_check(&reopened_conn), "ok");

    Ok(())
}

#[test]
fn test_plain_vacuum_incremental_autovacuum_still_unsupported() -> anyhow::Result<()> {
    let opts = DatabaseOpts::new()
        .with_encryption(true)
        .with_autovacuum(true);
    let (_temp_dir, tmp_db) = open_sqlite_autovacuum_db("incremental", opts)?;
    let conn = tmp_db.connect_limbo();

    let err = conn.execute("VACUUM").unwrap_err();
    assert_eq!(
        err.to_string(),
        "Internal error: Incremental auto-vacuum is not supported",
        "unexpected error: {err}"
    );

    Ok(())
}

#[test]
fn test_vacuum_into_incremental_autovacuum_still_unsupported() -> anyhow::Result<()> {
    let opts = DatabaseOpts::new()
        .with_encryption(true)
        .with_autovacuum(true);
    let (_temp_dir, tmp_db) = open_sqlite_autovacuum_db("incremental", opts)?;
    let conn = tmp_db.connect_limbo();
    let dest_dir = TempDir::new()?;
    let dest_path = dest_dir.path().join("vacuum-into.db");

    let err = conn
        .execute(format!("VACUUM INTO '{}'", dest_path.to_str().unwrap()))
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Internal error: Incremental auto-vacuum is not supported",
        "unexpected error: {err}"
    );
    assert!(
        !dest_path.exists(),
        "unsupported VACUUM INTO should not leave an output file behind"
    );

    Ok(())
}

/// Keep one non-ignored plain VACUUM test under the checksum feature so
/// `read_frames_batch` verifies checksums while reading the temp WAL and
/// `prepare_frames` writes checksummed source-WAL frames.
#[cfg(feature = "checksum")]
#[test]
fn test_plain_vacuum_checksum_multi_batch() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA page_size = 1024")?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB, tag TEXT)")?;
    conn.execute("CREATE INDEX idx_t_tag ON t(tag)")?;
    for i in 0..160 {
        conn.execute(format!(
            "INSERT INTO t VALUES({i}, zeroblob({}), 'tag-{}')",
            1400 + i % 19,
            i % 9
        ))?;
    }
    conn.execute("DELETE FROM t WHERE id % 4 = 0")?;

    let pre_pages = scalar_i64(&conn, "PRAGMA page_count");
    assert!(
        pre_pages > 64,
        "checksum workload should cross copy-back batch boundary, got {pre_pages}"
    );

    let before_hash = compute_plain_vacuum_dbhash(&tmp_db);
    conn.execute("VACUUM")?;

    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM t"), 120);
    assert_eq!(
        scalar_i64(&conn, "SELECT COUNT(*) FROM t WHERE tag = 'tag-3'"),
        scalar_i64(
            &conn,
            "SELECT COUNT(*) FROM t NOT INDEXED WHERE tag = 'tag-3'"
        )
    );
    assert_plain_vacuum_integrity_and_hash(&tmp_db, &conn, &before_hash);
    assert_plain_vacuum_folded_into_db_file(&tmp_db, &conn);
    Ok(())
}

/// Active readers must make VACUUM fail before publishing a new WAL image. The
/// same operation must succeed after the reader releases its snapshot.
#[test]
fn test_plain_vacuum_active_reader_blocks_fold_contract() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_empty();
    let writer = tmp_db.connect_limbo();
    let reader = tmp_db.connect_limbo();

    writer.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT)")?;
    for i in 0..120 {
        writer.execute(format!("INSERT INTO t VALUES({i}, '{}')", "x".repeat(300)))?;
    }
    writer.execute("DELETE FROM t WHERE id >= 20")?;

    reader.execute("BEGIN")?;
    assert_eq!(scalar_i64(&reader, "SELECT COUNT(*) FROM t"), 20);

    let result = writer.execute("VACUUM");
    assert!(
        result.is_err(),
        "VACUUM must fail while another connection holds a WAL read snapshot"
    );

    reader.execute("ROLLBACK")?;
    let final_writer = tmp_db.connect_limbo();
    let before_hash = compute_plain_vacuum_dbhash(&tmp_db);
    final_writer.execute("VACUUM")?;

    assert_eq!(scalar_i64(&final_writer, "SELECT COUNT(*) FROM t"), 20);
    assert_plain_vacuum_integrity_and_hash(&tmp_db, &final_writer, &before_hash);
    assert_plain_vacuum_folded_into_db_file(&tmp_db, &final_writer);
    Ok(())
}

fn populate_mvcc_vacuum_workload(conn: &Arc<Connection>) -> anyhow::Result<Vec<(i64, String)>> {
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, tag TEXT NOT NULL, payload TEXT)")?;
    conn.execute("CREATE INDEX idx_t_tag ON t(tag)")?;

    for id in 0..200 {
        conn.execute(format!(
            "INSERT INTO t VALUES({id}, 'tag-{}', '{}')",
            id % 5,
            "x".repeat(300)
        ))?;
    }
    conn.execute("DELETE FROM t WHERE id % 3 = 0")?;
    conn.execute(format!(
        "UPDATE t SET tag = 'hot', payload = '{}' WHERE id % 10 = 1",
        "y".repeat(350)
    ))?;

    Ok((0..200)
        .filter(|id| id % 3 != 0)
        .map(|id| {
            let tag = if id % 10 == 1 {
                "hot".to_string()
            } else {
                format!("tag-{}", id % 5)
            };
            (id, tag)
        })
        .collect())
}

fn assert_mvcc_vacuum_workload(
    conn: &Arc<Connection>,
    expected_rows: &[(i64, String)],
) -> anyhow::Result<()> {
    assert_eq!(run_integrity_check(conn), "ok");

    let rows: Vec<(i64, String)> = conn.exec_rows("SELECT id, tag FROM t ORDER BY id");
    assert_eq!(rows, expected_rows);

    let indexed_hot: Vec<(i64,)> = conn.exec_rows("SELECT id FROM t WHERE tag = 'hot' ORDER BY id");
    let table_scan_hot: Vec<(i64,)> =
        conn.exec_rows("SELECT id FROM t NOT INDEXED WHERE tag = 'hot' ORDER BY id");
    assert_eq!(indexed_hot, table_scan_hot);

    Ok(())
}

fn populate_mvcc_multi_object_vacuum_workload(conn: &Arc<Connection>) -> anyhow::Result<()> {
    for table in ["a", "b", "c"] {
        conn.execute(format!(
            "CREATE TABLE {table}(id INTEGER PRIMARY KEY, tag TEXT NOT NULL, payload TEXT)"
        ))?;
        conn.execute(format!("CREATE INDEX idx_{table}_tag ON {table}(tag)"))?;
    }

    for id in 0..90 {
        conn.execute(format!(
            "INSERT INTO a VALUES({id}, 'tag-{}', '{}')",
            id % 3,
            "a".repeat(120)
        ))?;
        conn.execute(format!(
            "INSERT INTO b VALUES({id}, 'tag-{}', '{}')",
            id % 4,
            "b".repeat(120)
        ))?;
        conn.execute(format!(
            "INSERT INTO c VALUES({id}, 'tag-{}', '{}')",
            id % 5,
            "c".repeat(120)
        ))?;
    }

    conn.execute("DELETE FROM a WHERE id % 4 = 0")?;
    conn.execute("DELETE FROM b WHERE id % 5 = 0")?;
    conn.execute("DELETE FROM c WHERE id % 6 = 0")?;
    conn.execute("UPDATE a SET tag = 'hot-a' WHERE id % 10 = 1")?;
    conn.execute("UPDATE b SET tag = 'hot-b' WHERE id % 9 = 2")?;
    conn.execute("UPDATE c SET tag = 'hot-c' WHERE id % 8 = 3")?;
    Ok(())
}

fn assert_mvcc_multi_object_vacuum_workload(conn: &Arc<Connection>) -> anyhow::Result<()> {
    assert_eq!(run_integrity_check(conn), "ok");

    assert_eq!(scalar_i64(conn, "SELECT COUNT(*) FROM a"), 67);
    assert_eq!(scalar_i64(conn, "SELECT COUNT(*) FROM b"), 72);
    assert_eq!(scalar_i64(conn, "SELECT COUNT(*) FROM c"), 75);

    let hot_a: Vec<(i64,)> = conn.exec_rows("SELECT id FROM a WHERE tag = 'hot-a' ORDER BY id");
    let hot_a_scan: Vec<(i64,)> =
        conn.exec_rows("SELECT id FROM a NOT INDEXED WHERE tag = 'hot-a' ORDER BY id");
    assert_eq!(hot_a, hot_a_scan);
    assert_eq!(
        hot_a,
        vec![(1,), (11,), (21,), (31,), (41,), (51,), (61,), (71,), (81,)]
    );

    let hot_b: Vec<(i64,)> = conn.exec_rows("SELECT id FROM b WHERE tag = 'hot-b' ORDER BY id");
    let hot_b_scan: Vec<(i64,)> =
        conn.exec_rows("SELECT id FROM b NOT INDEXED WHERE tag = 'hot-b' ORDER BY id");
    assert_eq!(hot_b, hot_b_scan);
    assert_eq!(
        hot_b,
        vec![(2,), (11,), (29,), (38,), (47,), (56,), (74,), (83,)]
    );

    let hot_c: Vec<(i64,)> = conn.exec_rows("SELECT id FROM c WHERE tag = 'hot-c' ORDER BY id");
    let hot_c_scan: Vec<(i64,)> =
        conn.exec_rows("SELECT id FROM c NOT INDEXED WHERE tag = 'hot-c' ORDER BY id");
    assert_eq!(hot_c, hot_c_scan);
    assert_eq!(
        hot_c,
        vec![
            (3,),
            (11,),
            (19,),
            (27,),
            (35,),
            (43,),
            (51,),
            (59,),
            (67,),
            (75,),
            (83,)
        ]
    );

    Ok(())
}

#[test]
fn test_mvcc_plain_vacuum_multi_object_rootpage_reset() -> anyhow::Result<()> {
    let tmp_db =
        TempDatabase::new_with_mvcc("test_mvcc_plain_vacuum_multi_object_rootpage_reset.db");
    let conn = tmp_db.connect_limbo();

    populate_mvcc_multi_object_vacuum_workload(&conn)?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_eq!(mvcc_log_file_size(&tmp_db), 0);

    assert_plain_vacuum_preserves_content_hash(&tmp_db, &conn)?;
    assert_plain_vacuum_folded_into_db_file(&tmp_db, &conn);
    assert_mvcc_multi_object_vacuum_workload(&conn)?;

    conn.execute("INSERT INTO a VALUES(1000, 'after-a', 'payload-a')")?;
    conn.execute("UPDATE a SET tag = 'after-a-updated' WHERE id = 1")?;
    conn.execute("INSERT INTO b VALUES(1001, 'after-b', 'payload-b')")?;
    conn.execute("DELETE FROM b WHERE id = 3")?;
    conn.execute("INSERT INTO c VALUES(1002, 'after-c', 'payload-c')")?;
    conn.execute("UPDATE c SET tag = 'after-c-updated' WHERE id = 11")?;

    let after_a: Vec<(i64,)> = conn.exec_rows("SELECT id FROM a WHERE tag = 'after-a' ORDER BY id");
    assert_eq!(after_a, vec![(1000,)]);
    let after_a_updated: Vec<(i64,)> =
        conn.exec_rows("SELECT id FROM a WHERE tag = 'after-a-updated' ORDER BY id");
    assert_eq!(after_a_updated, vec![(1,)]);
    let after_b: Vec<(i64,)> = conn.exec_rows("SELECT id FROM b WHERE tag = 'after-b' ORDER BY id");
    assert_eq!(after_b, vec![(1001,)]);
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM b WHERE id = 3"), 0);
    let after_c: Vec<(i64,)> = conn.exec_rows("SELECT id FROM c WHERE tag = 'after-c' ORDER BY id");
    assert_eq!(after_c, vec![(1002,)]);
    let after_c_updated: Vec<(i64,)> =
        conn.exec_rows("SELECT id FROM c WHERE tag = 'after-c-updated' ORDER BY id");
    assert_eq!(after_c_updated, vec![(11,)]);
    assert_eq!(run_integrity_check(&conn), "ok");

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_eq!(mvcc_log_file_size(&tmp_db), 0);

    let reopened = TempDatabase::new_with_existent_with_opts(&tmp_db.path, tmp_db.db_opts);
    let reopened_conn = reopened.connect_limbo();
    assert_eq!(run_integrity_check(&reopened_conn), "ok");
    let reopened_a: Vec<(i64,)> =
        reopened_conn.exec_rows("SELECT id FROM a WHERE tag = 'after-a' ORDER BY id");
    assert_eq!(reopened_a, vec![(1000,)]);
    let reopened_b: Vec<(i64,)> =
        reopened_conn.exec_rows("SELECT id FROM b WHERE tag = 'after-b' ORDER BY id");
    assert_eq!(reopened_b, vec![(1001,)]);
    let reopened_c: Vec<(i64,)> =
        reopened_conn.exec_rows("SELECT id FROM c WHERE tag = 'after-c' ORDER BY id");
    assert_eq!(reopened_c, vec![(1002,)]);

    Ok(())
}

#[test]
fn test_mvcc_plain_vacuum_discards_reused_index_rootpage_state() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_with_mvcc(
        "test_mvcc_plain_vacuum_discards_reused_index_rootpage_state.db",
    );
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("CREATE UNIQUE INDEX idx_old ON t(v COLLATE NOCASE)")?;
    conn.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")?;
    conn.execute("UPDATE t SET v = 'c' WHERE id = 1")?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_eq!(mvcc_log_file_size(&tmp_db), 0);

    conn.execute("DROP INDEX idx_old")?;
    conn.execute("DROP TABLE t")?;
    conn.execute("CREATE TABLE u(id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("CREATE UNIQUE INDEX idx_new ON u(v COLLATE BINARY)")?;
    // Plain MVCC VACUUM is only legal after logical changes are checkpointed
    // into the B-tree image. The stale state being tested is the empty MVCC
    // index bucket that checkpoint GC leaves behind.
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_eq!(mvcc_log_file_size(&tmp_db), 0);

    conn.execute("VACUUM")?;
    conn.execute("INSERT INTO u VALUES (1, 'a')")?;
    conn.execute("INSERT INTO u VALUES (2, 'A')")?;

    let indexed: Vec<(String,)> = conn.exec_rows("SELECT v FROM u INDEXED BY idx_new ORDER BY v");
    let scanned: Vec<(String,)> = conn.exec_rows("SELECT v FROM u NOT INDEXED ORDER BY v");
    assert_eq!(scanned, vec![("A".to_string(),), ("a".to_string(),)]);
    assert_eq!(
        indexed, scanned,
        "post-VACUUM MVCC index state must use idx_new's BINARY collation"
    );
    assert_eq!(run_integrity_check(&conn), "ok");

    Ok(())
}

#[test]
fn test_mvcc_plain_vacuum_requires_checkpointed_image() -> anyhow::Result<()> {
    let tmp_db =
        TempDatabase::new_with_mvcc("test_mvcc_plain_vacuum_requires_checkpointed_image.db");
    let conn = tmp_db.connect_limbo();

    let expected_rows = populate_mvcc_vacuum_workload(&conn)?;

    let err = conn.execute("VACUUM").unwrap_err();
    assert_eq!(
        err.to_string(),
        "Transaction error: cannot VACUUM an MVCC database with uncheckpointed changes; run PRAGMA wal_checkpoint(TRUNCATE) first",
        "MVCC VACUUM must reject uncheckpointed logical changes"
    );

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_eq!(
        mvcc_log_file_size(&tmp_db),
        0,
        "MVCC log should be empty before plain VACUUM starts"
    );

    let before_hash = compute_plain_vacuum_dbhash(&tmp_db);
    conn.execute("VACUUM")?;
    assert_plain_vacuum_integrity_and_hash(&tmp_db, &conn, &before_hash);
    assert_plain_vacuum_folded_into_db_file(&tmp_db, &conn);
    assert_mvcc_vacuum_workload(&conn, &expected_rows)?;

    Ok(())
}

#[test]
fn test_mvcc_plain_vacuum_after_checkpoint_preserves_mvcc_state() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_with_mvcc(
        "test_mvcc_plain_vacuum_after_checkpoint_preserves_mvcc_state.db",
    );
    let conn = tmp_db.connect_limbo();
    let mut expected_rows = populate_mvcc_vacuum_workload(&conn)?;

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_eq!(
        mvcc_log_file_size(&tmp_db),
        0,
        "MVCC log should be empty before plain VACUUM starts"
    );

    let before_hash = compute_plain_vacuum_dbhash(&tmp_db);
    conn.execute("VACUUM")?;
    assert_plain_vacuum_integrity_and_hash(&tmp_db, &conn, &before_hash);
    assert_plain_vacuum_folded_into_db_file(&tmp_db, &conn);
    assert_mvcc_vacuum_workload(&conn, &expected_rows)?;

    conn.execute("INSERT INTO t VALUES(1000, 'after-vacuum', 'z')")?;
    conn.execute("UPDATE t SET tag = 'after-update' WHERE id = 1")?;
    conn.execute("DELETE FROM t WHERE id = 2")?;

    expected_rows.retain(|(id, _)| *id != 2);
    if let Some((_, tag)) = expected_rows.iter_mut().find(|(id, _)| *id == 1) {
        *tag = "after-update".to_string();
    }
    expected_rows.push((1000, "after-vacuum".to_string()));
    expected_rows.sort_by_key(|(id, _)| *id);

    assert_mvcc_vacuum_workload(&conn, &expected_rows)?;
    assert!(
        mvcc_log_file_size(&tmp_db) > 0,
        "post-VACUUM MVCC writes should use the logical log"
    );

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_eq!(
        mvcc_log_file_size(&tmp_db),
        0,
        "checkpoint after post-VACUUM writes should truncate the MVCC log"
    );

    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);

    let reopened = TempDatabase::new_with_existent(&path);
    let reopened_conn = reopened.connect_limbo();
    assert_mvcc_vacuum_workload(&reopened_conn, &expected_rows)?;

    Ok(())
}

#[test]
fn test_mvcc_plain_vacuum_active_tx_returns_busy() -> anyhow::Result<()> {
    for (scenario, db_name) in [
        (
            "read",
            "test_mvcc_plain_vacuum_active_read_tx_returns_busy.db",
        ),
        (
            "write",
            "test_mvcc_plain_vacuum_active_write_tx_returns_busy.db",
        ),
    ] {
        let tmp_db = TempDatabase::new_with_mvcc(db_name);
        let vacuum_conn = tmp_db.connect_limbo();
        let blocker = tmp_db.connect_limbo();

        let expected_rows = populate_mvcc_vacuum_workload(&vacuum_conn)?;
        vacuum_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;

        match scenario {
            "read" => {
                blocker.execute("BEGIN")?;
                let rows: Vec<(i64,)> = blocker.exec_rows("SELECT COUNT(*) FROM t");
                assert_eq!(rows, vec![(expected_rows.len() as i64,)]);
            }
            "write" => {
                blocker.execute("BEGIN IMMEDIATE")?;
                blocker.execute("INSERT INTO t VALUES(1000, 'uncommitted', 'pending')")?;
            }
            other => unreachable!("unexpected MVCC VACUUM blocker scenario: {other}"),
        }

        let result = vacuum_conn.execute("VACUUM");
        assert!(
            matches!(result, Err(LimboError::Busy)),
            "active MVCC {scenario} transaction should make VACUUM return Busy, got {result:?}"
        );

        blocker.execute("ROLLBACK")?;
        let before_hash = compute_plain_vacuum_dbhash(&tmp_db);
        vacuum_conn.execute("VACUUM")?;
        assert_plain_vacuum_integrity_and_hash(&tmp_db, &vacuum_conn, &before_hash);
        assert_plain_vacuum_folded_into_db_file(&tmp_db, &vacuum_conn);
        assert_mvcc_vacuum_workload(&vacuum_conn, &expected_rows)?;
    }

    Ok(())
}

fn open_queued_db(io: Arc<QueuedIo>, path: &str) -> anyhow::Result<Arc<Database>> {
    Ok(Database::open_file_with_flags(
        io,
        path,
        Default::default(),
        DatabaseOpts::new().with_vacuum(true),
        None,
    )?)
}

fn populate_queued_multibatch(conn: &Arc<Connection>) -> anyhow::Result<()> {
    conn.execute("PRAGMA page_size = 512")?;
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, payload TEXT)")?;
    conn.execute("CREATE INDEX idx_t_payload ON t(payload)")?;
    for i in 0..220 {
        conn.execute(format!("INSERT INTO t VALUES({i}, '{}')", "q".repeat(350)))?;
    }
    conn.execute("DELETE FROM t WHERE id % 5 = 0")?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    Ok(())
}

#[cfg_attr(feature = "checksum", ignore)]
#[test]
fn test_plain_vacuum_reset_during_checkpoint_io_cleans_up_checkpoint_and_vacuum_locks(
) -> anyhow::Result<()> {
    // Park plain VACUUM after copy-back has committed but while the final
    // TRUNCATE checkpoint is still yielding I/O. Resetting the statement from
    // that state exercises the fallback `AbortCheckpoint` cleanup path in
    // `vacuum_in_place_cleanup()`: it must tear down checkpoint state and
    // release the VACUUM gate so a fresh reader, checkpoint, and VACUUM can
    // all proceed on the same database afterward.
    let io = Arc::new(QueuedIo::new());
    let path = "queued-vacuum-reset-during-checkpoint.db";
    let source_wal_path = format!("{path}-wal");
    let db = open_queued_db(io.clone(), path)?;
    let conn = db.connect()?;

    populate_queued_multibatch(&conn)?;

    let mut stmt = conn.prepare("VACUUM")?;
    let mut saw_source_wal_batch_write = false;
    let mut reached_checkpoint_io = false;

    loop {
        match stmt.step()? {
            StepResult::Done => {
                anyhow::bail!("VACUUM finished before reaching checkpoint I/O")
            }
            StepResult::Row => continue,
            StepResult::Busy | StepResult::Interrupt => {
                anyhow::bail!("unexpected non-IO result while staging checkpoint cleanup test")
            }
            StepResult::IO => {
                while let Some(event) = io.step_one()? {
                    if event.path == source_wal_path && event.kind == QueuedIoOpKind::Pwritev {
                        saw_source_wal_batch_write = true;
                    }

                    if saw_source_wal_batch_write
                        && event.path == path
                        && matches!(
                            event.kind,
                            QueuedIoOpKind::Pwrite
                                | QueuedIoOpKind::Sync
                                | QueuedIoOpKind::Truncate
                        )
                    {
                        reached_checkpoint_io = true;
                        break;
                    }
                }

                if reached_checkpoint_io {
                    break;
                }
            }
        }
    }

    stmt.reset()?;

    let reader = db.connect()?;
    assert_eq!(scalar_i64(&reader, "SELECT COUNT(*) FROM t"), 176);
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    let before_schema = normalized_schema_snapshot(&conn);
    conn.execute("VACUUM")?;
    assert_eq!(run_integrity_check(&conn), "ok");
    assert_eq!(scalar_i64(&conn, "SELECT COUNT(*) FROM t"), 176);
    assert_eq!(
        normalized_schema_snapshot(&conn),
        before_schema,
        "plain VACUUM should preserve normalized sqlite_schema entries"
    );

    Ok(())
}
