use crate::common::{do_flush, limbo_exec_rows, sqlite_exec_rows, ExecRows, TempDatabase};
use rusqlite::params;
use rusqlite::Connection as RusqliteConnection;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use turso_core::{Database, DatabaseOpts, OpenFlags};

const PAGE_SIZE_OFFSET: u64 = 16;
const RESERVED_SPACE_OFFSET: u64 = 20;

fn attach_enabled_db(opts: DatabaseOpts) -> TempDatabase {
    TempDatabase::builder()
        .with_opts(opts.with_encryption(true).with_attach(true))
        .build()
}

fn read_header_page_size(path: &Path) -> anyhow::Result<u16> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(PAGE_SIZE_OFFSET))?;
    let mut buf = [0u8; 2];
    file.read_exact(&mut buf)?;
    Ok(u16::from_be_bytes(buf))
}

fn read_header_reserved_space(path: &Path) -> anyhow::Result<u8> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(RESERVED_SPACE_OFFSET))?;
    let mut buf = [0u8; 1];
    file.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn checkpoint_attached_database(
    conn: &std::sync::Arc<turso_core::Connection>,
    db: &TempDatabase,
    alias: &str,
) -> anyhow::Result<()> {
    do_flush(conn, db)?;
    let pragma = format!("PRAGMA {alias}.wal_checkpoint(TRUNCATE)");
    let _ = limbo_exec_rows(conn, &pragma);
    do_flush(conn, db)?;
    Ok(())
}

#[turso_macros::test]
fn test_attached_schema_refreshes_after_other_connection_create(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let aux_path = tmp_db
        .path
        .parent()
        .unwrap()
        .join("attach_schema_refresh.db")
        .to_string_lossy()
        .to_string();

    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    conn1.execute(format!("ATTACH '{aux_path}' AS aux"))?;
    conn2.execute(format!("ATTACH '{aux_path}' AS aux"))?;

    conn1.execute("CREATE TABLE aux.bootstrap (x INTEGER)")?;

    // Populate conn2's attached-schema cache before conn1 adds another table.
    let _ = limbo_exec_rows(&conn2, "SELECT * FROM aux.bootstrap");

    conn1.execute("CREATE TABLE aux.created_later (y INTEGER)")?;
    conn1.execute("INSERT INTO aux.created_later VALUES (1)")?;

    let rows = limbo_exec_rows(&conn2, "SELECT y FROM aux.created_later");
    assert_eq!(
        rows.len(),
        1,
        "conn2 should see the newly created attached table"
    );
    assert_eq!(rows[0], vec![rusqlite::types::Value::Integer(1)]);

    Ok(())
}

#[turso_macros::test]
fn test_fresh_attach_inherits_main_page_size(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    let db = attach_enabled_db(DatabaseOpts::new());
    let conn = db.connect_limbo();
    conn.execute("PRAGMA page_size = 8192")?;

    let aux_path = db.path.with_extension("attach_page_size.db");
    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn.execute("CREATE TABLE aux.t(x INTEGER)")?;
    checkpoint_attached_database(&conn, &db, "aux")?;

    assert_eq!(read_header_page_size(&aux_path)?, 8192);
    Ok(())
}

#[turso_macros::test]
fn test_fresh_attach_inherits_main_reserved_space(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    const RESERVED_BYTES: u8 = 48;

    let db = attach_enabled_db(DatabaseOpts::new());
    let conn = db.connect_limbo();
    conn.set_reserved_bytes(RESERVED_BYTES)?;

    let aux_path = db.path.with_extension("attach_reserved_space.db");
    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn.execute("CREATE TABLE aux.t(x INTEGER)")?;
    checkpoint_attached_database(&conn, &db, "aux")?;

    assert_eq!(read_header_reserved_space(&aux_path)?, RESERVED_BYTES);
    Ok(())
}

#[turso_macros::test]
fn test_fresh_attach_from_legacy_main_respects_attached_reserved_space_minimum(
    _tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let main_path = temp_dir.path().join("legacy_main.db");
    let aux_path = temp_dir.path().join("legacy_aux.db");

    {
        let sqlite = RusqliteConnection::open(&main_path)?;
        sqlite.execute("CREATE TABLE main_t(x INTEGER)", ())?;
    }

    let db = TempDatabase::builder()
        .with_db_path(&main_path)
        .with_opts(DatabaseOpts::new().with_encryption(true).with_attach(true))
        .build();
    let conn = db.connect_limbo();

    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn.execute("CREATE TABLE aux.t(x INTEGER)")?;
    checkpoint_attached_database(&conn, &db, "aux")?;

    #[cfg(feature = "checksum")]
    assert_eq!(read_header_reserved_space(&aux_path)?, 8);
    #[cfg(not(feature = "checksum"))]
    assert_eq!(read_header_reserved_space(&aux_path)?, 0);

    Ok(())
}

#[turso_macros::test]
fn test_fresh_attach_inherits_mvcc_before_first_write(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    let db = attach_enabled_db(DatabaseOpts::new());
    let conn = db.connect_limbo();
    conn.pragma_update("journal_mode", "'mvcc'")?;

    let aux_path = db.path.with_extension("attach_mvcc.db");
    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn.execute("CREATE TABLE aux.t(x INTEGER)")?;
    conn.execute("INSERT INTO aux.t VALUES(1)")?;
    checkpoint_attached_database(&conn, &db, "aux")?;

    let aux_db = Database::open_file_with_flags(
        db.io.clone(),
        aux_path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    let aux_conn = aux_db.connect()?;
    assert!(aux_conn.mvcc_enabled());

    Ok(())
}

#[turso_macros::test]
fn test_attach_rejects_initialized_page_size_mismatch(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    let db = attach_enabled_db(DatabaseOpts::new());
    let conn = db.connect_limbo();
    conn.execute("PRAGMA page_size = 8192")?;
    conn.execute("CREATE TABLE main_t(x INTEGER)")?;

    let aux_path = db.path.with_extension("attach_page_size_mismatch.db");
    let aux_db = Database::open_file_with_flags(
        db.io.clone(),
        aux_path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
    )?;
    let aux_conn = aux_db.connect()?;
    aux_conn.execute("CREATE TABLE t(x INTEGER)")?;

    let err = conn
        .execute(format!("ATTACH '{}' AS aux", aux_path.display()))
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        format!(
            "Invalid argument supplied: cannot attach database 'aux': page size mismatch (main={:?}, attached={:?})",
            turso_core::storage::sqlite3_ondisk::PageSize::new(8192).unwrap(),
            turso_core::storage::sqlite3_ondisk::PageSize::new(4096).unwrap(),
        )
    );

    Ok(())
}

#[turso_macros::test]
fn test_attach_rejects_fresh_read_only_database(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    let db = attach_enabled_db(DatabaseOpts::new());
    let conn = db.connect_limbo();

    let aux_path = db.path.with_extension("attach_read_only.db");
    File::create(&aux_path)?;

    let err = conn
        .execute(format!(
            "ATTACH 'file:{}?mode=ro' AS aux",
            aux_path.display()
        ))
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "Invalid argument supplied: cannot attach database 'aux': fresh read-only databases cannot be initialized during attach"
    );

    Ok(())
}

#[turso_macros::test]
fn test_attach_rejects_zero_byte_database_with_existing_wal(
    _tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let aux_path = temp_dir.path().join("wal_backed_aux.db");
    let wal_path = aux_path.with_extension("db-wal");

    let sqlite = RusqliteConnection::open(&aux_path)?;
    sqlite.pragma_update(None, "journal_mode", "WAL")?;
    sqlite.execute("CREATE TABLE t(x INTEGER)", ())?;
    sqlite.execute("INSERT INTO t VALUES (1)", ())?;

    assert!(std::fs::metadata(&wal_path)?.len() > 0);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&aux_path)?
        .set_len(0)?;
    assert_eq!(std::fs::metadata(&aux_path)?.len(), 0);

    let db = attach_enabled_db(DatabaseOpts::new());
    let conn = db.connect_limbo();

    for attach_sql in [
        format!("ATTACH '{}' AS aux", aux_path.display()),
        format!("ATTACH 'file:{}?mode=ro' AS aux", aux_path.display()),
    ] {
        let err = conn.execute(attach_sql).unwrap_err().to_string();
        assert_eq!(
            err,
            "Invalid argument supplied: cannot attach database 'aux': main database file is uninitialized but WAL state exists"
        );
    }

    drop(sqlite);
    Ok(())
}

#[turso_macros::test]
fn test_fresh_mvcc_attach_rejects_custom_durable_storage_without_attached_backend(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let db_path = tmp_db.path.with_extension("main_custom_durable_storage.db");
    let log_path = db_path.with_extension("db-log");
    let aux_path = tmp_db.path.with_extension("aux_custom_durable_storage.db");

    let file = tmp_db
        .io
        .open_file(log_path.to_str().unwrap(), OpenFlags::default(), false)?;
    let durable_storage = Arc::new(turso_core::mvcc::persistent_storage::Storage::new(
        file,
        tmp_db.io.clone(),
        None,
    ));

    let db = Database::open_file_with_flags_and_durable_storage(
        tmp_db.io.clone(),
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        DatabaseOpts::new().with_attach(true),
        None,
        Some(durable_storage),
    )?;
    let conn = db.connect()?;
    conn.pragma_update("journal_mode", "'mvcc'")?;

    let err = conn
        .execute(format!("ATTACH '{}' AS aux", aux_path.display()))
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        "Invalid argument supplied: cannot attach database 'aux': fresh MVCC attach does not support inheriting custom durable storage"
    );

    Ok(())
}

#[turso_macros::test]
fn test_attach_inherits_generated_columns_flag_on_reattach(
    _tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let db = attach_enabled_db(DatabaseOpts::new().with_generated_columns(true));
    let conn = db.connect_limbo();

    let aux_path = db.path.with_extension("attach_generated_columns.db");
    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn.execute("CREATE TABLE aux.t(x INTEGER, y INTEGER GENERATED ALWAYS AS (x + 1) VIRTUAL)")?;
    conn.execute("INSERT INTO aux.t(x) VALUES (41)")?;
    conn.execute("DETACH aux")?;

    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    let rows: Vec<(i64, i64)> = conn.exec_rows("SELECT x, y FROM aux.t");
    assert_eq!(rows, vec![(41, 42)]);

    Ok(())
}

#[turso_macros::test]
fn test_attach_inherits_index_method_flag_on_reattach(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    let db = attach_enabled_db(DatabaseOpts::new().with_index_method(true));
    let conn = db.connect_limbo();

    let aux_path = db.path.with_extension("attach_index_method.db");
    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    conn.execute("CREATE TABLE aux.docs(id INTEGER PRIMARY KEY, content TEXT)")?;
    conn.execute("CREATE INDEX aux.fts_docs ON docs USING fts (content)")?;
    conn.execute("INSERT INTO aux.docs(content) VALUES ('hello world')")?;
    conn.execute("DETACH aux")?;

    conn.execute(format!("ATTACH '{}' AS aux", aux_path.display()))?;
    let rows: Vec<(i64,)> =
        conn.exec_rows("SELECT id FROM aux.docs WHERE fts_match(content, 'hello')");
    assert_eq!(rows, vec![(1,)]);

    Ok(())
}

#[test]
fn test_attach_create_stores_canonical_schema_sql() -> anyhow::Result<()> {
    let aux_db = TempDatabase::builder().build();
    let aux_conn = aux_db.connect_limbo();
    aux_conn.execute("CREATE TABLE t(name TEXT)")?;
    do_flush(&aux_conn, &aux_db)?;

    let db = attach_enabled_db(DatabaseOpts::new());
    let conn = db.connect_limbo();

    conn.execute(format!("ATTACH '{}' AS aux", aux_db.path.display()))?;
    conn.execute("CREATE TABLE aux.t2(col TEXT)")?;
    conn.execute("CREATE INDEX aux.idx_t_name ON t(name)")?;

    let rows: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM aux.sqlite_schema WHERE type = 'table' AND name = 't2'");
    assert_eq!(rows, vec![("CREATE TABLE t2 (col TEXT)".to_string(),)]);

    let rows: Vec<(String,)> = conn.exec_rows(
        "SELECT sql FROM aux.sqlite_schema WHERE type = 'index' AND name = 'idx_t_name'",
    );
    assert_eq!(
        rows,
        vec![("CREATE INDEX idx_t_name ON t (name)".to_string(),)]
    );

    Ok(())
}

/// this test is very much same like `test_attach_create_stores_canonical_schema_sql` except
/// we don't attach any db, rather access the main db as if it was attached
#[test]
fn test_attach_create_stores_canonical_schema_sql_on_main() -> anyhow::Result<()> {
    let db = TempDatabase::builder().build();
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(name TEXT)")?;
    do_flush(&conn, &db)?;

    conn.execute("CREATE TABLE main.t2(col TEXT)")?;
    conn.execute("CREATE INDEX main.idx_t_name ON t(name)")?;

    let rows: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 't2'");
    assert_eq!(rows, vec![("CREATE TABLE t2 (col TEXT)".to_string(),)]);

    let rows: Vec<(String,)> = conn
        .exec_rows("SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = 'idx_t_name'");
    assert_eq!(
        rows,
        vec![("CREATE INDEX idx_t_name ON t (name)".to_string(),)]
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Transaction-opcode emission tests
//
// SQLite emits `Transaction` for every open database (main, temp, each
// attached) on `BEGIN IMMEDIATE` and `BEGIN EXCLUSIVE`. Verify that turso
// emits the same number, covering the db-id set we expect.
// ---------------------------------------------------------------------------

/// Extract the set of `p1` (db index) values from `Transaction` opcodes
/// in an EXPLAIN result set. Column 1 = opcode name, column 2 = p1.
fn transaction_db_ids_from_explain(rows: &[Vec<rusqlite::types::Value>]) -> Vec<i64> {
    rows.iter()
        .filter_map(|row| {
            let opcode = match &row[1] {
                rusqlite::types::Value::Text(s) => s.as_str(),
                _ => return None,
            };
            if opcode != "Transaction" {
                return None;
            }
            match &row[2] {
                rusqlite::types::Value::Integer(db_id) => Some(*db_id),
                _ => None,
            }
        })
        .collect()
}

#[turso_macros::test]
fn test_begin_immediate_transaction_count_no_attached(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    let db = attach_enabled_db(DatabaseOpts::new());
    let turso = db.connect_limbo();
    let sqlite = RusqliteConnection::open_in_memory()?;

    let turso_ids =
        transaction_db_ids_from_explain(&limbo_exec_rows(&turso, "EXPLAIN BEGIN IMMEDIATE"));
    let sqlite_ids =
        transaction_db_ids_from_explain(&sqlite_exec_rows(&sqlite, "EXPLAIN BEGIN IMMEDIATE"));

    assert_eq!(
        turso_ids.len(),
        sqlite_ids.len(),
        "Transaction opcode count mismatch (no attached)\nturso db_ids: {turso_ids:?}\nsqlite db_ids: {sqlite_ids:?}"
    );
    assert!(
        turso_ids.contains(&0),
        "turso must emit Transaction for main (db=0)"
    );
    Ok(())
}

#[turso_macros::test]
fn test_begin_immediate_transaction_count_one_attached(
    _tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let db = attach_enabled_db(DatabaseOpts::new());
    let turso = db.connect_limbo();
    let sqlite = RusqliteConnection::open_in_memory()?;

    turso.execute("ATTACH ':memory:' AS aux")?;
    sqlite.execute("ATTACH ':memory:' AS aux", params![])?;

    turso.execute("CREATE TABLE aux.t(x INTEGER)")?;
    sqlite.execute("CREATE TABLE aux.t(x INTEGER)", params![])?;

    let turso_ids =
        transaction_db_ids_from_explain(&limbo_exec_rows(&turso, "EXPLAIN BEGIN IMMEDIATE"));
    let sqlite_ids =
        transaction_db_ids_from_explain(&sqlite_exec_rows(&sqlite, "EXPLAIN BEGIN IMMEDIATE"));

    assert_eq!(
        turso_ids.len(),
        sqlite_ids.len(),
        "Transaction opcode count mismatch (one attached)\nturso db_ids: {turso_ids:?}\nsqlite db_ids: {sqlite_ids:?}"
    );
    assert!(turso_ids.contains(&0), "must emit Transaction for main");
    // Attached db gets index 2 (slot 1 is always temp).
    assert!(
        turso_ids.iter().any(|&id| id >= 2),
        "must emit Transaction for attached db"
    );
    Ok(())
}

#[turso_macros::test]
fn test_begin_immediate_transaction_count_two_attached(
    _tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let db = attach_enabled_db(DatabaseOpts::new());
    let turso = db.connect_limbo();
    let sqlite = RusqliteConnection::open_in_memory()?;

    for alias in ["aux1", "aux2"] {
        turso.execute(format!("ATTACH ':memory:' AS {alias}"))?;
        sqlite.execute(&format!("ATTACH ':memory:' AS {alias}"), params![])?;
        turso.execute(format!("CREATE TABLE {alias}.t(x INTEGER)"))?;
        sqlite.execute(&format!("CREATE TABLE {alias}.t(x INTEGER)"), params![])?;
    }

    let turso_ids =
        transaction_db_ids_from_explain(&limbo_exec_rows(&turso, "EXPLAIN BEGIN IMMEDIATE"));
    let sqlite_ids =
        transaction_db_ids_from_explain(&sqlite_exec_rows(&sqlite, "EXPLAIN BEGIN IMMEDIATE"));

    assert_eq!(
        turso_ids.len(),
        sqlite_ids.len(),
        "Transaction opcode count mismatch (two attached)\nturso db_ids: {turso_ids:?}\nsqlite db_ids: {sqlite_ids:?}"
    );
    assert!(turso_ids.contains(&0), "must emit Transaction for main");
    let attached_count = turso_ids.iter().filter(|&&id| id >= 2).count();
    assert_eq!(
        attached_count, 2,
        "must emit Transaction for both attached dbs"
    );
    Ok(())
}

#[turso_macros::test]
fn test_begin_immediate_transaction_count_with_temp(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    let db = attach_enabled_db(DatabaseOpts::new());
    let turso = db.connect_limbo();
    let sqlite = RusqliteConnection::open_in_memory()?;

    turso.execute("CREATE TEMP TABLE tt(v INTEGER)")?;
    sqlite.execute("CREATE TEMP TABLE tt(v INTEGER)", params![])?;

    let turso_ids =
        transaction_db_ids_from_explain(&limbo_exec_rows(&turso, "EXPLAIN BEGIN IMMEDIATE"));
    let sqlite_ids =
        transaction_db_ids_from_explain(&sqlite_exec_rows(&sqlite, "EXPLAIN BEGIN IMMEDIATE"));

    assert_eq!(
        turso_ids.len(),
        sqlite_ids.len(),
        "Transaction opcode count mismatch (with temp)\nturso db_ids: {turso_ids:?}\nsqlite db_ids: {sqlite_ids:?}"
    );
    assert!(turso_ids.contains(&0), "must emit Transaction for main");
    assert!(
        turso_ids.contains(&1),
        "must emit Transaction for temp (db=1)"
    );
    Ok(())
}

#[turso_macros::test]
fn test_begin_deferred_emits_no_transaction_opcodes(_tmp_db: TempDatabase) -> anyhow::Result<()> {
    let db = attach_enabled_db(DatabaseOpts::new());
    let turso = db.connect_limbo();
    let sqlite = RusqliteConnection::open_in_memory()?;

    turso.execute("ATTACH ':memory:' AS aux")?;
    sqlite.execute("ATTACH ':memory:' AS aux", params![])?;
    turso.execute("CREATE TABLE aux.t(x INTEGER)")?;
    sqlite.execute("CREATE TABLE aux.t(x INTEGER)", params![])?;

    let turso_ids = transaction_db_ids_from_explain(&limbo_exec_rows(&turso, "EXPLAIN BEGIN"));
    let sqlite_ids = transaction_db_ids_from_explain(&sqlite_exec_rows(&sqlite, "EXPLAIN BEGIN"));

    assert!(
        turso_ids.is_empty(),
        "BEGIN (deferred) should emit no Transaction opcodes, got: {turso_ids:?}"
    );
    assert!(
        sqlite_ids.is_empty(),
        "SQLite BEGIN (deferred) should emit no Transaction opcodes, got: {sqlite_ids:?}"
    );
    Ok(())
}
