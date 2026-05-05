use crate::common::{do_flush, limbo_exec_rows, TempDatabase};
use rand::{rng, RngCore};
use rusqlite::types::Value as SqlValue;
use std::path::Path;

#[derive(Debug)]
struct HeaderSize {
    page_size: u64,
    page_count: u64,
    expected_bytes: u64,
    actual_bytes: u64,
}

fn single_i64(rows: Vec<Vec<SqlValue>>, query: &str) -> i64 {
    assert_eq!(rows.len(), 1, "{query} should return one row");
    assert_eq!(rows[0].len(), 1, "{query} should return one column");
    match rows[0][0] {
        SqlValue::Integer(value) => value,
        ref value => panic!("{query} should return an integer, got {value:?}"),
    }
}

fn read_header_size(db_path: &Path) -> HeaderSize {
    let bytes = std::fs::read(db_path).expect("read database file");
    assert!(
        bytes.len() >= 100,
        "database file should contain a SQLite header, got {} bytes",
        bytes.len()
    );
    assert_eq!(&bytes[..16], b"SQLite format 3\0");

    let raw_page_size = u16::from_be_bytes([bytes[16], bytes[17]]);
    let page_size = if raw_page_size == 1 {
        65_536
    } else {
        u64::from(raw_page_size)
    };
    let page_count = u32::from_be_bytes([bytes[28], bytes[29], bytes[30], bytes[31]]) as u64;
    let expected_bytes = page_size
        .checked_mul(page_count)
        .expect("header byte size should not overflow");
    let actual_bytes = std::fs::metadata(db_path)
        .expect("stat database file")
        .len();

    HeaderSize {
        page_size,
        page_count,
        expected_bytes,
        actual_bytes,
    }
}

fn sqlite_integrity_check(db_path: &Path) -> rusqlite::Result<Vec<String>> {
    let conn =
        rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut stmt = conn.prepare("PRAGMA integrity_check")?;
    let rows = stmt.query_map([], |row| row.get(0))?.collect();
    rows
}

fn sqlite_count_rows(db_path: &Path) -> rusqlite::Result<i64> {
    let conn =
        rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.query_row("SELECT count(*) FROM t", [], |row| row.get(0))
}

#[test]
fn stale_pending_page_size_checkpoint_truncates_data_pages_probe() {
    let _ = env_logger::try_init();
    let db_name = format!("stale-page-size-checkpoint-{}.db", rng().next_u32());
    let tmp_db = TempDatabase::new(&db_name);
    let db_path = tmp_db.path.clone();

    let conn_1024 = tmp_db.connect_limbo();
    let conn_4096 = tmp_db.connect_limbo();

    conn_1024.execute("PRAGMA page_size=1024").unwrap();
    conn_4096.execute("CREATE TABLE t(x)").unwrap();

    for i in 0..80 {
        conn_4096
            .execute(format!(
                "INSERT INTO t VALUES(hex(randomblob(3000)) || '{i:04}')"
            ))
            .unwrap();
    }
    do_flush(&conn_4096, &tmp_db).unwrap();

    let page_size = single_i64(limbo_exec_rows(&conn_4096, "PRAGMA page_size"), "page_size");
    let page_count = single_i64(
        limbo_exec_rows(&conn_4096, "PRAGMA page_count"),
        "page_count",
    );
    let count = single_i64(
        limbo_exec_rows(&conn_4096, "SELECT count(*) FROM t"),
        "count",
    );
    assert_eq!(page_size, 4096);
    assert_eq!(count, 80);
    assert!(
        page_count > 1,
        "test must grow beyond page 1, got page_count={page_count}"
    );

    let checkpoint_result = conn_1024.execute("PRAGMA wal_checkpoint(TRUNCATE)");
    let _ = do_flush(&conn_1024, &tmp_db);
    tmp_db.io.drain().expect("drain test IO");

    let header = read_header_size(&db_path);
    assert_eq!(
        header.page_size, 4096,
        "writer connection should materialize a 4096-byte-page database"
    );
    assert_eq!(
        header.page_count, page_count as u64,
        "header page count should match Turso page_count before checkpoint"
    );
    assert_eq!(
        header.actual_bytes, header.expected_bytes,
        "checkpoint_result={checkpoint_result:?}; database file is shorter than its SQLite header declares: {header:?}"
    );

    let integrity = sqlite_integrity_check(&db_path).expect("SQLite integrity_check should run");
    assert_eq!(integrity, vec!["ok".to_string()]);
    assert_eq!(sqlite_count_rows(&db_path).unwrap(), 80);
}
