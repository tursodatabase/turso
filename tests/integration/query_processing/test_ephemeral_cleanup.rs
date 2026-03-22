use crate::common::TempDatabase;
use std::sync::Arc;
use turso_core::Connection;

/// Count files matching the tursodb temp file pattern in the system temp dir.
fn count_temp_files() -> usize {
    let temp_dir = std::env::temp_dir();
    std::fs::read_dir(&temp_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            // TempFile uses tempfile::tempdir() which creates dirs like ".tmp*"
            // with a "tursodb_temp_file" inside. We look for the tempdir entries.
            name.starts_with("tursodb-ephemeral-")
        })
        .count()
}

/// Run a query that triggers OpenEphemeral (DISTINCT + ORDER BY) and step through all rows.
fn run_ephemeral_query(conn: &Arc<Connection>) {
    conn.execute("CREATE TABLE IF NOT EXISTS t_eph(x INTEGER)").unwrap();
    conn.execute("INSERT INTO t_eph VALUES(3),(1),(2),(1),(3)").unwrap();

    let mut stmt = conn.prepare("SELECT DISTINCT x FROM t_eph ORDER BY x").unwrap();
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        rows.push(row.get::<i64>(0).unwrap());
        Ok(())
    })
    .unwrap();
    assert_eq!(rows, vec![1, 2, 3]);
}

#[test]
fn test_ephemeral_temp_files_cleaned_up() {
    let before = count_temp_files();

    let db = TempDatabase::new("ephemeral_cleanup_test.db");
    let conn = db.connect_limbo();

    run_ephemeral_query(&conn);

    // Drop connection and database so all ProgramState instances are dropped,
    // which should clean up any TempFile entries.
    drop(conn);
    drop(db);

    let after = count_temp_files();
    assert_eq!(
        before, after,
        "Ephemeral temp files leaked: before={before}, after={after}"
    );
}
