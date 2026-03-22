use crate::common::{ExecRows, TempDatabase};
use std::collections::HashSet;
use std::path::PathBuf;

/// Snapshot all directory entries in the system temp dir.
fn snapshot_temp_dir() -> HashSet<PathBuf> {
    let temp_dir = std::env::temp_dir();
    std::fs::read_dir(&temp_dir)
        .expect("failed to read system temp dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect()
}

/// Find new directories that contain a `tursodb_temp_file` — these are leaked TempFiles.
fn find_leaked_temp_files(before: &HashSet<PathBuf>, after: &HashSet<PathBuf>) -> Vec<PathBuf> {
    after
        .difference(before)
        .filter(|p| p.is_dir() && p.join("tursodb_temp_file").exists())
        .cloned()
        .collect()
}

#[test]
fn test_ephemeral_temp_files_cleaned_up() {
    let before = snapshot_temp_dir();

    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE t_eph(x INTEGER)").unwrap();
    conn.execute("INSERT INTO t_eph VALUES(3),(1),(2),(1),(3)")
        .unwrap();

    let rows: Vec<(i64,)> = conn.exec_rows("SELECT DISTINCT x FROM t_eph ORDER BY x");
    assert_eq!(rows, vec![(1,), (2,), (3,)]);

    // Drop connection and database so all ProgramState instances are dropped,
    // which should clean up any TempFile entries.
    drop(conn);
    drop(db);

    let after = snapshot_temp_dir();
    let leaked = find_leaked_temp_files(&before, &after);
    assert!(leaked.is_empty(), "Ephemeral temp files leaked: {leaked:?}");
}
