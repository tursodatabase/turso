use crate::common::{ExecRows, TempDatabase};

#[turso_macros::test]
fn drop_table_frees_pages_of_backing_btree_index(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE example (name TEXT)").unwrap();
    conn.execute("CREATE INDEX example_idx ON example USING backing_btree (name)")
        .unwrap();
    conn.execute("DROP TABLE example").unwrap();

    let rows: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
    assert_eq!(rows, vec![("ok".to_string(),)]);
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn drop_table_frees_pages_of_fts_index(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE example (name TEXT)").unwrap();
    conn.execute("CREATE INDEX example_fts ON example USING fts (name)")
        .unwrap();
    conn.execute("DROP TABLE example").unwrap();

    let rows: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
    assert_eq!(rows, vec![("ok".to_string(),)]);
}
