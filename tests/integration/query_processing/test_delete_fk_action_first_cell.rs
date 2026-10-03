use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;

fn remaining_ids(tmp_db: &TempDatabase, statements: &[&str]) -> Vec<Vec<Value>> {
    let conn = tmp_db.connect_limbo();
    for sql in statements {
        conn.execute(sql).unwrap();
    }
    limbo_exec_rows(&conn, "SELECT group_concat(id) FROM t")
}

#[turso_macros::test]
fn delete_with_self_referencing_set_null_continues_after_first_cell_of_page(tmp_db: TempDatabase) {
    let rows = remaining_ids(
        &tmp_db,
        &[
            "PRAGMA foreign_keys=ON",
            "CREATE TABLE t(id INTEGER PRIMARY KEY, parent INTEGER REFERENCES t(id) ON DELETE SET NULL, pad TEXT)",
            "WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM s WHERE x < 12) \
             INSERT INTO t SELECT x, CASE WHEN x > 1 THEN x - 1 END, printf('%.900c', 'x') FROM s",
            "DELETE FROM t WHERE id > 4",
        ],
    );
    assert_eq!(rows, vec![vec![Value::Text("1,2,3,4".to_string())]]);
}

#[turso_macros::test]
fn delete_with_fk_update_write_back_continues_after_first_cell_of_page(tmp_db: TempDatabase) {
    let rows = remaining_ids(
        &tmp_db,
        &[
            "PRAGMA foreign_keys=ON",
            "CREATE TABLE t(id INTEGER PRIMARY KEY, cref INTEGER REFERENCES c(x) ON UPDATE SET NULL, pad TEXT)",
            "CREATE TABLE c(id INTEGER PRIMARY KEY, x INTEGER UNIQUE REFERENCES t(id) ON DELETE SET NULL)",
            "WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 12) \
             INSERT INTO t SELECT n, NULL, printf('%.900c', 'x') FROM s",
            "INSERT INTO c(x) VALUES (5)",
            "INSERT INTO t VALUES (1005, 5, NULL)",
            "DELETE FROM t WHERE id BETWEEN 5 AND 12",
        ],
    );
    assert_eq!(rows, vec![vec![Value::Text("1,2,3,4,1005".to_string())]]);
}
