use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;

fn count(conn: &std::sync::Arc<turso_core::Connection>, sql: &str) -> i64 {
    match limbo_exec_rows(conn, sql).as_slice() {
        [row] => match row.as_slice() {
            [Value::Integer(n)] => *n,
            other => panic!("expected integer count from `{sql}`, got {other:?}"),
        },
        other => panic!("expected one row from `{sql}`, got {other:?}"),
    }
}

#[test]
fn json_each_in_list_is_not_dropped() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    assert_eq!(
        count(
            &conn,
            "SELECT count(*) FROM json_each WHERE json = '[1,2,3]'"
        ),
        3
    );
    assert_eq!(
        count(
            &conn,
            "SELECT count(*) FROM json_each WHERE json IN ('[1,2,3]')"
        ),
        3
    );
    assert_eq!(
        count(
            &conn,
            "SELECT count(*) FROM json_each WHERE json IN ('[1,2,3]', '[4,5]')"
        ),
        5
    );
    assert_eq!(
        count(
            &conn,
            "SELECT count(*) FROM json_each WHERE json IN (SELECT '[1,2,3]')"
        ),
        3
    );
    assert_eq!(
        count(
            &conn,
            "SELECT count(*) FROM json_tree WHERE json IN ('[1,2,3]')"
        ),
        4
    );
}

#[test]
fn generate_series_in_stop_is_bounded() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    assert_eq!(
        count(
            &conn,
            "SELECT count(*) FROM generate_series WHERE start = 1 AND stop = 5"
        ),
        5
    );
    assert_eq!(
        count(
            &conn,
            "SELECT count(*) FROM generate_series WHERE start = 1 AND stop IN (5)"
        ),
        5
    );
}
