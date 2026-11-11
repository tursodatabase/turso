use crate::common::{ExecRows, TempDatabase};

#[turso_macros::test()]
fn test_nondet_counter_is_per_connection(tmp_db: TempDatabase) {
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    let first: Vec<(i64,)> = conn1.exec_rows("SELECT test_nondet_counter()");
    let other: Vec<(i64,)> = conn2.exec_rows("SELECT test_nondet_counter()");
    let second: Vec<(i64,)> = conn1.exec_rows("SELECT test_nondet_counter()");

    assert_eq!(first, vec![(0,)]);
    assert_eq!(other, vec![(0,)]);
    assert_eq!(second, vec![(1,)]);
}
