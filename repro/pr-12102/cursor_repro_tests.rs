
// ---- Checks of the four cursor claims in the description of PR 12102. ----
// build.sh appends this file to core/mvcc/database/tests.rs at the PR head
// a4c5e653. The tests use the helpers of that file: mixed_rows_fixture,
// mixed_rows_cursor and rowids_from_current_row.
// Fixture (from the PR): B-tree rows 10, 30, 50 and MVCC-only rows 20, 40, with v = id.
// Each test prints one line that starts with REPRO and shows the actual and the
// expected rows.

/// Claim 1: an exact lookup, then movement, must return the neighbor rows of both sources.
#[rstest::rstest]
fn repro_claim1_exact_seek_then_step(
    #[values(false, true)] index: bool,
    #[values(false, true)] backwards: bool,
    #[values(10, 20, 30, 40, 50)] target: i64,
) {
    use crate::types::{SeekKey, SeekOp};
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = mixed_rows_fixture(&db);
    let mut cursor = mixed_rows_cursor(&conn, index);
    let record =
        ImmutableRecord::from_values(&[Value::from_i64(target), Value::from_i64(target)], 2)
            .unwrap();
    let key = if index {
        SeekKey::IndexKey(record.as_record_ref())
    } else {
        SeekKey::TableRowId(target)
    };
    let op = if backwards {
        SeekOp::LE { eq_only: true }
    } else {
        SeekOp::GE { eq_only: true }
    };
    let result = conn.db.io.block(|| cursor.seek(key.clone(), op)).unwrap();
    assert!(matches!(result, SeekResult::Found));
    let actual = rowids_from_current_row(&conn, &mut cursor, backwards);
    drop(cursor);
    conn.execute("ROLLBACK").unwrap();
    let mut expected: Vec<i64> = [10, 20, 30, 40, 50]
        .into_iter()
        .filter(|id| if backwards { *id <= target } else { *id >= target })
        .collect();
    if backwards {
        expected.reverse();
    }
    eprintln!(
        "REPRO1 index={index} backwards={backwards} target={target} actual={actual:?} expected={expected:?}"
    );
    assert_eq!(actual, expected);
}

/// Claim 2: a successful exists(), then next(), must continue over both sources.
#[rstest::rstest]
fn repro_claim2_exists_then_next(#[values(10, 20, 30, 40, 50)] target: i64) {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = mixed_rows_fixture(&db);
    let mut cursor = mixed_rows_cursor(&conn, false);
    assert!(conn
        .db
        .io
        .block(|| cursor.exists(&Value::from_i64(target)))
        .unwrap());
    let actual = rowids_from_current_row(&conn, &mut cursor, false);
    drop(cursor);
    conn.execute("ROLLBACK").unwrap();
    let expected: Vec<i64> = [10, 20, 30, 40, 50]
        .into_iter()
        .filter(|id| *id >= target)
        .collect();
    eprintln!("REPRO2 target={target} actual={actual:?} expected={expected:?}");
    assert_eq!(actual, expected);
}

/// Claim 3: after an insert at another key, next() must continue from the inserted key.
#[rstest::rstest]
fn repro_claim3_insert_then_next(#[values(false, true)] index: bool) {
    use crate::types::{SeekKey, SeekOp};
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = mixed_rows_fixture(&db);
    let mut cursor = mixed_rows_cursor(&conn, index);
    let record =
        ImmutableRecord::from_values(&[Value::from_i64(10), Value::from_i64(10)], 2).unwrap();
    let key = if index {
        SeekKey::IndexKey(record.as_record_ref())
    } else {
        SeekKey::TableRowId(10)
    };
    let inserted =
        ImmutableRecord::from_values(&[Value::from_i64(35), Value::from_i64(35)], 2).unwrap();
    let inserted_key = if index {
        BTreeKey::IndexKey(inserted.as_record_ref())
    } else {
        BTreeKey::TableRowId((35, Some(&inserted)))
    };
    conn.db
        .io
        .block(|| cursor.seek(key.clone(), SeekOp::GE { eq_only: false }))
        .unwrap();
    conn.db.io.block(|| cursor.insert(&inserted_key)).unwrap();
    let actual = rowids_from_current_row(&conn, &mut cursor, false);
    drop(cursor);
    conn.execute("ROLLBACK").unwrap();
    eprintln!("REPRO3 index={index} actual={actual:?} expected=[35, 40, 50]");
    assert_eq!(actual, vec![35, 40, 50]);
}

/// Claim 4: a range seek, next(), then prev() must return the row it started on.
#[rstest::rstest]
fn repro_claim4_direction_change(
    #[values(false, true)] index: bool,
    #[values(10, 20, 30, 40)] target: i64,
) {
    use crate::types::{SeekKey, SeekOp};
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = mixed_rows_fixture(&db);
    let mut cursor = mixed_rows_cursor(&conn, index);
    let record =
        ImmutableRecord::from_values(&[Value::from_i64(target), Value::from_i64(target)], 2)
            .unwrap();
    let key = if index {
        SeekKey::IndexKey(record.as_record_ref())
    } else {
        SeekKey::TableRowId(target)
    };
    let result = conn
        .db
        .io
        .block(|| cursor.seek(key.clone(), SeekOp::GE { eq_only: false }))
        .unwrap();
    assert!(matches!(result, SeekResult::Found));
    conn.db.io.block(|| cursor.next()).unwrap();
    let next = conn.db.io.block(|| cursor.rowid()).unwrap();
    conn.db.io.block(|| cursor.prev()).unwrap();
    let previous = conn.db.io.block(|| cursor.rowid()).unwrap();
    drop(cursor);
    conn.execute("ROLLBACK").unwrap();
    eprintln!(
        "REPRO4 index={index} target={target} actual=(next {next:?}, prev {previous:?}) expected=(next {:?}, prev {:?})",
        Some(target + 10),
        Some(target)
    );
    assert_eq!((next, previous), (Some(target + 10), Some(target)));
}
