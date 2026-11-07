use crate::common::{limbo_exec_rows, TempDatabase};
use turso_core::storage::btree::{BTreeCursor, CursorTrait};
use turso_core::types::{IOResultOr, ImmutableRecord, SeekKey, SeekOp, SeekResult};
use turso_core::{IOResult, Pager, Value, MAIN_DB_ID};

fn block_on<T>(pager: &Pager, mut op: impl FnMut() -> IOResultOr<T>) -> T {
    loop {
        match op().unwrap() {
            IOResult::Done(value) => return value,
            IOResult::IO(io) => io.wait(pager.io.as_ref()).unwrap(),
        }
    }
}

fn current_row(pager: &Pager, cursor: &mut BTreeCursor) -> Vec<Value> {
    loop {
        match cursor.record().unwrap() {
            IOResult::Done(record) => return record.unwrap().get_values_owned().unwrap(),
            IOResult::IO(io) => io.wait(pager.io.as_ref()).unwrap(),
        }
    }
}

#[test]
fn table_and_index_cursors_over_a_read_transaction() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();
    conn.execute("CREATE TABLE t(a INTEGER PRIMARY KEY, b TEXT, c INTEGER)")
        .unwrap();
    conn.execute("CREATE INDEX t_c ON t(c)").unwrap();
    for i in 1..=500 {
        conn.execute(format!("INSERT INTO t VALUES ({i}, 'row{i}', {})", i * 10))
            .unwrap();
    }
    let table_root = root_page(&conn, "t");
    let index = conn
        .with_schema(MAIN_DB_ID, |schema| {
            schema
                .get_index(turso_core::NamespaceId::ROOT, "t", "t_c")
                .cloned()
        })
        .unwrap();

    let pager = conn.get_pager();
    pager.begin_read_tx().unwrap();

    let mut table = BTreeCursor::new_table(pager.clone(), table_root, 3);
    block_on(&pager, || table.rewind());
    let mut count = 0;
    while table.has_record() {
        count += 1;
        assert_eq!(block_on(&pager, || table.rowid()), Some(count));
        assert_eq!(
            current_row(&pager, &mut table),
            vec![
                Value::Null,
                Value::build_text(format!("row{count}")),
                Value::from_i64(count * 10)
            ]
        );
        block_on(&pager, || table.next());
    }
    assert_eq!(count, 500);

    let found = block_on(&pager, || {
        table.seek(SeekKey::TableRowId(321), SeekOp::GE { eq_only: true })
    });
    assert_eq!(found, SeekResult::Found);
    assert_eq!(block_on(&pager, || table.rowid()), Some(321));
    assert_eq!(
        current_row(&pager, &mut table)[1],
        Value::build_text("row321")
    );
    let missing = block_on(&pager, || {
        table.seek(SeekKey::TableRowId(501), SeekOp::GE { eq_only: true })
    });
    assert_eq!(missing, SeekResult::NotFound);

    let mut idx = BTreeCursor::new_index(pager.clone(), index.root_page, &index, 2).unwrap();
    let key = ImmutableRecord::from_values([Value::from_i64(1230)].iter(), 1).unwrap();
    let found = block_on(&pager, || {
        idx.seek(
            SeekKey::IndexKey(key.as_record_ref()),
            SeekOp::GE { eq_only: true },
        )
    });
    assert_eq!(found, SeekResult::Found);
    assert_eq!(
        current_row(&pager, &mut idx),
        vec![Value::from_i64(1230), Value::from_i64(123)]
    );

    drop(table);
    drop(idx);
    pager.end_read_tx();
}

fn root_page(conn: &std::sync::Arc<turso_core::Connection>, name: &str) -> i64 {
    let rows = limbo_exec_rows(
        conn,
        &format!("SELECT rootpage FROM sqlite_schema WHERE name = '{name}'"),
    );
    match rows[0][0] {
        rusqlite::types::Value::Integer(page) => page,
        ref other => panic!("unexpected rootpage {other:?}"),
    }
}
