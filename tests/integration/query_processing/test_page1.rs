use crate::common::TempDatabase;

/// Tests that require Page 1 to exist on an empty DB
#[turso_macros::test(mvcc)]
fn test_page1_init(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("values (1) intersect values (1);").unwrap();
    conn.execute("select group_concat(name) over () from sqlite_schema;")
        .unwrap();
}
