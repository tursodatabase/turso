use crate::common::TempDatabase;
use turso_core::Value;

#[turso_macros::test(mvcc)]
fn test_writes_of_a_trigger_do_not_count_as_changes(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE TABLE items (id int)").unwrap();
    conn.execute("CREATE TABLE audit (id int)").unwrap();
    conn.execute(
        "CREATE FUNCTION audit_item() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO audit VALUES (NEW.id);
             INSERT INTO audit VALUES (NEW.id * 10);
             RETURN NULL;
         END
         $$",
    )
    .unwrap();
    conn.execute(
        "CREATE TRIGGER items_audit AFTER INSERT ON items
         FOR EACH ROW EXECUTE FUNCTION audit_item()",
    )
    .unwrap();

    let mut insert = conn.prepare("INSERT INTO items VALUES (1), (2)").unwrap();
    insert.run_ignore_rows().unwrap();

    assert_eq!(insert.n_change(), 2);
    assert_eq!(
        conn.prepare("SELECT count(*) FROM audit")
            .unwrap()
            .run_collect_rows()
            .unwrap(),
        vec![vec![Value::from_i64(4)]]
    );
}
