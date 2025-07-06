use crate::common::TempDatabase;

#[test]
fn test_schema_change() {
    let tmp_db = TempDatabase::new_empty(false);
    let conn1 = tmp_db.connect_limbo();
    conn1.execute("CREATE TABLE t(x, y, z)").unwrap();
    conn1
        .execute("INSERT INTO t VALUES (1, 2, 3), (10, 20, 30)")
        .unwrap();
    let conn2 = tmp_db.connect_limbo();
    let mut stmt = conn2.prepare("SELECT x, z FROM t").unwrap();
    conn1.execute("ALTER TABLE t DROP COLUMN x").unwrap();
    let row = loop {
        match stmt.step() {
            Ok(turso_core::StepResult::Row) => {
                let row = stmt.row().unwrap();
                break row;
            }
            Ok(turso_core::StepResult::IO) => {
                stmt.run_once().unwrap();
            }
            _ => panic!("unexpected step result"),
        }
    };
    println!("{:?} {:?}", row.get_value(0), row.get_value(1));
}
