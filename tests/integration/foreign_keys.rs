use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;

fn opcode_names(rows: &[Vec<Value>]) -> Vec<&str> {
    rows.iter()
        .filter_map(|row| match row.get(1) {
            Some(Value::Text(opcode)) => Some(opcode.as_str()),
            _ => None,
        })
        .collect()
}

#[test]
fn parent_update_without_fk_action_omits_action_key_compare() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();

    conn.execute("PRAGMA foreign_keys=ON").unwrap();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, k INTEGER UNIQUE, v INTEGER)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, pk INTEGER REFERENCES parent(k) ON UPDATE NO ACTION)",
    )
    .unwrap();

    let rows = limbo_exec_rows(&conn, "EXPLAIN UPDATE parent SET v = v + 1 WHERE id = 1");
    let opcodes = opcode_names(&rows);

    assert!(
        !opcodes.contains(&"Eq"),
        "unrelated parent-column UPDATE should not emit FK action key-compare bytecode: {opcodes:?}"
    );
}

#[test]
fn parent_update_with_fk_action_still_emits_action_subprogram() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();

    conn.execute("PRAGMA foreign_keys=ON").unwrap();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, k INTEGER UNIQUE)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, pk INTEGER REFERENCES parent(k) ON UPDATE CASCADE)",
    )
    .unwrap();

    let rows = limbo_exec_rows(&conn, "EXPLAIN UPDATE parent SET k = k + 1 WHERE id = 1");
    let opcodes = opcode_names(&rows);

    assert!(
        opcodes.contains(&"Program"),
        "parent-key UPDATE with ON UPDATE CASCADE must still emit FK action subprogram: {opcodes:?}"
    );
}
