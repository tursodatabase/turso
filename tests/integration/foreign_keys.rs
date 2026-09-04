use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;

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

#[test]
fn parent_update_with_mixed_fks_emits_only_action_key_compare() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();

    conn.execute("PRAGMA foreign_keys=ON").unwrap();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, k INTEGER UNIQUE)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child_noaction(
            id INTEGER PRIMARY KEY,
            pk INTEGER REFERENCES parent(k) ON UPDATE NO ACTION
        )",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE child_cascade(
            id INTEGER PRIMARY KEY,
            pk INTEGER REFERENCES parent(k) ON UPDATE CASCADE
        )",
    )
    .unwrap();

    let rows = limbo_exec_rows(&conn, "EXPLAIN UPDATE parent SET k = k + 1 WHERE id = 1");
    let opcodes = main_program_opcode_names(&rows);
    let update_insert_pos = opcodes
        .iter()
        .rposition(|opcode| *opcode == "Insert")
        .expect("UPDATE should emit a table Insert");
    let action_phase = &opcodes[update_insert_pos + 1..];

    assert_eq!(
        action_phase
            .iter()
            .filter(|opcode| **opcode == "Eq")
            .count(),
        1,
        "only the CASCADE FK should emit a post-update key comparison: {opcodes:?}"
    );
    assert_eq!(
        action_phase
            .iter()
            .filter(|opcode| **opcode == "Program")
            .count(),
        1,
        "the CASCADE FK should still emit one action subprogram: {opcodes:?}"
    );
}

#[test]
fn upsert_unrelated_parent_column_omits_fk_action_subprogram() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();

    conn.execute("PRAGMA foreign_keys=ON").unwrap();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY, k INTEGER UNIQUE, v INTEGER)")
        .unwrap();
    conn.execute(
        "CREATE TABLE child(id INTEGER PRIMARY KEY, pk INTEGER REFERENCES parent(k) ON UPDATE CASCADE)",
    )
    .unwrap();

    let rows = limbo_exec_rows(
        &conn,
        "EXPLAIN INSERT INTO parent(id, k, v) VALUES (1, 10, 2)
         ON CONFLICT(id) DO UPDATE SET v = excluded.v",
    );
    let opcodes = opcode_names(&rows);

    assert!(
        !opcodes.contains(&"Program"),
        "UPSERT of an unrelated parent column should not emit an FK action subprogram: {opcodes:?}"
    );
}

fn opcode_names(rows: &[Vec<Value>]) -> Vec<&str> {
    rows.iter()
        .filter_map(|row| match row.get(1) {
            Some(Value::Text(opcode)) => Some(opcode.as_str()),
            _ => None,
        })
        .collect()
}

fn main_program_opcode_names(rows: &[Vec<Value>]) -> Vec<&str> {
    let mut seen_first_init = false;
    let mut opcodes = Vec::new();
    for row in rows {
        let addr = match row.first() {
            Some(Value::Integer(addr)) => *addr,
            _ => continue,
        };
        if addr == 0 {
            if seen_first_init {
                break;
            }
            seen_first_init = true;
        }
        if let Some(Value::Text(opcode)) = row.get(1) {
            opcodes.push(opcode.as_str());
        }
    }
    opcodes
}
