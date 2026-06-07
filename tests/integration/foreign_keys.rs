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
fn parent_update_with_mixed_fks_prunes_noaction_from_action_phase() {
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
    let program_pos = opcodes
        .iter()
        .position(|opcode| *opcode == "Program")
        .expect("ON UPDATE CASCADE should emit one FK action subprogram");

    assert!(
        !opcodes[program_pos + 1..].contains(&"Eq"),
        "NO ACTION FKs must not emit post-action key-compare bytecode after the action subprogram: {opcodes:?}"
    );
}
