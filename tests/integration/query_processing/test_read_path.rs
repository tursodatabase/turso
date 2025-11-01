use crate::common::{limbo_exec_rows, TempDatabase};
use turso_core::{LimboError, StepResult, Value};

#[test]
fn test_statement_reset_bind() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_with_rusqlite("create table test (i integer);");
    let conn = tmp_db.connect_limbo();

    let mut stmt = conn.prepare("select ?")?;

    stmt.bind_at(1.try_into()?, Value::Integer(1));

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                assert_eq!(
                    *row.get::<&Value>(0).unwrap(),
                    turso_core::Value::Integer(1)
                );
            }
            StepResult::IO => stmt.run_once()?,
            _ => break,
        }
    }

    stmt.reset();

    stmt.bind_at(1.try_into()?, Value::Integer(2));

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                assert_eq!(
                    *row.get::<&Value>(0).unwrap(),
                    turso_core::Value::Integer(2)
                );
            }
            StepResult::IO => stmt.run_once()?,
            _ => break,
        }
    }

    Ok(())
}

#[test]
fn test_statement_bind() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_with_rusqlite("create table test (i integer);");
    let conn = tmp_db.connect_limbo();

    let mut stmt = conn.prepare("select ?, ?1, :named, ?3, ?4")?;

    stmt.bind_at(1.try_into()?, Value::build_text("hello"));

    let i = stmt.parameters().index(":named").unwrap();
    stmt.bind_at(i, Value::Integer(42));

    stmt.bind_at(3.try_into()?, Value::from_blob(vec![0x1, 0x2, 0x3]));

    stmt.bind_at(4.try_into()?, Value::Float(0.5));

    assert_eq!(stmt.parameters().count(), 4);

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let turso_core::Value::Text(s) = row.get::<&Value>(0).unwrap() {
                    assert_eq!(s.as_str(), "hello")
                }

                if let turso_core::Value::Text(s) = row.get::<&Value>(1).unwrap() {
                    assert_eq!(s.as_str(), "hello")
                }

                if let turso_core::Value::Integer(i) = row.get::<&Value>(2).unwrap() {
                    assert_eq!(*i, 42)
                }

                if let turso_core::Value::Blob(b) = row.get::<&Value>(3).unwrap() {
                    assert_eq!(b.value.as_slice(), &vec![0x1_u8, 0x2, 0x3])
                }

                if let turso_core::Value::Float(f) = row.get::<&Value>(4).unwrap() {
                    assert_eq!(*f, 0.5)
                }
            }
            StepResult::IO => {
                stmt.run_once()?;
            }
            StepResult::Interrupt => break,
            StepResult::Done => break,
            StepResult::Busy => panic!("Database is busy"),
        };
    }
    Ok(())
}

#[test]
fn test_insert_parameter_remap() -> anyhow::Result<()> {
    // ───────────────────────  schema  ──────────────────────────────
    // Table             a     b     c     d
    // INSERT lists:     d ,   c ,   a ,   b
    // VALUES list:    22 ,   ?1 ,   7 ,   ?2
    //
    // Expected row on disk:  a = 7 , b = ?2 , c = ?1 , d = 22
    //
    // We bind ?1 = 111 and ?2 = 222 and expect (7,222,111,22).
    // ───────────────────────────────────────────────────────────────

    let tmp_db = TempDatabase::new_with_rusqlite(
        "create table test (a integer, b integer, c integer, d integer);",
    );
    let conn = tmp_db.connect_limbo();

    // prepare INSERT with re-ordered columns and constants
    let mut ins = conn.prepare("insert into test (d, c, a, b) values (22, ?, 7, ?);")?;
    let args = [Value::Integer(111), Value::Integer(222)];
    for (i, arg) in args.iter().enumerate() {
        let idx = i + 1;
        ins.bind_at(idx.try_into()?, arg.clone());
    }
    loop {
        match ins.step()? {
            StepResult::IO => ins.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
            _ => {}
        }
    }

    let mut sel = conn.prepare("select a, b, c, d from test;")?;
    loop {
        match sel.step()? {
            StepResult::Row => {
                let row = sel.row().unwrap();
                // insert_index = 3
                // A = 7
                assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(7));
                // insert_index = 4
                // B = 222
                assert_eq!(row.get::<&Value>(1).unwrap(), &Value::Integer(222));
                // insert_index = 2
                // C = 111
                assert_eq!(row.get::<&Value>(2).unwrap(), &Value::Integer(111));
                // insert_index = 1
                // D = 22
                assert_eq!(row.get::<&Value>(3).unwrap(), &Value::Integer(22));
            }
            StepResult::IO => sel.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
        }
    }

    // exactly two distinct parameters were used
    assert_eq!(ins.parameters().count(), 2);

    Ok(())
}

#[test]
fn test_insert_parameter_remap_all_params() -> anyhow::Result<()> {
    // ───────────────────────  schema  ──────────────────────────────
    // Table             a     b     c     d
    // INSERT lists:     d ,   a ,   c ,   b
    // VALUES list:     ?1 ,  ?2 ,  ?3 ,  ?4
    //
    // Expected row on disk:  a = ?2 , b = ?4 , c = ?3 , d = ?1
    //
    // We bind ?1 = 999, ?2 = 111, ?3 = 333, ?4 = 444.
    // The row should be (111, 444, 333, 999).
    // ───────────────────────────────────────────────────────────────

    let tmp_db = TempDatabase::new_with_rusqlite(
        "create table test (a integer, b integer, c integer, d integer);",
    );
    let conn = tmp_db.connect_limbo();
    let mut ins = conn.prepare("insert into test (d, a, c, b) values (?, ?, ?, ?);")?;

    let values = [
        Value::Integer(999), // ?1 → d
        Value::Integer(111), // ?2 → a
        Value::Integer(333), // ?3 → c
        Value::Integer(444), // ?4 → b
    ];
    for (i, value) in values.iter().enumerate() {
        let idx = i + 1;
        ins.bind_at(idx.try_into()?, value.clone());
    }

    // execute the insert (no rows returned)
    loop {
        match ins.step()? {
            StepResult::IO => ins.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
            _ => {}
        }
    }

    let mut sel = conn.prepare("select a, b, c, d from test;")?;
    loop {
        match sel.step()? {
            StepResult::Row => {
                let row = sel.row().unwrap();

                // insert_index = 2
                // A = 111
                assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(111));
                // insert_index = 4
                // B = 444
                assert_eq!(row.get::<&Value>(1).unwrap(), &Value::Integer(444));
                // insert_index = 3
                // C = 333
                assert_eq!(row.get::<&Value>(2).unwrap(), &Value::Integer(333));
                // insert_index = 1
                // D = 999
                assert_eq!(row.get::<&Value>(3).unwrap(), &Value::Integer(999));
            }
            StepResult::IO => sel.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
        }
    }
    assert_eq!(ins.parameters().count(), 4);
    Ok(())
}

#[test]
fn test_insert_parameter_multiple_remap_backwards() -> anyhow::Result<()> {
    // ───────────────────────  schema  ──────────────────────────────
    // Table             a     b     c     d
    // INSERT lists:     d ,   c ,   b ,   a
    // VALUES list:     ?1 ,  ?2 ,  ?3 ,  ?4
    //
    // Expected row on disk:  a = ?1 , b = ?2 , c = ?3 , d = ?4
    //
    // The row should be (111, 222, 333, 444)
    // ───────────────────────────────────────────────────────────────

    let tmp_db = TempDatabase::new_with_rusqlite(
        "create table test (a integer, b integer, c integer, d integer);",
    );
    let conn = tmp_db.connect_limbo();
    let mut ins = conn.prepare("insert into test (d,c,b,a) values (?, ?, ?, ?);")?;

    let values = [
        Value::Integer(444), // ?1 → d
        Value::Integer(333), // ?2 → c
        Value::Integer(222), // ?3 → b
        Value::Integer(111), // ?4 → a
    ];
    for (i, value) in values.iter().enumerate() {
        let idx = i + 1;
        ins.bind_at(idx.try_into()?, value.clone());
    }

    // execute the insert (no rows returned)
    loop {
        match ins.step()? {
            StepResult::IO => ins.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
            _ => {}
        }
    }

    let mut sel = conn.prepare("select a, b, c, d from test;")?;
    loop {
        match sel.step()? {
            StepResult::Row => {
                let row = sel.row().unwrap();

                // insert_index = 2
                // A = 111
                assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(111));
                // insert_index = 4
                // B = 444
                assert_eq!(row.get::<&Value>(1).unwrap(), &Value::Integer(222));
                // insert_index = 3
                // C = 333
                assert_eq!(row.get::<&Value>(2).unwrap(), &Value::Integer(333));
                // insert_index = 1
                // D = 999
                assert_eq!(row.get::<&Value>(3).unwrap(), &Value::Integer(444));
            }
            StepResult::IO => sel.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
        }
    }
    assert_eq!(ins.parameters().count(), 4);
    Ok(())
}
#[test]
fn test_insert_parameter_multiple_no_remap() -> anyhow::Result<()> {
    // ───────────────────────  schema  ──────────────────────────────
    // Table             a     b     c     d
    // INSERT lists:     a ,   b ,   c ,   d
    // VALUES list:     ?1 ,  ?2 ,  ?3 ,  ?4
    //
    // Expected row on disk:  a = ?1 , b = ?2 , c = ?3 , d = ?4
    //
    // The row should be (111, 222, 333, 444)
    // ───────────────────────────────────────────────────────────────

    let tmp_db = TempDatabase::new_with_rusqlite(
        "create table test (a integer, b integer, c integer, d integer);",
    );
    let conn = tmp_db.connect_limbo();
    let mut ins = conn.prepare("insert into test (a,b,c,d) values (?, ?, ?, ?);")?;

    let values = [
        Value::Integer(111), // ?1 → a
        Value::Integer(222), // ?2 → b
        Value::Integer(333), // ?3 → c
        Value::Integer(444), // ?4 → d
    ];
    for (i, value) in values.iter().enumerate() {
        let idx = i + 1;
        ins.bind_at(idx.try_into()?, value.clone());
    }

    // execute the insert (no rows returned)
    loop {
        match ins.step()? {
            StepResult::IO => ins.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
            _ => {}
        }
    }

    let mut sel = conn.prepare("select a, b, c, d from test;")?;
    loop {
        match sel.step()? {
            StepResult::Row => {
                let row = sel.row().unwrap();

                // insert_index = 2
                // A = 111
                assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(111));
                // insert_index = 4
                // B = 444
                assert_eq!(row.get::<&Value>(1).unwrap(), &Value::Integer(222));
                // insert_index = 3
                // C = 333
                assert_eq!(row.get::<&Value>(2).unwrap(), &Value::Integer(333));
                // insert_index = 1
                // D = 999
                assert_eq!(row.get::<&Value>(3).unwrap(), &Value::Integer(444));
            }
            StepResult::IO => sel.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
        }
    }
    assert_eq!(ins.parameters().count(), 4);
    Ok(())
}

#[test]
fn test_insert_parameter_multiple_row() -> anyhow::Result<()> {
    // ───────────────────────  schema  ──────────────────────────────
    // Table             a     b     c     d
    // INSERT lists:     b ,   a ,   d ,   c
    // VALUES list:     (?1 ,  ?2 ,  ?3 ,  ?4),
    //                  (?5,   ?6,   ?7,   ?8);
    //
    // The row should be (111, 222, 333, 444), (555, 666, 777, 888)
    // ───────────────────────────────────────────────────────────────

    let tmp_db = TempDatabase::new_with_rusqlite(
        "create table test (a integer, b integer, c integer, d integer);",
    );
    let conn = tmp_db.connect_limbo();
    let mut ins = conn.prepare("insert into test (b,a,d,c) values (?, ?, ?, ?), (?, ?, ?, ?);")?;

    let values = [
        Value::Integer(222), // ?1 → b
        Value::Integer(111), // ?2 → a
        Value::Integer(444), // ?3 → d
        Value::Integer(333), // ?4 → c
        Value::Integer(666), // ?1 → b
        Value::Integer(555), // ?2 → a
        Value::Integer(888), // ?3 → d
        Value::Integer(777), // ?4 → c
    ];
    for (i, value) in values.iter().enumerate() {
        let idx = i + 1;
        ins.bind_at(idx.try_into()?, value.clone());
    }

    // execute the insert (no rows returned)
    loop {
        match ins.step()? {
            StepResult::IO => ins.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
            _ => {}
        }
    }

    let mut sel = conn.prepare("select a, b, c, d from test;")?;
    let mut i = 0;
    loop {
        match sel.step()? {
            StepResult::Row => {
                let row = sel.row().unwrap();

                assert_eq!(
                    row.get::<&Value>(0).unwrap(),
                    &Value::Integer(if i == 0 { 111 } else { 555 })
                );
                assert_eq!(
                    row.get::<&Value>(1).unwrap(),
                    &Value::Integer(if i == 0 { 222 } else { 666 })
                );
                assert_eq!(
                    row.get::<&Value>(2).unwrap(),
                    &Value::Integer(if i == 0 { 333 } else { 777 })
                );
                assert_eq!(
                    row.get::<&Value>(3).unwrap(),
                    &Value::Integer(if i == 0 { 444 } else { 888 })
                );
                i += 1;
            }
            StepResult::IO => sel.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
        }
    }
    assert_eq!(ins.parameters().count(), 8);
    Ok(())
}

#[test]
fn test_bind_parameters_update_query() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_with_rusqlite("create table test (a integer, b text);");
    let conn = tmp_db.connect_limbo();
    let mut ins = conn.prepare("insert into test (a, b) values (3, 'test1');")?;
    loop {
        match ins.step()? {
            StepResult::IO => ins.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
            _ => {}
        }
    }
    let mut ins = conn.prepare("update test set a = ? where b = ?;")?;
    ins.bind_at(1.try_into()?, Value::Integer(222));
    ins.bind_at(2.try_into()?, Value::build_text("test1"));
    loop {
        match ins.step()? {
            StepResult::IO => ins.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
            _ => {}
        }
    }

    let mut sel = conn.prepare("select a, b from test;")?;
    loop {
        match sel.step()? {
            StepResult::Row => {
                let row = sel.row().unwrap();
                assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(222));
                assert_eq!(row.get::<&Value>(1).unwrap(), &Value::build_text("test1"),);
            }
            StepResult::IO => sel.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
        }
    }
    assert_eq!(ins.parameters().count(), 2);
    Ok(())
}

#[test]
fn test_bind_parameters_update_query_multiple_where() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_with_rusqlite(
        "create table test (a integer, b text, c integer, d integer);",
    );
    let conn = tmp_db.connect_limbo();
    let mut ins = conn.prepare("insert into test (a, b, c, d) values (3, 'test1', 4, 5);")?;
    loop {
        match ins.step()? {
            StepResult::IO => ins.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
            _ => {}
        }
    }
    let mut ins = conn.prepare("update test set a = ? where b = ? and c = 4 and d = ?;")?;
    ins.bind_at(1.try_into()?, Value::Integer(222));
    ins.bind_at(2.try_into()?, Value::build_text("test1"));
    ins.bind_at(3.try_into()?, Value::Integer(5));
    loop {
        match ins.step()? {
            StepResult::IO => ins.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
            _ => {}
        }
    }

    let mut sel = conn.prepare("select a, b, c, d from test;")?;
    loop {
        match sel.step()? {
            StepResult::Row => {
                let row = sel.row().unwrap();
                assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(222));
                assert_eq!(row.get::<&Value>(1).unwrap(), &Value::build_text("test1"),);
                assert_eq!(row.get::<&Value>(2).unwrap(), &Value::Integer(4));
                assert_eq!(row.get::<&Value>(3).unwrap(), &Value::Integer(5));
            }
            StepResult::IO => sel.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
        }
    }
    assert_eq!(ins.parameters().count(), 3);
    Ok(())
}

#[test]
fn test_bind_parameters_update_rowid_alias() -> anyhow::Result<()> {
    let tmp_db =
        TempDatabase::new_with_rusqlite("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);");
    let conn = tmp_db.connect_limbo();
    let mut ins = conn.prepare("insert into test (id, name) values (1, 'test');")?;
    loop {
        match ins.step()? {
            StepResult::IO => ins.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
            _ => {}
        }
    }

    let mut sel = conn.prepare("select id, name from test;")?;
    loop {
        match sel.step()? {
            StepResult::Row => {
                let row = sel.row().unwrap();
                assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(1));
                assert_eq!(row.get::<&Value>(1).unwrap(), &Value::build_text("test"),);
            }
            StepResult::IO => sel.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
        }
    }
    let mut ins = conn.prepare("update test set name = ? where id = ?;")?;
    ins.bind_at(1.try_into()?, Value::build_text("updated"));
    ins.bind_at(2.try_into()?, Value::Integer(1));
    loop {
        match ins.step()? {
            StepResult::IO => ins.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
            _ => {}
        }
    }

    let mut sel = conn.prepare("select id, name from test;")?;
    loop {
        match sel.step()? {
            StepResult::Row => {
                let row = sel.row().unwrap();
                assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(1));
                assert_eq!(row.get::<&Value>(1).unwrap(), &Value::build_text("updated"),);
            }
            StepResult::IO => sel.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
        }
    }
    assert_eq!(ins.parameters().count(), 2);
    Ok(())
}

#[test]
fn test_bind_parameters_update_rowid_alias_seek_rowid() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_with_rusqlite(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, age integer);",
    );
    let conn = tmp_db.connect_limbo();
    conn.execute("insert into test (id, name, age) values (1, 'test', 4);")?;
    conn.execute("insert into test (id, name, age) values (2, 'test', 11);")?;

    let mut sel = conn.prepare("select id, name, age from test;")?;
    let mut i = 0;
    loop {
        match sel.step()? {
            StepResult::Row => {
                let row = sel.row().unwrap();
                assert_eq!(
                    row.get::<&Value>(0).unwrap(),
                    &Value::Integer(if i == 0 { 1 } else { 2 })
                );
                assert_eq!(row.get::<&Value>(1).unwrap(), &Value::build_text("test"),);
                assert_eq!(
                    row.get::<&Value>(2).unwrap(),
                    &Value::Integer(if i == 0 { 4 } else { 11 })
                );
                i += 1;
            }
            StepResult::IO => sel.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
        }
    }
    let mut ins = conn.prepare("update test set name = ? where id < ? AND age between ? and ?;")?;
    ins.bind_at(1.try_into()?, Value::build_text("updated"));
    ins.bind_at(2.try_into()?, Value::Integer(2));
    ins.bind_at(3.try_into()?, Value::Integer(3));
    ins.bind_at(4.try_into()?, Value::Integer(5));
    loop {
        match ins.step()? {
            StepResult::IO => ins.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
            _ => {}
        }
    }

    let mut sel = conn.prepare("select name from test;")?;
    let mut i = 0;
    loop {
        match sel.step()? {
            StepResult::Row => {
                let row = sel.row().unwrap();
                assert_eq!(
                    row.get::<&Value>(0).unwrap(),
                    &Value::build_text(if i == 0 { "updated" } else { "test" }),
                );
                i += 1;
            }
            StepResult::IO => sel.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
        }
    }

    assert_eq!(ins.parameters().count(), 4);
    Ok(())
}

#[test]
fn test_bind_parameters_delete_rowid_alias_seek_out_of_order() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_with_rusqlite(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, age integer);",
    );
    let conn = tmp_db.connect_limbo();
    conn.execute("insert into test (id, name, age) values (1, 'correct', 4);")?;
    conn.execute("insert into test (id, name, age) values (5, 'test', 11);")?;

    let mut ins =
        conn.prepare("delete from test where age between ? and ? AND id > ? AND name = ?;")?;
    ins.bind_at(1.try_into()?, Value::Integer(10));
    ins.bind_at(2.try_into()?, Value::Integer(12));
    ins.bind_at(3.try_into()?, Value::Integer(4));
    ins.bind_at(4.try_into()?, Value::build_text("test"));
    loop {
        match ins.step()? {
            StepResult::IO => ins.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
            _ => {}
        }
    }

    let mut sel = conn.prepare("select name from test;")?;
    let mut i = 0;
    loop {
        match sel.step()? {
            StepResult::Row => {
                let row = sel.row().unwrap();
                assert_eq!(row.get::<&Value>(0).unwrap(), &Value::build_text("correct"),);
                i += 1;
            }
            StepResult::IO => sel.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
        }
    }
    assert_eq!(i, 1);
    assert_eq!(ins.parameters().count(), 4);
    Ok(())
}

#[test]
fn test_cte_alias() -> anyhow::Result<()> {
    let tmp_db =
        TempDatabase::new_with_rusqlite("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT);");
    let conn = tmp_db.connect_limbo();
    conn.execute("INSERT INTO test (id, name) VALUES (1, 'Limbo');")?;
    conn.execute("INSERT INTO test (id, name) VALUES (2, 'Turso');")?;

    let mut stmt1 = conn.prepare(
        "WITH a1 AS (SELECT id FROM test WHERE name = 'Limbo') SELECT a2.id FROM a1 AS a2",
    )?;
    loop {
        match stmt1.step()? {
            StepResult::Row => {
                let row = stmt1.row().unwrap();
                assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(1));
                break;
            }
            StepResult::Done => {
                panic!("Expected a row but got Done");
            }
            StepResult::IO => stmt1.run_once()?,
            _ => panic!("Unexpected step result"),
        }
    }

    let mut stmt2 = conn
        .prepare("WITH a1 AS (SELECT id FROM test WHERE name = 'Turso') SELECT a2.id FROM a1 a2")?;
    loop {
        match stmt2.step()? {
            StepResult::Row => {
                let row = stmt2.row().unwrap();
                assert_eq!(row.get::<&Value>(0).unwrap(), &Value::Integer(2));
                break;
            }
            StepResult::Done => {
                panic!("Expected a row but got Done");
            }
            StepResult::IO => stmt2.run_once()?,
            _ => panic!("Unexpected step result"),
        }
    }
    Ok(())
}

#[test]
fn test_avg_agg() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_with_rusqlite("create table t (x, y);");
    let conn = tmp_db.connect_limbo();
    conn.execute("insert into t values (1, null), (2, null), (3, null), (null, null), (4, null)")?;
    let mut rows = Vec::new();
    let mut stmt = conn.prepare("select avg(x), avg(y) from t")?;
    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            StepResult::Done => break,
            StepResult::IO => stmt.run_once()?,
            _ => panic!("Unexpected step result"),
        }
    }

    assert_eq!(stmt.num_columns(), 2);
    assert_eq!(stmt.get_column_name(0), "avg (t.x)");
    assert_eq!(stmt.get_column_name(1), "avg (t.y)");

    assert_eq!(
        rows,
        vec![vec![
            turso_core::Value::Float((1.0 + 2.0 + 3.0 + 4.0) / (4.0)),
            turso_core::Value::Null
        ]]
    );

    Ok(())
}

#[test]
fn test_offset_limit_bind() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE test (i INTEGER);");
    let conn = tmp_db.connect_limbo();

    conn.execute("INSERT INTO test VALUES (5), (4), (3), (2), (1)")?;

    for (limit, offset, expected) in [
        (
            2,
            1,
            vec![
                vec![turso_core::Value::Integer(4)],
                vec![turso_core::Value::Integer(3)],
            ],
        ),
        (0, 0, vec![]),
        (1, 0, vec![vec![turso_core::Value::Integer(5)]]),
        (0, 1, vec![]),
        (1, 1, vec![vec![turso_core::Value::Integer(4)]]),
    ] {
        let mut stmt = conn.prepare("SELECT * FROM test LIMIT ? OFFSET ?")?;
        stmt.bind_at(1.try_into()?, Value::Integer(limit));
        stmt.bind_at(2.try_into()?, Value::Integer(offset));

        let mut rows = Vec::new();
        loop {
            match stmt.step()? {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    rows.push(row.get_values().cloned().collect::<Vec<_>>());
                }
                StepResult::IO => stmt.run_once()?,
                _ => break,
            }
        }

        assert_eq!(rows, expected);
    }

    Ok(())
}

#[test]
fn test_upsert_parameters_order() -> anyhow::Result<()> {
    let tmp_db =
        TempDatabase::new_with_rusqlite("CREATE TABLE test (k INTEGER PRIMARY KEY, v INTEGER);");
    let conn = tmp_db.connect_limbo();

    conn.execute("INSERT INTO test VALUES (1, 2), (3, 4)")?;
    let mut stmt =
        conn.prepare("INSERT INTO test VALUES (?, ?), (?, ?) ON CONFLICT DO UPDATE SET v = ?")?;
    stmt.bind_at(1.try_into()?, Value::Integer(1));
    stmt.bind_at(2.try_into()?, Value::Integer(20));
    stmt.bind_at(3.try_into()?, Value::Integer(3));
    stmt.bind_at(4.try_into()?, Value::Integer(40));
    stmt.bind_at(5.try_into()?, Value::Integer(66));
    while let StepResult::Row | StepResult::IO = stmt.step()? {
        stmt.run_once()?;
    }

    let mut rows = Vec::new();
    let mut stmt = conn.prepare("SELECT * FROM test")?;
    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            StepResult::IO => stmt.run_once()?,
            _ => break,
        }
    }

    assert_eq!(
        rows,
        vec![
            vec![
                turso_core::Value::Integer(1),
                turso_core::Value::Integer(66)
            ],
            vec![
                turso_core::Value::Integer(3),
                turso_core::Value::Integer(66)
            ]
        ]
    );
    Ok(())
}

#[test]
fn test_multiple_connections_visibility() -> anyhow::Result<()> {
    let tmp_db =
        TempDatabase::new_with_rusqlite("CREATE TABLE test (k INTEGER PRIMARY KEY, v INTEGER);");
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();
    conn1.execute("BEGIN")?;
    conn1.execute("INSERT INTO test VALUES (1, 2), (3, 4)")?;
    let mut stmt = conn2.prepare("SELECT COUNT(*) FROM test").unwrap();
    let _ = stmt.step().unwrap();
    // intentionally drop not-fully-consumed statement in order to check that on Drop statement will execute reset with proper cleanup
    drop(stmt);
    conn1.execute("COMMIT")?;

    let rows = limbo_exec_rows(&tmp_db, &conn2, "SELECT COUNT(*) FROM test");
    assert_eq!(rows, vec![vec![rusqlite::types::Value::Integer(2)]]);
    Ok(())
}

#[test]
fn test_stmt_reset() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE test (x);");
    let conn1 = tmp_db.connect_limbo();
    let mut stmt1 = conn1.prepare("INSERT INTO test VALUES (?)").unwrap();
    for _ in 0..3 {
        stmt1.reset();
        stmt1.bind_at(1.try_into().unwrap(), Value::build_blob(vec![0u8; 1024]));
        loop {
            match stmt1.step().unwrap() {
                StepResult::Done => break,
                _ => tmp_db.io.step().unwrap(),
            }
        }
    }

    // force btree-page split which will be "unnoticed" by stmt1 if it will cache something in between of calls
    conn1
        .execute("INSERT INTO test VALUES (randomblob(1024))")
        .unwrap();

    stmt1.reset();
    stmt1.bind_at(1.try_into().unwrap(), Value::build_blob(vec![0u8; 1024]));
    loop {
        match stmt1.step().unwrap() {
            StepResult::Done => break,
            _ => tmp_db.io.step().unwrap(),
        }
    }
    let rows = limbo_exec_rows(&tmp_db, &conn1, "SELECT rowid FROM test");
    assert_eq!(
        rows,
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
            vec![rusqlite::types::Value::Integer(3)],
            vec![rusqlite::types::Value::Integer(4)],
            vec![rusqlite::types::Value::Integer(5)],
        ]
    );
    Ok(())
}

#[test]
/// Test that we can only join up to 63 tables, and trying to join more should fail with an error instead of panicing.
fn test_max_joined_tables_limit() {
    let tmp_db = TempDatabase::new("test_max_joined_tables_limit");
    let conn = tmp_db.connect_limbo();

    // Create 64 tables
    for i in 0..64 {
        conn.execute(format!("CREATE TABLE t{i} (id INTEGER)"))
            .unwrap();
    }

    // Try to join 64 tables - should fail
    let mut sql = String::from("SELECT * FROM t0");
    for i in 1..64 {
        sql.push_str(&format!(" JOIN t{i} ON t{i}.id = t0.id"));
    }

    let Err(LimboError::ParseError(result)) = conn.prepare(&sql) else {
        panic!("Expected an error but got no error");
    };
    assert!(result.contains("Only up to 63 tables can be joined"));
}

#[test]
/// Test that we can create and select from a table with 1000 columns.
fn test_many_columns() {
    let mut create_sql = String::from("CREATE TABLE test (");
    for i in 0..1000 {
        if i > 0 {
            create_sql.push_str(", ");
        }
        create_sql.push_str(&format!("col{i} INTEGER"));
    }
    create_sql.push(')');

    let tmp_db = TempDatabase::new("test_many_columns");
    let conn = tmp_db.connect_limbo();
    conn.execute(&create_sql).unwrap();

    // Insert a row with values 0-999
    let mut insert_sql = String::from("INSERT INTO test VALUES (");
    for i in 0..1000 {
        if i > 0 {
            insert_sql.push_str(", ");
        }
        insert_sql.push_str(&i.to_string());
    }
    insert_sql.push(')');
    conn.execute(&insert_sql).unwrap();

    // Select every 100th column
    let mut select_sql = String::from("SELECT ");
    let mut first = true;
    for i in (0..1000).step_by(100) {
        if !first {
            select_sql.push_str(", ");
        }
        select_sql.push_str(&format!("col{i}"));
        first = false;
    }
    select_sql.push_str(" FROM test");

    let mut rows = Vec::new();
    let mut stmt = conn.prepare(&select_sql).unwrap();
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                rows.push(row.get_values().cloned().collect::<Vec<_>>());
            }
            StepResult::IO => stmt.run_once().unwrap(),
            _ => break,
        }
    }

    // Verify we got values 0,100,200,...,900
    assert_eq!(
        rows,
        vec![vec![
            turso_core::Value::Integer(0),
            turso_core::Value::Integer(100),
            turso_core::Value::Integer(200),
            turso_core::Value::Integer(300),
            turso_core::Value::Integer(400),
            turso_core::Value::Integer(500),
            turso_core::Value::Integer(600),
            turso_core::Value::Integer(700),
            turso_core::Value::Integer(800),
            turso_core::Value::Integer(900),
        ]]
    );
}
