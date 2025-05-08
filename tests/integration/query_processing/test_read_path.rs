use crate::common::TempDatabase;
use limbo_core::{StepResult, Value};

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
                    limbo_core::Value::Integer(1)
                );
            }
            StepResult::IO => tmp_db.io.run_once()?,
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
                    limbo_core::Value::Integer(2)
                );
            }
            StepResult::IO => tmp_db.io.run_once()?,
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
                if let limbo_core::Value::Text(s) = row.get::<&Value>(0).unwrap() {
                    assert_eq!(s.as_str(), "hello")
                }

                if let limbo_core::Value::Text(s) = row.get::<&Value>(1).unwrap() {
                    assert_eq!(s.as_str(), "hello")
                }

                if let limbo_core::Value::Integer(i) = row.get::<&Value>(2).unwrap() {
                    assert_eq!(*i, 42)
                }

                if let limbo_core::Value::Blob(v) = row.get::<&Value>(3).unwrap() {
                    assert_eq!(v.as_slice(), &vec![0x1 as u8, 0x2, 0x3])
                }

                if let limbo_core::Value::Float(f) = row.get::<&Value>(4).unwrap() {
                    assert_eq!(*f, 0.5)
                }
            }
            StepResult::IO => {
                tmp_db.io.run_once()?;
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
            StepResult::IO => tmp_db.io.run_once()?,
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
            StepResult::IO => tmp_db.io.run_once()?,
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
            StepResult::IO => tmp_db.io.run_once()?,
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
            StepResult::IO => tmp_db.io.run_once()?,
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
            StepResult::IO => tmp_db.io.run_once()?,
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
            StepResult::IO => tmp_db.io.run_once()?,
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
            StepResult::IO => tmp_db.io.run_once()?,
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
            StepResult::IO => tmp_db.io.run_once()?,
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
            StepResult::IO => tmp_db.io.run_once()?,
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
            StepResult::IO => tmp_db.io.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
        }
    }
    assert_eq!(ins.parameters().count(), 8);
    Ok(())
}
