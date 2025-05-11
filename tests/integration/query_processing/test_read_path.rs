use crate::common::TempDatabase;
use limbo_core::{OwnedValue, StepResult};

#[test]
fn test_statement_reset_bind() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("create table test (i integer);");
    let conn = tmp_db.connect_limbo();

    let mut stmt = conn.prepare("select ?")?;

    stmt.bind_at(1.try_into()?, OwnedValue::Integer(1));

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                assert_eq!(
                    *row.get::<&OwnedValue>(0).unwrap(),
                    limbo_core::OwnedValue::Integer(1)
                );
            }
            StepResult::IO => tmp_db.io.run_once()?,
            _ => break,
        }
    }

    stmt.reset();

    stmt.bind_at(1.try_into()?, OwnedValue::Integer(2));

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                assert_eq!(
                    *row.get::<&OwnedValue>(0).unwrap(),
                    limbo_core::OwnedValue::Integer(2)
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
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("create table test (i integer);");
    let conn = tmp_db.connect_limbo();

    let mut stmt = conn.prepare("select ?, ?1, :named, ?3, ?4")?;

    stmt.bind_at(1.try_into()?, OwnedValue::build_text("hello"));

    let i = stmt.parameters().index(":named").unwrap();
    stmt.bind_at(i, OwnedValue::Integer(42));

    stmt.bind_at(3.try_into()?, OwnedValue::from_blob(vec![0x1, 0x2, 0x3]));

    stmt.bind_at(4.try_into()?, OwnedValue::Float(0.5));

    assert_eq!(stmt.parameters().count(), 4);

    loop {
        match stmt.step()? {
            StepResult::Row => {
                let row = stmt.row().unwrap();
                if let limbo_core::OwnedValue::Text(s) = row.get::<&OwnedValue>(0).unwrap() {
                    assert_eq!(s.as_str(), "hello")
                }

                if let limbo_core::OwnedValue::Text(s) = row.get::<&OwnedValue>(1).unwrap() {
                    assert_eq!(s.as_str(), "hello")
                }

                if let limbo_core::OwnedValue::Integer(i) = row.get::<&OwnedValue>(2).unwrap() {
                    assert_eq!(*i, 42)
                }

                if let limbo_core::OwnedValue::Blob(v) = row.get::<&OwnedValue>(3).unwrap() {
                    assert_eq!(v.as_slice(), &vec![0x1 as u8, 0x2, 0x3])
                }

                if let limbo_core::OwnedValue::Float(f) = row.get::<&OwnedValue>(4).unwrap() {
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
fn test_bind_parameters_update_query() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new_with_rusqlite("create table test (a integer, b text);");
    let conn = tmp_db.connect_limbo();
    let mut ins = conn.prepare("insert into test (a, b) values (3, 'test1');")?;
    loop {
        match ins.step()? {
            StepResult::IO => tmp_db.io.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
            _ => {}
        }
    }
    let mut ins = conn.prepare("update test set a = ? where b = ?;")?;
    ins.bind_at(1.try_into()?, OwnedValue::Integer(222));
    ins.bind_at(2.try_into()?, OwnedValue::build_text("test1"));
    loop {
        match ins.step()? {
            StepResult::IO => tmp_db.io.run_once()?,
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
                assert_eq!(
                    row.get::<&OwnedValue>(0).unwrap(),
                    &OwnedValue::Integer(222)
                );
                assert_eq!(
                    row.get::<&OwnedValue>(1).unwrap(),
                    &OwnedValue::build_text("test1"),
                );
            }
            StepResult::IO => tmp_db.io.run_once()?,
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
            StepResult::IO => tmp_db.io.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
            _ => {}
        }
    }
    let mut ins = conn.prepare("update test set a = ? where b = ? and c = 4 and d = ?;")?;
    ins.bind_at(1.try_into()?, OwnedValue::Integer(222));
    ins.bind_at(2.try_into()?, OwnedValue::build_text("test1"));
    ins.bind_at(3.try_into()?, OwnedValue::Integer(5));
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
                assert_eq!(
                    row.get::<&OwnedValue>(0).unwrap(),
                    &OwnedValue::Integer(222)
                );
                assert_eq!(
                    row.get::<&OwnedValue>(1).unwrap(),
                    &OwnedValue::build_text("test1"),
                );
                assert_eq!(row.get::<&OwnedValue>(2).unwrap(), &OwnedValue::Integer(4));
                assert_eq!(row.get::<&OwnedValue>(3).unwrap(), &OwnedValue::Integer(5));
            }
            StepResult::IO => tmp_db.io.run_once()?,
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
            StepResult::IO => tmp_db.io.run_once()?,
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
                assert_eq!(row.get::<&OwnedValue>(0).unwrap(), &OwnedValue::Integer(1));
                assert_eq!(
                    row.get::<&OwnedValue>(1).unwrap(),
                    &OwnedValue::build_text("test"),
                );
            }
            StepResult::IO => tmp_db.io.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
        }
    }
    let mut ins = conn.prepare("update test set name = ? where id = ?;")?;
    ins.bind_at(1.try_into()?, OwnedValue::build_text("updated"));
    ins.bind_at(2.try_into()?, OwnedValue::Integer(1));
    loop {
        match ins.step()? {
            StepResult::IO => tmp_db.io.run_once()?,
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
                assert_eq!(row.get::<&OwnedValue>(0).unwrap(), &OwnedValue::Integer(1));
                assert_eq!(
                    row.get::<&OwnedValue>(1).unwrap(),
                    &OwnedValue::build_text("updated"),
                );
            }
            StepResult::IO => tmp_db.io.run_once()?,
            StepResult::Done | StepResult::Interrupt => break,
            StepResult::Busy => panic!("database busy"),
        }
    }
    assert_eq!(ins.parameters().count(), 2);
    Ok(())
}
