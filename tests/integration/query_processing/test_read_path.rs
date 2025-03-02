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
                assert_eq!(*row.get_value(0), limbo_core::OwnedValue::Integer(1));
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
                assert_eq!(*row.get_value(0), limbo_core::OwnedValue::Integer(2));
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
                if let limbo_core::OwnedValue::Text(s) = row.get_value(0) {
                    assert_eq!(s.as_str(), "hello")
                }

                if let limbo_core::OwnedValue::Text(s) = row.get_value(1) {
                    assert_eq!(s.as_str(), "hello")
                }

                if let limbo_core::OwnedValue::Integer(i) = row.get_value(2) {
                    assert_eq!(*i, 42)
                }

                if let limbo_core::OwnedValue::Blob(v) = row.get_value(3) {
                    assert_eq!(v.as_ref(), &vec![0x1 as u8, 0x2, 0x3])
                }

                if let limbo_core::OwnedValue::Float(f) = row.get_value(4) {
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
