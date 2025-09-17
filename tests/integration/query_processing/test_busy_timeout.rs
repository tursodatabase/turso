use std::convert::TryInto;
use std::time::{Duration, Instant};

use crate::common::TempDatabase;
use turso_core::{StepResult, Value};

#[test]
fn busy_timeout_resets_after_statement_reset() -> anyhow::Result<()> {
    let tmp_db = TempDatabase::new("busy_timeout_reset.db", true);
    let conn1 = tmp_db.connect_limbo();
    let conn2 = tmp_db.connect_limbo();

    conn1.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")?;
    conn1.execute("BEGIN IMMEDIATE")?;
    conn1.execute("INSERT INTO test (id) VALUES (1)")?;

    conn2.busy_timeout(Some(Duration::from_millis(500)));

    let mut stmt = conn2.prepare("INSERT INTO test (id) VALUES (?)")?;

    let first_elapsed = {
        stmt.reset();
        stmt.bind_at(1.try_into()?, Value::Integer(2));
        let start = Instant::now();
        loop {
            match stmt.step()? {
                StepResult::IO => stmt.run_once()?,
                StepResult::Busy => break start.elapsed(),
                StepResult::Done | StepResult::Row => {
                    panic!("expected busy result while the write lock is held")
                }
                StepResult::Interrupt => panic!("unexpected interrupt"),
            }
        }
    };
    assert!(
        first_elapsed >= Duration::from_millis(250),
        "first attempt waited only {:?}",
        first_elapsed
    );

    let second_elapsed = {
        stmt.reset();
        stmt.bind_at(1.try_into()?, Value::Integer(3));
        let start = Instant::now();
        loop {
            match stmt.step()? {
                StepResult::IO => stmt.run_once()?,
                StepResult::Busy => break start.elapsed(),
                StepResult::Done | StepResult::Row => {
                    panic!("expected busy result while the write lock is held")
                }
                StepResult::Interrupt => panic!("unexpected interrupt"),
            }
        }
    };
    assert!(
        second_elapsed >= Duration::from_millis(250),
        "second attempt waited only {:?}",
        second_elapsed
    );

    conn1.execute("ROLLBACK")?;

    Ok(())
}
