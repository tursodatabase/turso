use crate::common::TempDatabase;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use turso_core::vdbe::StepResult;

/// The classic non-terminating recursive CTE. It has no I/O and no natural bound, so it is the
/// worst case for every bounded-execution mechanism the engine offers.
const RUNAWAY: &str =
    "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c;";

fn run_until_terminal(stmt: &mut turso_core::Statement) -> turso_core::Result<StepResult> {
    loop {
        match stmt.step()? {
            StepResult::IO => stmt._io().step()?,
            StepResult::Row | StepResult::Yield => continue,
            result => return Ok(result),
        }
    }
}

/// A runaway recursive CTE must be bounded by the generic query deadline. The recursive fixed
/// point is ordinary bytecode with a back-edge, so it re-enters the step loop on every opcode and
/// the deadline check fires exactly as it does for a runaway cross join.
#[turso_macros::test]
fn runaway_recursive_cte_is_bounded_by_query_timeout(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    conn.set_query_timeout(Duration::from_millis(50));

    let mut stmt = conn.prepare(RUNAWAY)?;
    let result = run_until_terminal(&mut stmt)?;
    assert!(
        matches!(result, StepResult::Interrupt),
        "expected the query deadline to interrupt the runaway recursion, got {result:?}"
    );
    Ok(())
}

/// A runaway recursive CTE must be interruptible from another thread, which is the contract
/// `sqlite3_interrupt` provides and the only thing a CLI Ctrl-C handler can rely on.
#[turso_macros::test]
fn runaway_recursive_cte_is_interruptible_from_another_thread(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    let stepping_finished = Arc::new(AtomicBool::new(false));

    let interrupter = {
        let conn = conn.clone();
        let stepping_finished = stepping_finished.clone();
        std::thread::spawn(move || {
            // The request is dropped unless a root statement is active, so keep asking until
            // the stepping thread reports that it is done.
            while !stepping_finished.load(Ordering::SeqCst) {
                conn.interrupt();
                std::thread::sleep(Duration::from_millis(1));
            }
        })
    };

    let mut stmt = conn.prepare(RUNAWAY)?;
    let result = run_until_terminal(&mut stmt)?;
    stepping_finished.store(true, Ordering::SeqCst);
    interrupter.join().unwrap();

    assert!(
        matches!(result, StepResult::Interrupt),
        "expected interrupt, got {result:?}"
    );
    Ok(())
}

/// Legitimately deep recursion must keep working. SQLite completes this query, so bounding
/// recursion by a low iteration count would be a compatibility regression rather than a fix.
#[turso_macros::test]
fn deep_recursive_cte_still_completes(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    let mut stmt = conn.prepare(
        "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM seq WHERE x < 100000)
         SELECT count(*) FROM seq;",
    )?;
    let mut count = None;
    stmt.run_with_row_callback(|row| {
        count = Some(row.get::<i64>(0).unwrap());
        Ok(())
    })?;
    assert_eq!(count, Some(100000));
    Ok(())
}
