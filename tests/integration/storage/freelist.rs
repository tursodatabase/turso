use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::common::{maybe_setup_tracing, TempDatabase};
use turso_core::mvcc::yield_points::{YieldInjector, YieldPoint};
use turso_core::storage::PagerYieldPoint;
use turso_core::{LimboError, Result, StepResult, Value};

#[derive(Debug)]
struct OnceYield {
    point: YieldPoint,
    yielded: AtomicBool,
}

impl OnceYield {
    const fn new(point: YieldPoint) -> Self {
        Self {
            point,
            yielded: AtomicBool::new(false),
        }
    }
}

impl YieldInjector for OnceYield {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        point == self.point && !self.yielded.swap(true, Ordering::SeqCst)
    }
}

#[allow(clippy::arc_with_non_send_sync)]
#[turso_macros::test]
fn abandoned_savepoint_delete_does_not_freelist_live_page(tmp_db: TempDatabase) -> Result<()> {
    maybe_setup_tracing();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA journal_mode=WAL;")?;
    conn.execute("CREATE TABLE calm_roof_888(id INTEGER PRIMARY KEY, cold_door_963 BLOB);")?;
    for id in 1..=3 {
        conn.execute(&format!(
            "INSERT INTO calm_roof_888 VALUES ({id}, zeroblob(9000));"
        ))?;
    }
    conn.execute("DELETE FROM calm_roof_888 WHERE id = 1;")?;

    conn.execute("BEGIN;")?;
    let duplicate = conn.execute("INSERT INTO calm_roof_888 VALUES (2, zeroblob(8395));");
    assert!(matches!(duplicate, Err(LimboError::Constraint(_))));
    conn.execute("SAVEPOINT sp;")?;

    conn.set_yield_injector(Some(Arc::new(OnceYield::new(
        PagerYieldPoint::FREE_PAGE_STATE_QUEUED,
    ))));
    let mut delete = conn.prepare("DELETE FROM calm_roof_888 WHERE id = 2;")?;
    match delete.step()? {
        StepResult::IO => {
            let _ = delete.take_io_completions();
        }
        other => panic!("DELETE should yield while a page is queued for the freelist: {other:?}"),
    }
    conn.set_yield_injector(None);

    drop(delete);
    conn.execute("ROLLBACK TO sp;")?;
    conn.execute("RELEASE sp;")?;
    conn.execute("DELETE FROM calm_roof_888 WHERE id = 3;")?;
    conn.execute("COMMIT;")?;

    let verify_db = TempDatabase::new_with_existent(&tmp_db.path);
    let verify_conn = verify_db.connect_limbo();
    let mut integrity = verify_conn.prepare("PRAGMA integrity_check;")?;
    let rows = integrity.run_collect_rows()?;
    assert_eq!(rows, vec![vec![Value::build_text("ok")]]);

    Ok(())
}

#[allow(clippy::arc_with_non_send_sync)]
#[turso_macros::test]
fn abandoned_overflow_allocation_does_not_drop_later_freelist_page(
    tmp_db: TempDatabase,
) -> Result<()> {
    maybe_setup_tracing();
    let conn = tmp_db.connect_limbo();

    conn.execute("PRAGMA journal_mode=WAL;")?;
    conn.execute("CREATE TABLE calm_roof_888(id INTEGER PRIMARY KEY, cold_door_963 BLOB);")?;
    for id in 1..=4 {
        conn.execute(&format!(
            "INSERT INTO calm_roof_888 VALUES ({id}, zeroblob(9000));"
        ))?;
    }
    conn.execute("DELETE FROM calm_roof_888 WHERE id = 1;")?;

    conn.execute("BEGIN;")?;
    conn.execute("SAVEPOINT sp;")?;

    conn.set_yield_injector(Some(Arc::new(OnceYield::new(
        PagerYieldPoint::ALLOCATE_PAGE_STATE_QUEUED,
    ))));
    let mut abandoned_insert =
        conn.prepare("INSERT INTO calm_roof_888 VALUES (5, zeroblob(9000));")?;
    match abandoned_insert.step()? {
        StepResult::IO => {
            let _ = abandoned_insert.take_io_completions();
        }
        other => panic!("INSERT should yield after queuing a freelist allocation: {other:?}"),
    }
    conn.set_yield_injector(None);
    drop(abandoned_insert);

    conn.execute("ROLLBACK TO sp;")?;
    conn.execute("DELETE FROM calm_roof_888 WHERE id = 2;")?;
    conn.execute("INSERT INTO calm_roof_888 VALUES (5, zeroblob(9000));")?;
    conn.execute("RELEASE sp;")?;
    conn.execute("COMMIT;")?;

    let verify_db = TempDatabase::new_with_existent(&tmp_db.path);
    let verify_conn = verify_db.connect_limbo();
    let mut integrity = verify_conn.prepare("PRAGMA integrity_check;")?;
    let rows = integrity.run_collect_rows()?;
    assert_eq!(rows, vec![vec![Value::build_text("ok")]]);

    Ok(())
}
