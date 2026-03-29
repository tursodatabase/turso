use crate::common::{ExecRows, TempDatabase};
use anyhow::Result;
use turso_core::LimboError;

#[turso_macros::test()]
fn nested_trigger_constraint_error_propagates_to_parent(tmp_db: TempDatabase) -> Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE outer_t(x INTEGER PRIMARY KEY)")?;
    conn.execute("CREATE TABLE inner_t(x INTEGER PRIMARY KEY)")?;
    conn.execute("INSERT INTO inner_t VALUES (1)")?;
    conn.execute(
        "CREATE TRIGGER outer_before_insert
         BEFORE INSERT ON outer_t
         BEGIN
             INSERT INTO inner_t VALUES (NEW.x);
         END",
    )?;

    let result = conn.execute("INSERT INTO outer_t VALUES (1)");
    assert!(
        matches!(result, Err(LimboError::Constraint(_))),
        "expected nested child constraint error, got {result:?}"
    );

    let outer_rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM outer_t ORDER BY x");
    let inner_rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM inner_t ORDER BY x");
    assert!(outer_rows.is_empty(), "outer row should be rolled back");
    assert_eq!(inner_rows, vec![(1,)]);

    Ok(())
}

#[turso_macros::test()]
fn nested_trigger_constraint_error_is_swallowed_under_or_ignore(
    tmp_db: TempDatabase,
) -> Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE outer_t(x INTEGER PRIMARY KEY)")?;
    conn.execute("CREATE TABLE inner_t(x INTEGER PRIMARY KEY)")?;
    conn.execute("INSERT INTO inner_t VALUES (1)")?;
    conn.execute(
        "CREATE TRIGGER outer_before_insert
         BEFORE INSERT ON outer_t
         BEGIN
             INSERT INTO inner_t VALUES (NEW.x);
         END",
    )?;

    conn.execute("INSERT OR IGNORE INTO outer_t VALUES (1)")?;

    let outer_rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM outer_t ORDER BY x");
    let inner_rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM inner_t ORDER BY x");
    assert_eq!(outer_rows, vec![(1,)]);
    assert_eq!(inner_rows, vec![(1,)]);

    Ok(())
}

#[turso_macros::test()]
fn nested_trigger_raise_ignore_skips_only_the_inner_row(tmp_db: TempDatabase) -> Result<()> {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE outer_t(x INTEGER PRIMARY KEY)")?;
    conn.execute("CREATE TABLE inner_t(x INTEGER PRIMARY KEY)")?;
    conn.execute(
        "CREATE TRIGGER outer_after_insert
         AFTER INSERT ON outer_t
         BEGIN
             INSERT INTO inner_t VALUES (NEW.x);
         END",
    )?;
    conn.execute(
        "CREATE TRIGGER inner_before_insert
         BEFORE INSERT ON inner_t
         WHEN NEW.x = 2
         BEGIN
             SELECT RAISE(IGNORE);
         END",
    )?;

    conn.execute("INSERT INTO outer_t VALUES (1)")?;
    conn.execute("INSERT INTO outer_t VALUES (2)")?;
    conn.execute("INSERT INTO outer_t VALUES (3)")?;

    let outer_rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM outer_t ORDER BY x");
    let inner_rows: Vec<(i64,)> = conn.exec_rows("SELECT x FROM inner_t ORDER BY x");
    assert_eq!(outer_rows, vec![(1,), (2,), (3,)]);
    assert_eq!(inner_rows, vec![(1,), (3,)]);

    Ok(())
}
