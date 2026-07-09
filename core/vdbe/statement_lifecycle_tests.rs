use std::collections::HashSet;

use crate::io::{MemoryIO, PlatformIO, IO};
use crate::mvcc::cursor::CursorYieldPoint;
use crate::mvcc::yield_hooks::YieldPointMarker;
use crate::mvcc::yield_points::{YieldInjector, YieldPoint};
use crate::sync::{Arc, Mutex};
use crate::{Connection, Database, DatabaseOpts, LimboError, OpenFlags, Result, Value};

#[derive(Debug)]
struct FixedYieldInjector {
    remaining: Mutex<HashSet<YieldPoint>>,
}

impl FixedYieldInjector {
    fn new(points: impl IntoIterator<Item = YieldPoint>) -> Arc<Self> {
        Arc::new(Self {
            remaining: Mutex::new(points.into_iter().collect()),
        })
    }
}

impl YieldInjector for FixedYieldInjector {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        self.remaining.lock().remove(&point)
    }
}

fn drive_attach(conn: &Arc<Connection>, path: &str, alias: &str) {
    let mut state = crate::connection::AttachDatabaseState::default();
    loop {
        match conn.attach_database(path, alias, &mut state).unwrap() {
            crate::IOResult::Done(()) => return,
            crate::IOResult::IO(io) => io.wait(conn.db.io.as_ref()).unwrap(),
        }
    }
}

fn get_rows(conn: &Arc<Connection>, query: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn.prepare(query).unwrap();
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        let values = row.get_values().cloned().collect::<Vec<_>>();
        rows.push(values);
        Ok(())
    })
    .unwrap();
    rows
}

fn open_mvcc_database_with_opts(path: &str, opts: DatabaseOpts) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(io, path, OpenFlags::default(), opts, None).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.close().unwrap();
    db
}

struct SameConnectionMvcc {
    conn: Arc<Connection>,
    observer: Arc<Connection>,
}

struct SameConnectionWal {
    conn: Arc<Connection>,
    observer: Arc<Connection>,
}

impl SameConnectionWal {
    fn new(path: &str) -> Self {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, path).unwrap();
        let conn = db.connect().unwrap();
        let observer = db.connect().unwrap();
        Self { conn, observer }
    }

    fn setup_rows_table(&self) {
        self.conn
            .execute("CREATE TABLE rows(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
    }
}

impl SameConnectionMvcc {
    fn new(path: &str) -> Self {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, path).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        let observer = db.connect().unwrap();
        Self { conn, observer }
    }

    fn setup_rows_table(&self) {
        self.conn
            .execute("CREATE TABLE rows(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
    }

    fn setup_rows_and_source_tables(&self) {
        self.setup_rows_table();
        self.conn
            .execute("CREATE TABLE src(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        self.conn
            .execute("INSERT INTO src VALUES (2, 'src-two')")
            .unwrap();
        self.conn
            .execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
    }

    fn observer_ids(&self) -> Vec<i64> {
        ids_from_query(&self.observer, "SELECT id FROM rows ORDER BY id")
    }

    fn observer_count(&self) -> i64 {
        scalar_i64(&self.observer, "SELECT COUNT(*) FROM rows")
    }

    fn observer_value_for_id(&self, id: i64) -> String {
        scalar_text(
            &self.observer,
            &format!("SELECT v FROM rows WHERE id = {id}"),
        )
    }
}

fn scalar_i64(conn: &Arc<Connection>, sql: &str) -> i64 {
    let rows = get_rows(conn, sql);
    assert_eq!(rows.len(), 1, "expected one row for {sql}, got {rows:?}");
    rows[0][0]
        .as_int()
        .unwrap_or_else(|| panic!("expected integer scalar for {sql}, got {:?}", rows[0][0]))
}

fn scalar_text(conn: &Arc<Connection>, sql: &str) -> String {
    let rows = get_rows(conn, sql);
    assert_eq!(rows.len(), 1, "expected one row for {sql}, got {rows:?}");
    rows[0][0].to_string()
}

fn ids_from_query(conn: &Arc<Connection>, sql: &str) -> Vec<i64> {
    get_rows(conn, sql)
        .into_iter()
        .map(|row| {
            row[0]
                .as_int()
                .unwrap_or_else(|| panic!("expected integer id for {sql}, got {:?}", row[0]))
        })
        .collect()
}

fn prepare_insert_returning(conn: &Arc<Connection>, id: i64, value: &str) -> crate::Statement {
    conn.prepare(format!(
        "INSERT INTO rows VALUES ({id}, '{value}') RETURNING id"
    ))
    .unwrap()
}

fn step_returning_id(stmt: &mut crate::Statement) -> i64 {
    match stmt.step().unwrap() {
        crate::StepResult::Row => stmt.row().unwrap().get::<i64>(0).unwrap(),
        other => panic!("expected RETURNING row, got {other:?}"),
    }
}

fn expect_step_busy(stmt: &mut crate::Statement, context: &str) {
    match stmt.step() {
        Ok(crate::StepResult::Busy) | Err(LimboError::Busy) => {}
        Ok(result) => panic!("expected Busy for {context}, got {result:?}"),
        Err(err) => panic!("expected Busy for {context}, got {err:?}"),
    }
}

fn expect_busy(result: Result<()>, context: &str) {
    let err = result.expect_err(context);
    assert!(
        matches!(err, LimboError::Busy),
        "expected Busy for {context}, got {err:?}"
    );
}

fn expect_unfinished_write_commit_error(result: Result<()>) {
    let err = result.expect_err("COMMIT should reject an abandoned unfinished write");
    assert!(
        matches!(&err, LimboError::TxError(message) if message.contains("unfinished write statement was abandoned")),
        "expected unfinished-write TxError, got {err:?}"
    );
}

/// Step until the injected yield suspends the statement mid-execution,
/// driving any genuine I/O encountered before the injection point. Injected
/// yields surface as `StepResult::Yield` (explicit yields are not stored as
/// pending I/O), so the statement stays parked at the same PC afterwards.
fn expect_injected_yield(stmt: &mut crate::Statement, context: &str) {
    loop {
        match stmt.step().unwrap() {
            crate::StepResult::Yield => return,
            crate::StepResult::IO => stmt.get_pager().io.step().unwrap(),
            other => panic!("expected injected yield during {context}, got {other:?}"),
        }
    }
}

fn finish_without_rows(stmt: &mut crate::Statement) {
    loop {
        match stmt.step().unwrap() {
            crate::StepResult::Done => return,
            crate::StepResult::IO => stmt.get_pager().io.step().unwrap(),
            crate::StepResult::Row => panic!("expected statement to finish without more rows"),
            other => panic!("expected statement to finish, got {other:?}"),
        }
    }
}

fn drain_returning_ids(stmt: &mut crate::Statement) -> Vec<i64> {
    let mut ids = Vec::new();
    loop {
        match stmt.step().unwrap() {
            crate::StepResult::Done => return ids,
            crate::StepResult::IO => stmt.get_pager().io.step().unwrap(),
            crate::StepResult::Row => ids.push(stmt.row().unwrap().get::<i64>(0).unwrap()),
            other => panic!("expected RETURNING rows or Done, got {other:?}"),
        }
    }
}

fn prepare_yielding_insert_select(env: &SameConnectionMvcc) -> crate::Statement {
    prepare_yielding_insert_select_sql(env, "INSERT INTO rows SELECT id, v FROM src")
}

fn prepare_yielding_insert_select_sql(env: &SameConnectionMvcc, sql: &str) -> crate::Statement {
    prepare_yielding_statement(&env.conn, sql, CursorYieldPoint::NextStart, "INSERT SELECT")
}

/// Prepare a statement, force it to suspend at one cursor yield point, then
/// disable injection so later steps exercise normal resume/cleanup behavior.
fn prepare_yielding_statement(
    conn: &Arc<Connection>,
    sql: impl AsRef<str>,
    yield_point: CursorYieldPoint,
    context: &str,
) -> crate::Statement {
    conn.set_yield_injector(Some(FixedYieldInjector::new([yield_point.point()])));
    let mut stmt = conn.prepare(sql).unwrap();
    expect_injected_yield(&mut stmt, context);
    conn.set_yield_injector(None);
    stmt
}

fn prepare_yielding_update_all_rows(conn: &Arc<Connection>, value: &str) -> crate::Statement {
    prepare_yielding_statement(
        conn,
        format!("UPDATE rows SET v = '{value}'"),
        CursorYieldPoint::NextStart,
        "UPDATE all rows",
    )
}

fn prepare_wal_update_yielding_on_table_read(
    env: &SameConnectionWal,
    value: &str,
) -> crate::Statement {
    let root_page = scalar_i64(
        &env.conn,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 'rows'",
    );
    env.conn.get_pager().arm_spill_yield_on_read(root_page, 0);
    let mut stmt = env
        .conn
        .prepare(format!("UPDATE rows SET v = '{value}'"))
        .unwrap();
    expect_injected_yield(&mut stmt, "WAL UPDATE table read");
    stmt
}

#[test]
fn test_returning_owner_drop_does_not_commit_interrupted_drop_table() {
    let io = Arc::new(MemoryIO::new());
    let path = ":memory:returning-owner-interrupted-drop-table";
    let db = Database::open_file(io.clone(), path).unwrap();
    let conn = db.connect().unwrap();

    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.execute(
        "CREATE TABLE core(\
            id INTEGER PRIMARY KEY, \
            row_number INTEGER, \
            deletion_timestamp INTEGER\
        )",
    )
    .unwrap();
    conn.execute("CREATE TABLE other(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.execute(
        "CREATE UNIQUE INDEX core_active_row_number_index \
         ON core (row_number) WHERE deletion_timestamp IS NULL",
    )
    .unwrap();

    let before = get_rows(
        &conn,
        "SELECT type, name FROM sqlite_schema \
         WHERE tbl_name = 'core' ORDER BY rowid",
    );
    assert_eq!(before.len(), 2);
    assert_eq!(before[0][0].to_string(), "table");
    assert_eq!(before[0][1].to_string(), "core");
    assert_eq!(before[1][0].to_string(), "index");
    assert_eq!(before[1][1].to_string(), "core_active_row_number_index");

    let mut returning_owner = conn
        .prepare("INSERT INTO other VALUES (1) RETURNING id")
        .unwrap();
    match returning_owner.step().unwrap() {
        crate::StepResult::Row => {
            let row = returning_owner.row().unwrap();
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
        }
        other => panic!("expected INSERT RETURNING to yield its row; got {other:?}"),
    }

    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CursorYieldPoint::NextStart.point()
    ])));
    let mut drop_stmt = conn.prepare("DROP TABLE core").unwrap();
    expect_step_busy(&mut drop_stmt, "DROP TABLE while RETURNING is active");
    conn.set_yield_injector(None);

    drop(returning_owner);
    drop(drop_stmt);
    drop(conn);
    drop(db);

    let db = Database::open_file(io, path).expect(
        "reopen should not fail; dropping a RETURNING owner must not commit another statement's interrupted DROP",
    );
    let conn = db.connect().unwrap();
    let after = get_rows(
        &conn,
        "SELECT type, name FROM sqlite_schema \
         WHERE tbl_name = 'core' ORDER BY rowid",
    );
    assert_eq!(after.len(), 2, "schema must not be half-dropped: {after:?}");
    assert_eq!(after[0][0].to_string(), "table");
    assert_eq!(after[0][1].to_string(), "core");
    assert_eq!(after[1][0].to_string(), "index");
    assert_eq!(after[1][1].to_string(), "core_active_row_number_index");
}

#[test]
fn test_drop_table_while_returning_active_is_table_locked() {
    let env = SameConnectionMvcc::new(":memory:drop-table-returning-active-locked");
    env.conn
        .execute("CREATE TABLE core(id INTEGER PRIMARY KEY)")
        .unwrap();
    env.conn
        .execute("CREATE TABLE other(id INTEGER PRIMARY KEY)")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut returning = env
        .conn
        .prepare("INSERT INTO other VALUES (1), (2) RETURNING id")
        .unwrap();
    assert_eq!(step_returning_id(&mut returning), 1);

    let mut dropper = env.conn.prepare("DROP TABLE core").unwrap();
    expect_step_busy(&mut dropper, "DROP TABLE while RETURNING is active");
    drop(dropper);

    assert_eq!(
        scalar_i64(
            &env.observer,
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'core'"
        ),
        1,
        "failed DROP must leave the table schema intact"
    );
    assert_eq!(
        scalar_i64(&env.observer, "SELECT COUNT(*) FROM other"),
        0,
        "failed DROP must not commit the active RETURNING writer"
    );

    assert_eq!(step_returning_id(&mut returning), 2);
    finish_without_rows(&mut returning);
    assert_eq!(scalar_i64(&env.observer, "SELECT COUNT(*) FROM other"), 2);
}

#[test]
fn test_second_returning_writer_is_busy_and_first_can_commit_after_second_drop() {
    let env = SameConnectionMvcc::new(":memory:multi-returning-first-then-second");
    env.setup_rows_table();

    let mut first = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut first), 1);
    let mut second = prepare_insert_returning(&env.conn, 2, "two");
    expect_step_busy(&mut second, "second RETURNING writer");

    assert_eq!(env.observer_ids(), Vec::<i64>::new());
    drop(second);
    drop(first);
    assert_eq!(env.observer_ids(), vec![1]);
}

#[test]
fn test_second_returning_writer_is_busy_and_first_can_commit_after_first_drop() {
    let env = SameConnectionMvcc::new(":memory:multi-returning-second-then-first");
    env.setup_rows_table();

    let mut first = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut first), 1);
    let mut second = prepare_insert_returning(&env.conn, 2, "two");
    expect_step_busy(&mut second, "second RETURNING writer");

    drop(second);
    assert_eq!(env.observer_ids(), Vec::<i64>::new());
    drop(first);
    assert_eq!(env.observer_ids(), vec![1]);
}

#[test]
fn test_busy_second_returning_writer_does_not_block_first_reset_commit() {
    let env = SameConnectionMvcc::new(":memory:returning-reset-defers");
    env.setup_rows_table();

    let mut first = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut first), 1);
    let mut second = prepare_insert_returning(&env.conn, 2, "two");
    expect_step_busy(&mut second, "second RETURNING writer");

    drop(second);
    first.reset().unwrap();
    assert_eq!(env.observer_ids(), vec![1]);
}

#[test]
fn test_plain_insert_done_does_not_commit_while_returning_writer_active() {
    let env = SameConnectionMvcc::new(":memory:plain-insert-waits-for-returning");
    env.setup_rows_table();

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    expect_busy(
        env.conn.execute("INSERT INTO rows VALUES (2, 'plain-two')"),
        "plain INSERT while RETURNING is active",
    );

    assert_eq!(
        env.observer_ids(),
        Vec::<i64>::new(),
        "completed plain INSERT must not commit the shared tx while RETURNING is active"
    );
    drop(returning);
    assert_eq!(env.observer_ids(), vec![1]);
}

#[test]
fn test_same_connection_select_does_not_commit_active_returning_writer() {
    let env = SameConnectionMvcc::new(":memory:select-does-not-commit-returning");
    env.setup_rows_table();

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);

    assert_eq!(
        scalar_i64(&env.conn, "SELECT COUNT(*) FROM rows"),
        1,
        "the owning connection sees its uncommitted write"
    );
    assert_eq!(
        env.observer_count(),
        0,
        "a read statement must not finalize the active implicit write tx"
    );
    drop(returning);
    assert_eq!(env.observer_ids(), vec![1]);
}

#[test]
fn test_suspended_read_does_not_finish_joined_returning_writer() {
    let env = SameConnectionMvcc::new(":memory:suspended-read-joined-writer");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'existing')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut reader = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut reader), 10);
    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);

    drop(reader);
    assert_eq!(
        env.observer_ids(),
        vec![10],
        "dropping the read that opened the transaction must not finish the joined writer"
    );
    drop(returning);
    assert_eq!(env.observer_ids(), vec![1, 10]);
}

#[test]
fn test_completed_writer_waits_for_sibling_mvcc_reader_to_commit() {
    let env = SameConnectionMvcc::new(":memory:completed-writer-waits-for-reader");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'existing')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut reader = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut reader), 10);

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    finish_without_rows(&mut returning);
    drop(returning);

    assert_eq!(
        env.observer_ids(),
        vec![10],
        "completed writer must not commit while the reader still holds the shared MVCC transaction"
    );

    finish_without_rows(&mut reader);
    assert_eq!(
        env.observer_ids(),
        vec![1, 10],
        "the last sibling statement should commit the completed writer's changes"
    );
}

#[test]
fn test_suspended_read_does_not_finish_sibling_read_transaction() {
    let env = SameConnectionMvcc::new(":memory:suspended-read-sibling-read");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'ten'), (20, 'twenty')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut first = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut first), 10);
    let mut second = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut second), 10);

    drop(first);
    assert_eq!(
        step_returning_id(&mut second),
        20,
        "dropping one read must not close the transaction under a sibling read"
    );
    finish_without_rows(&mut second);
    assert_eq!(env.conn.get_mv_tx(), None);
    assert_eq!(
        env.conn.get_tx_state(),
        crate::connection::TransactionState::None
    );
}

#[test]
fn test_wal_returning_writer_can_commit_while_sibling_reader_is_active() {
    let env = SameConnectionWal::new(":memory:wal-reader-active-writer-commit");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'ten')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut reader = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut reader), 10);

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    finish_without_rows(&mut returning);

    assert_eq!(
        ids_from_query(&env.observer, "SELECT id FROM rows ORDER BY id"),
        vec![1, 10],
        "WAL mode follows SQLite's only-active-writer rule: the writer can commit while a reader is still active"
    );
    finish_without_rows(&mut reader);
}

#[test]
fn test_wal_begin_immediate_does_not_count_as_active_writer() {
    let env = SameConnectionWal::new(":memory:wal-begin-immediate-not-active-writer");
    env.setup_rows_table();

    let mut begin = env.conn.prepare("BEGIN IMMEDIATE").unwrap();
    finish_without_rows(&mut begin);

    let mut insert = env
        .conn
        .prepare("INSERT INTO rows VALUES (1, 'one')")
        .unwrap();
    finish_without_rows(&mut insert);
    assert_eq!(
        scalar_i64(&env.observer, "SELECT COUNT(*) FROM rows"),
        0,
        "BEGIN IMMEDIATE opens the transaction, but the INSERT is still uncommitted"
    );

    env.conn.execute("COMMIT").unwrap();
    assert_eq!(
        ids_from_query(&env.observer, "SELECT id FROM rows ORDER BY id"),
        vec![1]
    );
}

#[test]
fn test_wal_dropping_reader_does_not_rollback_active_returning_writer() {
    let env = SameConnectionWal::new(":memory:wal-drop-reader-active-writer");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'ten')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut reader = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut reader), 10);
    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);

    drop(reader);
    assert_eq!(
        ids_from_query(&env.observer, "SELECT id FROM rows ORDER BY id"),
        vec![10],
        "dropping the reader must not commit the active writer"
    );

    finish_without_rows(&mut returning);
    assert_eq!(
        ids_from_query(&env.observer, "SELECT id FROM rows ORDER BY id"),
        vec![1, 10]
    );
}

#[test]
fn test_wal_dropping_one_reader_does_not_close_sibling_reader_transaction() {
    let env = SameConnectionWal::new(":memory:wal-sibling-readers");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'ten'), (20, 'twenty')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut first = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut first), 10);
    let mut second = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut second), 10);

    drop(first);
    assert_eq!(
        step_returning_id(&mut second),
        20,
        "dropping one reader must not close the transaction under a sibling reader"
    );
    finish_without_rows(&mut second);
}

#[test]
fn test_insert_select_is_busy_while_returning_writer_is_active() {
    let env = SameConnectionMvcc::new(":memory:suspended-insert-select-resume");
    env.setup_rows_and_source_tables();

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    let mut insert_select = env
        .conn
        .prepare("INSERT INTO rows SELECT id, v FROM src")
        .unwrap();
    expect_step_busy(
        &mut insert_select,
        "INSERT SELECT while RETURNING is active",
    );

    drop(insert_select);
    drop(returning);
    assert_eq!(env.observer_ids(), vec![1]);
}

#[test]
fn test_rejected_insert_select_does_not_roll_back_active_returning_writer() {
    let env = SameConnectionMvcc::new(":memory:suspended-insert-select-abandon");
    env.setup_rows_and_source_tables();

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    let mut insert_select = env
        .conn
        .prepare("INSERT INTO rows SELECT id, v FROM src")
        .unwrap();
    expect_step_busy(
        &mut insert_select,
        "INSERT SELECT while RETURNING is active",
    );

    drop(insert_select);
    drop(returning);
    assert_eq!(
        env.observer_ids(),
        vec![1],
        "rejected sibling writer must not roll back the active RETURNING writer"
    );
}

#[test]
fn test_first_writer_without_statement_savepoint_abandon_rolls_back_joined_writer() {
    let env = SameConnectionMvcc::new(":memory:first-writer-without-savepoint-abandon");
    env.setup_rows_and_source_tables();

    let insert_select = prepare_yielding_insert_select(&env);
    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    expect_step_busy(
        &mut returning,
        "RETURNING writer while INSERT SELECT is active",
    );

    drop(insert_select);
    drop(returning);

    assert_eq!(
        env.observer_ids(),
        Vec::<i64>::new(),
        "abandoning a non-final writer without a local rollback boundary must roll back the shared tx"
    );
}

#[test]
fn test_first_writer_without_statement_savepoint_resume_last_commits_joined_writer() {
    let env = SameConnectionMvcc::new(":memory:first-writer-without-savepoint-resume");
    env.setup_rows_and_source_tables();

    let mut insert_select = prepare_yielding_insert_select(&env);
    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    expect_step_busy(
        &mut returning,
        "RETURNING writer while INSERT SELECT is active",
    );

    drop(returning);
    finish_without_rows(&mut insert_select);

    assert_eq!(env.observer_ids(), vec![2]);
}

#[test]
fn test_first_writer_without_statement_savepoint_error_rolls_back_joined_writer() {
    let env = SameConnectionMvcc::new(":memory:first-writer-without-savepoint-error");
    env.setup_rows_table();
    env.conn
        .execute("CREATE TABLE src(id INTEGER, v TEXT)")
        .unwrap();
    env.conn
        .execute("INSERT INTO rows VALUES (1, 'existing')")
        .unwrap();
    env.conn
        .execute("INSERT INTO src VALUES (2, 'src-two'), (1, 'duplicate')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut insert_select =
        prepare_yielding_insert_select_sql(&env, "INSERT INTO rows SELECT id, v FROM src");
    let mut returning = prepare_insert_returning(&env.conn, 3, "joined");
    expect_step_busy(
        &mut returning,
        "RETURNING writer while INSERT SELECT is active",
    );

    let err = insert_select
        .step()
        .expect_err("resumed INSERT SELECT should hit duplicate primary key");
    assert!(
        matches!(err, LimboError::Constraint(_)),
        "expected duplicate-key constraint error, got {err:?}"
    );
    drop(insert_select);
    drop(returning);

    assert_eq!(
        env.observer_ids(),
        vec![1],
        "an error from a writer without a local rollback boundary must roll back the whole shared tx"
    );
}

#[test]
fn test_attached_only_returning_writer_defers_shared_auto_txn_commit() {
    let main_dir = tempfile::TempDir::new().unwrap();
    let main_path = main_dir.path().join("main.db");
    let db = open_mvcc_database_with_opts(
        main_path.to_str().unwrap(),
        DatabaseOpts::new().with_attach(true),
    );
    let aux_dir = tempfile::TempDir::new().unwrap();
    let aux_path = aux_dir.path().join("aux.db");
    let aux_path = aux_path.to_str().unwrap();

    let conn = db.connect().unwrap();
    drive_attach(&conn, aux_path, "aux");
    conn.execute("CREATE TABLE aux.rows(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let observer = db.connect().unwrap();
    drive_attach(&observer, aux_path, "aux");

    let mut returning = conn
        .prepare("INSERT INTO aux.rows VALUES (1, 'one'), (2, 'two') RETURNING id")
        .unwrap();
    assert_eq!(step_returning_id(&mut returning), 1);

    expect_busy(
        conn.execute("INSERT INTO aux.rows VALUES (3, 'plain')"),
        "attached INSERT while attached RETURNING is active",
    );
    assert_eq!(
        ids_from_query(&observer, "SELECT id FROM aux.rows ORDER BY id"),
        Vec::<i64>::new(),
        "rejected attached writer must not commit the active RETURNING writer"
    );

    assert_eq!(drain_returning_ids(&mut returning), vec![2]);
    assert_eq!(
        ids_from_query(&observer, "SELECT id FROM aux.rows ORDER BY id"),
        vec![1, 2]
    );
}

#[test]
fn test_mvcc_explicit_tx_unfinished_writer_poisons_transaction() {
    let env = SameConnectionMvcc::new(":memory:explicit-unfinished-writer-poison");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'ten'), (20, 'twenty')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    env.conn.execute("BEGIN").unwrap();

    let older = prepare_yielding_update_all_rows(&env.conn, "older");
    let mut newer = env
        .conn
        .prepare("UPDATE rows SET v = 'newer' RETURNING id")
        .unwrap();
    expect_step_busy(&mut newer, "second UPDATE writer in explicit transaction");

    drop(newer);
    drop(older);
    expect_unfinished_write_commit_error(env.conn.execute("COMMIT"));

    assert_eq!(
        ids_from_query(&env.observer, "SELECT id FROM rows ORDER BY id"),
        vec![10, 20]
    );
    assert_eq!(env.observer_value_for_id(10), "ten");
    assert_eq!(env.observer_value_for_id(20), "twenty");

    env.conn
        .execute("INSERT INTO rows VALUES (30, 'thirty')")
        .unwrap();
    assert_eq!(
        ids_from_query(&env.observer, "SELECT id FROM rows ORDER BY id"),
        vec![10, 20, 30]
    );
}

#[test]
fn test_wal_explicit_tx_unfinished_writer_poisons_transaction() {
    let env = SameConnectionWal::new(":memory:wal-explicit-unfinished-writer-poison");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'ten'), (20, 'twenty')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    env.conn.execute("BEGIN").unwrap();

    let writer = prepare_wal_update_yielding_on_table_read(&env, "partial");
    drop(writer);
    expect_unfinished_write_commit_error(env.conn.execute("COMMIT"));

    assert_eq!(
        ids_from_query(&env.observer, "SELECT id FROM rows ORDER BY id"),
        vec![10, 20]
    );
    assert_eq!(
        scalar_text(&env.observer, "SELECT v FROM rows WHERE id = 10"),
        "ten"
    );
    assert_eq!(
        scalar_text(&env.observer, "SELECT v FROM rows WHERE id = 20"),
        "twenty"
    );
}

#[test]
fn test_explicit_tx_rollback_clears_unfinished_writer_poison() {
    let env = SameConnectionMvcc::new(":memory:explicit-unfinished-writer-rollback-clear");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'ten'), (20, 'twenty')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    env.conn.execute("BEGIN").unwrap();

    let writer = prepare_yielding_update_all_rows(&env.conn, "partial");
    drop(writer);
    env.conn.execute("ROLLBACK").unwrap();

    env.conn
        .execute("INSERT INTO rows VALUES (30, 'thirty')")
        .unwrap();
    assert_eq!(
        ids_from_query(&env.observer, "SELECT id FROM rows ORDER BY id"),
        vec![10, 20, 30]
    );
}

#[test]
fn test_unfinished_drop_abandon_first_rolls_back_only_drop() {
    let env = SameConnectionMvcc::new(":memory:unfinished-drop-first");
    env.conn
        .execute("CREATE TABLE core(id INTEGER PRIMARY KEY)")
        .unwrap();
    env.setup_rows_table();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    let mut dropper = env.conn.prepare("DROP TABLE core").unwrap();
    expect_step_busy(&mut dropper, "DROP TABLE while RETURNING is active");

    drop(dropper);
    drop(returning);

    assert_eq!(
        env.observer_ids(),
        vec![1],
        "rejected DROP must not roll back the active RETURNING writer"
    );
    assert_eq!(
        scalar_i64(
            &env.observer,
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'core'"
        ),
        1
    );
}

#[test]
fn test_update_is_busy_while_returning_writer_is_active() {
    let env = SameConnectionMvcc::new(":memory:unfinished-update-abandon");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'original')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    let mut updater = env.conn.prepare("UPDATE rows SET v = 'updated'").unwrap();
    expect_step_busy(&mut updater, "UPDATE while RETURNING is active");

    drop(returning);
    drop(updater);

    assert_eq!(env.observer_ids(), vec![10]);
    assert_eq!(env.observer_value_for_id(10), "original");
}

#[test]
fn test_delete_is_busy_while_returning_writer_is_active() {
    let env = SameConnectionMvcc::new(":memory:unfinished-delete-abandon");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'original')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    let mut deleter = env.conn.prepare("DELETE FROM rows WHERE id = 10").unwrap();
    expect_step_busy(&mut deleter, "DELETE while RETURNING is active");

    drop(returning);
    drop(deleter);

    assert_eq!(env.observer_ids(), vec![10]);
    assert_eq!(env.observer_value_for_id(10), "original");
}

#[test]
fn test_duplicate_insert_is_busy_while_returning_writer_is_active() {
    let env = SameConnectionMvcc::new(":memory:constraint-error-rolls-back-failing");
    env.setup_rows_table();

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);

    expect_busy(
        env.conn.execute("INSERT INTO rows VALUES (1, 'duplicate')"),
        "duplicate INSERT while RETURNING is active",
    );
    drop(returning);

    assert_eq!(
        env.observer_ids(),
        vec![1],
        "rejected duplicate writer must not roll back the active RETURNING writer"
    );
}

#[test]
fn test_rejected_second_returning_and_drop_do_not_rollback_first_returning() {
    let env = SameConnectionMvcc::new(":memory:two-returning-then-unfinished-drop");
    env.conn
        .execute("CREATE TABLE core(id INTEGER PRIMARY KEY)")
        .unwrap();
    env.setup_rows_table();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut first = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut first), 1);
    let mut second = prepare_insert_returning(&env.conn, 2, "two");
    expect_step_busy(&mut second, "second RETURNING writer");
    let mut dropper = env.conn.prepare("DROP TABLE core").unwrap();
    expect_step_busy(&mut dropper, "DROP TABLE while RETURNING is active");

    drop(second);
    drop(dropper);
    drop(first);

    assert_eq!(env.observer_ids(), vec![1]);
    assert_eq!(
        scalar_i64(
            &env.observer,
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'core'"
        ),
        1
    );
}

#[test]
fn test_insert_is_busy_while_update_returning_writer_is_active() {
    let env = SameConnectionMvcc::new(":memory:update-returning-plus-insert-returning");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'original')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut update = env
        .conn
        .prepare("UPDATE rows SET v = 'updated' WHERE id = 10 RETURNING id")
        .unwrap();
    assert_eq!(step_returning_id(&mut update), 10);
    let mut insert = prepare_insert_returning(&env.conn, 1, "one");
    expect_step_busy(&mut insert, "INSERT while UPDATE RETURNING is active");

    drop(insert);
    drop(update);
    assert_eq!(env.observer_ids(), vec![10]);
    assert_eq!(env.observer_value_for_id(10), "updated");
}

#[test]
fn test_insert_is_busy_while_delete_returning_writer_is_active() {
    let env = SameConnectionMvcc::new(":memory:delete-returning-plus-insert-returning");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'original')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut delete = env
        .conn
        .prepare("DELETE FROM rows WHERE id = 10 RETURNING id")
        .unwrap();
    assert_eq!(step_returning_id(&mut delete), 10);
    let mut insert = prepare_insert_returning(&env.conn, 1, "one");
    expect_step_busy(&mut insert, "INSERT while DELETE RETURNING is active");

    drop(insert);
    drop(delete);
    assert_eq!(env.observer_ids(), Vec::<i64>::new());
}

// The two tests below pin known data-loss gaps in the MVCC deferred-commit
// model; they document current behavior, not desired behavior. A writer that
// finishes while a sibling statement holds the shared implicit MVCC
// transaction open defers its commit to the last sibling, so any path that
// ends that transaction in a rollback silently discards changes whose caller
// already observed success. The durable fix is committing at the writer's own
// halt; see the FIXME in `halt` (core/vdbe/execute.rs).

#[test]
fn test_mvcc_completed_writer_changes_lost_when_last_reader_abandoned() {
    let env = SameConnectionMvcc::new(":memory:completed-writer-lost-reader-abandoned");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'existing')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut reader = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut reader), 10);

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    finish_without_rows(&mut returning);
    drop(returning);

    // The reader is abandoned mid-scan instead of finishing, so the shared
    // transaction ends through the rollback path and the completed writer's
    // row is lost. Compare test_completed_writer_waits_for_sibling_mvcc_reader_to_commit,
    // where the reader finishes normally and the writer's row commits.
    drop(reader);

    assert_eq!(
        env.observer_ids(),
        vec![10],
        "pins the deferred-commit gap: an abandoned last reader discards the completed writer's row"
    );
}

#[test]
fn test_mvcc_completed_writer_changes_lost_when_joining_writer_errors() {
    let env = SameConnectionMvcc::new(":memory:completed-writer-lost-joining-writer-error");
    env.setup_rows_table();
    env.conn
        .execute("CREATE TABLE src(id INTEGER, v TEXT)")
        .unwrap();
    env.conn
        .execute("INSERT INTO src VALUES (2, 'two'), (1, 'duplicate')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut reader = env.conn.prepare("SELECT id FROM src ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut reader), 1);

    env.conn
        .execute("INSERT INTO rows VALUES (1, 'one')")
        .unwrap();

    // The joining writer changes a row (id 2) before hitting the duplicate,
    // and has no local rollback boundary, so the whole shared transaction is
    // rolled back — including the earlier INSERT that already reported success.
    let err = env
        .conn
        .execute("INSERT INTO rows SELECT id, v FROM src")
        .expect_err("second insert should hit duplicate primary key");
    assert!(
        matches!(err, LimboError::Constraint(_)),
        "expected duplicate-key constraint error, got {err:?}"
    );

    drop(reader);
    assert_eq!(
        env.observer_ids(),
        Vec::<i64>::new(),
        "pins the deferred-commit gap: a failing joined writer discards the completed writer's row"
    );
}
