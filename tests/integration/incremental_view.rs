use turso_core::security::roles::SessionRole;
use turso_core::sync::Arc;
use turso_core::ConnectOpts;
use turso_core::Database;
use turso_core::DatabaseOpts;
use turso_core::OpenFlags;
use turso_core::StepResult;
use turso_sqlite::SqliteDialect;

#[test]
fn aggregate_finalization_error_rolls_back_intermediate_state() {
    #[allow(clippy::arc_with_non_send_sync)]
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        "ivm-aggregate-finalization-rollback.db",
        OpenFlags::default(),
        DatabaseOpts::new().with_views(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE aggregate_base(x)").unwrap();
    conn.execute(
        "CREATE MATERIALIZED VIEW aggregate_view AS SELECT SUM(x) AS total FROM aggregate_base",
    )
    .unwrap();
    conn.execute("INSERT INTO aggregate_base VALUES (5)")
        .unwrap();

    let error = conn
        .execute("INSERT INTO aggregate_base VALUES (9223372036854775807), (1)")
        .expect_err("the completed integer sum must overflow");
    assert!(error.to_string().contains("integer overflow"));

    let mut query = conn
        .prepare("SELECT (SELECT COUNT(*) FROM aggregate_base), total FROM aggregate_view")
        .unwrap();
    assert!(matches!(query.step().unwrap(), StepResult::Row));
    let row = query.row().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(row.get::<i64>(1).unwrap(), 5);
    assert!(matches!(query.step().unwrap(), StepResult::Done));
    drop(query);

    conn.execute("INSERT INTO aggregate_base VALUES (2)")
        .unwrap();
    let mut query = conn.prepare("SELECT total FROM aggregate_view").unwrap();
    assert!(matches!(query.step().unwrap(), StepResult::Row));
    assert_eq!(query.row().unwrap().get::<i64>(0).unwrap(), 7);
}
