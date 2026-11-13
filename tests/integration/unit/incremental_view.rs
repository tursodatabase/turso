use turso_core::alloc::vec;
use turso_core::incremental::view::*;
use turso_core::incremental::yield_test_support::OneShotYieldInjector;
use turso_core::io::IO;
use turso_core::mvcc::yield_hooks::YieldPointMarker;
use turso_core::namespace::NamespaceId;
use turso_core::schema::{
    BTreeCharacteristics, BTreeTable, ColDef, ColDefFlags, Column as SchemaColumn, Schema, Type,
};
use turso_core::security::roles::SessionRole;
use turso_core::storage::btree::{BTreeWriteYieldPoint, BTREE_WRITE_YIELD_FAMILY};
use turso_core::sync::Arc;
use turso_core::types::IOResult;
use turso_core::types::Value;
use turso_core::TempStore;
use turso_core::{ConnectOpts, Database, DatabaseOpts, OpenFlags, StepResult};
use turso_parser::ast;
use turso_sqlite::SqliteDialect;

#[test]
fn hidden_state_ownership_parses_aggregate_suffixes() {
    assert!(state_key_belongs_to_view("orders__n12__a3", "orders"));
    assert!(state_key_belongs_to_view("orders__n12__g", "orders"));
    assert!(state_key_belongs_to_view("orders__n12__r", "orders"));
    assert!(!state_key_belongs_to_view("orders__n12__a", "orders"));
    assert!(!state_key_belongs_to_view("orders__n12__g_extra", "orders"));
    assert!(!state_key_belongs_to_view("orders__n12__r3", "orders"));
    assert!(!state_key_belongs_to_view(
        "orders__n12__a3_extra",
        "orders"
    ));
    assert!(!state_key_belongs_to_view("orders__n12__n3__a4", "orders"));
    assert!(state_key_belongs_to_view(
        "orders__n12__n3__a4",
        "orders__n12"
    ));
}

#[test]
fn transaction_change_log_captures_once_for_multiple_views() {
    let schema = create_test_schema();
    let table = schema
        .get_btree_table(NamespaceId::ROOT, "customers")
        .unwrap();
    let states = TransactionChanges::new();
    let views = vec!["first".to_string(), "second".to_string()];

    let result = states.insert(
        &table,
        &views,
        7,
        vec![Value::from_i64(7), Value::build_text("Ada")],
        TempStore::Memory,
    );
    assert!(matches!(result, Ok(false)));

    let changes = states.change_log.changes.lock();
    assert_eq!(changes.tail.len(), 1, "the base row is captured only once");
    drop(changes);
    let first = states.get("first").unwrap().table_delta(&table);
    let second = states.get("second").unwrap().table_delta(&table);
    assert_eq!(first.tail.len(), 1);
    assert_eq!(second.tail.len(), 1);
    assert!(
        Arc::ptr_eq(&first.tail, &second.tail),
        "subscribers must share the in-memory transaction-log tail"
    );
}

#[test]
fn transaction_change_log_preserves_non_unit_weights() {
    let schema = create_test_schema();
    let table = schema
        .get_btree_table(NamespaceId::ROOT, "customers")
        .unwrap();
    let states = TransactionChanges::new();
    let views = vec!["downstream".to_string()];
    let values = vec![Value::from_i64(7), Value::build_text("Ada")];

    assert!(matches!(
        states.change(&table, &views, 7, values.clone(), 3, TempStore::Memory,),
        Ok(false)
    ));
    let captured = states.get("downstream").unwrap().table_delta(&table);
    assert_eq!(captured.tail.len(), 1);
    assert_eq!(captured.tail[0].rowid, 7);
    assert_eq!(captured.tail[0].values, values);
    assert_eq!(captured.tail[0].weight, 3);
}

#[test]
fn transaction_change_log_orders_tables_globally_and_projects_by_identity() {
    let schema = create_test_schema();
    let customers = schema
        .get_btree_table(NamespaceId::ROOT, "customers")
        .unwrap();
    let orders = schema.get_btree_table(NamespaceId::ROOT, "orders").unwrap();
    let states = TransactionChanges::new();
    let views = vec!["joined_view".to_string()];

    assert!(matches!(
        states.insert(
            &customers,
            &views,
            1,
            vec![Value::from_i64(1), Value::build_text("Ada")],
            TempStore::Memory,
        ),
        Ok(false)
    ));
    assert!(matches!(
        states.insert(
            &orders,
            &views,
            10,
            vec![Value::from_i64(10), Value::from_i64(1), Value::from_i64(50)],
            TempStore::Memory,
        ),
        Ok(false)
    ));
    assert!(matches!(
        states.delete(
            &customers,
            &views,
            1,
            vec![Value::from_i64(1), Value::build_text("Ada")],
            TempStore::Memory,
        ),
        Ok(false)
    ));

    let changes = states.change_log.changes.lock();
    assert_eq!(changes.tail.len(), 3);
    assert_eq!(
        changes.tail[0].table_id,
        TableChangeId::RootPage(customers.root_page)
    );
    assert_eq!(
        changes.tail[1].table_id,
        TableChangeId::RootPage(orders.root_page)
    );
    assert_eq!(
        changes.tail[2].table_id,
        TableChangeId::RootPage(customers.root_page)
    );
    drop(changes);

    let state = states.get("joined_view").unwrap();
    let customers_delta = state.table_delta(&customers);
    let orders_delta = state.table_delta(&orders);
    assert_eq!(
        customers_delta
            .tail
            .iter()
            .filter(|event| event.table_id == TableChangeId::RootPage(customers.root_page))
            .count(),
        2
    );
    assert_eq!(
        orders_delta
            .tail
            .iter()
            .filter(|event| event.table_id == TableChangeId::RootPage(orders.root_page))
            .count(),
        1
    );
    assert!(
        Arc::ptr_eq(&customers_delta.tail, &orders_delta.tail),
        "table projections must share the canonical event stream"
    );

    let mut customers_cursor = turso_core::incremental::vdbe_maintenance::DeltaCursor::new(
        customers_delta,
        customers.columns().len(),
    );
    assert!(matches!(
        customers_cursor.rewind(),
        Ok(IOResult::Done(true))
    ));
    assert_eq!(customers_cursor.rowid(), 1);
    assert_eq!(
        customers_cursor.column(customers.columns().len()),
        Value::from_i64(1)
    );
    assert!(matches!(customers_cursor.next(), Ok(IOResult::Done(true))));
    assert_eq!(
        customers_cursor.column(customers.columns().len()),
        Value::from_i64(-1)
    );
    assert!(matches!(customers_cursor.next(), Ok(IOResult::Done(false))));
}

/// An UPDATE records its old row as a delete and its new row as an
/// insert. Both halves must be able to start their own spill: once the
/// delete half's spill is fully collected via `finish_spill`, recording
/// the insert half must find no spill in flight and be free to start a
/// second one of its own.
#[test]
fn update_delete_and_insert_halves_each_start_their_own_spill() {
    let schema = create_test_schema();
    let table = schema
        .get_btree_table(NamespaceId::ROOT, "customers")
        .unwrap();
    let io: Arc<dyn IO> = Arc::new(turso_core::MemoryIO::new());
    let states = TransactionChanges::with_io(io.clone());
    let views = vec!["v".to_string()];

    let old_large = vec![
        Value::from_i64(1),
        Value::Blob(vec![b'x'; CHANGE_LOG_SPILL_THRESHOLD]),
    ];
    let spill_started = states
        .delete(&table, &views, 1, old_large, TempStore::Memory)
        .unwrap();
    assert!(spill_started, "the delete half did not start a spill");
    loop {
        match states.finish_spill().unwrap() {
            IOResult::Done(()) => break,
            IOResult::IO(completions) => completions.wait(io.as_ref()).unwrap(),
        }
    }

    let new_large = vec![
        Value::from_i64(1),
        Value::Blob(vec![b'y'; CHANGE_LOG_SPILL_THRESHOLD]),
    ];
    let spill_started = states
        .insert(&table, &views, 1, new_large, TempStore::Memory)
        .unwrap();
    assert!(spill_started, "the insert half did not start a spill");
    loop {
        match states.finish_spill().unwrap() {
            IOResult::Done(()) => break,
            IOResult::IO(completions) => completions.wait(io.as_ref()).unwrap(),
        }
    }

    let changes = states.change_log.changes.lock();
    assert_eq!(
        changes.spilled.len(),
        2,
        "both halves must have spilled as their own segments"
    );
    assert_eq!(changes.spilled[0].rows, 1);
    assert_eq!(changes.spilled[1].rows, 1);
    assert!(
        changes.tail.is_empty(),
        "both rows were collected into spill segments"
    );
    drop(changes);
    assert!(
        !states.is_empty(),
        "spilled rows still count as captured changes"
    );
}

/// A spill that is still in flight when the owning statement fails must
/// not become a recorded segment: the statement rollback clears the log,
/// which drops the spill and leaves no spill in flight for the next
/// change to record.
#[test]
fn clear_drops_a_spill_still_in_flight() {
    let schema = create_test_schema();
    let table = schema
        .get_btree_table(NamespaceId::ROOT, "customers")
        .unwrap();
    let io: Arc<dyn IO> = Arc::new(turso_core::MemoryIO::new());
    let states = TransactionChanges::with_io(io.clone());
    let views = vec!["v".to_string()];

    let first = vec![Value::from_i64(1), Value::build_text("first")];
    assert!(matches!(
        states.insert(&table, &views, 1, first, TempStore::Memory),
        Ok(false)
    ));

    let large = vec![
        Value::from_i64(2),
        Value::Blob(vec![b'x'; CHANGE_LOG_SPILL_THRESHOLD]),
    ];
    let spill_started = states
        .insert(&table, &views, 2, large, TempStore::Memory)
        .unwrap();
    assert!(spill_started, "large capture did not start a spill");

    // The statement that captured both rows fails before the spill is
    // ever collected.
    states.clear();

    let changes = states.change_log.changes.lock();
    assert!(
        changes.spilled.is_empty(),
        "an uncollected spill must not become a recorded segment"
    );
    assert!(changes.tail.is_empty());
    drop(changes);
    assert!(states.is_empty());

    // No spill is in flight any more, so a later change may start its own.
    let spill_started = states
        .insert(
            &table,
            &views,
            3,
            vec![
                Value::from_i64(3),
                Value::Blob(vec![b'y'; CHANGE_LOG_SPILL_THRESHOLD]),
            ],
            TempStore::Memory,
        )
        .unwrap();
    assert!(spill_started);
}

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

#[test]
fn maintenance_insert_resumes_after_overflow_cell_yield() {
    #[allow(clippy::arc_with_non_send_sync)]
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        "ivm-maintenance-overflow-yield.db",
        OpenFlags::default(),
        DatabaseOpts::new().with_views(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db
        .connect(ConnectOpts::new(SessionRole::Superuser))
        .unwrap();
    conn.execute("CREATE TABLE overflow_base(id INTEGER PRIMARY KEY, payload BLOB)")
        .unwrap();
    for id in 1..260 {
        conn.execute(format!(
            "INSERT INTO overflow_base VALUES ({id}, zeroblob(3600))"
        ))
        .unwrap();
    }
    conn.execute("CREATE MATERIALIZED VIEW overflow_view AS SELECT id, payload FROM overflow_base")
        .unwrap();

    let view_root_page = {
        let view = conn
            .schema
            .read()
            .get_materialized_view(NamespaceId::ROOT, "overflow_view")
            .expect("materialized view must be present in the schema");
        let root_page = view.lock().get_root_page();
        root_page
    };
    let injector = OneShotYieldInjector::new(
        BTreeWriteYieldPoint::AfterInsertOverflowCellBeforeBalance.point(),
        BTREE_WRITE_YIELD_FAMILY ^ view_root_page as u64,
    );
    conn.set_yield_injector(Some(injector.clone()));

    let mut stmt = conn
        .prepare("INSERT INTO overflow_base VALUES (926, zeroblob(7200))")
        .unwrap();
    assert!(matches!(stmt.step().unwrap(), StepResult::Yield));
    assert!(injector.fired(), "targeted view B-tree yield did not fire");
    assert!(matches!(stmt.step().unwrap(), StepResult::Done));
    drop(stmt);
    conn.set_yield_injector(None);

    let mut query = conn
        .prepare("SELECT count(*), max(length(payload)) FROM overflow_view WHERE id = 926")
        .unwrap();
    assert!(matches!(query.step().unwrap(), StepResult::Row));
    let row = query.row().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
    assert_eq!(row.get::<i64>(1).unwrap(), 7200);
    drop(query);

    let mut integrity = conn.prepare("PRAGMA integrity_check").unwrap();
    assert!(matches!(integrity.step().unwrap(), StepResult::Row));
    assert_eq!(integrity.row().unwrap().get::<String>(0).unwrap(), "ok");
}

// Helper function to create a test schema with multiple tables
fn create_test_schema() -> Schema {
    let mut schema = Schema::with_options(true, &SqliteDialect).unwrap();

    // Create customers table
    let columns = vec![
        SchemaColumn::new(
            Some("id".to_string()),
            "INTEGER".to_string(),
            None,
            None,
            Type::Integer,
            None,
            ColDef {
                flags: ColDefFlags::PrimaryKey | ColDefFlags::RowIdAlias | ColDefFlags::NotNull,
                ..Default::default()
            },
        ),
        SchemaColumn::new_default_text(Some("name".to_string()), "TEXT".to_string(), None),
    ];
    let customers_table = BTreeTable::new(
        2,
        "customers".to_string(),
        vec![("id".to_string(), ast::SortOrder::Asc)],
        columns,
        BTreeCharacteristics::HAS_ROWID,
        vec![],
        vec![],
        vec![],
        None,
    );

    // Create orders table
    let columns = vec![
        SchemaColumn::new(
            Some("id".to_string()),
            "INTEGER".to_string(),
            None,
            None,
            Type::Integer,
            None,
            ColDef {
                flags: ColDefFlags::PrimaryKey | ColDefFlags::RowIdAlias | ColDefFlags::NotNull,
                ..Default::default()
            },
        ),
        SchemaColumn::new(
            Some("customer_id".to_string()),
            "INTEGER".to_string(),
            None,
            None,
            Type::Integer,
            None,
            ColDef::default(),
        ),
        SchemaColumn::new_default_integer(Some("total".to_string()), "INTEGER".to_string(), None),
    ];
    let orders_table = BTreeTable::new(
        3,
        "orders".to_string(),
        vec![("id".to_string(), ast::SortOrder::Asc)],
        columns,
        BTreeCharacteristics::HAS_ROWID,
        vec![],
        vec![],
        vec![],
        None,
    );

    // Create products table
    let columns = vec![
        SchemaColumn::new(
            Some("id".to_string()),
            "INTEGER".to_string(),
            None,
            None,
            Type::Integer,
            None,
            ColDef {
                flags: ColDefFlags::PrimaryKey | ColDefFlags::RowIdAlias | ColDefFlags::NotNull,
                ..Default::default()
            },
        ),
        SchemaColumn::new_default_text(Some("name".to_string()), "TEXT".to_string(), None),
        SchemaColumn::new(
            Some("price".to_string()),
            "REAL".to_string(),
            None,
            None,
            Type::Real,
            None,
            ColDef::default(),
        ),
    ];
    let products_table = BTreeTable::new(
        4,
        "products".to_string(),
        vec![("id".to_string(), ast::SortOrder::Asc)],
        columns,
        BTreeCharacteristics::HAS_ROWID,
        vec![],
        vec![],
        vec![],
        None,
    );

    // Create logs table - without a rowid alias (no INTEGER PRIMARY KEY)
    let columns = vec![
        SchemaColumn::new(
            Some("message".to_string()),
            "TEXT".to_string(),
            None,
            None,
            Type::Text,
            None,
            ColDef::default(),
        ),
        SchemaColumn::new_default_integer(Some("level".to_string()), "INTEGER".to_string(), None),
        SchemaColumn::new_default_integer(
            Some("timestamp".to_string()),
            "INTEGER".to_string(),
            None,
        ),
    ];
    // logs has no primary key (no rowid alias) but does have an implicit rowid.
    let logs_table = BTreeTable::new(
        5,
        "logs".to_string(),
        vec![],
        columns,
        BTreeCharacteristics::HAS_ROWID,
        vec![],
        vec![],
        vec![],
        None,
    );

    schema
        .add_btree_table(NamespaceId::ROOT, Arc::new(customers_table))
        .expect("Test setup: failed to add customers table");

    schema
        .add_btree_table(NamespaceId::ROOT, Arc::new(orders_table))
        .expect("Test setup: failed to add orders table");

    schema
        .add_btree_table(NamespaceId::ROOT, Arc::new(products_table))
        .expect("Test setup: failed to add products table");

    schema
        .add_btree_table(NamespaceId::ROOT, Arc::new(logs_table))
        .expect("Test setup: failed to add logs table");

    schema
}
