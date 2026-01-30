use crate::common::{ExecRows, TempDatabase};

#[turso_macros::test(mvcc)]
fn test_create_trigger(db: TempDatabase) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE test (x, y TEXT)").unwrap();

    conn.execute(
        "CREATE TRIGGER t1 BEFORE INSERT ON test BEGIN
         INSERT INTO test VALUES (100, 'triggered');
        END",
    )
    .unwrap();

    conn.execute("INSERT INTO test VALUES (1, 'hello')")
        .unwrap();

    let results: Vec<(i64, String)> = conn.exec_rows("SELECT * FROM test ORDER BY rowid");

    // Row inserted by trigger goes first
    assert_eq!(results[0], (100, "triggered".to_string()));
    assert_eq!(results[1], (1, "hello".to_string()));
}

#[turso_macros::test(mvcc)]
fn test_drop_trigger(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE test (x INTEGER PRIMARY KEY)")
        .unwrap();

    conn.execute("CREATE TRIGGER t1 BEFORE INSERT ON test BEGIN SELECT 1; END")
        .unwrap();

    // Verify trigger exists
    let results: Vec<(String,)> =
        conn.exec_rows("SELECT name FROM sqlite_schema WHERE type='trigger' AND name='t1'");
    assert_eq!(results.len(), 1);

    conn.execute("DROP TRIGGER t1").unwrap();

    // Verify trigger is gone
    let results: Vec<(String,)> =
        conn.exec_rows("SELECT name FROM sqlite_schema WHERE type='trigger' AND name='t1'");
    assert_eq!(results.len(), 0);
}

#[turso_macros::test(mvcc)]
fn test_trigger_after_insert(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE test (x INTEGER PRIMARY KEY, y TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE log (x INTEGER, y TEXT)")
        .unwrap();

    conn.execute(
        "CREATE TRIGGER t1 AFTER INSERT ON test BEGIN
         INSERT INTO log VALUES (NEW.x, NEW.y);
        END",
    )
    .unwrap();

    conn.execute("INSERT INTO test VALUES (1, 'hello')")
        .unwrap();

    let results: Vec<(i64, String)> = conn.exec_rows("SELECT * FROM log");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], (1, "hello".to_string()));
}

#[turso_macros::test(mvcc)]
fn test_trigger_when_clause(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE test (x INTEGER PRIMARY KEY, y INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE log (x INTEGER)").unwrap();

    conn.execute(
        "CREATE TRIGGER t1 AFTER INSERT ON test WHEN NEW.y > 10 BEGIN
         INSERT INTO log VALUES (NEW.x);
        END",
    )
    .unwrap();

    conn.execute("INSERT INTO test VALUES (1, 5)").unwrap();
    conn.execute("INSERT INTO test VALUES (2, 15)").unwrap();

    let results: Vec<(i64,)> = conn.exec_rows("SELECT * FROM log");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], (2,));
}

#[turso_macros::test(mvcc)]
fn test_trigger_drop_table_drops_triggers(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE test (x INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TRIGGER t1 BEFORE INSERT ON test BEGIN SELECT 1; END")
        .unwrap();

    // Verify trigger exists
    let results: Vec<(String,)> =
        conn.exec_rows("SELECT name FROM sqlite_schema WHERE type='trigger' AND name='t1'");
    assert_eq!(results.len(), 1);

    conn.execute("DROP TABLE test").unwrap();

    // Verify trigger is gone
    let results: Vec<(String,)> =
        conn.exec_rows("SELECT name FROM sqlite_schema WHERE type='trigger' AND name='t1'");
    assert_eq!(results.len(), 0);
}

#[turso_macros::test(mvcc)]
fn test_trigger_new_old_references(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE test (x INTEGER PRIMARY KEY, y TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE log (msg TEXT)").unwrap();

    conn.execute("INSERT INTO test VALUES (1, 'hello')")
        .unwrap();

    conn.execute(
        "CREATE TRIGGER t1 AFTER UPDATE ON test BEGIN
         INSERT INTO log VALUES ('old=' || OLD.y || ' new=' || NEW.y);
        END",
    )
    .unwrap();

    conn.execute("UPDATE test SET y = 'world' WHERE x = 1")
        .unwrap();

    let results: Vec<(String,)> = conn.exec_rows("SELECT * FROM log");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], ("old=hello new=world".to_string(),));
}

#[turso_macros::test(mvcc)]
fn test_multiple_triggers_same_event(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE test (x INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE log (msg TEXT)").unwrap();

    conn.execute(
        "CREATE TRIGGER t1 BEFORE INSERT ON test BEGIN
         INSERT INTO log VALUES ('trigger1');
        END",
    )
    .unwrap();

    conn.execute(
        "CREATE TRIGGER t2 BEFORE INSERT ON test BEGIN
         INSERT INTO log VALUES ('trigger2');
        END",
    )
    .unwrap();

    conn.execute("INSERT INTO test VALUES (1)").unwrap();

    let results: Vec<(String,)> = conn.exec_rows("SELECT * FROM log ORDER BY msg");

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], ("trigger1".to_string(),));
    assert_eq!(results[1], ("trigger2".to_string(),));
}

#[turso_macros::test(mvcc)]
fn test_two_triggers_on_same_table(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE test (x, msg TEXT)").unwrap();
    conn.execute("CREATE TABLE log (msg TEXT)").unwrap();

    // Trigger A: fires on INSERT to test, inserts into log and test (which would trigger B)
    conn.execute(
        "CREATE TRIGGER trigger_a AFTER INSERT ON test BEGIN
         INSERT INTO log VALUES ('trigger_a fired for x=' || NEW.x);
         INSERT INTO test VALUES (NEW.x + 100, 'from_a');
        END",
    )
    .unwrap();

    // Trigger B: fires on INSERT to test, inserts into log and test (which would trigger A)
    conn.execute(
        "CREATE TRIGGER trigger_b AFTER INSERT ON test BEGIN
         INSERT INTO log VALUES ('trigger_b fired for x=' || NEW.x);
         INSERT INTO test VALUES (NEW.x + 200, 'from_b');
        END",
    )
    .unwrap();

    // Insert initial row - this should trigger A, which triggers B, which tries to trigger A again (prevented)
    conn.execute("INSERT INTO test VALUES (1, 'initial')")
        .unwrap();

    // Check log entries to verify recursion was prevented
    let results: Vec<(String,)> = conn.exec_rows("SELECT * FROM log ORDER BY rowid");

    // At minimum, we should see both triggers fire and not infinite loop
    assert!(
        results.len() >= 2,
        "Expected at least 2 log entries, got {}",
        results.len()
    );
    assert!(
        results.iter().any(|s| s.0.contains("trigger_a")),
        "trigger_a should have fired"
    );
    assert!(
        results.iter().any(|s| s.0.contains("trigger_b")),
        "trigger_b should have fired"
    );
}

#[turso_macros::test(mvcc)]
fn test_trigger_mutual_recursion(db: TempDatabase) {
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE t (id INTEGER, msg TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE u (id INTEGER, msg TEXT)")
        .unwrap();

    // Trigger on T: fires on INSERT to t, inserts into u
    conn.execute(
        "CREATE TRIGGER trigger_on_t AFTER INSERT ON t BEGIN
         INSERT INTO u VALUES (NEW.id + 1000, 'from_t');
        END",
    )
    .unwrap();

    // Trigger on U: fires on INSERT to u, inserts into t
    conn.execute(
        "CREATE TRIGGER trigger_on_u AFTER INSERT ON u BEGIN
         INSERT INTO t VALUES (NEW.id + 2000, 'from_u');
        END",
    )
    .unwrap();

    // Insert initial row into t - this should trigger the chain
    conn.execute("INSERT INTO t VALUES (1, 'initial')").unwrap();

    // Check that both tables have entries
    let t_results: Vec<(i64, String)> = conn.exec_rows("SELECT * FROM t ORDER BY rowid");
    let u_results: Vec<(i64, String)> = conn.exec_rows("SELECT * FROM u ORDER BY rowid");

    // Verify the chain executed without infinite recursion
    assert!(!t_results.is_empty(), "Expected at least 1 entry in t");
    assert!(!u_results.is_empty(), "Expected at least 1 entry in u");

    // Verify initial insert
    assert_eq!(t_results[0], (1, "initial".to_string()));

    // Verify trigger on t fired (inserted into u)
    assert_eq!(u_results[0], (1001, "from_t".to_string()));
}

#[turso_macros::test(mvcc)]
fn test_after_insert_trigger(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table and log table
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE audit_log (action TEXT, item_id INTEGER, item_name TEXT)")
        .unwrap();

    // Create AFTER INSERT trigger
    conn.execute(
        "CREATE TRIGGER after_insert_items
         AFTER INSERT ON items
         BEGIN
             INSERT INTO audit_log VALUES ('INSERT', NEW.id, NEW.name);
         END",
    )
    .unwrap();

    // Insert data
    conn.execute("INSERT INTO items VALUES (1, 'apple')")
        .unwrap();
    conn.execute("INSERT INTO items VALUES (2, 'banana')")
        .unwrap();

    // Verify audit log
    let results: Vec<(String, i64, String)> =
        conn.exec_rows("SELECT * FROM audit_log ORDER BY rowid");

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], ("INSERT".to_string(), 1, "apple".to_string()));
    assert_eq!(results[1], ("INSERT".to_string(), 2, "banana".to_string()));
}

#[turso_macros::test(mvcc)]
fn test_before_update_of_trigger(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table with multiple columns
    conn.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)")
        .unwrap();
    conn.execute(
        "CREATE TABLE price_history (product_id INTEGER, old_price INTEGER, new_price INTEGER)",
    )
    .unwrap();

    // Create BEFORE UPDATE OF trigger - only fires when price column is updated
    conn.execute(
        "CREATE TRIGGER before_update_price
         BEFORE UPDATE OF price ON products
         BEGIN
             INSERT INTO price_history VALUES (OLD.id, OLD.price, NEW.price);
         END",
    )
    .unwrap();

    // Insert initial data
    conn.execute("INSERT INTO products VALUES (1, 'widget', 100)")
        .unwrap();
    conn.execute("INSERT INTO products VALUES (2, 'gadget', 200)")
        .unwrap();

    // Update price - should fire trigger
    conn.execute("UPDATE products SET price = 150 WHERE id = 1")
        .unwrap();

    // Update name only - should NOT fire trigger
    conn.execute("UPDATE products SET name = 'super widget' WHERE id = 1")
        .unwrap();

    // Update both name and price - should fire trigger
    conn.execute("UPDATE products SET name = 'mega gadget', price = 250 WHERE id = 2")
        .unwrap();

    // Verify price history
    let results: Vec<(i64, i64, i64)> =
        conn.exec_rows("SELECT * FROM price_history ORDER BY rowid");

    // Should have 2 entries (not 3, because name-only update didn't fire)
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (1, 100, 150));
    assert_eq!(results[1], (2, 200, 250));
}

#[turso_macros::test(mvcc)]
fn test_after_update_of_trigger(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table
    conn.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, salary INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE salary_changes (emp_id INTEGER, old_salary INTEGER, new_salary INTEGER, change_amount INTEGER)")
        .unwrap();

    // Create AFTER UPDATE OF trigger with multiple statements
    conn.execute(
        "CREATE TRIGGER after_update_salary
         AFTER UPDATE OF salary ON employees
         BEGIN
             INSERT INTO salary_changes VALUES (NEW.id, OLD.salary, NEW.salary, NEW.salary - OLD.salary);
         END",
    )
    .unwrap();

    // Insert initial data
    conn.execute("INSERT INTO employees VALUES (1, 'Alice', 50000)")
        .unwrap();
    conn.execute("INSERT INTO employees VALUES (2, 'Bob', 60000)")
        .unwrap();

    // Update salary
    conn.execute("UPDATE employees SET salary = 55000 WHERE id = 1")
        .unwrap();
    conn.execute("UPDATE employees SET salary = 65000 WHERE id = 2")
        .unwrap();

    // Verify salary changes
    let results: Vec<(i64, i64, i64, i64)> =
        conn.exec_rows("SELECT * FROM salary_changes ORDER BY rowid");

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (1, 50000, 55000, 5000));
    assert_eq!(results[1], (2, 60000, 65000, 5000));
}

fn log(s: &str) -> &str {
    tracing::info!("{}", s);
    s
}

#[turso_macros::test(mvcc)]
fn test_before_delete_trigger(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create tables
    conn.execute(log(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT)",
    ))
    .unwrap();
    conn.execute(log(
        "CREATE TABLE deleted_users (id INTEGER, username TEXT, deleted_at INTEGER)",
    ))
    .unwrap();

    // Create BEFORE DELETE trigger
    conn.execute(log("CREATE TRIGGER before_delete_users
         BEFORE DELETE ON users
         BEGIN
             INSERT INTO deleted_users VALUES (OLD.id, OLD.username, 12345);
         END"))
        .unwrap();

    // Insert data
    conn.execute(log("INSERT INTO users VALUES (1, 'alice')"))
        .unwrap();
    conn.execute(log("INSERT INTO users VALUES (2, 'bob')"))
        .unwrap();
    conn.execute(log("INSERT INTO users VALUES (3, 'charlie')"))
        .unwrap();

    // Delete some users
    conn.execute(log("DELETE FROM users WHERE id = 2")).unwrap();
    conn.execute(log("DELETE FROM users WHERE id = 3")).unwrap();

    // Verify deleted_users table
    let results: Vec<(i64, String, i64)> =
        conn.exec_rows("SELECT * FROM deleted_users ORDER BY id");

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (2, "bob".to_string(), 12345));
    assert_eq!(results[1], (3, "charlie".to_string(), 12345));

    // Verify remaining users
    let count: Vec<(i64,)> = conn.exec_rows("SELECT COUNT(*) FROM users");
    assert_eq!(count[0], (1,));
}

#[turso_macros::test(mvcc)]
fn test_after_delete_trigger(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create tables
    conn.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)",
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE order_archive (order_id INTEGER, customer_id INTEGER, amount INTEGER)",
    )
    .unwrap();

    // Create AFTER DELETE trigger
    conn.execute(
        "CREATE TRIGGER after_delete_orders
         AFTER DELETE ON orders
         BEGIN
             INSERT INTO order_archive VALUES (OLD.id, OLD.customer_id, OLD.amount);
         END",
    )
    .unwrap();

    // Insert data
    conn.execute("INSERT INTO orders VALUES (1, 100, 50)")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES (2, 101, 75)")
        .unwrap();
    conn.execute("INSERT INTO orders VALUES (3, 100, 100)")
        .unwrap();

    // Delete orders
    conn.execute("DELETE FROM orders WHERE customer_id = 100")
        .unwrap();

    // Verify archive
    let results: Vec<(i64, i64, i64)> =
        conn.exec_rows("SELECT * FROM order_archive ORDER BY order_id");

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (1, 100, 50));
    assert_eq!(results[1], (3, 100, 100));
}

#[turso_macros::test(mvcc)]
fn test_trigger_with_multiple_statements(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create tables
    conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")
        .unwrap();
    conn.execute(
        "CREATE TABLE transactions (account_id INTEGER, old_balance INTEGER, new_balance INTEGER)",
    )
    .unwrap();
    conn.execute("CREATE TABLE audit (message TEXT)").unwrap();

    // Create trigger with multiple statements
    conn.execute(
        "CREATE TRIGGER track_balance_changes
         AFTER UPDATE OF balance ON accounts
         BEGIN
             INSERT INTO transactions VALUES (NEW.id, OLD.balance, NEW.balance);
             INSERT INTO audit VALUES ('Balance changed for account ' || NEW.id);
         END",
    )
    .unwrap();

    // Insert initial data
    conn.execute("INSERT INTO accounts VALUES (1, 1000)")
        .unwrap();
    conn.execute("INSERT INTO accounts VALUES (2, 2000)")
        .unwrap();

    // Update balances
    conn.execute("UPDATE accounts SET balance = 1500 WHERE id = 1")
        .unwrap();
    conn.execute("UPDATE accounts SET balance = 2500 WHERE id = 2")
        .unwrap();

    // Verify transactions table
    let trans_results: Vec<(i64, i64, i64)> =
        conn.exec_rows("SELECT * FROM transactions ORDER BY rowid");

    assert_eq!(trans_results.len(), 2);
    assert_eq!(trans_results[0], (1, 1000, 1500));
    assert_eq!(trans_results[1], (2, 2000, 2500));

    // Verify audit table
    let audit_results: Vec<(String,)> = conn.exec_rows("SELECT * FROM audit ORDER BY rowid");

    assert_eq!(audit_results.len(), 2);
    assert_eq!(
        audit_results[0],
        ("Balance changed for account 1".to_string(),)
    );
    assert_eq!(
        audit_results[1],
        ("Balance changed for account 2".to_string(),)
    );
}

#[turso_macros::test()]
/// This test input used to cause corruption (https://github.com/tursodatabase/turso/issues/4017)
fn test_trigger_self_insert_regression(db: TempDatabase) -> anyhow::Result<()> {
    let conn = db.connect_limbo();

    // Create a table
    conn.execute(
        "CREATE TABLE spellbinding_occupation_9 (
            fearless_reitman_10 REAL,
            affectionate_lacazeduthiers_11 INTEGER,
            thoughtful_hunt_12 TEXT,
            outstanding_gorman_13 BLOB,
            rousing_mutualistsorg_14 REAL,
            brilliant_heller_15 INTEGER,
            sleek_minyi_16 REAL,
            technological_propos_17 TEXT,
            fantastic_hewetson_18 BLOB
        )",
    )?;

    conn.execute(
        "CREATE TRIGGER trigger_spellbinding_occupation_9_3199742326
         BEFORE INSERT ON spellbinding_occupation_9
         BEGIN
             INSERT INTO spellbinding_occupation_9
                 SELECT * FROM spellbinding_occupation_9 WHERE (TRUE);
         END",
    )?;

    conn.execute(
        "INSERT INTO spellbinding_occupation_9 VALUES
        (4424809535.610264, -578783662584182030, 'diplomatic_heredia', X'706C75636B795F626167696E736B69', -8706666618.225624, 308424772820097370, 8891353023.855804, 'excellent_mccarthy', X'616D617A696E675F6C616E65'),
        (6256766006.928358, 8443373764808777983, 'rousing_abacus', X'676C696D6D6572696E675F7A6971', -2628193223.9937954, 3459210825691415951, -295951398.30797577, 'kind_sharp', X'6C6F76696E675F656D6D61')"
    )?;

    conn.execute(
        "INSERT INTO spellbinding_occupation_9 VALUES
        (-6800098379.631634, 2482803385035812249, 'flexible_pentecost', X'6D6F76696E675F776F6F64776F726B', -5101433261.720527, -3567788410267959713, -8073502947.6008835, 'glistening_fulano', X'636F75726167656F75735F6B616C65'),
        (3306794031.008669, 6427602129275079032, 'wondrous_monaghan', X'64696C6967656E745F7363687761727A', 9631408519.53051, -1684447188648680268, 3252650683.341938, 'optimistic_nappalos', X'656C6567616E745F73746F776173736572')"
    )?;

    conn.execute(
        "INSERT INTO spellbinding_occupation_9 VALUES
        (7767161224.622696, -1743571527922740251, 'adaptable_reo', X'6D61676E69666963656E745F70617472697A6961', 4518833159.836601, 7745538090405886344, -5362785860.664702, 'qualified_hakiel', X'656E6761676966755F6E75727365'),
        (7238299930.889935, -6905021346313814225, 'sincere_aman', X'666162756C6F75735F73616D75647A69', 1746051365.7113361, 8456865750190177515, -9173223276.743935, 'spectacular_mogutin', X'6C696B61626C655F636C61726B'),
        (2386990685.0495243, 6605207765674540892, 'courageous_knabb', X'696E646570656E64656E745F726462', -1220491985.740755, -7244471264718141981, -4274556067.547324, 'thoughtful_enckell', X'70726F647563746976655F7261736B696E'),
        (3760241384.9700813, 7633896663664778180, 'spectacular_preti', X'64696C6967656E745F6B657272', 3129288675.6782093, 6385161053648972070, 5783058869.993795, 'resourceful_russo', X'7368696D6D6572696E675F64617669646E65656C'),
        (9654906624.491634, 5240627906285380382, 'spellbinding_submedia', X'6C6F76656C795F726F6F73', -620764284.3328953, -4506780404010403369, 4642790445.595289, 'shimmering_stirner', X'746563686E6F6C6F676963616C5F68756D616E')"
    )?;

    // Run integrity check using rusqlite to verify database is not corrupted
    let rusqlite_conn = rusqlite::Connection::open(db.path.clone())?;
    let mut stmt = rusqlite_conn.prepare("PRAGMA integrity_check")?;
    let mut rows = stmt.query([])?;

    let mut results = Vec::new();
    while let Some(row) = rows.next()? {
        let result: String = row.get(0)?;
        results.push(result);
    }

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0], "ok",
        "Database integrity check failed: {results:?}",
    );

    Ok(())
}

#[turso_macros::test(mvcc)]
fn test_alter_table_drop_column_fails_when_trigger_references_new_column(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table with columns
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y INTEGER)")
        .unwrap();

    // Create trigger that references y via NEW.y
    conn.execute(
        "CREATE TRIGGER tu BEFORE INSERT ON t BEGIN
         INSERT INTO t VALUES (NEW.x, NEW.y);
        END",
    )
    .unwrap();

    // Attempting to drop column y should fail because trigger references it
    let result = conn.execute("ALTER TABLE t DROP COLUMN y");
    assert!(
        result.is_err(),
        "Dropping column y should fail when trigger references NEW.y"
    );

    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("cannot drop column") && error_msg.contains("trigger"),
        "Error should mention column drop and trigger: {error_msg}",
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_drop_column_fails_when_trigger_references_old_column(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table with columns
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y INTEGER)")
        .unwrap();

    // Create trigger that references y via OLD.y
    conn.execute(
        "CREATE TRIGGER tu AFTER UPDATE ON t BEGIN
         INSERT INTO t VALUES (OLD.x, OLD.y);
        END",
    )
    .unwrap();

    // Attempting to drop column y should fail because trigger references it
    let result = conn.execute("ALTER TABLE t DROP COLUMN y");
    assert!(
        result.is_err(),
        "Dropping column y should fail when trigger references OLD.y"
    );

    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("cannot drop column") && error_msg.contains("trigger"),
        "Error should mention column drop and trigger: {error_msg}",
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_drop_column_fails_when_trigger_references_unqualified_column(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table with columns
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y INTEGER)")
        .unwrap();

    // Create trigger that references y as unqualified column (in WHEN clause)
    conn.execute(
        "CREATE TRIGGER tu BEFORE INSERT ON t WHEN y > 10 BEGIN
         INSERT INTO t VALUES (NEW.x, 100);
        END",
    )
    .unwrap();

    // Attempting to drop column y should fail because trigger references it
    let result = conn.execute("ALTER TABLE t DROP COLUMN y");
    assert!(
        result.is_err(),
        "Dropping column y should fail when trigger references it in WHEN clause"
    );

    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("cannot drop column") && error_msg.contains("trigger"),
        "Error should mention column drop and trigger: {error_msg}",
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_drop_column_succeeds_when_trigger_references_other_table(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create two tables
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE u (z INTEGER)").unwrap();

    // Create trigger on t that references column from u (not t)
    conn.execute(
        "CREATE TRIGGER tu BEFORE INSERT ON t BEGIN
         INSERT INTO u(z) VALUES (NEW.x);
        END",
    )
    .unwrap();

    // Dropping column y from t should succeed because trigger doesn't reference it
    conn.execute("ALTER TABLE t DROP COLUMN y").unwrap();

    // Verify column was dropped
    let columns: Vec<(String,)> = conn.exec_rows("SELECT name FROM pragma_table_info('t')");

    // Should only have x column now
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0], ("x".to_string(),));
}

#[turso_macros::test(mvcc)]
fn test_alter_table_drop_column_from_other_table_causes_parse_error_when_trigger_fires(
    db: TempDatabase,
) {
    let conn = db.connect_limbo();

    // Create two tables
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE u (z INTEGER, zer INTEGER)")
        .unwrap();

    // Create trigger on t that references column zer from u
    conn.execute(
        "CREATE TRIGGER tu BEFORE INSERT ON t BEGIN
         INSERT INTO u(z, zer) VALUES (NEW.x, NEW.x);
        END",
    )
    .unwrap();

    // Dropping column zer from u should succeed (trigger is on t, not u)
    conn.execute("ALTER TABLE u DROP COLUMN zer").unwrap();

    // Verify column was dropped
    let columns: Vec<(String,)> = conn.exec_rows("SELECT name FROM pragma_table_info('u')");

    // Should only have z column now
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0], ("z".to_string(),));

    // Now trying to insert into t should fail because trigger references non-existent column zer
    let result = conn.execute("INSERT INTO t VALUES (1)");
    assert!(
        result.is_err(),
        "Insert should fail because trigger references non-existent column zer"
    );

    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("no column named") || error_msg.contains("zer"),
        "Error should mention missing column: {error_msg}",
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_rename_column_propagates_to_trigger_on_owning_table(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y INTEGER)")
        .unwrap();

    // Create trigger on t that references y via NEW.y
    conn.execute(
        "CREATE TRIGGER tu BEFORE INSERT ON t BEGIN
         INSERT INTO t VALUES (NEW.x, NEW.y);
        END",
    )
    .unwrap();

    // Rename column y to y_new
    conn.execute("ALTER TABLE t RENAME COLUMN y TO y_new")
        .unwrap();

    // Verify trigger SQL was updated
    let results: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='tu'");
    let trigger_sql = &results[0].0;

    // Trigger SQL should reference y_new instead of y
    let normalized_sql = trigger_sql.split_whitespace().collect::<Vec<_>>().join(" ");
    assert_eq!(
        normalized_sql,
        "CREATE TRIGGER tu BEFORE INSERT ON t BEGIN INSERT INTO t VALUES (NEW.x, NEW.y_new); END"
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_rename_column_propagates_to_trigger_referencing_other_table(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create two tables
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE u (z INTEGER, zer INTEGER)")
        .unwrap();

    // Create trigger on t that references column z from u
    conn.execute(
        "CREATE TRIGGER tu BEFORE INSERT ON t BEGIN
         INSERT INTO u(z, zer) VALUES (NEW.x, NEW.x);
        END",
    )
    .unwrap();

    // Rename column z to zoo in table u
    conn.execute("ALTER TABLE u RENAME COLUMN z TO zoo")
        .unwrap();

    // Verify trigger SQL was updated to reference zoo
    let results: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='tu'");
    let trigger_sql = &results[0].0;

    // Trigger SQL should reference zoo instead of z
    let normalized_sql = trigger_sql.split_whitespace().collect::<Vec<_>>().join(" ");
    assert_eq!(
        normalized_sql,
        "CREATE TRIGGER tu BEFORE INSERT ON t BEGIN INSERT INTO u (zoo, zer) VALUES (NEW.x, NEW.x); END"
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_rename_column_propagates_to_trigger_with_multiple_references(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create two tables
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE u (z INTEGER, zer INTEGER)")
        .unwrap();

    // Create trigger on t that references y multiple times and z from u
    conn.execute(
        "CREATE TRIGGER tu BEFORE INSERT ON t BEGIN
         INSERT INTO u(z, zer) VALUES (NEW.y, NEW.y);
        END",
    )
    .unwrap();

    // Rename column y to y_new in table t
    conn.execute("ALTER TABLE t RENAME COLUMN y TO y_new")
        .unwrap();

    // Verify trigger SQL was updated
    let results: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='tu'");
    let trigger_sql = &results[0].0;

    // Trigger SQL should reference y_new instead of y
    let normalized_sql = trigger_sql.split_whitespace().collect::<Vec<_>>().join(" ");
    assert_eq!(
        normalized_sql,
        "CREATE TRIGGER tu BEFORE INSERT ON t BEGIN INSERT INTO u (z, zer) VALUES (NEW.y_new, NEW.y_new); END"
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_rename_column_fails_when_trigger_when_clause_references_column(
    db: TempDatabase,
) {
    let conn = db.connect_limbo();

    // Create table
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y INTEGER)")
        .unwrap();

    // Create trigger with WHEN clause referencing y
    conn.execute(
        "CREATE TRIGGER tu BEFORE INSERT ON t WHEN y > 10 BEGIN
         INSERT INTO t VALUES (NEW.x, 100);
        END",
    )
    .unwrap();

    // Rename column y to y_new should fail (SQLite fails if WHEN clause references the column)
    let result = conn.execute("ALTER TABLE t RENAME COLUMN y TO y_new");
    assert!(
        result.is_err(),
        "RENAME COLUMN should fail when trigger WHEN clause references the column"
    );

    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("error in trigger") && error_msg.contains("no such column"),
        "Error should mention trigger and column: {error_msg}",
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_rename_column_propagates_to_multiple_triggers(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y INTEGER)")
        .unwrap();

    // Create multiple triggers referencing y
    conn.execute(
        "CREATE TRIGGER t1 BEFORE INSERT ON t BEGIN
         INSERT INTO t VALUES (NEW.x, NEW.y);
        END",
    )
    .unwrap();

    conn.execute(
        "CREATE TRIGGER t2 AFTER UPDATE ON t BEGIN
         INSERT INTO t VALUES (OLD.x, OLD.y);
        END",
    )
    .unwrap();

    // Rename column y to y_new
    conn.execute("ALTER TABLE t RENAME COLUMN y TO y_new")
        .unwrap();

    // Verify both triggers were updated
    let trigger_sqls: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE type='trigger' ORDER BY name");

    // Both triggers should reference y_new
    assert_eq!(trigger_sqls.len(), 2);
    let normalized_t1 = trigger_sqls[0]
        .0
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let normalized_t2 = trigger_sqls[1]
        .0
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    assert_eq!(
        normalized_t1,
        "CREATE TRIGGER t1 BEFORE INSERT ON t BEGIN INSERT INTO t VALUES (NEW.x, NEW.y_new); END"
    );
    assert_eq!(
        normalized_t2,
        "CREATE TRIGGER t2 AFTER UPDATE ON t BEGIN INSERT INTO t VALUES (OLD.x, OLD.y_new); END"
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_drop_column_fails_with_old_reference_in_update_trigger(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y INTEGER)")
        .unwrap();

    // Create UPDATE trigger that references OLD.y
    conn.execute(
        "CREATE TRIGGER tu AFTER UPDATE ON t BEGIN
         INSERT INTO t VALUES (OLD.x, OLD.y);
        END",
    )
    .unwrap();

    // Attempting to drop column y should fail
    let result = conn.execute("ALTER TABLE t DROP COLUMN y");
    assert!(
        result.is_err(),
        "Dropping column y should fail when UPDATE trigger references OLD.y"
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_rename_column_in_insert_column_list(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create two tables
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE u (z INTEGER, zer INTEGER)")
        .unwrap();

    // Create trigger that inserts into u with explicit column list
    conn.execute(
        "CREATE TRIGGER tu BEFORE INSERT ON t BEGIN
         INSERT INTO u(z, zer) VALUES (NEW.x, NEW.x);
        END",
    )
    .unwrap();

    // Rename column zer to zercher in table u
    conn.execute("ALTER TABLE u RENAME COLUMN zer TO zercher")
        .unwrap();

    // Verify trigger SQL was updated
    let results: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='tu'");
    let trigger_sql = &results[0].0;

    // Trigger SQL should reference zercher in INSERT column list
    let normalized_sql = trigger_sql.split_whitespace().collect::<Vec<_>>().join(" ");
    assert_eq!(
        normalized_sql,
        "CREATE TRIGGER tu BEFORE INSERT ON t BEGIN INSERT INTO u (z, zercher) VALUES (NEW.x, NEW.x); END"
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_rename_column_in_trigger_table_does_not_rewrite_other_table_column(
    db: TempDatabase,
) {
    let conn = db.connect_limbo();

    // Create two tables, both with column 'x'
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE u (x INTEGER, z INTEGER)")
        .unwrap();

    // Create trigger on t that references both t.x (via NEW.x) and u.x (in INSERT column list)
    conn.execute(
        "CREATE TRIGGER tu BEFORE INSERT ON t BEGIN
         INSERT INTO u(x, z) VALUES (NEW.x, NEW.y);
        END",
    )
    .unwrap();

    // Rename column x to x_new in table t (the trigger's owning table)
    conn.execute("ALTER TABLE t RENAME COLUMN x TO x_new")
        .unwrap();

    // Verify trigger SQL was updated correctly:
    // - NEW.x should become NEW.x_new (refers to table t)
    // - INSERT INTO u(x, ...) should remain as x (refers to table u, not t)
    let results: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='tu'");
    let trigger_sql = &results[0].0;

    let normalized_sql = trigger_sql.split_whitespace().collect::<Vec<_>>().join(" ");
    // NEW.x should be rewritten to NEW.x_new
    assert!(
        normalized_sql.contains("NEW.x_new"),
        "Trigger SQL should contain NEW.x_new: {normalized_sql}",
    );
    // INSERT INTO u (x, ...) should still have x (not x_new) because it refers to table u's column
    assert!(
        normalized_sql.contains("INSERT INTO u (x,") || normalized_sql.contains("INSERT INTO u(x,"),
        "Trigger SQL should contain INSERT INTO u (x, (not u (x_new,): {normalized_sql}",
    );
    assert!(
        !normalized_sql.contains("INSERT INTO u (x_new,") && !normalized_sql.contains("INSERT INTO u(x_new,"),
        "Trigger SQL should NOT contain INSERT INTO u (x_new, (x refers to table u): {normalized_sql}",
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_rename_column_in_insert_target_table_does_not_rewrite_trigger_table_column(
    db: TempDatabase,
) {
    let conn = db.connect_limbo();

    // Create two tables, both with column 'x'
    conn.execute("CREATE TABLE t (x INTEGER PRIMARY KEY, y INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE u (x INTEGER, z INTEGER)")
        .unwrap();

    // Create trigger on t that references both t.x (via NEW.x) and u.x (in INSERT column list)
    conn.execute(
        "CREATE TRIGGER tu BEFORE INSERT ON t BEGIN
         INSERT INTO u(x, z) VALUES (NEW.x, NEW.y);
        END",
    )
    .unwrap();

    // Rename column x to x_new in table u (the INSERT target table)
    conn.execute("ALTER TABLE u RENAME COLUMN x TO x_new")
        .unwrap();

    // Verify trigger SQL was updated correctly:
    // - NEW.x should remain as NEW.x (refers to table t, not u)
    // - INSERT INTO u(x, ...) should become u(x_new, ...) (refers to table u)
    let results: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='tu'");
    let trigger_sql = &results[0].0;

    let normalized_sql = trigger_sql.split_whitespace().collect::<Vec<_>>().join(" ");
    // NEW.x should remain as NEW.x (not rewritten because it refers to table t)
    assert!(
        normalized_sql.contains("NEW.x"),
        "Trigger SQL should contain NEW.x (not NEW.x_new): {normalized_sql}",
    );
    assert!(
        !normalized_sql.contains("NEW.x_new"),
        "Trigger SQL should NOT contain NEW.x_new (x refers to table t, not u): {normalized_sql}",
    );
    // INSERT INTO u (x_new, ...) should have x_new (rewritten because it refers to table u)
    assert!(
        normalized_sql.contains("INSERT INTO u (x_new,")
            || normalized_sql.contains("INSERT INTO u(x_new,"),
        "Trigger SQL should contain INSERT INTO u (x_new, (not u (x,): {normalized_sql}",
    );
    assert!(
        !normalized_sql.contains("INSERT INTO u (x,") && !normalized_sql.contains("INSERT INTO u(x,"),
        "Trigger SQL should NOT contain INSERT INTO u (x, (x was renamed to x_new in table u): {normalized_sql}",
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_rename_column_update_where_clause_does_not_rewrite_target_table_column(
    db: TempDatabase,
) {
    let conn = db.connect_limbo();

    // Create two tables, both with column 'x'
    conn.execute("CREATE TABLE t (x INTEGER, y INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE u (x INTEGER, z INTEGER)")
        .unwrap();

    // Create trigger on t that updates u, with WHERE clause referencing u.x (unqualified)
    conn.execute(
        "CREATE TRIGGER tu AFTER UPDATE ON t BEGIN
         UPDATE u SET z = NEW.x WHERE x = OLD.x;
        END",
    )
    .unwrap();

    // Rename column x to x_new in table t (the trigger's owning table)
    conn.execute("ALTER TABLE t RENAME COLUMN x TO x_new")
        .unwrap();

    // Verify trigger SQL: WHERE x should remain as x (refers to table u, not t)
    let results: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='tu'");
    let trigger_sql = &results[0].0;

    let normalized_sql = trigger_sql.split_whitespace().collect::<Vec<_>>().join(" ");
    // NEW.x and OLD.x should be rewritten to x_new
    assert!(
        normalized_sql.contains("NEW.x_new") && normalized_sql.contains("OLD.x_new"),
        "Trigger SQL should contain NEW.x_new and OLD.x_new: {normalized_sql}",
    );
    // WHERE x should remain as x (not x_new) because it refers to table u's column
    assert!(
        normalized_sql.contains("WHERE x =") || normalized_sql.contains("WHERE x="),
        "Trigger SQL should contain WHERE x = (not WHERE x_new =): {normalized_sql}",
    );
    assert!(
        !normalized_sql.contains("WHERE x_new =") && !normalized_sql.contains("WHERE x_new="),
        "Trigger SQL should NOT contain WHERE x_new = (x refers to table u): {normalized_sql}",
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_rename_column_update_set_column_name_rewritten(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create two tables, both with column 'x'
    conn.execute("CREATE TABLE t (x INTEGER, y INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE u (x INTEGER, z INTEGER)")
        .unwrap();

    // Create trigger on t that updates u, setting u.x
    conn.execute(
        "CREATE TRIGGER tu AFTER UPDATE ON t BEGIN
         UPDATE u SET x = NEW.x WHERE u.x = OLD.x;
        END",
    )
    .unwrap();

    // Rename column x to x_new in table u (the UPDATE target table)
    conn.execute("ALTER TABLE u RENAME COLUMN x TO x_new")
        .unwrap();

    // Verify trigger SQL: SET x should become SET x_new, WHERE u.x should become u.x_new
    let results: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='tu'");
    let trigger_sql = &results[0].0;

    let normalized_sql = trigger_sql.split_whitespace().collect::<Vec<_>>().join(" ");
    // SET x should become SET x_new
    assert!(
        normalized_sql.contains("SET x_new =") || normalized_sql.contains("SET x_new="),
        "Trigger SQL should contain SET x_new =: {normalized_sql}",
    );
    assert!(
        !normalized_sql.contains("SET x =") && !normalized_sql.contains("SET x="),
        "Trigger SQL should NOT contain SET x = (x was renamed to x_new): {normalized_sql}",
    );
    // WHERE u.x should become u.x_new
    assert!(
        normalized_sql.contains("u.x_new =") || normalized_sql.contains("u.x_new="),
        "Trigger SQL should contain u.x_new =: {normalized_sql}",
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_rename_column_delete_where_clause_does_not_rewrite_target_table_column(
    db: TempDatabase,
) {
    let conn = db.connect_limbo();

    // Create two tables, both with column 'x'
    conn.execute("CREATE TABLE t (x INTEGER, y INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE u (x INTEGER, z INTEGER)")
        .unwrap();

    // Create trigger on t that deletes from u, with WHERE clause referencing u.x (unqualified)
    conn.execute(
        "CREATE TRIGGER tu AFTER DELETE ON t BEGIN
         DELETE FROM u WHERE x = OLD.x;
        END",
    )
    .unwrap();

    // Rename column x to x_new in table t (the trigger's owning table)
    conn.execute("ALTER TABLE t RENAME COLUMN x TO x_new")
        .unwrap();

    // Verify trigger SQL: WHERE x should remain as x (refers to table u, not t)
    let results: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='tu'");
    let trigger_sql = &results[0].0;

    let normalized_sql = trigger_sql.split_whitespace().collect::<Vec<_>>().join(" ");
    // OLD.x should be rewritten to OLD.x_new
    assert!(
        normalized_sql.contains("OLD.x_new"),
        "Trigger SQL should contain OLD.x_new: {normalized_sql}",
    );
    // WHERE x should remain as x (not x_new) because it refers to table u's column
    assert!(
        normalized_sql.contains("WHERE x =") || normalized_sql.contains("WHERE x="),
        "Trigger SQL should contain WHERE x = (not WHERE x_new =): {normalized_sql}",
    );
    assert!(
        !normalized_sql.contains("WHERE x_new =") && !normalized_sql.contains("WHERE x_new="),
        "Trigger SQL should NOT contain WHERE x_new = (x refers to table u): {normalized_sql}",
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_rename_column_update_of_column_list_rewritten(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table
    conn.execute("CREATE TABLE t (x INTEGER, y INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE u (x INTEGER, z INTEGER)")
        .unwrap();

    // Create trigger with UPDATE OF x
    conn.execute(
        "CREATE TRIGGER tu AFTER UPDATE OF x ON t BEGIN
         UPDATE u SET z = NEW.x WHERE x = OLD.x;
        END",
    )
    .unwrap();

    // Rename column x to x_new in table t
    conn.execute("ALTER TABLE t RENAME COLUMN x TO x_new")
        .unwrap();

    // Verify trigger SQL: UPDATE OF x should become UPDATE OF x_new
    let results: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='tu'");
    let trigger_sql = &results[0].0;

    let normalized_sql = trigger_sql.split_whitespace().collect::<Vec<_>>().join(" ");
    // UPDATE OF x should become UPDATE OF x_new
    assert!(
        normalized_sql.contains("UPDATE OF x_new"),
        "Trigger SQL should contain UPDATE OF x_new: {normalized_sql}",
    );
    assert!(
        !normalized_sql.contains("UPDATE OF x,") && !normalized_sql.contains("UPDATE OF x "),
        "Trigger SQL should NOT contain UPDATE OF x (x was renamed to x_new): {normalized_sql}",
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_rename_column_update_of_multiple_columns_rewritten(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table
    conn.execute("CREATE TABLE t (x INTEGER, y INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE u (x INTEGER, z INTEGER)")
        .unwrap();

    // Create trigger with UPDATE OF x, y
    conn.execute(
        "CREATE TRIGGER tu AFTER UPDATE OF x, y ON t BEGIN
         UPDATE u SET z = NEW.x WHERE x = OLD.x;
        END",
    )
    .unwrap();

    // Rename column x to x_new in table t
    conn.execute("ALTER TABLE t RENAME COLUMN x TO x_new")
        .unwrap();

    // Verify trigger SQL: UPDATE OF x, y should become UPDATE OF x_new, y
    let results: Vec<(String,)> =
        conn.exec_rows("SELECT sql FROM sqlite_schema WHERE type='trigger' AND name='tu'");
    let trigger_sql = &results[0].0;

    let normalized_sql = trigger_sql.split_whitespace().collect::<Vec<_>>().join(" ");
    // UPDATE OF x, y should become UPDATE OF x_new, y
    assert!(
        normalized_sql.contains("UPDATE OF x_new, y")
            || normalized_sql.contains("UPDATE OF x_new,y"),
        "Trigger SQL should contain UPDATE OF x_new, y: {normalized_sql}",
    );
    assert!(
        !normalized_sql.contains("UPDATE OF x,") && !normalized_sql.contains("UPDATE OF x "),
        "Trigger SQL should NOT contain UPDATE OF x (x was renamed to x_new): {normalized_sql}",
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_drop_column_allows_when_insert_targets_other_table(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create two tables, both with column 'x'
    conn.execute("CREATE TABLE t (x INTEGER, y INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE u (x INTEGER, z INTEGER)")
        .unwrap();

    // Create trigger on t that inserts into u(x) - this should NOT prevent dropping x from t
    conn.execute(
        "CREATE TRIGGER tu BEFORE INSERT ON t BEGIN
         INSERT INTO u(x) VALUES (NEW.y);
        END",
    )
    .unwrap();

    // Dropping column x from table t should succeed (INSERT INTO u(x) refers to u.x, not t.x)
    conn.execute("ALTER TABLE t DROP COLUMN x").unwrap();
}

#[turso_macros::test(mvcc)]
fn test_alter_table_drop_column_allows_when_update_targets_other_table(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create two tables, both with column 'x'
    conn.execute("CREATE TABLE t (x INTEGER, y INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE u (x INTEGER, z INTEGER)")
        .unwrap();

    // Create trigger on t that updates u SET x = ... - this should NOT prevent dropping x from t
    conn.execute(
        "CREATE TRIGGER tu BEFORE INSERT ON t BEGIN
         UPDATE u SET x = NEW.y WHERE z = 1;
        END",
    )
    .unwrap();

    // Dropping column x from table t should succeed (UPDATE u SET x refers to u.x, not t.x)
    conn.execute("ALTER TABLE t DROP COLUMN x").unwrap();
}

#[turso_macros::test(mvcc)]
fn test_alter_table_drop_column_allows_when_insert_targets_owning_table(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table
    conn.execute("CREATE TABLE t (x INTEGER, y INTEGER)")
        .unwrap();

    // Create trigger on t that inserts into t(x) - SQLite allows DROP COLUMN here
    // The error only occurs when the trigger is actually executed
    conn.execute(
        "CREATE TRIGGER tu BEFORE INSERT ON t BEGIN
         INSERT INTO t(x) VALUES (NEW.y);
        END",
    )
    .unwrap();

    // Dropping column x from table t should succeed (SQLite allows this)
    conn.execute("ALTER TABLE t DROP COLUMN x").unwrap();

    // Verify that executing the trigger now causes an error
    let result = conn.execute("INSERT INTO t VALUES (5)");
    assert!(
        result.is_err(),
        "INSERT should fail because trigger references dropped column"
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_drop_column_allows_when_update_set_targets_owning_table(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create table
    conn.execute("CREATE TABLE t (x INTEGER, y INTEGER)")
        .unwrap();

    // Create trigger on t that updates t SET x = ... - SQLite allows DROP COLUMN here
    // The error only occurs when the trigger is actually executed
    conn.execute(
        "CREATE TRIGGER tu BEFORE INSERT ON t BEGIN
         UPDATE t SET x = NEW.y WHERE y = 1;
        END",
    )
    .unwrap();

    // Dropping column x from table t should succeed (SQLite allows this)
    conn.execute("ALTER TABLE t DROP COLUMN x").unwrap();

    // Verify that executing the trigger now causes an error
    let result = conn.execute("INSERT INTO t VALUES (5)");
    assert!(
        result.is_err(),
        "INSERT should fail because trigger references dropped column"
    );
}

#[turso_macros::test(mvcc)]
fn test_alter_table_rename_column_qualified_reference_to_trigger_table(db: TempDatabase) {
    let conn = db.connect_limbo();

    // Create two tables
    conn.execute("CREATE TABLE t (x INTEGER, y INTEGER)")
        .unwrap();
    conn.execute("CREATE TABLE u (z INTEGER)").unwrap();

    // Note: SQLite doesn't support qualified references to the trigger's owning table (t.x).
    // SQLite fails the RENAME COLUMN operation with "error in trigger tu: no such column: t.x".
    // We match SQLite's behavior - the rename should fail.

    // Create trigger on t that uses qualified reference t.x (invalid in SQLite)
    conn.execute(
        "CREATE TRIGGER tu AFTER UPDATE ON t BEGIN
         UPDATE u SET z = t.x WHERE z = 1;
        END",
    )
    .unwrap();

    // Rename column x to x_new in table t should fail (SQLite fails with "no such column: t.x")
    let result = conn.execute("ALTER TABLE t RENAME COLUMN x TO x_new");
    assert!(
        result.is_err(),
        "RENAME COLUMN should fail when trigger uses qualified reference to trigger table"
    );

    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("error in trigger") || error_msg.contains("no such column"),
        "Error should mention trigger or column: {error_msg}",
    );
}

/// Regression test for issue #4801: AFTER trigger INSERT corrupts index cursor position.
///
/// When an AFTER UPDATE trigger does INSERT on the same table, it modifies the index being
/// used for the UPDATE scan. If the code reads columns from the index cursor (optimization
/// for non-covering indexes), the cursor position becomes stale after the trigger's INSERT,
/// causing wrong values to be read.
///
/// This test uses the exact SQL from the fuzz test seed 1768993693555 that exposed the bug
/// in the test 'table_index_mutation_fuzz'.
#[turso_macros::test]
fn test_after_trigger_insert_does_not_corrupt_index_cursor(db: TempDatabase) {
    use crate::common::{limbo_exec_rows, sqlite_exec_rows};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    // DDL/DML statements from the fuzz test
    let setup_stmts = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, c0 INTEGER, c1 INTEGER, c2 INTEGER, c3 INTEGER)",
        "CREATE INDEX idx_0 ON t(c0, c1, c2)",
        "CREATE INDEX idx_1 ON t(c0)",
        "CREATE INDEX idx_2 ON t(LOWER(c1))",
        "CREATE INDEX idx_3 ON t(c3, c2, c1)",
        "CREATE TRIGGER test_trigger AFTER UPDATE ON t BEGIN INSERT OR REPLACE INTO t (c0, c1, c2, c3) VALUES (928, NEW.c1, NEW.c2, OLD.c3); END",
        "INSERT OR IGNORE INTO t (c0, c1, c2, c3) VALUES (305, 505, 678, 408), (925, 33, 642, 768), (583, 336, 680, 881), (550, 569, 563, 307), (582, 496, 313, 909), (646, 904, 180, 6), (860, 120, 210, 757), (932, 252, 319, 425), (408, 927, 967, 472), (968, 655, 488, 815), (192, 492, 320, 583), (290, 819, 155, 412), (229, 397, 804, 872), (382, 602, 311, 134), (980, 231, 975, 87), (40, 982, 849, 979), (389, 110, 692, 784), (122, 17, 894, 15), (449, 34, 426, 718), (35, 553, 253, 866), (330, 314, 578, 799), (886, 650, 297, 945), (114, 798, 799, 339), (957, 424, 572, 357), (691, 192, 538, 906), (210, 676, 612, 188), (416, 555, 714, 692), (235, 851, 840, 175), (265, 625, 617, 161), (320, 655, 841, 693), (832, 354, 796, 910), (128, 87, 181, 298), (293, 5, 124, 647), (437, 43, 72, 59), (35, 974, 600, 448), (346, 726, 63, 230), (733, 887, 74, 528), (804, 583, 691, 264), (599, 130, 119, 263), (119, 712, 874, 40), (612, 711, 314, 876), (323, 885, 816, 999), (889, 154, 563, 566), (219, 803, 49, 686)",
        "UPDATE t SET c1 = 494, c1 = c2 - 643, c1 = c1 - 603 WHERE c2 = 853",
        "UPDATE t SET c1 = c1 + 569 WHERE c1 = 171",
        "UPDATE t SET c3 = c2 - 928 WHERE c1 = 295 AND c1 % 2 = 0",
        "UPDATE t SET c3 = c1 + 914, c3 = 766 WHERE c1 > 555",
        "UPDATE t SET c3 = 602 WHERE c1 > 761",
        "UPDATE t SET c2 = c2 + 26, c2 = 378 WHERE c2 < 41",
        "UPDATE t SET c2 = 969, c2 = 379, c2 = c2 + 376, c2 = 488 WHERE c1 > 565",
        // This UPDATE triggered the bug in issue #4801:
        // - WHERE c3 < 92 causes optimizer to use idx_3 for scanning
        // - SET c0 = 778 only modifies c0, so c1/c2/c3 are read from existing row
        // - AFTER trigger INSERTs, modifying idx_3 and corrupting parent's index cursor position
        "UPDATE t SET c0 = 550, c0 = c0 - 160, c0 = 225, c0 = 778 WHERE c3 < 92 AND c3 % 2 = 0",
    ];

    // Setup SQLite connection
    let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

    // Setup Limbo connection
    let limbo_conn = db.connect_limbo();

    // Execute all setup statements on both databases
    for stmt in &setup_stmts {
        sqlite_conn.execute(stmt, []).unwrap();
        limbo_conn.execute(stmt).unwrap();
    }

    // Compare results
    let query = "SELECT id, c0, c1, c2, c3 FROM t ORDER BY id";
    let sqlite_rows = sqlite_exec_rows(&sqlite_conn, query);
    let limbo_rows = limbo_exec_rows(&limbo_conn, query);

    assert_eq!(
        sqlite_rows.len(),
        limbo_rows.len(),
        "Row count mismatch: SQLite={}, Limbo={}",
        sqlite_rows.len(),
        limbo_rows.len()
    );

    for (i, (sqlite_row, limbo_row)) in sqlite_rows.iter().zip(limbo_rows.iter()).enumerate() {
        assert_eq!(
            sqlite_row, limbo_row,
            "Row {i} differs!\nSQLite: {sqlite_row:?}\nLimbo:  {limbo_row:?}\n\
             This indicates either a regression back to the bug in issue #4801 or a new bug",
        );
    }
}
