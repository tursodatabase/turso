use crate::common::TempDatabase;

#[test]
fn test_create_trigger() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let db = TempDatabase::new_empty();
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

    let mut stmt = conn.prepare("SELECT * FROM test ORDER BY rowid").unwrap();
    let mut results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push((
                    row.get_value(0).as_int().unwrap(),
                    row.get_value(1).cast_text().unwrap().to_string(),
                ));
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }

    // Row inserted by trigger goes first
    assert_eq!(results[0], (100, "triggered".to_string()));
    assert_eq!(results[1], (1, "hello".to_string()));
}

#[test]
fn test_drop_trigger() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE test (x INTEGER PRIMARY KEY)")
        .unwrap();

    conn.execute("CREATE TRIGGER t1 BEFORE INSERT ON test BEGIN SELECT 1; END")
        .unwrap();

    // Verify trigger exists
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_schema WHERE type='trigger' AND name='t1'")
        .unwrap();
    let mut results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(row.get_value(0).cast_text().unwrap().to_string());
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }
    assert_eq!(results.len(), 1);

    conn.execute("DROP TRIGGER t1").unwrap();

    // Verify trigger is gone
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_schema WHERE type='trigger' AND name='t1'")
        .unwrap();
    let mut results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(row.get_value(0).cast_text().unwrap().to_string());
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }
    assert_eq!(results.len(), 0);
}

#[test]
fn test_trigger_after_insert() {
    let db = TempDatabase::new_empty();
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

    let mut stmt = conn.prepare("SELECT * FROM log").unwrap();
    let mut results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push((
                    row.get_value(0).as_int().unwrap(),
                    row.get_value(1).cast_text().unwrap().to_string(),
                ));
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], (1, "hello".to_string()));
}

#[test]
fn test_trigger_when_clause() {
    let db = TempDatabase::new_empty();
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

    let mut stmt = conn.prepare("SELECT * FROM log").unwrap();
    let mut results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(row.get_value(0).as_int().unwrap());
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], 2);
}

#[test]
fn test_trigger_drop_table_drops_triggers() {
    let db = TempDatabase::new_empty();
    let conn = db.connect_limbo();

    conn.execute("CREATE TABLE test (x INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TRIGGER t1 BEFORE INSERT ON test BEGIN SELECT 1; END")
        .unwrap();

    // Verify trigger exists
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_schema WHERE type='trigger' AND name='t1'")
        .unwrap();
    let mut results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(row.get_value(0).cast_text().unwrap().to_string());
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }
    assert_eq!(results.len(), 1);

    conn.execute("DROP TABLE test").unwrap();

    // Verify trigger is gone
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_schema WHERE type='trigger' AND name='t1'")
        .unwrap();
    let mut results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(row.get_value(0).cast_text().unwrap().to_string());
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }
    assert_eq!(results.len(), 0);
}

#[test]
fn test_trigger_new_old_references() {
    let db = TempDatabase::new_empty();
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

    let mut stmt = conn.prepare("SELECT * FROM log").unwrap();
    let mut results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(row.get_value(0).cast_text().unwrap().to_string());
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], "old=hello new=world");
}

#[test]
fn test_multiple_triggers_same_event() {
    let db = TempDatabase::new_empty();
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

    let mut stmt = conn.prepare("SELECT * FROM log ORDER BY msg").unwrap();
    let mut results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(row.get_value(0).cast_text().unwrap().to_string());
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], "trigger1");
    assert_eq!(results[1], "trigger2");
}

#[test]
fn test_two_triggers_on_same_table() {
    let db = TempDatabase::new_empty();
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
    let mut stmt = conn.prepare("SELECT * FROM log ORDER BY rowid").unwrap();
    let mut results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push(row.get_value(0).cast_text().unwrap().to_string());
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }

    // At minimum, we should see both triggers fire and not infinite loop
    assert!(
        results.len() >= 2,
        "Expected at least 2 log entries, got {}",
        results.len()
    );
    assert!(
        results.iter().any(|s| s.contains("trigger_a")),
        "trigger_a should have fired"
    );
    assert!(
        results.iter().any(|s| s.contains("trigger_b")),
        "trigger_b should have fired"
    );
}

#[test]
fn test_trigger_mutual_recursion() {
    let db = TempDatabase::new_empty();
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
    let mut stmt = conn.prepare("SELECT * FROM t ORDER BY rowid").unwrap();
    let mut t_results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                t_results.push((
                    row.get_value(0).as_int().unwrap(),
                    row.get_value(1).cast_text().unwrap().to_string(),
                ));
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }

    let mut stmt = conn.prepare("SELECT * FROM u ORDER BY rowid").unwrap();
    let mut u_results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                u_results.push((
                    row.get_value(0).as_int().unwrap(),
                    row.get_value(1).cast_text().unwrap().to_string(),
                ));
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }

    // Verify the chain executed without infinite recursion
    assert!(!t_results.is_empty(), "Expected at least 1 entry in t");
    assert!(!u_results.is_empty(), "Expected at least 1 entry in u");

    // Verify initial insert
    assert_eq!(t_results[0], (1, "initial".to_string()));

    // Verify trigger on t fired (inserted into u)
    assert_eq!(u_results[0], (1001, "from_t".to_string()));
}

#[test]
fn test_after_insert_trigger() {
    let db = TempDatabase::new_empty();
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
    let mut stmt = conn
        .prepare("SELECT * FROM audit_log ORDER BY rowid")
        .unwrap();
    let mut results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push((
                    row.get_value(0).cast_text().unwrap().to_string(),
                    row.get_value(1).as_int().unwrap(),
                    row.get_value(2).cast_text().unwrap().to_string(),
                ));
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], ("INSERT".to_string(), 1, "apple".to_string()));
    assert_eq!(results[1], ("INSERT".to_string(), 2, "banana".to_string()));
}

#[test]
fn test_before_update_of_trigger() {
    let db = TempDatabase::new_empty();
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
    let mut stmt = conn
        .prepare("SELECT * FROM price_history ORDER BY rowid")
        .unwrap();
    let mut results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push((
                    row.get_value(0).as_int().unwrap(),
                    row.get_value(1).as_int().unwrap(),
                    row.get_value(2).as_int().unwrap(),
                ));
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }

    // Should have 2 entries (not 3, because name-only update didn't fire)
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (1, 100, 150));
    assert_eq!(results[1], (2, 200, 250));
}

#[test]
fn test_after_update_of_trigger() {
    let db = TempDatabase::new_empty();
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
    let mut stmt = conn
        .prepare("SELECT * FROM salary_changes ORDER BY rowid")
        .unwrap();
    let mut results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push((
                    row.get_value(0).as_int().unwrap(),
                    row.get_value(1).as_int().unwrap(),
                    row.get_value(2).as_int().unwrap(),
                    row.get_value(3).as_int().unwrap(),
                ));
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (1, 50000, 55000, 5000));
    assert_eq!(results[1], (2, 60000, 65000, 5000));
}

fn log(s: &str) -> &str {
    tracing::info!("{}", s);
    s
}

#[test]
fn test_before_delete_trigger() {
    let db = TempDatabase::new_empty();
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
    let mut stmt = conn
        .prepare(log("SELECT * FROM deleted_users ORDER BY id"))
        .unwrap();
    let mut results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push((
                    row.get_value(0).as_int().unwrap(),
                    row.get_value(1).cast_text().unwrap().to_string(),
                    row.get_value(2).as_int().unwrap(),
                ));
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (2, "bob".to_string(), 12345));
    assert_eq!(results[1], (3, "charlie".to_string(), 12345));

    // Verify remaining users
    let mut stmt = conn.prepare(log("SELECT COUNT(*) FROM users")).unwrap();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                assert_eq!(row.get_value(0).as_int().unwrap(), 1);
                break;
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }
}

#[test]
fn test_after_delete_trigger() {
    let db = TempDatabase::new_empty();
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
    let mut stmt = conn
        .prepare("SELECT * FROM order_archive ORDER BY order_id")
        .unwrap();
    let mut results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                results.push((
                    row.get_value(0).as_int().unwrap(),
                    row.get_value(1).as_int().unwrap(),
                    row.get_value(2).as_int().unwrap(),
                ));
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (1, 100, 50));
    assert_eq!(results[1], (3, 100, 100));
}

#[test]
fn test_trigger_with_multiple_statements() {
    let db = TempDatabase::new_empty();
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
    let mut stmt = conn
        .prepare("SELECT * FROM transactions ORDER BY rowid")
        .unwrap();
    let mut trans_results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                trans_results.push((
                    row.get_value(0).as_int().unwrap(),
                    row.get_value(1).as_int().unwrap(),
                    row.get_value(2).as_int().unwrap(),
                ));
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }

    assert_eq!(trans_results.len(), 2);
    assert_eq!(trans_results[0], (1, 1000, 1500));
    assert_eq!(trans_results[1], (2, 2000, 2500));

    // Verify audit table
    let mut stmt = conn.prepare("SELECT * FROM audit ORDER BY rowid").unwrap();
    let mut audit_results = Vec::new();
    loop {
        match stmt.step().unwrap() {
            turso_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                audit_results.push(row.get_value(0).cast_text().unwrap().to_string());
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => {
                stmt.run_once().unwrap();
            }
            _ => panic!("Unexpected step result"),
        }
    }

    assert_eq!(audit_results.len(), 2);
    assert_eq!(audit_results[0], "Balance changed for account 1");
    assert_eq!(audit_results[1], "Balance changed for account 2");
}
