use super::*;
use crate::incremental::dbsp::Delta;
use crate::incremental::operator::{FilterOperator, FilterPredicate};
use crate::schema::{
    BTreeCharacteristics, BTreeTable, ColDef, Column as SchemaColumn, Schema, Type,
};
use crate::storage::pager::CreateBTreeFlags;
use crate::sync::Arc;
use crate::translate::logical::{ColumnInfo, LogicalPlanBuilder, LogicalSchema};
use crate::util::IOExt;
use crate::SqliteDialect;
use crate::{Database, MemoryIO, Pager, IO};
use rustc_hash::FxHashSet as HashSet;
use turso_parser::ast;
use turso_parser::parser::Parser;

// Macro to create a test schema with a users table
macro_rules! test_schema {
    () => {{
        let mut schema = Schema::new();
        let columns = crate::alloc::vec![
            SchemaColumn::new(
                Some("id".to_string()),
                "INTEGER".to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef {
                    primary_key: true,
                    rowid_alias: true,
                    notnull: true,
                    ..Default::default()
                },
            ),
            SchemaColumn::new_default_text(Some("name".to_string()), "TEXT".to_string(), None),
            SchemaColumn::new_default_integer(
                Some("age".to_string()),
                "INTEGER".to_string(),
                None,
            ),
        ];
        let users_table = BTreeTable::new(
            2,
            "users".to_string(),
            crate::alloc::vec![("id".to_string(), turso_parser::ast::SortOrder::Asc)],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        );
        schema
            .add_btree_table(Arc::new(users_table))
            .expect("Test setup: failed to add users table");

        // Add products table for join tests
        let columns = crate::alloc::vec![
            SchemaColumn::new(
                Some("product_id".to_string()),
                "INTEGER".to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef {
                    primary_key: true,
                    rowid_alias: true,
                    notnull: true,
                    ..Default::default()
                },
            ),
            SchemaColumn::new_default_text(
                Some("product_name".to_string()),
                "TEXT".to_string(),
                None,
            ),
            SchemaColumn::new_default_integer(
                Some("price".to_string()),
                "INTEGER".to_string(),
                None,
            ),
        ];
        let products_table = BTreeTable::new(
            3,
            "products".to_string(),
            crate::alloc::vec![("product_id".to_string(), turso_parser::ast::SortOrder::Asc)],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        );
        schema
            .add_btree_table(Arc::new(products_table))
            .expect("Test setup: failed to add products table");

        // Add orders table for join tests
        let columns = crate::alloc::vec![
            SchemaColumn::new(
                Some("order_id".to_string()),
                "INTEGER".to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef {
                    primary_key: true,
                    rowid_alias: true,
                    notnull: true,
                    ..Default::default()
                },
            ),
            SchemaColumn::new_default_integer(
                Some("user_id".to_string()),
                "INTEGER".to_string(),
                None,
            ),
            SchemaColumn::new_default_integer(
                Some("product_id".to_string()),
                "INTEGER".to_string(),
                None,
            ),
            SchemaColumn::new_default_integer(
                Some("quantity".to_string()),
                "INTEGER".to_string(),
                None,
            ),
        ];
        let orders_table = BTreeTable::new(
            4,
            "orders".to_string(),
            crate::alloc::vec![("order_id".to_string(), turso_parser::ast::SortOrder::Asc)],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        );
        schema
            .add_btree_table(Arc::new(orders_table))
            .expect("Test setup: failed to add orders table");

        // Add customers table with id and name for testing column ambiguity
        let columns = crate::alloc::vec![
            SchemaColumn::new(
                Some("id".to_string()),
                "INTEGER".to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef {
                    primary_key: true,
                    rowid_alias: true,
                    notnull: true,
                    ..Default::default()
                },
            ),
            SchemaColumn::new_default_text(Some("name".to_string()), "TEXT".to_string(), None),
        ];
        let customers_table = BTreeTable::new(
            6,
            "customers".to_string(),
            crate::alloc::vec![("id".to_string(), turso_parser::ast::SortOrder::Asc)],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        );
        schema
            .add_btree_table(Arc::new(customers_table))
            .expect("Test setup: failed to add customers table");

        // Add purchases table (junction table for three-way join)
        let columns = crate::alloc::vec![
            SchemaColumn::new(
                Some("id".to_string()),
                "INTEGER".to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef {
                    primary_key: true,
                    rowid_alias: true,
                    notnull: true,
                    ..Default::default()
                },
            ),
            SchemaColumn::new_default_integer(
                Some("customer_id".to_string()),
                "INTEGER".to_string(),
                None,
            ),
            SchemaColumn::new_default_integer(
                Some("vendor_id".to_string()),
                "INTEGER".to_string(),
                None,
            ),
            SchemaColumn::new_default_integer(
                Some("quantity".to_string()),
                "INTEGER".to_string(),
                None,
            ),
        ];
        let purchases_table = BTreeTable::new(
            7,
            "purchases".to_string(),
            crate::alloc::vec![("id".to_string(), turso_parser::ast::SortOrder::Asc)],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        );
        schema
            .add_btree_table(Arc::new(purchases_table))
            .expect("Test setup: failed to add purchases table");

        // Add vendors table with id, name, and price (ambiguous columns with customers)
        let columns = crate::alloc::vec![
            SchemaColumn::new(
                Some("id".to_string()),
                "INTEGER".to_string(),
                None,
                None,
                Type::Integer,
                None,
                ColDef {
                    primary_key: true,
                    rowid_alias: true,
                    notnull: true,
                    ..Default::default()
                },
            ),
            SchemaColumn::new_default_text(Some("name".to_string()), "TEXT".to_string(), None),
            SchemaColumn::new_default_integer(
                Some("price".to_string()),
                "INTEGER".to_string(),
                None,
            ),
        ];
        let vendors_table = BTreeTable::new(
            8,
            "vendors".to_string(),
            crate::alloc::vec![("id".to_string(), turso_parser::ast::SortOrder::Asc)],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        );
        schema
            .add_btree_table(Arc::new(vendors_table))
            .expect("Test setup: failed to add vendors table");

        let columns = crate::alloc::vec![
            SchemaColumn::new_default_integer(
                Some("product_id".to_string()),
                "INTEGER".to_string(),
                None,
            ),
            SchemaColumn::new_default_integer(
                Some("amount".to_string()),
                "INTEGER".to_string(),
                None,
            ),
        ];
        let sales_table = BTreeTable::new(
            2,
            "sales".to_string(),
            crate::alloc::vec![],
            columns,
            BTreeCharacteristics::HAS_ROWID,
            crate::alloc::vec![],
            crate::alloc::vec![],
            crate::alloc::vec![],
            None,
        );
        schema
            .add_btree_table(Arc::new(sales_table))
            .expect("Test setup: failed to add sales table");

        schema
    }};
}

fn setup_btree_for_circuit() -> (Arc<Pager>, i64, i64, i64) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file(io.clone(), ":memory:", Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();
    let pager = conn.pager.load().clone();

    let _ = pager.io.block(|| pager.allocate_page1()).unwrap();

    let main_root_page = pager
        .io
        .block(|| pager.btree_create(&CreateBTreeFlags::new_table()))
        .unwrap() as i64;

    let dbsp_state_page = pager
        .io
        .block(|| pager.btree_create(&CreateBTreeFlags::new_table()))
        .unwrap() as i64;

    let dbsp_state_index_page = pager
        .io
        .block(|| pager.btree_create(&CreateBTreeFlags::new_index()))
        .unwrap() as i64;

    (
        pager,
        main_root_page,
        dbsp_state_page,
        dbsp_state_index_page,
    )
}

// Macro to compile SQL to DBSP circuit
macro_rules! compile_sql {
    ($sql:expr) => {{
        let (pager, main_root_page, dbsp_state_page, dbsp_state_index_page) =
            setup_btree_for_circuit();
        let schema = test_schema!();
        let mut parser = Parser::new($sql.as_bytes());
        let cmd = parser
            .next()
            .unwrap() // This returns Option<Result<Cmd, Error>>
            .unwrap(); // This unwraps the Result

        match cmd {
            ast::Cmd::Stmt(stmt) => {
                let mut builder = LogicalPlanBuilder::new(&schema);
                let logical_plan = builder.build_statement(&stmt).unwrap();
                (
                    DbspCompiler::new(main_root_page, dbsp_state_page, dbsp_state_index_page)
                        .compile(&logical_plan)
                        .unwrap(),
                    pager,
                )
            }
            _ => panic!("Only SQL statements are supported"),
        }
    }};
}

// Macro to assert circuit structure
macro_rules! assert_circuit {
    ($circuit:expr, depth: $depth:expr, root: $root_type:ident) => {
        assert_eq!($circuit.nodes.len(), $depth);
        let node = get_node_at_level(&$circuit, 0);
        assert!(matches!(node.operator, DbspOperator::$root_type { .. }));
    };
}

// Macro to assert operator properties
macro_rules! assert_operator {
    ($circuit:expr, $level:expr, Input { name: $name:expr }) => {{
        let node = get_node_at_level(&$circuit, $level);
        match &node.operator {
            DbspOperator::Input { name, .. } => assert_eq!(name, $name),
            _ => panic!("Expected Input operator at level {}", $level),
        }
    }};
    ($circuit:expr, $level:expr, Filter) => {{
        let node = get_node_at_level(&$circuit, $level);
        assert!(matches!(node.operator, DbspOperator::Filter { .. }));
    }};
    ($circuit:expr, $level:expr, Projection { columns: [$($col:expr),*] }) => {{
        let node = get_node_at_level(&$circuit, $level);
        match &node.operator {
            DbspOperator::Projection { exprs, .. } => {
                let expected_cols = vec![$($col),*];
                let actual_cols: Vec<String> = exprs.iter().map(|e| {
                    match e {
                        DbspExpr::Column(name) => name.clone(),
                        _ => "expr".to_string(),
                    }
                }).collect();
                assert_eq!(actual_cols, expected_cols);
            }
            _ => panic!("Expected Projection operator at level {}", $level),
        }
    }};
}

// Macro to assert filter predicate
macro_rules! assert_filter_predicate {
    ($circuit:expr, $level:expr, $col:literal > $val:literal) => {{
        let node = get_node_at_level(&$circuit, $level);
        match &node.operator {
            DbspOperator::Filter { predicate } => match predicate {
                DbspExpr::BinaryExpr { left, op, right } => {
                    assert!(matches!(op, ast::Operator::Greater));
                    assert!(matches!(&**left, DbspExpr::Column(name) if name == $col));
                    assert!(matches!(&**right, DbspExpr::Literal(Value::Numeric(Numeric::Integer($val)))));
                }
                _ => panic!("Expected binary expression in filter"),
            },
            _ => panic!("Expected Filter operator at level {}", $level),
        }
    }};
    ($circuit:expr, $level:expr, $col:literal < $val:literal) => {{
        let node = get_node_at_level(&$circuit, $level);
        match &node.operator {
            DbspOperator::Filter { predicate } => match predicate {
                DbspExpr::BinaryExpr { left, op, right } => {
                    assert!(matches!(op, ast::Operator::Less));
                    assert!(matches!(&**left, DbspExpr::Column(name) if name == $col));
                    assert!(matches!(&**right, DbspExpr::Literal(Value::Numeric(Numeric::Integer($val)))));
                }
                _ => panic!("Expected binary expression in filter"),
            },
            _ => panic!("Expected Filter operator at level {}", $level),
        }
    }};
    ($circuit:expr, $level:expr, $col:literal = $val:literal) => {{
        let node = get_node_at_level(&$circuit, $level);
        match &node.operator {
            DbspOperator::Filter { predicate } => match predicate {
                DbspExpr::BinaryExpr { left, op, right } => {
                    assert!(matches!(op, ast::Operator::Equals));
                    assert!(matches!(&**left, DbspExpr::Column(name) if name == $col));
                    assert!(matches!(&**right, DbspExpr::Literal(Value::Numeric(Numeric::Integer($val)))));
                }
                _ => panic!("Expected binary expression in filter"),
            },
            _ => panic!("Expected Filter operator at level {}", $level),
        }
    }};
}

// Helper to get node at specific level from root
fn get_node_at_level(circuit: &DbspCircuit, level: usize) -> &DbspNode {
    let mut current_id = circuit.root.expect("Circuit has no root");
    for _ in 0..level {
        let node = circuit.nodes.get(&current_id).expect("Node not found");
        if node.inputs.is_empty() {
            panic!("No more levels available, requested level {level}");
        }
        current_id = node.inputs[0];
    }
    circuit.nodes.get(&current_id).expect("Node not found")
}

// Helper function for tests to execute circuit and extract the Delta result
#[cfg(test)]
fn test_execute(
    circuit: &mut DbspCircuit,
    inputs: HashMap<String, Delta>,
    pager: Arc<Pager>,
) -> Result<Delta> {
    let mut execute_state = ExecuteState::Init {
        input_data: DeltaSet::from_map(inputs),
    };
    match circuit.execute(pager, &mut execute_state)? {
        IOResult::Done(delta) => Ok(delta),
        IOResult::IO(_) => panic!("Unexpected I/O in test"),
    }
}

// Helper to get the committed BTree state from main_data_root
// This reads the actual persisted data from the BTree
#[cfg(test)]
fn get_current_state(pager: Arc<Pager>, circuit: &DbspCircuit) -> Result<Delta> {
    use crate::storage::btree::CursorTrait;

    let mut delta = Delta::new();

    let main_data_root = circuit.main_data_root;
    let num_columns = circuit.output_schema.columns.len() + 1;

    // Create a cursor to read the btree
    let mut btree_cursor = BTreeCursor::new_table(pager.clone(), main_data_root, num_columns);

    // Rewind to the beginning
    pager.io.block(|| btree_cursor.rewind())?;

    // Read all rows from the BTree
    loop {
        // Check if cursor is empty (no more rows)
        if btree_cursor.is_empty() {
            break;
        }

        // Get the rowid
        let rowid = pager.io.block(|| btree_cursor.rowid()).unwrap().unwrap();

        // Get the record at this position
        let record = loop {
            match btree_cursor.record().unwrap() {
                IOResult::Done(r) => break r,
                IOResult::IO(io) => io.wait(&*pager.io).unwrap(),
            }
        }
        .unwrap()
        .to_owned();

        let num_data_columns = record.column_count() - 1;

        let mut values = Vec::with_capacity(num_data_columns);
        let mut values_iter = record.iter()?;

        for _ in 0..num_data_columns {
            let value = values_iter.next().expect("we already checked bounds")?;
            values.push(value.to_owned()?);
        }

        delta.insert(rowid, values);
        pager.io.block(|| btree_cursor.next()).unwrap();
    }
    Ok(delta)
}

#[test]
fn test_simple_projection() {
    let (circuit, _) = compile_sql!("SELECT name FROM users");

    // Circuit has 2 nodes with Projection at root
    assert_circuit!(circuit, depth: 2, root: Projection);

    // Verify operators at each level
    assert_operator!(circuit, 0, Projection { columns: ["name"] });
    assert_operator!(circuit, 1, Input { name: "users" });
}

#[test]
fn test_filter_with_projection() {
    let (circuit, _) = compile_sql!("SELECT name FROM users WHERE age > 18");

    // Circuit has 3 nodes with Projection at root
    assert_circuit!(circuit, depth: 3, root: Projection);

    // Verify operators at each level
    assert_operator!(circuit, 0, Projection { columns: ["name"] });
    assert_operator!(circuit, 1, Filter);
    assert_filter_predicate!(circuit, 1, "age" > 18);
    assert_operator!(circuit, 2, Input { name: "users" });
}

#[test]
fn test_select_star() {
    let (mut circuit, pager) = compile_sql!("SELECT * FROM users");

    // Create test data
    let mut input_delta = Delta::new();
    input_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    input_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(17),
        ],
    );

    // Create input map
    let mut inputs = HashMap::default();
    inputs.insert("users".to_string(), input_delta);

    let result = test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(inputs.clone(), pager.clone()))
        .unwrap();

    // Should have all rows with all columns
    assert_eq!(result.changes.len(), 2);

    // Verify both rows are present with all columns
    for (row, weight) in &result.changes {
        assert_eq!(*weight, 1);
        assert_eq!(row.values.len(), 3); // id, name, age
    }
}

#[test]
fn test_execute_filter() {
    let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE age > 18");

    // Create test data
    let mut input_delta = Delta::new();
    input_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    input_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(17),
        ],
    );
    input_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Charlie".into()),
            Value::from_i64(30),
        ],
    );

    // Create input map
    let mut inputs = HashMap::default();
    inputs.insert("users".to_string(), input_delta);

    let result = test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(inputs.clone(), pager.clone()))
        .unwrap();

    // Should only have Alice and Charlie (age > 18)
    assert_eq!(
        result.changes.len(),
        2,
        "Expected 2 rows after filtering, got {}",
        result.changes.len()
    );

    // Check that the filtered rows are correct
    let names: Vec<String> = result
        .changes
        .iter()
        .filter_map(|(row, weight)| {
            if *weight > 0 && row.values.len() > 1 {
                if let Value::Text(name) = &row.values[1] {
                    Some(name.to_string())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    assert!(
        names.contains(&"Alice".to_string()),
        "Alice should be in results"
    );
    assert!(
        names.contains(&"Charlie".to_string()),
        "Charlie should be in results"
    );
    assert!(
        !names.contains(&"Bob".to_string()),
        "Bob should not be in results"
    );
}

#[test]
fn test_simple_column_projection() {
    let (mut circuit, pager) = compile_sql!("SELECT name, age FROM users");

    // Create test data
    let mut input_delta = Delta::new();
    input_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    input_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(17),
        ],
    );

    // Create input map
    let mut inputs = HashMap::default();
    inputs.insert("users".to_string(), input_delta);

    let result = test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(inputs.clone(), pager.clone()))
        .unwrap();

    // Should have all rows but only 2 columns (name, age)
    assert_eq!(result.changes.len(), 2);

    for (row, _) in &result.changes {
        assert_eq!(row.values.len(), 2); // Only name and age
                                         // First value should be name (Text)
        assert!(matches!(&row.values[0], Value::Text(_)));
        // Second value should be age (Integer)
        assert!(matches!(
            &row.values[1],
            Value::Numeric(Numeric::Integer(_))
        ));
    }
}

#[test]
fn test_simple_aggregation() {
    // Test COUNT(*) with GROUP BY
    let (mut circuit, pager) = compile_sql!("SELECT age, COUNT(*) FROM users GROUP BY age");

    // Create test data
    let mut input_delta = Delta::new();
    input_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    input_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(25),
        ],
    );
    input_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Charlie".into()),
            Value::from_i64(30),
        ],
    );

    // Create input map
    let mut inputs = HashMap::default();
    inputs.insert("users".to_string(), input_delta);

    let result = test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(inputs.clone(), pager.clone()))
        .unwrap();

    // Should have 2 groups: age 25 with count 2, age 30 with count 1
    assert_eq!(result.changes.len(), 2);

    // Check the results
    let mut found_25 = false;
    let mut found_30 = false;

    for (row, weight) in &result.changes {
        assert_eq!(*weight, 1);
        assert_eq!(row.values.len(), 2); // age, count

        if let (Value::Numeric(Numeric::Integer(age)), Value::Numeric(Numeric::Integer(count))) =
            (&row.values[0], &row.values[1])
        {
            if *age == 25 {
                assert_eq!(*count, 2, "Age 25 should have count 2");
                found_25 = true;
            } else if *age == 30 {
                assert_eq!(*count, 1, "Age 30 should have count 1");
                found_30 = true;
            }
        }
    }

    assert!(found_25, "Should have group for age 25");
    assert!(found_30, "Should have group for age 30");
}

#[test]
fn test_sum_aggregation() {
    // Test SUM with GROUP BY
    let (mut circuit, pager) = compile_sql!("SELECT name, SUM(age) FROM users GROUP BY name");

    // Create test data - some names appear multiple times
    let mut input_delta = Delta::new();
    input_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    input_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Alice".into()),
            Value::from_i64(30),
        ],
    );
    input_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Bob".into()),
            Value::from_i64(20),
        ],
    );

    // Create input map
    let mut inputs = HashMap::default();
    inputs.insert("users".to_string(), input_delta);

    let result = test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(inputs.clone(), pager.clone()))
        .unwrap();

    // Should have 2 groups: Alice with sum 55, Bob with sum 20
    assert_eq!(result.changes.len(), 2);

    for (row, weight) in &result.changes {
        assert_eq!(*weight, 1);
        assert_eq!(row.values.len(), 2); // name, sum

        if let (Value::Text(name), Value::Numeric(Numeric::Float(sum))) =
            (&row.values[0], &row.values[1])
        {
            if name.as_str() == "Alice" {
                assert_eq!(*sum, 55.0, "Alice should have sum 55");
            } else if name.as_str() == "Bob" {
                assert_eq!(*sum, 20.0, "Bob should have sum 20");
            }
        }
    }
}

#[test]
fn test_aggregation_without_group_by() {
    // Test aggregation without GROUP BY - should produce a single row
    let (mut circuit, pager) = compile_sql!("SELECT COUNT(*), SUM(age), AVG(age) FROM users");

    // Create test data
    let mut input_delta = Delta::new();
    input_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    input_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(30),
        ],
    );
    input_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Charlie".into()),
            Value::from_i64(20),
        ],
    );

    // Create input map
    let mut inputs = HashMap::default();
    inputs.insert("users".to_string(), input_delta);

    let result = test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(inputs.clone(), pager.clone()))
        .unwrap();

    // Should have exactly 1 row with all aggregates
    assert_eq!(
        result.changes.len(),
        1,
        "Should have exactly one result row"
    );

    let (row, weight) = result.changes.first().unwrap();
    assert_eq!(*weight, 1);
    assert_eq!(row.values.len(), 3); // count, sum, avg

    // Check aggregate results
    // COUNT should be Integer
    if let Value::Numeric(Numeric::Integer(count)) = &row.values[0] {
        assert_eq!(*count, 3, "COUNT(*) should be 3");
    } else {
        panic!("COUNT should be Integer, got {:?}", row.values[0]);
    }

    // SUM can be Integer (if whole number) or Float
    match &row.values[1] {
        Value::Numeric(Numeric::Integer(sum)) => assert_eq!(*sum, 75, "SUM(age) should be 75"),
        Value::Numeric(Numeric::Float(sum)) => {
            assert_eq!(f64::from(*sum), 75.0, "SUM(age) should be 75.0")
        }
        other => panic!("SUM should be Integer or Float, got {other:?}"),
    }

    // AVG should be Float
    if let Value::Numeric(Numeric::Float(avg)) = &row.values[2] {
        assert_eq!(f64::from(*avg), 25.0, "AVG(age) should be 25.0");
    } else {
        panic!("AVG should be Float, got {:?}", row.values[2]);
    }
}

#[test]
fn test_expression_projection_execution() {
    // Test that complex expressions work through VDBE compilation
    let (mut circuit, pager) = compile_sql!("SELECT hex(id) FROM users");

    // Create test data
    let mut input_delta = Delta::new();
    input_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    input_delta.insert(
        2,
        vec![
            Value::from_i64(255),
            Value::Text("Bob".into()),
            Value::from_i64(17),
        ],
    );

    // Create input map
    let mut inputs = HashMap::default();
    inputs.insert("users".to_string(), input_delta);

    let result = test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(inputs.clone(), pager.clone()))
        .unwrap();

    assert_eq!(result.changes.len(), 2);

    let hex_values: HashMap<i64, String> = result
        .changes
        .iter()
        .map(|(row, _)| {
            let rowid = row.rowid;
            if let Value::Text(text) = &row.values[0] {
                (rowid, text.to_string())
            } else {
                panic!("Expected Text value for hex() result");
            }
        })
        .collect();

    assert_eq!(
        hex_values.get(&1).unwrap(),
        "31",
        "hex(1) should return '31' (hex of ASCII '1')"
    );

    assert_eq!(
        hex_values.get(&2).unwrap(),
        "323535",
        "hex(255) should return '323535' (hex of ASCII '2', '5', '5')"
    );
}

// TODO: This test currently fails on incremental updates.
// The initial execution works correctly, but incremental updates produce
// incorrect results (3 changes instead of 2, with wrong values).
// This tests that the aggregate operator correctly handles incremental
// updates when it's sandwiched between projection operators.
#[test]
fn test_projection_aggregation_projection_pattern() {
    // Test pattern: projection -> aggregation -> projection
    // Query: SELECT HEX(SUM(age + 2)) FROM users
    let (mut circuit, pager) = compile_sql!("SELECT HEX(SUM(age + 2)) FROM users");

    // Initial input data
    let mut input_delta = Delta::new();
    input_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".to_string().into()),
            Value::from_i64(25),
        ],
    );
    input_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".to_string().into()),
            Value::from_i64(30),
        ],
    );
    input_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Charlie".to_string().into()),
            Value::from_i64(35),
        ],
    );

    let mut input_data = HashMap::default();
    input_data.insert("users".to_string(), input_delta);

    let result = test_execute(&mut circuit, input_data.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(input_data.clone(), pager.clone()))
        .unwrap();

    // Expected: SUM(age + 2) = (25+2) + (30+2) + (35+2) = 27 + 32 + 37 = 96
    // HEX(96) should be the hex representation of the string "96" = "3936"
    assert_eq!(result.changes.len(), 1);
    let (row, _weight) = &result.changes[0];
    assert_eq!(row.values.len(), 1);

    // The hex function converts the number to string first, then to hex
    // SUM now returns Float, so 96.0 as string is "96.0", which in hex is "39362E30"
    // (hex of ASCII '9', '6', '.', '0')
    assert_eq!(
        row.values[0],
        Value::Text("39362E30".to_string().into()),
        "HEX(SUM(age + 2)) should return '39362E30' for sum of 96.0"
    );

    // Test incremental update: add a new user
    let mut input_delta = Delta::new();
    input_delta.insert(
        4,
        vec![
            Value::from_i64(4),
            Value::Text("David".to_string().into()),
            Value::from_i64(40),
        ],
    );

    let mut input_data = HashMap::default();
    input_data.insert("users".to_string(), input_delta);

    let result = test_execute(&mut circuit, input_data, pager).unwrap();

    // Expected: new SUM(age + 2) = 96.0 + (40+2) = 138.0
    // HEX(138.0) = hex of "138.0" = "3133382E30"
    assert_eq!(result.changes.len(), 2);

    // First change: remove old aggregate (96.0)
    let (row, weight) = &result.changes[0];
    assert_eq!(*weight, -1);
    assert_eq!(row.values[0], Value::Text("39362E30".to_string().into()));

    // Second change: add new aggregate (138.0)
    let (row, weight) = &result.changes[1];
    assert_eq!(*weight, 1);
    assert_eq!(
        row.values[0],
        Value::Text("3133382E30".to_string().into()),
        "HEX(SUM(age + 2)) should return '3133382E30' for sum of 138.0"
    );
}

#[test]
fn test_nested_projection_with_groupby() {
    // Test pattern: projection -> aggregation with GROUP BY -> projection
    // Query: SELECT name, HEX(SUM(age * 2)) FROM users GROUP BY name
    let (mut circuit, pager) =
        compile_sql!("SELECT name, HEX(SUM(age * 2)) FROM users GROUP BY name");

    // Initial input data
    let mut input_delta = Delta::new();
    input_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".to_string().into()),
            Value::from_i64(25),
        ],
    );
    input_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".to_string().into()),
            Value::from_i64(30),
        ],
    );
    input_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Alice".to_string().into()),
            Value::from_i64(35),
        ],
    );

    let mut input_data = HashMap::default();
    input_data.insert("users".to_string(), input_delta);

    let result = test_execute(&mut circuit, input_data.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(input_data.clone(), pager.clone()))
        .unwrap();

    // Expected results:
    // Alice: SUM(25*2 + 35*2) = 50 + 70 = 120.0, HEX("120.0") = "3132302E30"
    // Bob: SUM(30*2) = 60.0, HEX("60.0") = "36302E30"
    assert_eq!(result.changes.len(), 2);

    let results: HashMap<String, String> = result
        .changes
        .iter()
        .map(|(row, _weight)| {
            let name = match &row.values[0] {
                Value::Text(t) => t.to_string(),
                _ => panic!("Expected text for name"),
            };
            let hex_sum = match &row.values[1] {
                Value::Text(t) => t.to_string(),
                _ => panic!("Expected text for hex value"),
            };
            (name, hex_sum)
        })
        .collect();

    assert_eq!(
        results.get("Alice").unwrap(),
        "3132302E30",
        "Alice's HEX(SUM(age * 2)) should be '3132302E30' (120.0)"
    );
    assert_eq!(
        results.get("Bob").unwrap(),
        "36302E30",
        "Bob's HEX(SUM(age * 2)) should be '36302E30' (60.0)"
    );
}

#[test]
fn test_transaction_context() {
    // Test that uncommitted changes are visible within a transaction
    // but don't affect the operator's internal state
    let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE age > 18");

    // Initialize with some data
    let mut init_data = HashMap::default();
    let mut delta = Delta::new();
    delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(17),
        ],
    );
    init_data.insert("users".to_string(), delta);

    let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
    let state = pager
        .io
        .block(|| circuit.commit(init_data.clone(), pager.clone()))
        .unwrap();

    // Verify initial delta : only Alice (age > 18)
    assert_eq!(state.changes.len(), 1);
    assert_eq!(state.changes[0].0.values[1], Value::Text("Alice".into()));

    // Create uncommitted changes that would be visible in a transaction
    let mut uncommitted = HashMap::default();
    let mut uncommitted_delta = Delta::new();
    // Add Charlie (age 30) - should be visible in transaction
    uncommitted_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Charlie".into()),
            Value::from_i64(30),
        ],
    );
    // Add David (age 15) - should NOT be visible (filtered out)
    uncommitted_delta.insert(
        4,
        vec![
            Value::from_i64(4),
            Value::Text("David".into()),
            Value::from_i64(15),
        ],
    );
    uncommitted.insert("users".to_string(), uncommitted_delta);

    // Execute with uncommitted data - this simulates processing the uncommitted changes
    // through the circuit to see what would be visible
    let tx_result = test_execute(&mut circuit, uncommitted.clone(), pager.clone()).unwrap();

    // The result should show Charlie being added (passes filter, age > 18)
    // David is filtered out (age 15 < 18)
    assert_eq!(tx_result.changes.len(), 1, "Should see Charlie added");
    assert_eq!(
        tx_result.changes[0].0.values[1],
        Value::Text("Charlie".into())
    );

    // Now actually commit Charlie (without uncommitted context)
    let mut commit_data = HashMap::default();
    let mut commit_delta = Delta::new();
    commit_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Charlie".into()),
            Value::from_i64(30),
        ],
    );
    commit_data.insert("users".to_string(), commit_delta);

    let commit_result = test_execute(&mut circuit, commit_data.clone(), pager.clone()).unwrap();

    // The commit result should show Charlie being added
    assert_eq!(commit_result.changes.len(), 1, "Should see Charlie added");
    assert_eq!(
        commit_result.changes[0].0.values[1],
        Value::Text("Charlie".into())
    );

    // Commit the change to make it permanent
    pager
        .io
        .block(|| circuit.commit(commit_data.clone(), pager.clone()))
        .unwrap();

    // Now if we execute again with no changes, we should see no delta
    let empty_result = test_execute(&mut circuit, HashMap::default(), pager).unwrap();
    assert_eq!(empty_result.changes.len(), 0, "No changes when no new data");
}

#[test]
fn test_uncommitted_delete() {
    // Test that uncommitted deletes are handled correctly without affecting operator state
    let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE age > 18");

    // Initialize with some data
    let mut init_data = HashMap::default();
    let mut delta = Delta::new();
    delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(30),
        ],
    );
    delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Charlie".into()),
            Value::from_i64(20),
        ],
    );
    init_data.insert("users".to_string(), delta);

    let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
    let state = pager
        .io
        .block(|| circuit.commit(init_data.clone(), pager.clone()))
        .unwrap();

    // Verify initial delta: Alice, Bob, Charlie (all age > 18)
    assert_eq!(state.changes.len(), 3);

    // Create uncommitted delete for Bob
    let mut uncommitted = HashMap::default();
    let mut uncommitted_delta = Delta::new();
    uncommitted_delta.delete(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(30),
        ],
    );
    uncommitted.insert("users".to_string(), uncommitted_delta);

    // Execute with uncommitted delete
    let tx_result = test_execute(&mut circuit, uncommitted.clone(), pager.clone()).unwrap();

    // Result should show the deleted row that passed the filter
    assert_eq!(
        tx_result.changes.len(),
        1,
        "Should see the uncommitted delete"
    );

    // Verify operator's internal state is unchanged (still has all 3 users)
    let state_after = get_current_state(pager.clone(), &circuit).unwrap();
    assert_eq!(
        state_after.changes.len(),
        3,
        "Internal state should still have all 3 users"
    );

    // Now actually commit the delete
    let mut commit_data = HashMap::default();
    let mut commit_delta = Delta::new();
    commit_delta.delete(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(30),
        ],
    );
    commit_data.insert("users".to_string(), commit_delta);

    let commit_result = test_execute(&mut circuit, commit_data.clone(), pager.clone()).unwrap();

    // Actually commit the delete to update operator state
    pager
        .io
        .block(|| circuit.commit(commit_data.clone(), pager.clone()))
        .unwrap();

    // The commit result should show Bob being deleted
    assert_eq!(commit_result.changes.len(), 1, "Should see Bob deleted");
    assert_eq!(
        commit_result.changes[0].1, -1,
        "Delete should have weight -1"
    );
    assert_eq!(
        commit_result.changes[0].0.values[1],
        Value::Text("Bob".into())
    );

    // After commit, internal state should have only Alice and Charlie
    let final_state = get_current_state(pager, &circuit).unwrap();
    assert_eq!(
        final_state.changes.len(),
        2,
        "After commit, should have Alice and Charlie"
    );

    let names: Vec<String> = final_state
        .changes
        .iter()
        .map(|(row, _)| {
            if let Value::Text(name) = &row.values[1] {
                name.to_string()
            } else {
                panic!("Expected text value");
            }
        })
        .collect();
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Charlie".to_string()));
    assert!(!names.contains(&"Bob".to_string()));
}

#[test]
fn test_uncommitted_update() {
    // Test that uncommitted updates (delete + insert) are handled correctly
    let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE age > 18");

    // Initialize with some data
    let mut init_data = HashMap::default();
    let mut delta = Delta::new();
    delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(17),
        ],
    ); // Bob is 17, filtered out
    init_data.insert("users".to_string(), delta);

    let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(init_data.clone(), pager.clone()))
        .unwrap();

    // Create uncommitted update: Bob turns 19 (update from 17 to 19)
    // This is modeled as delete + insert
    let mut uncommitted = HashMap::default();
    let mut uncommitted_delta = Delta::new();
    uncommitted_delta.delete(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(17),
        ],
    );
    uncommitted_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(19),
        ],
    );
    uncommitted.insert("users".to_string(), uncommitted_delta);

    // Execute with uncommitted update
    let tx_result = test_execute(&mut circuit, uncommitted.clone(), pager.clone()).unwrap();

    // Bob should now appear in the result (age 19 > 18)
    // Consolidate to see the final state
    let mut final_result = tx_result;
    final_result.consolidate();

    assert_eq!(final_result.changes.len(), 1, "Bob should now be in view");
    assert_eq!(
        final_result.changes[0].0.values[1],
        Value::Text("Bob".into())
    );
    assert_eq!(final_result.changes[0].0.values[2], Value::from_i64(19));

    // Now actually commit the update
    let mut commit_data = HashMap::default();
    let mut commit_delta = Delta::new();
    commit_delta.delete(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(17),
        ],
    );
    commit_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(19),
        ],
    );
    commit_data.insert("users".to_string(), commit_delta);

    // Commit the update
    pager
        .io
        .block(|| circuit.commit(commit_data.clone(), pager.clone()))
        .unwrap();

    // After committing, Bob should be in the view's state
    let state = get_current_state(pager, &circuit).unwrap();
    let mut consolidated_state = state;
    consolidated_state.consolidate();

    // Should have both Alice and Bob now
    assert_eq!(
        consolidated_state.changes.len(),
        2,
        "Should have Alice and Bob"
    );

    let names: Vec<String> = consolidated_state
        .changes
        .iter()
        .map(|(row, _)| {
            if let Value::Text(name) = &row.values[1] {
                name.as_str().to_string()
            } else {
                panic!("Expected text value");
            }
        })
        .collect();
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Bob".to_string()));
}

#[test]
fn test_uncommitted_filtered_delete() {
    // Test deleting a row that doesn't pass the filter
    let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE age > 18");

    // Initialize with mixed data
    let mut init_data = HashMap::default();
    let mut delta = Delta::new();
    delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(15),
        ],
    ); // Bob doesn't pass filter
    init_data.insert("users".to_string(), delta);

    let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(init_data.clone(), pager.clone()))
        .unwrap();

    // Create uncommitted delete for Bob (who isn't in the view because age=15)
    let mut uncommitted = HashMap::default();
    let mut uncommitted_delta = Delta::new();
    uncommitted_delta.delete(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(15),
        ],
    );
    uncommitted.insert("users".to_string(), uncommitted_delta);

    // Execute with uncommitted delete - should produce no output changes
    let tx_result = test_execute(&mut circuit, uncommitted, pager.clone()).unwrap();

    // Bob wasn't in the view, so deleting him produces no output
    assert_eq!(
        tx_result.changes.len(),
        0,
        "Deleting filtered row produces no changes"
    );

    // The view state should still only have Alice
    let state = get_current_state(pager, &circuit).unwrap();
    assert_eq!(state.changes.len(), 1, "View still has only Alice");
    assert_eq!(state.changes[0].0.values[1], Value::Text("Alice".into()));
}

#[test]
fn test_uncommitted_mixed_operations() {
    // Test multiple uncommitted operations together
    let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE age > 18");

    // Initialize with some data
    let mut init_data = HashMap::default();
    let mut delta = Delta::new();
    delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(30),
        ],
    );
    init_data.insert("users".to_string(), delta);

    let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(init_data.clone(), pager.clone()))
        .unwrap();

    // Verify initial state
    let state = get_current_state(pager.clone(), &circuit).unwrap();
    assert_eq!(state.changes.len(), 2);

    // Create uncommitted changes:
    // - Delete Alice
    // - Update Bob's age to 35
    // - Insert Charlie (age 40)
    // - Insert David (age 16, filtered out)
    let mut uncommitted = HashMap::default();
    let mut uncommitted_delta = Delta::new();
    // Delete Alice
    uncommitted_delta.delete(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    // Update Bob (delete + insert)
    uncommitted_delta.delete(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(30),
        ],
    );
    uncommitted_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(35),
        ],
    );
    // Insert Charlie
    uncommitted_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Charlie".into()),
            Value::from_i64(40),
        ],
    );
    // Insert David (will be filtered)
    uncommitted_delta.insert(
        4,
        vec![
            Value::from_i64(4),
            Value::Text("David".into()),
            Value::from_i64(16),
        ],
    );
    uncommitted.insert("users".to_string(), uncommitted_delta);

    // Execute with uncommitted changes
    let tx_result = test_execute(&mut circuit, uncommitted.clone(), pager.clone()).unwrap();

    // Result should show all changes: delete Alice, update Bob, insert Charlie and David
    assert_eq!(
        tx_result.changes.len(),
        4,
        "Should see all uncommitted mixed operations"
    );

    // Verify operator's internal state is unchanged
    let state_after = get_current_state(pager.clone(), &circuit).unwrap();
    assert_eq!(state_after.changes.len(), 2, "Still has Alice and Bob");

    // Commit all changes
    let mut commit_data = HashMap::default();
    let mut commit_delta = Delta::new();
    commit_delta.delete(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    commit_delta.delete(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(30),
        ],
    );
    commit_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(35),
        ],
    );
    commit_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Charlie".into()),
            Value::from_i64(40),
        ],
    );
    commit_delta.insert(
        4,
        vec![
            Value::from_i64(4),
            Value::Text("David".into()),
            Value::from_i64(16),
        ],
    );
    commit_data.insert("users".to_string(), commit_delta);

    let commit_result = test_execute(&mut circuit, commit_data.clone(), pager.clone()).unwrap();

    // Should see: Alice deleted, Bob deleted, Bob inserted, Charlie inserted
    // (David filtered out)
    assert_eq!(commit_result.changes.len(), 4, "Should see 4 changes");

    // Actually commit the changes to update operator state
    pager
        .io
        .block(|| circuit.commit(commit_data.clone(), pager.clone()))
        .unwrap();

    // After all commits, execute with no changes should return empty delta
    let empty_result = test_execute(&mut circuit, HashMap::default(), pager).unwrap();
    assert_eq!(empty_result.changes.len(), 0, "No changes when no new data");
}

#[test]
fn test_uncommitted_aggregation() {
    // Test that aggregations work correctly with uncommitted changes
    // This tests the specific scenario where a transaction adds new data
    // and we need to see correct aggregation results within the transaction

    // Create a sales table schema for testing
    let _ = test_schema!();

    let (mut circuit, pager) = compile_sql!(
        "SELECT product_id, SUM(amount) as total, COUNT(*) as cnt FROM sales GROUP BY product_id"
    );

    // Initialize with base data: (1, 100), (1, 200), (2, 150), (2, 250)
    let mut init_data = HashMap::default();
    let mut delta = Delta::new();
    delta.insert(1, vec![Value::from_i64(1), Value::from_i64(100)]);
    delta.insert(2, vec![Value::from_i64(1), Value::from_i64(200)]);
    delta.insert(3, vec![Value::from_i64(2), Value::from_i64(150)]);
    delta.insert(4, vec![Value::from_i64(2), Value::from_i64(250)]);
    init_data.insert("sales".to_string(), delta);

    let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(init_data.clone(), pager.clone()))
        .unwrap();

    // Verify initial state: product 1 total=300, product 2 total=400
    let state = get_current_state(pager.clone(), &circuit).unwrap();
    assert_eq!(state.changes.len(), 2, "Should have 2 product groups");

    // Build a map of product_id -> (total, count)
    let initial_results: HashMap<i64, (i64, i64)> = state
        .changes
        .iter()
        .map(|(row, _)| {
            // SUM might return Integer or Float, COUNT returns Integer
            let product_id = match &row.values[0] {
                Value::Numeric(Numeric::Integer(id)) => *id,
                _ => panic!("Product ID should be Integer, got {:?}", row.values[0]),
            };

            let total = match &row.values[1] {
                Value::Numeric(Numeric::Integer(t)) => *t,
                Value::Numeric(Numeric::Float(t)) => f64::from(*t) as i64,
                _ => panic!("Total should be numeric, got {:?}", row.values[1]),
            };

            let count = match &row.values[2] {
                Value::Numeric(Numeric::Integer(c)) => *c,
                _ => panic!("Count should be Integer, got {:?}", row.values[2]),
            };

            (product_id, (total, count))
        })
        .collect();

    assert_eq!(
        initial_results.get(&1).unwrap(),
        &(300, 2),
        "Product 1 should have total=300, count=2"
    );
    assert_eq!(
        initial_results.get(&2).unwrap(),
        &(400, 2),
        "Product 2 should have total=400, count=2"
    );

    // Create uncommitted changes: INSERT (1, 50), (3, 300)
    let mut uncommitted = HashMap::default();
    let mut uncommitted_delta = Delta::new();
    uncommitted_delta.insert(5, vec![Value::from_i64(1), Value::from_i64(50)]); // Add to product 1
    uncommitted_delta.insert(6, vec![Value::from_i64(3), Value::from_i64(300)]); // New product 3
    uncommitted.insert("sales".to_string(), uncommitted_delta);

    // Execute with uncommitted data - simulating a read within transaction
    let tx_result = test_execute(&mut circuit, uncommitted.clone(), pager.clone()).unwrap();

    // Result should show the aggregate changes from uncommitted data
    // Product 1: retraction of (300, 2) and insertion of (350, 3)
    // Product 3: insertion of (300, 1) - new product
    assert_eq!(
        tx_result.changes.len(),
        3,
        "Should see aggregate changes from uncommitted data"
    );

    // IMPORTANT: Verify operator's internal state is unchanged
    let state_after = get_current_state(pager.clone(), &circuit).unwrap();
    assert_eq!(
        state_after.changes.len(),
        2,
        "Internal state should still have 2 groups"
    );

    // Verify the internal state still has original values
    let state_results: HashMap<i64, (i64, i64)> = state_after
        .changes
        .iter()
        .map(|(row, _)| {
            let product_id = match &row.values[0] {
                Value::Numeric(Numeric::Integer(id)) => *id,
                _ => panic!("Product ID should be Integer"),
            };

            let total = match &row.values[1] {
                Value::Numeric(Numeric::Integer(t)) => *t,
                Value::Numeric(Numeric::Float(t)) => f64::from(*t) as i64,
                _ => panic!("Total should be numeric"),
            };

            let count = match &row.values[2] {
                Value::Numeric(Numeric::Integer(c)) => *c,
                _ => panic!("Count should be Integer"),
            };

            (product_id, (total, count))
        })
        .collect();

    assert_eq!(
        state_results.get(&1).unwrap(),
        &(300, 2),
        "Product 1 unchanged"
    );
    assert_eq!(
        state_results.get(&2).unwrap(),
        &(400, 2),
        "Product 2 unchanged"
    );
    assert!(
        !state_results.contains_key(&3),
        "Product 3 should not be in committed state"
    );

    // Now actually commit the changes
    let mut commit_data = HashMap::default();
    let mut commit_delta = Delta::new();
    commit_delta.insert(5, vec![Value::from_i64(1), Value::from_i64(50)]);
    commit_delta.insert(6, vec![Value::from_i64(3), Value::from_i64(300)]);
    commit_data.insert("sales".to_string(), commit_delta);

    let commit_result = test_execute(&mut circuit, commit_data.clone(), pager.clone()).unwrap();

    // Should see changes for product 1 (updated) and product 3 (new)
    assert_eq!(
        commit_result.changes.len(),
        3,
        "Should see 3 changes (delete old product 1, insert new product 1, insert product 3)"
    );

    // Actually commit the changes to update operator state
    pager
        .io
        .block(|| circuit.commit(commit_data.clone(), pager.clone()))
        .unwrap();

    // After commit, verify final state
    let final_state = get_current_state(pager, &circuit).unwrap();
    assert_eq!(
        final_state.changes.len(),
        3,
        "Should have 3 product groups after commit"
    );

    let final_results: HashMap<i64, (i64, i64)> = final_state
        .changes
        .iter()
        .map(|(row, _)| {
            let product_id = match &row.values[0] {
                Value::Numeric(Numeric::Integer(id)) => *id,
                _ => panic!("Product ID should be Integer"),
            };

            let total = match &row.values[1] {
                Value::Numeric(Numeric::Integer(t)) => *t,
                Value::Numeric(Numeric::Float(t)) => f64::from(*t) as i64,
                _ => panic!("Total should be numeric"),
            };

            let count = match &row.values[2] {
                Value::Numeric(Numeric::Integer(c)) => *c,
                _ => panic!("Count should be Integer"),
            };

            (product_id, (total, count))
        })
        .collect();

    assert_eq!(
        final_results.get(&1).unwrap(),
        &(350, 3),
        "Product 1 should have total=350, count=3"
    );
    assert_eq!(
        final_results.get(&2).unwrap(),
        &(400, 2),
        "Product 2 should have total=400, count=2"
    );
    assert_eq!(
        final_results.get(&3).unwrap(),
        &(300, 1),
        "Product 3 should have total=300, count=1"
    );
}

#[test]
fn test_uncommitted_data_visible_in_transaction() {
    // Test that uncommitted INSERTs are visible within the same transaction
    // This simulates: BEGIN; INSERT ...; SELECT * FROM view; COMMIT;

    let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE age > 18");

    // Initialize with some data - need to match the schema (id, name, age)
    let mut init_data = HashMap::default();
    let mut delta = Delta::new();
    delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(30),
        ],
    );
    init_data.insert("users".to_string(), delta);

    let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(init_data.clone(), pager.clone()))
        .unwrap();

    // Verify initial state
    let state = get_current_state(pager.clone(), &circuit).unwrap();
    assert_eq!(
        state.len(),
        2,
        "Should have 2 users initially (both pass age > 18 filter)"
    );

    // Simulate a transaction: INSERT new users that pass the filter - match schema (id, name, age)
    let mut uncommitted = HashMap::default();
    let mut tx_delta = Delta::new();
    tx_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Charlie".into()),
            Value::from_i64(35),
        ],
    );
    tx_delta.insert(
        4,
        vec![
            Value::from_i64(4),
            Value::Text("David".into()),
            Value::from_i64(20),
        ],
    );
    uncommitted.insert("users".to_string(), tx_delta);

    // Execute with uncommitted data - this should return the uncommitted changes
    // that passed through the filter (age > 18)
    let tx_result = test_execute(&mut circuit, uncommitted.clone(), pager.clone()).unwrap();

    // IMPORTANT: tx_result should contain the filtered uncommitted changes!
    // Both Charlie (35) and David (20) should pass the age > 18 filter
    assert_eq!(
        tx_result.len(),
        2,
        "Should see 2 uncommitted rows that pass filter"
    );

    // Verify the uncommitted results contain the expected rows
    let has_charlie = tx_result.changes.iter().any(|(row, _)| row.rowid == 3);
    assert!(
        has_charlie,
        "Should find Charlie (rowid=3) in uncommitted results"
    );

    let has_david = tx_result.changes.iter().any(|(row, _)| row.rowid == 4);
    assert!(
        has_david,
        "Should find David (rowid=4) in uncommitted results"
    );

    // CRITICAL: Verify the operator state wasn't modified by uncommitted execution
    let state_after_uncommitted = get_current_state(pager, &circuit).unwrap();
    assert_eq!(
        state_after_uncommitted.len(),
        2,
        "State should STILL be 2 after uncommitted execution - only Alice and Bob"
    );

    // The state should not contain Charlie or David
    let has_charlie_in_state = state_after_uncommitted
        .changes
        .iter()
        .any(|(row, _)| row.rowid == 3);
    let has_david_in_state = state_after_uncommitted
        .changes
        .iter()
        .any(|(row, _)| row.rowid == 4);
    assert!(
        !has_charlie_in_state,
        "Charlie should NOT be in operator state (uncommitted)"
    );
    assert!(
        !has_david_in_state,
        "David should NOT be in operator state (uncommitted)"
    );
}

#[test]
fn test_uncommitted_aggregation_with_rollback() {
    // Test that rollback properly discards uncommitted aggregation changes
    // Similar to test_uncommitted_aggregation but explicitly tests rollback semantics

    // Create a simple aggregation circuit
    let (mut circuit, pager) = compile_sql!("SELECT age, COUNT(*) as cnt FROM users GROUP BY age");

    // Initialize with some data
    let mut init_data = HashMap::default();
    let mut delta = Delta::new();
    delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(30),
        ],
    );
    delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Charlie".into()),
            Value::from_i64(25),
        ],
    );
    delta.insert(
        4,
        vec![
            Value::from_i64(4),
            Value::Text("David".into()),
            Value::from_i64(30),
        ],
    );
    init_data.insert("users".to_string(), delta);

    let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(init_data.clone(), pager.clone()))
        .unwrap();

    // Verify initial state: age 25 count=2, age 30 count=2
    let state = get_current_state(pager.clone(), &circuit).unwrap();
    assert_eq!(state.changes.len(), 2);

    let initial_counts: HashMap<i64, i64> = state
        .changes
        .iter()
        .map(|(row, _)| {
            if let (
                Value::Numeric(Numeric::Integer(age)),
                Value::Numeric(Numeric::Integer(count)),
            ) = (&row.values[0], &row.values[1])
            {
                (*age, *count)
            } else {
                panic!("Unexpected value types");
            }
        })
        .collect();

    assert_eq!(initial_counts.get(&25).unwrap(), &2);
    assert_eq!(initial_counts.get(&30).unwrap(), &2);

    // Create uncommitted changes that would affect aggregations
    let mut uncommitted = HashMap::default();
    let mut uncommitted_delta = Delta::new();
    // Add more people aged 25
    uncommitted_delta.insert(
        5,
        vec![
            Value::from_i64(5),
            Value::Text("Eve".into()),
            Value::from_i64(25),
        ],
    );
    uncommitted_delta.insert(
        6,
        vec![
            Value::from_i64(6),
            Value::Text("Frank".into()),
            Value::from_i64(25),
        ],
    );
    // Add person aged 35 (new group)
    uncommitted_delta.insert(
        7,
        vec![
            Value::from_i64(7),
            Value::Text("Grace".into()),
            Value::from_i64(35),
        ],
    );
    // Delete Bob (age 30)
    uncommitted_delta.delete(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(30),
        ],
    );
    uncommitted.insert("users".to_string(), uncommitted_delta);

    // Execute with uncommitted changes
    let tx_result = test_execute(&mut circuit, uncommitted.clone(), pager.clone()).unwrap();

    // Should see the aggregate changes from uncommitted data
    // Age 25: retraction of count 1 and insertion of count 2
    // Age 30: insertion of count 1 (Bob is new for age 30)
    assert!(
        !tx_result.changes.is_empty(),
        "Should see aggregate changes from uncommitted data"
    );

    // Verify internal state is unchanged (simulating rollback by not committing)
    let state_after_rollback = get_current_state(pager, &circuit).unwrap();
    assert_eq!(
        state_after_rollback.changes.len(),
        2,
        "Should still have 2 age groups"
    );

    let rollback_counts: HashMap<i64, i64> = state_after_rollback
        .changes
        .iter()
        .map(|(row, _)| {
            if let (
                Value::Numeric(Numeric::Integer(age)),
                Value::Numeric(Numeric::Integer(count)),
            ) = (&row.values[0], &row.values[1])
            {
                (*age, *count)
            } else {
                panic!("Unexpected value types");
            }
        })
        .collect();

    // Verify counts are unchanged after rollback
    assert_eq!(
        rollback_counts.get(&25).unwrap(),
        &2,
        "Age 25 count unchanged"
    );
    assert_eq!(
        rollback_counts.get(&30).unwrap(),
        &2,
        "Age 30 count unchanged"
    );
    assert!(
        !rollback_counts.contains_key(&35),
        "Age 35 should not exist"
    );
}

#[test]
fn test_circuit_rowid_update_consolidation() {
    let (pager, p1, p2, p3) = setup_btree_for_circuit();

    // Test that circuit properly consolidates state when rowid changes
    let mut circuit = DbspCircuit::new(p1, p2, p3);

    // Create a simple filter node
    let schema = Arc::new(LogicalSchema::new(vec![
        ColumnInfo {
            name: "id".to_string(),
            ty: Type::Integer,
            database: None,
            table: None,
            table_alias: None,
        },
        ColumnInfo {
            name: "value".to_string(),
            ty: Type::Integer,
            database: None,
            table: None,
            table_alias: None,
        },
    ]));

    // First create an input node with InputOperator
    let input_id = circuit.add_node(
        DbspOperator::Input {
            name: "test".to_string(),
            schema: schema.clone(),
        },
        vec![],
        Box::new(InputOperator::new("test".to_string())),
    );

    let filter_op = FilterOperator::new(FilterPredicate::GreaterThan {
        column_idx: 1, // "value" is at index 1
        value: Value::from_i64(10),
    });

    // Create the filter predicate using DbspExpr
    let predicate = DbspExpr::BinaryExpr {
        left: Box::new(DbspExpr::Column("value".to_string())),
        op: ast::Operator::Greater,
        right: Box::new(DbspExpr::Literal(Value::from_i64(10))),
    };

    let filter_id = circuit.add_node(
        DbspOperator::Filter { predicate },
        vec![input_id], // Filter takes input from the input node
        Box::new(filter_op),
    );

    circuit.set_root(filter_id, schema);

    // Initialize with a row
    let mut init_data = HashMap::default();
    let mut delta = Delta::new();
    delta.insert(5, vec![Value::from_i64(5), Value::from_i64(20)]);
    init_data.insert("test".to_string(), delta);

    let _ = test_execute(&mut circuit, init_data.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(init_data.clone(), pager.clone()))
        .unwrap();

    // Verify initial state
    let state = get_current_state(pager.clone(), &circuit).unwrap();
    assert_eq!(state.changes.len(), 1);
    assert_eq!(state.changes[0].0.rowid, 5);

    // Now update the rowid from 5 to 3
    let mut update_data = HashMap::default();
    let mut update_delta = Delta::new();
    update_delta.delete(5, vec![Value::from_i64(5), Value::from_i64(20)]);
    update_delta.insert(3, vec![Value::from_i64(3), Value::from_i64(20)]);
    update_data.insert("test".to_string(), update_delta);

    test_execute(&mut circuit, update_data.clone(), pager.clone()).unwrap();

    // Commit the changes to update operator state
    pager
        .io
        .block(|| circuit.commit(update_data.clone(), pager.clone()))
        .unwrap();

    // The circuit should consolidate the state properly
    let final_state = get_current_state(pager, &circuit).unwrap();
    assert_eq!(
        final_state.changes.len(),
        1,
        "Circuit should consolidate to single row"
    );
    assert_eq!(final_state.changes[0].0.rowid, 3);
    assert_eq!(
        final_state.changes[0].0.values,
        vec![Value::from_i64(3), Value::from_i64(20)]
    );
    assert_eq!(final_state.changes[0].1, 1);
}

#[test]
fn test_circuit_respects_multiplicities() {
    let (mut circuit, pager) = compile_sql!("SELECT * from users");

    // Insert same row twice (multiplicity 2)
    let mut delta = Delta::new();
    delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );

    let mut inputs = HashMap::default();
    inputs.insert("users".to_string(), delta);
    test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(inputs.clone(), pager.clone()))
        .unwrap();

    // Delete once (should leave multiplicity 1)
    let mut delete_one = Delta::new();
    delete_one.delete(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );

    let mut inputs = HashMap::default();
    inputs.insert("users".to_string(), delete_one);
    test_execute(&mut circuit, inputs.clone(), pager.clone()).unwrap();
    pager
        .io
        .block(|| circuit.commit(inputs.clone(), pager.clone()))
        .unwrap();

    // With proper DBSP: row still exists (weight 2 - 1 = 1)
    let state = get_current_state(pager, &circuit).unwrap();
    let mut consolidated = state;
    consolidated.consolidate();
    assert_eq!(
        consolidated.len(),
        1,
        "Row should still exist with multiplicity 1"
    );
}

#[test]
fn test_join_with_aggregation() {
    // Test join followed by aggregation - verifying actual output
    let (mut circuit, pager) = compile_sql!(
        "SELECT u.name, SUM(o.quantity) as total_quantity
             FROM users u
             JOIN orders o ON u.id = o.user_id
             GROUP BY u.name"
    );

    // Create test data for users
    let mut users_delta = Delta::new();
    users_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(30),
        ],
    );
    users_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(25),
        ],
    );

    // Create test data for orders (order_id, user_id, product_id, quantity)
    let mut orders_delta = Delta::new();
    orders_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::from_i64(1),
            Value::from_i64(101),
            Value::from_i64(5),
        ],
    ); // Alice: 5
    orders_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::from_i64(1),
            Value::from_i64(102),
            Value::from_i64(3),
        ],
    ); // Alice: 3
    orders_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::from_i64(2),
            Value::from_i64(101),
            Value::from_i64(7),
        ],
    ); // Bob: 7
    orders_delta.insert(
        4,
        vec![
            Value::from_i64(4),
            Value::from_i64(1),
            Value::from_i64(103),
            Value::from_i64(2),
        ],
    ); // Alice: 2
    let inputs = HashMap::from_iter([
        ("users".to_string(), users_delta),
        ("orders".to_string(), orders_delta),
    ]);

    let result = test_execute(&mut circuit, inputs, pager).unwrap();

    // Should have 2 results: Alice with total 10, Bob with total 7
    assert_eq!(
        result.len(),
        2,
        "Should have aggregated results for Alice and Bob"
    );

    // Check the results
    let mut results_map: HashMap<String, f64> = HashMap::default();
    for (row, weight) in result.changes {
        assert_eq!(weight, 1);
        assert_eq!(row.values.len(), 2); // name and total_quantity

        if let (Value::Text(name), Value::Numeric(Numeric::Float(total))) =
            (&row.values[0], &row.values[1])
        {
            results_map.insert(name.to_string(), f64::from(*total));
        } else {
            panic!("Unexpected value types in result");
        }
    }

    assert_eq!(
        results_map.get("Alice"),
        Some(&10.0),
        "Alice should have total quantity 10"
    );
    assert_eq!(
        results_map.get("Bob"),
        Some(&7.0),
        "Bob should have total quantity 7"
    );
}

#[test]
fn test_join_aggregate_with_filter() {
    // Test complex query with join, filter, and aggregation - verifying output
    let (mut circuit, pager) = compile_sql!(
        "SELECT u.name, SUM(o.quantity) as total
             FROM users u
             JOIN orders o ON u.id = o.user_id
             WHERE u.age > 18
             GROUP BY u.name"
    );

    // Create test data for users
    let mut users_delta = Delta::new();
    users_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(30),
        ],
    ); // age > 18
    users_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(17),
        ],
    ); // age <= 18
    users_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Charlie".into()),
            Value::from_i64(25),
        ],
    ); // age > 18

    // Create test data for orders (order_id, user_id, product_id, quantity)
    let mut orders_delta = Delta::new();
    orders_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::from_i64(1),
            Value::from_i64(101),
            Value::from_i64(5),
        ],
    ); // Alice: 5
    orders_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::from_i64(2),
            Value::from_i64(102),
            Value::from_i64(10),
        ],
    ); // Bob: 10 (should be filtered)
    orders_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::from_i64(3),
            Value::from_i64(101),
            Value::from_i64(7),
        ],
    ); // Charlie: 7
    orders_delta.insert(
        4,
        vec![
            Value::from_i64(4),
            Value::from_i64(1),
            Value::from_i64(103),
            Value::from_i64(3),
        ],
    ); // Alice: 3

    let inputs = HashMap::from_iter([
        ("users".to_string(), users_delta),
        ("orders".to_string(), orders_delta),
    ]);

    let result = test_execute(&mut circuit, inputs, pager).unwrap();

    // Should only have results for Alice and Charlie (Bob filtered out due to age <= 18)
    assert_eq!(
        result.len(),
        2,
        "Should only have results for users with age > 18"
    );

    // Check the results
    let mut results_map: HashMap<String, f64> = HashMap::default();
    for (row, weight) in result.changes {
        assert_eq!(weight, 1);
        assert_eq!(row.values.len(), 2); // name and total

        if let (Value::Text(name), Value::Numeric(Numeric::Float(total))) =
            (&row.values[0], &row.values[1])
        {
            results_map.insert(name.to_string(), f64::from(*total));
        }
    }

    assert_eq!(
        results_map.get("Alice"),
        Some(&8.0),
        "Alice should have total 8"
    );
    assert_eq!(
        results_map.get("Charlie"),
        Some(&7.0),
        "Charlie should have total 7"
    );
    assert_eq!(results_map.get("Bob"), None, "Bob should be filtered out");
}

#[test]
fn test_three_way_join_execution() {
    // Test executing a 3-way join with aggregation
    let (mut circuit, pager) = compile_sql!(
        "SELECT u.name, p.product_name, SUM(o.quantity) as total
             FROM users u
             JOIN orders o ON u.id = o.user_id
             JOIN products p ON o.product_id = p.product_id
             GROUP BY u.name, p.product_name"
    );

    // Create test data for users
    let mut users_delta = Delta::new();
    users_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    users_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(30),
        ],
    );

    // Create test data for products
    let mut products_delta = Delta::new();
    products_delta.insert(
        100,
        vec![
            Value::from_i64(100),
            Value::Text("Widget".into()),
            Value::from_i64(50),
        ],
    );
    products_delta.insert(
        101,
        vec![
            Value::from_i64(101),
            Value::Text("Gadget".into()),
            Value::from_i64(75),
        ],
    );
    products_delta.insert(
        102,
        vec![
            Value::from_i64(102),
            Value::Text("Doohickey".into()),
            Value::from_i64(25),
        ],
    );

    // Create test data for orders joining users and products
    let mut orders_delta = Delta::new();
    // Alice orders 5 Widgets
    orders_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::from_i64(1),
            Value::from_i64(100),
            Value::from_i64(5),
        ],
    );
    // Alice orders 3 Gadgets
    orders_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::from_i64(1),
            Value::from_i64(101),
            Value::from_i64(3),
        ],
    );
    // Bob orders 7 Widgets
    orders_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::from_i64(2),
            Value::from_i64(100),
            Value::from_i64(7),
        ],
    );
    // Bob orders 2 Doohickeys
    orders_delta.insert(
        4,
        vec![
            Value::from_i64(4),
            Value::from_i64(2),
            Value::from_i64(102),
            Value::from_i64(2),
        ],
    );
    // Alice orders 4 more Widgets
    orders_delta.insert(
        5,
        vec![
            Value::from_i64(5),
            Value::from_i64(1),
            Value::from_i64(100),
            Value::from_i64(4),
        ],
    );

    let mut inputs = HashMap::default();
    inputs.insert("users".to_string(), users_delta);
    inputs.insert("products".to_string(), products_delta);
    inputs.insert("orders".to_string(), orders_delta);

    // Execute the 3-way join with aggregation
    let result = test_execute(&mut circuit, inputs.clone(), pager).unwrap();

    // We should get aggregated results for each user-product combination
    // Expected results:
    // - Alice, Widget: 9 (5 + 4)
    // - Alice, Gadget: 3
    // - Bob, Widget: 7
    // - Bob, Doohickey: 2
    assert_eq!(result.len(), 4, "Should have 4 aggregated results");

    // Verify aggregation results
    let mut found_results = HashSet::default();
    for (row, weight) in result.changes.iter() {
        assert_eq!(*weight, 1);
        // Row should have name, product_name, and sum columns
        assert_eq!(row.values.len(), 3);

        if let (Value::Text(name), Value::Text(product), Value::Numeric(Numeric::Float(total))) =
            (&row.values[0], &row.values[1], &row.values[2])
        {
            let key = format!("{}-{}", name.as_ref(), product.as_ref());
            found_results.insert(key.clone());

            match key.as_str() {
                "Alice-Widget" => {
                    assert_eq!(*total, 9.0, "Alice should have ordered 9 Widgets total")
                }
                "Alice-Gadget" => {
                    assert_eq!(*total, 3.0, "Alice should have ordered 3 Gadgets")
                }
                "Bob-Widget" => assert_eq!(*total, 7.0, "Bob should have ordered 7 Widgets"),
                "Bob-Doohickey" => {
                    assert_eq!(*total, 2.0, "Bob should have ordered 2 Doohickeys")
                }
                _ => panic!("Unexpected result: {key}"),
            }
        } else {
            panic!("Unexpected value types in result");
        }
    }

    // Ensure we found all expected combinations
    assert!(found_results.contains("Alice-Widget"));
    assert!(found_results.contains("Alice-Gadget"));
    assert!(found_results.contains("Bob-Widget"));
    assert!(found_results.contains("Bob-Doohickey"));
}

#[test]
fn test_join_execution() {
    let (mut circuit, pager) =
        compile_sql!("SELECT u.name, o.quantity FROM users u JOIN orders o ON u.id = o.user_id");

    // Create test data for users
    let mut users_delta = Delta::new();
    users_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    users_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(30),
        ],
    );

    // Create test data for orders
    let mut orders_delta = Delta::new();
    orders_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::from_i64(1),
            Value::from_i64(100),
            Value::from_i64(5),
        ],
    );
    orders_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::from_i64(1),
            Value::from_i64(101),
            Value::from_i64(3),
        ],
    );
    orders_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::from_i64(2),
            Value::from_i64(102),
            Value::from_i64(7),
        ],
    );

    let mut inputs = HashMap::default();
    inputs.insert("users".to_string(), users_delta);
    inputs.insert("orders".to_string(), orders_delta);

    // Execute the join
    let result = test_execute(&mut circuit, inputs.clone(), pager).unwrap();

    // We should get 3 results (2 orders for Alice, 1 for Bob)
    assert_eq!(result.len(), 3, "Should have 3 join results");

    // Verify the join results contain the correct data
    let results: Vec<_> = result.changes.iter().collect();

    // Check that we have the expected joined rows
    for (row, weight) in results {
        assert_eq!(*weight, 1); // All weights should be 1 for insertions
                                // Row should have name and quantity columns
        assert_eq!(row.values.len(), 2);
    }
}

#[test]
fn test_three_way_join_with_column_ambiguity() {
    // Test three-way join with aggregation where multiple tables have columns with the same name
    // Ensures that column references are correctly resolved to their respective tables
    // Tables: customers(id, name), purchases(id, customer_id, vendor_id, quantity), vendors(id, name, price)
    // Note: both customers and vendors have 'id' and 'name' columns which can cause ambiguity

    let sql = "SELECT c.name as customer_name, v.name as vendor_name,
                          SUM(p.quantity) as total_quantity,
                          SUM(p.quantity * v.price) as total_value
                   FROM customers c
                   JOIN purchases p ON c.id = p.customer_id
                   JOIN vendors v ON p.vendor_id = v.id
                   GROUP BY c.name, v.name";

    let (mut circuit, pager) = compile_sql!(sql);

    // Create test data for customers (id, name)
    let mut customers_delta = Delta::new();
    customers_delta.insert(1, vec![Value::from_i64(1), Value::Text("Alice".into())]);
    customers_delta.insert(2, vec![Value::from_i64(2), Value::Text("Bob".into())]);

    // Create test data for vendors (id, name, price)
    let mut vendors_delta = Delta::new();
    vendors_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Widget Co".into()),
            Value::from_i64(10),
        ],
    );
    vendors_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Gadget Inc".into()),
            Value::from_i64(20),
        ],
    );

    // Create test data for purchases (id, customer_id, vendor_id, quantity)
    let mut purchases_delta = Delta::new();
    // Alice purchases 5 units from Widget Co
    purchases_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::from_i64(1), // customer_id: Alice
            Value::from_i64(1), // vendor_id: Widget Co
            Value::from_i64(5),
        ],
    );
    // Alice purchases 3 units from Gadget Inc
    purchases_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::from_i64(1), // customer_id: Alice
            Value::from_i64(2), // vendor_id: Gadget Inc
            Value::from_i64(3),
        ],
    );
    // Bob purchases 2 units from Widget Co
    purchases_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::from_i64(2), // customer_id: Bob
            Value::from_i64(1), // vendor_id: Widget Co
            Value::from_i64(2),
        ],
    );
    // Alice purchases 4 more units from Widget Co
    purchases_delta.insert(
        4,
        vec![
            Value::from_i64(4),
            Value::from_i64(1), // customer_id: Alice
            Value::from_i64(1), // vendor_id: Widget Co
            Value::from_i64(4),
        ],
    );

    let inputs = HashMap::from_iter([
        ("customers".to_string(), customers_delta),
        ("purchases".to_string(), purchases_delta),
        ("vendors".to_string(), vendors_delta),
    ]);

    let result = test_execute(&mut circuit, inputs, pager).unwrap();

    // Expected results:
    // Alice|Gadget Inc|3|60    (3 units * 20 price = 60)
    // Alice|Widget Co|9|90     (9 units * 10 price = 90)
    // Bob|Widget Co|2|20       (2 units * 10 price = 20)

    assert_eq!(result.len(), 3, "Should have 3 aggregated results");

    // Sort results for consistent testing
    let mut results: Vec<_> = result.changes.into_iter().collect();
    results.sort_by(|a, b| {
        let a_cust = &a.0.values[0];
        let a_vend = &a.0.values[1];
        let b_cust = &b.0.values[0];
        let b_vend = &b.0.values[1];
        (a_cust, a_vend).cmp(&(b_cust, b_vend))
    });

    // Verify Alice's Gadget Inc purchases
    assert_eq!(results[0].0.values[0], Value::Text("Alice".into()));
    assert_eq!(results[0].0.values[1], Value::Text("Gadget Inc".into()));
    assert_eq!(results[0].0.values[2], Value::from_i64(3)); // total_quantity
    assert_eq!(results[0].0.values[3], Value::from_i64(60)); // total_value

    // Verify Alice's Widget Co purchases
    assert_eq!(results[1].0.values[0], Value::Text("Alice".into()));
    assert_eq!(results[1].0.values[1], Value::Text("Widget Co".into()));
    assert_eq!(results[1].0.values[2], Value::from_i64(9)); // total_quantity
    assert_eq!(results[1].0.values[3], Value::from_i64(90)); // total_value

    // Verify Bob's Widget Co purchases
    assert_eq!(results[2].0.values[0], Value::Text("Bob".into()));
    assert_eq!(results[2].0.values[1], Value::Text("Widget Co".into()));
    assert_eq!(results[2].0.values[2], Value::from_i64(2)); // total_quantity
    assert_eq!(results[2].0.values[3], Value::from_i64(20)); // total_value
}

#[test]
fn test_projection_with_function_and_ambiguous_columns() {
    // Test projection with functions operating on potentially ambiguous columns
    // Uses HEX() function on sum of columns from different tables with same names
    // Tables: customers(id, name), vendors(id, name, price), purchases(id, customer_id, vendor_id, quantity)
    // This test ensures column references are correctly resolved to their respective tables

    let sql = "SELECT HEX(c.id + v.id) as hex_sum,
                          UPPER(c.name) as customer_upper,
                          LOWER(v.name) as vendor_lower,
                          c.id * v.price as product_value
                   FROM customers c
                   JOIN vendors v ON c.id = v.id";

    let (mut circuit, pager) = compile_sql!(sql);

    // Create test data for customers (id, name)
    let mut customers_delta = Delta::new();
    customers_delta.insert(1, vec![Value::from_i64(1), Value::Text("Alice".into())]);
    customers_delta.insert(2, vec![Value::from_i64(2), Value::Text("Bob".into())]);
    customers_delta.insert(3, vec![Value::from_i64(3), Value::Text("Charlie".into())]);

    // Create test data for vendors (id, name, price)
    let mut vendors_delta = Delta::new();
    vendors_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Widget Co".into()),
            Value::from_i64(10),
        ],
    );
    vendors_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Gadget Inc".into()),
            Value::from_i64(20),
        ],
    );
    vendors_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Tool Corp".into()),
            Value::from_i64(30),
        ],
    );

    let inputs = HashMap::from_iter([
        ("customers".to_string(), customers_delta),
        ("vendors".to_string(), vendors_delta),
    ]);

    let result = test_execute(&mut circuit, inputs, pager).unwrap();

    // Expected results:
    // For customer 1 (Alice) + vendor 1:
    //   - HEX(1 + 1) = HEX(2) = "32"
    //   - UPPER("Alice") = "ALICE"
    //   - LOWER("Widget Co") = "widget co"
    //   - 1 * 10 = 10
    assert_eq!(result.len(), 3, "Should have 3 join results");

    let mut results = result.changes;
    results.sort_by_key(|(row, _)| {
        // Sort by the product_value column for predictable ordering
        match &row.values[3] {
            Value::Numeric(Numeric::Integer(n)) => *n,
            _ => 0,
        }
    });

    // First result: Alice + Widget Co
    assert_eq!(results[0].0.values[0], Value::Text("32".into())); // HEX(2)
    assert_eq!(results[0].0.values[1], Value::Text("ALICE".into()));
    assert_eq!(results[0].0.values[2], Value::Text("widget co".into()));
    assert_eq!(results[0].0.values[3], Value::from_i64(10)); // 1 * 10

    // Second result: Bob + Gadget Inc
    assert_eq!(results[1].0.values[0], Value::Text("34".into())); // HEX(4)
    assert_eq!(results[1].0.values[1], Value::Text("BOB".into()));
    assert_eq!(results[1].0.values[2], Value::Text("gadget inc".into()));
    assert_eq!(results[1].0.values[3], Value::from_i64(40)); // 2 * 20

    // Third result: Charlie + Tool Corp
    assert_eq!(results[2].0.values[0], Value::Text("36".into())); // HEX(6)
    assert_eq!(results[2].0.values[1], Value::Text("CHARLIE".into()));
    assert_eq!(results[2].0.values[2], Value::Text("tool corp".into()));
    assert_eq!(results[2].0.values[3], Value::from_i64(90)); // 3 * 30
}

#[test]
fn test_projection_column_selection_after_join() {
    // Test selecting specific columns after a join, especially with overlapping column names
    // This ensures the projection correctly picks columns by their qualified references

    let sql = "SELECT c.id as customer_id,
                          c.name as customer_name,
                          o.order_id,
                          o.quantity,
                          p.product_name
                   FROM users c
                   JOIN orders o ON c.id = o.user_id
                   JOIN products p ON o.product_id = p.product_id
                   WHERE o.quantity > 2";

    let (mut circuit, pager) = compile_sql!(sql);

    // Create test data for users (id, name, age)
    let mut users_delta = Delta::new();
    users_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    users_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(30),
        ],
    );

    // Create test data for orders (order_id, user_id, product_id, quantity)
    let mut orders_delta = Delta::new();
    orders_delta.insert(
        1,
        vec![
            Value::from_i64(101),
            Value::from_i64(1),   // Alice
            Value::from_i64(201), // Widget
            Value::from_i64(5),   // quantity > 2
        ],
    );
    orders_delta.insert(
        2,
        vec![
            Value::from_i64(102),
            Value::from_i64(2),   // Bob
            Value::from_i64(202), // Gadget
            Value::from_i64(1),   // quantity <= 2, filtered out
        ],
    );
    orders_delta.insert(
        3,
        vec![
            Value::from_i64(103),
            Value::from_i64(1),   // Alice
            Value::from_i64(202), // Gadget
            Value::from_i64(3),   // quantity > 2
        ],
    );

    // Create test data for products (product_id, product_name, price)
    let mut products_delta = Delta::new();
    products_delta.insert(
        201,
        vec![
            Value::from_i64(201),
            Value::Text("Widget".into()),
            Value::from_i64(10),
        ],
    );
    products_delta.insert(
        202,
        vec![
            Value::from_i64(202),
            Value::Text("Gadget".into()),
            Value::from_i64(20),
        ],
    );

    let inputs = HashMap::from_iter([
        ("users".to_string(), users_delta),
        ("orders".to_string(), orders_delta),
        ("products".to_string(), products_delta),
    ]);

    let result = test_execute(&mut circuit, inputs, pager).unwrap();

    // Should have 2 results (orders with quantity > 2)
    assert_eq!(result.len(), 2, "Should have 2 results after filtering");

    let mut results = result.changes;
    results.sort_by_key(|(row, _)| {
        match &row.values[2] {
            // Sort by order_id
            Value::Numeric(Numeric::Integer(n)) => *n,
            _ => 0,
        }
    });

    // First result: Alice's order 101 for Widget
    assert_eq!(results[0].0.values[0], Value::from_i64(1)); // customer_id
    assert_eq!(results[0].0.values[1], Value::Text("Alice".into())); // customer_name
    assert_eq!(results[0].0.values[2], Value::from_i64(101)); // order_id
    assert_eq!(results[0].0.values[3], Value::from_i64(5)); // quantity
    assert_eq!(results[0].0.values[4], Value::Text("Widget".into())); // product_name

    // Second result: Alice's order 103 for Gadget
    assert_eq!(results[1].0.values[0], Value::from_i64(1)); // customer_id
    assert_eq!(results[1].0.values[1], Value::Text("Alice".into())); // customer_name
    assert_eq!(results[1].0.values[2], Value::from_i64(103)); // order_id
    assert_eq!(results[1].0.values[3], Value::from_i64(3)); // quantity
    assert_eq!(results[1].0.values[4], Value::Text("Gadget".into())); // product_name
}

#[test]
fn test_projection_column_reordering_and_duplication() {
    // Test that projection can reorder columns and select the same column multiple times
    // This is important for views that need specific column arrangements

    let sql = "SELECT o.quantity,
                          u.name,
                          u.id,
                          o.quantity * 2 as double_quantity,
                          u.id as user_id_again
                   FROM users u
                   JOIN orders o ON u.id = o.user_id
                   WHERE u.id = 1";

    let (mut circuit, pager) = compile_sql!(sql);

    // Create test data for users
    let mut users_delta = Delta::new();
    users_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );

    // Create test data for orders
    let mut orders_delta = Delta::new();
    orders_delta.insert(
        1,
        vec![
            Value::from_i64(101),
            Value::from_i64(1),   // user_id
            Value::from_i64(201), // product_id
            Value::from_i64(5),   // quantity
        ],
    );
    orders_delta.insert(
        2,
        vec![
            Value::from_i64(102),
            Value::from_i64(1),   // user_id
            Value::from_i64(202), // product_id
            Value::from_i64(3),   // quantity
        ],
    );

    let inputs = HashMap::from_iter([
        ("users".to_string(), users_delta),
        ("orders".to_string(), orders_delta),
    ]);

    let result = test_execute(&mut circuit, inputs, pager).unwrap();

    assert_eq!(result.len(), 2, "Should have 2 results for user 1");

    // Check that columns are in the right order and values are correct
    for (row, _) in &result.changes {
        // Column 0: o.quantity (5 or 3)
        assert!(matches!(
            row.values[0],
            Value::Numeric(Numeric::Integer(5)) | Value::Numeric(Numeric::Integer(3))
        ));
        // Column 1: u.name
        assert_eq!(row.values[1], Value::Text("Alice".into()));
        // Column 2: u.id
        assert_eq!(row.values[2], Value::from_i64(1));
        // Column 3: o.quantity * 2 (10 or 6)
        assert!(matches!(
            row.values[3],
            Value::Numeric(Numeric::Integer(10)) | Value::Numeric(Numeric::Integer(6))
        ));
        // Column 4: u.id again
        assert_eq!(row.values[4], Value::from_i64(1));
    }
}

#[test]
fn test_join_with_aggregate_execution() {
    let (mut circuit, pager) = compile_sql!(
        "SELECT u.name, SUM(o.quantity) as total_quantity
             FROM users u
             JOIN orders o ON u.id = o.user_id
             GROUP BY u.name"
    );

    // Create test data for users
    let mut users_delta = Delta::new();
    users_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(25),
        ],
    );
    users_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(30),
        ],
    );

    // Create test data for orders
    let mut orders_delta = Delta::new();
    orders_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::from_i64(1),
            Value::from_i64(100),
            Value::from_i64(5),
        ],
    );
    orders_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::from_i64(1),
            Value::from_i64(101),
            Value::from_i64(3),
        ],
    );
    orders_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::from_i64(2),
            Value::from_i64(102),
            Value::from_i64(7),
        ],
    );

    let mut inputs = HashMap::default();
    inputs.insert("users".to_string(), users_delta);
    inputs.insert("orders".to_string(), orders_delta);

    // Execute the join with aggregation
    let result = test_execute(&mut circuit, inputs.clone(), pager).unwrap();

    // We should get 2 aggregated results (one for Alice, one for Bob)
    assert_eq!(result.len(), 2, "Should have 2 aggregated results");

    // Verify aggregation results
    for (row, weight) in result.changes.iter() {
        assert_eq!(*weight, 1);
        // Row should have name and sum columns
        assert_eq!(row.values.len(), 2);

        // Check the aggregated values
        if let Value::Text(name) = &row.values[0] {
            if name.as_ref() == "Alice" {
                // Alice should have total quantity of 8 (5 + 3)
                assert_eq!(row.values[1], Value::from_i64(8));
            } else if name.as_ref() == "Bob" {
                // Bob should have total quantity of 7
                assert_eq!(row.values[1], Value::from_i64(7));
            }
        }
    }
}

#[test]
fn test_filter_with_qualified_columns_in_join() {
    // Test that filters correctly handle qualified column names in joins
    // when multiple tables have columns with the SAME names.
    // Both users and customers tables have 'id' and 'name' columns which can be ambiguous.

    let (mut circuit, pager) = compile_sql!(
        "SELECT users.id, users.name, customers.id, customers.name
             FROM users
             JOIN customers ON users.id = customers.id
             WHERE users.id > 1 AND customers.id < 100"
    );

    // Create test data
    let mut users_delta = Delta::new();
    let mut customers_delta = Delta::new();

    // Users data: (id, name, age)
    users_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(30),
        ],
    ); // id = 1
    users_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(25),
        ],
    ); // id = 2
    users_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Charlie".into()),
            Value::from_i64(35),
        ],
    ); // id = 3

    // Customers data: (id, name, email)
    customers_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Customer Alice".into()),
            Value::Text("alice@example.com".into()),
        ],
    ); // id = 1
    customers_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Customer Bob".into()),
            Value::Text("bob@example.com".into()),
        ],
    ); // id = 2
    customers_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Customer Charlie".into()),
            Value::Text("charlie@example.com".into()),
        ],
    ); // id = 3

    let mut inputs = HashMap::default();
    inputs.insert("users".to_string(), users_delta);
    inputs.insert("customers".to_string(), customers_delta);

    let result = test_execute(&mut circuit, inputs.clone(), pager).unwrap();

    // Should get rows where users.id > 1 AND customers.id < 100
    // - users.id=2 (> 1) AND customers.id=2 (< 100) ✓
    // - users.id=3 (> 1) AND customers.id=3 (< 100) ✓
    // Alice excluded: users.id=1 (NOT > 1)
    assert_eq!(result.len(), 2, "Should have 2 filtered results");

    let (row, weight) = &result.changes[0];
    assert_eq!(*weight, 1);
    assert_eq!(row.values.len(), 4, "Should have 4 columns");

    // Verify the filter correctly used qualified columns for Bob
    assert_eq!(row.values[0], Value::from_i64(2), "users.id should be 2");
    assert_eq!(
        row.values[1],
        Value::Text("Bob".into()),
        "users.name should be Bob"
    );
    assert_eq!(
        row.values[2],
        Value::from_i64(2),
        "customers.id should be 2"
    );
    assert_eq!(
        row.values[3],
        Value::Text("Customer Bob".into()),
        "customers.name should be Customer Bob"
    );
}

#[test]
fn test_expression_in_where_clause() {
    // Test expressions in WHERE clauses like (quantity * price) >= 400
    let (mut circuit, pager) = compile_sql!("SELECT * FROM users WHERE (age * 2) > 30");

    // Create test data
    let mut input_delta = Delta::new();
    input_delta.insert(
        1,
        vec![
            Value::from_i64(1),
            Value::Text("Alice".into()),
            Value::from_i64(20), // age * 2 = 40 > 30, should pass
        ],
    );
    input_delta.insert(
        2,
        vec![
            Value::from_i64(2),
            Value::Text("Bob".into()),
            Value::from_i64(10), // age * 2 = 20 <= 30, should be filtered out
        ],
    );
    input_delta.insert(
        3,
        vec![
            Value::from_i64(3),
            Value::Text("Charlie".into()),
            Value::from_i64(16), // age * 2 = 32 > 30, should pass
        ],
    );

    // Create input map
    let mut inputs = HashMap::default();
    inputs.insert("users".to_string(), input_delta);

    let result = test_execute(&mut circuit, inputs.clone(), pager).unwrap();

    // Should only have Alice and Charlie (age * 2 > 30)
    assert_eq!(
        result.changes.len(),
        2,
        "Should have 2 rows after filtering"
    );

    // Check Alice
    let alice = result
        .changes
        .iter()
        .find(|(row, _)| row.values[0] == Value::from_i64(1))
        .expect("Alice should be in result");
    assert_eq!(alice.0.values[1], Value::Text("Alice".into()));
    assert_eq!(alice.0.values[2], Value::from_i64(20));

    // Check Charlie
    let charlie = result
        .changes
        .iter()
        .find(|(row, _)| row.values[0] == Value::from_i64(3))
        .expect("Charlie should be in result");
    assert_eq!(charlie.0.values[1], Value::Text("Charlie".into()));
    assert_eq!(charlie.0.values[2], Value::from_i64(16));

    // Bob should not be in result
    let bob = result
        .changes
        .iter()
        .find(|(row, _)| row.values[0] == Value::from_i64(2));
    assert!(bob.is_none(), "Bob should be filtered out");
}

fn make_column_info(name: &str, ty: Type, table: &str) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        ty,
        database: None,
        table: Some(table.to_string()),
        table_alias: None,
    }
}

#[test]
fn test_resolve_join_columns_normal_order() {
    // Normal case: left.id = right.id
    let left_schema = LogicalSchema::new(vec![
        ColumnInfo {
            name: "id".to_string(),
            ty: Type::Integer,
            database: None,
            table: Some("left".to_string()),
            table_alias: None,
        },
        ColumnInfo {
            name: "name".to_string(),
            ty: Type::Text,
            database: None,
            table: Some("left".to_string()),
            table_alias: None,
        },
    ]);
    let right_schema = LogicalSchema::new(vec![
        ColumnInfo {
            name: "id".to_string(),
            ty: Type::Integer,
            database: None,
            table: Some("right".to_string()),
            table_alias: None,
        },
        ColumnInfo {
            name: "value".to_string(),
            ty: Type::Integer,
            database: None,
            table: Some("right".to_string()),
            table_alias: None,
        },
    ]);

    let left_col = Column {
        name: "id".to_string(),
        table: Some("left".to_string()),
    };
    let right_col = Column {
        name: "id".to_string(),
        table: Some("right".to_string()),
    };

    let result =
        DbspCompiler::resolve_join_columns(&left_col, &right_col, &left_schema, &right_schema);
    assert!(result.is_ok());
    let (actual_left, left_idx, actual_right, right_idx) = result.unwrap();
    assert_eq!(actual_left.name, "id");
    assert_eq!(actual_left.table, Some("left".to_string()));
    assert_eq!(left_idx, 0);
    assert_eq!(actual_right.name, "id");
    assert_eq!(actual_right.table, Some("right".to_string()));
    assert_eq!(right_idx, 0);
}

#[test]
fn test_resolve_join_columns_swapped_order() {
    // Swapped case: right.id = left.id
    let left_schema = LogicalSchema::new(vec![
        make_column_info("id", Type::Integer, "left"),
        make_column_info("name", Type::Text, "left"),
    ]);
    let right_schema = LogicalSchema::new(vec![
        make_column_info("id", Type::Integer, "right"),
        make_column_info("value", Type::Integer, "right"),
    ]);

    let right_col = Column {
        name: "id".to_string(),
        table: Some("right".to_string()),
    };
    let left_col = Column {
        name: "id".to_string(),
        table: Some("left".to_string()),
    };

    let result =
        DbspCompiler::resolve_join_columns(&right_col, &left_col, &left_schema, &right_schema);
    assert!(result.is_ok());
    let (actual_left, left_idx, actual_right, right_idx) = result.unwrap();
    assert_eq!(actual_left.name, "id");
    assert_eq!(actual_left.table, Some("left".to_string()));
    assert_eq!(left_idx, 0);
    assert_eq!(actual_right.name, "id");
    assert_eq!(actual_right.table, Some("right".to_string()));
    assert_eq!(right_idx, 0);
}

#[test]
fn test_resolve_join_columns_one_ambiguous_one_not() {
    // Both tables have 'id', but only left has 'other_id'
    let left_schema = LogicalSchema::new(vec![
        make_column_info("id", Type::Integer, "left"),
        make_column_info("other_id", Type::Integer, "left"),
    ]);
    let right_schema = LogicalSchema::new(vec![
        make_column_info("id", Type::Integer, "right"),
        make_column_info("value", Type::Integer, "right"),
    ]);

    // Unqualified 'id' with qualified 'left.other_id'
    let id_col = Column {
        name: "id".to_string(),
        table: None,
    };
    let other_id_col = Column {
        name: "other_id".to_string(),
        table: Some("left".to_string()),
    };

    // id from right, other_id from left
    let result =
        DbspCompiler::resolve_join_columns(&id_col, &other_id_col, &left_schema, &right_schema);
    assert!(result.is_ok());
    let (actual_left, left_idx, actual_right, right_idx) = result.unwrap();
    assert_eq!(actual_left.name, "other_id");
    assert_eq!(left_idx, 1);
    assert_eq!(actual_right.name, "id");
    assert_eq!(right_idx, 0);
}

#[test]
fn test_resolve_join_columns_mixed_qualified() {
    // One qualified, one unqualified, column exists on both sides
    let left_schema = LogicalSchema::new(vec![
        make_column_info("id", Type::Integer, "left"),
        make_column_info("name", Type::Text, "left"),
    ]);
    let right_schema = LogicalSchema::new(vec![
        make_column_info("id", Type::Integer, "right"),
        make_column_info("name", Type::Text, "right"),
    ]);

    // Qualified left.id with unqualified name
    let left_id = Column {
        name: "id".to_string(),
        table: Some("left".to_string()),
    };
    let name_unqualified = Column {
        name: "name".to_string(),
        table: None,
    };

    let result = DbspCompiler::resolve_join_columns(
        &left_id,
        &name_unqualified,
        &left_schema,
        &right_schema,
    );
    // left.id is explicitly from left, so unqualified 'name' must be resolved from right
    assert!(result.is_ok());
    let (actual_left, left_idx, actual_right, right_idx) = result.unwrap();
    assert_eq!(actual_left.name, "id");
    assert_eq!(left_idx, 0);
    assert_eq!(actual_right.name, "name");
    assert_eq!(right_idx, 1);
}

#[test]
fn test_resolve_join_columns_both_from_same_side() {
    // Both columns from left table - should fail
    let left_schema = LogicalSchema::new(vec![
        make_column_info("id", Type::Integer, "left"),
        make_column_info("other_id", Type::Integer, "left"),
    ]);
    let right_schema = LogicalSchema::new(vec![make_column_info("value", Type::Integer, "right")]);

    let left_id = Column {
        name: "id".to_string(),
        table: Some("left".to_string()),
    };
    let left_other_id = Column {
        name: "other_id".to_string(),
        table: Some("left".to_string()),
    };

    let result =
        DbspCompiler::resolve_join_columns(&left_id, &left_other_id, &left_schema, &right_schema);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("must come from different input tables"));
}

#[test]
fn test_resolve_join_columns_nonexistent_column() {
    // Column doesn't exist in either table
    let left_schema = LogicalSchema::new(vec![make_column_info("id", Type::Integer, "left")]);
    let right_schema = LogicalSchema::new(vec![make_column_info("value", Type::Integer, "right")]);

    let id_col = Column {
        name: "id".to_string(),
        table: None,
    };
    let nonexistent_col = Column {
        name: "does_not_exist".to_string(),
        table: None,
    };

    let result =
        DbspCompiler::resolve_join_columns(&id_col, &nonexistent_col, &left_schema, &right_schema);
    assert!(result.is_err());
}

#[test]
fn test_resolve_join_columns_both_qualified() {
    // Both columns qualified - should work normally
    let left_schema = LogicalSchema::new(vec![
        make_column_info("id", Type::Integer, "left"),
        make_column_info("name", Type::Text, "left"),
    ]);
    let right_schema = LogicalSchema::new(vec![
        make_column_info("id", Type::Integer, "right"),
        make_column_info("value", Type::Integer, "right"),
    ]);

    let left_id = Column {
        name: "id".to_string(),
        table: Some("left".to_string()),
    };
    let right_id = Column {
        name: "id".to_string(),
        table: Some("right".to_string()),
    };

    let result =
        DbspCompiler::resolve_join_columns(&left_id, &right_id, &left_schema, &right_schema);
    assert!(result.is_ok());
    let (actual_left, left_idx, actual_right, right_idx) = result.unwrap();
    assert_eq!(actual_left.name, "id");
    assert_eq!(left_idx, 0);
    assert_eq!(actual_right.name, "id");
    assert_eq!(right_idx, 0);
}

#[test]
fn test_resolve_join_columns_both_unqualified_same_name() {
    // Both columns unqualified with same name existing in both tables - should succeed
    // (first match wins based on order of checking)
    let left_schema = LogicalSchema::new(vec![make_column_info("id", Type::Integer, "left")]);
    let right_schema = LogicalSchema::new(vec![make_column_info("id", Type::Integer, "right")]);

    let id_col1 = Column {
        name: "id".to_string(),
        table: None,
    };
    let id_col2 = Column {
        name: "id".to_string(),
        table: None,
    };

    let result =
        DbspCompiler::resolve_join_columns(&id_col1, &id_col2, &left_schema, &right_schema);
    // Should succeed - unqualified 'id' matches in both schemas
    assert!(result.is_ok());
}

#[test]
fn test_resolve_join_columns_first_not_found() {
    // First column doesn't exist anywhere
    let left_schema = LogicalSchema::new(vec![make_column_info("id", Type::Integer, "left")]);
    let right_schema = LogicalSchema::new(vec![make_column_info("value", Type::Integer, "right")]);

    let missing_col = Column {
        name: "missing".to_string(),
        table: None,
    };
    let value_col = Column {
        name: "value".to_string(),
        table: None,
    };

    let result =
        DbspCompiler::resolve_join_columns(&missing_col, &value_col, &left_schema, &right_schema);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("not found in either input"));
}

#[test]
fn test_resolve_join_columns_both_unqualified_different_names() {
    // Both unqualified, each exists in only one table
    let left_schema = LogicalSchema::new(vec![make_column_info("left_id", Type::Integer, "left")]);
    let right_schema =
        LogicalSchema::new(vec![make_column_info("right_id", Type::Integer, "right")]);

    let left_col = Column {
        name: "left_id".to_string(),
        table: None,
    };
    let right_col = Column {
        name: "right_id".to_string(),
        table: None,
    };

    let result =
        DbspCompiler::resolve_join_columns(&left_col, &right_col, &left_schema, &right_schema);
    assert!(result.is_ok());
    let (actual_left, left_idx, actual_right, right_idx) = result.unwrap();
    assert_eq!(actual_left.name, "left_id");
    assert_eq!(left_idx, 0);
    assert_eq!(actual_right.name, "right_id");
    assert_eq!(right_idx, 0);
}

mod write_row_view_repoll {
    use super::super::WriteRowView;
    use crate::incremental::yield_test_support::OneShotYieldInjector;
    use crate::mvcc::yield_hooks::YieldPointMarker;
    use crate::storage::btree::{
        BTreeCursor, BTreeWriteYieldPoint, CursorTrait, BTREE_WRITE_YIELD_FAMILY,
    };
    use crate::storage::pager::CreateBTreeFlags;
    use crate::sync::Arc;
    use crate::types::{SeekKey, SeekOp, SeekResult};
    use crate::util::IOExt;
    use crate::{Connection, Database, MemoryIO, SqliteDialect, Value, IO};

    fn setup() -> (Arc<Connection>, Arc<crate::Pager>, i64) {
        let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        let pager = conn.pager.load().clone();
        let _ = pager.io.block(|| pager.allocate_page1());
        let root = pager
            .io
            .block(|| pager.btree_create(&CreateBTreeFlags::new_table()))
            .unwrap() as i64;
        (conn, pager, root)
    }

    /// Same re-poll contract as `persistence::WriteRow`, for the per-row view cursor:
    /// a mid-balance yield must not lose the matview row.
    #[test]
    fn write_row_view_completes_yielded_overflowing_insert() {
        let (conn, pager, root) = setup();

        let injector = OneShotYieldInjector::new(
            BTreeWriteYieldPoint::AfterInsertOverflowCellBeforeBalance.point(),
            BTREE_WRITE_YIELD_FAMILY ^ root as u64,
        );
        conn.set_yield_injector(Some(injector.clone()));

        // ~1200-byte on-page cells fill leaves; the insert that overflows a page
        // triggers the mid-balance yield. Fresh per-row cursor, as in UpdateView.
        let mut victim_rowid = None;
        for rowid in 1i64..=200 {
            let mut cursor = BTreeCursor::new_table(pager.clone(), root, 2);
            cursor.install_yield_context(&conn);

            let key = SeekKey::TableRowId(rowid);
            let build = move |final_weight: isize| -> Vec<Value> {
                vec![
                    Value::from_slice(&[0xcd_u8; 1200]).unwrap(),
                    Value::from_i64(final_weight as i64),
                ]
            };

            let mut wr = WriteRowView::new();
            pager
                .io
                .block(|| wr.write_row(&mut cursor, key.clone(), build, 1))
                .unwrap();

            if injector.fired() {
                victim_rowid = Some(rowid);
                break;
            }
        }
        let victim_rowid =
            victim_rowid.expect("no insert ever overflowed a page; test does not exercise the bug");
        conn.set_yield_injector(None);

        let mut verify = BTreeCursor::new_table(pager.clone(), root, 2);
        let found = pager
            .io
            .block(|| {
                verify.seek(
                    SeekKey::TableRowId(victim_rowid),
                    SeekOp::GE { eq_only: true },
                )
            })
            .unwrap();
        assert!(
            matches!(found, SeekResult::Found),
            "matview row {victim_rowid} lost: WriteRowView advanced to Done past a yielded insert"
        );
    }
}
