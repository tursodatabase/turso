use super::*;
use crate::alloc::vec;
use crate::schema::{
    BTreeCharacteristics, BTreeTable, ColDef, Column as SchemaColumn, Schema, Type,
};
use crate::sync::Arc;
use turso_parser::ast;
use turso_parser::parser::Parser;

// Helper function to create a test schema with multiple tables
fn create_test_schema() -> Schema {
    let mut schema = Schema::new();

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
                primary_key: true,
                rowid_alias: true,
                notnull: true,
                explicit_notnull: false,
                unique: false,
                hidden: false,
                notnull_conflict_clause: None,
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
                primary_key: true,
                rowid_alias: true,
                notnull: true,
                explicit_notnull: false,
                unique: false,
                hidden: false,
                notnull_conflict_clause: None,
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
                primary_key: true,
                rowid_alias: true,
                notnull: true,
                explicit_notnull: false,
                unique: false,
                hidden: false,
                notnull_conflict_clause: None,
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
        .add_btree_table(Arc::new(customers_table))
        .expect("Test setup: failed to add customers table");

    schema
        .add_btree_table(Arc::new(orders_table))
        .expect("Test setup: failed to add orders table");

    schema
        .add_btree_table(Arc::new(products_table))
        .expect("Test setup: failed to add products table");

    schema
        .add_btree_table(Arc::new(logs_table))
        .expect("Test setup: failed to add logs table");

    schema
}

// Helper to parse SQL and extract the SELECT statement
fn parse_select(sql: &str) -> ast::Select {
    let mut parser = Parser::new(sql.as_bytes());
    let cmd = parser.next().unwrap().unwrap();
    match cmd {
        ast::Cmd::Stmt(ast::Stmt::Select(select)) => select,
        _ => panic!("Expected SELECT statement"),
    }
}

// Type alias for the complex return type of extract_all_tables
type ExtractedTableInfo = (
    Vec<Arc<BTreeTable>>,
    HashMap<String, String>,
    HashMap<String, String>,
    HashMap<String, Vec<Option<ast::Expr>>>,
);

fn extract_all_tables(select: &ast::Select, schema: &Schema) -> Result<ExtractedTableInfo> {
    let mut referenced_tables = Vec::new();
    let mut table_aliases = HashMap::default();
    let mut qualified_table_names = HashMap::default();
    let mut table_conditions = HashMap::default();
    IncrementalView::extract_all_tables(
        select,
        schema,
        &mut referenced_tables,
        &mut table_aliases,
        &mut qualified_table_names,
        &mut table_conditions,
    )?;
    Ok((
        referenced_tables,
        table_aliases,
        qualified_table_names,
        table_conditions,
    ))
}

#[test]
fn test_extract_single_table() {
    let schema = create_test_schema();
    let select = parse_select("SELECT * FROM customers");

    let (tables, _, _, _table_conditions) = extract_all_tables(&select, &schema).unwrap();

    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].name, "customers");
}

#[test]
fn test_tables_from_union() {
    let schema = create_test_schema();
    let select = parse_select("SELECT name FROM customers union SELECT name from products");

    let (tables, _, _, table_conditions) = extract_all_tables(&select, &schema).unwrap();

    assert_eq!(tables.len(), 2);
    assert!(table_conditions.contains_key("customers"));
    assert!(table_conditions.contains_key("products"));
}

#[test]
fn test_extract_tables_from_inner_join() {
    let schema = create_test_schema();
    let select = parse_select(
        "SELECT * FROM customers INNER JOIN orders ON customers.id = orders.customer_id",
    );

    let (tables, _, _, table_conditions) = extract_all_tables(&select, &schema).unwrap();

    assert_eq!(tables.len(), 2);
    assert!(table_conditions.contains_key("customers"));
    assert!(table_conditions.contains_key("orders"));
}

#[test]
fn test_extract_tables_from_multiple_joins() {
    let schema = create_test_schema();
    let select = parse_select(
        "SELECT * FROM customers
             INNER JOIN orders ON customers.id = orders.customer_id
             INNER JOIN products ON orders.id = products.id",
    );

    let (tables, _, _, table_conditions) = extract_all_tables(&select, &schema).unwrap();

    assert_eq!(tables.len(), 3);
    assert!(table_conditions.contains_key("customers"));
    assert!(table_conditions.contains_key("orders"));
    assert!(table_conditions.contains_key("products"));
}

#[test]
fn test_extract_tables_from_left_join() {
    let schema = create_test_schema();
    let select = parse_select(
        "SELECT * FROM customers LEFT JOIN orders ON customers.id = orders.customer_id",
    );

    let (tables, _, _, table_conditions) = extract_all_tables(&select, &schema).unwrap();

    assert_eq!(tables.len(), 2);
    assert!(table_conditions.contains_key("customers"));
    assert!(table_conditions.contains_key("orders"));
}

#[test]
fn test_extract_tables_from_cross_join() {
    let schema = create_test_schema();
    let select = parse_select("SELECT * FROM customers CROSS JOIN orders");

    let (tables, _, _, table_conditions) = extract_all_tables(&select, &schema).unwrap();

    assert_eq!(tables.len(), 2);
    assert!(table_conditions.contains_key("customers"));
    assert!(table_conditions.contains_key("orders"));
}

#[test]
fn test_extract_tables_with_aliases() {
    let schema = create_test_schema();
    let select =
        parse_select("SELECT * FROM customers c INNER JOIN orders o ON c.id = o.customer_id");

    let (tables, aliases, _, _table_conditions) = extract_all_tables(&select, &schema).unwrap();

    // Should still extract the actual table names, not aliases
    assert_eq!(tables.len(), 2);
    let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    assert!(table_names.contains(&"customers"));
    assert!(table_names.contains(&"orders"));

    // Check that aliases are correctly mapped
    assert_eq!(aliases.get("c"), Some(&"customers".to_string()));
    assert_eq!(aliases.get("o"), Some(&"orders".to_string()));
}

#[test]
fn test_extract_tables_nonexistent_table_error() {
    let schema = create_test_schema();
    let select = parse_select("SELECT * FROM nonexistent");

    let result = extract_all_tables(&select, &schema).map(|(tables, _, _, _)| tables);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Table 'nonexistent' not found"));
}

#[test]
fn test_extract_tables_nonexistent_join_table_error() {
    let schema = create_test_schema();
    let select = parse_select(
        "SELECT * FROM customers INNER JOIN nonexistent ON customers.id = nonexistent.id",
    );

    let result = extract_all_tables(&select, &schema).map(|(tables, _, _, _)| tables);

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Table 'nonexistent' not found"));
}

#[test]
fn test_sql_for_populate_simple_query_no_where() {
    // Test simple query with no WHERE clause
    let schema = create_test_schema();
    let select = parse_select("SELECT * FROM customers");

    let (tables, aliases, qualified_names, table_conditions) =
        extract_all_tables(&select, &schema).unwrap();
    let view = IncrementalView::new(
        "test_view".to_string(),
        select.clone(),
        tables,
        aliases,
        qualified_names,
        table_conditions,
        extract_view_columns(&select, &schema).unwrap(),
        &schema,
        1, // main_data_root
        2, // internal_state_root
        3, // internal_state_index_root
    )
    .unwrap();

    let queries = view.sql_for_populate().unwrap();

    assert_eq!(queries.len(), 1);
    // customers has id as rowid alias, so no need for explicit rowid
    assert_eq!(queries[0], "SELECT * FROM customers");
}

#[test]
fn test_sql_for_populate_simple_query_with_where() {
    // Test simple query with WHERE clause
    let schema = create_test_schema();
    let select = parse_select("SELECT * FROM customers WHERE id > 10");

    let (tables, aliases, qualified_names, table_conditions) =
        extract_all_tables(&select, &schema).unwrap();
    let view = IncrementalView::new(
        "test_view".to_string(),
        select.clone(),
        tables,
        aliases,
        qualified_names,
        table_conditions,
        extract_view_columns(&select, &schema).unwrap(),
        &schema,
        1, // main_data_root
        2, // internal_state_root
        3, // internal_state_index_root
    )
    .unwrap();

    let queries = view.sql_for_populate().unwrap();

    assert_eq!(queries.len(), 1);
    // For single-table queries, we should get the full WHERE clause
    assert_eq!(queries[0], "SELECT * FROM customers WHERE id > 10");
}

#[test]
fn test_sql_for_populate_join_with_where_on_both_tables() {
    // Test JOIN query with WHERE conditions on both tables
    let schema = create_test_schema();
    let select = parse_select(
        "SELECT * FROM customers c \
             JOIN orders o ON c.id = o.customer_id \
             WHERE c.id > 10 AND o.total > 100",
    );

    let (tables, aliases, qualified_names, table_conditions) =
        extract_all_tables(&select, &schema).unwrap();
    let view = IncrementalView::new(
        "test_view".to_string(),
        select.clone(),
        tables,
        aliases,
        qualified_names,
        table_conditions,
        extract_view_columns(&select, &schema).unwrap(),
        &schema,
        1, // main_data_root
        2, // internal_state_root
        3, // internal_state_index_root
    )
    .unwrap();

    let queries = view.sql_for_populate().unwrap();

    assert_eq!(queries.len(), 2);

    // With per-table WHERE extraction:
    // - customers table gets: c.id > 10
    // - orders table gets: o.total > 100
    assert!(queries
        .iter()
        .any(|q| q == "SELECT * FROM customers WHERE id > 10"));
    assert!(queries
        .iter()
        .any(|q| q == "SELECT * FROM orders WHERE total > 100"));
}

#[test]
fn test_sql_for_populate_complex_join_with_mixed_conditions() {
    // Test complex JOIN with WHERE conditions mixing both tables
    let schema = create_test_schema();
    let select = parse_select(
        "SELECT * FROM customers c \
             JOIN orders o ON c.id = o.customer_id \
             WHERE c.id > 10 AND o.total > 100 AND c.name = 'John' \
             AND o.customer_id = 5 AND (c.id = 15 OR o.total = 200)",
    );

    let (tables, aliases, qualified_names, table_conditions) =
        extract_all_tables(&select, &schema).unwrap();
    let view = IncrementalView::new(
        "test_view".to_string(),
        select.clone(),
        tables,
        aliases,
        qualified_names,
        table_conditions,
        extract_view_columns(&select, &schema).unwrap(),
        &schema,
        1, // main_data_root
        2, // internal_state_root
        3, // internal_state_index_root
    )
    .unwrap();

    let queries = view.sql_for_populate().unwrap();

    assert_eq!(queries.len(), 2);

    // With per-table WHERE extraction:
    // - customers gets: c.id > 10 AND c.name = 'John'
    // - orders gets: o.total > 100 AND o.customer_id = 5
    // Note: The OR condition (c.id = 15 OR o.total = 200) involves both tables,
    // so it cannot be extracted to either table individually
    // Check both queries exist (order doesn't matter)
    assert!(
        queries.contains(&"SELECT * FROM customers WHERE id > 10 AND name = 'John'".to_string())
    );
    assert!(
        queries.contains(&"SELECT * FROM orders WHERE total > 100 AND customer_id = 5".to_string())
    );
}

#[test]
fn test_sql_for_populate_table_without_rowid_alias() {
    let schema = create_test_schema();
    let select = parse_select("SELECT * FROM logs WHERE level > 2");

    let (tables, aliases, qualified_names, table_conditions) =
        extract_all_tables(&select, &schema).unwrap();
    let view = IncrementalView::new(
        "test_view".to_string(),
        select.clone(),
        tables,
        aliases,
        qualified_names,
        table_conditions,
        extract_view_columns(&select, &schema).unwrap(),
        &schema,
        1, // main_data_root
        2, // internal_state_root
        3, // internal_state_index_root
    )
    .unwrap();

    let queries = view.sql_for_populate().unwrap();

    assert_eq!(queries.len(), 1);
    // logs table has no rowid alias, so we need to explicitly select rowid
    assert_eq!(queries[0], "SELECT *, rowid FROM logs WHERE level > 2");
}

#[test]
fn test_sql_for_populate_join_with_and_without_rowid_alias() {
    // Test JOIN between a table with rowid alias and one without
    let schema = create_test_schema();
    let select = parse_select(
        "SELECT * FROM customers c \
             JOIN logs l ON c.id = l.level \
             WHERE c.id > 10 AND l.level > 2",
    );

    let (tables, aliases, qualified_names, table_conditions) =
        extract_all_tables(&select, &schema).unwrap();
    let view = IncrementalView::new(
        "test_view".to_string(),
        select.clone(),
        tables,
        aliases,
        qualified_names,
        table_conditions,
        extract_view_columns(&select, &schema).unwrap(),
        &schema,
        1, // main_data_root
        2, // internal_state_root
        3, // internal_state_index_root
    )
    .unwrap();

    let queries = view.sql_for_populate().unwrap();

    assert_eq!(queries.len(), 2);
    // customers has rowid alias (id), logs doesn't
    assert!(queries.contains(&"SELECT * FROM customers WHERE id > 10".to_string()));
    assert!(queries.contains(&"SELECT *, rowid FROM logs WHERE level > 2".to_string()));
}

#[test]
fn test_sql_for_populate_with_database_qualified_names() {
    // Test that database.table.column references are handled correctly
    // The table name in FROM should keep the database prefix,
    // but column names in WHERE should be unqualified
    let schema = create_test_schema();

    // Test with single table using database qualification
    let select = parse_select("SELECT * FROM main.customers WHERE main.customers.id > 10");

    let (tables, aliases, qualified_names, table_conditions) =
        extract_all_tables(&select, &schema).unwrap();
    let view = IncrementalView::new(
        "test_view".to_string(),
        select.clone(),
        tables,
        aliases,
        qualified_names,
        table_conditions,
        extract_view_columns(&select, &schema).unwrap(),
        &schema,
        1, // main_data_root
        2, // internal_state_root
        3, // internal_state_index_root
    )
    .unwrap();

    let queries = view.sql_for_populate().unwrap();

    assert_eq!(queries.len(), 1);
    // The FROM clause should preserve the database qualification,
    // but the WHERE clause should have unqualified column names
    assert_eq!(queries[0], "SELECT * FROM main.customers WHERE id > 10");
}

#[test]
fn test_sql_for_populate_join_with_database_qualified_names() {
    // Test JOIN with database-qualified table and column references
    let schema = create_test_schema();

    let select = parse_select(
        "SELECT * FROM main.customers c \
             JOIN main.orders o ON c.id = o.customer_id \
             WHERE main.customers.id > 10 AND main.orders.total > 100",
    );

    let (tables, aliases, qualified_names, table_conditions) =
        extract_all_tables(&select, &schema).unwrap();
    let view = IncrementalView::new(
        "test_view".to_string(),
        select.clone(),
        tables,
        aliases,
        qualified_names,
        table_conditions,
        extract_view_columns(&select, &schema).unwrap(),
        &schema,
        1, // main_data_root
        2, // internal_state_root
        3, // internal_state_index_root
    )
    .unwrap();

    let queries = view.sql_for_populate().unwrap();

    assert_eq!(queries.len(), 2);
    // The FROM clauses should preserve database qualification,
    // but WHERE clauses should have unqualified column names
    assert!(queries.contains(&"SELECT * FROM main.customers WHERE id > 10".to_string()));
    assert!(queries.contains(&"SELECT * FROM main.orders WHERE total > 100".to_string()));
}

#[test]
fn test_where_extraction_for_three_tables_with_aliases() {
    // Test that WHERE clause extraction correctly separates conditions for 3+ tables
    // This addresses the concern about conditions "piling up" as joins increase
    let schema = create_test_schema();
    let select = parse_select(
        "SELECT * FROM customers c
             JOIN orders o ON c.id = o.customer_id
             JOIN products p ON p.id = o.product_id
             WHERE c.id > 10 AND o.total > 100 AND p.price > 50",
    );

    let (tables, aliases, qualified_names, table_conditions) =
        extract_all_tables(&select, &schema).unwrap();

    // Verify we extracted all three tables
    assert_eq!(tables.len(), 3);
    let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    assert!(table_names.contains(&"customers"));
    assert!(table_names.contains(&"orders"));
    assert!(table_names.contains(&"products"));

    // Verify aliases are correctly mapped
    assert_eq!(aliases.get("c"), Some(&"customers".to_string()));
    assert_eq!(aliases.get("o"), Some(&"orders".to_string()));
    assert_eq!(aliases.get("p"), Some(&"products".to_string()));

    // Generate populate queries to verify each table gets its own conditions
    let queries = IncrementalView::generate_populate_queries(
        &select,
        &tables,
        &aliases,
        &qualified_names,
        &table_conditions,
    )
    .unwrap();

    assert_eq!(queries.len(), 3);

    // Verify the exact queries generated for each table
    // The order might vary, so check all possibilities
    let expected_queries = vec![
        "SELECT * FROM customers WHERE id > 10",
        "SELECT * FROM orders WHERE total > 100",
        "SELECT * FROM products WHERE price > 50",
    ];

    for expected in &expected_queries {
        assert!(
            queries.contains(&expected.to_string()),
            "Missing expected query: {expected}. Got: {queries:?}"
        );
    }
}

#[test]
fn test_sql_for_populate_complex_expressions_not_included() {
    // Test that complex expressions (subqueries, CASE, string concat) are NOT included in populate queries
    let schema = create_test_schema();
    let select = parse_select(
        "SELECT * FROM customers
             WHERE id > (SELECT MAX(customer_id) FROM orders)
               AND name || ' Customer' = 'John Customer'
               AND CASE WHEN id > 10 THEN 1 ELSE 0 END = 1
               AND EXISTS (SELECT 1 FROM orders WHERE customer_id = customers.id)",
    );

    let (tables, aliases, qualified_names, table_conditions) =
        extract_all_tables(&select, &schema).unwrap();

    let queries = IncrementalView::generate_populate_queries(
        &select,
        &tables,
        &aliases,
        &qualified_names,
        &table_conditions,
    )
    .unwrap();

    assert_eq!(queries.len(), 1);
    // Since customers table has an INTEGER PRIMARY KEY (id), we should get SELECT *
    // without rowid and without WHERE clause (all conditions are complex)
    assert_eq!(queries[0], "SELECT * FROM customers");
}

#[test]
fn test_sql_for_populate_unambiguous_unqualified_column() {
    // Test that unambiguous unqualified columns ARE extracted
    let schema = create_test_schema();
    let select = parse_select(
        "SELECT * FROM customers c \
             JOIN orders o ON c.id = o.customer_id \
             WHERE total > 100", // 'total' only exists in orders table
    );

    let (tables, aliases, qualified_names, table_conditions) =
        extract_all_tables(&select, &schema).unwrap();
    let view = IncrementalView::new(
        "test_view".to_string(),
        select.clone(),
        tables,
        aliases,
        qualified_names,
        table_conditions,
        extract_view_columns(&select, &schema).unwrap(),
        &schema,
        1, // main_data_root
        2, // internal_state_root
        3, // internal_state_index_root
    )
    .unwrap();

    let queries = view.sql_for_populate().unwrap();

    assert_eq!(queries.len(), 2);

    // 'total' is unambiguous (only in orders), so it should be extracted
    assert!(queries.contains(&"SELECT * FROM customers".to_string()));
    assert!(queries.contains(&"SELECT * FROM orders WHERE total > 100".to_string()));
}

#[test]
fn test_database_qualified_table_names() {
    let schema = create_test_schema();

    // Test with database-qualified table names
    let select = parse_select(
        "SELECT c.id, c.name, o.id, o.total
             FROM main.customers c
             JOIN main.orders o ON c.id = o.customer_id
             WHERE c.id > 10",
    );

    let (tables, aliases, qualified_names, table_conditions) =
        extract_all_tables(&select, &schema).unwrap();

    // Check that qualified names are preserved
    assert!(qualified_names.contains_key("customers"));
    assert_eq!(qualified_names.get("customers").unwrap(), "main.customers");
    assert!(qualified_names.contains_key("orders"));
    assert_eq!(qualified_names.get("orders").unwrap(), "main.orders");

    let view = IncrementalView::new(
        "test_view".to_string(),
        select.clone(),
        tables,
        aliases,
        qualified_names,
        table_conditions,
        extract_view_columns(&select, &schema).unwrap(),
        &schema,
        1, // main_data_root
        2, // internal_state_root
        3, // internal_state_index_root
    )
    .unwrap();

    let queries = view.sql_for_populate().unwrap();

    assert_eq!(queries.len(), 2);

    // The FROM clause should contain the database-qualified name
    // But the WHERE clause should use unqualified column names
    assert!(queries.contains(&"SELECT * FROM main.customers WHERE id > 10".to_string()));
    assert!(queries.contains(&"SELECT * FROM main.orders".to_string()));
}

#[test]
fn test_mixed_qualified_unqualified_tables() {
    let schema = create_test_schema();

    // Test with a mix of qualified and unqualified table names
    let select = parse_select(
        "SELECT c.id, c.name, o.id, o.total
             FROM main.customers c
             JOIN orders o ON c.id = o.customer_id
             WHERE c.id > 10 AND o.total < 1000",
    );

    let (tables, aliases, qualified_names, table_conditions) =
        extract_all_tables(&select, &schema).unwrap();

    // Check that qualified names are preserved where specified
    assert_eq!(qualified_names.get("customers").unwrap(), "main.customers");
    // Unqualified tables should not have an entry (or have the bare name)
    assert!(
        !qualified_names.contains_key("orders")
            || qualified_names.get("orders").unwrap() == "orders"
    );

    let view = IncrementalView::new(
        "test_view".to_string(),
        select.clone(),
        tables,
        aliases,
        qualified_names,
        table_conditions,
        extract_view_columns(&select, &schema).unwrap(),
        &schema,
        1, // main_data_root
        2, // internal_state_root
        3, // internal_state_index_root
    )
    .unwrap();

    let queries = view.sql_for_populate().unwrap();

    assert_eq!(queries.len(), 2);

    // The FROM clause should preserve qualification where specified
    assert!(queries.contains(&"SELECT * FROM main.customers WHERE id > 10".to_string()));
    assert!(queries.contains(&"SELECT * FROM orders WHERE total < 1000".to_string()));
}

#[test]
fn test_extract_tables_with_simple_cte() {
    let schema = create_test_schema();
    let select = parse_select(
        "WITH customer_totals AS (
                SELECT c.id, c.name, SUM(o.total) as total_spent
                FROM customers c
                JOIN orders o ON c.id = o.customer_id
                GROUP BY c.id, c.name
            )
            SELECT * FROM customer_totals WHERE total_spent > 1000",
    );

    let (tables, aliases, _qualified_names, _table_conditions) =
        extract_all_tables(&select, &schema).unwrap();

    // Check that we found both tables from the CTE
    assert_eq!(tables.len(), 2);
    let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    assert!(table_names.contains(&"customers"));
    assert!(table_names.contains(&"orders"));

    // Check aliases from the CTE
    assert_eq!(aliases.get("c"), Some(&"customers".to_string()));
    assert_eq!(aliases.get("o"), Some(&"orders".to_string()));
}

#[test]
fn test_extract_tables_with_multiple_ctes() {
    let schema = create_test_schema();
    let select = parse_select(
        "WITH
            high_value_customers AS (
                SELECT id, name
                FROM customers
                WHERE id IN (SELECT customer_id FROM orders WHERE total > 500)
            ),
            recent_orders AS (
                SELECT id, customer_id, total
                FROM orders
                WHERE id > 100
            )
            SELECT hvc.name, ro.total
            FROM high_value_customers hvc
            JOIN recent_orders ro ON hvc.id = ro.customer_id",
    );

    let (tables, _aliases, _qualified_names, _table_conditions) =
        extract_all_tables(&select, &schema).unwrap();

    // Check that we found both tables from both CTEs
    assert_eq!(tables.len(), 2);
    let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    assert!(table_names.contains(&"customers"));
    assert!(table_names.contains(&"orders"));
}

#[test]
fn test_sql_for_populate_union_mixed_conditions() {
    // Test UNION where same table appears with and without WHERE clause
    // This should drop ALL conditions to ensure we get all rows
    let schema = create_test_schema();

    let select = parse_select(
        "SELECT * FROM customers WHERE id > 10
             UNION ALL
             SELECT * FROM customers",
    );

    let (tables, aliases, qualified_names, table_conditions) =
        extract_all_tables(&select, &schema).unwrap();

    let view = IncrementalView::new(
        "union_view".to_string(),
        select.clone(),
        tables,
        aliases,
        qualified_names,
        table_conditions,
        extract_view_columns(&select, &schema).unwrap(),
        &schema,
        1, // main_data_root
        2, // internal_state_root
        3, // internal_state_index_root
    )
    .unwrap();

    let queries = view.sql_for_populate().unwrap();

    assert_eq!(queries.len(), 1);
    // When the same table appears with and without WHERE conditions in a UNION,
    // we must fetch ALL rows (no WHERE clause) because the conditions are incompatible
    assert_eq!(
        queries[0], "SELECT * FROM customers",
        "UNION with mixed conditions (some with WHERE, some without) should fetch ALL rows"
    );
}

#[test]
fn test_extract_tables_with_nested_cte() {
    let schema = create_test_schema();
    let select = parse_select(
        "WITH RECURSIVE customer_hierarchy AS (
                SELECT id, name, 0 as level
                FROM customers
                WHERE id = 1
                UNION ALL
                SELECT c.id, c.name, ch.level + 1
                FROM customers c
                JOIN orders o ON c.id = o.customer_id
                JOIN customer_hierarchy ch ON o.customer_id = ch.id
                WHERE ch.level < 3
            )
            SELECT * FROM customer_hierarchy",
    );

    let (tables, _aliases, _qualified_names, _table_conditions) =
        extract_all_tables(&select, &schema).unwrap();

    // Check that we found the tables referenced in the recursive CTE
    let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();

    // We're finding duplicates because "customers" appears twice in the recursive CTE
    // Let's deduplicate
    let unique_tables: HashSet<&str> = table_names.iter().cloned().collect();
    assert_eq!(unique_tables.len(), 2);
    assert!(unique_tables.contains("customers"));
    assert!(unique_tables.contains("orders"));
}

#[test]
fn test_extract_tables_with_cte_and_main_query() {
    let schema = create_test_schema();
    let select = parse_select(
        "WITH customer_stats AS (
                SELECT customer_id, COUNT(*) as order_count
                FROM orders
                GROUP BY customer_id
            )
            SELECT c.name, cs.order_count, p.name as product_name
            FROM customers c
            JOIN customer_stats cs ON c.id = cs.customer_id
            JOIN products p ON p.id = 1",
    );

    let (tables, aliases, _qualified_names, _table_conditions) =
        extract_all_tables(&select, &schema).unwrap();

    // Check that we found tables from both the CTE and the main query
    assert_eq!(tables.len(), 3);
    let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    assert!(table_names.contains(&"customers"));
    assert!(table_names.contains(&"orders"));
    assert!(table_names.contains(&"products"));

    // Check aliases from main query
    assert_eq!(aliases.get("c"), Some(&"customers".to_string()));
    assert_eq!(aliases.get("p"), Some(&"products".to_string()));
}

#[test]
fn test_sql_for_populate_simple_union() {
    let schema = create_test_schema();
    let select = parse_select(
        "SELECT * FROM orders WHERE total > 1000
             UNION ALL
             SELECT * FROM orders WHERE total < 100",
    );

    let (tables, aliases, qualified_names, table_conditions) =
        extract_all_tables(&select, &schema).unwrap();

    // Generate populate queries
    let queries = IncrementalView::generate_populate_queries(
        &select,
        &tables,
        &aliases,
        &qualified_names,
        &table_conditions,
    )
    .unwrap();

    // We should have deduplicated to a single table
    assert_eq!(tables.len(), 1, "Should have one unique table");
    assert_eq!(tables[0].name, "orders"); // Single table, order doesn't matter

    // Should have collected two conditions
    assert_eq!(table_conditions.get("orders").unwrap().len(), 2);

    // Should combine multiple conditions with OR
    assert_eq!(queries.len(), 1);
    // Conditions are combined with OR
    assert_eq!(
        queries[0],
        "SELECT * FROM orders WHERE (total > 1000) OR (total < 100)"
    );
}

#[test]
fn test_sql_for_populate_with_union_and_filters() {
    let schema = create_test_schema();

    // Test UNION with different WHERE conditions on the same table
    let select = parse_select(
        "SELECT * FROM orders WHERE total > 1000
             UNION ALL
             SELECT * FROM orders WHERE total < 100",
    );

    let view = IncrementalView::from_stmt(
        ast::QualifiedName {
            db_name: None,
            name: ast::Name::exact("test_view".to_string()),
            alias: None,
        },
        select,
        &schema,
        1,
        2,
        3,
    )
    .unwrap();

    let queries = view.sql_for_populate().unwrap();

    // We deduplicate tables, so we get 1 query for orders
    assert_eq!(queries.len(), 1);

    // Multiple conditions on the same table are combined with OR
    assert_eq!(
        queries[0],
        "SELECT * FROM orders WHERE (total > 1000) OR (total < 100)"
    );
}

#[test]
fn test_sql_for_populate_with_union_mixed_tables() {
    let schema = create_test_schema();

    // Test UNION with different tables
    let select = parse_select(
        "SELECT id, name FROM customers WHERE id > 10
             UNION ALL
             SELECT customer_id as id, 'Order' as name FROM orders WHERE total > 500",
    );

    let view = IncrementalView::from_stmt(
        ast::QualifiedName {
            db_name: None,
            name: ast::Name::exact("test_view".to_string()),
            alias: None,
        },
        select,
        &schema,
        1,
        2,
        3,
    )
    .unwrap();

    let queries = view.sql_for_populate().unwrap();

    assert_eq!(queries.len(), 2, "Should have one query per table");

    // Check that each table gets its appropriate WHERE clause
    let customers_query = queries
        .iter()
        .find(|q| q.contains("FROM customers"))
        .unwrap();
    let orders_query = queries.iter().find(|q| q.contains("FROM orders")).unwrap();

    assert!(customers_query.contains("WHERE id > 10"));
    assert!(orders_query.contains("WHERE total > 500"));
}

#[test]
fn test_sql_for_populate_duplicate_tables_conflicting_filters() {
    // This tests what happens when we have duplicate table references with different filters
    // We need to manually construct a view to simulate what would happen with CTEs
    let schema = create_test_schema();

    // Get the orders table twice (simulating what would happen with CTEs)
    let orders_table = schema.get_btree_table("orders").unwrap();

    let referenced_tables = std::vec![orders_table.clone(), orders_table];

    // Create a SELECT that would have conflicting WHERE conditions
    let select = parse_select(
        "SELECT * FROM orders WHERE total > 1000", // This is just for the AST
    );

    let view = IncrementalView::new(
        "test_view".to_string(),
        select.clone(),
        referenced_tables,
        HashMap::default(),
        HashMap::default(),
        HashMap::default(),
        extract_view_columns(&select, &schema).unwrap(),
        &schema,
        1,
        2,
        3,
    )
    .unwrap();

    let queries = view.sql_for_populate().unwrap();

    // With duplicates, we should get 2 identical queries
    assert_eq!(queries.len(), 2);

    // Both should be the same since they're from the same table reference
    assert_eq!(queries[0], queries[1]);
}

#[test]
fn test_table_extraction_with_nested_ctes_complex_conditions() {
    let schema = create_test_schema();
    let select = parse_select(
        "WITH
            customer_orders AS (
                SELECT c.*, o.total
                FROM customers c
                JOIN orders o ON c.id = o.customer_id
                WHERE c.name LIKE 'A%' AND o.total > 100
            ),
            top_customers AS (
                SELECT * FROM customer_orders WHERE total > 500
            )
            SELECT * FROM top_customers",
    );

    // Test table extraction directly without creating a view
    let mut tables = Vec::new();
    let mut aliases = HashMap::default();
    let mut qualified_names = HashMap::default();
    let mut table_conditions = HashMap::default();

    IncrementalView::extract_all_tables(
        &select,
        &schema,
        &mut tables,
        &mut aliases,
        &mut qualified_names,
        &mut table_conditions,
    )
    .unwrap();

    let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();

    // Should have one reference to each table
    assert_eq!(table_names.len(), 2, "Should have 2 table references");
    assert!(table_names.contains(&"customers"));
    assert!(table_names.contains(&"orders"));

    // Check aliases
    assert_eq!(aliases.get("c"), Some(&"customers".to_string()));
    assert_eq!(aliases.get("o"), Some(&"orders".to_string()));
}

#[test]
fn test_union_all_populate_queries() {
    // Test that UNION ALL generates correct populate queries
    let schema = create_test_schema();

    // Create a UNION ALL query that references the same table twice with different WHERE conditions
    let sql = "
            SELECT id, name FROM customers WHERE id < 5
            UNION ALL
            SELECT id, name FROM customers WHERE id > 10
        ";

    let mut parser = Parser::new(sql.as_bytes());
    let cmd = parser.next_cmd().unwrap();
    let select_stmt = match cmd.unwrap() {
        turso_parser::ast::Cmd::Stmt(ast::Stmt::Select(select)) => select,
        _ => panic!("Expected SELECT statement"),
    };

    // Extract tables and conditions
    let (tables, aliases, qualified_names, conditions) =
        extract_all_tables(&select_stmt, &schema).unwrap();

    // Generate populate queries
    let queries = IncrementalView::generate_populate_queries(
        &select_stmt,
        &tables,
        &aliases,
        &qualified_names,
        &conditions,
    )
    .unwrap();

    // Expected query - assuming customers table has INTEGER PRIMARY KEY
    // so we don't need to select rowid separately
    let expected = "SELECT * FROM customers WHERE (id < 5) OR (id > 10)";

    assert_eq!(
        queries.len(),
        1,
        "Should generate exactly 1 query for UNION ALL with same table"
    );
    assert_eq!(queries[0], expected, "Query should match expected format");
}

#[test]
fn test_union_all_different_tables_populate_queries() {
    // Test UNION ALL with different tables
    let schema = create_test_schema();

    let sql = "
            SELECT id, name FROM customers WHERE id < 5
            UNION ALL
            SELECT id, product_name FROM orders WHERE amount > 100
        ";

    let mut parser = Parser::new(sql.as_bytes());
    let cmd = parser.next_cmd().unwrap();
    let select_stmt = match cmd.unwrap() {
        turso_parser::ast::Cmd::Stmt(ast::Stmt::Select(select)) => select,
        _ => panic!("Expected SELECT statement"),
    };

    // Extract tables and conditions
    let (tables, aliases, qualified_names, conditions) =
        extract_all_tables(&select_stmt, &schema).unwrap();

    // Generate populate queries
    let queries = IncrementalView::generate_populate_queries(
        &select_stmt,
        &tables,
        &aliases,
        &qualified_names,
        &conditions,
    )
    .unwrap();

    // Should generate separate queries for each table
    assert_eq!(
        queries.len(),
        2,
        "Should generate 2 queries for different tables"
    );

    // Check we have queries for both tables
    let has_customers = queries.iter().any(|q| q.contains("customers"));
    let has_orders = queries.iter().any(|q| q.contains("orders"));
    assert!(has_customers, "Should have a query for customers table");
    assert!(has_orders, "Should have a query for orders table");

    // Verify the customers query has its WHERE clause
    let customers_query = queries
        .iter()
        .find(|q| q.contains("customers"))
        .expect("Should have customers query");
    assert!(
        customers_query.contains("WHERE"),
        "Customers query should have WHERE clause"
    );
}
