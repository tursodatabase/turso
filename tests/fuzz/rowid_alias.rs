use super::helpers;
use core_tester::common::{limbo_exec_rows, try_limbo_exec_rows, TempDatabase};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sql_generation::{
    generation::{Arbitrary, GenerationContext, Opts},
    model::{
        query::{Create, Insert, Select},
        table::{Column, ColumnType, Table},
    },
};
use turso_parser::ast::ColumnConstraint;

fn rng_from_time_or_env() -> (ChaCha8Rng, u64) {
    let seed = if let Ok(seed_str) = std::env::var("FUZZ_SEED") {
        seed_str.parse::<u64>().expect("Invalid FUZZ_SEED value")
    } else {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    };
    let rng = ChaCha8Rng::seed_from_u64(seed);
    (rng, seed)
}

// Our test context that implements GenerationContext
#[derive(Debug, Clone)]
struct FuzzTestContext {
    opts: Opts,
    tables: Vec<Table>,
    views: Vec<sql_generation::model::view::View>,
}

impl FuzzTestContext {
    fn new() -> Self {
        Self {
            opts: Opts::default(),
            tables: Vec::new(),
            views: Vec::new(),
        }
    }

    fn add_table(&mut self, table: Table) {
        self.tables.push(table);
    }
}

impl GenerationContext for FuzzTestContext {
    fn tables(&self) -> &Vec<Table> {
        &self.tables
    }

    fn views(&self) -> &Vec<sql_generation::model::view::View> {
        &self.views
    }

    fn opts(&self) -> &Opts {
        &self.opts
    }
}

// Convert a table's CREATE statement to use INTEGER PRIMARY KEY (rowid alias)
fn convert_to_rowid_alias(create_sql: &str) -> String {
    // Since we always generate INTEGER PRIMARY KEY, just return as-is
    create_sql.to_string()
}

// Convert a table's CREATE statement to NOT use rowid alias
fn convert_to_no_rowid_alias(create_sql: &str) -> String {
    // Replace INTEGER PRIMARY KEY with INT PRIMARY KEY to disable rowid alias
    create_sql.replace("INTEGER PRIMARY KEY", "INT PRIMARY KEY")
}

#[test]
#[ignore]
pub fn rowid_alias_differential_fuzz() {
    let (mut rng, seed) = rng_from_time_or_env();
    tracing::info!("rowid_alias_differential_fuzz seed: {}", seed);

    // Number of queries to test
    let num_queries = if let Ok(num) = std::env::var("FUZZ_NUM_QUERIES") {
        num.parse::<usize>().unwrap_or(1000)
    } else {
        helpers::fuzz_iterations(1000)
    };

    // Create two Limbo databases with indexes enabled
    let db_with_alias = TempDatabase::new_empty();
    let db_without_alias = TempDatabase::new_empty();

    // Connect to both databases
    let conn_with_alias = db_with_alias.connect_limbo();
    let conn_without_alias = db_without_alias.connect_limbo();

    // Create our test context
    let mut context = FuzzTestContext::new();

    let mut successful_queries = 0;
    let mut skipped_queries = 0;

    for iteration in 0..num_queries {
        // Decide whether to create a new table, insert data, or generate a query
        let action =
            if context.tables.is_empty() || (context.tables.len() < 5 && rng.random_bool(0.1)) {
                0 // Create a new table
            } else if rng.random_bool(0.3) {
                1 // Insert data
            } else {
                2 // Generate a SELECT query
            };

        match action {
            0 => {
                // Generate a new table with an integer primary key
                let primary_key = Column {
                    name: "id".to_string(),
                    column_type: ColumnType::Integer,
                    constraints: vec![ColumnConstraint::PrimaryKey {
                        order: None,
                        conflict_clause: None,
                        auto_increment: false,
                    }],
                };
                let table_name = format!("table_{}", context.tables.len());
                let table = Table::arbitrary_with_columns(
                    &mut rng,
                    &context,
                    table_name,
                    vec![primary_key],
                );
                let create = Create {
                    table: table.clone(),
                };

                // Create table with rowid alias in first database
                let create_with_alias = convert_to_rowid_alias(&create.to_string());
                let _ = limbo_exec_rows(&conn_with_alias, &create_with_alias);

                // Create table without rowid alias in second database
                let create_without_alias = convert_to_no_rowid_alias(&create.to_string());
                let _ = limbo_exec_rows(&conn_without_alias, &create_without_alias);

                // Add table to context for future query generation
                context.add_table(table);

                skipped_queries += 1;
                continue;
            }
            1 => {
                // Generate and execute an INSERT statement
                let insert = Insert::arbitrary(&mut rng, &context);
                let insert_str = insert.to_string();

                // Execute the insert in both databases, ignoring constraint violations
                // (e.g., UNIQUE constraint failures on primary key)
                let result_with_alias =
                    try_limbo_exec_rows(&db_with_alias, &conn_with_alias, &insert_str);
                let result_without_alias =
                    try_limbo_exec_rows(&db_without_alias, &conn_without_alias, &insert_str);

                // Only update context if both inserts succeeded
                if result_with_alias.is_ok() && result_without_alias.is_ok() {
                    // Update the table's rows in the context so predicate generation knows about the data
                    if let Insert::Values {
                        table: table_name,
                        values,
                        ..
                    } = &insert
                    {
                        for table in &mut context.tables {
                            if table.name == *table_name {
                                table.rows.extend(values.clone());
                                break;
                            }
                        }
                    }
                }

                skipped_queries += 1;
                continue;
            }
            _ => {
                // Continue to generate SELECT query below
            }
        }

        let select = Select::arbitrary(&mut rng, &context);
        let query_str = select.to_string();

        tracing::debug!("Comparing query {}: {}", iteration, query_str);

        let with_alias_results = limbo_exec_rows(&conn_with_alias, &query_str);
        let without_alias_results = limbo_exec_rows(&conn_without_alias, &query_str);

        let mut sorted_with_alias = with_alias_results;
        let mut sorted_without_alias = without_alias_results;

        // Sort results to handle different row ordering
        sorted_with_alias.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        sorted_without_alias.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));

        assert_eq!(
            sorted_with_alias, sorted_without_alias,
            "Query produced different results with and without rowid alias!\n\
            Query: {query_str}\n\
            With rowid alias: {sorted_with_alias:?}\n\
            Without rowid alias: {sorted_without_alias:?}\n\
            Seed: {seed}"
        );

        successful_queries += 1;
    }

    tracing::info!(
        "Rowid alias differential fuzz test completed: {} queries tested successfully, {} queries skipped",
        successful_queries,
        skipped_queries
    );
}
