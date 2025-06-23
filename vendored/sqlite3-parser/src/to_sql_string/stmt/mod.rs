use crate::ast;

use super::ToSqlString;

mod alter_table;
mod create_table;
mod create_trigger;
mod create_virtual_table;
mod delete;
mod insert;
mod select;
mod update;

impl ToSqlString for ast::Stmt {
    fn to_sql_string<C: super::ToSqlContext>(&self, context: &C) -> String {
        match self {
            Self::AlterTable(alter_table) => {
                let (name, body) = alter_table.as_ref();
                format!(
                    "ALTER TABLE {} {};",
                    name.to_sql_string(context),
                    body.to_sql_string(context)
                )
            }
            Self::Analyze(name) => {
                if let Some(name) = name {
                    format!("ANALYZE {};", name.to_sql_string(context))
                } else {
                    "ANALYZE;".to_string()
                }
            }
            Self::Attach {
                expr,
                db_name,
                key: _,
            } => {
                // TODO: what is `key` in the attach syntax?
                format!(
                    "ATTACH {} AS {};",
                    expr.to_sql_string(context),
                    db_name.to_sql_string(context)
                )
            }
            // TODO: not sure where name is applied here
            // https://www.sqlite.org/lang_transaction.html
            Self::Begin(transaction_type, _name) => {
                let t_type = transaction_type.map_or("", |t_type| match t_type {
                    ast::TransactionType::Deferred => " DEFERRED",
                    ast::TransactionType::Exclusive => " EXCLUSIVE",
                    ast::TransactionType::Immediate => " IMMEDIATE",
                });
                format!("BEGIN{t_type};")
            }
            // END or COMMIT are equivalent here, so just defaulting to COMMIT
            // TODO: again there are no names in the docs
            Self::Commit(_name) => "COMMIT;".to_string(),
            Self::CreateIndex {
                unique,
                if_not_exists,
                idx_name,
                tbl_name,
                columns,
                where_clause,
            } => format!(
                "CREATE {}INDEX {}{} ON {} ({}){};",
                unique.then_some("UNIQUE ").unwrap_or(""),
                if_not_exists.then_some("IF NOT EXISTS ").unwrap_or(""),
                idx_name.to_sql_string(context),
                tbl_name.0,
                columns
                    .iter()
                    .map(|col| col.to_sql_string(context))
                    .collect::<Vec<_>>()
                    .join(", "),
                where_clause
                    .as_ref()
                    .map_or("".to_string(), |where_clause| format!(
                        " WHERE {}",
                        where_clause.to_sql_string(context)
                    ))
            ),
            Self::CreateTable {
                temporary,
                if_not_exists,
                tbl_name,
                body,
            } => format!(
                "CREATE{} TABLE {}{} {};",
                temporary.then_some(" TEMP").unwrap_or(""),
                if_not_exists.then_some("IF NOT EXISTS ").unwrap_or(""),
                tbl_name.to_sql_string(context),
                body.to_sql_string(context)
            ),
            Self::CreateTrigger(trigger) => trigger.to_sql_string(context),
            Self::CreateView {
                temporary,
                if_not_exists,
                view_name,
                columns,
                select,
            } => {
                format!(
                    "CREATE{} VIEW {}{}{} AS {};",
                    temporary.then_some(" TEMP").unwrap_or(""),
                    if_not_exists.then_some("IF NOT EXISTS ").unwrap_or(""),
                    view_name.to_sql_string(context),
                    columns.as_ref().map_or("".to_string(), |columns| format!(
                        " ({})",
                        columns
                            .iter()
                            .map(|col| col.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )),
                    select.to_sql_string(context)
                )
            }
            Self::CreateVirtualTable(create_virtual_table) => {
                create_virtual_table.to_sql_string(context)
            }
            Self::Delete(delete) => delete.to_sql_string(context),
            Self::Detach(name) => format!("DETACH {};", name.to_sql_string(context)),
            Self::DropIndex {
                if_exists,
                idx_name,
            } => format!(
                "DROP INDEX{} {};",
                if_exists.then_some("IF EXISTS ").unwrap_or(""),
                idx_name.to_sql_string(context)
            ),
            Self::DropTable {
                if_exists,
                tbl_name,
            } => format!(
                "DROP TABLE{} {};",
                if_exists.then_some("IF EXISTS ").unwrap_or(""),
                tbl_name.to_sql_string(context)
            ),
            Self::DropTrigger {
                if_exists,
                trigger_name,
            } => format!(
                "DROP TRIGGER{} {};",
                if_exists.then_some("IF EXISTS ").unwrap_or(""),
                trigger_name.to_sql_string(context)
            ),
            Self::DropView {
                if_exists,
                view_name,
            } => format!(
                "DROP VIEW{} {};",
                if_exists.then_some("IF EXISTS ").unwrap_or(""),
                view_name.to_sql_string(context)
            ),
            Self::Insert(insert) => format!("{};", insert.to_sql_string(context)),
            Self::Pragma(name, body) => format!(
                "PRAGMA {}{};",
                name.to_sql_string(context),
                body.as_ref()
                    .map_or("".to_string(), |body| match body.as_ref() {
                        ast::PragmaBody::Equals(expr) =>
                            format!(" = {}", expr.to_sql_string(context)),
                        ast::PragmaBody::Call(expr) => format!("({})", expr.to_sql_string(context)),
                    })
            ),
            // TODO: missing collation name
            Self::Reindex { obj_name } => format!(
                "REINDEX{};",
                obj_name.as_ref().map_or("".to_string(), |name| format!(
                    " {}",
                    name.to_sql_string(context)
                ))
            ),
            Self::Release(name) => format!("RELEASE {};", name.0),
            Self::Rollback {
                // TODO: there is no transaction name in SQLITE
                // https://www.sqlite.org/lang_transaction.html
                tx_name: _,
                savepoint_name,
            } => format!(
                "ROLLBACK{};",
                savepoint_name
                    .as_ref()
                    .map_or("".to_string(), |name| format!(" TO {}", name.0))
            ),
            Self::Savepoint(name) => format!("SAVEPOINT {};", name.0),
            Self::Select(select) => format!("{};", select.to_sql_string(context)),
            Self::Update(update) => format!("{};", update.to_sql_string(context)),
            Self::Vacuum(name, expr) => {
                format!(
                    "VACUUM{}{};",
                    name.as_ref()
                        .map_or("".to_string(), |name| format!(" {}", name.0)),
                    expr.as_ref().map_or("".to_string(), |expr| format!(
                        " INTO {}",
                        expr.to_sql_string(context)
                    ))
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::to_sql_string::ToSqlContext;

    #[macro_export]
    /// Create a test that first parses then input, the converts the parsed ast back to a string and compares with original input
    macro_rules! to_sql_string_test {
        ($test_name:ident, $input:expr) => {
            #[test]
            fn $test_name() {
                let context = $crate::to_sql_string::stmt::tests::TestContext;
                let input = $input.split_whitespace().collect::<Vec<&str>>().join(" ");
                let mut parser = $crate::lexer::sql::Parser::new(input.as_bytes());
                let cmd = fallible_iterator::FallibleIterator::next(&mut parser)
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    input,
                    $crate::to_sql_string::ToSqlString::to_sql_string(cmd.stmt(), &context)
                );
            }
        };
        ($test_name:ident, $input:expr, $($attribute:meta),*) => {
            #[test]
            $(#[$attribute])*
            fn $test_name() {
                let context = $crate::to_sql_string::stmt::tests::TestContext;
                let input = $input.split_whitespace().collect::<Vec<&str>>().join(" ");
                let mut parser = $crate::lexer::sql::Parser::new(input.as_bytes());
                let cmd = fallible_iterator::FallibleIterator::next(&mut parser)
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    input,
                    $crate::to_sql_string::ToSqlString::to_sql_string(cmd.stmt(), &context)
                );
            }
        }
    }

    pub(crate) struct TestContext;

    // Placeholders for compilation
    // Context only necessary parsing inside turso_core or in the simulator
    impl ToSqlContext for TestContext {
        fn get_column_name(&self, _table_id: crate::ast::TableInternalId, _col_idx: usize) -> &str {
            todo!()
        }

        fn get_table_name(&self, _id: crate::ast::TableInternalId) -> &str {
            todo!()
        }
    }

    to_sql_string_test!(test_analyze, "ANALYZE;");

    to_sql_string_test!(
        test_analyze_table,
        "ANALYZE table;",
        ignore = "parser can't parse table name"
    );

    to_sql_string_test!(
        test_analyze_schema_table,
        "ANALYZE schema.table;",
        ignore = "parser can't parse schema.table name"
    );

    to_sql_string_test!(test_attach, "ATTACH './test.db' AS test_db;");

    to_sql_string_test!(test_transaction, "BEGIN;");

    to_sql_string_test!(test_transaction_deferred, "BEGIN DEFERRED;");

    to_sql_string_test!(test_transaction_immediate, "BEGIN IMMEDIATE;");

    to_sql_string_test!(test_transaction_exclusive, "BEGIN EXCLUSIVE;");

    to_sql_string_test!(test_commit, "COMMIT;");

    // Test a simple index on a single column
    to_sql_string_test!(
        test_create_index_simple,
        "CREATE INDEX idx_name ON employees (last_name);"
    );

    // Test a unique index to enforce uniqueness on a column
    to_sql_string_test!(
        test_create_unique_index,
        "CREATE UNIQUE INDEX idx_unique_email ON users (email);"
    );

    // Test a multi-column index
    to_sql_string_test!(
        test_create_index_multi_column,
        "CREATE INDEX idx_name_salary ON employees (last_name, salary);"
    );

    // Test a partial index with a WHERE clause
    to_sql_string_test!(
        test_create_partial_index,
        "CREATE INDEX idx_active_users ON users (username) WHERE active = true;"
    );

    // Test an index on an expression
    to_sql_string_test!(
        test_create_index_on_expression,
        "CREATE INDEX idx_upper_name ON employees (UPPER(last_name));"
    );

    // Test an index with descending order
    to_sql_string_test!(
        test_create_index_descending,
        "CREATE INDEX idx_salary_desc ON employees (salary DESC);"
    );

    // Test an index with mixed ascending and descending orders on multiple columns
    to_sql_string_test!(
        test_create_index_mixed_order,
        "CREATE INDEX idx_name_asc_salary_desc ON employees (last_name ASC, salary DESC);"
    );

    // Test 1: View with DISTINCT keyword
    to_sql_string_test!(
        test_create_view_distinct,
        "CREATE VIEW view_distinct AS SELECT DISTINCT name FROM employees;"
    );

    // Test 2: View with LIMIT clause
    to_sql_string_test!(
        test_create_view_limit,
        "CREATE VIEW view_limit AS SELECT id, name FROM employees LIMIT 10;"
    );

    // Test 3: View with CASE expression
    to_sql_string_test!(
        test_create_view_case,
        "CREATE VIEW view_case AS SELECT name, CASE WHEN salary > 70000 THEN 'High' ELSE 'Low' END AS salary_level FROM employees;"
    );

    // Test 4: View with LEFT JOIN
    to_sql_string_test!(
        test_create_view_left_join,
        "CREATE VIEW view_left_join AS SELECT e.name, d.name AS department FROM employees e LEFT JOIN departments d ON e.department_id = d.id;"
    );

    // Test 5: View with HAVING clause
    to_sql_string_test!(
        test_create_view_having,
        "CREATE VIEW view_having AS SELECT department_id, AVG(salary) AS avg_salary FROM employees GROUP BY department_id HAVING AVG(salary) > 55000;"
    );

    // Test 6: View with CTE (Common Table Expression)
    to_sql_string_test!(
        test_create_view_cte,
        "CREATE VIEW view_cte AS WITH high_earners AS (SELECT * FROM employees WHERE salary > 80000) SELECT id, name FROM high_earners;"
    );

    // Test 7: View with multiple conditions in WHERE
    to_sql_string_test!(
        test_create_view_multi_where,
        "CREATE VIEW view_multi_where AS SELECT id, name FROM employees WHERE salary > 50000 AND department_id = 3;"
    );

    // Test 8: View with NULL handling
    to_sql_string_test!(
        test_create_view_null,
        "CREATE VIEW view_null AS SELECT name, COALESCE(salary, 0) AS salary FROM employees;"
    );

    // Test 9: View with subquery in WHERE clause
    to_sql_string_test!(
        test_create_view_subquery_where,
        "CREATE VIEW view_subquery_where AS SELECT name FROM employees WHERE department_id IN (SELECT id FROM departments WHERE name = 'Sales');"
    );

    // Test 10: View with arithmetic expression
    to_sql_string_test!(
        test_create_view_arithmetic,
        "CREATE VIEW view_arithmetic AS SELECT name, salary * 1.1 AS adjusted_salary FROM employees;"
    );

    to_sql_string_test!(test_detach, "DETACH 'x.db';");

    to_sql_string_test!(test_drop_index, "DROP INDEX schema_name.test_index;");

    to_sql_string_test!(test_drop_table, "DROP TABLE schema_name.test_table;");

    to_sql_string_test!(test_drop_trigger, "DROP TRIGGER schema_name.test_trigger;");

    to_sql_string_test!(test_drop_view, "DROP VIEW schema_name.test_view;");

    to_sql_string_test!(test_pragma_equals, "PRAGMA schema_name.Pragma_name = 1;");

    to_sql_string_test!(test_pragma_call, "PRAGMA schema_name.Pragma_name_2(1);");

    to_sql_string_test!(test_reindex, "REINDEX schema_name.test_table;");

    to_sql_string_test!(test_reindex_2, "REINDEX;");

    to_sql_string_test!(test_release, "RELEASE savepoint_name;");

    to_sql_string_test!(test_rollback, "ROLLBACK;");

    to_sql_string_test!(test_rollback_2, "ROLLBACK TO savepoint_name;");

    to_sql_string_test!(test_savepoint, "SAVEPOINT savepoint_name;");

    to_sql_string_test!(test_vacuum, "VACUUM;");

    to_sql_string_test!(test_vacuum_2, "VACUUM schema_name;");

    to_sql_string_test!(test_vacuum_3, "VACUUM schema_name INTO test.db;");
}
