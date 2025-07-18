#[cfg(test)]
mod tests {
    use crate::to_sql_string_test;

    // Basic INSERT with all columns
    to_sql_string_test!(
        test_insert_basic,
        "INSERT INTO employees (id, name, salary) VALUES (1, 'John Doe', 50000)"
    );

    // INSERT with multiple rows
    to_sql_string_test!(
        test_insert_multiple_rows,
        "INSERT INTO employees (id, name, salary) VALUES (1, 'John Doe', 50000), (2, 'Jane Smith', 60000)"
    );

    // INSERT with specific columns
    to_sql_string_test!(
        test_insert_specific_columns,
        "INSERT INTO employees (name, salary) VALUES ('Alice Brown', 55000)"
    );

    // INSERT with DEFAULT VALUES
    to_sql_string_test!(
        test_insert_default_values,
        "INSERT INTO employees DEFAULT VALUES"
    );

    // INSERT with SELECT subquery
    to_sql_string_test!(
        test_insert_select_subquery,
        "INSERT INTO employees (id, name, salary) SELECT id, name, salary FROM temp_employees WHERE salary > 40000"
    );

    // INSERT with ON CONFLICT IGNORE
    to_sql_string_test!(
        test_insert_on_conflict_ignore,
        "INSERT INTO employees (id, name, salary) VALUES (1, 'John Doe', 50000) ON CONFLICT (id) DO NOTHING"
    );

    // INSERT with ON CONFLICT REPLACE
    to_sql_string_test!(
        test_insert_on_conflict_replace,
        "INSERT INTO employees (id, name, salary) VALUES (1, 'John Doe', 50000) ON CONFLICT (id) DO UPDATE SET name = excluded.name, salary = excluded.salary"
    );

    // INSERT with RETURNING clause
    to_sql_string_test!(
        test_insert_with_returning,
        "INSERT INTO employees (id, name, salary) VALUES (1, 'John Doe', 50000) RETURNING id, name"
    );

    // INSERT with NULL values
    to_sql_string_test!(
        test_insert_with_null,
        "INSERT INTO employees (id, name, salary, department_id) VALUES (1, 'John Doe', NULL, NULL)"
    );

    // INSERT with expression in VALUES
    to_sql_string_test!(
        test_insert_with_expression,
        "INSERT INTO employees (id, name, salary) VALUES (1, 'John Doe', 50000 * 1.1)"
    );

    // INSERT into schema-qualified table
    to_sql_string_test!(
        test_insert_schema_qualified,
        "INSERT INTO main.employees (id, name, salary) VALUES (1, 'John Doe', 50000)"
    );

    // INSERT with subquery and JOIN
    to_sql_string_test!(
        test_insert_subquery_join,
        "INSERT INTO employees (id, name, department_id) SELECT e.id, e.name, d.id FROM temp_employees e JOIN departments d ON e.dept_name = d.name"
    );

    // INSERT with all columns from SELECT
    to_sql_string_test!(
        test_insert_all_columns_select,
        "INSERT INTO employees SELECT * FROM temp_employees"
    );

    // INSERT with ON CONFLICT and WHERE clause
    to_sql_string_test!(
        test_insert_on_conflict_where,
        "INSERT INTO employees (id, name, salary) VALUES (1, 'John Doe', 50000) ON CONFLICT (id) DO UPDATE SET salary = excluded.salary WHERE excluded.salary > employees.salary"
    );

    // INSERT with quoted column names (reserved words)
    to_sql_string_test!(
        test_insert_quoted_columns,
        "INSERT INTO employees (\"select\", \"from\") VALUES (1, 'data')"
    );
}
