use crate::{ast, to_sql_string::ToSqlString};

impl ToSqlString for ast::Update {
    fn to_sql_string<C: crate::to_sql_string::ToSqlContext>(&self, context: &C) -> String {
        format!(
            "{}UPDATE {}{}{} SET {}{}{}{}",
            self.with.as_ref().map_or("".to_string(), |with| format!(
                "{} ",
                with.to_sql_string(context)
            )),
            self.or_conflict
                .map_or("".to_string(), |conflict| format!("OR {conflict} ")),
            self.tbl_name.to_sql_string(context),
            self.indexed
                .as_ref()
                .map_or("".to_string(), |indexed| format!(" {indexed}")),
            self.sets
                .iter()
                .map(|set| set.to_sql_string(context))
                .collect::<Vec<_>>()
                .join(", "),
            self.from.as_ref().map_or("".to_string(), |from| format!(
                " {}",
                from.to_sql_string(context)
            )),
            self.where_clause
                .as_ref()
                .map_or("".to_string(), |expr| format!(
                    " WHERE {}",
                    expr.to_sql_string(context)
                )),
            self.returning
                .as_ref()
                .map_or("".to_string(), |returning| format!(
                    " RETURNING {}",
                    returning
                        .iter()
                        .map(|col| col.to_sql_string(context))
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::to_sql_string_test;

    // Basic UPDATE with a single column
    to_sql_string_test!(
        test_update_single_column,
        "UPDATE employees SET salary = 55000;"
    );

    // UPDATE with multiple columns
    to_sql_string_test!(
        test_update_multiple_columns,
        "UPDATE employees SET salary = 60000, name = 'John Smith';"
    );

    // UPDATE with a WHERE clause
    to_sql_string_test!(
        test_update_with_where,
        "UPDATE employees SET salary = 60000 WHERE id = 1;"
    );

    // UPDATE with multiple WHERE conditions
    to_sql_string_test!(
        test_update_with_multi_where,
        "UPDATE employees SET salary = 65000 WHERE department_id = 3 AND salary < 50000;"
    );

    // UPDATE with a subquery in SET
    to_sql_string_test!(
        test_update_with_subquery_set,
        "UPDATE employees SET department_id = (SELECT id FROM departments WHERE name = 'Sales') WHERE id = 1;"
    );

    // UPDATE with a subquery in WHERE
    to_sql_string_test!(
        test_update_with_subquery_where,
        "UPDATE employees SET salary = 70000 WHERE department_id IN (SELECT id FROM departments WHERE name = 'Marketing');"
    );

    // UPDATE with EXISTS clause
    to_sql_string_test!(
        test_update_with_exists,
        "UPDATE employees SET salary = 75000 WHERE EXISTS (SELECT 1 FROM orders WHERE orders.employee_id = employees.id AND orders.status = 'pending');"
    );

    // UPDATE with FROM clause (join-like behavior)
    to_sql_string_test!(
        test_update_with_from,
        "UPDATE employees SET salary = 80000 FROM departments WHERE employees.department_id = departments.id AND departments.name = 'Engineering';"
    );

    // UPDATE with RETURNING clause
    to_sql_string_test!(
        test_update_with_returning,
        "UPDATE employees SET salary = 60000 WHERE id = 1 RETURNING id, name, salary;"
    );

    // UPDATE with expression in SET
    to_sql_string_test!(
        test_update_with_expression,
        "UPDATE employees SET salary = salary * 1.1 WHERE department_id = 2;"
    );

    // UPDATE with NULL value
    to_sql_string_test!(
        test_update_with_null,
        "UPDATE employees SET department_id = NULL WHERE id = 1;"
    );

    // UPDATE with schema-qualified table
    to_sql_string_test!(
        test_update_schema_qualified,
        "UPDATE main.employees SET salary = 65000 WHERE id = 1;"
    );

    // UPDATE with CASE expression
    to_sql_string_test!(
        test_update_with_case,
        "UPDATE employees SET salary = CASE WHEN salary < 50000 THEN 55000 ELSE salary * 1.05 END WHERE department_id = 3;"
    );

    // UPDATE with LIKE clause in WHERE
    to_sql_string_test!(
        test_update_with_like,
        "UPDATE employees SET name = 'Updated' WHERE name LIKE 'J%';"
    );

    // UPDATE with ON CONFLICT (upsert-like behavior)
    to_sql_string_test!(
        test_update_with_on_conflict,
        "INSERT INTO employees (id, name, salary) VALUES (1, 'John Doe', 50000) ON CONFLICT(id) DO UPDATE SET name = excluded.name, salary = excluded.salary;"
    );
}
