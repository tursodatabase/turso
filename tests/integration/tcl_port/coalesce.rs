#[cfg(test)]
mod tests {
    use crate::db_test;

    db_test!(coaslesce, "select coalesce(NULL, 1)", 1);

    db_test!(coaslesce_2, "select coalesce(NULL, NULL, 1)", 1);

    db_test!(
        coaslesce_nested,
        "select coalesce(NULL, coalesce(NULL, NULL))",
        [Null]
    );

    db_test!(
        coaslesce_nested_2,
        [
            "select coalesce(NULL, coalesce(NULL, 2))",
            " select coalesce(NULL, coalesce(1, 2))",
            "select coalesce(0, coalesce(1, 2))"
        ],
        [2, 1, 0]
    );

    db_test!(coaslesce_null, "select coalesce(NULL, NULL, NULL)", [Null]);

    db_test!(coaslesce_first, "select coalesce(1, 2, 3)", 1);

    db_test!(
        coaslesce_from_table,
        "select coalesce(NULL, 1) from users limit 1",
        1
    );

    db_test!(
        coaslesce_from_table_column,
        "select coalesce(NULL, age) from users where age = 94 limit 1",
        94
    );

    db_test!(
        coaslesce_from_table_multiple_columns,
        "select coalesce(NULL, age), coalesce(NULL, id) from users where age = 94 limit 1",
        [94, 1]
    );
}
