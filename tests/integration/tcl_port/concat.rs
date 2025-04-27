#[cfg(test)]
mod tests {
    use crate::db_test;

    db_test!(concat, "SELECT 'Hello' || ' ' || 'World'", "Hello World");

    db_test!(concat_2, "SELECT 'Hello' || NULL", [Null]);

    db_test!(concat_3, "SELECT 'Hello' || NULL", [Null]);

    db_test!(
        concat_4,
        "SELECT 'A good ' || name FROM products WHERE name = 'hat'",
        "A good hat"
    );

    db_test!(
        concat_5,
        "SELECT 'A good ' || name FROM products WHERE name = 'watch'"
    );
}
