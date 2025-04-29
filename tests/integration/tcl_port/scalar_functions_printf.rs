#[cfg(test)]
mod tests {
    use crate::db_test;

    // Basic string formatting
    db_test!(
        printf_basic_string,
        "SELECT printf('Hello World!')",
        "Hello World!"
    );

    db_test!(
        printf_string_replacement,
        "SELECT printf('Hello, %s', 'Alice')",
        "Hello, Alice"
    );

    db_test!(
        printf_numeric_replacement,
        "SELECT printf('My number is: %d', 42)",
        "My number is: 42"
    );
}
