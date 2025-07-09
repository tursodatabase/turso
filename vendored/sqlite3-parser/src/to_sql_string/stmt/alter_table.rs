#[cfg(test)]
mod tests {
    use crate::to_sql_string_test;

    to_sql_string_test!(
        test_alter_table_rename,
        "ALTER TABLE t RENAME TO new_table_name"
    );

    to_sql_string_test!(
        test_alter_table_add_column,
        "ALTER TABLE t ADD COLUMN c INTEGER"
    );

    to_sql_string_test!(
        test_alter_table_add_column_with_default,
        "ALTER TABLE t ADD COLUMN c TEXT DEFAULT 'value'"
    );

    to_sql_string_test!(
        test_alter_table_add_column_not_null_default,
        "ALTER TABLE t ADD COLUMN c REAL NOT NULL DEFAULT 0.0"
    );

    to_sql_string_test!(
        test_alter_table_add_column_unique,
        "ALTER TABLE t ADD COLUMN c TEXT UNIQUE",
        ignore = "ParserError = Cannot add a UNIQUE column"
    );

    to_sql_string_test!(
        test_alter_table_rename_column,
        "ALTER TABLE t RENAME COLUMN old_name TO new_name"
    );

    to_sql_string_test!(test_alter_table_drop_column, "ALTER TABLE t DROP COLUMN c");

    to_sql_string_test!(
        test_alter_table_add_column_check,
        "ALTER TABLE t ADD COLUMN c INTEGER CHECK (c > 0)"
    );

    to_sql_string_test!(
        test_alter_table_add_column_foreign_key,
        "ALTER TABLE t ADD COLUMN c INTEGER REFERENCES t2 (id) ON DELETE CASCADE"
    );

    to_sql_string_test!(
        test_alter_table_add_column_collate,
        "ALTER TABLE t ADD COLUMN c TEXT COLLATE NOCASE"
    );

    to_sql_string_test!(
        test_alter_table_add_column_primary_key,
        "ALTER TABLE t ADD COLUMN c INTEGER PRIMARY KEY",
        ignore = "ParserError = Cannot add a PRIMARY KEY column"
    );

    to_sql_string_test!(
        test_alter_table_add_column_primary_key_autoincrement,
        "ALTER TABLE t ADD COLUMN c INTEGER PRIMARY KEY AUTOINCREMENT",
        ignore = "ParserError = Cannot add a PRIMARY KEY column"
    );

    to_sql_string_test!(
        test_alter_table_add_generated_column,
        "ALTER TABLE t ADD COLUMN c_generated AS (a + b) STORED"
    );

    to_sql_string_test!(
        test_alter_table_add_column_schema,
        "ALTER TABLE schema_name.t ADD COLUMN c INTEGER"
    );
}
