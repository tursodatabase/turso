#[cfg(test)]
mod tests {
    use crate::to_sql_string_test;

    to_sql_string_test!(
        test_create_table_simple,
        "CREATE TABLE t (a INTEGER, b TEXT)"
    );

    to_sql_string_test!(
        test_create_table_primary_key,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"
    );

    to_sql_string_test!(
        test_create_table_multi_primary_key,
        "CREATE TABLE t (a INTEGER, b TEXT, PRIMARY KEY (a, b))"
    );

    to_sql_string_test!(
        test_create_table_data_types,
        "CREATE TABLE t (a INTEGER, b TEXT, c REAL, d BLOB, e NUMERIC)"
    );

    to_sql_string_test!(
        test_create_table_foreign_key,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, t_id INTEGER, FOREIGN KEY (t_id) REFERENCES t (id))"
    );

    to_sql_string_test!(
        test_create_table_foreign_key_cascade,
        "CREATE TABLE t2 (id INTEGER PRIMARY KEY, t_id INTEGER, FOREIGN KEY (t_id) REFERENCES t (id) ON DELETE CASCADE)"
    );

    to_sql_string_test!(
        test_create_table_unique,
        "CREATE TABLE t (a INTEGER UNIQUE, b TEXT)"
    );

    to_sql_string_test!(
        test_create_table_not_null,
        "CREATE TABLE t (a INTEGER NOT NULL, b TEXT)"
    );

    to_sql_string_test!(
        test_create_table_check,
        "CREATE TABLE t (a INTEGER CHECK (a > 0), b TEXT)"
    );

    to_sql_string_test!(
        test_create_table_default,
        "CREATE TABLE t (a INTEGER DEFAULT 0, b TEXT)"
    );

    to_sql_string_test!(
        test_create_table_multiple_constraints,
        "CREATE TABLE t (a INTEGER NOT NULL UNIQUE, b TEXT DEFAULT 'default')"
    );

    to_sql_string_test!(
        test_create_table_generated_column,
        "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER AS (a + b))"
    );

    to_sql_string_test!(
        test_create_table_generated_stored,
        "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER AS (a + b) STORED)"
    );

    to_sql_string_test!(
        test_create_table_generated_virtual,
        "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER AS (a + b) VIRTUAL)"
    );

    to_sql_string_test!(
        test_create_table_quoted_columns,
        "CREATE TABLE t (\"select\" INTEGER, \"from\" TEXT)"
    );

    to_sql_string_test!(
        test_create_table_quoted_table,
        "CREATE TABLE \"my table\" (a INTEGER)"
    );

    to_sql_string_test!(
        test_create_table_if_not_exists,
        "CREATE TABLE IF NOT EXISTS t (a INTEGER)"
    );

    to_sql_string_test!(test_create_temp_table, "CREATE TEMP TABLE t (a INTEGER)");

    to_sql_string_test!(
        test_create_table_without_rowid,
        "CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT) WITHOUT ROWID"
    );

    to_sql_string_test!(
        test_create_table_named_primary_key,
        "CREATE TABLE t (a INTEGER CONSTRAINT pk_a PRIMARY KEY)"
    );

    to_sql_string_test!(
        test_create_table_named_unique,
        "CREATE TABLE t (a INTEGER, CONSTRAINT unique_a UNIQUE (a))"
    );

    to_sql_string_test!(
        test_create_table_named_foreign_key,
        "CREATE TABLE t2 (id INTEGER, t_id INTEGER, CONSTRAINT fk_t FOREIGN KEY (t_id) REFERENCES t (id))"
    );

    to_sql_string_test!(
        test_create_table_complex,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b TEXT DEFAULT 'default', c INTEGER AS (a * 2), CONSTRAINT unique_a UNIQUE (a))"
    );

    to_sql_string_test!(
        test_create_table_multiple_foreign_keys,
        "CREATE TABLE t3 (id INTEGER PRIMARY KEY, t1_id INTEGER, t2_id INTEGER, FOREIGN KEY (t1_id) REFERENCES t1 (id), FOREIGN KEY (t2_id) REFERENCES t2 (id))"
    );
}
