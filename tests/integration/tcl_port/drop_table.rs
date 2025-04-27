#[cfg(test)]
mod tests {
    use crate::db_test;

    // Basic DROP TABLE functionality
    db_test!(
        memory,
        drop_table_basic_1,
        [
            "CREATE TABLE t1(x INTEGER PRIMARY KEY)",
            "INSERT INTO t1 VALUES (1)",
            "INSERT INTO t1 VALUES (2)",
            "DROP TABLE t1",
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='t1'"
        ],
        [0]
    );

    // Test DROP TABLE IF EXISTS on existing table
    db_test!(
        memory,
        drop_table_if_exists_1,
        [
            "CREATE TABLE t2(x INTEGER PRIMARY KEY)",
            "DROP TABLE IF EXISTS t2",
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='t2'"
        ],
        [0]
    );

    // Test DROP TABLE IF EXISTS on non-existent table
    db_test!(
        memory,
        drop_table_if_exists_2,
        ["DROP TABLE IF EXISTS nonexistent_table", "SELECT 'success'"],
        ["success"]
    );

    // Test dropping table with index
    /*
    db_test!(
        memory,
        drop_table_with_index_1,
        [
            "CREATE TABLE t3(x INTEGER PRIMARY KEY, y TEXT)",
            "CREATE INDEX idx_t3_y ON t3(y)",
            "INSERT INTO t3 VALUES(1, 'one')",
            "DROP TABLE t3",
            "SELECT count(*) FROM sqlite_schema WHERE tbl_name='t3'"
        ],
        [0]
    );
    */

    // Test dropping table cleans up related schema entries
    /*
    db_test!(
        memory,
        drop_table_schema_cleanup_1,
        [
            "CREATE TABLE t4(x INTEGER PRIMARY KEY, y TEXT)",
            "CREATE INDEX idx1_t4 ON t4(x)",
            "CREATE INDEX idx2_t4 ON t4(y)",
            "INSERT INTO t4 VALUES(1, 'one')",
            "DROP TABLE t4",
            "SELECT count(*) FROM sqlite_schema WHERE tbl_name='t4'"
        ],
        [0]
    );
    */

    // Test dropping table after multiple inserts and deletes
    db_test!(
        memory,
        drop_table_after_ops_1,
        [
            "CREATE TABLE t6(x INTEGER PRIMARY KEY)",
            "INSERT INTO t6 VALUES (1)",
            "INSERT INTO t6 VALUES (2)",
            "DELETE FROM t6 WHERE x = 1",
            "INSERT INTO t6 VALUES (3)",
            "DROP TABLE t6",
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='t6'"
        ],
        [0]
    );
}
