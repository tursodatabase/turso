#[cfg(test)]
mod tests {
    use crate::db_test;

    // Basic single row delete test
    db_test!(
        memory,
        delete_single_1,
        [
            "CREATE TABLE t1(x INTEGER PRIMARY KEY)",
            "INSERT INTO t1 VALUES (1)",
            "INSERT INTO t1 VALUES (2)",
            "INSERT INTO t1 VALUES (3)",
            "DELETE FROM t1 WHERE x = 2",
            "SELECT * FROM t1 ORDER BY x"
        ],
        [[1], [3]]
    );

    // Test alternating delete-insert pattern to stress freelist
    db_test!(
        memory,
        delete_insert_alternate_1,
        [
            "CREATE TABLE t4(x INTEGER PRIMARY KEY)",
            "INSERT INTO t4 VALUES (1)",
            "INSERT INTO t4 VALUES (2)",
            "INSERT INTO t4 VALUES (3)",
            "DELETE FROM t4 WHERE x = 2",
            "INSERT INTO t4 VALUES (4)",
            "DELETE FROM t4 WHERE x = 1",
            "INSERT INTO t4 VALUES (5)",
            "SELECT * FROM t4 ORDER BY x"
        ],
        [[3], [4], [5]]
    );

    // Test deleting from both ends
    db_test!(
        memory,
        delete_ends_1,
        [
            "CREATE TABLE t5(x INTEGER PRIMARY KEY)",
            "INSERT INTO t5 VALUES (1)",
            "INSERT INTO t5 VALUES (2)",
            "INSERT INTO t5 VALUES (3)",
            "INSERT INTO t5 VALUES (4)",
            "INSERT INTO t5 VALUES (5)",
            // Delete from both ends
            "DELETE FROM t5 WHERE x = 1",
            "DELETE FROM t5 WHERE x = 5",
            "SELECT * FROM t5 ORDER BY x"
        ],
        [[2], [3], [4]]
    );

    // Test delete-insert cycles with value reuse
    db_test!(
        memory,
        delete_reuse_1,
        [
            "CREATE TABLE t6(x INTEGER PRIMARY KEY)",
            "INSERT INTO t6 VALUES (1)",
            "INSERT INTO t6 VALUES (2)",
            "INSERT INTO t6 VALUES (3)",
            "DELETE FROM t6 WHERE x = 2",
            "INSERT INTO t6 VALUES (2)", // Reuse same value
            "SELECT * FROM t6 ORDER BY x"
        ],
        [[1], [2], [3]]
    );
}
