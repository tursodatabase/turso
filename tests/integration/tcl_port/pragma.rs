#[cfg(test)]
mod tests {
    use crate::db_test;

    db_test!(pragma_cache_size, "PRAGMA cache_size", -2000);

    db_test!(
        pragma_update_journal_mode_wal,
        "PRAGMA journal_mode=WAL",
        "wal"
    );

    db_test!(
        pragma_table_info_equal_syntax,
        "PRAGMA table_info=sqlite_schema",
        [
            [0, "type", "TEXT", 0, Null, 0],
            [1, "name", "TEXT", 0, Null, 0],
            [2, "tbl_name", "TEXT", 0, Null, 0],
            [3, "rootpage", "INT", 0, Null, 0],
            [4, "sql", "TEXT", 0, Null, 0]
        ]
    );

    db_test!(
        pragma_table_info_call_syntax,
        "PRAGMA table_info(sqlite_schema)",
        [
            [0, "type", "TEXT", 0, Null, 0],
            [1, "name", "TEXT", 0, Null, 0],
            [2, "tbl_name", "TEXT", 0, Null, 0],
            [3, "rootpage", "INT", 0, Null, 0],
            [4, "sql", "TEXT", 0, Null, 0]
        ]
    );

    db_test!(pragma_table_info_invalid_table, "PRAGMA table_info=pekka");

    // temporarily skip this test case. The issue is detailed in #1407
    // db_test!(memory, pragma_page_count_empty, "PRAGMA page_count", 0);

    db_test!(
        memory,
        pragma_page_count_table,
        ["CREATE TABLE foo(bar)", "PRAGMA page_count"],
        2
    );

    db_test!(
        ["testing/testing_user_version_10.db"],
        pragma_user_version_user_set,
        "PRAGMA user_version",
        10
    );

    db_test!(
        memory,
        pragma_user_version_default,
        "PRAGMA user_version",
        0
    );
}
