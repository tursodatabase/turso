#[cfg(test)]
mod tests {
    use crate::{db_test, sqlite_blob};

    // #[test]
    // fn select_const_2() {
    //     let root = get_workspace_root().unwrap();

    //     exec_sql(
    //         root.join("testing/testing.db"),
    //         "SELECT 1",
    //         vec![vec![1.into()]],
    //     );
    // }

    db_test!(select_const_1, "SELECT 1", 1);

    db_test!(select_const_2, "SELECT 2", 2);

    db_test!(select_const_3, "SELECT 0xDEAF", 57007);

    db_test!(select_const_4, "SELECT -0xA", -10);

    db_test!(select_true, "SELECT true", 1);

    db_test!(select_false, "SELECT false", 0);

    db_test!(select_text_escape_1, "SELECT '''a'", "'a");

    db_test!(select_text_blob_empty, "SELECT x''", sqlite_blob!(""));

    db_test!(
        select_blob_ascii,
        "SELECT x'6C696D626f'",
        sqlite_blob!("6C696D626f")
    );
}
