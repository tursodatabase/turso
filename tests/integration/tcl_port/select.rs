#[cfg(test)]
mod tests {
    use crate::db_test;

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

    db_test!(select_const_3, ["SELECT 2", "SELECT 2"], [vec![vec![2]]]);
}
