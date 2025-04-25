#[cfg(test)]
mod tests {
    use limbo_tests_macros::sqlite_values;

    use crate::{common::exec_sql, db_test};

    // #[test]
    // fn select_const_x() {

    //     exec_sql(
    //         "".into(),
    //         "SELECT 1",
    //         sqlite_values!(1),
    //     );
    // }

    // db_test!(select_const_1, "SELECT 1", 1);

    // db_test!(select_const_2, "SELECT 2", 2);

    // db_test!(select_const_3, "SELECT 0xDEAF", 57007);

    // db_test!(select_const_4, "SELECT -0xA", -10);

    // db_test!(select_true, "SELECT true", 1);

    // db_test!(select_false, "SELECT false", 0);

    // db_test!(select_text_escape_1, "SELECT '''a'", "'a");

    db_test!(select_text_blob_empty, "SELECT x''", b"");

    // db_test!(
    //     select_blob_ascii,
    //     "SELECT x'6C696D626f'",
    //     sqlite_blob!("6C696D626f")
    // );

    // db_test!(
    //     select_blob_emoji,
    //     "SELECT x'F09FA680'",
    //     sqlite_blob!("F09FA680")
    // );

    // db_test!(select_limit_0, "SELECT id FROM users LIMIT 0");

    // // ORDER BY id here because sqlite uses age_idx here and
    // // we (yet) don't so force it to evaluate in ID order
    // db_test!(
    //     select_limit_true,
    //     "SELECT id FROM users ORDER BY id LIMIT true",
    //     1
    // );

    db_test!(
        select_limit_false,
        "SELECT id FROM users ORDER BY id LIMIT false"
    );

    // db_test!(realify, "select price from products limit 1", 79.0);

    // db_test!(
    //     select_add,
    //     "select u.age + 1 from users u where u.age = 91 limit 1",
    //     92
    // );

    // db_test!(
    //     select_subtract,
    //     "select u.age - 1 from users u where u.age = 91 limit 1",
    //     90
    // );

    // db_test!(
    //     case_insensitive_columns,
    //     "select u.aGe + 1 from USERS u where U.AGe = 91 limit 1",
    //     92
    // );

    // db_test!(
    //     table_star,
    //     "select p.*, p.name from products p limit 1",
    //     vec![vec![1, "hat", 79.0, "hat"]]
    // );
}
