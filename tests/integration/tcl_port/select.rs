#[cfg(test)]
mod tests {
    use limbo_tests_macros::sqlite_values;

    use crate::{common::exec_sql, db_test};

    // #[test]
    // fn select_const_x() {

    //     exec_sql(
    //         "".into(),
    //         "SELECT 1",
    //         sqlite_values!(""),
    //     );
    // }

    // db_test!(select_const_1, "SELECT 1", 1);

    // db_test!(select_const_2, "SELECT 2", 2);

    // db_test!(select_const_3, "SELECT 0xDEAF", 57007);

    // db_test!(select_const_4, "SELECT -0xA", -10);

    // db_test!(select_true, "SELECT true", 1);

    // db_test!(select_false, "SELECT false", 0);

    // db_test!(select_text_escape_1, "SELECT '''a'", "'a");

    // db_test!(select_text_blob_empty, "SELECT x''", b"");

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

    // db_test!(
    //     select_limit_false,
    //     "SELECT id FROM users ORDER BY id LIMIT false"
    // );

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
    //     [1, "hat", 79.0, "hat"]
    // );

    // db_test!(
    //     table_star_2,
    //     "select p.*, u.first_name from users u join products p on u.id = p.id limit 1",
    //     [1, "hat", 79.0, "Jamie"]
    // );

    // db_test!(
    //     select_with_quoting,
    //     "select `users`.id from [users] where users.[id] = 5",
    //     5
    // );

    // db_test!(
    //     select_with_quoting_2,
    //     "select "users".`id` from users where `users`.[id] = 5",
    //     5
    // );

    // db_test!(
    //     select_rowid,
    //     "select u.rowid, first_name from users u where rowid = 5",
    //     [5, "Edward"]
    // );

    // db_test!(
    //     select_rowid_2,
    //     "select u.rowid, first_name from users u where rowid = 5",
    //     [5, "Edward"]
    // );

    // db_test!(
    //     seekrowid,
    //     "select * from users u where u.id = 5",
    //     [
    //         5,
    //         "Edward",
    //         "Miller",
    //         "christiankramer@example.com",
    //         "725-281-1033",
    //         "08522 English Plain",
    //         "Lake Keith",
    //         "ID",
    //         "23283",
    //         15
    //     ]
    // );

    // db_test!(
    //     select_parenthesized,
    //     "select (price + 100) from products limit 1",
    //     179.0
    // );

    // db_test!(
    //     select_case_base_else,
    //     "select case when 0 then 'false' when 1 then 'true' else 'null' end",
    //     true
    // );

    // db_test!(
    //     select_case_noelse_null,
    //     "select case when 0 then 0 end",
    //     [Null]
    // );

    // db_test!(
    //     select_base_case_else,
    //     "select case 1 when 0 then 'zero' when 1 then 'one' else 'two' end",
    //     "one"
    // );

    db_test!(
        select_base_case_null_result,
        [
            "select case NULL when 0 then 'first' else 'second' end",
            "select case NULL when NULL then 'first' else 'second' end",
            "select case 0 when 0 then 'first' else 'second' end"
        ],
        ["second", "second", "first"]
    );
}
