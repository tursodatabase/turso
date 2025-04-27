#[cfg(test)]
mod tests {
    use crate::db_test;

    db_test!(select_const_1, "SELECT 1", 1);

    db_test!(select_const_2, "SELECT 2", 2);

    db_test!(select_const_3, "SELECT 0xDEAF", 57007);

    // TODO: rust-analyzer bug
    db_test!(select_const_4, "SELECT -0xA", -10);

    db_test!(select_true, "SELECT true", 1);

    db_test!(select_false, "SELECT false", 0);

    db_test!(select_text_escape_1, "SELECT '''a'", "'a");

    db_test!(select_text_blob_empty, "SELECT x''", b"");

    db_test!(select_blob_ascii, "SELECT x'6C696D626f'", b"6C696D626f");

    db_test!(select_blob_emoji, "SELECT x'F09FA680'", b"F09FA680");

    db_test!(select_limit_0, "SELECT id FROM users LIMIT 0");

    // ORDER BY id here because sqlite uses age_idx here and
    // we (yet) don't so force it to evaluate in ID order
    db_test!(
        select_limit_true,
        "SELECT id FROM users ORDER BY id LIMIT true",
        1
    );

    db_test!(
        select_limit_false,
        "SELECT id FROM users ORDER BY id LIMIT false"
    );

    db_test!(realify, "select price from products limit 1", 79.0);

    db_test!(
        select_add,
        "select u.age + 1 from users u where u.age = 91 limit 1",
        92
    );

    db_test!(
        select_subtract,
        "select u.age - 1 from users u where u.age = 91 limit 1",
        90
    );

    db_test!(
        case_insensitive_columns,
        "select u.aGe + 1 from USERS u where U.AGe = 91 limit 1",
        92
    );

    db_test!(
        table_star,
        "select p.*, p.name from products p limit 1",
        [1, "hat", 79.0, "hat"]
    );

    db_test!(
        table_star_2,
        "select p.*, u.first_name from users u join products p on u.id = p.id limit 1",
        [1, "hat", 79.0, "Jamie"]
    );

    db_test!(
        select_with_quoting,
        "select `users`.id from [users] where users.[id] = 5",
        5
    );

    db_test!(
        select_with_quoting_2,
        "select \"users\".`id` from users where `users`.[id] = 5",
        5
    );

    db_test!(
        select_rowid,
        "select u.rowid, first_name from users u where rowid = 5",
        [5, "Edward"]
    );

    db_test!(
        select_rowid_2,
        "select u.rowid, first_name from users u where rowid = 5",
        [5, "Edward"]
    );

    db_test!(
        seekrowid,
        "select * from users u where u.id = 5",
        [
            5,
            "Edward",
            "Miller",
            "christiankramer@example.com",
            "725-281-1033",
            "08522 English Plain",
            "Lake Keith",
            "ID",
            "23283",
            15
        ]
    );

    db_test!(
        select_parenthesized,
        "select (price + 100) from products limit 1",
        179.0
    );

    db_test!(
        select_case_base_else,
        "select case when 0 then 'false' when 1 then 'true' else 'null' end",
        "true"
    );

    db_test!(
        select_case_noelse_null,
        "select case when 0 then 0 end",
        [Null]
    );

    db_test!(
        select_base_case_else,
        "select case 1 when 0 then 'zero' when 1 then 'one' else 'two' end",
        "one"
    );

    db_test!(
        select_base_case_null_result,
        [
            "select case NULL when 0 then 'first' else 'second' end",
            "select case NULL when NULL then 'first' else 'second' end",
            "select case 0 when 0 then 'first' else 'second' end"
        ],
        ["second", "second", "first"]
    );

    db_test!(
        select_base_case_noelse_null,
        "select case 'null else' when 0 then 0 when 1 then 1 end",
        [Null]
    );

    db_test!(
        select_is_null,
        [
            "select null is null, (1 / 0) is null, null is (1 / 0), (1 / 0) is (1 / 0)",
            "select 4 is null, '4' is null, 0 is null, (1 / 2) is null"
        ],
        [[1, 1, 1, 1], [0, 0, 0, 0]]
    );

    db_test!(
        select_is_not_null,
        [
            "select null is not null, (1 / 0) is not null, null is not (1 / 0), (1 / 0) is not (1 / 0)",
            "select 4 is not null, '4' is not null, 0 is not null, (1 / 2) is not null"
        ],
        [[0, 0, 0, 0], [1, 1, 1, 1]]
    );

    // TODO: there is a rust-analyzer bug that incorrectly expands negative integers in proc_macros
    // Waiting for this to be fixed here: https://github.com/rust-lang/rust-analyzer/pull/19434
    db_test!(
        select_bin_shr,
        [
            "select 997623670 >> 0, 997623670 >> 1, 997623670 >> 10, 997623670 >> 30",
            "select -997623670 >> 0, -997623670 >> 1, -997623670 >> 10, -997623670 >> 30",
            "select 997623670 << 0, 997623670 << -1, 997623670 << -10, 997623670 << -30",
            "select -997623670 << 0, -997623670 << -1, -997623670 << -10, -997623670 << -30"
        ],
        [
            [997623670, 498811835, 974241, 0],
            [-997623670, -498811835, -974242, -1],
            [997623670, 498811835, 974241, 0],
            [-997623670, -498811835, -974242, -1]
        ]
    );

    // TODO: there is a rust-analyzer bug that incorrectly expands negative integers in proc_macros
    // Waiting for this to be fixed here: https://github.com/rust-lang/rust-analyzer/pull/19434
    db_test!(
        select_bin_shl,
        [
            "select 997623670 << 0, 997623670 << 1, 997623670 << 10, 997623670 << 30",
            "select -997623670 << 0, -997623670 << 1, -997623670 << 10, -997623670 << 30",
            "select 997623670 >> 0, 997623670 >> -1, 997623670 >> -10, 997623670 >> -30",
            "select -997623670 >> 0, -997623670 >> -1, -997623670 >> -10, -997623670 >> -30"
        ],
        [
            [997623670, 1995247340, 1021566638080, 1071190259091374080],
            [
                -997623670,
                -1995247340,
                -1021566638080,
                -1071190259091374080
            ],
            [997623670, 1995247340, 1021566638080, 1071190259091374080],
            [
                -997623670,
                -1995247340,
                -1021566638080,
                -1071190259091374080
            ]
        ]
    );

    // Test LIKE in SELECT position
    db_test!(select_like_expression, "select 'bar' like 'bar%'", 1);

    db_test!(
        select_not_like_expression,
        "select 'bar' not like 'bar%'",
        0
    );

    // regression test for float divisor being cast to zero int and panicking
    db_test!(select_like_expression_2, "select 2 % 0.5", [Null]);

    db_test!(
        select_positive_infinite_float,
        "SELECT 1.7976931348623157E+308 + 1e308; -- f64::MAX + 1e308",
        [Inf]
    );

    db_test!(
        select_negative_infinite_float,
        "SELECT -1.7976931348623157E+308 - 1e308 -- f64::MIN - 1e308",
        [NegInf]
    );

    db_test!(
        select_shl_large_negative_float,
        [
            "SELECT 1 << -1e19",
            "SELECT 1 << -9223372036854775808;  -- i64::MIN",
            "SELECT 1 << 9223372036854775807;   -- i64::MAX"
        ],
        [0, 0, 0]
    );

    db_test!(
        select_shl_basic,
        [
            "SELECT 1 << 0, 1 << 1, 1 << 2, 1 << 3",
            "SELECT 2 << 0, 2 << 1, 2 << 2, 2 << 3"
        ],
        [[1, 2, 4, 8], [2, 4, 8, 16]]
    );

    // TODO: rust-analyzer bug
    db_test!(
        select_shl_negative_numbers,
        [
            "SELECT -1 << 0, -1 << 1, -1 << 2, -1 << 3",
            "SELECT -2 << 0, -2 << 1, -2 << 2, -2 << 3"
        ],
        [[-1, -2, -4, -8], [-2, -4, -8, -16]]
    );

    // TODO: rust-analyzer bug
    db_test!(
        select_shl_negative_shifts,
        [
            "SELECT 8 << -1, 8 << -2, 8 << -3, 8 << -4",
            "SELECT -8 << -1, -8 << -2, -8 << -3, -8 << -4"
        ],
        [[4, 2, 1, 0], [-4, -2, -1, -1]]
    );

    // TODO: rust-analyzer bug
    db_test!(
        select_shl_large_shifts,
        [
            "SELECT 1 << 62, 1 << 63, 1 << 64",
            "SELECT -1 << 62, -1 << 63, -1 << 64"
        ],
        [
            [4611686018427387904, -9223372036854775808, 0],
            [-4611686018427387904, -9223372036854775808, 0]
        ]
    );

    // TODO: rust-analyzer bug
    db_test!(
        select_shl_text_conversion,
        [
            "SELECT '1' << '2'",
            "SELECT '8' << '-2'",
            "SELECT '-4' << '2'"
        ],
        [4, 2, -16]
    );

    db_test!(
        select_shl_chained,
        ["SELECT (1 << 2) << 3", "SELECT (2 << 1) << (1 << 1)"],
        [32, 16]
    );

    db_test!(
        select_shl_numeric_types,
        [
            "SELECT CAST(1 AS INTEGER) << 2",
            "SELECT 1.0 << 2",
            "SELECT 1.5 << 2"
        ],
        [4, 4, 4]
    );

    // TODO: rust-analyzer bug
    db_test!(
        select_fuzz_failure_case,
        "SELECT (-9 << ((-6) << (9)) >> ((5)) % -10 - + - (-9))",
        -16
    );

    // regression test for https://github.com/tursodatabase/limbo/issues/1157
    db_test!(select_invalid_numeric_text, "select -'e'", 0);

    db_test!(select_invalid_numeric_text_2, "select -'E'", 0);
}
