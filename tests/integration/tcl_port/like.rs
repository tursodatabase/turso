#[cfg(test)]
mod tests {
    use crate::db_test;

    db_test!(
        like_fn,
        "SELECT name, like('sweat%', name) FROM products",
        [
            ["hat", 0],
            ["cap", 0],
            ["shirt", 0],
            ["sweater", 1],
            ["sweatshirt", 1],
            ["shorts", 0],
            ["jeans", 0],
            ["sneakers", 0],
            ["boots", 0],
            ["coat", 0],
            ["accessories", 0]
        ]
    );

    db_test!(
        where_like,
        "SELECT * FROM products WHERE name LIKE 'sweat%'",
        [[4, "sweater", 25.0], [5, "sweatshirt", 74.0]]
    );

    db_test!(
        where_like_case_insensitive,
        "SELECT * FROM products WHERE name LIKE 'SWEAT%'",
        [[4, "sweater", 25.0], [5, "sweatshirt", 74.0]]
    );

    db_test!(
        where_like_underscore,
        "SELECT * FROM products WHERE name LIKE 'sweat_r'",
        [[4, "sweater", 25.0]]
    );

    db_test!(
        where_like_underscore_case_insensitive,
        "SELECT * FROM products WHERE name LIKE 'SwEaT_R'",
        [[4, "sweater", 25.0]]
    );

    db_test!(
        where_like_fn,
        "SELECT * FROM products WHERE like('sweat%', name)=1",
        [[4, "sweater", 25.0], [5, "sweatshirt", 74.0]]
    );

    db_test!(
        where_not_like_and,
        "SELECT * FROM products WHERE name NOT LIKE 'sweat%' AND price >= 70.0",
        [
            [1, "hat", 79.0],
            [2, "cap", 82.0],
            [6, "shorts", 70.0],
            [7, "jeans", 78.0],
            [8, "sneakers", 82.0],
            [11, "accessories", 81.0]
        ]
    );

    db_test!(
        where_like_or,
        "SELECT * FROM products WHERE name LIKE 'sweat%' OR price >= 80.0",
        [
            [2, "cap", 82.0],
            [4, "sweater", 25.0],
            [5, "sweatshirt", 74.0],
            [8, "sneakers", 82.0],
            [11, "accessories", 81.0]
        ]
    );

    db_test!(
        where_like_another_column,
        "SELECT first_name, last_name FROM users WHERE last_name LIKE first_name",
        [
            ["James", "James"],
            ["Daniel", "Daniel"],
            ["Taylor", "Taylor"]
        ]
    );

    db_test!(
        where_like_another_column_prefix,
        "SELECT first_name, last_name FROM users WHERE last_name LIKE concat(first_name, '%')",
        [
            ["James", "James"],
            ["Daniel", "Daniel"],
            ["William", "Williams"],
            ["John", "Johnson"],
            ["Taylor", "Taylor"],
            ["John", "Johnson"],
            ["Stephen", "Stephens"],
            ["Robert", "Roberts"]
        ]
    );

    db_test!(
        where_like_another_column_prefix_2,
        "SELECT count(*) FROM users WHERE last_name LIKE 'Pe#rry' ESCAPE '#'",
        [19]
    );

    db_test!(
        where_like_impossible,
        "SELECT * FROM products WHERE 'foobar' LIKE 'fooba'"
    );

    db_test!(like_with_backslash, "SELECT like('\\\\%A', '\\\\A')", [1]);

    db_test!(like_with_dollar, "SELECT like('A$%', 'A$')", [1]);

    db_test!(like_with_dot, "SELECT like('%a.a', 'aaaa')", [0]);

    db_test!(like_fn_esc_1, "SELECT like('abcX%', 'abc%', 'X')", [1]);

    db_test!(like_fn_esc_2, "SELECT like('abcX%', 'abc5', 'X')", [0]);

    db_test!(like_fn_esc_3, "SELECT like('abcX%', 'abc', 'X')", [0]);

    db_test!(like_fn_esc_4, "SELECT like('abcX%', 'abcX%', 'X')", [0]);

    db_test!(like_fn_esc_5, "SELECT like('abcX%', 'abc%%', 'X')", [0]);

    db_test!(like_fn_esc_6, "SELECT like('abcX_', 'abc_', 'X')", [1]);

    db_test!(like_fn_esc_7, "SELECT like('abcX_', 'abc5', 'X')", [0]);

    db_test!(like_fn_esc_8, "SELECT like('abcX_', 'abc', 'X')", [0]);

    db_test!(like_fn_esc_9, "SELECT like('abcX_', 'abcX_', 'X')", [0]);

    db_test!(like_fn_esc_10, "SELECT like('abcX_', 'abc__', 'X')", [0]);

    db_test!(like_fn_esc_11, "SELECT like('abcXX', 'abcX', 'X')", [1]);

    db_test!(like_fn_esc_12, "SELECT like('abcXX', 'abc5', 'X')", [0]);

    db_test!(like_fn_esc_13, "SELECT like('abcXX', 'abc', 'X')", [0]);

    db_test!(like_fn_esc_14, "SELECT like('abcXX', 'abcXX', 'X')", [0]);
}
