#[cfg(test)]
mod tests {
    use crate::db_test;

    db_test!(
        glob_fn,
        "select name, glob('sweat*', name) from products;",
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
        where_glob,
        "select * from products where name glob 'sweat*';",
        [[4, "sweater", 25.0], [5, "sweatshirt", 74.0]]
    );

    db_test!(
        where_glob_question_mark,
        "select * from products where name glob 'sweat?r';",
        [4, "sweater", 25.0]
    );

    db_test!(
        where_glob_fn,
        "select * from products where glob('sweat*', name)=1",
        [[4, "sweater", 25.0], [5, "sweatshirt", 74.0]]
    );

    db_test!(
        where_not_glob_and,
        "select * from products where name not glob 'sweat*' and price >= 70.0;",
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
        where_glob_or,
        "select * from products where name glob 'sweat*' or price >= 80.0;",
        [
            [2, "cap", 82.0],
            [4, "sweater", 25.0],
            [5, "sweatshirt", 74.0],
            [8, "sneakers", 82.0],
            [11, "accessories", 81.0]
        ]
    );

    db_test!(
        where_glob_another_column,
        "select first_name, last_name from users where last_name glob first_name;",
        [
            ["James", "James"],
            ["Daniel", "Daniel"],
            ["Taylor", "Taylor"]
        ]
    );

    db_test!(
        where_glob_another_column_prefix,
        "select first_name, last_name from users where last_name glob concat(first_name, '*');",
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
        where_glob_impossible,
        "select * from products where 'foobar' glob 'fooba';"
    );

    db_test!(glob_1_1, "SELECT glob ( 'abcdefg' , 'abcdefg' )", 1);
    db_test!(glob_2_1, "SELECT glob ( 'abcdefG' , 'abcdefg' )", 0);
    db_test!(glob_3_1, "SELECT glob ( 'abcdef' , 'abcdefg' )", 0);
    db_test!(glob_4_1, "SELECT glob ( 'abcdefgh' , 'abcdefg' )", 0);
    db_test!(glob_5_1, "SELECT glob ( 'abcdef?' , 'abcdefg' )", 1);
    db_test!(glob_6_1, "SELECT glob ( 'abcdef?' , 'abcdef' )", 0);
    db_test!(glob_7_1, "SELECT glob ( 'abcdef?' , 'abcdefgh' )", 0);
    db_test!(glob_8_1, "SELECT glob ( 'abcdefg' , 'abcdef?' )", 0);
    db_test!(glob_9_1, "SELECT glob ( 'abcdef?' , 'abcdef?' )", 1);
    db_test!(glob_10_1, "SELECT glob ( 'abc/def' , 'abc/def' )", 1);
    db_test!(glob_11_1, "SELECT glob ( 'abc//def' , 'abc/def' )", 0);
    db_test!(glob_12_1, "SELECT glob ( '*/abc/*' , 'x/abc/y' )", 1);
    db_test!(glob_13_1, "SELECT glob ( '*/abc/*' , '/abc/' )", 1);
    db_test!(glob_16_1, "SELECT glob ( '*/abc/*' , 'x///a/ab/abc' )", 0);
    db_test!(glob_17_1, "SELECT glob ( '*/abc/*' , 'x//a/ab/abc/' )", 1);
    db_test!(glob_16_1_2, "SELECT glob ( '*/abc/*' , 'x///a/ab/abc' )", 0);
    db_test!(glob_17_1_2, "SELECT glob ( '*/abc/*' , 'x//a/ab/abc/' )", 1);
    db_test!(glob_18_1, "SELECT glob ( '**/abc/**' , 'x//a/ab/abc/' )", 1);
    db_test!(
        glob_19_1,
        "SELECT glob ( '*?/abc/*?' , 'x//a/ab/abc/y' )",
        1
    );
    db_test!(
        glob_20_1,
        "SELECT glob ( '?*/abc/?*' , 'x//a/ab/abc/y' )",
        1
    );
    db_test!(glob_21_1, "SELECT glob ( 'abc[cde]efg' , 'abcbefg' )", 0);
    db_test!(glob_22_1, "SELECT glob ( 'abc[cde]efg' , 'abccefg' )", 1);
    db_test!(glob_23_1, "SELECT glob ( 'abc[cde]efg' , 'abcdefg' )", 1);
    db_test!(glob_24_1, "SELECT glob ( 'abc[cde]efg' , 'abceefg' )", 1);
    db_test!(glob_25_1, "SELECT glob ( 'abc[cde]efg' , 'abcfefg' )", 0);
    db_test!(glob_26_1, "SELECT glob ( 'abc[^cde]efg' , 'abcbefg' )", 1);
    db_test!(glob_27_1, "SELECT glob ( 'abc[^cde]efg' , 'abccefg' )", 0);
    db_test!(glob_28_1, "SELECT glob ( 'abc[^cde]efg' , 'abcdefg' )", 0);
    db_test!(glob_29_1, "SELECT glob ( 'abc[^cde]efg' , 'abceefg' )", 0);
    db_test!(glob_30_1, "SELECT glob ( 'abc[^cde]efg' , 'abcfefg' )", 1);
    db_test!(glob_31_1, "SELECT glob ( 'abc[c-e]efg' , 'abcbefg' )", 0);
    db_test!(glob_32_1, "SELECT glob ( 'abc[c-e]efg' , 'abccefg' )", 1);
    db_test!(glob_33_1, "SELECT glob ( 'abc[c-e]efg' , 'abcdefg' )", 1);
    db_test!(glob_34_1, "SELECT glob ( 'abc[c-e]efg' , 'abceefg' )", 1);
    db_test!(glob_35_1, "SELECT glob ( 'abc[c-e]efg' , 'abcfefg' )", 0);
    db_test!(glob_36_1, "SELECT glob ( 'abc[^c-e]efg' , 'abcbefg' )", 1);
    db_test!(glob_37_1, "SELECT glob ( 'abc[^c-e]efg' , 'abccefg' )", 0);
    db_test!(glob_38_1, "SELECT glob ( 'abc[^c-e]efg' , 'abcdefg' )", 0);
    db_test!(glob_39_1, "SELECT glob ( 'abc[^c-e]efg' , 'abceefg' )", 0);
    db_test!(glob_40_1, "SELECT glob ( 'abc[^c-e]efg' , 'abcfefg' )", 1);
    db_test!(glob_41_1, "SELECT glob ( 'abc[c-e]efg' , 'abc-efg' )", 0);
    db_test!(glob_42_1, "SELECT glob ( 'abc[-ce]efg' , 'abc-efg' )", 1);
    db_test!(glob_43_1, "SELECT glob ( 'abc[ce-]efg' , 'abc-efg' )", 1);
    db_test!(glob_44_1, "SELECT glob ( 'abc[][*?]efg' , 'abc]efg' )", 1);
    db_test!(glob_45_1, "SELECT glob ( 'abc[][*?]efg' , 'abc*efg' )", 1);
    db_test!(glob_46_1, "SELECT glob ( 'abc[][*?]efg' , 'abc?efg' )", 1);
    db_test!(glob_47_1, "SELECT glob ( 'abc[][*?]efg' , 'abc[efg' )", 1);
    db_test!(glob_48_1, "SELECT glob ( 'abc[^][*?]efg' , 'abc]efg' )", 0);
    db_test!(glob_49_1, "SELECT glob ( 'abc[^][*?]efg' , 'abc*efg' )", 0);
    db_test!(glob_50_1, "SELECT glob ( 'abc[^][*?]efg' , 'abc?efg' )", 0);
    db_test!(glob_51_1, "SELECT glob ( 'abc[^][*?]efg' , 'abc[efg' )", 0);
    db_test!(glob_52_1, "SELECT glob ( 'abc[^][*?]efg' , 'abcdefg' )", 1);
    db_test!(glob_53_1, "SELECT glob ( '*[xyz]efg' , 'abcxefg' )", 1);
    db_test!(glob_54_1, "SELECT glob ( '*[xyz]efg' , 'abcwefg' )", 0);
    db_test!(glob_55_1, "SELECT glob ( '[-c]' , 'c' )", 1);
    db_test!(glob_56_1, "SELECT glob ( '[-c]' , '-' )", 1);
    db_test!(glob_57_1, "SELECT glob ( '[-c]' , 'x' )", 0);

    db_test!(glob_unenclosed_1_1, "SELECT glob ( 'abc[' , 'abc[' )", 0);
    db_test!(glob_unenclosed_2_1, "SELECT glob ( 'abc[' , 'abc' )", 0);
    db_test!(glob_unenclosed_3_1, "SELECT glob ( 'a]b' , 'a]b' )", 1);
    db_test!(glob_unenclosed_4_1, "SELECT glob ( 'a]b' , 'a[b' )", 0);
}
