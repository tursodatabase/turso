#[cfg(test)]
mod tests {
    use crate::db_test;

    db_test!(
        ["testing/testing_small.db"],
        where_is_null,
        "SELECT count(*) FROM demo WHERE value IS NULL",
        2
    );

    db_test!(
        ["testing/testing_small.db"],
        where_equals_null,
        "SELECT count(*) FROM demo WHERE value = NULL",
        0
    );

    db_test!(
        ["testing/testing_small.db"],
        where_is_not_null,
        "SELECT count(*) FROM demo WHERE value IS NOT NULL",
        3
    );

    db_test!(
        ["testing/testing_small.db"],
        where_not_equal_null,
        "SELECT count(*) FROM demo WHERE value != NULL",
        0
    );

    db_test!(
        ["testing/testing_small.db"],
        where_is_a_with_nulls,
        "SELECT count(*) FROM demo WHERE value IS 'A'",
        1
    );

    db_test!(
        ["testing/testing_small.db"],
        where_equals_a_with_nulls,
        "SELECT count(*) FROM demo WHERE value == 'A'",
        1
    );

    db_test!(
        ["testing/testing_small.db"],
        where_is_not_a_with_nulls,
        "SELECT count(*) FROM demo WHERE value IS NOT 'A'",
        4
    );

    db_test!(
        ["testing/testing_small.db"],
        where_not_equals_a_with_nulls,
        "SELECT count(*) FROM demo WHERE value != 'A'",
        2
    );

    db_test!(
        ["testing/testing_small.db"],
        where_is_null_combined,
        "SELECT * FROM demo WHERE value IS NULL OR id = 3",
        [[2, Null], [3, "B"], [4, Null]]
    );

    db_test!(
        ["testing/testing_small.db"],
        where_is_not_null_combined,
        "SELECT * FROM demo WHERE value IS NOT NULL OR id = 2",
        [[1, "A"], [2, Null], [3, "B"], [5, "C"]]
    );

    db_test!(
        where_clause_eq,
        "SELECT last_name FROM users WHERE id = 2000",
        "Rodriguez"
    );

    db_test!(
        where_clause_eq_string,
        "SELECT count(1) FROM users WHERE last_name = 'Rodriguez'",
        61
    );

    db_test!(
        where_clause_isnull,
        "SELECT count(1) FROM users WHERE last_name ISNULL",
        0
    );

    db_test!(
        where_clause_notnull,
        "SELECT count(1) FROM users WHERE last_name NOT NULL",
        10000
    );

    db_test!(
        where_clause_ne,
        "SELECT count(1) FROM users WHERE id != 2000",
        9999
    );

    db_test!(
        where_clause_gt,
        "SELECT count(1) FROM users WHERE id > 2000",
        8000
    );

    db_test!(
        where_clause_gte,
        "SELECT count(1) FROM users WHERE id >= 2000",
        8001
    );

    db_test!(
        where_clause_lt,
        "SELECT count(1) FROM users WHERE id < 2000",
        1999
    );

    db_test!(
        where_clause_lte,
        "SELECT count(1) FROM users WHERE id <= 2000",
        2000
    );

    db_test!(
        where_clause_unary_true,
        "SELECT count(1) FROM users WHERE 1",
        10000
    );

    // not correct? should be 0?
    db_test!(
        where_clause_unary_false,
        "SELECT count(1) FROM users WHERE 0",
        0
    );

    db_test!(
        where_clause_no_table_constant_condition_true,
        "SELECT 1 WHERE 1",
        1
    );

    db_test!(
        where_clause_no_table_constant_condition_identifier_true,
        "SELECT 1 WHERE true",
        1
    );

    db_test!(
        where_clause_no_table_constant_condition_true_2,
        "SELECT 1 WHERE '1'",
        1
    );

    db_test!(
        where_clause_no_table_constant_condition_true_3,
        "SELECT 1 WHERE 6.66",
        1
    );

    db_test!(
        where_clause_no_table_constant_condition_true_4,
        "SELECT 1 WHERE '6.66'",
        1
    );

    db_test!(
        where_clause_no_table_constant_condition_true_5,
        "SELECT 1 WHERE -1",
        1
    );

    db_test!(
        where_clause_no_table_constant_condition_true_6,
        "SELECT 1 WHERE '-1'",
        1
    );

    db_test!(
        where_clause_no_table_constant_condition_false,
        "SELECT 1 WHERE 0"
    );

    db_test!(
        where_clause_no_table_constant_condition_identifier_false,
        "SELECT 1 WHERE false"
    );

    db_test!(
        where_clause_no_table_constant_condition_false_2,
        "SELECT 1 WHERE '0'"
    );

    db_test!(
        where_clause_no_table_constant_condition_false_3,
        "SELECT 1 WHERE 0.0"
    );

    db_test!(
        where_clause_no_table_constant_condition_false_4,
        "SELECT 1 WHERE '0.0'"
    );

    db_test!(
        where_clause_no_table_constant_condition_false_5,
        "SELECT 1 WHERE -0.0"
    );

    db_test!(
        where_clause_no_table_constant_condition_false_6,
        "SELECT 1 WHERE '-0.0'"
    );

    db_test!(
        where_clause_no_table_constant_condition_false_7,
        "SELECT 1 WHERE 'hamburger'"
    );

    // this test functions as an assertion that the index on users.age is being used, since the results are ordered by age without an order by.
    db_test!(
        select_where_and,
        "SELECT first_name, age FROM users WHERE first_name = 'Jamie' AND age > 80",
        [
            ["Jamie", 87],
            ["Jamie", 88],
            ["Jamie", 88],
            ["Jamie", 92],
            ["Jamie", 94],
            ["Jamie", 99]
        ]
    );

    db_test!(
        select_where_or,
        "SELECT first_name, age FROM users WHERE first_name = 'Jamie' AND age > 80",
        [
            ["Jamie", 87],
            ["Jamie", 88],
            ["Jamie", 88],
            ["Jamie", 92],
            ["Jamie", 94],
            ["Jamie", 99]
        ]
    );

    db_test!(
        select_where_and_or,
        "SELECT first_name, age FROM users WHERE first_name = 'Jamie' OR age = 1 AND age = 2",
        [
            ["Jamie", 94],
            ["Jamie", 88],
            ["Jamie", 31],
            ["Jamie", 26],
            ["Jamie", 71],
            ["Jamie", 50],
            ["Jamie", 28],
            ["Jamie", 46],
            ["Jamie", 17],
            ["Jamie", 64],
            ["Jamie", 76],
            ["Jamie", 99],
            ["Jamie", 92],
            ["Jamie", 47],
            ["Jamie", 27],
            ["Jamie", 54],
            ["Jamie", 47],
            ["Jamie", 15],
            ["Jamie", 12],
            ["Jamie", 71],
            ["Jamie", 87],
            ["Jamie", 34],
            ["Jamie", 88],
            ["Jamie", 41],
            ["Jamie", 73]
        ]
    );

    db_test!(
        where_float_int,
        "SELECT * FROM products WHERE price > 50 AND name != 'hat'",
        [
            [2, "cap", 82.0],
            [5, "sweatshirt", 74.0],
            [6, "shorts", 70.0],
            [7, "jeans", 78.0],
            [8, "sneakers", 82.0],
            [11, "accessories", 81.0]
        ]
    );

    db_test!(
        where_multiple_and,
        "SELECT * FROM products WHERE price > 50 AND name != 'sweatshirt' AND price < 75",
        [[6, "shorts", 70.0]]
    );

    db_test!(
        where_multiple_or,
        "SELECT * FROM products WHERE price > 75 OR name = 'shirt' OR name = 'coat'",
        [
            [1, "hat", 79.0],
            [2, "cap", 82.0],
            [3, "shirt", 18.0],
            [7, "jeans", 78.0],
            [8, "sneakers", 82.0],
            [10, "coat", 33.0],
            [11, "accessories", 81.0]
        ]
    );

    db_test!(
        where_in_list,
        "SELECT * FROM products WHERE name IN ('hat', 'sweatshirt', 'shorts')",
        [
            [1, "hat", 79.0],
            [5, "sweatshirt", 74.0],
            [6, "shorts", 70.0]
        ]
    );

    db_test!(
        where_not_in_list,
        "SELECT * FROM products WHERE name NOT IN ('hat', 'sweatshirt', 'shorts')",
        [
            [2, "cap", 82.0],
            [3, "shirt", 18.0],
            [4, "sweater", 25.0],
            [7, "jeans", 78.0],
            [8, "sneakers", 82.0],
            [9, "boots", 1.0],
            [10, "coat", 33.0],
            [11, "accessories", 81.0]
        ]
    );

    db_test!(
        where_in_list_or_another_list,
        "SELECT * FROM products WHERE name IN ('hat', 'sweatshirt', 'shorts') OR price IN (81.0, 82.0)",
        [
            [1, "hat", 79.0], [2, "cap", 82.0], [5, "sweatshirt", 74.0],
            [6, "shorts", 70.0], [8, "sneakers", 82.0], [11, "accessories", 81.0]
        ]
    );

    db_test!(
        where_not_in_list_and_not_in_another_list,
        "SELECT * FROM products WHERE name NOT IN ('hat', 'sweatshirt', 'shorts') AND price NOT IN (81.0, 82.0, 78.0, 1.0, 33.0)",
        [[3, "shirt", 18.0], [4, "sweater", 25.0]]
    );

    db_test!(
        where_in_list_or_not_in_another_list,
        "SELECT * FROM products WHERE name IN ('hat', 'sweatshirt', 'shorts') OR price NOT IN (82.0, 18.0, 78.0, 33.0, 81.0)",
        [
            [1, "hat", 79.0], [4, "sweater", 25.0], [5, "sweatshirt", 74.0],
            [6, "shorts", 70.0], [9, "boots", 1.0]
        ]
    );

    db_test!(
        where_in_empty_list,
        "SELECT * FROM products WHERE name IN ()"
    );

    db_test!(
        where_not_in_empty_list,
        "SELECT * FROM products WHERE name NOT IN ()",
        [
            [1, "hat", 79.0],
            [2, "cap", 82.0],
            [3, "shirt", 18.0],
            [4, "sweater", 25.0],
            [5, "sweatshirt", 74.0],
            [6, "shorts", 70.0],
            [7, "jeans", 78.0],
            [8, "sneakers", 82.0],
            [9, "boots", 1.0],
            [10, "coat", 33.0],
            [11, "accessories", 81.0]
        ]
    );

    db_test!(
        where_name_in_list_and_price_gt_70_or_name_exactly_boots,
        "SELECT * FROM products WHERE name IN ('hat', 'sweatshirt', 'shorts') AND price > 70 OR name = 'boots'",
        [
            [1, "hat", 79.0], [5, "sweatshirt", 74.0], [9, "boots", 1.0]
        ]
    );

    db_test!(
        where_name_in_list_or_price_gt_70_and_name_like_shirt,
        "SELECT * FROM products WHERE name IN ('hat', 'shorts') OR price > 70 AND name LIKE '%shirt%'",
        [
            [1, "hat", 79.0], [5, "sweatshirt", 74.0], [6, "shorts", 70.0]
        ]
    );

    db_test!(
        where_name_not_in_list_or_name_eq_shirt,
        "SELECT * FROM products WHERE name NOT IN ('shirt', 'boots') OR name = 'shirt'",
        [
            [1, "hat", 79.0],
            [2, "cap", 82.0],
            [3, "shirt", 18.0],
            [4, "sweater", 25.0],
            [5, "sweatshirt", 74.0],
            [6, "shorts", 70.0],
            [7, "jeans", 78.0],
            [8, "sneakers", 82.0],
            [10, "coat", 33.0],
            [11, "accessories", 81.0]
        ]
    );

    db_test!(
        where_multiple,
        "SELECT id, first_name, age FROM users WHERE id = 5 AND age < 50",
        [[5, "Edward", 15]]
    );

    db_test!(
        where_multiple_flipped,
        "SELECT id, first_name, age FROM users WHERE age < 50 AND id = 5",
        [[5, "Edward", 15]]
    );

    db_test!(
        where_parentheses_and,
        "SELECT id, name FROM products WHERE (id = 5 AND name = 'sweatshirt') AND (id = 5 AND name = 'sweatshirt') ORDER BY id",
        [[5, "sweatshirt"]]
    );

    db_test!(
        where_nested_parentheses,
        "SELECT id, name FROM products WHERE ((id = 5 AND name = 'sweatshirt') OR (id = 1 AND name = 'hat')) ORDER BY id",
        [[1, "hat"], [5, "sweatshirt"]]
    );

    db_test!(
        where_complex_parentheses,
        "SELECT id, name FROM products WHERE ((id = 5 AND name = 'sweatshirt') OR (id = 1 AND name = 'hat')) AND (name = 'sweatshirt' OR name = 'hat') ORDER BY id",
        [[1, "hat"], [5, "sweatshirt"]]
    );

    // regression test for primary key index behavior
    db_test!(
        where_id_index_seek_regression_test,
        "SELECT id FROM users WHERE id > 9995",
        [[9996], [9997], [9998], [9999], [10000]]
    );

    db_test!(
        where_id_index_seek_regression_test_opposite,
        [
            "SELECT id FROM users WHERE 9999 < id",
            "SELECT id FROM users WHERE 10000 <= id",
            "SELECT id FROM users WHERE 2 > id",
            "SELECT id FROM users WHERE 1 >= id"
        ],
        [[10000], [10000], [1], [1]]
    );

    db_test!(
        where_id_index_seek_regression_test_2,
        "SELECT count(1) FROM users WHERE id > 0",
        10000
    );

    // regression test for secondary index (users.age) behavior
    db_test!(
        where_age_index_seek_regression_test,
        "SELECT age FROM users WHERE age >= 100 LIMIT 20",
        [
            [100],
            [100],
            [100],
            [100],
            [100],
            [100],
            [100],
            [100],
            [100],
            [100],
            [100],
            [100],
            [100],
            [100],
            [100],
            [100],
            [100],
            [100],
            [100],
            [100]
        ]
    );

    db_test!(
        where_age_index_seek_regression_test_2,
        "SELECT count(1) FROM users WHERE age > 0",
        10000
    );

    db_test!(
        where_age_index_seek_regression_test_3,
        "SELECT age FROM users WHERE age > 90 LIMIT 1",
        91
    );

    db_test!(
        where_simple_between,
        "SELECT * FROM products WHERE price BETWEEN 70 AND 100",
        [
            [1, "hat", 79.0],
            [2, "cap", 82.0],
            [5, "sweatshirt", 74.0],
            [6, "shorts", 70.0],
            [7, "jeans", 78.0],
            [8, "sneakers", 82.0],
            [11, "accessories", 81.0]
        ]
    );

    db_test!(
        between_price_range_with_names,
        "SELECT * FROM products WHERE (price BETWEEN 70 AND 100) AND (name = 'sweatshirt' OR name = 'sneakers')",
        [[5, "sweatshirt", 74.0], [8, "sneakers", 82.0]]
    );

    db_test!(
        where_between_true_and_2,
        "SELECT id FROM users WHERE id BETWEEN true AND 2",
        [[1], [2]]
    );

    db_test!(
        nested_parens_conditionals_or_and_or,
        "SELECT count(*) FROM users WHERE ((age > 25 OR age < 18) AND (city = 'Boston' OR state = 'MA'))",
        146
    );

    db_test!(
        nested_parens_conditionals_and_or_and,
        "SELECT * FROM users WHERE (((age > 18 AND city = 'New Mario') OR age = 92) AND city = 'Lake Paul')",
        [[9989, "Timothy", "Harrison", "woodsmichael@example.net", "+1-447-830-5123", "782 Wright Harbors", "Lake Paul", "ID", "52330", 92]]
    );

    db_test!(
        nested_parens_conditionals_and_double_or,
        "SELECT * FROM users WHERE ((age > 30 OR age < 20) AND (state = 'NY' OR state = 'CA')) AND first_name GLOB 'An*' ORDER BY id",
        [
            [1738, "Angelica", "Pena", "jacksonjonathan@example.net", "(867)536-1578x039", "663 Jacqueline Estate Apt. 652", "Clairehaven", "NY", "64172", 74],
            [1811, "Andrew", "Mckee", "jchen@example.net", "359.939.9548", "19809 Blair Junction Apt. 438", "New Lawrencefort", "NY", "26240", 42],
            [3773, "Andrew", "Peterson", "cscott@example.com", "(405)410-4972x90408", "90513 Munoz Radial Apt. 786", "Travisfurt", "CA", "52951", 43],
            [3875, "Anthony", "Cordova", "ocross@example.org", "+1-356-999-4070x557", "77081 Aguilar Turnpike", "Michaelfurt", "CA", "73353", 37],
            [4909, "Andrew", "Carson", "michelle31@example.net", "823.423.1516", "78514 Luke Springs", "Lake Crystal", "CA", "49481", 74],
            [5498, "Anna", "Hall", "elizabethheath@example.org", "9778473725", "5803 Taylor Tunnel", "New Nicholaston", "NY", "21825", 14],
            [6340, "Angela", "Freeman", "juankelly@example.net", "501.372.4720", "3912 Ricardo Mission", "West Nancyville", "NY", "60823", 34],
            [8171, "Andrea", "Lee", "dgarrison@example.com", "001-594-430-0646", "452 Anthony Stravenue", "Sandraville", "CA", "28572", 12],
            [9110, "Anthony", "Barrett", "steven05@example.net", "(562)928-9177x8454", "86166 Foster Inlet Apt. 284", "North Jeffreyburgh", "CA", "80147", 97],
            [9279, "Annette", "Lynn", "joanne37@example.com", "(272)700-7181", "2676 Laura Points Apt. 683", "Tristanville", "NY", "48646", 91]
        ]
    );

    // Regression test for nested parens + OR + AND. This returned 0 rows before the fix.
    // It should always return 1 row because it is true for id = 6.
    db_test!(
        nested_parens_and_inside_or_regression_test,
        "SELECT count(1) FROM users WHERE (((id != 5 AND (id = 5 OR TRUE)) OR FALSE) AND (id = 6 OR FALSE))",
        1
    );

    // Binary operator tests for '='
    db_test!(
        where_binary_one_operand_null_eq,
        "SELECT * FROM users WHERE first_name = NULL"
    );

    db_test!(
        where_binary_one_operand_null_or_eq,
        "SELECT first_name FROM users WHERE first_name = NULL OR id = 1",
        "Jamie"
    );

    db_test!(
        where_binary_one_operand_null_and_eq,
        "SELECT first_name FROM users WHERE first_name = NULL AND id = 1"
    );

    // Binary operator tests for '>'
    db_test!(
        where_binary_one_operand_null_gt,
        "SELECT * FROM users WHERE first_name > NULL"
    );

    db_test!(
        where_binary_one_operand_null_or_gt,
        "SELECT first_name FROM users WHERE first_name > NULL OR id = 1",
        "Jamie"
    );

    db_test!(
        where_binary_one_operand_null_and_gt,
        "SELECT first_name FROM users WHERE first_name > NULL AND id = 1"
    );

    // Binary operator tests for '<'
    db_test!(
        where_binary_one_operand_null_lt,
        "SELECT * FROM users WHERE first_name < NULL"
    );

    db_test!(
        where_binary_one_operand_null_or_lt,
        "SELECT first_name FROM users WHERE first_name < NULL OR id = 1",
        "Jamie"
    );

    db_test!(
        where_binary_one_operand_null_and_lt,
        "SELECT first_name FROM users WHERE first_name < NULL AND id = 1"
    );

    // Binary operator tests for '>='
    db_test!(
        where_binary_one_operand_null_gte,
        "SELECT * FROM users WHERE first_name >= NULL"
    );

    db_test!(
        where_binary_one_operand_null_or_gte,
        "SELECT first_name FROM users WHERE first_name >= NULL OR id = 1",
        "Jamie"
    );

    db_test!(
        where_binary_one_operand_null_and_gte,
        "SELECT first_name FROM users WHERE first_name >= NULL AND id = 1"
    );

    // Binary operator tests for '<='
    db_test!(
        where_binary_one_operand_null_lte,
        "SELECT * FROM users WHERE first_name <= NULL"
    );

    db_test!(
        where_binary_one_operand_null_or_lte,
        "SELECT first_name FROM users WHERE first_name <= NULL OR id = 1",
        "Jamie"
    );

    db_test!(
        where_binary_one_operand_null_and_lte,
        "SELECT first_name FROM users WHERE first_name <= NULL AND id = 1"
    );

    // Binary operator tests for '!='
    db_test!(
        where_binary_one_operand_null_ne,
        "SELECT * FROM users WHERE first_name != NULL"
    );

    db_test!(
        where_binary_one_operand_null_or_ne,
        "SELECT first_name FROM users WHERE first_name != NULL OR id = 1",
        "Jamie"
    );

    db_test!(
        where_binary_one_operand_null_and_ne,
        "SELECT first_name FROM users WHERE first_name != NULL AND id = 1"
    );

    db_test!(
        where_literal_string,
        "SELECT count(*) FROM users WHERE 'yes'",
        0
    );

    // FIXME: should return 0
    // db_test!(
    //     where_literal_number,
    //     "SELECT count(*) FROM users WHERE x'DEADBEEF'",
    //     0
    // );

    db_test!(
        where_cast_string_to_int,
        "SELECT count(*) FROM users WHERE CAST('1' AS INTEGER)",
        10000
    );

    db_test!(
        where_cast_float_to_int,
        "SELECT count(*) FROM users WHERE CAST('0' AS INTEGER)",
        0
    );

    db_test!(
        where_function_length,
        "SELECT count(*) FROM users WHERE length(first_name)",
        10000
    );

    db_test!(
        where_case_simple,
        "SELECT count(*) FROM users WHERE CASE WHEN age > 0 THEN 1 ELSE 0 END",
        10000
    );

    db_test!(
        where_case_searched,
        "SELECT count(*) FROM users WHERE CASE age WHEN 0 THEN 0 ELSE 1 END",
        10000
    );

    db_test!(
        where_unary_not,
        "SELECT count(*) FROM users WHERE NOT (id = 1)",
        9999
    );

    db_test!(
        where_unary_plus,
        "SELECT count(*) FROM users WHERE +1",
        10000
    );

    db_test!(
        where_unary_minus,
        "SELECT count(*) FROM users WHERE -1",
        10000
    );

    db_test!(
        where_unary_bitnot,
        "SELECT count(*) FROM users WHERE ~1",
        10000
    );

    db_test!(
        where_binary_add,
        "SELECT count(*) FROM users WHERE 1 + 1",
        10000
    );

    db_test!(
        where_binary_subtract,
        "SELECT count(*) FROM users WHERE 2 - 1",
        10000
    );

    db_test!(
        where_binary_multiply,
        "SELECT count(*) FROM users WHERE 2 * 1",
        10000
    );

    db_test!(
        where_binary_divide,
        "SELECT count(*) FROM users WHERE 2 / 2",
        10000
    );

    db_test!(
        where_binary_modulo,
        "SELECT count(*) FROM users WHERE 3 % 2",
        10000
    );

    db_test!(
        where_binary_shift_left,
        "SELECT count(*) FROM users WHERE 1 << 1",
        10000
    );

    db_test!(
        where_binary_shift_right,
        "SELECT count(*) FROM users WHERE 2 >> 1",
        10000
    );

    db_test!(
        where_binary_bitwise_and,
        "SELECT count(*) FROM users WHERE 3 & 1",
        10000
    );

    db_test!(
        where_binary_bitwise_or,
        "SELECT count(*) FROM users WHERE 2 | 1",
        10000
    );

    db_test!(
        where_constant_condition_no_tables,
        "SELECT 1 WHERE 1 IS NULL"
    );

    db_test!(
        where_constant_condition_no_tables_2,
        "SELECT 1 WHERE 1 IS NOT NULL",
        1
    );

    // We had a bug where NULL was incorrectly used as a seek key, returning all rows (because NULL < everything in index keys)
    db_test!(
        where_null_comparison_index_seek_regression_test,
        "SELECT age FROM users WHERE age > NULL"
    );

    // We had a bug where Limbo tried to use an index when there was a WHERE term like 't.x = t.x'
    db_test!(
        where_self_referential_regression,
        "SELECT count(1) FROM users WHERE id = id",
        10000
    );
}
