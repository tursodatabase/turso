#[cfg(test)]
mod tests {
    use crate::db_test;

    // math_expression_fuzz_run failure with seed 1743159584
    db_test!(
        fuzz_test_failure,
        "SELECT mod(atanh(tanh(-1.0)), ((1.0))) / ((asinh(-1.0) / 2.0 * 1.0) + pow(0.0, 1.0) + 0.5)",
        [-16.85965165556754]
    );

    db_test!(add_int_1, "SELECT 10 + 1", [11]);

    db_test!(add_int_2, "SELECT 0xA + 0xFF", [265]);

    db_test!(add_int_3, "SELECT 0xA + 1", [11]);

    db_test!(add_float, "SELECT 10.1 + 0.3", [10.4]);

    db_test!(add_int_float_1, "SELECT 10 + 0.1", [10.1]);

    db_test!(add_int_float_2, "SELECT 0xa + 0.1", [10.1]);

    db_test!(add_agg_int_agg_int_1, "SELECT sum(1) + sum(2)", [3]);

    db_test!(add_agg_int_agg_float_1, "SELECT sum(1) + sum(2.5)", [3.5]);

    db_test!(add_agg_float_agg_int, "SELECT sum(1.5) + sum(2)", [3.5]);

    db_test!(add_text_1, "SELECT 'a' + 'a'", [0]);

    db_test!(add_text_2, "SELECT 'a' + 10", [10]);

    db_test!(add_text_3, "SELECT 10 + 'a'", [10]);

    db_test!(add_text_4, "SELECT 'a' + 11.0", [11.0]);

    db_test!(add_text_5, "SELECT 11.0 + 'a'", [11.0]);

    db_test!(add_text_7, "SELECT '1' + '2'", [3]);

    db_test!(add_text_8, "SELECT '1.0' + '2'", [3.0]);

    db_test!(add_text_9, "SELECT '1.0' + '3.0'", [4.0]);

    db_test!(
        add_overflow_1,
        "SELECT '9223372036854775807' + '0'",
        [9223372036854775807]
    );

    db_test!(
        add_overflow_2,
        "SELECT '9223372036854775807' + '1'",
        [9.223372036854776e18]
    );

    db_test!(
        add_overflow_3,
        "SELECT 9223372036854775807 + 0",
        [9223372036854775807]
    );

    db_test!(
        add_overflow_4,
        "SELECT 9223372036854775807 + 1",
        [9.223372036854776e18]
    );

    db_test!(
        add_overflow_5,
        "SELECT '-9223372036854775808' + '0'",
        [-9223372036854775808]
    );

    db_test!(
        add_overflow_6,
        "SELECT '-9223372036854775808' + '-1'",
        [-9.223372036854776e18]
    );

    db_test!(
        add_overflow_7,
        "SELECT -9223372036854775808 + 0",
        [-9223372036854775808]
    );

    db_test!(
        add_overflow_8,
        "SELECT -9223372036854775808 + -1",
        [-9.223372036854776e18]
    );

    db_test!(subtract_int, "SELECT 10 - 1", [9]);

    db_test!(subtract_float, "SELECT 10.2 - 0.1", [10.1]);

    db_test!(subtract_int_float, "SELECT 10 - 0.1", [9.9]);

    db_test!(subtract_agg_int_agg_int_1, "SELECT sum(3) - sum(1)", [2]);

    db_test!(
        subtract_agg_int_agg_float_1,
        "SELECT sum(3) - sum(1.5)",
        [1.5]
    );

    db_test!(
        subtract_agg_float_agg_int_1,
        "SELECT sum(3.5) - sum(1)",
        [2.5]
    );

    db_test!(subtract_blob, "SELECT -x'11'", [0]);

    db_test!(subtract_blob_empty, "SELECT -x''", [0]);

    db_test!(subtract_blob_charcter, "SELECT -'hi'", [0]);

    db_test!(subtract_text_1, "SELECT 'a' - 'a'", [0]);

    db_test!(subtract_text_2, "SELECT 'a' - 10", [-10]);

    db_test!(subtract_text_3, "SELECT 10 - 'a'", [10]);

    db_test!(subtract_text_4, "SELECT 'a' - 11.0", [-11.0]);

    db_test!(subtract_text_5, "SELECT 11.0 - 'a'", [11.0]);

    db_test!(subtract_text_7, "SELECT '1' - '2'", [-1]);

    db_test!(subtract_text_8, "SELECT '1.0' - '2'", [-1.0]);

    db_test!(subtract_text_9, "SELECT '1.0' - '3.0'", [-2.0]);

    db_test!(
        subtract_overflow_1,
        "SELECT '9223372036854775807' - '0'",
        [9223372036854775807]
    );

    db_test!(
        subtract_overflow_2,
        "SELECT '9223372036854775807' - '-1'",
        [9.223372036854776e18]
    );

    db_test!(
        subtract_overflow_3,
        "SELECT 9223372036854775807 - 0",
        [9223372036854775807]
    );

    db_test!(
        subtract_overflow_4,
        "SELECT 9223372036854775807 - -1",
        [9.223372036854776e18]
    );

    db_test!(
        subtract_overflow_5,
        "SELECT '-9223372036854775808' - '0'",
        [-9223372036854775808]
    );

    db_test!(
        subtract_overflow_6,
        "SELECT '-9223372036854775808' - '1'",
        [-9.223372036854776e18]
    );

    db_test!(
        subtract_overflow_7,
        "SELECT -9223372036854775808 - 0",
        [-9223372036854775808]
    );

    db_test!(
        subtract_overflow_8,
        "SELECT -9223372036854775808 - 1",
        [-9.223372036854776e18]
    );

    db_test!(multiply_int, "SELECT 10 * 2", [20]);

    db_test!(multiply_float, "SELECT 10.2 * 2.2", [22.44]);

    db_test!(multiply_int_float, "SELECT 10 * 1.45", [14.5]);

    db_test!(multiply_float_int, "SELECT 1.45 * 10", [14.5]);

    db_test!(multiply_agg_int_agg_int_1, "SELECT sum(2) * sum(3)", [6]);

    db_test!(
        multiply_agg_int_agg_float_1,
        "SELECT sum(2) * sum(3.5)",
        [7.0]
    );

    db_test!(
        multiply_agg_float_agg_int_1,
        "SELECT sum(2.5) * sum(3)",
        [7.5]
    );

    db_test!(multiply_text_1, "SELECT 'a' * 'a'", [0]);

    db_test!(multiply_text_2, "SELECT 'a' * 10", [0]);

    db_test!(multiply_text_3, "SELECT 10 * 'a'", [0]);

    db_test!(multiply_text_4, "SELECT 'a' * 11.0", [0.0]);

    db_test!(multiply_text_5, "SELECT 11.0 * 'a'", [0.0]);

    db_test!(multiply_text_7, "SELECT '1' * '2'", [2]);

    db_test!(multiply_text_8, "SELECT '1.0' * '2'", [2.0]);

    db_test!(multiply_text_9, "SELECT '1.0' * '3.0'", [3.0]);

    db_test!(
        multiply_overflow_1,
        "SELECT '9223372036854775807' * '1'",
        [9223372036854775807]
    );

    db_test!(
        multiply_overflow_2,
        "SELECT '9223372036854775807' * '2'",
        [1.8446744073709552e19]
    );

    db_test!(
        multiply_overflow_3,
        "SELECT 9223372036854775807 * 1",
        [9223372036854775807]
    );

    db_test!(
        multiply_overflow_4,
        "SELECT 9223372036854775807 * 2",
        [1.8446744073709552e19]
    );

    db_test!(
        multiply_overflow_5,
        "SELECT '-9223372036854775808' * '1'",
        [-9223372036854775808]
    );

    db_test!(
        multiply_overflow_6,
        "SELECT '-9223372036854775808' * '2'",
        [-1.8446744073709552e19]
    );

    db_test!(
        multiply_overflow_7,
        "SELECT -9223372036854775808 * 1",
        [-9223372036854775808]
    );

    db_test!(
        multiply_overflow_8,
        "SELECT -9223372036854775808 * 2",
        [-1.8446744073709552e19]
    );

    db_test!(divide_int, "SELECT 10 / 2", [5]);

    db_test!(divide_int_no_fraction, "SELECT 10 / 3", [3]);

    db_test!(divide_float, "SELECT 10.6 / 2.5", [4.24]);

    db_test!(divide_int_float, "SELECT 10 / 4.0", [2.5]);

    db_test!(divide_float_int, "SELECT 10.0 / 4", [2.5]);

    db_test!(divide_by_zero, "SELECT 10 / 0", [Null]);

    db_test!(divide_int_null, "SELECT 10 / null", [Null]);

    db_test!(divide_null_int, "SELECT null / 10", [Null]);

    db_test!(divide_null, "SELECT null / null", [Null]);

    db_test!(divide_agg_int_agg_int_1, "SELECT sum(4) / sum(2)", [2]);

    db_test!(
        divide_agg_int_agg_float_1,
        "SELECT sum(4) / sum(2.0)",
        [2.0]
    );

    db_test!(
        divide_agg_float_agg_int_1,
        "SELECT sum(4.0) / sum(2)",
        [2.0]
    );

    db_test!(divide_text_1, "SELECT 'a' / 'a'", [Null]);

    db_test!(divide_text_2, "SELECT 'a' / 10", [0]);

    db_test!(divide_text_3, "SELECT 10 / 'a'", [Null]);

    db_test!(divide_text_4, "SELECT 'a' / 11.0", [0.0]);

    db_test!(divide_text_5, "SELECT 11.0 / 'a'", [Null]);

    db_test!(divide_text_7, "SELECT '1' / '2'", [0]);

    db_test!(divide_text_8, "SELECT '1.0' / '2'", [0.5]);

    db_test!(divide_text_9, "SELECT '1.0' / '4.0'", [0.25]);

    db_test!(
        divide_overflow_5,
        "SELECT '-9223372036854775808' / '0'",
        [Null]
    );

    db_test!(
        divide_overflow_6,
        "SELECT '-9223372036854775808' / '-1'",
        [9.223372036854776e18]
    );

    db_test!(divide_overflow_7, "SELECT -9223372036854775808 / 0", [Null]);

    db_test!(
        divide_overflow_8,
        "SELECT -9223372036854775808 / -1",
        [9.223372036854776e18]
    );

    db_test!(add_agg_int, "SELECT sum(id) + 10 FROM products", [76]);

    db_test!(add_int_agg, "SELECT 10 + sum(id) FROM products", [76]);

    db_test!(add_agg_float, "SELECT sum(id) + 10.1 FROM products", [76.1]);

    db_test!(add_float_agg, "SELECT 10.1 + sum(id) FROM products", [76.1]);

    db_test!(
        add_agg_int_agg_int_2,
        "SELECT sum(id) + sum(id) FROM products",
        [132]
    );

    db_test!(
        add_agg_float_agg_float,
        "SELECT sum(price) + sum(price) FROM products",
        [1246.0]
    );

    db_test!(
        add_agg_int_agg_float_2,
        "SELECT sum(id) + sum(price) FROM products",
        [689.0]
    );

    db_test!(
        add_float_text_edgecase,
        "SELECT '-123.22342-24' + '232.3x32'",
        [109.07658]
    );

    db_test!(
        add_str_edgecase,
        "SELECT '-1+23.22342-24' + '2-32.3x32'",
        [1]
    );

    db_test!(subtract_agg_int, "SELECT sum(id) - 10 FROM products", [56]);

    db_test!(subtract_int_agg, "SELECT 10 - sum(id) FROM products", [-56]);

    db_test!(
        subtract_agg_float,
        "SELECT sum(id) - 10.1 FROM products",
        [55.9]
    );

    db_test!(
        subtract_float_agg,
        "SELECT 10.1 - sum(id) FROM products",
        [-55.9]
    );

    db_test!(
        subtract_agg_int_agg_int_2,
        "SELECT sum(id) - sum(id) FROM products",
        [0]
    );

    db_test!(
        subtract_agg_float_agg_float,
        "SELECT sum(price) - sum(price) FROM products",
        [0.0]
    );

    db_test!(
        subtract_agg_int_agg_float_2,
        "SELECT sum(id) - sum(price) FROM products",
        [-557.0]
    );

    db_test!(
        subtract_agg_float_agg_int_2,
        "SELECT sum(price) - sum(id) FROM products",
        [557.0]
    );

    db_test!(
        subtract_str_float_edgecase,
        "SELECT '-123.22342-24' - '232.3x32'",
        [-355.52342]
    );

    db_test!(multiply_agg_int, "SELECT sum(id) * 10 FROM products", [660]);

    db_test!(multiply_int_agg, "SELECT 10 * sum(id) FROM products", [660]);

    db_test!(
        multiply_agg_float,
        "SELECT sum(id) * 10.1 FROM products",
        [666.6]
    );

    db_test!(
        multiply_float_agg,
        "SELECT 10.1 * sum(id) FROM products",
        [666.6]
    );

    db_test!(
        multiply_agg_int_agg_int_2,
        "SELECT sum(id) * sum(id) FROM products",
        [4356]
    );

    db_test!(
        multiply_agg_float_agg_float,
        "SELECT sum(price) * sum(price) FROM products",
        [388129.0]
    );

    db_test!(
        multiply_agg_int_agg_float_2,
        "SELECT sum(id) * sum(price) FROM products",
        [41118.0]
    );

    db_test!(
        multiply_agg_float_agg_int_2,
        "SELECT sum(price) * sum(id) FROM products",
        [41118.0]
    );

    db_test!(
        multiply_str_floats_edgecase,
        "SELECT '-123.22341-24' * '232.3x32'",
        [-28624.798143]
    );

    db_test!(divide_agg_int, "SELECT sum(id) / 10 FROM products", [6]);

    db_test!(divide_int_agg, "SELECT 660 / sum(id) FROM products", [10]);

    db_test!(
        divide_agg_float,
        "SELECT sum(id) / 1.5 FROM products",
        [44.0]
    );

    db_test!(
        divide_float_agg,
        "SELECT 66.0 / sum(id) FROM products",
        [1.0]
    );

    db_test!(
        divide_agg_int_agg_int_2,
        "SELECT sum(id) / sum(id) FROM products",
        [1]
    );

    db_test!(
        divide_agg_float_agg_float,
        "SELECT sum(price) / sum(price) FROM products",
        [1.0]
    );

    db_test!(
        divide_agg_int_agg_float_2,
        "SELECT sum(id) / min(price) FROM products",
        [66.0]
    );

    db_test!(
        divide_agg_float_agg_int_2,
        "SELECT min(price) / min(id) FROM products",
        [1.0]
    );

    db_test!(bitwise_and_int_null, "SELECT 1234 & NULL", [Null]);

    db_test!(bitwise_and_int_int, "SELECT 1234 & 1234", [1234]);

    db_test!(bitwise_and_int_float, "SELECT 660 & 261.8", [4]);

    db_test!(bitwise_and_float_float, "SELECT 660.63 & 261.8", [4]);

    db_test!(bitwise_and_float_int_rev, "SELECT 261.8 & 660", [4]);

    db_test!(
        bitwise_and_int_agg_int,
        "SELECT 8261 & sum(id) FROM products",
        [64]
    );

    db_test!(
        bitwise_and_int_agg_float,
        "SELECT 1036.6 & sum(id) FROM products",
        [0]
    );

    db_test!(
        bitwise_and_int_agg_int_agg_1,
        "SELECT sum(id) & sum(id) FROM products",
        [66]
    );

    db_test!(bitwise_and_text_1, "SELECT 'a' & 'a'", [0]);

    db_test!(bitwise_and_text_2, "SELECT 'a' & 10", [0]);

    db_test!(bitwise_and_text_3, "SELECT 10 & 'a'", [0]);

    db_test!(bitwise_and_text_4, "SELECT 'a' & 11.0", [0]);

    db_test!(bitwise_and_text_5, "SELECT 11.0 & 'a'", [0]);

    db_test!(bitwise_and_text_7, "SELECT '1' & '2'", [0]);

    db_test!(bitwise_and_text_8, "SELECT '1.0' & '2'", [0]);

    db_test!(bitwise_and_text_9, "SELECT '1.0' & '4.0'", [0]);

    db_test!(bitwise_and_text_10, "SELECT '1.0' & '1.0'", [1]);

    db_test!(bitwise_and_text_11, "SELECT '1' & '1'", [1]);

    db_test!(bitwise_or_int_null, "SELECT 1234 | NULL", [Null]);

    db_test!(bitwise_or_null_int, "SELECT NULL | 1234", [Null]);

    db_test!(bitwise_or_int_int, "SELECT 4321 | 1234", [5363]);

    db_test!(bitwise_or_int_float, "SELECT 660 | 1234.0", [1750]);

    db_test!(
        bitwise_or_int_agg,
        "SELECT 18823 | sum(id) FROM products",
        [18887]
    );

    db_test!(bitwise_or_float_float, "SELECT 1234.6 | 5432.2", [5626]);

    db_test!(bitwise_or_text_1, "SELECT 'a' | 'a'", [0]);

    db_test!(bitwise_or_text_2, "SELECT 'a' | 10", [10]);

    db_test!(bitwise_or_text_3, "SELECT 10 | 'a'", [10]);

    db_test!(bitwise_or_text_4, "SELECT 'a' | 11.0", [11]);

    db_test!(bitwise_or_text_5, "SELECT 11.0 | 'a'", [11]);

    db_test!(bitwise_or_text_7, "SELECT '1' | '2'", [3]);

    db_test!(bitwise_or_text_8, "SELECT '1.0' | '2'", [3]);

    db_test!(bitwise_or_text_9, "SELECT '1.0' | '4.0'", [5]);

    db_test!(bitwise_or_text_10, "SELECT '1.0' | '1.0'", [1]);

    db_test!(bitwise_or_text_11, "SELECT '1' | '1'", [1]);

    db_test!(
        bitwise_and_int_agg_int_agg_2,
        "SELECT sum(id) | sum(id) FROM products",
        [66]
    );

    db_test!(shift_left_int_int, "SELECT 1 << 2", [4]);

    db_test!(shift_left_int_neg_int, "SELECT 8 << -2", [2]);

    db_test!(shift_left_int_float, "SELECT 1 << 4.0", [16]);

    db_test!(shift_left_int_text, "SELECT 1 << 'a'", [1]);

    db_test!(shift_left_int_text_float, "SELECT 1 << '3.0'", [8]);

    db_test!(shift_left_int_text_int, "SELECT 1 << '1'", [2]);

    db_test!(shift_left_int_null, "SELECT 1 << NULL", [Null]);

    db_test!(shift_left_int_int_overflow, "SELECT 1 << 64", [0]);

    db_test!(shift_left_int_int_underflow, "SELECT 1 << -64", [0]);

    db_test!(shift_left_int_float_overflow, "SELECT 1 << 64.0", [0]);

    db_test!(shift_left_int_float_underflow, "SELECT 1 << -64.0", [0]);

    db_test!(shift_left_float_int, "SELECT 1.0 << 2", [4]);

    db_test!(shift_left_float_neg_int, "SELECT 8.0 << -2", [2]);

    db_test!(shift_left_float_float, "SELECT 1.0 << 4.0", [16]);

    db_test!(shift_left_float_text, "SELECT 1.0 << 'a'", [1]);

    db_test!(shift_left_float_text_float, "SELECT 1.0 << '3.0'", [8]);

    db_test!(shift_left_float_text_int, "SELECT 1.0 << '1'", [2]);

    db_test!(shift_left_float_null, "SELECT 1.0 << NULL", [Null]);

    db_test!(shift_left_float_int_overflow, "SELECT 1.0 << 64", [0]);

    db_test!(shift_left_float_int_underflow, "SELECT 1.0 << -64", [0]);

    db_test!(shift_left_float_float_overflow, "SELECT 1.0 << 64.0", [0]);

    db_test!(shift_left_float_float_underflow, "SELECT 1.0 << -64.0", [0]);

    db_test!(shift_left_text_int, "SELECT 'a' << 2", [0]);

    db_test!(shift_left_text_float, "SELECT 'a' << 4.0", [0]);

    db_test!(shift_left_text_text, "SELECT 'a' << 'a'", [0]);

    db_test!(shift_left_text_int_text_int, "SELECT '1' << '1'", [2]);

    db_test!(shift_left_text_int_text_float, "SELECT '1' << '3.0'", [8]);

    db_test!(shift_left_text_int_text, "SELECT '1' << 'a'", [1]);

    db_test!(shift_left_text_float_text_int, "SELECT '1.0' << '1'", [2]);

    db_test!(
        shift_left_text_float_text_float,
        "SELECT '1.0' << '3.0'",
        [8]
    );

    db_test!(shift_left_text_float_text, "SELECT '1.0' << 'a'", [1]);

    db_test!(shift_left_text_null, "SELECT '1' << NULL", [Null]);

    db_test!(shift_left_null_int, "SELECT NULL << 2", [Null]);

    db_test!(shift_left_null_float, "SELECT NULL << 4.0", [Null]);

    db_test!(shift_left_null_text, "SELECT NULL << 'a'", [Null]);

    db_test!(shift_left_null_null, "SELECT NULL << NULL", [Null]);

    db_test!(shift_right_int_int, "SELECT 8 >> 2", [2]);

    db_test!(shift_right_int_neg_int, "SELECT 8 >> -2", [32]);

    db_test!(shift_right_int_float, "SELECT 8 >> 1.0", [4]);

    db_test!(shift_right_int_text, "SELECT 8 >> 'a'", [8]);

    db_test!(shift_right_int_text_float, "SELECT 8 >> '3.0'", [1]);

    db_test!(shift_right_int_text_int, "SELECT 8 >> '1'", [4]);

    db_test!(shift_right_int_null, "SELECT 8 >> NULL", [Null]);

    db_test!(shift_right_int_int_overflow, "SELECT 8 >> 64", [0]);

    db_test!(shift_right_int_int_underflow, "SELECT 8 >> -64", [0]);

    db_test!(shift_right_int_float_overflow, "SELECT 8 >> 64.0", [0]);

    db_test!(shift_right_int_float_underflow, "SELECT 8 >> -64.0", [0]);

    db_test!(shift_right_float_int, "SELECT 8.0 >> 2", [2]);

    db_test!(shift_right_float_neg_int, "SELECT 8.0 >> -2", [32]);

    db_test!(shift_right_float_float, "SELECT 8.0 >> 1.0", [4]);

    db_test!(shift_right_float_text, "SELECT 8.0 >> 'a'", [8]);

    db_test!(shift_right_float_text_float, "SELECT 8.0 >> '3.0'", [1]);

    db_test!(shift_right_float_text_int, "SELECT 8.0 >> '1'", [4]);

    db_test!(shift_right_float_null, "SELECT 8.0 >> NULL", [Null]);

    db_test!(shift_right_float_int_overflow, "SELECT 8.0 >> 64", [0]);

    db_test!(shift_right_float_int_underflow, "SELECT 8.0 >> -64", [0]);

    db_test!(shift_right_float_float_overflow, "SELECT 8.0 >> 64.0", [0]);

    db_test!(
        shift_right_float_float_underflow,
        "SELECT 8.0 >> -64.0",
        [0]
    );

    db_test!(shift_right_text_int, "SELECT 'a' >> 2", [0]);

    db_test!(shift_right_text_float, "SELECT 'a' >> 4.0", [0]);

    db_test!(shift_right_text_text, "SELECT 'a' >> 'a'", [0]);

    db_test!(shift_right_text_int_text_int, "SELECT '8' >> '1'", [4]);

    db_test!(shift_right_text_int_text_float, "SELECT '8' >> '3.0'", [1]);

    db_test!(shift_right_text_int_text, "SELECT '8' >> 'a'", [8]);

    db_test!(shift_right_text_float_text_int, "SELECT '8.0' >> '1'", [4]);

    db_test!(
        shift_right_text_float_text_float,
        "SELECT '8.0' >> '3.0'",
        [1]
    );

    db_test!(shift_right_text_float_text, "SELECT '8.0' >> 'a'", [8]);

    db_test!(shift_right_text_int_text_edge, "SELECT '123-23' >> 2", [30]);

    db_test!(
        shift_right_text_signed_text_edge,
        "SELECT '-123' >> '2xi'",
        [-31]
    );

    db_test!(shift_right_text_null, "SELECT '8' >> NULL", [Null]);

    db_test!(shift_right_null_int, "SELECT NULL >> 2", [Null]);

    db_test!(shift_right_null_float, "SELECT NULL >> 4.0", [Null]);

    db_test!(shift_right_null_text, "SELECT NULL >> 'a'", [Null]);

    db_test!(shift_right_null_null, "SELECT NULL >> NULL", [Null]);

    db_test!(bitwise_not_null, "SELECT ~NULL", [Null]);

    db_test!(bitwise_not_int, "SELECT ~1234", [-1235]);

    db_test!(bitwise_not_float, "SELECT ~823.34", [-824]);

    db_test!(bitwise_not_text_float, "SELECT ~'823.34'", [-824]);

    db_test!(bitwise_not_text_int_1, "SELECT ~'1234'", [-1235]);

    db_test!(bitwise_not_text_int_2, "SELECT ~0xA", [-11]);

    db_test!(bitwise_not_scalar_float, "SELECT ~abs(693.9)", [-694]);

    db_test!(bitwise_not_scalar_int, "SELECT ~abs(7566)", [-7567]);

    db_test!(bitwise_not_agg_int, "SELECT ~sum(693)", [-694]);

    db_test!(
        bitwise_not_agg_and_agg,
        "SELECT ~sum(693) & sum(-302)",
        [-958]
    );

    db_test!(bitwise_not_zero, "SELECT ~0", [-1]);

    db_test!(bitwise_not_empty_blob, "SELECT ~x''", [-1]);

    db_test!(bitwise_not_cast_blob, "SELECT ~CAST('af' AS BLOB)", [-1]);

    db_test!(bitwise_not_blob, "SELECT ~x'0000'", [-1]);

    db_test!(bitwise_not_blob_2, "SELECT ~x'0001'", [-1]);

    db_test!(pi, "SELECT pi()", [3.141592653589793]);

    db_test!(acos_int, "SELECT acos(1)", [0.0]);

    db_test!(acos_float, "SELECT acos(-0.5)", [2.0943951023931957]);

    db_test!(acos_str, "SELECT acos('-0.5')", [2.0943951023931957]);

    db_test!(acos_null, "SELECT acos(null)", [Null]);

    db_test!(acosh_int, "SELECT acosh(1)", [0.0]);

    db_test!(acosh_float, "SELECT acosh(1.5)", [0.9624236501192069]);

    db_test!(acosh_str, "SELECT acosh('1.5')", [0.9624236501192069]);

    db_test!(acosh_invalid, "SELECT acosh(0.99)", [Null]);

    db_test!(acosh_null, "SELECT acosh(null)", [Null]);

    db_test!(asin_int, "SELECT asin(1)", [1.5707963267948966]);

    // TODO: Limbo outputs -0.5235987755982989
    // db_test!(asin_float, "SELECT asin(-0.5)", [-0.5235987755982988]);

    // TODO: Limbo outputs -0.5235987755982989
    // db_test!(asin_str, "SELECT asin('-0.5')", [-0.5235987755982988]);

    db_test!(asin_null, "SELECT asin(null)", [Null]);

    db_test!(sin_int, "SELECT sin(1)", [0.8414709848078965]);

    db_test!(sin_float, "SELECT sin(-0.5)", [-0.479425538604203]);

    db_test!(sin_str, "SELECT sin('-0.5')", [-0.479425538604203]);

    db_test!(sin_null, "SELECT sin(null)", [Null]);

    // TODO: Limbo outputs off incorrect result. Instead of [Real(-0.7568024953079282)] it outputs [Real(-0.7568024953079283)]
    // db_test!(
    //     sin_products_id,
    //     "SELECT sin(id) FROM products LIMIT 5",
    //     [
    //         [0.8414709848078965],
    //         [0.9092974268256817],
    //         [0.1411200080598672],
    //         [-0.7568024953079283],
    //         [-0.9589242746631385]
    //     ]
    // );

    db_test!(asinh_int, "SELECT asinh(1)", [0.881373587019543]);

    db_test!(asinh_float, "SELECT asinh(-0.5)", [-0.48121182505960347]);

    db_test!(asinh_str, "SELECT asinh('-0.5')", [-0.48121182505960347]);

    db_test!(asinh_null, "SELECT asinh(null)", [Null]);

    db_test!(atan_int, "SELECT atan(1)", [0.7853981633974483]);

    // TODO: Limbo outputs -0.4636476090008061 instead of -0.46364760900080615
    // db_test!(atan_float, "SELECT atan(-0.5)", [-0.46364760900080615]);

    // TODO: Limbo outputs -0.4636476090008061 instead of -0.46364760900080615
    // db_test!(atan_str, "SELECT atan('-0.5')", [-0.46364760900080615]);

    db_test!(atan_null, "SELECT atan(null)", [Null]);

    // TODO: Limbo incorrect precision. Limbo outputs Real(1.5574077246549023)
    // db_test!(tan_int, "SELECT tan(1)", [1.557407724654902]);

    db_test!(tan_float, "SELECT tan(-0.5)", [-0.5463024898437905]);

    db_test!(tan_str, "SELECT tan('-0.5')", [-0.5463024898437905]);

    db_test!(tan_null, "SELECT tan(null)", [Null]);

    db_test!(atanh_int, "SELECT atanh(0)", [0.0]);

    // TODO: Limbo outputs -0.5493061443340548 instead of -0.5493061443340549
    // db_test!(atanh_float, "SELECT atanh(-0.5)", [-0.5493061443340549]);

    // TODO: Limbo outputs -0.5493061443340548 instead of -0.5493061443340549
    // db_test!(atanh_str, "SELECT atanh('-0.5')", [-0.5493061443340549]);

    db_test!(atanh_null, "SELECT atanh(null)", [Null]);

    db_test!(ceil_int, "SELECT ceil(1)", [1]);

    db_test!(ceil_float, "SELECT ceil(-1.5)", [-1.0]);

    db_test!(ceil_str, "SELECT ceil('1.5')", [2.0]);

    db_test!(ceil_null, "SELECT ceil(null)", [Null]);

    db_test!(ceiling_int, "SELECT ceiling(1)", [1]);

    db_test!(ceiling_float, "SELECT ceiling(-1.5)", [-1.0]);

    db_test!(ceiling_str, "SELECT ceiling('1.5')", [2.0]);

    db_test!(ceiling_null, "SELECT ceiling(null)", [Null]);

    db_test!(cos_int, "SELECT cos(1)", [0.5403023058681398]);

    db_test!(cos_float, "SELECT cos(-0.5)", [0.8775825618903728]);

    db_test!(cos_str, "SELECT cos('-0.5')", [0.8775825618903728]);

    db_test!(cos_null, "SELECT cos(null)", [Null]);

    // TODO: Limbo outputs Real(1.543080634815244) instead of Real(1.5430806348152437)
    // db_test!(cosh_int, "SELECT cosh(1)", [1.5430806348152437]);

    db_test!(cosh_float, "SELECT cosh(-0.5)", [1.1276259652063807]);

    db_test!(cosh_str, "SELECT cosh('-0.5')", [1.1276259652063807]);

    db_test!(cosh_null, "SELECT cosh(null)", [Null]);

    db_test!(degrees_int, "SELECT degrees(1)", [57.29577951308232]);

    db_test!(degrees_float, "SELECT degrees(-0.5)", [-28.64788975654116]);

    db_test!(degrees_str, "SELECT degrees('-0.5')", [-28.64788975654116]);

    db_test!(degrees_null, "SELECT degrees(null)", [Null]);

    // TODO: Limbo outputs Real(2.7182818284590455) instead of Real(2.718281828459045)
    // db_test!(exp_int, "SELECT exp(1)", [2.718281828459045]);

    db_test!(exp_float, "SELECT exp(-0.5)", [0.6065306597126334]);

    db_test!(exp_str, "SELECT exp('-0.5')", [0.6065306597126334]);

    db_test!(exp_null, "SELECT exp(null)", [Null]);

    db_test!(floor_int, "SELECT floor(1)", [1]);

    db_test!(floor_float, "SELECT floor(-1.5)", [-2.0]);

    db_test!(floor_str, "SELECT floor('1.5')", [1.0]);

    db_test!(floor_null, "SELECT floor(null)", [Null]);

    db_test!(ln_int, "SELECT ln(1)", [0.0]);

    db_test!(ln_float, "SELECT ln(0.5)", [-0.6931471805599453]);

    db_test!(ln_str, "SELECT ln('0.5')", [-0.6931471805599453]);

    db_test!(ln_negative, "SELECT ln(-0.5)", [Null]);

    db_test!(ln_null, "SELECT ln(null)", [Null]);

    db_test!(log10_int, "SELECT log10(1)", [0.0]);

    db_test!(log10_float, "SELECT log10(0.5)", [-0.3010299956639812]);

    db_test!(log10_str, "SELECT log10('0.5')", [-0.3010299956639812]);

    db_test!(log10_negative, "SELECT log10(-0.5)", [Null]);

    db_test!(log10_null, "SELECT log10(null)", [Null]);

    db_test!(log2_int, "SELECT log2(1)", [0.0]);

    db_test!(log2_float, "SELECT log2(0.5)", [-1.0]);

    db_test!(log2_str, "SELECT log2('0.5')", [-1.0]);

    db_test!(log2_negative, "SELECT log2(-0.5)", [Null]);

    db_test!(log2_null, "SELECT log2(null)", [Null]);

    db_test!(radians_int, "SELECT radians(1)", [0.017453292519943295]);

    db_test!(
        radians_float,
        "SELECT radians(-0.5)",
        [-0.008726646259971648]
    );

    db_test!(
        radians_str,
        "SELECT radians('-0.5')",
        [-0.008726646259971648]
    );

    db_test!(radians_null, "SELECT radians(null)", [Null]);

    db_test!(sinh_int, "SELECT sinh(1)", [1.1752011936438014]);

    db_test!(sinh_float, "SELECT sinh(-0.5)", [-0.5210953054937474]);

    db_test!(sinh_str, "SELECT sinh('-0.5')", [-0.5210953054937474]);

    db_test!(sinh_null, "SELECT sinh(null)", [Null]);

    db_test!(sqrt_int, "SELECT sqrt(1)", [1.0]);

    db_test!(sqrt_float, "SELECT sqrt(0.5)", [0.7071067811865476]);

    db_test!(sqrt_str, "SELECT sqrt('0.5')", [0.7071067811865476]);

    db_test!(sqrt_negative, "SELECT sqrt(-0.5)", [Null]);

    db_test!(sqrt_null, "SELECT sqrt(null)", [Null]);

    db_test!(tanh_int, "SELECT tanh(1)", [0.7615941559557649]);

    db_test!(tanh_float, "SELECT tanh(-0.5)", [-0.46211715726000974]);

    db_test!(tanh_str, "SELECT tanh('-0.5')", [-0.46211715726000974]);

    db_test!(tanh_null, "SELECT tanh(null)", [Null]);

    db_test!(trunc_int, "SELECT trunc(1)", [1]);

    db_test!(trunc_float, "SELECT trunc(2.5)", [2.0]);

    db_test!(trunc_float_negative, "SELECT trunc(-2.5)", [-2.0]);

    db_test!(trunc_str, "SELECT trunc('2.5')", [2.0]);

    db_test!(trunc_null, "SELECT trunc(null)", [Null]);

    db_test!(atan2_int_int, "SELECT atan2(5, -1)", [1.7681918866447774]);

    db_test!(atan2_int_float, "SELECT atan2(5, -1.5)", [1.8622531212727638]);

    db_test!(atan2_int_str, "SELECT atan2(5, '-1.5')", [1.8622531212727638]);

    // TODO: Limbo outputs 0.5028432109278609
    // db_test!(
    //     atan2_float_int,
    //     "SELECT atan2(5.5, 10)",
    //     [0.5028432109278608]
    // );

    db_test!(
        atan2_float_float,
        "SELECT atan2(5.5, -1.5)",
        [1.837048375945822]
    );

    db_test!(
        atan2_float_str,
        "SELECT atan2(5.5, '-1.5')",
        [1.837048375945822]
    );

    db_test!(
        atan2_str_str,
        "SELECT atan2('5.5', '-1.5')",
        [1.837048375945822]
    );

    db_test!(atan2_null_int, "SELECT atan2(null, 5)", [Null]);

    db_test!(atan2_int_null, "SELECT atan2(5, null)", [Null]);

    db_test!(mod_int_int, "SELECT mod(10, -3)", [1.0]);

    db_test!(mod_int_float, "SELECT mod(5, -1.5)", [0.5]);

    db_test!(mod_int_str, "SELECT mod(5, '-1.5')", [0.5]);

    db_test!(mod_float_int, "SELECT mod(5.5, 2)", [1.5]);

    db_test!(mod_float_float, "SELECT mod(5.5, -1.5)", [1.0]);

    db_test!(mod_float_str, "SELECT mod(5.5, '-1.5')", [1.0]);

    db_test!(mod_str_str, "SELECT mod('5.5', '-1.5')", [1.0]);

    db_test!(mod_null_int, "SELECT mod(null, 5)", [Null]);

    db_test!(mod_int_null, "SELECT mod(5, null)", [Null]);

    db_test!(mod_float_zero, "SELECT mod(1.5, 0)", [Null]);

    db_test!(
        mod_tricky,
        "SELECT mod(atanh(tanh(-1.0)), 1.0)",
        [-0.9999999999999999]
    );

    db_test!(
        mod_products_id,
        "SELECT mod(products.id, 3) FROM products LIMIT 5",
        [[1.0], [2.0], [0.0], [1.0], [2.0]]
    );

    db_test!(
        mod_products_price_id,
        "SELECT mod(products.price, products.id) FROM products LIMIT 5",
        [[0.0], [0.0], [0.0], [1.0], [4.0]]
    );

    db_test!(pow_int_int, "SELECT pow(5, -1)", [0.2]);

    db_test!(pow_int_float, "SELECT pow(5, -1.5)", [0.08944271909999159]);

    db_test!(pow_int_str, "SELECT pow(5, '-1.5')", [0.08944271909999159]);

    db_test!(pow_float_int, "SELECT pow(5.5, 2)", [30.25]);

    db_test!(
        pow_float_float,
        "SELECT pow(5.5, -1.5)",
        [0.07752753322022198]
    );

    db_test!(
        pow_float_str,
        "SELECT pow(5.5, '-1.5')",
        [0.07752753322022198]
    );

    db_test!(
        pow_str_str,
        "SELECT pow('5.5', '-1.5')",
        [0.07752753322022198]
    );

    db_test!(pow_null_int, "SELECT pow(null, 5)", [Null]);

    db_test!(pow_int_null, "SELECT pow(5, null)", [Null]);

    db_test!(power_int_int, "SELECT power(5, -1)", [0.2]);

    db_test!(
        power_int_float,
        "SELECT power(5, -1.5)",
        [0.08944271909999159]
    );

    db_test!(
        power_int_str,
        "SELECT power(5, '-1.5')",
        [0.08944271909999159]
    );

    db_test!(power_float_int, "SELECT power(5.5, 2)", [30.25]);

    db_test!(
        power_float_float,
        "SELECT power(5.5, -1.5)",
        [0.07752753322022198]
    );

    db_test!(
        power_float_str,
        "SELECT power(5.5, '-1.5')",
        [0.07752753322022198]
    );

    db_test!(
        power_str_str,
        "SELECT power('5.5', '-1.5')",
        [0.07752753322022198]
    );

    db_test!(power_null_int, "SELECT power(null, 5)", [Null]);

    db_test!(power_int_null, "SELECT power(5, null)", [Null]);

    db_test!(log_int, "SELECT log(1)", [0.0]);

    db_test!(log_float, "SELECT log(1.5)", [0.17609125905568124]);

    db_test!(log_str, "SELECT log('1.5')", [0.17609125905568124]);

    db_test!(log_negative, "SELECT log(-1.5)", [Null]);

    db_test!(log_null, "SELECT log(null)", [Null]);

    db_test!(log_int_int, "SELECT log(5, 1)", [0.0]);

    db_test!(log_int_float, "SELECT log(5, 1.5)", [0.25192963641259225]);

    db_test!(log_int_str, "SELECT log(5, '1.5')", [0.25192963641259225]);

    db_test!(log_float_int, "SELECT log(5.5, 10)", [1.350689350219849]);

    db_test!(
        log_float_float,
        "SELECT log(5.5, 1.5)",
        [0.23784458827331317]
    );

    db_test!(
        log_float_str,
        "SELECT log(5.5, '1.5')",
        [0.23784458827331317]
    );

    db_test!(
        log_str_str,
        "SELECT log('5.5', '1.5')",
        [0.23784458827331317]
    );

    db_test!(log_negative_negative, "SELECT log(-1.5, -1.5)", [Null]);

    db_test!(log_float_negative, "SELECT log(1.5, -1.5)", [Null]);

    db_test!(log_null_int, "SELECT log(null, 5)", [Null]);

    db_test!(log_int_null, "SELECT log(5, null)", [Null]);

    db_test!(remainder_int_null, "SELECT 183 % null", [Null]);

    db_test!(remainder_int_0, "SELECT 183 % 0", [Null]);

    db_test!(remainder_int_int, "SELECT 183 % 10", [3]);

    db_test!(remainder_int_float, "SELECT 38 % 10.35", [8.0]);

    db_test!(remainder_float_int, "SELECT 38.43 % 13", [12.0]);

    db_test!(remainder_0_float, "SELECT 0 % 12.0", [0.0]);

    db_test!(remainder_float_0, "SELECT 23.14 % 0", [Null]);

    db_test!(remainder_float_float, "SELECT 23.14 % 12.0", [11.0]);

    db_test!(
        remainder_float_agg,
        "SELECT 23.14 % sum(id) FROM products",
        [23.0]
    );

    db_test!(remainder_int_agg, "SELECT 17 % sum(id) FROM users", [17]);

    db_test!(remainder_agg_int, "SELECT count(*) % 17 FROM users", [4]);

    db_test!(
        remainder_agg_float,
        "SELECT count(*) % 2.43 FROM users",
        [0.0]
    );

    db_test!(remainder_text_1, "SELECT 'a' % 'a'", [Null]);

    db_test!(remainder_text_2, "SELECT 'a' % 10", [0]);

    db_test!(remainder_text_3, "SELECT 10 % 'a'", [Null]);

    db_test!(remainder_text_4, "SELECT 'a' % 11.0", [0.0]);

    db_test!(remainder_text_5, "SELECT 11.0 % 'a'", [Null]);

    db_test!(remainder_text_7, "SELECT '10' % '3'", [1]);

    db_test!(remainder_text_8, "SELECT '10.0' % '3'", [1.0]);

    db_test!(remainder_text_9, "SELECT '10.0' % -3", [1.0]);

    db_test!(
        remainder_overflow_1,
        "SELECT '-9223372036854775808' % '-1'",
        [0]
    );

    db_test!(
        remainder_overflow_2,
        "SELECT -9223372036854775808 % -1",
        [0]
    );

    db_test!(
        remainder_overflow_3,
        "SELECT -9223372036854775809 % -1",
        [0.0]
    );

    db_test!(comp_float_float, "SELECT 0.0 = 0.0", [1]);

    db_test!(comp_int_float, "SELECT 0 = 0.0", [1]);

    db_test!(comp_float_int, "SELECT 0.0 = 0", [1]);

    db_test!(comp_int_string, "SELECT 0 = '0'", [0]);

    db_test!(comp_string_int, "SELECT '0' = 0", [0]);

    db_test!(comp_string_blob, "SELECT '0' = CAST('0' AS BLOB)", [0]);

    db_test!(comp_blob_string, "SELECT CAST('0' AS BLOB) = '0'", [0]);

    db_test!(
        comp_blob_blob,
        "SELECT CAST('0' AS BLOB) = CAST('0' AS BLOB)",
        [1]
    );

    db_test!(unary_plus_noop_string, "SELECT +'000'", ["000"]);

    db_test!(unary_plus_noop_blob, "SELECT typeof(+x'00') = 'blob'", [1]);
}
