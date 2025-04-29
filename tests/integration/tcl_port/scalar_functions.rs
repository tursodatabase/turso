#[cfg(test)]
mod tests {
    use crate::db_test;

    db_test!(concat_chars, "SELECT concat('l', 'i')", "li");

    db_test!(concat_char_and_number, "SELECT concat('l', 1)", "l1");

    db_test!(concat_char_and_decimal, "SELECT concat('l', 1.5)", "l1.5");

    db_test!(concat_char_null_char, "SELECT concat('l', null, 'i')", "li");

    db_test!(concat_ws_numbers, "SELECT concat_ws(',', 1, 2)", "1,2");

    db_test!(concat_ws_single_number, "SELECT concat_ws(',', 1)", "1");

    db_test!(concat_ws_null, "SELECT concat_ws(null, 1, 2)", [Null]);

    db_test!(
        concat_ws_multiple,
        "SELECT concat_ws(',', 1, 2), concat_ws(',', 3, 4)",
        ["1,2", "3,4"]
    );

    db_test!(char, "SELECT char(108, 105)", "li");

    db_test!(char_nested, "SELECT char(106 + 2, 105)", "li");

    db_test!(char_empty, "SELECT char()", "");

    // TODO: Limbo ouputs Text("")
    // db_test!(char_null, "SELECT char(null)", "\0");

    // TODO: Limbo ouputs Text("")
    // db_test!(char_non_integer, "SELECT char('a')", "\0");

    db_test!(abs, "SELECT abs(1)", 1);

    db_test!(abs_negative, "SELECT abs(-1)", 1);

    db_test!(abs_char, "SELECT abs('a')", 0.0);

    db_test!(abs_null, "SELECT abs(null)", [Null]);

    db_test!(ifnull_1, "SELECT ifnull(1, 2)", 1);

    db_test!(ifnull_2, "SELECT ifnull(null, 2)", 2);

    db_test!(iif_true, "SELECT iif(1, 'pass', 'fail')", "pass");

    db_test!(iif_false, "SELECT iif(0, 'fail', 'pass')", "pass");

    db_test!(instr_str, "SELECT instr('limbo', 'im')", 2);

    db_test!(instr_str_not_found, "SELECT instr('limbo', 'xyz')", 0);

    db_test!(instr_blob_1, "SELECT instr(x'000102', x'01')", 2);

    db_test!(instr_blob_not_found_1, "SELECT instr(x'000102', x'10')", 0);

    db_test!(instr_null, "SELECT instr(null, 'limbo')", [Null]);

    db_test!(instr_integer, "SELECT instr(123, 2)", 2);

    db_test!(instr_integer_not_found, "SELECT instr(123, 5)", 0);

    db_test!(instr_integer_leading_zeros, "SELECT instr(0001, 0)", 0);

    db_test!(instr_real, "SELECT instr(12.34, 2.3)", 2);

    db_test!(instr_real_not_found, "SELECT instr(12.34, 5)", 0);

    db_test!(instr_real_trailing_zeros, "SELECT instr(1.10000, 0)", 0);

    db_test!(instr_blob_2, "SELECT instr(x'01020304', x'02')", 2);

    db_test!(
        instr_blob_not_found_2,
        "SELECT instr(x'01020304', x'05')",
        0
    );

    db_test!(upper, "SELECT upper('Limbo')", "LIMBO");

    // TODO: Limbo outputs Integer(1)
    // db_test!(upper_number, "SELECT upper(1)", "1");

    db_test!(upper_char, "SELECT upper('a')", "A");

    db_test!(upper_null, "SELECT upper(null)", [Null]);

    db_test!(lower, "SELECT lower('Limbo')", "limbo");

    // TODO: Limbo outputs Integer(1)
    // db_test!(lower_number, "SELECT lower(1)", "1");

    db_test!(lower_char, "SELECT lower('A')", "a");

    db_test!(lower_null, "SELECT lower(null)", [Null]);

    db_test!(
        replace,
        "SELECT replace('test', 'test', 'example')",
        "example"
    );

    db_test!(replace_number, "SELECT replace('tes3', 3, 0.3)", "tes0.3");

    db_test!(
        replace_null,
        "SELECT replace('test', null, 'example')",
        [Null]
    );

    db_test!(hex, "SELECT hex('limbo')", "6C696D626F");

    db_test!(hex_number, "SELECT hex(100)", "313030");

    // TODO: Limbo output Null
    // db_test!(hex_null, "SELECT hex(null)", "");

    db_test!(likely, "SELECT likely('limbo')", "limbo");

    db_test!(likely_int, "SELECT likely(100)", 100);

    db_test!(likely_decimal, "SELECT likely(12.34)", 12.34);

    db_test!(likely_null, "SELECT likely(NULL)", [Null]);

    db_test!(
        likelihood_string,
        "SELECT likelihood('limbo', 0.5)",
        "limbo"
    );

    db_test!(
        likelihood_string_high_probability,
        "SELECT likelihood('database', 0.9375)",
        "database"
    );

    db_test!(likelihood_integer, "SELECT likelihood(100, 0.0625)", 100);

    db_test!(
        likelihood_integer_probability_1,
        "SELECT likelihood(42, 1.0)",
        42
    );

    db_test!(likelihood_decimal, "SELECT likelihood(12.34, 0.5)", 12.34);

    db_test!(likelihood_null, "SELECT likelihood(NULL, 0.5)", [Null]);

    db_test!(
        likelihood_blob,
        "SELECT hex(likelihood(x'01020304', 0.5))",
        "01020304"
    );

    db_test!(
        likelihood_zero_probability,
        "SELECT likelihood(999, 0.0)",
        999
    );

    db_test!(unhex_str_ab, "SELECT unhex('6162')", b"6162");

    db_test!(unhex_int_ab, "SELECT unhex(6162)", b"6162");

    // Had to convert "." to its blob representation
    db_test!(unhex_dot_uppercase, "SELECT unhex('2E')", b"2E");

    // Had to convert "." to its blob representation
    db_test!(unhex_dot_lowercase, "SELECT unhex('2e')", b"2E");

    db_test!(unhex_no_hex, "SELECT unhex('x')", [Null]);

    db_test!(unhex_null, "SELECT unhex(NULL)", [Null]);

    // Had to convert "." to its blob representation
    db_test!(unhex_x_y_prefix, "SELECT unhex('x2E', 'x')", b"2E");

    // Had to convert "." to its blob representation
    db_test!(unhex_x_y_suffix, "SELECT unhex('2Ex', 'x')", b"2E");

    // Had to convert "." to its blob representation
    db_test!(unhex_x_y_prefix_suffix, "SELECT unhex('x2Ex', 'x')", b"2E");

    db_test!(
        unhex_x_y_incorrect_suffix,
        "SELECT unhex('x2Ey', 'x')",
        [Null]
    );

    // Had to convert "." to its blob representation
    db_test!(unhex_x_y_long_prefix, "SELECT unhex('xyz2E', 'xyz')", b"2E");

    // Had to convert "." to its blob representation
    db_test!(
        unhex_x_y_shorter_suffix,
        "SELECT unhex('xyz2Exy', 'xyz')",
        b"2E"
    );

    // Had to convert "." to its blob representation
    db_test!(
        unhex_x_y_shorter_prefix,
        "SELECT unhex('xy2Exyz', 'xyz')",
        b"2E"
    );

    // Had to convert "." to its blob representation
    db_test!(
        unhex_x_y_random_order,
        "SELECT unhex('yx2Ezyx', 'xyz')",
        b"2E"
    );

    db_test!(
        unhex_x_y_char_in_the_middle,
        "SELECT unhex('yx2xEzyx', 'xyz')",
        [Null]
    );

    db_test!(
        unhex_x_y_character_outside_set,
        "SELECT unhex('yxn2Ezyx', 'xyz')",
        [Null]
    );

    db_test!(trim, "SELECT trim('   Limbo    ')", "Limbo");

    // TODO: Limbo outputs Integer(1)
    // db_test!(trim_number, "SELECT trim(1)", "1");

    db_test!(trim_null, "SELECT trim(null)", [Null]);

    db_test!(
        trim_leading_whitespace,
        "SELECT trim('   Leading')",
        "Leading"
    );

    db_test!(
        trim_trailing_whitespace,
        "SELECT trim('Trailing   ')",
        "Trailing"
    );

    db_test!(trim_pattern, "SELECT trim('Limbo', 'Limbo')", "");

    db_test!(trim_pattern_number, "SELECT trim(1, '1')", "");

    db_test!(trim_pattern_null, "SELECT trim(null, 'null')", [Null]);

    db_test!(
        trim_no_match_pattern,
        "SELECT trim('Limbo', 'xyz')",
        "Limbo"
    );

    db_test!(ltrim, "SELECT ltrim('   Limbo    ')", "Limbo    ");

    // TODO: Limbo outputs Integer(1)
    // db_test!(ltrim_number, "SELECT ltrim(1)", "1");

    db_test!(ltrim_null, "SELECT ltrim(null)", [Null]);

    db_test!(
        ltrim_leading_whitespace,
        "SELECT ltrim('   Leading')",
        "Leading"
    );

    db_test!(
        ltrim_no_leading_whitespace,
        "SELECT ltrim('Limbo')",
        "Limbo"
    );

    db_test!(ltrim_pattern, "SELECT ltrim('Limbo', 'Limbo')", "");

    db_test!(ltrim_pattern_number, "SELECT ltrim(1, '1')", "");

    db_test!(ltrim_pattern_null, "SELECT ltrim(null, 'null')", [Null]);

    db_test!(
        ltrim_no_match_pattern,
        "SELECT ltrim('Limbo', 'xyz')",
        "Limbo"
    );

    db_test!(rtrim, "SELECT rtrim('   Limbo    ')", "   Limbo");

    // TODO: Limbo outputs Integer(1)
    // db_test!(rtrim_number, "SELECT rtrim(1)", "1");

    db_test!(rtrim_null, "SELECT rtrim(null)", [Null]);

    db_test!(
        rtrim_trailing_whitespace,
        "SELECT rtrim('Trailing   ')",
        "Trailing"
    );

    db_test!(
        rtrim_no_trailing_whitespace,
        "SELECT rtrim('Limbo')",
        "Limbo"
    );

    db_test!(rtrim_pattern, "SELECT rtrim('Limbo', 'Limbo')", "");

    db_test!(rtrim_pattern_number, "SELECT rtrim(1, '1')", "");

    db_test!(rtrim_pattern_null, "SELECT rtrim(null, 'null')", [Null]);

    db_test!(
        rtrim_no_match_pattern,
        "SELECT rtrim('Limbo', 'xyz')",
        "Limbo"
    );

    db_test!(round_float_no_precision, "SELECT round(123.456)", 123.0);

    db_test!(
        round_float_with_precision,
        "SELECT round(123.456, 2)",
        123.46
    );

    db_test!(
        round_float_with_text_precision,
        "SELECT round(123.5, '1')",
        123.5
    );

    db_test!(round_text_parsable, "SELECT round('123.456', 2)", 123.46);

    db_test!(round_text_non_parsable, "SELECT round('abc', 1)", 0.0);

    db_test!(round_integer_with_precision, "SELECT round(123, 1)", 123.0);

    db_test!(
        round_float_negative_precision,
        "SELECT round(123.456, -1)",
        123.0
    );

    db_test!(
        round_float_zero_precision,
        "SELECT round(123.456, 0)",
        123.0
    );

    db_test!(round_null_precision, "SELECT round(123.456, null)", [Null]);

    db_test!(length_text, "SELECT length('limbo')", 5);

    db_test!(length_text_utf8_chars, "SELECT length('ąłóżźć')", 6);

    db_test!(length_integer, "SELECT length(12345)", 5);

    db_test!(length_float, "SELECT length(123.456)", 7);

    db_test!(length_null, "SELECT length(NULL)", [Null]);

    db_test!(length_empty_text, "SELECT length('')", 0);

    db_test!(octet_length_text, "SELECT length('limbo')", 5);

    db_test!(
        octet_length_text_utf8_chars,
        "SELECT octet_length('ąłóżźć')",
        12
    );

    db_test!(octet_length_integer, "SELECT octet_length(12345)", 5);

    db_test!(octet_length_float, "SELECT octet_length(123.456)", 7);

    db_test!(octet_length_null, "SELECT octet_length(NULL)", [Null]);

    db_test!(octet_length_empty_text, "SELECT octet_length('')", 0);

    db_test!(
        octet_length_date_binary_expr,
        "SELECT octet_length(date('now')) = 10",
        1
    );

    db_test!(min_number, "SELECT min(-10,2,3,+4)", -10);

    db_test!(min_str, "SELECT min('b','a','z')", "a");

    db_test!(min_str_number, "SELECT min('42',100)", 100);

    db_test!(min_blob_number, "SELECT min(3.14,x'616263')", 3.14);

    db_test!(max_str_number, "SELECT max('42',100)", "42");

    // Had to change "abc" to its blob representation
    db_test!(max_blob_number, "SELECT max(3.14,x'616263')", b"616263");

    db_test!(min_null, "SELECT min(null,null)", [Null]);

    db_test!(max_number, "SELECT max(-10,2,3)", 3);

    db_test!(max_str, "SELECT max('b','a','z')", "z");

    db_test!(min_int_float, "SELECT min(1,5.0)", 1);

    db_test!(max_int_float, "SELECT max(1,5.0)", 5.0);

    db_test!(min_float_int, "SELECT min(5.0,1)", 1);

    db_test!(max_null, "SELECT max(null,null)", [Null]);

    db_test!(nullif_1, "SELECT nullif(1, 2)", 1);

    db_test!(nullif_2, "SELECT nullif(1, 1)", [Null]);

    db_test!(nullif_3, "SELECT nullif('limbo', 'limbo')", [Null]);

    db_test!(substr_3_args, "SELECT substr('limbo', 1, 3)", "lim");

    db_test!(
        substr_3_args_exceed_length,
        "SELECT substr('limbo', 1, 10)",
        "limbo"
    );

    db_test!(
        substr_3_args_start_exceed_length,
        "SELECT substr('limbo', 10, 3)",
        ""
    );

    db_test!(substr_2_args, "SELECT substr('limbo', 3)", "mbo");

    db_test!(
        substr_cases,
        [
            "SELECT substr('limbo', 0)",
            "SELECT substr('limbo', 0, 3)",
            "SELECT substr('limbo', -2)",
            "SELECT substr('limbo', -2, 1)",
            "SELECT substr('limbo', -10, 7)",
            "SELECT substr('limbo', 10, -7)"
        ],
        [["limbo"], ["li"], ["bo"], ["b"], ["li"], ["mbo"]]
    );

    db_test!(substring_3_args, "SELECT substring('limbo', 1, 3)", "lim");

    db_test!(
        substring_3_args_exceed_length,
        "SELECT substring('limbo', 1, 10)",
        "limbo"
    );

    db_test!(
        substring_3_args_start_exceed_length,
        "SELECT substring('limbo', 10, 3)",
        ""
    );

    db_test!(substring_2_args, "SELECT substring('limbo', 3)", "mbo");

    db_test!(
        substring_2_args_exceed_length,
        "SELECT substring('limbo', 10)",
        ""
    );

    db_test!(typeof_null, "SELECT typeof(null)", "null");

    db_test!(typeof_null_case, "SELECT typeof(nuLL)", "null");

    db_test!(typeof_text, "SELECT typeof('hello')", "text");

    db_test!(typeof_text_empty, "SELECT typeof('')", "text");

    db_test!(typeof_integer, "SELECT typeof(123)", "integer");

    db_test!(typeof_real, "SELECT typeof(1.0)", "real");

    db_test!(typeof_blob, "SELECT typeof(x'61')", "blob");

    db_test!(typeof_blob_empty, "SELECT typeof(x'')", "blob");

    db_test!(
        typeof_sum_integer,
        "SELECT typeof(sum(age)) from users",
        "integer"
    );

    db_test!(
        typeof_sum_real,
        "SELECT typeof(sum(price)) from products",
        "real"
    );

    db_test!(
        typeof_group_concat,
        "SELECT typeof(group_concat(name)) from products",
        "text"
    );

    db_test!(unicode_a, "SELECT unicode('a')", 97);

    db_test!(unicode_emoji, "SELECT unicode('😊')", 128522);

    db_test!(unicode_empty, "SELECT unicode('')", [Null]);

    db_test!(unicode_number, "SELECT unicode(23)", 50);

    db_test!(unicode_float, "SELECT unicode(23.45)", 50);

    db_test!(unicode_null, "SELECT unicode(NULL)", [Null]);

    db_test!(
        quote_string_embedded_nul,
        "SELECT quote(concat('abc', char(0), 'def'))",
        "'abc'"
    );

    db_test!(quote_string, "SELECT quote('limbo')", "'limbo'");

    db_test!(quote_escape, "SELECT quote('''quote''')", "'''quote'''");

    db_test!(quote_null, "SELECT quote(null)", "NULL");

    // TODO: Limbo outputs Integer(123)
    // db_test!(quote_integer, "SELECT quote(123)", "123");

    db_test!(sign_positive_integer, "SELECT sign(42)", 1);

    db_test!(sign_negative_integer, "SELECT sign(-42)", -1);

    db_test!(sign_zero, "SELECT sign(0)", 0);

    db_test!(sign_positive_float, "SELECT sign(42.0)", 1);

    db_test!(sign_negative_float, "SELECT sign(-42.0)", -1);

    db_test!(sign_zero_float, "SELECT sign(0.0)", 0);

    db_test!(sign_text_positive_integer, "SELECT sign('42')", 1);

    db_test!(sign_text_negative_integer, "SELECT sign('-42')", -1);

    db_test!(sign_text_zero, "SELECT sign('0')", 0);

    db_test!(sign_text_non_numeric, "SELECT sign('abc')", [Null]);

    db_test!(sign_null, "SELECT sign(NULL)", [Null]);

    db_test!(randomblob_int_2, "SELECT length(randomblob(2))", 2);

    db_test!(randomblob_int_0, "SELECT length(randomblob(0))", 1);

    db_test!(randomblob_int_negative, "SELECT length(randomblob(-2))", 1);

    db_test!(randomblob_str_2, "SELECT length(randomblob('2'))", 2);

    db_test!(zeroblob_int_0, "SELECT zeroblob(0) = x''", 1);

    db_test!(zeroblob_int_1, "SELECT zeroblob(1) = x'00'", 1);

    db_test!(zeroblob_str_3, "SELECT zeroblob('3') = x'000000'", 1);

    db_test!(zeroblob_str_a, "SELECT zeroblob('a') = x''", 1);

    db_test!(zeroblob_blob, "SELECT zeroblob(x'01') = x''", 1);

    // CAST tests - INTEGER affinity
    db_test!(cast_text_to_integer, "SELECT CAST('123' AS INTEGER)", 123);

    db_test!(
        cast_text_with_spaces_to_integer,
        "SELECT CAST('  123  ' AS INTEGER)",
        123
    );

    db_test!(
        cast_text_with_trailing_junk_to_integer,
        "SELECT CAST('1abc' AS INTEGER)",
        1
    );

    db_test!(
        cast_invalid_text_to_integer,
        "SELECT CAST('abc' AS INTEGER)",
        0
    );

    db_test!(
        cast_text_prefix_to_integer,
        "SELECT CAST('123abc' AS INTEGER)",
        123
    );

    db_test!(cast_float_to_integer, "SELECT CAST(123.45 AS INTEGER)", 123);

    db_test!(
        cast_float_to_integer_rounding,
        [
            "SELECT CAST(0.6 AS INTEGER)",
            "SELECT CAST(1.0 AS INTEGER)",
            "SELECT CAST(1.6 AS INTEGER)",
            "SELECT CAST(-0.6 AS INTEGER)",
            "SELECT CAST(-1.0 AS INTEGER)",
            "SELECT CAST(-1.6 AS INTEGER)"
        ],
        [[0], [1], [1], [0], [-1], [-1]]
    );

    db_test!(
        cast_large_float_to_integer,
        "SELECT CAST(9223372036854775808.0 AS INTEGER)",
        9223372036854775807
    );

    db_test!(
        cast_small_float_to_integer,
        "SELECT CAST(-9223372036854775809.0 AS INTEGER)",
        -9223372036854775808
    );

    db_test!(
        cast_text_exp_to_integer,
        "SELECT CAST('123e+5' AS INTEGER)",
        123
    );

    // CAST tests - REAL affinity
    db_test!(cast_text_to_real, "SELECT CAST('123.45' AS REAL)", 123.45);

    db_test!(
        cast_text_with_spaces_to_real,
        "SELECT CAST('  123.45  ' AS REAL)",
        123.45
    );

    db_test!(cast_invalid_text_to_real, "SELECT CAST('abc' AS REAL)", 0.0);

    db_test!(
        cast_text_prefix_to_real,
        "SELECT CAST('123.45abc' AS REAL)",
        123.45
    );

    db_test!(cast_integer_to_real, "SELECT CAST(123 AS REAL)", 123.0);

    // CAST tests - TEXT affinity
    db_test!(cast_integer_to_text, "SELECT CAST(123 AS TEXT)", "123");

    db_test!(cast_real_to_text, "SELECT CAST(123.45 AS TEXT)", "123.45");

    db_test!(
        cast_blob_to_text,
        "SELECT CAST(x'68656C6C6F' AS TEXT)",
        "hello"
    );

    // CAST tests - BLOB affinity
    db_test!(
        cast_text_to_blob,
        "SELECT hex(CAST('hello' AS BLOB))",
        "68656C6C6F"
    );

    db_test!(
        cast_integer_to_blob,
        "SELECT hex(CAST(123 AS BLOB))",
        "313233"
    );

    // CAST tests - NUMERIC affinity
    db_test!(
        cast_integer_text_to_numeric,
        "SELECT typeof(CAST('123' AS NUMERIC)), CAST('123' AS NUMERIC)",
        ["integer", 123]
    );

    db_test!(
        cast_float_text_to_numeric,
        "SELECT typeof(CAST('123.45' AS NUMERIC)), CAST('123.45' AS NUMERIC)",
        ["real", 123.45]
    );

    db_test!(
        cast_small_float_to_numeric,
        "SELECT typeof(CAST('1.23' AS NUMERIC)), CAST('1.23' AS NUMERIC)",
        ["real", 1.23]
    );

    db_test!(
        cast_signed_edgecase_int_to_numeric,
        "SELECT typeof(CAST('-1230-23.40e24' AS NUMERIC)), CAST('-1230-23.40e24' AS NUMERIC)",
        ["integer", -1230]
    );

    db_test!(
        cast_edgecase_int_to_numeric_1,
        "SELECT typeof(CAST('1230-23.40e24' AS NUMERIC)), CAST('1230-23.40e24' AS NUMERIC)",
        ["integer", 1230]
    );

    db_test!(
        cast_edgecase_int_to_numeric_2,
        "SELECT typeof(CAST('123023.4024' AS NUMERIC)), CAST('123023.4024' AS NUMERIC)",
        ["real", 123023.4024]
    );

    // TODO: This does
    db_test!(
        regex,
        regex_sqlite_version_should_return_valid_output,
        "SELECT sqlite_version()",
        r"\d+\.\d+\.\d+"
    );

    // TODO COMPAT: sqlite returns 9.22337203685478e+18, do we care...?
    /*
    db_test!(
        cast_large_text_to_numeric,
        "SELECT typeof(CAST('9223372036854775808' AS NUMERIC)), CAST('9223372036854775808' AS NUMERIC)",
        ["real", 9.223372036854776e18]
    );
    */

    db_test!(
        cast_null_to_any,
        "SELECT CAST(NULL AS INTEGER), CAST(NULL AS TEXT), CAST(NULL AS BLOB), CAST(NULL AS REAL), CAST(NULL AS NUMERIC)",
        [null, null, null, null, null]
    );

    db_test!(
        cast_in_where,
        "SELECT age from users where age = cast('45' as integer) limit 1",
        45
    );

    // TODO: sqlite seems not enable soundex() by default unless build it with SQLITE_SOUNDEX enabled.
    /*
    db_test!(
        soundex_text,
        "SELECT soundex('Pfister'), soundex('husobee'), soundex('Tymczak'), soundex('Ashcraft'), soundex('Robert'), soundex('Rupert'), soundex('Rubin'), soundex('Kant'), soundex('Knuth'), soundex('x'), soundex('')",
        ["P236", "H210", "T522", "A261", "R163", "R163", "R150", "K530", "K530", "X000", "0000"]
    );
    */
}
