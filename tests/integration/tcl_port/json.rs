#[cfg(test)]
mod tests {
    use crate::db_test;

    db_test!(
        json5_ecma_script_1,
        "select json('{a:5,b:6}')",
        r#"{"a":5,"b":6}"#
    );

    db_test!(
        json5_ecma_script_2,
        "select json('{a:5,a:3}')",
        r#"{"a":5,"a":3}"#
    );

    db_test!(
        json5_ecma_script_3,
        "SELECT json('{ MNO_123$xyz : 789 }')",
        r#"{"MNO_123$xyz":789}"#
    );

    db_test!(
        json5_with_single_trailing_comma_valid,
        "select json('{\"a\":5, \"b\":6, }')",
        r#"{"a":5,"b":6}"#
    );

    db_test!(
        json5_single_quoted,
        "SELECT json('{\"a\": ''abcd''}')",
        r#"{"a":"abcd"}"#
    );

    db_test!(json5_hexadecimal_1, "SELECT json('{a: 0x0}')", r#"{"a":0}"#);

    db_test!(
        json5_hexadecimal_2,
        "SELECT json('{a: 0xabcdef}')",
        r#"{"a":11259375}"#
    );

    // Note: There are two tests with the same name in the original TCL code
    // Renaming the second one to json5_hexadecimal_3 for clarity
    db_test!(
        json5_hexadecimal_3,
        "SELECT json('{a: -0xabcdef}')",
        r#"{"a":-11259375}"#
    );

    db_test!(json5_number_1, "SELECT json('{x: 4.}')", r#"{"x":4.0}"#);

    db_test!(json5_number_2, "SELECT json('{x: +4.}')", r#"{"x":4.0}"#);

    db_test!(json5_number_3, "SELECT json('{x: -4.}')", r#"{"x":-4.0}"#);

    db_test!(
        json5_number_5,
        "SELECT json('{x: Infinity}')",
        r#"{"x":9e999}"#
    );

    db_test!(
        json5_number_6,
        "SELECT json('{x: -Infinity}')",
        r#"{"x":-9e999}"#
    );

    db_test!(
        json5_multi_comment,
        "SELECT json(' /* abc */ { /*def*/ aaa /* xyz */ : // to the end of line
          123 /* xyz */ , /* 123 */ }')",
        r#"{"aaa":123}"#
    );

    db_test!(
        json5_ecma_script_1_pretty,
        "select json_pretty('{a:5,b:6}')",
        r#"{
    "a": 5,
    "b": 6
}"#
    );

    db_test!(
        json5_ecma_script_2_pretty,
        "select json_pretty('{a:5,a:3}')",
        r#"{
    "a": 5,
    "a": 3
}"#
    );

    db_test!(
        json5_ecma_script_3_pretty,
        "SELECT json_pretty('{ MNO_123$xyz : 789 }')",
        r#"{
    "MNO_123$xyz": 789
}"#
    );

    db_test!(
        json5_with_single_trailing_comma_valid_pretty,
        "select json_pretty('{\"a\":5, \"b\":6, }')",
        r#"{
    "a": 5,
    "b": 6
}"#
    );

    db_test!(
        json5_single_quoted_pretty,
        "SELECT json_pretty('{\"a\": ''abcd''}')",
        r#"{
    "a": "abcd"
}"#
    );

    db_test!(
        json5_hexadecimal_1_pretty,
        "SELECT json_pretty('{a: 0x0}')",
        r#"{
    "a": 0
}"#
    );

    db_test!(
        json5_hexadecimal_2_pretty,
        "SELECT json_pretty('{a: 0xabcdef}')",
        r#"{
    "a": 11259375
}"#
    );

    // Note: There are two tests with the same name in the original TCL code
    // Renaming the second one to json5_hexadecimal_3_pretty for clarity
    db_test!(
        json5_hexadecimal_3_pretty,
        "SELECT json_pretty('{a: -0xabcdef}')",
        r#"{
    "a": -11259375
}"#
    );

    db_test!(
        json5_number_1_pretty,
        "SELECT json_pretty('{x: 4.}')",
        r#"{
    "x": 4.0
}"#
    );

    db_test!(
        json5_number_2_pretty,
        "SELECT json_pretty('{x: +4.}')",
        r#"{
    "x": 4.0
}"#
    );

    db_test!(
        json5_number_3_pretty,
        "SELECT json_pretty('{x: -4.}')",
        r#"{
    "x": -4.0
}"#
    );

    db_test!(
        json5_number_5_pretty,
        "SELECT json_pretty('{x: Infinity}')",
        r#"{
    "x": 9e999
}"#
    );

    db_test!(
        json5_number_6_pretty,
        "SELECT json_pretty('{x: -Infinity}')",
        r#"{
    "x": -9e999
}"#
    );

    db_test!(
        json5_multi_comment_pretty,
        "SELECT json_pretty(' /* abc */ { /*def*/ aaa /* xyz */ : // to the end of line
          123 /* xyz */ , /* 123 */ }')",
        r#"{
    "aaa": 123
}"#
    );

    db_test!(
        json_pretty_ident_1,
        "SELECT json_pretty('{x: 1}', '')",
        r#"{
"x": 1
}"#
    );

    db_test!(
        json_pretty_ident_2,
        "SELECT json_pretty('{x: 1}', '11')",
        r#"{
11"x": 1
}"#
    );

    db_test!(
        json_pretty_ident_null,
        "SELECT json_pretty('{x: 1}', NULL)",
        r#"{
    "x": 1
}"#
    );

    db_test!(
        json_pretty_ident_blob_1,
        "SELECT json_pretty('{x: 1}', x'33')",
        r#"{
3"x": 1
}"#
    );

    // TODO
    // Currently conversion from blob to string is not exactly the same as in sqlite.
    // The blob below should evaluate to two whitespaces TEXT value

    // db_test!(
    //     json_pretty_ident_blob_2,
    //     "SELECT json_pretty('{x: 1}', x'1111')",
    //     r#"{
    //   "x": 1
    // }"#
    // );

    db_test!(json_array_str, "SELECT json_array('a')", r#"["a"]"#);

    db_test!(
        json_array_numbers,
        "SELECT json_array(1, 1.5)",
        r#"[1,1.5]"#
    );

    db_test!(
        json_array_numbers_2,
        "SELECT json_array(1., +2., -2.)",
        r#"[1.0,2.0,-2.0]"#
    );

    db_test!(json_array_null, "SELECT json_array(null)", r#"[null]"#);

    db_test!(
        json_array_not_json,
        "SELECT json_array('{\"a\":1}')",
        r#"["{\"a\":1}"]"#
    );

    db_test!(
        json_array_json,
        "SELECT json_array(json('{\"a\":1}'))",
        r#"[{"a":1}]"#
    );

    db_test!(
        json_array_nested,
        "SELECT json_array(json_array(1,2,3), json('[1,2,3]'), '[1,2,3]')",
        r#"[[1,2,3],[1,2,3],"[1,2,3]"]"#
    );

    db_test!(json_extract_null, "SELECT json_extract(null, '$')", [Null]);

    db_test!(
        json_extract_json_null_type,
        "SELECT typeof(json_extract('null', '$'))",
        "null"
    );

    db_test!(
        json_arrow_json_null_type,
        "SELECT typeof('null' -> '$')",
        "text"
    );

    db_test!(
        json_arrow_shift_json_null_type,
        "SELECT typeof('null' ->> '$')",
        "null"
    );

    db_test!(json_extract_empty, "SELECT json_extract()", [Null]);

    db_test!(json_extract_single_param, "SELECT json_extract(1)", [Null]);

    db_test!(
        json_extract_null_invalid_path,
        "SELECT json_extract(null, 1)",
        [Null]
    );

    db_test!(
        json_extract_null_invalid_path_2,
        "SELECT json_extract(null, CAST(1 AS BLOB))",
        [Null]
    );

    db_test!(
        json_extract_multiple_nulls,
        "SELECT json_extract(null, CAST(1 AS BLOB), null, 1, 2, 3)",
        [Null]
    );

    db_test!(json_extract_number, "SELECT json_extract(1, '$')", 1);

    db_test!(
        json_extract_number_type,
        "SELECT typeof(json_extract(1, '$'))",
        "integer"
    );

    db_test!(json_arrow_number, "SELECT 1 -> '$'", "1");

    db_test!(json_arrow_number_type, "SELECT typeof(1 -> '$')", "text");

    db_test!(json_arrow_shift_number, "SELECT 1 -> '$'", "1");

    db_test!(
        json_arrow_shift_number_type,
        "SELECT typeof(1 ->> '$')",
        "integer"
    );

    db_test!(
        json_extract_object_1,
        "SELECT json_extract('{\"a\": [1,2,3]}', '$.a')",
        r#"[1,2,3]"#
    );

    db_test!(
        json_arrow_object,
        "SELECT '{\"a\": [1,2,3]}' -> '$.a'",
        r#"[1,2,3]"#
    );

    db_test!(
        json_arrow_shift_object,
        "SELECT '{\"a\": [1,2,3]}' ->> '$.a'",
        r#"[1,2,3]"#
    );

    db_test!(
        json_extract_object_2,
        "SELECT json_extract('{\"a\": [1,2,3]}', '$.a', '$.a[0]', '$.a[1]', '$.a[3]')",
        r#"[[1,2,3],1,2,null]"#
    );

    db_test!(
        json_extract_object_3,
        "SELECT json_extract('{\"a\": [1,2,3]}', '$.a', '$.a[0]', '$.a[1]', null, '$.a[3]')",
        [Null]
    );

    // \x61 is the ASCII code for 'a'
    db_test!(
        json_extract_with_escaping,
        "SELECT json_extract('{\"\\x61\": 1}', '$.a')",
        1
    );

    db_test!(
        json_extract_with_escaping_2,
        "SELECT json_extract('{\"a\": 1}', '$.\"\\x61\"')",
        1
    );

    db_test!(
        json_extract_null_path,
        "SELECT json_extract(1, null)",
        [Null]
    );

    db_test!(json_arrow_null_path, "SELECT 1 -> null", [Null]);

    db_test!(json_arrow_shift_null_path, "SELECT 1 ->> null", [Null]);

    db_test!(
        json_extract_float,
        "SELECT typeof(json_extract(1.0, '$'))",
        "real"
    );

    db_test!(json_arrow_float, "SELECT typeof(1.0 -> '$')", "text");

    db_test!(json_arrow_shift_float, "SELECT typeof(1.0 ->> '$')", "real");

    db_test!(json_extract_true, "SELECT json_extract('true', '$')", 1);

    db_test!(
        json_extract_true_type,
        "SELECT typeof(json_extract('true', '$'))",
        "integer"
    );

    db_test!(json_arrow_true, "SELECT 'true' -> '$'", "true");

    db_test!(json_arrow_true_type, "SELECT typeof('true' -> '$')", "text");

    db_test!(json_arrow_shift_true, "SELECT 'true' ->> '$'", 1);

    db_test!(
        json_arrow_shift_true_type,
        "SELECT typeof('true' ->> '$')",
        "integer"
    );

    db_test!(json_extract_false, "SELECT json_extract('false', '$')", 0);

    db_test!(
        json_extract_false_type,
        "SELECT typeof(json_extract('false', '$'))",
        "integer"
    );

    db_test!(json_arrow_false, "SELECT 'false' -> '$'", "false");

    db_test!(
        json_arrow_false_type,
        "SELECT typeof('false' -> '$')",
        "text"
    );

    db_test!(json_arrow_shift_false, "SELECT 'false' ->> '$'", 0);

    db_test!(
        json_arrow_shift_false_type,
        "SELECT typeof('false' ->> '$')",
        "integer"
    );

    db_test!(
        json_extract_string,
        "SELECT json_extract('\"string\"', '$')",
        "string"
    );

    db_test!(
        json_extract_string_type,
        "SELECT typeof(json_extract('\"string\"', '$'))",
        "text"
    );

    db_test!(
        json_arrow_string,
        "SELECT '\"string\"' -> '$'",
        r#""string""#
    );

    db_test!(
        json_arrow_string_type,
        "SELECT typeof('\"string\"' -> '$')",
        "text"
    );

    db_test!(
        json_arrow_shift_string,
        "SELECT '\"string\"' ->> '$'",
        "string"
    );

    db_test!(
        json_arrow_shift_string_type,
        "SELECT typeof('\"string\"' ->> '$')",
        "text"
    );

    db_test!(
        json_arrow_implicit_root_path,
        "SELECT '{\"a\":1}' -> 'a'",
        "1"
    );

    db_test!(
        json_arrow_shift_implicit_root_path,
        "SELECT '{\"a\":1}' ->> 'a'",
        [1]
    );

    db_test!(
        json_arrow_implicit_root_path_undefined_key,
        "SELECT '{\"a\":1}' -> 'x'",
        [Null]
    );

    db_test!(
        json_arrow_shift_implicit_root_path_undefined_key,
        "SELECT '{\"a\":1}' ->> 'x'",
        [Null]
    );

    db_test!(
        json_arrow_implicit_root_path_array,
        "SELECT '[1,2,3]' -> 1",
        ["2"]
    );

    db_test!(
        json_arrow_shift_implicit_root_path_array,
        "SELECT '[1,2,3]' ->> 1",
        [2]
    );

    db_test!(
        json_arrow_implicit_root_path_array_negative_idx,
        "SELECT '[1,2,3]' -> -1",
        ["3"]
    );

    db_test!(
        json_arrow_shift_implicit_root_path_array_negative_idx,
        "SELECT '[1,2,3]' ->> -1",
        [3]
    );

    db_test!(
        json_arrow_implicit_real_cast,
        "SELECT '{\"1.5\":\"abc\"}' -> 1.5",
        ["\"abc\""]
    );

    db_test!(
        json_arrow_shift_implicit_real_cast,
        "SELECT '{\"1.5\":\"abc\"}' -> 1.5",
        ["\"abc\""]
    );

    db_test!(
        json_arrow_implicit_true_cast,
        "SELECT '[1,2,3]' -> true",
        ["2"]
    );

    db_test!(
        json_arrow_shift_implicit_true_cast,
        "SELECT '[1,2,3]' ->> true",
        [2]
    );

    db_test!(
        json_arrow_implicit_false_cast,
        "SELECT '[1,2,3]' -> false",
        ["1"]
    );

    db_test!(
        json_arrow_shift_implicit_false_cast,
        "SELECT '[1,2,3]' ->> false",
        [1]
    );

    db_test!(
        json_arrow_chained,
        "SELECT '{\"a\":2,\"c\":[4,5,{\"f\":7}]}' -> 'c' -> 2 ->> 'f'",
        [7]
    );

    db_test!(
        json_extract_multiple_null_paths,
        "SELECT json_extract(1, null, null, null)",
        [Null]
    );

    db_test!(
        json_extract_array,
        "SELECT json_extract('[1,2,3]', '$')",
        ["[1,2,3]"]
    );

    db_test!(json_arrow_array, "SELECT '[1,2,3]' -> '$'", ["[1,2,3]"]);

    db_test!(
        json_arrow_shift_array,
        "SELECT '[1,2,3]' ->> '$'",
        ["[1,2,3]"]
    );

    // TODO: divergence from TCL here. CLI does some transformation on the value in Limbo. In sqlite it just outputs nothing
    /*
       do_execsql_test json_extract_quote {
           SELECT json_extract('{"\"":1 }', '$."\""')
       } {{1}}
    */
    db_test!(
        json_extract_quote,
        "SELECT json_extract('{\"\\\"\":1 }', '$.\\\"')",
        [Null]
    );

    // Overflows 2**32 is equivalent to 0
    db_test!(
        json_extract_overflow_int32_1,
        "SELECT json_extract('[1,2,3]', '$[4294967296]')",
        [1]
    );

    // Overflows 2**32 + 1 is equivalent to 1
    db_test!(
        json_extract_overflow_int32_2,
        "SELECT json_extract('[1,2,3]', '$[4294967297]')",
        [2]
    );

    // Overflows -2**32 - 1 is equivalent to -1
    db_test!(
        json_extract_overflow_int32_3,
        "SELECT json_extract('[1,2,3]', '$[#-4294967297]')",
        [3]
    );

    // Overflows -2**32 - 2 is equivalent to -2
    db_test!(
        json_extract_overflow_int32_4,
        "SELECT json_extract('[1,2,3]', '$[#-4294967298]')",
        [2]
    );

    // pow(2,63) + 1 == 9223372036854775808
    db_test!(
        json_extract_overflow_int64,
        "SELECT json_extract('[1,2,3]', '$[9223372036854775808]')",
        [1]
    );

    // TODO: fix me - this passes on SQLite and needs to be fixed in Limbo.
    // pow(2, 127) + 1 == 170141183460469231731687303715884105729
    // db_test!(
    //     json_extract_overflow_int128,
    //     "SELECT json_extract('[1, 2, 3]', '$[170141183460469231731687303715884105729]')",
    //     [2]
    // );

    // TODO: fix me - this passes on SQLite and needs to be fixed in Limbo.
    // db_test!(
    //     json_extract_blob,
    //     "SELECT json_extract(CAST('[1,2,3]' as BLOB), '$[1]')",
    //     [2]
    // );

    db_test!(
        json_array_length,
        "SELECT json_array_length('[1,2,3,4]')",
        [4]
    );

    db_test!(
        json_array_length_empty,
        "SELECT json_array_length('[]')",
        [0]
    );

    db_test!(
        json_array_length_root,
        "SELECT json_array_length('[1,2,3,4]', '$')",
        [4]
    );

    db_test!(
        json_array_length_not_array,
        "SELECT json_array_length('{\"one\":[1,2,3]}')",
        [0]
    );

    db_test!(
        json_array_length_via_prop,
        "SELECT json_array_length('{\"one\":[1,2,3]}', '$.one')",
        [3]
    );

    db_test!(
        json_array_length_via_index,
        "SELECT json_array_length('[[1,2,3,4]]', '$[0]')",
        [4]
    );

    db_test!(
        json_array_length_via_index_not_array,
        "SELECT json_array_length('[1,2,3,4]', '$[2]')",
        [0]
    );

    db_test!(
        json_array_length_via_bad_prop,
        "SELECT json_array_length('{\"one\":[1,2,3]}', '$.two')",
        [Null]
    );

    db_test!(
        json_array_length_nested,
        "SELECT json_array_length('{\"one\":[[1,2,3],2,3]}', '$.one[0]')",
        [3]
    );

    db_test!(
        json_type_no_path,
        "SELECT json_type('{\"a\":[2,3.5,true,false,null,\"x\"]}')",
        ["object"]
    );

    db_test!(
        json_type_root_path,
        "SELECT json_type('{\"a\":[2,3.5,true,false,null,\"x\"]}','$')",
        ["object"]
    );

    db_test!(
        json_type_array,
        "SELECT json_type('{\"a\":[2,3.5,true,false,null,\"x\"]}','$.a')",
        ["array"]
    );

    db_test!(
        json_type_integer,
        "SELECT json_type('{\"a\":[2,3.5,true,false,null,\"x\"]}','$.a[0]')",
        ["integer"]
    );

    db_test!(
        json_type_real,
        "SELECT json_type('{\"a\":[2,3.5,true,false,null,\"x\"]}','$.a[1]')",
        ["real"]
    );

    db_test!(
        json_type_true,
        "SELECT json_type('{\"a\":[2,3.5,true,false,null,\"x\"]}','$.a[2]')",
        ["true"]
    );

    db_test!(
        json_type_false,
        "SELECT json_type('{\"a\":[2,3.5,true,false,null,\"x\"]}','$.a[3]')",
        ["false"]
    );

    db_test!(
        json_type_null,
        "SELECT json_type('{\"a\":[2,3.5,true,false,null,\"x\"]}','$.a[4]')",
        ["null"]
    );

    db_test!(
        json_type_text,
        "SELECT json_type('{\"a\":[2,3.5,true,false,null,\"x\"]}','$.a[5]')",
        ["text"]
    );

    db_test!(
        json_type_null_2,
        "SELECT json_type('{\"a\":[2,3.5,true,false,null,\"x\"]}','$.a[6]')",
        [Null]
    );

    db_test!(json_type_cast, "SELECT json_type(1)", ["integer"]);

    db_test!(json_type_null_arg, "SELECT json_type(null)", [Null]);

    db_test!(
        json_error_position_valid,
        "SELECT json_error_position('{\"a\":55,\"b\":72,}')",
        [0]
    );
}
