use divan::{black_box, Bencher};
use std::collections::HashMap;
use turso_core::types::Value;
use turso_core::vdbe::value::construct_like_regex;

// =============================================================================
// String Case Functions
// =============================================================================

#[divan::bench]
fn lower_short_string(bencher: Bencher) {
    let value = Value::build_text("HELLO");
    bencher.bench_local(|| black_box(&value).exec_lower());
}

#[divan::bench]
fn lower_long_string(bencher: Bencher) {
    let value = Value::build_text("THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG");
    bencher.bench_local(|| black_box(&value).exec_lower());
}

#[divan::bench]
fn lower_integer(bencher: Bencher) {
    let value = Value::Integer(12345);
    bencher.bench_local(|| black_box(&value).exec_lower());
}

#[divan::bench]
fn upper_short_string(bencher: Bencher) {
    let value = Value::build_text("hello");
    bencher.bench_local(|| black_box(&value).exec_upper());
}

#[divan::bench]
fn upper_long_string(bencher: Bencher) {
    let value = Value::build_text("the quick brown fox jumps over the lazy dog");
    bencher.bench_local(|| black_box(&value).exec_upper());
}

#[divan::bench]
fn upper_integer(bencher: Bencher) {
    let value = Value::Integer(12345);
    bencher.bench_local(|| black_box(&value).exec_upper());
}

// =============================================================================
// Length Functions
// =============================================================================

#[divan::bench]
fn length_short_text(bencher: Bencher) {
    let value = Value::build_text("hello");
    bencher.bench_local(|| black_box(&value).exec_length());
}

#[divan::bench]
fn length_long_text(bencher: Bencher) {
    let value = Value::build_text("the quick brown fox jumps over the lazy dog");
    bencher.bench_local(|| black_box(&value).exec_length());
}

#[divan::bench]
fn length_unicode_text(bencher: Bencher) {
    let value = Value::build_text("héllo wörld 你好世界");
    bencher.bench_local(|| black_box(&value).exec_length());
}

#[divan::bench]
fn length_integer(bencher: Bencher) {
    let value = Value::Integer(123456789);
    bencher.bench_local(|| black_box(&value).exec_length());
}

#[divan::bench]
fn length_float(bencher: Bencher) {
    let value = Value::Float(123.456789);
    bencher.bench_local(|| black_box(&value).exec_length());
}

#[divan::bench]
fn length_blob(bencher: Bencher) {
    let value = Value::Blob(vec![0u8; 100]);
    bencher.bench_local(|| black_box(&value).exec_length());
}

#[divan::bench]
fn octet_length_text(bencher: Bencher) {
    let value = Value::build_text("héllo wörld");
    bencher.bench_local(|| black_box(&value).exec_octet_length());
}

#[divan::bench]
fn octet_length_unicode(bencher: Bencher) {
    let value = Value::build_text("你好世界");
    bencher.bench_local(|| black_box(&value).exec_octet_length());
}

// =============================================================================
// Trim Functions
// =============================================================================

#[divan::bench]
fn trim_spaces(bencher: Bencher) {
    let value = Value::build_text("     hello world     ");
    bencher.bench_local(|| black_box(&value).exec_trim(None));
}

#[divan::bench]
fn trim_with_pattern(bencher: Bencher) {
    let value = Value::build_text("xxxhello worldxxx");
    let pattern = Value::build_text("x");
    bencher.bench_local(|| black_box(&value).exec_trim(Some(black_box(&pattern))));
}

#[divan::bench]
fn ltrim_spaces(bencher: Bencher) {
    let value = Value::build_text("     hello world");
    bencher.bench_local(|| black_box(&value).exec_ltrim(None));
}

#[divan::bench]
fn ltrim_with_pattern(bencher: Bencher) {
    let value = Value::build_text("xxxhello world");
    let pattern = Value::build_text("x");
    bencher.bench_local(|| black_box(&value).exec_ltrim(Some(black_box(&pattern))));
}

#[divan::bench]
fn rtrim_spaces(bencher: Bencher) {
    let value = Value::build_text("hello world     ");
    bencher.bench_local(|| black_box(&value).exec_rtrim(None));
}

#[divan::bench]
fn rtrim_with_pattern(bencher: Bencher) {
    let value = Value::build_text("hello worldxxx");
    let pattern = Value::build_text("x");
    bencher.bench_local(|| black_box(&value).exec_rtrim(Some(black_box(&pattern))));
}

// =============================================================================
// Substring Function
// =============================================================================

#[divan::bench]
fn substring_simple(bencher: Bencher) {
    let value = Value::build_text("hello world");
    let start = Value::Integer(1);
    let length = Value::Integer(5);
    bencher.bench_local(|| {
        Value::exec_substring(
            black_box(&value),
            black_box(&start),
            Some(black_box(&length)),
        )
    });
}

#[divan::bench]
fn substring_long_text(bencher: Bencher) {
    let value = Value::build_text("the quick brown fox jumps over the lazy dog");
    let start = Value::Integer(5);
    let length = Value::Integer(15);
    bencher.bench_local(|| {
        Value::exec_substring(
            black_box(&value),
            black_box(&start),
            Some(black_box(&length)),
        )
    });
}

#[divan::bench]
fn substring_unicode(bencher: Bencher) {
    let value = Value::build_text("héllo wörld 你好");
    let start = Value::Integer(1);
    let length = Value::Integer(10);
    bencher.bench_local(|| {
        Value::exec_substring(
            black_box(&value),
            black_box(&start),
            Some(black_box(&length)),
        )
    });
}

#[divan::bench]
fn substring_negative_start(bencher: Bencher) {
    let value = Value::build_text("hello world");
    let start = Value::Integer(-5);
    let length = Value::Integer(5);
    bencher.bench_local(|| {
        Value::exec_substring(
            black_box(&value),
            black_box(&start),
            Some(black_box(&length)),
        )
    });
}

#[divan::bench]
fn substring_blob(bencher: Bencher) {
    let value = Value::Blob(b"hello world".to_vec());
    let start = Value::Integer(1);
    let length = Value::Integer(5);
    bencher.bench_local(|| {
        Value::exec_substring(
            black_box(&value),
            black_box(&start),
            Some(black_box(&length)),
        )
    });
}

// =============================================================================
// Instr Function
// =============================================================================

#[divan::bench]
fn instr_found_early(bencher: Bencher) {
    let value = Value::build_text("hello world");
    let pattern = Value::build_text("ell");
    bencher.bench_local(|| black_box(&value).exec_instr(black_box(&pattern)));
}

#[divan::bench]
fn instr_found_late(bencher: Bencher) {
    let value = Value::build_text("the quick brown fox jumps over the lazy dog");
    let pattern = Value::build_text("dog");
    bencher.bench_local(|| black_box(&value).exec_instr(black_box(&pattern)));
}

#[divan::bench]
fn instr_not_found(bencher: Bencher) {
    let value = Value::build_text("hello world");
    let pattern = Value::build_text("xyz");
    bencher.bench_local(|| black_box(&value).exec_instr(black_box(&pattern)));
}

#[divan::bench]
fn instr_blob(bencher: Bencher) {
    let value = Value::Blob(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let pattern = Value::Blob(vec![5, 6, 7]);
    bencher.bench_local(|| black_box(&value).exec_instr(black_box(&pattern)));
}

// =============================================================================
// Replace Function
// =============================================================================

#[divan::bench]
fn replace_single_occurrence(bencher: Bencher) {
    let source = Value::build_text("hello world");
    let pattern = Value::build_text("world");
    let replacement = Value::build_text("there");
    bencher.bench_local(|| {
        Value::exec_replace(
            black_box(&source),
            black_box(&pattern),
            black_box(&replacement),
        )
    });
}

#[divan::bench]
fn replace_multiple_occurrences(bencher: Bencher) {
    let source = Value::build_text("banana banana banana");
    let pattern = Value::build_text("banana");
    let replacement = Value::build_text("apple");
    bencher.bench_local(|| {
        Value::exec_replace(
            black_box(&source),
            black_box(&pattern),
            black_box(&replacement),
        )
    });
}

#[divan::bench]
fn replace_empty_pattern(bencher: Bencher) {
    let source = Value::build_text("hello world");
    let pattern = Value::build_text("");
    let replacement = Value::build_text("x");
    bencher.bench_local(|| {
        Value::exec_replace(
            black_box(&source),
            black_box(&pattern),
            black_box(&replacement),
        )
    });
}

// =============================================================================
// Quote Function
// =============================================================================

#[divan::bench]
fn quote_text(bencher: Bencher) {
    let value = Value::build_text("hello world");
    bencher.bench_local(|| black_box(&value).exec_quote());
}

#[divan::bench]
fn quote_text_with_quotes(bencher: Bencher) {
    let value = Value::build_text("hello'world");
    bencher.bench_local(|| black_box(&value).exec_quote());
}

#[divan::bench]
fn quote_integer(bencher: Bencher) {
    let value = Value::Integer(12345);
    bencher.bench_local(|| black_box(&value).exec_quote());
}

#[divan::bench]
fn quote_blob(bencher: Bencher) {
    let value = Value::Blob(vec![0x01, 0x02, 0xAB, 0xCD, 0xEF]);
    bencher.bench_local(|| black_box(&value).exec_quote());
}

#[divan::bench]
fn quote_null(bencher: Bencher) {
    let value = Value::Null;
    bencher.bench_local(|| black_box(&value).exec_quote());
}

// =============================================================================
// Soundex Function
// =============================================================================

#[divan::bench]
fn soundex_simple(bencher: Bencher) {
    let value = Value::build_text("Robert");
    bencher.bench_local(|| black_box(&value).exec_soundex());
}

#[divan::bench]
fn soundex_complex(bencher: Bencher) {
    let value = Value::build_text("Ashcraft");
    bencher.bench_local(|| black_box(&value).exec_soundex());
}

#[divan::bench]
fn soundex_non_ascii(bencher: Bencher) {
    let value = Value::build_text("闪电五连鞭");
    bencher.bench_local(|| black_box(&value).exec_soundex());
}

// =============================================================================
// Type Functions
// =============================================================================

#[divan::bench]
fn typeof_integer(bencher: Bencher) {
    let value = Value::Integer(12345);
    bencher.bench_local(|| black_box(&value).exec_typeof());
}

#[divan::bench]
fn typeof_float(bencher: Bencher) {
    let value = Value::Float(123.456);
    bencher.bench_local(|| black_box(&value).exec_typeof());
}

#[divan::bench]
fn typeof_text(bencher: Bencher) {
    let value = Value::build_text("hello");
    bencher.bench_local(|| black_box(&value).exec_typeof());
}

#[divan::bench]
fn typeof_blob(bencher: Bencher) {
    let value = Value::Blob(vec![1, 2, 3]);
    bencher.bench_local(|| black_box(&value).exec_typeof());
}

#[divan::bench]
fn typeof_null(bencher: Bencher) {
    let value = Value::Null;
    bencher.bench_local(|| black_box(&value).exec_typeof());
}

// =============================================================================
// Cast Function
// =============================================================================

#[divan::bench]
fn cast_integer_to_text(bencher: Bencher) {
    let value = Value::Integer(12345);
    bencher.bench_local(|| black_box(&value).exec_cast("TEXT"));
}

#[divan::bench]
fn cast_float_to_integer(bencher: Bencher) {
    let value = Value::Float(123.456);
    bencher.bench_local(|| black_box(&value).exec_cast("INT"));
}

#[divan::bench]
fn cast_text_to_integer(bencher: Bencher) {
    let value = Value::build_text("12345");
    bencher.bench_local(|| black_box(&value).exec_cast("INT"));
}

#[divan::bench]
fn cast_text_to_real(bencher: Bencher) {
    let value = Value::build_text("123.456");
    bencher.bench_local(|| black_box(&value).exec_cast("REAL"));
}

#[divan::bench]
fn cast_text_to_blob(bencher: Bencher) {
    let value = Value::build_text("hello world");
    bencher.bench_local(|| black_box(&value).exec_cast("BLOB"));
}

#[divan::bench]
fn cast_text_to_numeric(bencher: Bencher) {
    let value = Value::build_text("123.456");
    bencher.bench_local(|| black_box(&value).exec_cast("NUMERIC"));
}

// =============================================================================
// Hex/Unhex Functions
// =============================================================================

#[divan::bench]
fn hex_text(bencher: Bencher) {
    let value = Value::build_text("hello");
    bencher.bench_local(|| black_box(&value).exec_hex());
}

#[divan::bench]
fn hex_blob(bencher: Bencher) {
    let value = Value::Blob(vec![0x01, 0x02, 0xAB, 0xCD, 0xEF]);
    bencher.bench_local(|| black_box(&value).exec_hex());
}

#[divan::bench]
fn hex_integer(bencher: Bencher) {
    let value = Value::Integer(255);
    bencher.bench_local(|| black_box(&value).exec_hex());
}

#[divan::bench]
fn unhex_valid(bencher: Bencher) {
    let value = Value::build_text("48656C6C6F");
    bencher.bench_local(|| black_box(&value).exec_unhex(None));
}

#[divan::bench]
fn unhex_with_ignored(bencher: Bencher) {
    let value = Value::build_text("  48656C6C6F  ");
    let ignore = Value::build_text(" ");
    bencher.bench_local(|| black_box(&value).exec_unhex(Some(black_box(&ignore))));
}

// =============================================================================
// Unicode Function
// =============================================================================

#[divan::bench]
fn unicode_ascii(bencher: Bencher) {
    let value = Value::build_text("A");
    bencher.bench_local(|| black_box(&value).exec_unicode());
}

#[divan::bench]
fn unicode_emoji(bencher: Bencher) {
    let value = Value::build_text("😊");
    bencher.bench_local(|| black_box(&value).exec_unicode());
}

#[divan::bench]
fn unicode_cjk(bencher: Bencher) {
    let value = Value::build_text("你");
    bencher.bench_local(|| black_box(&value).exec_unicode());
}

// =============================================================================
// Numeric Functions
// =============================================================================

#[divan::bench]
fn abs_positive_integer(bencher: Bencher) {
    let value = Value::Integer(12345);
    bencher.bench_local(|| black_box(&value).exec_abs());
}

#[divan::bench]
fn abs_negative_integer(bencher: Bencher) {
    let value = Value::Integer(-12345);
    bencher.bench_local(|| black_box(&value).exec_abs());
}

#[divan::bench]
fn abs_float(bencher: Bencher) {
    let value = Value::Float(-123.456);
    bencher.bench_local(|| black_box(&value).exec_abs());
}

#[divan::bench]
fn abs_text_numeric(bencher: Bencher) {
    let value = Value::build_text("-123.456");
    bencher.bench_local(|| black_box(&value).exec_abs());
}

#[divan::bench]
fn sign_positive(bencher: Bencher) {
    let value = Value::Integer(42);
    bencher.bench_local(|| black_box(&value).exec_sign());
}

#[divan::bench]
fn sign_negative(bencher: Bencher) {
    let value = Value::Integer(-42);
    bencher.bench_local(|| black_box(&value).exec_sign());
}

#[divan::bench]
fn sign_zero(bencher: Bencher) {
    let value = Value::Integer(0);
    bencher.bench_local(|| black_box(&value).exec_sign());
}

#[divan::bench]
fn sign_float(bencher: Bencher) {
    let value = Value::Float(-42.5);
    bencher.bench_local(|| black_box(&value).exec_sign());
}

// =============================================================================
// Round Function
// =============================================================================

#[divan::bench]
fn round_no_precision(bencher: Bencher) {
    let value = Value::Float(123.456);
    bencher.bench_local(|| black_box(&value).exec_round(None));
}

#[divan::bench]
fn round_with_precision(bencher: Bencher) {
    let value = Value::Float(123.456789);
    let precision = Value::Integer(2);
    bencher.bench_local(|| black_box(&value).exec_round(Some(black_box(&precision))));
}

#[divan::bench]
fn round_high_precision(bencher: Bencher) {
    let value = Value::Float(std::f64::consts::PI);
    let precision = Value::Integer(10);
    bencher.bench_local(|| black_box(&value).exec_round(Some(black_box(&precision))));
}

// =============================================================================
// Log Function
// =============================================================================

#[divan::bench]
fn log_base_10(bencher: Bencher) {
    let value = Value::Float(100.0);
    bencher.bench_local(|| black_box(&value).exec_math_log(None));
}

#[divan::bench]
fn log_base_2(bencher: Bencher) {
    let value = Value::Float(8.0);
    let base = Value::Float(2.0);
    bencher.bench_local(|| black_box(&value).exec_math_log(Some(black_box(&base))));
}

#[divan::bench]
fn log_arbitrary_base(bencher: Bencher) {
    let value = Value::Float(100.0);
    let base = Value::Float(7.0);
    bencher.bench_local(|| black_box(&value).exec_math_log(Some(black_box(&base))));
}

// =============================================================================
// Arithmetic Operations
// =============================================================================

#[divan::bench]
fn add_integers(bencher: Bencher) {
    let a = Value::Integer(1000);
    let b = Value::Integer(2000);
    bencher.bench_local(|| black_box(&a).exec_add(black_box(&b)));
}

#[divan::bench]
fn add_floats(bencher: Bencher) {
    let a = Value::Float(100.5);
    let b = Value::Float(200.5);
    bencher.bench_local(|| black_box(&a).exec_add(black_box(&b)));
}

#[divan::bench]
fn add_mixed(bencher: Bencher) {
    let a = Value::Integer(100);
    let b = Value::Float(200.5);
    bencher.bench_local(|| black_box(&a).exec_add(black_box(&b)));
}

#[divan::bench]
fn subtract_integers(bencher: Bencher) {
    let a = Value::Integer(2000);
    let b = Value::Integer(1000);
    bencher.bench_local(|| black_box(&a).exec_subtract(black_box(&b)));
}

#[divan::bench]
fn multiply_integers(bencher: Bencher) {
    let a = Value::Integer(100);
    let b = Value::Integer(200);
    bencher.bench_local(|| black_box(&a).exec_multiply(black_box(&b)));
}

#[divan::bench]
fn divide_integers(bencher: Bencher) {
    let a = Value::Integer(1000);
    let b = Value::Integer(10);
    bencher.bench_local(|| black_box(&a).exec_divide(black_box(&b)));
}

#[divan::bench]
fn remainder_integers(bencher: Bencher) {
    let a = Value::Integer(17);
    let b = Value::Integer(5);
    bencher.bench_local(|| black_box(&a).exec_remainder(black_box(&b)));
}

// =============================================================================
// Bitwise Operations
// =============================================================================

#[divan::bench]
fn bit_and(bencher: Bencher) {
    let a = Value::Integer(0b11110000);
    let b = Value::Integer(0b10101010);
    bencher.bench_local(|| black_box(&a).exec_bit_and(black_box(&b)));
}

#[divan::bench]
fn bit_or(bencher: Bencher) {
    let a = Value::Integer(0b11110000);
    let b = Value::Integer(0b00001111);
    bencher.bench_local(|| black_box(&a).exec_bit_or(black_box(&b)));
}

#[divan::bench]
fn bit_not(bencher: Bencher) {
    let a = Value::Integer(0b11110000);
    bencher.bench_local(|| black_box(&a).exec_bit_not());
}

#[divan::bench]
fn shift_left(bencher: Bencher) {
    let a = Value::Integer(1);
    let b = Value::Integer(8);
    bencher.bench_local(|| black_box(&a).exec_shift_left(black_box(&b)));
}

#[divan::bench]
fn shift_right(bencher: Bencher) {
    let a = Value::Integer(256);
    let b = Value::Integer(4);
    bencher.bench_local(|| black_box(&a).exec_shift_right(black_box(&b)));
}

// =============================================================================
// Boolean Operations
// =============================================================================

#[divan::bench]
fn boolean_not_true(bencher: Bencher) {
    let value = Value::Integer(1);
    bencher.bench_local(|| black_box(&value).exec_boolean_not());
}

#[divan::bench]
fn boolean_not_false(bencher: Bencher) {
    let value = Value::Integer(0);
    bencher.bench_local(|| black_box(&value).exec_boolean_not());
}

#[divan::bench]
fn and_true_true(bencher: Bencher) {
    let a = Value::Integer(1);
    let b = Value::Integer(1);
    bencher.bench_local(|| black_box(&a).exec_and(black_box(&b)));
}

#[divan::bench]
fn and_true_false(bencher: Bencher) {
    let a = Value::Integer(1);
    let b = Value::Integer(0);
    bencher.bench_local(|| black_box(&a).exec_and(black_box(&b)));
}

#[divan::bench]
fn or_false_false(bencher: Bencher) {
    let a = Value::Integer(0);
    let b = Value::Integer(0);
    bencher.bench_local(|| black_box(&a).exec_or(black_box(&b)));
}

#[divan::bench]
fn or_true_false(bencher: Bencher) {
    let a = Value::Integer(1);
    let b = Value::Integer(0);
    bencher.bench_local(|| black_box(&a).exec_or(black_box(&b)));
}

// =============================================================================
// Concat Functions
// =============================================================================

#[divan::bench]
fn concat_two_strings(bencher: Bencher) {
    let a = Value::build_text("hello ");
    let b = Value::build_text("world");
    bencher.bench_local(|| black_box(&a).exec_concat(black_box(&b)));
}

#[divan::bench]
fn concat_string_integer(bencher: Bencher) {
    let a = Value::build_text("count: ");
    let b = Value::Integer(42);
    bencher.bench_local(|| black_box(&a).exec_concat(black_box(&b)));
}

#[divan::bench]
fn concat_blobs(bencher: Bencher) {
    let a = Value::Blob(b"hello ".to_vec());
    let b = Value::Blob(b"world".to_vec());
    bencher.bench_local(|| black_box(&a).exec_concat(black_box(&b)));
}

#[divan::bench]
fn concat_strings_multiple(bencher: Bencher) {
    let values = [
        Value::build_text("the "),
        Value::build_text("quick "),
        Value::build_text("brown "),
        Value::build_text("fox"),
    ];
    bencher.bench_local(|| Value::exec_concat_strings(black_box(values.iter())));
}

#[divan::bench]
fn concat_ws_strings(bencher: Bencher) {
    let values = [
        Value::build_text(", "),
        Value::build_text("apple"),
        Value::build_text("banana"),
        Value::build_text("cherry"),
    ];
    bencher.bench_local(|| Value::exec_concat_ws(black_box(values.iter())));
}

// =============================================================================
// Char Function
// =============================================================================

#[divan::bench]
fn char_single(bencher: Bencher) {
    let values = [Value::Integer(65)];
    bencher.bench_local(|| Value::exec_char(black_box(values.iter())));
}

#[divan::bench]
fn char_multiple(bencher: Bencher) {
    let values = [
        Value::Integer(72),
        Value::Integer(101),
        Value::Integer(108),
        Value::Integer(108),
        Value::Integer(111),
    ];
    bencher.bench_local(|| Value::exec_char(black_box(values.iter())));
}

// =============================================================================
// Min/Max Functions
// =============================================================================

#[divan::bench]
fn min_integers(bencher: Bencher) {
    let values = [
        Value::Integer(5),
        Value::Integer(3),
        Value::Integer(8),
        Value::Integer(1),
        Value::Integer(9),
    ];
    bencher.bench_local(|| Value::exec_min(black_box(values.iter())));
}

#[divan::bench]
fn max_integers(bencher: Bencher) {
    let values = [
        Value::Integer(5),
        Value::Integer(3),
        Value::Integer(8),
        Value::Integer(1),
        Value::Integer(9),
    ];
    bencher.bench_local(|| Value::exec_max(black_box(values.iter())));
}

#[divan::bench]
fn min_strings(bencher: Bencher) {
    let values = [
        Value::build_text("banana"),
        Value::build_text("apple"),
        Value::build_text("cherry"),
    ];
    bencher.bench_local(|| Value::exec_min(black_box(values.iter())));
}

#[divan::bench]
fn max_strings(bencher: Bencher) {
    let values = [
        Value::build_text("banana"),
        Value::build_text("apple"),
        Value::build_text("cherry"),
    ];
    bencher.bench_local(|| Value::exec_max(black_box(values.iter())));
}

// =============================================================================
// Nullif Function
// =============================================================================

#[divan::bench]
fn nullif_equal(bencher: Bencher) {
    let a = Value::Integer(42);
    let b = Value::Integer(42);
    bencher.bench_local(|| black_box(&a).exec_nullif(black_box(&b)));
}

#[divan::bench]
fn nullif_not_equal(bencher: Bencher) {
    let a = Value::Integer(42);
    let b = Value::Integer(100);
    bencher.bench_local(|| black_box(&a).exec_nullif(black_box(&b)));
}

#[divan::bench]
fn nullif_strings(bencher: Bencher) {
    let a = Value::build_text("hello");
    let b = Value::build_text("hello");
    bencher.bench_local(|| black_box(&a).exec_nullif(black_box(&b)));
}

// =============================================================================
// Zeroblob Function
// =============================================================================

#[divan::bench]
fn zeroblob_small(bencher: Bencher) {
    let value = Value::Integer(10);
    bencher.bench_local(|| black_box(&value).exec_zeroblob().unwrap());
}

#[divan::bench]
fn zeroblob_medium(bencher: Bencher) {
    let value = Value::Integer(1000);
    bencher.bench_local(|| black_box(&value).exec_zeroblob().unwrap());
}

#[divan::bench]
fn zeroblob_large(bencher: Bencher) {
    let value = Value::Integer(10000);
    bencher.bench_local(|| black_box(&value).exec_zeroblob().unwrap());
}

// =============================================================================
// If/Conditional Function
// =============================================================================

#[divan::bench]
fn exec_if_true(bencher: Bencher) {
    let value = Value::Integer(1);
    bencher.bench_local(|| black_box(&value).exec_if(false, false));
}

#[divan::bench]
fn exec_if_false(bencher: Bencher) {
    let value = Value::Integer(0);
    bencher.bench_local(|| black_box(&value).exec_if(false, false));
}

#[divan::bench]
fn exec_if_null(bencher: Bencher) {
    let value = Value::Null;
    bencher.bench_local(|| black_box(&value).exec_if(true, false));
}

#[divan::bench]
fn exec_if_not(bencher: Bencher) {
    let value = Value::Integer(1);
    bencher.bench_local(|| black_box(&value).exec_if(false, true));
}

// =============================================================================
// LIKE Pattern (construct_like_regex)
// =============================================================================

#[divan::bench]
fn construct_like_regex_simple(bencher: Bencher) {
    bencher.bench_local(|| construct_like_regex(black_box("hello")));
}

#[divan::bench]
fn construct_like_regex_with_percent(bencher: Bencher) {
    bencher.bench_local(|| construct_like_regex(black_box("%hello%")));
}

#[divan::bench]
fn construct_like_regex_with_underscore(bencher: Bencher) {
    bencher.bench_local(|| construct_like_regex(black_box("h_llo")));
}

#[divan::bench]
fn construct_like_regex_complex(bencher: Bencher) {
    bencher.bench_local(|| construct_like_regex(black_box("%h_llo%w_rld%")));
}

// =============================================================================
// exec_like with caching
// =============================================================================

#[divan::bench]
fn exec_like_no_cache(bencher: Bencher) {
    bencher.bench_local(|| {
        Value::exec_like(None, black_box("hello%"), black_box("hello world")).unwrap()
    });
}

#[divan::bench]
fn exec_like_with_cache_miss(bencher: Bencher) {
    bencher.bench_local(|| {
        let mut cache = HashMap::new();
        Value::exec_like(
            Some(&mut cache),
            black_box("hello%"),
            black_box("hello world"),
        )
        .unwrap()
    });
}

#[divan::bench]
fn exec_like_with_cache_hit(bencher: Bencher) {
    let mut cache = HashMap::new();
    // Warm up the cache
    let _ = Value::exec_like(Some(&mut cache), "hello%", "hello world");

    bencher.bench_local(|| {
        Value::exec_like(
            Some(black_box(&mut cache)),
            black_box("hello%"),
            black_box("hello world"),
        )
        .unwrap()
    });
}

#[divan::bench]
fn exec_like_fallback_no_cache(bencher: Bencher) {
    bencher.bench_local(|| {
        Value::exec_like(None, black_box("hello_world"), black_box("hello world")).unwrap()
    });
}

#[divan::bench]
fn exec_like_fallback_with_cache_miss(bencher: Bencher) {
    bencher.bench_local(|| {
        let mut cache = HashMap::new();
        Value::exec_like(
            Some(&mut cache),
            black_box("hello_world"),
            black_box("hello world"),
        )
        .unwrap()
    });
}

#[divan::bench]
fn exec_like_fallback_with_cache_hit(bencher: Bencher) {
    let mut cache = HashMap::new();
    // Warm up the cache with the regex pattern
    let _ = Value::exec_like(Some(&mut cache), "hello_world", "hello world").unwrap();

    bencher.bench_local(|| {
        Value::exec_like(
            Some(black_box(&mut cache)),
            black_box("hello_world"),
            black_box("hello world"),
        )
        .unwrap()
    });
}

// =============================================================================
// Random Functions
// =============================================================================

#[divan::bench]
fn exec_random(bencher: Bencher) {
    bencher.bench_local(|| Value::exec_random(|| 42));
}

#[divan::bench]
fn exec_randomblob_small(bencher: Bencher) {
    let length = Value::Integer(10);
    bencher.bench_local(|| black_box(&length).exec_randomblob(|buf| buf.fill(0)).unwrap());
}

#[divan::bench]
fn exec_randomblob_medium(bencher: Bencher) {
    let length = Value::Integer(100);
    bencher.bench_local(|| black_box(&length).exec_randomblob(|buf| buf.fill(0)).unwrap());
}

#[divan::bench]
fn exec_randomblob_large(bencher: Bencher) {
    let length = Value::Integer(1000);
    bencher.bench_local(|| black_box(&length).exec_randomblob(|buf| buf.fill(0)).unwrap());
}
