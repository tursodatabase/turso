use divan::{black_box, Bencher};
use turso_core::types::Value;

// =============================================================================
// String Case Functions
// =============================================================================

#[divan::bench]
fn lower_short_string(bencher: Bencher) {
    let value = Value::build_text("HELLO");
    bencher.bench_local(|| black_box(black_box(&value).exec_lower()));
}

#[divan::bench]
fn lower_long_string(bencher: Bencher) {
    let value = Value::build_text("THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG");
    bencher.bench_local(|| black_box(black_box(&value).exec_lower()));
}

#[divan::bench]
fn lower_integer(bencher: Bencher) {
    let value = Value::from_i64(12345);
    bencher.bench_local(|| black_box(black_box(&value).exec_lower()));
}

#[divan::bench]
fn upper_short_string(bencher: Bencher) {
    let value = Value::build_text("hello");
    bencher.bench_local(|| black_box(black_box(&value).exec_upper()));
}

#[divan::bench]
fn upper_long_string(bencher: Bencher) {
    let value = Value::build_text("the quick brown fox jumps over the lazy dog");
    bencher.bench_local(|| black_box(black_box(&value).exec_upper()));
}

#[divan::bench]
fn upper_integer(bencher: Bencher) {
    let value = Value::from_i64(12345);
    bencher.bench_local(|| black_box(black_box(&value).exec_upper()));
}

// =============================================================================
// Length Functions
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn length_short_text(bencher: Bencher) {
    let value = Value::build_text("hello");
    bencher.bench_local(|| black_box(black_box(&value).exec_length()));
}

#[divan::bench]
fn length_long_text(bencher: Bencher) {
    let value = Value::build_text("the quick brown fox jumps over the lazy dog");
    bencher.bench_local(|| black_box(black_box(&value).exec_length()));
}

#[divan::bench]
fn length_unicode_text(bencher: Bencher) {
    let value = Value::build_text("héllo wörld 你好世界");
    bencher.bench_local(|| black_box(black_box(&value).exec_length()));
}

#[divan::bench]
fn length_integer(bencher: Bencher) {
    let value = Value::from_i64(123456789);
    bencher.bench_local(|| black_box(black_box(&value).exec_length()));
}

#[divan::bench]
fn length_float(bencher: Bencher) {
    let value = Value::from_f64(123.456789);
    bencher.bench_local(|| black_box(black_box(&value).exec_length()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn length_blob(bencher: Bencher) {
    let value = Value::Blob(vec![0u8; 100]);
    bencher.bench_local(|| black_box(black_box(&value).exec_length()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn octet_length_text(bencher: Bencher) {
    let value = Value::build_text("héllo wörld");
    bencher.bench_local(|| black_box(black_box(&value).exec_octet_length()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn octet_length_unicode(bencher: Bencher) {
    let value = Value::build_text("你好世界");
    bencher.bench_local(|| black_box(black_box(&value).exec_octet_length()));
}

// =============================================================================
// Trim Functions
// =============================================================================

#[divan::bench]
fn trim_spaces(bencher: Bencher) {
    let value = Value::build_text("     hello world     ");
    bencher.bench_local(|| black_box(black_box(&value).exec_trim(None)));
}

#[divan::bench]
fn trim_with_pattern(bencher: Bencher) {
    let value = Value::build_text("xxxhello worldxxx");
    let pattern = Value::build_text("x");
    bencher.bench_local(|| black_box(black_box(&value).exec_trim(Some(black_box(&pattern)))));
}

#[divan::bench]
fn ltrim_spaces(bencher: Bencher) {
    let value = Value::build_text("     hello world");
    bencher.bench_local(|| black_box(black_box(&value).exec_ltrim(None)));
}

#[divan::bench]
fn ltrim_with_pattern(bencher: Bencher) {
    let value = Value::build_text("xxxhello world");
    let pattern = Value::build_text("x");
    bencher.bench_local(|| black_box(black_box(&value).exec_ltrim(Some(black_box(&pattern)))));
}

#[divan::bench]
fn rtrim_spaces(bencher: Bencher) {
    let value = Value::build_text("hello world     ");
    bencher.bench_local(|| black_box(black_box(&value).exec_rtrim(None)));
}

#[divan::bench]
fn rtrim_with_pattern(bencher: Bencher) {
    let value = Value::build_text("hello worldxxx");
    let pattern = Value::build_text("x");
    bencher.bench_local(|| black_box(black_box(&value).exec_rtrim(Some(black_box(&pattern)))));
}

// =============================================================================
// Substring Function
// =============================================================================

#[divan::bench]
fn substring_simple(bencher: Bencher) {
    let value = Value::build_text("hello world");
    let start = Value::from_i64(1);
    let length = Value::from_i64(5);
    bencher.bench_local(|| {
        black_box(Value::exec_substring(
            black_box(&value),
            black_box(&start),
            Some(black_box(&length)),
        ))
    });
}

#[divan::bench]
fn substring_long_text(bencher: Bencher) {
    let value = Value::build_text("the quick brown fox jumps over the lazy dog");
    let start = Value::from_i64(5);
    let length = Value::from_i64(15);
    bencher.bench_local(|| {
        black_box(Value::exec_substring(
            black_box(&value),
            black_box(&start),
            Some(black_box(&length)),
        ))
    });
}

#[divan::bench]
fn substring_unicode(bencher: Bencher) {
    let value = Value::build_text("héllo wörld 你好");
    let start = Value::from_i64(1);
    let length = Value::from_i64(10);
    bencher.bench_local(|| {
        black_box(Value::exec_substring(
            black_box(&value),
            black_box(&start),
            Some(black_box(&length)),
        ))
    });
}

#[divan::bench]
fn substring_negative_start(bencher: Bencher) {
    let value = Value::build_text("hello world");
    let start = Value::from_i64(-5);
    let length = Value::from_i64(5);
    bencher.bench_local(|| {
        black_box(Value::exec_substring(
            black_box(&value),
            black_box(&start),
            Some(black_box(&length)),
        ))
    });
}

#[divan::bench]
fn substring_blob(bencher: Bencher) {
    let value = Value::Blob(b"hello world".to_vec());
    let start = Value::from_i64(1);
    let length = Value::from_i64(5);
    bencher.bench_local(|| {
        black_box(Value::exec_substring(
            black_box(&value),
            black_box(&start),
            Some(black_box(&length)),
        ))
    });
}

// =============================================================================
// Instr Function
// =============================================================================

#[divan::bench]
fn instr_found_early(bencher: Bencher) {
    let value = Value::build_text("hello world");
    let pattern = Value::build_text("ell");
    bencher.bench_local(|| black_box(black_box(&value).exec_instr(black_box(&pattern))));
}

#[divan::bench]
fn instr_found_late(bencher: Bencher) {
    let value = Value::build_text("the quick brown fox jumps over the lazy dog");
    let pattern = Value::build_text("dog");
    bencher.bench_local(|| black_box(black_box(&value).exec_instr(black_box(&pattern))));
}

#[divan::bench]
fn instr_not_found(bencher: Bencher) {
    let value = Value::build_text("hello world");
    let pattern = Value::build_text("xyz");
    bencher.bench_local(|| black_box(black_box(&value).exec_instr(black_box(&pattern))));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn instr_blob(bencher: Bencher) {
    let value = Value::Blob(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let pattern = Value::Blob(vec![5, 6, 7]);
    bencher.bench_local(|| black_box(black_box(&value).exec_instr(black_box(&pattern))));
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
        black_box(Value::exec_replace(
            black_box(&source),
            black_box(&pattern),
            black_box(&replacement),
        ))
    });
}

#[divan::bench]
fn replace_multiple_occurrences(bencher: Bencher) {
    let source = Value::build_text("banana banana banana");
    let pattern = Value::build_text("banana");
    let replacement = Value::build_text("apple");
    bencher.bench_local(|| {
        black_box(Value::exec_replace(
            black_box(&source),
            black_box(&pattern),
            black_box(&replacement),
        ))
    });
}

#[divan::bench]
fn replace_empty_pattern(bencher: Bencher) {
    let source = Value::build_text("hello world");
    let pattern = Value::build_text("");
    let replacement = Value::build_text("x");
    bencher.bench_local(|| {
        black_box(Value::exec_replace(
            black_box(&source),
            black_box(&pattern),
            black_box(&replacement),
        ))
    });
}

// =============================================================================
// Quote Function
// =============================================================================

#[divan::bench]
fn quote_text(bencher: Bencher) {
    let value = Value::build_text("hello world");
    bencher.bench_local(|| black_box(black_box(&value).exec_quote()));
}

#[divan::bench]
fn quote_text_with_quotes(bencher: Bencher) {
    let value = Value::build_text("hello'world");
    bencher.bench_local(|| black_box(black_box(&value).exec_quote()));
}

#[divan::bench]
fn quote_integer(bencher: Bencher) {
    let value = Value::from_i64(12345);
    bencher.bench_local(|| black_box(black_box(&value).exec_quote()));
}

#[divan::bench]
fn quote_blob(bencher: Bencher) {
    let value = Value::Blob(vec![0x01, 0x02, 0xAB, 0xCD, 0xEF]);
    bencher.bench_local(|| black_box(black_box(&value).exec_quote()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn quote_null(bencher: Bencher) {
    let value = Value::Null;
    bencher.bench_local(|| black_box(black_box(&value).exec_quote()));
}

// =============================================================================
// Soundex Function
// =============================================================================

#[divan::bench]
fn soundex_simple(bencher: Bencher) {
    let value = Value::build_text("Robert");
    bencher.bench_local(|| black_box(black_box(&value).exec_soundex()));
}

#[divan::bench]
fn soundex_complex(bencher: Bencher) {
    let value = Value::build_text("Ashcraft");
    bencher.bench_local(|| black_box(black_box(&value).exec_soundex()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn soundex_non_ascii(bencher: Bencher) {
    let value = Value::build_text("闪电五连鞭");
    bencher.bench_local(|| black_box(black_box(&value).exec_soundex()));
}

// =============================================================================
// Type Functions
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn typeof_integer(bencher: Bencher) {
    let value = Value::from_i64(12345);
    bencher.bench_local(|| black_box(black_box(&value).exec_typeof()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn typeof_float(bencher: Bencher) {
    let value = Value::from_f64(123.456);
    bencher.bench_local(|| black_box(black_box(&value).exec_typeof()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn typeof_text(bencher: Bencher) {
    let value = Value::build_text("hello");
    bencher.bench_local(|| black_box(black_box(&value).exec_typeof()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn typeof_blob(bencher: Bencher) {
    let value = Value::Blob(vec![1, 2, 3]);
    bencher.bench_local(|| black_box(black_box(&value).exec_typeof()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn typeof_null(bencher: Bencher) {
    let value = Value::Null;
    bencher.bench_local(|| black_box(black_box(&value).exec_typeof()));
}

// =============================================================================
// Cast Function
// =============================================================================

#[divan::bench]
fn cast_integer_to_text(bencher: Bencher) {
    let value = Value::from_i64(12345);
    bencher.bench_local(|| black_box(black_box(&value).exec_cast("TEXT")));
}

#[divan::bench]
fn cast_float_to_integer(bencher: Bencher) {
    let value = Value::from_f64(123.456);
    bencher.bench_local(|| black_box(black_box(&value).exec_cast("INT")));
}

#[divan::bench]
fn cast_text_to_integer(bencher: Bencher) {
    let value = Value::build_text("12345");
    bencher.bench_local(|| black_box(black_box(&value).exec_cast("INT")));
}

#[divan::bench]
fn cast_text_to_real(bencher: Bencher) {
    let value = Value::build_text("123.456");
    bencher.bench_local(|| black_box(black_box(&value).exec_cast("REAL")));
}

#[divan::bench]
fn cast_text_to_blob(bencher: Bencher) {
    let value = Value::build_text("hello world");
    bencher.bench_local(|| black_box(black_box(&value).exec_cast("BLOB")));
}

#[divan::bench]
fn cast_text_to_numeric(bencher: Bencher) {
    let value = Value::build_text("123.456");
    bencher.bench_local(|| black_box(black_box(&value).exec_cast("NUMERIC")));
}

// =============================================================================
// Hex/Unhex Functions
// =============================================================================

#[divan::bench]
fn hex_text(bencher: Bencher) {
    let value = Value::build_text("hello");
    bencher.bench_local(|| black_box(black_box(&value).exec_hex()));
}

#[divan::bench]
fn hex_blob(bencher: Bencher) {
    let value = Value::Blob(vec![0x01, 0x02, 0xAB, 0xCD, 0xEF]);
    bencher.bench_local(|| black_box(black_box(&value).exec_hex()));
}

#[divan::bench]
fn hex_integer(bencher: Bencher) {
    let value = Value::from_i64(255);
    bencher.bench_local(|| black_box(black_box(&value).exec_hex()));
}

#[divan::bench]
fn unhex_valid(bencher: Bencher) {
    let value = Value::build_text("48656C6C6F");
    bencher.bench_local(|| black_box(black_box(&value).exec_unhex(None)));
}

#[divan::bench]
fn unhex_with_ignored(bencher: Bencher) {
    let value = Value::build_text("  48656C6C6F  ");
    let ignore = Value::build_text(" ");
    bencher.bench_local(|| black_box(black_box(&value).exec_unhex(Some(black_box(&ignore)))));
}

// =============================================================================
// Unicode Function
// =============================================================================

#[divan::bench]
fn unicode_ascii(bencher: Bencher) {
    let value = Value::build_text("A");
    bencher.bench_local(|| black_box(black_box(&value).exec_unicode()));
}

#[divan::bench]
fn unicode_emoji(bencher: Bencher) {
    let value = Value::build_text("😊");
    bencher.bench_local(|| black_box(black_box(&value).exec_unicode()));
}

#[divan::bench]
fn unicode_cjk(bencher: Bencher) {
    let value = Value::build_text("你");
    bencher.bench_local(|| black_box(black_box(&value).exec_unicode()));
}

// =============================================================================
// Numeric Functions
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn abs_positive_integer(bencher: Bencher) {
    let value = Value::from_i64(12345);
    bencher.bench_local(|| black_box(black_box(&value).exec_abs()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn abs_negative_integer(bencher: Bencher) {
    let value = Value::from_i64(-12345);
    bencher.bench_local(|| black_box(black_box(&value).exec_abs()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn abs_float(bencher: Bencher) {
    let value = Value::from_f64(-123.456);
    bencher.bench_local(|| black_box(black_box(&value).exec_abs()));
}

#[divan::bench]
fn abs_text_numeric(bencher: Bencher) {
    let value = Value::build_text("-123.456");
    bencher.bench_local(|| black_box(black_box(&value).exec_abs()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn sign_positive(bencher: Bencher) {
    let value = Value::from_i64(42);
    bencher.bench_local(|| black_box(black_box(&value).exec_sign()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn sign_negative(bencher: Bencher) {
    let value = Value::from_i64(-42);
    bencher.bench_local(|| black_box(black_box(&value).exec_sign()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn sign_zero(bencher: Bencher) {
    let value = Value::from_i64(0);
    bencher.bench_local(|| black_box(black_box(&value).exec_sign()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn sign_float(bencher: Bencher) {
    let value = Value::from_f64(-42.5);
    bencher.bench_local(|| black_box(black_box(&value).exec_sign()));
}

// =============================================================================
// Round Function
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn round_no_precision(bencher: Bencher) {
    let value = Value::from_f64(123.456);
    bencher.bench_local(|| black_box(black_box(&value).exec_round(None)));
}

#[divan::bench]
fn round_with_precision(bencher: Bencher) {
    let value = Value::from_f64(123.456789);
    let precision = Value::from_i64(2);
    bencher.bench_local(|| black_box(black_box(&value).exec_round(Some(black_box(&precision)))));
}

#[divan::bench]
fn round_high_precision(bencher: Bencher) {
    let value = Value::from_f64(std::f64::consts::PI);
    let precision = Value::from_i64(10);
    bencher.bench_local(|| black_box(black_box(&value).exec_round(Some(black_box(&precision)))));
}

// =============================================================================
// Log Function
// =============================================================================

#[divan::bench]
fn log_base_10(bencher: Bencher) {
    let value = Value::from_f64(100.0);
    bencher.bench_local(|| black_box(black_box(&value).exec_math_log(None)));
}

#[divan::bench]
fn log_base_2(bencher: Bencher) {
    let value = Value::from_f64(8.0);
    let base = Value::from_f64(2.0);
    bencher.bench_local(|| black_box(black_box(&value).exec_math_log(Some(black_box(&base)))));
}

#[divan::bench]
fn log_arbitrary_base(bencher: Bencher) {
    let value = Value::from_f64(100.0);
    let base = Value::from_f64(7.0);
    bencher.bench_local(|| black_box(black_box(&value).exec_math_log(Some(black_box(&base)))));
}

// =============================================================================
// Arithmetic Operations
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn add_integers(bencher: Bencher) {
    let a = Value::from_i64(1000);
    let b = Value::from_i64(2000);
    bencher.bench_local(|| black_box(black_box(&a).exec_add(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn add_floats(bencher: Bencher) {
    let a = Value::from_f64(100.5);
    let b = Value::from_f64(200.5);
    bencher.bench_local(|| black_box(black_box(&a).exec_add(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn add_mixed(bencher: Bencher) {
    let a = Value::from_i64(100);
    let b = Value::from_f64(200.5);
    bencher.bench_local(|| black_box(black_box(&a).exec_add(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn subtract_integers(bencher: Bencher) {
    let a = Value::from_i64(2000);
    let b = Value::from_i64(1000);
    bencher.bench_local(|| black_box(black_box(&a).exec_subtract(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn multiply_integers(bencher: Bencher) {
    let a = Value::from_i64(100);
    let b = Value::from_i64(200);
    bencher.bench_local(|| black_box(black_box(&a).exec_multiply(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn divide_integers(bencher: Bencher) {
    let a = Value::from_i64(1000);
    let b = Value::from_i64(10);
    bencher.bench_local(|| black_box(black_box(&a).exec_divide(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn remainder_integers(bencher: Bencher) {
    let a = Value::from_i64(17);
    let b = Value::from_i64(5);
    bencher.bench_local(|| black_box(black_box(&a).exec_remainder(black_box(&b))));
}

// =============================================================================
// Bitwise Operations
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn bit_and(bencher: Bencher) {
    let a = Value::from_i64(0b11110000);
    let b = Value::from_i64(0b10101010);
    bencher.bench_local(|| black_box(black_box(&a).exec_bit_and(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn bit_or(bencher: Bencher) {
    let a = Value::from_i64(0b11110000);
    let b = Value::from_i64(0b00001111);
    bencher.bench_local(|| black_box(black_box(&a).exec_bit_or(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn bit_not(bencher: Bencher) {
    let a = Value::from_i64(0b11110000);
    bencher.bench_local(|| black_box(black_box(&a).exec_bit_not()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn shift_left(bencher: Bencher) {
    let a = Value::from_i64(1);
    let b = Value::from_i64(8);
    bencher.bench_local(|| black_box(black_box(&a).exec_shift_left(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn shift_right(bencher: Bencher) {
    let a = Value::from_i64(256);
    let b = Value::from_i64(4);
    bencher.bench_local(|| black_box(black_box(&a).exec_shift_right(black_box(&b))));
}

// =============================================================================
// Boolean Operations
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn boolean_not_true(bencher: Bencher) {
    let value = Value::from_i64(1);
    bencher.bench_local(|| black_box(black_box(&value).exec_boolean_not()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn boolean_not_false(bencher: Bencher) {
    let value = Value::from_i64(0);
    bencher.bench_local(|| black_box(black_box(&value).exec_boolean_not()));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn and_true_true(bencher: Bencher) {
    let a = Value::from_i64(1);
    let b = Value::from_i64(1);
    bencher.bench_local(|| black_box(black_box(&a).exec_and(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn and_true_false(bencher: Bencher) {
    let a = Value::from_i64(1);
    let b = Value::from_i64(0);
    bencher.bench_local(|| black_box(black_box(&a).exec_and(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn or_false_false(bencher: Bencher) {
    let a = Value::from_i64(0);
    let b = Value::from_i64(0);
    bencher.bench_local(|| black_box(black_box(&a).exec_or(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn or_true_false(bencher: Bencher) {
    let a = Value::from_i64(1);
    let b = Value::from_i64(0);
    bencher.bench_local(|| black_box(black_box(&a).exec_or(black_box(&b))));
}

// =============================================================================
// Concat Functions
// =============================================================================

#[divan::bench]
fn concat_two_strings(bencher: Bencher) {
    let a = Value::build_text("hello ");
    let b = Value::build_text("world");
    bencher.bench_local(|| black_box(black_box(&a).exec_concat(black_box(&b))));
}

#[divan::bench]
fn concat_string_integer(bencher: Bencher) {
    let a = Value::build_text("count: ");
    let b = Value::from_i64(42);
    bencher.bench_local(|| black_box(black_box(&a).exec_concat(black_box(&b))));
}

#[divan::bench]
fn concat_blobs(bencher: Bencher) {
    let a = Value::Blob(b"hello ".to_vec());
    let b = Value::Blob(b"world".to_vec());
    bencher.bench_local(|| black_box(black_box(&a).exec_concat(black_box(&b))));
}

#[divan::bench]
fn concat_strings_multiple(bencher: Bencher) {
    let values = [
        Value::build_text("the "),
        Value::build_text("quick "),
        Value::build_text("brown "),
        Value::build_text("fox"),
    ];
    bencher.bench_local(|| black_box(Value::exec_concat_strings(black_box(values.iter()))));
}

#[divan::bench]
fn concat_ws_strings(bencher: Bencher) {
    let values = [
        Value::build_text(", "),
        Value::build_text("apple"),
        Value::build_text("banana"),
        Value::build_text("cherry"),
    ];
    bencher.bench_local(|| black_box(Value::exec_concat_ws(black_box(values.iter()))));
}

// =============================================================================
// Char Function
// =============================================================================

#[divan::bench]
fn char_single(bencher: Bencher) {
    let values = [Value::from_i64(65)];
    bencher.bench_local(|| black_box(Value::exec_char(black_box(values.iter()))));
}

#[divan::bench]
fn char_multiple(bencher: Bencher) {
    let values = [
        Value::from_i64(72),
        Value::from_i64(101),
        Value::from_i64(108),
        Value::from_i64(108),
        Value::from_i64(111),
    ];
    bencher.bench_local(|| black_box(Value::exec_char(black_box(values.iter()))));
}

// =============================================================================
// Min/Max Functions
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn min_integers(bencher: Bencher) {
    let values = [
        Value::from_i64(5),
        Value::from_i64(3),
        Value::from_i64(8),
        Value::from_i64(1),
        Value::from_i64(9),
    ];
    bencher.bench_local(|| black_box(Value::exec_min(black_box(values.iter()))));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn max_integers(bencher: Bencher) {
    let values = [
        Value::from_i64(5),
        Value::from_i64(3),
        Value::from_i64(8),
        Value::from_i64(1),
        Value::from_i64(9),
    ];
    bencher.bench_local(|| black_box(Value::exec_max(black_box(values.iter()))));
}

#[divan::bench]
fn min_strings(bencher: Bencher) {
    let values = [
        Value::build_text("banana"),
        Value::build_text("apple"),
        Value::build_text("cherry"),
    ];
    bencher.bench_local(|| black_box(Value::exec_min(black_box(values.iter()))));
}

#[divan::bench]
fn max_strings(bencher: Bencher) {
    let values = [
        Value::build_text("banana"),
        Value::build_text("apple"),
        Value::build_text("cherry"),
    ];
    bencher.bench_local(|| black_box(Value::exec_max(black_box(values.iter()))));
}

// =============================================================================
// Nullif Function
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn nullif_equal(bencher: Bencher) {
    let a = Value::from_i64(42);
    let b = Value::from_i64(42);
    bencher.bench_local(|| black_box(black_box(&a).exec_nullif(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn nullif_not_equal(bencher: Bencher) {
    let a = Value::from_i64(42);
    let b = Value::from_i64(100);
    bencher.bench_local(|| black_box(black_box(&a).exec_nullif(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn nullif_strings(bencher: Bencher) {
    let a = Value::build_text("hello");
    let b = Value::build_text("hello");
    bencher.bench_local(|| black_box(black_box(&a).exec_nullif(black_box(&b))));
}

// =============================================================================
// Zeroblob Function
// =============================================================================

#[divan::bench]
fn zeroblob_small(bencher: Bencher) {
    let value = Value::from_i64(10);
    bencher.bench_local(|| black_box(black_box(&value).exec_zeroblob().unwrap()));
}

#[divan::bench]
fn zeroblob_medium(bencher: Bencher) {
    let value = Value::from_i64(1000);
    bencher.bench_local(|| black_box(black_box(&value).exec_zeroblob().unwrap()));
}

#[divan::bench]
fn zeroblob_large(bencher: Bencher) {
    let value = Value::from_i64(10000);
    bencher.bench_local(|| black_box(black_box(&value).exec_zeroblob().unwrap()));
}

// =============================================================================
// If/Conditional Function
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn exec_if_true(bencher: Bencher) {
    let value = Value::from_i64(1);
    bencher.bench_local(|| black_box(black_box(&value).exec_if(false, false)));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn exec_if_false(bencher: Bencher) {
    let value = Value::from_i64(0);
    bencher.bench_local(|| black_box(black_box(&value).exec_if(false, false)));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn exec_if_null(bencher: Bencher) {
    let value = Value::Null;
    bencher.bench_local(|| black_box(black_box(&value).exec_if(true, false)));
}

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn exec_if_not(bencher: Bencher) {
    let value = Value::from_i64(1);
    bencher.bench_local(|| black_box(black_box(&value).exec_if(false, true)));
}

// =============================================================================
// LIKE Pattern
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn construct_like_exact(bencher: Bencher) {
    bencher.bench_local(|| {
        black_box(Value::exec_like(
            black_box("hello"),
            black_box("hello"),
            None,
        ))
        .unwrap()
    });
}

#[divan::bench]
fn construct_like_contains(bencher: Bencher) {
    bencher.bench_local(|| {
        black_box(Value::exec_like(
            black_box("%hello%"),
            black_box("hello"),
            None,
        ))
        .unwrap()
    });
}

#[divan::bench]
fn construct_like_with_single_wildcard(bencher: Bencher) {
    bencher.bench_local(|| {
        black_box(Value::exec_like(
            black_box("h_llo"),
            black_box("hello"),
            None,
        ))
        .unwrap()
    });
}

#[divan::bench]
fn construct_like_complex(bencher: Bencher) {
    bencher.bench_local(|| {
        black_box(Value::exec_like(
            black_box("%h_llo%w_rld%"),
            black_box("hello world"),
            None,
        ))
        .unwrap()
    });
}

// =============================================================================
// Random Functions
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[divan::bench]
fn exec_random(bencher: Bencher) {
    bencher.bench_local(|| black_box(Value::exec_random(|| 42)));
}

#[divan::bench]
fn exec_randomblob_small(bencher: Bencher) {
    let length = Value::from_i64(10);
    bencher.bench_local(|| {
        black_box(
            black_box(&length)
                .exec_randomblob(|buf| buf.fill(0))
                .unwrap(),
        )
    });
}

#[divan::bench]
fn exec_randomblob_medium(bencher: Bencher) {
    let length = Value::from_i64(100);
    bencher.bench_local(|| {
        black_box(
            black_box(&length)
                .exec_randomblob(|buf| buf.fill(0))
                .unwrap(),
        )
    });
}

#[divan::bench]
fn exec_randomblob_large(bencher: Bencher) {
    let length = Value::from_i64(1000);
    bencher.bench_local(|| {
        black_box(
            black_box(&length)
                .exec_randomblob(|buf| buf.fill(0))
                .unwrap(),
        )
    });
}
