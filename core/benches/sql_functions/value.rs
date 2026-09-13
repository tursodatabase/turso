use divan::{black_box, Bencher};
use turso_core::types::Value;

// =============================================================================
// String Case Functions
// =============================================================================

#[turso_macros::divan_bench]
fn lower_short_string(bencher: Bencher) {
    let value = Value::build_text("HELLO");
    bencher.bench_local(|| black_box(black_box(&value).exec_lower()));
}

#[turso_macros::divan_bench]
fn lower_long_string(bencher: Bencher) {
    let value = Value::build_text("THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG");
    bencher.bench_local(|| black_box(black_box(&value).exec_lower()));
}

#[turso_macros::divan_bench]
fn lower_integer(bencher: Bencher) {
    let value = Value::from_i64(12345);
    bencher.bench_local(|| black_box(black_box(&value).exec_lower()));
}

#[turso_macros::divan_bench]
fn upper_short_string(bencher: Bencher) {
    let value = Value::build_text("hello");
    bencher.bench_local(|| black_box(black_box(&value).exec_upper()));
}

#[turso_macros::divan_bench]
fn upper_long_string(bencher: Bencher) {
    let value = Value::build_text("the quick brown fox jumps over the lazy dog");
    bencher.bench_local(|| black_box(black_box(&value).exec_upper()));
}

#[turso_macros::divan_bench]
fn upper_integer(bencher: Bencher) {
    let value = Value::from_i64(12345);
    bencher.bench_local(|| black_box(black_box(&value).exec_upper()));
}

// =============================================================================
// Length Functions
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn length_short_text(bencher: Bencher) {
    let value = Value::build_text("hello");
    bencher.bench_local(|| black_box(black_box(&value).exec_length()));
}

#[turso_macros::divan_bench]
fn length_long_text(bencher: Bencher) {
    let value = Value::build_text("the quick brown fox jumps over the lazy dog");
    bencher.bench_local(|| black_box(black_box(&value).exec_length()));
}

#[turso_macros::divan_bench]
fn length_unicode_text(bencher: Bencher) {
    let value = Value::build_text("héllo wörld 你好世界");
    bencher.bench_local(|| black_box(black_box(&value).exec_length()));
}

#[turso_macros::divan_bench]
fn length_integer(bencher: Bencher) {
    let value = Value::from_i64(123456789);
    bencher.bench_local(|| black_box(black_box(&value).exec_length()));
}

#[turso_macros::divan_bench]
fn length_float(bencher: Bencher) {
    let value = Value::from_f64(123.456789);
    bencher.bench_local(|| black_box(black_box(&value).exec_length()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn length_blob(bencher: Bencher) {
    let value = Value::from_slice(&[0u8; 100]).expect(turso_core::alloc::ALLOC_ERR_MSG);
    bencher.bench_local(|| black_box(black_box(&value).exec_length()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn octet_length_text(bencher: Bencher) {
    let value = Value::build_text("héllo wörld");
    bencher.bench_local(|| black_box(black_box(&value).exec_octet_length()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn octet_length_unicode(bencher: Bencher) {
    let value = Value::build_text("你好世界");
    bencher.bench_local(|| black_box(black_box(&value).exec_octet_length()));
}

// =============================================================================
// Trim Functions
// =============================================================================

#[turso_macros::divan_bench]
fn trim_spaces(bencher: Bencher) {
    let value = Value::build_text("     hello world     ");
    bencher.bench_local(|| black_box(black_box(&value).exec_trim(None)));
}

#[turso_macros::divan_bench]
fn trim_with_pattern(bencher: Bencher) {
    let value = Value::build_text("xxxhello worldxxx");
    let pattern = Value::build_text("x");
    bencher.bench_local(|| black_box(black_box(&value).exec_trim(Some(black_box(&pattern)))));
}

#[turso_macros::divan_bench]
fn ltrim_spaces(bencher: Bencher) {
    let value = Value::build_text("     hello world");
    bencher.bench_local(|| black_box(black_box(&value).exec_ltrim(None)));
}

#[turso_macros::divan_bench]
fn ltrim_with_pattern(bencher: Bencher) {
    let value = Value::build_text("xxxhello world");
    let pattern = Value::build_text("x");
    bencher.bench_local(|| black_box(black_box(&value).exec_ltrim(Some(black_box(&pattern)))));
}

#[turso_macros::divan_bench]
fn rtrim_spaces(bencher: Bencher) {
    let value = Value::build_text("hello world     ");
    bencher.bench_local(|| black_box(black_box(&value).exec_rtrim(None)));
}

#[turso_macros::divan_bench]
fn rtrim_with_pattern(bencher: Bencher) {
    let value = Value::build_text("hello worldxxx");
    let pattern = Value::build_text("x");
    bencher.bench_local(|| black_box(black_box(&value).exec_rtrim(Some(black_box(&pattern)))));
}

// =============================================================================
// Substring Function
// =============================================================================

#[turso_macros::divan_bench]
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

#[turso_macros::divan_bench]
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

#[turso_macros::divan_bench]
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

#[turso_macros::divan_bench]
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

#[turso_macros::divan_bench]
fn substring_blob(bencher: Bencher) {
    let value = Value::from_slice(b"hello world").expect(turso_core::alloc::ALLOC_ERR_MSG);
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

#[turso_macros::divan_bench]
fn instr_found_early(bencher: Bencher) {
    let value = Value::build_text("hello world");
    let pattern = Value::build_text("ell");
    bencher.bench_local(|| black_box(black_box(&value).exec_instr(black_box(&pattern))));
}

#[turso_macros::divan_bench]
fn instr_found_late(bencher: Bencher) {
    let value = Value::build_text("the quick brown fox jumps over the lazy dog");
    let pattern = Value::build_text("dog");
    bencher.bench_local(|| black_box(black_box(&value).exec_instr(black_box(&pattern))));
}

#[turso_macros::divan_bench]
fn instr_not_found(bencher: Bencher) {
    let value = Value::build_text("hello world");
    let pattern = Value::build_text("xyz");
    bencher.bench_local(|| black_box(black_box(&value).exec_instr(black_box(&pattern))));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn instr_blob(bencher: Bencher) {
    let value = Value::from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        .expect(turso_core::alloc::ALLOC_ERR_MSG);
    let pattern = Value::from_slice(&[5, 6, 7]).expect(turso_core::alloc::ALLOC_ERR_MSG);
    bencher.bench_local(|| black_box(black_box(&value).exec_instr(black_box(&pattern))));
}

// =============================================================================
// Replace Function
// =============================================================================

#[turso_macros::divan_bench]
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

#[turso_macros::divan_bench]
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

#[turso_macros::divan_bench]
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

#[turso_macros::divan_bench]
fn quote_text(bencher: Bencher) {
    let value = Value::build_text("hello world");
    bencher.bench_local(|| black_box(black_box(&value).exec_quote()));
}

#[turso_macros::divan_bench]
fn quote_text_with_quotes(bencher: Bencher) {
    let value = Value::build_text("hello'world");
    bencher.bench_local(|| black_box(black_box(&value).exec_quote()));
}

#[turso_macros::divan_bench]
fn quote_integer(bencher: Bencher) {
    let value = Value::from_i64(12345);
    bencher.bench_local(|| black_box(black_box(&value).exec_quote()));
}

#[turso_macros::divan_bench]
fn quote_blob(bencher: Bencher) {
    let value =
        Value::from_slice(&[0x01, 0x02, 0xAB, 0xCD, 0xEF]).expect(turso_core::alloc::ALLOC_ERR_MSG);
    bencher.bench_local(|| black_box(black_box(&value).exec_quote()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn quote_null(bencher: Bencher) {
    let value = Value::Null;
    bencher.bench_local(|| black_box(black_box(&value).exec_quote()));
}

// =============================================================================
// Soundex Function
// =============================================================================

#[turso_macros::divan_bench]
fn soundex_simple(bencher: Bencher) {
    let value = Value::build_text("Robert");
    bencher.bench_local(|| black_box(black_box(&value).exec_soundex()));
}

#[turso_macros::divan_bench]
fn soundex_complex(bencher: Bencher) {
    let value = Value::build_text("Ashcraft");
    bencher.bench_local(|| black_box(black_box(&value).exec_soundex()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn soundex_non_ascii(bencher: Bencher) {
    let value = Value::build_text("闪电五连鞭");
    bencher.bench_local(|| black_box(black_box(&value).exec_soundex()));
}

// =============================================================================
// Type Functions
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn typeof_integer(bencher: Bencher) {
    let value = Value::from_i64(12345);
    bencher.bench_local(|| black_box(black_box(&value).exec_typeof()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn typeof_float(bencher: Bencher) {
    let value = Value::from_f64(123.456);
    bencher.bench_local(|| black_box(black_box(&value).exec_typeof()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn typeof_text(bencher: Bencher) {
    let value = Value::build_text("hello");
    bencher.bench_local(|| black_box(black_box(&value).exec_typeof()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn typeof_blob(bencher: Bencher) {
    let value = Value::from_slice(&[1, 2, 3]).expect(turso_core::alloc::ALLOC_ERR_MSG);
    bencher.bench_local(|| black_box(black_box(&value).exec_typeof()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn typeof_null(bencher: Bencher) {
    let value = Value::Null;
    bencher.bench_local(|| black_box(black_box(&value).exec_typeof()));
}

// =============================================================================
// Cast Function
// =============================================================================

#[turso_macros::divan_bench]
fn cast_integer_to_text(bencher: Bencher) {
    let value = Value::from_i64(12345);
    bencher.bench_local(|| black_box(black_box(&value).exec_cast("TEXT")));
}

#[turso_macros::divan_bench]
fn cast_float_to_integer(bencher: Bencher) {
    let value = Value::from_f64(123.456);
    bencher.bench_local(|| black_box(black_box(&value).exec_cast("INT")));
}

#[turso_macros::divan_bench]
fn cast_text_to_integer(bencher: Bencher) {
    let value = Value::build_text("12345");
    bencher.bench_local(|| black_box(black_box(&value).exec_cast("INT")));
}

#[turso_macros::divan_bench]
fn cast_text_to_real(bencher: Bencher) {
    let value = Value::build_text("123.456");
    bencher.bench_local(|| black_box(black_box(&value).exec_cast("REAL")));
}

#[turso_macros::divan_bench]
fn cast_text_to_blob(bencher: Bencher) {
    let value = Value::build_text("hello world");
    bencher.bench_local(|| black_box(black_box(&value).exec_cast("BLOB")));
}

#[turso_macros::divan_bench]
fn cast_text_to_numeric(bencher: Bencher) {
    let value = Value::build_text("123.456");
    bencher.bench_local(|| black_box(black_box(&value).exec_cast("NUMERIC")));
}

// =============================================================================
// Hex/Unhex Functions
// =============================================================================

#[turso_macros::divan_bench]
fn hex_text(bencher: Bencher) {
    let value = Value::build_text("hello");
    bencher.bench_local(|| black_box(black_box(&value).exec_hex()));
}

#[turso_macros::divan_bench]
fn hex_blob(bencher: Bencher) {
    let value =
        Value::from_slice(&[0x01, 0x02, 0xAB, 0xCD, 0xEF]).expect(turso_core::alloc::ALLOC_ERR_MSG);
    bencher.bench_local(|| black_box(black_box(&value).exec_hex()));
}

#[turso_macros::divan_bench]
fn hex_integer(bencher: Bencher) {
    let value = Value::from_i64(255);
    bencher.bench_local(|| black_box(black_box(&value).exec_hex()));
}

#[turso_macros::divan_bench]
fn unhex_valid(bencher: Bencher) {
    let value = Value::build_text("48656C6C6F");
    bencher.bench_local(|| black_box(black_box(&value).exec_unhex(None)));
}

#[turso_macros::divan_bench]
fn unhex_with_ignored(bencher: Bencher) {
    let value = Value::build_text("  48656C6C6F  ");
    let ignore = Value::build_text(" ");
    bencher.bench_local(|| black_box(black_box(&value).exec_unhex(Some(black_box(&ignore)))));
}

// =============================================================================
// Unicode Function
// =============================================================================

#[turso_macros::divan_bench]
fn unicode_ascii(bencher: Bencher) {
    let value = Value::build_text("A");
    bencher.bench_local(|| black_box(black_box(&value).exec_unicode()));
}

#[turso_macros::divan_bench]
fn unicode_emoji(bencher: Bencher) {
    let value = Value::build_text("😊");
    bencher.bench_local(|| black_box(black_box(&value).exec_unicode()));
}

#[turso_macros::divan_bench]
fn unicode_cjk(bencher: Bencher) {
    let value = Value::build_text("你");
    bencher.bench_local(|| black_box(black_box(&value).exec_unicode()));
}

// =============================================================================
// Numeric Functions
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn abs_positive_integer(bencher: Bencher) {
    let value = Value::from_i64(12345);
    bencher.bench_local(|| black_box(black_box(&value).exec_abs()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn abs_negative_integer(bencher: Bencher) {
    let value = Value::from_i64(-12345);
    bencher.bench_local(|| black_box(black_box(&value).exec_abs()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn abs_float(bencher: Bencher) {
    let value = Value::from_f64(-123.456);
    bencher.bench_local(|| black_box(black_box(&value).exec_abs()));
}

#[turso_macros::divan_bench]
fn abs_text_numeric(bencher: Bencher) {
    let value = Value::build_text("-123.456");
    bencher.bench_local(|| black_box(black_box(&value).exec_abs()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn sign_positive(bencher: Bencher) {
    let value = Value::from_i64(42);
    bencher.bench_local(|| black_box(black_box(&value).exec_sign()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn sign_negative(bencher: Bencher) {
    let value = Value::from_i64(-42);
    bencher.bench_local(|| black_box(black_box(&value).exec_sign()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn sign_zero(bencher: Bencher) {
    let value = Value::from_i64(0);
    bencher.bench_local(|| black_box(black_box(&value).exec_sign()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn sign_float(bencher: Bencher) {
    let value = Value::from_f64(-42.5);
    bencher.bench_local(|| black_box(black_box(&value).exec_sign()));
}

// =============================================================================
// Round Function
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn round_no_precision(bencher: Bencher) {
    let value = Value::from_f64(123.456);
    bencher.bench_local(|| black_box(black_box(&value).exec_round(None)));
}

#[turso_macros::divan_bench]
fn round_with_precision(bencher: Bencher) {
    let value = Value::from_f64(123.456789);
    let precision = Value::from_i64(2);
    bencher.bench_local(|| black_box(black_box(&value).exec_round(Some(black_box(&precision)))));
}

#[turso_macros::divan_bench]
fn round_high_precision(bencher: Bencher) {
    let value = Value::from_f64(std::f64::consts::PI);
    let precision = Value::from_i64(10);
    bencher.bench_local(|| black_box(black_box(&value).exec_round(Some(black_box(&precision)))));
}

// =============================================================================
// Log Function
// =============================================================================

#[turso_macros::divan_bench]
fn log_base_10(bencher: Bencher) {
    let value = Value::from_f64(100.0);
    bencher.bench_local(|| black_box(black_box(&value).exec_math_log(None)));
}

#[turso_macros::divan_bench]
fn log_base_2(bencher: Bencher) {
    let value = Value::from_f64(8.0);
    let base = Value::from_f64(2.0);
    bencher.bench_local(|| black_box(black_box(&value).exec_math_log(Some(black_box(&base)))));
}

#[turso_macros::divan_bench]
fn log_arbitrary_base(bencher: Bencher) {
    let value = Value::from_f64(100.0);
    let base = Value::from_f64(7.0);
    bencher.bench_local(|| black_box(black_box(&value).exec_math_log(Some(black_box(&base)))));
}

// =============================================================================
// Arithmetic Operations
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn add_integers(bencher: Bencher) {
    let a = Value::from_i64(1000);
    let b = Value::from_i64(2000);
    bencher.bench_local(|| black_box(black_box(&a).exec_add(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn add_floats(bencher: Bencher) {
    let a = Value::from_f64(100.5);
    let b = Value::from_f64(200.5);
    bencher.bench_local(|| black_box(black_box(&a).exec_add(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn add_mixed(bencher: Bencher) {
    let a = Value::from_i64(100);
    let b = Value::from_f64(200.5);
    bencher.bench_local(|| black_box(black_box(&a).exec_add(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn subtract_integers(bencher: Bencher) {
    let a = Value::from_i64(2000);
    let b = Value::from_i64(1000);
    bencher.bench_local(|| black_box(black_box(&a).exec_subtract(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn multiply_integers(bencher: Bencher) {
    let a = Value::from_i64(100);
    let b = Value::from_i64(200);
    bencher.bench_local(|| black_box(black_box(&a).exec_multiply(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn divide_integers(bencher: Bencher) {
    let a = Value::from_i64(1000);
    let b = Value::from_i64(10);
    bencher.bench_local(|| black_box(black_box(&a).exec_divide(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn remainder_integers(bencher: Bencher) {
    let a = Value::from_i64(17);
    let b = Value::from_i64(5);
    bencher.bench_local(|| black_box(black_box(&a).exec_remainder(black_box(&b))));
}

// =============================================================================
// Bitwise Operations
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn bit_and(bencher: Bencher) {
    let a = Value::from_i64(0b11110000);
    let b = Value::from_i64(0b10101010);
    bencher.bench_local(|| black_box(black_box(&a).exec_bit_and(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn bit_or(bencher: Bencher) {
    let a = Value::from_i64(0b11110000);
    let b = Value::from_i64(0b00001111);
    bencher.bench_local(|| black_box(black_box(&a).exec_bit_or(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn bit_not(bencher: Bencher) {
    let a = Value::from_i64(0b11110000);
    bencher.bench_local(|| black_box(black_box(&a).exec_bit_not()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn shift_left(bencher: Bencher) {
    let a = Value::from_i64(1);
    let b = Value::from_i64(8);
    bencher.bench_local(|| black_box(black_box(&a).exec_shift_left(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn shift_right(bencher: Bencher) {
    let a = Value::from_i64(256);
    let b = Value::from_i64(4);
    bencher.bench_local(|| black_box(black_box(&a).exec_shift_right(black_box(&b))));
}

// =============================================================================
// Boolean Operations
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn boolean_not_true(bencher: Bencher) {
    let value = Value::from_i64(1);
    bencher.bench_local(|| black_box(black_box(&value).exec_boolean_not()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn boolean_not_false(bencher: Bencher) {
    let value = Value::from_i64(0);
    bencher.bench_local(|| black_box(black_box(&value).exec_boolean_not()));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn and_true_true(bencher: Bencher) {
    let a = Value::from_i64(1);
    let b = Value::from_i64(1);
    bencher.bench_local(|| black_box(black_box(&a).exec_and(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn and_true_false(bencher: Bencher) {
    let a = Value::from_i64(1);
    let b = Value::from_i64(0);
    bencher.bench_local(|| black_box(black_box(&a).exec_and(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn or_false_false(bencher: Bencher) {
    let a = Value::from_i64(0);
    let b = Value::from_i64(0);
    bencher.bench_local(|| black_box(black_box(&a).exec_or(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn or_true_false(bencher: Bencher) {
    let a = Value::from_i64(1);
    let b = Value::from_i64(0);
    bencher.bench_local(|| black_box(black_box(&a).exec_or(black_box(&b))));
}

// =============================================================================
// Concat Functions
// =============================================================================

#[turso_macros::divan_bench]
fn concat_two_strings(bencher: Bencher) {
    let a = Value::build_text("hello ");
    let b = Value::build_text("world");
    bencher.bench_local(|| black_box(black_box(&a).exec_concat(black_box(&b))));
}

#[turso_macros::divan_bench]
fn concat_string_integer(bencher: Bencher) {
    let a = Value::build_text("count: ");
    let b = Value::from_i64(42);
    bencher.bench_local(|| black_box(black_box(&a).exec_concat(black_box(&b))));
}

#[turso_macros::divan_bench]
fn concat_blobs(bencher: Bencher) {
    let a = Value::from_slice(b"hello ").expect(turso_core::alloc::ALLOC_ERR_MSG);
    let b = Value::from_slice(b"world").expect(turso_core::alloc::ALLOC_ERR_MSG);
    bencher.bench_local(|| black_box(black_box(&a).exec_concat(black_box(&b))));
}

#[turso_macros::divan_bench]
fn concat_strings_multiple(bencher: Bencher) {
    let values = [
        Value::build_text("the "),
        Value::build_text("quick "),
        Value::build_text("brown "),
        Value::build_text("fox"),
    ];
    bencher.bench_local(|| black_box(Value::exec_concat_strings(black_box(values.iter()))));
}

#[turso_macros::divan_bench]
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

#[turso_macros::divan_bench]
fn char_single(bencher: Bencher) {
    let values = [Value::from_i64(65)];
    bencher.bench_local(|| black_box(Value::exec_char(black_box(values.iter()))));
}

#[turso_macros::divan_bench]
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
#[turso_macros::divan_bench]
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
#[turso_macros::divan_bench]
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

#[turso_macros::divan_bench]
fn min_strings(bencher: Bencher) {
    let values = [
        Value::build_text("banana"),
        Value::build_text("apple"),
        Value::build_text("cherry"),
    ];
    bencher.bench_local(|| black_box(Value::exec_min(black_box(values.iter()))));
}

#[turso_macros::divan_bench]
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
#[turso_macros::divan_bench]
fn nullif_equal(bencher: Bencher) {
    let a = Value::from_i64(42);
    let b = Value::from_i64(42);
    bencher.bench_local(|| black_box(black_box(&a).exec_nullif(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn nullif_not_equal(bencher: Bencher) {
    let a = Value::from_i64(42);
    let b = Value::from_i64(100);
    bencher.bench_local(|| black_box(black_box(&a).exec_nullif(black_box(&b))));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn nullif_strings(bencher: Bencher) {
    let a = Value::build_text("hello");
    let b = Value::build_text("hello");
    bencher.bench_local(|| black_box(black_box(&a).exec_nullif(black_box(&b))));
}

// =============================================================================
// Zeroblob Function
// =============================================================================

#[turso_macros::divan_bench]
fn zeroblob_small(bencher: Bencher) {
    let value = Value::from_i64(10);
    bencher.bench_local(|| black_box(black_box(&value).exec_zeroblob().unwrap()));
}

#[turso_macros::divan_bench]
fn zeroblob_medium(bencher: Bencher) {
    let value = Value::from_i64(1000);
    bencher.bench_local(|| black_box(black_box(&value).exec_zeroblob().unwrap()));
}

#[turso_macros::divan_bench]
fn zeroblob_large(bencher: Bencher) {
    let value = Value::from_i64(10000);
    bencher.bench_local(|| black_box(black_box(&value).exec_zeroblob().unwrap()));
}

// =============================================================================
// If/Conditional Function
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn exec_if_true(bencher: Bencher) {
    let value = Value::from_i64(1);
    bencher.bench_local(|| black_box(black_box(&value).exec_if(false, false)));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn exec_if_false(bencher: Bencher) {
    let value = Value::from_i64(0);
    bencher.bench_local(|| black_box(black_box(&value).exec_if(false, false)));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn exec_if_null(bencher: Bencher) {
    let value = Value::Null;
    bencher.bench_local(|| black_box(black_box(&value).exec_if(true, false)));
}

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
fn exec_if_not(bencher: Bencher) {
    let value = Value::from_i64(1);
    bencher.bench_local(|| black_box(black_box(&value).exec_if(false, true)));
}

// =============================================================================
// LIKE Pattern
// =============================================================================

#[cfg(feature = "nanosecond-bench")]
#[turso_macros::divan_bench]
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

#[turso_macros::divan_bench]
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

#[turso_macros::divan_bench]
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

#[turso_macros::divan_bench]
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
#[turso_macros::divan_bench]
fn exec_random(bencher: Bencher) {
    bencher.bench_local(|| black_box(Value::exec_random(|| 42)));
}

#[turso_macros::divan_bench]
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

#[turso_macros::divan_bench]
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

#[turso_macros::divan_bench]
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
