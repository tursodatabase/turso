use divan::{black_box, Bencher};
use turso_core::numeric::{format_float, str_to_f64, str_to_i64, Numeric};
use turso_core::types::Value;

// =============================================================================
// str_to_i64 Benchmarks - Integer String Parsing
// =============================================================================

#[divan::bench]
fn str_to_i64_simple_positive(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_i64(black_box("12345"))));
}

#[divan::bench]
fn str_to_i64_simple_negative(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_i64(black_box("-12345"))));
}

#[divan::bench]
fn str_to_i64_with_plus_sign(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_i64(black_box("+12345"))));
}

#[divan::bench]
fn str_to_i64_max_value(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_i64(black_box("9223372036854775807"))));
}

#[divan::bench]
fn str_to_i64_min_value(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_i64(black_box("-9223372036854775808"))));
}

#[divan::bench]
fn str_to_i64_overflow(bencher: Bencher) {
    // Should return i64::MAX
    bencher.bench_local(|| black_box(str_to_i64(black_box("99999999999999999999999"))));
}

#[divan::bench]
fn str_to_i64_with_whitespace(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_i64(black_box("  12345  "))));
}

#[divan::bench]
fn str_to_i64_zero(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_i64(black_box("0"))));
}

#[divan::bench]
fn str_to_i64_empty(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_i64(black_box(""))));
}

#[divan::bench]
fn str_to_i64_non_numeric(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_i64(black_box("abc"))));
}

#[divan::bench]
fn str_to_i64_mixed_content(bencher: Bencher) {
    // Should parse leading digits
    bencher.bench_local(|| black_box(str_to_i64(black_box("123abc456"))));
}

// =============================================================================
// str_to_f64 Benchmarks - Float String Parsing
// =============================================================================

#[divan::bench]
fn str_to_f64_simple_integer(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_f64(black_box("12345"))));
}

#[divan::bench]
fn str_to_f64_simple_decimal(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_f64(black_box("123.456"))));
}

#[divan::bench]
fn str_to_f64_negative_decimal(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_f64(black_box("-123.456"))));
}

#[divan::bench]
fn str_to_f64_scientific_positive_exp(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_f64(black_box("1.23e10"))));
}

#[divan::bench]
fn str_to_f64_scientific_negative_exp(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_f64(black_box("1.23e-10"))));
}

#[divan::bench]
fn str_to_f64_scientific_uppercase(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_f64(black_box("1.23E10"))));
}

#[divan::bench]
fn str_to_f64_very_small(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_f64(black_box("0.000000000001"))));
}

#[divan::bench]
fn str_to_f64_very_large(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_f64(black_box("999999999999999999999"))));
}

#[divan::bench]
fn str_to_f64_leading_decimal(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_f64(black_box(".456"))));
}

#[divan::bench]
fn str_to_f64_trailing_decimal(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_f64(black_box("123."))));
}

#[divan::bench]
fn str_to_f64_with_whitespace(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_f64(black_box("  123.456  "))));
}

#[divan::bench]
fn str_to_f64_zero(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_f64(black_box("0.0"))));
}

#[divan::bench]
fn str_to_f64_many_decimal_places(bencher: Bencher) {
    bencher.bench_local(|| black_box(str_to_f64(black_box("3.141592653589793238462643383279"))));
}

#[divan::bench]
fn str_to_f64_prefix_only(bencher: Bencher) {
    // Should parse leading number
    bencher.bench_local(|| black_box(str_to_f64(black_box("123.456abc"))));
}

// =============================================================================
// format_float Benchmarks - Float to String Formatting
// =============================================================================

#[divan::bench]
fn format_float_simple(bencher: Bencher) {
    bencher.bench_local(|| black_box(format_float(black_box(123.456))));
}

#[divan::bench]
fn format_float_integer(bencher: Bencher) {
    bencher.bench_local(|| black_box(format_float(black_box(12345.0))));
}

#[divan::bench]
fn format_float_negative(bencher: Bencher) {
    bencher.bench_local(|| black_box(format_float(black_box(-123.456))));
}

#[divan::bench]
fn format_float_zero(bencher: Bencher) {
    bencher.bench_local(|| black_box(format_float(black_box(0.0))));
}

#[divan::bench]
fn format_float_very_small(bencher: Bencher) {
    bencher.bench_local(|| black_box(format_float(black_box(1e-100))));
}

#[divan::bench]
fn format_float_very_large(bencher: Bencher) {
    bencher.bench_local(|| black_box(format_float(black_box(1e100))));
}

#[divan::bench]
fn format_float_scientific_needed(bencher: Bencher) {
    // Should trigger scientific notation
    bencher.bench_local(|| black_box(format_float(black_box(9.93e-322))));
}

#[divan::bench]
fn format_float_nan(bencher: Bencher) {
    bencher.bench_local(|| black_box(format_float(black_box(f64::NAN))));
}

#[divan::bench]
fn format_float_infinity(bencher: Bencher) {
    bencher.bench_local(|| black_box(format_float(black_box(f64::INFINITY))));
}

#[divan::bench]
fn format_float_neg_infinity(bencher: Bencher) {
    bencher.bench_local(|| black_box(format_float(black_box(f64::NEG_INFINITY))));
}

#[divan::bench]
fn format_float_precision_edge(bencher: Bencher) {
    // Test precision handling
    bencher.bench_local(|| black_box(format_float(black_box(0.1 + 0.2))));
}

// =============================================================================
// Numeric Type Conversion Benchmarks
// =============================================================================

#[divan::bench]
fn numeric_from_integer_value(bencher: Bencher) {
    let value = Value::from_i64(12345);
    bencher.bench_local(|| black_box(Numeric::from(black_box(&value))));
}

#[divan::bench]
fn numeric_from_float_value(bencher: Bencher) {
    let value = Value::from_f64(123.456);
    bencher.bench_local(|| black_box(Numeric::from(black_box(&value))));
}

#[divan::bench]
fn numeric_from_text_integer(bencher: Bencher) {
    let value = Value::build_text("12345");
    bencher.bench_local(|| black_box(Numeric::from(black_box(&value))));
}

#[divan::bench]
fn numeric_from_text_float(bencher: Bencher) {
    let value = Value::build_text("123.456");
    bencher.bench_local(|| black_box(Numeric::from(black_box(&value))));
}

#[divan::bench]
fn numeric_from_text_scientific(bencher: Bencher) {
    let value = Value::build_text("1.23e10");
    bencher.bench_local(|| black_box(Numeric::from(black_box(&value))));
}

#[divan::bench]
fn numeric_from_null(bencher: Bencher) {
    let value = Value::Null;
    bencher.bench_local(|| black_box(Numeric::from(black_box(&value))));
}

#[divan::bench]
fn numeric_from_blob(bencher: Bencher) {
    let value = Value::Blob(b"12345".to_vec());
    bencher.bench_local(|| black_box(Numeric::from(black_box(&value))));
}

#[divan::bench]
fn numeric_from_string_ref(bencher: Bencher) {
    bencher.bench_local(|| black_box(Numeric::from(black_box("123.456"))));
}

// =============================================================================
// Numeric Arithmetic Benchmarks
// =============================================================================

#[divan::bench]
fn numeric_add_integers(bencher: Bencher) {
    let a = Numeric::Integer(1000);
    let b = Numeric::Integer(2000);
    bencher.bench_local(|| black_box(black_box(a) + black_box(b)));
}

#[divan::bench]
fn numeric_add_floats(bencher: Bencher) {
    let a = Numeric::from("100.5");
    let b = Numeric::from("200.5");
    bencher.bench_local(|| black_box(black_box(a) + black_box(b)));
}

#[divan::bench]
fn numeric_add_mixed(bencher: Bencher) {
    let a = Numeric::Integer(100);
    let b = Numeric::from("200.5");
    bencher.bench_local(|| black_box(black_box(a) + black_box(b)));
}

#[divan::bench]
fn numeric_sub_integers(bencher: Bencher) {
    let a = Numeric::Integer(2000);
    let b = Numeric::Integer(1000);
    bencher.bench_local(|| black_box(black_box(a) - black_box(b)));
}

#[divan::bench]
fn numeric_mul_integers(bencher: Bencher) {
    let a = Numeric::Integer(100);
    let b = Numeric::Integer(200);
    bencher.bench_local(|| black_box(black_box(a) * black_box(b)));
}

#[divan::bench]
fn numeric_div_integers(bencher: Bencher) {
    let a = Numeric::Integer(1000);
    let b = Numeric::Integer(10);
    bencher.bench_local(|| black_box(black_box(a) / black_box(b)));
}

#[divan::bench]
fn numeric_neg_integer(bencher: Bencher) {
    let a = Numeric::Integer(12345);
    bencher.bench_local(|| black_box(-black_box(a)));
}

#[divan::bench]
fn numeric_add_overflow(bencher: Bencher) {
    // Should overflow to float
    let a = Numeric::Integer(i64::MAX);
    let b = Numeric::Integer(1);
    bencher.bench_local(|| black_box(black_box(a) + black_box(b)));
}

// =============================================================================
// Numeric Strict Conversion Benchmarks
// =============================================================================

#[divan::bench]
fn numeric_strict_from_integer(bencher: Bencher) {
    let value = Value::from_i64(12345);
    bencher.bench_local(|| black_box(Numeric::from_value_strict(black_box(&value))));
}

#[divan::bench]
fn numeric_strict_from_float(bencher: Bencher) {
    let value = Value::from_f64(123.456);
    bencher.bench_local(|| black_box(Numeric::from_value_strict(black_box(&value))));
}

#[divan::bench]
fn numeric_strict_from_text_valid(bencher: Bencher) {
    let value = Value::build_text("123.456");
    bencher.bench_local(|| black_box(Numeric::from_value_strict(black_box(&value))));
}

#[divan::bench]
fn numeric_strict_from_text_invalid_prefix(bencher: Bencher) {
    // Should return Null in strict mode
    let value = Value::build_text("123abc");
    bencher.bench_local(|| black_box(Numeric::from_value_strict(black_box(&value))));
}

#[divan::bench]
fn numeric_strict_from_blob(bencher: Bencher) {
    // Blob is always Null in strict mode
    let value = Value::Blob(b"12345".to_vec());
    bencher.bench_local(|| black_box(Numeric::from_value_strict(black_box(&value))));
}
