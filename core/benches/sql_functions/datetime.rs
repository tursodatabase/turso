use divan::{black_box, Bencher};
use turso_core::functions::datetime::{
    exec_date, exec_datetime_full, exec_julianday, exec_strftime, exec_time, exec_timediff,
    exec_unixepoch,
};
use turso_core::types::Value;

// =============================================================================
// Fast Path Parsing Benchmarks
// These formats use the optimized custom parser (no chrono overhead)
// =============================================================================

#[divan::bench]
fn fast_path_date_only(bencher: Bencher) {
    // YYYY-MM-DD (10 chars) - fast path
    let args = [Value::build_text("2024-07-21")];
    bencher.bench_local(|| exec_date(black_box(&args)));
}

#[divan::bench]
fn fast_path_datetime_hhmm(bencher: Bencher) {
    // YYYY-MM-DD HH:MM (16 chars) - fast path
    let args = [Value::build_text("2024-07-21 14:30")];
    bencher.bench_local(|| exec_datetime_full(black_box(&args)));
}

#[divan::bench]
fn fast_path_datetime_hhmmss(bencher: Bencher) {
    // YYYY-MM-DD HH:MM:SS (19 chars) - fast path
    let args = [Value::build_text("2024-07-21 14:30:45")];
    bencher.bench_local(|| exec_datetime_full(black_box(&args)));
}

#[divan::bench]
fn fast_path_datetime_with_frac(bencher: Bencher) {
    // YYYY-MM-DD HH:MM:SS.fff (23 chars) - fast path
    let args = [Value::build_text("2024-07-21 14:30:45.123")];
    bencher.bench_local(|| exec_datetime_full(black_box(&args)));
}

#[divan::bench]
fn fast_path_datetime_t_separator(bencher: Bencher) {
    // YYYY-MM-DDTHH:MM:SS - fast path with T separator
    let args = [Value::build_text("2024-07-21T14:30:45")];
    bencher.bench_local(|| exec_datetime_full(black_box(&args)));
}

#[divan::bench]
fn fast_path_time_hhmm(bencher: Bencher) {
    // HH:MM (5 chars) - fast path
    let args = [Value::build_text("14:30")];
    bencher.bench_local(|| exec_time(black_box(&args)));
}

#[divan::bench]
fn fast_path_time_hhmmss(bencher: Bencher) {
    // HH:MM:SS (8 chars) - fast path
    let args = [Value::build_text("14:30:45")];
    bencher.bench_local(|| exec_time(black_box(&args)));
}

#[divan::bench]
fn fast_path_time_with_frac(bencher: Bencher) {
    // HH:MM:SS.fff - fast path
    let args = [Value::build_text("14:30:45.123")];
    bencher.bench_local(|| exec_time(black_box(&args)));
}

// =============================================================================
// Slow Path Parsing Benchmarks
// These formats require chrono's parser (timezone handling)
// =============================================================================

#[divan::bench]
fn slow_path_datetime_utc_z(bencher: Bencher) {
    // Ends with Z - triggers slow path for timezone
    let args = [Value::build_text("2024-07-21 14:30:45Z")];
    bencher.bench_local(|| exec_datetime_full(black_box(&args)));
}

#[divan::bench]
fn slow_path_datetime_tz_offset(bencher: Bencher) {
    // Has timezone offset - slow path
    let args = [Value::build_text("2024-07-21 14:30:45+02:00")];
    bencher.bench_local(|| exec_datetime_full(black_box(&args)));
}

#[divan::bench]
fn slow_path_datetime_negative_tz(bencher: Bencher) {
    // Negative timezone offset - slow path
    let args = [Value::build_text("2024-07-21 14:30:45-05:00")];
    bencher.bench_local(|| exec_datetime_full(black_box(&args)));
}

#[divan::bench]
fn slow_path_time_with_tz(bencher: Bencher) {
    // Time with timezone - slow path
    let args = [Value::build_text("14:30:45+02:00")];
    bencher.bench_local(|| exec_time(black_box(&args)));
}

// =============================================================================
// Numeric Input Benchmarks (Julian Day)
// =============================================================================

#[divan::bench]
fn julian_day_float_input(bencher: Bencher) {
    // Float julian day value
    let args = [Value::Float(2460512.5)];
    bencher.bench_local(|| exec_date(black_box(&args)));
}

#[divan::bench]
fn julian_day_integer_input(bencher: Bencher) {
    // Integer julian day value
    let args = [Value::Integer(2460513)];
    bencher.bench_local(|| exec_date(black_box(&args)));
}

#[divan::bench]
fn julian_day_string_numeric(bencher: Bencher) {
    // Numeric string that parses as julian day
    let args = [Value::build_text("2460512.5")];
    bencher.bench_local(|| exec_date(black_box(&args)));
}

// =============================================================================
// Output Type Benchmarks
// =============================================================================

#[divan::bench]
fn output_date(bencher: Bencher) {
    let args = [Value::build_text("2024-07-21 14:30:45")];
    bencher.bench_local(|| exec_date(black_box(&args)));
}

#[divan::bench]
fn output_time(bencher: Bencher) {
    let args = [Value::build_text("2024-07-21 14:30:45")];
    bencher.bench_local(|| exec_time(black_box(&args)));
}

#[divan::bench]
fn output_datetime(bencher: Bencher) {
    let args = [Value::build_text("2024-07-21 14:30:45")];
    bencher.bench_local(|| exec_datetime_full(black_box(&args)));
}

#[divan::bench]
fn output_julianday(bencher: Bencher) {
    let args = [Value::build_text("2024-07-21 14:30:45")];
    bencher.bench_local(|| exec_julianday(black_box(&args)));
}

#[divan::bench]
fn output_unixepoch(bencher: Bencher) {
    let args = [Value::build_text("2024-07-21 14:30:45")];
    bencher.bench_local(|| exec_unixepoch(black_box(&args)));
}

// =============================================================================
// strftime Benchmarks
// =============================================================================

#[divan::bench]
fn strftime_simple_format(bencher: Bencher) {
    let args = [
        Value::build_text("%Y-%m-%d"),
        Value::build_text("2024-07-21 14:30:45"),
    ];
    bencher.bench_local(|| exec_strftime(black_box(&args)));
}

#[divan::bench]
fn strftime_complex_format(bencher: Bencher) {
    let args = [
        Value::build_text("%Y-%m-%d %H:%M:%S"),
        Value::build_text("2024-07-21 14:30:45"),
    ];
    bencher.bench_local(|| exec_strftime(black_box(&args)));
}

#[divan::bench]
fn strftime_with_julian(bencher: Bencher) {
    // %J is SQLite-specific julian day format
    let args = [
        Value::build_text("%J"),
        Value::build_text("2024-07-21 14:30:45"),
    ];
    bencher.bench_local(|| exec_strftime(black_box(&args)));
}

#[divan::bench]
fn strftime_weekday_format(bencher: Bencher) {
    let args = [
        Value::build_text("%w %W"),
        Value::build_text("2024-07-21 14:30:45"),
    ];
    bencher.bench_local(|| exec_strftime(black_box(&args)));
}

// =============================================================================
// Modifier Benchmarks
// =============================================================================

#[divan::bench]
fn modifier_add_days(bencher: Bencher) {
    let args = [
        Value::build_text("2024-07-21"),
        Value::build_text("+5 days"),
    ];
    bencher.bench_local(|| exec_date(black_box(&args)));
}

#[divan::bench]
fn modifier_add_fractional_days(bencher: Bencher) {
    // Fractional modifier (new feature from PR)
    let args = [
        Value::build_text("2024-07-21 12:00:00"),
        Value::build_text("+1.5 days"),
    ];
    bencher.bench_local(|| exec_datetime_full(black_box(&args)));
}

#[divan::bench]
fn modifier_add_months(bencher: Bencher) {
    let args = [
        Value::build_text("2024-07-21"),
        Value::build_text("+3 months"),
    ];
    bencher.bench_local(|| exec_date(black_box(&args)));
}

#[divan::bench]
fn modifier_start_of_month(bencher: Bencher) {
    let args = [
        Value::build_text("2024-07-21 14:30:45"),
        Value::build_text("start of month"),
    ];
    bencher.bench_local(|| exec_date(black_box(&args)));
}

#[divan::bench]
fn modifier_start_of_year(bencher: Bencher) {
    let args = [
        Value::build_text("2024-07-21 14:30:45"),
        Value::build_text("start of year"),
    ];
    bencher.bench_local(|| exec_date(black_box(&args)));
}

#[divan::bench]
fn modifier_weekday(bencher: Bencher) {
    let args = [
        Value::build_text("2024-07-21"),
        Value::build_text("weekday 0"),
    ];
    bencher.bench_local(|| exec_date(black_box(&args)));
}

#[divan::bench]
fn modifier_unixepoch(bencher: Bencher) {
    // unixepoch modifier for numeric input
    let args = [Value::Integer(1721577045), Value::build_text("unixepoch")];
    bencher.bench_local(|| exec_datetime_full(black_box(&args)));
}

#[divan::bench]
fn modifier_auto_unixepoch(bencher: Bencher) {
    // auto modifier detecting unix epoch
    let args = [Value::Integer(1721577045), Value::build_text("auto")];
    bencher.bench_local(|| exec_datetime_full(black_box(&args)));
}

#[divan::bench]
fn modifier_auto_julianday(bencher: Bencher) {
    // auto modifier detecting julian day
    let args = [Value::Float(2460512.5), Value::build_text("auto")];
    bencher.bench_local(|| exec_datetime_full(black_box(&args)));
}

#[divan::bench]
fn modifier_localtime(bencher: Bencher) {
    let args = [
        Value::build_text("2024-07-21 14:30:45"),
        Value::build_text("localtime"),
    ];
    bencher.bench_local(|| exec_datetime_full(black_box(&args)));
}

#[divan::bench]
fn modifier_chain_multiple(bencher: Bencher) {
    // Multiple modifiers chained
    let args = [
        Value::build_text("2024-07-21"),
        Value::build_text("+1 month"),
        Value::build_text("start of month"),
        Value::build_text("+7 days"),
    ];
    bencher.bench_local(|| exec_date(black_box(&args)));
}

// =============================================================================
// timediff Benchmarks
// =============================================================================

#[divan::bench]
fn timediff_same_day(bencher: Bencher) {
    let args = [
        Value::build_text("2024-07-21 14:30:45"),
        Value::build_text("2024-07-21 10:15:30"),
    ];
    bencher.bench_local(|| exec_timediff(black_box(&args)));
}

#[divan::bench]
fn timediff_different_days(bencher: Bencher) {
    let args = [
        Value::build_text("2024-07-25 14:30:45"),
        Value::build_text("2024-07-21 10:15:30"),
    ];
    bencher.bench_local(|| exec_timediff(black_box(&args)));
}

#[divan::bench]
fn timediff_different_years(bencher: Bencher) {
    let args = [
        Value::build_text("2024-07-21 14:30:45"),
        Value::build_text("2020-01-15 10:15:30"),
    ];
    bencher.bench_local(|| exec_timediff(black_box(&args)));
}

// =============================================================================
// Special Cases
// =============================================================================

#[divan::bench]
fn special_now(bencher: Bencher) {
    let args = [Value::build_text("now")];
    bencher.bench_local(|| exec_datetime_full(black_box(&args)));
}

#[divan::bench]
fn special_no_args_current_time(bencher: Bencher) {
    // No arguments - returns current time
    let args: [Value; 0] = [];
    bencher.bench_local(|| exec_datetime_full(black_box(&args)));
}

#[divan::bench]
fn special_subsec_modifier(bencher: Bencher) {
    let args = [
        Value::build_text("2024-07-21 14:30:45.123456"),
        Value::build_text("subsec"),
    ];
    bencher.bench_local(|| exec_time(black_box(&args)));
}

#[divan::bench]
fn special_date_overflow(bencher: Bencher) {
    // Invalid date that overflows (Feb 30 -> Mar 2)
    let args = [Value::build_text("2024-02-30")];
    bencher.bench_local(|| exec_date(black_box(&args)));
}

#[divan::bench]
fn special_negative_year(bencher: Bencher) {
    // Negative year (BCE dates)
    let args = [Value::build_text("-0044-03-15")];
    bencher.bench_local(|| exec_date(black_box(&args)));
}
