use divan::black_box;
use turso_benches::Bencher;
use turso_core::exec_printf_values;
use turso_core::types::Value;

// =============================================================================
// printf Benchmarks - Float Conversions
// =============================================================================

#[turso_macros::divan_bench]
fn printf_fixed(bencher: Bencher) {
    let format = Value::build_text("%f");
    let value = Value::from_f64(1234.5678);
    bencher.bench_local(|| black_box(exec_printf_values(black_box(&format), &[black_box(&value)])));
}

#[turso_macros::divan_bench]
fn printf_fixed_precision(bencher: Bencher) {
    let format = Value::build_text("%.10f");
    let value = Value::from_f64(0.000123456789);
    bencher.bench_local(|| black_box(exec_printf_values(black_box(&format), &[black_box(&value)])));
}

#[turso_macros::divan_bench]
fn printf_exponential(bencher: Bencher) {
    let format = Value::build_text("%.3e");
    let value = Value::from_f64(12345.678);
    bencher.bench_local(|| black_box(exec_printf_values(black_box(&format), &[black_box(&value)])));
}

#[turso_macros::divan_bench]
fn printf_general(bencher: Bencher) {
    let format = Value::build_text("%g");
    let value = Value::from_f64(1234.5678);
    bencher.bench_local(|| black_box(exec_printf_values(black_box(&format), &[black_box(&value)])));
}

#[turso_macros::divan_bench]
fn printf_general_exponent(bencher: Bencher) {
    let format = Value::build_text("%.15g");
    let value = Value::from_f64(1.5e-300);
    bencher.bench_local(|| black_box(exec_printf_values(black_box(&format), &[black_box(&value)])));
}
