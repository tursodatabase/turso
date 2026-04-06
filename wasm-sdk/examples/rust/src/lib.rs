#![no_std]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use turso_wasm_sdk::turso_wasm;
use turso_wasm_sdk::{args, ret};

// ── Proc macro examples ──────────────────────────────────────────────────────

/// Integer add: SELECT add(1, 2) → 3
#[turso_wasm]
fn add(a: i64, b: i64) -> i64 {
    a + b
}

/// Text uppercasing: SELECT upper('hello') → 'HELLO'
#[turso_wasm]
fn upper(s: &str) -> String {
    s.to_uppercase()
}

/// Nullable argument: SELECT nullable_len(NULL) → NULL, SELECT nullable_len('hi') → 2
#[turso_wasm]
fn nullable_len(s: Option<&str>) -> Option<i64> {
    s.map(|s| s.len() as i64)
}

/// Float multiply: SELECT float_mul(2.5, 4.0) → 10.0
#[turso_wasm]
fn float_mul(a: f64, b: f64) -> f64 {
    a * b
}

/// Blob reverse: SELECT blob_reverse(X'0102') → X'0201'
#[turso_wasm]
fn blob_reverse(data: &[u8]) -> Vec<u8> {
    let mut v = data.to_vec();
    v.reverse();
    v
}

/// Optional integer double: SELECT opt_double(NULL) → NULL, SELECT opt_double(5) → 10
#[turso_wasm]
fn opt_double(v: Option<i64>) -> Option<i64> {
    v.map(|n| n * 2)
}

/// Optional float negate: SELECT opt_negate(NULL) → NULL, SELECT opt_negate(3.14) → -3.14
#[turso_wasm]
fn opt_negate(v: Option<f64>) -> Option<f64> {
    v.map(|f| -f)
}

/// Void function (side-effect only): SELECT do_nothing() → NULL
#[turso_wasm]
fn do_nothing() {
    // no-op
}

/// Optional text upper: SELECT opt_upper(NULL) → NULL, SELECT opt_upper('hi') → 'HI'
#[turso_wasm]
fn opt_upper(s: Option<&str>) -> Option<String> {
    s.map(|s| s.to_uppercase())
}

/// Optional blob: SELECT opt_blob_len(NULL) → NULL, SELECT opt_blob_len(X'0102') → 2
#[turso_wasm]
fn opt_blob_len(data: Option<&[u8]>) -> Option<i64> {
    data.map(|b| b.len() as i64)
}

// ── Manual (no proc macro) examples ──────────────────────────────────────────

/// Squared Euclidean distance: SELECT distance_sq(0, 0, 3, 4) → 25.0
#[no_mangle]
pub unsafe extern "C" fn distance_sq(_argc: i32, argv: i32) -> i64 {
    let x1 = args::arg_real(argv, 0);
    let y1 = args::arg_real(argv, 1);
    let x2 = args::arg_real(argv, 2);
    let y2 = args::arg_real(argv, 3);
    let dx = x2 - x1;
    let dy = y2 - y1;
    ret::ret_real(dx * dx + dy * dy)
}

/// Identity function using ret_integer (old convention): SELECT echo_int(42) → 42
/// This exercises ret_integer's tagged return path. Regression test for the bug
/// where ret_integer returned raw i64 values that unmarshal_result misinterpreted
/// as tagged pointers when the value fell within the WASM memory range.
///
/// Before calling ret_integer, this function writes TAG_BLOB (0x04) to the WASM
/// memory address equal to the argument value. This "poisons" the memory so that
/// if ret_integer returns the raw value (the bug), unmarshal_result reads 0x04
/// at that address and misinterprets it as a blob. With the fix (tagged return),
/// the return pointer is on the heap and the poisoned address is never read.
#[no_mangle]
pub unsafe extern "C" fn echo_int(_argc: i32, argv: i32) -> i64 {
    let v = args::arg_integer(argv, 0);
    // Poison: write TAG_BLOB at address `v` so unmarshal_result will fail
    // if it tries to read a tag from this address (i.e., if ret_integer
    // returns the raw value instead of a tagged pointer).
    if v >= 0 && v < 512 {
        let ptr = v as *mut u8;
        core::ptr::write(ptr, 0x04); // TAG_BLOB = 0x04
    }
    ret::ret_integer(v)
}

// ── Required for no_std WASM ─────────────────────────────────────────────────

#[panic_handler]
fn panic(_: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}
