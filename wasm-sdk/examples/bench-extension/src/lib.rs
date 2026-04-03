#![no_std]

extern crate alloc;

use turso_wasm_sdk::{args, ret, vtab};

// ── Extension init ───────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn turso_ext_init(_argc: i32, _argv: i32) -> i64 {
    let manifest = concat!(
        r#"{"functions":["#,
        r#"{"name":"bench_add","export":"bench_add","narg":2}"#,
        r#"],"types":[],"vtabs":["#,
        r#"{"name":"bench_counter","columns":[{"name":"value","type":"INTEGER","hidden":false}],"open":"bench_counter_open","filter":"bench_counter_filter","column":"bench_counter_column","next":"bench_counter_next","eof":"bench_counter_eof","rowid":"bench_counter_rowid"}"#,
        r#"]}"#
    );
    ret::ret_text(manifest)
}

// ── Scalar function ──────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn bench_add(_argc: i32, argv: i32) -> i64 {
    let a = args::arg_integer(argv, 0);
    let b = args::arg_integer(argv, 1);
    ret::ret_integer(a + b)
}

// ── Virtual table: bench_counter ─────────────────────────────────────────────
// Returns rows with value = 1..10000 (hardcoded N for benchmarking).

const COUNTER_N: i64 = 10000;

#[repr(C)]
#[derive(Clone, Copy)]
struct CounterCursor {
    current: i64,
    is_eof: i32,
}

#[no_mangle]
pub unsafe extern "C" fn bench_counter_open(_argc: i32, _argv: i32) -> i64 {
    vtab::alloc_cursor(CounterCursor {
        current: 1,
        is_eof: 0,
    })
}

#[no_mangle]
pub unsafe extern "C" fn bench_counter_filter(_argc: i32, argv: i32) -> i64 {
    let handle = args::arg_integer(argv, 0);
    let cursor = vtab::get_cursor_mut::<CounterCursor>(handle);
    cursor.current = 1;
    cursor.is_eof = 0;
    ret::ret_integer(0)
}

#[no_mangle]
pub unsafe extern "C" fn bench_counter_column(_argc: i32, argv: i32) -> i64 {
    let handle = args::arg_integer(argv, 0);
    let cursor = vtab::get_cursor::<CounterCursor>(handle);
    ret::ret_integer(cursor.current)
}

#[no_mangle]
pub unsafe extern "C" fn bench_counter_next(_argc: i32, argv: i32) -> i64 {
    let handle = args::arg_integer(argv, 0);
    let cursor = vtab::get_cursor_mut::<CounterCursor>(handle);
    cursor.current += 1;
    if cursor.current > COUNTER_N {
        cursor.is_eof = 1;
    }
    ret::ret_integer(if cursor.is_eof != 0 { 1 } else { 0 })
}

#[no_mangle]
pub unsafe extern "C" fn bench_counter_eof(_argc: i32, argv: i32) -> i64 {
    let handle = args::arg_integer(argv, 0);
    let cursor = vtab::get_cursor::<CounterCursor>(handle);
    ret::ret_integer(cursor.is_eof as i64)
}

#[no_mangle]
pub unsafe extern "C" fn bench_counter_rowid(_argc: i32, argv: i32) -> i64 {
    let handle = args::arg_integer(argv, 0);
    let cursor = vtab::get_cursor::<CounterCursor>(handle);
    cursor.current
}

#[panic_handler]
fn panic(_: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}
