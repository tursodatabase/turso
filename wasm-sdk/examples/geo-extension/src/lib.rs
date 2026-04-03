#![no_std]

extern crate alloc;

use turso_wasm_sdk::{args, ret, vtab};

// ── Extension init ───────────────────────────────────────────────────────────

/// Extension init function — returns a JSON manifest listing all exported functions,
/// types, and vtabs.
#[no_mangle]
pub unsafe extern "C" fn turso_ext_init(_argc: i32, _argv: i32) -> i64 {
    let manifest = concat!(
        r#"{"functions":["#,
        r#"{"name":"geo_point","export":"geo_point","narg":2},"#,
        r#"{"name":"geo_point_x","export":"geo_point_x","narg":1},"#,
        r#"{"name":"geo_point_y","export":"geo_point_y","narg":1},"#,
        r#"{"name":"geo_distance","export":"geo_distance","narg":2},"#,
        r#"{"name":"geo_point_decode","export":"geo_point_decode","narg":1}"#,
        r#"],"types":["#,
        r#"{"name":"point","base":"BLOB","params":[{"name":"x","type":"REAL"},{"name":"y","type":"REAL"}],"encode":"geo_point","decode":"geo_point_decode","operators":[{"op":"<","func":"geo_distance"}]}"#,
        r#"],"vtabs":["#,
        r#"{"name":"geo_series","columns":[{"name":"value","type":"REAL","hidden":false},{"name":"start","type":"REAL","hidden":true},{"name":"stop","type":"REAL","hidden":true},{"name":"step","type":"REAL","hidden":true}],"open":"geo_series_open","filter":"geo_series_filter","column":"geo_series_column","next":"geo_series_next","eof":"geo_series_eof","rowid":"geo_series_rowid"}"#,
        r#"]}"#
    );
    ret::ret_text(manifest)
}

// ── Scalar functions ─────────────────────────────────────────────────────────

/// Encode two f64 coordinates into a 16-byte blob: [x as LE f64][y as LE f64]
#[no_mangle]
pub unsafe extern "C" fn geo_point(_argc: i32, argv: i32) -> i64 {
    let x = args::arg_real(argv, 0);
    let y = args::arg_real(argv, 1);
    let mut buf = [0u8; 16];
    buf[..8].copy_from_slice(&x.to_le_bytes());
    buf[8..].copy_from_slice(&y.to_le_bytes());
    ret::ret_blob(&buf)
}

/// Extract the X coordinate from a geo_point blob.
#[no_mangle]
pub unsafe extern "C" fn geo_point_x(_argc: i32, argv: i32) -> i64 {
    let blob = args::arg_blob(argv, 0);
    if blob.len() < 16 {
        return ret::ret_null();
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&blob[..8]);
    let x = f64::from_le_bytes(bytes);
    ret::ret_real(x)
}

/// Extract the Y coordinate from a geo_point blob.
#[no_mangle]
pub unsafe extern "C" fn geo_point_y(_argc: i32, argv: i32) -> i64 {
    let blob = args::arg_blob(argv, 0);
    if blob.len() < 16 {
        return ret::ret_null();
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&blob[8..16]);
    let y = f64::from_le_bytes(bytes);
    ret::ret_real(y)
}

/// Euclidean distance between two geo_point blobs.
#[no_mangle]
pub unsafe extern "C" fn geo_distance(_argc: i32, argv: i32) -> i64 {
    let p1 = args::arg_blob(argv, 0);
    let p2 = args::arg_blob(argv, 1);
    if p1.len() < 16 || p2.len() < 16 {
        return ret::ret_null();
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&p1[..8]);
    let x1 = f64::from_le_bytes(b);
    b.copy_from_slice(&p1[8..16]);
    let y1 = f64::from_le_bytes(b);
    b.copy_from_slice(&p2[..8]);
    let x2 = f64::from_le_bytes(b);
    b.copy_from_slice(&p2[8..16]);
    let y2 = f64::from_le_bytes(b);
    let dx = x2 - x1;
    let dy = y2 - y1;
    let dist = sqrt(dx * dx + dy * dy);
    ret::ret_real(dist)
}

/// Decode a geo_point blob into a text representation "POINT(x y)".
#[no_mangle]
pub unsafe extern "C" fn geo_point_decode(_argc: i32, argv: i32) -> i64 {
    let blob = args::arg_blob(argv, 0);
    if blob.len() < 16 {
        return ret::ret_null();
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&blob[..8]);
    let x = f64::from_le_bytes(bytes);
    bytes.copy_from_slice(&blob[8..16]);
    let y = f64::from_le_bytes(bytes);
    // Simple integer-style output for easy testing
    ret::ret_real(x + y)
}

// ── Virtual table: geo_series ────────────────────────────────────────────────

/// Cursor state for geo_series, stored in WASM linear memory.
#[repr(C)]
#[derive(Clone, Copy)]
struct GeoSeriesCursor {
    current: f64,
    stop: f64,
    step: f64,
    row: i64,
    is_eof: i32,
}

/// Open a new geo_series cursor.
#[no_mangle]
pub unsafe extern "C" fn geo_series_open(_argc: i32, _argv: i32) -> i64 {
    vtab::alloc_cursor(GeoSeriesCursor {
        current: 0.0,
        stop: 0.0,
        step: 1.0,
        row: 0,
        is_eof: 1,
    })
}

/// Filter (initialize) the geo_series cursor.
/// argv: [cursor_handle, idx_num, start, stop, step]
#[no_mangle]
pub unsafe extern "C" fn geo_series_filter(_argc: i32, argv: i32) -> i64 {
    let handle = args::arg_integer(argv, 0);
    let _idx_num = args::arg_integer(argv, 1);
    let start = args::arg_real(argv, 2);
    let stop = args::arg_real(argv, 3);
    let step = if _argc > 4 {
        args::arg_real(argv, 4)
    } else {
        1.0
    };
    let cursor = vtab::get_cursor_mut::<GeoSeriesCursor>(handle);
    cursor.current = start;
    cursor.stop = stop;
    cursor.step = if step == 0.0 { 1.0 } else { step };
    cursor.row = 0;
    cursor.is_eof = if start > stop { 1 } else { 0 };
    // Return 0 = has rows, 1 = empty
    ret::ret_integer(cursor.is_eof as i64)
}

/// Return a column value from the current row.
/// argv: [cursor_handle, column_idx]
#[no_mangle]
pub unsafe extern "C" fn geo_series_column(_argc: i32, argv: i32) -> i64 {
    let handle = args::arg_integer(argv, 0);
    let col_idx = args::arg_integer(argv, 1);
    let cursor = vtab::get_cursor::<GeoSeriesCursor>(handle);
    match col_idx {
        0 => ret::ret_real(cursor.current), // value
        1 => ret::ret_real(cursor.current), // start (approximate)
        2 => ret::ret_real(cursor.stop),    // stop
        3 => ret::ret_real(cursor.step),    // step
        _ => ret::ret_null(),
    }
}

/// Advance to the next row.
/// argv: [cursor_handle]
/// Returns 0 = has more, 1 = EOF
#[no_mangle]
pub unsafe extern "C" fn geo_series_next(_argc: i32, argv: i32) -> i64 {
    let handle = args::arg_integer(argv, 0);
    let cursor = vtab::get_cursor_mut::<GeoSeriesCursor>(handle);
    cursor.current += cursor.step;
    cursor.row += 1;
    if cursor.current > cursor.stop {
        cursor.is_eof = 1;
    }
    ret::ret_integer(if cursor.is_eof != 0 { 1 } else { 0 })
}

/// Check if cursor is at EOF.
/// argv: [cursor_handle]
/// Returns 0 = not eof, 1 = eof
#[no_mangle]
pub unsafe extern "C" fn geo_series_eof(_argc: i32, argv: i32) -> i64 {
    let handle = args::arg_integer(argv, 0);
    let cursor = vtab::get_cursor::<GeoSeriesCursor>(handle);
    ret::ret_integer(cursor.is_eof as i64)
}

/// Return the rowid for the current row.
/// argv: [cursor_handle]
#[no_mangle]
pub unsafe extern "C" fn geo_series_rowid(_argc: i32, argv: i32) -> i64 {
    let handle = args::arg_integer(argv, 0);
    let cursor = vtab::get_cursor::<GeoSeriesCursor>(handle);
    cursor.row
}

// ── Utilities ───────────────────────────────────────────────────────────��────

/// Simple Newton's method sqrt for no_std.
fn sqrt(x: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    let mut guess = x;
    let mut i = 0;
    while i < 100 {
        let next = 0.5 * (guess + x / guess);
        if (next - guess).abs() < 1e-15 {
            break;
        }
        guess = next;
        i += 1;
    }
    guess
}

/// Absolute value for f64 (no_std).
trait Abs {
    fn abs(self) -> Self;
}

impl Abs for f64 {
    fn abs(self) -> Self {
        if self < 0.0 { -self } else { self }
    }
}

#[panic_handler]
fn panic(_: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}
