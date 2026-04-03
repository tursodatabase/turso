#![no_std]

//! SDK for writing Turso WASM user-defined functions.
//!
//! Provides argument readers, return value builders, and a bump allocator
//! that match the Turso WASM UDF ABI.
//!
//! # Quick start (with proc macro)
//! ```ignore
//! use turso_wasm_sdk::turso_wasm;
//!
//! #[turso_wasm]
//! fn add(a: i64, b: i64) -> i64 { a + b }
//! ```
//!
//! # Quick start (manual)
//! ```ignore
//! use turso_wasm_sdk::{args, ret};
//!
//! #[no_mangle]
//! pub unsafe extern "C" fn add(argc: i32, argv: i32) -> i64 {
//!     let a = args::arg_integer(argv, 0);
//!     let b = args::arg_integer(argv, 1);
//!     ret::ret_integer(a + b)
//! }
//! ```

// Re-export the proc macro.
pub use turso_wasm_sdk_macros::turso_wasm;

// ── Typed ABI constants ─────────────────────────────────────────────────────

/// Offset in WASM linear memory where the null flag byte is stored.
/// Used for Option<i64> and Option<f64> returns where all bit patterns are valid.
pub const NULL_FLAG_OFFSET: usize = 1016;

/// Offset in WASM linear memory where the return type tag is written.
/// The old-convention `ret::*` functions write their type tag here so the host
/// can determine the return type without ambiguity. This eliminates the need
/// for inline tag bytes in the returned buffer for integers, reals, and nulls
/// (zero allocation), and removes the tag prefix for text/blob (saves 1 byte).
pub const RET_TYPE_ADDR: usize = 1017;

// ── ABI tag constants (must match core/wasm/mod.rs) ──────────────────────────

pub const TAG_INTEGER: u8 = 0x01;
pub const TAG_REAL: u8 = 0x02;
pub const TAG_TEXT: u8 = 0x03;
pub const TAG_BLOB: u8 = 0x04;
pub const TAG_NULL: u8 = 0x05;

// ── Argument readers ─────────────────────────────────────────────────────────

pub mod args {
    use super::*;

    /// Read the raw i64 value from argv slot `index`.
    ///
    /// # Safety
    /// `argv` must point to a valid argument array and `index` must be < argc.
    #[inline]
    unsafe fn read_slot(argv: i32, index: usize) -> i64 {
        let ptr = (argv as usize + index * 8) as *const i64;
        core::ptr::read_unaligned(ptr)
    }

    /// Read an integer argument.
    ///
    /// # Safety
    /// Caller must ensure the argument at `index` is an integer.
    #[inline]
    pub unsafe fn arg_integer(argv: i32, index: usize) -> i64 {
        read_slot(argv, index)
    }

    /// Read a real (f64) argument.
    ///
    /// # Safety
    /// Caller must ensure the argument at `index` is a real.
    #[inline]
    pub unsafe fn arg_real(argv: i32, index: usize) -> f64 {
        f64::from_bits(read_slot(argv, index) as u64)
    }

    /// Read a text argument. Returns a `&str` pointing into WASM linear memory.
    ///
    /// # Safety
    /// Caller must ensure the argument at `index` is text.
    #[inline]
    pub unsafe fn arg_text(argv: i32, index: usize) -> &'static str {
        let ptr = read_slot(argv, index) as usize;
        // Skip TAG_TEXT byte
        let start = (ptr + 1) as *const u8;
        let mut len = 0;
        while *start.add(len) != 0 {
            len += 1;
        }
        let bytes = core::slice::from_raw_parts(start, len);
        core::str::from_utf8_unchecked(bytes)
    }

    /// Read a blob argument. Returns a `&[u8]` pointing into WASM linear memory.
    ///
    /// # Safety
    /// Caller must ensure the argument at `index` is a blob.
    #[inline]
    pub unsafe fn arg_blob(argv: i32, index: usize) -> &'static [u8] {
        let ptr = read_slot(argv, index) as usize;
        // Skip TAG_BLOB byte, read 4-byte LE size
        let size_ptr = (ptr + 1) as *const u8;
        let size = u32::from_le_bytes([
            *size_ptr,
            *size_ptr.add(1),
            *size_ptr.add(2),
            *size_ptr.add(3),
        ]) as usize;
        let data_ptr = (ptr + 5) as *const u8;
        core::slice::from_raw_parts(data_ptr, size)
    }

    /// Check if the argument at `index` is NULL.
    ///
    /// # Safety
    /// `argv` must point to a valid argument array and `index` must be < argc.
    #[inline]
    pub unsafe fn arg_is_null(argv: i32, index: usize) -> bool {
        let val = read_slot(argv, index);
        let ptr = val as usize;
        let tag = *(ptr as *const u8);
        tag == TAG_NULL
    }
}

// ── Return value builders ────────────────────────────────────────────────────

pub mod ret {
    use super::*;

    extern "C" {
        fn turso_malloc(size: i32) -> i32;
    }

    /// Write the return type tag to the sideband address.
    #[inline]
    unsafe fn set_ret_type(tag: u8) {
        *(RET_TYPE_ADDR as *mut u8) = tag;
    }

    /// Return an integer value. Zero allocation — writes type to sideband,
    /// returns raw i64.
    #[inline]
    pub fn ret_integer(val: i64) -> i64 {
        unsafe { set_ret_type(TAG_INTEGER) };
        val
    }

    /// Return a real value. Zero allocation — writes type to sideband,
    /// returns f64 bits as i64.
    #[inline]
    pub fn ret_real(val: f64) -> i64 {
        unsafe { set_ret_type(TAG_REAL) };
        val.to_bits() as i64
    }

    /// Return a text value. Allocates `[utf8 bytes][0x00]` via turso_malloc.
    #[inline]
    pub fn ret_text(s: &str) -> i64 {
        unsafe {
            set_ret_type(TAG_TEXT);
            let total = s.len() + 1;
            let ptr = turso_malloc(total as i32);
            let base = ptr as *mut u8;
            core::ptr::copy_nonoverlapping(s.as_ptr(), base, s.len());
            *base.add(s.len()) = 0;
            ptr as i64
        }
    }

    /// Return a blob value. Allocates `[4-byte LE size][data]` via turso_malloc.
    #[inline]
    pub fn ret_blob(data: &[u8]) -> i64 {
        unsafe {
            set_ret_type(TAG_BLOB);
            let total = 4 + data.len();
            let ptr = turso_malloc(total as i32);
            let base = ptr as *mut u8;
            let size_bytes = (data.len() as u32).to_le_bytes();
            core::ptr::copy_nonoverlapping(size_bytes.as_ptr(), base, 4);
            core::ptr::copy_nonoverlapping(data.as_ptr(), base.add(4), data.len());
            ptr as i64
        }
    }

    /// Return a NULL value. Zero allocation — writes type to sideband.
    #[inline]
    pub fn ret_null() -> i64 {
        unsafe { set_ret_type(TAG_NULL) };
        0
    }
}

// ── Typed ABI return helpers ─────────────────────────────────────────────────

/// Return a text value via the typed ABI: allocate via turso_malloc, copy the
/// string, and return packed `(len << 32) | ptr` as i64.
#[inline]
pub fn ret_packed_text(s: &str) -> i64 {
    unsafe {
        extern "C" {
            fn turso_malloc(size: i32) -> i32;
        }
        let ptr = turso_malloc(s.len() as i32);
        let base = ptr as *mut u8;
        core::ptr::copy_nonoverlapping(s.as_ptr(), base, s.len());
        ((s.len() as i64) << 32) | (ptr as i64 & 0xFFFF_FFFF)
    }
}

/// Return a blob value via the typed ABI: allocate via turso_malloc, copy the
/// data, and return packed `(len << 32) | ptr` as i64.
#[inline]
pub fn ret_packed_blob(data: &[u8]) -> i64 {
    unsafe {
        extern "C" {
            fn turso_malloc(size: i32) -> i32;
        }
        let ptr = turso_malloc(data.len() as i32);
        let base = ptr as *mut u8;
        core::ptr::copy_nonoverlapping(data.as_ptr(), base, data.len());
        ((data.len() as i64) << 32) | (ptr as i64 & 0xFFFF_FFFF)
    }
}

// ── Extension helpers ────────────────────────────────────────────────────────

/// Helpers for writing extension init functions.
pub mod ext {
    use super::ret;

    /// Return a JSON manifest string from `turso_ext_init`.
    /// Convenience wrapper around `ret::ret_text`.
    #[inline]
    pub fn return_manifest(json: &str) -> i64 {
        ret::ret_text(json)
    }
}

// ── Dynamic Value enum ───────────────────────────────────────────────────────

/// A dynamically-typed SQLite value for use in WASM UDFs.
pub enum Value<'a> {
    Integer(i64),
    Real(f64),
    Text(&'a str),
    Blob(&'a [u8]),
    Null,
}

impl Value<'_> {
    /// Read a value from an argv slot, detecting the type via tag byte.
    ///
    /// For values that are pointers to tagged memory (TEXT, BLOB, NULL),
    /// the tag byte is used to determine the type. Otherwise the value
    /// is treated as an integer.
    ///
    /// # Safety
    /// `argv` must point to a valid argument array and `index` must be < argc.
    pub unsafe fn from_arg(argv: i32, index: usize) -> Self {
        let raw = args::arg_integer(argv, index);
        let ptr = raw as usize;
        let tag = *(ptr as *const u8);
        match tag {
            TAG_TEXT => Value::Text(args::arg_text(argv, index)),
            TAG_BLOB => Value::Blob(args::arg_blob(argv, index)),
            TAG_NULL => Value::Null,
            _ => Value::Integer(raw),
        }
    }

    /// Convert this value into a WASM return i64.
    pub fn into_ret(self) -> i64 {
        match self {
            Value::Integer(v) => ret::ret_integer(v),
            Value::Real(v) => ret::ret_real(v),
            Value::Text(s) => ret::ret_text(s),
            Value::Blob(b) => ret::ret_blob(b),
            Value::Null => ret::ret_null(),
        }
    }
}

// ── Default bump allocator ───────────────────────────────────────────────────

#[cfg(feature = "allocator")]
mod allocator {
    use core::alloc::{GlobalAlloc, Layout};

    /// Start of the bump region (after the 1024-byte argv area).
    const BUMP_START: usize = 1024;

    static mut BUMP_PTR: usize = BUMP_START;

    /// A simple bump allocator for WASM linear memory.
    ///
    /// Starts allocating at byte 1024 (after the argv area) and never frees.
    /// Use [`reset_allocator`] between UDF calls to reclaim memory.
    ///
    /// This is exported as `turso_malloc` for the Turso WASM ABI, and can also
    /// serve as the Rust global allocator for `String`/`Vec` support:
    ///
    /// ```ignore
    /// #[global_allocator]
    /// static ALLOC: turso_wasm_sdk::BumpAllocator = turso_wasm_sdk::BumpAllocator;
    /// ```
    pub struct BumpAllocator;

    unsafe impl GlobalAlloc for BumpAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let align = layout.align();
            let aligned = (BUMP_PTR + align - 1) & !(align - 1);
            BUMP_PTR = aligned + layout.size();
            aligned as *mut u8
        }

        unsafe fn dealloc(&self, _ptr: *mut u8, _layout: Layout) {
            // Bump allocator does not free individual allocations.
        }
    }

    /// The `turso_malloc` export required by the Turso WASM UDF ABI.
    ///
    /// # Safety
    /// Called by the host to allocate memory for argument marshalling.
    #[no_mangle]
    pub unsafe extern "C" fn turso_malloc(size: i32) -> i32 {
        let ptr = BUMP_PTR;
        BUMP_PTR += size as usize;
        ptr as i32
    }

    /// Reset the bump allocator to its initial state.
    ///
    /// Call this at the start of each UDF invocation to reclaim memory
    /// from previous calls. The host resets between calls automatically,
    /// but this can be useful for manual control.
    pub fn reset_allocator() {
        unsafe {
            BUMP_PTR = BUMP_START;
        }
    }

    // Set the global allocator on WASM targets so String/Vec work out of the box.
    #[cfg(target_arch = "wasm32")]
    #[global_allocator]
    static ALLOC: BumpAllocator = BumpAllocator;
}

#[cfg(feature = "allocator")]
pub use allocator::{reset_allocator, BumpAllocator};

// ── VTab cursor helpers ──────────────────────────────────────────────────────

/// Helpers for implementing virtual table cursors in WASM extensions.
pub mod vtab {
    /// Allocate a cursor struct in WASM linear memory, returning a handle (pointer as i64).
    ///
    /// # Safety
    /// The caller must ensure `T` is a valid, `Copy`-like struct that can be safely
    /// stored at an arbitrary aligned address in WASM linear memory.
    pub unsafe fn alloc_cursor<T>(initial: T) -> i64 {
        extern "C" {
            fn turso_malloc(size: i32) -> i32;
        }
        let size = core::mem::size_of::<T>() as i32;
        let ptr = turso_malloc(size);
        let typed = ptr as *mut T;
        core::ptr::write(typed, initial);
        ptr as i64
    }

    /// Get a mutable reference to the cursor struct from its handle.
    ///
    /// # Safety
    /// The handle must have been returned by `alloc_cursor::<T>`.
    pub unsafe fn get_cursor_mut<T>(handle: i64) -> &'static mut T {
        &mut *(handle as *mut T)
    }

    /// Get a shared reference to the cursor struct from its handle.
    ///
    /// # Safety
    /// The handle must have been returned by `alloc_cursor::<T>`.
    pub unsafe fn get_cursor<T>(handle: i64) -> &'static T {
        &*(handle as *const T)
    }
}
