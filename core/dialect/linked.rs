//! Rules that the frontend crate gives to core when the binary is linked.
//!
//! Each binary links one frontend crate, and that crate defines the
//! functions that this module declares. A binary that links no frontend, or
//! two, does not link.

extern "Rust" {
    fn turso_frontend_nan_is_a_value() -> bool;
}

/// True when a REAL can hold NaN in the databases of this binary.
pub fn nan_is_a_value() -> bool {
    // SAFETY: the frontend crate of the binary defines the function with
    // this signature.
    unsafe { turso_frontend_nan_is_a_value() }
}

#[cfg(test)]
mod unit_test_frontend {
    #[no_mangle]
    fn turso_frontend_nan_is_a_value() -> bool {
        false
    }
}
