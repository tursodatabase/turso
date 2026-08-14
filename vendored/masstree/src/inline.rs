//! Inline value storage infrastructure.
//!
//! # Modules
//!
//! - [`bits`]: The [`InlineBits`](bits::InlineBits) trait for value encoding/decoding
//! - [`sentinel`]: Sentinel pointer utilities for distinguishing inline vs heap values

pub mod bits;
pub mod sentinel;

