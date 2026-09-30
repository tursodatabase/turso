#[cfg(not(nightly))]
pub use allocator_api2::{
    alloc::{AllocError, Allocator as ApiAllocator, Global, Layout},
    collections::TryReserveError,
};

#[cfg(nightly)]
pub use std::alloc::{AllocError, Allocator as ApiAllocator, Global, Layout};
