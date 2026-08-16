#![cfg_attr(
    nightly,
    feature(
        allocator_api,
        btreemap_alloc,
        clone_from_ref,
        min_specialization,
        try_with_capacity,
        trusted_len,
        vec_push_within_capacity
    )
)]

pub mod alloc;
pub mod assert;
pub mod error;
pub mod fast_lock;
pub mod skiplist;
pub mod stack;
pub mod sync;
pub mod thread;

pub use error::{io_error, CompletionError, LimboError};

pub type Result<T, E = LimboError> = std::result::Result<T, E>;
