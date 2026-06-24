#[cfg(nightly)]
#[path = "arc/nightly.rs"]
mod imp;

#[cfg(not(nightly))]
#[path = "arc/stable.rs"]
mod imp;

pub use imp::{try_arc_slice_from_slice, ArcSlice};
