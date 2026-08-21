//! Specialized `TursoFromIterator` collection for `Result` items.
//!
//! Lives in its own file because `default fn` is gated syntax: stable rustc
//! (>= 1.98) warns about it before cfg stripping, so it must not appear in any
//! file a stable build parses. The `#[cfg(nightly)]` on the `mod` declaration
//! keeps this file out of stable builds entirely.

use super::super::{trusted_len, TursoFromIterator, TursoTryWithCapacityExt};
use super::try_from_result_iter;
use crate::alloc::{TryReserveError, Vec};
use std::iter::TrustedLen;

pub(super) trait TrySpecFromResultIter<T, E, I>: Sized {
    fn try_from_result_iter(iter: I) -> Result<Self, TryReserveError>;
}

impl<T, E, F, C, I> TrySpecFromResultIter<T, E, I> for Result<C, F>
where
    C: TursoFromIterator<T>,
    F: From<E>,
    I: Iterator<Item = Result<T, E>>,
{
    default fn try_from_result_iter(iter: I) -> Result<Self, TryReserveError> {
        try_from_result_iter(iter)
    }
}

impl<T, E, F, I> TrySpecFromResultIter<T, E, I> for Result<Vec<T>, F>
where
    F: From<E>,
    I: TrustedLen<Item = Result<T, E>>,
{
    fn try_from_result_iter(iter: I) -> Result<Self, TryReserveError> {
        // Specialize before the fallback's `map_while`, which cannot retain
        // `TrustedLen` because an error may stop collection early.
        let additional = trusted_len(iter.size_hint())?;
        let mut values = <Vec<T> as TursoTryWithCapacityExt>::try_with_capacity_ext(additional)?;

        for item in iter {
            match item {
                Ok(value) => {
                    let _ = values.push_within_capacity(value);
                }
                Err(error) => return Ok(Err(F::from(error))),
            }
        }
        Ok(Ok(values))
    }
}
