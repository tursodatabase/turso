use std::ops::DerefMut;

use downcast_rs::impl_downcast;

use crate::docset::DocSet;
use crate::Score;

/// Scored set of documents matching a query within a specific segment.
///
/// See [`Query`](crate::query::Query).
pub trait Scorer: downcast_rs::Downcast + DocSet + 'static {
    /// Returns the score.
    ///
    /// This method will perform a bit of computation and is not cached.
    fn score(&mut self) -> Score;
}

impl_downcast!(Scorer);

impl Scorer for Box<dyn Scorer> {
    #[inline]
    fn score(&mut self) -> Score {
        self.deref_mut().score()
    }
}
