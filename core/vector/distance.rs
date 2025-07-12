use super::vector_types::Vector;
use crate::Result;

pub(crate) mod euclidean;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DistanceType {
    /// Euclidean distance. This is a very common distance metric that
    /// accounts for both magnitude and direction when determining the distance
    /// between vectors. Euclidean distance has a range of [0, ∞).
    Euclidean,

    // TODO(asukamilet): Refactor the current `vector_types.rs` to integrate
    #[allow(dead_code)]
    /// Cosine distance. This is a measure of similarity between two vectors
    Cosine,
}

pub trait DistanceCalculator {
    #[allow(unused)]
    fn distance_type() -> DistanceType;

    fn calculate(v1: &Vector, v2: &Vector) -> Result<f64>;
}
