use rand::distr::weighted::WeightedIndex;
use sql_generation::generation::GenerationContext;

use crate::runner::env::ShadowTablesMut;

pub mod plan;
pub mod property;
pub mod query;

/// Shadow trait for types that can be "shadowed" in the simulator environment.
/// Shadowing is a process of applying a transformation to the simulator environment
/// that reflects the changes made by the query or operation represented by the type.
/// The result of the shadowing is typically a vector of rows, which can be used to
/// update the simulator environment or to verify the correctness of the operation.
/// The `Result` type is used to indicate the type of the result of the shadowing
/// operation, which can vary depending on the type of the operation being shadowed.
/// For example, a `Create` operation might return an empty vector, while an `Insert` operation
/// might return a vector of rows that were inserted into the table.
pub(crate) trait Shadow {
    type Result;
    fn shadow(&self, tables: &mut ShadowTablesMut<'_>) -> Self::Result;
}

pub(super) trait WeightedDistribution {
    type Item;
    type GenItem;
    fn items(&self) -> &[Self::Item];
    fn weights(&self) -> &WeightedIndex<u32>;
    fn sample<R: rand::Rng + ?Sized, C: GenerationContext>(
        &self,
        rng: &mut R,
        context: &C,
    ) -> Self::GenItem;
}
