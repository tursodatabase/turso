//! Traits for SQL statement generation.
//!
//! This module provides the `SqlGeneratorKind` trait which generalizes the pattern
//! of having enumerated "kinds" of SQL operations that can be filtered and weighted.

use proptest::prelude::*;

use crate::profile::StatementProfile;

/// Trait for kinds of SQL operations that can be generated.
///
/// This trait abstracts the common pattern of having an enum of operation kinds
/// where each kind can:
/// - Check if it's available given some context (e.g., schema state)
/// - Check if it's supported (arbitrary filter for unsupported operations)
/// - Build a proptest strategy to generate that kind of operation
///
/// # Type Parameters
///
/// - `Context<'a>`: The context type needed for availability checks and strategy building.
///   This is a GAT (Generic Associated Type) that supports lifetimes, allowing contexts
///   that borrow data (e.g., `AlterTableContext<'a>`).
/// - `Output`: The type of statement/operation this generator produces.
///
/// # Example
///
/// ```ignore
/// impl SqlGeneratorKind for MyOpKind {
///     type Context<'a> = MyContext<'a>;
///     type Output = MyStatement;
///
///     fn available(&self, ctx: &Self::Context<'_>) -> bool {
///         // Check if this operation can be generated given the context
///     }
///
///     fn strategy(&self, ctx: &Self::Context<'_>, profile: &StatementProfile) -> BoxedStrategy<Self::Output> {
///         // Build the proptest strategy using the profile
///     }
/// }
/// ```
pub trait SqlGeneratorKind: Sized + Copy {
    /// The context type needed for availability checks and strategy building.
    /// This is a GAT that supports lifetimes for contexts that borrow data.
    type Context<'a>: ?Sized;

    /// The output type produced by this generator.
    type Output;

    /// Returns true if this kind can be generated given the context.
    ///
    /// This is used to filter out operations that cannot be performed
    /// given the current state (e.g., DROP COLUMN when table has only one column).
    fn available(&self, ctx: &Self::Context<'_>) -> bool;

    /// Returns true if this kind is currently supported for generation.
    ///
    /// This is an arbitrary filter that can be used to disable certain
    /// operations entirely, regardless of context. Useful for temporarily
    /// disabling operations that are not yet fully implemented.
    fn supported(&self) -> bool {
        true
    }

    /// Builds a proptest strategy for generating this kind of operation.
    ///
    /// Caller should ensure `available(ctx)` and `supported()` return true
    /// before calling this method.
    ///
    /// The `profile` parameter provides fine-grained control over generation.
    /// Pass `&StatementProfile::default()` for default generation behavior.
    fn strategy<'a>(
        &self,
        ctx: &Self::Context<'a>,
        profile: &StatementProfile,
    ) -> BoxedStrategy<Self::Output>;
}

/// Extension trait providing helper methods for iterators of (Kind, weight) pairs.
pub trait WeightedKindIteratorExt<K, O>: Iterator<Item = (K, u32)>
where
    K: SqlGeneratorKind<Output = O>,
{
    /// Filters to only supported and available operations, then builds weighted strategies.
    ///
    /// This is a convenience method that combines the common pattern of:
    /// 1. Filtering by `supported()` and `available(ctx)`
    /// 2. Mapping each kind to its weighted strategy
    fn into_weighted_strategies(
        self,
        ctx: &K::Context<'_>,
        profile: &StatementProfile,
    ) -> Vec<(u32, BoxedStrategy<O>)>
    where
        Self: Sized,
    {
        self.filter(|(kind, _)| kind.supported() && kind.available(ctx))
            .map(|(kind, weight)| (weight, kind.strategy(ctx, profile)))
            .collect()
    }
}

impl<I, K, O> WeightedKindIteratorExt<K, O> for I
where
    I: Iterator<Item = (K, u32)>,
    K: SqlGeneratorKind<Output = O>,
{
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy)]
    enum TestKind {
        A,
        B,
    }

    impl SqlGeneratorKind for TestKind {
        type Context<'a> = bool; // Simple context: true = available, false = not
        type Output = &'static str;

        fn available(&self, ctx: &Self::Context<'_>) -> bool {
            match self {
                TestKind::A => true,
                TestKind::B => *ctx,
            }
        }

        fn strategy(
            &self,
            _ctx: &Self::Context<'_>,
            _profile: &StatementProfile,
        ) -> BoxedStrategy<Self::Output> {
            match self {
                TestKind::A => Just("A").boxed(),
                TestKind::B => Just("B").boxed(),
            }
        }
    }

    #[test]
    fn test_weighted_kind_iterator() {
        let kinds = [(TestKind::A, 10), (TestKind::B, 20)];
        let profile = StatementProfile::default();

        // When context is true, both should be available
        let strategies: Vec<_> = kinds
            .iter()
            .copied()
            .into_weighted_strategies(&true, &profile);
        assert_eq!(strategies.len(), 2);

        // When context is false, only A should be available
        let strategies: Vec<_> = kinds
            .iter()
            .copied()
            .into_weighted_strategies(&false, &profile);
        assert_eq!(strategies.len(), 1);
    }
}
