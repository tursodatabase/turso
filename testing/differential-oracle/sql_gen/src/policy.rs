//! Runtime policy for controlling generation weights.
//!
//! The Policy struct provides soft constraints through weights that control
//! the probability of generating different SQL constructs. Unlike capabilities
//! which are enforced at compile time, policy weights can be adjusted at runtime.

use crate::context::Context;
use crate::error::GenError;
use crate::trace::StmtKind;

/// Runtime policy controlling generation weights and limits.
#[derive(Debug, Clone)]
pub struct Policy {
    /// Weights for statement types (0 = disabled).
    pub stmt_weights: StmtWeights,

    /// Weights for expression types.
    pub expr_weights: ExprWeights,

    /// Weights for binary operators.
    pub binop_weights: BinOpWeights,

    /// Maximum recursion depth for expressions.
    pub max_expr_depth: usize,

    /// Maximum recursion depth for subqueries.
    pub max_subquery_depth: usize,

    /// Probability of generating NULL literal [0.0, 1.0].
    pub null_probability: f64,

    /// Maximum number of columns in SELECT.
    pub max_select_columns: usize,

    /// Maximum number of tables in FROM/JOIN.
    pub max_tables: usize,

    /// Whether to generate aliases.
    pub generate_aliases: bool,

    /// Maximum number of rows for INSERT.
    pub max_insert_rows: usize,

    /// Maximum number of values in IN list.
    pub max_in_list_size: usize,

    /// Maximum LIMIT value.
    pub max_limit: u64,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            stmt_weights: StmtWeights::default(),
            expr_weights: ExprWeights::default(),
            binop_weights: BinOpWeights::default(),
            max_expr_depth: 4,
            max_subquery_depth: 2,
            null_probability: 0.05,
            max_select_columns: 10,
            max_tables: 3,
            generate_aliases: true,
            max_insert_rows: 5,
            max_in_list_size: 10,
            max_limit: 1000,
        }
    }
}

impl Policy {
    /// Create a new policy with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder method to set statement weights.
    pub fn with_stmt_weights(mut self, weights: StmtWeights) -> Self {
        self.stmt_weights = weights;
        self
    }

    /// Builder method to set expression weights.
    pub fn with_expr_weights(mut self, weights: ExprWeights) -> Self {
        self.expr_weights = weights;
        self
    }

    /// Builder method to set max expression depth.
    pub fn with_max_expr_depth(mut self, depth: usize) -> Self {
        self.max_expr_depth = depth;
        self
    }

    /// Builder method to set max subquery depth.
    pub fn with_max_subquery_depth(mut self, depth: usize) -> Self {
        self.max_subquery_depth = depth;
        self
    }

    /// Builder method to set null probability.
    pub fn with_null_probability(mut self, prob: f64) -> Self {
        self.null_probability = prob.clamp(0.0, 1.0);
        self
    }

    /// Builder method to disable aliases.
    pub fn without_aliases(mut self) -> Self {
        self.generate_aliases = false;
        self
    }

    /// Select a statement kind from candidates based on weights.
    pub fn select_stmt_kind(
        &self,
        ctx: &mut Context,
        candidates: &[StmtKind],
    ) -> Result<StmtKind, GenError> {
        let weights: Vec<u32> = candidates
            .iter()
            .map(|k| self.stmt_weights.weight_for(*k))
            .collect();

        let idx = ctx.weighted_index(&weights).ok_or_else(|| {
            GenError::exhausted(ctx.current_scope(), "all statement candidates have zero weight")
        })?;
        Ok(candidates[idx])
    }

    /// Select from weighted items.
    pub fn select_weighted<T: Clone>(
        &self,
        ctx: &mut Context,
        items: &[(T, u32)],
    ) -> Result<T, GenError> {
        let weights: Vec<u32> = items.iter().map(|(_, w)| *w).collect();

        let idx = ctx.weighted_index(&weights).ok_or_else(|| {
            GenError::exhausted(ctx.current_scope(), "all candidates have zero weight")
        })?;
        Ok(items[idx].0.clone())
    }
}

/// Weights for statement types.
#[derive(Debug, Clone)]
pub struct StmtWeights {
    // DML
    pub select: u32,
    pub insert: u32,
    pub update: u32,
    pub delete: u32,

    // DDL
    pub create_table: u32,
    pub drop_table: u32,
    pub create_index: u32,
    pub drop_index: u32,

    // Transactions
    pub begin: u32,
    pub commit: u32,
    pub rollback: u32,
}

impl Default for StmtWeights {
    fn default() -> Self {
        Self {
            // DML (higher weights for more common statements)
            select: 40,
            insert: 20,
            update: 15,
            delete: 10,
            // DDL (lower weights - less common)
            create_table: 2,
            drop_table: 1,
            create_index: 2,
            drop_index: 1,
            // Transactions (disabled by default)
            begin: 0,
            commit: 0,
            rollback: 0,
        }
    }
}

impl StmtWeights {
    /// Create weights for DML-only generation.
    pub fn dml_only() -> Self {
        Self {
            select: 40,
            insert: 30,
            update: 20,
            delete: 10,
            create_table: 0,
            drop_table: 0,
            create_index: 0,
            drop_index: 0,
            begin: 0,
            commit: 0,
            rollback: 0,
        }
    }

    /// Create weights for SELECT-only generation.
    pub fn select_only() -> Self {
        Self {
            select: 100,
            insert: 0,
            update: 0,
            delete: 0,
            create_table: 0,
            drop_table: 0,
            create_index: 0,
            drop_index: 0,
            begin: 0,
            commit: 0,
            rollback: 0,
        }
    }

    /// Get the weight for a statement kind.
    pub fn weight_for(&self, kind: StmtKind) -> u32 {
        match kind {
            StmtKind::Select => self.select,
            StmtKind::Insert => self.insert,
            StmtKind::Update => self.update,
            StmtKind::Delete => self.delete,
            StmtKind::CreateTable => self.create_table,
            StmtKind::DropTable => self.drop_table,
            StmtKind::CreateIndex => self.create_index,
            StmtKind::DropIndex => self.drop_index,
            StmtKind::Begin => self.begin,
            StmtKind::Commit => self.commit,
            StmtKind::Rollback => self.rollback,
        }
    }

    /// Returns an iterator over enabled statement kinds with their weights.
    pub fn enabled(&self) -> impl Iterator<Item = (StmtKind, u32)> {
        [
            (StmtKind::Select, self.select),
            (StmtKind::Insert, self.insert),
            (StmtKind::Update, self.update),
            (StmtKind::Delete, self.delete),
            (StmtKind::CreateTable, self.create_table),
            (StmtKind::DropTable, self.drop_table),
            (StmtKind::CreateIndex, self.create_index),
            (StmtKind::DropIndex, self.drop_index),
            (StmtKind::Begin, self.begin),
            (StmtKind::Commit, self.commit),
            (StmtKind::Rollback, self.rollback),
        ]
        .into_iter()
        .filter(|(_, w)| *w > 0)
    }
}

/// Weights for expression types.
#[derive(Debug, Clone)]
pub struct ExprWeights {
    pub column_ref: u32,
    pub literal: u32,
    pub binary_op: u32,
    pub unary_op: u32,
    pub function_call: u32,
    pub subquery: u32,
    pub case_expr: u32,
    pub cast: u32,
    pub between: u32,
    pub in_list: u32,
    pub is_null: u32,
}

impl Default for ExprWeights {
    fn default() -> Self {
        Self {
            column_ref: 30,
            literal: 25,
            binary_op: 15,
            unary_op: 5,
            function_call: 0, // Disabled initially
            subquery: 3,
            case_expr: 3,
            cast: 3,
            between: 5,
            in_list: 5,
            is_null: 6,
        }
    }
}

impl ExprWeights {
    /// Create simple expression weights (only column refs and literals).
    pub fn simple() -> Self {
        Self {
            column_ref: 50,
            literal: 50,
            binary_op: 0,
            unary_op: 0,
            function_call: 0,
            subquery: 0,
            case_expr: 0,
            cast: 0,
            between: 0,
            in_list: 0,
            is_null: 0,
        }
    }

    /// Create complex expression weights.
    pub fn complex() -> Self {
        Self {
            column_ref: 20,
            literal: 15,
            binary_op: 20,
            unary_op: 8,
            function_call: 15,
            subquery: 5,
            case_expr: 7,
            cast: 5,
            between: 2,
            in_list: 2,
            is_null: 1,
        }
    }
}

/// Weights for binary operators.
#[derive(Debug, Clone)]
pub struct BinOpWeights {
    // Comparison
    pub eq: u32,
    pub ne: u32,
    pub lt: u32,
    pub le: u32,
    pub gt: u32,
    pub ge: u32,

    // Logical
    pub and: u32,
    pub or: u32,

    // Arithmetic
    pub add: u32,
    pub sub: u32,
    pub mul: u32,
    pub div: u32,

    // String
    pub concat: u32,
    pub like: u32,
    pub glob: u32,
}

impl Default for BinOpWeights {
    fn default() -> Self {
        Self {
            // Comparison (most common)
            eq: 20,
            ne: 10,
            lt: 10,
            le: 8,
            gt: 10,
            ge: 8,
            // Logical
            and: 15,
            or: 10,
            // Arithmetic
            add: 8,
            sub: 6,
            mul: 4,
            div: 3,
            // String
            concat: 5,
            like: 8,
            glob: 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_policy() {
        let policy = Policy::default();
        assert_eq!(policy.max_expr_depth, 4);
        assert_eq!(policy.max_subquery_depth, 2);
        assert!(policy.generate_aliases);
    }

    #[test]
    fn test_stmt_weights_enabled() {
        let weights = StmtWeights::default();
        let enabled: Vec<_> = weights.enabled().collect();
        assert!(!enabled.is_empty());
        assert!(enabled.iter().any(|(k, _)| *k == StmtKind::Select));
    }

    #[test]
    fn test_stmt_weights_dml_only() {
        let weights = StmtWeights::dml_only();
        assert!(weights.select > 0);
        assert!(weights.insert > 0);
        assert_eq!(weights.create_table, 0);
        assert_eq!(weights.begin, 0);
    }

    #[test]
    fn test_expr_weights_simple() {
        let weights = ExprWeights::simple();
        assert!(weights.column_ref > 0);
        assert!(weights.literal > 0);
        assert_eq!(weights.binary_op, 0);
        assert_eq!(weights.subquery, 0);
    }

    #[test]
    fn test_policy_builder() {
        let policy = Policy::default()
            .with_max_expr_depth(2)
            .with_null_probability(0.1)
            .without_aliases();

        assert_eq!(policy.max_expr_depth, 2);
        assert!((policy.null_probability - 0.1).abs() < f64::EPSILON);
        assert!(!policy.generate_aliases);
    }
}
