//! Generation context with RNG, trace, and coverage.

use anarchist_readable_name_generator_lib::readable_name_custom;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::trace::{Coverage, ExprKind, Origin, OriginPath, StmtKind, Trace, TraceBuilder};

/// Context for SQL generation.
///
/// The context holds the RNG, trace builder, coverage tracker, and current
/// scope information. It is passed to all generation functions.
pub struct Context {
    rng: StdRng,
    trace_builder: TraceBuilder,
    coverage: Coverage,
    origin_stack: OriginPath,
    depth: usize,
    subquery_depth: usize,
}

impl Context {
    /// Create a new context with a random seed.
    pub fn new() -> Self {
        Self::new_with_seed(rand::rng().random())
    }

    /// Create a new context with a specific seed.
    pub fn new_with_seed(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            trace_builder: TraceBuilder::new(),
            coverage: Coverage::new(),
            origin_stack: OriginPath::new(),
            depth: 0,
            subquery_depth: 0,
        }
    }

    /// Get the current scope as a string.
    pub fn current_scope(&self) -> &str {
        // Return a static string based on current origin
        match self.origin_stack.current() {
            Origin::Root => "root",
            Origin::Select => "select",
            Origin::From => "from",
            Origin::Where => "where",
            Origin::GroupBy => "group_by",
            Origin::Having => "having",
            Origin::OrderBy => "order_by",
            Origin::Limit => "limit",
            Origin::Insert => "insert",
            Origin::InsertValues => "insert_values",
            Origin::Update => "update",
            Origin::UpdateSet => "update_set",
            Origin::Delete => "delete",
            Origin::AlterTable => "alter_table",
            Origin::AlterTableAction => "alter_table_action",
            Origin::Subquery => "subquery",
            Origin::CaseWhen => "case_when",
            Origin::CaseThen => "case_then",
            Origin::CaseElse => "case_else",
            Origin::FunctionArg => "function_arg",
            Origin::BinaryOpLeft => "binary_op_left",
            Origin::BinaryOpRight => "binary_op_right",
            Origin::CreateTrigger => "create_trigger",
            Origin::TriggerWhen => "trigger_when",
            Origin::TriggerBody => "trigger_body",
        }
    }

    /// Get the current origin path.
    pub fn current_origin_path(&self) -> &OriginPath {
        &self.origin_stack
    }

    /// Get the current expression depth.
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Get the current subquery depth.
    pub fn subquery_depth(&self) -> usize {
        self.subquery_depth
    }

    /// Record an expression kind (automatically tracked by origin).
    pub fn record(&mut self, kind: ExprKind) {
        self.coverage.record_expr(&self.origin_stack, kind);
        self.trace_builder.add_expr(kind);
    }

    /// Record a statement kind.
    pub fn record_stmt(&mut self, kind: StmtKind) {
        self.coverage.record_stmt(kind);
        self.trace_builder.add_stmt(kind);
    }

    /// Run the closure under the given scope. Should only be used for generation
    pub fn scope<T>(&mut self, origin: Origin, func: impl FnOnce(&mut Self) -> T) -> T {
        self.enter_scope(origin);
        let t = func(self);
        self.exit_scope();
        t
    }

    /// Enter a named scope.
    fn enter_scope(&mut self, origin: Origin) {
        self.origin_stack.push(origin);
        self.depth += 1;
        self.trace_builder.push_scope(origin);

        if matches!(origin, Origin::Subquery) {
            self.subquery_depth += 1;
        }
    }

    /// Exit the current scope.
    fn exit_scope(&mut self) {
        let origin = self.origin_stack.current();
        if matches!(origin, Origin::Subquery) {
            self.subquery_depth = self.subquery_depth.saturating_sub(1);
        }

        self.origin_stack.pop();
        self.depth = self.depth.saturating_sub(1);
        self.trace_builder.pop_scope();
    }

    /// Get a reference to the coverage data.
    pub fn coverage(&self) -> &Coverage {
        &self.coverage
    }

    /// Take the coverage data, leaving an empty Coverage in place.
    pub fn take_coverage(&mut self) -> Coverage {
        std::mem::take(&mut self.coverage)
    }

    /// Build the trace tree.
    pub fn into_trace(self) -> Trace {
        self.trace_builder.build()
    }

    // RNG methods

    /// Generate a random boolean.
    pub fn gen_bool(&mut self) -> bool {
        self.rng.random()
    }

    /// Generate a random boolean with the given probability of being true.
    pub fn gen_bool_with_prob(&mut self, prob: f64) -> bool {
        self.rng.random_bool(prob)
    }

    /// Generate a random usize in the range [0, max).
    pub fn gen_range(&mut self, max: usize) -> usize {
        if max == 0 {
            0
        } else {
            self.rng.random_range(0..max)
        }
    }

    /// Generate a random usize in the range [min, max].
    pub fn gen_range_inclusive(&mut self, min: usize, max: usize) -> usize {
        if min >= max {
            min
        } else {
            self.rng.random_range(min..=max)
        }
    }

    /// Generate a random i64 in the given range.
    pub fn gen_i64_range(&mut self, min: i64, max: i64) -> i64 {
        if min >= max {
            min
        } else {
            self.rng.random_range(min..=max)
        }
    }

    /// Generate a random f64 in the given range.
    pub fn gen_f64_range(&mut self, min: f64, max: f64) -> f64 {
        if min >= max {
            min
        } else {
            self.rng.random_range(min..=max)
        }
    }

    /// Select a random element from a slice.
    pub fn choose<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        if items.is_empty() {
            None
        } else {
            let idx = self.gen_range(items.len());
            Some(&items[idx])
        }
    }

    /// Select a random index based on weights.
    ///
    /// Returns `None` if all weights are zero (no valid options).
    pub fn weighted_index(&mut self, weights: &[u32]) -> Option<usize> {
        let total: u32 = weights.iter().sum();
        if total == 0 {
            return None;
        }

        let mut threshold = self.rng.random_range(0..total);
        for (i, &weight) in weights.iter().enumerate() {
            if threshold < weight {
                return Some(i);
            }
            threshold -= weight;
        }

        Some(weights.len().saturating_sub(1))
    }

    /// Generate a random string of the given length.
    pub fn gen_string(&mut self, len: usize) -> String {
        const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        (0..len)
            .map(|_| {
                let idx = self.gen_range(CHARSET.len());
                CHARSET[idx] as char
            })
            .collect()
    }

    /// Generate random bytes of the given length.
    pub fn gen_bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.rng.random()).collect()
    }

    /// Generate a readable identifier name.
    ///
    /// Uses the anarchist readable name generator to create human-readable
    /// names like "happy_elephant" or "swift_falcon".
    pub fn gen_readable_name(&mut self) -> String {
        readable_name_custom("_", &mut self.rng).replace('-', "_")
    }

    /// Generate a unique name with the given prefix that doesn't exist in the provided set.
    ///
    /// Generates readable names in the format `{prefix}_{readable_name}` until finding one
    /// that is not present in `existing`. Example: "tbl_happy_elephant".
    ///
    /// # Panics
    /// Panics if unable to find a unique name after 1,000,000 attempts.
    pub fn gen_unique_name(
        &mut self,
        prefix: &str,
        existing: &std::collections::HashSet<String>,
    ) -> String {
        for _ in 0..1_000_000 {
            let readable = self.gen_readable_name();
            let name = format!("{prefix}_{readable}");
            if !existing.contains(&name) {
                return name;
            }
        }
        panic!("failed to generate unique name with prefix '{prefix}' after 1,000,000 attempts");
    }

    /// Generate a unique name with the given prefix, excluding both an existing set
    /// and a specific excluded name.
    ///
    /// Useful when renaming something - the new name must not match existing names
    /// AND must not match the current name. Example: "col_swift_falcon".
    ///
    /// # Panics
    /// Panics if unable to find a unique name after 1,000,000 attempts.
    pub fn gen_unique_name_excluding(
        &mut self,
        prefix: &str,
        existing: &std::collections::HashSet<String>,
        exclude: &str,
    ) -> String {
        for _ in 0..1_000_000 {
            let readable = self.gen_readable_name();
            let name = format!("{prefix}_{readable}");
            if !existing.contains(&name) && name != exclude {
                return name;
            }
        }
        panic!(
            "failed to generate unique name with prefix '{prefix}' (excluding '{exclude}') after 1,000,000 attempts"
        );
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_creation() {
        let ctx = Context::new_with_seed(42);
        assert_eq!(ctx.depth(), 0);
        assert_eq!(ctx.subquery_depth(), 0);
    }

    #[test]
    fn test_scope_management() {
        let mut ctx = Context::new_with_seed(42);

        ctx.enter_scope(Origin::Select);
        assert_eq!(ctx.depth(), 1);
        assert_eq!(ctx.current_scope(), "select");

        ctx.enter_scope(Origin::Where);
        assert_eq!(ctx.depth(), 2);
        assert_eq!(ctx.current_scope(), "where");

        ctx.exit_scope();
        assert_eq!(ctx.depth(), 1);
        assert_eq!(ctx.current_scope(), "select");

        ctx.exit_scope();
        assert_eq!(ctx.depth(), 0);
    }

    #[test]
    fn test_subquery_depth_tracking() {
        let mut ctx = Context::new_with_seed(42);

        ctx.enter_scope(Origin::Select);
        assert_eq!(ctx.subquery_depth(), 0);

        ctx.enter_scope(Origin::Subquery);
        assert_eq!(ctx.subquery_depth(), 1);

        ctx.enter_scope(Origin::Subquery);
        assert_eq!(ctx.subquery_depth(), 2);

        ctx.exit_scope();
        assert_eq!(ctx.subquery_depth(), 1);

        ctx.exit_scope();
        assert_eq!(ctx.subquery_depth(), 0);

        ctx.exit_scope();
    }

    #[test]
    fn test_recording() {
        let mut ctx = Context::new_with_seed(42);

        ctx.record(ExprKind::ColumnRef);
        ctx.record(ExprKind::Literal);
        ctx.record_stmt(StmtKind::Select);

        assert_eq!(ctx.coverage().total_exprs(), 2);
        assert_eq!(ctx.coverage().total_stmts(), 1);
    }

    #[test]
    fn test_rng_methods() {
        let mut ctx = Context::new_with_seed(42);

        // Test range generation
        for _ in 0..100 {
            let val = ctx.gen_range(10);
            assert!(val < 10);
        }

        // Test weighted index
        let weights = [10, 20, 30, 40];
        let mut counts = [0; 4];
        for _ in 0..1000 {
            let idx = ctx.weighted_index(&weights).unwrap();
            counts[idx] += 1;
        }
        // The last item (weight 40) should be chosen more often than the first (weight 10)
        assert!(counts[3] > counts[0]);

        // Test weighted index with all zeros returns None
        let zero_weights = [0, 0, 0];
        assert!(ctx.weighted_index(&zero_weights).is_none());
    }

    #[test]
    fn test_choose() {
        let mut ctx = Context::new_with_seed(42);
        let items = vec!["a", "b", "c"];

        let chosen = ctx.choose(&items);
        assert!(chosen.is_some());
        assert!(items.contains(chosen.unwrap()));

        let empty: Vec<&str> = vec![];
        assert!(ctx.choose(&empty).is_none());
    }

    #[test]
    fn test_gen_string() {
        let mut ctx = Context::new_with_seed(42);
        let s = ctx.gen_string(10);
        assert_eq!(s.len(), 10);
        assert!(s.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn test_gen_unique_name() {
        let mut ctx = Context::new_with_seed(42);

        let existing: std::collections::HashSet<String> = ["tbl_1", "tbl_2", "tbl_3"]
            .into_iter()
            .map(String::from)
            .collect();
        let name = ctx.gen_unique_name("tbl", &existing);
        assert!(name.starts_with("tbl_"));
        assert!(!existing.contains(&name));
    }

    #[test]
    fn test_gen_unique_name_excluding() {
        let mut ctx = Context::new_with_seed(42);

        let existing: std::collections::HashSet<String> =
            ["tbl_1", "tbl_2"].into_iter().map(String::from).collect();
        let name = ctx.gen_unique_name_excluding("tbl", &existing, "tbl_100");
        assert!(name.starts_with("tbl_"));
        assert!(!existing.contains(&name));
        assert_ne!(name, "tbl_100");
    }
}
