//! Runtime policy for controlling generation weights and parameters.
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

    /// Weights for unary operators.
    pub unary_weights: UnaryOpWeights,

    /// Configuration for literal generation.
    pub literal_config: LiteralConfig,

    /// Configuration for SELECT statement generation.
    pub select_config: SelectConfig,

    /// Configuration for INSERT statement generation.
    pub insert_config: InsertConfig,

    /// Configuration for UPDATE statement generation.
    pub update_config: UpdateConfig,

    /// Configuration for DELETE statement generation.
    pub delete_config: DeleteConfig,

    /// Configuration for identifier generation.
    pub identifier_config: IdentifierConfig,

    /// Configuration for expression generation.
    pub expr_config: ExprConfig,

    /// Configuration for ALTER TABLE statement generation.
    pub alter_table_config: AlterTableConfig,

    /// Configuration for trigger generation.
    pub trigger_config: TriggerConfig,

    /// Maximum recursion depth for expressions.
    pub max_expr_depth: usize,

    /// Maximum recursion depth for subqueries.
    pub max_subquery_depth: usize,

    /// Maximum number of tables in FROM/JOIN.
    pub max_tables: usize,

    /// Maximum LIMIT value.
    pub max_limit: u64,

    /// Maximum number of values in IN list.
    pub max_in_list_size: usize,

    /// Maximum number of WHEN clauses in CASE expression.
    pub max_case_branches: usize,

    /// Maximum number of items in ORDER BY clause.
    pub max_order_by_items: usize,

    /// Maximum number of items in GROUP BY clause.
    pub max_group_by_items: usize,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            stmt_weights: StmtWeights::default(),
            expr_weights: ExprWeights::default(),
            binop_weights: BinOpWeights::default(),
            unary_weights: UnaryOpWeights::default(),
            literal_config: LiteralConfig::default(),
            select_config: SelectConfig::default(),
            insert_config: InsertConfig::default(),
            update_config: UpdateConfig::default(),
            delete_config: DeleteConfig::default(),
            identifier_config: IdentifierConfig::default(),
            expr_config: ExprConfig::default(),
            alter_table_config: AlterTableConfig::default(),
            trigger_config: TriggerConfig::default(),
            max_expr_depth: 4,
            max_subquery_depth: 2,
            max_tables: 3,
            max_limit: 1000,
            max_in_list_size: 10,
            max_case_branches: 3,
            max_order_by_items: 3,
            max_group_by_items: 3,
        }
    }
}

impl Policy {
    /// Create a new policy with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    // =========================================================================
    // Builder methods
    // =========================================================================

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

    /// Builder method to set binary operator weights.
    pub fn with_binop_weights(mut self, weights: BinOpWeights) -> Self {
        self.binop_weights = weights;
        self
    }

    /// Builder method to set unary operator weights.
    pub fn with_unary_weights(mut self, weights: UnaryOpWeights) -> Self {
        self.unary_weights = weights;
        self
    }

    /// Builder method to set literal configuration.
    pub fn with_literal_config(mut self, config: LiteralConfig) -> Self {
        self.literal_config = config;
        self
    }

    /// Builder method to set SELECT configuration.
    pub fn with_select_config(mut self, config: SelectConfig) -> Self {
        self.select_config = config;
        self
    }

    /// Builder method to set INSERT configuration.
    pub fn with_insert_config(mut self, config: InsertConfig) -> Self {
        self.insert_config = config;
        self
    }

    /// Builder method to set UPDATE configuration.
    pub fn with_update_config(mut self, config: UpdateConfig) -> Self {
        self.update_config = config;
        self
    }

    /// Builder method to set DELETE configuration.
    pub fn with_delete_config(mut self, config: DeleteConfig) -> Self {
        self.delete_config = config;
        self
    }

    /// Builder method to set identifier configuration.
    pub fn with_identifier_config(mut self, config: IdentifierConfig) -> Self {
        self.identifier_config = config;
        self
    }

    /// Builder method to set expression configuration.
    pub fn with_expr_config(mut self, config: ExprConfig) -> Self {
        self.expr_config = config;
        self
    }

    /// Builder method to set trigger configuration.
    pub fn with_trigger_config(mut self, config: TriggerConfig) -> Self {
        self.trigger_config = config;
        self
    }

    /// Builder method to set ALTER TABLE configuration.
    pub fn with_alter_table_config(mut self, config: AlterTableConfig) -> Self {
        self.alter_table_config = config;
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
        self.literal_config.null_probability = prob.clamp(0.0, 1.0);
        self
    }

    /// Builder method to disable aliases.
    pub fn without_aliases(mut self) -> Self {
        self.identifier_config.generate_table_aliases = false;
        self.identifier_config.generate_column_aliases = false;
        self
    }

    // =========================================================================
    // Selection methods
    // =========================================================================

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

    // =========================================================================
    // Convenience accessors (for backwards compatibility)
    // =========================================================================

    /// Get null probability.
    pub fn null_probability(&self) -> f64 {
        self.literal_config.null_probability
    }

    /// Get max select columns.
    pub fn max_select_columns(&self) -> usize {
        self.select_config.max_columns
    }

    /// Get max insert rows.
    pub fn max_insert_rows(&self) -> usize {
        self.insert_config.max_rows
    }

    /// Whether to generate aliases.
    pub fn generate_aliases(&self) -> bool {
        self.identifier_config.generate_table_aliases
            || self.identifier_config.generate_column_aliases
    }
}

// =============================================================================
// Statement Weights
// =============================================================================

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
    pub alter_table: u32,
    pub create_index: u32,
    pub drop_index: u32,
    pub create_trigger: u32,
    pub drop_trigger: u32,

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
            alter_table: 1,
            create_index: 2,
            drop_index: 1,
            create_trigger: 1,
            drop_trigger: 1,
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
            alter_table: 0,
            create_index: 0,
            drop_index: 0,
            create_trigger: 0,
            drop_trigger: 0,
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
            alter_table: 0,
            create_index: 0,
            drop_index: 0,
            create_trigger: 0,
            drop_trigger: 0,
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
            StmtKind::AlterTable => self.alter_table,
            StmtKind::CreateIndex => self.create_index,
            StmtKind::DropIndex => self.drop_index,
            StmtKind::CreateTrigger => self.create_trigger,
            StmtKind::DropTrigger => self.drop_trigger,
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
            (StmtKind::AlterTable, self.alter_table),
            (StmtKind::CreateIndex, self.create_index),
            (StmtKind::DropIndex, self.drop_index),
            (StmtKind::CreateTrigger, self.create_trigger),
            (StmtKind::DropTrigger, self.drop_trigger),
            (StmtKind::Begin, self.begin),
            (StmtKind::Commit, self.commit),
            (StmtKind::Rollback, self.rollback),
        ]
        .into_iter()
        .filter(|(_, w)| *w > 0)
    }
}

// =============================================================================
// Expression Weights
// =============================================================================

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

// =============================================================================
// Binary Operator Weights
// =============================================================================

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
    pub modulo: u32,

    // Bitwise
    pub bit_and: u32,
    pub bit_or: u32,
    pub left_shift: u32,
    pub right_shift: u32,

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
            modulo: 2,
            // Bitwise (disabled by default)
            bit_and: 0,
            bit_or: 0,
            left_shift: 0,
            right_shift: 0,
            // String
            concat: 5,
            like: 8,
            glob: 3,
        }
    }
}

impl BinOpWeights {
    /// Create weights for comparison-only operations.
    pub fn comparison_only() -> Self {
        Self {
            eq: 30,
            ne: 15,
            lt: 15,
            le: 10,
            gt: 15,
            ge: 10,
            and: 0,
            or: 0,
            add: 0,
            sub: 0,
            mul: 0,
            div: 0,
            modulo: 0,
            bit_and: 0,
            bit_or: 0,
            left_shift: 0,
            right_shift: 0,
            concat: 0,
            like: 0,
            glob: 0,
        }
    }

    /// Create weights that include bitwise operations.
    pub fn with_bitwise() -> Self {
        Self {
            bit_and: 3,
            bit_or: 3,
            left_shift: 2,
            right_shift: 2,
            ..Default::default()
        }
    }
}

// =============================================================================
// Unary Operator Weights
// =============================================================================

/// Weights for unary operators.
#[derive(Debug, Clone)]
pub struct UnaryOpWeights {
    pub neg: u32,
    pub not: u32,
    pub bit_not: u32,
    pub plus: u32,
}

impl Default for UnaryOpWeights {
    fn default() -> Self {
        Self {
            neg: 30,
            not: 40,
            bit_not: 10,
            plus: 20,
        }
    }
}

// =============================================================================
// Literal Configuration
// =============================================================================

/// Configuration for literal value generation.
#[derive(Debug, Clone)]
pub struct LiteralConfig {
    /// Probability of generating NULL literal [0.0, 1.0].
    pub null_probability: f64,

    /// Minimum integer value.
    pub int_min: i64,

    /// Maximum integer value.
    pub int_max: i64,

    /// Minimum real value.
    pub real_min: f64,

    /// Maximum real value.
    pub real_max: f64,

    /// Minimum string length.
    pub string_min_len: usize,

    /// Maximum string length.
    pub string_max_len: usize,

    /// Minimum blob size.
    pub blob_min_size: usize,

    /// Maximum blob size.
    pub blob_max_size: usize,

    /// Character set for generated strings.
    pub string_charset: StringCharset,

    /// Weights for literal types.
    pub type_weights: LiteralTypeWeights,
}

impl Default for LiteralConfig {
    fn default() -> Self {
        Self {
            null_probability: 0.05,
            int_min: -1_000_000,
            int_max: 1_000_000,
            real_min: -1_000_000.0,
            real_max: 1_000_000.0,
            string_min_len: 1,
            string_max_len: 50,
            blob_min_size: 1,
            blob_max_size: 100,
            string_charset: StringCharset::Alphanumeric,
            type_weights: LiteralTypeWeights::default(),
        }
    }
}

impl LiteralConfig {
    /// Create config for small integer ranges (useful for testing).
    pub fn small_integers() -> Self {
        Self {
            int_min: -100,
            int_max: 100,
            real_min: -100.0,
            real_max: 100.0,
            ..Default::default()
        }
    }

    /// Create config for positive integers only.
    pub fn positive_integers() -> Self {
        Self {
            int_min: 0,
            int_max: 1_000_000,
            real_min: 0.0,
            real_max: 1_000_000.0,
            ..Default::default()
        }
    }

    /// Create config with higher null probability.
    pub fn nullable(probability: f64) -> Self {
        Self {
            null_probability: probability.clamp(0.0, 1.0),
            ..Default::default()
        }
    }
}

/// Character set options for string generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringCharset {
    /// a-zA-Z0-9
    Alphanumeric,
    /// a-zA-Z
    Alpha,
    /// 0-9
    Numeric,
    /// ASCII printable characters
    AsciiPrintable,
    /// Include some unicode characters
    Unicode,
}

/// Weights for choosing literal types when type is not constrained.
#[derive(Debug, Clone)]
pub struct LiteralTypeWeights {
    pub integer: u32,
    pub real: u32,
    pub text: u32,
    pub blob: u32,
    pub null: u32,
}

impl Default for LiteralTypeWeights {
    fn default() -> Self {
        Self {
            integer: 40,
            real: 20,
            text: 30,
            blob: 5,
            null: 5,
        }
    }
}

// =============================================================================
// SELECT Configuration
// =============================================================================

/// Configuration for SELECT statement generation.
#[derive(Debug, Clone)]
pub struct SelectConfig {
    /// Minimum number of columns in SELECT (0 means SELECT * is possible).
    pub min_columns: usize,

    /// Maximum number of columns in SELECT.
    pub max_columns: usize,

    /// Probability of generating SELECT * (only if min_columns == 0).
    pub select_star_probability: f64,

    /// Probability of generating WHERE clause.
    pub where_probability: f64,

    /// Probability of generating ORDER BY clause.
    pub order_by_probability: f64,

    /// Probability of generating LIMIT clause.
    pub limit_probability: f64,

    /// Probability of generating OFFSET clause (only if LIMIT exists).
    pub offset_probability: f64,

    /// Probability of generating GROUP BY clause.
    pub group_by_probability: f64,

    /// Probability of generating HAVING clause (only if GROUP BY exists).
    pub having_probability: f64,

    /// Probability of generating DISTINCT.
    pub distinct_probability: f64,

    /// Probability of generating table alias.
    pub table_alias_probability: f64,

    /// Probability of generating column alias.
    pub column_alias_probability: f64,

    /// Maximum offset value.
    pub max_offset: u64,

    /// Order direction weights.
    pub order_direction_weights: OrderDirectionWeights,

    /// Nulls ordering weights.
    pub nulls_order_weights: NullsOrderWeights,
}

impl Default for SelectConfig {
    fn default() -> Self {
        Self {
            min_columns: 0,
            max_columns: 10,
            select_star_probability: 0.2,
            where_probability: 0.7,
            order_by_probability: 0.3,
            limit_probability: 0.4,
            offset_probability: 0.2,
            group_by_probability: 0.0, // Disabled by default
            having_probability: 0.3,
            distinct_probability: 0.1,
            table_alias_probability: 0.3,
            column_alias_probability: 0.2,
            max_offset: 100,
            order_direction_weights: OrderDirectionWeights::default(),
            nulls_order_weights: NullsOrderWeights::default(),
        }
    }
}

impl SelectConfig {
    /// Create config for simple queries (no optional clauses).
    pub fn simple() -> Self {
        Self {
            where_probability: 0.0,
            order_by_probability: 0.0,
            limit_probability: 0.0,
            offset_probability: 0.0,
            group_by_probability: 0.0,
            having_probability: 0.0,
            distinct_probability: 0.0,
            ..Default::default()
        }
    }

    /// Create config with all optional clauses highly likely.
    pub fn complex() -> Self {
        Self {
            where_probability: 0.9,
            order_by_probability: 0.7,
            limit_probability: 0.6,
            offset_probability: 0.4,
            group_by_probability: 0.3,
            having_probability: 0.5,
            distinct_probability: 0.2,
            ..Default::default()
        }
    }
}

/// Weights for ORDER BY direction.
#[derive(Debug, Clone)]
pub struct OrderDirectionWeights {
    pub asc: u32,
    pub desc: u32,
    pub unspecified: u32,
}

impl Default for OrderDirectionWeights {
    fn default() -> Self {
        Self {
            asc: 40,
            desc: 40,
            unspecified: 20,
        }
    }
}

/// Weights for NULLS ordering.
#[derive(Debug, Clone)]
pub struct NullsOrderWeights {
    pub first: u32,
    pub last: u32,
    pub unspecified: u32,
}

impl Default for NullsOrderWeights {
    fn default() -> Self {
        Self {
            first: 10,
            last: 10,
            unspecified: 80,
        }
    }
}

// =============================================================================
// INSERT Configuration
// =============================================================================

/// Configuration for INSERT statement generation.
#[derive(Debug, Clone)]
pub struct InsertConfig {
    /// Minimum number of rows to insert.
    pub min_rows: usize,

    /// Maximum number of rows to insert.
    pub max_rows: usize,

    /// Probability of specifying all columns explicitly.
    pub explicit_columns_probability: f64,

    /// Probability of using DEFAULT for a nullable column.
    pub default_probability: f64,

    /// Probability of using INSERT OR REPLACE.
    pub or_replace_probability: f64,

    /// Probability of using INSERT OR IGNORE.
    pub or_ignore_probability: f64,
}

impl Default for InsertConfig {
    fn default() -> Self {
        Self {
            min_rows: 1,
            max_rows: 5,
            explicit_columns_probability: 0.7,
            default_probability: 0.1,
            or_replace_probability: 0.0,
            or_ignore_probability: 0.0,
        }
    }
}

impl InsertConfig {
    /// Create config for single-row inserts.
    pub fn single_row() -> Self {
        Self {
            min_rows: 1,
            max_rows: 1,
            ..Default::default()
        }
    }

    /// Create config for bulk inserts.
    pub fn bulk(max_rows: usize) -> Self {
        Self {
            min_rows: 1,
            max_rows,
            ..Default::default()
        }
    }
}

// =============================================================================
// UPDATE Configuration
// =============================================================================

/// Configuration for UPDATE statement generation.
#[derive(Debug, Clone)]
pub struct UpdateConfig {
    /// Minimum number of SET clauses.
    pub min_set_clauses: usize,

    /// Maximum number of SET clauses.
    pub max_set_clauses: usize,

    /// Probability of generating WHERE clause.
    pub where_probability: f64,

    /// Probability of using UPDATE OR REPLACE.
    pub or_replace_probability: f64,

    /// Probability of using UPDATE OR IGNORE.
    pub or_ignore_probability: f64,
}

impl Default for UpdateConfig {
    fn default() -> Self {
        Self {
            min_set_clauses: 1,
            max_set_clauses: 3,
            where_probability: 0.8,
            or_replace_probability: 0.0,
            or_ignore_probability: 0.0,
        }
    }
}

// =============================================================================
// DELETE Configuration
// =============================================================================

/// Configuration for DELETE statement generation.
#[derive(Debug, Clone)]
pub struct DeleteConfig {
    /// Probability of generating WHERE clause (high by default to avoid deleting everything).
    pub where_probability: f64,

    /// Probability of generating LIMIT clause.
    pub limit_probability: f64,

    /// Maximum LIMIT value for DELETE.
    pub max_limit: u64,
}

impl Default for DeleteConfig {
    fn default() -> Self {
        Self {
            where_probability: 0.95,
            limit_probability: 0.1,
            max_limit: 100,
        }
    }
}

impl DeleteConfig {
    /// Create config that always has WHERE clause.
    pub fn safe() -> Self {
        Self {
            where_probability: 1.0,
            ..Default::default()
        }
    }
}

// =============================================================================
// ALTER TABLE Configuration
// =============================================================================

/// Configuration for ALTER TABLE statement generation.
#[derive(Debug, Clone, Default)]
pub struct AlterTableConfig {
    /// Weights for ALTER TABLE actions.
    pub action_weights: AlterTableActionWeights,
}

/// Weights for ALTER TABLE actions.
#[derive(Debug, Clone)]
pub struct AlterTableActionWeights {
    pub rename_table: u32,
    pub add_column: u32,
    pub drop_column: u32,
    pub rename_column: u32,
}

impl Default for AlterTableActionWeights {
    fn default() -> Self {
        Self {
            rename_table: 20,
            add_column: 40,
            drop_column: 20,
            rename_column: 20,
        }
    }
}

// =============================================================================
// Trigger Configuration
// =============================================================================

/// Configuration for trigger generation.
#[derive(Debug, Clone)]
pub struct TriggerConfig {
    /// Minimum number of statements in trigger body.
    pub min_body_statements: usize,

    /// Maximum number of statements in trigger body.
    pub max_body_statements: usize,

    /// Probability of generating WHEN clause.
    pub when_probability: f64,

    /// Probability of using FOR EACH ROW.
    pub for_each_row_probability: f64,

    /// Probability of using IF NOT EXISTS.
    pub if_not_exists_probability: f64,

    /// Probability of using IF EXISTS in DROP TRIGGER.
    pub if_exists_probability: f64,

    /// Weights for trigger timing (BEFORE, AFTER, INSTEAD OF).
    pub timing_weights: TriggerTimingWeights,

    /// Weights for trigger events (INSERT, UPDATE, DELETE).
    pub event_weights: TriggerEventWeights,

    /// Weights for statement types in trigger body.
    pub body_stmt_weights: TriggerBodyStmtWeights,

    /// Probability of UPDATE OF specifying columns.
    pub update_of_columns_probability: f64,

    /// Maximum number of columns in UPDATE OF clause.
    pub max_update_of_columns: usize,
}

impl Default for TriggerConfig {
    fn default() -> Self {
        Self {
            min_body_statements: 1,
            max_body_statements: 3,
            when_probability: 0.3,
            for_each_row_probability: 0.8,
            if_not_exists_probability: 0.5,
            if_exists_probability: 0.7,
            timing_weights: TriggerTimingWeights::default(),
            event_weights: TriggerEventWeights::default(),
            body_stmt_weights: TriggerBodyStmtWeights::default(),
            update_of_columns_probability: 0.3,
            max_update_of_columns: 3,
        }
    }
}

/// Weights for trigger timing.
#[derive(Debug, Clone)]
pub struct TriggerTimingWeights {
    pub before: u32,
    pub after: u32,
    pub instead_of: u32,
}

impl Default for TriggerTimingWeights {
    fn default() -> Self {
        Self {
            before: 40,
            after: 50,
            instead_of: 10,
        }
    }
}

/// Weights for trigger events.
#[derive(Debug, Clone)]
pub struct TriggerEventWeights {
    pub insert: u32,
    pub update: u32,
    pub delete: u32,
}

impl Default for TriggerEventWeights {
    fn default() -> Self {
        Self {
            insert: 35,
            update: 35,
            delete: 30,
        }
    }
}

/// Weights for statement types in trigger body.
#[derive(Debug, Clone)]
pub struct TriggerBodyStmtWeights {
    pub insert: u32,
    pub update: u32,
    pub delete: u32,
    pub select: u32,
}

impl Default for TriggerBodyStmtWeights {
    fn default() -> Self {
        Self {
            insert: 30,
            update: 25,
            delete: 20,
            select: 25,
        }
    }
}

// =============================================================================
// Identifier Configuration
// =============================================================================

/// Configuration for identifier generation.
#[derive(Debug, Clone)]
pub struct IdentifierConfig {
    /// Whether to generate table aliases.
    pub generate_table_aliases: bool,

    /// Whether to generate column aliases.
    pub generate_column_aliases: bool,

    /// Prefix for generated table aliases.
    pub table_alias_prefix: String,

    /// Prefix for generated column aliases.
    pub column_alias_prefix: String,

    /// Prefix for generated expression aliases.
    pub expr_alias_prefix: String,

    /// Maximum length for generated identifiers.
    pub max_identifier_length: usize,
}

impl Default for IdentifierConfig {
    fn default() -> Self {
        Self {
            generate_table_aliases: true,
            generate_column_aliases: true,
            table_alias_prefix: "t".to_string(),
            column_alias_prefix: "col".to_string(),
            expr_alias_prefix: "expr".to_string(),
            max_identifier_length: 64,
        }
    }
}

impl IdentifierConfig {
    /// Create config with no aliases.
    pub fn no_aliases() -> Self {
        Self {
            generate_table_aliases: false,
            generate_column_aliases: false,
            ..Default::default()
        }
    }
}

// =============================================================================
// Expression Configuration
// =============================================================================

/// Configuration for expression generation.
#[derive(Debug, Clone)]
pub struct ExprConfig {
    /// Probability of negating IS NULL expressions (IS NOT NULL).
    pub is_null_negation_probability: f64,

    /// Probability of negating BETWEEN expressions (NOT BETWEEN).
    pub between_negation_probability: f64,

    /// Probability of negating IN expressions (NOT IN).
    pub in_list_negation_probability: f64,

    /// Probability of generating ELSE clause in CASE expressions.
    pub case_else_probability: f64,

    /// Minimum number of WHEN clauses in CASE expression.
    pub case_min_branches: usize,

    /// Weights for binary operator categories (comparison, logical, arithmetic).
    pub binop_category_weights: BinOpCategoryWeights,

    /// Weights for compound condition operators (AND vs OR).
    pub compound_op_weights: CompoundOpWeights,
}

impl Default for ExprConfig {
    fn default() -> Self {
        Self {
            is_null_negation_probability: 0.5,
            between_negation_probability: 0.3,
            in_list_negation_probability: 0.3,
            case_else_probability: 0.5,
            case_min_branches: 1,
            binop_category_weights: BinOpCategoryWeights::default(),
            compound_op_weights: CompoundOpWeights::default(),
        }
    }
}

impl ExprConfig {
    /// Create config that never negates expressions.
    pub fn no_negation() -> Self {
        Self {
            is_null_negation_probability: 0.0,
            between_negation_probability: 0.0,
            in_list_negation_probability: 0.0,
            ..Default::default()
        }
    }

    /// Create config that always negates expressions.
    pub fn always_negate() -> Self {
        Self {
            is_null_negation_probability: 1.0,
            between_negation_probability: 1.0,
            in_list_negation_probability: 1.0,
            ..Default::default()
        }
    }
}

/// Weights for binary operator categories.
#[derive(Debug, Clone)]
pub struct BinOpCategoryWeights {
    pub comparison: u32,
    pub logical: u32,
    pub arithmetic: u32,
}

impl Default for BinOpCategoryWeights {
    fn default() -> Self {
        Self {
            comparison: 40,
            logical: 30,
            arithmetic: 30,
        }
    }
}

impl BinOpCategoryWeights {
    /// Create weights for comparison-only operations.
    pub fn comparison_only() -> Self {
        Self {
            comparison: 100,
            logical: 0,
            arithmetic: 0,
        }
    }
}

/// Weights for compound condition operators (AND/OR).
#[derive(Debug, Clone)]
pub struct CompoundOpWeights {
    pub and: u32,
    pub or: u32,
}

impl Default for CompoundOpWeights {
    fn default() -> Self {
        Self { and: 60, or: 40 }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_policy() {
        let policy = Policy::default();
        assert_eq!(policy.max_expr_depth, 4);
        assert_eq!(policy.max_subquery_depth, 2);
        assert!(policy.generate_aliases());
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
        assert!((policy.null_probability() - 0.1).abs() < f64::EPSILON);
        assert!(!policy.generate_aliases());
    }

    #[test]
    fn test_literal_config() {
        let config = LiteralConfig::small_integers();
        assert_eq!(config.int_min, -100);
        assert_eq!(config.int_max, 100);
    }

    #[test]
    fn test_select_config() {
        let config = SelectConfig::simple();
        assert_eq!(config.where_probability, 0.0);
        assert_eq!(config.order_by_probability, 0.0);

        let complex = SelectConfig::complex();
        assert!(complex.where_probability > 0.5);
    }

    #[test]
    fn test_insert_config() {
        let config = InsertConfig::single_row();
        assert_eq!(config.min_rows, 1);
        assert_eq!(config.max_rows, 1);

        let bulk = InsertConfig::bulk(100);
        assert_eq!(bulk.max_rows, 100);
    }

    #[test]
    fn test_delete_config() {
        let config = DeleteConfig::safe();
        assert_eq!(config.where_probability, 1.0);
    }

    #[test]
    fn test_identifier_config() {
        let config = IdentifierConfig::no_aliases();
        assert!(!config.generate_table_aliases);
        assert!(!config.generate_column_aliases);
    }

    #[test]
    fn test_granular_policy_construction() {
        let policy = Policy::default()
            .with_literal_config(LiteralConfig::small_integers())
            .with_select_config(SelectConfig::complex())
            .with_insert_config(InsertConfig::single_row())
            .with_delete_config(DeleteConfig::safe())
            .with_binop_weights(BinOpWeights::comparison_only());

        assert_eq!(policy.literal_config.int_min, -100);
        assert!(policy.select_config.where_probability > 0.5);
        assert_eq!(policy.insert_config.max_rows, 1);
        assert_eq!(policy.delete_config.where_probability, 1.0);
        assert_eq!(policy.binop_weights.add, 0);
    }

    #[test]
    fn test_expr_config() {
        let config = ExprConfig::default();
        assert!((config.is_null_negation_probability - 0.5).abs() < f64::EPSILON);
        assert!((config.case_else_probability - 0.5).abs() < f64::EPSILON);
        assert_eq!(config.case_min_branches, 1);

        let no_neg = ExprConfig::no_negation();
        assert_eq!(no_neg.is_null_negation_probability, 0.0);
        assert_eq!(no_neg.between_negation_probability, 0.0);
        assert_eq!(no_neg.in_list_negation_probability, 0.0);

        let always_neg = ExprConfig::always_negate();
        assert_eq!(always_neg.is_null_negation_probability, 1.0);
        assert_eq!(always_neg.between_negation_probability, 1.0);
        assert_eq!(always_neg.in_list_negation_probability, 1.0);
    }

    #[test]
    fn test_binop_category_weights() {
        let weights = BinOpCategoryWeights::default();
        assert!(weights.comparison > 0);
        assert!(weights.logical > 0);
        assert!(weights.arithmetic > 0);

        let comp_only = BinOpCategoryWeights::comparison_only();
        assert_eq!(comp_only.comparison, 100);
        assert_eq!(comp_only.logical, 0);
        assert_eq!(comp_only.arithmetic, 0);
    }
}
