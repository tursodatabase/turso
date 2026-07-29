//! proptest Strategy implementation.

use std::fmt;

use proptest::prelude::*;
use proptest::strategy::{NewTree, ValueTree};
use proptest::test_runner::TestRunner;

use crate::SqlGen;
use crate::ast::Stmt;
use crate::capabilities::Capabilities;
use crate::context::Context;
use crate::trace::Coverage;

/// The generated output containing SQL, AST, and coverage.
#[derive(Clone, Debug)]
pub struct GeneratedSql {
    /// The generated SQL string.
    pub sql: String,
    /// The AST of the generated statement.
    pub ast: Stmt,
    /// Coverage statistics.
    pub coverage: Coverage,
}

/// proptest Strategy for generating SQL.
pub struct SqlStrategy<C: Capabilities> {
    generator: SqlGen<C>,
}

impl<C: Capabilities> fmt::Debug for SqlStrategy<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlStrategy")
            .field("schema_tables", &self.generator.schema().tables.len())
            .finish()
    }
}

impl<C: Capabilities> SqlStrategy<C> {
    /// Create a new SQL strategy from a generator.
    pub fn new(generator: SqlGen<C>) -> Self {
        Self { generator }
    }
}

impl<C: Capabilities + Clone> Strategy for SqlStrategy<C> {
    type Tree = SqlValueTree;
    type Value = GeneratedSql;

    fn new_tree(&self, runner: &mut TestRunner) -> NewTree<Self> {
        // Use the test runner's RNG to seed our context
        let seed: u64 = runner.rng().random();
        let mut ctx = Context::new_with_seed(seed);

        match self.generator.statement(&mut ctx) {
            Ok(stmt) => {
                let sql = stmt.to_string();
                let coverage = ctx.take_coverage();

                Ok(SqlValueTree {
                    current: GeneratedSql {
                        sql,
                        ast: stmt,
                        coverage,
                    },
                    shrink_state: ShrinkState::default(),
                })
            }
            Err(e) => Err(e.to_string().into()),
        }
    }
}

/// Value tree for SQL generation (supports shrinking).
pub struct SqlValueTree {
    current: GeneratedSql,
    shrink_state: ShrinkState,
}

struct ShrinkState {
    attempts: usize,
    max_attempts: usize,
}

impl Default for ShrinkState {
    fn default() -> Self {
        Self {
            attempts: 0,
            max_attempts: 10,
        }
    }
}

impl ValueTree for SqlValueTree {
    type Value = GeneratedSql;

    fn current(&self) -> Self::Value {
        self.current.clone()
    }

    fn simplify(&mut self) -> bool {
        // Basic shrinking: try to simplify the SQL
        // For now, we don't implement shrinking - return false
        // Future: could remove optional clauses, simplify expressions, etc.
        if self.shrink_state.attempts >= self.shrink_state.max_attempts {
            return false;
        }

        self.shrink_state.attempts += 1;

        // No actual shrinking implemented yet
        false
    }

    fn complicate(&mut self) -> bool {
        // No complication supported
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Full;
    use crate::policy::Policy;
    use crate::schema::{ColumnDef, DataType, SchemaBuilder, Table};

    fn test_generator() -> SqlGen<Full> {
        let schema = SchemaBuilder::new()
            .table(Table::new(
                "users",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("name", DataType::Text),
                    ColumnDef::new("age", DataType::Integer),
                ],
            ))
            .build();

        SqlGen::new(schema, Policy::default())
    }

    #[test]
    fn test_strategy_generates_sql() {
        let generator = test_generator();
        let strategy = generator.strategy();

        let mut runner = TestRunner::default();
        let tree = strategy.new_tree(&mut runner);

        assert!(tree.is_ok());
        let tree = tree.unwrap();
        let generated = tree.current();

        assert!(!generated.sql.is_empty());
    }

    #[test]
    fn test_strategy_with_proptest() {
        let generator = test_generator();
        let strategy = generator.strategy();

        proptest!(|(sql in strategy)| {
            // Verify we got valid SQL
            prop_assert!(!sql.sql.is_empty());
            // Verify coverage is recorded
            prop_assert!(sql.coverage.total_stmts() > 0 || sql.coverage.total_exprs() > 0);
        });
    }

    #[test]
    fn test_strategy_deterministic_with_seed() {
        let schema = SchemaBuilder::new()
            .table(Table::new(
                "users",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("name", DataType::Text),
                ],
            ))
            .build();

        let gen1: SqlGen<Full> = SqlGen::new(schema.clone(), Policy::default());
        let gen2: SqlGen<Full> = SqlGen::new(schema, Policy::default());

        // Using the same seed should produce the same SQL
        let mut ctx1 = Context::new_with_seed(42);
        let mut ctx2 = Context::new_with_seed(42);

        let sql1 = gen1.statement(&mut ctx1).unwrap().to_string();
        let sql2 = gen2.statement(&mut ctx2).unwrap().to_string();

        assert_eq!(sql1, sql2);
    }
}
