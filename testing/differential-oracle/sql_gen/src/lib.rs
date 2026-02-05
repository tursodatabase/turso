//! SQL generator with type-state capabilities, runtime policy, and invisible tracing.
//!
//! This crate provides a schema-constrained SQL generator with:
//! - **Type-state + trait bounds** for compile-time hard restrictions
//! - **Runtime policy** with weights for soft restrictions
//! - **Invisible tracing** that automatically records what was generated
//! - **Hierarchical coverage** with origin tracking
//! - **proptest integration** via `Strategy` trait
//!
//! # Example
//!
//! ```
//! use sql_gen::{SqlGen, SqlGenBuilder, Policy, SelectOnly, SchemaBuilder, Table, ColumnDef, DataType};
//!
//! let schema = SchemaBuilder::new()
//!     .table(Table::new("users", vec![
//!         ColumnDef::new("id", DataType::Integer).primary_key(),
//!         ColumnDef::new("name", DataType::Text).not_null(),
//!         ColumnDef::new("email", DataType::Text),
//!     ]))
//!     .build();
//!
//! let generator: SqlGen<SelectOnly> = SqlGenBuilder::new()
//!     .schema(schema)
//!     .policy(Policy::default())
//!     .capabilities::<SelectOnly>()
//!     .build()
//!     .unwrap();
//!
//! // Use with proptest
//! // proptest!(|(sql in generator.strategy())| {
//! //     println!("Generated: {}", sql.sql);
//! // });
//! ```

pub mod ast;
pub mod builder;
pub mod capabilities;
pub mod context;
pub mod error;
pub mod functions;
pub mod generate;
pub mod policy;
pub mod schema;
pub mod strategy;
pub mod trace;

// Re-export main types
pub use ast::{
    BinOp, Expr, Literal, Stmt, TriggerBodyStmtKind, TriggerEvent, TriggerEventKind, TriggerStmt,
    TriggerTiming, UnaryOp,
};
pub use builder::SqlGenBuilder;
pub use capabilities::{
    CanAggregate, CanCte, CanDelete, CanInsert, CanSelect, CanSubquery, CanUpdate, CanWindowFn,
    Capabilities, DmlOnly, Full, NoSubquery, SelectOnly,
};
pub use context::Context;
pub use error::{GenError, GenErrorKind};
pub use functions::{FunctionCategory, FunctionDef, SCALAR_FUNCTIONS};
pub use policy::{
    BinOpCategoryWeights, BinOpWeights, CompoundOpWeights, DeleteConfig, ExprConfig, ExprWeights,
    FunctionConfig, IdentifierConfig, InsertConfig, LiteralConfig, LiteralTypeWeights,
    NullsOrderWeights, OrderDirectionWeights, Policy, SelectConfig, StmtWeights, StringCharset,
    TriggerBodyStmtWeights, TriggerConfig, TriggerEventWeights, TriggerTimingWeights,
    UnaryOpWeights, UpdateConfig,
};
pub use schema::{ColumnDef, DataType, Index, Schema, SchemaBuilder, Table};
pub use strategy::{GeneratedSql, SqlStrategy};
pub use trace::{
    Coverage, CoverageReport, ExprKind, Origin, OriginPath, StmtKind, Trace, TraceNode,
};

// Re-export procedural macros
pub use sql_gen_macros::trace_gen;

use std::marker::PhantomData;

/// The main SQL generator, parameterized by capability constraints.
///
/// The type parameter `C` determines which SQL constructs are allowed:
/// - `Full`: All statements allowed (default)
/// - `DmlOnly`: Only SELECT, INSERT, UPDATE, DELETE
/// - `SelectOnly`: Only SELECT statements
/// - `NoSubquery`: No subqueries in expressions
pub struct SqlGen<C: Capabilities = Full> {
    schema: Schema,
    policy: Policy,
    _cap: PhantomData<C>,
}

impl<C: Capabilities> SqlGen<C> {
    /// Create a new SqlGen with the given schema and policy.
    pub fn new(schema: Schema, policy: Policy) -> Self {
        Self {
            schema,
            policy,
            _cap: PhantomData,
        }
    }

    /// Create a builder for constructing an SqlGen.
    pub fn builder() -> SqlGenBuilder<Full> {
        SqlGenBuilder::new()
    }

    /// Get a reference to the schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Get a reference to the policy.
    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    /// Generate a statement respecting capability constraints.
    pub fn statement(&self, ctx: &mut Context) -> Result<Stmt, GenError> {
        generate::stmt::generate_statement::<C>(self, ctx)
    }

    /// Convert this generator into a proptest strategy.
    pub fn strategy(self) -> SqlStrategy<C> {
        SqlStrategy::new(self)
    }
}

// Methods only available with SELECT capability
impl<C: CanSelect> SqlGen<C> {
    /// Generate a SELECT statement.
    pub fn select_stmt(&self, ctx: &mut Context) -> Result<Stmt, GenError> {
        generate::select::generate_select(self, ctx)
    }
}

// Methods only available with INSERT capability
impl<C: CanInsert> SqlGen<C> {
    /// Generate an INSERT statement.
    pub fn insert_stmt(&self, ctx: &mut Context) -> Result<Stmt, GenError> {
        generate::stmt::generate_insert(self, ctx)
    }
}

// Methods only available with UPDATE capability
impl<C: CanUpdate> SqlGen<C> {
    /// Generate an UPDATE statement.
    pub fn update_stmt(&self, ctx: &mut Context) -> Result<Stmt, GenError> {
        generate::stmt::generate_update(self, ctx)
    }
}

// Methods only available with DELETE capability
impl<C: CanDelete> SqlGen<C> {
    /// Generate a DELETE statement.
    pub fn delete_stmt(&self, ctx: &mut Context) -> Result<Stmt, GenError> {
        generate::stmt::generate_delete(self, ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> Schema {
        SchemaBuilder::new()
            .table(Table::new(
                "users",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("name", DataType::Text).not_null(),
                    ColumnDef::new("email", DataType::Text),
                    ColumnDef::new("age", DataType::Integer),
                ],
            ))
            .table(Table::new(
                "posts",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("user_id", DataType::Integer).not_null(),
                    ColumnDef::new("title", DataType::Text).not_null(),
                    ColumnDef::new("content", DataType::Text),
                ],
            ))
            .build()
    }

    #[test]
    fn test_builder_creates_generator() {
        let schema = test_schema();
        let generator: SqlGen<Full> = SqlGenBuilder::new()
            .schema(schema)
            .policy(Policy::default())
            .build()
            .unwrap();

        assert_eq!(generator.schema().tables.len(), 2);
    }

    #[test]
    fn test_generate_statement() {
        let schema = test_schema();
        let generator: SqlGen<Full> = SqlGenBuilder::new()
            .schema(schema)
            .policy(Policy::default())
            .build()
            .unwrap();

        let mut ctx = Context::new_with_seed(42);
        let stmt = generator.statement(&mut ctx);
        assert!(stmt.is_ok());
    }

    #[test]
    fn test_select_only_capability() {
        let schema = test_schema();
        let generator: SqlGen<SelectOnly> = SqlGenBuilder::new()
            .schema(schema)
            .policy(Policy::default())
            .capabilities::<SelectOnly>()
            .build()
            .unwrap();

        let mut ctx = Context::new_with_seed(42);
        // Should only generate SELECT statements
        for _ in 0..10 {
            let stmt = generator.statement(&mut ctx).unwrap();
            assert!(matches!(stmt, Stmt::Select(_)));
        }
    }

    #[test]
    fn test_coverage_tracking() {
        let schema = test_schema();
        let generator: SqlGen<Full> = SqlGenBuilder::new()
            .schema(schema)
            .policy(Policy::default())
            .build()
            .unwrap();

        let mut ctx = Context::new_with_seed(42);
        for _ in 0..10 {
            let _ = generator.statement(&mut ctx);
        }

        let coverage = ctx.coverage();
        assert!(coverage.total_exprs() > 0);
    }
}
