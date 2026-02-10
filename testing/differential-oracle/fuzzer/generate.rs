//! SQL generation abstraction layer.
//!
//! Provides a trait-based interface to switch between different SQL generation
//! backends (sql_gen and sql_gen_prop) via a config flag.

use anyhow::Result;
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::TestRunner;
use sql_gen::{Full, Policy, SqlGen, StmtKind};

/// Output of SQL generation with metadata needed by the oracle.
#[derive(Debug, Clone)]
pub struct GeneratedStatement {
    pub sql: String,
    pub is_ddl: bool,
    pub has_unordered_limit: bool,
}

impl std::fmt::Display for GeneratedStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.sql)
    }
}

/// Which generation backend to use.
#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub enum GeneratorKind {
    /// Type-state SQL generator (sql_gen crate)
    #[default]
    SqlGen,
    /// Proptest-based SQL generator (sql_gen_prop crate)
    SqlGenProp,
}

/// Trait abstracting SQL generation backends.
pub trait SqlGenerator {
    /// Generate the next SQL statement given the current schema.
    fn generate(&mut self, schema: &sql_gen::Schema) -> Result<GeneratedStatement>;

    /// Take accumulated coverage data, if the backend supports it.
    fn take_coverage(&mut self) -> Option<sql_gen::Coverage> {
        None
    }
}

/// sql_gen (type-state) backend.
pub struct SqlGenBackend {
    ctx: sql_gen::Context,
    policy: Policy,
}

impl SqlGenBackend {
    pub fn new(seed: u64) -> Self {
        let ctx = sql_gen::Context::new_with_seed(seed);
        let mut policy = Policy::default()
            .with_stmt_weights(sql_gen::StmtWeights {
                create_trigger: 0,
                drop_trigger: 0,
                ..sql_gen::StmtWeights::default()
            })
            .with_function_config(
                sql_gen::FunctionConfig::deterministic().disable(&["LIKELY", "UNLIKELY"]),
            );
        policy.select_config.nulls_order_weights.first = 0;
        policy.select_config.nulls_order_weights.last = 0;
        policy.select_config.require_order_by_with_limit = true;
        // Disable expression values and conflict clauses for now
        policy.insert_config.expression_value_probability = 0.0;
        policy.insert_config.or_replace_probability = 0.0;
        policy.insert_config.or_ignore_probability = 0.0;
        policy.update_config.expression_value_probability = 0.0;
        policy.update_config.or_replace_probability = 0.0;
        policy.update_config.or_ignore_probability = 0.0;
        Self { ctx, policy }
    }
}

impl SqlGenerator for SqlGenBackend {
    fn generate(&mut self, schema: &sql_gen::Schema) -> Result<GeneratedStatement> {
        let generator: SqlGen<Full> = SqlGen::new(schema.clone(), self.policy.clone());
        let stmt = generator
            .statement(&mut self.ctx)
            .map_err(|e| anyhow::anyhow!("Failed to generate statement: {e}"))?;
        let sql = stmt.to_string();
        let is_ddl = StmtKind::from(&stmt).is_ddl();
        let has_unordered_limit = stmt.has_unordered_limit();
        Ok(GeneratedStatement {
            sql,
            is_ddl,
            has_unordered_limit,
        })
    }

    fn take_coverage(&mut self) -> Option<sql_gen::Coverage> {
        Some(self.ctx.take_coverage())
    }
}

/// sql_gen_prop (proptest) backend.
pub struct PropTestBackend {
    test_runner: TestRunner,
    profile: sql_gen_prop::StatementProfile,
}

impl PropTestBackend {
    pub fn new(seed_bytes: [u8; 32]) -> Self {
        let test_runner = TestRunner::new_with_rng(
            proptest::test_runner::Config::default(),
            proptest::test_runner::TestRng::from_seed(
                proptest::test_runner::RngAlgorithm::ChaCha,
                &seed_bytes,
            ),
        );
        let mut profile = sql_gen_prop::StatementProfile::default();
        profile
            .generation
            .expression
            .base
            .order_by_allow_integer_positions = false;
        Self {
            test_runner,
            profile,
        }
    }
}

impl SqlGenerator for PropTestBackend {
    fn generate(&mut self, schema: &sql_gen::Schema) -> Result<GeneratedStatement> {
        let prop_schema = to_prop_schema(schema);
        let strategy = sql_gen_prop::strategies::statement_for_schema(&prop_schema, &self.profile);
        let value_tree = strategy
            .new_tree(&mut self.test_runner)
            .map_err(|e| anyhow::anyhow!("Failed to generate statement: {e}"))?;
        let stmt = value_tree.current();
        let sql = stmt.to_string();
        let is_ddl = sql_gen_prop::StatementKind::from(&stmt).is_ddl();
        let has_unordered_limit = stmt.has_unordered_limit();
        Ok(GeneratedStatement {
            sql,
            is_ddl,
            has_unordered_limit,
        })
    }
}

/// Convert a `sql_gen::Schema` to a `sql_gen_prop::Schema`.
fn to_prop_schema(schema: &sql_gen::Schema) -> sql_gen_prop::Schema {
    let mut builder = sql_gen_prop::SchemaBuilder::new();
    for table in &schema.tables {
        let columns: Vec<sql_gen_prop::ColumnDef> = table
            .columns
            .iter()
            .map(|c| {
                let dt = match c.data_type {
                    sql_gen::DataType::Integer => sql_gen_prop::DataType::Integer,
                    sql_gen::DataType::Real => sql_gen_prop::DataType::Real,
                    sql_gen::DataType::Text => sql_gen_prop::DataType::Text,
                    sql_gen::DataType::Blob => sql_gen_prop::DataType::Blob,
                    sql_gen::DataType::Null => sql_gen_prop::DataType::Null,
                };
                let mut col = sql_gen_prop::ColumnDef::new(c.name.clone(), dt);
                if !c.nullable {
                    col = col.not_null();
                }
                if c.primary_key {
                    col = col.primary_key();
                }
                if c.unique {
                    col = col.unique();
                }
                if let Some(ref default) = c.default {
                    col = col.default_value(default.clone());
                }
                col
            })
            .collect();
        builder = builder.add_table(sql_gen_prop::Table::new(table.name.clone(), columns));
    }
    for index in &schema.indexes {
        let mut idx = sql_gen_prop::Index::new(
            index.name.clone(),
            index.table_name.clone(),
            index.columns.clone(),
        );
        if index.unique {
            idx = idx.unique();
        }
        builder = builder.add_index(idx);
    }
    builder.build()
}
