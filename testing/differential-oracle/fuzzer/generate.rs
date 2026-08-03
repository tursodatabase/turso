//! SQL generation abstraction layer.
//!
//! Provides a trait-based interface to switch between different SQL generation
//! backends (sql_gen and sql_gen_prop) via a config flag.

use anyhow::Result;
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::TestRunner;
use sql_gen::{Full, Policy, SqlGen, StmtKind, WindowFramePolicy};

/// Output of SQL generation with metadata needed by the oracle.
#[derive(Debug, Clone)]
pub struct GeneratedStatement {
    pub sql: String,
    pub is_ddl: bool,
    pub mutates_data: bool,
    pub has_unordered_limit: bool,
    pub unordered_limit_reason: Option<String>,
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

/// A named mix of top-level statement weights. Each profile stresses a
/// different part of the engine so CI can cover several statement mixes
/// instead of the single default distribution. Profiles are static, so a
/// failing run reproduces from its seed once the same profile is selected.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum WeightProfile {
    /// The general-purpose mix: mostly reads and writes, a little DDL.
    #[default]
    Balanced,
    /// Heavy schema churn: create/drop/alter tables and indexes.
    Ddl,
    /// Heavy trigger creation plus writes, so triggers fire often.
    Triggers,
    /// Heavy insert/update/delete to stress constraint and conflict paths.
    Writes,
}

impl WeightProfile {
    /// The top-level statement weights for this profile. Transaction and
    /// not-yet-implemented statements stay at 0, matching the fuzzer's scope.
    fn stmt_weights(self) -> sql_gen::StmtWeights {
        let base = |select,
                    insert,
                    update,
                    delete,
                    create_table,
                    drop_table,
                    alter_table,
                    create_index,
                    drop_index,
                    pragma_foreign_key_list,
                    create_trigger,
                    drop_trigger| {
            sql_gen::StmtWeights {
                select,
                insert,
                update,
                delete,
                create_table,
                drop_table,
                alter_table,
                create_index,
                drop_index,
                pragma_foreign_key_list,
                create_trigger,
                drop_trigger,
                ..sql_gen::StmtWeights::default()
            }
        };
        match self {
            //                sel ins upd del  ct dt at  ci di pfk cg dg
            WeightProfile::Balanced => base(40, 20, 30, 10, 2, 1, 1, 2, 1, 1, 1, 1),
            WeightProfile::Ddl => base(15, 20, 10, 10, 20, 12, 20, 15, 10, 5, 5, 3),
            WeightProfile::Triggers => base(10, 25, 25, 20, 8, 3, 3, 5, 2, 2, 30, 10),
            WeightProfile::Writes => base(10, 35, 30, 20, 5, 2, 3, 5, 2, 1, 5, 3),
        }
    }
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

fn disable_alter_actions_that_revalidate_schema(policy: &mut Policy) {
    policy.alter_table_config.action_weights.rename_table = 0;
    policy.alter_table_config.action_weights.drop_column = 0;
    policy.alter_table_config.action_weights.rename_column = 0;
}

fn disable_prop_alter_actions_that_revalidate_schema(profile: &mut sql_gen_prop::StatementProfile) {
    profile.alter_table.extra.rename_to = 0;
    profile.alter_table.extra.drop_column = 0;
    profile.alter_table.extra.rename_column = 0;
}

/// True when two tables share a name in different database scopes, e.g. a TEMP
/// table shadowing a permanent table of the same name. SQLite resolves the
/// shared name to the temp table, so re-validating an index or trigger that
/// belongs to the permanent table can fail against the temp table's columns.
fn schema_has_a_shadowed_table_name(schema: &sql_gen::Schema) -> bool {
    schema.tables.iter().any(|table| {
        schema
            .tables
            .iter()
            .any(|other| other.name == table.name && other.database != table.database)
    })
}

impl SqlGenBackend {
    pub fn new(seed: u64) -> Self {
        Self::new_with_window_weight(seed, 0.0, WeightProfile::default())
    }

    /// Construct with a non-zero probability that each expression-list
    /// result column is a window function (used by the window-function-
    /// focused fuzzing path) and a chosen statement-weight profile.
    pub fn new_with_window_weight(
        seed: u64,
        window_function_probability: f64,
        profile: WeightProfile,
    ) -> Self {
        let ctx = sql_gen::Context::new_with_seed(seed);
        let stmt_weights = profile.stmt_weights();
        tracing::info!("Statement weight profile {profile:?}: {stmt_weights:?}");
        let mut policy = Policy::default()
            .with_stmt_weights(stmt_weights)
            .with_function_config(
                sql_gen::FunctionConfig::deterministic().disable(&["LIKELY", "UNLIKELY"]),
            );
        policy.select_config.require_order_by_with_limit = true;
        policy.select_config.window_function_probability = window_function_probability;
        if window_function_probability > 0.0 {
            policy.select_config.window_frame_policy = WindowFramePolicy::Exclude;
        }
        // Disable expression values for inserts, enable conflict clauses for updates
        policy.insert_config.expression_value_probability = 0.0;
        policy.insert_config.or_replace_probability = 0.0;
        policy.insert_config.or_ignore_probability = 0.0;
        policy.update_config.expression_value_probability = 0.0;
        policy.update_config.or_replace_probability = 0.1;
        // If several rows try to set the same UNIQUE value, OR IGNORE keeps
        // whichever row is visited first. SQLite and Turso may visit the rows
        // in a different order. Both results are allowed, but the final tables
        // do not match.
        policy.update_config.or_ignore_probability = 0.0;
        // Boost UPDATE FROM coverage
        policy.update_config.from_probability = 0.4;
        policy.update_config.returning_probability = 0.2;
        // An UPDATE ... FROM whose source matches a target row several times
        // uses one of them, chosen by scan order. For a single source table the
        // generator forces NOT INDEXED so both engines do a rowid-order table
        // scan and agree. That does not extend to joins: Turso builds an
        // ephemeral index to evaluate a JOIN while SQLite scans, so the match
        // order still differs and NOT INDEXED (which only pins base-table
        // access) cannot align them. Keep UPDATE FROM to a single real table:
        // no joins, no self-joins, and no subquery sources.
        policy.update_config.self_join_probability = 0.0;
        policy.update_config.join_in_from_probability = 0.0;
        policy.update_config.subquery_from_probability = 0.0;
        policy.update_config.target_alias_probability = 0.2;
        policy.update_config.from_set_reference_probability = 0.5;
        Self { ctx, policy }
    }
}

impl SqlGenerator for SqlGenBackend {
    fn generate(&mut self, schema: &sql_gen::Schema) -> Result<GeneratedStatement> {
        let mut policy = self.policy.clone();
        if !schema.triggers.is_empty() || schema_has_a_shadowed_table_name(schema) {
            // SQLite re-resolves every stored index and trigger during a table
            // rename, column rename, or column drop. Turso does not, so it may
            // accept an ALTER that SQLite rejects. Two situations hit this:
            //   - A trigger body refers to a table that was dropped earlier.
            //     The fuzzer records the table a trigger belongs to, but not
            //     every table and column its body uses, so it cannot tell
            //     whether a DROP left a trigger broken.
            //   - A TEMP table shadows a permanent table of the same name.
            //     SQLite re-resolves an index or trigger on the permanent table
            //     against the temp table, which lacks the column.
            // Do not generate these ALTER actions in either case. Separate
            // tests still cover them with schemas that are known to be valid.
            disable_alter_actions_that_revalidate_schema(&mut policy);
        }
        let generator: SqlGen<Full> = SqlGen::new(schema.clone(), policy);
        let stmt = generator
            .statement(&mut self.ctx)
            .map_err(|e| anyhow::anyhow!("Failed to generate statement: {e}"))?;
        let sql = stmt.to_string();
        let stmt_kind = StmtKind::from(&stmt);
        let is_ddl = stmt_kind.is_ddl();
        let mutates_data = matches!(
            stmt_kind,
            StmtKind::Insert | StmtKind::Update | StmtKind::Delete
        );
        let has_unordered_limit =
            stmt.has_unordered_limit() || stmt.non_unique_order_by_reason(schema).is_some();
        let unordered_limit_reason = stmt
            .unordered_limit_reason()
            .or_else(|| stmt.non_unique_order_by_reason(schema))
            .map(str::to_string);
        Ok(GeneratedStatement {
            sql,
            is_ddl,
            mutates_data,
            has_unordered_limit,
            unordered_limit_reason,
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
    recursive_cte_focus: bool,
}

impl PropTestBackend {
    pub fn new(seed_bytes: [u8; 32], recursive_cte_focus: bool) -> Self {
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
        if recursive_cte_focus {
            profile = profile.read_only();
            profile.generation.expression = profile.generation.expression.clone().simple();
            profile.select.extra.allow_aggregates = false;
            let cte = &mut profile.select.extra.cte_profile;
            cte.cte_weight = 100;
            cte.no_cte_weight = 0;
            cte.cte_count_range = 1..=3;
            cte.recursive_weight = 100;
            cte.non_recursive_weight = 0;
        }
        Self {
            test_runner,
            profile,
            recursive_cte_focus,
        }
    }
}

impl SqlGenerator for PropTestBackend {
    fn generate(&mut self, schema: &sql_gen::Schema) -> Result<GeneratedStatement> {
        let prop_schema = to_prop_schema(schema);
        let mut profile = if self.recursive_cte_focus && prop_schema.tables.is_empty() {
            sql_gen_prop::StatementProfile::default()
        } else {
            self.profile.clone()
        };
        if !schema.triggers.is_empty() || schema_has_a_shadowed_table_name(schema) {
            disable_prop_alter_actions_that_revalidate_schema(&mut profile);
        }
        let strategy = sql_gen_prop::strategies::statement_for_schema(&prop_schema, &profile);
        let value_tree = strategy
            .new_tree(&mut self.test_runner)
            .map_err(|e| anyhow::anyhow!("Failed to generate statement: {e}"))?;
        let mut stmt = value_tree.current();
        // SQLite 3.50.2, currently bundled by rusqlite in this workspace,
        // has an ORDER BY elision regression for recursive CTEs that was
        // fixed in later SQLite versions. Avoid an outer LIMIT/OFFSET on any
        // statement with a recursive CTE - the default profile generates them
        // too - so that this oracle bug cannot change the compared row set.
        // Recursive LIMIT/OFFSET and priority ordering remain fully generated
        // inside the CTE.
        if let sql_gen_prop::SqlStatement::Select(select) = &mut stmt {
            if select.has_recursive_cte() {
                select.limit = None;
                select.offset = None;
            }
        }
        let sql = stmt.to_string();
        let stmt_kind = sql_gen_prop::StatementKind::from(&stmt);
        let is_ddl = stmt_kind.is_ddl();
        let mutates_data = matches!(
            stmt_kind,
            sql_gen_prop::StatementKind::Insert
                | sql_gen_prop::StatementKind::Update
                | sql_gen_prop::StatementKind::Delete
        );
        let has_unordered_limit = stmt.has_unordered_limit();
        Ok(GeneratedStatement {
            sql,
            is_ddl,
            mutates_data,
            has_unordered_limit,
            unordered_limit_reason: None,
        })
    }
}

/// Convert a `sql_gen::Schema` to a `sql_gen_prop::Schema`.
fn to_prop_schema(schema: &sql_gen::Schema) -> sql_gen_prop::Schema {
    let mut builder = sql_gen_prop::SchemaBuilder::new();
    for db in &schema.attached_databases {
        builder = builder.add_database(db.clone());
    }
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
                    // Array types have no prop equivalent — map to Blob
                    sql_gen::DataType::IntegerArray
                    | sql_gen::DataType::RealArray
                    | sql_gen::DataType::TextArray => sql_gen_prop::DataType::Blob,
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
        let prop_table = if table.strict {
            sql_gen_prop::Table::new_strict(table.name.clone(), columns)
        } else {
            sql_gen_prop::Table::new(table.name.clone(), columns)
        };
        let prop_table = match &table.database {
            Some(db) => prop_table.in_database(db.clone()),
            None => prop_table,
        };
        builder = builder.add_table(prop_table);
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
        if let Some(db) = &index.database {
            idx = idx.in_database(db.clone());
        }
        builder = builder.add_index(idx);
    }
    for trigger in &schema.triggers {
        let mut prop_trigger =
            sql_gen_prop::Trigger::new(trigger.name.clone(), trigger.table_name.clone());
        if let Some(db) = &trigger.database {
            prop_trigger = prop_trigger.in_database(db.clone());
        }
        builder = builder.add_trigger(prop_trigger);
    }
    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn updates_that_can_choose_different_rows_are_disabled() {
        let sql_gen = SqlGenBackend::new(1);
        assert_eq!(sql_gen.policy.update_config.or_ignore_probability, 0.0);
        assert_eq!(sql_gen.policy.update_config.self_join_probability, 0.0);
    }

    #[test]
    fn disabling_alter_actions_leaves_add_column_enabled() {
        let mut policy = Policy::default();
        disable_alter_actions_that_revalidate_schema(&mut policy);
        assert_eq!(policy.alter_table_config.action_weights.rename_table, 0);
        assert_eq!(policy.alter_table_config.action_weights.drop_column, 0);
        assert_eq!(policy.alter_table_config.action_weights.rename_column, 0);
        assert_ne!(policy.alter_table_config.action_weights.add_column, 0);

        let mut profile = sql_gen_prop::StatementProfile::default();
        disable_prop_alter_actions_that_revalidate_schema(&mut profile);
        assert_eq!(profile.alter_table.extra.rename_to, 0);
        assert_eq!(profile.alter_table.extra.drop_column, 0);
        assert_eq!(profile.alter_table.extra.rename_column, 0);
        assert_ne!(profile.alter_table.extra.add_column, 0);
    }

    #[test]
    fn a_temp_table_shadowing_a_permanent_one_counts_as_shadowed() {
        use sql_gen::{ColumnDef, DataType, Table};
        let make = |name: &str, database: Option<&str>| Table {
            name: name.to_string(),
            columns: vec![ColumnDef::new("x", DataType::Integer)],
            database: database.map(str::to_string),
            strict: false,
        };

        // Same name in main and temp scopes: shadowed.
        let schema = sql_gen::Schema {
            tables: vec![make("t", None), make("t", Some("temp"))],
            ..Default::default()
        };
        assert!(schema_has_a_shadowed_table_name(&schema));

        // Distinct names, and the same name in one scope only: not shadowed.
        let schema = sql_gen::Schema {
            tables: vec![make("t", None), make("u", Some("temp"))],
            ..Default::default()
        };
        assert!(!schema_has_a_shadowed_table_name(&schema));
    }

    #[test]
    fn every_profile_can_read_and_write() {
        // A profile that never selects, inserts, updates, or deletes would
        // generate an empty or read-only workload and quietly cover nothing.
        for profile in [
            WeightProfile::Balanced,
            WeightProfile::Ddl,
            WeightProfile::Triggers,
            WeightProfile::Writes,
        ] {
            let w = profile.stmt_weights();
            assert!(w.select > 0, "{profile:?} never selects");
            assert!(w.insert > 0, "{profile:?} never inserts");
            assert!(w.update > 0, "{profile:?} never updates");
            assert!(w.delete > 0, "{profile:?} never deletes");
        }
    }

    #[test]
    fn profiles_emphasize_their_theme() {
        let ddl = WeightProfile::Ddl.stmt_weights();
        assert!(
            ddl.create_table > WeightProfile::Balanced.stmt_weights().create_table,
            "ddl profile should create tables more often than balanced"
        );
        let triggers = WeightProfile::Triggers.stmt_weights();
        assert!(
            triggers.create_trigger > WeightProfile::Balanced.stmt_weights().create_trigger,
            "triggers profile should create triggers more often than balanced"
        );
    }
}
