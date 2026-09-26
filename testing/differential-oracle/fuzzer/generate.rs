//! SQL generation abstraction layer.
//!
//! Provides a trait-based interface to switch between different SQL generation
//! backends (sql_gen and sql_gen_prop) via a config flag.

use std::collections::BTreeMap;

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
    /// The generator added an outer-column dependency to a subquery, so the
    /// forced-rewrite and disabled-rewrite plans should return the same result.
    pub check_unnesting_invariant: bool,
}

impl std::fmt::Display for GeneratedStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.sql)
    }
}

/// Live materialized views by name, with their result columns. Ordered, so
/// that a seed generates the same statements on every run.
pub type Matviews = BTreeMap<String, Vec<sql_gen_prop::ColumnDef>>;

/// One generated step for the runner.
#[derive(Debug, Clone)]
pub enum Generated {
    /// A statement that runs unchanged on both engines.
    Statement(GeneratedStatement),
    /// A materialized view on Turso, and the same SELECT as a plain view on SQLite.
    CreateMatview {
        turso_sql: String,
        sqlite_sql: String,
        name: String,
        columns: Vec<sql_gen_prop::ColumnDef>,
    },
    DropMatview {
        sql: String,
        name: String,
    },
}

impl Generated {
    /// The SQL that runs on Turso.
    pub fn sql(&self) -> &str {
        match self {
            Generated::Statement(stmt) => &stmt.sql,
            Generated::CreateMatview { turso_sql, .. } => turso_sql,
            Generated::DropMatview { sql, .. } => sql,
        }
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

/// A named workload mix. Each profile stresses a different part of the engine
/// by changing statement weights and, when needed, SELECT generation. Profiles
/// are static, so a failing run reproduces from its seed and profile.
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
    /// SELECT-heavy workload with every supported correlated subquery rewrite.
    CorrelatedSubqueries,
    /// SELECT-heavy workload with one to three inner or left equality joins.
    Joins,
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
            WeightProfile::CorrelatedSubqueries => base(80, 8, 8, 4, 2, 1, 1, 1, 1, 1, 1, 1),
            WeightProfile::Joins => base(80, 8, 8, 4, 2, 1, 1, 2, 1, 1, 1, 1),
        }
    }

    fn configure_policy(self, policy: &mut Policy) {
        if self == WeightProfile::CorrelatedSubqueries {
            let config = &mut policy.select_config;
            config.subquery_correlation_probability = 1.0;
            config.subquery_aggregate_probability = 1.0;
            config.subquery_group_by_probability = 0.0;
            config.subquery_order_by_probability = 0.0;
            config.subquery_distinct_probability = 0.0;
            config.cte_probability = 0.0;
            config.compound_probability = 0.0;
            config.expression_count_range = 1..=2;

            policy.max_expr_depth = 2;
            policy.max_subquery_depth = 1;
            policy.max_case_branches = 1;
            policy.max_in_list_size = 3;
            policy.max_order_by_items = 2;
            policy.max_group_by_items = 2;
            policy.expr_weights.case_expr = 0;
            policy.expr_weights.subquery = 20;
            policy.expr_weights.in_subquery = 20;
            policy.expr_weights.exists = 20;
            policy.expr_config.in_subquery_negation_probability = 0.0;
            policy.expr_config.exists_negation_probability = 0.5;
            policy.literal_config.string_max_len = 20;
            policy.literal_config.blob_max_size = 16;
        }

        if self == WeightProfile::Joins {
            let config = &mut policy.select_config;
            config.join_config.join_probability = 1.0;
            config.join_config.max_joins = 3;
            config.join_config.join_type_weights.inner = 60;
            config.join_config.join_type_weights.left = 40;
            config.join_config.join_type_weights.cross = 0;
            config.join_config.join_type_weights.natural = 0;
            config.join_config.equi_join_probability = 1.0;
            config.join_config.self_join_probability = 0.0;
            config.cte_probability = 0.0;
            config.compound_probability = 0.0;
        }
    }
}

/// Trait abstracting SQL generation backends.
pub trait SqlGenerator {
    /// Generate the next step given the current schema and materialized views.
    fn generate(&mut self, schema: &sql_gen::Schema, matviews: &Matviews) -> Result<Generated>;

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
                sql_gen::FunctionConfig::deterministic()
                    .disable(&["LIKELY", "UNLIKELY"])
                    .without_order_dependent_aggregates(),
            );
        policy.select_config.require_order_by_with_limit = true;
        profile.configure_policy(&mut policy);
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
    fn generate(&mut self, schema: &sql_gen::Schema, matviews: &Matviews) -> Result<Generated> {
        assert!(
            matviews.is_empty(),
            "sql-gen does not generate materialized views"
        );
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
        let check_unnesting_invariant = self.ctx.take_generated_correlated_subquery();
        Ok(Generated::Statement(GeneratedStatement {
            sql,
            is_ddl,
            mutates_data,
            has_unordered_limit,
            unordered_limit_reason,
            check_unnesting_invariant,
        }))
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
    pub fn new(
        seed_bytes: [u8; 32],
        recursive_cte_focus: bool,
        weight_profile: WeightProfile,
        matview: bool,
    ) -> Self {
        let test_runner = TestRunner::new_with_rng(
            proptest::test_runner::Config::default(),
            proptest::test_runner::TestRng::from_seed(
                proptest::test_runner::RngAlgorithm::ChaCha,
                &seed_bytes,
            ),
        );
        let w = weight_profile.stmt_weights();
        tracing::info!("Statement weight profile {weight_profile:?}: {w:?}");
        let mut profile = prop_statement_profile(&w);
        profile
            .generation
            .expression
            .base
            .order_by_allow_integer_positions = false;
        profile
            .generation
            .expression
            .base
            .function_profile
            .allow_order_dependent_aggregates = false;
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
        if matview {
            profile.create_materialized_view_weight = 8;
            profile.drop_materialized_view_weight = 2;
            profile.create_table.extra.main_schema_only = true;
            // Turso refuses ALTER TABLE on a table that a materialized view reads.
            profile.alter_table.weight = 0;
            profile.insert_or_replace_weight = profile.insert.weight / 5;
            profile.upsert_weight = profile.insert.weight / 5;
            profile.create_table.extra.shared_column_names = true;
            // Repeated keys let a DELETE or UPDATE empty a group and let a
            // replace hit an existing row.
            profile.generation.value = profile.generation.value.narrow();
            profile.generation.table_spelling.other_case = true;
            profile.generation.table_spelling.target_alias = true;
        }
        Self {
            test_runner,
            profile,
            recursive_cte_focus,
        }
    }
}

/// sql_gen_prop does not generate triggers, so the trigger weights are not mapped.
fn prop_statement_profile(w: &sql_gen::StmtWeights) -> sql_gen_prop::StatementProfile {
    sql_gen_prop::StatementProfile::default()
        .with_select(w.select)
        .with_insert(w.insert)
        .with_update(w.update)
        .with_delete(w.delete)
        .with_create_table(w.create_table)
        .with_drop_table(w.drop_table)
        .with_alter_table(w.alter_table)
        .with_create_index(w.create_index)
        .with_drop_index(w.drop_index)
}

impl SqlGenerator for PropTestBackend {
    fn generate(&mut self, schema: &sql_gen::Schema, matviews: &Matviews) -> Result<Generated> {
        let prop_schema = to_prop_schema(schema, matviews);
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
        match stmt {
            sql_gen_prop::SqlStatement::CreateMaterializedView(create) => {
                return Ok(Generated::CreateMatview {
                    turso_sql: create.to_string(),
                    sqlite_sql: create.plain_view_sql(),
                    name: create.view_name,
                    columns: create.output_columns,
                });
            }
            sql_gen_prop::SqlStatement::DropMaterializedView(drop) => {
                return Ok(Generated::DropMatview {
                    sql: drop.to_string(),
                    name: drop.view_name,
                });
            }
            _ => {}
        }
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
                | sql_gen_prop::StatementKind::InsertOrReplace
                | sql_gen_prop::StatementKind::Upsert
                | sql_gen_prop::StatementKind::Update
                | sql_gen_prop::StatementKind::Delete
        );
        let has_unordered_limit = stmt.has_unordered_limit();
        Ok(Generated::Statement(GeneratedStatement {
            sql,
            is_ddl,
            mutates_data,
            has_unordered_limit,
            unordered_limit_reason: None,
            check_unnesting_invariant: false,
        }))
    }
}

/// Convert a `sql_gen::Schema` and the live materialized views to a `sql_gen_prop::Schema`.
fn to_prop_schema(schema: &sql_gen::Schema, matviews: &Matviews) -> sql_gen_prop::Schema {
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
    for (name, columns) in matviews {
        builder =
            builder.add_materialized_view(sql_gen_prop::Table::new(name.clone(), columns.clone()));
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
    fn aggregates_that_depend_on_input_order_are_disabled() {
        let sql_gen = SqlGenBackend::new(1);
        assert!(
            !sql_gen
                .policy
                .function_config
                .allow_order_dependent_aggregates
        );

        let prop = PropTestBackend::new([1; 32], false, WeightProfile::default(), false);
        assert!(
            !prop
                .profile
                .generation
                .expression
                .base
                .function_profile
                .allow_order_dependent_aggregates
        );
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
            WeightProfile::CorrelatedSubqueries,
            WeightProfile::Joins,
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

        let joins = SqlGenBackend::new_with_window_weight(1, 0.0, WeightProfile::Joins);
        assert_eq!(joins.policy.select_config.join_config.join_probability, 1.0);
        assert_eq!(
            joins.policy.select_config.join_config.equi_join_probability,
            1.0
        );
        assert_eq!(joins.policy.select_config.join_config.max_joins, 3);
    }

    #[test]
    fn prop_profile_takes_each_statement_weight_from_the_same_statement_kind() {
        let w = sql_gen::StmtWeights {
            select: 101,
            insert: 102,
            update: 103,
            delete: 104,
            create_table: 105,
            drop_table: 106,
            alter_table: 107,
            create_index: 108,
            drop_index: 109,
            ..sql_gen::StmtWeights::default()
        };
        let p = prop_statement_profile(&w);
        assert_eq!(
            [
                p.select.weight,
                p.insert.weight,
                p.update.weight,
                p.delete.weight,
                p.create_table.weight,
                p.drop_table_weight,
                p.alter_table.weight,
                p.create_index.weight,
                p.drop_index_weight,
            ],
            [101, 102, 103, 104, 105, 106, 107, 108, 109]
        );
    }

    #[test]
    fn prop_backend_takes_its_statement_weights_from_the_weight_profile() {
        fn weights(p: &sql_gen_prop::StatementProfile) -> [u32; 9] {
            [
                p.select.weight,
                p.insert.weight,
                p.update.weight,
                p.delete.weight,
                p.create_table.weight,
                p.drop_table_weight,
                p.alter_table.weight,
                p.create_index.weight,
                p.drop_index_weight,
            ]
        }
        let backend = PropTestBackend::new([1; 32], false, WeightProfile::Ddl, false);
        let expected = weights(&prop_statement_profile(&WeightProfile::Ddl.stmt_weights()));
        assert_ne!(
            expected,
            weights(&sql_gen_prop::StatementProfile::default())
        );
        assert_eq!(weights(&backend.profile), expected);
    }

    fn generated_dml(matview: bool) -> Vec<String> {
        use sql_gen::{ColumnDef, DataType, Table};
        let schema = sql_gen::Schema {
            tables: vec![Table::new(
                "msg",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("v", DataType::Text),
                ],
            )],
            ..Default::default()
        };
        let mut backend = PropTestBackend::new([3; 32], false, WeightProfile::Writes, matview);
        (0..300)
            .filter_map(
                |_| match backend.generate(&schema, &Matviews::new()).unwrap() {
                    Generated::Statement(stmt) if stmt.mutates_data => Some(stmt.sql),
                    _ => None,
                },
            )
            .collect()
    }

    const OTHER_SPELLINGS: [&str; 5] = [
        "INTO MSG ",
        "INTO Msg ",
        "UPDATE MSG ",
        "FROM Msg ",
        "msg AS tgt ",
    ];

    #[test]
    fn matview_mode_writes_table_names_in_other_cases_and_with_aliases() {
        let dml = generated_dml(true);
        for spelling in OTHER_SPELLINGS {
            assert!(
                dml.iter().any(|sql| sql.contains(spelling)),
                "no DML contains {spelling:?}"
            );
        }
        assert!(dml.iter().any(|sql| sql.contains(" msg ")));
    }

    #[test]
    fn default_mode_writes_table_names_as_created() {
        let dml = generated_dml(false);
        assert!(!dml.is_empty());
        assert!(dml.iter().all(|sql| {
            OTHER_SPELLINGS
                .iter()
                .all(|spelling| !sql.contains(spelling))
        }));
    }

    #[test]
    fn every_generated_materialized_view_is_accepted_by_turso_and_sqlite() {
        use crate::oracle::{DifferentialOracle, QueryResult};
        use sql_gen_prop::{ColumnDef, DataType, SchemaBuilder, Table};
        use std::sync::Arc;

        let turso_db = turso_core::Database::open_file_with_flags(
            Arc::new(crate::memory::MemorySimIO::new(7)),
            "matview-shapes.db",
            turso_core::OpenFlags::default(),
            turso_core::DatabaseOpts::new().with_views(true),
            None,
            Arc::new(turso_core::SqliteDialect),
        )
        .unwrap();
        let turso = turso_db.connect().unwrap();
        let sqlite = rusqlite::Connection::open_in_memory().unwrap();
        let tables = [
            (
                "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, score REAL, payload BLOB, qty INTEGER NOT NULL)",
                Table::new(
                    "items",
                    vec![
                        ColumnDef::new("id", DataType::Integer).primary_key(),
                        ColumnDef::new("name", DataType::Text),
                        ColumnDef::new("score", DataType::Real),
                        ColumnDef::new("payload", DataType::Blob),
                        ColumnDef::new("qty", DataType::Integer).not_null(),
                    ],
                ),
            ),
            (
                "CREATE TABLE tags(tag TEXT, item INTEGER)",
                Table::new(
                    "tags",
                    vec![
                        ColumnDef::new("tag", DataType::Text),
                        ColumnDef::new("item", DataType::Integer),
                    ],
                ),
            ),
            (
                "CREATE TABLE notes(id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)",
                Table::new(
                    "notes",
                    vec![
                        ColumnDef::new("id", DataType::Integer).primary_key(),
                        ColumnDef::new("name", DataType::Text),
                        ColumnDef::new("qty", DataType::Integer),
                    ],
                ),
            ),
            (
                "CREATE TABLE blobs(b BLOB)",
                Table::new("blobs", vec![ColumnDef::new("b", DataType::Blob)]),
            ),
            (
                "CREATE TABLE strict_kv(k INTEGER PRIMARY KEY, v TEXT) STRICT",
                Table::new_strict(
                    "strict_kv",
                    vec![
                        ColumnDef::new("k", DataType::Integer).primary_key(),
                        ColumnDef::new("v", DataType::Text),
                    ],
                ),
            ),
        ];
        for (sql, _) in &tables {
            turso.execute(sql).unwrap();
            sqlite.execute(sql, []).unwrap();
        }

        let mut matviews = Matviews::new();
        let mut runner = TestRunner::deterministic();
        let (mut same_name_joins, mut self_joins) = (0, 0);
        for _ in 0..200 {
            let mut builder = SchemaBuilder::new();
            for (_, table) in &tables {
                builder = builder.add_table(table.clone());
            }
            for (name, columns) in &matviews {
                builder = builder.add_materialized_view(Table::new(name.clone(), columns.clone()));
            }
            let create = sql_gen_prop::strategies::create_materialized_view(&builder.build())
                .new_tree(&mut runner)
                .unwrap()
                .current();
            let turso_sql = create.to_string();
            let turso_result = DifferentialOracle::execute_turso(&turso, &turso_sql);
            let sqlite_result =
                DifferentialOracle::execute_sqlite(&sqlite, &create.plain_view_sql());
            assert!(
                !matches!(turso_result, QueryResult::Error(_))
                    && !matches!(sqlite_result, QueryResult::Error(_)),
                "{turso_sql}\n  Turso: {turso_result:?}\n  SQLite: {sqlite_result:?}"
            );
            same_name_joins += usize::from(turso_sql.contains("_l, "));
            self_joins += usize::from(turso_sql.contains(" AS sjk, "));
            matviews.insert(create.view_name, create.output_columns);
        }
        assert!(same_name_joins > 0 && self_joins > 0);
    }
}
