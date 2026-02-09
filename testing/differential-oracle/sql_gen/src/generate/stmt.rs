//! Statement generation.

use crate::SqlGen;
use crate::ast::{
    AlterTableAction, AlterTableActionKind, AlterTableStmt, ColumnDefStmt, ConflictClause,
    CreateIndexStmt, CreateTableStmt, CreateTriggerStmt, DeleteStmt, DropIndexStmt, DropTableStmt,
    DropTriggerStmt, Expr, InsertStmt, Stmt, TriggerBodyStmtKind, TriggerEvent, TriggerEventKind,
    TriggerStmt, TriggerTiming, UpdateStmt,
};
use crate::capabilities::Capabilities;
use crate::context::Context;
use crate::error::GenError;
use crate::generate::expr::{generate_condition, generate_expr};
use crate::generate::literal::generate_literal;
use crate::generate::select::generate_select;
use crate::policy::AlterTableConfig;
use crate::schema::DataType;
use crate::trace::{Origin, StmtKind};
use sql_gen_macros::trace_gen;

/// Generate a statement respecting capability constraints.
pub fn generate_statement<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Stmt, GenError> {
    let candidates = build_stmt_candidates::<C>(generator)?;

    let kind = generator
        .policy()
        .select_stmt_kind(ctx, &candidates, generator.schema())?;
    ctx.record_stmt(kind);

    dispatch_stmt_generation(generator, ctx, kind)
}

/// Build list of allowed statement kinds based on capabilities, policy weights, and schema validity.
fn build_stmt_candidates<C: Capabilities>(
    generator: &SqlGen<C>,
) -> Result<Vec<StmtKind>, GenError> {
    let capability_candidates = collect_capability_allowed_stmts::<C>();

    if capability_candidates.is_empty() {
        return Err(GenError::exhausted(
            "statement",
            "no statement types allowed by capabilities",
        ));
    }

    let weighted_candidates = filter_by_policy_weight(generator, capability_candidates);

    let valid_candidates: Vec<StmtKind> =
        filter_by_schema_validity(generator, weighted_candidates).collect();

    if valid_candidates.is_empty() {
        return Err(GenError::exhausted(
            "statement",
            "no statement types valid for current schema state",
        ));
    }

    Ok(valid_candidates)
}

/// Collect statement kinds allowed by the capability type parameter.
fn collect_capability_allowed_stmts<C: Capabilities>() -> Vec<StmtKind> {
    let mut candidates = Vec::new();

    if C::SELECT {
        candidates.push(StmtKind::Select);
    }
    if C::INSERT {
        candidates.push(StmtKind::Insert);
    }
    if C::UPDATE {
        candidates.push(StmtKind::Update);
    }
    if C::DELETE {
        candidates.push(StmtKind::Delete);
    }
    if C::CREATE_TABLE {
        candidates.push(StmtKind::CreateTable);
    }
    if C::DROP_TABLE {
        candidates.push(StmtKind::DropTable);
    }
    if C::ALTER_TABLE {
        candidates.push(StmtKind::AlterTable);
    }
    if C::CREATE_INDEX {
        candidates.push(StmtKind::CreateIndex);
    }
    if C::DROP_INDEX {
        candidates.push(StmtKind::DropIndex);
    }
    if C::BEGIN {
        candidates.push(StmtKind::Begin);
    }
    if C::COMMIT {
        candidates.push(StmtKind::Commit);
    }
    if C::ROLLBACK {
        candidates.push(StmtKind::Rollback);
    }
    if C::CREATE_TRIGGER {
        candidates.push(StmtKind::CreateTrigger);
    }
    if C::DROP_TRIGGER {
        candidates.push(StmtKind::DropTrigger);
    }
    // Stubs
    if C::CREATE_VIEW {
        candidates.push(StmtKind::CreateView);
    }
    if C::DROP_VIEW {
        candidates.push(StmtKind::DropView);
    }
    if C::VACUUM {
        candidates.push(StmtKind::Vacuum);
    }
    if C::REINDEX {
        candidates.push(StmtKind::Reindex);
    }
    if C::ANALYZE {
        candidates.push(StmtKind::Analyze);
    }
    if C::SAVEPOINT {
        candidates.push(StmtKind::Savepoint);
    }
    if C::RELEASE {
        candidates.push(StmtKind::Release);
    }

    candidates
}

/// Filter candidates to only those with positive policy weight.
fn filter_by_policy_weight<C: Capabilities>(
    generator: &SqlGen<C>,
    candidates: impl IntoIterator<Item = StmtKind>,
) -> impl Iterator<Item = StmtKind> {
    candidates
        .into_iter()
        .filter(|k| generator.policy().stmt_weights.weight_for(*k) > 0)
}

/// Filter candidates to only those valid for the current schema state.
///
/// This prevents attempting to generate statements that will clearly fail,
/// such as INSERT when there are no tables.
fn filter_by_schema_validity<C: Capabilities>(
    generator: &SqlGen<C>,
    candidates: impl Iterator<Item = StmtKind>,
) -> impl Iterator<Item = StmtKind> {
    let schema = generator.schema();
    let has_tables = !schema.tables.is_empty();
    let has_indexes = !schema.indexes.is_empty();
    let has_triggers = !schema.triggers.is_empty();
    candidates
        .filter(move |kind| is_stmt_valid_for_schema(*kind, has_tables, has_indexes, has_triggers))
}

/// Check if a statement kind is valid given the current schema state.
fn is_stmt_valid_for_schema(
    kind: StmtKind,
    has_tables: bool,
    has_indexes: bool,
    has_triggers: bool,
) -> bool {
    match kind {
        // SELECT is always valid (SELECT 1, SELECT ABS(-5), etc.)
        StmtKind::Select => true,
        // INSERT/DELETE/UPDATE require tables
        StmtKind::Insert | StmtKind::Delete | StmtKind::Update => has_tables,
        // DDL that operates on existing tables
        StmtKind::DropTable
        | StmtKind::AlterTable
        | StmtKind::CreateIndex
        | StmtKind::CreateTrigger => has_tables,
        // DROP INDEX requires indexes to exist
        StmtKind::DropIndex => has_indexes,
        // DROP TRIGGER requires triggers to exist
        StmtKind::DropTrigger => has_triggers,
        // These are always valid
        StmtKind::CreateTable | StmtKind::Begin | StmtKind::Commit | StmtKind::Rollback => true,
        // Stubs — always valid (would require tables for views but weight is 0 anyway)
        StmtKind::CreateView => has_tables,
        StmtKind::DropView => true,
        StmtKind::Vacuum | StmtKind::Analyze => true,
        StmtKind::Reindex => has_tables || has_indexes,
        StmtKind::Savepoint | StmtKind::Release => true,
    }
}

/// Dispatch to the appropriate statement generator.
fn dispatch_stmt_generation<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    kind: StmtKind,
) -> Result<Stmt, GenError> {
    match kind {
        StmtKind::Select => generate_select(generator, ctx),
        StmtKind::Insert => generate_insert(generator, ctx),
        StmtKind::Update => generate_update(generator, ctx),
        StmtKind::Delete => generate_delete(generator, ctx),
        StmtKind::CreateTable => generate_create_table(generator, ctx),
        StmtKind::DropTable => generate_drop_table(generator, ctx),
        StmtKind::AlterTable => generate_alter_table(generator, ctx),
        StmtKind::CreateIndex => generate_create_index(generator, ctx),
        StmtKind::DropIndex => generate_drop_index(generator, ctx),
        StmtKind::Begin => Ok(Stmt::Begin),
        StmtKind::Commit => Ok(Stmt::Commit),
        StmtKind::Rollback => Ok(Stmt::Rollback),
        StmtKind::CreateTrigger => generate_create_trigger(generator, ctx),
        StmtKind::DropTrigger => generate_drop_trigger(generator, ctx),
        // Stubs
        StmtKind::CreateView => todo!("CREATE VIEW generation"),
        StmtKind::DropView => todo!("DROP VIEW generation"),
        StmtKind::Vacuum => todo!("VACUUM generation"),
        StmtKind::Reindex => todo!("REINDEX generation"),
        StmtKind::Analyze => todo!("ANALYZE generation"),
        StmtKind::Savepoint => todo!("SAVEPOINT generation"),
        StmtKind::Release => todo!("RELEASE generation"),
    }
}

/// Generate an INSERT statement.
#[trace_gen(Origin::Insert)]
pub fn generate_insert<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Stmt, GenError> {
    let insert_config = &generator.policy().insert_config;

    let table = ctx
        .choose(&generator.schema().tables)
        .ok_or_else(|| GenError::schema_empty("tables"))?
        .clone();

    // Generate column list (all columns or subset)
    let columns: Vec<String> = if ctx.gen_bool_with_prob(insert_config.explicit_columns_probability)
    {
        // All columns
        table.columns.iter().map(|c| c.name.clone()).collect()
    } else {
        // Subset (but always include non-nullable columns)
        table
            .columns
            .iter()
            .filter(|c| !c.nullable || ctx.gen_bool())
            .map(|c| c.name.clone())
            .collect()
    };

    if columns.is_empty() {
        return Err(GenError::exhausted("insert", "no columns to insert"));
    }

    // Generate conflict clause
    let conflict = generate_conflict_clause(
        ctx,
        insert_config.or_replace_probability,
        insert_config.or_ignore_probability,
    );

    // Generate values (push table into scope for expression generation)
    let values = ctx.with_table_scope([(table.clone(), None)], |ctx| {
        generate_insert_values(generator, ctx, &columns)
    });

    // --- INSERT ... SELECT (not yet implemented) ---
    if ctx.gen_bool_with_prob(insert_config.insert_select_probability) {
        return generate_insert_select(generator, ctx).map(|_| unreachable!());
    }

    // --- Upsert (not yet implemented) ---
    if ctx.gen_bool_with_prob(insert_config.upsert_probability) {
        let _ = generate_upsert(generator, ctx);
    }

    // --- DEFAULT VALUES (not yet implemented) ---
    if ctx.gen_bool_with_prob(insert_config.default_values_probability) {
        return generate_insert_default_values(generator, ctx);
    }

    // --- RETURNING (not yet implemented) ---
    if ctx.gen_bool_with_prob(insert_config.returning_probability) {
        let _ = generate_insert_returning(generator, ctx);
    }

    Ok(Stmt::Insert(InsertStmt {
        table: table.name,
        columns,
        values,
        conflict,
    }))
}

/// Generate the VALUES clause for an INSERT statement.
///
/// Each cell may be a literal or a full expression, controlled by
/// `InsertConfig::expression_value_probability`. Expressions use `column_ref: 0`
/// since INSERT VALUES has no implicit table scope for bare column refs.
#[trace_gen(Origin::InsertValues)]
fn generate_insert_values<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    columns: &[String],
) -> Vec<Vec<Expr>> {
    let insert_config = &generator.policy().insert_config;
    let expr_prob = insert_config.expression_value_probability;
    let expr_max_depth = insert_config.expression_value_max_depth;
    let num_rows = ctx.gen_range_inclusive(insert_config.min_rows, insert_config.max_rows);
    let mut values = Vec::with_capacity(num_rows);

    // For INSERT expressions, create a generator with column_ref disabled
    // since bare column refs are invalid in VALUES context.
    let insert_expr_gen = if expr_prob > 0.0 {
        let mut policy = generator.policy().clone();
        policy.expr_weights.column_ref = 0;
        policy.max_expr_depth = expr_max_depth;
        Some(SqlGen::<C>::new(generator.schema().clone(), policy))
    } else {
        None
    };

    // Read table columns from scope
    let table_columns = ctx.tables_in_scope()[0].table.columns.clone();

    for _ in 0..num_rows {
        let mut row = Vec::with_capacity(columns.len());
        for col_name in columns {
            let col = table_columns.iter().find(|c| &c.name == col_name).unwrap();
            if let Some(ref expr_gen) = insert_expr_gen {
                if ctx.gen_bool_with_prob(expr_prob) {
                    if let Ok(expr) = generate_expr(expr_gen, ctx, 0) {
                        row.push(expr);
                        continue;
                    }
                }
            }
            let lit = generate_literal(ctx, col.data_type, generator.policy());
            row.push(Expr::literal(ctx, lit));
        }
        values.push(row);
    }

    values
}

/// Generate an UPDATE statement.
#[trace_gen(Origin::Update)]
pub fn generate_update<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Stmt, GenError> {
    let update_config = &generator.policy().update_config;

    let table = ctx
        .choose(&generator.schema().tables)
        .ok_or_else(|| GenError::schema_empty("tables"))?
        .clone();

    if table.columns.is_empty() {
        return Err(GenError::schema_empty("columns"));
    }

    ctx.with_table_scope([(table.clone(), None)], |ctx| {
        // Generate conflict clause
        let conflict = generate_conflict_clause(
            ctx,
            update_config.or_replace_probability,
            update_config.or_ignore_probability,
        );

        // Generate SET clause
        let sets = generate_update_sets(generator, ctx)?;

        // Generate optional WHERE clause
        let where_clause = if ctx.gen_bool_with_prob(update_config.where_probability) {
            Some(generate_condition(generator, ctx)?)
        } else {
            None
        };

        // --- UPDATE ... FROM (not yet implemented) ---
        if ctx.gen_bool_with_prob(update_config.from_probability) {
            let _ = generate_update_from(generator, ctx);
        }

        // --- RETURNING (not yet implemented) ---
        if ctx.gen_bool_with_prob(update_config.returning_probability) {
            let _ = generate_update_returning(generator, ctx);
        }

        Ok(Stmt::Update(UpdateStmt {
            table: table.name.clone(),
            sets,
            where_clause,
            conflict,
        }))
    })
}

/// Generate the SET clause for an UPDATE statement.
///
/// All columns are eligible. Primary key columns are included with lower
/// probability controlled by `UpdateConfig::primary_key_update_probability`.
/// Each assignment value may be an expression (with column refs enabled, so
/// `SET x = x + 1` is valid) controlled by `UpdateConfig::expression_value_probability`.
#[trace_gen(Origin::UpdateSet)]
fn generate_update_sets<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Vec<(String, Expr)>, GenError> {
    let update_config = &generator.policy().update_config;
    let pk_prob = update_config.primary_key_update_probability;
    let expr_prob = update_config.expression_value_probability;
    let expr_max_depth = update_config.expression_value_max_depth;

    // Read table columns from scope
    let table_columns = ctx.tables_in_scope()[0].table.columns.clone();

    // Build candidate columns: always include non-PK, include PK with probability
    let candidates: Vec<_> = table_columns
        .iter()
        .filter(|c| !c.primary_key || ctx.gen_bool_with_prob(pk_prob))
        .cloned()
        .collect();

    // If all PK columns were filtered out and there are no others, fall back to all columns
    let candidates = if candidates.is_empty() {
        table_columns
    } else {
        candidates
    };

    let max_sets = update_config.max_set_clauses.min(candidates.len());
    let min_sets = update_config.min_set_clauses.min(max_sets);
    let num_sets = ctx.gen_range_inclusive(min_sets, max_sets);
    let mut sets = Vec::with_capacity(num_sets);

    // For UPDATE expressions, create a generator with capped depth.
    // Column refs are valid (e.g. SET x = x + 1).
    let update_expr_gen = if expr_prob > 0.0 {
        let mut policy = generator.policy().clone();
        policy.max_expr_depth = expr_max_depth;
        Some(SqlGen::<C>::new(generator.schema().clone(), policy))
    } else {
        None
    };

    for _ in 0..num_sets {
        let col = ctx.choose(&candidates).unwrap();
        if let Some(ref expr_gen) = update_expr_gen {
            if ctx.gen_bool_with_prob(expr_prob) {
                if let Ok(expr) = generate_expr(expr_gen, ctx, 0) {
                    sets.push((col.name.clone(), expr));
                    continue;
                }
            }
        }
        let lit = generate_literal(ctx, col.data_type, generator.policy());
        sets.push((col.name.clone(), Expr::literal(ctx, lit)));
    }

    Ok(sets)
}

/// Generate an optional conflict clause (OR ABORT/FAIL/IGNORE/REPLACE/ROLLBACK).
///
/// Uses the existing `or_replace_probability` and `or_ignore_probability` from the config,
/// plus equal weights for Abort, Fail, and Rollback. If neither or_replace nor or_ignore
/// has any probability, no conflict clause is generated.
fn generate_conflict_clause(
    ctx: &mut Context,
    or_replace_prob: f64,
    or_ignore_prob: f64,
) -> Option<ConflictClause> {
    // Total probability of getting any conflict clause
    let total_prob = or_replace_prob + or_ignore_prob;
    if total_prob <= 0.0 {
        return None;
    }

    // First decide whether to generate a conflict clause at all
    if !ctx.gen_bool_with_prob(total_prob.min(1.0)) {
        return None;
    }

    // Weighted selection among the five variants.
    // Scale replace/ignore to integer weights, give abort/fail/rollback equal share of the remaining space.
    let replace_w = (or_replace_prob * 100.0) as u32;
    let ignore_w = (or_ignore_prob * 100.0) as u32;
    let other_w = ((replace_w + ignore_w) / 3).max(1);

    let items = [
        (ConflictClause::Replace, replace_w),
        (ConflictClause::Ignore, ignore_w),
        (ConflictClause::Abort, other_w),
        (ConflictClause::Fail, other_w),
        (ConflictClause::Rollback, other_w),
    ];

    let weights: Vec<u32> = items.iter().map(|(_, w)| *w).collect();
    ctx.weighted_index(&weights).map(|idx| items[idx].0)
}

/// Generate a DELETE statement.
#[trace_gen(Origin::Delete)]
pub fn generate_delete<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Stmt, GenError> {
    let delete_config = &generator.policy().delete_config;

    let table = ctx
        .choose(&generator.schema().tables)
        .ok_or_else(|| GenError::schema_empty("tables"))?
        .clone();

    ctx.with_table_scope([(table.clone(), None)], |ctx| {
        // Generate optional WHERE clause (almost always have one to avoid deleting everything)
        let where_clause = if ctx.gen_bool_with_prob(delete_config.where_probability) {
            Some(generate_condition(generator, ctx)?)
        } else {
            None
        };

        // --- RETURNING (not yet implemented) ---
        if ctx.gen_bool_with_prob(delete_config.returning_probability) {
            let _ = generate_delete_returning(generator, ctx);
        }

        Ok(Stmt::Delete(DeleteStmt {
            table: table.name.clone(),
            where_clause,
        }))
    })
}

/// Generate a CREATE TABLE statement.
pub fn generate_create_table<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Stmt, GenError> {
    let create_table_config = &generator.policy().create_table_config;

    // Generate a unique table name
    let existing_names = generator.schema().table_names();
    let table_name = ctx.gen_unique_name("tbl", &existing_names);

    // Generate columns
    let num_cols = ctx.gen_range_inclusive(
        create_table_config.min_columns,
        create_table_config.max_columns,
    );
    let mut columns = Vec::with_capacity(num_cols);
    let mut col_names: std::collections::HashSet<String> = std::collections::HashSet::new();

    // First column is usually the primary key
    let pk_name = ctx.gen_unique_name("col", &col_names);
    col_names.insert(pk_name.clone());
    columns.push(ColumnDefStmt {
        name: pk_name,
        data_type: DataType::Integer,
        primary_key: true,
        not_null: true,
        unique: false,
        default: None,
    });

    // Generate additional columns
    let types = [
        DataType::Integer,
        DataType::Real,
        DataType::Text,
        DataType::Blob,
    ];
    for _ in 1..num_cols {
        let data_type = *ctx.choose(&types).unwrap();
        let not_null = ctx.gen_bool_with_prob(create_table_config.not_null_probability);
        let unique = ctx.gen_bool_with_prob(create_table_config.unique_probability);
        let default = if ctx.gen_bool_with_prob(create_table_config.default_probability) {
            let lit = generate_literal(ctx, data_type, generator.policy());
            Some(Expr::literal(ctx, lit))
        } else {
            None
        };
        let name = ctx.gen_unique_name("col", &col_names);
        col_names.insert(name.clone());

        columns.push(ColumnDefStmt {
            name,
            data_type,
            primary_key: false,
            not_null,
            unique,
            default,
        });
    }

    // --- CREATE TABLE ... AS SELECT (not yet implemented) ---
    if ctx.gen_bool_with_prob(create_table_config.as_select_probability) {
        return generate_create_table_as_select(generator, ctx);
    }

    // --- Table-level constraints (not yet implemented) ---
    if ctx.gen_bool_with_prob(create_table_config.check_constraint_probability) {
        let _ = generate_check_constraint(generator, ctx);
    }
    if ctx.gen_bool_with_prob(create_table_config.foreign_key_probability) {
        let _ = generate_foreign_key(generator, ctx);
    }

    Ok(Stmt::CreateTable(CreateTableStmt {
        table: table_name,
        columns,
        if_not_exists: ctx.gen_bool_with_prob(create_table_config.if_not_exists_probability),
    }))
}

/// Generate a DROP TABLE statement.
pub fn generate_drop_table<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Stmt, GenError> {
    let drop_table_config = &generator.policy().drop_table_config;

    let table = ctx
        .choose(&generator.schema().tables)
        .ok_or_else(|| GenError::schema_empty("tables"))?;

    Ok(Stmt::DropTable(DropTableStmt {
        table: table.name.clone(),
        if_exists: ctx.gen_bool_with_prob(drop_table_config.if_exists_probability),
    }))
}

/// Generate an ALTER TABLE statement.
#[trace_gen(Origin::AlterTable)]
pub fn generate_alter_table<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Stmt, GenError> {
    let alter_config = &generator.policy().alter_table_config;

    let table = ctx
        .choose(&generator.schema().tables)
        .ok_or_else(|| GenError::schema_empty("tables"))?
        .clone();

    let action = generate_alter_table_action(generator, ctx, &table, alter_config)?;

    Ok(Stmt::AlterTable(AlterTableStmt {
        table: table.name.clone(),
        action,
    }))
}

/// Generate an ALTER TABLE action.
#[trace_gen(Origin::AlterTableAction)]
fn generate_alter_table_action<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &crate::schema::Table,
    config: &AlterTableConfig,
) -> Result<AlterTableAction, GenError> {
    let weights = &config.action_weights;

    // Check which actions are possible
    let droppable_columns: Vec<_> = table.columns.iter().filter(|c| !c.primary_key).collect();
    let can_drop_column = !droppable_columns.is_empty();
    let can_rename_column = !table.columns.is_empty();

    // Build weights, disabling impossible actions
    let items = [
        (AlterTableActionKind::RenameTo, weights.rename_table),
        (AlterTableActionKind::AddColumn, weights.add_column),
        (
            AlterTableActionKind::DropColumn,
            if can_drop_column {
                weights.drop_column
            } else {
                0
            },
        ),
        (
            AlterTableActionKind::RenameColumn,
            if can_rename_column {
                weights.rename_column
            } else {
                0
            },
        ),
    ];

    let weight_vec: Vec<u32> = items.iter().map(|(_, w)| *w).collect();
    let idx = ctx.weighted_index(&weight_vec).ok_or_else(|| {
        GenError::exhausted(
            "alter_table_action",
            "no valid alter table actions available",
        )
    })?;

    let existing_table_names = generator.schema().table_names();
    let existing_col_names: std::collections::HashSet<String> =
        table.columns.iter().map(|c| c.name.clone()).collect();

    match items[idx].0 {
        AlterTableActionKind::RenameTo => {
            let new_name = ctx.gen_unique_name_excluding("tbl", &existing_table_names, &table.name);
            Ok(AlterTableAction::RenameTo(new_name))
        }
        AlterTableActionKind::AddColumn => {
            let col_name = ctx.gen_unique_name("col", &existing_col_names);

            let types = [
                DataType::Integer,
                DataType::Real,
                DataType::Text,
                DataType::Blob,
            ];
            let data_type = *ctx.choose(&types).unwrap();
            let not_null = ctx.gen_bool_with_prob(config.not_null_probability);

            Ok(AlterTableAction::AddColumn(ColumnDefStmt {
                name: col_name,
                data_type,
                primary_key: false,
                not_null,
                unique: false,
                default: None,
            }))
        }
        AlterTableActionKind::DropColumn => {
            let col = ctx.choose(&droppable_columns).unwrap();
            Ok(AlterTableAction::DropColumn(col.name.clone()))
        }
        AlterTableActionKind::RenameColumn => {
            let col = ctx.choose(&table.columns).unwrap();
            let new_name = ctx.gen_unique_name("col", &existing_col_names);

            Ok(AlterTableAction::RenameColumn {
                old_name: col.name.clone(),
                new_name,
            })
        }
    }
}

/// Generate a CREATE INDEX statement.
pub fn generate_create_index<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Stmt, GenError> {
    let create_index_config = &generator.policy().create_index_config;

    let table = ctx
        .choose(&generator.schema().tables)
        .ok_or_else(|| GenError::schema_empty("tables"))?
        .clone();

    // Generate unique index name
    let existing_names = generator.schema().index_names();
    let prefix = format!("idx_{}", table.name);
    let index_name = ctx.gen_unique_name(&prefix, &existing_names);

    // Select columns for the index
    let max_cols = table.columns.len().min(create_index_config.max_columns);
    let num_cols = ctx.gen_range_inclusive(1, max_cols);
    let columns: Vec<String> = (0..num_cols)
        .filter_map(|_| ctx.choose(&table.columns).map(|c| c.name.clone()))
        .collect();

    if columns.is_empty() {
        return Err(GenError::exhausted("create_index", "no columns for index"));
    }

    // --- Partial index (not yet implemented) ---
    if ctx.gen_bool_with_prob(create_index_config.partial_index_probability) {
        let _ = generate_partial_index(generator, ctx);
    }

    // --- Expression index (not yet implemented) ---
    if ctx.gen_bool_with_prob(create_index_config.expression_index_probability) {
        let _ = generate_expression_index(generator, ctx);
    }

    Ok(Stmt::CreateIndex(CreateIndexStmt {
        name: index_name,
        table: table.name,
        columns,
        unique: ctx.gen_bool_with_prob(create_index_config.unique_probability),
        if_not_exists: ctx.gen_bool_with_prob(create_index_config.if_not_exists_probability),
    }))
}

/// Generate a DROP INDEX statement.
pub fn generate_drop_index<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Stmt, GenError> {
    let drop_index_config = &generator.policy().drop_index_config;
    let ident_config = &generator.policy().identifier_config;

    // Try to use an existing index, or generate a plausible name
    let index_name = if let Some(index) = ctx.choose(&generator.schema().indexes) {
        index.name.clone()
    } else {
        format!("idx_{}", ctx.gen_range(ident_config.name_suffix_range))
    };

    Ok(Stmt::DropIndex(DropIndexStmt {
        name: index_name,
        if_exists: ctx.gen_bool_with_prob(drop_index_config.if_exists_probability),
    }))
}

/// Generate a CREATE TRIGGER statement.
#[trace_gen(Origin::CreateTrigger)]
pub fn generate_create_trigger<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Stmt, GenError> {
    let trigger_config = &generator.policy().trigger_config;
    let ident_config = &generator.policy().identifier_config;

    let table = ctx
        .choose(&generator.schema().tables)
        .ok_or_else(|| GenError::schema_empty("tables"))?
        .clone();

    // Generate unique trigger name
    let trigger_name = format!(
        "trg_{}_{}",
        table.name,
        ctx.gen_range(ident_config.name_suffix_range)
    );

    // Select timing (BEFORE, AFTER, INSTEAD OF)
    let timing = generate_trigger_timing(ctx, generator)?;

    // Select event (INSERT, UPDATE, DELETE)
    let event = generate_trigger_event(ctx, generator, &table)?;

    // Generate FOR EACH ROW
    let for_each_row = ctx.gen_bool_with_prob(trigger_config.for_each_row_probability);

    // Generate optional WHEN clause
    let when_clause = if ctx.gen_bool_with_prob(trigger_config.when_probability) {
        Some(ctx.with_table_scope([(table.clone(), None)], |ctx| {
            generate_trigger_when(generator, ctx)
        })?)
    } else {
        None
    };

    // Generate trigger body (one or more statements)
    let body = generate_trigger_body(generator, ctx, trigger_config)?;

    // IF NOT EXISTS
    let if_not_exists = ctx.gen_bool_with_prob(trigger_config.if_not_exists_probability);

    Ok(Stmt::CreateTrigger(CreateTriggerStmt {
        name: trigger_name,
        table: table.name.clone(),
        timing,
        event,
        for_each_row,
        when_clause,
        body,
        if_not_exists,
    }))
}

/// Generate trigger timing (BEFORE, AFTER, INSTEAD OF).
fn generate_trigger_timing<C: Capabilities>(
    ctx: &mut Context,
    generator: &SqlGen<C>,
) -> Result<TriggerTiming, GenError> {
    let weights = &generator.policy().trigger_config.timing_weights;
    let items = [
        (TriggerTiming::Before, weights.before),
        (TriggerTiming::After, weights.after),
        (TriggerTiming::InsteadOf, weights.instead_of),
    ];

    let weight_vec: Vec<u32> = items.iter().map(|(_, w)| *w).collect();
    let idx = ctx
        .weighted_index(&weight_vec)
        .ok_or_else(|| GenError::exhausted("trigger_timing", "no valid trigger timing options"))?;
    Ok(items[idx].0)
}

/// Generate trigger event (INSERT, UPDATE, DELETE).
fn generate_trigger_event<C: Capabilities>(
    ctx: &mut Context,
    generator: &SqlGen<C>,
    table: &crate::schema::Table,
) -> Result<TriggerEvent, GenError> {
    let trigger_config = &generator.policy().trigger_config;
    let weights = &trigger_config.event_weights;
    let items = [
        (TriggerEventKind::Insert, weights.insert),
        (TriggerEventKind::Update, weights.update),
        (TriggerEventKind::Delete, weights.delete),
    ];

    let weight_vec: Vec<u32> = items.iter().map(|(_, w)| *w).collect();
    let idx = ctx
        .weighted_index(&weight_vec)
        .ok_or_else(|| GenError::exhausted("trigger_event", "no valid trigger event options"))?;

    Ok(match items[idx].0 {
        TriggerEventKind::Insert => TriggerEvent::Insert,
        TriggerEventKind::Update => {
            // UPDATE event - optionally with specific columns
            if ctx.gen_bool_with_prob(trigger_config.update_of_columns_probability)
                && !table.columns.is_empty()
            {
                let num_cols = ctx.gen_range_inclusive(
                    1,
                    trigger_config
                        .max_update_of_columns
                        .min(table.columns.len()),
                );
                let columns: Vec<String> = (0..num_cols)
                    .filter_map(|_| ctx.choose(&table.columns).map(|c| c.name.clone()))
                    .collect();
                TriggerEvent::Update(columns)
            } else {
                TriggerEvent::Update(vec![])
            }
        }
        TriggerEventKind::Delete => TriggerEvent::Delete,
    })
}

/// Generate the WHEN clause for a trigger.
#[trace_gen(Origin::TriggerWhen)]
fn generate_trigger_when<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Expr, GenError> {
    // Generate a simple condition using NEW or OLD references
    generate_condition(generator, ctx)
}

/// Generate the body of a trigger.
#[trace_gen(Origin::TriggerBody)]
fn generate_trigger_body<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    config: &crate::policy::TriggerConfig,
) -> Result<Vec<TriggerStmt>, GenError> {
    let num_stmts = ctx.gen_range_inclusive(config.min_body_statements, config.max_body_statements);
    let mut body = Vec::with_capacity(num_stmts);

    for _ in 0..num_stmts {
        let stmt = generate_trigger_body_stmt(generator, ctx)?;
        body.push(stmt);
    }

    Ok(body)
}

/// Generate a single statement for a trigger body.
fn generate_trigger_body_stmt<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<TriggerStmt, GenError> {
    let body_weights = &generator.policy().trigger_config.body_stmt_weights;

    let weights = [
        (TriggerBodyStmtKind::Insert, body_weights.insert),
        (TriggerBodyStmtKind::Update, body_weights.update),
        (TriggerBodyStmtKind::Delete, body_weights.delete),
        (TriggerBodyStmtKind::Select, body_weights.select),
    ];

    let weight_vec: Vec<u32> = weights.iter().map(|(_, w)| *w).collect();
    let idx = ctx.weighted_index(&weight_vec).ok_or_else(|| {
        GenError::exhausted(
            "trigger_body_stmt",
            "no valid trigger body statement options",
        )
    })?;

    match weights[idx].0 {
        TriggerBodyStmtKind::Insert => match generate_insert(generator, ctx)? {
            Stmt::Insert(stmt) => Ok(TriggerStmt::Insert(stmt)),
            _ => unreachable!(),
        },
        TriggerBodyStmtKind::Update => match generate_update(generator, ctx)? {
            Stmt::Update(stmt) => Ok(TriggerStmt::Update(stmt)),
            _ => unreachable!(),
        },
        TriggerBodyStmtKind::Delete => match generate_delete(generator, ctx)? {
            Stmt::Delete(stmt) => Ok(TriggerStmt::Delete(stmt)),
            _ => unreachable!(),
        },
        TriggerBodyStmtKind::Select => match generate_select(generator, ctx)? {
            Stmt::Select(stmt) => Ok(TriggerStmt::Select(stmt)),
            _ => unreachable!(),
        },
    }
}

/// Generate a DROP TRIGGER statement.
pub fn generate_drop_trigger<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Stmt, GenError> {
    let trigger_config = &generator.policy().trigger_config;
    let ident_config = &generator.policy().identifier_config;

    // Generate a plausible trigger name
    let table = ctx.choose(&generator.schema().tables);
    let trigger_name = if let Some(t) = table {
        format!(
            "trg_{}_{}",
            t.name,
            ctx.gen_range(ident_config.name_suffix_range)
        )
    } else {
        format!("trg_{}", ctx.gen_range(ident_config.name_suffix_range))
    };

    Ok(Stmt::DropTrigger(DropTriggerStmt {
        name: trigger_name,
        if_exists: ctx.gen_bool_with_prob(trigger_config.if_exists_probability),
    }))
}

// ---- INSERT features ----

#[trace_gen(Origin::InsertSelect)]
fn generate_insert_select<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<Vec<Vec<Expr>>, GenError> {
    todo!("INSERT ... SELECT generation")
}

#[trace_gen(Origin::Upsert)]
fn generate_upsert<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<(), GenError> {
    todo!("ON CONFLICT (upsert) generation")
}

#[trace_gen(Origin::InsertDefaultValues)]
fn generate_insert_default_values<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<Stmt, GenError> {
    todo!("INSERT ... DEFAULT VALUES generation")
}

#[trace_gen(Origin::InsertReturning)]
fn generate_insert_returning<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<Vec<Expr>, GenError> {
    todo!("INSERT ... RETURNING generation")
}

// ---- UPDATE features ----

#[trace_gen(Origin::UpdateFrom)]
fn generate_update_from<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<(), GenError> {
    todo!("UPDATE ... FROM generation")
}

#[trace_gen(Origin::UpdateReturning)]
fn generate_update_returning<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<Vec<Expr>, GenError> {
    todo!("UPDATE ... RETURNING generation")
}

// ---- DELETE features ----

#[trace_gen(Origin::DeleteReturning)]
fn generate_delete_returning<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<Vec<Expr>, GenError> {
    todo!("DELETE ... RETURNING generation")
}

// ---- CREATE TABLE features ----

#[trace_gen(Origin::CheckConstraint)]
fn generate_check_constraint<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<Expr, GenError> {
    todo!("CHECK constraint generation")
}

#[trace_gen(Origin::ForeignKey)]
fn generate_foreign_key<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<(), GenError> {
    todo!("FOREIGN KEY generation")
}

#[trace_gen(Origin::Autoincrement)]
fn generate_autoincrement<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<(), GenError> {
    todo!("AUTOINCREMENT generation")
}

#[trace_gen(Origin::GeneratedColumn)]
fn generate_generated_column<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<(), GenError> {
    todo!("generated column generation")
}

#[trace_gen(Origin::CreateTableAsSelect)]
fn generate_create_table_as_select<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<Stmt, GenError> {
    todo!("CREATE TABLE ... AS SELECT generation")
}

// ---- Index features ----

#[trace_gen(Origin::PartialIndex)]
fn generate_partial_index<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<Expr, GenError> {
    todo!("partial index WHERE generation")
}

#[trace_gen(Origin::ExpressionIndex)]
fn generate_expression_index<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<Expr, GenError> {
    todo!("expression index generation")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::Policy;
    use crate::schema::{ColumnDef, SchemaBuilder, Table};
    use crate::{DmlOnly, Full, SelectOnly};

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
    fn test_generate_statement() {
        let generator = test_generator();
        let mut ctx = Context::new_with_seed(42);

        let stmt = generate_statement(&generator, &mut ctx);
        assert!(stmt.is_ok());
    }

    #[test]
    fn test_generate_insert() {
        let generator = test_generator();
        let mut ctx = Context::new_with_seed(42);

        let stmt = generate_insert(&generator, &mut ctx);
        assert!(stmt.is_ok());

        let sql = stmt.unwrap().to_string();
        assert!(sql.starts_with("INSERT INTO"));
    }

    #[test]
    fn test_generate_update() {
        let generator = test_generator();
        let mut ctx = Context::new_with_seed(42);

        let stmt = generate_update(&generator, &mut ctx);
        assert!(stmt.is_ok());

        let sql = stmt.unwrap().to_string();
        assert!(sql.starts_with("UPDATE"));
    }

    #[test]
    fn test_generate_delete() {
        let generator = test_generator();
        let mut ctx = Context::new_with_seed(42);

        let stmt = generate_delete(&generator, &mut ctx);
        assert!(stmt.is_ok());

        let sql = stmt.unwrap().to_string();
        assert!(sql.starts_with("DELETE FROM"));
    }

    #[test]
    fn test_select_only_capability() {
        let schema = SchemaBuilder::new()
            .table(Table::new(
                "users",
                vec![ColumnDef::new("id", DataType::Integer)],
            ))
            .build();

        let generator: SqlGen<SelectOnly> = SqlGen::new(schema, Policy::default());
        let mut ctx = Context::new_with_seed(42);

        // Should only generate SELECT
        for _ in 0..10 {
            let stmt = generate_statement(&generator, &mut ctx).unwrap();
            assert!(matches!(stmt, Stmt::Select(_)));
        }
    }

    #[test]
    fn test_dml_only_capability() {
        let schema = SchemaBuilder::new()
            .table(Table::new(
                "users",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("name", DataType::Text),
                ],
            ))
            .build();

        let generator: SqlGen<DmlOnly> = SqlGen::new(schema, Policy::default());
        let mut ctx = Context::new_with_seed(42);

        // Should only generate DML (SELECT, INSERT, UPDATE, DELETE)
        for _ in 0..20 {
            let stmt = generate_statement(&generator, &mut ctx).unwrap();
            assert!(matches!(
                stmt,
                Stmt::Select(_) | Stmt::Insert(_) | Stmt::Update(_) | Stmt::Delete(_)
            ));
        }
    }

    #[test]
    fn test_generate_create_trigger() {
        let generator = test_generator();
        let mut ctx = Context::new_with_seed(42);

        let stmt = generate_create_trigger(&generator, &mut ctx);
        assert!(stmt.is_ok());

        let sql = stmt.unwrap().to_string();
        assert!(sql.starts_with("CREATE TRIGGER"));
        assert!(sql.contains("ON users"));
        assert!(sql.contains("BEGIN"));
        assert!(sql.contains("END"));
    }

    #[test]
    fn test_generate_drop_trigger() {
        let generator = test_generator();
        let mut ctx = Context::new_with_seed(42);

        let stmt = generate_drop_trigger(&generator, &mut ctx);
        assert!(stmt.is_ok());

        let sql = stmt.unwrap().to_string();
        assert!(sql.starts_with("DROP TRIGGER"));
    }

    #[test]
    fn test_trigger_timing_variants() {
        let generator = test_generator();

        // Test multiple seeds to exercise different timing variants
        for seed in [42, 123, 456, 789, 1000] {
            let mut ctx = Context::new_with_seed(seed);
            let stmt = generate_create_trigger(&generator, &mut ctx).unwrap();

            let sql = stmt.to_string();
            // Should contain one of BEFORE, AFTER, or INSTEAD OF
            let has_timing =
                sql.contains("BEFORE ") || sql.contains("AFTER ") || sql.contains("INSTEAD OF ");
            assert!(has_timing, "Trigger should have timing: {sql}");
        }
    }

    #[test]
    fn test_trigger_event_variants() {
        let generator = test_generator();

        // Test multiple seeds to exercise different event variants
        for seed in [42, 123, 456, 789, 1000] {
            let mut ctx = Context::new_with_seed(seed);
            let stmt = generate_create_trigger(&generator, &mut ctx).unwrap();

            let sql = stmt.to_string();
            // Should contain one of INSERT, UPDATE, or DELETE
            let has_event = sql.contains(" INSERT ON")
                || sql.contains(" UPDATE ON")
                || sql.contains(" UPDATE OF ")
                || sql.contains(" DELETE ON");
            assert!(has_event, "Trigger should have event: {sql}");
        }
    }

    #[test]
    fn test_generate_alter_table() {
        let generator = test_generator();
        let mut ctx = Context::new_with_seed(42);

        let stmt = generate_alter_table(&generator, &mut ctx);
        assert!(stmt.is_ok());

        let sql = stmt.unwrap().to_string();
        assert!(sql.starts_with("ALTER TABLE"));
    }

    #[test]
    fn test_alter_table_action_variants() {
        let generator = test_generator();

        // Test multiple seeds to exercise different action variants
        for seed in [42, 123, 456, 789, 1000, 2000, 3000, 4000] {
            let mut ctx = Context::new_with_seed(seed);
            let stmt = generate_alter_table(&generator, &mut ctx).unwrap();

            let sql = stmt.to_string();
            // Should contain one of RENAME TO, ADD COLUMN, DROP COLUMN, or RENAME COLUMN
            let has_action = sql.contains("RENAME TO ")
                || sql.contains("ADD COLUMN ")
                || sql.contains("DROP COLUMN ")
                || sql.contains("RENAME COLUMN ");
            assert!(has_action, "ALTER TABLE should have action: {sql}");
        }
    }
}
