//! Statement generation.

use crate::SqlGen;
use crate::ast::{
    AlterTableAction, AlterTableActionKind, AlterTableStmt, ColumnDefStmt, CreateIndexStmt,
    CreateTableStmt, CreateTriggerStmt, DeleteStmt, DropIndexStmt, DropTableStmt, DropTriggerStmt,
    Expr, InsertStmt, Stmt, TriggerBodyStmtKind, TriggerEvent, TriggerEventKind, TriggerStmt,
    TriggerTiming, UpdateStmt,
};
use crate::capabilities::Capabilities;
use crate::context::Context;
use crate::error::GenError;
use crate::generate::expr::generate_condition;
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

    let kind = generator.policy().select_stmt_kind(ctx, &candidates)?;
    ctx.record_stmt(kind);

    dispatch_stmt_generation(generator, ctx, kind)
}

/// Build list of allowed statement kinds based on capabilities and policy weights.
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

    if weighted_candidates.is_empty() {
        return Err(GenError::exhausted(
            "statement",
            "all allowed statement types have zero weight",
        ));
    }

    Ok(weighted_candidates)
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

    candidates
}

/// Filter candidates to only those with positive policy weight.
fn filter_by_policy_weight<C: Capabilities>(
    generator: &SqlGen<C>,
    candidates: Vec<StmtKind>,
) -> Vec<StmtKind> {
    candidates
        .into_iter()
        .filter(|k| generator.policy().stmt_weights.weight_for(*k) > 0)
        .collect()
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

    // Generate values
    let values = generate_insert_values(generator, ctx, &table, &columns);

    Ok(Stmt::Insert(InsertStmt {
        table: table.name.clone(),
        columns,
        values,
    }))
}

/// Generate the VALUES clause for an INSERT statement.
#[trace_gen(Origin::InsertValues)]
fn generate_insert_values<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &crate::schema::Table,
    columns: &[String],
) -> Vec<Vec<Expr>> {
    let insert_config = &generator.policy().insert_config;
    let num_rows = ctx.gen_range_inclusive(insert_config.min_rows, insert_config.max_rows);
    let mut values = Vec::with_capacity(num_rows);

    for _ in 0..num_rows {
        let mut row = Vec::with_capacity(columns.len());
        for col_name in columns {
            let col = table.columns.iter().find(|c| &c.name == col_name).unwrap();
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

    // Get updatable columns (non-primary key)
    let updatable: Vec<_> = table.updatable_columns().collect();
    if updatable.is_empty() {
        return Err(GenError::exhausted("update", "no updatable columns"));
    }

    // Generate SET clause
    let sets = generate_update_sets(generator, ctx, &updatable)?;

    // Generate optional WHERE clause
    let where_clause = if ctx.gen_bool_with_prob(update_config.where_probability) {
        Some(generate_condition(generator, ctx, &table)?)
    } else {
        None
    };

    Ok(Stmt::Update(UpdateStmt {
        table: table.name.clone(),
        sets,
        where_clause,
    }))
}

/// Generate the SET clause for an UPDATE statement.
#[trace_gen(Origin::UpdateSet)]
fn generate_update_sets<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    updatable: &[&crate::schema::ColumnDef],
) -> Result<Vec<(String, Expr)>, GenError> {
    let update_config = &generator.policy().update_config;
    let max_sets = update_config.max_set_clauses.min(updatable.len());
    let min_sets = update_config.min_set_clauses.min(max_sets);
    let num_sets = ctx.gen_range_inclusive(min_sets, max_sets);
    let mut sets = Vec::with_capacity(num_sets);

    for _ in 0..num_sets {
        let col = ctx.choose(updatable).unwrap();
        let lit = generate_literal(ctx, col.data_type, generator.policy());
        sets.push((col.name.clone(), Expr::literal(ctx, lit)));
    }

    Ok(sets)
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

    // Generate optional WHERE clause (almost always have one to avoid deleting everything)
    let where_clause = if ctx.gen_bool_with_prob(delete_config.where_probability) {
        Some(generate_condition(generator, ctx, &table)?)
    } else {
        None
    };

    Ok(Stmt::Delete(DeleteStmt {
        table: table.name.clone(),
        where_clause,
    }))
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

    // First column is usually the primary key
    columns.push(ColumnDefStmt {
        name: "id".to_string(),
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
    for i in 1..num_cols {
        let data_type = *ctx.choose(&types).unwrap();
        let not_null = ctx.gen_bool_with_prob(create_table_config.not_null_probability);
        let unique = !not_null && ctx.gen_bool_with_prob(create_table_config.unique_probability);

        columns.push(ColumnDefStmt {
            name: format!("col{i}"),
            data_type,
            primary_key: false,
            not_null,
            unique,
            default: None,
        });
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

    Ok(Stmt::CreateIndex(CreateIndexStmt {
        name: index_name,
        table: table.name.clone(),
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
        Some(generate_trigger_when(generator, ctx, &table)?)
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
    table: &crate::schema::Table,
) -> Result<Expr, GenError> {
    // Generate a simple condition using NEW or OLD references
    generate_condition(generator, ctx, table)
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
