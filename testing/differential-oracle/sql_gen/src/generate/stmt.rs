//! Statement generation.

use crate::SqlGen;
use crate::ast::{
    ColumnDefStmt, CreateIndexStmt, CreateTableStmt, DeleteStmt, DropIndexStmt, DropTableStmt,
    Expr, InsertStmt, Stmt, UpdateStmt,
};
use crate::capabilities::Capabilities;
use crate::context::Context;
use crate::error::GenError;
use crate::generate::expr::generate_condition;
use crate::generate::literal::generate_literal;
use crate::generate::select::generate_select;
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
        StmtKind::CreateIndex => generate_create_index(generator, ctx),
        StmtKind::DropIndex => generate_drop_index(generator, ctx),
        StmtKind::Begin => Ok(Stmt::Begin),
        StmtKind::Commit => Ok(Stmt::Commit),
        StmtKind::Rollback => Ok(Stmt::Rollback),
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
    // Generate a unique table name
    let existing_names = generator.schema().table_names();
    let table_name = loop {
        let name = format!("tbl_{}", ctx.gen_range(10000));
        if !existing_names.contains(&name) {
            break name;
        }
    };

    // Generate columns
    let num_cols = ctx.gen_range_inclusive(2, 6);
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
        let not_null = ctx.gen_bool_with_prob(0.3);
        let unique = !not_null && ctx.gen_bool_with_prob(0.1);

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
        if_not_exists: ctx.gen_bool_with_prob(0.5),
    }))
}

/// Generate a DROP TABLE statement.
pub fn generate_drop_table<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Stmt, GenError> {
    let table = ctx
        .choose(&generator.schema().tables)
        .ok_or_else(|| GenError::schema_empty("tables"))?;

    Ok(Stmt::DropTable(DropTableStmt {
        table: table.name.clone(),
        if_exists: ctx.gen_bool_with_prob(0.5),
    }))
}

/// Generate a CREATE INDEX statement.
pub fn generate_create_index<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Stmt, GenError> {
    let table = ctx
        .choose(&generator.schema().tables)
        .ok_or_else(|| GenError::schema_empty("tables"))?
        .clone();

    // Generate unique index name
    let existing_names = generator.schema().index_names();
    let index_name = loop {
        let name = format!("idx_{}_{}", table.name, ctx.gen_range(10000));
        if !existing_names.contains(&name) {
            break name;
        }
    };

    // Select columns for the index
    let num_cols = ctx.gen_range_inclusive(1, table.columns.len().min(3));
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
        unique: ctx.gen_bool_with_prob(0.2),
        if_not_exists: ctx.gen_bool_with_prob(0.5),
    }))
}

/// Generate a DROP INDEX statement.
pub fn generate_drop_index<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Stmt, GenError> {
    // Try to use an existing index, or generate a plausible name
    let index_name = if let Some(index) = ctx.choose(&generator.schema().indexes) {
        index.name.clone()
    } else {
        format!("idx_{}", ctx.gen_range(10000))
    };

    Ok(Stmt::DropIndex(DropIndexStmt {
        name: index_name,
        if_exists: ctx.gen_bool_with_prob(0.7),
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
}
