//! SELECT statement generation.

use crate::SqlGen;
use crate::ast::{Expr, OrderByItem, OrderDirection, SelectColumn, SelectStmt};
use crate::capabilities::Capabilities;
use crate::context::Context;
use crate::error::GenError;
use crate::generate::expr::{generate_condition, generate_expr};
use crate::generate::literal::generate_literal;
use crate::schema::{DataType, Table};
use crate::trace::Origin;
use sql_gen_macros::trace_gen;

/// Generate a SELECT statement.
pub fn generate_select<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<crate::ast::Stmt, GenError> {
    if generator.schema().tables.is_empty() {
        let select = generate_tableless_select(generator, ctx)?;
        return Ok(crate::ast::Stmt::Select(select));
    }

    let table = ctx
        .choose(&generator.schema().tables)
        .unwrap()
        .clone();

    let select = generate_select_for_table(generator, ctx, &table)?;
    Ok(crate::ast::Stmt::Select(select))
}

/// Generate a table-less SELECT statement (e.g. `SELECT 1+2, abs(-5)`).
///
/// Used when no tables exist in the schema. Only generates literal and
/// function-call expressions (no column refs, no WHERE/ORDER BY).
#[trace_gen(Origin::Select)]
pub fn generate_tableless_select<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<SelectStmt, GenError> {
    let num_cols = ctx.gen_range_inclusive(1, 3);
    let mut columns = Vec::with_capacity(num_cols);
    let types = [DataType::Integer, DataType::Real, DataType::Text];

    for i in 0..num_cols {
        let data_type = *ctx.choose(&types).unwrap();
        let lit = generate_literal(ctx, data_type, generator.policy());
        let expr = Expr::literal(ctx, lit);
        columns.push(SelectColumn {
            expr,
            alias: Some(format!("expr{i}")),
        });
    }

    Ok(SelectStmt {
        columns,
        from: None,
        from_alias: None,
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
    })
}

/// Generate a SELECT statement for a specific table.
#[trace_gen(Origin::Select)]
pub fn generate_select_for_table<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
) -> Result<SelectStmt, GenError> {
    let select_config = &generator.policy().select_config;
    let ident_config = &generator.policy().identifier_config;

    // Generate columns
    let columns = generate_select_columns(generator, ctx, table)?;

    // Generate optional WHERE clause
    let where_clause = if ctx.gen_bool_with_prob(select_config.where_probability) {
        Some(generate_condition(generator, ctx, table)?)
    } else {
        None
    };

    // Generate optional ORDER BY
    let order_by = if ctx.gen_bool_with_prob(select_config.order_by_probability) {
        generate_order_by(generator, ctx, table)?
    } else {
        vec![]
    };

    // Generate optional LIMIT
    let limit = if ctx.gen_bool_with_prob(select_config.limit_probability) {
        Some(ctx.gen_range_inclusive(1, generator.policy().max_limit as usize) as u64)
    } else {
        None
    };

    // Generate optional OFFSET (only if LIMIT is present)
    let offset = if limit.is_some() && ctx.gen_bool_with_prob(select_config.offset_probability) {
        Some(ctx.gen_range_inclusive(0, select_config.max_offset as usize) as u64)
    } else {
        None
    };

    // Generate alias if policy allows
    let from_alias = if ident_config.generate_table_aliases
        && ctx.gen_bool_with_prob(select_config.table_alias_probability)
    {
        Some(format!(
            "{}{}",
            ident_config.table_alias_prefix,
            ctx.gen_range(ident_config.alias_suffix_range)
        ))
    } else {
        None
    };

    Ok(SelectStmt {
        columns,
        from: Some(table.name.clone()),
        from_alias,
        where_clause,
        group_by: vec![],
        having: None,
        order_by,
        limit,
        offset,
    })
}

/// Generate a simple single-column SELECT (for scalar subqueries).
pub fn generate_simple_select<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
) -> Result<SelectStmt, GenError> {
    let select_config = &generator.policy().select_config;

    // Pick one column
    let col = ctx
        .choose(&table.columns)
        .ok_or_else(|| GenError::schema_empty("columns"))?;

    let columns = vec![SelectColumn {
        expr: Expr::column_ref(ctx, None, col.name.clone()),
        alias: None,
    }];

    // Maybe add WHERE
    let where_clause = if ctx.gen_bool_with_prob(select_config.subquery_where_probability) {
        Some(generate_condition(generator, ctx, table)?)
    } else {
        None
    };

    // Always add LIMIT 1 for scalar subqueries
    Ok(SelectStmt {
        columns,
        from: Some(table.name.clone()),
        from_alias: None,
        where_clause,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: Some(1),
        offset: None,
    })
}

/// Generate SELECT column list.
///
/// Uses a weighted three-way strategy:
/// 1. SELECT * (empty vec)
/// 2. Column list (subsequence of table columns)
/// 3. Expression list (generated expressions)
fn generate_select_columns<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
) -> Result<Vec<SelectColumn>, GenError> {
    let select_config = &generator.policy().select_config;
    let ident_config = &generator.policy().identifier_config;

    let star_weight = if select_config.min_columns == 0 {
        select_config.select_star_weight
    } else {
        0
    };
    let weights = [
        star_weight,
        select_config.column_list_weight,
        select_config.expression_list_weight,
    ];

    let strategy = ctx.weighted_index(&weights).unwrap_or(1);

    match strategy {
        // SELECT *
        0 => Ok(vec![]),
        // Column list: random subsequence of table columns
        1 => {
            let cols = ctx.subsequence(&table.columns);
            Ok(cols
                .into_iter()
                .map(|col| SelectColumn {
                    expr: Expr::column_ref(ctx, None, col.name.clone()),
                    alias: None,
                })
                .collect())
        }
        // Expression list
        _ => {
            let range = &select_config.expression_count_range;
            let num_cols = ctx.gen_range_inclusive((*range.start()).max(1), *range.end());
            let mut columns = Vec::with_capacity(num_cols);

            for i in 0..num_cols {
                let expr = generate_expr(generator, ctx, table, 0)?;
                columns.push(SelectColumn {
                    expr,
                    alias: if ident_config.generate_column_aliases {
                        Some(format!("{}{i}", ident_config.expr_alias_prefix))
                    } else {
                        None
                    },
                });
            }
            Ok(columns)
        }
    }
}

/// Generate ORDER BY clause.
fn generate_order_by<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
) -> Result<Vec<OrderByItem>, GenError> {
    let select_config = &generator.policy().select_config;
    let max_items = generator.policy().max_order_by_items;

    let num_items = ctx.gen_range_inclusive(1, max_items.min(table.columns.len()));
    let mut items = Vec::with_capacity(num_items);

    for _ in 0..num_items {
        let col = ctx.choose(&table.columns).unwrap();
        let direction = select_order_direction(ctx, &select_config.order_direction_weights);

        items.push(OrderByItem {
            expr: Expr::column_ref(ctx, None, col.name.clone()),
            direction,
            nulls: None,
        });
    }

    Ok(items)
}

/// Select an order direction based on weights.
fn select_order_direction(
    ctx: &mut Context,
    weights: &crate::policy::OrderDirectionWeights,
) -> OrderDirection {
    let candidates = [
        (OrderDirection::Asc, weights.asc),
        (OrderDirection::Desc, weights.desc),
    ];

    match ctx.weighted_index(&[weights.asc, weights.desc]) {
        Some(idx) => candidates[idx].0,
        None => OrderDirection::Asc, // Default if all weights are zero
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
    fn test_generate_select() {
        let generator = test_generator();
        let mut ctx = Context::new_with_seed(42);

        let stmt = generate_select(&generator, &mut ctx);
        assert!(stmt.is_ok());

        let sql = stmt.unwrap().to_string();
        assert!(sql.starts_with("SELECT"));
        assert!(sql.contains("FROM users"));
    }

    #[test]
    fn test_generate_simple_select() {
        let generator = test_generator();
        let table = &generator.schema().tables[0];
        let mut ctx = Context::new_with_seed(42);

        let select = generate_simple_select(&generator, &mut ctx, table);
        assert!(select.is_ok());

        let select = select.unwrap();
        assert_eq!(select.columns.len(), 1);
        assert_eq!(select.limit, Some(1));
    }
}
