//! SELECT statement generation.

use crate::SqlGen;
use crate::ast::{Expr, OrderByItem, OrderDirection, SelectColumn, SelectStmt};
use crate::capabilities::Capabilities;
use crate::context::Context;
use crate::error::GenError;
use crate::generate::expr::{generate_condition, generate_expr};
use crate::schema::Table;
use crate::trace::Origin;

/// Generate a SELECT statement.
pub fn generate_select<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<crate::ast::Stmt, GenError> {
    let table = ctx
        .choose(&generator.schema().tables)
        .ok_or_else(|| GenError::schema_empty("tables"))?
        .clone();

    let select = generate_select_for_table(generator, ctx, &table)?;
    Ok(crate::ast::Stmt::Select(select))
}

/// Generate a SELECT statement for a specific table.
pub fn generate_select_for_table<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
) -> Result<SelectStmt, GenError> {
    ctx.enter_scope(Origin::Select);

    // Generate columns
    let columns = generate_select_columns(generator, ctx, table)?;

    // Generate optional WHERE clause
    let where_clause = if ctx.gen_bool_with_prob(0.7) {
        Some(generate_condition(generator, ctx, table)?)
    } else {
        None
    };

    // Generate optional ORDER BY
    let order_by = if ctx.gen_bool_with_prob(0.3) {
        generate_order_by(generator, ctx, table)?
    } else {
        vec![]
    };

    // Generate optional LIMIT
    let limit = if ctx.gen_bool_with_prob(0.4) {
        Some(ctx.gen_range_inclusive(1, generator.policy().max_limit as usize) as u64)
    } else {
        None
    };

    // Generate optional OFFSET (only if LIMIT is present)
    let offset = if limit.is_some() && ctx.gen_bool_with_prob(0.2) {
        Some(ctx.gen_range_inclusive(0, 100) as u64)
    } else {
        None
    };

    // Generate alias if policy allows
    let from_alias = if generator.policy().generate_aliases && ctx.gen_bool_with_prob(0.3) {
        Some(format!("t{}", ctx.gen_range(1000)))
    } else {
        None
    };

    ctx.exit_scope();
    Ok(SelectStmt {
        columns,
        from: table.name.clone(),
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
    // Pick one column
    let col = ctx
        .choose(&table.columns)
        .ok_or_else(|| GenError::schema_empty("columns"))?;

    let columns = vec![SelectColumn {
        expr: Expr::column_ref(ctx, None, col.name.clone()),
        alias: None,
    }];

    // Maybe add WHERE
    let where_clause = if ctx.gen_bool_with_prob(0.5) {
        Some(generate_condition(generator, ctx, table)?)
    } else {
        None
    };

    // Always add LIMIT 1 for scalar subqueries
    Ok(SelectStmt {
        columns,
        from: table.name.clone(),
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
fn generate_select_columns<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
) -> Result<Vec<SelectColumn>, GenError> {
    let policy = generator.policy();

    // Decide: SELECT * or specific columns
    if ctx.gen_bool_with_prob(0.2) {
        // SELECT * - return empty vec which displays as *
        return Ok(vec![]);
    }

    let num_cols = ctx.gen_range_inclusive(1, policy.max_select_columns.min(table.columns.len()));
    let mut columns = Vec::with_capacity(num_cols);

    for i in 0..num_cols {
        let col = if ctx.gen_bool_with_prob(0.7) {
            // Column reference
            let table_col = ctx.choose(&table.columns).unwrap();
            SelectColumn {
                expr: Expr::column_ref(ctx, None, table_col.name.clone()),
                alias: if policy.generate_aliases && ctx.gen_bool_with_prob(0.2) {
                    Some(format!("col{i}"))
                } else {
                    None
                },
            }
        } else {
            // Expression
            let expr = generate_expr(generator, ctx, table, 0)?;
            SelectColumn {
                expr,
                alias: if policy.generate_aliases {
                    Some(format!("expr{i}"))
                } else {
                    None
                },
            }
        };

        columns.push(col);
    }

    Ok(columns)
}

/// Generate ORDER BY clause.
fn generate_order_by<C: Capabilities>(
    _generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
) -> Result<Vec<OrderByItem>, GenError> {
    // Note: We don't use scope guard here to avoid borrow conflicts
    // The origin tracking happens at higher levels

    let num_items = ctx.gen_range_inclusive(1, 3);
    let mut items = Vec::with_capacity(num_items);

    for _ in 0..num_items {
        let col = ctx.choose(&table.columns).unwrap();
        let direction = if ctx.gen_bool() {
            OrderDirection::Asc
        } else {
            OrderDirection::Desc
        };

        items.push(OrderByItem {
            expr: Expr::column_ref(ctx, None, col.name.clone()),
            direction,
            nulls: None,
        });
    }

    Ok(items)
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
