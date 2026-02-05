//! SELECT statement generation.

use crate::SqlGen;
use crate::ast::{
    BinOp, Expr, GroupByClause, NullsOrder, OrderByItem, OrderDirection, SelectColumn, SelectStmt,
};
use crate::capabilities::Capabilities;
use crate::context::Context;
use crate::error::GenError;
use crate::functions::AGGREGATE_FUNCTIONS;
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

    let table = ctx.choose(&generator.schema().tables).unwrap().clone();

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
        distinct: false,
        columns,
        from: None,
        from_alias: None,
        where_clause: None,
        group_by: None,
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

    // Generate optional GROUP BY (with optional HAVING)
    let group_by = if ctx.gen_bool_with_prob(select_config.group_by_probability) {
        Some(generate_group_by_clause(generator, ctx, table)?)
    } else {
        None
    };

    // Generate columns — dispatch based on GROUP BY
    let columns = if let Some(gb) = &group_by {
        generate_grouped_select_columns(generator, ctx, table, &gb.exprs)?
    } else {
        generate_select_columns(generator, ctx, table)?
    };

    // Generate optional WHERE clause
    let where_clause = if ctx.gen_bool_with_prob(select_config.where_probability) {
        Some(generate_condition(generator, ctx, table)?)
    } else {
        None
    };

    // Generate optional ORDER BY — dispatch based on GROUP BY
    let order_by = if ctx.gen_bool_with_prob(select_config.order_by_probability) {
        if let Some(gb) = &group_by {
            generate_grouped_order_by(generator, ctx, table, &gb.exprs)?
        } else {
            generate_order_by(generator, ctx, table)?
        }
    } else {
        vec![]
    };

    // Generate optional DISTINCT
    let distinct = ctx.gen_bool_with_prob(select_config.distinct_probability);

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
        distinct,
        columns,
        from: Some(table.name.clone()),
        from_alias,
        where_clause,
        group_by,
        order_by,
        limit,
        offset,
    })
}

/// Generate a simple single-column SELECT (for scalar subqueries).
///
/// Always returns exactly 1 column with LIMIT 1. Optionally includes
/// GROUP BY (with aggregate output), ORDER BY, and DISTINCT.
pub fn generate_simple_select<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
) -> Result<SelectStmt, GenError> {
    let select_config = &generator.policy().select_config;

    let use_group_by = ctx.gen_bool_with_prob(select_config.subquery_group_by_probability);

    let (columns, group_by) = if use_group_by && table.columns.len() >= 2 {
        // GROUP BY path: output a single aggregate call, group by another column
        let gb_col = ctx
            .choose(&table.columns)
            .ok_or_else(|| GenError::schema_empty("columns"))?
            .clone();
        let gb_expr = Expr::column_ref(ctx, None, gb_col.name.clone());
        let agg = generate_aggregate_call(ctx, table)?;
        let columns = vec![SelectColumn {
            expr: agg,
            alias: None,
        }];
        let group_by = Some(GroupByClause {
            exprs: vec![gb_expr],
            having: None,
        });
        (columns, group_by)
    } else {
        // Simple path: pick one column ref
        let col = ctx
            .choose(&table.columns)
            .ok_or_else(|| GenError::schema_empty("columns"))?;
        let columns = vec![SelectColumn {
            expr: Expr::column_ref(ctx, None, col.name.clone()),
            alias: None,
        }];
        (columns, None)
    };

    // Maybe add WHERE
    let where_clause = if ctx.gen_bool_with_prob(select_config.subquery_where_probability) {
        Some(generate_condition(generator, ctx, table)?)
    } else {
        None
    };

    // Maybe add ORDER BY
    let order_by = if ctx.gen_bool_with_prob(select_config.subquery_order_by_probability) {
        if let Some(gb) = &group_by {
            generate_grouped_order_by(generator, ctx, table, &gb.exprs)?
        } else {
            generate_order_by(generator, ctx, table)?
        }
    } else {
        vec![]
    };

    // Maybe add DISTINCT (only for non-grouped queries)
    let distinct =
        group_by.is_none() && ctx.gen_bool_with_prob(select_config.subquery_distinct_probability);

    // Always add LIMIT 1 for scalar subqueries
    Ok(SelectStmt {
        distinct,
        columns,
        from: Some(table.name.clone()),
        from_alias: None,
        where_clause,
        group_by,
        order_by,
        limit: Some(1),
        offset: None,
    })
}

/// Generate a GROUP BY clause with optional HAVING.
#[trace_gen(Origin::GroupBy)]
fn generate_group_by_clause<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
) -> Result<GroupByClause, GenError> {
    if table.columns.is_empty() {
        return Err(GenError::schema_empty("columns"));
    }
    let select_config = &generator.policy().select_config;

    // Pick GROUP BY columns
    let max = generator.policy().max_group_by_items;
    let cols = ctx.subsequence(&table.columns, 1..=max);
    let exprs: Vec<Expr> = cols
        .into_iter()
        .map(|col| Expr::column_ref(ctx, None, col.name.clone()))
        .collect();

    // Optionally generate HAVING
    let having = if ctx.gen_bool_with_prob(select_config.having_probability) {
        Some(generate_having(generator, ctx, table)?)
    } else {
        None
    };

    Ok(GroupByClause { exprs, having })
}

/// Generate an aggregate function call on a random column.
fn generate_aggregate_call(ctx: &mut Context, table: &Table) -> Result<Expr, GenError> {
    let func = ctx
        .choose(AGGREGATE_FUNCTIONS)
        .ok_or_else(|| GenError::schema_empty("aggregate_functions"))?;
    let col = ctx
        .choose(&table.columns)
        .ok_or_else(|| GenError::schema_empty("columns"))?;
    let arg = Expr::column_ref(ctx, None, col.name.clone());
    Ok(Expr::function_call(ctx, func.name.to_string(), vec![arg]))
}

/// Generate SELECT columns for a grouped query.
///
/// Each column is either a GROUP BY column ref or an aggregate call,
/// ensuring non-aggregated columns appear in GROUP BY.
fn generate_grouped_select_columns<C: Capabilities>(
    _generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    group_by_exprs: &[Expr],
) -> Result<Vec<SelectColumn>, GenError> {
    // Extract GROUP BY column names
    let group_by_names: Vec<&str> = group_by_exprs
        .iter()
        .filter_map(|e| match e {
            Expr::ColumnRef(cr) => Some(cr.column.as_str()),
            _ => None,
        })
        .collect();

    let num_cols = ctx.gen_range_inclusive(1, group_by_names.len() + 2);
    let mut columns = Vec::with_capacity(num_cols);
    let mut has_group_col = false;

    for _ in 0..num_cols {
        if ctx.gen_bool_with_prob(0.5) && !group_by_names.is_empty() {
            // Pick a GROUP BY column ref
            let name = *ctx.choose(&group_by_names).unwrap();
            columns.push(SelectColumn {
                expr: Expr::column_ref(ctx, None, name.to_string()),
                alias: None,
            });
            has_group_col = true;
        } else {
            // Generate an aggregate call
            columns.push(SelectColumn {
                expr: generate_aggregate_call(ctx, table)?,
                alias: None,
            });
        }
    }

    // Ensure at least one GROUP BY column is present
    if !has_group_col && !group_by_names.is_empty() {
        let name = *ctx.choose(&group_by_names).unwrap();
        columns[0] = SelectColumn {
            expr: Expr::column_ref(ctx, None, name.to_string()),
            alias: None,
        };
    }

    Ok(columns)
}

/// Generate HAVING clause: `aggregate_call comparison_op literal`.
#[trace_gen(Origin::Having)]
fn generate_having<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
) -> Result<Expr, GenError> {
    let agg = generate_aggregate_call(ctx, table)?;
    let ops = BinOp::comparison();
    let op = *ctx
        .choose(ops)
        .ok_or_else(|| GenError::schema_empty("comparison_ops"))?;
    let lit = generate_literal(ctx, DataType::Integer, generator.policy());
    let right = Expr::literal(ctx, lit);
    Ok(Expr::binary_op(ctx, agg, op, right))
}

/// Generate ORDER BY clause for a grouped query.
///
/// Only orders by columns that appear in GROUP BY.
fn generate_grouped_order_by<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    _table: &Table,
    group_by_exprs: &[Expr],
) -> Result<Vec<OrderByItem>, GenError> {
    let select_config = &generator.policy().select_config;
    let max_items = generator.policy().max_order_by_items;
    let picked = ctx.subsequence(group_by_exprs, 1..=max_items);
    let items = picked
        .into_iter()
        .map(|expr| {
            let direction = select_order_direction(ctx, &select_config.order_direction_weights);
            let nulls = select_nulls_order(ctx, &select_config.nulls_order_weights);
            OrderByItem {
                expr,
                direction,
                nulls,
            }
        })
        .collect();

    Ok(items)
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
            let cols = ctx.subsequence(&table.columns, 1..=table.columns.len());
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
        let col_w = select_config.order_by_column_weight;
        let expr_w = select_config.order_by_expr_weight;

        let expr = match ctx.weighted_index(&[col_w, expr_w]) {
            Some(1) => {
                let e = generate_expr(generator, ctx, table, 0)?;
                // Avoid bare literals — SQLite interprets integer literals in
                // ORDER BY as column-ordinal positions (e.g. ORDER BY 2).
                if matches!(e, Expr::Literal(_)) {
                    let col = ctx.choose(&table.columns).unwrap();
                    Expr::column_ref(ctx, None, col.name.clone())
                } else {
                    e
                }
            }
            _ => {
                let col = ctx.choose(&table.columns).unwrap();
                Expr::column_ref(ctx, None, col.name.clone())
            }
        };

        let direction = select_order_direction(ctx, &select_config.order_direction_weights);
        let nulls = select_nulls_order(ctx, &select_config.nulls_order_weights);

        items.push(OrderByItem {
            expr,
            direction,
            nulls,
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

/// Select a NULLS ordering based on weights.
fn select_nulls_order(
    ctx: &mut Context,
    weights: &crate::policy::NullsOrderWeights,
) -> Option<NullsOrder> {
    match ctx.weighted_index(&[weights.first, weights.last, weights.unspecified]) {
        Some(0) => Some(NullsOrder::First),
        Some(1) => Some(NullsOrder::Last),
        _ => None,
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

    #[test]
    fn test_generate_group_by_clause() {
        let generator = test_generator();
        let table = &generator.schema().tables[0];
        let mut ctx = Context::new_with_seed(42);

        let clause = generate_group_by_clause(&generator, &mut ctx, table).unwrap();
        assert!(!clause.exprs.is_empty());
        assert!(clause.exprs.len() <= generator.policy().max_group_by_items);
    }

    #[test]
    fn test_generate_select_with_group_by() {
        let policy = Policy::default().with_select_config(crate::policy::SelectConfig {
            group_by_probability: 1.0,
            ..Default::default()
        });
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
        let generator: SqlGen<Full> = SqlGen::new(schema, policy);
        let table = &generator.schema().tables[0];
        let mut ctx = Context::new_with_seed(42);

        let select = generate_select_for_table(&generator, &mut ctx, table).unwrap();
        let sql = select.to_string();
        assert!(
            sql.contains("GROUP BY"),
            "SQL should contain GROUP BY: {sql}"
        );
    }

    #[test]
    fn test_generate_select_with_distinct() {
        let policy = Policy::default().with_select_config(crate::policy::SelectConfig {
            distinct_probability: 1.0,
            ..Default::default()
        });
        let schema = SchemaBuilder::new()
            .table(Table::new(
                "users",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("name", DataType::Text),
                ],
            ))
            .build();
        let generator: SqlGen<Full> = SqlGen::new(schema, policy);
        let table = &generator.schema().tables[0];
        let mut ctx = Context::new_with_seed(42);

        let select = generate_select_for_table(&generator, &mut ctx, table).unwrap();
        let sql = select.to_string();
        assert!(
            sql.contains("DISTINCT"),
            "SQL should contain DISTINCT: {sql}"
        );
    }

    #[test]
    fn test_expression_order_by() {
        let policy = Policy::default().with_select_config(crate::policy::SelectConfig {
            order_by_probability: 1.0,
            order_by_column_weight: 0,
            order_by_expr_weight: 100,
            group_by_probability: 0.0,
            ..Default::default()
        });
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
        let generator: SqlGen<Full> = SqlGen::new(schema, policy);
        let table = &generator.schema().tables[0];

        let mut found_expr_order_by = false;
        for seed in 0..50 {
            let mut ctx = Context::new_with_seed(seed);
            if let Ok(select) = generate_select_for_table(&generator, &mut ctx, table) {
                let sql = select.to_string();
                if sql.contains("ORDER BY") {
                    // Expression ORDER BY will contain operators or function calls,
                    // not just bare column names
                    let order_part = sql.split("ORDER BY").nth(1).unwrap_or("");
                    if order_part.contains('(')
                        || order_part.contains('+')
                        || order_part.contains('-')
                        || order_part.contains('*')
                        || order_part.contains("CASE")
                        || order_part.contains("CAST")
                    {
                        found_expr_order_by = true;
                        break;
                    }
                }
            }
        }
        assert!(
            found_expr_order_by,
            "Should generate expression-based ORDER BY items"
        );
    }

    #[test]
    fn test_nulls_ordering() {
        let policy = Policy::default().with_select_config(crate::policy::SelectConfig {
            order_by_probability: 1.0,
            group_by_probability: 0.0,
            nulls_order_weights: crate::policy::NullsOrderWeights {
                first: 50,
                last: 50,
                unspecified: 0,
            },
            ..Default::default()
        });
        let schema = SchemaBuilder::new()
            .table(Table::new(
                "users",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("name", DataType::Text),
                ],
            ))
            .build();
        let generator: SqlGen<Full> = SqlGen::new(schema, policy);
        let table = &generator.schema().tables[0];

        let mut found_nulls = false;
        for seed in 0..30 {
            let mut ctx = Context::new_with_seed(seed);
            if let Ok(select) = generate_select_for_table(&generator, &mut ctx, table) {
                let sql = select.to_string();
                if sql.contains("NULLS FIRST") || sql.contains("NULLS LAST") {
                    found_nulls = true;
                    break;
                }
            }
        }
        assert!(found_nulls, "Should generate NULLS FIRST or NULLS LAST");
    }

    #[test]
    fn test_rich_subquery_with_group_by() {
        let policy = Policy::default().with_select_config(crate::policy::SelectConfig {
            subquery_group_by_probability: 1.0,
            subquery_where_probability: 0.0,
            subquery_order_by_probability: 0.0,
            ..Default::default()
        });
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
        let generator: SqlGen<Full> = SqlGen::new(schema, policy);
        let table = &generator.schema().tables[0];

        let mut found_grouped = false;
        for seed in 0..30 {
            let mut ctx = Context::new_with_seed(seed);
            if let Ok(select) = generate_simple_select(&generator, &mut ctx, table) {
                let sql = select.to_string();
                assert_eq!(select.columns.len(), 1, "Should have exactly 1 column");
                assert_eq!(select.limit, Some(1), "Should have LIMIT 1");
                if sql.contains("GROUP BY") {
                    found_grouped = true;
                    // Verify the output column contains an aggregate
                    let col_str = select.columns[0].expr.to_string();
                    assert!(
                        col_str.contains('('),
                        "Grouped subquery column should be aggregate: {col_str}"
                    );
                    break;
                }
            }
        }
        assert!(
            found_grouped,
            "Should generate subqueries with GROUP BY"
        );
    }

    #[test]
    fn test_having_only_with_group_by() {
        let policy = Policy::default().with_select_config(crate::policy::SelectConfig {
            group_by_probability: 0.0,
            having_probability: 1.0,
            ..Default::default()
        });
        let schema = SchemaBuilder::new()
            .table(Table::new(
                "users",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("name", DataType::Text),
                ],
            ))
            .build();
        let generator: SqlGen<Full> = SqlGen::new(schema, policy);
        let table = &generator.schema().tables[0];
        let mut ctx = Context::new_with_seed(42);

        let select = generate_select_for_table(&generator, &mut ctx, table).unwrap();
        let sql = select.to_string();
        assert!(
            !sql.contains("HAVING"),
            "SQL should not contain HAVING without GROUP BY: {sql}"
        );
    }
}
