//! SELECT statement generation.

use crate::SqlGen;
use crate::ast::{
    BinOp, Expr, FromClause, GroupByClause, JoinClause, JoinConstraint, JoinType, NullsOrder,
    OrderByItem, OrderDirection, SelectColumn, SelectStmt,
};
use crate::capabilities::Capabilities;
use crate::context::Context;
use crate::error::GenError;
use crate::functions::AGGREGATE_FUNCTIONS;
use crate::generate::expr::generate_condition;
use crate::generate::expr::generate_expr;
use crate::generate::literal::generate_literal;
use crate::schema::DataType;
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

    let select = generate_select_impl(generator, ctx, SelectMode::Full)?;
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
        joins: vec![],
        where_clause: None,
        group_by: None,
        order_by: vec![],
        limit: None,
        offset: None,
    })
}

/// Controls whether `generate_select_impl` produces a full SELECT or a
/// single-column scalar subquery.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SelectMode {
    /// Normal SELECT — arbitrary columns, clauses governed by `SelectConfig`.
    Full,
    /// Scalar subquery — exactly 1 output column, LIMIT 1, no alias/offset.
    /// Probabilities come from the `subquery_*` fields in `SelectConfig`.
    Scalar,
}

/// Generate a simple single-column SELECT (for scalar subqueries).
///
/// Always returns exactly 1 column with LIMIT 1.
pub fn generate_simple_select<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<SelectStmt, GenError> {
    generate_select_impl(generator, ctx, SelectMode::Scalar)
}

/// Shared implementation for both full and scalar SELECT generation.
///
/// Chooses a table, optionally generates a CTE and alias, then enters
/// a table scope with the primary table before delegating to the inner
/// implementation.
fn generate_select_impl<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    mode: SelectMode,
) -> Result<SelectStmt, GenError> {
    let table = ctx
        .choose(&generator.schema().tables)
        .ok_or_else(|| GenError::schema_empty("tables"))?
        .clone();
    let select_config = &generator.policy().select_config;
    let ident_config = &generator.policy().identifier_config;

    // --- CTE (not yet implemented) ---
    if ctx.gen_bool_with_prob(select_config.cte_probability) {
        generate_cte(generator, ctx)?;
    }

    // --- Alias for primary table ---
    let from_alias = if mode == SelectMode::Full
        && ident_config.generate_table_aliases
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

    ctx.with_table_scope([(table, from_alias)], |ctx| {
        generate_select_impl_inner(generator, ctx, mode)
    })
}

/// Inner implementation, runs inside a table scope frame.
///
/// The primary table is already in scope when this is called.
fn generate_select_impl_inner<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    mode: SelectMode,
) -> Result<SelectStmt, GenError> {
    let select_config = &generator.policy().select_config;

    // --- JOINs ---
    let join_config = &select_config.join_config;
    let joins = if mode == SelectMode::Full
        && ctx.gen_bool_with_prob(join_config.join_probability)
        && !generator.schema().tables.is_empty()
    {
        generate_join_clauses(generator, ctx)?
    } else {
        vec![]
    };

    let gb_prob = match mode {
        SelectMode::Full => select_config.group_by_probability,
        SelectMode::Scalar => select_config.subquery_group_by_probability,
    };
    let where_prob = match mode {
        SelectMode::Full => select_config.where_probability,
        SelectMode::Scalar => select_config.subquery_where_probability,
    };
    let order_by_prob = match mode {
        SelectMode::Full => select_config.order_by_probability,
        SelectMode::Scalar => select_config.subquery_order_by_probability,
    };

    // --- GROUP BY ---
    let group_by = if ctx.gen_bool_with_prob(gb_prob) {
        match mode {
            SelectMode::Full => Some(generate_group_by_clause(generator, ctx)?),
            // Scalar: single GROUP BY column, no HAVING
            SelectMode::Scalar if ctx.tables_in_scope()[0].table.columns.len() >= 2 => {
                let expr = pick_scoped_column_ref(ctx)?;
                Some(GroupByClause {
                    exprs: vec![expr],
                    having: None,
                })
            }
            _ => None,
        }
    } else {
        None
    };

    // --- Columns ---
    let mut columns = match mode {
        SelectMode::Full => {
            if let Some(gb) = &group_by {
                generate_grouped_select_columns(generator, ctx, &gb.exprs)?
            } else {
                generate_select_columns(generator, ctx)?
            }
        }
        SelectMode::Scalar => {
            if group_by.is_some() {
                // Aggregate output column
                vec![SelectColumn {
                    expr: generate_aggregate_call(ctx)?,
                    alias: None,
                }]
            } else {
                vec![SelectColumn {
                    expr: pick_scoped_column_ref(ctx)?,
                    alias: None,
                }]
            }
        }
    };

    // When there is no GROUP BY and `restrict_mixed_aggregates` is enabled,
    // the result column list must not mix aggregate and non-aggregate
    // expressions (e.g. `SELECT COUNT(a), b FROM t` is invalid).
    // If a mix is detected, replace all aggregate columns with column refs.
    if group_by.is_none() && select_config.restrict_mixed_aggregates && !columns.is_empty() {
        let has_agg = columns.iter().any(|c| c.expr.contains_aggregate());
        let has_non_agg = columns.iter().any(|c| !c.expr.contains_aggregate());
        if has_agg && has_non_agg {
            for col in &mut columns {
                if col.expr.contains_aggregate() {
                    col.expr = pick_scoped_column_ref(ctx)?;
                }
            }
        }
    }

    // --- WHERE ---
    let where_clause = if ctx.gen_bool_with_prob(where_prob) {
        Some(generate_condition(generator, ctx)?)
    } else {
        None
    };

    // --- ORDER BY ---
    let order_by = if ctx.gen_bool_with_prob(order_by_prob) {
        if let Some(gb) = &group_by {
            generate_grouped_order_by(generator, ctx, &gb.exprs)?
        } else {
            generate_order_by(generator, ctx)?
        }
    } else {
        vec![]
    };

    // --- DISTINCT ---
    let distinct = match mode {
        SelectMode::Full => ctx.gen_bool_with_prob(select_config.distinct_probability),
        // Scalar: only when not grouped
        SelectMode::Scalar => {
            group_by.is_none()
                && ctx.gen_bool_with_prob(select_config.subquery_distinct_probability)
        }
    };

    // --- LIMIT / OFFSET ---
    let (limit, offset) = match mode {
        SelectMode::Full => generate_limit_offset(generator, ctx),
        SelectMode::Scalar => (Some(1), None),
    };

    let from = {
        let primary_name = ctx.tables_in_scope()[0].table.name.clone();
        let primary_qualifier = ctx.tables_in_scope()[0].qualifier.clone();
        let from_alias = if primary_qualifier != primary_name {
            Some(primary_qualifier)
        } else {
            None
        };
        ctx.scope(Origin::From, |_ctx| {
            Some(FromClause {
                table: primary_name,
                alias: from_alias,
            })
        })
    };

    // --- Derived table (not yet implemented) ---
    if ctx.gen_bool_with_prob(select_config.derived_table_probability) {
        let _ = generate_derived_table(generator, ctx);
    }

    // --- Compound (not yet implemented) ---
    if ctx.gen_bool_with_prob(select_config.compound_probability) {
        let _ = generate_compound_union(generator, ctx);
    }

    Ok(SelectStmt {
        distinct,
        columns,
        from,
        joins,
        where_clause,
        group_by,
        order_by,
        limit,
        offset,
    })
}

/// Generate a GROUP BY clause with optional HAVING.
#[trace_gen(Origin::GroupBy)]
fn generate_group_by_clause<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<GroupByClause, GenError> {
    let select_config = &generator.policy().select_config;
    let multi = ctx.has_multiple_tables();

    // Collect (qualifier, col_name) pairs from scoped tables
    let qualified_cols: Vec<(Option<String>, String)> = if multi {
        ctx.tables_in_scope()
            .iter()
            .flat_map(|st| {
                let q = st.qualifier.clone();
                st.table
                    .columns
                    .iter()
                    .map(move |c| (Some(q.clone()), c.name.clone()))
            })
            .collect()
    } else {
        ctx.tables_in_scope()
            .iter()
            .flat_map(|st| st.table.columns.iter().map(|c| (None, c.name.clone())))
            .collect()
    };

    if qualified_cols.is_empty() {
        return Err(GenError::schema_empty("columns"));
    }

    // Pick GROUP BY columns
    let max = generator.policy().max_group_by_items;
    let picked = ctx.subsequence(&qualified_cols, 1..=max);
    let exprs: Vec<Expr> = picked
        .into_iter()
        .map(|(q, name)| Expr::column_ref(ctx, q, name))
        .collect();

    // Optionally generate HAVING
    let having = if ctx.gen_bool_with_prob(select_config.having_probability) {
        Some(generate_having(generator, ctx)?)
    } else {
        None
    };

    Ok(GroupByClause { exprs, having })
}

/// Generate an aggregate function call on a random column.
fn generate_aggregate_call(ctx: &mut Context) -> Result<Expr, GenError> {
    let func = ctx
        .choose(AGGREGATE_FUNCTIONS)
        .ok_or_else(|| GenError::schema_empty("aggregate_functions"))?;

    // Pick a column from scoped tables, with qualifier when multi-table
    let multi = ctx.has_multiple_tables();
    let qualified_cols: Vec<(Option<String>, String)> = ctx
        .tables_in_scope()
        .iter()
        .flat_map(|st| {
            let q = st.qualifier.clone();
            let is_multi = multi;
            st.table.columns.iter().map(move |c| {
                let qualifier = if is_multi { Some(q.clone()) } else { None };
                (qualifier, c.name.clone())
            })
        })
        .collect();

    if qualified_cols.is_empty() {
        return Err(GenError::schema_empty("columns"));
    }
    let (qualifier, col_name) = ctx.choose(&qualified_cols).unwrap().clone();

    let arg = Expr::column_ref(ctx, qualifier, col_name);
    Ok(Expr::function_call(ctx, func.name.to_string(), vec![arg]))
}

/// Generate SELECT columns for a grouped query.
///
/// Each column is either a GROUP BY column ref or an aggregate call,
/// ensuring non-aggregated columns appear in GROUP BY.
fn generate_grouped_select_columns<C: Capabilities>(
    _generator: &SqlGen<C>,
    ctx: &mut Context,
    group_by_exprs: &[Expr],
) -> Result<Vec<SelectColumn>, GenError> {
    // Extract GROUP BY column refs (qualifier, name) so we reproduce them exactly
    let group_by_refs: Vec<(Option<String>, String)> = group_by_exprs
        .iter()
        .filter_map(|e| match e {
            Expr::ColumnRef(cr) => Some((cr.table.clone(), cr.column.clone())),
            _ => None,
        })
        .collect();

    let num_cols = ctx.gen_range_inclusive(1, group_by_refs.len() + 2);
    let mut columns = Vec::with_capacity(num_cols);
    let mut has_group_col = false;

    for _ in 0..num_cols {
        if ctx.gen_bool_with_prob(0.5) && !group_by_refs.is_empty() {
            // Pick a GROUP BY column ref (preserving qualifier)
            let (q, name) = ctx.choose(&group_by_refs).unwrap().clone();
            columns.push(SelectColumn {
                expr: Expr::column_ref(ctx, q, name),
                alias: None,
            });
            has_group_col = true;
        } else {
            // Generate an aggregate call
            columns.push(SelectColumn {
                expr: generate_aggregate_call(ctx)?,
                alias: None,
            });
        }
    }

    // Ensure at least one GROUP BY column is present
    if !has_group_col && !group_by_refs.is_empty() {
        let (q, name) = ctx.choose(&group_by_refs).unwrap().clone();
        columns[0] = SelectColumn {
            expr: Expr::column_ref(ctx, q, name),
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
) -> Result<Expr, GenError> {
    let agg = generate_aggregate_call(ctx)?;
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
#[trace_gen(Origin::OrderBy)]
fn generate_grouped_order_by<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
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
    let multi = ctx.has_multiple_tables();

    match strategy {
        // SELECT *
        0 => Ok(vec![]),
        // Column list: random subsequence of table columns (with qualifiers if multi-table)
        1 => {
            let qualified_cols: Vec<(Option<String>, String)> = if multi {
                ctx.tables_in_scope()
                    .iter()
                    .flat_map(|st| {
                        let q = st.qualifier.clone();
                        st.table
                            .columns
                            .iter()
                            .map(move |c| (Some(q.clone()), c.name.clone()))
                    })
                    .collect()
            } else {
                ctx.tables_in_scope()
                    .iter()
                    .flat_map(|st| st.table.columns.iter().map(|c| (None, c.name.clone())))
                    .collect()
            };
            let max_cols = qualified_cols.len().max(1);
            let picked = ctx.subsequence(&qualified_cols, 1..=max_cols);
            Ok(picked
                .into_iter()
                .map(|(q, name)| SelectColumn {
                    expr: Expr::column_ref(ctx, q, name),
                    alias: None,
                })
                .collect())
        }
        // Expression list
        _ => {
            let range = &select_config.expression_count_range;
            let num_cols = ctx.gen_range_inclusive((*range.start()).max(1), *range.end());
            let mut columns = Vec::with_capacity(num_cols);
            let restrict = select_config.restrict_mixed_aggregates;

            for i in 0..num_cols {
                let expr = generate_expr(generator, ctx, 0)?;
                // When restricting mixed aggregates and there is no GROUP BY,
                // an expression that mixes aggregate calls with bare column
                // refs (e.g. `COUNT(a) + b`) is invalid SQL.  Replace it with
                // a plain column reference.
                let expr = if restrict && expr.contains_aggregate() && expr.contains_column_ref() {
                    pick_scoped_column_ref(ctx)?
                } else {
                    expr
                };
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

/// Pick a random column ref from scoped tables (qualified when multi-table).
///
/// Returns an error if the scope is empty — this indicates a generation bug.
fn pick_scoped_column_ref(ctx: &mut Context) -> Result<Expr, GenError> {
    let multi = ctx.has_multiple_tables();
    if multi {
        let qualified_cols: Vec<(String, String)> = ctx
            .tables_in_scope()
            .iter()
            .flat_map(|st| {
                let q = st.qualifier.clone();
                st.table
                    .columns
                    .iter()
                    .map(move |c| (q.clone(), c.name.clone()))
            })
            .collect();
        if !qualified_cols.is_empty() {
            let (q, name) = ctx.choose(&qualified_cols).unwrap().clone();
            return Ok(Expr::column_ref(ctx, Some(q), name));
        }
    }
    // Single table from scope
    let col_names: Vec<String> = ctx
        .tables_in_scope()
        .iter()
        .flat_map(|st| st.table.columns.iter().map(|c| c.name.clone()))
        .collect();
    if col_names.is_empty() {
        return Err(GenError::exhausted(
            "column_ref",
            "no tables in scope (generation bug)",
        ));
    }
    let name = ctx.choose(&col_names).unwrap().clone();
    Ok(Expr::column_ref(ctx, None, name))
}

/// Generate ORDER BY clause.
#[trace_gen(Origin::OrderBy)]
fn generate_order_by<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Vec<OrderByItem>, GenError> {
    let select_config = &generator.policy().select_config;
    let max_items = generator.policy().max_order_by_items;

    // Count total columns across scoped tables
    let total_cols: usize = ctx
        .tables_in_scope()
        .iter()
        .map(|st| st.table.columns.len())
        .sum();

    let num_items = ctx.gen_range_inclusive(1, max_items.min(total_cols.max(1)));
    let mut items = Vec::with_capacity(num_items);

    for _ in 0..num_items {
        let col_w = select_config.order_by_column_weight;
        let expr_w = select_config.order_by_expr_weight;

        let expr = match ctx.weighted_index(&[col_w, expr_w]) {
            Some(1) => {
                let e = generate_expr(generator, ctx, 0)?;
                // Avoid bare literals — SQLite interprets integer literals in
                // ORDER BY as column-ordinal positions (e.g. ORDER BY 2).
                // Also catch unary wrappers like -478008 or +3.
                if looks_like_literal(&e) {
                    pick_scoped_column_ref(ctx)?
                } else {
                    e
                }
            }
            _ => pick_scoped_column_ref(ctx)?,
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

/// Generate LIMIT / OFFSET values.
#[trace_gen(Origin::Limit)]
fn generate_limit_offset<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> (Option<u64>, Option<u64>) {
    let select_config = &generator.policy().select_config;
    let limit = if ctx.gen_bool_with_prob(select_config.limit_probability) {
        Some(ctx.gen_range_inclusive(1, generator.policy().max_limit as usize) as u64)
    } else {
        None
    };
    let offset = if limit.is_some() && ctx.gen_bool_with_prob(select_config.offset_probability) {
        Some(ctx.gen_range_inclusive(0, select_config.max_offset as usize) as u64)
    } else {
        None
    };
    (limit, offset)
}

/// Check whether an expression is a bare literal or a unary op wrapping one
/// (e.g. `-42`, `+3`). SQLite interprets such values as column-ordinal
/// positions in ORDER BY, so we must avoid generating them there.
fn looks_like_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) => true,
        Expr::UnaryOp(u) => looks_like_literal(&u.operand),
        Expr::Parenthesized(inner) => looks_like_literal(inner),
        _ => false,
    }
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

// ---------------------------------------------------------------------------
// Stub generation functions for SELECT-related features (not yet implemented).
// These appear as "not hit" in coverage reports, making gaps visible.
// ---------------------------------------------------------------------------

/// Generate JOIN clauses for a SELECT statement.
///
/// Picks a random number of joins (1..=max_joins), selects join types by weight,
/// and generates ON conditions for INNER/LEFT joins. Each joined table is pushed
/// into the current scope before generating its ON condition.
fn generate_join_clauses<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Vec<JoinClause>, GenError> {
    let join_config = &generator.policy().select_config.join_config;
    let max_tables = generator.policy().max_tables;
    let schema_tables = &generator.schema().tables;

    if schema_tables.is_empty() {
        return Ok(vec![]);
    }

    // Clone the primary table from scope
    let primary_table = ctx.tables_in_scope()[0].table.clone();

    let max_joins = join_config.max_joins.min(max_tables.saturating_sub(1));
    if max_joins == 0 {
        return Ok(vec![]);
    }

    let num_joins = ctx.gen_range_inclusive(1, max_joins);
    let mut joins = Vec::with_capacity(num_joins);

    let type_weights = &join_config.join_type_weights;

    for _ in 0..num_joins {
        // Pick join type
        let weights = [
            type_weights.inner,
            type_weights.left,
            type_weights.cross,
            type_weights.natural,
        ];
        let join_type = match ctx.weighted_index(&weights) {
            Some(0) => JoinType::Inner,
            Some(1) => JoinType::Left,
            Some(2) => JoinType::Cross,
            Some(3) => JoinType::Natural,
            _ => JoinType::Inner,
        };

        // Pick table (with self-join probability)
        let is_self_join = ctx.gen_bool_with_prob(join_config.self_join_probability);
        let joined_table = if is_self_join {
            primary_table.clone()
        } else {
            ctx.choose(schema_tables).unwrap().clone()
        };

        // Determine alias. Self-joins require aliases to avoid ambiguity.
        let needs_alias = is_self_join
            || joined_table.name == primary_table.name
            || ctx.gen_bool_with_prob(generator.policy().select_config.table_alias_probability);
        let alias = if needs_alias {
            Some(ctx.next_table_alias())
        } else {
            None
        };

        // Push joined table into scope so ON condition and subsequent clauses see it
        ctx.push_table(joined_table.clone(), alias.clone());

        // Generate ON constraint for INNER/LEFT joins
        let constraint = match join_type {
            JoinType::Inner | JoinType::Left => {
                let on_expr = generate_join_on_condition(generator, ctx)?;
                Some(JoinConstraint::On(on_expr))
            }
            JoinType::Natural => {
                // NATURAL JOIN: only valid if tables share column names.
                // If they don't, fall back to INNER JOIN with ON condition.
                let shared = primary_table
                    .columns
                    .iter()
                    .any(|c| joined_table.columns.iter().any(|jc| jc.name == c.name));
                if shared {
                    None
                } else {
                    // Fall back to INNER with ON
                    let on_expr = generate_join_on_condition(generator, ctx)?;
                    joins.push(JoinClause {
                        join_type: JoinType::Inner,
                        table: joined_table.name.clone(),
                        alias,
                        constraint: Some(JoinConstraint::On(on_expr)),
                    });
                    continue;
                }
            }
            JoinType::Cross => None,
        };

        // Record the join origin for coverage
        let origin = match join_type {
            JoinType::Inner => Origin::Join,
            JoinType::Left => Origin::LeftJoin,
            JoinType::Cross => Origin::CrossJoin,
            JoinType::Natural => Origin::NaturalJoin,
        };
        ctx.scope(origin, |_| {});

        joins.push(JoinClause {
            join_type,
            table: joined_table.name.clone(),
            alias,
            constraint,
        });
    }

    Ok(joins)
}

/// Generate the ON condition for a JOIN.
///
/// With `equi_join_probability`, generates `left_qualifier.col = right_qualifier.col`
/// using compatible columns. Otherwise generates a general boolean expression.
/// Both tables are read from the current scope: the primary table is `[0]` and the
/// just-pushed joined table is the last entry.
fn generate_join_on_condition<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Expr, GenError> {
    let join_config = &generator.policy().select_config.join_config;

    // Read tables from scope
    let left_qualifier = ctx.tables_in_scope()[0].qualifier.clone();
    let left_table = ctx.tables_in_scope()[0].table.clone();
    let right_scope = ctx.tables_in_scope().last().unwrap();
    let right_qualifier = right_scope.qualifier.clone();
    let right_table = right_scope.table.clone();

    if ctx.gen_bool_with_prob(join_config.equi_join_probability) {
        // Try to find compatible columns (same data type) between the two tables
        let left_cols: Vec<_> = left_table
            .filterable_columns()
            .map(|c| (c.name.clone(), c.data_type))
            .collect();
        let right_cols: Vec<_> = right_table
            .filterable_columns()
            .map(|c| (c.name.clone(), c.data_type))
            .collect();

        if !left_cols.is_empty() && !right_cols.is_empty() {
            let (left_name, left_dt) = ctx.choose(&left_cols).unwrap().clone();
            // Try to find a right column with matching type
            let compatible: Vec<_> = right_cols.iter().filter(|(_, dt)| *dt == left_dt).collect();
            let right_name = if compatible.is_empty() {
                ctx.choose(&right_cols).unwrap().0.clone()
            } else {
                ctx.choose(&compatible).unwrap().0.clone()
            };

            let left_expr = Expr::column_ref(ctx, Some(left_qualifier), left_name);
            let right_expr = Expr::column_ref(ctx, Some(right_qualifier), right_name);

            return Ok(Expr::binary_op(ctx, left_expr, BinOp::Eq, right_expr));
        }
    }

    // Fall back to a general condition using a column from the right table
    let right_col_names: Vec<(String, DataType)> = right_table
        .filterable_columns()
        .map(|c| (c.name.clone(), c.data_type))
        .collect();
    if right_col_names.is_empty() {
        // If no filterable columns, generate a literal condition
        let lit = generate_literal(ctx, DataType::Integer, generator.policy());
        return Ok(Expr::literal(ctx, lit));
    }
    let (col_name, col_dt) = ctx.choose(&right_col_names).unwrap().clone();
    let col_expr = Expr::column_ref(ctx, Some(right_qualifier), col_name);
    let lit = generate_literal(ctx, col_dt, generator.policy());
    let lit_expr = Expr::literal(ctx, lit);
    let ops = BinOp::comparison();
    let op = *ctx.choose(ops).unwrap();
    Ok(Expr::binary_op(ctx, col_expr, op, lit_expr))
}

#[trace_gen(Origin::CompoundUnion)]
fn generate_compound_union<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<SelectStmt, GenError> {
    todo!("UNION generation")
}

#[trace_gen(Origin::CompoundUnionAll)]
fn generate_compound_union_all<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<SelectStmt, GenError> {
    todo!("UNION ALL generation")
}

#[trace_gen(Origin::CompoundIntersect)]
fn generate_compound_intersect<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<SelectStmt, GenError> {
    todo!("INTERSECT generation")
}

#[trace_gen(Origin::CompoundExcept)]
fn generate_compound_except<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<SelectStmt, GenError> {
    todo!("EXCEPT generation")
}

#[trace_gen(Origin::Cte)]
fn generate_cte<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<(), GenError> {
    todo!("CTE generation")
}

#[trace_gen(Origin::RecursiveCte)]
fn generate_recursive_cte<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<(), GenError> {
    todo!("recursive CTE generation")
}

#[trace_gen(Origin::DerivedTable)]
fn generate_derived_table<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<(), GenError> {
    todo!("derived table generation")
}

#[trace_gen(Origin::AggregateDistinct)]
fn generate_aggregate_distinct<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<Expr, GenError> {
    todo!("aggregate DISTINCT generation")
}

#[trace_gen(Origin::AggregateFilter)]
fn generate_aggregate_filter<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
) -> Result<Expr, GenError> {
    todo!("aggregate FILTER generation")
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
        let mut ctx = Context::new_with_seed(42);

        let select = generate_simple_select(&generator, &mut ctx);
        assert!(select.is_ok());

        let select = select.unwrap();
        assert_eq!(select.columns.len(), 1);
        assert_eq!(select.limit, Some(1));
    }

    #[test]
    fn test_generate_group_by_clause() {
        let generator = test_generator();
        let table = generator.schema().tables[0].clone();
        let mut ctx = Context::new_with_seed(42);

        let clause = ctx
            .with_table_scope([(table, None)], |ctx| {
                generate_group_by_clause(&generator, ctx)
            })
            .unwrap();
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
        let mut ctx = Context::new_with_seed(42);

        let select = generate_select_impl(&generator, &mut ctx, SelectMode::Full).unwrap();
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
        let mut ctx = Context::new_with_seed(42);

        let select = generate_select_impl(&generator, &mut ctx, SelectMode::Full).unwrap();
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

        let mut found_expr_order_by = false;
        for seed in 0..50 {
            let mut ctx = Context::new_with_seed(seed);
            if let Ok(select) = generate_select_impl(&generator, &mut ctx, SelectMode::Full) {
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

        let mut found_nulls = false;
        for seed in 0..30 {
            let mut ctx = Context::new_with_seed(seed);
            if let Ok(select) = generate_select_impl(&generator, &mut ctx, SelectMode::Full) {
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

        let mut found_grouped = false;
        for seed in 0..30 {
            let mut ctx = Context::new_with_seed(seed);
            if let Ok(select) = generate_simple_select(&generator, &mut ctx) {
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
        assert!(found_grouped, "Should generate subqueries with GROUP BY");
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
        let mut ctx = Context::new_with_seed(42);

        let select = generate_select_impl(&generator, &mut ctx, SelectMode::Full).unwrap();
        let sql = select.to_string();
        assert!(
            !sql.contains("HAVING"),
            "SQL should not contain HAVING without GROUP BY: {sql}"
        );
    }

    #[test]
    fn test_restrict_mixed_aggregates_default_on() {
        let config = crate::policy::SelectConfig::default();
        assert!(
            config.restrict_mixed_aggregates,
            "restrict_mixed_aggregates should be true by default"
        );
    }

    #[test]
    fn test_generate_select_with_join() {
        use crate::policy::JoinConfig;

        let policy = Policy::default().with_select_config(crate::policy::SelectConfig {
            join_config: JoinConfig {
                join_probability: 1.0,
                max_joins: 2,
                ..Default::default()
            },
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
            .table(Table::new(
                "orders",
                vec![
                    ColumnDef::new("order_id", DataType::Integer).primary_key(),
                    ColumnDef::new("user_id", DataType::Integer),
                    ColumnDef::new("amount", DataType::Real),
                ],
            ))
            .build();
        let generator: SqlGen<Full> = SqlGen::new(schema, policy);

        let mut found_join = false;
        for seed in 0..50 {
            let mut ctx = Context::new_with_seed(seed);
            if let Ok(select) = generate_select_impl(&generator, &mut ctx, SelectMode::Full) {
                let sql = select.to_string();
                if sql.contains("JOIN") {
                    found_join = true;
                    break;
                }
            }
        }
        assert!(found_join, "Should generate at least one JOIN query");
    }

    #[test]
    fn test_join_generates_qualified_columns() {
        use crate::policy::JoinConfig;

        let policy = Policy::default().with_select_config(crate::policy::SelectConfig {
            join_config: JoinConfig {
                join_probability: 1.0,
                max_joins: 1,
                self_join_probability: 0.0,
                ..Default::default()
            },
            group_by_probability: 0.0,
            select_star_weight: 0,
            column_list_weight: 100,
            expression_list_weight: 0,
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
            .table(Table::new(
                "orders",
                vec![
                    ColumnDef::new("order_id", DataType::Integer).primary_key(),
                    ColumnDef::new("amount", DataType::Real),
                ],
            ))
            .build();
        let generator: SqlGen<Full> = SqlGen::new(schema, policy);

        let mut found_qualified = false;
        for seed in 0..100 {
            let mut ctx = Context::new_with_seed(seed);
            if let Ok(select) = generate_select_impl(&generator, &mut ctx, SelectMode::Full) {
                if !select.joins.is_empty() {
                    let sql = select.to_string();
                    // With multiple tables, column refs should be qualified (contain a dot)
                    // Check that the SELECT columns part contains qualified references
                    if let Some(select_part) = sql.strip_prefix("SELECT ") {
                        if let Some(cols_part) = select_part.split(" FROM ").next() {
                            if cols_part.contains('.') {
                                found_qualified = true;
                                break;
                            }
                        }
                    }
                }
            }
        }
        assert!(
            found_qualified,
            "JOIN queries should produce qualified column references"
        );
    }

    #[test]
    fn test_self_join_uses_aliases() {
        use crate::policy::JoinConfig;

        let policy = Policy::default().with_select_config(crate::policy::SelectConfig {
            join_config: JoinConfig {
                join_probability: 1.0,
                max_joins: 1,
                self_join_probability: 1.0,
                ..Default::default()
            },
            group_by_probability: 0.0,
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

        let mut found_self_join = false;
        for seed in 0..50 {
            let mut ctx = Context::new_with_seed(seed);
            if let Ok(select) = generate_select_impl(&generator, &mut ctx, SelectMode::Full) {
                if !select.joins.is_empty() {
                    let sql = select.to_string();
                    // Self-join should have aliases (AS tN)
                    if sql.contains("JOIN users AS ") {
                        found_self_join = true;
                        // The joined table must have an alias
                        assert!(
                            select.joins[0].alias.is_some(),
                            "Self-join must have alias: {sql}"
                        );
                        break;
                    }
                }
            }
        }
        assert!(found_self_join, "Should generate self-join with aliases");
    }

    #[test]
    fn test_no_mixed_aggregates_in_non_grouped_select() {
        // Force expression-list strategy, disable GROUP BY
        let policy = Policy::default().with_select_config(crate::policy::SelectConfig {
            select_star_weight: 0,
            column_list_weight: 0,
            expression_list_weight: 100,
            group_by_probability: 0.0,
            restrict_mixed_aggregates: true,
            ..Default::default()
        });
        let schema = SchemaBuilder::new()
            .table(Table::new(
                "t",
                vec![
                    ColumnDef::new("a", DataType::Integer).primary_key(),
                    ColumnDef::new("b", DataType::Text),
                    ColumnDef::new("c", DataType::Integer),
                ],
            ))
            .build();
        let generator: SqlGen<Full> = SqlGen::new(schema, policy);

        for seed in 0..200 {
            let mut ctx = Context::new_with_seed(seed);
            if let Ok(select) = generate_select_impl(&generator, &mut ctx, SelectMode::Full) {
                if select.group_by.is_some() {
                    continue; // skip grouped (shouldn't happen with prob 0.0)
                }
                let has_agg = select.columns.iter().any(|c| c.expr.contains_aggregate());
                let has_non_agg = select.columns.iter().any(|c| !c.expr.contains_aggregate());
                assert!(
                    !(has_agg && has_non_agg),
                    "seed {seed}: non-grouped SELECT mixes aggregates and non-aggregates: {select}"
                );
            }
        }
    }
}
