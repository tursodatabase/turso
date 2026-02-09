//! Expression generation.

use crate::ast::{BinOp, Expr, Literal, UnaryOp};
use crate::capabilities::Capabilities;
use crate::context::Context;
use crate::error::GenError;
use crate::functions::FunctionDef;
use crate::generate::literal::generate_literal;
use crate::generate::select::{generate_select, generate_simple_select};
use crate::schema::{DataType, Table};
use crate::trace::{ExprKind, Origin};
use crate::{SqlGen, Stmt};
use sql_gen_macros::trace_gen;

/// Generate an expression.
pub fn generate_expr<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
) -> Result<Expr, GenError> {
    let candidates = build_expr_candidates::<C>(generator, ctx, depth)?;

    let expr_type = generator
        .policy()
        .select_weighted(ctx, &candidates)
        .map_err(|e| e.with_context("generating expression"))?;

    dispatch_expr_generation(generator, ctx, table, depth, expr_type)
}

/// Build list of allowed expression kinds based on capabilities, policy weights, and depth validity.
fn build_expr_candidates<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &Context,
    depth: usize,
) -> Result<Vec<(ExprKind, u32)>, GenError> {
    let capability_candidates = collect_capability_allowed_exprs::<C>();

    if capability_candidates.is_empty() {
        return Err(GenError::exhausted(
            "expression",
            "no expression types allowed by capabilities",
        ));
    }

    let weighted_candidates = filter_by_expr_weight(generator, capability_candidates);

    let valid_candidates: Vec<(ExprKind, u32)> =
        filter_by_depth_validity(generator, ctx, depth, weighted_candidates).collect();

    if valid_candidates.is_empty() {
        return Err(GenError::exhausted(
            "expression",
            "no expression types valid for current depth",
        ));
    }

    Ok(valid_candidates)
}

/// Collect expression kinds allowed by the capability type parameter.
fn collect_capability_allowed_exprs<C: Capabilities>() -> Vec<ExprKind> {
    let mut candidates = vec![
        // Simple expressions (always available)
        ExprKind::ColumnRef,
        ExprKind::Literal,
        // Complex expressions
        ExprKind::BinaryOp,
        ExprKind::UnaryOp,
        ExprKind::IsNull,
        ExprKind::Between,
        ExprKind::InList,
        ExprKind::FunctionCall,
        ExprKind::Case,
        ExprKind::Cast,
    ];

    // Subquery expressions require capability
    if C::SUBQUERY {
        candidates.push(ExprKind::Subquery);
        candidates.push(ExprKind::InSubquery);
        candidates.push(ExprKind::Exists);
    }

    // Window functions require capability
    if C::WINDOW_FN {
        candidates.push(ExprKind::WindowFunction);
    }

    // Always available (but weight 0)
    candidates.push(ExprKind::Collate);
    candidates.push(ExprKind::Raise);

    candidates
}

/// Filter candidates to only those with positive policy weight.
fn filter_by_expr_weight<C: Capabilities>(
    generator: &SqlGen<C>,
    candidates: impl IntoIterator<Item = ExprKind>,
) -> impl Iterator<Item = ExprKind> {
    candidates
        .into_iter()
        .filter(|k| generator.policy().expr_weights.weight_for(*k) > 0)
}

/// Filter candidates to only those valid for the current expression depth.
///
/// This prevents attempting to generate complex expressions when at max depth,
/// and filters out subqueries when at max subquery depth.
fn filter_by_depth_validity<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &Context,
    depth: usize,
    candidates: impl Iterator<Item = ExprKind>,
) -> impl Iterator<Item = (ExprKind, u32)> {
    let max_expr_depth = generator.policy().max_expr_depth;
    let max_subquery_depth = generator.policy().max_subquery_depth;
    let subquery_depth = ctx.subquery_depth();
    let weights = generator.policy().expr_weights.clone();

    candidates.filter_map(move |kind| {
        let valid = is_expr_valid_for_depth(
            kind,
            depth,
            max_expr_depth,
            subquery_depth,
            max_subquery_depth,
        );
        if valid {
            Some((kind, weights.weight_for(kind)))
        } else {
            None
        }
    })
}

/// Check if an expression kind is valid given the current depth state.
fn is_expr_valid_for_depth(
    kind: ExprKind,
    depth: usize,
    max_expr_depth: usize,
    subquery_depth: usize,
    max_subquery_depth: usize,
) -> bool {
    match kind {
        // Simple expressions are always valid
        ExprKind::ColumnRef | ExprKind::Literal => true,

        // Complex expressions require depth budget
        ExprKind::BinaryOp
        | ExprKind::UnaryOp
        | ExprKind::IsNull
        | ExprKind::Between
        | ExprKind::InList
        | ExprKind::FunctionCall
        | ExprKind::Case
        | ExprKind::Cast => depth < max_expr_depth,

        // Subquery expressions require both depth budgets
        ExprKind::Subquery | ExprKind::InSubquery | ExprKind::Exists => {
            depth < max_expr_depth && subquery_depth < max_subquery_depth
        }

        // Parenthesized is not generated directly
        ExprKind::Parenthesized => false,

        // Window functions require depth budget and subquery budget (they contain nested SELECTs conceptually)
        ExprKind::WindowFunction => depth < max_expr_depth,

        // Collate and Raise require depth budget
        ExprKind::Collate | ExprKind::Raise => depth < max_expr_depth,
    }
}

/// Dispatch to the appropriate expression generator.
fn dispatch_expr_generation<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
    expr_type: ExprKind,
) -> Result<Expr, GenError> {
    match expr_type {
        ExprKind::ColumnRef => generate_column_ref(ctx, table),
        ExprKind::Literal => generate_literal_expr(generator, ctx, table),
        ExprKind::BinaryOp => generate_binary_op(generator, ctx, table, depth),
        ExprKind::UnaryOp => generate_unary_op(generator, ctx, table, depth),
        ExprKind::FunctionCall => generate_function_call(generator, ctx, table, depth),
        ExprKind::IsNull => generate_is_null(generator, ctx, table, depth),
        ExprKind::Between => generate_between(generator, ctx, table, depth),
        ExprKind::InList => generate_in_list(generator, ctx, table, depth),
        ExprKind::InSubquery => generate_in_subquery(generator, ctx, table, depth),
        ExprKind::Case => generate_case(generator, ctx, table, depth),
        ExprKind::Cast => generate_cast(generator, ctx, table, depth),
        ExprKind::Subquery => generate_subquery_expr(generator, ctx, table),
        ExprKind::Exists => generate_exists(generator, ctx),
        // Parenthesized is not generated directly - it's just for grouping
        ExprKind::Parenthesized => unreachable!("parenthesized is not generated directly"),
        // Stubs
        ExprKind::WindowFunction => todo!("window function generation"),
        ExprKind::Collate => todo!("COLLATE expression generation"),
        ExprKind::Raise => todo!("RAISE expression generation"),
    }
}

/// Generate a column reference.
fn generate_column_ref(ctx: &mut Context, table: &Table) -> Result<Expr, GenError> {
    let cols: Vec<_> = table.filterable_columns().collect();
    if cols.is_empty() {
        return Err(GenError::exhausted("column_ref", "no filterable columns"));
    }

    let col = ctx.choose(&cols).unwrap();
    Ok(Expr::column_ref(ctx, None, col.name.clone()))
}

/// Generate a literal expression.
fn generate_literal_expr<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
) -> Result<Expr, GenError> {
    // Pick a random data type from the table's columns, or default to Integer
    let data_type = if !table.columns.is_empty() {
        let col = ctx.choose(&table.columns).unwrap();
        col.data_type
    } else {
        DataType::Integer
    };

    let lit = generate_literal(ctx, data_type, generator.policy());
    Ok(Expr::literal(ctx, lit))
}

/// Generate a binary operation.
fn generate_binary_op<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
) -> Result<Expr, GenError> {
    let expr_config = &generator.policy().expr_config;
    let category_weights = &expr_config.binop_category_weights;

    // Pick operator category using weights
    let candidates = [
        (OpType::Comparison, category_weights.comparison),
        (OpType::Logical, category_weights.logical),
        (OpType::Arithmetic, category_weights.arithmetic),
    ];

    let op_type = generator.policy().select_weighted(ctx, &candidates)?;

    let ops = match op_type {
        OpType::Comparison => BinOp::comparison(),
        OpType::Logical => BinOp::logical(),
        OpType::Arithmetic => BinOp::arithmetic(),
    };

    let op = *ctx
        .choose(ops)
        .ok_or_else(|| GenError::exhausted("binary_op", "no operators available"))?;

    let left = generate_binop_left(generator, ctx, table, depth)?;
    let right = generate_binop_right(generator, ctx, table, depth)?;

    // --- LIKE ... ESCAPE (not yet implemented) ---
    if matches!(op, BinOp::Like)
        && ctx.gen_bool_with_prob(generator.policy().expr_config.like_escape_probability)
    {
        return generate_like_escape(generator, ctx, table, depth);
    }

    Ok(Expr::binary_op(ctx, left, op, right))
}

/// Generate the left operand of a binary operation.
#[trace_gen(Origin::BinaryOpLeft)]
fn generate_binop_left<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
) -> Result<Expr, GenError> {
    generate_expr(generator, ctx, table, depth + 1)
}

/// Generate the right operand of a binary operation.
#[trace_gen(Origin::BinaryOpRight)]
fn generate_binop_right<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
) -> Result<Expr, GenError> {
    generate_expr(generator, ctx, table, depth + 1)
}

#[derive(Clone, Copy)]
enum OpType {
    Comparison,
    Logical,
    Arithmetic,
}

/// Generate a unary operation.
fn generate_unary_op<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
) -> Result<Expr, GenError> {
    let ops = [UnaryOp::Neg, UnaryOp::Not, UnaryOp::BitNot];
    let op = *ctx.choose(&ops).unwrap();

    let operand = generate_expr(generator, ctx, table, depth + 1)?;
    Ok(Expr::unary_op(ctx, op, operand))
}

/// Generate a function call expression.
fn generate_function_call<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
) -> Result<Expr, GenError> {
    let func_config = &generator.policy().function_config;
    let func = func_config.select_function(ctx)?;

    let args = generate_function_args(generator, ctx, table, depth, func)?;

    Ok(Expr::function_call(ctx, func.name.to_string(), args))
}

/// Generate arguments for a function call.
#[trace_gen(Origin::FunctionArg)]
fn generate_function_args<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
    func: &FunctionDef,
) -> Result<Vec<Expr>, GenError> {
    let arg_count = func.arg_count(ctx);
    let mut args = Vec::with_capacity(arg_count);

    for i in 0..arg_count {
        let arg = generate_function_arg(generator, ctx, table, depth, func, i)?;
        args.push(arg);
    }

    Ok(args)
}

/// Generate a single function argument.
fn generate_function_arg<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
    func: &FunctionDef,
    arg_index: usize,
) -> Result<Expr, GenError> {
    // Check if this function has integer argument constraints (e.g., zeroblob)
    if let Some(max_val) = func.int_arg_max {
        if let Some(expected_type) = func.arg_type_at(arg_index) {
            if expected_type == DataType::Integer {
                // Generate a constrained integer literal
                let val = ctx.gen_range_inclusive(0, max_val as usize) as i64;
                return Ok(Expr::literal(ctx, Literal::Integer(val)));
            }
        }
        // If no specific type, still apply the constraint for safety
        if func.arg_types.is_empty() {
            let val = ctx.gen_range_inclusive(0, max_val as usize) as i64;
            return Ok(Expr::literal(ctx, Literal::Integer(val)));
        }
    }

    // Check if there's a specific expected type for this argument
    if let Some(expected_type) = func.arg_type_at(arg_index) {
        // Generate a literal of the expected type
        let lit = generate_literal(ctx, expected_type, generator.policy());
        return Ok(Expr::literal(ctx, lit));
    }

    // Otherwise, generate a general expression
    generate_expr(generator, ctx, table, depth + 1)
}

/// Generate an IS NULL / IS NOT NULL expression.
fn generate_is_null<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
) -> Result<Expr, GenError> {
    let expr_config = &generator.policy().expr_config;
    let negated = ctx.gen_bool_with_prob(expr_config.is_null_negation_probability);
    let expr = generate_expr(generator, ctx, table, depth + 1)?;
    Ok(Expr::is_null(ctx, expr, negated))
}

/// Generate a BETWEEN expression.
fn generate_between<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
) -> Result<Expr, GenError> {
    let expr_config = &generator.policy().expr_config;
    let negated = ctx.gen_bool_with_prob(expr_config.between_negation_probability);
    let expr = generate_expr(generator, ctx, table, depth + 1)?;
    let low = generate_expr(generator, ctx, table, depth + 1)?;
    let high = generate_expr(generator, ctx, table, depth + 1)?;
    Ok(Expr::between(ctx, expr, low, high, negated))
}

/// Generate an IN list expression.
fn generate_in_list<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
) -> Result<Expr, GenError> {
    let expr_config = &generator.policy().expr_config;
    let negated = ctx.gen_bool_with_prob(expr_config.in_list_negation_probability);
    let expr = generate_expr(generator, ctx, table, depth + 1)?;

    let list_size = ctx.gen_range_inclusive(1, generator.policy().max_in_list_size.min(5));
    let mut list = Vec::with_capacity(list_size);
    for _ in 0..list_size {
        list.push(generate_expr(generator, ctx, table, depth)?);
    }

    Ok(Expr::in_list(ctx, expr, list, negated))
}

/// Generate a CASE expression.
fn generate_case<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
) -> Result<Expr, GenError> {
    let expr_config = &generator.policy().expr_config;
    let max_branches = generator.policy().max_case_branches;
    let min_branches = expr_config.case_min_branches.min(max_branches);

    let num_when = ctx.gen_range_inclusive(min_branches, max_branches);
    let mut when_clauses = Vec::with_capacity(num_when);

    for _ in 0..num_when {
        let when_expr = generate_case_when(generator, ctx, table, depth)?;
        let then_expr = generate_case_then(generator, ctx, table, depth)?;
        when_clauses.push((when_expr, then_expr));
    }

    let else_clause = if ctx.gen_bool_with_prob(expr_config.case_else_probability) {
        Some(generate_case_else(generator, ctx, table, depth)?)
    } else {
        None
    };

    Ok(Expr::case_expr(ctx, None, when_clauses, else_clause))
}

/// Generate the WHEN condition of a CASE expression.
#[trace_gen(Origin::CaseWhen)]
fn generate_case_when<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
) -> Result<Expr, GenError> {
    generate_expr(generator, ctx, table, depth + 1)
}

/// Generate the THEN result of a CASE expression.
#[trace_gen(Origin::CaseThen)]
fn generate_case_then<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
) -> Result<Expr, GenError> {
    generate_expr(generator, ctx, table, depth + 1)
}

/// Generate the ELSE result of a CASE expression.
#[trace_gen(Origin::CaseElse)]
fn generate_case_else<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
) -> Result<Expr, GenError> {
    generate_expr(generator, ctx, table, depth + 1)
}

/// Generate a CAST expression.
fn generate_cast<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
) -> Result<Expr, GenError> {
    let types = [DataType::Integer, DataType::Real, DataType::Text];
    let target_type = *ctx.choose(&types).unwrap();

    let expr = generate_expr(generator, ctx, table, depth + 1)?;
    Ok(Expr::cast(ctx, expr, target_type))
}

/// Generate a scalar subquery expression.
/// Scalar subqueries must return exactly 1 column.
fn generate_subquery_expr<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
) -> Result<Expr, GenError> {
    let select = generate_simple_select(generator, ctx, table)?;
    Ok(Expr::subquery(ctx, select))
}

/// Generate the SELECT statement for a subquery.
#[trace_gen(Origin::Subquery)]
fn generate_subquery_select<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<crate::ast::SelectStmt, GenError> {
    generate_select(generator, ctx).map(|stmt| {
        let Stmt::Select(select) = stmt else {
            unreachable!()
        };
        select
    })
}

/// Generate an IN subquery expression (expr IN (SELECT ...) or expr NOT IN (SELECT ...)).
fn generate_in_subquery<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
) -> Result<Expr, GenError> {
    let expr_config = &generator.policy().expr_config;
    let negated = ctx.gen_bool_with_prob(expr_config.in_subquery_negation_probability);

    // Generate the left-hand expression (typically a column or simple expression)
    let expr = generate_expr(generator, ctx, table, depth + 1)?;

    // IN subqueries must return exactly 1 column
    let subquery = generate_simple_select(generator, ctx, table)?;

    Ok(Expr::in_subquery(ctx, expr, subquery, negated))
}

/// Generate an EXISTS expression (EXISTS (SELECT ...) or NOT EXISTS (SELECT ...)).
fn generate_exists<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Expr, GenError> {
    let expr_config = &generator.policy().expr_config;
    let negated = ctx.gen_bool_with_prob(expr_config.exists_negation_probability);

    // Generate the subquery
    let subquery = generate_subquery_select(generator, ctx)?;

    Ok(Expr::exists(ctx, subquery, negated))
}

/// Generate a WHERE clause condition.
#[trace_gen(Origin::Where)]
pub fn generate_condition<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
) -> Result<Expr, GenError> {
    generate_expr(generator, ctx, table, 0)
}

/// Generate a LIKE ... ESCAPE expression (stub).
#[trace_gen(Origin::LikeEscape)]
fn generate_like_escape<C: Capabilities>(
    _generator: &SqlGen<C>,
    _ctx: &mut Context,
    _table: &Table,
    _depth: usize,
) -> Result<Expr, GenError> {
    todo!("LIKE ... ESCAPE expression generation")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Full;
    use crate::policy::Policy;
    use crate::schema::{ColumnDef, SchemaBuilder, Table};

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
    fn test_generate_expr() {
        let generator = test_generator();
        let table = &generator.schema().tables[0];
        let mut ctx = Context::new_with_seed(42);

        let expr = generate_expr(&generator, &mut ctx, table, 0);
        assert!(expr.is_ok());
    }

    #[test]
    fn test_generate_condition() {
        let generator = test_generator();
        let table = &generator.schema().tables[0];
        let mut ctx = Context::new_with_seed(42);

        let cond = generate_condition(&generator, &mut ctx, table);
        assert!(cond.is_ok());
    }

    #[test]
    fn test_depth_limiting() {
        let generator = test_generator();
        let table = &generator.schema().tables[0];
        let mut ctx = Context::new_with_seed(42);

        // At max depth, should only generate simple expressions
        let expr = generate_expr(&generator, &mut ctx, table, 10);
        assert!(expr.is_ok());
    }

    #[test]
    fn test_generate_function_call() {
        use crate::policy::ExprWeights;

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

        // Create policy with high function_call weight to ensure we hit it
        let policy = Policy::default().with_expr_weights(ExprWeights {
            function_call: 100,
            column_ref: 0,
            literal: 0,
            binary_op: 0,
            unary_op: 0,
            subquery: 0,
            case_expr: 0,
            cast: 0,
            between: 0,
            in_list: 0,
            in_subquery: 0,
            is_null: 0,
            exists: 0,
            window_function: 0,
            collate: 0,
            raise: 0,
        });

        let generator: SqlGen<Full> = SqlGen::new(schema, policy);
        let table = &generator.schema().tables[0];
        let mut ctx = Context::new_with_seed(42);

        let expr = generate_function_call(&generator, &mut ctx, table, 0);
        assert!(expr.is_ok());

        let expr_str = expr.unwrap().to_string();
        // Should contain a function call with parentheses
        assert!(expr_str.contains('('));
        assert!(expr_str.contains(')'));
    }

    #[test]
    fn test_function_call_in_select() {
        use crate::policy::ExprWeights;

        let schema = SchemaBuilder::new()
            .table(Table::new(
                "users",
                vec![
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("name", DataType::Text),
                ],
            ))
            .build();

        let policy = Policy::default().with_expr_weights(ExprWeights {
            function_call: 50,
            column_ref: 25,
            literal: 25,
            binary_op: 0,
            unary_op: 0,
            subquery: 0,
            case_expr: 0,
            cast: 0,
            between: 0,
            in_list: 0,
            in_subquery: 0,
            is_null: 0,
            exists: 0,
            window_function: 0,
            collate: 0,
            raise: 0,
        });

        let generator: SqlGen<Full> = SqlGen::new(schema, policy);
        let mut ctx = Context::new_with_seed(123);

        // Get all function names from the static definitions
        let function_names: Vec<_> = crate::functions::scalar_functions()
            .chain(crate::functions::aggregate_functions())
            .map(|f| f.name)
            .collect();

        // Generate several statements; some should contain function calls
        let mut found_function_call = false;
        for _ in 0..50 {
            if let Ok(stmt) = generator.statement(&mut ctx) {
                let sql = stmt.to_string();
                // Check if any known function name appears in the SQL
                if function_names.iter().any(|name| sql.contains(name)) {
                    found_function_call = true;
                    break;
                }
            }
        }
        assert!(found_function_call, "Should generate function calls");
    }

    #[test]
    fn test_show_generated_functions() {
        use crate::policy::ExprWeights;

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

        // High function_call weight to generate lots of functions
        let policy = Policy::default().with_expr_weights(ExprWeights {
            function_call: 60,
            column_ref: 20,
            literal: 20,
            binary_op: 0,
            unary_op: 0,
            subquery: 0,
            case_expr: 0,
            cast: 0,
            between: 0,
            in_list: 0,
            in_subquery: 0,
            is_null: 0,
            exists: 0,
            window_function: 0,
            collate: 0,
            raise: 0,
        });

        let generator: SqlGen<Full> = SqlGen::new(schema, policy);
        let mut ctx = Context::new_with_seed(12345);

        println!("\n=== Generated SQL with Function Calls ===");
        let mut function_count = 0;
        for i in 0..15 {
            if let Ok(stmt) = generator.statement(&mut ctx) {
                let sql = stmt.to_string();
                println!("{}. {}", i + 1, sql);
                if sql.contains('(') && !sql.contains("INSERT") {
                    function_count += 1;
                }
            }
        }
        println!("=== Found {function_count} statements with function calls ===\n");
        assert!(
            function_count > 0,
            "Should have generated some function calls"
        );
    }

    #[test]
    fn test_where_with_subqueries() {
        use crate::policy::{ExprWeights, SelectConfig};

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

        // High weights for EXISTS and IN subquery so WHERE clauses contain them
        let policy = Policy::default()
            .with_expr_weights(ExprWeights {
                column_ref: 5,
                literal: 5,
                binary_op: 5,
                unary_op: 0,
                function_call: 0,
                subquery: 10,
                case_expr: 0,
                cast: 0,
                between: 0,
                in_list: 0,
                in_subquery: 30,
                is_null: 0,
                exists: 30,
                window_function: 0,
                collate: 0,
                raise: 0,
            })
            .with_select_config(SelectConfig {
                where_probability: 1.0,
                ..Default::default()
            });

        let generator: SqlGen<Full> = SqlGen::new(schema, policy);
        let mut ctx = Context::new_with_seed(42);

        let mut found_exists_in_where = false;
        let mut found_in_select_in_where = false;

        for _ in 0..100 {
            if let Ok(stmt) = generator.statement(&mut ctx) {
                let sql = stmt.to_string();
                if let Some(where_part) = sql.split("WHERE").nth(1) {
                    if where_part.contains("EXISTS") {
                        found_exists_in_where = true;
                    }
                    if where_part.contains("IN (SELECT") || where_part.contains("NOT IN (SELECT") {
                        found_in_select_in_where = true;
                    }
                }
                if found_exists_in_where && found_in_select_in_where {
                    break;
                }
            }
        }

        assert!(
            found_exists_in_where || found_in_select_in_where,
            "WHERE clauses should contain EXISTS or IN (SELECT ...) expressions"
        );
    }

    #[test]
    fn test_generate_exists_and_in_subquery() {
        use crate::policy::ExprWeights;

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
                    ColumnDef::new("id", DataType::Integer).primary_key(),
                    ColumnDef::new("user_id", DataType::Integer),
                ],
            ))
            .build();

        // High weights for EXISTS and IN subquery
        let policy = Policy::default().with_expr_weights(ExprWeights {
            column_ref: 10,
            literal: 10,
            binary_op: 0,
            unary_op: 0,
            function_call: 0,
            subquery: 0,
            case_expr: 0,
            cast: 0,
            between: 0,
            in_list: 0,
            in_subquery: 40,
            is_null: 0,
            exists: 40,
            window_function: 0,
            collate: 0,
            raise: 0,
        });

        let generator: SqlGen<Full> = SqlGen::new(schema, policy);
        let mut ctx = Context::new_with_seed(42);

        let mut found_exists = false;
        let mut found_in_subquery = false;

        for _ in 0..50 {
            if let Ok(stmt) = generator.statement(&mut ctx) {
                let sql = stmt.to_string();
                if sql.contains("EXISTS") {
                    found_exists = true;
                }
                if sql.contains(" IN (SELECT") || sql.contains(" NOT IN (SELECT") {
                    found_in_subquery = true;
                }
                if found_exists && found_in_subquery {
                    break;
                }
            }
        }

        assert!(
            found_exists || found_in_subquery,
            "Should generate EXISTS or IN subquery expressions"
        );
    }
}
