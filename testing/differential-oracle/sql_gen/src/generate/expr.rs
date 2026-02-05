//! Expression generation.

use crate::SqlGen;
use crate::ast::{BinOp, Expr, Literal, UnaryOp};
use crate::capabilities::Capabilities;
use crate::context::Context;
use crate::error::GenError;
use crate::generate::literal::generate_literal;
use crate::schema::{DataType, Table};
use crate::trace::Origin;

/// Generate an expression.
pub fn generate_expr<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
) -> Result<Expr, GenError> {
    // At max depth, only generate simple expressions
    if depth >= generator.policy().max_expr_depth {
        return generate_simple_expr(generator, ctx, table);
    }

    let candidates = build_expr_candidates::<C>(generator, ctx, depth);

    let expr_type = generator
        .policy()
        .select_weighted(ctx, &candidates)
        .map_err(|e| e.with_context("generating expression"))?;

    dispatch_expr_generation(generator, ctx, table, depth, expr_type)
}

#[derive(Clone, Copy)]
enum ExprType {
    ColumnRef,
    Literal,
    BinaryOp,
    UnaryOp,
    IsNull,
    Between,
    InList,
    CaseExpr,
    Cast,
    Subquery,
}

/// Build expression candidates with their weights.
fn build_expr_candidates<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &Context,
    depth: usize,
) -> Vec<(ExprType, u32)> {
    let mut candidates = build_simple_expr_candidates(generator);

    if depth < generator.policy().max_expr_depth {
        add_complex_expr_candidates::<C>(generator, ctx, &mut candidates);
    }

    candidates
}

/// Build candidates for simple expressions (always available).
fn build_simple_expr_candidates<C: Capabilities>(generator: &SqlGen<C>) -> Vec<(ExprType, u32)> {
    let weights = &generator.policy().expr_weights;
    vec![
        (ExprType::ColumnRef, weights.column_ref),
        (ExprType::Literal, weights.literal),
    ]
}

/// Add complex expression candidates when depth budget allows.
fn add_complex_expr_candidates<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &Context,
    candidates: &mut Vec<(ExprType, u32)>,
) {
    let weights = &generator.policy().expr_weights;

    // Core complex expressions
    candidates.push((ExprType::BinaryOp, weights.binary_op));
    candidates.push((ExprType::UnaryOp, weights.unary_op));
    candidates.push((ExprType::IsNull, weights.is_null));
    candidates.push((ExprType::Between, weights.between));
    candidates.push((ExprType::InList, weights.in_list));

    // Optional expressions (only if weight > 0)
    if weights.case_expr > 0 {
        candidates.push((ExprType::CaseExpr, weights.case_expr));
    }

    if weights.cast > 0 {
        candidates.push((ExprType::Cast, weights.cast));
    }

    // Subqueries require capability and depth budget
    if C::SUBQUERY
        && weights.subquery > 0
        && ctx.subquery_depth() < generator.policy().max_subquery_depth
    {
        candidates.push((ExprType::Subquery, weights.subquery));
    }
}

/// Dispatch to the appropriate expression generator.
fn dispatch_expr_generation<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    depth: usize,
    expr_type: ExprType,
) -> Result<Expr, GenError> {
    match expr_type {
        ExprType::ColumnRef => generate_column_ref(ctx, table),
        ExprType::Literal => generate_literal_expr(generator, ctx, table),
        ExprType::BinaryOp => generate_binary_op(generator, ctx, table, depth),
        ExprType::UnaryOp => generate_unary_op(generator, ctx, table, depth),
        ExprType::IsNull => generate_is_null(generator, ctx, table, depth),
        ExprType::Between => generate_between(generator, ctx, table, depth),
        ExprType::InList => generate_in_list(generator, ctx, table, depth),
        ExprType::CaseExpr => generate_case(generator, ctx, table, depth),
        ExprType::Cast => generate_cast(generator, ctx, table, depth),
        ExprType::Subquery => generate_subquery_expr(generator, ctx),
    }
}

/// Generate a simple expression (column ref or literal).
fn generate_simple_expr<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
) -> Result<Expr, GenError> {
    if ctx.gen_bool() && !table.columns.is_empty() {
        generate_column_ref(ctx, table)
    } else {
        generate_literal_expr(generator, ctx, table)
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

    ctx.enter_scope(Origin::BinaryOpLeft);
    let left = generate_expr(generator, ctx, table, depth + 1)?;
    ctx.exit_scope();

    ctx.enter_scope(Origin::BinaryOpRight);
    let right = generate_expr(generator, ctx, table, depth + 1)?;
    ctx.exit_scope();

    Ok(Expr::binary_op(ctx, left, op, right))
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
        list.push(generate_simple_expr(generator, ctx, table)?);
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
        ctx.enter_scope(Origin::CaseWhen);
        let when_expr = generate_expr(generator, ctx, table, depth + 1)?;
        ctx.exit_scope();

        ctx.enter_scope(Origin::CaseThen);
        let then_expr = generate_expr(generator, ctx, table, depth + 1)?;
        ctx.exit_scope();

        when_clauses.push((when_expr, then_expr));
    }

    let else_clause = if ctx.gen_bool_with_prob(expr_config.case_else_probability) {
        ctx.enter_scope(Origin::CaseElse);
        let expr = generate_expr(generator, ctx, table, depth + 1)?;
        ctx.exit_scope();
        Some(expr)
    } else {
        None
    };

    Ok(Expr::case_expr(ctx, None, when_clauses, else_clause))
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

/// Generate a subquery expression.
fn generate_subquery_expr<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Expr, GenError> {
    ctx.enter_scope(Origin::Subquery);

    // Pick a random table
    let table = ctx
        .choose(&generator.schema().tables)
        .ok_or_else(|| GenError::schema_empty("tables"))?;

    // Generate a simple single-column SELECT
    let result = crate::generate::select::generate_simple_select(generator, ctx, table);
    ctx.exit_scope();

    let select = result?;
    Ok(Expr::subquery(ctx, select))
}

/// Generate a WHERE clause condition.
pub fn generate_condition<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
) -> Result<Expr, GenError> {
    ctx.enter_scope(Origin::Where);

    let candidates = build_condition_candidates(generator, ctx);
    let cond_type = generator.policy().select_weighted(ctx, &candidates)?;
    let result = dispatch_condition_generation(generator, ctx, table, cond_type);

    ctx.exit_scope();
    result
}

#[derive(Clone, Copy)]
enum CondType {
    Comparison,
    IsNull,
    Between,
    InList,
    Compound,
}

/// Build condition candidates with their weights.
fn build_condition_candidates<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &Context,
) -> Vec<(CondType, u32)> {
    let weights = &generator.policy().expr_weights;

    let mut candidates = vec![
        (CondType::Comparison, weights.column_ref + weights.literal),
        (CondType::IsNull, weights.is_null),
        (CondType::Between, weights.between),
        (CondType::InList, weights.in_list),
    ];

    // Compound conditions with AND/OR (limited depth to avoid deep nesting)
    if ctx.depth() < 3 {
        candidates.push((CondType::Compound, weights.binary_op));
    }

    candidates
}

/// Dispatch to the appropriate condition generator.
fn dispatch_condition_generation<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
    cond_type: CondType,
) -> Result<Expr, GenError> {
    match cond_type {
        CondType::Comparison => generate_comparison(generator, ctx, table),
        CondType::IsNull => generate_is_null(generator, ctx, table, 0),
        CondType::Between => generate_between(generator, ctx, table, 0),
        CondType::InList => generate_in_list(generator, ctx, table, 0),
        CondType::Compound => generate_compound_condition(generator, ctx, table),
    }
}

/// Generate a simple comparison (column op value).
fn generate_comparison<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
) -> Result<Expr, GenError> {
    let cols: Vec<_> = table.filterable_columns().collect();
    if cols.is_empty() {
        // Fall back to a tautology - create literals before binary_op to avoid borrow issues
        let left = Expr::literal(ctx, Literal::Integer(1));
        let right = Expr::literal(ctx, Literal::Integer(1));
        return Ok(Expr::binary_op(ctx, left, BinOp::Eq, right));
    }

    let col = ctx.choose(&cols).unwrap();
    let op = *ctx.choose(BinOp::comparison()).unwrap();

    let left = Expr::column_ref(ctx, None, col.name.clone());
    let lit = generate_literal(ctx, col.data_type, generator.policy());
    let right = Expr::literal(ctx, lit);

    Ok(Expr::binary_op(ctx, left, op, right))
}

/// Generate a compound condition (AND/OR).
fn generate_compound_condition<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    table: &Table,
) -> Result<Expr, GenError> {
    let compound_weights = &generator.policy().expr_config.compound_op_weights;

    let candidates = [
        (BinOp::And, compound_weights.and),
        (BinOp::Or, compound_weights.or),
    ];

    let op = generator.policy().select_weighted(ctx, &candidates)?;

    let left = generate_comparison(generator, ctx, table)?;
    let right = generate_comparison(generator, ctx, table)?;

    Ok(Expr::binary_op(ctx, left, op, right))
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
}
