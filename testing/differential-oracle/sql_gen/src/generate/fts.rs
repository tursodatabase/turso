//! FTS-specific expression generation.

use crate::SqlGen;
use crate::ast::{ColumnRef, Expr, FtsMatchSyntax, Literal};
use crate::capabilities::Capabilities;
use crate::context::{Context, ScopedTable};
use crate::error::GenError;
use crate::schema::{DataType, Index};

#[derive(Clone)]
struct FtsTarget {
    columns: Vec<ColumnRef>,
}

pub fn has_fts_index_in_scope<C: Capabilities>(generator: &SqlGen<C>, ctx: &Context) -> bool {
    !fts_targets_in_scope(generator, ctx).is_empty()
}

pub fn generate_fts_match_expr<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Expr, GenError> {
    let target = choose_fts_target(generator, ctx)?;
    let syntax = generate_match_syntax(generator, ctx)?;
    let query = generate_fts_query_arg(generator, ctx, &target)?;
    Ok(Expr::fts_match(ctx, target.columns, query, syntax))
}

pub fn generate_fts_score_expr<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<Expr, GenError> {
    let target = choose_fts_target(generator, ctx)?;
    let mut args: Vec<Expr> = target
        .columns
        .iter()
        .map(|col| Expr::ColumnRef(col.clone()))
        .collect();
    args.push(generate_fts_query_arg(generator, ctx, &target)?);
    Ok(Expr::function_call(ctx, "fts_score".to_string(), args))
}

fn choose_fts_target<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<FtsTarget, GenError> {
    let targets = fts_targets_in_scope(generator, ctx);
    ctx.choose(&targets)
        .cloned()
        .ok_or_else(|| GenError::exhausted("fts", "no FTS index visible in query scope"))
}

fn fts_targets_in_scope<C: Capabilities>(generator: &SqlGen<C>, ctx: &Context) -> Vec<FtsTarget> {
    ctx.tables_in_scope()
        .iter()
        .flat_map(|scoped| fts_targets_for_scoped_table(generator, ctx, scoped))
        .collect()
}

fn fts_targets_for_scoped_table<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &Context,
    scoped: &ScopedTable,
) -> Vec<FtsTarget> {
    generator
        .schema()
        .indexes_for_table_in_database(&scoped.table.name, scoped.table.database.as_deref())
        .into_iter()
        .filter(|idx| idx.is_fts())
        .map(|idx| FtsTarget {
            columns: fts_column_refs(ctx, scoped, idx),
        })
        .filter(|target| !target.columns.is_empty())
        .collect()
}

fn fts_column_refs(ctx: &Context, scoped: &ScopedTable, index: &Index) -> Vec<ColumnRef> {
    let qualifier = if ctx.has_multiple_tables() {
        Some(scoped.qualifier.clone())
    } else {
        None
    };

    index
        .columns
        .iter()
        .filter(|name| scoped.table.columns.iter().any(|col| col.name == **name))
        .map(|column| ColumnRef {
            table: qualifier.clone(),
            column: column.clone(),
        })
        .collect()
}

fn generate_match_syntax<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
) -> Result<FtsMatchSyntax, GenError> {
    let fts_config = &generator.policy().fts_config;
    match ctx.weighted_index(&[
        fts_config.match_function_weight,
        fts_config.tuple_match_weight,
    ]) {
        Some(0) => Ok(FtsMatchSyntax::Function),
        Some(1) => Ok(FtsMatchSyntax::Match),
        _ => Err(GenError::exhausted(
            "fts_match_syntax",
            "all FTS match syntax weights are zero",
        )),
    }
}

fn generate_fts_query_arg<C: Capabilities>(
    generator: &SqlGen<C>,
    ctx: &mut Context,
    target: &FtsTarget,
) -> Result<Expr, GenError> {
    let fts_config = &generator.policy().fts_config;
    match ctx.weighted_index(&[
        fts_config.query_text_weight,
        fts_config.query_field_filter_weight,
        fts_config.query_weird_arg_weight,
        fts_config.query_column_arg_weight,
    ]) {
        Some(0) => {
            let query = generate_fts_query_text(ctx);
            Ok(Expr::literal(ctx, Literal::Text(query)))
        }
        Some(1) => {
            let query = generate_fts_field_filter_query(ctx, target)?;
            Ok(Expr::literal(ctx, Literal::Text(query)))
        }
        Some(2) => Ok(generate_weird_fts_query_arg(ctx)),
        Some(3) => Ok(
            generate_fts_query_column_arg(ctx, target).unwrap_or_else(|| {
                let query = generate_fts_query_text(ctx);
                Expr::literal(ctx, Literal::Text(query))
            }),
        ),
        _ => Err(GenError::exhausted(
            "fts_query_arg",
            "all FTS query argument weights are zero",
        )),
    }
}

pub fn generate_fts_query_text(ctx: &mut Context) -> String {
    generate_fts_query_expr(ctx, 0)
}

pub fn generate_fts_document_text(ctx: &mut Context) -> String {
    let term_count = 1 + ctx.gen_range(8);
    let mut text = Vec::with_capacity(term_count);
    for _ in 0..term_count {
        text.push(generate_fts_term(ctx));
    }
    text.join(" ")
}

fn generate_fts_query_column_arg(ctx: &mut Context, target: &FtsTarget) -> Option<Expr> {
    let mut external_columns = Vec::new();
    let mut target_columns = Vec::new();

    for scoped in ctx.tables_in_scope() {
        let qualifier = ctx.has_multiple_tables().then(|| scoped.qualifier.clone());
        for column in scoped
            .table
            .columns
            .iter()
            .filter(|column| column.data_type == DataType::Text)
        {
            let column_ref = ColumnRef {
                table: qualifier.clone(),
                column: column.name.clone(),
            };
            if target.columns.iter().any(|target_column| {
                target_column.table == column_ref.table && target_column.column == column_ref.column
            }) {
                target_columns.push(column_ref);
            } else {
                external_columns.push(column_ref);
            }
        }
    }

    let candidates = if external_columns.is_empty() {
        &target_columns
    } else {
        &external_columns
    };
    ctx.choose(candidates).cloned().map(Expr::ColumnRef)
}

fn generate_fts_field_filter_query(
    ctx: &mut Context,
    target: &FtsTarget,
) -> Result<String, GenError> {
    let column = ctx
        .choose(&target.columns)
        .ok_or_else(|| GenError::exhausted("fts_field_filter", "FTS target has no columns"))?
        .column
        .clone();
    Ok(format!("{}:{}", column, generate_fts_query_text(ctx)))
}

fn generate_fts_query_expr(ctx: &mut Context, depth: usize) -> String {
    if depth >= 2 {
        return generate_fts_query_atom(ctx);
    }

    match ctx.gen_range(8) {
        0 | 1 => generate_fts_query_atom(ctx),
        2 => format!("\"{} {}\"", generate_fts_term(ctx), generate_fts_term(ctx)),
        3 => format!(
            "{} AND {}",
            generate_fts_query_expr(ctx, depth + 1),
            generate_fts_query_expr(ctx, depth + 1)
        ),
        4 => format!(
            "{} OR {}",
            generate_fts_query_expr(ctx, depth + 1),
            generate_fts_query_expr(ctx, depth + 1)
        ),
        5 => format!("NOT {}", generate_fts_query_expr(ctx, depth + 1)),
        6 => format!("({})", generate_fts_query_expr(ctx, depth + 1)),
        _ => format!("{}*", generate_fts_term(ctx)),
    }
}

fn generate_fts_query_atom(ctx: &mut Context) -> String {
    match ctx.gen_range(5) {
        0 => generate_fts_term(ctx),
        1 => generate_fts_term(ctx).to_uppercase(),
        2 => format!("{}{}", generate_fts_term(ctx), ctx.gen_range(10)),
        3 => "missing".to_string(),
        _ => format!("{}-{}", generate_fts_term(ctx), generate_fts_term(ctx)),
    }
}

fn generate_fts_term(ctx: &mut Context) -> String {
    const TERMS: &[&str] = &[
        "database", "rust", "search", "full", "text", "index", "query", "token", "row", "commit",
        "rollback", "optimize",
    ];
    ctx.choose(TERMS).copied().unwrap_or("database").to_string()
}

fn generate_weird_fts_query_arg(ctx: &mut Context) -> Expr {
    match ctx.gen_range(4) {
        0 => Expr::literal(ctx, Literal::Null),
        1 => {
            let value = ctx.gen_i64_range(-4, 16);
            Expr::literal(ctx, Literal::Integer(value))
        }
        2 => {
            let value = ctx.gen_f64_range(-2.0, 4.0);
            Expr::literal(ctx, Literal::Real(value))
        }
        _ => Expr::literal(ctx, Literal::Blob(vec![0xde, 0xad])),
    }
}
