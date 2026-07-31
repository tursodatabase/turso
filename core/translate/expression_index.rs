use crate::schema_expr::SchemaExpr;
use crate::translate::plan::ColumnUsedMask;
use crate::translate::plan_expr::{plan_expr_dependencies, PlanColumnUse, PlanExpr, PlanSourceId};
use crate::Result;

/// Determine whether an expression reads columns from exactly one plan source
/// and, if so, which table columns it reads.
///
/// Semantic analysis has already resolved every column to a source identity.
/// This helper therefore only inspects identities; it never searches names or
/// parser syntax.
pub fn single_table_column_usage(
    expr: &PlanExpr,
) -> Result<Option<(PlanSourceId, ColumnUsedMask)>> {
    let dependencies = plan_expr_dependencies(expr)?;
    if !dependencies.outputs.is_empty() || !dependencies.subqueries.is_empty() {
        return Ok(None);
    }

    let mut sources = dependencies.sources();
    let Some(source) = sources.next() else {
        return Ok(None);
    };
    if sources.any(|candidate| candidate != source) {
        return Ok(None);
    }

    let mut columns = ColumnUsedMask::default();
    for &(candidate, usage) in &dependencies.source_uses {
        if candidate != source {
            return Ok(None);
        }
        if let PlanColumnUse::Column(column) = usage {
            columns.set(column)?;
        }
    }
    Ok(Some((source, columns)))
}

/// Return the set of owning-table columns read by a stored index key.
///
/// Index expressions are positional [`SchemaExpr`] values, so this is a
/// native dependency query with no binding or parser-AST compatibility path.
pub fn expression_index_column_usage(expr: &SchemaExpr) -> Result<ColumnUsedMask> {
    let mut mask = ColumnUsedMask::default();
    for &column in expr.as_valid()?.dependencies().columns() {
        mask.set(column)?;
    }
    Ok(mask)
}
