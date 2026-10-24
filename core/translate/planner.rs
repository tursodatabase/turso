use crate::translate::plan::BitSet;
use crate::translate::semantic::hir::{self, HirDocument};
use crate::Result;

/// Valid ways to refer to the rowid of a btree table.
pub const ROWID_STRS: [&str; 3] = ["rowid", "_rowid_", "oid"];

/**
  Returns the earliest point at which a WHERE term can be evaluated.
  For expressions referencing tables, this is the innermost loop that contains a row for each
  table referenced in the expression.
  For expressions not referencing any tables (e.g. constants), this is before the main loop is
  opened, because they do not need any table data.
*/
/// A bitmask representing a set of tables in a query plan.
/// Tables are numbered by their position in the FROM clause.
/// In the bitmask, the first bit is unused so that a mask with all zeros
/// can represent "no tables".
///
/// E.g. table 0 is represented by bit index 1, table 1 by bit index 2, etc.
///
/// Usage in Join Optimization
///
/// In join optimization, [TableMask] is used to:
/// - Generate subsets of tables for dynamic programming in join optimization
/// - Ensure tables are joined in valid orders (e.g., respecting LEFT JOIN order)
///
/// Usage with constraints (WHERE clause)
///
/// [TableMask] helps determine:
/// - Which tables are referenced in a constraint
/// - When a constraint can be applied as a join condition (all referenced tables must be on the left side of the table being joined)
///
/// Note that although [TableReference]s contain an internal ID as well, in join order optimization
/// the [TableMask] refers to the index of the table in the original join order, not the internal ID.
/// This is simply because we want to represent the tables as a contiguous set of bits, and the internal ID
/// might not be contiguous after e.g. subquery unnesting or other transformations.
pub type TableMask = BitSet;

/// Source positions and outer-join facts needed by HIR constraint planning.
pub(crate) trait HirFromScope {
    fn source_position(&self, source: hir::SourceId) -> Option<usize>;
    fn outer_join_may_null_extend(&self, source: hir::SourceId) -> bool;
    fn full_join_may_null_extend(&self, source: hir::SourceId) -> bool;
}

impl HirFromScope for hir::From {
    fn source_position(&self, source: hir::SourceId) -> Option<usize> {
        hir::From::source_position(self, source)
    }

    fn outer_join_may_null_extend(&self, source: hir::SourceId) -> bool {
        hir::From::outer_join_may_null_extend(self, source)
    }

    fn full_join_may_null_extend(&self, source: hir::SourceId) -> bool {
        hir::From::full_join_may_null_extend(self, source)
    }
}

pub(crate) fn table_mask_from_hir_scope(
    document: &HirDocument,
    from: &impl HirFromScope,
    expression: &hir::Expr,
) -> Result<TableMask> {
    table_mask_from_hir_positions(document, expression, |source| from.source_position(source))
}

/// Return direct FROM positions for tests and parser-shaped HIR callers.
/// Sources captured from an outer query are absent from the local mask.
#[cfg(test)]
pub(crate) fn table_mask_from_hir_expr(
    document: &HirDocument,
    from: Option<&hir::From>,
    expression: &hir::Expr,
) -> Result<TableMask> {
    table_mask_from_hir_positions(document, expression, |source| {
        from.and_then(|from| from.source_position(source))
    })
}

fn table_mask_from_hir_positions(
    document: &HirDocument,
    expression: &hir::Expr,
    position: impl Fn(hir::SourceId) -> Option<usize>,
) -> Result<TableMask> {
    let mut mask = TableMask::default();
    let mut allocation_error = None;

    document.visit_expr_sources(expression, &mut |source| {
        let Some(position) = position(source) else {
            return;
        };
        if allocation_error.is_none() {
            allocation_error = mask.set(position).err();
        }
    });

    if let Some(error) = allocation_error {
        return Err(error.into());
    }
    Ok(mask)
}
