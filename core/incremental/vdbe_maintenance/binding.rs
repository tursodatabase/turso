use super::stream::EphemeralDelta;
use rustc_hash::FxHashMap;
use turso_parser::ast::{self, TableInternalId};

use crate::incremental::dag;
use crate::schema::BTreeTable;
use crate::sync::Arc;
use crate::translate::emitter::Resolver;
use crate::translate::expr::{walk_expr_mut, WalkControl};
use crate::translate::plan::{
    ColumnUsedMask, IterationDirection, JoinedTable, Operation, Scan, TableReferences,
};
use crate::turso_assert;
use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::insn::Insn;
use crate::{LimboError, Result};

/// A [`JoinedTable`] scan entry for synthesized maintenance-program bindings.
/// Expressions are already planner-bound, so no SQL name resolution or
/// USING/NATURAL metadata is reconstructed here.
pub(super) fn make_joined_table(
    table: &Arc<BTreeTable>,
    identifier: &str,
    id: TableInternalId,
) -> JoinedTable {
    JoinedTable {
        op: Operation::Scan(Scan::BTreeTable {
            iter_dir: IterationDirection::Forwards,
            index: None,
        }),
        table: crate::schema::Table::BTree(table.clone()),
        identifier: identifier.to_string(),
        internal_id: id,
        join_info: None,
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id: 0,
        indexed: None,
    }
}

pub(super) type BindingRemap = FxHashMap<TableInternalId, TableInternalId>;

pub(super) fn stream_table_references(
    program: &mut ProgramBuilder,
    schema: &dag::StreamSchema,
) -> (TableReferences, BindingRemap) {
    let mut remap = FxHashMap::default();
    let tables = schema
        .bindings
        .iter()
        .map(|binding| {
            let phase_id = program.table_reference_counter.next();
            let previous = remap.insert(binding.logical_id, phase_id);
            turso_assert!(
                previous.is_none(),
                "a stream schema must expose each logical binding exactly once"
            );
            make_joined_table(&binding.table, &binding.identifier, phase_id)
        })
        .collect();
    (TableReferences::new(tables, vec![]), remap)
}

/// Retarget a planner-bound expression to one emitter phase's cursor
/// bindings. This is a mechanical id substitution: identifier spelling,
/// DQS, aliases, and rowid-name resolution never run a second time.
pub(super) fn remap_bound_expr(expr: &ast::Expr, remap: &BindingRemap) -> Result<ast::Expr> {
    let mut bound = expr.clone();
    walk_expr_mut(&mut bound, &mut |node| {
        let logical_id = match node {
            ast::Expr::Column { table, .. } | ast::Expr::RowId { table, .. } => Some(table),
            ast::Expr::Id(_) | ast::Expr::Qualified(_, _) | ast::Expr::DoublyQualified(_, _, _) => {
                return Err(LimboError::InternalError(
                    "maintenance DAG contains an unresolved identifier".to_string(),
                ));
            }
            _ => None,
        };
        if let Some(logical_id) = logical_id {
            *logical_id = *remap.get(logical_id).ok_or_else(|| {
                LimboError::InternalError(
                    "maintenance expression references a binding outside its declared input"
                        .to_string(),
                )
            })?;
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(bound)
}

pub(super) fn seed_ephemeral_stream_cache<'a>(
    program: &mut ProgramBuilder,
    channel: &EphemeralDelta,
    remap: &BindingRemap,
    resolver: &mut Resolver<'a>,
) -> Result<()> {
    resolver.enable_expr_to_reg_cache();
    for (column, stream_expr) in channel.schema.columns.iter().enumerate() {
        let Some(expr) = stream_expr else {
            continue;
        };
        let value_reg = program.alloc_register();
        program.emit_insn(Insn::Column {
            cursor_id: channel.cursor_id,
            column: channel.value_start + column,
            dest: value_reg,
            default: None,
        });
        let bound = remap_bound_expr(expr, remap)?;
        resolver.cache_expr_reg(std::borrow::Cow::Owned(bound), value_reg, false, None);
    }
    turso_assert!(
        channel.binding_rowid_columns.len() == channel.schema.bindings.len(),
        "rowid provenance must have one entry per stream binding",
        {
            "provenance_count": channel.binding_rowid_columns.len(),
            "binding_count": channel.schema.bindings.len()
        }
    );
    for (binding_rowid_column, binding) in channel
        .binding_rowid_columns
        .iter()
        .zip(channel.schema.bindings.iter())
    {
        let Some(binding_rowid_column) = binding_rowid_column else {
            continue;
        };
        let value_reg = program.alloc_register();
        program.emit_insn(Insn::Column {
            cursor_id: channel.cursor_id,
            column: *binding_rowid_column,
            dest: value_reg,
            default: None,
        });
        let phase_id = *remap.get(&binding.logical_id).ok_or_else(|| {
            LimboError::InternalError(
                "rowid stream identity has no phase-local binding".to_string(),
            )
        })?;
        let expr = ast::Expr::RowId {
            database: None,
            table: phase_id,
        };
        resolver.cache_expr_reg(std::borrow::Cow::Owned(expr), value_reg, false, None);
    }
    Ok(())
}
