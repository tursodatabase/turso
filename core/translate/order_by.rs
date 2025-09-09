use turso_parser::ast::{self, SortOrder};

use crate::{
    schema::PseudoCursorType,
    translate::collate::CollationSeq,
    util::exprs_are_equivalent,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::Insn,
    },
    Result,
};

use super::{
    emitter::{Resolver, TranslateCtx},
    expr::translate_expr,
    plan::{Distinctness, ResultSetColumn, SelectPlan, TableReferences},
    result_row::{emit_offset, emit_result_row_and_limit},
};

// Metadata for handling ORDER BY operations
#[derive(Debug)]
pub struct SortMetadata {
    // cursor id for the Sorter table where the sorted rows are stored
    pub sort_cursor: usize,
    // register where the sorter data is inserted and later retrieved from
    pub reg_sorter_data: usize,
    // We need to emit result columns in the order they are present in the SELECT, but they may not be in the same order in the ORDER BY sorter.
    // This vector holds the indexes of the result columns in the ORDER BY sorter.
    // This vector must be the same length as the result columns.
    pub remappings: Vec<OrderByRemapping>,
}

/// Initialize resources needed for ORDER BY processing
pub fn init_order_by(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    result_columns: &[ResultSetColumn],
    order_by: &[(Box<ast::Expr>, SortOrder)],
    referenced_tables: &TableReferences,
) -> Result<()> {
    let sort_cursor = program.alloc_cursor_id(CursorType::Sorter);
    t_ctx.meta_sort = Some(SortMetadata {
        sort_cursor,
        reg_sorter_data: program.alloc_register(),
        remappings: order_by_deduplicate_result_columns(order_by, result_columns),
    });

    /*
     * Terms of the ORDER BY clause that is part of a SELECT statement may be assigned a collating sequence using the COLLATE operator,
     * in which case the specified collating function is used for sorting.
     * Otherwise, if the expression sorted by an ORDER BY clause is a column,
     * then the collating sequence of the column is used to determine sort order.
     * If the expression is not a column and has no COLLATE clause, then the BINARY collating sequence is used.
     */
    let collations = order_by
        .iter()
        .map(|(expr, _)| match expr.as_ref() {
            ast::Expr::Collate(_, collation_name) => {
                CollationSeq::new(collation_name.as_str()).map(Some)
            }
            ast::Expr::Column { table, column, .. } => {
                let table = referenced_tables.find_table_by_internal_id(*table).unwrap();

                let Some(table_column) = table.get_column_at(*column) else {
                    crate::bail_parse_error!("column index out of bounds");
                };

                Ok(table_column.collation)
            }
            _ => Ok(Some(CollationSeq::default())),
        })
        .collect::<Result<Vec<_>>>()?;
    program.emit_insn(Insn::SorterOpen {
        cursor_id: sort_cursor,
        columns: order_by.len(),
        order: order_by.iter().map(|(_, direction)| *direction).collect(),
        collations,
    });
    Ok(())
}

/// Emits the bytecode for outputting rows from an ORDER BY sorter.
/// This is called when the main query execution loop has finished processing,
/// and we can now emit rows from the ORDER BY sorter.
pub fn emit_order_by(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    plan: &SelectPlan,
) -> Result<()> {
    let order_by = &plan.order_by;
    let result_columns = &plan.result_columns;
    let sort_loop_start_label = program.allocate_label();
    let sort_loop_next_label = program.allocate_label();
    let sort_loop_end_label = program.allocate_label();
    let SortMetadata {
        sort_cursor,
        reg_sorter_data,
        ref remappings,
    } = *t_ctx.meta_sort.as_ref().unwrap();
    let sorter_column_count =
        order_by.len() + remappings.iter().filter(|r| !r.deduplicated).count();

    let pseudo_cursor = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
        column_count: sorter_column_count,
    }));

    program.emit_insn(Insn::OpenPseudo {
        cursor_id: pseudo_cursor,
        content_reg: reg_sorter_data,
        num_fields: sorter_column_count,
    });

    program.emit_insn(Insn::SorterSort {
        cursor_id: sort_cursor,
        pc_if_empty: sort_loop_end_label,
    });
    program.preassign_label_to_next_insn(sort_loop_start_label);

    emit_offset(
        program,
        plan,
        sort_loop_next_label,
        t_ctx.reg_offset,
        &t_ctx.resolver,
    );

    program.emit_insn(Insn::SorterData {
        cursor_id: sort_cursor,
        dest_reg: reg_sorter_data,
        pseudo_cursor,
    });

    // We emit the columns in SELECT order, not sorter order (sorter always has the sort keys first).
    // This is tracked in sort_metadata.remappings.
    let cursor_id = pseudo_cursor;
    let start_reg = t_ctx.reg_result_cols_start.unwrap();
    for i in 0..result_columns.len() {
        let reg = start_reg + i;
        let column_idx = remappings
            .get(i)
            .expect("remapping must exist for all result columns")
            .orderby_sorter_idx;
        program.emit_column_or_rowid(cursor_id, column_idx, reg);
    }

    emit_result_row_and_limit(
        program,
        plan,
        start_reg,
        t_ctx.limit_ctx,
        Some(sort_loop_end_label),
    )?;

    program.resolve_label(sort_loop_next_label, program.offset());
    program.emit_insn(Insn::SorterNext {
        cursor_id: sort_cursor,
        pc_if_next: sort_loop_start_label,
    });
    program.preassign_label_to_next_insn(sort_loop_end_label);

    Ok(())
}

/// Emits the bytecode for inserting a row into an ORDER BY sorter.
pub fn order_by_sorter_insert(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    sort_metadata: &SortMetadata,
    plan: &SelectPlan,
) -> Result<()> {
    let order_by = &plan.order_by;
    let order_by_len = order_by.len();
    let result_columns = &plan.result_columns;
    let result_columns_to_skip_len = sort_metadata
        .remappings
        .iter()
        .filter(|r| r.deduplicated)
        .count();

    // The ORDER BY sorter has the sort keys first, then the result columns.
    let orderby_sorter_column_count =
        order_by_len + result_columns.len() - result_columns_to_skip_len;
    let start_reg = program.alloc_registers(orderby_sorter_column_count);
    for (i, (expr, _)) in order_by.iter().enumerate() {
        let key_reg = start_reg + i;
        translate_expr(
            program,
            Some(&plan.table_references),
            expr,
            key_reg,
            resolver,
        )?;
    }
    let mut cur_reg = start_reg + order_by_len;
    for (i, rc) in result_columns.iter().enumerate() {
        // If the result column is an exact duplicate of a sort key, we skip it.
        if sort_metadata
            .remappings
            .get(i)
            .expect("remapping must exist for all result columns")
            .deduplicated
        {
            continue;
        }
        translate_expr(
            program,
            Some(&plan.table_references),
            &rc.expr,
            cur_reg,
            resolver,
        )?;
        cur_reg += 1;
    }

    // Handle SELECT DISTINCT deduplication
    if let Distinctness::Distinct { ctx } = &plan.distinctness {
        let distinct_ctx = ctx.as_ref().expect("distinct context must exist");

        // For distinctness checking with Insn::Found, we need a contiguous run of registers containing all the result columns.
        // The emitted columns are in the ORDER BY sorter order, which may be different from the SELECT order, and obviously the
        // ORDER BY clause may not have all the result columns.
        // Hence, we need to allocate new registers and Copy from the existing ones to make a contiguous run of registers.
        let mut needs_reordering = false;

        // Check if result columns in sorter are in SELECT order
        let mut prev = None;
        for (select_idx, _rc) in result_columns.iter().enumerate() {
            let sorter_idx = sort_metadata
                .remappings
                .get(select_idx)
                .expect("remapping must exist for all result columns")
                .orderby_sorter_idx;

            if prev.is_some_and(|p| sorter_idx != p + 1) {
                needs_reordering = true;
                break;
            }
            prev = Some(sorter_idx);
        }

        if needs_reordering {
            // Allocate registers for reordered result columns.
            // TODO: it may be possible to optimize this to minimize the number of Insn::Copy we do, but for now
            // we will just allocate a new reg for every result column.
            let reordered_start_reg = program.alloc_registers(result_columns.len());

            for (select_idx, _rc) in result_columns.iter().enumerate() {
                let src_reg = sort_metadata
                    .remappings
                    .get(select_idx)
                    .map(|r| start_reg + r.orderby_sorter_idx)
                    .expect("remapping must exist for all result columns");

                let dst_reg = reordered_start_reg + select_idx;

                program.emit_insn(Insn::Copy {
                    src_reg,
                    dst_reg,
                    extra_amount: 0,
                });
            }

            distinct_ctx.emit_deduplication_insns(
                program,
                result_columns.len(),
                reordered_start_reg,
            );
        } else {
            // Result columns are already in SELECT order, use them directly
            let start_reg = sort_metadata
                .remappings
                .first()
                .map(|r| start_reg + r.orderby_sorter_idx)
                .expect("remapping must exist for all result columns");
            distinct_ctx.emit_deduplication_insns(program, result_columns.len(), start_reg);
        }
    }

    let SortMetadata {
        sort_cursor,
        reg_sorter_data,
        ..
    } = sort_metadata;

    sorter_insert(
        program,
        start_reg,
        orderby_sorter_column_count,
        *sort_cursor,
        *reg_sorter_data,
    );
    Ok(())
}

/// Emits the bytecode for inserting a row into a sorter.
/// This can be either a GROUP BY sorter or an ORDER BY sorter.
pub fn sorter_insert(
    program: &mut ProgramBuilder,
    start_reg: usize,
    column_count: usize,
    cursor_id: usize,
    record_reg: usize,
) {
    program.emit_insn(Insn::MakeRecord {
        start_reg,
        count: column_count,
        dest_reg: record_reg,
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::SorterInsert {
        cursor_id,
        record_reg,
    });
}

#[derive(Debug)]
/// A mapping between a result column and its index in the ORDER BY sorter.
/// ORDER BY columns are emitted first, then the result columns.
/// If a result column is an exact duplicate of a sort key, we skip it.
/// If we skip a result column, we need to keep track which ORDER BY column it matches.
pub struct OrderByRemapping {
    pub orderby_sorter_idx: usize,
    pub deduplicated: bool,
}

/// In case any of the ORDER BY sort keys are exactly equal to a result column, we can skip emitting that result column.
/// If we skip a result column, we need to keep track what index in the ORDER BY sorter the result columns have,
/// because the result columns should be emitted in the SELECT clause order, not the ORDER BY clause order.
pub fn order_by_deduplicate_result_columns(
    order_by: &[(Box<ast::Expr>, SortOrder)],
    result_columns: &[ResultSetColumn],
) -> Vec<OrderByRemapping> {
    let mut result_column_remapping: Vec<OrderByRemapping> = Vec::new();
    let order_by_len = order_by.len();

    let mut i = 0;
    for rc in result_columns.iter() {
        let found = order_by
            .iter()
            .enumerate()
            .find(|(_, (expr, _))| exprs_are_equivalent(expr, &rc.expr));
        if let Some((j, _)) = found {
            result_column_remapping.push(OrderByRemapping {
                orderby_sorter_idx: j,
                deduplicated: true,
            });
        } else {
            // This result column is not a duplicate of any ORDER BY key, so its sorter
            // index comes after all ORDER BY entries (hence the +order_by_len). The
            // counter `i` tracks how many such non-duplicate result columns we've seen.
            result_column_remapping.push(OrderByRemapping {
                orderby_sorter_idx: i + order_by_len,
                deduplicated: false,
            });
            i += 1;
        }
    }

    result_column_remapping
}
