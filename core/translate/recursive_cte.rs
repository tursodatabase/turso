use crate::alloc::TursoVecExt;
use crate::schema::{Index, IndexColumn, PseudoCursorType};
use crate::sync::Arc;
use crate::translate::collate::{get_collseq_from_expr, CollationSeq};
use crate::translate::compound_select::{
    emit_program_for_compound_select, set_select_plan_destination,
};
use crate::translate::emitter::{
    init_limit,
    select::{emit_materialized_build_inputs, emit_query},
    Resolver, TranslateCtx,
};
use crate::translate::plan::{Plan, QueryDestination, RecursiveCtePlan, RecursiveCteQueueKey};
use crate::translate::result_row::{emit_columns_to_destination, emit_offset};
use crate::vdbe::builder::{CursorKey, CursorType, ProgramBuilder};
use crate::vdbe::insn::{to_u32, Insn};
use crate::{emit_explain, LimboError, Result};
use turso_parser::ast::{NullsOrder, SortOrder};

pub(crate) fn emit_recursive_cte(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    recursive_cte: &mut RecursiveCtePlan,
) -> Result<usize> {
    let num_result_columns = recursive_cte.initial_query.select_result_columns().len();
    let input_record_reg = program.alloc_register();
    let input_cursor_id = program.alloc_cursor_id_keyed_if_not_exists(
        CursorKey::table(recursive_cte.input_table_id),
        CursorType::Pseudo(PseudoCursorType {
            column_count: num_result_columns,
        }),
    );
    program.emit_insn(Insn::OpenPseudo {
        cursor_id: input_cursor_id,
        content_reg: input_record_reg,
        num_fields: num_result_columns,
    });

    let mut queue_index_columns = crate::alloc::try_vec![]?;
    let mut queue_sort_keys = crate::alloc::try_vec![]?;
    if let Some(queue_order) = &recursive_cte.queue_order {
        for (result_column_index, order, nulls, explicit_collation) in queue_order {
            let default_nulls = match order {
                SortOrder::Asc => NullsOrder::First,
                SortOrder::Desc => NullsOrder::Last,
            };
            let nulls_override = nulls.filter(|nulls| *nulls != default_nulls);
            if nulls_override.is_some() {
                queue_index_columns.try_push(IndexColumn {
                    name: format!("null-rank-{}", queue_sort_keys.len()),
                    order: SortOrder::Asc,
                    nulls_order: None,
                    pos_in_table: queue_index_columns.len(),
                    collation: None,
                    default: None,
                    expr: None,
                })?;
            }
            let collation = explicit_collation.or(recursive_cte_result_column_collation(
                recursive_cte,
                *result_column_index,
            )?);
            queue_index_columns.try_push(IndexColumn {
                name: format!("priority-{}", queue_sort_keys.len()),
                order: *order,
                nulls_order: None,
                pos_in_table: queue_index_columns.len(),
                collation,
                default: None,
                expr: None,
            })?;
            queue_sort_keys.try_push(RecursiveCteQueueKey {
                result_column_index: *result_column_index,
                nulls_override,
            })?;
        }
    }
    let queue_sort_column_count = queue_index_columns.len();
    queue_index_columns.try_push(IndexColumn::new("sequence", queue_index_columns.len()))?;
    for result_column_index in 0..num_result_columns {
        queue_index_columns.try_push(IndexColumn::new(
            format!("result-{result_column_index}"),
            queue_index_columns.len(),
        ))?;
    }

    let queue_index = Arc::new(Index {
        name: format!("recursive-queue-{}", recursive_cte.name),
        table_name: String::new(),
        root_page: 0,
        columns: queue_index_columns,
        unique: true,
        ephemeral: true,
        has_rowid: false,
        where_clause: None,
        index_method: None,
        on_conflict: None,
    });
    let queue_cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(queue_index.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: queue_cursor_id,
        is_table: false,
    });
    let seen_rows = if recursive_cte.union_all {
        None
    } else {
        let mut seen_row_index_columns = crate::alloc::try_vec![]?;
        for result_column_index in 0..num_result_columns {
            seen_row_index_columns.try_push(IndexColumn {
                name: format!("distinct-{result_column_index}"),
                order: SortOrder::Asc,
                nulls_order: None,
                pos_in_table: result_column_index,
                collation: recursive_cte_result_column_collation(
                    recursive_cte,
                    result_column_index,
                )?,
                default: None,
                expr: None,
            })?;
        }
        let seen_row_index = Arc::new(Index {
            name: format!("recursive-distinct-{}", recursive_cte.name),
            table_name: String::new(),
            root_page: 0,
            columns: seen_row_index_columns,
            unique: false,
            ephemeral: true,
            has_rowid: false,
            where_clause: None,
            index_method: None,
            on_conflict: None,
        });
        let cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(seen_row_index.clone()));
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id,
            is_table: false,
        });
        Some((cursor_id, seen_row_index))
    };
    let seen_rows_cursor_id = seen_rows.as_ref().map(|(cursor_id, _)| *cursor_id);
    let queue_destination = QueryDestination::RecursiveCteQueue {
        cursor_id: queue_cursor_id,
        index: queue_index,
        sort_keys: queue_sort_keys,
        seen_rows,
    };

    set_select_plan_destination(&mut recursive_cte.initial_query, &queue_destination);
    set_select_plan_destination(&mut recursive_cte.recursive_query, &queue_destination);

    let recursive_cte_end = program.allocate_label();
    let dequeue_next_row = program.allocate_label();
    let run_recursive_query = program.allocate_label();
    let mut output_limit_context = TranslateCtx::new(program, resolver.fork(), 0, false);
    output_limit_context.label_main_loop_end = Some(recursive_cte_end);
    init_limit(
        program,
        &mut output_limit_context,
        &recursive_cte.limit,
        &recursive_cte.offset,
    )?;

    emit_explain!(program, true, "SETUP".to_owned());
    emit_recursive_cte_query(program, resolver, &mut recursive_cte.initial_query)?;
    program.pop_current_parent_explain();

    program.preassign_label_to_next_insn(dequeue_next_row);
    program.emit_insn(Insn::Rewind {
        cursor_id: queue_cursor_id,
        pc_if_empty: recursive_cte_end,
    });

    let queue_column_count = queue_sort_column_count + 1 + num_result_columns;
    let queue_row_regs = program.alloc_registers(queue_column_count);
    for queue_column_index in 0..queue_column_count {
        program.emit_insn(Insn::Column {
            cursor_id: queue_cursor_id,
            column: queue_column_index,
            dest: queue_row_regs + queue_column_index,
            default: None,
        });
    }
    let result_row_regs = queue_row_regs + queue_sort_column_count + 1;
    program.emit_insn(Insn::IdxDelete {
        start_reg: queue_row_regs,
        num_regs: queue_column_count,
        cursor_id: queue_cursor_id,
        raise_error_if_no_matching_entry: false,
    });

    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(result_row_regs),
        count: to_u32(num_result_columns),
        dest_reg: to_u32(input_record_reg),
        index_name: None,
        affinity_str: None,
    });

    emit_offset(
        program,
        run_recursive_query,
        output_limit_context.reg_offset,
    );
    emit_columns_to_destination(
        program,
        &recursive_cte.query_destination,
        result_row_regs,
        num_result_columns,
    )?;
    if let Some(limit) = output_limit_context.limit_ctx {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit.reg_limit,
            target_pc: recursive_cte_end,
        });
    }

    program.preassign_label_to_next_insn(run_recursive_query);
    emit_explain!(program, true, "RECURSIVE STEP".to_owned());
    emit_recursive_cte_query(program, resolver, &mut recursive_cte.recursive_query)?;
    program.pop_current_parent_explain();
    program.emit_insn(Insn::Goto {
        target_pc: dequeue_next_row,
    });

    program.preassign_label_to_next_insn(recursive_cte_end);
    program.emit_insn(Insn::Close {
        cursor_id: queue_cursor_id,
    });
    if let Some(cursor_id) = seen_rows_cursor_id {
        program.emit_insn(Insn::Close { cursor_id });
    }
    program.emit_insn(Insn::Close {
        cursor_id: input_cursor_id,
    });
    program.result_columns = recursive_cte.initial_query.select_result_columns().to_vec();
    program.reg_result_cols_start = Some(result_row_regs);
    Ok(result_row_regs)
}

fn recursive_cte_result_column_collation(
    recursive_cte: &RecursiveCtePlan,
    result_column_index: usize,
) -> Result<Option<CollationSeq>> {
    let initial_query_collation = recursive_cte_query_result_column_collation(
        &recursive_cte.initial_query,
        result_column_index,
    )?;
    if initial_query_collation.is_some() {
        Ok(initial_query_collation)
    } else {
        recursive_cte_query_result_column_collation(
            &recursive_cte.recursive_query,
            result_column_index,
        )
    }
}

fn recursive_cte_query_result_column_collation(
    query: &Plan,
    result_column_index: usize,
) -> Result<Option<CollationSeq>> {
    match query {
        Plan::Select(select) => {
            let expr = select
                .values
                .first()
                .and_then(|row| row.get(result_column_index))
                .unwrap_or(&select.result_columns[result_column_index].expr);
            get_collseq_from_expr(expr, &select.table_references)
        }
        Plan::CompoundSelect {
            left, right_most, ..
        } => {
            for (select, _) in left {
                let expr = select
                    .values
                    .first()
                    .and_then(|row| row.get(result_column_index))
                    .unwrap_or(&select.result_columns[result_column_index].expr);
                let collation = get_collseq_from_expr(expr, &select.table_references)?;
                if collation.is_some() {
                    return Ok(collation);
                }
            }
            let expr = right_most
                .values
                .first()
                .and_then(|row| row.get(result_column_index))
                .unwrap_or(&right_most.result_columns[result_column_index].expr);
            get_collseq_from_expr(expr, &right_most.table_references)
        }
        Plan::RecursiveCte(_) | Plan::Delete(_) | Plan::Update(_) => Err(
            LimboError::InternalError("recursive CTE query is not a SELECT".to_string()),
        ),
    }
}

fn emit_recursive_cte_query(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    query: &mut Plan,
) -> Result<()> {
    match query {
        Plan::Select(select_plan) => {
            let mut context = TranslateCtx::new(
                program,
                resolver.fork(),
                select_plan.joined_tables().len(),
                false,
            );
            context.materialized_build_inputs =
                emit_materialized_build_inputs(program, &context.resolver, select_plan)?;
            emit_query(program, select_plan, &mut context)?;
            Ok(())
        }
        Plan::CompoundSelect { .. } => {
            emit_program_for_compound_select(program, resolver, query).map(|_| ())
        }
        Plan::RecursiveCte(_) | Plan::Delete(_) | Plan::Update(_) => Err(
            LimboError::InternalError("recursive CTE query is not a SELECT".to_string()),
        ),
    }
}
