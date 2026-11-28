use crate::alloc::TursoVecExt;
use crate::ast::{NullsOrder, SortOrder};
use crate::schema::{BTreeCharacteristics, BTreeTable, Index, IndexColumn, PseudoCursorType};
use crate::sync::Arc;
use crate::translate::collate::{first_collation, CollationSeq};
use crate::translate::compound_select::{
    emit_program_for_compound_select, set_select_plan_destination,
};
use crate::translate::emitter::{
    init_limit,
    select::{emit_materialized_build_inputs, emit_query},
    Resolver, TranslateCtx,
};
use crate::translate::eqp::EqpDetail;
use crate::translate::expr_context::ExprContext;
use crate::translate::plan::{
    Plan, QueryDestination, RecursiveCtePlan, RecursiveCteQueue, RecursiveCteQueueKey, SelectPlan,
};
use crate::translate::result_row::{emit_columns_to_destination, emit_offset};
use crate::vdbe::builder::{CursorKey, CursorType, ProgramBuilder};
use crate::vdbe::insn::{to_u32, Insn};
use crate::vdbe::CursorID;
use crate::{emit_explain, LimboError, Result};

/// Emits a recursive CTE, and returns the first register of its result columns.
///
/// This function is on the stack one time for each level of nested recursive
/// CTEs. The cursors are opened in [open_recursive_cte_cursors], which is not
/// inlined, so that its values are not in this frame while the queries of the
/// CTE are emitted.
pub(crate) fn emit_recursive_cte(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    recursive_cte: &mut RecursiveCtePlan,
) -> Result<usize> {
    let RecursiveCteCursors {
        num_result_columns,
        input_record_reg,
        input_cursor_id,
        queue_cursor_id,
        queue,
        seen_rows_cursor_id,
    } = open_recursive_cte_cursors(program, resolver, recursive_cte)?;

    let recursive_cte_end = program.allocate_label();
    let dequeue_next_row = program.allocate_label();
    let run_recursive_query = program.allocate_label();
    let mut output_limit_context = TranslateCtx::new_boxed(program, resolver.fork(), 0, false);
    output_limit_context.label_main_loop_end = Some(recursive_cte_end);
    init_limit(
        program,
        &mut output_limit_context,
        &recursive_cte.limit,
        &recursive_cte.offset,
        recursive_cte_end,
    )?;

    emit_explain!(program, true, EqpDetail::RecursiveSetup);
    emit_recursive_cte_query(program, resolver, &mut recursive_cte.initial_query)?;
    program.pop_current_parent_explain();

    program.preassign_label_to_next_insn(dequeue_next_row);
    program.emit_insn(Insn::Rewind {
        cursor_id: queue_cursor_id,
        pc_if_empty: recursive_cte_end,
    });

    let result_row_regs = match &queue {
        RecursiveCteQueue::Fifo { table } => {
            let result_row_regs = program.alloc_registers(num_result_columns);
            // The queued record is the row itself, so it becomes the content
            // of the input cursor without being decoded and encoded again.
            program.emit_insn(Insn::RowData {
                cursor_id: queue_cursor_id,
                dest: input_record_reg,
            });
            program.emit_insn(Insn::Delete {
                cursor_id: queue_cursor_id,
                table_name: table.name.clone(),
                is_part_of_update: false,
            });
            for result_column_index in 0..num_result_columns {
                program.emit_insn(Insn::Column {
                    cursor_id: input_cursor_id,
                    column: result_column_index,
                    dest: result_row_regs + result_column_index,
                    default: None,
                });
            }
            result_row_regs
        }
        RecursiveCteQueue::Ordered { index, .. } => {
            let queue_column_count = index.columns.len();
            let queue_row_regs = program.alloc_registers(queue_column_count);
            for queue_column_index in 0..queue_column_count {
                program.emit_insn(Insn::Column {
                    cursor_id: queue_cursor_id,
                    column: queue_column_index,
                    dest: queue_row_regs + queue_column_index,
                    default: None,
                });
            }
            program.emit_insn(Insn::IdxDelete {
                start_reg: queue_row_regs,
                num_regs: queue_column_count,
                cursor_id: queue_cursor_id,
                raise_error_if_no_matching_entry: false,
            });
            // The result columns follow the sort keys and the sequence number.
            let result_row_regs = queue_row_regs + queue_column_count - num_result_columns;
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(result_row_regs),
                count: to_u32(num_result_columns),
                dest_reg: to_u32(input_record_reg),
                index_name: None,
                affinity_str: None,
            });
            result_row_regs
        }
    };

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
    emit_explain!(program, true, EqpDetail::RecursiveStep);
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

/// The cursors and registers that a recursive CTE reads and writes.
struct RecursiveCteCursors {
    num_result_columns: usize,
    input_record_reg: usize,
    input_cursor_id: CursorID,
    queue_cursor_id: CursorID,
    queue: RecursiveCteQueue,
    seen_rows_cursor_id: Option<CursorID>,
}

/// Opens the input cursor, the queue and the index of seen rows of a recursive
/// CTE, and makes both of its queries write to the queue.
#[inline(never)]
fn open_recursive_cte_cursors(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    recursive_cte: &mut RecursiveCtePlan,
) -> Result<RecursiveCteCursors> {
    let num_result_columns = recursive_cte.initial_query.select_result_columns().len();
    // Computed for UNION ALL too, which compares no rows: each column is one
    // result of both queries, so this fails when they give it two types that
    // do not compare, as for a compound SELECT.
    let collations = (0..num_result_columns)
        .map(|column| recursive_cte_result_column_collation(recursive_cte, column, resolver))
        .collect::<Result<Vec<_>>>()?;
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

    let queue_name = format!("recursive-queue-{}", recursive_cte.name);
    let (queue_cursor_id, queue) = match &recursive_cte.queue_order {
        None => {
            let table = Arc::new(BTreeTable::new(
                0,
                queue_name,
                crate::alloc::vec![],
                crate::alloc::vec![],
                BTreeCharacteristics::HAS_ROWID,
                crate::alloc::vec![],
                crate::alloc::vec![],
                crate::alloc::vec![],
                None,
            ));
            let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table.clone()));
            program.emit_insn(Insn::OpenEphemeral {
                cursor_id,
                is_table: true,
            });
            (cursor_id, RecursiveCteQueue::Fifo { table })
        }
        Some(queue_order) => {
            let mut queue_index_columns = crate::alloc::try_vec![]?;
            let mut queue_sort_keys = crate::alloc::try_vec![]?;
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
                        collation: CollationSeq::BINARY,
                        explicit_collation: None,
                        default: None,
                        expr: None,
                    })?;
                }
                let collation = match explicit_collation {
                    Some(collation) => *collation,
                    None => collations[*result_column_index],
                };
                queue_index_columns.try_push(IndexColumn {
                    name: format!("priority-{}", queue_sort_keys.len()),
                    order: *order,
                    nulls_order: None,
                    pos_in_table: queue_index_columns.len(),
                    collation,
                    explicit_collation: None,
                    default: None,
                    expr: None,
                })?;
                queue_sort_keys.try_push(RecursiveCteQueueKey {
                    result_column_index: *result_column_index,
                    nulls_override,
                })?;
            }
            queue_index_columns.try_push(IndexColumn::new(
                "sequence",
                queue_index_columns.len(),
                CollationSeq::BINARY,
            ))?;
            for result_column_index in 0..num_result_columns {
                queue_index_columns.try_push(IndexColumn::new(
                    format!("result-{result_column_index}"),
                    queue_index_columns.len(),
                    CollationSeq::BINARY,
                ))?;
            }
            let index = Arc::new(Index {
                declared_name: queue_name.clone(),
                name: queue_name,
                table_name: String::new(),
                root_page: 0,
                columns: queue_index_columns,
                unique: true,
                ephemeral: true,
                has_rowid: false,
                where_clause: None,
                index_method: None,
                on_conflict: None,
                constraint_name: None,
            });
            let cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
            program.emit_insn(Insn::OpenEphemeral {
                cursor_id,
                is_table: false,
            });
            (
                cursor_id,
                RecursiveCteQueue::Ordered {
                    index,
                    sort_keys: queue_sort_keys,
                },
            )
        }
    };
    let seen_rows = if recursive_cte.union_all {
        None
    } else {
        let mut seen_row_index_columns = crate::alloc::try_vec![]?;
        for (result_column_index, collation) in collations.iter().enumerate() {
            seen_row_index_columns.try_push(IndexColumn {
                name: format!("distinct-{result_column_index}"),
                order: SortOrder::Asc,
                nulls_order: None,
                pos_in_table: result_column_index,
                collation: *collation,
                explicit_collation: None,
                default: None,
                expr: None,
            })?;
        }
        let seen_row_index = Arc::new(Index {
            declared_name: format!("recursive-distinct-{}", recursive_cte.name),
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
            constraint_name: None,
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
        queue: queue.clone(),
        seen_rows,
    };

    set_select_plan_destination(&mut recursive_cte.initial_query, &queue_destination);
    set_select_plan_destination(&mut recursive_cte.recursive_query, &queue_destination);
    Ok(RecursiveCteCursors {
        num_result_columns,
        input_record_reg,
        input_cursor_id,
        queue_cursor_id,
        queue,
        seen_rows_cursor_id,
    })
}

/// The collation of a result column of a recursive CTE: that of the first
/// SELECT, initial query first, whose column has one.
fn recursive_cte_result_column_collation(
    recursive_cte: &RecursiveCtePlan,
    result_column_index: usize,
    resolver: &Resolver,
) -> Result<CollationSeq> {
    let mut columns = Vec::new();
    for query in [&recursive_cte.initial_query, &recursive_cte.recursive_query] {
        let selects: Vec<&SelectPlan> = match query.as_ref() {
            Plan::Select(select) => vec![select.as_ref()],
            Plan::CompoundSelect {
                left, right_most, ..
            } => left
                .iter()
                .map(|(select, _)| select)
                .chain([right_most.as_ref()])
                .collect(),
            Plan::RecursiveCte(_) | Plan::Delete(_) | Plan::Update(_) => {
                return Err(LimboError::InternalError(
                    "recursive CTE query is not a SELECT".to_string(),
                ))
            }
        };
        for select in selects {
            let context = ExprContext::new(Some(&select.table_references), resolver);
            columns.extend(
                select
                    .column_exprs(result_column_index)
                    .map(|expr| (expr, context)),
            );
        }
    }
    first_collation(columns)
}

fn emit_recursive_cte_query(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    query: &mut Plan,
) -> Result<()> {
    match query {
        Plan::Select(select_plan) => {
            let mut context = TranslateCtx::new_boxed(
                program,
                resolver.fork_with_expr_cache(),
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
