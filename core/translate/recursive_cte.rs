use crate::alloc::TursoVecExt;
use crate::schema::{Index, IndexColumn};
use crate::sync::Arc;
use crate::translate::collate::CollationSeq;
use crate::translate::eqp::EqpDetail;
use crate::translate::plan::{QueryDestination, RecursiveCteQueueKey};
use crate::translate::result_row::{emit_columns_to_destination, emit_offset};
use crate::vdbe::builder::{CursorType, ProgramBuilder};
use crate::vdbe::insn::Insn;
use crate::vdbe::BranchOffset;
use crate::{emit_explain, LimboError, Result};
use turso_parser::ast::{NullsOrder, SortOrder};

#[derive(Clone, Copy)]
pub(crate) struct RecursiveRuntimeOrder {
    pub(crate) result_column: usize,
    pub(crate) order: SortOrder,
    pub(crate) nulls: Option<NullsOrder>,
    pub(crate) explicit_collation: Option<CollationSeq>,
}

pub(crate) struct RecursiveRuntimeSpec<'a> {
    pub(crate) name: &'a str,
    pub(crate) result_columns: usize,
    pub(crate) union_all: bool,
    pub(crate) comparison_collations: &'a [Option<CollationSeq>],
    pub(crate) queue_order: &'a [RecursiveRuntimeOrder],
}

#[derive(Clone, Copy)]
pub(crate) struct RecursiveRuntimeLimit {
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
}

#[derive(Clone, Copy)]
pub(crate) enum RecursivePhase {
    Seed,
    Step,
}

pub(crate) fn emit_recursive_runtime(
    program: &mut ProgramBuilder,
    spec: &RecursiveRuntimeSpec<'_>,
    destination: &QueryDestination,
    initialize_limit: impl FnOnce(&mut ProgramBuilder, BranchOffset) -> Result<RecursiveRuntimeLimit>,
    mut expose_input: impl FnMut(&mut ProgramBuilder, usize) -> Result<()>,
    mut emit_phase: impl FnMut(&mut ProgramBuilder, RecursivePhase, &QueryDestination) -> Result<()>,
) -> Result<usize> {
    if spec.result_columns == 0
        || spec.comparison_collations.len() != spec.result_columns
        || spec
            .queue_order
            .iter()
            .any(|term| term.result_column >= spec.result_columns)
    {
        return Err(LimboError::InternalError(format!(
            "recursive CTE {} has inconsistent runtime columns",
            spec.name
        )));
    }

    let mut queue_index_columns = crate::alloc::try_vec![]?;
    let mut queue_sort_keys = crate::alloc::try_vec![]?;
    for term in spec.queue_order {
        let default_nulls = match term.order {
            SortOrder::Asc => NullsOrder::First,
            SortOrder::Desc => NullsOrder::Last,
        };
        let nulls_override = term.nulls.filter(|nulls| *nulls != default_nulls);
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
        queue_index_columns.try_push(IndexColumn {
            name: format!("priority-{}", queue_sort_keys.len()),
            order: term.order,
            nulls_order: None,
            pos_in_table: queue_index_columns.len(),
            collation: term
                .explicit_collation
                .or(spec.comparison_collations[term.result_column]),
            default: None,
            expr: None,
        })?;
        queue_sort_keys.try_push(RecursiveCteQueueKey {
            result_column_index: term.result_column,
            nulls_override,
        })?;
    }
    let queue_sort_column_count = queue_index_columns.len();
    queue_index_columns.try_push(IndexColumn::new("sequence", queue_index_columns.len()))?;
    for result_column in 0..spec.result_columns {
        queue_index_columns.try_push(IndexColumn::new(
            format!("result-{result_column}"),
            queue_index_columns.len(),
        ))?;
    }

    let queue_index = Arc::new(Index {
        name: format!("recursive-queue-{}", spec.name),
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
    let queue_cursor = program.alloc_cursor_id(CursorType::BTreeIndex(queue_index.clone()));
    program.emit_insn(Insn::OpenEphemeral {
        cursor_id: queue_cursor,
        is_table: false,
    });
    let seen_rows = if spec.union_all {
        None
    } else {
        let mut columns = crate::alloc::try_vec![]?;
        for (result_column, collation) in spec.comparison_collations.iter().enumerate() {
            columns.try_push(IndexColumn {
                name: format!("distinct-{result_column}"),
                order: SortOrder::Asc,
                nulls_order: None,
                pos_in_table: result_column,
                collation: *collation,
                default: None,
                expr: None,
            })?;
        }
        let index = Arc::new(Index {
            name: format!("recursive-distinct-{}", spec.name),
            table_name: String::new(),
            root_page: 0,
            columns,
            unique: false,
            ephemeral: true,
            has_rowid: false,
            where_clause: None,
            index_method: None,
            on_conflict: None,
        });
        let cursor = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: cursor,
            is_table: false,
        });
        Some((cursor, index))
    };
    let seen_rows_cursor = seen_rows.as_ref().map(|(cursor, _)| *cursor);
    let queue_destination = QueryDestination::RecursiveCteQueue {
        cursor_id: queue_cursor,
        index: queue_index,
        sort_keys: queue_sort_keys,
        seen_rows,
    };

    let done = program.allocate_label();
    let dequeue = program.allocate_label();
    let step = program.allocate_label();
    let limit = initialize_limit(program, done)?;

    emit_explain!(program, true, EqpDetail::RecursiveSetup);
    emit_phase(program, RecursivePhase::Seed, &queue_destination)?;
    program.pop_current_parent_explain();

    program.preassign_label_to_next_insn(dequeue);
    program.emit_insn(Insn::Rewind {
        cursor_id: queue_cursor,
        pc_if_empty: done,
    });
    let queue_columns = queue_sort_column_count + 1 + spec.result_columns;
    let queue_row_start = program.alloc_registers(queue_columns);
    for column in 0..queue_columns {
        program.emit_insn(Insn::Column {
            cursor_id: queue_cursor,
            column,
            dest: queue_row_start + column,
            default: None,
        });
    }
    let result_start = queue_row_start + queue_sort_column_count + 1;
    program.emit_insn(Insn::IdxDelete {
        start_reg: queue_row_start,
        num_regs: queue_columns,
        cursor_id: queue_cursor,
        raise_error_if_no_matching_entry: false,
    });
    expose_input(program, result_start)?;

    emit_offset(program, step, limit.offset);
    emit_columns_to_destination(program, destination, result_start, spec.result_columns)?;
    if let Some(limit) = limit.limit {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit,
            target_pc: done,
        });
    }

    program.preassign_label_to_next_insn(step);
    emit_explain!(program, true, EqpDetail::RecursiveStep);
    emit_phase(program, RecursivePhase::Step, &queue_destination)?;
    program.pop_current_parent_explain();
    program.emit_insn(Insn::Goto { target_pc: dequeue });

    program.preassign_label_to_next_insn(done);
    program.emit_insn(Insn::Close {
        cursor_id: queue_cursor,
    });
    if let Some(cursor_id) = seen_rows_cursor {
        program.emit_insn(Insn::Close { cursor_id });
    }
    Ok(result_start)
}
