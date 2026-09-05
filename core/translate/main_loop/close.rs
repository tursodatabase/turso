use super::*;
use crate::translate::main_loop::hash::{
    emit_hash_join_unmatched_build_rows, GraceHashLoop, HashProbeCloseEmitter,
};
use crate::translate::main_loop::open::{
    emit_materialized_subquery_result_columns, emit_right_join_key, emit_virtual_table_scan_start,
};
use crate::translate::subquery::emit_non_from_clause_subquery;

/// Set every source left of a right-preserving join to its NULL-row state.
///
/// The unmatched-right-row subroutine reuses the normal result body. These NULL
/// states make that body produce the same row shape as SQLite.
fn set_left_sources_to_null(
    program: &mut ProgramBuilder,
    tables: &TableReferences,
    right_table_index: usize,
    mode: &OperationMode,
) -> Result<()> {
    for table in tables.joined_tables().iter().take(right_table_index) {
        let (table_cursor_id, index_cursor_id) = table.resolve_cursors(program, mode.clone())?;
        emit_null_row_for_source(program, table, table_cursor_id, index_cursor_id);
    }
    Ok(())
}

/// Emit SQLite's null-row steps for one outer-join source.
///
/// A subquery can read cached result registers, so SQLite clears those too.
/// `NullRow` leaves a recursive pseudo-row unchanged in both engines.
fn emit_null_row_for_source(
    program: &mut ProgramBuilder,
    table: &JoinedTable,
    table_cursor_id: Option<CursorID>,
    index_cursor_id: Option<CursorID>,
) {
    match &table.table {
        Table::FromClauseSubquery(subquery) => {
            if let Some(start_reg) = subquery.result_columns_start_reg {
                if !subquery.columns.is_empty() {
                    program.emit_insn(Insn::Null {
                        dest: start_reg,
                        dest_end: Some(start_reg + subquery.columns.len() - 1),
                    });
                }
            }
        }
        Table::BTree(_) | Table::Virtual(_) | Table::RecursiveCteInput(_) => {}
    }
    for cursor_id in [table_cursor_id, index_cursor_id].into_iter().flatten() {
        program.emit_insn(Insn::NullRow { cursor_id });
    }
}

/// Emit each right-side row that the main join loop did not match.
///
/// Restartable sources use their unmatched-right read and check the stored rowid set.
/// A recursive CTE exposes only its current pseudo-row, so it uses a direct check.
fn emit_unmatched_right_rows(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'_>,
    tables: &TableReferences,
    table_index: usize,
    right_join: &RightJoinMetadata,
    mode: &OperationMode,
) -> Result<()> {
    set_left_sources_to_null(program, tables, table_index, mode)?;

    let main_table = &tables.joined_tables()[table_index];
    let (table_cursor_id, main_index_cursor_id) =
        main_table.resolve_cursors(program, mode.clone())?;
    let table_cursor_id =
        table_cursor_id.expect("a right-preserving join must keep its table cursor open");

    if matches!(main_table.table, Table::RecursiveCteInput(_)) {
        // SQLite checks the current recursive row directly because a pseudo-row cannot rewind.
        let scan_end = program.allocate_label();
        emit_right_join_key(program, right_join, table_cursor_id, None);
        // A pseudo-row has a NULL rowid. Turso's bloom filter rejects NULL keys,
        // but the exact ephemeral-index lookup supports them and decides the result.
        program.emit_insn(Insn::Found {
            cursor_id: right_join.matched_rows_cursor_id,
            target_pc: scan_end,
            record_reg: right_join.rowid_reg,
            num_regs: 1,
        });
        program.emit_insn(Insn::Gosub {
            target_pc: right_join.body_label,
            return_reg: right_join.return_reg,
        });
        program.preassign_label_to_next_insn(scan_end);
        return Ok(());
    }

    // A source without a separate operation uses its default scan.
    let UnmatchedRightRowsPlan {
        operation: mut unmatched_rows_operation,
        subqueries: mut unmatched_rows_subqueries,
        conditions: mut unmatched_rows_conditions,
    } = main_table
        .unmatched_right_rows_plan
        .clone()
        .unwrap_or_else(|| UnmatchedRightRowsPlan {
            operation: Operation::default_scan_for(&main_table.table),
            subqueries: Vec::new(),
            conditions: Vec::new(),
        });

    // Result expressions already refer to the main index cursor. SQLite leaves
    // that cursor null while its separate right-side read uses another cursor.
    if let Some(main_index_cursor_id) = main_index_cursor_id {
        program.emit_insn(Insn::NullRow {
            cursor_id: main_index_cursor_id,
        });
    }

    // SQLite's nested planner allocates new cursors for this read. Allocate one
    // new cursor for each index in Turso's stored operation.
    let mut unmatched_indexes = unmatched_rows_operation
        .index()
        .cloned()
        .into_iter()
        .collect::<Vec<_>>();
    if let Operation::MultiIndexScan(multi_index) = &unmatched_rows_operation {
        unmatched_indexes.extend(
            multi_index
                .branches
                .iter()
                .filter_map(|branch| branch.index.clone()),
        );
    }
    let mut unmatched_index_cursors: Vec<(Arc<Index>, CursorID)> = Vec::new();
    for index in unmatched_indexes {
        if unmatched_index_cursors
            .iter()
            .any(|(other_index, _)| other_index.name == index.name)
        {
            continue;
        }
        // Keep the key as cursor metadata for EXPLAIN. The temporary mapping
        // below selects this new cursor when the main read has the same key.
        let cursor_id = program.alloc_cursor_index(
            Some(CursorKey::index(main_table.internal_id, index.clone())),
            &index,
        )?;
        unmatched_index_cursors.push((index, cursor_id));
    }
    let index_cursor_id = unmatched_rows_operation.index().map(|index| {
        unmatched_index_cursors
            .iter()
            .find(|(candidate, _)| candidate.name == index.name)
            .map(|(_, cursor_id)| *cursor_id)
            .expect("the unmatched-right index must have a cursor")
    });
    let cursor_overrides = unmatched_index_cursors
        .iter()
        .map(|(index, cursor_id)| {
            (
                CursorKey::index(main_table.internal_id, index.clone()),
                *cursor_id,
            )
        })
        .collect::<Vec<_>>();

    // SQLite plans this read with one FROM table and no join type. Turso stores
    // resolved cursor and expression-index information in this table list.
    // Bound expressions can still read earlier cursors and their expression indexes.
    // The join types also identify earlier cursors that contain NULL rows. Thus,
    // this copy keeps all tables and join types. It changes only the operation
    // for the right table. Cursor overrides select the new index cursor.
    let mut unmatched_tables = tables.clone();
    unmatched_tables.joined_tables_mut()[table_index].op = unmatched_rows_operation.clone();
    let tables = &unmatched_tables;
    let table = &tables.joined_tables()[table_index];

    match &table.table {
        Table::BTree(btree) => {
            program.emit_insn(Insn::OpenRead {
                cursor_id: table_cursor_id,
                root_page: btree.root_page,
                db: table.database_id,
            });
            for (index, index_cursor_id) in &unmatched_index_cursors {
                program.emit_insn(Insn::OpenRead {
                    cursor_id: *index_cursor_id,
                    root_page: index.root_page,
                    db: table.database_id,
                });
            }
        }
        Table::Virtual(_) => {
            // SQLite opens a new virtual cursor because the main loop leaves
            // the first cursor at its end.
            program.emit_insn(Insn::VOpen {
                cursor_id: table_cursor_id,
            });
        }
        Table::FromClauseSubquery(subquery) if subquery.materialized_cursor_id.is_some() => {}
        _ => {
            // Turso cannot restart a coroutine for this pass.
            return Err(crate::LimboError::InternalError(
                "right-preserving joins need a restartable right source".to_string(),
            ));
        }
    }
    let scan_start = program.allocate_label();
    let scan_end = program.allocate_label();
    let next_row = program.allocate_label();
    let emit_row = program.allocate_label();

    let mut copied_subqueries = Vec::with_capacity(unmatched_rows_subqueries.len());
    for mut subquery in unmatched_rows_subqueries.drain(..) {
        let old_query_type = subquery.query_type.clone();
        let query_type = assign_new_subquery_output(program, &mut subquery);
        let operation_uses_subquery = replace_subquery_output(
            &mut unmatched_rows_operation,
            &mut unmatched_rows_conditions,
            subquery.internal_id,
            &old_query_type,
            &query_type,
        )?;
        copied_subqueries.push((subquery, operation_uses_subquery));
    }

    // A subquery used by the table read must run before that read starts.
    // This includes table-function arguments and values used by a seek.
    for (subquery, operation_uses_subquery) in &mut copied_subqueries {
        if *operation_uses_subquery {
            emit_unmatched_rows_subquery(
                program,
                &t_ctx.resolver,
                subquery,
                table_index,
                &cursor_overrides,
            )?;
        }
    }

    match &unmatched_rows_operation {
        Operation::Scan(Scan::BTreeTable { iter_dir, .. }) => {
            let scan_cursor_id = index_cursor_id.unwrap_or(table_cursor_id);
            if *iter_dir == IterationDirection::Backwards {
                program.emit_insn(Insn::Last {
                    cursor_id: scan_cursor_id,
                    pc_if_empty: scan_end,
                });
            } else {
                program.emit_insn(Insn::Rewind {
                    cursor_id: scan_cursor_id,
                    pc_if_empty: scan_end,
                });
            }
            program.preassign_label_to_next_insn(scan_start);
            if let Some(index_cursor_id) = index_cursor_id {
                program.emit_deferred_seek(index_cursor_id, table_cursor_id);
            }
        }
        Operation::Scan(Scan::Subquery { iter_dir }) => {
            if *iter_dir == IterationDirection::Backwards {
                program.emit_insn(Insn::Last {
                    cursor_id: table_cursor_id,
                    pc_if_empty: scan_end,
                });
            } else {
                program.emit_insn(Insn::Rewind {
                    cursor_id: table_cursor_id,
                    pc_if_empty: scan_end,
                });
            }
            program.preassign_label_to_next_insn(scan_start);
        }
        Operation::Scan(Scan::VirtualTable {
            idx_num,
            idx_str,
            constraints,
        }) => {
            program.with_cursor_overrides(&cursor_overrides, |program| {
                emit_virtual_table_scan_start(
                    program,
                    tables,
                    &t_ctx.resolver,
                    table_cursor_id,
                    *idx_num,
                    idx_str.as_deref(),
                    constraints,
                    scan_start,
                    scan_end,
                )
            })?;
        }
        Operation::Search(Search::RowidEq { cmp_expr }) => {
            let src_reg = program.alloc_register();
            program.with_cursor_overrides(&cursor_overrides, |program| {
                translate_expr(program, Some(tables), cmp_expr, src_reg, &t_ctx.resolver)
            })?;
            program.emit_insn(Insn::SeekRowid {
                cursor_id: table_cursor_id,
                src_reg,
                target_pc: scan_end,
            });
        }
        Operation::Search(Search::Seek {
            index, seek_def, ..
        }) => {
            let seek_cursor_id = index_cursor_id.unwrap_or(table_cursor_id);
            let max_registers = seek_def
                .size(&seek_def.start)
                .max(seek_def.size(&seek_def.end));
            let start_reg = program.alloc_registers(max_registers);
            program.with_cursor_overrides(&cursor_overrides, |program| {
                SeekEmitter::new(
                    program,
                    tables,
                    seek_def,
                    t_ctx,
                    seek_cursor_id,
                    start_reg,
                    scan_end,
                    index.as_ref(),
                )
                .emit(scan_start, false)
            })?;
            if let Some(index_cursor_id) = index_cursor_id {
                program.emit_deferred_seek(index_cursor_id, table_cursor_id);
            }
        }
        Operation::Search(Search::InSeek { index, source }) => {
            let meta = program.with_cursor_overrides(&cursor_overrides, |program| {
                emit_in_seek_start(
                    program,
                    tables,
                    &t_ctx.resolver,
                    index.as_ref(),
                    source,
                    Some(table_cursor_id),
                    index_cursor_id,
                    scan_start,
                    scan_end,
                )
            })?;
            // The main loop is closed, so this read can reuse the per-table state slot.
            t_ctx.meta_in_seeks[table_index] = Some(meta);
        }
        Operation::MultiIndexScan(multi_index) => {
            program.with_cursor_overrides(&cursor_overrides, |program| {
                emit_multi_index_scan_loop(
                    program,
                    t_ctx,
                    table,
                    tables,
                    multi_index,
                    scan_start,
                    scan_end,
                )
            })?;
        }
        _ => {
            return Err(crate::LimboError::InternalError(
                "the unmatched-right pass has an unsupported table read".to_string(),
            ));
        }
    }

    // Other copied subqueries belong to the copied WHERE clause. This code is
    // inside the right-table loop, so it runs again for each candidate row.
    for (subquery, operation_uses_subquery) in &mut copied_subqueries {
        if !*operation_uses_subquery {
            emit_unmatched_rows_subquery(
                program,
                &t_ctx.resolver,
                subquery,
                table_index,
                &cursor_overrides,
            )?;
        }
    }

    // SQLite checks its copied WHERE clause before it looks for the row in the
    // matched-row set. The shared body checks these conditions again.
    for condition in &unmatched_rows_conditions {
        let condition_is_true = program.allocate_label();
        program.with_cursor_overrides(&cursor_overrides, |program| {
            translate_condition_expr(
                program,
                tables,
                condition,
                ConditionMetadata {
                    jump_if_condition_is_true: false,
                    jump_target_when_true: condition_is_true,
                    jump_target_when_false: next_row,
                    jump_target_when_null: next_row,
                },
                &t_ctx.resolver,
            )
        })?;
        program.preassign_label_to_next_insn(condition_is_true);
    }

    emit_right_join_key(program, right_join, table_cursor_id, index_cursor_id);
    // The bloom filter can skip most exact lookups. It never decides that a row matched.
    program.emit_insn(Insn::Filter {
        cursor_id: right_join.matched_rows_cursor_id,
        target_pc: emit_row,
        key_reg: right_join.rowid_reg,
        num_keys: 1,
    });
    program.emit_insn(Insn::Found {
        cursor_id: right_join.matched_rows_cursor_id,
        target_pc: next_row,
        record_reg: right_join.rowid_reg,
        num_regs: 1,
    });
    program.preassign_label_to_next_insn(emit_row);
    if let Table::FromClauseSubquery(subquery) = &table.table {
        // Result expressions read a materialized subquery through registers.
        // The code loads the register values after the match set selects this row.
        emit_materialized_subquery_result_columns(
            program,
            subquery,
            &table.col_used_mask,
            table_cursor_id,
            None,
        );
    }
    program.emit_insn(Insn::Gosub {
        target_pc: right_join.body_label,
        return_reg: right_join.return_reg,
    });
    program.preassign_label_to_next_insn(next_row);
    match &unmatched_rows_operation {
        Operation::Scan(Scan::BTreeTable { iter_dir, .. })
        | Operation::Scan(Scan::Subquery { iter_dir }) => {
            let scan_cursor_id = index_cursor_id.unwrap_or(table_cursor_id);
            if *iter_dir == IterationDirection::Backwards {
                program.emit_insn(Insn::Prev {
                    cursor_id: scan_cursor_id,
                    pc_if_prev: scan_start,
                    fullscan: true,
                });
            } else {
                program.emit_insn(Insn::Next {
                    cursor_id: scan_cursor_id,
                    pc_if_next: scan_start,
                    fullscan: true,
                });
            }
        }
        Operation::Search(Search::RowidEq { .. }) => {
            // `SeekRowid` finds at most one row, so this operation has no next step.
        }
        Operation::Search(Search::Seek { seek_def, .. }) => {
            let scan_cursor_id = index_cursor_id.unwrap_or(table_cursor_id);
            if seek_def.iter_dir == IterationDirection::Backwards {
                program.emit_insn(Insn::Prev {
                    cursor_id: scan_cursor_id,
                    pc_if_prev: scan_start,
                    fullscan: false,
                });
            } else {
                program.emit_insn(Insn::Next {
                    cursor_id: scan_cursor_id,
                    pc_if_next: scan_start,
                    fullscan: false,
                });
            }
        }
        Operation::Search(Search::InSeek { index, .. }) => {
            let meta = t_ctx.meta_in_seeks[table_index]
                .as_ref()
                .expect("an IN search must have loop state");
            let matching_rows_cursor_id = index
                .as_ref()
                .map(|_| index_cursor_id.expect("an indexed IN search needs a cursor"));
            emit_in_seek_end(program, matching_rows_cursor_id, scan_start, meta);
        }
        Operation::Scan(Scan::VirtualTable { .. }) => {
            program.emit_insn(Insn::VNext {
                cursor_id: table_cursor_id,
                pc_if_next: scan_start,
            });
        }
        Operation::MultiIndexScan(_) => {
            // `RowSetRead` advances this operation, so the jump resumes it.
            program.emit_insn(Insn::Goto {
                target_pc: scan_start,
            });
        }
        _ => unreachable!("the unmatched-right table read was checked above"),
    }
    program.preassign_label_to_next_insn(scan_end);
    Ok(())
}

/// Replace one subquery result in the copied WHERE clause and its table read.
///
/// SQLite gives the copied subquery new storage. The table read must use that
/// storage when a WHERE term became a seek or a virtual-table argument.
/// The result is true when the table read needs the subquery before it starts.
fn replace_subquery_output(
    operation: &mut Operation,
    conditions: &mut [Expr],
    subquery_id: TableInternalId,
    old_query_type: &SubqueryType,
    new_query_type: &SubqueryType,
) -> Result<bool> {
    fn replace_in_expr(
        expr: &mut Expr,
        subquery_id: TableInternalId,
        new_query_type: &SubqueryType,
    ) -> Result<bool> {
        let mut found = false;
        walk_expr_mut(expr, &mut |expr| {
            if let Expr::SubqueryResult {
                subquery_id: expr_subquery_id,
                query_type,
                ..
            } = expr
            {
                if *expr_subquery_id == subquery_id {
                    *query_type = new_query_type.clone();
                    found = true;
                }
            }
            Ok(WalkControl::Continue)
        })?;
        Ok(found)
    }

    fn replace_in_seek(
        seek: &mut SeekDef,
        subquery_id: TableInternalId,
        new_query_type: &SubqueryType,
    ) -> Result<bool> {
        let mut found = false;
        for key in &mut seek.prefix {
            for expr in [
                key.eq.as_mut().map(|value| &mut value.1),
                key.lower_bound.as_mut().map(|value| &mut value.1),
                key.upper_bound.as_mut().map(|value| &mut value.1),
            ]
            .into_iter()
            .flatten()
            {
                found |= replace_in_expr(expr, subquery_id, new_query_type)?;
            }
        }
        for key in [&mut seek.start, &mut seek.end] {
            if let SeekKeyComponent::Expr(expr) = &mut key.last_component {
                found |= replace_in_expr(expr, subquery_id, new_query_type)?;
            }
        }
        Ok(found)
    }

    fn replace_in_source(
        source: &mut InSeekSource,
        subquery_id: TableInternalId,
        old_query_type: &SubqueryType,
        new_query_type: &SubqueryType,
    ) -> Result<bool> {
        match source {
            InSeekSource::LiteralList { values, .. } => {
                let mut found = false;
                for value in values {
                    found |= replace_in_expr(value, subquery_id, new_query_type)?;
                }
                Ok(found)
            }
            InSeekSource::Subquery { cursor_id } => {
                // An IN search stores only its result cursor. The old result
                // identifies the search that must use the new cursor.
                let (
                    SubqueryType::In {
                        cursor_id: old_cursor,
                        ..
                    },
                    SubqueryType::In {
                        cursor_id: new_cursor,
                        ..
                    },
                ) = (old_query_type, new_query_type)
                else {
                    return Ok(false);
                };
                if *cursor_id != *old_cursor {
                    return Ok(false);
                }
                *cursor_id = *new_cursor;
                Ok(true)
            }
        }
    }

    let mut operation_uses_subquery = false;
    match operation {
        Operation::Scan(Scan::VirtualTable { constraints, .. }) => {
            for constraint in constraints {
                operation_uses_subquery |=
                    replace_in_expr(constraint, subquery_id, new_query_type)?;
            }
        }
        Operation::Search(Search::RowidEq { cmp_expr }) => {
            operation_uses_subquery |= replace_in_expr(cmp_expr, subquery_id, new_query_type)?;
        }
        Operation::Search(Search::Seek { seek_def, .. }) => {
            operation_uses_subquery |= replace_in_seek(seek_def, subquery_id, new_query_type)?;
        }
        Operation::Search(Search::InSeek { source, .. }) => {
            operation_uses_subquery |=
                replace_in_source(source, subquery_id, old_query_type, new_query_type)?;
        }
        Operation::MultiIndexScan(scan) => {
            for branch in &mut scan.branches {
                operation_uses_subquery |= match &mut branch.access {
                    MultiIndexBranchAccess::Seek { seek_def } => {
                        replace_in_seek(seek_def, subquery_id, new_query_type)?
                    }
                    MultiIndexBranchAccess::InSeek { source } => {
                        replace_in_source(source, subquery_id, old_query_type, new_query_type)?
                    }
                };
                if let Some(filters) = &mut branch.union_residuals {
                    for expr in filters
                        .pre_filter_exprs
                        .iter_mut()
                        .chain(filters.post_filter_exprs.iter_mut())
                    {
                        operation_uses_subquery |=
                            replace_in_expr(expr, subquery_id, new_query_type)?;
                    }
                }
            }
        }
        Operation::Scan(Scan::BTreeTable { .. } | Scan::Subquery { .. })
        | Operation::Scan(Scan::RecursiveCteInput)
        | Operation::IndexMethodQuery(_)
        | Operation::HashJoin(_) => {}
    }

    for condition in conditions {
        replace_in_expr(condition, subquery_id, new_query_type)?;
    }
    Ok(operation_uses_subquery)
}

/// Emit one subquery from SQLite's copied unmatched-right WHERE clause.
fn emit_unmatched_rows_subquery(
    program: &mut ProgramBuilder,
    resolver: &Resolver<'_>,
    subquery: &mut NonFromClauseSubquery,
    table_index: usize,
    cursor_overrides: &[(CursorKey, CursorID)],
) -> Result<()> {
    let subquery_plan = subquery.consume_plan(EvalAt::Loop(table_index));
    program.with_cursor_overrides(cursor_overrides, |program| {
        emit_non_from_clause_subquery(
            program,
            resolver,
            *subquery_plan,
            &subquery.query_type,
            subquery.correlated,
            false,
        )
    })
}

/// Give a copied WHERE subquery its own output storage.
///
/// SQLite gives the copied subquery new result storage. Separate storage prevents
/// this read from changing a value that a later unmatched read needs.
fn assign_new_subquery_output(
    program: &mut ProgramBuilder,
    subquery: &mut NonFromClauseSubquery,
) -> SubqueryType {
    let SubqueryState::Unevaluated { plan: Some(plan) } = &mut subquery.state else {
        panic!("a copied WHERE subquery must keep its plan");
    };
    let query_type = match &subquery.query_type {
        SubqueryType::Exists { .. } => {
            let result_reg = program.alloc_register();
            *plan
                .select_query_destination_mut()
                .expect("a subquery must have a query destination") =
                QueryDestination::ExistsSubqueryResult { result_reg };
            SubqueryType::Exists { result_reg }
        }
        SubqueryType::RowValue { num_regs, .. } => {
            let result_reg_start = program.alloc_registers(*num_regs);
            *plan
                .select_query_destination_mut()
                .expect("a subquery must have a query destination") =
                QueryDestination::RowValueSubqueryResult {
                    result_reg_start,
                    num_regs: *num_regs,
                };
            SubqueryType::RowValue {
                result_reg_start,
                num_regs: *num_regs,
            }
        }
        SubqueryType::In { affinity_str, .. } => {
            let (index, destination_affinity, is_delete) = match plan
                .select_query_destination()
                .expect("a subquery must have a query destination")
            {
                QueryDestination::EphemeralIndex {
                    index,
                    affinity_str,
                    is_delete,
                    ..
                } => (index.clone(), affinity_str.clone(), *is_delete),
                _ => panic!("an IN subquery must write to an ephemeral index"),
            };
            let cursor_id = program.alloc_cursor_id(CursorType::BTreeIndex(index.clone()));
            *plan
                .select_query_destination_mut()
                .expect("a subquery must have a query destination") =
                QueryDestination::EphemeralIndex {
                    cursor_id,
                    index,
                    affinity_str: destination_affinity,
                    is_delete,
                };
            SubqueryType::In {
                cursor_id,
                affinity_str: affinity_str.clone(),
            }
        }
    };
    subquery.query_type = query_type.clone();
    query_type
}

/// Represents final step of Loop emission
pub struct CloseLoop;

impl CloseLoop {
    pub fn emit<'a>(
        program: &mut ProgramBuilder,
        t_ctx: &mut TranslateCtx<'a>,
        tables: &TableReferences,
        join_order: &[JoinOrderMember],
        mode: OperationMode,
        select_plan: Option<&'a SelectPlan>,
    ) -> Result<()> {
        // We close the loops for all tables in reverse order, i.e. innermost first.
        // OPEN t1
        //   OPEN t2
        //     OPEN t3
        //       <do stuff>
        //     CLOSE t3
        //   CLOSE t2
        // CLOSE t1
        for join in join_order.iter().rev() {
            let table_index = join.original_idx;
            let table = &tables.joined_tables()[table_index];
            let loop_labels = *t_ctx
                .labels_main_loop
                .get(table_index)
                .expect("source has no loop labels");

            // SEMI/ANTI-JOIN: emit Goto -> outer_next right after the body.
            // For semi-join: after body runs (one match found), skip inner's Next.
            // For anti-join: after body runs (inner exhausted), move to next outer row.
            let is_semi_or_anti = table
                .join_info
                .as_ref()
                .is_some_and(|ji| ji.is_semi_or_anti());
            if is_semi_or_anti {
                let sa_meta = t_ctx.meta_semi_anti_joins[table_index]
                    .as_ref()
                    .expect("semi/anti-join must have SemiAntiJoinMetadata");
                let comment = if table.join_info.as_ref().unwrap().is_semi() {
                    "semi-join: early out after first match"
                } else {
                    "anti-join: exit body, next outer row"
                };
                program.add_comment(program.offset(), comment);
                program.emit_insn(Insn::Goto {
                    target_pc: sa_meta.label_next_outer,
                });
            }

            if let Some(right_join) = t_ctx.meta_right_joins[table_index].as_ref() {
                // The unmatched-row scan enters the normal body with Gosub.
                // Return sends that path back, but normal loop execution falls through.
                program.preassign_label_to_next_insn(right_join.return_label);
                program.emit_insn(Insn::Return {
                    return_reg: right_join.return_reg,
                    can_fallthrough: true,
                });
            }

            let (table_cursor_id, index_cursor_id) =
                table.resolve_cursors(program, mode.clone())?;
            // Track the "next iteration" anchor label for semi/anti-join label resolution.
            // For most operations this is loop_labels.next itself;
            // HashJoin overrides it to anchor at the Gosub Return or HashNext instead.
            let mut semi_anti_next_anchor: Option<BranchOffset> = None;
            // Helper: preassign loop_labels.next and record it as the semi/anti anchor.
            let mut resolve_next = |program: &mut ProgramBuilder| {
                program.preassign_label_to_next_insn(loop_labels.next);
                semi_anti_next_anchor = Some(loop_labels.next);
            };
            match &table.op {
                Operation::Scan(scan) => {
                    resolve_next(program);
                    match scan {
                        Scan::BTreeTable { iter_dir, .. } => {
                            let iteration_cursor_id = if let OperationMode::UPDATE(
                                UpdateRowSource::PrebuiltEphemeralTable {
                                    ephemeral_table_cursor_id,
                                    ..
                                },
                            ) = &mode
                            {
                                *ephemeral_table_cursor_id
                            } else {
                                index_cursor_id.unwrap_or_else(|| {
                                    table_cursor_id.expect(
                                        "Either ephemeral or index or table cursor must be opened",
                                    )
                                })
                            };
                            // An unconstrained scan is a full scan no matter
                            // what it steps: the table, a covering index, or
                            // the prebuilt ephemeral copy an UPDATE scans.
                            // SQLite tags any WHERE loop without a constraint;
                            // constrained loops go through Operation::Search.
                            let fullscan = true;
                            if *iter_dir == IterationDirection::Backwards {
                                program.emit_insn(Insn::Prev {
                                    cursor_id: iteration_cursor_id,
                                    pc_if_prev: loop_labels.loop_start,
                                    fullscan,
                                });
                            } else {
                                program.emit_insn(Insn::Next {
                                    cursor_id: iteration_cursor_id,
                                    pc_if_next: loop_labels.loop_start,
                                    fullscan,
                                });
                            }
                        }
                        Scan::VirtualTable { .. } => {
                            program.emit_insn(Insn::VNext {
                                cursor_id: table_cursor_id
                                    .expect("Virtual tables do not support covering indexes"),
                                pc_if_next: loop_labels.loop_start,
                            });
                        }
                        Scan::Subquery { iter_dir } => {
                            // Check if this is a materialized CTE (EphemeralTable) or coroutine
                            if let Table::FromClauseSubquery(subquery) = &table.table {
                                if let Some(QueryDestination::EphemeralTable {
                                    cursor_id, ..
                                }) = subquery.plan.select_query_destination()
                                {
                                    if *iter_dir == IterationDirection::Backwards {
                                        program.emit_insn(Insn::Prev {
                                            cursor_id: *cursor_id,
                                            pc_if_prev: loop_labels.loop_start,
                                            fullscan: false,
                                        });
                                    } else {
                                        program.emit_insn(Insn::Next {
                                            cursor_id: *cursor_id,
                                            pc_if_next: loop_labels.loop_start,
                                            fullscan: false,
                                        });
                                    }
                                } else {
                                    turso_assert_eq!(
                                        *iter_dir,
                                        IterationDirection::Forwards,
                                        "coroutine-backed subqueries cannot scan backwards"
                                    );
                                    // Coroutine-based subquery - use Goto to Yield
                                    program.emit_insn(Insn::Goto {
                                        target_pc: loop_labels.loop_start,
                                    });
                                }
                            } else {
                                // A subquery has no cursor to call Next on, so it just emits a Goto
                                // to the Yield instruction, which in turn jumps back to the main loop of the subquery,
                                // so that the next row from the subquery can be read.
                                program.emit_insn(Insn::Goto {
                                    target_pc: loop_labels.loop_start,
                                });
                            }
                        }
                        Scan::RecursiveCteInput => {}
                    }
                    program.preassign_label_to_next_insn(loop_labels.loop_end);
                }
                Operation::Search(search) => {
                    // Materialized subqueries with ephemeral indexes are allowed
                    let is_materialized_subquery = matches!(
                        &table.table,
                        Table::FromClauseSubquery(_)
                    ) && matches!(search, Search::Seek { index: Some(idx), .. } if idx.ephemeral);
                    turso_assert_some!(
                        {
                            is_from_clause: !matches!(table.table, Table::FromClauseSubquery(_)),
                            is_materialized_subquery: is_materialized_subquery
                        },
                        "Subqueries do not support index seeks unless materialized"
                    );
                    resolve_next(program);
                    let iteration_cursor_id =
                        if let OperationMode::UPDATE(UpdateRowSource::PrebuiltEphemeralTable {
                            ephemeral_table_cursor_id,
                            ..
                        }) = &mode
                        {
                            *ephemeral_table_cursor_id
                        } else if is_materialized_subquery {
                            // Table-backed materialized subquery seeks iterate the
                            // auxiliary ephemeral index cursor.
                            index_cursor_id.expect("materialized subquery must have index cursor")
                        } else {
                            index_cursor_id.unwrap_or_else(|| {
                                table_cursor_id.expect(
                                    "Either ephemeral or index or table cursor must be opened",
                                )
                            })
                        };
                    // Rowid equality point lookups are handled with a SeekRowid instruction which does not loop, so there is no need to emit a Next instruction.
                    match search {
                        Search::RowidEq { .. } => {}
                        Search::Seek { seek_def, .. } => {
                            if seek_def.iter_dir == IterationDirection::Backwards {
                                program.emit_insn(Insn::Prev {
                                    cursor_id: iteration_cursor_id,
                                    pc_if_prev: loop_labels.loop_start,
                                    fullscan: false,
                                });
                            } else {
                                program.emit_insn(Insn::Next {
                                    cursor_id: iteration_cursor_id,
                                    pc_if_next: loop_labels.loop_start,
                                    fullscan: false,
                                });
                            }
                        }
                        Search::InSeek { index, .. } => {
                            let meta = t_ctx.meta_in_seeks[table_index]
                                .as_ref()
                                .expect("InSeek must have metadata");
                            let matching_rows_cursor_id =
                                index.as_ref().map(|_| iteration_cursor_id);
                            emit_in_seek_end(
                                program,
                                matching_rows_cursor_id,
                                loop_labels.loop_start,
                                meta,
                            );
                        }
                    }
                    program.preassign_label_to_next_insn(loop_labels.loop_end);
                }
                Operation::IndexMethodQuery(_) => {
                    resolve_next(program);
                    program.emit_insn(Insn::Next {
                        cursor_id: index_cursor_id.unwrap(),
                        pc_if_next: loop_labels.loop_start,
                        fullscan: false,
                    });
                    program.preassign_label_to_next_insn(loop_labels.loop_end);
                }
                Operation::HashJoin(ref hash_join_op) => {
                    if let Some(hash_ctx) = t_ctx
                        .hash_table_contexts
                        .get(&hash_join_op.build_table_idx)
                        .cloned()
                    {
                        // Emit the close-loop teardown for a hash-join probe table.
                        semi_anti_next_anchor = HashProbeCloseEmitter::new(
                            program,
                            t_ctx,
                            hash_join_op,
                            hash_ctx,
                            select_plan,
                            table_index,
                        )
                        .emit()?
                        .semi_anti_next_anchor;
                    }

                    // Advance probe cursor.
                    program.preassign_label_to_next_insn(loop_labels.next);
                    let probe_cursor_id = table_cursor_id.expect("Probe table must have a cursor");
                    program.emit_insn(Insn::Next {
                        cursor_id: probe_cursor_id,
                        pc_if_next: loop_labels.loop_start,
                        fullscan: false,
                    });
                    program.preassign_label_to_next_insn(loop_labels.loop_end);

                    // Outer joins: emit unmatched build rows with NULLs for the probe side.
                    // This runs BEFORE grace so that in-memory partitions (with valid
                    // matched_bits from the main probe) are scanned while still available.
                    // At runtime, the scan skips spilled partitions — those are handled
                    // per-partition inside the grace loop where matched_bits are still live.
                    if matches!(
                        hash_join_op.join_type,
                        HashJoinType::LeftOuter | HashJoinType::FullOuter
                    ) {
                        if let Some(hash_ctx) = t_ctx
                            .hash_table_contexts
                            .get(&hash_join_op.build_table_idx)
                            .cloned()
                        {
                            emit_hash_join_unmatched_build_rows(
                                program,
                                t_ctx,
                                hash_join_op,
                                &hash_ctx,
                                select_plan,
                                table_index,
                                probe_cursor_id,
                            )?;
                        }
                    }

                    // Grace hash join processing: process spilled partition pairs.
                    // At runtime, this is a no-op if the build side didn't spill.
                    // For LEFT/FULL OUTER, each grace partition gets its own unmatched
                    // scan before eviction (so matched_bits are still live).
                    if let Some(hash_ctx) = t_ctx
                        .hash_table_contexts
                        .get(&hash_join_op.build_table_idx)
                        .cloned()
                    {
                        // emit grace processing loop after the probe cursor is exhausted.
                        GraceHashLoop::emit(
                            program,
                            t_ctx,
                            hash_join_op,
                            &hash_ctx,
                            select_plan,
                            table_index,
                            probe_cursor_id,
                        )?;
                    }
                }
                Operation::MultiIndexScan(_) => {
                    // MultiIndexScan uses RowSetRead for iteration - the next is handled
                    // at the end of the RowSet read loop in emit_multi_index_scan_loop
                    resolve_next(program);
                    program.emit_insn(Insn::Goto {
                        target_pc: loop_labels.loop_start,
                    });
                    program.preassign_label_to_next_insn(loop_labels.loop_end);
                }
            }

            if let Some(right_join) = t_ctx.meta_right_joins[table_index].as_ref() {
                // Join-condition failure can reach the loop end from the same subroutine.
                // Keep a Return on that path before outer-loop cleanup starts.
                program.emit_insn(Insn::Return {
                    return_reg: right_join.return_reg,
                    can_fallthrough: true,
                });
            }

            // Resolve any semi/anti-join "outer next" labels targeting this table.
            if let Some(anchor) = semi_anti_next_anchor {
                for meta in t_ctx.meta_semi_anti_joins.iter().flatten() {
                    if meta.outer_table_idx == table_index {
                        program.link_label_to_other_label(meta.label_next_outer, anchor);
                    }
                }
            }

            // SEMI/ANTI-JOIN: after loop_end (inner loop exhausted).
            // Semi-join: no match found -> skip outer row (Goto -> next_outer).
            // Anti-join: no match found -> run body (Goto -> label_body, jumps backward).
            if is_semi_or_anti {
                let sa_meta = t_ctx.meta_semi_anti_joins[table_index]
                    .as_ref()
                    .expect("semi/anti-join must have SemiAntiJoinMetadata");
                let join_info = table.join_info.as_ref().unwrap();
                if join_info.is_semi() {
                    program.add_comment(program.offset(), "semi-join: no match, skip outer row");
                    program.emit_insn(Insn::Goto {
                        target_pc: sa_meta.label_next_outer,
                    });
                } else {
                    // Anti-join: inner exhausted without match -> run body
                    program.add_comment(program.offset(), "anti-join: no match, emit outer row");
                    program.emit_insn(Insn::Goto {
                        target_pc: sa_meta.label_body,
                    });
                }
            }

            // OUTER JOIN: may still need to emit NULLs for the right table.
            // Outer hash join probes are handled above via check_outer / unmatched scan.
            let is_outer_hash_join_probe = matches!(
                table.op,
                Operation::HashJoin(ref hj) if matches!(
                    hj.join_type,
                    HashJoinType::LeftOuter | HashJoinType::FullOuter
                )
            );
            if let Some(join_info) = table.join_info.as_ref() {
                if join_info.keeps_left_rows() && !is_outer_hash_join_probe {
                    let lj_meta = t_ctx.meta_left_joins[table_index].as_ref().unwrap();
                    // The left join match flag is set to 1 when there is any match on the right table
                    // (e.g. SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a).
                    // If the left join match flag has been set to 1, we jump to the next row on the outer table,
                    // i.e. continue to the next row of t1 in our example.
                    program.preassign_label_to_next_insn(lj_meta.label_match_flag_check_value);
                    let label_when_right_table_notnull = program.allocate_label();
                    program.emit_insn(Insn::IfPos {
                        reg: lj_meta.reg_match_flag,
                        target_pc: label_when_right_table_notnull,
                        decrement_by: 0,
                    });
                    // The normal body must see a NULL right source for this unmatched left row.
                    emit_null_row_for_source(program, table, table_cursor_id, index_cursor_id);
                    // Re-enter the loop body at match-flag set so
                    // post-join predicates are re-evaluated with right-table NULLs.
                    program.emit_insn(Insn::Goto {
                        target_pc: lj_meta.label_match_flag_set_true,
                    });
                    program.preassign_label_to_next_insn(label_when_right_table_notnull);
                }
            }
        }

        // Scan unmatched right rows in source order. An earlier scan can enter
        // later join loops and add matches that those later scans must see.
        for table_index in 0..t_ctx.meta_right_joins.len() {
            if let Some(right_join) = t_ctx.meta_right_joins[table_index].clone() {
                emit_unmatched_right_rows(program, t_ctx, tables, table_index, &right_join, &mode)?;
            }
        }

        // After ALL loops are closed, emit HashClose for any hash tables that were built.
        // This must happen at the very end because hash join probe loops may be nested
        // inside outer loops that re-enter them. Hash tables used by materialization
        // subplans can be kept open and are skipped here.
        //
        // When inside a nested subquery (correlated or non-correlated), skip HashClose
        // because the hash build is protected by Once and must persist across subquery
        // re-invocations. The hash table will be cleaned up by ProgramState::reset().
        if !program.is_nested() {
            for join in join_order.iter() {
                let table_index = join.original_idx;
                let table = &tables.joined_tables()[table_index];
                if let Operation::HashJoin(hash_join_op) = &table.op {
                    let build_table = &tables.joined_tables()[hash_join_op.build_table_idx];
                    let hash_table_reg: usize = build_table.internal_id.into();
                    if !program.should_keep_hash_table_open(hash_table_reg) {
                        program.emit_insn(Insn::HashClose {
                            hash_table_id: hash_table_reg,
                        });
                        program.clear_hash_build_signature(hash_table_reg);
                    }
                }
            }
        }

        Ok(())
    }
}

pub(super) struct AutoIndexResult {
    pub(super) use_bloom_filter: bool,
}

pub(super) struct AutoIndexBuild<'a> {
    pub(super) index: &'a Arc<Index>,
    pub(super) table_cursor_id: CursorID,
    pub(super) index_cursor_id: CursorID,
    pub(super) table_has_rowid: bool,
    pub(super) num_seek_keys: usize,
    pub(super) seek_def: &'a SeekDef,
    pub(super) affinity_str: Option<&'a Arc<String>>,
    /// Table columns needed for transparent virtual column computation.
    pub(super) table_columns: Option<&'a [crate::schema::Column]>,
    pub(super) table_ref_id: turso_parser::ast::TableInternalId,
    pub(super) table_references: &'a TableReferences,
    pub(super) resolver: &'a Resolver<'a>,
}

/// Open an ephemeral index cursor and build an automatic index on a table.
/// This is used as a last-resort to avoid a nested full table scan
/// Returns the cursor id of the ephemeral index cursor.
pub(super) fn emit_autoindex(
    program: &mut ProgramBuilder,
    build: AutoIndexBuild<'_>,
) -> Result<AutoIndexResult> {
    let AutoIndexBuild {
        index,
        table_cursor_id,
        index_cursor_id,
        table_has_rowid,
        num_seek_keys,
        seek_def,
        affinity_str,
        table_columns,
        table_ref_id,
        table_references,
        resolver,
    } = build;
    turso_assert!(index.ephemeral, "index must be ephemeral", { "index_name": &index.name });
    let label_ephemeral_build_end = program.allocate_label();
    // Since this typically happens in an inner loop, we only build it once.
    program.emit_insn(Insn::Once {
        target_pc_when_reentered: label_ephemeral_build_end,
    });
    program.emit_insn(Insn::OpenAutoindex {
        cursor_id: index_cursor_id,
    });
    // Rewind source table
    let label_ephemeral_build_loop_start = program.allocate_label();
    program.emit_insn(Insn::Rewind {
        cursor_id: table_cursor_id,
        pc_if_empty: label_ephemeral_build_loop_start,
    });
    program.preassign_label_to_next_insn(label_ephemeral_build_loop_start);
    let label_ephemeral_build_loop_next = program.allocate_label();
    if let Some(filter) = &index.where_clause {
        let filter_passed = program.allocate_label();
        // The table's planned operation reads from the new index. While that
        // index is being built, expressions must read the source cursor.
        program.set_table_cursor_override(table_ref_id, table_cursor_id);
        let result = translate_condition_expr(
            program,
            table_references,
            filter,
            ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: filter_passed,
                jump_target_when_false: label_ephemeral_build_loop_next,
                jump_target_when_null: label_ephemeral_build_loop_next,
            },
            resolver,
        );
        program.clear_table_cursor_override(table_ref_id);
        result?;
        program.preassign_label_to_next_insn(filter_passed);
    }
    // Emit all columns from source table that are needed in the ephemeral index.
    // Also reserve a register for the rowid if the source table has rowids.
    let num_regs_to_reserve = index.columns.len() + table_has_rowid as usize;
    let ephemeral_cols_start_reg = program.alloc_registers(num_regs_to_reserve);
    for (i, col) in index.columns.iter().enumerate() {
        let reg = ephemeral_cols_start_reg + i;
        if let Some(columns) = table_columns {
            if let Some(column_def) = columns.get(col.pos_in_table) {
                if column_def.is_virtual_generated() {
                    // Override the table cursor to the base table, because generated
                    // columns may need to read from it to compute their expression.
                    program.set_table_cursor_override(table_ref_id, table_cursor_id);
                    let result = crate::translate::expr::emit_table_column(
                        program,
                        table_cursor_id,
                        table_ref_id,
                        table_references,
                        column_def,
                        col.pos_in_table,
                        reg,
                        resolver,
                    );
                    program.clear_table_cursor_override(table_ref_id);
                    result?;
                    continue;
                }
            }
        }
        program.emit_column_or_rowid(table_cursor_id, col.pos_in_table, reg);
    }
    if table_has_rowid {
        program.emit_insn(Insn::RowId {
            cursor_id: table_cursor_id,
            dest: ephemeral_cols_start_reg + index.columns.len(),
        });
    }
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(ephemeral_cols_start_reg),
        count: to_u32(num_regs_to_reserve),
        dest_reg: to_u32(record_reg),
        index_name: Some(index.name.clone()),
        affinity_str: affinity_str.map(|s| (**s).clone()),
    });
    // Skip bloom filter for non-binary collations since it uses binary hashing.
    // Also skip it when any seek key component comes from a NULL-matching `IS`:
    // the probe treats a NULL key as "definitely absent", which would skip rows
    // whose key IS NULL, so such a seek never probes — and then building the
    // filter would be wasted work on every row.
    let use_bloom_filter = index.columns.iter().take(num_seek_keys).all(|col| {
        col.collation
            .is_none_or(|coll| matches!(coll, CollationSeq::Binary | CollationSeq::Unset))
    }) && seek_def.start.op.eq_only()
        && (0..num_seek_keys).all(|i| !seek_def.is_null_matching_key_component(i));
    if use_bloom_filter {
        program.emit_insn(Insn::FilterAdd {
            cursor_id: index_cursor_id,
            key_reg: ephemeral_cols_start_reg,
            num_keys: num_seek_keys,
        });
    }
    program.emit_insn(Insn::IdxInsert {
        cursor_id: index_cursor_id,
        record_reg,
        unpacked_start: Some(ephemeral_cols_start_reg),
        unpacked_count: Some(num_regs_to_reserve as u32),
        flags: IdxInsertFlags::new().use_seek(false),
    });
    program.preassign_label_to_next_insn(label_ephemeral_build_loop_next);
    program.emit_insn(Insn::Next {
        cursor_id: table_cursor_id,
        pc_if_next: label_ephemeral_build_loop_start,
        fullscan: false,
    });
    program.preassign_label_to_next_insn(label_ephemeral_build_end);
    Ok(AutoIndexResult { use_bloom_filter })
}
