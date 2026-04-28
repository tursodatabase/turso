use super::{Resolver, Result, TranslateCtx};
use crate::{
    emit_explain,
    schema::{BTreeTable, ColumnLayout},
    sync::Arc,
    translate::{
        display::format_eqp_detail,
        emitter::{
            emit_cdc_autocommit_commit, emit_cdc_full_record, emit_cdc_insns,
            emit_index_column_value_old_image, emit_program_for_select,
            get_triggers_including_temp, has_triggers_including_temp, init_limit, OperationMode,
            TriggerTime,
        },
        expr::{
            emit_returning_results, emit_returning_scan_back, emit_table_column,
            restore_returning_row_image_in_cache, seed_returning_row_image_in_cache,
            translate_expr_no_constant_opt, NoConstantOptReason, ReturningBufferCtx,
        },
        fkeys::{
            build_index_affinity_string, emit_guarded_fk_decrement, open_read_index,
            open_read_table, ForeignKeyActions,
        },
        main_loop::{CloseLoop, InitLoop, OpenLoop},
        plan::{
            DeletePlan, EvalAt, JoinOrderMember, JoinedTable, NonFromClauseSubquery, Operation,
            ResultSetColumn, Search, TableReferences,
        },
        subquery::{emit_non_from_clause_subqueries_for_eval_at, emit_non_from_clause_subquery},
        trigger_exec::{fire_trigger, TriggerContext},
    },
    vdbe::{
        builder::{CursorKey, CursorType, ProgramBuilder},
        insn::{Insn, RegisterOrLiteral},
    },
    CaptureDataChangesExt, Connection,
};
use tracing::{instrument, Level};
use turso_parser::ast::TriggerEvent;

#[instrument(skip_all, level = Level::DEBUG)]
pub fn emit_program_for_delete(
    connection: &Arc<Connection>,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    mut plan: DeletePlan,
) -> Result<()> {
    let mut t_ctx = Box::new(TranslateCtx::new(
        program,
        resolver.fork(),
        plan.table_references.joined_tables().len(),
        connection.db.opts.unsafe_testing,
    ));

    let after_main_loop_label = program.allocate_label();
    t_ctx.label_main_loop_end = Some(after_main_loop_label);

    // Open an ephemeral table for buffering RETURNING results.
    // All DML completes before any RETURNING rows are yielded to the caller.
    let returning_buffer = if !plan.result_columns.is_empty() {
        let table_ref = plan.table_references.joined_tables().first().unwrap();
        let btree_table = table_ref
            .table
            .btree()
            .expect("DELETE target must be a BTree table");
        let ret_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(btree_table));
        program.emit_insn(Insn::OpenEphemeral {
            cursor_id: ret_cursor_id,
            is_table: true,
        });
        Some(ReturningBufferCtx {
            cursor_id: ret_cursor_id,
            num_columns: plan.result_columns.len(),
        })
    } else {
        None
    };

    init_limit(program, &mut t_ctx, &plan.limit, &plan.offset)?;

    // No rows will be read from source table loops if there is a constant false condition eg. WHERE 0
    if plan.contains_constant_false_condition {
        program.emit_insn(Insn::Goto {
            target_pc: after_main_loop_label,
        });
    }

    let join_order = plan
        .table_references
        .joined_tables()
        .iter()
        .enumerate()
        .map(|(i, t)| JoinOrderMember {
            table_id: t.internal_id,
            original_idx: i,
            is_outer: false,
        })
        .collect::<Vec<_>>();

    // Evaluate uncorrelated subqueries as early as possible (only for normal path without rowset)
    // For the rowset path, subqueries are handled by emit_program_for_select on the rowset_plan.
    if plan.rowset_plan.is_none() {
        emit_non_from_clause_subqueries_for_eval_at(
            program,
            &t_ctx.resolver,
            &mut plan.non_from_clause_subqueries,
            &join_order,
            Some(&plan.table_references),
            EvalAt::BeforeLoop,
            |_| true,
        )?;
    }

    // Initialize cursors and other resources needed for query execution
    InitLoop::emit(
        program,
        &mut t_ctx,
        &plan.table_references,
        &mut [],
        &OperationMode::DELETE,
        &plan.where_clause,
        &join_order,
        &mut plan.non_from_clause_subqueries,
    )?;

    // If there's a rowset_plan, materialize rowids into a RowSet first and then iterate the RowSet
    // to delete the rows.
    if let Some(rowset_plan) = plan.rowset_plan.take() {
        let rowset_reg = plan
            .rowset_reg
            .expect("rowset_reg must be Some if rowset_plan is Some");

        // Initialize the RowSet register with NULL (RowSet will be created on first RowSetAdd)
        program.emit_insn(Insn::Null {
            dest: rowset_reg,
            dest_end: None,
        });

        // Execute the rowset SELECT plan to populate the rowset.
        program.nested(|program| emit_program_for_select(program, resolver, rowset_plan))?;

        // Close the read cursor(s) opened by the rowset plan before opening for writing
        let table_ref = plan.table_references.joined_tables().first().unwrap();
        let table_cursor_id_read =
            program.resolve_cursor_id(&CursorKey::table(table_ref.internal_id));
        program.emit_insn(Insn::Close {
            cursor_id: table_cursor_id_read,
        });

        // Open the table cursor for writing
        let table_cursor_id = table_cursor_id_read;

        if let Some(btree_table) = table_ref.table.btree() {
            program.emit_insn(Insn::OpenWrite {
                cursor_id: table_cursor_id,
                root_page: RegisterOrLiteral::Literal(btree_table.root_page),
                db: table_ref.database_id,
            });

            // Open all indexes for writing (needed for DELETE)
            let write_indices: Vec<_> = resolver.with_schema(table_ref.database_id, |s| {
                s.get_indices(table_ref.table.get_name()).cloned().collect()
            });
            for index in &write_indices {
                let index_cursor_id = program.alloc_cursor_index(
                    Some(CursorKey::index(table_ref.internal_id, index.clone())),
                    index,
                )?;
                program.emit_insn(Insn::OpenWrite {
                    cursor_id: index_cursor_id,
                    root_page: RegisterOrLiteral::Literal(index.root_page),
                    db: table_ref.database_id,
                });
            }
        }

        // Now iterate over the RowSet and delete each rowid
        let rowset_loop_start = program.allocate_label();
        let rowset_loop_end = program.allocate_label();
        let rowid_reg = program.alloc_register();
        if table_ref.table.virtual_table().is_some() {
            // VUpdate requires a NULL second argument ("new rowid") for deletion
            let new_rowid_reg = program.alloc_register();
            program.emit_insn(Insn::Null {
                dest: new_rowid_reg,
                dest_end: None,
            });
        }

        program.preassign_label_to_next_insn(rowset_loop_start);

        // Read next rowid from RowSet
        // Note: rowset_loop_end will be resolved later when we assign it
        program.emit_insn(Insn::RowSetRead {
            rowset_reg,
            pc_if_empty: rowset_loop_end,
            dest_reg: rowid_reg,
        });

        emit_delete_insns_when_triggers_present(
            connection,
            program,
            &mut t_ctx,
            &mut plan.table_references,
            &mut plan.non_from_clause_subqueries,
            &plan.result_columns,
            rowid_reg,
            table_cursor_id,
            resolver,
            returning_buffer.as_ref(),
        )?;

        // Continue loop
        program.emit_insn(Insn::Goto {
            target_pc: rowset_loop_start,
        });

        // Assign the end label here, after all loop body code
        program.preassign_label_to_next_insn(rowset_loop_end);
    } else {
        // Normal DELETE path without RowSet

        // Emit EXPLAIN QUERY PLAN annotation
        let table_ref = plan
            .table_references
            .joined_tables()
            .first()
            .expect("DELETE always has one joined table");
        emit_explain!(program, true, format_eqp_detail(table_ref));

        // Set up main query execution loop
        OpenLoop::emit(
            program,
            &mut t_ctx,
            &plan.table_references,
            &join_order,
            &plan.where_clause,
            None,
            OperationMode::DELETE,
            &mut plan.non_from_clause_subqueries,
        )?;

        emit_delete_insns(
            connection,
            program,
            &mut t_ctx,
            &mut plan.table_references,
            &mut plan.non_from_clause_subqueries,
            &plan.result_columns,
            resolver,
            returning_buffer.as_ref(),
        )?;

        // Clean up and close the main execution loop
        CloseLoop::emit(
            program,
            &mut t_ctx,
            &plan.table_references,
            &join_order,
            OperationMode::DELETE,
            None,
        )?;
    }
    program.preassign_label_to_next_insn(after_main_loop_label);
    if let Some(cdc_cursor_id) = t_ctx.cdc_cursor_id {
        emit_cdc_autocommit_commit(program, resolver, cdc_cursor_id)?;
    }
    // Emit scan-back loop for buffered RETURNING results.
    // All DML is complete at this point; now yield the buffered rows to the caller.
    // FkCheck must come before the scan-back so that FK violations prevent
    // RETURNING rows from being emitted (matching SQLite behavior).
    if let Some(ref buf) = returning_buffer {
        program.emit_insn(Insn::FkCheck { deferred: false });
        emit_returning_scan_back(program, buf);
    }
    // Finalize program
    program.result_columns = plan.result_columns;
    program.table_references.extend(plan.table_references);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn emit_fk_child_decrement_on_delete(
    program: &mut ProgramBuilder,
    child_tbl: &BTreeTable,
    child_table_name: &str,
    child_cursor_id: usize,
    child_rowid_reg: usize,
    database_id: usize,
    resolver: &Resolver,
) -> crate::Result<()> {
    for fk_ref in
        resolver.with_schema(database_id, |s| s.resolved_fks_for_child(child_table_name))?
    {
        if !fk_ref.fk.deferred {
            continue;
        }
        // Fast path: if any FK column is NULL can't be a violation
        let null_skip = program.allocate_label();
        for cname in &fk_ref.fk.child_columns {
            let (pos, col) = child_tbl.get_column(cname).unwrap();
            let src = if col.is_rowid_alias() {
                child_rowid_reg
            } else {
                let tmp = program.alloc_register();
                program.emit_insn(Insn::Column {
                    cursor_id: child_cursor_id,
                    column: pos,
                    dest: tmp,
                    default: None,
                });
                tmp
            };
            program.emit_insn(Insn::IsNull {
                reg: src,
                target_pc: null_skip,
            });
        }

        if fk_ref.parent_uses_rowid {
            // Probe parent table by rowid
            let parent_tbl = resolver
                .with_schema(database_id, |s| s.get_btree_table(&fk_ref.fk.parent_table))
                .expect("parent btree");
            let pcur = open_read_table(program, &parent_tbl, database_id);

            let (pos, col) = child_tbl.get_column(&fk_ref.fk.child_columns[0]).unwrap();
            let val = if col.is_rowid_alias() {
                child_rowid_reg
            } else {
                let tmp = program.alloc_register();
                program.emit_insn(Insn::Column {
                    cursor_id: child_cursor_id,
                    column: pos,
                    dest: tmp,
                    default: None,
                });
                tmp
            };
            let tmpi = program.alloc_register();
            program.emit_insn(Insn::Copy {
                src_reg: val,
                dst_reg: tmpi,
                extra_amount: 0,
            });
            program.emit_insn(Insn::MustBeInt { reg: tmpi });

            // NotExists jumps when the parent key is missing, so we decrement there
            let missing = program.allocate_label();
            let done = program.allocate_label();

            program.emit_insn(Insn::NotExists {
                cursor: pcur,
                rowid_reg: tmpi,
                target_pc: missing,
            });

            // Parent FOUND, no decrement
            program.emit_insn(Insn::Close { cursor_id: pcur });
            program.emit_insn(Insn::Goto { target_pc: done });

            // Parent MISSING, decrement is guarded by FkIfZero to avoid underflow
            program.preassign_label_to_next_insn(missing);
            program.emit_insn(Insn::Close { cursor_id: pcur });
            emit_guarded_fk_decrement(program, done, true);
            program.preassign_label_to_next_insn(done);
        } else {
            // Probe parent unique index
            let parent_tbl = resolver
                .with_schema(database_id, |s| s.get_btree_table(&fk_ref.fk.parent_table))
                .expect("parent btree");
            let idx = fk_ref.parent_unique_index.as_ref().expect("unique index");
            let icur = open_read_index(program, idx, database_id);

            // Build probe from current child row
            let n = fk_ref.fk.child_columns.len();
            let probe = program.alloc_registers(n);
            for (i, cname) in fk_ref.fk.child_columns.iter().enumerate() {
                let (pos, col) = child_tbl.get_column(cname).unwrap();
                let src = if col.is_rowid_alias() {
                    child_rowid_reg
                } else {
                    let r = program.alloc_register();
                    program.emit_insn(Insn::Column {
                        cursor_id: child_cursor_id,
                        column: pos,
                        dest: r,
                        default: None,
                    });
                    r
                };
                program.emit_insn(Insn::Copy {
                    src_reg: src,
                    dst_reg: probe + i,
                    extra_amount: 0,
                });
            }
            program.emit_insn(Insn::Affinity {
                start_reg: probe,
                count: std::num::NonZeroUsize::new(n).unwrap(),
                affinities: build_index_affinity_string(idx, &parent_tbl),
            });

            let ok = program.allocate_label();
            program.emit_insn(Insn::Found {
                cursor_id: icur,
                target_pc: ok,
                record_reg: probe,
                num_regs: n,
            });
            program.emit_insn(Insn::Close { cursor_id: icur });
            emit_guarded_fk_decrement(program, ok, true);
            program.preassign_label_to_next_insn(ok);
            program.emit_insn(Insn::Close { cursor_id: icur });
        }
        program.preassign_label_to_next_insn(null_skip);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_delete_insns<'a>(
    connection: &Arc<Connection>,
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    table_references: &mut TableReferences,
    non_from_clause_subqueries: &mut [NonFromClauseSubquery],
    result_columns: &'a [ResultSetColumn],
    resolver: &Resolver,
    returning_buffer: Option<&ReturningBufferCtx>,
) -> Result<()> {
    // we can either use this obviously safe raw pointer or we can clone it
    let table_reference: *const JoinedTable = table_references.joined_tables().first().unwrap();
    if unsafe { &*table_reference }
        .virtual_table()
        .is_some_and(|t| t.readonly())
    {
        return Err(crate::LimboError::ReadOnly);
    }
    let internal_id = unsafe { (*table_reference).internal_id };

    let cursor_id = match unsafe { &(*table_reference).op } {
        Operation::Scan { .. } => program.resolve_cursor_id(&CursorKey::table(internal_id)),
        Operation::Search(search) => match search {
            Search::RowidEq { .. }
            | Search::Seek { index: None, .. }
            | Search::InSeek { index: None, .. } => {
                program.resolve_cursor_id(&CursorKey::table(internal_id))
            }
            Search::Seek {
                index: Some(index), ..
            }
            | Search::InSeek {
                index: Some(index), ..
            } => program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone())),
        },
        Operation::IndexMethodQuery(query) => {
            program.resolve_cursor_id(&CursorKey::index(internal_id, query.index.clone()))
        }
        Operation::HashJoin(_) => {
            unreachable!("access through HashJoin is not supported for delete statements")
        }
        Operation::MultiIndexScan(_) => {
            unreachable!("access through MultiIndexScan is not supported for delete statements")
        }
    };
    let btree_table = unsafe { &*table_reference }.btree();
    let database_id = unsafe { (*table_reference).database_id };
    let main_table_cursor_id = program.resolve_cursor_id(&CursorKey::table(internal_id));
    let has_returning = !result_columns.is_empty();
    let has_delete_triggers = if let Some(btree_table) = btree_table {
        has_triggers_including_temp(
            &t_ctx.resolver,
            database_id,
            TriggerEvent::Delete,
            None,
            &btree_table,
        )
    } else {
        false
    };

    // Apply OFFSET: skip the first N matching rows before deleting
    if let Some(offset) = t_ctx.reg_offset {
        let loop_labels = *t_ctx
            .labels_main_loop
            .first()
            .expect("loop labels to exist");
        program.emit_insn(Insn::IfPos {
            reg: offset,
            target_pc: loop_labels.next,
            decrement_by: 1,
        });
    }

    let cols_len = unsafe { &*table_reference }.columns().len();
    let (columns_start_reg, rowid_reg): (Option<usize>, usize) = {
        // Get rowid for RETURNING
        let rowid_reg = program.alloc_register();
        program.emit_insn(Insn::RowId {
            cursor_id: main_table_cursor_id,
            dest: rowid_reg,
        });
        if unsafe { &*table_reference }.virtual_table().is_some() {
            // VUpdate requires a NULL second argument ("new rowid") for deletion
            let new_rowid_reg = program.alloc_register();
            program.emit_insn(Insn::Null {
                dest: new_rowid_reg,
                dest_end: None,
            });
        }

        if !has_returning && !has_delete_triggers {
            (None, rowid_reg)
        } else {
            // Allocate registers for column values
            let columns_start_reg = program.alloc_registers(cols_len);

            // Read all column values from the row to be deleted
            for (i, column) in unsafe { &*table_reference }.columns().iter().enumerate() {
                emit_table_column(
                    program,
                    main_table_cursor_id,
                    internal_id,
                    table_references,
                    column,
                    i,
                    columns_start_reg + i,
                    resolver,
                )?;
            }

            (Some(columns_start_reg), rowid_reg)
        }
    };

    // Get the index that is being used to iterate the deletion loop, if there is one.
    let iteration_index = unsafe { &*table_reference }.op.index();

    // Capture iteration index key values BEFORE deleting the main table row,
    // since the main table cursor will be invalidated after deletion.
    let iteration_idx_delete_ctx = if let Some(index) = iteration_index {
        let iteration_index_cursor =
            program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone()));
        let num_regs = index.columns.len() + 1;
        let start_reg = program.alloc_registers(num_regs);
        for (reg_offset, column_index) in index.columns.iter().enumerate() {
            emit_index_column_value_old_image(
                program,
                &t_ctx.resolver,
                table_references,
                main_table_cursor_id,
                internal_id,
                column_index,
                start_reg + reg_offset,
            )?;
        }
        program.emit_insn(Insn::RowId {
            cursor_id: main_table_cursor_id,
            dest: start_reg + num_regs - 1,
        });
        Some((iteration_index_cursor, start_reg, num_regs, index))
    } else {
        None
    };

    emit_delete_row_common(
        connection,
        program,
        t_ctx,
        table_references,
        non_from_clause_subqueries,
        result_columns,
        table_reference,
        rowid_reg,
        columns_start_reg,
        main_table_cursor_id,
        iteration_index,
        Some(cursor_id), // Use the cursor_id from the operation for virtual tables
        resolver,
        returning_buffer,
    )?;

    // Delete from the iteration index after deleting from the main table,
    // using the key values captured above.
    if let Some((iteration_index_cursor, start_reg, num_regs, index)) = iteration_idx_delete_ctx {
        program.emit_insn(Insn::IdxDelete {
            start_reg,
            num_regs,
            cursor_id: iteration_index_cursor,
            raise_error_if_no_matching_entry: index.where_clause.is_none(),
        });
    }
    if let Some(limit_ctx) = t_ctx.limit_ctx {
        program.emit_insn(Insn::DecrJumpZero {
            reg: limit_ctx.reg_limit,
            target_pc: t_ctx.label_main_loop_end.unwrap(),
        })
    }

    Ok(())
}

/// Common deletion logic shared between normal DELETE and RowSet-based DELETE.
///
/// Parameters:
/// - `rowid_reg`: Register containing the rowid of the row to delete
/// - `columns_start_reg`: Start register containing column values (already read)
/// - `skip_iteration_index`: If Some(index), skip deleting from this index (used when iterating over an index)
/// - `virtual_table_cursor_id`: If Some, use this cursor for virtual table deletion
#[allow(clippy::too_many_arguments)]
fn emit_delete_row_common(
    connection: &Arc<Connection>,
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &mut TableReferences,
    non_from_clause_subqueries: &mut [NonFromClauseSubquery],
    result_columns: &[ResultSetColumn],
    table_reference: *const JoinedTable,
    rowid_reg: usize,
    columns_start_reg: Option<usize>, // must be provided when there are triggers or RETURNING
    main_table_cursor_id: usize,
    skip_iteration_index: Option<&Arc<crate::schema::Index>>,
    virtual_table_cursor_id: Option<usize>,
    resolver: &Resolver,
    returning_buffer: Option<&ReturningBufferCtx>,
) -> Result<()> {
    let internal_id = unsafe { (*table_reference).internal_id };
    let table_name = unsafe { &*table_reference }.table.get_name();

    // Phase 1: Before Delete - build parent key registers and handle NoAction/Restrict.
    // CASCADE/SetNull/SetDefault actions are prepared but deferred until after Delete.
    let prepared_fk_actions = if connection.foreign_keys_enabled() {
        let delete_db_id = unsafe { (*table_reference).database_id };
        if let Some(table) = unsafe { &*table_reference }.btree() {
            let prepared = if t_ctx
                .resolver
                .with_schema(delete_db_id, |s| s.any_resolved_fks_referencing(table_name))
            {
                ForeignKeyActions::prepare_fk_delete_actions(
                    program,
                    &mut t_ctx.resolver,
                    table_name,
                    main_table_cursor_id,
                    rowid_reg,
                    None,
                    delete_db_id,
                )?
            } else {
                ForeignKeyActions::default()
            };
            if t_ctx
                .resolver
                .with_schema(delete_db_id, |s| s.has_child_fks(table_name))
            {
                emit_fk_child_decrement_on_delete(
                    program,
                    &table,
                    table_name,
                    main_table_cursor_id,
                    rowid_reg,
                    delete_db_id,
                    &t_ctx.resolver,
                )?;
            }
            prepared
        } else {
            ForeignKeyActions::default()
        }
    } else {
        ForeignKeyActions::default()
    };

    if unsafe { &*table_reference }.virtual_table().is_some() {
        let conflict_action = 0u16;
        let cursor_id = virtual_table_cursor_id.unwrap_or(main_table_cursor_id);

        program.emit_insn(Insn::VUpdate {
            cursor_id,
            arg_count: 2,
            start_reg: rowid_reg,
            conflict_action,
        });
    } else {
        // Delete from all indexes before deleting from the main table.
        let db_id = unsafe { (*table_reference).database_id };
        let all_indices: Vec<_> = t_ctx
            .resolver
            .with_schema(db_id, |s| s.get_indices(table_name).cloned().collect());

        // Get indexes to delete from (skip the iteration index if specified)
        let indexes_to_delete = all_indices
            .iter()
            .filter(|index| {
                skip_iteration_index
                    .as_ref()
                    .is_none_or(|skip_idx| !Arc::ptr_eq(skip_idx, index))
            })
            .map(|index| {
                (
                    index.clone(),
                    program.resolve_cursor_id(&CursorKey::index(internal_id, index.clone())),
                )
            })
            .collect::<Vec<_>>();

        for (index, index_cursor_id) in indexes_to_delete {
            let skip_delete_label = if index.where_clause.is_some() {
                let where_copy = index
                    .bind_where_expr(Some(table_references), resolver)
                    .expect("where clause to exist");
                let skip_label = program.allocate_label();
                let reg = program.alloc_register();
                translate_expr_no_constant_opt(
                    program,
                    Some(table_references),
                    &where_copy,
                    reg,
                    &t_ctx.resolver,
                    NoConstantOptReason::RegisterReuse,
                )?;
                program.emit_insn(Insn::IfNot {
                    reg,
                    jump_if_null: true,
                    target_pc: skip_label,
                });
                Some(skip_label)
            } else {
                None
            };
            let num_regs = index.columns.len() + 1;
            let start_reg = program.alloc_registers(num_regs);
            for (reg_offset, column_index) in index.columns.iter().enumerate() {
                emit_index_column_value_old_image(
                    program,
                    &t_ctx.resolver,
                    table_references,
                    main_table_cursor_id,
                    internal_id,
                    column_index,
                    start_reg + reg_offset,
                )?;
            }
            program.emit_insn(Insn::RowId {
                cursor_id: main_table_cursor_id,
                dest: start_reg + num_regs - 1,
            });
            program.emit_insn(Insn::IdxDelete {
                start_reg,
                num_regs,
                cursor_id: index_cursor_id,
                raise_error_if_no_matching_entry: index.where_clause.is_none(),
            });
            if let Some(label) = skip_delete_label {
                program.preassign_label_to_next_insn(label);
            }
        }

        // Emit update in the CDC table if necessary (before DELETE updated the table)
        if let Some(cdc_cursor_id) = t_ctx.cdc_cursor_id {
            let cdc_has_before = program.capture_data_changes_info().has_before();
            let before_record_reg = if cdc_has_before {
                let table_reference = unsafe { &*table_reference };
                Some(emit_cdc_full_record(
                    program,
                    table_reference.table.columns(),
                    main_table_cursor_id,
                    rowid_reg,
                    table_reference
                        .table
                        .btree()
                        .is_some_and(|btree| btree.is_strict),
                ))
            } else {
                None
            };
            emit_cdc_insns(
                program,
                &t_ctx.resolver,
                OperationMode::DELETE,
                cdc_cursor_id,
                rowid_reg,
                before_record_reg,
                None,
                None,
                table_name,
            )?;
        }

        program.emit_insn(Insn::Delete {
            cursor_id: main_table_cursor_id,
            table_name: table_name.to_string(),
            is_part_of_update: false,
        });
    }

    // Emit RETURNING after the row is deleted, but against the cached OLD row image.
    // This matches SQLite: target-table scans inside RETURNING see the post-delete table
    // state, while direct column references still resolve to the deleted row.
    if !result_columns.is_empty() {
        let columns_start_reg = columns_start_reg
            .expect("columns_start_reg must be provided when there are triggers or RETURNING");
        let delete_table = unsafe { &*table_reference };
        let delete_layout = ColumnLayout::from_columns(delete_table.columns());
        let cache_state = seed_returning_row_image_in_cache(
            program,
            table_references,
            columns_start_reg,
            rowid_reg,
            &mut t_ctx.resolver,
            &delete_layout,
        )?;
        let result: Result<()> = (|| {
            for subquery in non_from_clause_subqueries
                .iter_mut()
                .filter(|s| !s.has_been_evaluated() && s.is_post_write_returning())
            {
                let rerun_for_target_scan =
                    subquery.reads_table(delete_table.database_id, delete_table.table.get_name());
                let subquery_plan = subquery.consume_plan(EvalAt::Loop(0));
                emit_non_from_clause_subquery(
                    program,
                    &t_ctx.resolver,
                    *subquery_plan,
                    &subquery.query_type,
                    subquery.correlated || rerun_for_target_scan,
                    true,
                )?;
            }
            Ok(())
        })();
        restore_returning_row_image_in_cache(&mut t_ctx.resolver, cache_state);
        result?;
        emit_returning_results(
            program,
            table_references,
            result_columns,
            columns_start_reg,
            rowid_reg,
            &mut t_ctx.resolver,
            returning_buffer,
            &delete_layout,
        )?;
    }

    // Phase 2: After Delete - fire CASCADE/SetNull/SetDefault FK actions.
    // Per SQLite docs, the parent row must be deleted before FK cascade actions fire,
    // so triggers during cascade see the parent row as already deleted.
    {
        let delete_db_id = unsafe { (*table_reference).database_id };
        prepared_fk_actions.fire_prepared_fk_delete_actions(
            program,
            &mut t_ctx.resolver,
            connection,
            delete_db_id,
        )?;
    }

    Ok(())
}

#[expect(clippy::too_many_arguments)]
/// Helper function to delete a row when we've already seeked to it (e.g., from a RowSet).
/// This is similar to emit_delete_insns but assumes the cursor is already positioned at the row.
fn emit_delete_insns_when_triggers_present(
    connection: &Arc<Connection>,
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    table_references: &mut TableReferences,
    non_from_clause_subqueries: &mut [NonFromClauseSubquery],
    result_columns: &[ResultSetColumn],
    rowid_reg: usize,
    main_table_cursor_id: usize,
    resolver: &Resolver,
    returning_buffer: Option<&ReturningBufferCtx>,
) -> Result<()> {
    // Seek to the rowid and delete it
    let skip_not_found_label = program.allocate_label();

    // Skip if row with rowid pulled from the rowset does not exist in the table.
    program.emit_insn(Insn::NotExists {
        cursor: main_table_cursor_id,
        rowid_reg,
        target_pc: skip_not_found_label,
    });

    let table_reference: *const JoinedTable = table_references.joined_tables().first().unwrap();
    if unsafe { &*table_reference }
        .virtual_table()
        .is_some_and(|t| t.readonly())
    {
        return Err(crate::LimboError::ReadOnly);
    }
    let btree_table = unsafe { &*table_reference }.btree();
    let database_id = unsafe { (*table_reference).database_id };
    let has_returning = !result_columns.is_empty();
    let has_delete_triggers = if let Some(btree_table) = btree_table {
        has_triggers_including_temp(
            &t_ctx.resolver,
            database_id,
            TriggerEvent::Delete,
            None,
            &btree_table,
        )
    } else {
        false
    };
    let cols_len = unsafe { &*table_reference }.columns().len();

    let internal_id = unsafe { (*table_reference).internal_id };
    let columns_start_reg = if !has_returning && !has_delete_triggers {
        None
    } else {
        let columns_start_reg = program.alloc_registers(cols_len);
        for (i, column) in unsafe { &*table_reference }.columns().iter().enumerate() {
            emit_table_column(
                program,
                main_table_cursor_id,
                internal_id,
                table_references,
                column,
                i,
                columns_start_reg + i,
                resolver,
            )?;
        }
        Some(columns_start_reg)
    };

    let cols_len = unsafe { &*table_reference }.columns().len();

    // Fire BEFORE DELETE triggers
    if let Some(btree_table) = unsafe { &*table_reference }.btree() {
        let relevant_triggers = get_triggers_including_temp(
            &t_ctx.resolver,
            database_id,
            TriggerEvent::Delete,
            TriggerTime::Before,
            None,
            &btree_table,
        );
        if !relevant_triggers.is_empty() {
            let columns_start_reg = columns_start_reg
                .expect("columns_start_reg must be provided when there are triggers or RETURNING");
            let old_registers = (0..cols_len)
                .map(|i| columns_start_reg + i)
                .chain(std::iter::once(rowid_reg))
                .collect::<Vec<_>>();
            // If the program has a trigger_conflict_override, propagate it to the trigger context.
            let trigger_ctx = if let Some(override_conflict) = program.trigger_conflict_override {
                TriggerContext::new_with_override_conflict(
                    btree_table,
                    None, // No NEW for DELETE
                    Some(old_registers),
                    override_conflict,
                )
            } else {
                TriggerContext::new(
                    btree_table,
                    None, // No NEW for DELETE
                    Some(old_registers),
                )
            };

            for trigger in relevant_triggers {
                fire_trigger(
                    program,
                    &mut t_ctx.resolver,
                    trigger,
                    &trigger_ctx,
                    connection,
                    database_id,
                    skip_not_found_label,
                )?;
            }
        }
    }

    // BEFORE DELETE Triggers may have altered the btree so we need to seek again.
    program.emit_insn(Insn::NotExists {
        cursor: main_table_cursor_id,
        rowid_reg,
        target_pc: skip_not_found_label,
    });

    emit_delete_row_common(
        connection,
        program,
        t_ctx,
        table_references,
        non_from_clause_subqueries,
        result_columns,
        table_reference,
        rowid_reg,
        columns_start_reg,
        main_table_cursor_id,
        None, // Don't skip any indexes when deleting from RowSet
        None, // Use main_table_cursor_id for virtual tables
        resolver,
        returning_buffer,
    )?;

    // Fire AFTER DELETE triggers
    if let Some(btree_table) = unsafe { &*table_reference }.btree() {
        let relevant_triggers = get_triggers_including_temp(
            &t_ctx.resolver,
            database_id,
            TriggerEvent::Delete,
            TriggerTime::After,
            None,
            &btree_table,
        );
        if !relevant_triggers.is_empty() {
            let columns_start_reg = columns_start_reg
                .expect("columns_start_reg must be provided when there are triggers or RETURNING");
            let old_registers = (0..cols_len)
                .map(|i| columns_start_reg + i)
                .chain(std::iter::once(rowid_reg))
                .collect::<Vec<_>>();
            // If the program has a trigger_conflict_override, propagate it to the trigger context.
            let trigger_ctx_after =
                if let Some(override_conflict) = program.trigger_conflict_override {
                    TriggerContext::new_with_override_conflict(
                        btree_table,
                        None, // No NEW for DELETE
                        Some(old_registers),
                        override_conflict,
                    )
                } else {
                    TriggerContext::new(
                        btree_table,
                        None, // No NEW for DELETE
                        Some(old_registers),
                    )
                };

            for trigger in relevant_triggers {
                fire_trigger(
                    program,
                    &mut t_ctx.resolver,
                    trigger,
                    &trigger_ctx_after,
                    connection,
                    database_id,
                    skip_not_found_label,
                )?;
            }
        }
    }

    program.preassign_label_to_next_insn(skip_not_found_label);

    Ok(())
}
