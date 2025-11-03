use std::num::NonZeroUsize;
use std::sync::Arc;
use turso_parser::ast::{
    self, Expr, InsertBody, OneSelect, QualifiedName, ResolveType, ResultColumn, Upsert, UpsertDo,
};

use crate::error::{
    SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_PRIMARYKEY, SQLITE_CONSTRAINT_UNIQUE,
};
use crate::schema::{self, Affinity, BTreeTable, Index, ResolvedFkRef, Table};
use crate::translate::emitter::{
    emit_cdc_insns, emit_cdc_patch_record, prepare_cdc_if_necessary, OperationMode,
};
use crate::translate::expr::{
    bind_and_rewrite_expr, emit_returning_results, process_returning_clause, walk_expr_mut,
    BindingBehavior, ReturningValueRegisters, WalkControl,
};
use crate::translate::fkeys::{
    build_index_affinity_string, emit_fk_violation, emit_guarded_fk_decrement, index_probe,
    open_read_index, open_read_table,
};
use crate::translate::plan::{ResultSetColumn, TableReferences};
use crate::translate::planner::ROWID_STRS;
use crate::translate::upsert::{
    collect_set_clauses_for_upsert, emit_upsert, resolve_upsert_target, ResolvedUpsertTarget,
};
use crate::util::normalize_ident;
use crate::vdbe::builder::ProgramBuilderOpts;
use crate::vdbe::insn::{CmpInsFlags, IdxInsertFlags, InsertFlags, RegisterOrLiteral};
use crate::vdbe::BranchOffset;
use crate::{
    schema::{Column, Schema},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::Insn,
    },
};
use crate::{Connection, Result, VirtualTable};

use super::emitter::Resolver;
use super::expr::{translate_expr, translate_expr_no_constant_opt, NoConstantOptReason};
use super::plan::QueryDestination;
use super::select::translate_select;

/// Validate anything with this insert statement that should throw an early parse error
fn validate(table_name: &str, resolver: &Resolver, table: &Table) -> Result<()> {
    // Check if this is a system table that should be protected from direct writes
    if crate::schema::is_system_table(table_name) {
        crate::bail_parse_error!("table {} may not be modified", table_name);
    }
    // Check if this table has any incompatible dependent views
    let incompatible_views = resolver.schema.has_incompatible_dependent_views(table_name);
    if !incompatible_views.is_empty() {
        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
        crate::bail_parse_error!(
            "Cannot INSERT into table '{}' because it has incompatible dependent materialized view(s): {}. \n\
             These views were created with a different DBSP version than the current version ({}). \n\
             Please DROP and recreate the view(s) before modifying this table.",
            table_name,
            incompatible_views.join(", "),
            DBSP_CIRCUIT_VERSION
        );
    }

    // Check if this is a materialized view
    if resolver.schema.is_materialized_view(table_name) {
        crate::bail_parse_error!("cannot modify materialized view {}", table_name);
    }
    if resolver.schema.table_has_indexes(table_name) && !resolver.schema.indexes_enabled() {
        // Let's disable altering a table with indices altogether instead of checking column by
        // column to be extra safe.
        crate::bail_parse_error!(
            "INSERT to table with indexes is disabled. Omit the `--experimental-indexes=false` flag to enable this feature."
        );
    }
    if table.btree().is_some_and(|t| !t.has_rowid) {
        crate::bail_parse_error!("INSERT into WITHOUT ROWID table is not supported");
    }

    Ok(())
}

pub struct TempTableCtx {
    cursor_id: usize,
    loop_start_label: BranchOffset,
    loop_end_label: BranchOffset,
}

#[allow(dead_code)]
pub struct InsertEmitCtx<'a> {
    /// Parent table being inserted into
    pub table: &'a Arc<BTreeTable>,

    /// Index cursors we need to populate for this table
    /// (idx name, root_page, idx cursor id)
    pub idx_cursors: Vec<(&'a String, i64, usize)>,

    /// Context for if the insert values are materialized first
    /// into a temporary table
    pub temp_table_ctx: Option<TempTableCtx>,
    /// on conflict, default to ABORT
    pub on_conflict: ResolveType,
    /// Arity of the insert values
    pub num_values: usize,
    /// The yield register, if a coroutine is used to yield multiple rows
    pub yield_reg_opt: Option<usize>,
    /// The register to hold the rowid of a conflicting row
    pub conflict_rowid_reg: usize,
    /// The cursor id of the table being inserted into
    pub cursor_id: usize,

    /// Label to jump to on HALT
    pub halt_label: BranchOffset,
    /// Label to jump to when a row is done processing (either inserted or upserted)
    pub row_done_label: BranchOffset,
    /// Jump here at the complete end of the statement
    pub stmt_epilogue: BranchOffset,
    /// Beginning of the loop for multiple-row inserts
    pub loop_start_label: BranchOffset,
    /// Label to jump to when a generated key is ready for uniqueness check
    pub key_ready_for_uniqueness_check_label: BranchOffset,
    /// Label to jump to when no key is provided and one must be generated
    pub key_generation_label: BranchOffset,
    /// Jump here when the insert value SELECT source has been fully exhausted
    pub select_exhausted_label: Option<BranchOffset>,

    /// CDC table info
    pub cdc_table: Option<(usize, Arc<BTreeTable>)>,
    /// Autoincrement sequence table info
    pub autoincrement_meta: Option<AutoincMeta>,
}

impl<'a> InsertEmitCtx<'a> {
    fn new(
        program: &mut ProgramBuilder,
        resolver: &'a Resolver,
        table: &'a Arc<BTreeTable>,
        on_conflict: Option<ResolveType>,
        cdc_table: Option<(usize, Arc<BTreeTable>)>,
        num_values: usize,
        temp_table_ctx: Option<TempTableCtx>,
    ) -> Result<Self> {
        // allocate cursor id's for each btree index cursor we'll need to populate the indexes
        let indices = resolver.schema.get_indices(table.name.as_str());
        let mut idx_cursors = Vec::new();
        for idx in indices {
            idx_cursors.push((
                &idx.name,
                idx.root_page,
                program.alloc_cursor_index(None, idx)?,
            ));
        }
        let halt_label = program.allocate_label();
        let loop_start_label = program.allocate_label();
        let row_done_label = program.allocate_label();
        let stmt_epilogue = program.allocate_label();
        let key_ready_for_uniqueness_check_label = program.allocate_label();
        let key_generation_label = program.allocate_label();
        Ok(Self {
            table,
            idx_cursors,
            temp_table_ctx,
            on_conflict: on_conflict.unwrap_or(ResolveType::Abort),
            yield_reg_opt: None,
            conflict_rowid_reg: program.alloc_register(),
            select_exhausted_label: None,
            cursor_id: 0, // set later in emit_source_emission
            halt_label,
            row_done_label,
            stmt_epilogue,
            loop_start_label,
            cdc_table,
            num_values,
            key_ready_for_uniqueness_check_label,
            key_generation_label,
            autoincrement_meta: None,
        })
    }
}

#[allow(clippy::too_many_arguments)]
pub fn translate_insert(
    resolver: &Resolver,
    on_conflict: Option<ResolveType>,
    tbl_name: QualifiedName,
    columns: Vec<ast::Name>,
    mut body: InsertBody,
    mut returning: Vec<ResultColumn>,
    mut program: ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> Result<ProgramBuilder> {
    let opts = ProgramBuilderOpts {
        num_cursors: 1,
        approx_num_insns: 30,
        approx_num_labels: 5,
    };
    program.extend(&opts);

    let table_name = &tbl_name.name;
    let table = match resolver.schema.get_table(table_name.as_str()) {
        Some(table) => table,
        None => crate::bail_parse_error!("no such table: {}", table_name),
    };
    validate(table_name.as_str(), resolver, &table)?;

    let fk_enabled = connection.foreign_keys_enabled();
    if let Some(virtual_table) = &table.virtual_table() {
        program = translate_virtual_table_insert(
            program,
            virtual_table.clone(),
            columns,
            body,
            on_conflict,
            resolver,
        )?;
        return Ok(program);
    }

    let Some(btree_table) = table.btree() else {
        crate::bail_parse_error!("no such table: {}", table_name);
    };

    let BoundInsertResult {
        mut values,
        mut upsert_actions,
        inserting_multiple_rows,
    } = bind_insert(
        &mut program,
        resolver,
        &table,
        &mut body,
        connection,
        on_conflict.unwrap_or(ResolveType::Abort),
    )?;

    if inserting_multiple_rows && btree_table.has_autoincrement {
        ensure_sequence_initialized(&mut program, resolver.schema, &btree_table)?;
    }

    let cdc_table = prepare_cdc_if_necessary(&mut program, resolver.schema, table.get_name())?;

    // Process RETURNING clause using shared module
    let (mut result_columns, _) = process_returning_clause(
        &mut returning,
        &table,
        table_name.as_str(),
        &mut program,
        connection,
    )?;
    let has_fks = fk_enabled
        && (resolver.schema.has_child_fks(table_name.as_str())
            || resolver
                .schema
                .any_resolved_fks_referencing(table_name.as_str()));

    let mut ctx = InsertEmitCtx::new(
        &mut program,
        resolver,
        &btree_table,
        on_conflict,
        cdc_table,
        values.len(),
        None,
    )?;

    program = init_source_emission(
        program,
        &table,
        connection,
        &mut ctx,
        resolver,
        &mut values,
        body,
        &columns,
    )?;
    let has_upsert = !upsert_actions.is_empty();

    // Set up the program to return result columns if RETURNING is specified
    if !result_columns.is_empty() {
        program.result_columns = result_columns.clone();
    }
    let insertion = build_insertion(&mut program, &table, &columns, ctx.num_values)?;

    translate_rows_and_open_tables(
        &mut program,
        resolver,
        &insertion,
        &ctx,
        &values,
        inserting_multiple_rows,
    )?;

    let has_user_provided_rowid = insertion.key.is_provided_by_user();

    if ctx.table.has_autoincrement {
        init_autoincrement(&mut program, &mut ctx, resolver)?;
    }

    if has_user_provided_rowid {
        let must_be_int_label = program.allocate_label();

        program.emit_insn(Insn::NotNull {
            reg: insertion.key_register(),
            target_pc: must_be_int_label,
        });

        program.emit_insn(Insn::Goto {
            target_pc: ctx.key_generation_label,
        });

        program.preassign_label_to_next_insn(must_be_int_label);
        program.emit_insn(Insn::MustBeInt {
            reg: insertion.key_register(),
        });

        program.emit_insn(Insn::Goto {
            target_pc: ctx.key_ready_for_uniqueness_check_label,
        });
    }

    program.preassign_label_to_next_insn(ctx.key_generation_label);

    emit_rowid_generation(&mut program, resolver, &ctx, &insertion)?;

    program.preassign_label_to_next_insn(ctx.key_ready_for_uniqueness_check_label);

    if ctx.table.is_strict {
        program.emit_insn(Insn::TypeCheck {
            start_reg: insertion.first_col_register(),
            count: insertion.col_mappings.len(),
            check_generated: true,
            table_reference: Arc::clone(ctx.table),
        });
    }

    // Build a list of upsert constraints/indexes we need to run preflight
    // checks against, in the proper order of evaluation,
    let constraints = build_constraints_to_check(
        resolver,
        table_name.as_str(),
        &upsert_actions,
        has_user_provided_rowid,
    );

    // We need to separate index handling and insertion into a `preflight` and a
    // `commit` phase, because in UPSERT mode we might need to skip the actual insertion, as we can
    // have a naked ON CONFLICT DO NOTHING, so if we eagerly insert any indexes, we could insert
    // invalid index entries before we hit a conflict down the line.
    emit_preflight_constraint_checks(
        &mut program,
        &ctx,
        resolver,
        &insertion,
        &upsert_actions,
        &constraints,
    )?;

    emit_notnulls(&mut program, &ctx, &insertion);

    // Create and insert the record
    let affinity_str = insertion
        .col_mappings
        .iter()
        .map(|col_mapping| col_mapping.column.affinity().aff_mask())
        .collect::<String>();
    program.emit_insn(Insn::MakeRecord {
        start_reg: insertion.first_col_register(),
        count: insertion.col_mappings.len(),
        dest_reg: insertion.record_register(),
        index_name: None,
        affinity_str: Some(affinity_str),
    });

    if has_upsert {
        emit_commit_phase(&mut program, resolver, &insertion, &ctx)?;
    }

    if has_fks {
        // Child-side check must run before Insert (may HALT or increment deferred counter)
        emit_fk_child_insert_checks(
            &mut program,
            resolver,
            &btree_table,
            insertion.first_col_register(),
            insertion.key_register(),
        )?;
    }

    program.emit_insn(Insn::Insert {
        cursor: ctx.cursor_id,
        key_reg: insertion.key_register(),
        record_reg: insertion.record_register(),
        flag: InsertFlags::new(),
        table_name: table_name.to_string(),
    });

    if has_fks {
        // After the row is actually present, repair deferred counters for children referencing this NEW parent key.
        emit_parent_side_fk_decrement_on_insert(&mut program, resolver, &btree_table, &insertion)?;
    }

    if let Some(AutoincMeta {
        seq_cursor_id,
        r_seq,
        r_seq_rowid,
        table_name_reg,
    }) = ctx.autoincrement_meta
    {
        let no_update_needed_label = program.allocate_label();
        program.emit_insn(Insn::Le {
            lhs: insertion.key_register(),
            rhs: r_seq,
            target_pc: no_update_needed_label,
            flags: Default::default(),
            collation: None,
        });

        emit_update_sqlite_sequence(
            &mut program,
            resolver.schema,
            seq_cursor_id,
            r_seq_rowid,
            table_name_reg,
            insertion.key_register(),
        )?;

        program.preassign_label_to_next_insn(no_update_needed_label);
        program.emit_insn(Insn::Close {
            cursor_id: seq_cursor_id,
        });
    }

    // Emit update in the CDC table if necessary (after the INSERT updated the table)
    if let Some((cdc_cursor_id, _)) = &ctx.cdc_table {
        let cdc_has_after = program.capture_data_changes_mode().has_after();
        let after_record_reg = if cdc_has_after {
            Some(emit_cdc_patch_record(
                &mut program,
                &table,
                insertion.first_col_register(),
                insertion.record_register(),
                insertion.key_register(),
            ))
        } else {
            None
        };
        emit_cdc_insns(
            &mut program,
            resolver,
            OperationMode::INSERT,
            *cdc_cursor_id,
            insertion.key_register(),
            None,
            after_record_reg,
            None,
            table_name.as_str(),
        )?;
    }

    // Emit RETURNING results if specified
    if !result_columns.is_empty() {
        let value_registers = ReturningValueRegisters {
            rowid_register: insertion.key_register(),
            columns_start_register: insertion.first_col_register(),
            num_columns: table.columns().len(),
        };

        emit_returning_results(&mut program, &result_columns, &value_registers)?;
    }
    program.emit_insn(Insn::Goto {
        target_pc: ctx.row_done_label,
    });

    resolve_upserts(
        &mut program,
        resolver,
        &mut upsert_actions,
        &ctx,
        &insertion,
        &table,
        &mut result_columns,
        connection,
    )?;

    emit_epilogue(&mut program, &ctx, inserting_multiple_rows);

    program.set_needs_stmt_subtransactions(true);
    Ok(program)
}

fn emit_epilogue(program: &mut ProgramBuilder, ctx: &InsertEmitCtx, inserting_multiple_rows: bool) {
    if inserting_multiple_rows {
        if let Some(temp_table_ctx) = &ctx.temp_table_ctx {
            program.resolve_label(ctx.row_done_label, program.offset());

            program.emit_insn(Insn::Next {
                cursor_id: temp_table_ctx.cursor_id,
                pc_if_next: temp_table_ctx.loop_start_label,
            });
            program.preassign_label_to_next_insn(temp_table_ctx.loop_end_label);

            program.emit_insn(Insn::Close {
                cursor_id: temp_table_ctx.cursor_id,
            });
            program.emit_insn(Insn::Goto {
                target_pc: ctx.stmt_epilogue,
            });
        } else {
            // For multiple rows which not require a temp table, loop back
            program.resolve_label(ctx.row_done_label, program.offset());
            program.emit_insn(Insn::Goto {
                target_pc: ctx.loop_start_label,
            });
            if let Some(sel_eof) = ctx.select_exhausted_label {
                program.preassign_label_to_next_insn(sel_eof);
                program.emit_insn(Insn::Goto {
                    target_pc: ctx.stmt_epilogue,
                });
            }
        }
    } else {
        program.resolve_label(ctx.row_done_label, program.offset());
        // single-row falls through to epilogue
        program.emit_insn(Insn::Goto {
            target_pc: ctx.stmt_epilogue,
        });
    }
    program.preassign_label_to_next_insn(ctx.stmt_epilogue);
    program.resolve_label(ctx.halt_label, program.offset());
}

// COMMIT PHASE: no preflight jumps happened; emit the actual index writes now
// We re-check partial-index predicates against the NEW image, produce packed records,
// and insert into all applicable indexes, we do not re-probe uniqueness here, as preflight
// already guaranteed non-conflict.
fn emit_commit_phase(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    insertion: &Insertion,
    ctx: &InsertEmitCtx,
) -> Result<()> {
    for index in resolver.schema.get_indices(ctx.table.name.as_str()) {
        let idx_cursor_id = ctx
            .idx_cursors
            .iter()
            .find(|(name, _, _)| *name == &index.name)
            .map(|(_, _, c_id)| *c_id)
            .expect("no cursor found for index");

        // Re-evaluate partial predicate on the would-be inserted image
        let commit_skip_label = if let Some(where_clause) = &index.where_clause {
            let mut where_for_eval = where_clause.as_ref().clone();
            rewrite_partial_index_where(&mut where_for_eval, insertion)?;
            let reg = program.alloc_register();
            translate_expr_no_constant_opt(
                program,
                Some(&TableReferences::new_empty()),
                &where_for_eval,
                reg,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
            let lbl = program.allocate_label();
            program.emit_insn(Insn::IfNot {
                reg,
                target_pc: lbl,
                jump_if_null: true,
            });
            Some(lbl)
        } else {
            None
        };

        let num_cols = index.columns.len();
        let idx_start_reg = program.alloc_registers(num_cols + 1);

        // Build [key cols..., rowid] from insertion registers
        for (i, idx_col) in index.columns.iter().enumerate() {
            let Some(cm) = insertion.get_col_mapping_by_name(&idx_col.name) else {
                return Err(crate::LimboError::PlanningError(
                    "Column not found in INSERT (commit phase)".to_string(),
                ));
            };
            program.emit_insn(Insn::Copy {
                src_reg: cm.register,
                dst_reg: idx_start_reg + i,
                extra_amount: 0,
            });
        }
        program.emit_insn(Insn::Copy {
            src_reg: insertion.key_register(),
            dst_reg: idx_start_reg + num_cols,
            extra_amount: 0,
        });

        let record_reg = program.alloc_register();
        program.emit_insn(Insn::MakeRecord {
            start_reg: idx_start_reg,
            count: num_cols + 1,
            dest_reg: record_reg,
            index_name: Some(index.name.clone()),
            affinity_str: None,
        });
        program.emit_insn(Insn::IdxInsert {
            cursor_id: idx_cursor_id,
            record_reg,
            unpacked_start: Some(idx_start_reg),
            unpacked_count: Some((num_cols + 1) as u16),
            flags: IdxInsertFlags::new().nchange(true),
        });

        if let Some(lbl) = commit_skip_label {
            program.resolve_label(lbl, program.offset());
        }
    }
    Ok(())
}

fn translate_rows_and_open_tables(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    insertion: &Insertion,
    ctx: &InsertEmitCtx,
    values: &[Box<Expr>],
    inserting_multiple_rows: bool,
) -> Result<()> {
    if inserting_multiple_rows {
        let select_result_start_reg = program
            .reg_result_cols_start
            .unwrap_or(ctx.yield_reg_opt.unwrap() + 1);
        translate_rows_multiple(
            program,
            insertion,
            select_result_start_reg,
            resolver,
            &ctx.temp_table_ctx,
        )?;
    } else {
        // Single row - populate registers directly
        program.emit_insn(Insn::OpenWrite {
            cursor_id: ctx.cursor_id,
            root_page: RegisterOrLiteral::Literal(ctx.table.root_page),
            db: 0,
        });

        translate_rows_single(program, values, insertion, resolver)?;
    }

    // Open all the index btrees for writing
    for idx_cursor in ctx.idx_cursors.iter() {
        program.emit_insn(Insn::OpenWrite {
            cursor_id: idx_cursor.2,
            root_page: idx_cursor.1.into(),
            db: 0,
        });
    }
    Ok(())
}

fn emit_rowid_generation(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    ctx: &InsertEmitCtx,
    insertion: &Insertion,
) -> Result<()> {
    if let Some(AutoincMeta {
        r_seq,
        seq_cursor_id,
        r_seq_rowid,
        table_name_reg,
        ..
    }) = ctx.autoincrement_meta
    {
        let r_max = program.alloc_register();

        let dummy_reg = program.alloc_register();

        program.emit_insn(Insn::NewRowid {
            cursor: ctx.cursor_id,
            rowid_reg: dummy_reg,
            prev_largest_reg: r_max,
        });

        program.emit_insn(Insn::Copy {
            src_reg: r_seq,
            dst_reg: insertion.key_register(),
            extra_amount: 0,
        });
        program.emit_insn(Insn::MemMax {
            dest_reg: insertion.key_register(),
            src_reg: r_max,
        });

        let no_overflow_label = program.allocate_label();
        let max_i64_reg = program.alloc_register();
        program.emit_insn(Insn::Integer {
            dest: max_i64_reg,
            value: i64::MAX,
        });
        program.emit_insn(Insn::Ne {
            lhs: insertion.key_register(),
            rhs: max_i64_reg,
            target_pc: no_overflow_label,
            flags: Default::default(),
            collation: None,
        });

        program.emit_insn(Insn::Halt {
            err_code: crate::error::SQLITE_FULL,
            description: "database or disk is full".to_string(),
        });

        program.preassign_label_to_next_insn(no_overflow_label);

        program.emit_insn(Insn::AddImm {
            register: insertion.key_register(),
            value: 1,
        });

        emit_update_sqlite_sequence(
            program,
            resolver.schema,
            seq_cursor_id,
            r_seq_rowid,
            table_name_reg,
            insertion.key_register(),
        )?;
    } else {
        program.emit_insn(Insn::NewRowid {
            cursor: ctx.cursor_id,
            rowid_reg: insertion.key_register(),
            prev_largest_reg: 0,
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn resolve_upserts(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    upsert_actions: &mut [(ResolvedUpsertTarget, BranchOffset, Box<Upsert>)],
    ctx: &InsertEmitCtx,
    insertion: &Insertion,
    table: &Table,
    result_columns: &mut [ResultSetColumn],
    connection: &Arc<crate::Connection>,
) -> Result<()> {
    for (_, label, upsert) in upsert_actions {
        program.preassign_label_to_next_insn(*label);

        if let UpsertDo::Set {
            ref mut sets,
            ref mut where_clause,
        } = upsert.do_clause
        {
            // Normalize SET pairs once
            let mut rewritten_sets = collect_set_clauses_for_upsert(table, sets)?;

            emit_upsert(
                program,
                table,
                ctx,
                insertion,
                &mut rewritten_sets,
                where_clause,
                resolver,
                result_columns,
                connection,
            )?;
        } else {
            // UpsertDo::Nothing case
            program.emit_insn(Insn::Goto {
                target_pc: ctx.row_done_label,
            });
        }
    }
    Ok(())
}

fn init_autoincrement(
    program: &mut ProgramBuilder,
    ctx: &mut InsertEmitCtx,
    resolver: &Resolver,
) -> Result<()> {
    let seq_table = resolver
        .schema
        .get_btree_table("sqlite_sequence")
        .ok_or_else(|| {
            crate::error::LimboError::InternalError("sqlite_sequence table not found".to_string())
        })?;
    let seq_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(seq_table.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: seq_cursor_id,
        root_page: seq_table.root_page.into(),
        db: 0,
    });

    let table_name_reg = program.emit_string8_new_reg(ctx.table.name.clone());
    let r_seq = program.alloc_register();
    let r_seq_rowid = program.alloc_register();

    ctx.autoincrement_meta = Some(AutoincMeta {
        seq_cursor_id,
        r_seq,
        r_seq_rowid,
        table_name_reg,
    });

    program.emit_insn(Insn::Integer {
        dest: r_seq,
        value: 0,
    });
    program.emit_insn(Insn::Null {
        dest: r_seq_rowid,
        dest_end: None,
    });

    let loop_start_label = program.allocate_label();
    let loop_end_label = program.allocate_label();
    let found_label = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: seq_cursor_id,
        pc_if_empty: loop_end_label,
    });
    program.preassign_label_to_next_insn(loop_start_label);

    let name_col_reg = program.alloc_register();
    program.emit_column_or_rowid(seq_cursor_id, 0, name_col_reg);
    program.emit_insn(Insn::Ne {
        lhs: table_name_reg,
        rhs: name_col_reg,
        target_pc: found_label,
        flags: Default::default(),
        collation: None,
    });

    program.emit_column_or_rowid(seq_cursor_id, 1, r_seq);
    program.emit_insn(Insn::RowId {
        cursor_id: seq_cursor_id,
        dest: r_seq_rowid,
    });
    program.emit_insn(Insn::Goto {
        target_pc: loop_end_label,
    });

    program.preassign_label_to_next_insn(found_label);
    program.emit_insn(Insn::Next {
        cursor_id: seq_cursor_id,
        pc_if_next: loop_start_label,
    });
    program.preassign_label_to_next_insn(loop_end_label);
    Ok(())
}

fn emit_notnulls(program: &mut ProgramBuilder, ctx: &InsertEmitCtx, insertion: &Insertion) {
    for column_mapping in insertion
        .col_mappings
        .iter()
        .filter(|column_mapping| column_mapping.column.notnull())
    {
        // if this is rowid alias - turso-db will emit NULL as a column value and always use rowid for the row as a column value
        if column_mapping.column.is_rowid_alias() {
            continue;
        }
        program.emit_insn(Insn::HaltIfNull {
            target_reg: column_mapping.register,
            err_code: SQLITE_CONSTRAINT_NOTNULL,
            description: {
                let mut description = String::with_capacity(
                    ctx.table.name.as_str().len()
                        + column_mapping
                            .column
                            .name
                            .as_ref()
                            .expect("Column name must be present")
                            .len()
                        + 2,
                );
                description.push_str(ctx.table.name.as_str());
                description.push('.');
                description.push_str(
                    column_mapping
                        .column
                        .name
                        .as_ref()
                        .expect("Column name must be present"),
                );
                description
            },
        });
    }
}

struct BoundInsertResult {
    #[allow(clippy::vec_box)]
    values: Vec<Box<Expr>>,
    upsert_actions: Vec<(ResolvedUpsertTarget, BranchOffset, Box<Upsert>)>,
    inserting_multiple_rows: bool,
}

fn bind_insert(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    table: &Table,
    body: &mut InsertBody,
    connection: &Arc<Connection>,
    on_conflict: ResolveType,
) -> Result<BoundInsertResult> {
    let mut values: Vec<Box<Expr>> = vec![];
    let mut upsert: Option<Box<Upsert>> = None;
    let mut upsert_actions: Vec<(ResolvedUpsertTarget, BranchOffset, Box<Upsert>)> = Vec::new();
    let mut inserting_multiple_rows = false;
    match body {
        InsertBody::DefaultValues => {
            // Generate default values for the table
            values = table
                .columns()
                .iter()
                .filter(|c| !c.hidden())
                .map(|c| {
                    c.default
                        .clone()
                        .unwrap_or(Box::new(ast::Expr::Literal(ast::Literal::Null)))
                })
                .collect();
        }
        InsertBody::Select(select, upsert_opt) => {
            if select.body.compounds.is_empty() {
                match &mut select.body.select {
                    // TODO see how to avoid clone
                    OneSelect::Values(values_expr) if values_expr.len() <= 1 => {
                        if values_expr.is_empty() {
                            crate::bail_parse_error!("no values to insert");
                        }
                        for expr in values_expr.iter_mut().flat_map(|v| v.iter_mut()) {
                            match expr.as_mut() {
                                Expr::Id(name) => {
                                    if name.quoted_with('"') {
                                        *expr =
                                            Expr::Literal(ast::Literal::String(name.as_literal()))
                                                .into();
                                    } else {
                                        // an INSERT INTO ... VALUES (...) cannot reference columns
                                        crate::bail_parse_error!("no such column: {name}");
                                    }
                                }
                                Expr::Qualified(first_name, second_name) => {
                                    // an INSERT INTO ... VALUES (...) cannot reference columns
                                    crate::bail_parse_error!(
                                        "no such column: {first_name}.{second_name}"
                                    );
                                }
                                _ => {}
                            }
                            bind_and_rewrite_expr(
                                expr,
                                None,
                                None,
                                connection,
                                &mut program.param_ctx,
                                BindingBehavior::ResultColumnsNotAllowed,
                            )?;
                        }
                        values = values_expr.pop().unwrap_or_else(Vec::new);
                    }
                    _ => inserting_multiple_rows = true,
                }
            } else {
                inserting_multiple_rows = true;
            }
            upsert = upsert_opt.take();
        }
    }
    match on_conflict {
        ResolveType::Ignore => {
            upsert.replace(Box::new(ast::Upsert {
                do_clause: UpsertDo::Nothing,
                index: None,
                next: None,
            }));
        }
        ResolveType::Abort => {
            // This is the default conflict resolution strategy for INSERT in SQLite.
        }
        _ => {
            crate::bail_parse_error!(
                "INSERT OR {} is only supported with UPSERT",
                on_conflict.to_string()
            );
        }
    }
    while let Some(mut upsert_opt) = upsert.take() {
        if let UpsertDo::Set {
            ref mut sets,
            ref mut where_clause,
        } = &mut upsert_opt.do_clause
        {
            for set in sets.iter_mut() {
                bind_and_rewrite_expr(
                    &mut set.expr,
                    None,
                    None,
                    connection,
                    &mut program.param_ctx,
                    BindingBehavior::AllowUnboundIdentifiers,
                )?;
            }
            if let Some(ref mut where_expr) = where_clause {
                bind_and_rewrite_expr(
                    where_expr,
                    None,
                    None,
                    connection,
                    &mut program.param_ctx,
                    BindingBehavior::AllowUnboundIdentifiers,
                )?;
            }
        }
        let next = upsert_opt.next.take();
        upsert_actions.push((
            // resolve the constrained target for UPSERT in the chain
            resolve_upsert_target(resolver.schema, table, &upsert_opt)?,
            program.allocate_label(),
            upsert_opt,
        ));
        upsert = next;
    }
    Ok(BoundInsertResult {
        values,
        upsert_actions,
        inserting_multiple_rows,
    })
}

/// Depending on the InsertBody, we begin to initialize the source of the insert values
/// into registers using the following methods:
///
/// Values with a single row, expressions are directly evaluated into registers, so nothing
/// is emitted here, we simply allocate the cursor ID and store the arity.
///
/// Values with multiple rows, we use a coroutine to yield each row into registers directly.
///
/// Select, we use a coroutine to yield each row from the SELECT into registers,
/// materializing into a temporary table if the target table is also read by the SELECT.
///
/// For DefaultValues, we allocate the cursor and extend the empty values vector with either the
/// default expressions registered for the columns, or NULLs, so they can be translated into
/// registers later.
#[allow(clippy::too_many_arguments, clippy::vec_box)]
fn init_source_emission<'a>(
    mut program: ProgramBuilder,
    table: &Table,
    connection: &Arc<Connection>,
    ctx: &mut InsertEmitCtx<'a>,
    resolver: &Resolver,
    values: &mut Vec<Box<Expr>>,
    body: InsertBody,
    columns: &'a [ast::Name],
) -> Result<ProgramBuilder> {
    let required_column_count = if columns.is_empty() {
        table.columns().len()
    } else {
        columns.len()
    };
    if !values.is_empty() {
        // If we had a single tuple in VALUES, it was inserted into the values vector parameter.
        if values.len() != required_column_count {
            crate::bail_parse_error!(
                "{} values for {required_column_count} columns",
                values.len()
            );
        }
    }
    let (num_values, cursor_id) = match body {
        InsertBody::Select(select, _) => {
            // Simple common case of INSERT INTO <table> VALUES (...) without compounds.
            if select.body.compounds.is_empty()
                && matches!(&select.body.select, OneSelect::Values(values) if values.len() <= 1)
            {
                (
                    values.len(),
                    program.alloc_cursor_id(CursorType::BTreeTable(ctx.table.clone())),
                )
            } else {
                // Multiple rows - use coroutine for value population
                let yield_reg = program.alloc_register();
                let jump_on_definition_label = program.allocate_label();
                let start_offset_label = program.allocate_label();
                program.emit_insn(Insn::InitCoroutine {
                    yield_reg,
                    jump_on_definition: jump_on_definition_label,
                    start_offset: start_offset_label,
                });
                program.preassign_label_to_next_insn(start_offset_label);

                let query_destination = QueryDestination::CoroutineYield {
                    yield_reg,
                    coroutine_implementation_start: ctx.halt_label,
                };
                program.incr_nesting();
                let result =
                    translate_select(select, resolver, program, query_destination, connection)?;
                if result.num_result_cols != required_column_count {
                    crate::bail_parse_error!(
                        "{} values for {required_column_count} columns",
                        result.num_result_cols,
                    );
                }
                program = result.program;
                program.decr_nesting();

                program.emit_insn(Insn::EndCoroutine { yield_reg });
                program.preassign_label_to_next_insn(jump_on_definition_label);

                let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(ctx.table.clone()));

                // From SQLite
                /* Set useTempTable to TRUE if the result of the SELECT statement
                 ** should be written into a temporary table (template 4).  Set to
                 ** FALSE if each output row of the SELECT can be written directly into
                 ** the destination table (template 3).
                 **
                 ** A temp table must be used if the table being updated is also one
                 ** of the tables being read by the SELECT statement.  Also use a
                 ** temp table in the case of row triggers.
                 */
                if program.is_table_open(table) {
                    let temp_cursor_id =
                        program.alloc_cursor_id(CursorType::BTreeTable(ctx.table.clone()));
                    ctx.temp_table_ctx = Some(TempTableCtx {
                        cursor_id: temp_cursor_id,
                        loop_start_label: program.allocate_label(),
                        loop_end_label: program.allocate_label(),
                    });

                    program.emit_insn(Insn::OpenEphemeral {
                        cursor_id: temp_cursor_id,
                        is_table: true,
                    });

                    // Main loop
                    program.preassign_label_to_next_insn(ctx.loop_start_label);
                    let yield_label = program.allocate_label();
                    program.emit_insn(Insn::Yield {
                        yield_reg,
                        end_offset: yield_label, // stays local, we’ll route at loop end
                    });

                    let record_reg = program.alloc_register();
                    let affinity_str = if columns.is_empty() {
                        ctx.table
                            .columns
                            .iter()
                            .filter(|col| !col.hidden())
                            .map(|col| col.affinity().aff_mask())
                            .collect::<String>()
                    } else {
                        columns
                            .iter()
                            .map(|col_name| {
                                let column_name = normalize_ident(col_name.as_str());
                                if ROWID_STRS
                                    .iter()
                                    .any(|s| s.eq_ignore_ascii_case(&column_name))
                                {
                                    return Affinity::Integer.aff_mask();
                                }
                                table
                                    .get_column_by_name(&column_name)
                                    .unwrap()
                                    .1
                                    .affinity()
                                    .aff_mask()
                            })
                            .collect::<String>()
                    };

                    program.emit_insn(Insn::MakeRecord {
                        start_reg: program.reg_result_cols_start.unwrap_or(yield_reg + 1),
                        count: result.num_result_cols,
                        dest_reg: record_reg,
                        index_name: None,
                        affinity_str: Some(affinity_str),
                    });

                    let rowid_reg = program.alloc_register();
                    program.emit_insn(Insn::NewRowid {
                        cursor: temp_cursor_id,
                        rowid_reg,
                        prev_largest_reg: 0,
                    });
                    program.emit_insn(Insn::Insert {
                        cursor: temp_cursor_id,
                        key_reg: rowid_reg,
                        record_reg,
                        // since we are not doing an Insn::NewRowid or an Insn::NotExists here, we need to seek to ensure the insertion happens in the correct place.
                        flag: InsertFlags::new().require_seek(),
                        table_name: "".to_string(),
                    });
                    // loop back
                    program.emit_insn(Insn::Goto {
                        target_pc: ctx.loop_start_label,
                    });
                    program.preassign_label_to_next_insn(yield_label);

                    program.emit_insn(Insn::OpenWrite {
                        cursor_id,
                        root_page: RegisterOrLiteral::Literal(ctx.table.root_page),
                        db: 0,
                    });
                } else {
                    program.emit_insn(Insn::OpenWrite {
                        cursor_id,
                        root_page: RegisterOrLiteral::Literal(ctx.table.root_page),
                        db: 0,
                    });

                    program.preassign_label_to_next_insn(ctx.loop_start_label);

                    // on EOF, jump to select_exhausted to check FK constraints
                    let select_exhausted = program.allocate_label();
                    ctx.select_exhausted_label = Some(select_exhausted);
                    program.emit_insn(Insn::Yield {
                        yield_reg,
                        end_offset: select_exhausted,
                    });
                }

                ctx.yield_reg_opt = Some(yield_reg);
                (result.num_result_cols, cursor_id)
            }
        }
        InsertBody::DefaultValues => {
            let num_values = table.columns().len();
            values.extend(table.columns().iter().map(|c| {
                c.default
                    .clone()
                    .unwrap_or(Box::new(ast::Expr::Literal(ast::Literal::Null)))
            }));
            (
                num_values,
                program.alloc_cursor_id(CursorType::BTreeTable(ctx.table.clone())),
            )
        }
    };
    ctx.num_values = num_values;
    ctx.cursor_id = cursor_id;
    Ok(program)
}

pub struct AutoincMeta {
    seq_cursor_id: usize,
    r_seq: usize,
    r_seq_rowid: usize,
    table_name_reg: usize,
}

pub const ROWID_COLUMN: Column = Column::new(
    None,          // name
    String::new(), // type string
    None,          // default
    schema::Type::Integer,
    None,
    true,  // primary key
    true,  // rowid alias
    true,  // notnull
    false, // unique
    false, // hidden
);

/// Represents how a table should be populated during an INSERT.
#[derive(Debug)]
pub struct Insertion<'a> {
    /// The integer key ("rowid") provided to the VDBE.
    key: InsertionKey<'a>,
    /// The column values that will be fed to the MakeRecord instruction to insert the row.
    /// If the table has a rowid alias column, it will also be included in this record,
    /// but a NULL will be stored for it.
    col_mappings: Vec<ColMapping<'a>>,
    /// The register that will contain the record built using the MakeRecord instruction.
    record_reg: usize,
}

impl<'a> Insertion<'a> {
    /// Return the register that contains the rowid.
    pub fn key_register(&self) -> usize {
        self.key.register()
    }

    /// Return the first register of the values that used to build the record
    /// for the main table insert.
    pub fn first_col_register(&self) -> usize {
        self.col_mappings
            .first()
            .expect("columns must be present")
            .register
    }

    /// Return the register that contains the record built using the MakeRecord instruction.
    pub fn record_register(&self) -> usize {
        self.record_reg
    }

    /// Returns the column mapping for a given column name.
    pub fn get_col_mapping_by_name(&self, name: &str) -> Option<&ColMapping<'a>> {
        if let InsertionKey::RowidAlias(mapping) = &self.key {
            // If the key is a rowid alias, a NULL is emitted as the column value,
            // so we need to return the key mapping instead so that the non-NULL rowid is used
            // for the index insert.
            if mapping
                .column
                .name
                .as_ref()
                .is_some_and(|n| n.eq_ignore_ascii_case(name))
            {
                return Some(mapping);
            }
        }
        self.col_mappings.iter().find(|col| {
            col.column
                .name
                .as_ref()
                .is_some_and(|n| n.eq_ignore_ascii_case(name))
        })
    }
}

#[derive(Debug)]
enum InsertionKey<'a> {
    /// Rowid is not provided by user and will be autogenerated.
    Autogenerated { register: usize },
    /// Rowid is provided via the 'rowid' keyword.
    LiteralRowid {
        value_index: Option<usize>,
        register: usize,
    },
    /// Rowid is provided via a rowid alias column.
    RowidAlias(ColMapping<'a>),
}

impl InsertionKey<'_> {
    fn register(&self) -> usize {
        match self {
            InsertionKey::Autogenerated { register } => *register,
            InsertionKey::LiteralRowid { register, .. } => *register,
            InsertionKey::RowidAlias(x) => x.register,
        }
    }
    fn is_provided_by_user(&self) -> bool {
        !matches!(self, InsertionKey::Autogenerated { .. })
    }

    fn column_name(&self) -> &str {
        match self {
            InsertionKey::RowidAlias(x) => x
                .column
                .name
                .as_ref()
                .expect("rowid alias column must be present")
                .as_str(),
            InsertionKey::LiteralRowid { .. } => ROWID_STRS[0],
            InsertionKey::Autogenerated { .. } => ROWID_STRS[0],
        }
    }
}

/// Represents how a column in a table should be populated during an INSERT.
/// In a vector of [ColMapping], the index of a given [ColMapping] is
/// the position of the column in the table.
#[derive(Debug)]
pub struct ColMapping<'a> {
    /// Column definition
    pub column: &'a Column,
    /// Index of the value to use from a tuple in the insert statement.
    /// This is needed because the values in the insert statement are not necessarily
    /// in the same order as the columns in the table, nor do they necessarily contain
    /// all of the columns in the table.
    /// If None, a NULL will be emitted for the column, unless it has a default value.
    /// A NULL rowid alias column's value will be autogenerated.
    pub value_index: Option<usize>,
    /// Register where the value will be stored for insertion into the table.
    pub register: usize,
}

/// Resolves how each column in a table should be populated during an INSERT.
/// Returns an [Insertion] struct that contains the key and record for the insertion.
fn build_insertion<'a>(
    program: &mut ProgramBuilder,
    table: &'a Table,
    columns: &'a [ast::Name],
    num_values: usize,
) -> Result<Insertion<'a>> {
    let table_columns = table.columns();
    let rowid_register = program.alloc_register();
    let mut insertion_key = InsertionKey::Autogenerated {
        register: rowid_register,
    };
    let mut column_mappings = table
        .columns()
        .iter()
        .map(|c| ColMapping {
            column: c,
            value_index: None,
            register: program.alloc_register(),
        })
        .collect::<Vec<_>>();

    if columns.is_empty() {
        // Case 1: No columns specified - map values to columns in order
        if num_values != table_columns.iter().filter(|c| !c.hidden()).count() {
            crate::bail_parse_error!(
                "table {} has {} columns but {} values were supplied",
                &table.get_name(),
                table_columns.len(),
                num_values
            );
        }
        let mut value_idx = 0;
        for (i, col) in table_columns.iter().enumerate() {
            if col.hidden() {
                // Hidden columns are not taken into account.
                continue;
            }
            if col.is_rowid_alias() {
                insertion_key = InsertionKey::RowidAlias(ColMapping {
                    column: col,
                    value_index: Some(value_idx),
                    register: rowid_register,
                });
            } else {
                column_mappings[i].value_index = Some(value_idx);
            }
            value_idx += 1;
        }
    } else {
        // Case 2: Columns specified - map named columns to their values
        // Map each named column to its value index
        for (value_index, column_name) in columns.iter().enumerate() {
            let column_name = normalize_ident(column_name.as_str());
            if let Some((idx_in_table, col_in_table)) = table.get_column_by_name(&column_name) {
                // Named column
                if col_in_table.is_rowid_alias() {
                    insertion_key = InsertionKey::RowidAlias(ColMapping {
                        column: col_in_table,
                        value_index: Some(value_index),
                        register: rowid_register,
                    });
                } else {
                    column_mappings[idx_in_table].value_index = Some(value_index);
                }
            } else if ROWID_STRS
                .iter()
                .any(|s| s.eq_ignore_ascii_case(&column_name))
            {
                // Explicit use of the 'rowid' keyword
                if let Some(col_in_table) = table.columns().iter().find(|c| c.is_rowid_alias()) {
                    insertion_key = InsertionKey::RowidAlias(ColMapping {
                        column: col_in_table,
                        value_index: Some(value_index),
                        register: rowid_register,
                    });
                } else {
                    insertion_key = InsertionKey::LiteralRowid {
                        value_index: Some(value_index),
                        register: rowid_register,
                    };
                }
            } else {
                crate::bail_parse_error!(
                    "table {} has no column named {}",
                    &table.get_name(),
                    column_name
                );
            }
        }
    }

    Ok(Insertion {
        key: insertion_key,
        col_mappings: column_mappings,
        record_reg: program.alloc_register(),
    })
}

/// Populates the column registers with values for multiple rows.
/// This is used for INSERT INTO <table> VALUES (...), (...), ... or INSERT INTO <table> SELECT ...
/// which use either a coroutine or an ephemeral table as the value source.
fn translate_rows_multiple<'short, 'long: 'short>(
    program: &mut ProgramBuilder,
    insertion: &'short Insertion<'long>,
    yield_reg: usize,
    resolver: &Resolver,
    temp_table_ctx: &Option<TempTableCtx>,
) -> Result<()> {
    if let Some(ref temp_table_ctx) = temp_table_ctx {
        // Rewind loop to read from ephemeral table
        program.emit_insn(Insn::Rewind {
            cursor_id: temp_table_ctx.cursor_id,
            pc_if_empty: temp_table_ctx.loop_end_label,
        });
        program.preassign_label_to_next_insn(temp_table_ctx.loop_start_label);
    }
    let translate_value_fn =
        |prg: &mut ProgramBuilder, value_index: usize, column_register: usize| {
            if let Some(temp_table_ctx) = temp_table_ctx {
                prg.emit_insn(Insn::Column {
                    cursor_id: temp_table_ctx.cursor_id,
                    column: value_index,
                    dest: column_register,
                    default: None,
                });
            } else {
                prg.emit_insn(Insn::Copy {
                    src_reg: yield_reg + value_index,
                    dst_reg: column_register,
                    extra_amount: 0,
                });
            }
            Ok(())
        };
    translate_rows_base(program, insertion, translate_value_fn, resolver)
}
/// Populates the column registers with values for a single row
#[allow(clippy::too_many_arguments)]
fn translate_rows_single(
    program: &mut ProgramBuilder,
    value: &[Box<Expr>],
    insertion: &Insertion,
    resolver: &Resolver,
) -> Result<()> {
    let translate_value_fn =
        |prg: &mut ProgramBuilder, value_index: usize, column_register: usize| -> Result<()> {
            translate_expr_no_constant_opt(
                prg,
                None,
                value.get(value_index).unwrap_or_else(|| {
                    panic!("value index out of bounds: {value_index} for value: {value:?}")
                }),
                column_register,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
            Ok(())
        };
    translate_rows_base(program, insertion, translate_value_fn, resolver)
}

/// Translate the key and the columns of the insertion.
/// This function is called by both [translate_rows_single] and [translate_rows_multiple],
/// each providing a different [translate_value_fn] implementation, because for multiple rows
/// we need to emit the values in a loop, from either an ephemeral table or a coroutine,
/// whereas for the single row the translation happens in a single pass without looping.
fn translate_rows_base<'short, 'long: 'short>(
    program: &mut ProgramBuilder,
    insertion: &'short Insertion<'long>,
    mut translate_value_fn: impl FnMut(&mut ProgramBuilder, usize, usize) -> Result<()>,
    resolver: &Resolver,
) -> Result<()> {
    translate_key(program, insertion, &mut translate_value_fn, resolver)?;
    for col in insertion.col_mappings.iter() {
        translate_column(
            program,
            col.column,
            col.register,
            col.value_index,
            &mut translate_value_fn,
            resolver,
        )?;
    }

    Ok(())
}

/// Translate the [InsertionKey].
fn translate_key(
    program: &mut ProgramBuilder,
    insertion: &Insertion,
    mut translate_value_fn: impl FnMut(&mut ProgramBuilder, usize, usize) -> Result<()>,
    resolver: &Resolver,
) -> Result<()> {
    match &insertion.key {
        InsertionKey::RowidAlias(rowid_alias_column) => translate_column(
            program,
            rowid_alias_column.column,
            rowid_alias_column.register,
            rowid_alias_column.value_index,
            &mut translate_value_fn,
            resolver,
        ),
        InsertionKey::LiteralRowid {
            value_index,
            register,
        } => translate_column(
            program,
            &ROWID_COLUMN,
            *register,
            *value_index,
            &mut translate_value_fn,
            resolver,
        ),
        InsertionKey::Autogenerated { .. } => Ok(()), // will be populated later
    }
}

fn translate_column(
    program: &mut ProgramBuilder,
    column: &Column,
    column_register: usize,
    value_index: Option<usize>,
    translate_value_fn: &mut impl FnMut(&mut ProgramBuilder, usize, usize) -> Result<()>,
    resolver: &Resolver,
) -> Result<()> {
    if let Some(value_index) = value_index {
        translate_value_fn(program, value_index, column_register)?;
    } else if column.is_rowid_alias() {
        // Although a non-NULL integer key is used for the insertion key,
        // the rowid alias column is emitted as NULL.
        program.emit_insn(Insn::SoftNull {
            reg: column_register,
        });
    } else if column.hidden() {
        // Emit NULL for not-explicitly-mentioned hidden columns, even ignoring DEFAULT.
        program.emit_insn(Insn::Null {
            dest: column_register,
            dest_end: None,
        });
    } else if let Some(default_expr) = column.default.as_ref() {
        translate_expr(program, None, default_expr, column_register, resolver)?;
    } else {
        let nullable = !column.notnull() && !column.primary_key() && !column.unique();
        if !nullable {
            crate::bail_parse_error!(
                "column {} is not nullable",
                column
                    .name
                    .as_ref()
                    .expect("column name must be present")
                    .as_str()
            );
        }
        program.emit_insn(Insn::Null {
            dest: column_register,
            dest_end: None,
        });
    }
    Ok(())
}

// Preflight phase: evaluate each applicable UNIQUE constraint and probe with NoConflict.
// If any probe hits:
// DO NOTHING -> jump to row_done_label.
//
// DO UPDATE (matching target) -> fetch conflicting rowid and jump to `upsert_entry`.
//
// otherwise, raise SQLITE_CONSTRAINT_UNIQUE
fn emit_preflight_constraint_checks(
    program: &mut ProgramBuilder,
    ctx: &InsertEmitCtx,
    resolver: &Resolver,
    insertion: &Insertion,
    upsert_actions: &[(ResolvedUpsertTarget, BranchOffset, Box<Upsert>)],
    constraints: &ConstraintsToCheck,
) -> Result<()> {
    for (constraint, position) in &constraints.constraints_to_check {
        match constraint {
            ResolvedUpsertTarget::PrimaryKey => {
                let make_record_label = program.allocate_label();
                program.emit_insn(Insn::NotExists {
                    cursor: ctx.cursor_id,
                    rowid_reg: insertion.key_register(),
                    target_pc: make_record_label,
                });
                let rowid_column_name = insertion.key.column_name();

                // Conflict on rowid: attempt to route through UPSERT if it targets the PK, otherwise raise constraint.
                // emit Halt for every case *except* when upsert handles the conflict
                'emit_halt: {
                    if let Some(position) = position.or(constraints.upsert_catch_all_position) {
                        // PK conflict: the conflicting rowid is exactly the attempted key
                        program.emit_insn(Insn::Copy {
                            src_reg: insertion.key_register(),
                            dst_reg: ctx.conflict_rowid_reg,
                            extra_amount: 0,
                        });
                        program.emit_insn(Insn::Goto {
                            target_pc: upsert_actions[position].1,
                        });
                        break 'emit_halt;
                    }
                    let mut description =
                        String::with_capacity(ctx.table.name.len() + rowid_column_name.len() + 2);
                    description.push_str(ctx.table.name.as_str());
                    description.push('.');
                    description.push_str(rowid_column_name);
                    program.emit_insn(Insn::Halt {
                        err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                        description,
                    });
                }
                program.preassign_label_to_next_insn(make_record_label);
            }
            ResolvedUpsertTarget::Index(index) => {
                let column_mappings = index
                    .columns
                    .iter()
                    .map(|idx_col| insertion.get_col_mapping_by_name(&idx_col.name));
                // find which cursor we opened earlier for this index
                let idx_cursor_id = ctx
                    .idx_cursors
                    .iter()
                    .find(|(name, _, _)| *name == &index.name)
                    .map(|(_, _, c_id)| *c_id)
                    .expect("no cursor found for index");

                let maybe_skip_probe_label = if let Some(where_clause) = &index.where_clause {
                    let mut where_for_eval = where_clause.as_ref().clone();
                    rewrite_partial_index_where(&mut where_for_eval, insertion)?;
                    let reg = program.alloc_register();
                    translate_expr_no_constant_opt(
                        program,
                        Some(&TableReferences::new_empty()),
                        &where_for_eval,
                        reg,
                        resolver,
                        NoConstantOptReason::RegisterReuse,
                    )?;
                    let lbl = program.allocate_label();
                    program.emit_insn(Insn::IfNot {
                        reg,
                        target_pc: lbl,
                        jump_if_null: true,
                    });
                    Some(lbl)
                } else {
                    None
                };

                let num_cols = index.columns.len();
                // allocate scratch registers for the index columns plus rowid
                let idx_start_reg = program.alloc_registers(num_cols + 1);

                // build unpacked key [idx_start_reg .. idx_start_reg+num_cols-1], and rowid in last reg,
                // copy each index column from the table's column registers into these scratch regs
                for (i, column_mapping) in column_mappings.clone().enumerate() {
                    // copy from the table's column register over to the index's scratch register
                    let Some(col_mapping) = column_mapping else {
                        return Err(crate::LimboError::PlanningError(
                            "Column not found in INSERT".to_string(),
                        ));
                    };
                    program.emit_insn(Insn::Copy {
                        src_reg: col_mapping.register,
                        dst_reg: idx_start_reg + i,
                        extra_amount: 0,
                    });
                }
                // last register is the rowid
                program.emit_insn(Insn::Copy {
                    src_reg: insertion.key_register(),
                    dst_reg: idx_start_reg + num_cols,
                    extra_amount: 0,
                });

                if index.unique {
                    let aff = index
                        .columns
                        .iter()
                        .map(|ic| ctx.table.columns[ic.pos_in_table].affinity().aff_mask())
                        .collect::<String>();
                    program.emit_insn(Insn::Affinity {
                        start_reg: idx_start_reg,
                        count: NonZeroUsize::new(num_cols).expect("nonzero col count"),
                        affinities: aff,
                    });

                    if !upsert_actions.is_empty() {
                        let next_check = program.allocate_label();
                        program.emit_insn(Insn::NoConflict {
                            cursor_id: idx_cursor_id,
                            target_pc: next_check,
                            record_reg: idx_start_reg,
                            num_regs: num_cols,
                        });

                        // Conflict detected, figure out if this UPSERT handles the conflict
                        if let Some(position) = position.or(constraints.upsert_catch_all_position) {
                            match &upsert_actions[position].2.do_clause {
                                UpsertDo::Nothing => {
                                    // Bail out without writing anything
                                    program.emit_insn(Insn::Goto {
                                        target_pc: ctx.row_done_label,
                                    });
                                }
                                UpsertDo::Set { .. } => {
                                    // Route to DO UPDATE: capture conflicting rowid then jump
                                    program.emit_insn(Insn::IdxRowId {
                                        cursor_id: idx_cursor_id,
                                        dest: ctx.conflict_rowid_reg,
                                    });
                                    program.emit_insn(Insn::Goto {
                                        target_pc: upsert_actions[position].1,
                                    });
                                }
                            }
                        }
                        // No matching UPSERT handler so we emit constraint error
                        // (if conflict clause matched - VM will jump to later instructions and skip halt)
                        program.emit_insn(Insn::Halt {
                            err_code: SQLITE_CONSTRAINT_UNIQUE,
                            description: format_unique_violation_desc(
                                ctx.table.name.as_str(),
                                index,
                            ),
                        });

                        // continue preflight with next constraint
                        program.preassign_label_to_next_insn(next_check);
                    } else {
                        // No UPSERT fast-path: probe and immediately insert
                        let ok = program.allocate_label();
                        program.emit_insn(Insn::NoConflict {
                            cursor_id: idx_cursor_id,
                            target_pc: ok,
                            record_reg: idx_start_reg,
                            num_regs: num_cols,
                        });
                        // Unique violation without ON CONFLICT clause -> error
                        program.emit_insn(Insn::Halt {
                            err_code: SQLITE_CONSTRAINT_UNIQUE,
                            description: format_unique_violation_desc(
                                ctx.table.name.as_str(),
                                index,
                            ),
                        });
                        program.preassign_label_to_next_insn(ok);

                        // In the non-UPSERT case, we insert the index
                        let record_reg = program.alloc_register();
                        program.emit_insn(Insn::MakeRecord {
                            start_reg: idx_start_reg,
                            count: num_cols + 1,
                            dest_reg: record_reg,
                            index_name: Some(index.name.clone()),
                            affinity_str: None,
                        });
                        program.emit_insn(Insn::IdxInsert {
                            cursor_id: idx_cursor_id,
                            record_reg,
                            unpacked_start: Some(idx_start_reg),
                            unpacked_count: Some((num_cols + 1) as u16),
                            flags: IdxInsertFlags::new().nchange(true),
                        });
                    }
                } else {
                    // Non-unique index: in UPSERT mode we postpone writes to commit phase.
                    if upsert_actions.is_empty() {
                        // eager insert for non-unique, no UPSERT
                        let record_reg = program.alloc_register();
                        program.emit_insn(Insn::MakeRecord {
                            start_reg: idx_start_reg,
                            count: num_cols + 1,
                            dest_reg: record_reg,
                            index_name: Some(index.name.clone()),
                            affinity_str: None,
                        });
                        program.emit_insn(Insn::IdxInsert {
                            cursor_id: idx_cursor_id,
                            record_reg,
                            unpacked_start: Some(idx_start_reg),
                            unpacked_count: Some((num_cols + 1) as u16),
                            flags: IdxInsertFlags::new().nchange(true),
                        });
                    }
                }

                // Close the partial-index skip (preflight)
                if let Some(lbl) = maybe_skip_probe_label {
                    program.resolve_label(lbl, program.offset());
                }
            }
            ResolvedUpsertTarget::CatchAll => unreachable!(),
        }
    }
    Ok(())
}

// TODO: comeback here later to apply the same improvements on select
fn translate_virtual_table_insert(
    mut program: ProgramBuilder,
    virtual_table: Arc<VirtualTable>,
    columns: Vec<ast::Name>,
    mut body: InsertBody,
    on_conflict: Option<ResolveType>,
    resolver: &Resolver,
) -> Result<ProgramBuilder> {
    if virtual_table.readonly() {
        crate::bail_constraint_error!("Table is read-only: {}", virtual_table.name);
    }
    let (num_values, value) = match &mut body {
        InsertBody::Select(select, None) => match &mut select.body.select {
            OneSelect::Values(values) => (values[0].len(), values.pop().unwrap()),
            _ => crate::bail_parse_error!("Virtual tables only support VALUES clause in INSERT"),
        },
        InsertBody::DefaultValues => (0, vec![]),
        _ => crate::bail_parse_error!("Unsupported INSERT body for virtual tables"),
    };
    let table = Table::Virtual(virtual_table.clone());
    /* *
     * Inserts for virtual tables are done in a single step.
     * argv[0] = (NULL for insert)
     */
    let registers_start = program.alloc_register();
    program.emit_insn(Insn::Null {
        dest: registers_start,
        dest_end: None,
    });
    /* *
     * argv[1] = (rowid for insert - NULL in most cases)
     * argv[2..] = column values
     * */
    let insertion = build_insertion(&mut program, &table, &columns, num_values)?;

    translate_rows_single(&mut program, &value, &insertion, resolver)?;
    let conflict_action = on_conflict.as_ref().map(|c| c.bit_value()).unwrap_or(0) as u16;

    let cursor_id = program.alloc_cursor_id(CursorType::VirtualTable(virtual_table.clone()));

    program.emit_insn(Insn::VUpdate {
        cursor_id,
        arg_count: insertion.col_mappings.len() + 2, // +1 for NULL, +1 for rowid
        start_reg: registers_start,
        conflict_action,
    });

    let halt_label = program.allocate_label();
    program.resolve_label(halt_label, program.offset());

    Ok(program)
}

///  makes sure that an AUTOINCREMENT table has a sequence row in `sqlite_sequence`, inserting one with 0 if missing.
fn ensure_sequence_initialized(
    program: &mut ProgramBuilder,
    schema: &Schema,
    table: &schema::BTreeTable,
) -> Result<()> {
    let seq_table = schema.get_btree_table("sqlite_sequence").ok_or_else(|| {
        crate::error::LimboError::InternalError("sqlite_sequence table not found".to_string())
    })?;

    let seq_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(seq_table.clone()));

    program.emit_insn(Insn::OpenWrite {
        cursor_id: seq_cursor_id,
        root_page: seq_table.root_page.into(),
        db: 0,
    });

    let table_name_reg = program.emit_string8_new_reg(table.name.clone());

    let loop_start_label = program.allocate_label();
    let entry_exists_label = program.allocate_label();
    let insert_new_label = program.allocate_label();

    program.emit_insn(Insn::Rewind {
        cursor_id: seq_cursor_id,
        pc_if_empty: insert_new_label,
    });

    program.preassign_label_to_next_insn(loop_start_label);

    let name_col_reg = program.alloc_register();

    program.emit_column_or_rowid(seq_cursor_id, 0, name_col_reg);

    program.emit_insn(Insn::Eq {
        lhs: table_name_reg,
        rhs: name_col_reg,
        target_pc: entry_exists_label,
        flags: Default::default(),
        collation: None,
    });

    program.emit_insn(Insn::Next {
        cursor_id: seq_cursor_id,
        pc_if_next: loop_start_label,
    });

    program.preassign_label_to_next_insn(insert_new_label);

    let record_reg = program.alloc_register();
    let record_start_reg = program.alloc_registers(2);
    let zero_reg = program.alloc_register();

    program.emit_insn(Insn::Integer {
        dest: zero_reg,
        value: 0,
    });

    program.emit_insn(Insn::Copy {
        src_reg: table_name_reg,
        dst_reg: record_start_reg,
        extra_amount: 0,
    });

    program.emit_insn(Insn::Copy {
        src_reg: zero_reg,
        dst_reg: record_start_reg + 1,
        extra_amount: 0,
    });

    let affinity_str = seq_table
        .columns
        .iter()
        .map(|c| c.affinity().aff_mask())
        .collect();

    program.emit_insn(Insn::MakeRecord {
        start_reg: record_start_reg,
        count: 2,
        dest_reg: record_reg,
        index_name: None,
        affinity_str: Some(affinity_str),
    });

    let new_rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: seq_cursor_id,
        rowid_reg: new_rowid_reg,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: seq_cursor_id,
        key_reg: new_rowid_reg,
        record_reg,
        flag: InsertFlags::new(),
        table_name: "sqlite_sequence".to_string(),
    });

    program.preassign_label_to_next_insn(entry_exists_label);
    program.emit_insn(Insn::Close {
        cursor_id: seq_cursor_id,
    });

    Ok(())
}
#[inline]
/// Build the UNIQUE constraint error description to match sqlite
/// single column: `t.c1`
/// multi-column:  `t.(k, c1)`
pub fn format_unique_violation_desc(table_name: &str, index: &Index) -> String {
    if index.columns.len() == 1 {
        let mut s = String::with_capacity(table_name.len() + 1 + index.columns[0].name.len());
        s.push_str(table_name);
        s.push('.');
        s.push_str(&index.columns[0].name);
        s
    } else {
        let mut s = String::with_capacity(table_name.len() + 3 + 4 * index.columns.len());
        s.push_str(table_name);
        s.push_str(".(");
        s.push_str(
            &index
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        );
        s.push(')');
        s
    }
}

/// Rewrite WHERE clause for partial index to reference insertion registers
pub fn rewrite_partial_index_where(
    expr: &mut ast::Expr,
    insertion: &Insertion,
) -> crate::Result<WalkControl> {
    let col_reg = |name: &str| -> Option<usize> {
        if ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(name)) {
            Some(insertion.key_register())
        } else if let Some(c) = insertion.get_col_mapping_by_name(name) {
            if c.column.is_rowid_alias() {
                Some(insertion.key_register())
            } else {
                Some(c.register)
            }
        } else {
            None
        }
    };
    walk_expr_mut(
        expr,
        &mut |e: &mut ast::Expr| -> crate::Result<WalkControl> {
            match e {
                // NOTE: should not have ANY Expr::Columns bound to the expr
                Expr::Id(name) => {
                    let normalized = normalize_ident(name.as_str());
                    if let Some(reg) = col_reg(&normalized) {
                        *e = Expr::Register(reg);
                    }
                }
                Expr::Qualified(_, col) | Expr::DoublyQualified(_, _, col) => {
                    let normalized = normalize_ident(col.as_str());
                    if let Some(reg) = col_reg(&normalized) {
                        *e = Expr::Register(reg);
                    }
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        },
    )
}

struct ConstraintsToCheck {
    constraints_to_check: Vec<(ResolvedUpsertTarget, Option<usize>)>,
    upsert_catch_all_position: Option<usize>,
}

fn build_constraints_to_check(
    resolver: &Resolver,
    table_name: &str,
    upsert_actions: &[(ResolvedUpsertTarget, BranchOffset, Box<Upsert>)],
    has_user_provided_rowid: bool,
) -> ConstraintsToCheck {
    let mut constraints_to_check = Vec::new();
    if has_user_provided_rowid {
        // Check uniqueness constraint for rowid if it was provided by user.
        // When the DB allocates it there are no need for separate uniqueness checks.
        let position = upsert_actions
            .iter()
            .position(|(target, ..)| matches!(target, ResolvedUpsertTarget::PrimaryKey));
        constraints_to_check.push((ResolvedUpsertTarget::PrimaryKey, position));
    }
    for index in resolver.schema.get_indices(table_name) {
        let position = upsert_actions
            .iter()
            .position(|(target, ..)| matches!(target, ResolvedUpsertTarget::Index(x) if Arc::ptr_eq(x, index)));
        constraints_to_check.push((ResolvedUpsertTarget::Index(index.clone()), position));
    }

    constraints_to_check.sort_by(|(_, p1), (_, p2)| match (p1, p2) {
        (Some(p1), Some(p2)) => p1.cmp(p2),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });

    let upsert_catch_all_position =
        if let Some((ResolvedUpsertTarget::CatchAll, ..)) = upsert_actions.last() {
            Some(upsert_actions.len() - 1)
        } else {
            None
        };
    ConstraintsToCheck {
        constraints_to_check,
        upsert_catch_all_position,
    }
}

fn emit_update_sqlite_sequence(
    program: &mut ProgramBuilder,
    schema: &Schema,
    seq_cursor_id: usize,
    r_seq_rowid: usize,
    table_name_reg: usize,
    new_key_reg: usize,
) -> Result<()> {
    let record_reg = program.alloc_register();
    let record_start_reg = program.alloc_registers(2);
    program.emit_insn(Insn::Copy {
        src_reg: table_name_reg,
        dst_reg: record_start_reg,
        extra_amount: 0,
    });
    program.emit_insn(Insn::Copy {
        src_reg: new_key_reg,
        dst_reg: record_start_reg + 1,
        extra_amount: 0,
    });

    let seq_table = schema.get_btree_table("sqlite_sequence").unwrap();
    let affinity_str = seq_table
        .columns
        .iter()
        .map(|col| col.affinity().aff_mask())
        .collect::<String>();
    program.emit_insn(Insn::MakeRecord {
        start_reg: record_start_reg,
        count: 2,
        dest_reg: record_reg,
        index_name: None,
        affinity_str: Some(affinity_str),
    });

    let update_existing_label = program.allocate_label();
    let end_update_label = program.allocate_label();
    program.emit_insn(Insn::NotNull {
        reg: r_seq_rowid,
        target_pc: update_existing_label,
    });

    program.emit_insn(Insn::NewRowid {
        cursor: seq_cursor_id,
        rowid_reg: r_seq_rowid,
        prev_largest_reg: 0,
    });
    program.emit_insn(Insn::Insert {
        cursor: seq_cursor_id,
        key_reg: r_seq_rowid,
        record_reg,
        flag: InsertFlags::new(),
        table_name: "sqlite_sequence".to_string(),
    });
    program.emit_insn(Insn::Goto {
        target_pc: end_update_label,
    });

    program.preassign_label_to_next_insn(update_existing_label);
    program.emit_insn(Insn::Insert {
        cursor: seq_cursor_id,
        key_reg: r_seq_rowid,
        record_reg,
        flag: InsertFlags(turso_parser::ast::ResolveType::Replace.bit_value() as u8),
        table_name: "sqlite_sequence".to_string(),
    });

    program.preassign_label_to_next_insn(end_update_label);

    Ok(())
}

/// Child-side FK checks for INSERT of a single row:
/// For each outgoing FK on `child_tbl`, if the NEW tuple's FK columns are all non-NULL,
/// verify that the referenced parent key exists.
pub fn emit_fk_child_insert_checks(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    child_tbl: &BTreeTable,
    new_start_reg: usize,
    new_rowid_reg: usize,
) -> crate::Result<()> {
    for fk_ref in resolver.schema.resolved_fks_for_child(&child_tbl.name)? {
        let is_self_ref = fk_ref.fk.parent_table.eq_ignore_ascii_case(&child_tbl.name);

        // Short-circuit if any NEW component is NULL
        let fk_ok = program.allocate_label();
        for cname in &fk_ref.child_cols {
            let (i, col) = child_tbl.get_column(cname).unwrap();
            let src = if col.is_rowid_alias() {
                new_rowid_reg
            } else {
                new_start_reg + i
            };
            program.emit_insn(Insn::IsNull {
                reg: src,
                target_pc: fk_ok,
            });
        }
        let parent_tbl = resolver
            .schema
            .get_btree_table(&fk_ref.fk.parent_table)
            .expect("parent btree");
        if fk_ref.parent_uses_rowid {
            let pcur = open_read_table(program, &parent_tbl);

            // first child col carries rowid
            let (i_child, col_child) = child_tbl.get_column(&fk_ref.child_cols[0]).unwrap();
            let val_reg = if col_child.is_rowid_alias() {
                new_rowid_reg
            } else {
                new_start_reg + i_child
            };

            // Normalize rowid to integer for both the probe and the same-row fast path.
            let tmp = program.alloc_register();
            program.emit_insn(Insn::Copy {
                src_reg: val_reg,
                dst_reg: tmp,
                extra_amount: 0,
            });
            program.emit_insn(Insn::MustBeInt { reg: tmp });

            // If this is a self-reference *and* the child FK equals NEW rowid,
            // the constraint will be satisfied once this row is inserted
            if is_self_ref {
                program.emit_insn(Insn::Eq {
                    lhs: tmp,
                    rhs: new_rowid_reg,
                    target_pc: fk_ok,
                    flags: CmpInsFlags::default(),
                    collation: None,
                });
            }

            let violation = program.allocate_label();
            program.emit_insn(Insn::NotExists {
                cursor: pcur,
                rowid_reg: tmp,
                target_pc: violation,
            });
            program.emit_insn(Insn::Close { cursor_id: pcur });
            program.emit_insn(Insn::Goto { target_pc: fk_ok });

            // Missing parent: immediate vs deferred as usual
            program.preassign_label_to_next_insn(violation);
            program.emit_insn(Insn::Close { cursor_id: pcur });
            emit_fk_violation(program, &fk_ref.fk)?;
            program.preassign_label_to_next_insn(fk_ok);
        } else {
            let idx = fk_ref
                .parent_unique_index
                .as_ref()
                .expect("parent unique index required");
            let icur = open_read_index(program, idx);
            let ncols = fk_ref.child_cols.len();

            // Build NEW child probe from child NEW values, apply parent-index affinities.
            let probe = {
                let start = program.alloc_registers(ncols);
                for (k, cname) in fk_ref.child_cols.iter().enumerate() {
                    let (i, col) = child_tbl.get_column(cname).unwrap();
                    program.emit_insn(Insn::Copy {
                        src_reg: if col.is_rowid_alias() {
                            new_rowid_reg
                        } else {
                            new_start_reg + i
                        },
                        dst_reg: start + k,
                        extra_amount: 0,
                    });
                }
                if let Some(cnt) = NonZeroUsize::new(ncols) {
                    program.emit_insn(Insn::Affinity {
                        start_reg: start,
                        count: cnt,
                        affinities: build_index_affinity_string(idx, &parent_tbl),
                    });
                }
                start
            };
            if is_self_ref {
                // Determine the parent column order to compare against:
                let parent_cols: Vec<&str> =
                    idx.columns.iter().map(|ic| ic.name.as_str()).collect();

                // Build new parent-key image from this same row’s new values, in the index order.
                let parent_new = program.alloc_registers(ncols);
                for (i, pname) in parent_cols.iter().enumerate() {
                    let (pos, col) = child_tbl.get_column(pname).unwrap();
                    program.emit_insn(Insn::Copy {
                        src_reg: if col.is_rowid_alias() {
                            new_rowid_reg
                        } else {
                            new_start_reg + pos
                        },
                        dst_reg: parent_new + i,
                        extra_amount: 0,
                    });
                }
                if let Some(cnt) = NonZeroUsize::new(ncols) {
                    program.emit_insn(Insn::Affinity {
                        start_reg: parent_new,
                        count: cnt,
                        affinities: build_index_affinity_string(idx, &parent_tbl),
                    });
                }

                // Compare child probe to NEW parent image column-by-column.
                let mismatch = program.allocate_label();
                for i in 0..ncols {
                    let cont = program.allocate_label();
                    program.emit_insn(Insn::Eq {
                        lhs: probe + i,
                        rhs: parent_new + i,
                        target_pc: cont,
                        flags: CmpInsFlags::default().jump_if_null(),
                        collation: Some(super::collate::CollationSeq::Binary),
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: mismatch,
                    });
                    program.preassign_label_to_next_insn(cont);
                }
                // All equal: same-row OK
                program.emit_insn(Insn::Goto { target_pc: fk_ok });
                program.preassign_label_to_next_insn(mismatch);
            }
            index_probe(
                program,
                icur,
                probe,
                ncols,
                // on_found: parent exists, FK satisfied
                |_p| Ok(()),
                // on_not_found: behave like a normal FK
                |p| {
                    emit_fk_violation(p, &fk_ref.fk)?;
                    Ok(())
                },
            )?;
            program.emit_insn(Insn::Goto { target_pc: fk_ok });
            program.preassign_label_to_next_insn(fk_ok);
        }
    }
    Ok(())
}

/// Build NEW parent key image in FK parent-column order into a contiguous register block.
/// Handles 3 shapes:
/// - parent_uses_rowid: single "rowid" component
/// - explicit fk.parent_columns
/// - fk.parent_columns empty => use parent's declared PK columns (order-preserving)
fn build_parent_key_image_for_insert(
    program: &mut ProgramBuilder,
    parent_table: &BTreeTable,
    pref: &ResolvedFkRef,
    insertion: &Insertion,
) -> crate::Result<(usize, usize)> {
    // Decide column list
    let parent_cols: Vec<String> = if pref.parent_uses_rowid {
        vec!["rowid".to_string()]
    } else if !pref.fk.parent_columns.is_empty() {
        pref.fk.parent_columns.clone()
    } else {
        // fall back to the declared PK of the parent table, in schema order
        parent_table
            .primary_key_columns
            .iter()
            .map(|(n, _)| n.clone())
            .collect()
    };

    let ncols = parent_cols.len();
    let start = program.alloc_registers(ncols);
    // Copy from the would-be parent insertion
    for (i, pname) in parent_cols.iter().enumerate() {
        let src = if pname.eq_ignore_ascii_case("rowid") {
            insertion.key_register()
        } else {
            // For rowid-alias parents, get_col_mapping_by_name will return the key mapping,
            // not the NULL placeholder in col_mappings.
            insertion
                .get_col_mapping_by_name(pname)
                .ok_or_else(|| {
                    crate::LimboError::PlanningError(format!(
                        "Column '{}' not present in INSERT image for parent {}",
                        pname, parent_table.name
                    ))
                })?
                .register
        };
        program.emit_insn(Insn::Copy {
            src_reg: src,
            dst_reg: start + i,
            extra_amount: 0,
        });
    }

    // Apply affinities of the parent columns (or integer for rowid)
    let aff: String = if pref.parent_uses_rowid {
        "i".to_string()
    } else {
        parent_cols
            .iter()
            .map(|name| {
                let (_, col) = parent_table.get_column(name).ok_or_else(|| {
                    crate::LimboError::InternalError(format!("parent col {name} missing"))
                })?;
                Ok::<_, crate::LimboError>(col.affinity().aff_mask())
            })
            .collect::<Result<String, _>>()?
    };
    if let Some(count) = NonZeroUsize::new(ncols) {
        program.emit_insn(Insn::Affinity {
            start_reg: start,
            count,
            affinities: aff,
        });
    }

    Ok((start, ncols))
}

/// Parent-side: when inserting into the parent, decrement the counter
/// if any child rows reference the NEW parent key.
/// We *always* do this for deferred FKs, and we *also* do it for
/// self-referential FKs (even if immediate) because the insert can
/// “repair” a prior child-insert count recorded earlier in the same statement.
pub fn emit_parent_side_fk_decrement_on_insert(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    parent_table: &BTreeTable,
    insertion: &Insertion,
) -> crate::Result<()> {
    for pref in resolver
        .schema
        .resolved_fks_referencing(&parent_table.name)?
    {
        let is_self_ref = pref
            .child_table
            .name
            .eq_ignore_ascii_case(&parent_table.name);
        // Skip only when it cannot repair anything: non-deferred and not self-referencing
        if !pref.fk.deferred && !is_self_ref {
            continue;
        }
        let (new_pk_start, n_cols) =
            build_parent_key_image_for_insert(program, parent_table, &pref, insertion)?;

        let child_tbl = &pref.child_table;
        let child_cols = &pref.fk.child_columns;
        let idx = resolver.schema.get_indices(&child_tbl.name).find(|ix| {
            ix.columns.len() == child_cols.len()
                && ix
                    .columns
                    .iter()
                    .zip(child_cols.iter())
                    .all(|(ic, cc)| ic.name.eq_ignore_ascii_case(cc))
        });

        if let Some(ix) = idx {
            let icur = open_read_index(program, ix);
            // Copy key into probe regs and apply child-index affinities
            let probe_start = program.alloc_registers(n_cols);
            for i in 0..n_cols {
                program.emit_insn(Insn::Copy {
                    src_reg: new_pk_start + i,
                    dst_reg: probe_start + i,
                    extra_amount: 0,
                });
            }
            if let Some(count) = NonZeroUsize::new(n_cols) {
                program.emit_insn(Insn::Affinity {
                    start_reg: probe_start,
                    count,
                    affinities: build_index_affinity_string(ix, child_tbl),
                });
            }

            let found = program.allocate_label();
            program.emit_insn(Insn::Found {
                cursor_id: icur,
                target_pc: found,
                record_reg: probe_start,
                num_regs: n_cols,
            });

            // Not found, nothing to decrement
            program.emit_insn(Insn::Close { cursor_id: icur });
            let skip = program.allocate_label();
            program.emit_insn(Insn::Goto { target_pc: skip });

            // Found: guarded counter decrement
            program.resolve_label(found, program.offset());
            program.emit_insn(Insn::Close { cursor_id: icur });
            emit_guarded_fk_decrement(program, skip);
            program.resolve_label(skip, program.offset());
        } else {
            // fallback scan :(
            let ccur = open_read_table(program, child_tbl);
            let done = program.allocate_label();
            program.emit_insn(Insn::Rewind {
                cursor_id: ccur,
                pc_if_empty: done,
            });
            let loop_top = program.allocate_label();
            let next_row = program.allocate_label();
            program.resolve_label(loop_top, program.offset());

            for (i, child_name) in child_cols.iter().enumerate() {
                let (pos, _) = child_tbl.get_column(child_name).ok_or_else(|| {
                    crate::LimboError::InternalError(format!("child col {child_name} missing"))
                })?;
                let tmp = program.alloc_register();
                program.emit_insn(Insn::Column {
                    cursor_id: ccur,
                    column: pos,
                    dest: tmp,
                    default: None,
                });

                program.emit_insn(Insn::IsNull {
                    reg: tmp,
                    target_pc: next_row,
                });

                let cont = program.allocate_label();
                program.emit_insn(Insn::Eq {
                    lhs: tmp,
                    rhs: new_pk_start + i,
                    target_pc: cont,
                    flags: CmpInsFlags::default().jump_if_null(),
                    collation: Some(super::collate::CollationSeq::Binary),
                });
                program.emit_insn(Insn::Goto {
                    target_pc: next_row,
                });
                program.resolve_label(cont, program.offset());
            }
            // Matched one child row: guarded decrement of counter
            emit_guarded_fk_decrement(program, next_row);
            program.resolve_label(next_row, program.offset());
            program.emit_insn(Insn::Next {
                cursor_id: ccur,
                pc_if_next: loop_top,
            });
            program.resolve_label(done, program.offset());
            program.emit_insn(Insn::Close { cursor_id: ccur });
        }
    }
    Ok(())
}
