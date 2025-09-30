use std::num::NonZeroUsize;
use std::sync::Arc;
use turso_parser::ast::{
    self, Expr, InsertBody, OneSelect, QualifiedName, ResolveType, ResultColumn, Upsert, UpsertDo,
    With,
};

use crate::error::{
    SQLITE_CONSTRAINT_NOTNULL, SQLITE_CONSTRAINT_PRIMARYKEY, SQLITE_CONSTRAINT_UNIQUE,
};
use crate::schema::{self, Index, Table};
use crate::translate::emitter::{
    emit_cdc_insns, emit_cdc_patch_record, prepare_cdc_if_necessary, OperationMode,
};
use crate::translate::expr::{
    bind_and_rewrite_expr, emit_returning_results, process_returning_clause, walk_expr_mut,
    BindingBehavior, ReturningValueRegisters, WalkControl,
};
use crate::translate::plan::TableReferences;
use crate::translate::planner::ROWID_STRS;
use crate::translate::upsert::{
    collect_set_clauses_for_upsert, emit_upsert, resolve_upsert_target, ResolvedUpsertTarget,
};
use crate::util::normalize_ident;
use crate::vdbe::builder::ProgramBuilderOpts;
use crate::vdbe::insn::{IdxInsertFlags, InsertFlags, RegisterOrLiteral};
use crate::vdbe::BranchOffset;
use crate::{
    schema::{Column, Schema},
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::Insn,
    },
};
use crate::{Result, VirtualTable};

use super::emitter::Resolver;
use super::expr::{translate_expr, translate_expr_no_constant_opt, NoConstantOptReason};
use super::plan::QueryDestination;
use super::select::translate_select;

struct TempTableCtx {
    cursor_id: usize,
    loop_start_label: BranchOffset,
    loop_end_label: BranchOffset,
}

#[allow(clippy::too_many_arguments)]
pub fn translate_insert(
    with: Option<With>,
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
    if with.is_some() {
        crate::bail_parse_error!("WITH clause is not supported");
    }

    if on_conflict.is_some() {
        crate::bail_parse_error!("ON CONFLICT clause is not supported");
    }

    if resolver
        .schema
        .table_has_indexes(&tbl_name.name.to_string())
        && !resolver.schema.indexes_enabled()
    {
        // Let's disable altering a table with indices altogether instead of checking column by
        // column to be extra safe.
        crate::bail_parse_error!(
            "INSERT to table with indexes is disabled. Omit the `--experimental-indexes=false` flag to enable this feature."
        );
    }
    let table_name = &tbl_name.name;

    // Check if this is a system table that should be protected from direct writes
    if crate::schema::is_system_table(table_name.as_str()) {
        crate::bail_parse_error!("table {} may not be modified", table_name);
    }

    let table = match resolver.schema.get_table(table_name.as_str()) {
        Some(table) => table,
        None => crate::bail_parse_error!("no such table: {}", table_name),
    };

    // Check if this is a materialized view
    if resolver.schema.is_materialized_view(table_name.as_str()) {
        crate::bail_parse_error!("cannot modify materialized view {}", table_name);
    }

    // Check if this table has any incompatible dependent views
    let incompatible_views = resolver
        .schema
        .has_incompatible_dependent_views(table_name.as_str());
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
    if !btree_table.has_rowid {
        crate::bail_parse_error!("INSERT into WITHOUT ROWID table is not supported");
    }

    let root_page = btree_table.root_page;

    let mut values: Option<Vec<Box<Expr>>> = None;
    let mut upsert_opt: Option<Upsert> = None;

    let mut inserting_multiple_rows = false;
    if let InsertBody::Select(select, upsert) = &mut body {
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
                                    Expr::Literal(ast::Literal::String(name.as_literal())).into();
                            } else {
                                // an INSERT INTO ... VALUES (...) cannot reference columns
                                crate::bail_parse_error!("no such column: {name}");
                            }
                        }
                        Expr::Qualified(first_name, second_name) => {
                            // an INSERT INTO ... VALUES (...) cannot reference columns
                            crate::bail_parse_error!("no such column: {first_name}.{second_name}");
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
                values = values_expr.pop();
            }
            _ => inserting_multiple_rows = true,
        }
        if let Some(ref mut upsert) = upsert {
            if let UpsertDo::Set {
                ref mut sets,
                ref mut where_clause,
            } = &mut upsert.do_clause
            {
                for set in sets.iter_mut() {
                    bind_and_rewrite_expr(
                        &mut set.expr,
                        None,
                        None,
                        connection,
                        &mut program.param_ctx,
                        BindingBehavior::ResultColumnsNotAllowed,
                    )?;
                }
                if let Some(ref mut where_expr) = where_clause {
                    bind_and_rewrite_expr(
                        where_expr,
                        None,
                        None,
                        connection,
                        &mut program.param_ctx,
                        BindingBehavior::ResultColumnsNotAllowed,
                    )?;
                }
            }
        }
        upsert_opt = upsert.as_deref().cloned();
    }
    // resolve the constrained target for UPSERT if specified
    let resolved_upsert = if let Some(upsert) = &upsert_opt {
        Some(resolve_upsert_target(resolver.schema, &table, upsert)?)
    } else {
        None
    };

    if inserting_multiple_rows && btree_table.has_autoincrement {
        ensure_sequence_initialized(&mut program, resolver.schema, &btree_table)?;
    }

    let halt_label = program.allocate_label();
    let loop_start_label = program.allocate_label();
    let row_done_label = program.allocate_label();

    let cdc_table = prepare_cdc_if_necessary(&mut program, resolver.schema, table.get_name())?;

    // Process RETURNING clause using shared module
    let (mut result_columns, _) = process_returning_clause(
        &mut returning,
        &table,
        table_name.as_str(),
        &mut program,
        connection,
    )?;

    let mut yield_reg_opt = None;
    let mut temp_table_ctx = None;
    let (num_values, cursor_id) = match body {
        InsertBody::Select(select, _) => {
            // Simple Common case of INSERT INTO <table> VALUES (...)
            if matches!(&select.body.select, OneSelect::Values(values) if values.len() <= 1) {
                (
                    values.as_ref().unwrap().len(),
                    program.alloc_cursor_id(CursorType::BTreeTable(btree_table.clone())),
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
                    coroutine_implementation_start: halt_label,
                };
                program.incr_nesting();
                let result =
                    translate_select(select, resolver, program, query_destination, connection)?;
                program = result.program;
                program.decr_nesting();

                program.emit_insn(Insn::EndCoroutine { yield_reg });
                program.preassign_label_to_next_insn(jump_on_definition_label);

                let cursor_id =
                    program.alloc_cursor_id(CursorType::BTreeTable(btree_table.clone()));

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
                if program.is_table_open(&table) {
                    let temp_cursor_id =
                        program.alloc_cursor_id(CursorType::BTreeTable(btree_table.clone()));
                    temp_table_ctx = Some(TempTableCtx {
                        cursor_id: temp_cursor_id,
                        loop_start_label: program.allocate_label(),
                        loop_end_label: program.allocate_label(),
                    });

                    program.emit_insn(Insn::OpenEphemeral {
                        cursor_id: temp_cursor_id,
                        is_table: true,
                    });

                    // Main loop
                    // FIXME: rollback is not implemented. E.g. if you insert 2 rows and one fails to unique constraint violation,
                    // the other row will still be inserted.
                    program.preassign_label_to_next_insn(loop_start_label);

                    let yield_label = program.allocate_label();

                    program.emit_insn(Insn::Yield {
                        yield_reg,
                        end_offset: yield_label,
                    });
                    let record_reg = program.alloc_register();

                    let affinity_str = if columns.is_empty() {
                        btree_table
                            .columns
                            .iter()
                            .filter(|col| !col.hidden)
                            .map(|col| col.affinity().aff_mask())
                            .collect::<String>()
                    } else {
                        columns
                            .iter()
                            .map(|col_name| {
                                let column_name = normalize_ident(col_name.as_str());
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
                        start_reg: yield_reg + 1,
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
                        target_pc: loop_start_label,
                    });

                    program.preassign_label_to_next_insn(yield_label);

                    program.emit_insn(Insn::OpenWrite {
                        cursor_id,
                        root_page: RegisterOrLiteral::Literal(root_page),
                        db: 0,
                    });
                } else {
                    program.emit_insn(Insn::OpenWrite {
                        cursor_id,
                        root_page: RegisterOrLiteral::Literal(root_page),
                        db: 0,
                    });

                    // Main loop
                    // FIXME: rollback is not implemented. E.g. if you insert 2 rows and one fails to unique constraint violation,
                    // the other row will still be inserted.
                    program.preassign_label_to_next_insn(loop_start_label);
                    program.emit_insn(Insn::Yield {
                        yield_reg,
                        end_offset: halt_label,
                    });
                }

                yield_reg_opt = Some(yield_reg);
                (result.num_result_cols, cursor_id)
            }
        }
        InsertBody::DefaultValues => {
            let num_values = table.columns().len();
            values = Some(
                table
                    .columns()
                    .iter()
                    .map(|c| {
                        c.default
                            .clone()
                            .unwrap_or(Box::new(ast::Expr::Literal(ast::Literal::Null)))
                    })
                    .collect(),
            );
            (
                num_values,
                program.alloc_cursor_id(CursorType::BTreeTable(btree_table.clone())),
            )
        }
    };
    let has_upsert = upsert_opt.is_some();

    // Set up the program to return result columns if RETURNING is specified
    if !result_columns.is_empty() {
        program.result_columns = result_columns.clone();
    }

    // allocate cursor id's for each btree index cursor we'll need to populate the indexes
    // (idx name, root_page, idx cursor id)
    let idx_cursors = resolver
        .schema
        .get_indices(table_name.as_str())
        .map(|idx| {
            (
                &idx.name,
                idx.root_page,
                program.alloc_cursor_id(CursorType::BTreeIndex(idx.clone())),
            )
        })
        .collect::<Vec<(&String, i64, usize)>>();

    let insertion = build_insertion(&mut program, &table, &columns, num_values)?;

    let upsert_entry = program.allocate_label();
    let conflict_rowid_reg = program.alloc_register();

    if inserting_multiple_rows {
        translate_rows_multiple(
            &mut program,
            &insertion,
            yield_reg_opt.unwrap() + 1,
            resolver,
            &temp_table_ctx,
        )?;
    } else {
        // Single row - populate registers directly
        program.emit_insn(Insn::OpenWrite {
            cursor_id,
            root_page: RegisterOrLiteral::Literal(root_page),
            db: 0,
        });

        translate_rows_single(&mut program, &values.unwrap(), &insertion, resolver)?;
    }

    // Open all the index btrees for writing
    for idx_cursor in idx_cursors.iter() {
        program.emit_insn(Insn::OpenWrite {
            cursor_id: idx_cursor.2,
            root_page: idx_cursor.1.into(),
            db: 0,
        });
    }

    let has_user_provided_rowid = insertion.key.is_provided_by_user();
    let key_ready_for_uniqueness_check_label = program.allocate_label();
    let key_generation_label = program.allocate_label();

    let mut autoincrement_meta = None;

    if btree_table.has_autoincrement {
        let seq_table = resolver
            .schema
            .get_btree_table("sqlite_sequence")
            .ok_or_else(|| {
                crate::error::LimboError::InternalError(
                    "sqlite_sequence table not found".to_string(),
                )
            })?;
        let seq_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(seq_table.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id: seq_cursor_id,
            root_page: seq_table.root_page.into(),
            db: 0,
        });

        let table_name_reg = program.emit_string8_new_reg(btree_table.name.clone());
        let r_seq = program.alloc_register();
        let r_seq_rowid = program.alloc_register();
        autoincrement_meta = Some((seq_cursor_id, r_seq, r_seq_rowid, table_name_reg));

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
    }

    if has_user_provided_rowid {
        let must_be_int_label = program.allocate_label();

        program.emit_insn(Insn::NotNull {
            reg: insertion.key_register(),
            target_pc: must_be_int_label,
        });

        program.emit_insn(Insn::Goto {
            target_pc: key_generation_label,
        });

        program.preassign_label_to_next_insn(must_be_int_label);
        program.emit_insn(Insn::MustBeInt {
            reg: insertion.key_register(),
        });

        program.emit_insn(Insn::Goto {
            target_pc: key_ready_for_uniqueness_check_label,
        });
    }

    program.preassign_label_to_next_insn(key_generation_label);
    if let Some((_, r_seq, _, _)) = autoincrement_meta {
        let r_max = program.alloc_register();

        let dummy_reg = program.alloc_register();

        program.emit_insn(Insn::NewRowid {
            cursor: cursor_id,
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

        if let Some((seq_cursor_id, _, r_seq_rowid, table_name_reg)) = autoincrement_meta {
            emit_update_sqlite_sequence(
                &mut program,
                resolver.schema,
                seq_cursor_id,
                r_seq_rowid,
                table_name_reg,
                insertion.key_register(),
            )?;
        }
    } else {
        program.emit_insn(Insn::NewRowid {
            cursor: cursor_id,
            rowid_reg: insertion.key_register(),
            prev_largest_reg: 0,
        });
    }

    program.preassign_label_to_next_insn(key_ready_for_uniqueness_check_label);

    // Check uniqueness constraint for rowid if it was provided by user.
    // When the DB allocates it there are no need for separate uniqueness checks.

    if has_user_provided_rowid {
        let make_record_label = program.allocate_label();
        program.emit_insn(Insn::NotExists {
            cursor: cursor_id,
            rowid_reg: insertion.key_register(),
            target_pc: make_record_label,
        });
        let rowid_column_name = insertion.key.column_name();

        // Conflict on rowid: attempt to route through UPSERT if it targets the PK, otherwise raise constraint.
        // emit Halt for every case *except* when upsert handles the conflict
        'emit_halt: {
            if let (Some(_), Some(ref target)) = (upsert_opt.as_mut(), resolved_upsert.as_ref()) {
                if matches!(
                    target,
                    ResolvedUpsertTarget::CatchAll | ResolvedUpsertTarget::PrimaryKey
                ) {
                    // PK conflict: the conflicting rowid is exactly the attempted key
                    program.emit_insn(Insn::Copy {
                        src_reg: insertion.key_register(),
                        dst_reg: conflict_rowid_reg,
                        extra_amount: 0,
                    });
                    program.emit_insn(Insn::Goto {
                        target_pc: upsert_entry,
                    });
                    break 'emit_halt;
                }
            }
            let mut description =
                String::with_capacity(table_name.as_str().len() + rowid_column_name.len() + 2);
            description.push_str(table_name.as_str());
            description.push('.');
            description.push_str(rowid_column_name);
            program.emit_insn(Insn::Halt {
                err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
                description,
            });
        }
        program.preassign_label_to_next_insn(make_record_label);
    }

    match table.btree() {
        Some(t) if t.is_strict => {
            program.emit_insn(Insn::TypeCheck {
                start_reg: insertion.first_col_register(),
                count: insertion.col_mappings.len(),
                check_generated: true,
                table_reference: Arc::clone(&t),
            });
        }
        _ => (),
    }

    // We need to separate index handling and insertion into a `preflight` and a
    // `commit` phase, because in UPSERT mode we might need to skip the actual insertion, as we can
    // have a naked ON CONFLICT DO NOTHING, so if we eagerly insert any indexes, we could insert
    // invalid index entries before we hit a conflict down the line.
    //
    // Preflight phase: evaluate each applicable UNIQUE constraint and probe with NoConflict.
    // If any probe hits:
    // DO NOTHING -> jump to row_done_label.
    //
    // DO UPDATE (matching target) -> fetch conflicting rowid and jump to `upsert_entry`.
    //
    // otherwise, raise SQLITE_CONSTRAINT_UNIQUE
    for index in resolver.schema.get_indices(table_name.as_str()) {
        let column_mappings = index
            .columns
            .iter()
            .map(|idx_col| insertion.get_col_mapping_by_name(&idx_col.name));
        // find which cursor we opened earlier for this index
        let idx_cursor_id = idx_cursors
            .iter()
            .find(|(name, _, _)| *name == &index.name)
            .map(|(_, _, c_id)| *c_id)
            .expect("no cursor found for index");

        let maybe_skip_probe_label = if let Some(where_clause) = &index.where_clause {
            let mut where_for_eval = where_clause.as_ref().clone();
            rewrite_partial_index_where(&mut where_for_eval, &insertion)?;
            let reg = program.alloc_register();
            translate_expr_no_constant_opt(
                &mut program,
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
                .map(|ic| table.columns()[ic.pos_in_table].affinity().aff_mask())
                .collect::<String>();
            program.emit_insn(Insn::Affinity {
                start_reg: idx_start_reg,
                count: NonZeroUsize::new(num_cols).expect("nonzero col count"),
                affinities: aff,
            });

            if has_upsert {
                let next_check = program.allocate_label();
                program.emit_insn(Insn::NoConflict {
                    cursor_id: idx_cursor_id,
                    target_pc: next_check,
                    record_reg: idx_start_reg,
                    num_regs: num_cols,
                });

                // Conflict detected, figure out if this UPSERT handles the conflict
                let upsert_matches_this_index = if let (Some(_u), Some(ref target)) =
                    (upsert_opt.as_ref(), resolved_upsert.as_ref())
                {
                    match target {
                        ResolvedUpsertTarget::CatchAll => true,
                        ResolvedUpsertTarget::Index(tgt) => Arc::ptr_eq(tgt, index),
                        // note: PK handled earlier by rowid path; this is a secondary index
                        ResolvedUpsertTarget::PrimaryKey => false,
                    }
                } else {
                    false
                };

                if upsert_matches_this_index {
                    // Distinguish DO NOTHING vs DO UPDATE
                    match upsert_opt.as_ref().unwrap().do_clause {
                        UpsertDo::Nothing => {
                            // Bail out without writing anything
                            program.emit_insn(Insn::Goto {
                                target_pc: row_done_label,
                            });
                        }
                        UpsertDo::Set { .. } => {
                            // Route to DO UPDATE: capture conflicting rowid then jump
                            program.emit_insn(Insn::IdxRowId {
                                cursor_id: idx_cursor_id,
                                dest: conflict_rowid_reg,
                            });
                            program.emit_insn(Insn::Goto {
                                target_pc: upsert_entry,
                            });
                        }
                    }
                } else {
                    // No matching UPSERT handler so we emit constraint error
                    program.emit_insn(Insn::Halt {
                        err_code: SQLITE_CONSTRAINT_UNIQUE,
                        description: format_unique_violation_desc(table_name.as_str(), index),
                    });
                }

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
                    description: format_unique_violation_desc(table_name.as_str(), index),
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
            if !has_upsert {
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
    for column_mapping in insertion
        .col_mappings
        .iter()
        .filter(|column_mapping| column_mapping.column.notnull)
    {
        // if this is rowid alias - turso-db will emit NULL as a column value and always use rowid for the row as a column value
        if column_mapping.column.is_rowid_alias {
            continue;
        }
        program.emit_insn(Insn::HaltIfNull {
            target_reg: column_mapping.register,
            err_code: SQLITE_CONSTRAINT_NOTNULL,
            description: {
                let mut description = String::with_capacity(
                    table_name.as_str().len()
                        + column_mapping
                            .column
                            .name
                            .as_ref()
                            .expect("Column name must be present")
                            .len()
                        + 2,
                );
                description.push_str(table_name.as_str());
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
        // COMMIT PHASE: no preflight jumps happened; emit the actual index writes now
        // We re-check partial-index predicates against the NEW image, produce packed records,
        // and insert into all applicable indexes, we do not re-probe uniqueness here, as preflight
        // already guaranteed non-conflict.
        for index in resolver.schema.get_indices(table_name.as_str()) {
            let idx_cursor_id = idx_cursors
                .iter()
                .find(|(name, _, _)| *name == &index.name)
                .map(|(_, _, c_id)| *c_id)
                .expect("no cursor found for index");

            // Re-evaluate partial predicate on the would-be inserted image
            let commit_skip_label = if let Some(where_clause) = &index.where_clause {
                let mut where_for_eval = where_clause.as_ref().clone();
                rewrite_partial_index_where(&mut where_for_eval, &insertion)?;
                let reg = program.alloc_register();
                translate_expr_no_constant_opt(
                    &mut program,
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
    }

    program.emit_insn(Insn::Insert {
        cursor: cursor_id,
        key_reg: insertion.key_register(),
        record_reg: insertion.record_register(),
        flag: InsertFlags::new(),
        table_name: table_name.to_string(),
    });

    if let Some((seq_cursor_id, r_seq, r_seq_rowid, table_name_reg)) = autoincrement_meta {
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
    if let Some((cdc_cursor_id, _)) = &cdc_table {
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
        target_pc: row_done_label,
    });

    // Normal INSERT path is done above
    // Any conflict routed to UPSERT jumps past all that to here:
    program.preassign_label_to_next_insn(upsert_entry);
    if let (Some(mut upsert), Some(_)) = (upsert_opt.take(), resolved_upsert.clone()) {
        // Only DO UPDATE (SET ...); DO NOTHING should have already jumped to row_done_label earlier.
        if let UpsertDo::Set {
            ref mut sets,
            ref mut where_clause,
        } = upsert.do_clause
        {
            // Normalize SET pairs once
            let mut rewritten_sets = collect_set_clauses_for_upsert(&table, sets)?;

            emit_upsert(
                &mut program,
                &table,
                &insertion,
                cursor_id,
                conflict_rowid_reg,
                &mut rewritten_sets,
                where_clause,
                resolver,
                &idx_cursors,
                &mut result_columns,
                cdc_table.as_ref().map(|c| c.0),
                row_done_label,
            )?;
        } else {
            // UpsertDo::Nothing case
            program.emit_insn(Insn::Goto {
                target_pc: row_done_label,
            });
        }
    }

    if inserting_multiple_rows {
        if let Some(temp_table_ctx) = temp_table_ctx {
            program.resolve_label(row_done_label, program.offset());

            program.emit_insn(Insn::Next {
                cursor_id: temp_table_ctx.cursor_id,
                pc_if_next: temp_table_ctx.loop_start_label,
            });
            program.preassign_label_to_next_insn(temp_table_ctx.loop_end_label);

            program.emit_insn(Insn::Close {
                cursor_id: temp_table_ctx.cursor_id,
            });
        } else {
            // For multiple rows which not require a temp table, loop back
            program.resolve_label(row_done_label, program.offset());
            program.emit_insn(Insn::Goto {
                target_pc: loop_start_label,
            });
        }
    } else {
        program.resolve_label(row_done_label, program.offset());
    }

    program.resolve_label(halt_label, program.offset());

    Ok(program)
}

pub const ROWID_COLUMN: &Column = &Column {
    name: None,
    ty: schema::Type::Integer,
    ty_str: String::new(),
    primary_key: true,
    is_rowid_alias: true,
    notnull: true,
    default: None,
    unique: false,
    collation: None,
    hidden: false,
};

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
        if num_values != table_columns.iter().filter(|c| !c.hidden).count() {
            crate::bail_parse_error!(
                "table {} has {} columns but {} values were supplied",
                &table.get_name(),
                table_columns.len(),
                num_values
            );
        }
        let mut value_idx = 0;
        for (i, col) in table_columns.iter().enumerate() {
            if col.hidden {
                // Hidden columns are not taken into account.
                continue;
            }
            if col.is_rowid_alias {
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
                if col_in_table.is_rowid_alias {
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
                if let Some(col_in_table) = table.columns().iter().find(|c| c.is_rowid_alias) {
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
                prg.emit_column_or_rowid(temp_table_ctx.cursor_id, value_index, column_register);
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
            ROWID_COLUMN,
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
    } else if column.is_rowid_alias {
        // Although a non-NULL integer key is used for the insertion key,
        // the rowid alias column is emitted as NULL.
        program.emit_insn(Insn::SoftNull {
            reg: column_register,
        });
    } else if column.hidden {
        // Emit NULL for not-explicitly-mentioned hidden columns, even ignoring DEFAULT.
        program.emit_insn(Insn::Null {
            dest: column_register,
            dest_end: None,
        });
    } else if let Some(default_expr) = column.default.as_ref() {
        translate_expr(program, None, default_expr, column_register, resolver)?;
    } else {
        let nullable = !column.notnull && !column.primary_key && !column.unique;
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
            if c.column.is_rowid_alias {
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
