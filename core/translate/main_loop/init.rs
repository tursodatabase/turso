use super::*;

/// Allocates DISTINCT tracking state for SELECT DISTINCT result rows.
pub fn init_distinct(program: &mut ProgramBuilder, plan: &SelectPlan) -> Result<DistinctCtx> {
    let collations = plan
        .result_columns
        .iter()
        .map(|col| {
            get_collseq_from_expr(&col.expr, &plan.table_references)
                .map(|c| c.unwrap_or(CollationSeq::Binary))
        })
        .collect::<Result<Vec<_>>>()?;
    let hash_table_id = program.alloc_hash_table_id();
    let ctx = DistinctCtx {
        hash_table_id,
        collations,
        label_on_conflict: program.allocate_label(),
    };

    Ok(ctx)
}

/// Open all remaining indexes (not already opened by the operation) for writing.
/// Used by DELETE operations which need write access to all indexes.
fn open_write_remaining_indexes(
    program: &mut ProgramBuilder,
    t_ctx: &TranslateCtx,
    table: &JoinedTable,
) -> Result<()> {
    let indices: Vec<_> = t_ctx.resolver.with_schema(table.database_id, |s| {
        s.get_indices(table.table.get_name()).cloned().collect()
    });
    for idx in &indices {
        if table
            .op
            .index()
            .is_some_and(|table_index| table_index.name == idx.name)
        {
            continue;
        }
        let cursor_id = program
            .alloc_cursor_index(Some(CursorKey::index(table.internal_id, idx.clone())), idx)?;
        program.emit_insn(Insn::OpenWrite {
            cursor_id,
            root_page: idx.root_page.into(),
            db: table.database_id,
        });
    }
    Ok(())
}

/// Open cursors for a BTreeTable scan operation.
#[allow(clippy::too_many_arguments)]
fn init_cursor_btree_scan(
    program: &mut ProgramBuilder,
    t_ctx: &TranslateCtx,
    table: &JoinedTable,
    index: &Option<Arc<Index>>,
    table_cursor_id: Option<CursorID>,
    index_cursor_id: Option<CursorID>,
    mode: &OperationMode,
) -> Result<()> {
    let Table::BTree(btree) = &table.table else {
        return Ok(());
    };
    let root_page = btree.root_page;
    match mode {
        OperationMode::SELECT => {
            if let Some(cursor_id) = table_cursor_id {
                program.emit_insn(Insn::OpenRead {
                    cursor_id,
                    root_page,
                    db: table.database_id,
                });
            }
            if let Some(index_cursor_id) = index_cursor_id {
                program.emit_insn(Insn::OpenRead {
                    cursor_id: index_cursor_id,
                    root_page: index.as_ref().unwrap().root_page,
                    db: table.database_id,
                });
            }
        }
        OperationMode::DELETE => {
            program.emit_insn(Insn::OpenWrite {
                cursor_id: table_cursor_id
                    .expect("table cursor is always opened in OperationMode::DELETE"),
                root_page: root_page.into(),
                db: table.database_id,
            });
            if let Some(index_cursor_id) = index_cursor_id {
                program.emit_insn(Insn::OpenWrite {
                    cursor_id: index_cursor_id,
                    root_page: index.as_ref().unwrap().root_page.into(),
                    db: table.database_id,
                });
            }
            open_write_remaining_indexes(program, t_ctx, table)?;
        }
        OperationMode::UPDATE(update_mode) => {
            match &update_mode {
                UpdateRowSource::Normal => {
                    program.emit_insn(Insn::OpenWrite {
                        cursor_id: table_cursor_id
                            .expect("table cursor is always opened in OperationMode::UPDATE"),
                        root_page: root_page.into(),
                        db: table.database_id,
                    });
                }
                UpdateRowSource::PrebuiltEphemeralTable { target_table, .. } => {
                    let target_table_cursor_id =
                        program.resolve_cursor_id(&CursorKey::table(target_table.internal_id));
                    program.emit_insn(Insn::OpenWrite {
                        cursor_id: target_table_cursor_id,
                        root_page: target_table.btree().unwrap().root_page.into(),
                        db: target_table.database_id,
                    });
                }
            }
            let write_db_id = match &update_mode {
                UpdateRowSource::PrebuiltEphemeralTable { target_table, .. } => {
                    target_table.database_id
                }
                _ => table.database_id,
            };
            if let Some(index_cursor_id) = index_cursor_id {
                program.emit_insn(Insn::OpenWrite {
                    cursor_id: index_cursor_id,
                    root_page: index.as_ref().unwrap().root_page.into(),
                    db: write_db_id,
                });
            }
        }
        _ => {}
    }
    Ok(())
}

/// Open cursors for a VirtualTable scan operation.
#[allow(unused_variables)]
fn init_cursor_virtual_table(
    program: &mut ProgramBuilder,
    t_ctx: &TranslateCtx,
    table: &JoinedTable,
    table_cursor_id: Option<CursorID>,
    mode: &OperationMode,
) -> Result<()> {
    if let Table::Virtual(tbl) = &table.table {
        let is_write = matches!(
            mode,
            OperationMode::INSERT | OperationMode::UPDATE { .. } | OperationMode::DELETE
        );
        let allow_dbpage_write = {
            #[cfg(feature = "cli_only")]
            {
                t_ctx.unsafe_testing && tbl.name == crate::dbpage::DBPAGE_TABLE_NAME
            }
            #[cfg(not(feature = "cli_only"))]
            {
                false
            }
        };
        if is_write && tbl.readonly() && !allow_dbpage_write {
            return Err(crate::LimboError::ReadOnly);
        }
        if let Some(cursor_id) = table_cursor_id {
            program.emit_insn(Insn::VOpen { cursor_id });
            if is_write && !allow_dbpage_write {
                program.emit_insn(Insn::VBegin { cursor_id });
            }
        }
    }
    Ok(())
}

/// Open cursors for a Search (index seek or rowid lookup) operation.
#[allow(clippy::too_many_arguments)]
fn init_cursor_search(
    program: &mut ProgramBuilder,
    t_ctx: &TranslateCtx,
    table: &JoinedTable,
    search: &Search,
    table_cursor_id: Option<CursorID>,
    index_cursor_id: Option<CursorID>,
    mode: &OperationMode,
) -> Result<()> {
    match mode {
        OperationMode::SELECT => {
            if let Some(table_cursor_id) = table_cursor_id {
                program.emit_insn(Insn::OpenRead {
                    cursor_id: table_cursor_id,
                    root_page: table.table.get_root_page()?,
                    db: table.database_id,
                });
            }
        }
        OperationMode::DELETE | OperationMode::UPDATE { .. } => {
            let table_cursor_id = table_cursor_id.expect(
                "table cursor is always opened in OperationMode::DELETE or OperationMode::UPDATE",
            );

            program.emit_insn(Insn::OpenWrite {
                cursor_id: table_cursor_id,
                root_page: table.table.get_root_page()?.into(),
                db: table.database_id,
            });

            // For DELETE, we need to open all the indexes for writing
            // UPDATE opens these in emit_program_for_update() separately
            if matches!(mode, OperationMode::DELETE) {
                open_write_remaining_indexes(program, t_ctx, table)?;
            }
        }
        _ => {
            return Err(crate::LimboError::InternalError(
                "INSERT mode is not supported for Search operations".to_string(),
            ));
        }
    }

    if let Search::Seek {
        index: Some(index), ..
    } = search
    {
        // Ephemeral index cursor are opened ad-hoc when needed.
        if !index.ephemeral {
            match mode {
                OperationMode::SELECT => {
                    program.emit_insn(Insn::OpenRead {
                        cursor_id: index_cursor_id
                            .expect("index cursor is always opened in Seek with index"),
                        root_page: index.root_page,
                        db: table.database_id,
                    });
                }
                OperationMode::UPDATE { .. } | OperationMode::DELETE => {
                    program.emit_insn(Insn::OpenWrite {
                        cursor_id: index_cursor_id
                            .expect("index cursor is always opened in Seek with index"),
                        root_page: index.root_page.into(),
                        db: table.database_id,
                    });
                }
                _ => {
                    return Err(crate::LimboError::InternalError(
                        "INSERT mode is not supported for indexed Search operations".to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Open cursors for an IndexMethodQuery operation.
fn init_cursor_index_method_query(
    program: &mut ProgramBuilder,
    table: &JoinedTable,
    table_cursor_id: Option<CursorID>,
    index_cursor_id: Option<CursorID>,
    mode: &OperationMode,
) -> Result<()> {
    match mode {
        OperationMode::SELECT => {
            if let Some(table_cursor_id) = table_cursor_id {
                program.emit_insn(Insn::OpenRead {
                    cursor_id: table_cursor_id,
                    root_page: table.table.get_root_page()?,
                    db: table.database_id,
                });
            }
            let index_cursor_id = index_cursor_id.unwrap();
            program.emit_insn(Insn::OpenRead {
                cursor_id: index_cursor_id,
                root_page: table.op.index().unwrap().root_page,
                db: table.database_id,
            });
        }
        _ => panic!("only SELECT is supported for index method"),
    }
    Ok(())
}

/// Open cursors for a HashJoin probe table.
fn init_cursor_hash_join_probe(
    program: &mut ProgramBuilder,
    table: &JoinedTable,
    table_cursor_id: Option<CursorID>,
    mode: &OperationMode,
) -> Result<()> {
    match mode {
        OperationMode::SELECT => {
            if let Some(table_cursor_id) = table_cursor_id {
                let Table::BTree(btree) = &table.table else {
                    panic!("Expected hash join probe table to be a BTree table");
                };
                program.emit_insn(Insn::OpenRead {
                    cursor_id: table_cursor_id,
                    root_page: btree.root_page,
                    db: table.database_id,
                });
            }
        }
        _ => unreachable!("Hash joins should only occur in SELECT operations"),
    }
    Ok(())
}

/// Open cursors for a MultiIndexScan operation.
fn init_cursor_multi_index_scan(
    program: &mut ProgramBuilder,
    table: &JoinedTable,
    table_cursor_id: Option<CursorID>,
    multi_idx_op: &MultiIndexScanOp,
    mode: &OperationMode,
) -> Result<()> {
    match mode {
        OperationMode::SELECT => {
            let Table::BTree(btree) = &table.table else {
                panic!("Expected multi-index scan table to be a BTree table");
            };
            if let Some(table_cursor_id) = table_cursor_id {
                program.emit_insn(Insn::OpenRead {
                    cursor_id: table_cursor_id,
                    root_page: btree.root_page,
                    db: table.database_id,
                });
            }
            for branch in &multi_idx_op.branches {
                if let Some(index) = &branch.index {
                    let branch_cursor_id = program.alloc_cursor_index(
                        Some(CursorKey::index(table.internal_id, index.clone())),
                        index,
                    )?;
                    program.emit_insn(Insn::OpenRead {
                        cursor_id: branch_cursor_id,
                        root_page: index.root_page,
                        db: table.database_id,
                    });
                }
            }
        }
        _ => unreachable!("Multi-index scans should only occur in SELECT operations"),
    }
    Ok(())
}

/// Include all tables needed by the join order plus hash-join build tables.
fn required_joined_table_indices(
    tables: &TableReferences,
    join_order: &[JoinOrderMember],
) -> HashSet<usize> {
    let mut required_tables: HashSet<usize> = join_order
        .iter()
        .map(|member| member.original_idx)
        .collect();
    for table in tables.joined_tables() {
        if let Operation::HashJoin(hash_join_op) = &table.op {
            required_tables.insert(hash_join_op.build_table_idx);
        }
    }
    required_tables
}

/// Map joined table index -> join-order position for O(1) lookup.
fn join_order_position_by_table_index(
    join_order: &[JoinOrderMember],
    table_count: usize,
) -> Vec<Option<usize>> {
    let mut positions = vec![None; table_count];
    for (join_idx, join_member) in join_order.iter().enumerate() {
        positions[join_member.original_idx] = Some(join_idx);
    }
    positions
}

/// Allocates join metadata (LEFT / SEMI / ANTI) for one joined table.
fn init_table_join_metadata(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &TableReferences,
    join_order: &[JoinOrderMember],
    join_positions: &[Option<usize>],
    table_index: usize,
    table: &JoinedTable,
) {
    if let Some(join_info) = table.join_info.as_ref() {
        if join_info.is_outer() {
            let lj_metadata = LeftJoinMetadata {
                reg_match_flag: program.alloc_register(),
                label_match_flag_set_true: program.allocate_label(),
                label_match_flag_check_value: program.allocate_label(),
            };
            t_ctx.meta_left_joins[table_index] = Some(lj_metadata);
        }
        if join_info.is_semi_or_anti() {
            let join_idx = join_positions[table_index].expect("table must be in join_order");
            let outer_table_idx =
                find_previous_non_semi_anti_table_idx(join_order, tables.joined_tables(), join_idx);
            // For hash join probe tables, loop_labels.next points to the probe
            // cursor's Next (which advances to the next outer row), but we need
            // to jump to the HashNext (which advances to the next hash match
            // for the current outer row). We allocate a fresh label here and
            // resolve it in close_loop at the right point.
            let sa_metadata = SemiAntiJoinMetadata {
                label_body: program.allocate_label(),
                label_next_outer: program.allocate_label(),
                outer_table_idx,
            };
            t_ctx.meta_semi_anti_joins[table_index] = Some(sa_metadata);
        }
    }
}

#[allow(clippy::too_many_arguments)]
/// Opens the cursors needed by one operation kind.
fn init_table_cursors_for_operation(
    program: &mut ProgramBuilder,
    t_ctx: &TranslateCtx,
    table: &JoinedTable,
    op: &Operation,
    table_cursor_id: Option<CursorID>,
    index_cursor_id: Option<CursorID>,
    mode: &OperationMode,
) -> Result<()> {
    match op {
        Operation::Scan(Scan::BTreeTable { index, .. }) => {
            init_cursor_btree_scan(
                program,
                t_ctx,
                table,
                index,
                table_cursor_id,
                index_cursor_id,
                mode,
            )?;
        }
        Operation::Scan(Scan::VirtualTable { .. }) => {
            init_cursor_virtual_table(program, t_ctx, table, table_cursor_id, mode)?;
        }
        Operation::Scan(_) => {}
        Operation::Search(search) => {
            init_cursor_search(
                program,
                t_ctx,
                table,
                search,
                table_cursor_id,
                index_cursor_id,
                mode,
            )?;
        }
        Operation::IndexMethodQuery(_) => {
            init_cursor_index_method_query(program, table, table_cursor_id, index_cursor_id, mode)?;
        }
        Operation::HashJoin(_) => {
            init_cursor_hash_join_probe(program, table, table_cursor_id, mode)?;
        }
        Operation::MultiIndexScan(multi_idx_op) => {
            init_cursor_multi_index_scan(program, table, table_cursor_id, multi_idx_op, mode)?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
/// Initializes one required table before main-loop execution starts.
fn init_required_joined_table(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &TableReferences,
    join_order: &[JoinOrderMember],
    join_positions: &[Option<usize>],
    table_index: usize,
    table: &JoinedTable,
    mode: &OperationMode,
) -> Result<()> {
    // Ensure attached databases have a Transaction instruction for read access.
    if crate::is_attached_db(table.database_id) {
        let schema_cookie = t_ctx
            .resolver
            .with_schema(table.database_id, |s| s.schema_version);
        program.begin_read_on_database(table.database_id, schema_cookie);
    }

    init_table_join_metadata(
        program,
        t_ctx,
        tables,
        join_order,
        join_positions,
        table_index,
        table,
    );

    let (table_cursor_id, index_cursor_id) =
        table.open_cursors(program, mode.clone(), t_ctx.resolver.schema())?;
    init_table_cursors_for_operation(
        program,
        t_ctx,
        table,
        &table.op,
        table_cursor_id,
        index_cursor_id,
        mode,
    )?;
    Ok(())
}

/// Emits WHERE terms that must run before entering the loop nest.
fn emit_before_loop_conditions(
    program: &mut ProgramBuilder,
    resolver: &Resolver<'_>,
    tables: &TableReferences,
    where_clause: &[WhereTerm],
    join_order: &[JoinOrderMember],
    subqueries: &[NonFromClauseSubquery],
    fail_target: BranchOffset,
) -> Result<()> {
    for cond in where_clause
        .iter()
        .filter(|c| c.should_eval_before_loop(join_order, subqueries, Some(tables)))
    {
        emit_condition_with_fail_target(program, resolver, tables, &cond.expr, fail_target)?;
    }
    Ok(())
}

/// Initialize resources needed for the source operators (tables, joins, etc)
#[allow(clippy::too_many_arguments)]
pub fn init_loop(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx,
    tables: &TableReferences,
    aggregates: &mut [Aggregate],
    mode: OperationMode,
    where_clause: &[WhereTerm],
    join_order: &[JoinOrderMember],
    subqueries: &mut [NonFromClauseSubquery],
) -> Result<()> {
    turso_assert_eq!(
        t_ctx.meta_left_joins.len(),
        tables.joined_tables().len(),
        "meta_left_joins length must match tables length"
    );

    if matches!(
        &mode,
        OperationMode::INSERT | OperationMode::UPDATE { .. } | OperationMode::DELETE
    ) {
        turso_assert_eq!(tables.joined_tables().len(), 1);
        let changed_table = &tables.joined_tables()[0].table;
        let prepared =
            prepare_cdc_if_necessary(program, t_ctx.resolver.schema(), changed_table.get_name())?;
        if let Some((cdc_cursor_id, _)) = prepared {
            t_ctx.cdc_cursor_id = Some(cdc_cursor_id);
        }
    }

    // Initialize distinct aggregates using hash tables
    for agg in aggregates.iter_mut().filter(|agg| agg.is_distinct()) {
        turso_assert_eq!(
            agg.args.len(),
            1,
            "DISTINCT aggregate functions must have exactly one argument"
        );
        let collations = vec![
            get_collseq_from_expr(&agg.original_expr, tables)?.unwrap_or(CollationSeq::Binary)
        ];
        let hash_table_id = program.alloc_hash_table_id();
        agg.distinctness = Distinctness::Distinct {
            ctx: Some(DistinctCtx {
                hash_table_id,
                collations,
                label_on_conflict: program.allocate_label(),
            }),
        };
        emit_explain!(
            program,
            false,
            format!("USE HASH TABLE FOR {}(DISTINCT)", agg.func)
        );
    }
    let required_tables = required_joined_table_indices(tables, join_order);
    let join_positions =
        join_order_position_by_table_index(join_order, tables.joined_tables().len());

    for (table_index, table) in tables.joined_tables().iter().enumerate() {
        if !required_tables.contains(&table_index) {
            continue;
        }
        init_required_joined_table(
            program,
            t_ctx,
            tables,
            join_order,
            &join_positions,
            table_index,
            table,
            &mode,
        )?;
    }

    emit_before_loop_conditions(
        program,
        &t_ctx.resolver,
        tables,
        where_clause,
        join_order,
        subqueries,
        t_ctx.label_main_loop_end.unwrap(),
    )?;

    Ok(())
}
