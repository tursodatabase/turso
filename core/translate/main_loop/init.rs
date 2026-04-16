use super::*;

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

/// First step of Loop emission, opens cursors for all tables and initializes distinct aggregate
/// hash tables. Also emits condition checks for any WHERE clause terms that need to be evaluated
/// before the loop (e.g. those that reference only tables that are on the outermost level of the
/// join order).
pub struct InitLoop;
impl InitLoop {
    #[allow(clippy::too_many_arguments)]
    pub fn emit<'a>(
        program: &mut ProgramBuilder,
        t_ctx: &mut TranslateCtx<'a>,
        tables: &TableReferences,
        aggregates: &mut [Aggregate],
        mode: &OperationMode,
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
            let prepared = prepare_cdc_if_necessary(
                program,
                t_ctx.resolver.schema(),
                changed_table.get_name(),
            )?;
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
            let collations =
                vec![get_collseq_from_expr(&agg.original_expr, tables)?
                    .unwrap_or(CollationSeq::Binary)];
            let hash_table_id = program.alloc_hash_table_id();
            agg.distinctness = Distinctness::Distinct {
                ctx: Some(DistinctCtx {
                    hash_table_id,
                    collations,
                    label_on_conflict: program.allocate_label(),
                }),
            };
            // DISTINCT aggregate hash tables live in ProgramState, so a correlated
            // subquery can re-enter with rows from the previous invocation still
            // recorded unless we clear the seen-set here.
            program.emit_insn(Insn::HashClear { hash_table_id });
            emit_explain!(
                program,
                false,
                format!("USE HASH TABLE FOR {}(DISTINCT)", agg.func)
            );
        }
        // Include hash-join build tables so their cursors are opened for hash build.
        let mut required_tables: TableMask = join_order
            .iter()
            .map(|member| member.original_idx)
            .collect();
        for table in tables.joined_tables().iter() {
            if let Operation::HashJoin(hash_join_op) = &table.op {
                required_tables.set(hash_join_op.build_table_idx);
            }
        }

        for (table_index, table) in tables.joined_tables().iter().enumerate() {
            if !required_tables.get(table_index) {
                continue;
            }
            // Ensure non-main databases have a Transaction instruction for read access.
            let schema_cookie = t_ctx
                .resolver
                .with_schema(table.database_id, |s| s.schema_version);
            program.begin_read_on_database(table.database_id, schema_cookie);
            // Initialize bookkeeping for OUTER JOIN
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
                    let join_idx = join_order
                        .iter()
                        .position(|m| m.original_idx == table_index)
                        .expect("table must be in join_order");
                    let outer_table_idx =
                        find_non_semi_anti_ancestor(join_order, tables.joined_tables(), join_idx);
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
            let (table_cursor_id, index_cursor_id) =
                table.open_cursors(program, mode.clone(), t_ctx.resolver.schema())?;
            match &table.op {
                Operation::Scan(Scan::BTreeTable { index, .. }) => match (&mode, &table.table) {
                    (OperationMode::SELECT, Table::BTree(btree)) => {
                        let root_page = btree.root_page;
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
                    (OperationMode::DELETE, Table::BTree(btree)) => {
                        let root_page = btree.root_page;
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
                        // For delete, we need to open all the other indexes too for writing
                        let indices: Vec<_> = t_ctx.resolver.with_schema(table.database_id, |s| {
                            s.get_indices(table.table.get_name()).cloned().collect()
                        });
                        for index in &indices {
                            if table
                                .op
                                .index()
                                .is_some_and(|table_index| table_index.name == index.name)
                            {
                                continue;
                            }
                            let cursor_id = program.alloc_cursor_index(
                                Some(CursorKey::index(table.internal_id, index.clone())),
                                index,
                            )?;
                            program.emit_insn(Insn::OpenWrite {
                                cursor_id,
                                root_page: index.root_page.into(),
                                db: table.database_id,
                            });
                        }
                    }
                    (OperationMode::UPDATE(update_mode), Table::BTree(btree)) => {
                        let root_page = btree.root_page;
                        match &update_mode {
                            UpdateRowSource::Normal => {
                                program.emit_insn(Insn::OpenWrite {
                                    cursor_id: table_cursor_id.expect(
                                        "table cursor is always opened in OperationMode::UPDATE",
                                    ),
                                    root_page: root_page.into(),
                                    db: table.database_id,
                                });
                            }
                            UpdateRowSource::PrebuiltEphemeralTable { target_table, .. } => {
                                let target_table_cursor_id = program
                                    .resolve_cursor_id(&CursorKey::table(target_table.internal_id));
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
                },
                Operation::Scan(Scan::VirtualTable { .. }) => {
                    if let Table::Virtual(tbl) = &table.table {
                        let is_write = matches!(
                            mode,
                            OperationMode::INSERT
                                | OperationMode::UPDATE { .. }
                                | OperationMode::DELETE
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
                }
                Operation::Scan(_) => {}
                Operation::Search(search) => {
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
                                let indices: Vec<_> =
                                    t_ctx.resolver.with_schema(table.database_id, |s| {
                                        s.get_indices(table.table.get_name()).cloned().collect()
                                    });
                                for index in &indices {
                                    if table
                                        .op
                                        .index()
                                        .is_some_and(|table_index| table_index.name == index.name)
                                    {
                                        continue;
                                    }
                                    let cursor_id = program.alloc_cursor_index(
                                        Some(CursorKey::index(table.internal_id, index.clone())),
                                        index,
                                    )?;
                                    program.emit_insn(Insn::OpenWrite {
                                        cursor_id,
                                        root_page: index.root_page.into(),
                                        db: table.database_id,
                                    });
                                }
                            }
                        }
                        _ => {
                            return Err(crate::LimboError::InternalError(
                                "INSERT mode is not supported for Search operations".to_string(),
                            ));
                        }
                    }

                    let search_index = match search {
                        Search::Seek {
                            index: Some(index), ..
                        }
                        | Search::InSeek {
                            index: Some(index), ..
                        } => Some(index),
                        _ => None,
                    };
                    if let Some(index) = search_index {
                        // Ephemeral index cursor are opened ad-hoc when needed.
                        if !index.ephemeral {
                            match mode {
                                OperationMode::SELECT => {
                                    program.emit_insn(Insn::OpenRead {
                                        cursor_id: index_cursor_id.expect(
                                            "index cursor is always opened in Seek with index",
                                        ),
                                        root_page: index.root_page,
                                        db: table.database_id,
                                    });
                                }
                                OperationMode::UPDATE { .. } | OperationMode::DELETE => {
                                    program.emit_insn(Insn::OpenWrite {
                                        cursor_id: index_cursor_id.expect(
                                            "index cursor is always opened in Seek with index",
                                        ),
                                        root_page: index.root_page.into(),
                                        db: table.database_id,
                                    });
                                }
                                _ => {
                                    return Err(crate::LimboError::InternalError(
                                    "INSERT mode is not supported for indexed Search operations"
                                        .to_string(),
                                ));
                                }
                            }
                        }
                    }
                }
                Operation::IndexMethodQuery(_) => match mode {
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
                    OperationMode::DELETE => {
                        if let Some(table_cursor_id) = table_cursor_id {
                            program.emit_insn(Insn::OpenWrite {
                                cursor_id: table_cursor_id,
                                root_page: table.table.get_root_page()?.into(),
                                db: table.database_id,
                            });
                        }
                        let index_cursor_id = index_cursor_id.expect("index cursor is always opened in OperationMode::DELETE for IndexMethodQuery");
                        program.emit_insn(Insn::OpenWrite {
                            cursor_id: index_cursor_id,
                            root_page: table.op.index().expect("index to exist").root_page.into(),
                            db: table.database_id,
                        });
                        let indices: Vec<_> = t_ctx.resolver.with_schema(table.database_id, |s| {
                            s.get_indices(table.table.get_name()).cloned().collect()
                        });
                        for index in &indices {
                            if table
                                .op
                                .index()
                                .is_some_and(|table_index| table_index.name == index.name)
                            {
                                continue;
                            }
                            let cursor_id = program.alloc_cursor_index(
                                Some(CursorKey::index(table.internal_id, index.clone())),
                                index,
                            )?;
                            program.emit_insn(Insn::OpenWrite {
                                cursor_id,
                                root_page: index.root_page.into(),
                                db: table.database_id,
                            });
                        }
                    }
                    OperationMode::UPDATE { .. } => {
                        let table_cursor_id = table_cursor_id.expect(
                        "table cursor is always opened in OperationMode::UPDATE for IndexMethodQuery",
                    );
                        program.emit_insn(Insn::OpenWrite {
                            cursor_id: table_cursor_id,
                            root_page: table.table.get_root_page()?.into(),
                            db: table.database_id,
                        });
                        let index_cursor_id = index_cursor_id.unwrap();
                        program.emit_insn(Insn::OpenWrite {
                            cursor_id: index_cursor_id,
                            root_page: table.op.index().expect("index to exist").root_page.into(),
                            db: table.database_id,
                        });
                    }
                    _ => panic!("Unsupported operation mode for index method"),
                },
                Operation::HashJoin(_) => {
                    match mode {
                        OperationMode::SELECT => {
                            // Open probe table cursor, the build table cursor should already be open from a previous iteration.
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
                }
                Operation::MultiIndexScan(multi_idx_op) => {
                    match mode {
                        OperationMode::SELECT => {
                            let Table::BTree(btree) = &table.table else {
                                panic!("Expected multi-index scan table to be a BTree table");
                            };
                            // Open the table cursor
                            if let Some(table_cursor_id) = table_cursor_id {
                                program.emit_insn(Insn::OpenRead {
                                    cursor_id: table_cursor_id,
                                    root_page: btree.root_page,
                                    db: table.database_id,
                                });
                            }
                            // Open cursors for each index branch
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
                        _ => {
                            unreachable!("Multi-index scans should only occur in SELECT operations")
                        }
                    }
                }
            }
        }

        for cond in where_clause
            .iter()
            .filter(|c| c.should_eval_before_loop(join_order, subqueries, Some(tables)))
        {
            let jump_target = program.allocate_label();
            let meta = ConditionMetadata {
                jump_if_condition_is_true: false,
                jump_target_when_true: jump_target,
                jump_target_when_false: t_ctx.label_main_loop_end.expect(
                    "main_loop_end label should be set before emitting condition expressions",
                ),
                jump_target_when_null: t_ctx.label_main_loop_end.expect(
                    "main_loop_end label should be set before emitting condition expressions",
                ),
            };
            translate_condition_expr(program, tables, &cond.expr, meta, &t_ctx.resolver)?;
            program.preassign_label_to_next_insn(jump_target);
        }

        Ok(())
    }
}
